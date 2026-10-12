#!/usr/bin/env bash
# Live Debezium CDC drill against the default chart configuration (outbox.publisher.mode=cdc).
# Inserts outbox rows directly, restarts the connector between them, and asserts that each
# row reaches the canonical topic exactly once and is marked processed.
set -euo pipefail

NS="${NAMESPACE:-event-driven-lab}"
TOPIC="eventdrivenmicroservices-outbox-events"
CONNECTOR="eventdrivenmicroservices-postgres-outbox-connector"
CONNECT_URL="http://localhost:8083/connectors/${CONNECTOR}"
TIMEOUT="${DRILL_TIMEOUT_SECONDS:-90}"

pg() {
  kubectl exec -n "$NS" deploy/eventdrivenmicroservices-postgres -- \
    sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tA -c "$1"' _ "$1"
}

connect() {
  kubectl exec -n "$NS" deploy/eventdrivenmicroservices-debezium -- curl --fail --silent --show-error "$@"
}

wait_for() {
  local description="$1"; shift
  local deadline=$((SECONDS + TIMEOUT))
  until "$@" >/dev/null 2>&1; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "FAIL: timed out waiting for ${description}" >&2
      exit 1
    fi
    sleep 2
  done
  echo "OK: ${description}"
}

connector_running() {
  local running
  running="$(connect "${CONNECT_URL}/status" | grep -oE '"state"[[:space:]]*:[[:space:]]*"RUNNING"' | wc -l)"
  [ "$running" -ge 2 ]
}

insert_event() {
  local id="$1"
  pg "INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload)
      VALUES ('${id}', 'CdcDrill', '${id}', 'CdcDrillEvent', '{\"drill\":true}')"
}

is_processed() {
  [ "$(pg "SELECT processed FROM outbox_events WHERE id='$1'")" = "t" ]
}

canonical_count() {
  kubectl exec -n "$NS" deploy/eventdrivenmicroservices-kafka -- \
    kafka-console-consumer --bootstrap-server localhost:9092 --topic "$TOPIC" \
    --from-beginning --timeout-ms 15000 2>/dev/null | grep -c "$1" || true
}

wait_for "connector and task RUNNING" connector_running

first="$(cat /proc/sys/kernel/random/uuid)"
insert_event "$first"
wait_for "first row marked processed" is_processed "$first"

connect --request POST "${CONNECT_URL}/restart?includeTasks=true&onlyFailed=false" >/dev/null
wait_for "connector RUNNING after restart" connector_running

second="$(cat /proc/sys/kernel/random/uuid)"
insert_event "$second"
wait_for "second row marked processed after restart" is_processed "$second"

# Connect pod loss: kill the pod, write while it is down, expect replay from the stored offsets.
kubectl delete pod -n "$NS" -l app=eventdrivenmicroservices-debezium --wait=false >/dev/null
third="$(cat /proc/sys/kernel/random/uuid)"
insert_event "$third"
kubectl rollout status -n "$NS" deploy/eventdrivenmicroservices-debezium --timeout="${TIMEOUT}s"
wait_for "connector RUNNING after Connect pod loss" connector_running
wait_for "third row (written during Connect outage) marked processed" is_processed "$third"

# Bridge crash: kill the platform pod right after the insert. Delivery is at-least-once, so the
# row must end up processed with at least one canonical message (a replay may duplicate it).
fourth="$(cat /proc/sys/kernel/random/uuid)"
insert_event "$fourth"
kubectl delete pod -n "$NS" -l app=platform-engine --wait=false >/dev/null
kubectl rollout status -n "$NS" deploy/platform-engine --timeout="${TIMEOUT}s"
wait_for "fourth row marked processed after bridge pod loss" is_processed "$fourth"

for id in "$first" "$second" "$third"; do
  count="$(canonical_count "$id")"
  if [ "$count" != "1" ]; then
    echo "FAIL: expected exactly one canonical message for ${id}, found ${count}" >&2
    exit 1
  fi
  echo "OK: exactly one canonical message for ${id}"
done

fourth_count="$(canonical_count "$fourth")"
if [ "$fourth_count" -lt 1 ]; then
  echo "FAIL: no canonical message for ${fourth} after bridge crash" >&2
  exit 1
fi
echo "OK: ${fourth_count} canonical message(s) for ${fourth} after bridge crash (at-least-once)"

# Latency: sequential single-event inserts; latency is processed_at - created_at on the same host clock.
LATENCY_SAMPLES="${CDC_LATENCY_SAMPLES:-20}"
LATENCY_P95_BUDGET_MS="${CDC_LATENCY_P95_BUDGET_MS:-500}"
latency_ids=()
for _ in $(seq 1 "$LATENCY_SAMPLES"); do
  id="$(cat /proc/sys/kernel/random/uuid)"
  latency_ids+=("$id")
  insert_event "$id"
  wait_for "latency sample ${id} processed" is_processed "$id"
done
id_list="$(printf "'%s'," "${latency_ids[@]}")"; id_list="${id_list%,}"
stats="$(pg "SELECT round(percentile_cont(0.5) WITHIN GROUP (ORDER BY ms)::numeric), round(percentile_cont(0.95) WITHIN GROUP (ORDER BY ms)::numeric), round(max(ms)::numeric)
  FROM (SELECT extract(epoch FROM (processed_at - created_at)) * 1000 AS ms FROM outbox_events WHERE id IN (${id_list})) s")"
IFS='|' read -r p50 p95 pmax <<<"$stats"
echo "CDC latency over ${LATENCY_SAMPLES} samples: p50=${p50}ms p95=${p95}ms max=${pmax}ms (budget p95 <= ${LATENCY_P95_BUDGET_MS}ms)"
if [ "$p95" -gt "$LATENCY_P95_BUDGET_MS" ]; then
  echo "FAIL: CDC p95 latency ${p95}ms exceeds ${LATENCY_P95_BUDGET_MS}ms" >&2
  exit 1
fi

# Monitoring: alert rules must load without errors and the CDC-mode signals must be scraped.
prom() {
  kubectl exec -n "$NS" deploy/eventdrivenmicroservices-prometheus -- wget -qO- "http://localhost:9090$1"
}
rules_healthy() {
  local rules
  rules="$(prom /api/v1/rules)"
  echo "$rules" | grep -q '"name":"outbox-cdc"' && ! echo "$rules" | grep -q '"health":"err"'
}
metric_present() {
  prom "/api/v1/query?query=$1" | grep -q '"result":\[{'
}
wait_for "Prometheus loaded outbox-cdc alert rules without errors" rules_healthy
for metric in eventdrivenmicroservices_outbox_pending eventdrivenmicroservices_outbox_oldest_pending_age_seconds \
              eventdrivenmicroservices_replication_slot_retained_bytes eventdrivenmicroservices_replication_slot_inactive \
              eventdrivenmicroservices_replication_slot_count eventdrivenmicroservices_outbox_publish_success_total; do
  wait_for "metric ${metric} scraped" metric_present "$metric"
done
slot_count="$(prom '/api/v1/query?query=eventdrivenmicroservices_replication_slot_count' | grep -oE '"value":\[[^]]*\]' | grep -oE '"[0-9.]+"\]$' | tr -d '"]')"
if [ "${slot_count%.*}" -lt 1 ]; then
  echo "FAIL: expected an active logical replication slot, gauge reports ${slot_count}" >&2
  exit 1
fi
echo "OK: replication slot gauge reports ${slot_count} slot(s)"

# One HTTP request must produce a single Tempo trace spanning HTTP, ledger, CDC publish and Kafka consume.
TRACE_ID="$(openssl rand -hex 16)"
kubectl -n "$NS" port-forward deploy/platform-engine 18080:8080 >/dev/null 2>&1 &
PF_APP=$!
kubectl -n "$NS" port-forward deploy/eventdrivenmicroservices-tempo 13200:3200 >/dev/null 2>&1 &
PF_TEMPO=$!
trap 'kill $PF_APP $PF_TEMPO 2>/dev/null || true' EXIT
sleep 5
ADMIN_PASSWORD="$(kubectl -n "$NS" get deploy platform-engine -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="SPRING_SECURITY_USER_PASSWORD")].value}' 2>/dev/null || true)"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-local-dev-password}"
curl -fsS -o /dev/null -u "admin:${ADMIN_PASSWORD}" -H "Content-Type: application/json" \
  -H "traceparent: 00-${TRACE_ID}-b7ad6b7169203331-01" \
  -d '{"transactionId":"trace-drill","amount":15000.00,"currency":"USD","merchantId":"trace-drill-merchant","settlementStatus":"PENDING"}' \
  http://localhost:18080/api/v1/transactions
trace_complete() {
  local body
  body="$(curl -fsS "http://localhost:13200/api/traces/${TRACE_ID}" 2>/dev/null)" || return 1
  for span in 'http post /api/v1/transactions' 'ledger.append' 'outbox.cdc.publish' 'outbox.consume'; do
    echo "$body" | grep -q "\"name\":\"${span}\"" || return 1
  done
}
wait_for "single Tempo trace ${TRACE_ID} spans HTTP, ledger, CDC publish and consume" trace_complete

# Observability pods have no probes, so a crash loop would otherwise look healthy.
for app in prometheus grafana otel-collector tempo loki; do
  restarts="$(kubectl -n "$NS" get pods -l "app=eventdrivenmicroservices-${app}" \
    -o jsonpath='{.items[*].status.containerStatuses[*].restartCount}')"
  phase="$(kubectl -n "$NS" get pods -l "app=eventdrivenmicroservices-${app}" -o jsonpath='{.items[0].status.phase}')"
  if [ "$phase" != "Running" ] || [ "${restarts// /}" != "0" ]; then
    echo "FAIL: ${app} phase=${phase} restartCount='${restarts}'" >&2
    exit 1
  fi
done
echo "OK: observability pods Running with 0 restarts"

echo "CDC drill passed"
