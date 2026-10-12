#!/usr/bin/env bash
# Live Debezium CDC drill against the default chart configuration (outbox.publisher.mode=cdc).
# Inserts outbox rows directly and drills connector, broker, and bridge recovery against the
# canonical topic. Normal-path samples require exactly-once publication; outage replay is at-least-once.
set -euo pipefail

NS="${NAMESPACE:-event-driven-lab}"
TOPIC="eventdrivenmicroservices-outbox-events"
CONNECTOR="eventdrivenmicroservices-postgres-outbox-connector"
CONNECT_URL="http://localhost:8083/connectors/${CONNECTOR}"
TIMEOUT="${DRILL_TIMEOUT_SECONDS:-90}"
KAFKA_OUTAGE_ACTIVE=false
PF_APP=""
PF_TEMPO=""
PF_PROM=""

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

is_unprocessed() {
  [ "$(pg "SELECT processed FROM outbox_events WHERE id='$1'")" = "f" ]
}

 cleanup() {
   local port_forward_pids=()
   if [ -n "$PF_APP" ]; then
     port_forward_pids+=("$PF_APP")
   fi
   if [ -n "$PF_TEMPO" ]; then
     port_forward_pids+=("$PF_TEMPO")
   fi
   if [ -n "$PF_PROM" ]; then
     port_forward_pids+=("$PF_PROM")
   fi
   if [ "${#port_forward_pids[@]}" -gt 0 ]; then
     kill "${port_forward_pids[@]}" 2>/dev/null || true
   fi
   if [ "$KAFKA_OUTAGE_ACTIVE" = "true" ] \
       && ! kubectl scale -n "$NS" deployment/eventdrivenmicroservices-kafka --replicas=1 \
           --timeout="${TIMEOUT}s" >/dev/null; then
     echo "WARNING: unable to restore the Kafka deployment to one replica" >&2
   fi
 }
 trap cleanup EXIT

kafka_endpoints_empty() {
  [ -z "$(kubectl get endpoints -n "$NS" eventdrivenmicroservices-kafka \
    -o jsonpath='{.subsets[*].addresses[*].ip}')" ]
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

for id in "$first" "$second" "$third"; do
  count="$(canonical_count "$id")"
  if [ "$count" != "1" ]; then
    echo "FAIL: expected exactly one canonical message for ${id}, found ${count}" >&2
    exit 1
  fi
  echo "OK: exactly one canonical message for ${id}"
done

# The single-broker lab has no persistent Kafka volume, so this drill destroys broker-local
# topics and offsets. Run it only in a disposable cluster with explicit operator opt-in.
kafka_outage=""
if [ "${ALLOW_KAFKA_DATA_LOSS_DRILL:-false}" = "true" ]; then
  echo "Starting Kafka outage drill (broker-local Kafka data will be lost)"
  KAFKA_OUTAGE_ACTIVE=true
  kubectl scale -n "$NS" deployment/eventdrivenmicroservices-kafka --replicas=0 \
    --timeout="${TIMEOUT}s" >/dev/null
  wait_for "Kafka broker endpoint removed" kafka_endpoints_empty

  kafka_outage="$(cat /proc/sys/kernel/random/uuid)"
  insert_event "$kafka_outage"
  wait_for "outbox row remains unprocessed while Kafka is down" is_unprocessed "$kafka_outage"

  kubectl scale -n "$NS" deployment/eventdrivenmicroservices-kafka --replicas=1 \
    --timeout="${TIMEOUT}s" >/dev/null
  kubectl rollout status -n "$NS" deployment/eventdrivenmicroservices-kafka --timeout="${TIMEOUT}s"
  KAFKA_OUTAGE_ACTIVE=false

  kubectl exec -n "$NS" deploy/eventdrivenmicroservices-kafka -- \
    cub kafka-ready -b localhost:9092 1 120 >/dev/null
  kubectl rollout restart -n "$NS" deployment/eventdrivenmicroservices-debezium >/dev/null
  kubectl rollout status -n "$NS" deployment/eventdrivenmicroservices-debezium --timeout="${TIMEOUT}s"
  kubectl exec -n "$NS" deploy/eventdrivenmicroservices-kafka -- \
    kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists \
      --topic eventdrivenmicroservices-outbox-events.DLT --partitions 1 --replication-factor 1 \
      --config cleanup.policy=delete --config retention.ms=604800000 >/dev/null
  connector_config="$(kubectl get configmap -n "$NS" eventdrivenmicroservices-debezium-connector-config \
    -o jsonpath='{.data.postgres-outbox-connector\.json}')"
  connect --request PUT --header 'Content-Type: application/json' \
    --data-binary "$connector_config" "${CONNECT_URL}/config" >/dev/null

  wait_for "connector and task RUNNING after Kafka recovery" connector_running
  wait_for "row written during Kafka outage marked processed after recovery" is_processed "$kafka_outage"

  outage_count="$(canonical_count "$kafka_outage")"
  if [ "$outage_count" -lt 1 ]; then
    echo "FAIL: no canonical message for ${kafka_outage} after Kafka recovery" >&2
    exit 1
  fi
  echo "OK: ${outage_count} canonical message(s) for ${kafka_outage} after Kafka recovery"
else
  echo "SKIP: Kafka outage drill requires ALLOW_KAFKA_DATA_LOSS_DRILL=true; broker-local data is ephemeral"
fi

# Bridge crash: kill the platform pod right after the insert. Delivery is at-least-once, so the
# row must end up processed with at least one canonical message (a replay may duplicate it).
fourth="$(cat /proc/sys/kernel/random/uuid)"
insert_event "$fourth"
kubectl delete pod -n "$NS" -l app=platform-engine --wait=false >/dev/null
kubectl rollout status -n "$NS" deploy/platform-engine --timeout="${TIMEOUT}s"
wait_for "fourth row marked processed after bridge pod loss" is_processed "$fourth"

fourth_count="$(canonical_count "$fourth")"
if [ "$fourth_count" -lt 1 ]; then
  echo "FAIL: no canonical message for ${fourth} after bridge crash" >&2
  exit 1
fi
echo "OK: ${fourth_count} canonical message(s) for ${fourth} after bridge crash (at-least-once)"

# DLT operations: a malformed record must be dead-lettered with original coordinates, inspection must not
# print its payload, replay must be refused without confirmation, and a confirmed replay must reach the topic.
poison="poison-$(cat /proc/sys/kernel/random/uuid)"
dlt_count() { "$(dirname "$0")/dlt-ops.sh" inspect | grep -c 'x-original-topic' || true; }
dlt_before="$(dlt_count)"
echo "$poison" | kubectl exec -i -n "$NS" deploy/eventdrivenmicroservices-kafka -- \
  kafka-console-producer --bootstrap-server localhost:9092 --topic "$TOPIC" >/dev/null
dlt_grew() { [ "$(dlt_count)" -gt "$dlt_before" ]; }
wait_for "malformed record dead-lettered" dlt_grew
dlt_listing="$("$(dirname "$0")/dlt-ops.sh" inspect)"
if echo "$dlt_listing" | grep -q "$poison"; then
  echo "FAIL: DLT inspection exposed a payload" >&2
  exit 1
fi
echo "$dlt_listing" | grep -q "x-original-topic:${TOPIC}" || { echo "FAIL: DLT record lacks original topic" >&2; exit 1; }
echo "OK: DLT inspection shows original coordinates without payloads"
dlt_last="$(echo "$dlt_listing" | grep 'x-original-topic' | tail -1)"
dlt_partition="$(echo "$dlt_last" | grep -oE 'Partition:[0-9]+' | cut -d: -f2)"
dlt_offset="$(echo "$dlt_last" | grep -oE 'Offset:[0-9]+' | cut -d: -f2)"
if "$(dirname "$0")/dlt-ops.sh" replay "$dlt_partition" "$dlt_offset" >/dev/null 2>&1; then
  echo "FAIL: replay ran without CONFIRM_REPLAY=true" >&2
  exit 1
fi
echo "OK: replay refused without confirmation"
dlt_before="$(dlt_count)"
CONFIRM_REPLAY=true "$(dirname "$0")/dlt-ops.sh" replay "$dlt_partition" "$dlt_offset"
wait_for "replayed malformed record dead-lettered again (replay reached the canonical topic)" dlt_grew

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
prom_query() {
  curl -fsSG "http://localhost:19090/api/v1/query" --data-urlencode "query=$1"
}
prom_query_valid() {
  prom_query "$1" | grep -q '"status":"success"'
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
              eventdrivenmicroservices_replication_slot_count eventdrivenmicroservices_outbox_publish_success_total \
              kafka_consumer_fetch_manager_records_lag_max eventdrivenmicroservices_connector_running \
              eventdrivenmicroservices_connector_tasks_running eventdrivenmicroservices_connector_tasks_failed \
              eventdrivenmicroservices_outbox_publish_duration_seconds_bucket eventdrivenmicroservices_outbox_retry_total \
              eventdrivenmicroservices_outbox_processing_failure_total; do
  wait_for "metric ${metric} scraped" metric_present "$metric"
done
kubectl -n "$NS" port-forward svc/eventdrivenmicroservices-prometheus 19090:9090 >/dev/null 2>&1 &
PF_PROM=$!
wait_for "Prometheus query API port-forward" prom_query_valid eventdrivenmicroservices_connector_running
for query in \
    'rate(eventdrivenmicroservices_anomalies_detected_total[1m])' \
    'eventdrivenmicroservices_anomaly_latest_zscore' \
    'sum(rate(http_server_requests_seconds_count[1m])) by (uri, status)' \
    'eventdrivenmicroservices_outbox_pending' \
    'eventdrivenmicroservices_outbox_oldest_pending_age_seconds' \
    'rate(eventdrivenmicroservices_outbox_publish_success_total[5m])' \
    'rate(eventdrivenmicroservices_outbox_processing_failure_total[5m])' \
    'rate(eventdrivenmicroservices_outbox_retry_total[5m])' \
    'histogram_quantile(0.95, sum(rate(eventdrivenmicroservices_outbox_publish_duration_seconds_bucket[5m])) by (le))' \
    'eventdrivenmicroservices_connector_running' \
    'eventdrivenmicroservices_connector_tasks_running' \
    'eventdrivenmicroservices_connector_tasks_failed' \
    'max(kafka_consumer_fetch_manager_records_lag_max)' \
    'eventdrivenmicroservices_replication_slot_retained_bytes' \
    'eventdrivenmicroservices_replication_slot_inactive' \
    'eventdrivenmicroservices_replication_slot_count'; do
  wait_for "live Prometheus query is valid: ${query}" prom_query_valid "$query"
done
echo "OK: live Prometheus accepted every dashboard PromQL expression"
slot_count="$(prom '/api/v1/query?query=eventdrivenmicroservices_replication_slot_count' | grep -oE '"value":\[[^]]*\]' | grep -oE '"[0-9.]+"\]$' | tr -d '"]')"
if [ "${slot_count%.*}" -lt 1 ]; then
  echo "FAIL: expected an active logical replication slot, gauge reports ${slot_count}" >&2
  exit 1
fi
echo "OK: replication slot gauge reports ${slot_count} slot(s)"

connector_up() {
  prom '/api/v1/query?query=eventdrivenmicroservices_connector_running' | grep -qE '"value":\[[^]]*"1"\]'
}
wait_for "Connect reports the connector RUNNING" connector_up

# Alerting: pausing the connector must fire DebeziumConnectorNotRunning, and resuming must clear it.
alert_state() {
  prom /api/v1/alerts | grep -o '"alertname":"'"$1"'"[^}]*}[^]]*"state":"[a-z]*"' | grep -o '"state":"[a-z]*"' | head -1
}
alert_firing() { [ "$(alert_state DebeziumConnectorNotRunning)" = '"state":"firing"' ]; }
alert_cleared() { [ -z "$(alert_state DebeziumConnectorNotRunning)" ]; }
connect -X PUT "${CONNECT_URL}/pause" >/dev/null
TIMEOUT=180 wait_for "DebeziumConnectorNotRunning alert FIRING while the connector is paused" alert_firing
connect -X PUT "${CONNECT_URL}/resume" >/dev/null
wait_for "connector RUNNING after resume" connector_running
TIMEOUT=180 wait_for "DebeziumConnectorNotRunning alert cleared after resume" alert_cleared

# One HTTP request must produce a single Tempo trace spanning HTTP, ledger, CDC publish and Kafka consume.
TRACE_ID="$(openssl rand -hex 16)"
kubectl -n "$NS" port-forward deploy/platform-engine 18080:8080 >/dev/null 2>&1 &
PF_APP=$!
kubectl -n "$NS" port-forward deploy/eventdrivenmicroservices-tempo 13200:3200 >/dev/null 2>&1 &
PF_TEMPO=$!
sleep 5
ADMIN_PASSWORD="${ADMIN_PASSWORD:-$(kubectl -n "$NS" get secret eventdrivenmicroservices-secrets -o jsonpath='{.data.ADMIN_PASSWORD}' | base64 -d)}"
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
