#!/usr/bin/env bash
# Live Debezium CDC drill. Requires a cluster deployed with --set outbox.publisher.mode=cdc.
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

for id in "$first" "$second"; do
  count="$(canonical_count "$id")"
  if [ "$count" != "1" ]; then
    echo "FAIL: expected exactly one canonical message for ${id}, found ${count}" >&2
    exit 1
  fi
  echo "OK: exactly one canonical message for ${id}"
done

echo "CDC drill passed"
