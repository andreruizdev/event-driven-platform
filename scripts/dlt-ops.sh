#!/usr/bin/env bash
# Operator tooling for the outbox dead-letter topic (DLT) in the Kind lab.
#   dlt-ops.sh inspect                      list DLT records (partition/offset/timestamp/headers); payloads are never printed
#   dlt-ops.sh replay <partition> <offset>  re-publish one DLT record to its x-original-topic; requires CONFIRM_REPLAY=true
# Replay pipes the record inside the Kafka pod, so payloads never reach the operator terminal. Replay only after the
# cause (consumer bug, transient dependency) is fixed: an unfixed malformed record is dead-lettered again.
# Replay is at-least-once; consumers dedupe by eventId.
set -euo pipefail

NS="${NAMESPACE:-event-driven-lab}"
DLT="${DLT_TOPIC:-eventdrivenmicroservices-outbox-events.DLT}"
CANONICAL="eventdrivenmicroservices-outbox-events"
TIMEOUT_MS="${DLT_TIMEOUT_MS:-15000}"

kafka_exec() {
  kubectl exec -n "$NS" deploy/eventdrivenmicroservices-kafka -- "$@"
}

inspect() {
  kafka_exec kafka-console-consumer --bootstrap-server localhost:9092 --topic "$DLT" --from-beginning \
    --timeout-ms "$TIMEOUT_MS" --property print.timestamp=true --property print.partition=true \
    --property print.offset=true --property print.headers=true --property print.key=false \
    --property print.value=false 2>/dev/null | grep -v '^Processed a total' || true
}

replay() {
  local partition="$1" offset="$2"
  if [ "${CONFIRM_REPLAY:-false}" != "true" ]; then
    echo "Refusing to replay DLT ${partition}:${offset} without CONFIRM_REPLAY=true" >&2
    exit 2
  fi
  [[ "$partition" =~ ^[0-9]+$ && "$offset" =~ ^[0-9]+$ ]] || { echo "partition and offset must be integers" >&2; exit 2; }

  local headers original
  headers="$(kafka_exec kafka-console-consumer --bootstrap-server localhost:9092 --topic "$DLT" \
    --partition "$partition" --offset "$offset" --max-messages 1 --timeout-ms "$TIMEOUT_MS" \
    --property print.headers=true --property print.value=false 2>/dev/null | head -1)"
  [ -n "$headers" ] || { echo "No DLT record at ${partition}:${offset}" >&2; exit 1; }
  original="$(echo "$headers" | grep -oE 'x-original-topic:[^,[:space:]]+' | head -1 | cut -d: -f2)"
  original="${original:-$CANONICAL}"
  if [ "$original" != "$CANONICAL" ]; then
    echo "Refusing to replay to unexpected topic ${original}" >&2
    exit 2
  fi

  kubectl exec -n "$NS" deploy/eventdrivenmicroservices-kafka -- sh -c '
    set -e
    kafka-console-consumer --bootstrap-server localhost:9092 --topic "$1" --partition "$2" --offset "$3" \
      --max-messages 1 --timeout-ms "$5" --property print.key=true --property key.separator="|" 2>/dev/null \
    | kafka-console-producer --bootstrap-server localhost:9092 --topic "$4" \
      --property parse.key=true --property key.separator="|" >/dev/null' _ "$DLT" "$partition" "$offset" "$original" "$TIMEOUT_MS"
  echo "Replayed DLT ${partition}:${offset} to ${original}"
}

case "${1:-}" in
  inspect) inspect ;;
  replay) [ "$#" -eq 3 ] || { echo "usage: $0 replay <partition> <offset>" >&2; exit 2; }; replay "$2" "$3" ;;
  *) echo "usage: $0 inspect | replay <partition> <offset>" >&2; exit 2 ;;
esac
