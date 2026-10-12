#!/usr/bin/env bash
# Small, dependency-free load test against a running Kind deployment (curl + xargs only).
# Sends N transactions at concurrency C, then reconciles database rows, outbox state and canonical Kafka
# messages. Optionally kills the platform pod mid-burst (KILL_POD=1) to measure replay under failure.
# Gates: zero lost events, p95 ingest latency and error rate within budget.
set -euo pipefail

NS="${NAMESPACE:-event-driven-lab}"
TOTAL="${LOAD_TOTAL:-500}"
CONCURRENCY="${LOAD_CONCURRENCY:-20}"
P95_BUDGET_MS="${LOAD_P95_BUDGET_MS:-1500}"
MAX_ERROR_PCT="${LOAD_MAX_ERROR_PCT:-1}"
KILL_POD="${KILL_POD:-0}"
RUN="load-$(date +%s)"
TOPIC="eventdrivenmicroservices-outbox-events"
OUT="$(mktemp)"
trap 'kill ${PF:-} 2>/dev/null || true; rm -f "$OUT"' EXIT

pg() {
  kubectl exec -n "$NS" deploy/eventdrivenmicroservices-postgres -- \
    sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tA -c "$1"' _ "$1"
}

kubectl -n "$NS" port-forward deploy/platform-engine 18081:8080 >/dev/null 2>&1 &
PF=$!
sleep 5
AUTH_MODE="${LOAD_AUTH:-bearer}"
PASSWORD="${SPRING_SECURITY_USER_PASSWORD:-local-dev-password}"
b64url() { openssl base64 -A | tr '+/' '-_' | tr -d '='; }
if [ "$AUTH_MODE" = "bearer" ]; then
  JWT_SECRET="$(kubectl -n "$NS" get secret eventdrivenmicroservices-secrets -o jsonpath='{.data.JWT_SECRET_KEY}' | base64 -d)"
  hdr="$(printf '{"alg":"HS256","typ":"JWT"}' | b64url)"
  pl="$(printf '{"sub":"load-test","exp":%d}' $(( $(date +%s) + 3600 )) | b64url)"
  sig="$(printf '%s.%s' "$hdr" "$pl" | openssl dgst -sha256 -hmac "$JWT_SECRET" -binary | b64url)"
  AUTH_HEADER="Authorization: Bearer ${hdr}.${pl}.${sig}"
else
  AUTH_HEADER="Authorization: Basic $(printf 'admin:%s' "$PASSWORD" | base64 -w0)"
fi
export AUTH_HEADER

send() {
  local i="$1"
  # A distinct merchant per request keeps the velocity detector quiet so every event is a plain TransactionCreated.
  curl -s -o /dev/null -w "%{http_code} %{time_total}\n" -H "$AUTH_HEADER" \
    -H "Content-Type: application/json" \
    -d "{\"transactionId\":\"${RUN}-${i}\",\"amount\":10.00,\"currency\":\"USD\",\"merchantId\":\"${RUN}-m-${i}\",\"correlationId\":\"${RUN}-${i}\",\"settlementStatus\":\"PENDING\"}" \
    http://localhost:18081/api/v1/transactions || echo "000 0"
}
export -f send
export RUN

start=$SECONDS
if [ "$KILL_POD" = "1" ]; then
  (sleep 3; kubectl -n "$NS" delete pod -l app=platform-engine --wait=false >/dev/null) &
fi
seq 1 "$TOTAL" | xargs -P "$CONCURRENCY" -I{} bash -c 'send {}' > "$OUT"
elapsed=$((SECONDS - start))
[ "$elapsed" -gt 0 ] || elapsed=1

ok="$(awk '$1==202{c++} END{print c+0}' "$OUT")"
fail=$((TOTAL - ok))
pct() {
  awk -v q="$1" '$1==202{print int($2*1000)}' "$OUT" | sort -n | awk -v q="$1" '{a[NR]=$1} END{if(NR==0){print 0; exit} i=int(NR*q); if(i<NR) i++; print a[i]}'
}
p50="$(pct 0.50)"
p95="$(pct 0.95)"
echo "Ingest: ${ok}/${TOTAL} accepted in ${elapsed}s (~$((TOTAL / elapsed)) req/s), p50=${p50}ms p95=${p95}ms, failed=${fail}"

# Reconcile: every accepted request must have exactly one transaction row and one outbox row, all published.
sleep 10
deadline=$((SECONDS + 120))
while :; do
  rows="$(pg "SELECT count(*) FROM financial_transactions WHERE transaction_id LIKE '${RUN}-%'")"
  outbox="$(pg "SELECT count(*) FROM outbox_events WHERE payload LIKE '%${RUN}-%'")"
  pending="$(pg "SELECT count(*) FROM outbox_events WHERE payload LIKE '%${RUN}-%' AND processed = false")"
  [ "$pending" = "0" ] && break
  [ "$SECONDS" -ge "$deadline" ] && break
  sleep 3
done
published="$(kubectl exec -n "$NS" deploy/eventdrivenmicroservices-kafka -- \
  kafka-console-consumer --bootstrap-server localhost:9092 --topic "$TOPIC" \
  --from-beginning --timeout-ms 20000 2>/dev/null | grep "${RUN}-" | grep -o '"eventId":"[^"]*"' | sort | uniq -c || true)"
distinct="$(printf '%s\n' "$published" | grep -c . || true)"
duplicates="$(printf '%s\n' "$published" | awk '$1>1{d+=$1-1} END{print d+0}')"
echo "Reconcile: rows=${rows} outbox=${outbox} unpublished=${pending} kafka_distinct=${distinct} kafka_duplicates=${duplicates}"

status=0
if [ "$rows" -lt "$ok" ] || [ "$outbox" -lt "$ok" ]; then echo "FAIL: accepted requests missing from database (lost writes)" >&2; status=1; fi
if [ "$pending" != "0" ]; then echo "FAIL: ${pending} outbox rows never published" >&2; status=1; fi
if [ "$distinct" -lt "$outbox" ]; then echo "FAIL: $((outbox - distinct)) outbox events missing from Kafka (lost events)" >&2; status=1; fi
if [ "$p95" -gt "$P95_BUDGET_MS" ]; then echo "FAIL: p95 ${p95}ms exceeds budget ${P95_BUDGET_MS}ms" >&2; status=1; fi
if [ "$KILL_POD" != "1" ] && [ $((fail * 100)) -gt $((TOTAL * MAX_ERROR_PCT)) ]; then
  echo "FAIL: error rate above ${MAX_ERROR_PCT}%" >&2; status=1
fi
[ "$status" = "0" ] && echo "Load test passed (lost=0, duplicates=${duplicates})"
exit "$status"
