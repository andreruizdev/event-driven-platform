#!/usr/bin/env bash
# Assertion-based demo: exits non-zero if any step does not behave as documented.
# Requires curl and jq. For a Helm/Kind deployment pass the cluster secret:
#   ADMIN_PASSWORD="$(kubectl get secret eventdrivenmicroservices-secrets -o jsonpath='{.data.ADMIN_PASSWORD}' | base64 -d)" ./scripts/demo.sh
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
USERNAME="${SPRING_SECURITY_USER_NAME:-admin}"
PASSWORD="${SPRING_SECURITY_USER_PASSWORD:-${ADMIN_PASSWORD:-local-dev-password}}"
DRAIN_TIMEOUT_SECONDS="${DRAIN_TIMEOUT_SECONDS:-60}"
RUN_ID="$(date +%s)-$RANDOM"
APPLICANT_ID="11111111-1111-4111-8111-111111111111"
MERCHANT_ID="demo-merchant-$RUN_ID"

command -v jq >/dev/null || { echo "jq is required" >&2; exit 2; }

PASSED=0
fail() { echo "FAIL: $*" >&2; exit 1; }
pass() { PASSED=$((PASSED + 1)); echo "PASS: $*"; }

request() {
  local method="$1" path="$2" body="${3:-}"
  local args=(--fail-with-body --silent --show-error --user "$USERNAME:$PASSWORD" --request "$method")
  if [[ -n "$body" ]]; then
    args+=(--header "Content-Type: application/json" --data "$body")
  fi
  curl "${args[@]}" "$BASE_URL$path"
}

echo "EventDrivenMicroservices demo"
echo "Target: $BASE_URL (run $RUN_ID)"

echo
echo '[1/6] Health'
status="$(request GET /actuator/health | jq -r '.status')"
[[ "$status" == "UP" ]] || fail "health status is '$status'"
pass "application health is UP"

before="$(request GET /api/v1/ledger/verify | jq -r '.eventsChecked')"

echo
echo '[2/6] Loan application'
loan="$(request POST /api/v1/loans "{\"applicantId\":\"$APPLICANT_ID\",\"amount\":25000.00,\"termMonths\":36}")"
loan_id="$(jq -r '.id // empty' <<<"$loan")"
[[ -n "$loan_id" ]] || fail "loan response has no id: $loan"
pass "loan accepted with id $loan_id"

echo
echo '[3/6] Normal transaction and velocity burst'
submitted=0
for index in normal 1 2 3 4 5 6; do
  amount=2000.00
  [[ "$index" == "normal" ]] && amount=100.00
  txid="demo-$RUN_ID-$index"
  response="$(request POST /api/v1/transactions "{\"transactionId\":\"$txid\",\"amount\":$amount,\"currency\":\"USD\",\"merchantId\":\"$MERCHANT_ID\",\"correlationId\":\"$txid\",\"settlementStatus\":\"PENDING\"}")"
  [[ "$response" == "Transaction Accepted" ]] || fail "transaction $txid answered '$response'"
  submitted=$((submitted + 1))
done
pass "$submitted transactions accepted for $MERCHANT_ID"

echo
echo '[4/6] Outbox drains'
deadline=$((SECONDS + DRAIN_TIMEOUT_SECONDS))
pending=-1
while (( SECONDS < deadline )); do
  pending="$(request GET /api/v1/outbox/status | jq -r '.pendingCount')"
  [[ "$pending" == "0" ]] && break
  sleep 1
done
[[ "$pending" == "0" ]] || fail "outbox still has $pending pending events after ${DRAIN_TIMEOUT_SECONDS}s"
pass "outbox has no pending events"

echo
echo '[5/6] Ledger growth and integrity'
verify="$(request GET /api/v1/ledger/verify)"
valid="$(jq -r '.valid' <<<"$verify")"
after="$(jq -r '.eventsChecked' <<<"$verify")"
[[ "$valid" == "true" ]] || fail "ledger chain invalid: $verify"
(( after - before >= 2 )) || fail "ledger grew by $((after - before)); expected the loan plus at least one flagged anomaly"
pass "ledger chain valid; grew by $((after - before)) events"
hash="$(request GET /api/v1/ledger/latest-hash | jq -r '.currentHash')"
[[ "${#hash}" == 64 ]] || fail "latest hash is not a SHA-256 hex digest: '$hash'"
pass "latest ledger hash is a 64-character digest"

echo
echo '[6/6] Outbox evidence'
recent="$(request GET /api/v1/outbox/recent)"
jq -e 'length > 0' <<<"$recent" >/dev/null || fail "no recent outbox events"
pass "recent outbox events are listed"

echo
echo "Demo complete: $PASSED checks passed. Correlation IDs are demo-$RUN_ID-*."
