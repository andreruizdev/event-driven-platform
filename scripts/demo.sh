#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
USERNAME="${SPRING_SECURITY_USER_NAME:-admin}"
PASSWORD="${SPRING_SECURITY_USER_PASSWORD:-local-dev-password}"
APPLICANT_ID="11111111-1111-4111-8111-111111111111"
MERCHANT_ID="demo-merchant-001"

request() {
  local method="$1"
  local path="$2"
  local body="${3:-}"
  if [[ -n "$body" ]]; then
    curl --fail-with-body --silent --show-error \
      --user "$USERNAME:$PASSWORD" \
      --header "Content-Type: application/json" \
      --request "$method" \
      --data "$body" \
      "$BASE_URL$path"
  else
    curl --fail-with-body --silent --show-error \
      --user "$USERNAME:$PASSWORD" \
      --request "$method" \
      "$BASE_URL$path"
  fi
}

echo "EventDrivenMicroservices demo"
echo "Target: $BASE_URL"

echo
printf '%s\n' '[1/5] Checking application health...'
request GET /actuator/health

echo
printf '%s\n' '[2/5] Submitting a loan application...'
request POST /api/v1/loans "{\"applicantId\":\"$APPLICANT_ID\",\"amount\":25000.00,\"termMonths\":36}"

echo
printf '%s\n' '[3/5] Sending a normal transaction...'
request POST /api/v1/transactions "{\"transactionId\":\"demo-normal-001\",\"amount\":100.00,\"currency\":\"USD\",\"merchantId\":\"$MERCHANT_ID\",\"correlationId\":\"demo-normal-001\",\"settlementStatus\":\"PENDING\"}"

echo
printf '%s\n' '[4/5] Sending a velocity burst...'
for index in 1 2 3 4 5 6; do
  request POST /api/v1/transactions "{\"transactionId\":\"demo-burst-$index\",\"amount\":2000.00,\"currency\":\"USD\",\"merchantId\":\"$MERCHANT_ID\",\"correlationId\":\"demo-burst-$index\",\"settlementStatus\":\"PENDING\"}" >/dev/null
done
printf 'Submitted six transactions for %s.\n' "$MERCHANT_ID"

echo
printf '%s\n' '[5/5] Reading outbox and ledger evidence...'
echo 'Outbox status:'
request GET /api/v1/outbox/status
echo
echo 'Latest ledger hash:'
request GET /api/v1/ledger/latest-hash
echo
echo 'Ledger events:'
request GET /api/v1/ledger
echo
echo 'Demo complete. Use the correlation IDs above with application logs and the observability stack.'
