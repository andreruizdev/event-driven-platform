#!/usr/bin/env bash
# PostgreSQL durability drill: writes a marker row, deletes the PostgreSQL pod, waits for its
# replacement, and asserts the marker row survived. Requires persistence.enabled (the chart default).
set -euo pipefail

NS="${NAMESPACE:-event-driven-lab}"
DEPLOY="deploy/eventdrivenmicroservices-postgres"
TIMEOUT="${DRILL_TIMEOUT_SECONDS:-180}"

pg() {
  kubectl exec -n "$NS" "$DEPLOY" -- \
    sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tA -c "$1"' _ "$1"
}

marker="$(cat /proc/sys/kernel/random/uuid)"
pg "INSERT INTO loan_applications (applicant_id, amount, term_months, status)
    VALUES ('durability-${marker}', 1, 1, 'DRILL')"
echo "OK: wrote marker ${marker}"

old_pod="$(kubectl get pod -n "$NS" -l app=eventdrivenmicroservices-postgres -o jsonpath='{.items[0].metadata.name}')"
kubectl delete pod -n "$NS" "$old_pod" --wait=true
kubectl rollout status -n "$NS" "$DEPLOY" --timeout="${TIMEOUT}s"
kubectl wait -n "$NS" --for=condition=ready pod -l app=eventdrivenmicroservices-postgres --timeout="${TIMEOUT}s"
new_pod="$(kubectl get pod -n "$NS" -l app=eventdrivenmicroservices-postgres -o jsonpath='{.items[0].metadata.name}')"
if [ "$new_pod" = "$old_pod" ]; then
  echo "FAIL: PostgreSQL pod was not replaced" >&2
  exit 1
fi
echo "OK: pod replaced (${old_pod} -> ${new_pod})"

deadline=$((SECONDS + TIMEOUT))
until [ "$(pg "SELECT count(*) FROM loan_applications WHERE applicant_id='durability-${marker}'" 2>/dev/null || true)" = "1" ]; do
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "FAIL: marker row did not survive pod replacement" >&2
    exit 1
  fi
  sleep 2
done
echo "OK: marker row survived pod replacement"
echo "PostgreSQL durability drill passed"
