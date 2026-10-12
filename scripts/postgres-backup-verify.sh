#!/usr/bin/env bash
# Creates a portable custom-format PostgreSQL backup, restores it to a temporary database,
# and compares representative table row counts before publishing the backup file.
set -euo pipefail

NS="${NAMESPACE:-event-driven-lab}"
DEPLOY="deploy/eventdrivenmicroservices-postgres"
BACKUP_FILE="${BACKUP_FILE:-}"

if [ -z "$BACKUP_FILE" ]; then
  echo "Set BACKUP_FILE to the destination for the verified backup archive." >&2
  exit 2
fi
if [ -e "$BACKUP_FILE" ]; then
  echo "Refusing to overwrite existing backup: $BACKUP_FILE" >&2
  exit 2
fi

backup_dir="$(dirname "$BACKUP_FILE")"
mkdir -p "$backup_dir"
umask 077
temporary_file="$(mktemp "${BACKUP_FILE}.tmp.XXXXXX")"
database_suffix="$(cat /proc/sys/kernel/random/uuid | tr -d '-')"
verification_db="backup_verify_${database_suffix}"

pg() {
  kubectl exec -n "$NS" "$DEPLOY" -- \
    sh -c 'psql -U "$POSTGRES_USER" -d "$1" -tA -c "$2"' _ "$1" "$2"
}

cleanup() {
  if [ -n "${verification_db:-}" ]; then
    pg postgres "DROP DATABASE IF EXISTS \"$verification_db\"" >/dev/null 2>&1 || true
  fi
  if [ -n "${temporary_file:-}" ]; then
    rm -f "$temporary_file"
  fi
}
trap cleanup EXIT

kubectl exec -n "$NS" "$DEPLOY" -- \
  sh -c 'pg_dump --format=custom -U "$POSTGRES_USER" -d "$POSTGRES_DB"' > "$temporary_file"
if [ ! -s "$temporary_file" ]; then
  echo "PostgreSQL produced an empty backup archive." >&2
  exit 1
fi

pg postgres "CREATE DATABASE \"$verification_db\"" >/dev/null
kubectl exec -i -n "$NS" "$DEPLOY" -- \
  sh -c 'pg_restore --exit-on-error --no-owner --no-privileges -U "$POSTGRES_USER" -d "$1"' \
  _ "$verification_db" < "$temporary_file"

for table in loan_applications outbox_events ledger_events telemetry_events; do
  source_count="$(pg eventdrivenmicroservices_db "SELECT count(*) FROM $table")"
  restored_count="$(pg "$verification_db" "SELECT count(*) FROM $table")"
  if [ "$source_count" != "$restored_count" ]; then
    echo "Backup verification failed: $table count differs ($source_count != $restored_count)." >&2
    exit 1
  fi
done

pg postgres "DROP DATABASE \"$verification_db\"" >/dev/null
verification_db=""
mv "$temporary_file" "$BACKUP_FILE"
temporary_file=""
echo "Verified PostgreSQL backup written to $BACKUP_FILE"
