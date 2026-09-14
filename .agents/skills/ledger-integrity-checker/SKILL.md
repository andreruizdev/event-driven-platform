---
name: ledger-integrity-checker
description: "Verifies cryptographic SHA-256 hash chaining and immutability across ledger events."
---

# Ledger Integrity Checker Skill

When invoked, perform the following actions:

1. **Query Ledger Events**: Fetch the latest entries from the `ledger_events` PostgreSQL table.
2. **Verify SHA-256 Chain**: Recompute SHA-256 hashes (`SHA256(previous_hash + type + payload)`) and assert equality with `current_hash`.
3. **Verify Previous Hash References**: Confirm that each record's `previous_hash` matches the preceding record's `current_hash`.
4. **Assert Immutability**: Verify no `UPDATE` or `DELETE` SQL operations have been executed against `ledger_events`.
