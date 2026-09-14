# Kubernetes Security & Infrastructure Compliance

## Operating Directives

1. **Non-Root Execution**: All containers in the Event-Driven Microservices Lab must execute under a non-privileged `USER` context (`runAsNonRoot: true`, `runAsUser: 1000`).
2. **Immutable Filesystems**: Enforce `readOnlyRootFilesystem: true` across deployment specs, providing writeable `emptyDir` volumes only for temporary `/tmp` paths.
3. **No Hardcoded Secrets**: Secrets must be injected via environment variables sourced from the external Kubernetes `Secret` resource `eventdrivenmicroservices-secrets`.
4. **Least Privilege Capabilities**: Container specs must explicitly drop unnecessary Linux capabilities (`capabilities: drop: ["ALL"]`).
