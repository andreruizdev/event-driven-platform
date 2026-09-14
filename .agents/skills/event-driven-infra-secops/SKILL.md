---
name: event-driven-infra-secops
description: "Use when auditing Kubernetes, Helm, Terraform, or Docker security controls and secret boundaries."
---

# EventDrivenMicroservices Infrastructure Security

1. Check non-root execution, dropped capabilities, privilege escalation, read-only filesystems, and temporary volume mounts.
2. Ensure secrets are externalized through `eventdrivenmicroservices-secrets` and never embedded in chart templates.
3. Check image tags, probes, resource limits, and namespace-scoped deployment behavior.
4. Report gaps separately for application, infrastructure, and CI layers.