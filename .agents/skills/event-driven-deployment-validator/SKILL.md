---
name: event-driven-deployment-validator
description: "Use when validating Docker images, Terraform Kind provisioning, Helm rendering, Kubernetes readiness, or local deployment scripts."
---

# EventDrivenMicroservices Deployment Validator

1. Validate Dockerfile build and non-root runtime assumptions.
2. Run Terraform plan for the local Kind environment without applying destructive changes.
3. Run `helm lint` and `helm template` for `deploy/helm/event-driven-lab`.
4. Verify external secret creation, service DNS names, probes, init containers, resource limits, and security contexts.
5. Require runtime pod readiness evidence before claiming deployment completion.