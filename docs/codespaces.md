# Continue EventDrivenMicroservices in Codespaces

## Current Handoff

The repository is currently clean and synchronized with `origin/main`.

- Baseline commit: `49d5dca v1`
- Runtime target: Java 21
- Gradle wrapper: 8.4
- Canonical plan: [rebuild_guide.md](rebuild_guide.md)
- Refactoring status: [codebase-analysis-refactoring-steps.md](codebase-analysis-refactoring-steps.md)
- Testing strategy: [testing.md](testing.md)
- Architecture verdict: [Verdict.md](Verdict.md)

The current application is an observable modular monolith with a working browser control room, deterministic demo scripts, read APIs, Kafka event envelopes, and focused contract tests.

## Open Codespace

1. Open the GitHub repository.
2. Select **Code** -> **Codespaces** -> **Create codespace on main**.
3. Wait for the `.devcontainer/devcontainer.json` configuration to finish.
4. Open a terminal in the Codespace.

The current devcontainer provides Java 21, Docker-in-Docker, Python, kubectl, Helm, and Minikube tooling. The local Terraform/Kind workflow also requires Terraform, Kind, and the ability to run Docker containers. Check these explicitly before attempting the full deployment.

## First Commands

```bash
java --version
cd apps/platform-engine
./gradlew --version
./gradlew clean compileJava compileTestJava --no-daemon
```

From the repository root:

```bash
helm version
kubectl version --client
terraform version
kind version
```

If `terraform` or `kind` is unavailable, do not claim the Kind deployment is verified yet. The fast Java and Helm checks can still run independently.

## Fast Verification

```bash
cd apps/platform-engine
./gradlew test \
  --tests '*ControllerTest' \
  --tests '*ServiceTest' \
  --tests '*EventEnvelopeTest' \
  --tests '*AnomalyEventListenerTest' \
  --tests '*ArchitectureTest' \
  --no-daemon
```

Then validate the chart:

```bash
cd ../..
helm lint deploy/helm/event-driven-lab
helm template event-driven-lab deploy/helm/event-driven-lab >/tmp/event-driven-lab.yaml
```

## Full Test Baseline

Run only after Docker-in-Docker is available:

```bash
cd apps/platform-engine
./gradlew test --no-daemon
```

The last recorded baseline was 17 tests: 12 passed and 5 failed.

- 2 failures were Docker/Testcontainers initialization failures.
- 1 anomaly test exposed a threshold/fixture mismatch.
- 1 transaction controller test received `401`.
- 1 telemetry controller test used MVC test infrastructure in a WebFlux application.

The immediate repair task is to fix those three local failures, then rerun the full suite before adding the Testcontainers trace-flow test.

## Run The Application

For a dependency-free UI smoke check, the application still needs its configured PostgreSQL and Kafka dependencies. With those services available and environment variables loaded:

```bash
cd apps/platform-engine
./gradlew bootRun --no-daemon
```

Open the forwarded port in the Codespace:

```text
http://localhost:8080/
```

The UI supports loan submission, transaction velocity bursts, telemetry baseline/outlier injection, ledger inspection, outbox status, correlation IDs, and trace IDs when a span is active.

## Run The Demo Scripts

PowerShell:

```powershell
./scripts/demo.ps1 -BaseUrl http://localhost:8080
```

Bash:

```bash
BASE_URL=http://localhost:8080 ./scripts/demo.sh
```

These scripts assume the application and dependencies are already running. They do not provision infrastructure.

## Resume The Work

The next implementation sequence is:

1. Repair the three local full-suite failures.
2. Rerun the full Gradle suite in Codespaces with Docker available.
3. Add a Testcontainers trace-flow integration test.
4. Verify PostgreSQL -> outbox -> Kafka -> anomaly -> ledger behavior.
5. Configure and verify Tempo/Loki Grafana datasources.
6. Add failure/retry assertions for outbox publication.
7. Reassess whether service extraction is justified.

Before ending a Codespace session:

```bash
git status --short
git add .
git commit -m "Describe the focused change"
git push origin main
```

Do not commit `.env`, `.env.local`, Terraform state, generated build output, or credentials.
