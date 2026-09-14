---
name: event-driven-phase-reviewer
description: "Use when reviewing rebuild guide phases, Phase 0 completion, implementation evidence, or the next dependency-ordered rebuild task."
---

# EventDrivenMicroservices Phase Reviewer

1. Read `docs/rebuild_guide.md` and identify the requested phase.
2. Inspect only the owning source, tests, deployment files, and verification commands for that phase.
3. Report met, unproven, and failing acceptance criteria with exact file evidence.
4. Never mark a phase complete from documentation alone; require an executable check where one exists.
5. Update repository memory only with durable findings and the next smallest executable step.