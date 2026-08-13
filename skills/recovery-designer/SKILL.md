---
name: recovery-designer
description: Design or revise Agent Flow recovery-pipeline YAML with structured failure evidence, bounded remediation, retry routing, and unresolved outcomes. Use when a failed step may be diagnosed and repaired before retry.
---

# Recovery Designer

1. Identify recoverable failures and the authoritative failure payload for each.
2. Route failure evidence to a bounded classifier or remediation workflow.
3. Define `on_remediated`, `on_unresolved`, retry limits, duration limits, and a terminal escape path.
4. Preserve attempt-scoped evidence and keep remediation write authority narrow.
5. Inspect every success, retry, exhaustion, and unresolved path with non-executing authoring commands.

Read [references/recovery-patterns.md](references/recovery-patterns.md) for the required recovery contract.

Never execute a workflow. Do not invoke `run`, `resume`, `inject`, or any command that starts or mutates a run.
