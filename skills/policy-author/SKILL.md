---
name: policy-author
description: Author or harden Agent Flow YAML policies for budgets, model use, file scopes, approvals, unsafe operations, cleanup, retention, authority, and failure behavior. Use when a workflow needs least-authority and fail-closed controls.
---

# Policy Author

1. Inventory each side effect, data sensitivity, model/provider call, writer, and consequential decision.
2. Set positive limits and least-authority scopes before granting capabilities.
3. Require approval or denial for consequential, destructive, external, cleanup, merge, or publication operations.
4. Define failure, pause, invalidation, retention, and exhaustion behavior explicitly.
5. Inspect policy normalization and routes with non-executing authoring commands.

Read [references/policy-checklist.md](references/policy-checklist.md) for the fail-closed policy checklist.

Never execute a workflow. Do not invoke `run`, `resume`, `inject`, or any command that starts or mutates a run.
