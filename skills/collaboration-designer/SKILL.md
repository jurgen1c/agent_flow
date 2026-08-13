---
name: collaboration-designer
description: Design or revise collaborative Agent Flow workflow YAML with explicit session roles, ownership, authority, file scopes, reviews, consultations, approvals, and bounded disagreement handling.
---

# Collaboration Designer

1. Confirm that independent judgment or ownership justifies collaboration.
2. Assign one clear owner for each artifact, decision, and writable file scope.
3. Grant each session only the authority its role requires; omitted authority remains advisory.
4. Make reviews, consultations, approvals, handoffs, and disagreement outcomes explicit and bounded.
5. Inspect normalized roles and routes with `validate`, `lint`, `explain`, `graph`, and fixture-backed `simulate`.

Read [references/collaboration-patterns.md](references/collaboration-patterns.md) for authority and ownership checks.

Never execute a workflow. Do not invoke `run`, `resume`, `inject`, or any command that starts or mutates a run.
