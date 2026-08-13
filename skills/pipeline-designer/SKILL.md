---
name: pipeline-designer
description: Create or revise simple Agent Flow pipeline YAML for sequential automation, bounded conditions, approvals, and artifact handoffs. Use when the process does not need recovery routing or multi-session collaboration.
---

# Pipeline Designer

1. Express the happy path as an ordered list of steps.
2. Add only required branches, interactions, and terminal outcomes.
3. Declare inputs and artifact handoffs explicitly; avoid using chat history as state.
4. Add time, attempt, model, and file-policy bounds appropriate to each step.
5. Inspect the result with `validate`, `lint`, `explain`, `graph`, and fixture-backed `simulate`.

Read [references/pipeline-patterns.md](references/pipeline-patterns.md) for selection rules and a review checklist.

Never execute a workflow. Do not invoke `run`, `resume`, `inject`, or any command that starts or mutates a run.
