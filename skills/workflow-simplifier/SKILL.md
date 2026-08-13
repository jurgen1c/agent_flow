---
name: workflow-simplifier
description: Simplify Agent Flow workflow YAML while preserving required behavior, artifact contracts, policy gates, ownership, and terminal outcomes. Use to remove unnecessary sessions, steps, branches, loops, or duplicated policy safely.
---

# Workflow Simplifier

1. Record required inputs, outputs, side-effect boundaries, approvals, owners, and terminal outcomes.
2. Establish a non-executing baseline with `validate`, `lint`, `explain`, `graph`, and representative simulation fixtures.
3. Apply one simplification pattern from [references/simplification.md](references/simplification.md) at a time.
4. Re-run the baseline after each change and compare reachable outcomes and artifact contracts.
5. Explain what was removed and which invariant proves behavior remains covered.

Never execute a workflow. Do not invoke `run`, `resume`, `inject`, or any command that starts or mutates a run.
