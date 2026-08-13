---
name: workflow-debugger
description: Diagnose Agent Flow YAML that fails parsing, validation, linting, graphing, or fixture simulation. Use to isolate schema, routing, artifact, policy, ownership, or fixture defects without starting a run.
---

# Workflow Debugger

1. Preserve the failing YAML, fixture, exact command, and diagnostic.
2. Reproduce with the narrowest non-executing command.
3. Classify the defect using [references/debugging.md](references/debugging.md).
4. Reduce to the smallest failing route or contract, then correct one cause at a time.
5. Re-run the original check and adjacent `validate`, `lint`, `graph`, or `simulate` coverage.

Never execute a workflow. Do not invoke `run`, `resume`, `inject`, or any command that starts or mutates a run.
