---
name: workflow-reviewer
description: Review Agent Flow workflow YAML without executing it. Use to find schema and reference defects, unreachable or dead paths, unsafe or unbounded policy, authority and ownership conflicts, and missing terminal behavior.
---

# Workflow Reviewer

1. Read the workflow, referenced prompts or fixtures, and stated requirements.
2. Run only non-executing inspection: `validate`, `lint`, `explain`, `graph`, and fixture-backed `simulate`.
3. Review each category in [references/review-checklist.md](references/review-checklist.md).
4. Report findings by severity with the YAML location, violated invariant, consequence, and smallest safe correction.
5. Separate actionable defects from questions and optional simplifications. Do not rewrite unless asked.

Never execute a workflow. Do not invoke `run`, `resume`, `inject`, or any command that starts or mutates a run.
