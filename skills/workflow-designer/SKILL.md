---
name: workflow-designer
description: Design or revise Agent Flow workflow YAML from a process, specification, or automation goal. Use for choosing a workflow style, defining artifact contracts and control flow, and producing a safe draft that can be validated without execution.
---

# Workflow Designer

1. Read the requirements and existing YAML or repository conventions.
2. Choose the least-complex fitting style: `pipeline`, `recovery_pipeline`, or `collaborative`.
3. Define inputs, durable artifacts, step transitions, terminal outcomes, sessions, limits, and policies before writing YAML.
4. Produce the smallest complete workflow and state any assumptions beside it.
5. Check the draft with `agent-flow validate`, `lint`, `explain`, and `graph`; use `simulate` only with an explicit fixture.

Read [references/authoring.md](references/authoring.md) for the style decision, authoring checklist, and safety rules.

Never execute a workflow. Do not invoke `run`, `resume`, `inject`, or any command that starts or mutates a run.
