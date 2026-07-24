# Agentflow Platform Spec

## 1. Purpose

Agentflow is a local workflow engine for durable, inspectable, resumable agent workflows. It lets developers define workflows that combine:

- Deterministic code execution.
- Local model sessions.
- Frontier model sessions.
- MCP/tool calls.
- Human input and approvals.
- Notifications.
- Artifact retention and cleanup.
- Simple pipelines, recovery loops, or collaborative multi-session workflows.

The goal is to make agentic automation feel less like a long chat and more like a controlled execution system with state, policies, logs, and reviewable outcomes.

## 2. Workflow Styles

Agentflow supports three workflow styles. Pipelines should stay simple by default. Collaboration should be opt-in.

| Style | Primary Use | Core Shape |
|---|---|---|
| Pipeline | Straight-line automation | Step A -> Step B -> Step C |
| Recovery Pipeline | Automation with failures and remediation | Step fails -> classify/fix -> return |
| Collaborative Workflow | Multiple sessions with roles, reviews, consultations, approvals | Implementer + reviewer + specialists |

The same engine runs all three styles, but each style has its own recommended schema patterns and guardrails.

## 3. Core Concepts

### Workflow

A named definition that includes inputs, sessions, steps, artifacts, policies, notifications, and retention rules.

### Run

One execution of a workflow. Each run has a unique ID, state file, event log, artifacts, and optional session IDs.

### Step

One executable unit. A step can run a command, call a model session, call MCP, evaluate a condition, run a loop, request user input, or invoke another workflow.

### Session

A resumable model interaction context. Sessions may use a local model, frontier model, or named Codex profile.

### Artifact

A durable file or structured output used as the contract between steps. Artifacts are authoritative; session history is helpful but not required for correctness.

### Failure Payload

A structured artifact that describes a failed step, including command, exit code, logs, classification, and remediation status.

### Policy

A rule that controls safety, budgets, approvals, file scopes, model usage, notifications, and cleanup.

## 4. Directory Layout

```text
.agentflow/
  config.yml
  workflows/
    ticket-lifecycle.yml
    ci-triage.yml
  prompts/
    implement.md
    review.md
  templates/
    workflow-failed.md
  runs/
    run_123/
      state.json
      events.jsonl
      artifacts/
      sessions/
      failures/
      notifications.jsonl
```

## 5. Run State

`state.json`:

```json
{
  "run_id": "run_123",
  "workflow": "ticket-lifecycle",
  "style": "recovery_pipeline",
  "status": "running",
  "current_step": "ci",
  "inputs": {},
  "sessions": {},
  "steps": {},
  "artifacts": {},
  "failures": {},
  "approvals": {},
  "budget": {},
  "created_at": "...",
  "updated_at": "..."
}
```

Valid run statuses:

| Status | Meaning |
|---|---|
| `pending` | Run created but not started |
| `running` | Run is actively executing |
| `waiting` | Run is waiting on timer, review, or external event |
| `paused` | Run needs user input or manual intervention |
| `completed` | Run succeeded |
| `failed` | Run reached terminal failure |
| `cancelled` | User cancelled the run |

## 6. Step Types

| Type | Purpose |
|---|---|
| `command` | Run a deterministic shell command |
| `session_request` | Send prompt/artifacts to a named model session |
| `mcp_call` | Call an MCP tool directly |
| `workflow` | Invoke another workflow as a step |
| `condition` | Branch based on state or artifacts |
| `loop` | Repeat steps until a condition or limit |
| `manual_gate` | Ask user for approval |
| `input_request` | Ask user for missing information |
| `artifact_transform` | Normalize or transform artifacts |
| `parallel` | Run independent steps concurrently |
| `result` | Return status from nested workflow |

## 7. Workflow Definition Skeleton

```yaml
name: example-workflow
version: 1
style: pipeline
maturity: draft

inputs: {}

sessions: {}

artifacts:
  root: .agentflow/runs/{{ run.id }}/artifacts

steps: []

policies: {}

notify: []

retention: {}
```

## 8. Validation and Tooling

Agentflow must provide strong authoring tools because workflows can become hard to reason about.

```bash
agentflow validate workflows/ticket-lifecycle.yml
agentflow lint workflows/ticket-lifecycle.yml
agentflow graph workflows/ticket-lifecycle.yml
agentflow simulate workflows/ticket-lifecycle.yml --fixture fixtures/ticket.json
agentflow explain workflows/ticket-lifecycle.yml
```

Validation should catch:

- Unknown step types.
- Missing required fields.
- Invalid references.
- Invalid `goto`, `return_to`, or loop targets.
- Loops without max iterations or max duration.
- Sessions referenced but not defined.
- Artifacts used before being created.
- Failure states without terminal behavior.
- Parallel file writers with overlapping scopes.
- Notification channels referenced but not configured.
- Unsafe command patterns.
- Secrets passed into prompts.
- Unbounded frontier model usage.
- Deadlock-prone approvals.

### Policy Contract

Workflow policy is enforced at two boundaries: validation before a run is
persisted, and a stateless runtime check before an operation consumes budget,
uses a model, writes a file, performs cleanup, or attempts an unsafe action.
Runtime checks return `allow`, `pause`, or `fail` with a stable code and an
actionable message. A caller must not perform the requested operation unless
the decision is `allow`. File-write and cleanup checks receive a trusted root
path so existing symlink components can be rejected. Cleanup callers must also
declare whether deletion is recursive so retained descendants remain protected.

```yaml
limits:
  max_frontier_calls: 4
  max_model_calls: 8
  max_step_attempts: { implementer: 3 }

policies:
  model_usage:
    allowed_providers: [local, frontier]
  approvals:
    required_for: [publish]
  cleanup: require_approval
  unsafe_operations: deny

sessions:
  implementer:
    provider: frontier
    authority:
      can_modify_files: true
    file_scope:
      include: [app/**, tests/**]
      exclude: [config/credentials/**]
```

Frontier sessions require a positive `limits.max_frontier_calls` value.
Per-step attempt checks pass the step identifier to select its declared
`limits.max_step_attempts` bound.
Sessions that can modify files must have an effective file scope unless every
write is already constrained by a parallel-branch scope. Cleanup follows the
applicable `retention.on_success`, `retention.on_failure`, or
`retention.on_cancelled` rule after any retention period and required approval
have been satisfied. A non-empty `delete` list narrows cleanup to those paths;
rules such as `keep_all_for_days` or `ask_user` without a `delete` list permit
the requested paths once their guard expires or is approved. Unsafe operations
are denied by default; an explicit `require_approval` policy pauses instead of
executing them until approval is recorded.

## 9. Authoring Skills

Agentflow should ship with skills that help users create and maintain workflows.

| Skill | Purpose |
|---|---|
| `workflow-designer` | Interview user and draft a workflow |
| `pipeline-designer` | Create simple straight-line workflows |
| `recovery-designer` | Add failure payloads, triage, retries, and return behavior |
| `collaboration-designer` | Define roles, authority, reviews, consults, and approvals |
| `workflow-reviewer` | Find schema issues, dead paths, unsafe policies, and unclear ownership |
| `workflow-debugger` | Explain failed runs and recommend fixes |
| `workflow-simplifier` | Reduce overcomplicated workflows |
| `policy-author` | Add safety, budgets, notifications, retention, and approvals |

## 10. Maturity Levels

```yaml
maturity: draft | experimental | stable | trusted
```

| Maturity | Behavior |
|---|---|
| `draft` | Validation and simulation encouraged; extra approvals |
| `experimental` | Can run manually; cannot auto-merge/destructively mutate by default |
| `stable` | Normal execution allowed |
| `trusted` | Eligible for batch/scheduled execution if policies allow |

## 11. Notifications

Notifications are engine-level events, not ad hoc step logic.

Supported channels:

- `terminal`
- `system`
- `email`
- `slack`
- `webhook`
- `command`

Example:

```yaml
notify:
  - on: workflow.completed
    channels: [email_personal, terminal]
  - on: workflow.failed
    channels: [email_personal, slack_dev, terminal]
  - on: workflow.paused
    channels: [system, terminal]
```

Notification delivery failure must not fail the workflow unless `required: true`.

## 12. Retention and Cleanup

Workflows must be able to clean up generated artifacts after completion.

```yaml
retention:
  on_success:
    keep:
      - state.json
      - events.jsonl
      - final-summary.md
      - decision-records/**
    delete:
      - temp/**
      - raw-mcp/**
      - ci/*.log
    after_days: 14
  on_failure:
    keep_all_for_days: 30
  on_cancelled:
    ask_user: true
```

Commands:

```bash
agentflow cleanup run_123
agentflow cleanup --older-than 30d --status completed
agentflow archive run_123
agentflow export run_123 --format zip
```

## 13. CLI

```bash
agentflow init
agentflow validate <workflow>
agentflow lint <workflow>
agentflow graph <workflow>
agentflow simulate <workflow>
agentflow run <workflow> --input key=value
agentflow resume <run-id> --outcome <choice> [--fixture <file>]
agentflow resume <run-id> --answer <value> [--fixture <file>]
agentflow status <run-id>
agentflow logs <run-id>
agentflow artifacts <run-id>
agentflow session <run-id> <session-name>
agentflow inject <run-id> <session-name> "context"
agentflow retry <run-id> <step-id>
agentflow skip <run-id> <step-id> --reason "..."
agentflow pause <run-id> --reason "..."
agentflow cancel <run-id>
agentflow cleanup <run-id>
```

## 14. Implementation Phases

1. Workflow schema parser and validator.
2. Run state, artifact registry, and event log.
3. Command, session, condition, manual gate, and input request runners.
4. Pipeline workflow support.
5. Failure payloads and recovery workflows.
6. Nested workflows and loops.
7. Collaboration model.
8. Notifications.
9. Cleanup and archival.
10. Authoring skills and graph/simulation tooling.
