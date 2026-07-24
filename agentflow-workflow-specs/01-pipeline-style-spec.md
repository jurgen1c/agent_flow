# Agentflow Pipeline Style Spec

## 1. Purpose

Pipeline workflows are the simplest Agentflow style. They execute a mostly linear sequence of steps where each step produces artifacts for later steps.

Use pipeline workflows when the process is predictable and failures can simply pause, retry, or fail.

Examples:

- Run formatter, tests, and build.
- Generate release notes.
- Export data and upload report.
- Fetch Jira ticket and create a spec.
- Run a deterministic maintenance checklist.

## 2. Design Goals

1. Keep workflow definitions short and readable.
2. Make each step's inputs and outputs explicit.
3. Support simple branching without turning into a complex graph.
4. Keep model use optional.
5. Make failures easy to understand.

## 3. Minimal Example

```yaml
name: local-ci
version: 1
style: pipeline

steps:
  - id: bundle_install
    type: command
    command: bundle install

  - id: lint
    type: command
    command: bin/rubocop

  - id: test
    type: command
    command: bin/rails test
```

## 4. Full Example

```yaml
name: generate-ticket-spec
version: 1
style: pipeline

inputs:
  ticket_key:
    required: true

sessions:
  lm:
    provider: local
    resume: true

artifacts:
  root: .agentflow/runs/{{ run.id }}/artifacts

steps:
  - id: fetch_ticket
    type: mcp_call
    server: atlassian-rovo-mcp
    tool: get_issue
    arguments:
      key: "{{ inputs.ticket_key }}"
    outputs:
      - ticket.json

  - id: render_ticket_markdown
    type: artifact_transform
    input: ticket.json
    output: ticket.md
    transform: jira_ticket_to_markdown

  - id: create_spec
    type: session_request
    session: lm
    prompt: prompts/create-spec.md
    inputs:
      - ticket.json
      - ticket.md
    outputs:
      - spec.md
```

## 5. Step Ordering

Pipeline steps execute in listed order unless:

- A step has `then`.
- A condition step chooses a branch.
- A failure policy changes control flow.
- A manual gate pauses execution.

Simple pipelines should avoid arbitrary `goto` unless there is a clear reason.

## 6. Inputs and Outputs

Each step should declare artifacts it reads and writes.

```yaml
- id: create_spec
  type: session_request
  inputs:
    - ticket.md
  outputs:
    - spec.md
```

Validation should warn when:

- A step reads an artifact that no previous step creates.
- A step writes an artifact also written by another step without `overwrite: true`.
- A model step lacks bounded inputs.

### 6.1 Session Request Contract

`session_request` is a bounded model operation, not an open-ended chat. Each
request must declare:

- A static session name that resolves to a session with a static `provider`.
- A repo-relative prompt file.
- One or more input artifacts and one or more output artifacts.
- A boolean session `resume` setting when resumability is enabled.

The runtime selects an explicitly registered provider adapter, reads prompts
and input artifacts with size limits, and requires the adapter to return every
declared output and no undeclared outputs. It records a deterministic request
metadata artifact under `session-requests/<step-id>-<hash>.json`, writes provider
outputs through the artifact registry, and leaves the session in `waiting`
state with its external session ID so a later request can resume it.
Each adapter receives an abort signal, lifecycle cancellation interrupts a
pending request, and configured model/frontier call budgets are reserved before
the provider is invoked.

Provider calls are never implicit in simulation. Simulation uses declared JSON
fixture outputs, and runtime tests can register the fixture provider adapter to
return deterministic responses without network access.
The CLI exposes that same deterministic boundary with
`agentflow run <workflow> --id <run-id> --fixture <file>` for workflows whose
session provider is `fixture`; non-fixture provider adapters remain an explicit
embedding concern.

## 7. Failure Behavior

Pipeline failures should be boring.

Supported simple policies:

```yaml
on_failure:
  retry: 1
  then: pause
```

```yaml
on_failure:
  then: fail
```

```yaml
on_failure:
  then: continue
  allowed: true
  reason: "Optional docs generation"
```

If failure handling needs diagnosis, remediation, or return-to-step behavior, use the recovery pipeline style.

Each failed executable attempt persists a failure summary with the attempt
number and normalized outcome: `retry`, `pause`, `fail`, or `continue`.
Exhausting retries without an explicit terminal target pauses a pipeline.
`continue` is never implicit: it requires `allowed: true`, leaves the failed
step out of the completed-step list, and records the continuation decision
before the next listed step starts.

## 8. Conditions

Conditions are allowed but should remain simple.

```yaml
- id: maybe_publish
  type: condition
  if: inputs.publish == true
  then: publish
  else: finish
```

Validation should warn when a pipeline contains many branches, because it may be better represented as a recovery or collaborative workflow.

## 9. Manual Gates

Pipeline workflows can pause for approval.

```yaml
- id: approve_publish
  type: manual_gate
  message: "Publish release notes?"
  options: [approve, pause, cancel]
```

Manual gates must define all possible outcomes.

At runtime, a manual gate persists its message and valid outcomes in the run's
waiting state, records a requested approval, and pauses before starting any
later step. Resuming requires one declared outcome:

```bash
agentflow resume <run-id> --outcome approve
```

An invalid outcome must leave the run paused and must not mutate the approval or
start later steps. `approve` continues through `on_approve` when declared,
`reject` continues through `on_reject` or cancels by default, `cancel` continues
through `on_cancel` or cancels by default, and `pause` leaves the gate waiting.
The resume path completes the waiting step exactly once and continues from its
success route without replaying earlier steps.

## 10. Input Requests

Pipeline workflows can pause for missing information and declare where the
answer is saved:

```yaml
- id: deployment_target
  type: input_request
  question: "Which environment should receive this release?"
  save_as: answers/deployment-target.json
```

The run persists the question, waiting reason, current step, and declared answer
artifact before pausing. Resuming requires an explicit answer:

```bash
agentflow resume <run-id> --answer '{"environment":"staging"}'
```

String answers are stored as UTF-8 text. Other JSON values are stored as
UTF-8 JSON. The artifact is attributed to the input-request step and must obey
the same normalized, repo-relative, collision-safe artifact contract as other
declared outputs. CLI fixture-backed providers are restored from the fixture
path persisted by `agentflow run`; `--fixture <file>` may be supplied again to
use an updated deterministic response set.

## 11. Notifications

Pipeline notifications are emitted for configured completion, failure, and
paused outcomes. The built-in runtime channels are `terminal` and `system`;
embedders can register additional named channel adapters. Delivery attempts are
currently synchronous; a promise-returning adapter is rejected as a delivery
failure so a required channel cannot be recorded as delivered before it settles.
Each attempt is recorded in the ordered run event log. A failed delivery is
diagnostic by default and does not change the pipeline outcome. Set
`required: true` on a notification rule when failure to deliver any channel
must fail a completed or paused pipeline.

```yaml
notify:
  - on: workflow.completed
    channels: [terminal]
  - on: workflow.failed
    channels: [terminal]
  - on: workflow.paused
    channels: [terminal, system]
    required: true
```

## 12. Cleanup

Every completed, failed, or cancelled pipeline writes a deterministic
runtime-owned `final-summary.md` artifact. Paused runs remain resumable and do
not write a final summary until they reach a terminal outcome. Workflow steps
cannot publish that reserved path. Pipeline runs often generate temporary logs,
so terminal runs can define retention rules for registered artifact paths.

```yaml
retention:
  on_success:
    keep:
      - state.json
      - events.jsonl
      - final-summary.md
    delete:
      - temp/**
      - raw/**
```

Retention removes only the backing content for matched registered artifacts;
the artifact metadata remains queryable with `missing` status. Run state and
ordered events live in SQLite and are never cleanup candidates, while
`final-summary.md` is always retained. `keep` overrides `delete`. Rules with a
positive `after_days` or `keep_all_for_days`, or with `ask_user: true`, are
recorded as deferred for a later cleanup command instead of running early.
Cleanup policy decisions and deletion failures are recorded without rewriting
the already-determined pipeline outcome.

## 13. Validation Rules

Pipeline validation should enforce:

- No unresolved step references.
- No unbounded loops.
- No parallel file writers.
- No undeclared session use.
- No missing manual gate outcomes.
- No secret files passed into model prompts.

## 14. Good Defaults

Pipeline workflows should default to:

- One step at a time.
- Direct child process command execution.
- No auto-destructive commands.
- Terminal notifications only.
- Pause on unexpected failure.
- Explicit artifact outputs.
