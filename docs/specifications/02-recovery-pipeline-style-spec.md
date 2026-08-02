# Agent Flow Recovery Pipeline Style Spec

## 1. Purpose

Recovery pipeline workflows extend simple pipelines with structured failure handling. They support retrying, classifying failures, routing failures into sessions or nested workflows, remediating the problem, and returning to the failed step.

Use this style when failure is expected and the workflow should try to recover.

Examples:

- `bin/ci` fails, FM fixes the issue, then CI reruns.
- GitHub PR checks fail, LM classifies, FM resolves implementation errors.
- A deploy smoke test fails and routes to rollback or pause.
- PR comments arrive and route to FM for remediation.

## 2. Design Goals

1. Treat failures as structured artifacts.
2. Make failure routing explicit.
3. Support remediation workflows.
4. Return to the failed step after remediation.
5. Avoid infinite repair loops.
6. Preserve clear audit logs.

## 3. Failure Payload

Every failed step writes a failure payload.

```json
{
  "id": "command:ci:attempt-1",
  "step_id": "ci",
  "step_type": "command",
  "status": "failed",
  "attempt": 1,
  "exit_code": 1,
  "command": "bin/ci",
  "summary": "RSpec failure in ExporterSpec",
  "artifacts": {
    "available": [
      "logs/ci/attempt-1/stdout.log",
      "logs/ci/attempt-1/stderr.log"
    ],
    "withheld": []
  },
  "logs": {
    "stdout": "logs/ci/attempt-1/stdout.log",
    "stderr": "logs/ci/attempt-1/stderr.log"
  },
  "classification": "command_failure",
  "remediation_status": null,
  "path": "failures/command-ci-attempt-1-<sha256-12>.json",
  "redactions": {
    "applied": false,
    "marker": "[REDACTED]",
    "fields": [],
    "unscanned_artifacts": []
  }
}
```

The payload is registered as a run artifact at:

```text
failures/<failure-id-slug>-<sha256-12>.json
```

The artifact registry stores its backing bytes under the run's generated
`.agent-flow/runs/` tree and indexes the declared path in both artifact metadata
and `listFailures(runId)`. Recovery routes use the declared path, never the
opaque backing filename.

Before a failure payload can become session input, Agent Flow scans textual
attachments within per-file, aggregate-byte, and attachment-count limits and
snapshots every available attachment explicitly tagged with the failed attempt
under the failure path. The immutable, attempt-scoped snapshots prevent a later
retry from changing the bytes exposed by an earlier failure or stale output
from being attributed to a later attempt. Secret-like command, summary, and
attachment content is replaced with `[REDACTED]`.
Binary, oversized, missing, or otherwise unscannable attachments are withheld
from the available list and reported in redaction metadata. A failure in the
artifact store itself remains indexed with a payload-persistence diagnostic so
the original runtime failure is not hidden. Retention always preserves
artifacts whose kinds are `failure_payload` or `failure_attachment`, even when a
workflow declares a broader deletion pattern, so an indexed recovery route
never points to a payload or attachment deleted during terminal finalization.

## 4. Example: CI Recovery

```yaml
name: ticket-with-ci-recovery
version: 1
style: recovery_pipeline

sessions:
  lm:
    provider: local
    resume: true
  fm:
    provider: frontier
    resume: true

steps:
  - id: implement
    type: session_request
    session: fm
    prompt: prompts/implement.md
    inputs:
      - ticket.json
    outputs:
      - implementation-summary.md

  - id: ci
    type: command
    command: bin/ci
    timeout_seconds: 1800
    outputs:
      - ci/latest.log
    on_failure:
      capture:
        combined_log: ci/latest.log
        exit_code: true
        command: true
      route_to:
        workflow: ci-triage
        inputs:
          failure_payload: "{{ failure.path }}"
          failed_step: "{{ step.id }}"
      on_remediated:
        return_to: ci
      on_unresolved:
        then: pause
```

## 5. Triage Workflow

```yaml
name: ci-triage
version: 1
style: recovery_pipeline

inputs:
  failure_payload:
    required: true
  failed_step:
    required: true

sessions:
  lm:
    provider: local
    resume: true
  fm:
    provider: frontier
    resume: true

steps:
  - id: classify
    type: session_request
    session: lm
    prompt: prompts/classify-ci-failure.md
    inputs:
      - "{{ inputs.failure_payload }}"
    outputs:
      - ci/failure-classification.json

  - id: route
    type: condition
    branches:
      - if: artifacts.ci.failure_classification.kind == "flake"
        then: return_remediated
      - if: artifacts.ci.failure_classification.kind == "implementation_error"
        then: fix_with_fm
      - if: artifacts.ci.failure_classification.kind == "environment_error"
        then: return_unresolved

  - id: fix_with_fm
    type: session_request
    session: fm
    prompt: prompts/fix-ci-failure.md
    inputs:
      - "{{ inputs.failure_payload }}"
      - ci/failure-classification.json
    outputs:
      - implementation-summary.md

  - id: return_remediated
    type: result
    status: remediated
    return_to: "{{ inputs.failed_step }}"

  - id: return_unresolved
    type: result
    status: unresolved
```

## 6. Failure Actions

| Action | Meaning |
|---|---|
| `retry` | Retry the same step |
| `route_to.session` | Send failure to a model session |
| `route_to.workflow` | Run nested workflow |
| `return_to` | Return to failed step after remediation |
| `goto` | Jump to a specific step |
| `pause` | Wait for user |
| `fail` | End workflow |
| `ignore` | Continue despite failure |

## 7. Return Semantics

`return_to` means:

1. The failed step remains the point of truth.
2. Remediation steps may modify files or artifacts.
3. The engine reruns the failed step after remediation.
4. The rerun increments attempt count.
5. A remediated failure remains unresolved until the returned step succeeds.
6. If the step passes, the workflow resolves the remediated failure and
   continues after the original failed step.

Each rerun keeps its own persisted step-attempt row and `step.started` event.
Every remediation cycle keeps its recovery decision and routed/completed
events. A successful returned attempt adds a `recovery.returned` event for each
remediated failure it resolves; if the rerun fails or a configured recovery or
attempt limit stops execution, those failures remain unresolved.

Example:

```yaml
on_remediated:
  return_to: ci
```

### Runtime Recovery Contract

Recovery routes declare exactly one static target:

- `route_to.workflow` names a workflow registered with
  `AgentFlowWorkflowRegistry`. The runtime creates a linked child run, copies
  referenced parent artifacts into that run, and handles its `result` status.
  Copied failure artifacts and inputs that overlap child runtime-managed paths
  are remapped beneath `recovery-inputs/`; the persisted child inputs and JSON
  artifact references contain the remapped paths. Other artifacts retain their
  declared path so static child steps can consume them. Copied artifacts carry
  explicit `recovery_input` provenance, independent of authored step IDs.
  When the child returns `remediated`, its written, declared step outputs are
  atomically promoted into the parent run before child retention and the parent
  outcome handler execute; existing parent artifacts at those paths are
  overwritten with recovery provenance metadata so a `return_to` retry reads
  the repaired content. All child inputs marked `required: true` must be present
  in the resolved `route_to.inputs` mapping before the child can be created.
- `route_to.session` names a declared session and requires `route_to.prompt`.
  When `route_to.inputs` is present, the runtime resolves and supplies the
  complete mapping as a persisted JSON input alongside referenced artifacts.
  Its provider response must return no undeclared outputs and must include
  `metadata.recovery_status` set to `remediated` or `unresolved`.

Both routes must declare `on_remediated` and `on_unresolved`. A nested workflow
that returns `remediated` follows the former handler. `unresolved`, failed,
paused, cancelled, missing, or unsupported recovery results follow the latter.
Only a remediated result may resolve the indexed parent failure.
An `on_remediated.return_to` target must name the failed step so the runtime
reruns the operation that originally failed.

The runtime persists `recovery.routed` and `recovery.completed` events, writes a
`recovery_decision` artifact, and records the route, target, result, and child
run ID (when applicable) in the failure index. Child runs use `parent_run_id`
and `recovery_of_run_id` to preserve the recovery relationship.

## 8. Limits

Recovery workflows must prevent endless repair loops.

```yaml
limits:
  max_recovery_cycles: 3
  max_step_attempts:
    ci: 4
  max_frontier_calls: 5
  max_duration_minutes: 120

policies:
  recovery_limits: pause
```

`max_duration_seconds` may be used instead of `max_duration_minutes`; declaring
both is invalid. The duration is measured from the persisted run start time and
checked before each subsequent step. Model budgets are reserved atomically
before provider invocation, so an exhausted frontier budget cannot start
another remediation call.

When a recovery, step-attempt, model-call, or duration limit is reached, the
workflow pauses by default. Set `policies.recovery_limits: fail` to fail instead.
The runtime writes a structured failure and a `recovery.limit_reached` event
with the limit, message, and selected outcome before terminal finalization.

## 9. Failure Classification

Recommended classification schema:

```json
{
  "kind": "implementation_error",
  "confidence": "high",
  "summary": "Test expected JSON key missing from exporter output.",
  "recommended_owner": "fm",
  "safe_to_retry": false,
  "requires_user": false
}
```

Known kinds:

| Kind | Default Route |
|---|---|
| `flake` | Retry failed step |
| `implementation_error` | FM fix |
| `formatting_error` | LM or command fix |
| `environment_error` | Pause or retry |
| `missing_requirement` | Ask user |
| `unsafe_change` | Pause |
| `unknown` | Pause |

## 10. Short Circuits

Recovery pipelines should stop early when continued automation is risky.

```yaml
short_circuit_if:
  - risk.high == true
  - budget.frontier_calls_remaining == 0
  - failures.ci.attempts >= 4
```

Short circuits may be declared for the workflow or an individual step and are
evaluated before that step starts. Ordinary input and JSON-artifact condition
references remain available. The runtime also exposes remaining persisted
model budgets through `budget.<kind>_remaining` and the highest indexed failure
attempt through `failures.<step>.attempts`. Missing references do not match.
Artifact aliases rooted at `budget` or `failures` are reserved in recovery
pipelines so explicit artifacts cannot shadow these virtual safety namespaces.
A matched short circuit always pauses before further automation, persists a
`recovery_short_circuit` failure, and emits `recovery.short_circuited`, even
when the general recovery-limit policy is `fail`. If an available artifact is
malformed or otherwise cannot be evaluated safely, the runtime also pauses,
persists `recovery_short_circuit_evaluation`, and emits
`recovery.short_circuit_failed`.

## 11. Notifications

Recommended:

```yaml
notify:
  - on: step.failed
    channels: [terminal]
    throttle_seconds: 300
  - on: workflow.paused
    channels: [system, email_personal]
  - on: workflow.failed
    channels: [email_personal]
  - on: workflow.completed
    channels: [terminal]
```

## 12. Edge Cases

| Edge Case | Required Behavior |
|---|---|
| Remediation changes unrelated files | Pause or revert only if explicitly allowed |
| Same failure repeats | Stop after max cycles |
| Triage cannot classify | Pause |
| FM fix creates new failure | Continue until recovery limit, then pause |
| Failure log contains secrets | Redact before session input |
| Nested workflow fails | Parent follows `on_unresolved` |
| User injects context during recovery | Mark current remediation step dirty and rerun if needed |

## 13. Validation Rules

Recovery validation should enforce:

- Every recovery loop has max cycles.
- `return_to` targets an existing step.
- Nested workflow result statuses are handled.
- Failure payload references are valid.
- Failure routes do not create unbounded cycles.
- Model sessions used for remediation are defined.
- User escalation exists for unresolved failures.
