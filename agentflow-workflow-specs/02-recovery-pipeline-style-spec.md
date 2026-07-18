# Agentflow Recovery Pipeline Style Spec

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
  "id": "failure_001",
  "step_id": "ci",
  "step_type": "command",
  "status": "failed",
  "attempt": 1,
  "exit_code": 1,
  "command": "bin/ci",
  "summary": "RSpec failure in ExporterSpec",
  "artifacts": {
    "combined_log": "ci/latest.log"
  },
  "classification": null,
  "remediation_status": null
}
```

The payload is saved under:

```text
.agentflow/runs/<run-id>/failures/failure_001.json
```

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
5. If the step passes, the workflow continues after the original failed step.

Example:

```yaml
on_remediated:
  return_to: ci
```

## 8. Limits

Recovery workflows must prevent endless repair loops.

```yaml
limits:
  max_recovery_cycles: 3
  max_step_attempts:
    ci: 4
  max_frontier_calls: 5
  max_duration_minutes: 120
```

When a limit is reached, the workflow should pause or fail according to policy.

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
