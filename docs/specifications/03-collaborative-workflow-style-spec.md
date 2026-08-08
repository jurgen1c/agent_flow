# Agent Flow Collaborative Workflow Style Spec

## 1. Purpose

Collaborative workflows allow multiple named sessions to work together with explicit roles, authority, handoffs, reviews, consultations, approvals, and decision records.

Use this style when one session should not own all judgment. Examples:

- Implementer writes code; reviewer reviews.
- Implementer consults designer for UX feedback.
- Marketing session drafts copy; product session approves.
- Security session must approve auth changes.
- Orchestrator coordinates specialists and escalates disagreements.

## 2. Design Goals

1. Make collaboration structured, not free-form endless chat.
2. Define authority and ownership explicitly.
3. Support parallel work safely.
4. Record decisions and approvals.
5. Prevent infinite debate loops.
6. Invalidate approvals when underlying artifacts change.

## 3. Collaboration Principles

1. Pipelines remain the default. Collaboration is opt-in.
2. Only one writer should mutate a file scope at a time unless scopes are declared.
3. Advisory sessions cannot block progress unless granted authority.
4. Reviews must produce structured findings.
5. Consultations must ask bounded questions.
6. Disagreements must have a terminal policy.
7. Human override is always possible.

## 4. Session Roles

```yaml
collaboration:
  enabled: true

sessions:
  orchestrator:
    provider: local
    role: workflow_owner
    owns:
      - workflow_state
      - routing
      - notifications
    authority:
      can_pause: true
      can_merge: true

  implementer:
    provider: frontier
    role: code_implementer
    owns:
      - code_changes
      - tests
    file_scope:
      include:
        - app/**
        - spec/**
    authority:
      can_modify_files: true

  reviewer:
    provider: frontier
    role: code_reviewer
    owns:
      - code_review
    authority:
      can_request_changes: true
      can_approve: true
      can_modify_files: false

  designer:
    provider: frontier
    role: design_advisor
    owns:
      - ux_feedback
    authority:
      can_advise: true
      can_block: false
```

## 5. Authority Model

Stronger, non-advisory authority must be explicit.

| Authority | Meaning |
|---|---|
| `can_modify_files` | Session may edit files |
| `can_request_changes` | Session may send work back to owner |
| `can_approve` | Session may approve an artifact or step |
| `can_block` | Session may prevent continuation |
| `can_merge` | Session may perform merge if policies pass |
| `can_pause` | Session may pause workflow |
| `can_advise` | Session may give non-blocking recommendations |

Default rule: sessions can only advise unless granted stronger authority.
An omitted authority mapping therefore has effective `advisory` authority. A
session that declares a `file_scope` must grant `can_modify_files: true`;
blocking consultations must grant the consulted session `can_block: true`;
formal reviewers must grant `can_request_changes: true` and `can_approve: true`;
and approval actors must grant `can_approve: true`.

`agent-flow explain` and `agent-flow graph` render each session's role,
ownership, effective authority, and file scope. Programmatic graph output
exposes the same normalized metadata in its top-level `sessions` list so
inspection consumers do not need to reinterpret raw YAML.

## 6. Collaboration Step Types

Collaborative workflows use normal step types plus these collaboration patterns:

| Pattern | Purpose |
|---|---|
| `handoff` | Transfer artifact ownership/context |
| `consult` | Ask bounded advice from another session |
| `review` | Formal review of artifacts or diff |
| `challenge` | Reviewer asks implementer for rationale |
| `approval` | Session or human approves/rejects |
| `decision_record` | Persist a decision and owner |

These are executable specialized step types with strict runtime contracts.

## 7. Implement and Review Example

```yaml
name: implement-with-review
version: 1
style: collaborative
maturity: experimental

collaboration:
  enabled: true
  max_review_cycles: 3
  on_disagreement: ask_user

sessions:
  implementer:
    provider: frontier
    role: code_implementer
    resume: true
    authority:
      can_modify_files: true
  reviewer:
    provider: frontier
    role: code_reviewer
    resume: true
    authority:
      can_request_changes: true
      can_approve: true

steps:
  - id: implement
    type: session_request
    session: implementer
    prompt: prompts/implement.md
    inputs:
      - spec.md
    outputs:
      - implementation-summary.md

  - id: review
    type: review
    reviewer: reviewer
    subject: implementer
    artifacts:
      - implementation-summary.md
      - git.diff
    outputs:
      - reviews/code-review.json

  - id: route_review
    type: condition
    branches:
      - if: reviews.code_review.status == "approved"
        then: approved
      - if: reviews.code_review.status == "changes_requested"
        then: address_review

  - id: address_review
    type: session_request
    session: implementer
    prompt: prompts/address-review.md
    inputs:
      - reviews/code-review.json
      - implementation-summary.md
    outputs:
      - implementation-summary.md
    overwrite: true
    then: review
```

Formal review outputs use the `schemas/review.schema.json` contract. Every
declared output is JSON with a `status` of `approved`, `changes_requested`, or
`unresolved`, plus a `findings` array. Each finding is an object with a
non-empty `summary`; implementations may add severity, artifact, location, or
other structured fields. The runtime validates every result before atomically
publishing it as a `review_output` artifact, so condition steps can route on
the persisted status and malformed findings cannot influence routing.

## 8. Consultation Example

```yaml
- id: consult_designer
  type: consult
  from: implementer
  to: designer
  question: "Does this UI handle empty, loading, and error states well?"
  artifacts:
    - screenshots/component.png
    - app/components/progress_panel.tsx
  output: consultations/designer-feedback.json
  blocking: false
```

Consultation output:

```json
{
  "status": "advice",
  "blocking": false,
  "summary": "Loading and empty states are clear, but error state lacks recovery action.",
  "recommendations": [
    {
      "priority": "medium",
      "recommendation": "Add retry action to error state."
    }
  ]
}
```

Consult questions are one static question ending in a single question mark and
are limited to 4096 UTF-8 bytes. The `artifacts`, `.json` `output`, and boolean
`blocking` fields are required. `schemas/consult.schema.json` defines the
exported JSON contract; at runtime, `parseAgentFlowConsultResult` enforces the
same fields and invariants before publishing a `consult_output`. An advisory
consult cannot publish `blocking: true` or `status: "blocked"`.

## 9. Challenge and Rationale

Reviewers may ask implementers why something was done.

```yaml
- id: reviewer_challenge
  type: challenge
  from: reviewer
  to: implementer
  question: "Why did you add a new service instead of extending ExporterService?"
  artifacts:
    - git.diff
  output: challenges/exporter-service-rationale.json
```

Challenge result:

```json
{
  "status": "answered",
  "rationale": "ExporterService owns the existing wire format; the new service isolates the incompatible format.",
  "evidence": ["git.diff"]
}
```

Challenge questions follow the same single-question and 4096-byte bound.
`artifacts` and one `.json` `output` are required.
`schemas/challenge.schema.json` defines the exported JSON contract; at runtime,
`parseAgentFlowChallengeResult` enforces the same fields and invariants before
persisting a `challenge_output`. Malformed or missing rationale fails closed
instead of starting a free-form debate.

## 10. Approvals

Session approval invokes a declared session with explicit `can_approve`
authority. The provider writes strict JSON to `output`, or to
`approvals/<step-id>.json` when `output` is omitted. When an ID contains
characters that require filename sanitization, the generated filename includes
a stable digest so distinct step IDs cannot collide:

```yaml
- id: approve_release
  type: approval
  reviewer: release_reviewer
  artifacts: [release-notes.md]
  on_approve: publish
  on_reject: revise
```

The JSON result contains only `status` (`approved` or `rejected`) and a
non-empty `decision`. Human approval uses `reviewer: human`; it pauses with
`approve`, `reject`, and `cancel` outcomes and resumes through the same CLI
interaction contract as a manual gate. Both paths persist their outcome in
the run-state approval registry.

## 11. Decision Records

Every meaningful decision should be persisted.

```json
{
  "decision_id": "dec_001",
  "owner": "implementer",
  "topic": "Use existing ExporterService",
  "rationale_summary": "Existing service already owns CSV formatting and authorization context.",
  "consulted": ["reviewer"],
  "approved_by": ["reviewer"],
  "artifacts": [
    "implementation-summary.md",
    "reviews/code-review.json"
  ],
  "created_at": "..."
}
```

Decision records are retained by default.
The executable step accepts `owner`, `topic`, `artifacts`, and optional
`rationale_summary`, `consulted`, `approved_by`, and `output`. It writes to
`decision-records/<step-id>.json` by default, adding the same stable digest when
the ID requires filename sanitization, and fails closed if any referenced
artifact is unavailable.

## 12. Parallel Collaboration

Parallel work is allowed when safe.

```yaml
- id: parallel_feature_work
  type: parallel
  strategy: fail_fast
  branches:
    - id: backend
      session: backend_implementer
      file_scope:
        include: ["app/services/**", "spec/services/**"]
    - id: frontend
      session: frontend_implementer
      file_scope:
        include: ["app/javascript/**", "app/views/**"]
    - id: docs
      session: docs_writer
      file_scope:
        include: ["docs/**"]
```

Rules:

1. Collaborative parallel steps must declare `strategy: fail_fast`; this is the only supported parent branch-failure strategy.
2. Parallel writers must declare non-overlapping file scopes.
3. If scopes overlap, workflow validation fails unless `allow_overlap: true` and a non-empty conflict policy is set.
4. Read-only advisory sessions may run in parallel without file scopes.

This contract validates safe declarations. It does not add a concurrent runtime
scheduler or automatically apply a declared conflict policy. Fixture simulation
continues to collect branch diagnostics and reports conflicting artifact values
as unresolved.

## 13. Approval Invalidation

Approvals must be invalidated when relevant artifacts change.

Example:

- Reviewer approves `git.diff`.
- Implementer changes code after approval.
- Engine marks reviewer approval stale.
- Review step must rerun before merge.

Invalidation config:

```yaml
approvals:
  approve_release:
    invalidated_by:
      - git.diff
      - implementation-summary.md
```

Each key must name an `approval` step. `invalidated_by` is a non-empty,
duplicate-free list of normalized static repository-relative artifact paths,
and it cannot include that approval step's own output or the runtime-managed
`final-summary.md` artifact. The runtime records an
approved outcome as `stale` when a watched artifact is replaced, deleted, or
found to have checksum drift. It retains the original decision and records the
invalidation evidence in approval context.

Successful retention cleanup preserves the evidence and outcome artifacts for
approved decisions so cleanup cannot retroactively stale a completed run.

A stale approval makes its output artifact unreadable, pauses policy checks
that require the approval, blocks sessions with `can_merge: true`, and prevents
successful workflow completion. The workflow must route through the affected
approval step again before reaching those continuation points; the newest
attempt for that step determines whether continuation is allowed.

## 14. Disagreement Handling

Disagreements must have a defined policy.

```yaml
collaboration:
  on_disagreement:
    strategy: arbiter_then_user
    arbiter: architecture_reviewer
    max_rounds: 1
```

Supported strategies:

| Strategy | Meaning |
|---|---|
| `ask_user` | Pause and ask human |
| `arbiter` | Ask designated arbiter session |
| `arbiter_then_user` | Try arbiter once, then human |
| `owner_decides` | Artifact owner decides |
| `fail` | End workflow |

## 15. Collaboration Edge Cases

| Edge Case | Required Behavior |
|---|---|
| Reviewer and implementer loop forever | Enforce max cycles |
| Reviewer asks vague question | Require structured challenge schema |
| Designer gives blocking feedback without authority | Treat as advisory |
| Implementer edits outside file scope | Pause or fail |
| Parallel branches edit same file | Detect conflict and pause |
| Approval becomes stale | Invalidate and rerun approval |
| Consultation includes sensitive logs | Redact or deny based on policy |
| Arbiter disagrees with reviewer | Follow configured strategy |
| Human injects new requirement | Mark affected decisions and approvals stale |

## 16. Notifications

Collaborative workflows should notify on:

- Review changes requested.
- Approval waiting.
- Disagreement escalated.
- Human input needed.
- Workflow completed or failed.

Example:

```yaml
notify:
  - on: approval.waiting
    channels: [system, terminal]
  - on: collaboration.disagreement
    channels: [email_personal]
  - on: workflow.paused
    channels: [email_personal, terminal]
```

## 17. Validation Rules

Collaborative validation should enforce:

- Collaboration is explicitly enabled.
- Sessions have roles.
- Authority is explicit for blocking/modifying/approving.
- Parallel file scopes do not overlap.
- Review cycles have max limits.
- Disagreement strategy is defined.
- Approval invalidation is configured for mutable artifacts.
- Advisory sessions cannot block unless `can_block: true`.
- Decision records are enabled for key approvals.

## 18. Recommended Defaults

For v1:

- One writer at a time by default.
- Parallel advisory sessions allowed.
- Parallel file writers require explicit non-overlapping scopes.
- Implementer/reviewer cycles capped at 3.
- Human escalation on unresolved disagreement.
- Reviewers cannot modify files by default.
- Decision records retained permanently unless user cleans them manually.
