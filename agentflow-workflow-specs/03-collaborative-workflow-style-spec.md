# Agentflow Collaborative Workflow Style Spec

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

Authority must be explicit.

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

These can be represented as specialized step types or as `session_request` steps with required schemas.

## 7. Implement and Review Example

```yaml
name: implement-with-review
version: 1
style: collaborative

collaboration:
  enabled: true
  max_review_cycles: 3
  on_disagreement: ask_user

sessions:
  implementer:
    provider: frontier
    resume: true
    authority:
      can_modify_files: true
  reviewer:
    provider: frontier
    resume: true
    authority:
      can_request_changes: true
      can_approve: true

steps:
  - id: implement
    type: session_request
    session: implementer
    prompt: prompts/implement.md
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
    then: review
```

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

Challenge result should be bounded and recorded. It should not become a free-form debate.

## 10. Decision Records

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

## 11. Parallel Collaboration

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

1. Parallel writers must declare non-overlapping file scopes.
2. If scopes overlap, workflow validation fails unless `allow_overlap: true` and conflict policy is set.
3. Read-only advisory sessions may run in parallel without file scopes.
4. Parent workflow decides what to do if one branch fails.

## 12. Approval Invalidation

Approvals must be invalidated when relevant artifacts change.

Example:

- Reviewer approves `git.diff`.
- Implementer changes code after approval.
- Engine marks reviewer approval stale.
- Review step must rerun before merge.

Invalidation config:

```yaml
approvals:
  reviewer_approval:
    invalidated_by:
      - git.diff
      - implementation-summary.md
```

## 13. Disagreement Handling

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

## 14. Collaboration Edge Cases

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

## 15. Notifications

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

## 16. Validation Rules

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

## 17. Recommended Defaults

For v1:

- One writer at a time by default.
- Parallel advisory sessions allowed.
- Parallel file writers require explicit non-overlapping scopes.
- Implementer/reviewer cycles capped at 3.
- Human escalation on unresolved disagreement.
- Reviewers cannot modify files by default.
- Decision records retained permanently unless user cleans them manually.

