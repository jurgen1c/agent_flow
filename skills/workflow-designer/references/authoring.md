# Workflow authoring reference

## Style decision

- Use `pipeline` for a mostly straight-line process with bounded branches.
- Use `recovery_pipeline` only when failed work should be classified, remediated, and retried.
- Use `collaborative` only when separate sessions need explicit roles, ownership, review, consultation, or approval.

## Minimal skeleton

```yaml
name: example
version: 1
style: pipeline
maturity: draft
inputs: {}
sessions: {}
steps: []
policies: {}
```

## Authoring checklist

1. Give every step a unique ID and every transition a defined target.
2. Treat artifacts as the contract between steps. Declare each output before a later step reads it.
3. Bound loops, retries, duration, model calls, and frontier calls.
4. Give writing sessions explicit authority and non-overlapping file scopes.
5. Add terminal behavior for failures, exhausted bounds, unresolved reviews, and rejected approvals.
6. Require approval for consequential publication, cleanup, or unsafe operations.
7. Keep secrets out of prompts, command arguments, fixtures, and retained artifacts.

## Safe inspection

The authoring surface is limited to `validate`, `lint`, `explain`, `graph`, and fixture-backed `simulate`. These commands inspect or traverse a definition without executing its steps. Never use lifecycle commands while authoring.
