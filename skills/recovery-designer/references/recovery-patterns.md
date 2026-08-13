# Recovery patterns

A recovery route must consume the persisted failure payload, not inferred session history or a mutable log pathname.

```yaml
on_failure:
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

Require all of the following:

- positive bounds for attempts, recovery cycles, duration, and model calls;
- immutable attempt-scoped failure evidence with secret redaction;
- a defined `remediated` return target and `unresolved` terminal route;
- no retry path that bypasses its budget check;
- explicit file authority for remediation sessions;
- no deletion of failure payloads or attachments needed for audit or retry;
- fixtures for success, remediated, unresolved, and exhausted outcomes.

Use `validate`, `lint`, `explain`, `graph`, and fixture-backed `simulate` only.
