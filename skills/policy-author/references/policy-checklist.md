# Policy checklist

## Bounds

Set positive maximums for frontier calls, total model calls, step attempts, review or recovery cycles, loop iterations, and duration wherever those resources exist. Route exhaustion to a defined pause or terminal failure.

## Authority and files

Default sessions to advisory. Grant modification, approval, blocking, merge, or pause authority only when required. Give every writer a narrow include scope, sensitive exclusions, and one owner. Reject ambiguous or overlapping writes unless reconciliation is explicit.

## Consequential operations

Deny unsafe operations by default. Require explicit approval for publication, merge, deployment, destructive commands, and cleanup. Ensure a denied or indeterminate runtime check prevents the action rather than logging and continuing.

## Data and evidence

Keep secrets out of prompts and arguments. Reference protected artifacts instead of embedding credentials. Preserve failure payloads, approved evidence, decision records, and final summaries. Invalidate approvals when protected inputs change.

## Verification

Use `validate`, `lint`, `explain`, `graph`, and branch-specific simulation fixtures. Inspect every allowed, paused, denied, exhausted, and invalidated path without executing workflow steps.
