# Pipeline patterns

Prefer a pipeline when one owner can describe the process as inputs, ordered work, bounded decisions, and outputs.

```yaml
name: checked-task
version: 1
style: pipeline
maturity: draft
steps:
  - id: check
    type: command
    command: mkdir -p reports && printf 'checks passed\n' > reports/check.log
    timeout_seconds: 900
    outputs: [reports/check.log]
    then: summarize
  - id: summarize
    type: command
    command: mkdir -p summaries && wc -l < reports/check.log > summaries/check.txt
    outputs: [summaries/check.txt]
```

Check that:

- each referenced step, session, input, and artifact exists;
- every branch has an explicit continuation or terminal outcome;
- artifacts are written before use and overwrite is explicit;
- loops and calls are bounded;
- commands are deterministic and do not hide downloads or destructive operations;
- a pipeline is not carrying collaboration roles or recovery machinery it does not need.

Use `simulate` with fixtures to exercise each branch without executing commands.
