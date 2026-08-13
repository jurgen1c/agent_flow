# Agent Flow Example Workflows

These examples show how Agent Flow workflows can start simple and grow into recovery or collaborative automation.

## Examples

| File | Style | Shows |
|---|---|---|
| `workflows/simple-ci.yml` | Pipeline | Run deterministic local checks |
| `workflows/jira-ticket-spec.yml` | Pipeline | Fetch Jira ticket JSON, transform it to Markdown, and ask LM to create a concise spec |
| `workflows/ticket-lifecycle.yml` | Recovery pipeline | LM/FM ticket implementation lifecycle with CI and PR feedback |
| `workflows/ci-triage.yml` | Recovery pipeline | Reusable nested workflow for failed CI |
| `workflows/pr-feedback-loop.yml` | Recovery pipeline | Poll PR comments and route actionable feedback to FM |
| `workflows/implement-review-collab.yml` | Collaborative | Implementer/reviewer loop with decision records |
| `workflows/content-review-collab.yml` | Collaborative | Marketing copy with product approval |

## Suggested Demo Order

1. Run `simple-ci.yml` to show Agent Flow can run normal commands.
2. Run `jira-ticket-spec.yml` to show LM summarization with MCP.
3. Run `ticket-lifecycle.yml` to show LM/FM orchestration.
4. Trigger a fake CI failure and show `ci-triage.yml`.
5. Run `pr-feedback-loop.yml` to show bounded PR comment and CI recovery.
6. Show `implement-review-collab.yml` to demonstrate code collaboration.
7. Show `content-review-collab.yml` to demonstrate content collaboration and product approval.

## Collaborative Examples

The collaborative workflows use `provider: fixture` so every documented path can be demonstrated offline. Treat them as templates: replace fixture providers, repository-specific artifact inputs, file scopes, notification channels, and limits before using them in a live repository.

Both examples include approved, changes-requested, and unresolved fixtures. The changes-requested path exhausts the bounded review loop and pauses for human disagreement resolution; the unresolved path routes directly to an explicit user question.

```sh
bun run build
bun run dist/agent-flow.js validate examples/workflows/implement-review-collab.yml
bun run dist/agent-flow.js simulate examples/workflows/implement-review-collab.yml \
  --fixture examples/fixtures/implement-review-collab/approved.json
bun run dist/agent-flow.js run examples/workflows/implement-review-collab.yml \
  --id implement-review-demo \
  --fixture examples/fixtures/implement-review-collab/approved.json
```

`implement-review-collab.yml` demonstrates formal code review, a bounded revision loop, explicit implementation approval, a retained decision record, and human escalation. `content-review-collab.yml` adds advisory product feedback before formal review, explicit product approval, and a retained content decision record.

Use the corresponding fixture directories to substitute `changes-requested.json` or `unresolved.json`. Fixture runs create `.agent-flow/` state in the current repository; use a unique run ID for each run.

## Notes

`jira-ticket-spec.yml` uses the built-in `jira_ticket_to_markdown` transform.
Fixture simulation can provide `ticket.json` and inspect the derived `ticket.md`
without network access or free-form scripting.

`ci-triage.yml` has offline fixtures for flake, formatting, implementation,
environment, unknown, and user-required classifications. From the repository
root, validate and simulate one with:

```sh
bun run build
bun run dist/agent-flow.js validate examples/workflows/ci-triage.yml
bun run dist/agent-flow.js simulate examples/workflows/ci-triage.yml \
  --fixture examples/fixtures/ci-triage/flake.json
```

The example's `fixture` session providers also support controlled execution:

```sh
bun run dist/agent-flow.js run examples/workflows/ci-triage.yml \
  --id ci-triage-demo \
  --fixture examples/fixtures/ci-triage/implementation.json
```

The formatting path runs `examples/scripts/fix-formatting.sh`, which prefers a
repository `bin/rubocop -A` command and otherwise uses `bun run lint --fix`.
Unknown classifications pause before automatic routing. User-required
classifications pause at `ask_user`; environment failures return `unresolved`.
Flake, formatting, and fixture-backed FM remediation return `remediated` so a
parent workflow can retry its failed CI step.

`pr-feedback-loop.yml` also has fully offline simulation fixtures for a clean
completion, actionable-comment remediation, recovered and unresolved CI,
high-risk and frontier-budget short circuits, and bounded-loop timeout. For
example:

```sh
bun run dist/agent-flow.js validate examples/workflows/pr-feedback-loop.yml
bun run dist/agent-flow.js simulate examples/workflows/pr-feedback-loop.yml \
  --fixture examples/fixtures/pr-feedback-loop/remediated.json
bun run dist/agent-flow.js simulate examples/workflows/pr-feedback-loop.yml \
  --fixture examples/fixtures/pr-feedback-loop/timeout.json
```

Simulation fixtures declare how a loop ended with `loop_termination`:
`condition_met`, `max_iterations`, or `max_duration`. Exhausting either bound
reports `timed_out` and does not fall through to the workflow's completion
step. The fixtures provide PR state, classified comments, CI outcomes, and
push outcomes without contacting GitHub or another network service.

These files are examples, not guaranteed to run unchanged in every repo. Users should adapt:

- CI commands.
- MCP server names and tool names.
- GitHub/Jira configuration.
- Prompt paths.
- Notification channels.
- File scopes.
