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
5. Show `implement-review-collab.yml` to demonstrate collaboration.

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

These files are examples, not guaranteed to run unchanged in every repo. Users should adapt:

- CI commands.
- MCP server names and tool names.
- GitHub/Jira configuration.
- Prompt paths.
- Notification channels.
- File scopes.
