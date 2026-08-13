# Agent Flow Command Reference

The installed executable is `agent-flow`. Run `agent-flow help` for the command
list and `agent-flow help <command>` for a command synopsis. `init` is reserved
for a future runtime surface and is not active.

## General and authoring commands

| Command | Purpose |
|---|---|
| `agent-flow help [command]` | Show general help or one command's usage. |
| `agent-flow --version` | Print the installed package version. `version` and `-v` are aliases. |
| `agent-flow skills list` | List bundled workflow authoring and review skills. |
| `agent-flow skills install --destination agents` | Install skills under the current repository's `.agents/skills/`. Existing skill directories are not replaced. |
| `agent-flow skills install --destination codex` | Install skills under `$CODEX_HOME/skills`, falling back to `~/.codex/skills`. Existing skill directories are not replaced. |
| `agent-flow validate <workflow>` | Validate YAML structure, references, policies, and safety constraints. |
| `agent-flow lint <workflow>` | Report complexity and risky authoring patterns. |
| `agent-flow explain <workflow>` | Explain normalized steps, artifacts, policies, collaboration, and warnings. |
| `agent-flow graph <workflow>` | Print a deterministic workflow graph. |
| `agent-flow simulate <workflow> --fixture <file>` | Traverse fixture-defined paths and materialize a simulation summary without executing workflow steps. |

The authoring skills help create and review workflow YAML. They do not invoke
lifecycle commands.

## Run lifecycle commands

| Command | Purpose |
|---|---|
| `agent-flow run <workflow> --id <run-id> [--fixture <file>]` | Create or reuse a persistent run and execute supported steps. CLI session providers currently require a fixture unless an application configures a provider programmatically. |
| `agent-flow resume <run-id> --outcome <choice> [--fixture <file>]` | Continue a paused approval or other choice-based interaction. |
| `agent-flow resume <run-id> --answer <value> [--fixture <file>]` | Continue a paused input request. JSON scalar, array, and object values are parsed; other input remains text. |
| `agent-flow inject <run-id> <session-name> <context>` | Persist additional context for an active recovery session and mark it dirty for rerun. |
| `agent-flow status <run-id>` | Print the durable run status and current lifecycle details. |
| `agent-flow logs <run-id>` | Print ordered events with sequence, timestamp, event type, and JSON payload. |
| `agent-flow artifacts <run-id>` | List artifact path, status, kind, and content type. |
| `agent-flow pause <run-id>` | Pause an active run. |
| `agent-flow cancel <run-id>` | Cancel a non-terminal run. |

Run IDs beginning with an option-like prefix can be separated from options
with `--` where the command synopsis shows `[--]`.

## Retention and portability commands

```text
agent-flow cleanup ([--] <run-id> | --older-than <duration> [--status <status>]) [--approve]
agent-flow archive [--] <run-id> [--output <file>]
agent-flow export [--] <run-id> --format zip [--output <file>]
```

- `cleanup` applies the persisted workflow's declared retention policy. A
  duration is an integer followed by `m`, `h`, or `d`. Status filters are
  `pending`, `running`, `waiting`, `paused`, `completed`, `failed`, or
  `cancelled`. `--approve` supplies explicit approval to a policy that requires
  it; it does not bypass a denied or invalid policy.
- On Linux, `archive` writes a portable ZIP to `.agent-flow/archives/` by
  default, and `export --format zip` writes one in the repository root by
  default. Portable ZIP creation fails closed on other platforms until an
  equivalent descriptor-relative publication primitive is available.
- `--output <file>` selects a different repository-contained destination.
  Existing targets, path escapes, unsafe directory replacement, and oversized
  content fail closed.

Cleanup can remove artifact backing files, but it preserves the SQLite run and
event history plus retention-protected evidence. Archive and export capture a
consistent run snapshot with state, ordered events, artifact metadata,
failures, approvals, sessions, and available registered artifact content.

## Exit behavior

Successful commands, including an explicit `pause`, exit with status `0`. A
`run` or `resume` execution that ends paused exits with status `3` so automation
can distinguish a durable pause from completion. Invalid usage, validation or
execution failures, unavailable runs, and unknown commands return nonzero
statuses and diagnostics on standard error. Scripts should use the exit status
and persisted `status` output rather than matching prose alone.
