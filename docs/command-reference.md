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
| `agent-flow config validate [--config <file>]` | Validate global targets, repository aliases, kinds, endpoints, and references without contacting a model. |
| `agent-flow providers list [--config <file>]` | Print the redacted resolved alias-to-target catalog. |
| `agent-flow providers doctor [--config <file>]` | Check credential environment variables and target readiness without generating model output. |
| `agent-flow validate <workflow>` | Validate YAML structure, references, policies, and safety constraints. |
| `agent-flow lint <workflow>` | Report complexity and risky authoring patterns. |
| `agent-flow explain <workflow>` | Explain normalized steps, artifacts, policies, collaboration, and warnings. |
| `agent-flow graph <workflow>` | Print a deterministic workflow graph. |
| `agent-flow simulate <workflow> --fixture <file>` | Load and validate the reachable workflow registry, then traverse fixture-defined paths and materialize a summary without executing workflow steps. |

The authoring skills help create and review workflow YAML. They do not invoke
lifecycle commands.

## Run lifecycle commands

| Command | Purpose |
|---|---|
| `agent-flow run <workflow> --id <run-id> [--fixture <file>] [--input-file <json>] [--input <key=value>]... [--config <file>] [--provider <alias=target>]...` | Create or reuse a persistent run, merge validated inputs, load configured providers, and execute supported steps. Input and provider flags are repeatable; provider overrides are new-run only. |
| `agent-flow run <workflow> --id <run-id> [--profile <name>] [--model <name>] [--reasoning-effort <level>]` | Start a run with optional Codex overrides. The selected values are persisted and reused when the run resumes. |
| `agent-flow resume <run-id> --outcome <choice> [--fixture <file>] [--config <file>]` | Continue a paused approval or other choice-based interaction, or settle/resume a linked child workflow, after verifying pinned provider bindings. |
| `agent-flow resume <run-id> --answer <value> [--fixture <file>] [--config <file>]` | Continue a paused input request after verifying pinned provider bindings. JSON scalar, array, and object values are parsed; other input remains text. |
| `agent-flow resume <run-id> --reset-session <session-name> [--fixture <file>] [--config <file>]` | Explicitly discard a missing native CLI session ID and retry the waiting step in a fresh Codex or Claude session. |
| `agent-flow inject <run-id> <session-name> <context>` | Persist additional context for an active recovery session and mark it dirty for rerun. |
| `agent-flow status <run-id>` | Print the durable run status and current lifecycle details. |
| `agent-flow logs <run-id>` | Print ordered events with sequence, timestamp, event type, and JSON payload. |
| `agent-flow artifacts <run-id>` | List artifact path, status, kind, and content type. |
| `agent-flow pause <run-id>` | Pause an active run. |
| `agent-flow cancel <run-id>` | Cancel a non-terminal run. |

Run IDs beginning with an option-like prefix can be separated from options
with `--` where the command synopsis shows `[--]`.

Run inputs merge from `--fixture`, then `--input-file`, then repeatable
`--input` flags, so the later source wins across sources. An input file must be
a JSON object. Each CLI value uses `key=value`; JSON values are parsed and
other values remain text. Duplicate CLI keys, unknown inputs, missing required
inputs, and attempts to change a persisted run input fail closed.

Codex precedence is step, run flags, session, configured target, then Codex's
normal configuration. Profiles contain only letters, numbers, hyphens, and
underscores. Reasoning effort is `minimal`, `low`, `medium`, `high`, or
`xhigh`.

The CLI resolves `type: workflow` children beside the entry workflow unless
repository `.agent-flow.yml` sets a repository-relative `workflows` file or
directory. Reachable children are validated before execution, run as linked
children, and resume through the parent run. Nested input expressions must be
whole values using `step.id`, `inputs.<name>`, or `artifacts.<path>`; embedded
expressions are rejected during preflight. A direct `mcp_call` requires a
programmatic host adapter; the stock CLI supports `via: codex` with a named,
resumable Codex session, exactly one output artifact, and the MCP server
configured in the installed Codex. That artifact is bound to the completed MCP
event result rather than Codex's final model-authored message.

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
