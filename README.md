# Agent Flow

Agent Flow is a standalone, persistent workflow runtime and CLI for
policy-aware agent pipelines.

It validates and simulates workflow YAML, runs command, MCP, session-request,
formal review, approval, decision-record, condition, and artifact-transform
steps, persists lifecycle state in SQLite, and records artifacts, failures,
notifications, and retention outcomes.

## Install

```bash
npm install @jurgen1c/agent-flow
```

Node.js 25.9.0 or newer is required. Bun is used for development and testing.

Install the bundled workflow authoring and review skills into the current
repository's `.agents/skills` directory or into the Codex user skill directory
(`$CODEX_HOME/skills`, falling back to `~/.codex/skills`):

```bash
agent-flow skills list
agent-flow skills install --destination agents
agent-flow skills install --destination codex
agent-flow config validate
agent-flow providers list
agent-flow providers doctor
```

Installation refuses to replace an existing skill directory. The skills only
author, inspect, review, debug, simplify, or policy-harden workflow YAML; they
do not invoke workflow lifecycle commands.

## Create a workflow with an agent

After installing the skills, give your coding agent a concrete process and ask
it to use the workflow designer. For example:

```text
Use $workflow-designer to create workflows/pr-check.yml for this repository.

The workflow should run the repository's formatter, tests, and type checker in
that order. If a check fails, preserve useful failure evidence and stop; do not
attempt an automatic fix. Keep generated run state out of version control,
choose the simplest supported workflow style, and add explicit limits and
least-authority policies wherever they apply.

After writing the YAML, run agent-flow validate, lint, explain, and graph.
Do not run the workflow. Summarize any assumptions I need to review.
```

Replace the process and filename with your own. Add requirements such as a
local model, a cloud model, recovery, review, or human approval only when the
workflow needs them. The [quickstart](docs/quickstart.md#have-an-agent-author-a-workflow)
contains a reusable prompt template.

## CLI

```bash
agent-flow help
agent-flow skills list
agent-flow skills install --destination agents
agent-flow validate workflow.yml
agent-flow lint workflow.yml
agent-flow explain workflow.yml
agent-flow graph workflow.yml
agent-flow simulate workflow.yml --fixture fixture.json
agent-flow run workflow.yml --id example-run
agent-flow run workflow.yml --id alternate-model --provider drafter=gemma-local
agent-flow resume example-run --reset-session coder
agent-flow inject example-run fixer "Additional remediation context"
agent-flow status example-run
agent-flow logs example-run
agent-flow artifacts example-run
agent-flow pause example-run
agent-flow cancel example-run
agent-flow cleanup example-run
agent-flow cleanup --older-than 30d --status completed
agent-flow archive example-run
agent-flow export example-run --format zip
```

Repository-local state is stored in `.agent-flow/`. Do not commit it.
Cleanup applies each persisted workflow's retention policy while preserving
SQLite run state and events, final summaries, failure evidence, decision
records, and approved evidence. `archive` writes to `.agent-flow/archives/`;
`export` writes a portable ZIP in the repository root. Both ZIP forms contain
run state, ordered events, artifact metadata, failures, approvals, sessions,
and every available registered artifact. Use `--output <file>` to select a
different repository-contained file destination. On Linux, publication walks
each output directory from a repository-root descriptor and refuses directory
replacement races; other platforms fail closed until they provide an equivalent
descriptor-relative primitive. Portable archive
content is capped at 64 MiB, with JSON encoded incrementally against the same
bound before buffers are materialized. Publication staging links use randomized
hidden names and are retained because Node does not expose an identity-bound
unlink primitive; this avoids deleting a pathname concurrently replaced by
another process.

## API

```ts
import {
  createAgentFlowWorkflowRegistry,
  injectAgentFlowRecoveryContext,
  openAgentFlowRunState,
  parseAgentFlowFailureClassification,
  parseAgentFlowWorkflowOrThrow,
  validateAgentFlowWorkflow
} from "@jurgen1c/agent-flow";

const workflow = parseAgentFlowWorkflowOrThrow(source);
const ciTriageWorkflow = parseAgentFlowWorkflowOrThrow(ciTriageSource);
const validation = validateAgentFlowWorkflow(workflow);
const classification = parseAgentFlowFailureClassification(classifierOutput);
const store = await openAgentFlowRunState({ cwd: process.cwd() });
const recoveryWorkflows = createAgentFlowWorkflowRegistry()
  .register("ci-triage", ciTriageWorkflow);
// While a recovery provider is running:
// injectAgentFlowRecoveryContext(store, runId, "fixer", "New user context");
```

The CLI loads concrete model targets from the user config and portable aliases
from the repository. Programmatic registration remains available for custom
integrations; see [Session provider boundaries](docs/session-providers.md).
The configuration layer is additive: the original fixture, local, frontier,
named Codex-profile, and custom registry APIs remain supported. A workflow
host can continue registering `kind: "custom"` adapters without either config
file. The installed CLI cannot load arbitrary application code, so those
providers still run through the programmatic API.

Put concrete models and endpoints in
`${XDG_CONFIG_HOME:-~/.config}/agent-flow/config.yml`:

```yaml
version: 1
targets:
  codex-main:
    kind: frontier
    driver: codex-cli
    model: YOUR_CODEX_MODEL
    profile: deep-review
    reasoning_effort: high
    enabled: true
  claude-main:
    kind: frontier
    driver: claude-code
    model: YOUR_CLAUDE_MODEL
    enabled: true
  qwen-local:
    kind: local
    driver: openai-compatible
    base_url: http://127.0.0.1:11434/v1
    model: qwen3
    enabled: true
```

Map stable workflow aliases in the committed `.agent-flow.yml`:

```yaml
version: 1
providers:
  implementer: { kind: frontier, target: codex-main }
  drafter: { kind: local, target: qwen-local }
```

Steps select sessions, and sessions select those aliases:

```yaml
sessions:
  writer: { provider: drafter }

policies:
  model_usage:
    allowed_providers: [drafter]

limits:
  max_model_calls: 2
```

API targets name a credential environment variable with `api_key_env`; secrets
never belong in either YAML file. Use `--provider alias=target` on a new run to
swap Qwen for Gemma, Claude for Codex, or another same-kind target. See
[Configure local or cloud models](docs/quickstart.md#configure-local-or-cloud-models)
for native CLI and HTTP drivers plus a multi-model workflow. `codex-cli` and
`claude-code` use the developer's existing CLI installation and login. Native
targets require an explicit `model`. Codex targets may also select a named
`profile` and pin `reasoning_effort` to `minimal`, `low`, `medium`, `high`, or
`xhigh`. Agent Flow passes model and reasoning as CLI overrides, hashes the
Codex base config and selected profile file into the target fingerprint, and
rejects resume if any of them drift. Provider doctor also strict-loads the
selected profile with the installed Codex CLI without starting a model request.
Sessions with
`resume: true` persist the native Codex thread or Claude session across steps.
If that external session disappears, inspect the paused run and explicitly
start a fresh native session with
`agent-flow resume <run-id> --reset-session <session-name>`.

Native CLIs run at the repository root. Read-only sessions use the CLI's
read-only mode. File-writing sessions require `authority.can_modify_files: true`
and a non-empty effective `file_scope`; Agent Flow audits all resulting changes
against every scope layer and fails closed on out-of-scope writes. It does not
roll those filesystem changes back. Built-in native execution currently
requires Linux with `bubblewrap` and `flock`; the host sandbox makes the
checkout read-only unless write authority is granted, keeps `.git` read-only,
hides `.agent-flow`, and leaves unrelated host paths unmounted. Audited native
invocations share a per-repository write lock with Agent Flow command steps and
file-writing custom adapters. The child receives a minimal environment:
CLI authentication/provider variables, proxy and certificate settings, locale,
and basic process variables such as `PATH` and `HOME`; unrelated secrets are not
inherited. The CLI's agent-facing sandbox also denies access to its mounted
login and session-state directory. Targets without a profile disable ambient
Codex user configuration. Profile targets load Codex's base-plus-profile
configuration layers and hash both files for drift detection. Repository-local
`.codex/config.toml` remains hidden, and user skills, hooks, MCP servers, apps,
plugins, web search, analytics/telemetry, notifications, and other ambient
hosted tools remain disabled for native provider invocations. Profile and base
layers that refer to mutable instruction, project-document discovery,
model-catalog, sub-agent, skill, or SQLite files are rejected so resumable-run
drift detection remains complete. Agent Flow owns the shell environment policy,
so model-spawned commands receive core process variables without inheriting the
credentials forwarded to Codex. For a selected custom Codex model provider,
only environment variables named by its `env_key` or `env_http_headers` are
forwarded, and both provider doctor and execution preflight fail when one is
missing; unrelated `OPENAI_*` credentials are omitted unless explicitly required.
Profiled provider endpoints must use HTTPS without embedded
credentials, queries, or fragments. Command-backed provider authentication,
the built-in `amazon-bedrock` provider, custom shell-environment policies, and
overrides of Agent Flow's reserved `permissions.agent_flow_native` profile are
rejected.

Ordinary custom registrations preserve the previous behavior: Agent Flow pins
the provider name in the workflow but cannot fingerprint changes inside
application-owned adapter code. Applications that want configured-provider
drift protection for their own adapter can use `registerConfigured` with a
complete local/frontier descriptor and a privacy-safe target fingerprint.

Pass the workflow registry as the final `executeAgentFlowCommandPipeline`
argument when a workflow uses `route_to.workflow`. Recovery session providers
report `metadata.recovery_status` as `remediated` or `unresolved`.
Routed remediation that writes repository files must explicitly authorize them
through the existing session authority and layered `file_scope` policy. Context
injected into an active recovery session marks it dirty and reruns the provider
with a persisted `recovery-context/injected.md` input.

Programmatic CLI routing is exported from `@jurgen1c/agent-flow/cli`.
Schemas are exported from `@jurgen1c/agent-flow/schemas/approval`,
`@jurgen1c/agent-flow/schemas/challenge`,
`@jurgen1c/agent-flow/schemas/config`, `@jurgen1c/agent-flow/schemas/consult`,
`@jurgen1c/agent-flow/schemas/decision-record`,
`@jurgen1c/agent-flow/schemas/disagreement`,
`@jurgen1c/agent-flow/schemas/failure-classification`,
`@jurgen1c/agent-flow/schemas/review`, and
`@jurgen1c/agent-flow/schemas/workflow`.

Collaborative workflows opt in with `collaboration.enabled: true`. Their
sessions declare roles and may declare ownership, authority, and file scopes;
sessions remain advisory unless stronger authority is explicit. Both `explain`
and `graph` surface the normalized role and authority model.
Formal `review` steps invoke the declared reviewer session, require explicit
`can_request_changes` and `can_approve` authority, and publish JSON artifacts
with `approved`, `changes_requested`, or `unresolved` status plus a structured
`findings` array. Conditions can route directly from those persisted results.
Bounded `consult` and `challenge` steps invoke their declared target session
with one static question and explicit input artifacts. Consults publish advice
and recommendations and may block only when both the step and target authority
allow it. Challenges persist an answered or unresolved rationale. Both result
types are strict JSON artifacts and malformed provider output fails closed.
Approval steps either invoke a declared session with `can_approve: true` or
pause for `reviewer: human`; both paths persist the outcome in run state.
Top-level `approvals.<step-id>.invalidated_by` declarations watch normalized
artifact paths. Changes mark the approved attempt and its output stale, and a
fresh attempt is required before workflow completion or a `can_merge` session.
Decision-record steps publish durable JSON under `decision-records/` by
default, validate every referenced artifact at execution time, and are exempt
from default retention deletion.
Collaborative `parallel` steps must declare `strategy: fail_fast`. Read-only
sessions may share scopes, while writers need non-overlapping effective
`file_scope` declarations unless `allow_overlap: true` and a non-empty
`conflict_policy` explicitly authorize reconciliation.
Collaborative review loops declare both `max_review_cycles` and an
`on_disagreement` terminal policy. Policies may ask the user, invoke a bounded
arbiter, fall back from an arbiter to the user, let the reviewed owner decide,
or fail. Resolution rounds are persisted under unique per-episode paths below
`disagreements/`, and the run event log records the selected resolution path.
Workflow completion, failure, pause, human approval waiting, and collaborative
disagreement events can notify configured channels. `terminal` and `system`
are built in; applications can inject synchronous `email`, `slack`, `webhook`,
and `command` adapters or register other named channels. Delivery failures are
recorded and remain non-terminal unless the notification rule is required.

## Architecture

Agent Flow depends on `@jurgen1c/agent-core` for strict YAML, repository/path
safety, and portable SQLite. It does not depend on Agent Memory and contains
no Memory adapter. The optional integration adapter lives in
`@jurgen1c/agentic-development`.

See [docs/architecture.md](docs/architecture.md) for boundaries and
[docs/specifications](docs/specifications) for workflow behavior.

## Documentation

The [documentation hub](docs/README.md) provides an end-to-end guide:

- [Quickstart](docs/quickstart.md) for installation and a first persistent run.
- [Command reference](docs/command-reference.md) for every active CLI command.
- [Concepts and safety](docs/concepts-and-safety.md) for workflow styles,
  policies, run state, artifacts, and the committed/generated file boundary.
- [Execution security](docs/execution-security.md) for command and adapter
  boundaries, sensitive-input handling, and deny-by-default unsafe channels.
- [Packages and Memory integration](docs/packages-and-memory.md) for the roles of
  Agent Flow, Agent Memory, Agentic Development, and Agent Core.
- [Operations and packaging](docs/operations-and-packaging.md) for validation,
  simulation, cleanup, archives, package verification, and release expectations.
- [Run inspection API](docs/run-inspection-api.md) for token-protected local run
  state, timelines, failures, approvals, and decisions.

## Development

```bash
bun install --frozen-lockfile
bun run ci
bun run verify:package
```

Release instructions are in [docs/releasing.md](docs/releasing.md).
