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
workflows: workflows
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
`claude-code` use the developer's existing CLI installation and login. Codex
can also be selected directly with `provider: codex`, without an Agent Flow
provider catalog. Model, profile, and reasoning overrides may be declared on a
session or step, or supplied to `agent-flow run`; omitted settings come from
Codex's normal configuration.
Sessions with
`resume: true` persist the native Codex thread or Claude session across steps.
If that external session disappears, inspect the paused run and explicitly
start a fresh native session with
`agent-flow resume <run-id> --reset-session <session-name>`.

Codex runs at the repository root with the normal process environment and owns
its configuration, authentication, permissions/sandbox, rules, skills,
plugins, and MCP servers. Agent Flow does not add an outer `bubblewrap` or
`flock` boundary and does not suppress ambient Codex features. Claude and HTTP
provider boundaries are unchanged.

Codex-mediated `mcp_call` steps declare one output artifact. Agent Flow verifies
the exact completed MCP event and derives that artifact from the tool result,
not from the model's final response.

Run inputs can come from a fixture, a JSON object file, or repeatable CLI flags:

```bash
agent-flow run workflow.yml --id AF-123 \
  --input-file inputs.json --input ticket_key=AF-123 --input dry_run=true
```

They merge in that order, with later sources winning. Duplicate `--input` keys,
unknown inputs, and missing required inputs fail before run creation.

Ordinary custom registrations preserve the previous behavior: Agent Flow pins
the provider name in the workflow but cannot fingerprint changes inside
application-owned adapter code. Applications that want configured-provider
drift protection for their own adapter can use `registerConfigured` with a
complete local/frontier descriptor and a privacy-safe target fingerprint.

Nested `type: workflow` steps execute a referenced workflow as a linked child
run. By default the CLI resolves referenced workflows from the entry workflow's
directory; set `workflows:` in `.agent-flow.yml` to use an explicit registry
file or directory. The child receives the declared `inputs`, pauses and resumes
through its parent, and promotes only the paths declared by the parent step's
`outputs` after the child completes. Missing, duplicate, and recursive workflow
references fail before execution. Programmatic hosts pass the workflow registry
as the final `executeAgentFlowCommandPipeline` argument. Child input expressions
must occupy the whole value and may use only `step.id`, `inputs.<name>`, or
`artifacts.<path>`; plain values without expression delimiters remain literal.

Recovery session providers
report `metadata.recovery_status` as `remediated` or `unresolved`.
Routed remediation that writes repository files must explicitly authorize them
through the existing session authority and layered `file_scope` policy. Codex
remediation still requires those declarations, but Codex's own sandbox is the
filesystem enforcement boundary for the pass-through provider. Context
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
