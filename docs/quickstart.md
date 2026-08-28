# Agent Flow Quickstart

## Requirements

- Node.js 25.9.0 or newer.
- A Git repository for the workflow and its repository-local state.
- Bun only when developing or testing Agent Flow itself.

Install the CLI globally when it is a user tool:

```bash
npm install --global @jurgen1c/agent-flow
agent-flow --version
```

For a repository-pinned installation, install it as a development dependency
and replace `agent-flow` below with `npx agent-flow`:

```bash
npm install --save-dev @jurgen1c/agent-flow
```

## Create a workflow

Create `workflows/quickstart.yml`:

```yaml
name: quickstart
version: 1
style: pipeline
maturity: draft

description: Run one deterministic repository check.

steps:
  - id: node-version
    type: command
    command: node --version
    on_failure:
      then: fail
```

Keep workflow definitions in version control. Add generated run state to the
repository ignore file before executing anything:

```gitignore
.agent-flow/
```

## Check before running

Use the read-only authoring commands first:

```bash
agent-flow validate workflows/quickstart.yml
agent-flow lint workflows/quickstart.yml
agent-flow explain workflows/quickstart.yml
agent-flow graph workflows/quickstart.yml
```

`validate` checks structure, references, and safety declarations. `lint`
reports risky or overcomplicated authoring patterns. `explain` and `graph`
make steps, policies, artifact flow, and branches easier to inspect.

Simulation is also non-executing, but it requires a fixture file. The packaged
[examples](../examples/README.md) include workflows and fixtures that can be
validated and simulated offline.

## Run and inspect

Run the workflow with a repository-unique run ID:

```bash
agent-flow run workflows/quickstart.yml --id quickstart-001
agent-flow status quickstart-001
agent-flow logs quickstart-001
agent-flow artifacts quickstart-001
```

The run persists even after the command exits. `status` reports the durable
lifecycle state, `logs` prints ordered events, and `artifacts` lists registered
artifact metadata. `resume` continues pauses backed by a persisted approval,
manual gate, input request, or disagreement; operator-requested pauses and
failure routes ending in `pause` do not create a resumable CLI interaction. An
active recovery session can receive new context with `inject`.

Generated state is written below `.agent-flow/` and must not be committed. The
workflow YAML and other intentional project inputs remain committed.

## Have an agent author a workflow

Agent Flow ships workflow-authoring skills for coding agents. Install them in
the current repository for agents that discover `.agents/skills`, or in your
Codex user skills directory:

```bash
agent-flow skills install --destination agents
# Or: agent-flow skills install --destination codex
```

Then adapt this prompt:

```text
Use $workflow-designer to create workflows/<name>.yml for this repository.

Goal:
- <describe the outcome of the workflow>

Process:
- <list the ordered steps and important branches>

Inputs and outputs:
- <list existing files/artifacts the workflow may read>
- <list durable artifacts it should produce>

Execution:
- Use the configured <provider alias> for each model-backed session.
- Require human approval before <any consequential action>.
- Do not allow writes outside <the permitted paths>.
- Bound retries, duration, model calls, and review cycles.

Choose the simplest supported style. Keep secrets out of prompts, arguments,
fixtures, and artifacts. After writing the YAML, run agent-flow validate, lint,
explain, and graph. Do not run the workflow. Report assumptions and any adapter
or fixture configuration still required.
```

The skill may also recommend `$pipeline-designer`, `$recovery-designer`,
`$collaboration-designer`, or `$policy-author` when the process needs a more
specific design pass. Authoring skills inspect and write definitions; they do
not start workflow runs.

For a concrete multi-model starting point, use:

```text
Create an Agent Flow workflow at workflows/feature-delivery.yml.

Use the `planner` provider to turn the feature request into plan.md.
Use `implementer` to turn the plan into implementation-design.md.
Use `local-reviewer` to review the result and produce review.md.
Make implementation depend on planning and review depend on implementation.
Keep every configured model session artifact-only, declare every input and
output explicitly, and add appropriate timeouts and retry limits.

Validate the workflow with Agent Flow and show me the commands to explain,
simulate, and run it. Do not start the run.
```

## Configure local or cloud models

Agent Flow keeps machine-specific model settings outside workflow YAML. Create
the global config at
`${XDG_CONFIG_HOME:-~/.config}/agent-flow/config.yml` (or set
`AGENT_FLOW_CONFIG`/pass `--config`):

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

  openai-api:
    kind: frontier
    driver: openai-responses
    model: YOUR_OPENAI_MODEL
    api_key_env: OPENAI_API_KEY
    enabled: true

  anthropic-api:
    kind: frontier
    driver: anthropic-messages
    model: YOUR_ANTHROPIC_MODEL
    api_key_env: ANTHROPIC_API_KEY
    max_output_tokens: 4096
    enabled: true

  qwen-local:
    kind: local
    driver: openai-compatible
    base_url: http://127.0.0.1:11434/v1
    model: qwen3
    enabled: true

  gemma-local:
    kind: local
    driver: openai-compatible
    base_url: http://127.0.0.1:11434/v1
    model: gemma4
    enabled: true
```

Do not put API keys in YAML. `api_key_env` names the environment variable to
read for API drivers. Native drivers use the existing `codex` or `claude`
executable and its current login. Codex also works without either config file:
declare `provider: codex` in a session. Agent Flow lets the installed Codex load
its normal user and repository config, permissions/sandbox, rules, skills,
plugins, and MCP servers. It does not require `bubblewrap` or `flock` for Codex.

Codex model, profile, and reasoning settings are optional. Configure them under
`codex:` on a session or step, or pass `--model`, `--profile`, and
`--reasoning-effort` when starting a run. Precedence is step, run, session,
configured target, then Codex config. See the official Codex
[profile documentation](https://learn.chatgpt.com/docs/config-file/config-advanced#profiles)
and [`model_reasoning_effort` reference](https://learn.chatgpt.com/docs/config-file/config-reference).

Define separate targets when workflow roles need different Codex guarantees:

```yaml
targets:
  codex-implement:
    kind: frontier
    driver: codex-cli
    model: YOUR_CODEX_MODEL
    profile: implementation
    reasoning_effort: high
    enabled: true
  codex-review:
    kind: frontier
    driver: codex-cli
    model: YOUR_CODEX_MODEL
    profile: deep-review
    reasoning_effort: xhigh
    enabled: true
```

Map them to repository aliases and select those aliases from workflow sessions.
A session with `resume: true` keeps one Codex thread across every step that uses
that session; a separate Agent Flow run always starts a fresh thread.

Use `agent-flow providers doctor` to verify configured CLI targets. Claude Code
authentication is checked directly; Codex readiness is limited to the executable
because its active provider may authenticate through ambient custom-provider
configuration rather than `codex login`.

Commit a repository-root `.agent-flow.yml` containing portable aliases:

```yaml
version: 1
providers:
  planner: { kind: frontier, target: claude-main }
  implementer: { kind: frontier, target: codex-main }
  local-drafter: { kind: local, target: qwen-local }
  local-reviewer: { kind: local, target: gemma-local }
workflows: workflows
```

The alias kind is a safety boundary: a local alias cannot be redirected to a
frontier target, or vice versa. Validate and inspect the resolved catalog
without contacting a model:

```bash
agent-flow config validate
agent-flow providers list
agent-flow providers doctor
```

`doctor` checks required credential variables and target readiness only. It
does not make a paid API call or generate model output.

The optional `workflows` path is relative to the repository root. A
`type: workflow` step names a workflow found there (or, when omitted, beside
the entry workflow), supplies its declared inputs, and lists the exact child
artifacts to promote after completion. Child approvals and input requests pause
the parent and continue when the parent run is resumed.

Use aliases in workflow sessions. Each step selects its session, so one step
can use Claude, another Codex, and local steps can use Qwen and Gemma:

```yaml
sessions:
  planner: { provider: planner, resume: true }
  implementer:
    provider: implementer
    resume: true
  drafter: { provider: local-drafter }
  reviewer: { provider: local-reviewer }

policies:
  model_usage:
    allowed_providers: [planner, implementer, local-drafter, local-reviewer]

limits:
  max_model_calls: 8
  max_frontier_calls: 4
```

Swap a target for one new run without editing either file:

```bash
agent-flow run examples/workflows/multi-provider.yml \
  --id gemma-draft \
  --fixture examples/fixtures/multi-provider/inputs.json \
  --provider local-drafter=gemma-local
```

Overrides are repeatable and apply only when creating a run. Agent Flow pins a
privacy-safe target fingerprint; resume rejects model, endpoint, driver, kind,
or permission drift. Credential rotation does not change the fingerprint.

All built-in drivers accept UTF-8 text inputs and require one structured JSON
response with exactly the declared output paths. Provider retries are not
hidden: the workflow's retry policy remains authoritative. The three HTTP
drivers are artifact-only and non-resumable. `codex-cli` and `claude-code` run
at the repository root and preserve native context when their workflow session
declares `resume: true`.

Native read-only sessions use Codex read-only or Claude plan mode. To let a CLI
edit the checkout, grant `authority.can_modify_files: true` and a non-empty
`file_scope` on the session. Agent Flow snapshots the checkout before and after
the process, rejects changes not allowed by every scope layer, and leaves those
changes in place for inspection rather than attempting an unsafe rollback. Its
host sandbox keeps `.git` read-only, hides `.agent-flow`, and leaves unrelated
host paths unmounted. Audited native invocations share a per-repository write
lock with command steps and file-writing custom adapters
so concurrent Agent Flow runs cannot be attributed to one another. Native
agents receive only the selected CLI's authentication/provider
variables plus basic process, locale, proxy, and certificate variables—not the
parent process's arbitrary secrets. Only the selected CLI, its required
read-only interpreter/toolchain files, system runtime files, its own state, and
the repository are visible.

If Codex or Claude no longer has a persisted external session, the run pauses
instead of silently losing context. Start a fresh native session explicitly:

```bash
agent-flow status <run-id>
agent-flow resume <run-id> --reset-session <session-name> [--fixture <file>] [--config <file>]
```

For an implement-review-fix sequence that deliberately keeps one native
conversation, use one resumable workflow session for all three steps:

```yaml
sessions:
  coder:
    provider: implementer
    resume: true
    authority: { can_modify_files: true }
    file_scope:
      include: [src/**, tests/**, docs/**]

steps:
  - { id: implement, type: session_request, session: coder, prompt: prompts/implement.md, inputs: [task.md], outputs: [implementation-summary.md] }
  - { id: review, type: session_request, session: coder, prompt: prompts/review.md, inputs: [implementation-summary.md], outputs: [review.md] }
  - { id: fix, type: session_request, session: coder, prompt: prompts/fix.md, inputs: [review.md], outputs: [implementation-summary.md], overwrite: true }
```

The complete checked example is
[`examples/workflows/native-cli-session.yml`](../examples/workflows/native-cli-session.yml).

See [session provider boundaries](session-providers.md) for registration kinds,
the programmatic extension API, permission behavior, and evidence handling.

### Use the original programmatic registrations

The original provider names are reserved registration paths, not automatically
configured defaults. They work when an application constructs a registry and
passes it to Agent Flow; they do not require the global target config or the
repository `.agent-flow.yml` alias file.

```ts
import { createAgentFlowSessionProviderRegistry } from "@jurgen1c/agent-flow";

const providers = createAgentFlowSessionProviderRegistry([
  { kind: "fixture", adapter: fixtureAdapter },
  { kind: "local", enabled: true, adapter: localAdapter },
  { kind: "frontier", enabled: true, adapter: frontierAdapter },
  {
    kind: "codex_profile",
    profile: "reviewer",
    enabled: true,
    adapter: codexReviewerAdapter
  },
  {
    kind: "custom",
    name: "private-control-plane",
    adapter: privateControlPlaneAdapter
  }
]);
```

Workflow sessions name those registered providers, and model-backed steps name
the session they should use:

```yaml
sessions:
  drafter:
    provider: local
  reviewer:
    provider: codex:reviewer

steps:
  - id: draft
    type: session_request
    session: drafter
    prompt: prompts/draft.md
    inputs: [request.md]
    outputs: [draft.md]

  - id: review
    type: session_request
    session: reviewer
    prompt: prompts/review.md
    inputs: [request.md, draft.md]
    outputs: [review.md]

policies:
  model_usage:
    allowed_providers: [local, codex:reviewer]

limits:
  max_model_calls: 2
  max_frontier_calls: 1
```

Use `provider: fixture`, `provider: frontier`, or
`provider: private-control-plane` the same way when the corresponding adapter is
registered. The packaged CLI can supply `fixture` with `--fixture`; the other
programmatic registrations require an application host to pass `providers` to
`executeAgentFlowCommandPipeline` and `resumeAgentFlowCommandPipeline`.

### Keep using a custom provider

The YAML configuration path does not replace the original provider registry.
Use a custom provider when an application already owns the model client, needs
a different coding CLI, a provider-specific resume protocol, or a custom
filesystem/process sandbox:

```ts
import {
  createAgentFlowSessionProviderRegistry,
  executeAgentFlowCommandPipeline,
  type AgentFlowSessionProviderAdapter
} from "@jurgen1c/agent-flow";

const customAdapter: AgentFlowSessionProviderAdapter = async (request) => ({
  outputs: await privateControlPlane.generate(request)
});

const providers = createAgentFlowSessionProviderRegistry([
  { kind: "custom", name: "private-control-plane", adapter: customAdapter }
]);

await executeAgentFlowCommandPipeline(
  store,
  runId,
  workflow,
  undefined,
  providers
);
```

The workflow uses `provider: private-control-plane` directly; it does not need
a global target or repository alias. This path is programmatic because the
installed CLI does not load arbitrary adapter code from YAML.

Custom registrations retain the previous logical-name binding. Agent Flow
cannot detect that an application changed the implementation behind that name
between processes. If that adapter needs target/model drift detection and
local/frontier budget classification, register it with `registerConfigured`
and provide complete, privacy-safe target metadata instead.

## Try the offline examples

From a clone of this repository, the collaborative example can complete with a
deterministic fixture and no external model provider:

```bash
bun run build
bun run dist/agent-flow.js validate examples/workflows/implement-review-collab.yml
bun run dist/agent-flow.js simulate examples/workflows/implement-review-collab.yml \
  --fixture examples/fixtures/implement-review-collab/approved.json
bun run dist/agent-flow.js run examples/workflows/implement-review-collab.yml \
  --id implement-review-demo \
  --fixture examples/fixtures/implement-review-collab/approved.json
```

Use a new run ID when repeating an example. See the
[command reference](command-reference.md) for lifecycle, cleanup, archive, and
export commands.
