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
    driver: openai-responses
    model: YOUR_CODEX_MODEL
    api_key_env: OPENAI_API_KEY
    enabled: true

  claude-main:
    kind: frontier
    driver: anthropic-messages
    model: YOUR_CLAUDE_MODEL
    api_key_env: ANTHROPIC_API_KEY
    max_output_tokens: 4096
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
read. Built-in drivers send only the declared prompt and input artifact content;
native coding CLIs require an application-defined custom adapter with its own
filesystem and process boundary.

Commit a repository-root `.agent-flow.yml` containing portable aliases:

```yaml
version: 1
providers:
  planner: { kind: frontier, target: claude-main }
  implementer: { kind: frontier, target: codex-main }
  local-drafter: { kind: local, target: qwen-local }
  local-reviewer: { kind: local, target: gemma-local }
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

Use aliases in workflow sessions. Each step selects its session, so one step
can use Claude, another Codex, and local steps can use Qwen and Gemma:

```yaml
sessions:
  planner: { provider: planner }
  implementer:
    provider: implementer
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

All built-in configured drivers return declared artifacts without
file-modification authority and receive only the declared prompt and input
artifacts. Native coding CLIs and file-writing providers require a custom
adapter with an application-enforced execution boundary.

All built-in drivers accept UTF-8 text inputs and require one structured JSON
response with exactly the declared output paths. Provider retries are not
hidden: the workflow's retry policy remains authoritative. All three built-in
HTTP drivers are non-resumable; use a custom adapter when a provider needs to
preserve native conversational state.

See [session provider boundaries](session-providers.md) for registration kinds,
the programmatic extension API, permission behavior, and evidence handling.

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
