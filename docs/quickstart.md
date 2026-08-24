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
- Use <no model / fixture / local / frontier / codex:profile> for model-backed steps.
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

## Configure local or cloud models

Agent Flow separates workflow policy from model-specific configuration. The
workflow names a provider boundary; the application that hosts the runtime
chooses the actual endpoint and model and supplies credentials to an adapter.

For a local model, declare `provider: local`. For a remote model, use
`provider: frontier`, `provider: codex:<profile>`, or an application-defined
custom provider name. Allow the exact name in policy and add budgets:

```yaml
sessions:
  writer:
    provider: local
    resume: false

policies:
  model_usage:
    allowed_providers: [local]

limits:
  max_model_calls: 2
```

Changing `local` to `frontier` also requires a suitable
`max_frontier_calls` limit. Named Codex profiles, such as `codex:reviewer`, are
treated as frontier providers for budgeting.

There is intentionally no built-in `model` field in workflow YAML and no
automatic credential or provider discovery. A host application registers the
adapter explicitly:

```ts
import {
  createAgentFlowSessionProviderRegistry,
  type AgentFlowSessionProviderAdapter
} from "@jurgen1c/agent-flow";

const localModel = process.env.MY_APP_LOCAL_MODEL;
const cloudModel = process.env.MY_APP_CLOUD_MODEL;

if (!localModel || !cloudModel) {
  throw new Error("Configure MY_APP_LOCAL_MODEL and MY_APP_CLOUD_MODEL.");
}

const localAdapter: AgentFlowSessionProviderAdapter = async (request) => ({
  outputs: await localModelClient.generateOutputs({
    model: localModel,
    prompt: request.prompt.content,
    inputs: request.inputs,
    outputPaths: request.outputs,
    signal: request.signal
  }),
  metadata: { model: localModel }
});

const cloudAdapter: AgentFlowSessionProviderAdapter = async (request) => ({
  outputs: await cloudModelClient.generateOutputs({
    model: cloudModel,
    prompt: request.prompt.content,
    inputs: request.inputs,
    outputPaths: request.outputs,
    signal: request.signal
  }),
  metadata: { model: cloudModel }
});

const providers = createAgentFlowSessionProviderRegistry([
  { kind: "local", enabled: true, adapter: localAdapter },
  { kind: "frontier", enabled: true, adapter: cloudAdapter }
]);
```

`localModelClient` and `cloudModelClient` represent your application or model
SDK. Their `generateOutputs` function must return an object whose keys exactly
match `request.outputs`. Keep API keys in the host process or secret manager;
never put them in workflow YAML, prompts, returned metadata, or artifacts. Pass
the resulting registry to `executeAgentFlowCommandPipeline` and to
`resumeAgentFlowCommandPipeline` when resuming a run.

The packaged `agent-flow run` command currently creates only a fixture
provider. It does not load live adapters, so live local/cloud execution requires
a host application using the programmatic API. For deterministic CLI execution,
declare `provider: fixture` in the workflow and supply the matching fixture:

```bash
agent-flow run workflows/fixture-backed.yml \
  --id fixture-backed-demo \
  --fixture fixtures/fixture-backed.json
```

A workflow that declares `local`, `frontier`, `codex:<profile>`, or a custom
live provider cannot be switched to fixture mode only by adding `--fixture`;
use a fixture-provider authoring variant or change the declaration deliberately.

See [session provider boundaries](session-providers.md) for registration kinds,
the full adapter request/response contract, opt-in behavior, and evidence
handling.

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
