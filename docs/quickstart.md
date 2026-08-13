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
