# Packages and Agent Memory Integration

Agent Flow and Agent Memory are separate products. They share low-level
primitives through Agent Core, and the optional integration lives in Agentic
Development.

| Package | Executable | Role |
|---|---|---|
| `@jurgen1c/agent-memory-cli` | `agent-memory` | Owns committed repository Memory, retrieval, validation, and generated Memory indexes. |
| `@jurgen1c/agent-flow` | `agent-flow` | Owns workflow validation, simulation, execution, policies, run state, artifacts, retention, archive, and export. |
| `@jurgen1c/agentic-development` | none | Thin integration package. It owns the explicit Memory-to-Flow adapter and no runtime state. |
| `@jurgen1c/agent-core` | none | Shared strict YAML, repository/path safety, and portable SQLite primitives used by the standalone products. |

The old `@jurgen1c/agentflow-cli` and `@jurgen1c/agent-tools` planning names are
not the package names published by these repositories. Use
`@jurgen1c/agent-flow` and `@jurgen1c/agentic-development`.

## Different sources of truth

Agent Memory captures durable project knowledge. Its committed Markdown and
YAML under `docs/agent-memory/` are the source of truth; its SQLite database is
a generated retrieval index.

Agent Flow captures one workflow definition plus each run's changing execution
state. Commit the workflow definition, prompts, templates, and intentional
fixtures. Do not commit `.agent-flow/`, which contains generated SQLite state
and run artifacts.

Using Agent Flow does not initialize Memory, and using Agent Memory does not
start a workflow. Neither product imports the other.

## Explicit Memory context capture

Install the integration package only when a consuming application needs to
capture Agent Memory context as an Agent Flow artifact:

```bash
npm install @jurgen1c/agent-memory-cli @jurgen1c/agent-flow \
  @jurgen1c/agentic-development
```

```ts
import { openAgentFlowRunState } from "@jurgen1c/agent-flow";
import {
  createMemoryContextAdapter
} from "@jurgen1c/agentic-development/memory-flow-adapter";

const runState = await openAgentFlowRunState();
const adapter = createMemoryContextAdapter({ runState });

await adapter.captureContext({
  runId: "run-123",
  boundary: { kind: "run_start" },
  request: { task: "Review the authentication change" }
});
```

The consuming repository must already have a compiled Agent Memory cache and a
matching Agent Flow run. The adapter reads Memory through its public API and
writes the snapshot through Flow's public artifact API. It does not copy either
product's database or make Flow's generated run state into canonical Memory.

Treat captured context like any other run artifact: bound it to the task, avoid
secrets, declare the required file and model authority, and give it an explicit
retention policy. Promote durable conclusions into canonical Agent Memory only
through Agent Memory's own reviewed update workflow.
