# Agent Flow

Agent Flow is a standalone, persistent workflow runtime and CLI for
policy-aware agent pipelines.

It validates and simulates workflow YAML, runs command, MCP, session-request,
condition, and artifact-transform steps, persists lifecycle state in SQLite,
and records artifacts, failures, notifications, and retention outcomes.

## Install

```bash
npm install @jurgen1c/agent-flow
```

Node.js 25.9.0 or newer is required. Bun is used for development and testing.

## CLI

```bash
agent-flow help
agent-flow validate workflow.yml
agent-flow lint workflow.yml
agent-flow explain workflow.yml
agent-flow graph workflow.yml
agent-flow simulate workflow.yml --fixture fixture.json
agent-flow run workflow.yml --id example-run --fixture fixture.json
agent-flow inject example-run fixer "Additional remediation context"
agent-flow status example-run
agent-flow logs example-run
agent-flow artifacts example-run
agent-flow pause example-run
agent-flow cancel example-run
```

Repository-local state is stored in `.agent-flow/`. Do not commit it.

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

Pass the workflow registry as the final `executeAgentFlowCommandPipeline`
argument when a workflow uses `route_to.workflow`. Recovery session providers
report `metadata.recovery_status` as `remediated` or `unresolved`.
Routed remediation that writes repository files must explicitly authorize them
through the existing session authority and layered `file_scope` policy. Context
injected into an active recovery session marks it dirty and reruns the provider
with a persisted `recovery-context/injected.md` input.

Programmatic CLI routing is exported from `@jurgen1c/agent-flow/cli`.
Schemas are exported from `@jurgen1c/agent-flow/schemas/config`,
`@jurgen1c/agent-flow/schemas/failure-classification`, and
`@jurgen1c/agent-flow/schemas/workflow`.

Collaborative workflows opt in with `collaboration.enabled: true`. Their
sessions declare roles and may declare ownership, authority, and file scopes;
sessions remain advisory unless stronger authority is explicit. Both `explain`
and `graph` surface the normalized role and authority model.

## Architecture

Agent Flow depends on `@jurgen1c/agent-core` for strict YAML, repository/path
safety, and portable SQLite. It does not depend on Agent Memory and contains
no Memory adapter. The optional integration adapter lives in
`@jurgen1c/agentic-development`.

See [docs/architecture.md](docs/architecture.md) for boundaries and
[docs/specifications](docs/specifications) for workflow behavior.

## Development

```bash
bun install --frozen-lockfile
bun run ci
bun run verify:package
```

Release instructions are in [docs/releasing.md](docs/releasing.md).
