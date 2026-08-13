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
```

Installation refuses to replace an existing skill directory. The skills only
author, inspect, review, debug, simplify, or policy-harden workflow YAML; they
do not invoke workflow lifecycle commands.

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
agent-flow run workflow.yml --id example-run --fixture fixture.json
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

## Development

```bash
bun install --frozen-lockfile
bun run ci
bun run verify:package
```

Release instructions are in [docs/releasing.md](docs/releasing.md).
