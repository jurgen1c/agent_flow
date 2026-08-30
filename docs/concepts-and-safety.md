# Agent Flow Concepts and Safety

## Choose the smallest workflow style

| Style | Use it when | Typical controls |
|---|---|---|
| `pipeline` | Steps are mostly linear and a failure can retry, pause, or fail. | Step failure routes, approvals, file scopes, retention. |
| `recovery_pipeline` | Expected failures need classification, bounded remediation, and a return to the failed step. | Recovery routes, retry and duration limits, remediated/unresolved results. |
| `collaborative` | Named sessions need explicit roles, handoffs, formal review, consultation, challenge, or disagreement handling. | Session authority, non-overlapping file scopes, bounded review cycles, approval and disagreement policies. |

Start with a pipeline. Move to recovery only when failure handling is a real
part of the process, and move to collaboration only when separate roles and
authority boundaries add value. The [examples](../examples/README.md) show all
three styles with offline fixtures.

## Committed and generated directories

A typical consuming repository separates source-of-truth inputs from rebuildable
or execution-specific state:

```text
repository/
├── workflows/                       # committed Agent Flow YAML
├── .agent-flow.yml                  # committed provider aliases
├── docs/agent-memory/               # committed Agent Memory Markdown/YAML
├── agent-memory.config.yaml         # committed Memory configuration
├── .agent-flow/                     # generated Flow execution state
│   ├── agent-flow.sqlite
│   ├── archives/
│   └── runs/
└── .agent-memory/                   # generated local compatibility state
```

Agent Memory global-mode SQLite caches live outside the repository. Whether
Memory uses global or local cache mode, canonical files under
`docs/agent-memory/` remain committed and generated SQLite state does not.
Agent Flow's `.agent-flow/` directory is always repository-local generated
state and must be ignored by Git. The similarly named `.agent-flow.yml` file is
intentional configuration and should be committed.

## Run state and artifacts

`.agent-flow/agent-flow.sqlite` is authoritative for run records, ordered
events, session state, approvals, failures, notifications, retention outcomes,
and artifact metadata. Files below `.agent-flow/runs/` back registered artifact
content. A missing or retention-deleted backing file remains visible through
its metadata instead of erasing the audit trail.

Use the CLI rather than editing generated files:

```bash
agent-flow status <run-id>
agent-flow logs <run-id>
agent-flow artifacts <run-id>
```

Final summaries, failure evidence, decision records, and evidence supporting an
approved decision receive stronger retention treatment than ordinary transient
logs. Each workflow still needs an explicit retention policy appropriate to its
data and audit requirements.

Workflow inputs are persisted run values. Within `session_request`, `inputs`
has a narrower meaning: it is only the list of artifact paths read by that
step. Use the step's scalar `context` mapping to carry values such as a ticket
key into the provider prompt. Context is bounded, sensitivity-checked, and
checksum-audited without persisting its resolved values in request evidence.

## Safety model

Agent Flow validates a workflow before execution and fails closed on malformed
YAML, unsafe references, invalid policies, path escapes, undeclared authority,
or inconsistent collaboration rules. Important controls include:

- repository containment for artifact, archive, and export paths;
- explicit `file_scope` limits for file-writing sessions and operations;
- model, budget, approval, cleanup, and unsafe-operation policies;
- declared session roles and authority, with advisory behavior by default;
- bounded frontier calls, durations, recovery attempts, and review cycles;
- stale approval detection when declared evidence changes;
- deterministic simulation fixtures that traverse paths without executing
  commands or model sessions;
- durable pauses for human input or approval instead of implicit consent;
- retention rules that are evaluated from the workflow persisted with the run.

The built-in Codex provider is an explicit exception to Agent Flow's
session-filesystem boundary. Agent Flow invokes the installed Codex with its
normal environment, so Codex configuration and sandboxing—not Agent Flow
`file_scope`—enforce its repository access. Direct MCP and other custom adapters
remain host-provided capabilities.

Validation proves the declaration is internally safe; it does not make an
arbitrary shell command safe. Review commands, scripts, model prompts, MCP
targets, notification adapters, file scopes, and artifact inputs as code before
running them. Use least authority and keep credentials out of workflow YAML,
fixtures, prompts, artifacts, events, and portable archives.

## Safe promotion path

Before a live run:

1. Review the YAML and every referenced prompt or script.
2. Run `config validate`, `providers doctor`, `validate`, `lint`, `explain`,
   and `graph`.
3. Exercise important branches with bounded, non-secret simulation fixtures.
4. Confirm file scopes, policies, approvals, limits, notifications, and
   retention behavior.
5. Run with a unique ID and inspect durable state after completion or pause.
6. Use cleanup, archive, and export only after checking their policy and data
   consequences.

See the [workflow specifications](specifications/) for the exact contracts.
