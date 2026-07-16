# Agentflow Monorepo Architecture

Status: implemented workspace shell, run persistence foundation, and Agent Memory context adapter
Ticket: AM-6, AM-7, AM-9, AM-18, AM-19, AM-21

## Intent

This document defines the package boundaries for bringing Agentflow into the
Agent Memory repository without weakening the existing Agent Memory contract.
The monorepo should make shared code easier to version, but Agent Memory must
remain usable as a standalone repository-local memory CLI.

Agentflow is the workflow runtime. Agent Memory is the durable repository
knowledge system. They can integrate, but they should not collapse into one
runtime.

## Package Boundaries

| Boundary | Workspace path | Public package name | Responsibility |
| --- | --- | --- | --- |
| Agent Memory CLI | root `package.json` publishes the package; `packages/cli` remains the private CLI workspace | `@jurgen1c/agent-memory-cli`; workspace name is private `@jurgen1c/agent-memory-cli-workspace` | Published `agent-memory` executable, CLI routing, command parsing, help text, and Node-compatible bundle entrypoint. |
| Agent Memory core | `packages/core` | Private workspace shell named `@jurgen1c/agent-memory-core` until its API is intentionally published | Repository root detection, config loading, Markdown/YAML parsing, validation, compilation, retrieval, audit, coverage, recipes, plans, profiles, hooks, and UI server APIs. |
| Agent Memory schemas | `packages/schemas` | Private workspace shell named `@jurgen1c/agent-memory-schemas` until schemas are intentionally published | JSON schemas for committed memory files, config, plans, recipes, profiles, graphs, and generated metadata contracts. |
| Agent Memory web UI | `packages/web` | Private workspace shell named `@jurgen1c/agent-memory-web`; bundled inside `@jurgen1c/agent-memory-cli` by default | Local browser UI for inspecting committed memory and generated read models. It should consume core API shapes and static assets, not own repository memory semantics. |
| Agentflow CLI and runtime | `packages/agentflow` | `@jurgen1c/agentflow` | Workflow definition validation, run creation, resumable execution, step scheduling, event logs, artifact management, policies, approvals, retries, and cleanup. |
| Agentflow core | `packages/agentflow-core` | Private workspace shell named `@jurgen1c/agentflow-core` until the runtime API is intentionally published | Typed workflow/run primitives, planned command names, and runtime package boundary metadata. |
| Agentflow CLI | `packages/agentflow-cli` | Public package named `@jurgen1c/agentflow-cli` | `agentflow` executable entrypoint, help and version output, workflow validation and lint adapters, and command gating while runtime behavior is unavailable. |
| Agentflow schemas | `packages/agentflow-schemas` | Private workspace shell named `@jurgen1c/agentflow-schemas` until schemas are intentionally published | JSON schemas for Agentflow project config and workflow definitions. |
| Agentflow Agent Memory adapter | `packages/agentflow-agent-memory-adapter` | Private workspace shell named `@jurgen1c/agentflow-agent-memory-adapter` until adapter APIs are intentionally published | Typed adapter contract for Agentflow steps that need Agent Memory context. |
| Agentflow examples | `packages/agentflow-examples` or `examples/agentflow` | `@jurgen1c/agentflow-examples` if published; otherwise examples only | Reviewable workflow, prompt, and template examples for pipeline, recovery, and collaborative workflow styles. Examples must not be required at runtime. |
| Agent tools meta package | `packages/agent-tools` | `@jurgen1c/agent-tools` | Lightweight discovery package that documents the Agent Memory and Agentflow CLI packages and exports package metadata without replacing either CLI. |

The first public compatibility promise for repository memory stays on
`@jurgen1c/agent-memory-cli`. Agentflow has a separate public CLI package, while
the runtime, schema, adapter, web, and core package APIs remain private
workspace shells until their APIs are intentionally documented and versioned.

## Dependency Direction

Agent Memory owns durable knowledge APIs. Agentflow may call those APIs through
documented adapters:

```text
@jurgen1c/agentflow -> @jurgen1c/agentflow-core
@jurgen1c/agentflow-cli -> @jurgen1c/agentflow-core
@jurgen1c/agentflow-agent-memory-adapter -> @jurgen1c/agentflow-core
@jurgen1c/agentflow-agent-memory-adapter -> @jurgen1c/agent-memory-core
@jurgen1c/agentflow-core -> @jurgen1c/agentflow-schemas
@jurgen1c/agentflow-core -> @jurgen1c/agent-tools types only
@jurgen1c/agent-tools -> no Agent Memory or Agentflow runtime dependency
```

Agent Memory must not import Agentflow runtime code:

```text
@jurgen1c/agent-memory-cli
  -> @jurgen1c/agent-memory-core
  -> @jurgen1c/agent-memory-schemas
```

This keeps `agent-memory` installable in repositories that do not use
Agentflow. Agentflow integration should be additive: workflow steps can ask
Agent Memory for context, claims, recipes, or plan templates, but Agent Memory
commands must not require an Agentflow run database, scheduler, session store,
or workflow runtime.

## Source of Truth and Generated State

Agent Memory source of truth is committed repository data:

```text
docs/agent-memory/
  claims/**/*.md
  graph/**/*.yaml
  indexes/**/*.yaml
  recipes/**/*.yaml
  plans/**/*.yaml
  profiles/**/*.yaml
  waivers/**/*.yaml
```

Agent Memory generated cache is rebuildable local data:

```text
.agent-memory/
  memory.sqlite
  plans/**/*.yaml
  locks/
```

`.agent-memory/memory.sqlite` is a compiled read model. Local plan runs under
`.agent-memory/plans` are generated task state unless they are explicitly
promoted into committed plan templates under `docs/agent-memory/plans`.

Agentflow authored project files and mutable run state share the `.agentflow`
root. The SQLite database and run artifact directories are generated output:

```text
.agentflow/
  config.yml                 # committed project configuration
  workflows/**/*.yml         # committed workflow definitions
  prompts/**/*.md            # committed prompt templates
  templates/**/*.md          # committed notification/output templates
  agentflow.sqlite            # authoritative mutable run-state database
  runs/<run-id>/
    artifacts/
    failures/
    sessions/
```

`.agentflow/agentflow.sqlite` is the authoritative mutable state for resumable
workflow executions and remains separate from `.agent-memory/memory.sqlite`.
Its schema stores runs, step attempts, artifact metadata, ordered events,
sessions, failures, approvals, and budgets. Run records support `pending`,
`running`, `waiting`, `paused`, `completed`, `failed`, and `cancelled` statuses;
terminal runs are excluded from resume lookup. Parent and recovery links model
platform orchestration and recovery, while step, session, artifact, approval,
and budget records model pipeline and collaboration state.

Artifact content and other potentially large generated files live under
`.agentflow/runs/r-<sha256(run-id)>/`; the SQLite artifact rows record their repo-relative
paths and metadata. The artifact registry maps declared paths to fixed-length,
lowercase hexadecimal digests beneath each run's digested `artifacts/` subtree,
rejects traversal and symlink escapes, records a
SHA-256 checksum, producer step, kind, content type, size, status, and
timestamps, and requires explicit overwrite intent. Registry inspection
reconciles persisted metadata with the filesystem as `available`, `missing`,
`stale`, or `overwritten`. Ordered event records and artifact records remain
queryable after the process that created them exits. Neither the database nor
run directories should be committed. Run state can record which Agent Memory context was read, but it must
not become the canonical store for claims, recipes, profile traits, or memory
waivers. If a workflow discovers durable repository knowledge, the workflow
should create a reviewable change in `docs/agent-memory`, not mutate generated
cache files.

## Adapter Contracts

Agentflow should integrate with Agent Memory through explicit adapter surfaces:

- Context retrieval: call core APIs equivalent to `agent-memory context`.
- Memory validation: call validation and compile APIs before using generated
  read models.
- Recipe and plan lookup: read committed recipes and plan templates through
  Agent Memory APIs, then copy selected workflow state into Agentflow artifacts.
- Coverage and audit: run Agent Memory checks as workflow steps, with results
  stored as Agentflow artifacts.

Adapters should pass plain JSON-compatible data. They should not pass live
database handles, shell-specific CLI output, or process-global state between
packages.

The Agent Memory context adapter captures a versioned JSON snapshot at run start
or a named step boundary. Each snapshot is written through the Agentflow
artifact registry and includes the context request, memory database path,
compile metadata, selected claim/recipe/profile-trait IDs, warnings,
verification commands, memory-update prompts, and the complete context bundle.
Run-start snapshots use `memory/context/run-start.json`; step snapshots use a
readable step slug plus a digest so distinct step IDs cannot collide after
normalization. Replacing a published boundary snapshot requires explicit
overwrite intent.

Context capture opens `.agent-memory/memory.sqlite` read-only. It does not
compile memory, write canonical files, or copy Agentflow run state into Agent
Memory. Callers must compile or sync Agent Memory before capture, and durable
knowledge discovered during a run still becomes a reviewable canonical-file
change rather than an adapter-side database mutation.

## Agent Tools Meta Package

`@jurgen1c/agent-tools` is the discovery package for users who are looking for
the available Agent Memory and Agentflow command-line packages. It points users
to the concrete packages and command names:

| CLI package | Binary | Role |
| --- | --- | --- |
| `@jurgen1c/agent-memory-cli` | `agent-memory` | Repository-local memory CLI. |
| `@jurgen1c/agentflow-cli` | `agentflow` | Workflow runtime CLI. |

The package intentionally has no runtime dependency on either CLI package. It
does not provide replacement binaries, and installing it must not make
`agent-memory` require Agentflow or make `agentflow` require Agent Memory.
The root `@jurgen1c/agent-memory-cli` package still includes the compatibility
`agentflow` binary during the transition, but new install guidance points to
`@jurgen1c/agentflow-cli` as the package that owns the Agentflow command.

## Current Authoring Constraint

The `agentflow` built executable currently supports help, version, deterministic
workflow validation, read-only workflow linting, workflow explanation,
deterministic graph inspection, fixture-backed workflow simulation, and the
persistent run-lifecycle shell. `run <workflow> --id <run-id>` creates or
idempotently reopens a pending run, while `status`, `logs`, `artifacts`, `pause`,
`resume`, and `cancel` inspect or transition that run through the repo-local
SQLite store. Lifecycle changes append ordered events and survive process
restart. `run` and `resume` persist the requested lifecycle change, then fail
with exit code 7 and an actionable message because step runners and the
execution scheduler are not implemented and no workflow steps were executed.
Execution commands such as `cleanup` remain reserved placeholders until those
runtime adapters are implemented. The core persistence
surface now initializes repo-local `.agentflow/agentflow.sqlite` state and
provides typed create, update, resume-lookup, step, artifact, event, session,
failure, approval, and budget writes for later runtime phases. Its event log
provides deterministic sequence-ordered reads, and its artifact registry owns
safe run-scoped content writes plus restart-safe metadata/status reads. Schema
version 2 migrates version-1 artifact metadata in place.

The phase-1 authoring boundary exposes parsing, validation, and linting from
`@jurgen1c/agentflow-core`. Validation returns stable issue codes for structure,
references, safety, sessions, loops, artifacts, parallel writers, and approvals.
Linting reports complexity, budget, overwrite, secret-input, and risky-command
warnings without mutating workflow files. Explanation summarizes workflow
metadata, steps, sessions, artifacts, policies, collaboration, and lint warnings.
Graph inspection emits stable nodes and labeled edges for sequence, control-flow
targets, loop bodies, parallel branches, nested workflows, manual gates, and
collaboration steps. These inspection commands never execute workflow steps.
Simulation accepts a JSON fixture containing optional initial `inputs` and
`artifacts`, plus step records keyed by step ID. Step records can select
outcomes, declared outputs, condition targets, manual-gate choices, loop
iteration counts, and input-request values. The simulator traverses those
contracts in memory and reports visited steps, available and missing artifacts,
unresolved branches, and terminal states. It does not run commands, model
sessions, nested workflows, or MCP calls, and it never writes workflow, fixture,
artifact, or run-state files.
The CLI exposes these APIs through `agentflow validate <workflow>`,
`agentflow lint <workflow>`, `agentflow explain <workflow>`, and
`agentflow graph <workflow>`, plus
`agentflow simulate <workflow> --fixture <file>`. The lifecycle surface is:

```text
agentflow run <workflow> --id <run-id>
agentflow resume <run-id>
agentflow status <run-id>
agentflow logs <run-id>
agentflow artifacts <run-id>
agentflow pause <run-id>
agentflow cancel <run-id>
```

Example simulation fixture:

```json
{
  "artifacts": { "spec.md": "fixture content" },
  "steps": {
    "implement": { "outputs": ["implementation-summary.md"] },
    "route_review": { "condition": "record_approval" },
    "approval_gate": { "choice": "approve" },
    "review_loop": { "iterations": 1 }
  }
}
```

## Implementation Order

1. Document the target boundaries and compatibility promises.
2. Keep the current Agent Memory workspace layout green while adding any
   package metadata needed for future boundaries.
3. Extract shared schema and utility code only when at least two packages use
   it.
4. Add Agentflow schemas and validation before adding the runtime scheduler.
5. Add Agentflow run-state persistence and artifact storage.
6. Add adapters from Agentflow steps to Agent Memory context, validation,
   coverage, and audit APIs.
7. Add example workflows after the runtime can validate or simulate them.
8. Publish new packages only after their APIs have tests, docs, and release
   notes.

Do not move existing Agent Memory command behavior behind an Agentflow workflow
as part of the initial split. The existing CLI remains the compatibility anchor.

## Release Compatibility

The published `agent-memory` executable must keep working for existing users:

- `@jurgen1c/agent-memory-cli` remains the package users install.
- The `agent-memory` binary name remains stable.
- `bin/memory` wrappers generated by `agent-memory init` continue to resolve the
  installed CLI, global CLI, or package-manager fallback.
- Existing config defaults continue to point at `docs/agent-memory` for source
  data and `.agent-memory/memory.sqlite` for generated cache.
- New Agentflow packages must be optional dependencies or separate installs
  until Agentflow is part of the documented Agent Memory contract.
- Package version sync must keep workspace versions aligned when packages are
  released together.

Breaking changes to Agent Memory file formats, command output, or generated
wrapper behavior require a documented migration path before a release.
