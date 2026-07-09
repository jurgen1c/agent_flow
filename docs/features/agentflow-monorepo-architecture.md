# Agentflow Monorepo Architecture

Status: implemented workspace shell
Ticket: AM-6, AM-7, AM-9

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
| Agentflow CLI | `packages/agentflow-cli` | Public package named `@jurgen1c/agentflow-cli` | `agentflow` executable entrypoint, help text, version output, and command gating while runtime behavior is unavailable. |
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
root, but only `runs/` is generated output:

```text
.agentflow/
  config.yml                 # committed project configuration
  workflows/**/*.yml         # committed workflow definitions
  prompts/**/*.md            # committed prompt templates
  templates/**/*.md          # committed notification/output templates
  runs/<run-id>/
    state.json
    events.jsonl
    artifacts/
    failures/
    sessions/
```

Agentflow run state under `.agentflow/runs/` is authoritative for one workflow
execution and should not be committed. It can record which Agent Memory context
was read, but it must not become the canonical store for claims, recipes,
profile traits, or memory waivers. If a workflow discovers durable repository
knowledge, the workflow should create a reviewable change in
`docs/agent-memory`, not mutate generated cache files.

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

## Initial Skeleton Constraint

The first `agentflow` built executable is intentionally limited to help and
version output. Runtime command names such as `validate`, `run`, `resume`, and
`cleanup` are reserved placeholders until the platform behavior is implemented
behind schemas, validation, persistence, and tests.

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
