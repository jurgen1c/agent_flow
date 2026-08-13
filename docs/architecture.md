# Agent Flow Architecture

Agent Flow is a standalone workflow product. It is independently installable,
versioned, tested, and released as `@jurgen1c/agent-flow`.

## Dependency boundary

```text
@jurgen1c/agent-core
          ^
          |
@jurgen1c/agent-flow
```

Agent Flow consumes shared YAML, repository/path-containment, and SQLite APIs
from Agent Core. Agent Flow owns workflow parsing, schemas, validation,
simulation, persistent run state, policies, lifecycle transitions, command and
artifact execution, notifications, retention, failure payloads, and its CLI.

Agent Flow has no Agent Memory dependency and contains no Agent Memory adapter.
The optional Memory-to-Flow adapter belongs to
`@jurgen1c/agentic-development`.

## Source layout

- `src/runtime/` owns workflow and run behavior.
- `src/cli/` is a thin command parser and process entrypoint.
- `schemas/` contains the public config, workflow, and failure-classification
  JSON schemas.
- `examples/` contains non-runtime workflow, prompt, and template examples.
- `docs/specifications/` preserves the detailed workflow specifications and
  their filtered repository history.
- `tests/` covers runtime, persistence, CLI, safety, and package boundaries.

## Public package

The root export exposes the runtime API and generated declarations. The
`./cli` subpath exposes programmatic CLI dispatch. JSON schemas are available
from `./schemas/config`, `./schemas/failure-classification`, and
`./schemas/workflow`.

The package installs one executable:

```text
agent-flow
```

Local persistent state stays under:

```text
.agent-flow/
├── agent-flow.sqlite
├── archives/
└── runs/
```

Generated state must remain uncommitted. Repository and artifact paths are
validated by Agent Core before filesystem access.
Retention cleanup deletes only policy-matched artifact backings and leaves
their SQLite metadata inspectable. Portable archives are ZIP32 containers with
run state, JSONL events, metadata, and available registered artifact content;
they never expose local artifact storage paths. Archive collection holds the
run-state write lock for one consistent snapshot. On Linux, publication opens
each path component relative to a verified repository-root descriptor and
fails if the directory chain changes; unsupported platforms fail closed.
Randomized hidden staging links are conservatively retained after publication
because a separate pathname identity check followed by unlink would permit a
replacement race.
A 64 MiB content limit is enforced during incremental JSON encoding before
buffers are materialized and bounds in-memory ZIP construction.

## Build and release

Bun runs tests and produces Node-targeted JavaScript. TypeScript produces the
published declaration files. Runtime dependencies remain external to the
bundle so the tarball does not contain copied Agent Core implementation.

CI verifies frozen installation, audit, tests, lint, type checking, build,
tarball contents, clean consumer installation, exported APIs, and the
`agent-flow` binary. A published `vX.Y.Z` GitHub Release triggers npm
publication with public access and provenance.
