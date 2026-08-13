# Operations and Packaging

## Workflow readiness gate

Run these commands before executing a new or materially changed workflow:

```bash
agent-flow validate workflows/example.yml
agent-flow lint workflows/example.yml
agent-flow explain workflows/example.yml
agent-flow graph workflows/example.yml
agent-flow simulate workflows/example.yml --fixture fixtures/example.json
```

Simulation is not a substitute for validation, and a single happy-path fixture
is not enough for a branched workflow. Include representative success, failure,
pause, approval, exhaustion, and unresolved paths. Fixtures must be bounded,
deterministic, non-secret test inputs.

Before a live run, review:

- commands, prompts, transforms, MCP targets, and notification adapters;
- session roles, authority, and effective file scopes;
- model, budget, approval, cleanup, and unsafe-operation policies;
- retry, frontier-call, duration, recovery, and review-cycle limits;
- artifact declarations, overwrite behavior, and approval invalidation;
- retention rules for completed, failed, paused, and cancelled runs.

## Inspect and retain runs

Use `status`, `logs`, and `artifacts` for the live durable view. Do not infer a
run outcome from a command process alone.

Cleanup evaluates the workflow definition persisted with each run. Batch
cleanup processes matching runs independently and reports per-run errors:

```bash
agent-flow cleanup --older-than 30d --status completed
```

Use `--approve` only when the reviewed workflow policy requires explicit
cleanup approval. Cleanup preserves SQLite run and event history plus protected
evidence while marking unavailable artifact backing files accordingly.

Create a consistent, portable run record before moving or sharing evidence:

```bash
agent-flow archive run-123
agent-flow export run-123 --format zip --output exports/run-123.zip
unzip -t exports/run-123.zip
```

Portable ZIP creation is supported on Linux. Archive and export fail closed on
other platforms until they provide an equivalent descriptor-relative
publication primitive.

Portable ZIPs include a manifest, run state, ordered events, approvals,
failures, sessions, and available registered artifacts. They can contain
sensitive inputs or outputs even though local storage paths are excluded;
review access and destination controls before sharing them.

## Repository development gate

Agent Flow itself uses Bun for development and Node-targeted package output:

```bash
bun install --frozen-lockfile
bun run ci
bun run verify:package
```

`bun run ci` performs dependency audit, the full test suite, lint, type checking,
and the build. `bun run verify:package` then checks the npm tarball allowlist,
rejects source, tests, databases, credentials, logs, and other forbidden files,
installs the tarball into a clean temporary consumer, audits that install, and
smoke-tests public imports plus the `agent-flow` binary.

The package intentionally includes compiled runtime and declarations, versioned
documentation, schemas, examples, bundled skills, the root README, license, and
package metadata. Runtime dependencies use registry semver ranges and remain
external to the bundle.

## Release expectations

Agent Flow releases independently from Agent Memory, Agentic Development, and
Agent Core. A maintainer must:

1. start from clean `main` and choose an unused version;
2. run CI, package verification, CLI help, and version checks;
3. commit and push the version, then create the matching annotated tag;
4. publish a GitHub Release and wait for the publish workflow;
5. verify the npm registry version and provenance-backed public package.

The authoritative, current command sequence and first-publication credential
setup are in the [release checklist](releasing.md). GitHub Actions repeats the
CI and package gates before `npm publish --provenance --access public`.
