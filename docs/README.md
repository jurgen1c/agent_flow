# Agent Flow Documentation

Agent Flow is the independently installable workflow runtime and CLI published
as `@jurgen1c/agent-flow`. It runs persistent, policy-aware pipelines inside a
repository. It can use Agent Memory context through an explicit adapter, but it
does not replace Agent Memory or depend on it.

> Earlier planning called the package `@jurgen1c/agentflow-cli`, the executable
> `agentflow`, and the integration package `@jurgen1c/agent-tools`. The current
> names are `@jurgen1c/agent-flow`, `agent-flow`, and
> `@jurgen1c/agentic-development`. These docs use the current names.

## Start here

1. Follow the [quickstart](quickstart.md) to install `agent-flow`, validate a
   workflow, run it, and inspect its persisted state.
2. Use the [command reference](command-reference.md) for exact command syntax.
3. Read [concepts and safety](concepts-and-safety.md) before enabling file
   writes, model sessions, approvals, recovery loops, or cleanup.
4. Read [execution security](execution-security.md) for command policy,
   adapter capabilities, sensitive-input redaction, and unsafe-channel defaults.
5. Read [session provider boundaries](session-providers.md) before configuring
   local, frontier, named Codex-profile, or custom adapters.
6. Read [packages and Memory integration](packages-and-memory.md) when a
   repository also uses Agent Memory.
7. Use [operations and packaging](operations-and-packaging.md) to promote a
   workflow from draft to execution and to validate a package or release.

## Deeper references

- [Architecture](architecture.md) defines source, dependency, persistence, and
  publication boundaries.
- [Execution security](execution-security.md) defines command, adapter,
  sensitive-input, and unsafe-channel enforcement.
- [Session provider boundaries](session-providers.md) defines typed provider
  registration, live opt-in, request/response evidence, and failure behavior.
- [Example workflows](../examples/README.md) cover pipeline, recovery, and
  collaborative flows with offline fixtures.
- [Workflow specifications](specifications/) define the supported workflow
  styles and runtime behavior in detail.
- [Release checklist](releasing.md) documents the maintainer release process.

Documentation in this directory is versioned with the runtime. That keeps
commands, schemas, examples, and operational guidance reviewable at the same
commit instead of maintaining a separate, drifting wiki copy.
