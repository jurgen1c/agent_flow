# Session Provider Boundaries

Agent Flow owns workflow state, policies, budgets, artifacts, approvals, and
terminal outcomes. A session provider owns only the external model interaction
needed to satisfy one bounded request. Provider-native transcripts can help a
provider resume, but they are never required to determine whether the workflow
is correct.

## Provider kinds

The public registry distinguishes five provider boundaries:

| Kind | Workflow provider | Intended use |
|---|---|---|
| `fixture` | `fixture` | Deterministic offline execution and tests. |
| `local` | `local` | An explicitly configured local model runtime. |
| `frontier` | `frontier` | An explicitly configured remote frontier model runtime. |
| `codex_profile` | `codex:<profile>` | An explicitly configured named Codex profile. |
| `custom` | Application-defined | A replaceable external adapter such as an experimental control plane. |

The registry starts empty. Fixture and custom adapters must be registered;
local, frontier, and Codex-profile adapters additionally require
`enabled: true`. Agent Flow never discovers credentials, starts a provider, or
selects a live model implicitly.

The compatibility `register(name, adapter, options)` API routes reserved live
names through the same boundary, so `local`, `frontier`, and `codex:<profile>`
also require `{ enabled: true }`. Custom registrations cannot claim those
reserved names.

```ts
import {
  createAgentFlowFixtureSessionProvider,
  createAgentFlowSessionProviderRegistry
} from "@jurgen1c/agent-flow";

if (process.env.ENABLE_CODEX_REVIEWER !== "true") {
  throw new Error("Codex reviewer provider is not explicitly enabled.");
}

const providers = createAgentFlowSessionProviderRegistry([
  {
    kind: "fixture",
    adapter: createAgentFlowFixtureSessionProvider({
      draft: { outputs: { "draft.md": "Deterministic output\n" } }
    })
  },
  {
    kind: "codex_profile",
    profile: "reviewer",
    enabled: true,
    adapter: codexReviewerAdapter
  }
]);
```

An application should construct live registrations only after validating its
own provider configuration. A registration does not bypass workflow policy:
`policies.model_usage.allowed_providers`, model budgets, frontier-call budgets,
artifact bounds, sensitive-input handling, lifecycle cancellation, and output
validation still run before or around every adapter invocation.

## Adapter contract

Every adapter receives the run, step, session, provider name and kind, a
bounded prompt, checksummed inputs, exact declared output paths, resume state,
an optional external session ID, and an abort signal. A Codex-profile request
also receives its configured profile name. The adapter must return exactly the
declared outputs. It may return a stable external session ID and bounded,
JSON-compatible, privacy-safe response metadata.

Agent Flow persists a checksummed request artifact containing provider
identity, prompt and input evidence, declared outputs, external session ID when
present, and validated response metadata. Output artifacts remain authoritative
even when the provider has no transcript or the session is non-resumable.
Secrets, full transcripts, and provider-native credentials do not belong in
metadata.

Unsupported or disabled providers fail before invocation with an actionable
registration message. Custom providers use `kind: "custom"` and a static name;
they receive the same policy, security, persistence, and output guarantees as
the built-in provider kinds.
