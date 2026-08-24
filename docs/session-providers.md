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

## Where model configuration lives

Workflow YAML selects a provider boundary through `sessions.<name>.provider`.
It does not select a vendor model or endpoint. The host application owns:

- the model name and endpoint;
- credentials and secret loading;
- vendor SDK calls, retries, and rate-limit behavior inside the adapter; and
- translation from the provider response into the exact output paths declared
  by the workflow step.

This separation lets the same workflow use a different local or cloud model
without embedding credentials or vendor-specific configuration in durable
workflow state. It also means setting a model-related environment variable does
nothing by itself: the host application must read it and use it while building
the adapter.

For example, a local and a cloud registration can close over different clients
and model settings:

```ts
import {
  createAgentFlowSessionProviderRegistry,
  type AgentFlowSessionProviderAdapter
} from "@jurgen1c/agent-flow";

const adapterFor = (
  model: string,
  generateOutputs: (request: {
    model: string;
    prompt: string;
    inputs: readonly unknown[];
    outputPaths: readonly string[];
    signal: AbortSignal;
  }) => Promise<Record<string, string>>
): AgentFlowSessionProviderAdapter => async (request) => ({
  outputs: await generateOutputs({
    model,
    prompt: request.prompt.content,
    inputs: request.inputs,
    outputPaths: request.outputs,
    signal: request.signal
  }),
  metadata: { model }
});

const providers = createAgentFlowSessionProviderRegistry([
  {
    kind: "local",
    enabled: true,
    adapter: adapterFor(process.env.MY_APP_LOCAL_MODEL!, localClient.generateOutputs)
  },
  {
    kind: "frontier",
    enabled: true,
    adapter: adapterFor(process.env.MY_APP_CLOUD_MODEL!, cloudClient.generateOutputs)
  }
]);
```

The client functions above are application-defined. Validate configuration
before constructing the registry, bind methods when required by the client
library, and keep credentials out of prompts, inputs, metadata, and output
artifacts. Pass `providers` to both `executeAgentFlowCommandPipeline` and
`resumeAgentFlowCommandPipeline`.

The packaged CLI only constructs a fixture adapter for `run` and `resume`.
Consequently, it executes session steps only when their workflow sessions
declare `provider: fixture` and the command supplies `--fixture`. Local,
frontier, Codex-profile, and custom live adapters require a programmatic host.

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
