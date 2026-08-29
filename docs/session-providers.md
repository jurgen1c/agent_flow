# Session Provider Configuration and Boundaries

Agent Flow owns workflow state, policy, budgets, artifacts, approvals, and
terminal outcomes. A provider owns only the bounded model interaction needed
for one session request.

## Configuration layers

Concrete targets are user- or machine-specific. Put them in
`${XDG_CONFIG_HOME:-~/.config}/agent-flow/config.yml`, or select another global
file with `AGENT_FLOW_CONFIG` or `--config`:

```yaml
version: 1
targets:
  codex-main:
    kind: frontier
    driver: codex-cli
    model: YOUR_CODEX_MODEL
    profile: deep-review
    reasoning_effort: high
    enabled: true

  claude-main:
    kind: frontier
    driver: claude-code
    model: YOUR_CLAUDE_MODEL
    enabled: true

  openai-api:
    kind: frontier
    driver: openai-responses
    model: YOUR_OPENAI_MODEL
    api_key_env: OPENAI_API_KEY
    enabled: true

  anthropic-api:
    kind: frontier
    driver: anthropic-messages
    model: YOUR_ANTHROPIC_MODEL
    api_key_env: ANTHROPIC_API_KEY
    max_output_tokens: 4096
    enabled: true

  qwen-local:
    kind: local
    driver: openai-compatible
    base_url: http://127.0.0.1:11434/v1
    model: qwen3
    enabled: true
```

Stable aliases belong in the committed repository-root `.agent-flow.yml`:

```yaml
version: 1
providers:
  planner: { kind: frontier, target: claude-main }
  implementer: { kind: frontier, target: codex-main }
  local-drafter: { kind: local, target: qwen-local }
```

Global config contains targets only; repository config contains aliases only.
Agent Flow does not load `.env` files or interpolate arbitrary environment
variables. API targets use `api_key_env`; literal secret fields are rejected as
unknown configuration.

Use these commands before a run:

```bash
agent-flow config validate [--config path/to/config.yml]
agent-flow providers list [--config path/to/config.yml]
agent-flow providers doctor [--config path/to/config.yml]
```

`list` is redacted. `doctor` checks environment-variable presence and target
readiness without calling a model. For Codex, readiness verifies the executable
but does not assume `codex login` is authoritative because ambient custom model
providers may use their own authentication.

## Per-step selection and swapping

Workflow sessions name aliases, and each model-backed step names its session:

```yaml
sessions:
  planner: { provider: planner }
  implementer:
    provider: implementer

steps:
  - id: plan
    type: session_request
    session: planner
    prompt: prompts/create-plan.md
    inputs: [request.md]
    outputs: [plan.md]

  - id: implement
    type: session_request
    session: implementer
    prompt: prompts/implement-plan.md
    inputs: [plan.md]
    outputs: [implementation-summary.md]
```

A new run may replace one or more alias defaults:

```bash
agent-flow run workflow.yml --id alternate \
  --provider planner=codex-main \
  --provider local-drafter=gemma-local
```

An override must preserve the alias kind. Resumes do not accept overrides.
Each configured alias persists a fingerprint of its target, driver, model,
endpoint identity, profile name, and reasoning effort. Resume fails closed if
those configured fields drift; credential rotation is intentionally excluded.
The reserved `codex` provider persists explicit run overrides but intentionally
does not fingerprint ambient Codex config, authentication, permissions, rules,
skills, plugins, or MCP servers. Codex owns drift and trust decisions for those
pass-through inputs.

## Built-in drivers

| Driver | Target kind | Authentication | Resume |
|---|---|---|---|
| `openai-responses` | `frontier` | `api_key_env` | Non-resumable |
| `anthropic-messages` | `frontier` | `api_key_env` | Non-resumable |
| `openai-compatible` | `local` or `frontier` | Optional `api_key_env` | Non-resumable |
| `codex-cli` | `frontier` | Existing Codex CLI configuration | Native thread ID |
| `claude-code` | `frontier` | Existing Claude Code login | Native session ID |

Driver request shapes follow the official
[OpenAI Responses API](https://developers.openai.com/api/reference/cli/resources/responses/methods/create),
[Anthropic Messages API](https://platform.claude.com/docs/en/api/messages/create),
and [Ollama structured output](https://docs.ollama.com/capabilities/structured-outputs)
references. Codex execution follows the official
[non-interactive `codex exec` contract](https://learn.chatgpt.com/docs/non-interactive-mode),
including JSONL thread IDs and explicit resume.

OpenAI-compatible local targets must use an HTTP(S) loopback URL.
Frontier-compatible targets require HTTPS. URLs containing
credentials, queries, or fragments are rejected, and redirects are not
followed.

Built-in drivers accept UTF-8 text only and request structured JSON containing
exactly the declared output paths. Missing, extra, malformed, or oversized
outputs fail before artifact publication. Drivers add no hidden retries.

The HTTP drivers generate artifacts only, reject file-modification authority,
and send only declared prompt and artifact content. Claude keeps its existing
host isolation. Codex is intentionally pass-through: Agent Flow starts the
installed `codex exec` at the repository root with the normal environment and
does not suppress user/repository configuration, rules, skills, plugins, MCP,
permissions, or sandbox settings. Codex therefore owns its authentication and
execution boundary; `bubblewrap` and `flock` are not Agent Flow prerequisites
for Codex.

A workflow may use the reserved provider directly:

```yaml
sessions:
  implementer:
    provider: codex
    resume: true
    codex:
      profile: implementation
      reasoning_effort: high
```

`model`, `profile`, and `reasoning_effort` are optional Codex overrides. Their
precedence is step, `agent-flow run` flags, session, configured target, then
Codex's normal config. Run flags are `--model`, `--profile`, and
`--reasoning-effort`. Allowed reasoning values are `minimal`, `low`, `medium`,
`high`, and `xhigh`. Agent Flow passes overrides through using Codex's supported
CLI/config flags and persists the selected run overrides for resume.

For MCP, ordinary Codex session requests may use any ambient server Codex has
configured. An `mcp_call` uses `via: direct` by default and requires a
programmatically registered host adapter. `via: codex` requires `session:` to
name a resumable Codex session. Agent Flow asks Codex to call the exact static
server/tool with the resolved arguments and publishes outputs only after the
JSONL stream reports a matching completed MCP call.

See the official Codex [profile documentation](https://learn.chatgpt.com/docs/config-file/config-advanced#profiles),
[`model_reasoning_effort` reference](https://learn.chatgpt.com/docs/config-file/config-reference),
and [MCP documentation](https://learn.chatgpt.com/docs/extend/mcp?surface=cli).
The CLI receives prompts on standard input, never through a shell, and output
is bounded and schema-validated before Agent Flow publishes artifacts.

When a workflow session declares `resume: true`, Agent Flow persists the Codex
thread ID or Claude session ID as soon as the native process reports it. Later
steps using that same workflow session resume the native conversation. If the
external conversation is missing, the run pauses; it never silently starts over.
This applies to ordinary session steps, disagreement resolvers, and recovery
sessions. After reset, Agent Flow retries the containing workflow step without
the missing provider context.
After inspecting the run, explicitly reset it with:

```bash
agent-flow resume <run-id> --reset-session <session-name> [--fixture <file>] [--config <file>]
```

Configured frontier aliases count against `max_frontier_calls`, and policy
allowlists continue matching the logical alias rather than the machine-specific
target name.

## Programmatic extension API

The original typed registry remains available for fixtures, reserved local or
frontier boundaries, named Codex profiles, and application-defined adapters:

```ts
import {
  createAgentFlowSessionProviderRegistry,
  type AgentFlowSessionProviderAdapter
} from "@jurgen1c/agent-flow";

const customAdapter: AgentFlowSessionProviderAdapter = async (request) => ({
  outputs: await customClient.generate({
    prompt: request.prompt.content,
    inputs: request.inputs,
    outputPaths: request.outputs,
    fileScopeLayers: request.fileScope?.layers ?? [],
    signal: request.signal
  })
});

const providers = createAgentFlowSessionProviderRegistry([
  { kind: "custom", name: "private-control-plane", adapter: customAdapter }
]);
```

Reserved live programmatic registrations still require explicit
`enabled: true`. Applications can also call `loadAgentFlowProviderCatalog` and
`createAgentFlowConfiguredProviderRegistry` directly.

### Compatibility with the original provider system

The configured-provider layer is additive. It does not remove or reinterpret
existing registrations:

| Provider path | Registration | Config files required | Drift identity |
|---|---|---|---|
| Fixture | `{ kind: "fixture", adapter }` | No | Logical `fixture` name |
| Reserved local/frontier | `{ kind: "local" | "frontier", enabled: true, adapter }` | No | Logical reserved name |
| Named Codex profile | `{ kind: "codex_profile", profile, enabled: true, adapter }` | No | Logical profile name |
| Custom | `{ kind: "custom", name, adapter }` or `.register(name, adapter)` | No | Logical custom name |
| Configured built-in | Global target plus repository alias | Yes | Target, kind, driver, model hash, endpoint settings, and fingerprint |
| Programmatic configured | `.registerConfigured(descriptor, adapter)` | No | Complete descriptor and fingerprint |

These are registration paths, not automatically configured defaults. See the
[original programmatic registration example](quickstart.md#use-the-original-programmatic-registrations)
for a complete registry, session, and step definition.

Workflows using a custom registration name it directly:

```yaml
sessions:
  implementer:
    provider: private-control-plane
```

No `.agent-flow.yml` entry is required. The application must pass that registry
to `executeAgentFlowCommandPipeline` and `resumeAgentFlowCommandPipeline`; the
installed CLI cannot discover or execute arbitrary application adapter code.

An ordinary custom provider intentionally keeps the previous reproducibility
boundary. Agent Flow persists the logical provider name and request evidence,
but it cannot tell whether application code changed the adapter implementation
behind that name. Use `registerConfigured` when an application-owned adapter
should also receive:

- local or frontier budget classification;
- complete target/model identity evidence; and
- resume rejection when its privacy-safe fingerprint changes.

`registerConfigured` requires `name`, `kind`, `target`, `driver`, `model`, and
`fingerprint`. Its adapter remains application-defined; registration neither loads
executable code from YAML nor grants filesystem or process authority.

`request.fileScope.layers` preserves the global, session, and operation scopes
separately. A file-writing adapter must require every layer to allow a path;
combining include globs into one union would broaden authority incorrectly.

## Request evidence

Adapters receive bounded prompt and input content, checksums, exact outputs,
provider identity, repository and authority context, resume state, an optional
external session ID, an abort signal, and a `reportExternalSessionId` callback.
A resumable adapter should call that callback as soon as a new external session
exists so Agent Flow can preserve it even if the provider later fails. Responses
may contain only declared outputs, the same external session ID, and bounded
privacy-safe metadata.

Agent Flow persists the logical alias, resolved target, kind, driver, a model
identity hash, safe target fingerprint, input evidence, declared outputs, and
validated response metadata. It never persists model identifiers, credential
values, or full provider transcripts.
Provider-native transcripts can support resume, but durable Agent Flow state
remains authoritative.

Built-in HTTP drivers never launch a shell or give the provider ambient
filesystem access. Claude uses Agent Flow's authority, subprocess sandbox,
workspace audit, and write-lock boundary. Codex uses the durable session and
bounded-output contract but delegates configuration, filesystem authority,
sandboxing, and MCP access to the installed Codex. Other native CLI integrations
remain a programmatic extension boundary so the host application can supply
isolation appropriate to that CLI and platform.
