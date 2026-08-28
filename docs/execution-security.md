# Execution security

Agent Flow treats workflow YAML, prompt files, artifact content, MCP arguments,
and adapter implementations as untrusted input. A workflow is validated before
it is persisted and again when a persisted definition is resumed. Runtime
executors also verify that the supplied workflow and step exactly match the
persisted definition before invoking an adapter or shell.

## Execution boundaries

- `command` steps reject known destructive shell forms by default. Set
  `policies.unsafe_operations` to `require_approval` or `allow` only for a
  reviewed workflow. Arbitrary shell commands cannot be confined by
  `file_scope`, so command execution fails when a global file scope is present.
- `session_request` providers and `mcp_call` servers are capabilities. Providers
  must resolve through an explicit config alias or programmatic registration;
  Direct MCP adapters remain programmatic. Static provider, server, and tool names are
  required, model budgets are checked before calls, and output paths must be
  normalized repository-contained artifact paths.
- API provider credentials are read only from the named `api_key_env` variable.
  The three built-in HTTP drivers are artifact-only, cannot write or inspect the
  checkout, and receive only declared prompt and input artifact content. The
  Codex runs without a shell at the repository root and owns its normal config,
  login, permissions/sandbox, rules, plugins, and MCP servers. Agent Flow does
  not wrap Codex with `bubblewrap`/`flock` or filter its environment. Claude's
  existing host sandbox remains unchanged. Other coding CLIs require a custom
  programmatic adapter with a host-enforced filesystem and process boundary.
- Workflow and session file writes require explicit authority and effective
  `file_scope` includes. Artifact, prompt, cleanup, archive, and export paths
  reject absolute, escaping, non-canonical, and unsafe symlink paths.
  Custom and Claude adapters retain their existing scope enforcement. Codex
  filesystem authority is enforced by Codex itself.
- Cleanup is limited by the persisted workflow retention rule. Protected run
  state, summaries, failure evidence, decisions, and approved evidence are not
  removed by broad artifact rules.
- The `command` notification channel is denied unless
  `policies.unsafe_operations: allow` is present and application code explicitly
  registers the reviewed adapter. Built-in system notifications use argument
  arrays rather than a shell.

## Sensitive external inputs

Before an ordinary or recovery session provider, or an MCP adapter, receives
text, Agent Flow applies `policies.sensitive_inputs`:

```yaml
policies:
  sensitive_inputs: redact # default
```

- `redact` replaces recognized credentials, authorization values, secret
  assignments, and private-key material with `[REDACTED]`. Declared and
  JSON-shaped input is parsed and reserialized after structured redaction.
  Secret-like paths, including normalized `file:` URLs, host credential files
  such as `/etc/shadow`, Linux process environment, memory, descriptor, and
  command-line pseudo-files, and sensitive YAML
  are blocked because complete, structure-preserving redaction cannot be
  proven. Non-UTF-8 inputs are also blocked because they cannot be inspected
  safely.
- `deny` blocks any recognized secret-like content or path.
- `allow` sends the original content. This is an explicit escape hatch for a
  reviewed workflow and provider boundary; it should be narrow and temporary.

Session request metadata retains source artifact checksums for concurrency and
approval evidence. When provider bytes were redacted, it separately records the
provider checksum and `redacted: true`, without persisting the removed value.
MCP request audit artifacts record redacted arguments and the redaction fact.
Adapter error messages and failure payloads are redacted before durable event or
recovery evidence is written.

Safety checks return or persist a stable failure/pause reason before the unsafe
operation runs. Registering an adapter grants only access to that named runtime
boundary; it does not bypass workflow validation, budgets, path containment,
authority, retention, or sensitive-input policy.
