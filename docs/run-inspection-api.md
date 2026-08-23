# Run inspection API

Agent Flow exposes a read-only HTTP API for local run inspection. It binds to
`127.0.0.1:4318` by default, generates a session token when one is not supplied,
and accepts only numeric loopback bind addresses (`127.0.0.1` or `::1`).

```ts
import { startAgentFlowRunInspectionApi } from "@jurgen1c/agent-flow";

const server = await startAgentFlowRunInspectionApi({ cwd: process.cwd() });

console.log(server.url);
console.log(server.token);
console.log(server.uiUrl);

// Keep the process alive while the API is in use, then close it explicitly.
await server.close();
```

Open `server.uiUrl` in a browser to use the run inspector. The UI shows the run
list, status and current step, an incrementally rendered event timeline, step
attempts, artifact metadata, failure payloads, approvals, decision records,
warnings, and raw run state. Structured evidence has copy controls. Evidence
tabs load only when opened and request bounded 100-record pages, so large runs
do not download or construct every hidden section up front. Empty, loading,
failed-load, narrow-screen, and large-run states are handled in the bundled
dependency-free interface.

The UI URL carries the token in its fragment. URL fragments are not sent to the
HTTP server, and the UI sends the token only through the same
`x-agent-flow-token` request header used by API clients. The browser assets do
not contain run data or the token and can be loaded without authentication;
all inspection data remains token-protected.

Pass the same `databasePath` used by `openAgentFlowRunState` when run state is
stored outside the default `.agent-flow/agent-flow.sqlite` path.
Starting the server opens that database once through the normal run-state path
so any required schema migration finishes before requests are accepted. Each
request then uses a separate read-only connection.

Callers must send the token in the `x-agent-flow-token` header on every
request. Tokens are not accepted in query strings.

```bash
curl \
  -H "x-agent-flow-token: $AGENT_FLOW_INSPECTION_TOKEN" \
  http://127.0.0.1:4318/api/runs
```

The server provides the browser UI plus these data routes:

| Method | Path | Response |
| --- | --- | --- |
| `GET` | `/` | Run inspection UI. |
| `GET` | `/api/runs` | Run summaries without inputs or internal context. |
| `GET` | `/api/runs/:run-id` | Run state, step attempts, event timeline, artifact metadata, failures and their redacted payloads, approvals, decision records, and warnings. |
| `GET` | `/api/run` | The same run detail selected by a percent-encoded `x-agent-flow-run-id` header, used by the browser UI so dot-segment IDs remain opaque. |

The browser requests `/api/run?section=overview` first and loads `state` only
when its tab opens. It lazily requests `events`, `steps`, `artifacts`,
`failures`, `approvals`, `decisions`, and `warnings` with a non-negative
`offset` and a `limit` from 1 to 200. Paged section responses include `items`
and a nullable `nextOffset`. Omitting `section` preserves the complete detail
response for API clients.

For warning pages, `offset` and `nextOffset` track inspected warning-source
records rather than the number of emitted warnings. This bounds each request
even when a large run has few or no warnings.

All other methods return `405 Method Not Allowed`; the API does not expose
resume, pause, cancel, approval, cleanup, or other state-changing operations.
Responses use JSON, disable caching, and return `403 Forbidden` when the token
is missing or invalid.

Failure payloads and decision records are read only from registered artifacts.
Each inspected JSON document is capped at 1 MiB before its content is read or
hashed. Other artifacts receive bounded metadata checks only. Unavailable,
stale, oversized, or malformed documents remain represented by their artifact
metadata and add a warning to the inspection model.
