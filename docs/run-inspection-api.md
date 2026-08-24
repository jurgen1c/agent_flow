# Run inspection API

Agent Flow exposes a local HTTP API for run inspection and a narrow set of
guarded run actions. It binds to
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
warnings, raw run state, and the actions currently allowed for the selected
run. Structured evidence has copy controls. Evidence
tabs load only when opened and request bounded 100-record pages, so large runs
do not download or construct every hidden section up front. Empty, loading,
failed-load, narrow-screen, and large-run states are handled in the bundled
dependency-free interface.

The UI URL carries the token in its fragment. URL fragments are not sent to the
HTTP server, and the UI sends the token only through the same
`x-agent-flow-token` request header used by API clients. The browser assets do
not contain run data or the token and can be loaded without authentication;
all inspection data and action requests remain token-protected.

Pass the same `databasePath` used by `openAgentFlowRunState` when run state is
stored outside the default `.agent-flow/agent-flow.sqlite` path.
Starting the server opens that database once through the normal run-state path
so any required schema migration finishes before requests are accepted.
Inspection and action-preflight requests use separate read-only connections;
accepted action requests open the normal writable run-state connection.

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
| `GET` | `/api/run/actions` | Available actions, policy and stale-approval warnings, and the freshness guard for the run selected by `x-agent-flow-run-id`. |
| `POST` | `/api/run/actions` | Apply one guarded `approve`, `reject`, `provide_input`, `resume`, `pause`, or `cancel` action. |

The browser requests `/api/run?section=overview` first and loads `state` only
when its tab opens. It lazily requests `events`, `steps`, `artifacts`,
`failures`, `approvals`, `decisions`, and `warnings` with a non-negative
`offset` and a `limit` from 1 to 200. Paged section responses include `items`
and a nullable `nextOffset`. Omitting `section` preserves the complete detail
response for API clients.

For warning pages, `offset` and `nextOffset` track inspected warning-source
records rather than the number of emitted warnings. This bounds each request
even when a large run has few or no warnings.

Every action body is JSON and includes the `guard` returned by the latest
action snapshot. The server rechecks that guard after acquiring any required
run lock and returns `409 AGENT_FLOW_ACTION_STALE` without applying the action
when run state, the persisted workflow, a waiting interaction, approval state,
or guarded evidence has changed. Approval and input responses use the existing
pipeline response path; plain resume re-enters workflow execution, while pause
and cancel use the lifecycle transition path. Those paths preserve their
transactional state, event, approval, artifact, notification, and run-lock
boundaries.

```json
{ "action": "approve", "guard": "guard-from-the-latest-action-snapshot" }
```

`provide_input` additionally requires an `answer`, which may be any finite JSON
value nested no more than 50 levels. The body is capped at 64 KiB. Applications whose resumed workflows need custom
session, MCP, transform, notification, or workflow registries can pass them in
`actionRuntime` when starting the server. Destructive cleanup and archive or
export actions are intentionally not exposed.

All other methods return `405 Method Not Allowed`. Responses use JSON, disable
caching, and return `403 Forbidden` when the token is missing or invalid.

Failure payloads and decision records are read only from registered artifacts.
Each inspected JSON document is capped at 1 MiB before its content is read or
hashed. Other artifacts receive bounded metadata checks only. Unavailable,
stale, oversized, or malformed documents remain represented by their artifact
metadata and add a warning to the inspection model.
