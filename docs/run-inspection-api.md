# Run inspection API

Agent Flow exposes a read-only HTTP API for local run inspection. It binds to
`127.0.0.1:4318` by default, generates a session token when one is not supplied,
and accepts only numeric loopback bind addresses (`127.0.0.1` or `::1`).

```ts
import { startAgentFlowRunInspectionApi } from "@jurgen1c/agent-flow";

const server = await startAgentFlowRunInspectionApi({ cwd: process.cwd() });

console.log(server.url);
console.log(server.token);

// Keep the process alive while the API is in use, then close it explicitly.
await server.close();
```

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

The API provides two endpoints:

| Method | Path | Response |
| --- | --- | --- |
| `GET` | `/api/runs` | Run summaries without inputs or internal context. |
| `GET` | `/api/runs/:run-id` | Run state, step attempts, event timeline, artifact metadata, failures and their redacted payloads, approvals, decision records, and warnings. |

All other methods return `405 Method Not Allowed`; the API does not expose
resume, pause, cancel, approval, cleanup, or other state-changing operations.
Responses use JSON, disable caching, and return `403 Forbidden` when the token
is missing or invalid.

Failure payloads and decision records are read only from registered artifacts.
Each inspected JSON document is capped at 1 MiB before its content is read or
hashed. Other artifacts receive bounded metadata checks only. Unavailable,
stale, oversized, or malformed documents remain represented by their artifact
metadata and add a warning to the inspection model.
