import crypto from "node:crypto";
import http from "node:http";
import {
  AgentFlowRunStateError,
  openAgentFlowRunState,
  openAgentFlowRunStateForInspection
} from "./run_state";
import {
  buildAgentFlowRunInspectionModel,
  buildAgentFlowRunInspectionOverview,
  buildAgentFlowRunInspectionPage,
  buildAgentFlowRunInspectionState,
  listAgentFlowRunInspectionSummaries
} from "./run_inspection";
import type { AgentFlowRunInspectionSection } from "./run_inspection";
import {
  AGENT_FLOW_RUN_INSPECTION_UI_CSS,
  AGENT_FLOW_RUN_INSPECTION_UI_HTML,
  AGENT_FLOW_RUN_INSPECTION_UI_JAVASCRIPT
} from "./run_inspection_ui";

export const DEFAULT_AGENT_FLOW_RUN_INSPECTION_API_HOST = "127.0.0.1";
export const DEFAULT_AGENT_FLOW_RUN_INSPECTION_API_PORT = 4318;
export const AGENT_FLOW_RUN_INSPECTION_TOKEN_HEADER = "x-agent-flow-token";
export const AGENT_FLOW_RUN_INSPECTION_RUN_ID_HEADER = "x-agent-flow-run-id";

export interface AgentFlowRunInspectionApiOptions {
  cwd?: string;
  databasePath?: string;
  host?: string;
  port?: number;
  token?: string;
}

export interface AgentFlowRunInspectionApiHandle {
  host: string;
  port: number;
  url: string;
  uiUrl: string;
  token: string;
  close(): Promise<void>;
}

class AgentFlowRunInspectionApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
    readonly code: string
  ) {
    super(message);
    this.name = "AgentFlowRunInspectionApiError";
  }
}

export async function startAgentFlowRunInspectionApi(
  options: AgentFlowRunInspectionApiOptions = {}
): Promise<AgentFlowRunInspectionApiHandle> {
  const host = normalizeLocalHost(options.host ?? DEFAULT_AGENT_FLOW_RUN_INSPECTION_API_HOST);
  const port = normalizePort(options.port ?? DEFAULT_AGENT_FLOW_RUN_INSPECTION_API_PORT);
  const token = options.token === undefined
    ? crypto.randomBytes(18).toString("base64url")
    : normalizeToken(options.token);
  const inspectionTarget = await prepareInspectionDatabase(options.cwd, options.databasePath);
  const requestContext = {
    cwd: inspectionTarget.repoRoot,
    databasePath: inspectionTarget.databasePath,
    token
  };
  const server = http.createServer((request, response) => {
    void handleInspectionRequest(request, response, requestContext);
  });
  const listeningPort = await listenOnAvailablePort(server, host, port);

  const url = `http://${formatUrlHost(host)}:${listeningPort}`;
  return {
    host,
    port: listeningPort,
    url,
    uiUrl: `${url}/#token=${encodeURIComponent(token)}`,
    token,
    close: () => new Promise((resolve, reject) => {
      server.close((error) => error === undefined ? resolve() : reject(error));
    })
  };
}

async function prepareInspectionDatabase(
  cwd?: string,
  databasePath?: string
): Promise<{ repoRoot: string; databasePath: string }> {
  const store = await openAgentFlowRunState({ cwd, databasePath });
  const target = { repoRoot: store.repoRoot, databasePath: store.databasePath };
  store.close();
  return target;
}

async function handleInspectionRequest(
  request: http.IncomingMessage,
  response: http.ServerResponse,
  context: { cwd?: string; databasePath?: string; token: string }
): Promise<void> {
  try {
    const requestPath = rawRequestPath(request.url ?? "/");
    if (request.method === "GET" && sendInspectionUiAsset(response, requestPath)) return;
    requireToken(request.headers[AGENT_FLOW_RUN_INSPECTION_TOKEN_HEADER], context.token);
    if (request.method !== "GET") {
      response.setHeader("allow", "GET");
      throw new AgentFlowRunInspectionApiError(
        "Method not allowed.",
        405,
        "AGENT_FLOW_INSPECTION_METHOD_NOT_ALLOWED"
      );
    }

    const route = inspectionRoute(
      requestPath,
      request.headers[AGENT_FLOW_RUN_INSPECTION_RUN_ID_HEADER]
    );
    const store = await openAgentFlowRunStateForInspection({
      cwd: context.cwd,
      databasePath: context.databasePath
    });
    try {
      if (route.kind === "list") {
        sendJson(response, 200, { runs: listAgentFlowRunInspectionSummaries(store) });
        return;
      }
      const query = inspectionQuery(request.url ?? "/");
      if (query.section === "overview") {
        sendJson(response, 200, buildAgentFlowRunInspectionOverview(store, route.runId));
        return;
      }
      if (query.section === "state") {
        sendJson(response, 200, { state: buildAgentFlowRunInspectionState(store, route.runId) });
        return;
      }
      if (query.section !== null) {
        sendJson(response, 200, buildAgentFlowRunInspectionPage(
          store,
          route.runId,
          query.section,
          query.offset,
          query.limit
        ));
        return;
      }
      const model = buildAgentFlowRunInspectionModel(store, route.runId);
      sendJson(response, 200, model);
      return;
    } finally {
      store.close();
    }
  } catch (error) {
    if (response.headersSent) {
      response.end();
      return;
    }
    if (error instanceof AgentFlowRunInspectionApiError) {
      sendJson(response, error.status, { error: error.message, code: error.code });
      return;
    }
    if (error instanceof AgentFlowRunStateError && error.code === "AGENT_FLOW_RUN_NOT_FOUND") {
      sendJson(response, 404, { error: error.message, code: error.code });
      return;
    }
    sendJson(response, 500, {
      error: "Could not inspect Agent Flow run state.",
      code: "AGENT_FLOW_INSPECTION_ERROR"
    });
  }
}

function sendInspectionUiAsset(response: http.ServerResponse, requestPath: string): boolean {
  if (requestPath === "/" || requestPath === "/index.html") {
    sendAsset(response, "text/html; charset=utf-8", AGENT_FLOW_RUN_INSPECTION_UI_HTML);
    return true;
  }
  if (requestPath === "/inspection.css") {
    sendAsset(response, "text/css; charset=utf-8", AGENT_FLOW_RUN_INSPECTION_UI_CSS);
    return true;
  }
  if (requestPath === "/inspection.js") {
    sendAsset(response, "text/javascript; charset=utf-8", AGENT_FLOW_RUN_INSPECTION_UI_JAVASCRIPT);
    return true;
  }
  return false;
}

function requireToken(value: string | string[] | undefined, expected: string): void {
  const values = Array.isArray(value) ? value : value === undefined ? [] : [value];
  if (!values.some((candidate) => tokensEqual(candidate, expected))) {
    throw new AgentFlowRunInspectionApiError(
      "Missing or invalid Agent Flow inspection token.",
      403,
      "AGENT_FLOW_INSPECTION_FORBIDDEN"
    );
  }
}

function rawRequestPath(requestTarget: string): string {
  const queryIndex = requestTarget.indexOf("?");
  return queryIndex === -1 ? requestTarget : requestTarget.slice(0, queryIndex);
}

function inspectionQuery(requestTarget: string): {
  section: "overview" | "state" | AgentFlowRunInspectionSection | null;
  offset: number;
  limit: number;
} {
  const queryIndex = requestTarget.indexOf("?");
  if (queryIndex === -1) return { section: null, offset: 0, limit: 100 };
  const parameters = new URLSearchParams(requestTarget.slice(queryIndex + 1));
  const rawSection = parameters.get("section");
  if (rawSection === null) return { section: null, offset: 0, limit: 100 };
  const sections = new Set([
    "overview", "state", "events", "steps", "artifacts", "failures", "approvals", "decisions", "warnings"
  ]);
  if (!sections.has(rawSection)) {
    throw new AgentFlowRunInspectionApiError(
      "Unknown run inspection section.",
      400,
      "AGENT_FLOW_INSPECTION_BAD_REQUEST"
    );
  }
  if (rawSection === "overview") return { section: "overview", offset: 0, limit: 100 };
  if (rawSection === "state") return { section: "state", offset: 0, limit: 100 };
  const limit = integerQueryParameter(parameters, "limit", 100, 1, 200);
  const offset = integerQueryParameter(
    parameters,
    "offset",
    0,
    0,
    Number.MAX_SAFE_INTEGER - limit - 1
  );
  return { section: rawSection as AgentFlowRunInspectionSection, offset, limit };
}

function integerQueryParameter(
  parameters: URLSearchParams,
  name: string,
  fallback: number,
  minimum: number,
  maximum: number
): number {
  const raw = parameters.get(name);
  if (raw === null) return fallback;
  if (!/^\d+$/.test(raw)) {
    throw new AgentFlowRunInspectionApiError(
      `Inspection ${name} must be an integer from ${minimum} to ${maximum}.`,
      400,
      "AGENT_FLOW_INSPECTION_BAD_REQUEST"
    );
  }
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    throw new AgentFlowRunInspectionApiError(
      `Inspection ${name} must be an integer from ${minimum} to ${maximum}.`,
      400,
      "AGENT_FLOW_INSPECTION_BAD_REQUEST"
    );
  }
  return value;
}

function inspectionRoute(
  requestPath: string,
  encodedRunIdHeader?: string | string[]
): { kind: "list" } | { kind: "detail"; runId: string } {
  if (requestPath === "/api/runs") return { kind: "list" };
  if (requestPath === "/api/run") {
    if (encodedRunIdHeader === undefined || Array.isArray(encodedRunIdHeader)) {
      throw new AgentFlowRunInspectionApiError(
        "Run inspection requires one encoded run ID header.",
        400,
        "AGENT_FLOW_INSPECTION_BAD_REQUEST"
      );
    }
    return { kind: "detail", runId: decodeRunId(encodedRunIdHeader) };
  }
  const match = /^\/api\/runs\/([^/]+)$/.exec(requestPath);
  if (match === null) {
    throw new AgentFlowRunInspectionApiError(
      "Endpoint not found.",
      404,
      "AGENT_FLOW_INSPECTION_NOT_FOUND"
    );
  }
  return { kind: "detail", runId: decodeRunId(match[1]!) };
}

function decodeRunId(value: string): string {
  let runId: string;
  try {
    runId = decodeURIComponent(value);
  } catch {
    throw new AgentFlowRunInspectionApiError(
      "Run ID must use valid URL encoding.",
      400,
      "AGENT_FLOW_INSPECTION_BAD_REQUEST"
    );
  }
  if (runId.length === 0 || runId !== runId.trim()) {
    throw new AgentFlowRunInspectionApiError(
      "Run ID must be a non-empty canonical identifier without surrounding whitespace.",
      400,
      "AGENT_FLOW_INSPECTION_BAD_REQUEST"
    );
  }
  return runId;
}

function tokensEqual(left: string, right: string): boolean {
  const leftBytes = Buffer.from(left);
  const rightBytes = Buffer.from(right);
  return leftBytes.length === rightBytes.length && crypto.timingSafeEqual(leftBytes, rightBytes);
}

function normalizeLocalHost(value: string): string {
  const host = value.trim().toLowerCase().replace(/^\[|\]$/g, "");
  if (!["127.0.0.1", "::1"].includes(host)) {
    throw new AgentFlowRunInspectionApiError(
      `Agent Flow run inspection API host must be a numeric loopback address; received ${value}.`,
      400,
      "AGENT_FLOW_INSPECTION_HOST"
    );
  }
  return host;
}

function normalizePort(value: number): number {
  if (!Number.isInteger(value) || value < 0 || value > 65_535) {
    throw new AgentFlowRunInspectionApiError(
      "Agent Flow run inspection API port must be an integer from 0 to 65535.",
      400,
      "AGENT_FLOW_INSPECTION_PORT"
    );
  }
  return value;
}

function normalizeToken(value: string): string {
  if (!/^[\x21-\x7E]+$/.test(value)) {
    throw new AgentFlowRunInspectionApiError(
      "Agent Flow run inspection token must be non-empty header-safe ASCII without whitespace.",
      400,
      "AGENT_FLOW_INSPECTION_TOKEN"
    );
  }
  return value;
}

function listen(server: http.Server, host: string, port: number): Promise<number> {
  return new Promise((resolve, reject) => {
    const onError = (error: Error) => reject(error);
    server.once("error", onError);
    server.listen(port, host, () => {
      server.off("error", onError);
      const address = server.address();
      if (address === null || typeof address === "string") {
        reject(new Error("Agent Flow run inspection API did not bind a TCP port."));
        return;
      }
      resolve(address.port);
    });
  });
}

async function listenOnAvailablePort(server: http.Server, host: string, requestedPort: number): Promise<number> {
  if (requestedPort !== 0) return listen(server, host, requestedPort);
  const start = crypto.randomInt(49_152, 65_536);
  let lastError: unknown;
  for (let offset = 0; offset < 100; offset += 1) {
    const port = 49_152 + ((start - 49_152 + offset) % (65_536 - 49_152));
    try {
      return await listen(server, host, port);
    } catch (error) {
      lastError = error;
      if ((error as NodeJS.ErrnoException).code !== "EADDRINUSE") throw error;
    }
  }
  throw lastError;
}

function formatUrlHost(host: string): string {
  return host.includes(":") ? `[${host}]` : host;
}

function sendJson(response: http.ServerResponse, status: number, data: unknown): void {
  response.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store",
    "x-content-type-options": "nosniff"
  });
  response.end(JSON.stringify(data, null, 2));
}

function sendAsset(response: http.ServerResponse, contentType: string, content: string): void {
  response.writeHead(200, {
    "content-type": contentType,
    "cache-control": "no-store",
    "content-security-policy": "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'",
    "referrer-policy": "no-referrer",
    "x-content-type-options": "nosniff",
    "x-frame-options": "DENY"
  });
  response.end(content);
}
