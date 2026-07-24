import { createHash } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import {
  normalizeAgentflowArtifactPath,
  type AgentflowArtifactRecord,
  type AgentflowRunStateStore,
  type AgentflowRunStopStatus,
  type AgentflowRunStateValue,
  type WriteAgentflowArtifactInput
} from "./run_state";
import type { AgentflowWorkflow, AgentflowWorkflowStep, AgentflowYamlMapping } from "./workflow";

export const MAX_AGENTFLOW_MCP_OUTPUT_BYTES = 10 * 1024 * 1024;
export const MAX_AGENTFLOW_MCP_METADATA_BYTES = 1024 * 1024;
export const MAX_AGENTFLOW_MCP_ARGUMENT_BYTES = 1024 * 1024;
export const MAX_AGENTFLOW_MCP_CONTENT_TYPE_BYTES = 64 * 1024;

export interface AgentflowMcpCallRequest {
  runId: string;
  stepId: string;
  server: string;
  tool: string;
  arguments: Record<string, AgentflowRunStateValue>;
  outputs: string[];
  signal: AbortSignal;
}

export interface AgentflowMcpCallResponse {
  outputs: Record<string, AgentflowRunStateValue | Uint8Array>;
  contentTypes?: Record<string, string>;
  metadata?: Record<string, AgentflowRunStateValue>;
}

export type AgentflowMcpCallAdapter = (
  request: AgentflowMcpCallRequest
) => AgentflowMcpCallResponse | Promise<AgentflowMcpCallResponse>;

export interface AgentflowMcpCallExecutionResult {
  server: string;
  tool: string;
  requestArtifact: AgentflowArtifactRecord;
  outputArtifacts: AgentflowArtifactRecord[];
}

export interface ExecuteAgentflowMcpCallOptions {
  beforePublish?: () => void;
  stopStatus?: () => AgentflowRunStopStatus | undefined;
}

export class AgentflowMcpCallError extends Error {
  readonly code: string;

  constructor(message: string, code = "AGENTFLOW_MCP_CALL", options?: ErrorOptions) {
    super(message, options);
    this.name = "AgentflowMcpCallError";
    this.code = code;
  }
}

export class AgentflowMcpCallInterruptedError extends AgentflowMcpCallError {
  constructor(readonly status: AgentflowRunStopStatus) {
    super(`MCP call was interrupted because the run was ${status}.`, "AGENTFLOW_MCP_INTERRUPTED");
  }
}

export class AgentflowMcpCallRegistry {
  private readonly servers = new Map<string, AgentflowMcpCallAdapter>();

  register(server: string, adapter: AgentflowMcpCallAdapter): this {
    const normalized = requiredName(server, "MCP server name");
    if (this.servers.has(normalized)) {
      throw new AgentflowMcpCallError(
        `MCP server ${normalized} is already registered.`,
        "AGENTFLOW_MCP_SERVER_COLLISION"
      );
    }
    this.servers.set(normalized, adapter);
    return this;
  }

  get(server: string): AgentflowMcpCallAdapter | undefined {
    return this.servers.get(requiredName(server, "MCP server name"));
  }

  names(): string[] {
    return [...this.servers.keys()].sort();
  }
}

export function createAgentflowMcpCallRegistry(): AgentflowMcpCallRegistry {
  return new AgentflowMcpCallRegistry();
}

export function createAgentflowFixtureMcpAdapter(
  responses: Record<string, AgentflowMcpCallResponse>
): AgentflowMcpCallAdapter {
  const fixtures = new Map(Object.entries(responses));
  return (request) => {
    const response = fixtures.get(request.stepId);
    if (response === undefined) {
      throw new AgentflowMcpCallError(
        `Fixture MCP adapter has no response for step ${request.stepId}.`,
        "AGENTFLOW_MCP_FIXTURE_MISSING"
      );
    }
    return response;
  };
}

export async function executeAgentflowMcpCall(
  store: AgentflowRunStateStore,
  runId: string,
  workflow: AgentflowWorkflow,
  step: AgentflowWorkflowStep,
  registry: AgentflowMcpCallRegistry,
  options: ExecuteAgentflowMcpCallOptions = {}
): Promise<AgentflowMcpCallExecutionResult> {
  const run = store.getRun(runId);
  if (run === null || run.status !== "running") {
    throw new AgentflowMcpCallError(
      run === null
        ? `Agentflow run ${runId} was not found.`
        : `Agentflow run ${runId} must be running before an MCP tool can be invoked; current status is ${run.status}.`,
      "AGENTFLOW_MCP_RUN_STATUS"
    );
  }
  const stepId = requiredName(step.id, "MCP call step ID");
  const declaredStep = findWorkflowStep(workflow.steps, stepId);
  if (requiredName(step.type, `MCP call ${stepId} type`) !== "mcp_call"
      || !isDeepStrictEqual(run.context.workflow, workflow)
      || declaredStep === undefined || !isDeepStrictEqual(declaredStep, step)) {
    throw new AgentflowMcpCallError(
      `MCP call ${stepId} must match a step in the workflow persisted for run ${runId}.`,
      "AGENTFLOW_MCP_WORKFLOW_MISMATCH"
    );
  }

  const server = requiredName(step.server, `MCP call ${stepId} server`);
  const tool = requiredName(step.tool, `MCP call ${stepId} tool`);
  if ([server, tool].some((value) => value.includes("{{") || value.includes("}}"))) {
    throw new AgentflowMcpCallError(
      `MCP call ${stepId} server and tool must be static non-empty names.`,
      "AGENTFLOW_MCP_CALL_INVALID"
    );
  }
  const adapter = registry.get(server);
  if (adapter === undefined) {
    throw new AgentflowMcpCallError(
      `No adapter is registered for MCP server ${server}; register it explicitly before running step ${stepId}.`,
      "AGENTFLOW_MCP_SERVER_UNKNOWN"
    );
  }
  const declaredArguments = mapping(step.arguments);
  if (declaredArguments === undefined) {
    throw new AgentflowMcpCallError(
      `MCP call ${stepId} arguments must be a mapping.`,
      "AGENTFLOW_MCP_CALL_INVALID"
    );
  }
  const requestArguments = resolveAgentflowMcpArguments(declaredArguments, run.inputs, stepId);
  const auditArguments = normalizeJsonValue(requestArguments, new Set(), 0) as Record<string, AgentflowRunStateValue>;
  if (Buffer.byteLength(stableJson(auditArguments)) > MAX_AGENTFLOW_MCP_ARGUMENT_BYTES) {
    throw new AgentflowMcpCallError(
      `MCP call ${stepId} arguments exceed the ${MAX_AGENTFLOW_MCP_ARGUMENT_BYTES}-byte limit.`,
      "AGENTFLOW_MCP_ARGUMENTS_TOO_LARGE"
    );
  }
  const outputs = validateAgentflowMcpOutputPaths(step.outputs, stepId);
  const requestPath = `mcp-calls/${safePathSegment(stepId).slice(0, 200)}-${digest(stepId).slice(0, 12)}.json`;
  const artifactSnapshots = preflightCollisions(
    store,
    runId,
    stepId,
    server,
    tool,
    step.overwrite === true,
    outputs,
    requestPath
  );

  const request: AgentflowMcpCallRequest = {
    runId,
    stepId,
    server,
    tool,
    arguments: requestArguments,
    outputs: [...outputs],
    signal: new AbortController().signal
  };
  let response: AgentflowMcpCallResponse;
  try {
    response = await invokeAdapter(adapter, request, options.stopStatus);
  } catch (error) {
    if (error instanceof AgentflowMcpCallInterruptedError) throw error;
    if (error instanceof AgentflowMcpCallError) throw error;
    throw new AgentflowMcpCallError(
      `MCP server ${server} failed while invoking ${tool} for step ${stepId}: ${errorMessage(error)}`,
      "AGENTFLOW_MCP_ADAPTER_FAILED",
      { cause: error }
    );
  }

  const returned = validateResponse(stepId, outputs, response);
  const responseMetadata = validateMetadata(stepId, response.metadata);
  options.beforePublish?.();
  const requestMetadata = {
    stepId,
    server,
    tool,
    arguments: auditArguments,
    outputs,
    ...(responseMetadata === undefined ? {} : { responseMetadata })
  };
  const requestArtifactId = `mcp-request:${digest(requestPath)}`;
  const existingRequestArtifact = artifactSnapshots.request.artifact;
  const publications = [
    {
      id: requestArtifactId,
      runId,
      stepId,
      path: requestPath,
      kind: "mcp_request",
      contentType: "application/json; charset=utf-8",
      content: `${stableJson(requestMetadata)}\n`,
      overwrite: existingRequestArtifact !== null,
      requiredRunStatus: "running" as const,
      requiredCurrentArtifact: artifactSnapshots.request.required,
      metadata: { server, tool }
    },
    ...outputs.map((outputPath) => {
      const output = returned.get(outputPath)!;
      const snapshot = artifactSnapshots.outputs.get(outputPath)!;
      const existing = snapshot.artifact;
      return {
        id: existing?.id ?? `mcp-output:${digest(outputPath)}`,
        runId,
        stepId,
        path: outputPath,
        kind: "mcp_output",
        contentType: output.contentType ?? contentTypeFor(outputPath, output.content),
        content: serializeContent(output.content),
        overwrite: step.overwrite === true || ownedMcpOutput(existing, stepId, server, tool),
        requiredRunStatus: "running" as const,
        requiredCurrentArtifact: snapshot.required,
        metadata: { server, tool, requestArtifact: requestPath }
      };
    })
  ];
  const published = store.writeArtifactsAtomically(publications);
  const byPath = new Map(published.map((artifact) => [artifact.declaredPath, artifact]));
  return {
    server,
    tool,
    requestArtifact: byPath.get(requestPath)!,
    outputArtifacts: outputs.map((output) => byPath.get(output)!)
  };
}

function validateResponse(
  stepId: string,
  outputs: string[],
  response: AgentflowMcpCallResponse
): Map<string, { content: AgentflowRunStateValue | Uint8Array; contentType?: string }> {
  if (!plainObject(response) || !plainObject(response.outputs)) {
    throw new AgentflowMcpCallError(
      `MCP adapter response for step ${stepId} must contain an outputs mapping.`,
      "AGENTFLOW_MCP_OUTPUT_INVALID"
    );
  }
  const declared = new Set(outputs);
  const missing = outputs.find((output) => !Object.hasOwn(response.outputs, output));
  const extra = Object.keys(response.outputs).find((output) => !declared.has(output));
  if (missing !== undefined || extra !== undefined) {
    throw new AgentflowMcpCallError(
      missing !== undefined
        ? `MCP adapter response for step ${stepId} is missing declared output ${missing}.`
        : `MCP adapter response for step ${stepId} returned undeclared output ${extra}.`,
      "AGENTFLOW_MCP_OUTPUT_INVALID"
    );
  }
  const contentTypeEntries = response.contentTypes === undefined || !plainObject(response.contentTypes)
    ? undefined
    : Object.entries(response.contentTypes);
  if (response.contentTypes !== undefined && (contentTypeEntries === undefined
      || contentTypeEntries.some(([output, contentType]) =>
        !declared.has(output) || typeof contentType !== "string" || contentType.trim().length === 0)
      || contentTypeEntries.reduce((bytes, [output, contentType]) =>
        bytes + Buffer.byteLength(output) + Buffer.byteLength(String(contentType)), 0) > MAX_AGENTFLOW_MCP_CONTENT_TYPE_BYTES)) {
    throw new AgentflowMcpCallError(
      `MCP adapter response content types for step ${stepId} must map declared outputs to non-empty strings within the ${MAX_AGENTFLOW_MCP_CONTENT_TYPE_BYTES}-byte limit.`,
      "AGENTFLOW_MCP_OUTPUT_INVALID"
    );
  }

  const contentTypes = new Map(contentTypeEntries ?? []);
  let totalBytes = 0;
  return new Map(outputs.map((outputPath) => {
    const value = response.outputs[outputPath];
    let content: AgentflowRunStateValue | Uint8Array;
    let size: number;
    try {
      if (value instanceof Uint8Array) {
        size = value.byteLength;
        validateOutputSize(stepId, size, totalBytes + size);
        content = Uint8Array.from(value);
      } else if (typeof value === "string") {
        size = Buffer.byteLength(value);
        validateOutputSize(stepId, size, totalBytes + size);
        content = value;
      } else {
        content = normalizeJsonValue(value, new Set(), 0);
        size = serializeContent(content).byteLength;
        validateOutputSize(stepId, size, totalBytes + size);
      }
    } catch (error) {
      if (error instanceof AgentflowMcpCallError) throw error;
      throw new AgentflowMcpCallError(
        `MCP adapter output ${outputPath} for step ${stepId} must contain JSON-compatible, string, or binary content: ${errorMessage(error)}`,
        "AGENTFLOW_MCP_OUTPUT_INVALID",
        { cause: error }
      );
    }
    totalBytes += size;
    return [outputPath, {
      content,
      ...(contentTypes.has(outputPath)
        ? { contentType: contentTypes.get(outputPath)!.trim() }
        : {})
    }];
  }));
}

function validateOutputSize(stepId: string, size: number, totalBytes: number): void {
  if (size > MAX_AGENTFLOW_MCP_OUTPUT_BYTES || totalBytes > MAX_AGENTFLOW_MCP_OUTPUT_BYTES) {
    throw new AgentflowMcpCallError(
      `MCP adapter outputs for step ${stepId} exceed the ${MAX_AGENTFLOW_MCP_OUTPUT_BYTES}-byte limit.`,
      "AGENTFLOW_MCP_OUTPUT_TOO_LARGE"
    );
  }
}

async function invokeAdapter(
  adapter: AgentflowMcpCallAdapter,
  request: AgentflowMcpCallRequest,
  stopStatus: ExecuteAgentflowMcpCallOptions["stopStatus"]
): Promise<AgentflowMcpCallResponse> {
  const initialStatus = stopStatus?.();
  if (initialStatus !== undefined) throw new AgentflowMcpCallInterruptedError(initialStatus);
  if (stopStatus === undefined) return adapter(request);

  const controller = new AbortController();
  request.signal = controller.signal;
  let timer: ReturnType<typeof setInterval> | undefined;
  const interrupted = new Promise<never>((_resolve, reject) => {
    timer = setInterval(() => {
      const status = stopStatus();
      if (status === undefined) return;
      const error = new AgentflowMcpCallInterruptedError(status);
      controller.abort(error);
      reject(error);
    }, 25);
  });
  try {
    return await Promise.race([Promise.resolve(adapter(request)), interrupted]);
  } finally {
    if (timer !== undefined) clearInterval(timer);
  }
}

function validateMetadata(
  stepId: string,
  metadata: AgentflowMcpCallResponse["metadata"]
): Record<string, AgentflowRunStateValue> | undefined {
  if (metadata === undefined) return undefined;
  if (runStateMapping(metadata) === undefined) {
    throw new AgentflowMcpCallError(
      `MCP adapter metadata for step ${stepId} must be a plain object.`,
      "AGENTFLOW_MCP_METADATA_INVALID"
    );
  }
  let normalized: Record<string, AgentflowRunStateValue>;
  try {
    normalized = normalizeJsonValue(metadata, new Set(), 0) as Record<string, AgentflowRunStateValue>;
  } catch (error) {
    throw new AgentflowMcpCallError(
      `MCP adapter metadata for step ${stepId} must contain only valid JSON values: ${errorMessage(error)}`,
      "AGENTFLOW_MCP_METADATA_INVALID",
      { cause: error }
    );
  }
  if (Buffer.byteLength(stableJson(normalized)) > MAX_AGENTFLOW_MCP_METADATA_BYTES) {
    throw new AgentflowMcpCallError(
      `MCP adapter metadata for step ${stepId} exceeds the ${MAX_AGENTFLOW_MCP_METADATA_BYTES}-byte limit.`,
      "AGENTFLOW_MCP_METADATA_TOO_LARGE"
    );
  }
  return normalized;
}

function preflightCollisions(
  store: AgentflowRunStateStore,
  runId: string,
  stepId: string,
  server: string,
  tool: string,
  overwrite: boolean,
  outputs: string[],
  requestPath: string
): { request: McpArtifactSnapshot; outputs: Map<string, McpArtifactSnapshot> } {
  if (outputs.includes(requestPath)) {
    throw new AgentflowMcpCallError(
      `MCP output ${requestPath} conflicts with the runtime request metadata artifact.`,
      "AGENTFLOW_MCP_OUTPUT_COLLISION"
    );
  }
  const requestArtifactId = `mcp-request:${digest(requestPath)}`;
  const requestIdOwner = store.getArtifactById(runId, requestArtifactId);
  if (requestIdOwner !== null && requestIdOwner.declaredPath !== requestPath) {
    throw new AgentflowMcpCallError(
      `MCP request metadata ID ${requestArtifactId} is already registered at ${requestIdOwner.declaredPath}.`,
      "AGENTFLOW_MCP_OUTPUT_COLLISION"
    );
  }
  const requestArtifact = store.getArtifact(runId, requestPath);
  if (requestArtifact !== null &&
      !ownedMcpRequest(requestArtifact, requestArtifactId, stepId, server, tool)) {
    throw new AgentflowMcpCallError(
      `MCP request metadata path ${requestPath} is already owned by another artifact.`,
      "AGENTFLOW_MCP_OUTPUT_COLLISION"
    );
  }
  const requestSnapshot = currentArtifactSnapshot(store, runId, requestPath, requestArtifact);
  const outputSnapshots = new Map(outputs.map((output) => {
    const artifact = store.getArtifact(runId, output);
    const generatedId = `mcp-output:${digest(output)}`;
    const idOwner = artifact === null ? store.getArtifactById(runId, generatedId) : null;
    if (idOwner !== null && idOwner.declaredPath !== output) {
      throw new AgentflowMcpCallError(
        `MCP output ID ${generatedId} is already registered at ${idOwner.declaredPath}.`,
        "AGENTFLOW_MCP_OUTPUT_COLLISION"
      );
    }
    return [output, currentArtifactSnapshot(store, runId, output, artifact)] as const;
  }));
  const collision = overwrite ? undefined : outputs.find((output) => {
    const snapshot = outputSnapshots.get(output)!;
    return snapshot.artifact !== null && !ownedMcpOutput(snapshot.artifact, stepId, server, tool);
  });
  if (collision !== undefined) {
    throw new AgentflowMcpCallError(
      `MCP output ${collision} already exists; declare overwrite: true to replace it.`,
      "AGENTFLOW_MCP_OUTPUT_COLLISION"
    );
  }
  return { request: requestSnapshot, outputs: outputSnapshots };
}

interface McpArtifactSnapshot {
  artifact: AgentflowArtifactRecord | null;
  required: NonNullable<WriteAgentflowArtifactInput["requiredCurrentArtifact"]>;
}

function ownedMcpRequest(
  artifact: AgentflowArtifactRecord,
  artifactId: string,
  stepId: string,
  server: string,
  tool: string
): boolean {
  return artifact.kind === "mcp_request"
    && artifact.id === artifactId
    && artifact.producerStepId === stepId
    && artifact.metadata.server === server
    && artifact.metadata.tool === tool;
}

function currentArtifactSnapshot(
  store: AgentflowRunStateStore,
  runId: string,
  artifactPath: string,
  artifact: AgentflowArtifactRecord | null
): McpArtifactSnapshot {
  store.recoverArtifactBacking(runId, artifactPath);
  const backing = store.getArtifactBackingSnapshot(runId, artifactPath);
  const backingMatches = artifact?.checksum === null
    ? !backing.exists
    : artifact === null || (backing.exists && backing.checksum === artifact.checksum);
  if (!backingMatches) {
    throw new AgentflowMcpCallError(
      `MCP artifact ${artifactPath} backing file does not match its registry record.`,
      "AGENTFLOW_MCP_OUTPUT_COLLISION"
    );
  }
  return {
    artifact,
    required: {
      artifact: artifact === null ? null : {
        id: artifact.id,
        producerStepId: artifact.producerStepId,
        kind: artifact.kind,
        contentType: artifact.contentType,
        checksum: artifact.checksum,
        generation: artifact.generation,
        metadata: artifact.metadata
      },
      backingExists: backing.exists,
      backingChecksum: backing.checksum
    }
  };
}

function ownedMcpOutput(
  artifact: AgentflowArtifactRecord | null,
  stepId: string,
  server: string,
  tool: string
): boolean {
  return artifact?.kind === "mcp_output"
    && artifact.producerStepId === stepId
    && artifact.metadata.server === server
    && artifact.metadata.tool === tool;
}

export function validateAgentflowMcpOutputPaths(value: unknown, stepId: string): string[] {
  if (!Array.isArray(value) || value.length === 0 ||
      !value.every((entry) => typeof entry === "string" && entry.trim().length > 0)) {
    throw new AgentflowMcpCallError(
      `MCP call ${stepId} outputs must be a non-empty list of artifact paths.`,
      "AGENTFLOW_MCP_CALL_INVALID"
    );
  }
  const outputs = value.map((entry) => {
    const declared = (entry as string).trim();
    let normalized: string;
    try {
      normalized = normalizeAgentflowArtifactPath(declared);
    } catch (error) {
      throw new AgentflowMcpCallError(
        `MCP call ${stepId} output ${declared} must be a normalized static repo-relative artifact path.`,
        "AGENTFLOW_MCP_CALL_INVALID",
        { cause: error }
      );
    }
    if (declared.includes("{{") || declared.includes("}}") || normalized !== declared) {
      throw new AgentflowMcpCallError(
        `MCP call ${stepId} output ${declared} must be a normalized static repo-relative artifact path.`,
        "AGENTFLOW_MCP_CALL_INVALID"
      );
    }
    return normalized;
  });
  if (new Set(outputs).size !== outputs.length) {
    throw new AgentflowMcpCallError(
      `MCP call ${stepId} outputs must not contain duplicate artifact paths.`,
      "AGENTFLOW_MCP_CALL_INVALID"
    );
  }
  return outputs;
}

export function resolveAgentflowMcpArguments(
  value: unknown,
  inputs: Record<string, AgentflowRunStateValue>,
  stepId: string
): Record<string, AgentflowRunStateValue> {
  const declaredArguments = mapping(value);
  if (declaredArguments === undefined) {
    throw new AgentflowMcpCallError(
      `MCP call ${stepId} arguments must be a mapping.`,
      "AGENTFLOW_MCP_CALL_INVALID"
    );
  }
  validateAgentflowMcpArgumentExpressions(declaredArguments, stepId);
  const argumentsValue = resolveValue(
    declaredArguments as Record<string, AgentflowRunStateValue>,
    inputs,
    stepId
  );
  const resolvedArguments = runStateMapping(argumentsValue);
  if (resolvedArguments === undefined) {
    throw new AgentflowMcpCallError(
      `MCP call ${stepId} arguments must resolve to a mapping.`,
      "AGENTFLOW_MCP_ARGUMENTS_INVALID"
    );
  }
  return normalizeJsonValue(resolvedArguments, new Set(), 0) as Record<string, AgentflowRunStateValue>;
}

export function validateAgentflowMcpArgumentExpressions(value: unknown, stepId: string): void {
  if (Array.isArray(value)) {
    for (const entry of value) validateAgentflowMcpArgumentExpressions(entry, stepId);
    return;
  }
  const record = runStateMapping(value);
  if (record !== undefined) {
    for (const entry of Object.values(record)) validateAgentflowMcpArgumentExpressions(entry, stepId);
    return;
  }
  if (typeof value !== "string") return;
  const remainder = value.replace(/(?<!\{)\{\{\s*inputs\.([A-Za-z_][A-Za-z0-9_-]*)\s*}}(?!})/g, "");
  if (remainder.includes("{{") || remainder.includes("}}")) {
    throw new AgentflowMcpCallError(
      `MCP call ${stepId} argument contains an unsupported input expression.`,
      "AGENTFLOW_MCP_ARGUMENT_UNRESOLVED"
    );
  }
}

function resolveValue(
  value: AgentflowRunStateValue,
  inputs: Record<string, AgentflowRunStateValue>,
  stepId: string
): AgentflowRunStateValue {
  if (Array.isArray(value)) return value.map((entry) => resolveValue(entry, inputs, stepId));
  const record = runStateMapping(value);
  if (record !== undefined) {
    return Object.fromEntries(Object.entries(record).map(([key, entry]) => [key, resolveValue(entry, inputs, stepId)]));
  }
  if (typeof value !== "string") return value;
  const exact = /^\{\{\s*inputs\.([A-Za-z_][A-Za-z0-9_-]*)\s*}}$/.exec(value);
  if (exact !== null) {
    if (!Object.hasOwn(inputs, exact[1]!)) {
      throw new AgentflowMcpCallError(
        `MCP call ${stepId} argument ${value} references missing persisted input ${exact[1]}.`,
        "AGENTFLOW_MCP_ARGUMENT_UNRESOLVED"
      );
    }
    return inputs[exact[1]!]!;
  }
  return value.replace(/(?<!\{)\{\{\s*inputs\.([A-Za-z_][A-Za-z0-9_-]*)\s*}}(?!})/g, (_match, name: string) => {
    if (!Object.hasOwn(inputs, name)) {
      throw new AgentflowMcpCallError(
        `MCP call ${stepId} inline argument reference inputs.${name} is missing from persisted inputs.`,
        "AGENTFLOW_MCP_ARGUMENT_UNRESOLVED"
      );
    }
    const resolved = inputs[name];
    if (resolved === undefined || typeof resolved === "object") {
      throw new AgentflowMcpCallError(
        `MCP call ${stepId} inline argument reference inputs.${name} must resolve to a scalar value.`,
        "AGENTFLOW_MCP_ARGUMENT_UNRESOLVED"
      );
    }
    return String(resolved);
  });
}

function findWorkflowStep(steps: AgentflowWorkflowStep[], stepId: string): AgentflowWorkflowStep | undefined {
  for (const step of steps) {
    if (typeof step.id === "string" && step.id.trim() === stepId) return step;
    for (const field of ["branches", "body", "steps"] as const) {
      const nested = step[field];
      if (!Array.isArray(nested)) continue;
      const found = findWorkflowStep(nested.filter(mapping) as AgentflowWorkflowStep[], stepId);
      if (found !== undefined) return found;
    }
  }
  return undefined;
}

function serializeContent(value: AgentflowRunStateValue | Uint8Array): Buffer {
  if (value instanceof Uint8Array) return Buffer.from(value);
  if (typeof value === "string") return Buffer.from(value);
  return Buffer.from(`${stableJson(value)}\n`);
}

function contentTypeFor(path: string, value: AgentflowRunStateValue | Uint8Array): string {
  if (value instanceof Uint8Array) return "application/octet-stream";
  if (typeof value !== "string") return "application/json; charset=utf-8";
  if (path.endsWith(".json")) return "application/json; charset=utf-8";
  if (path.endsWith(".md")) return "text/markdown; charset=utf-8";
  return "text/plain; charset=utf-8";
}

function stableJson(value: AgentflowRunStateValue): string {
  return JSON.stringify(value, (_key, entry) => {
    if (entry !== null && typeof entry === "object" && !Array.isArray(entry)) {
      return Object.fromEntries(Object.keys(entry).sort().map((key) => [key, entry[key]]));
    }
    return entry;
  });
}

function normalizeJsonValue(value: unknown, ancestors: Set<object>, depth: number): AgentflowRunStateValue {
  if (depth > 50) throw new Error("JSON nesting exceeds 50 levels");
  if (value === null || typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new Error("JSON numbers must be finite");
    return value;
  }
  if (typeof value !== "object") throw new Error(`unsupported ${typeof value} value`);
  if (ancestors.has(value)) throw new Error("JSON value contains a cycle");
  const prototype = Object.getPrototypeOf(value);
  if (!Array.isArray(value) && prototype !== Object.prototype && prototype !== null) {
    throw new Error("JSON objects must be plain objects");
  }
  ancestors.add(value);
  try {
    if (Array.isArray(value)) {
      if (Object.keys(value).some((key) => !/^(0|[1-9]\d*)$/.test(key)) || Object.keys(value).length !== value.length) {
        throw new Error("JSON arrays cannot be sparse or have named properties");
      }
      return value.map((entry) => normalizeJsonValue(entry, ancestors, depth + 1));
    }
    return Object.fromEntries(
      Object.entries(value).map(([key, entry]) => [key, normalizeJsonValue(entry, ancestors, depth + 1)])
    );
  } finally {
    ancestors.delete(value);
  }
}

function mapping(value: unknown): AgentflowYamlMapping | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value) && !(value instanceof Uint8Array)
    ? value as AgentflowYamlMapping
    : undefined;
}

function plainObject(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value) || value instanceof Uint8Array) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function runStateMapping(value: unknown): Record<string, AgentflowRunStateValue> | undefined {
  return plainObject(value) ? value as Record<string, AgentflowRunStateValue> : undefined;
}

function requiredName(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new AgentflowMcpCallError(`${label} must be a non-empty string.`, "AGENTFLOW_MCP_CALL_INVALID");
  }
  return value.trim();
}

function safePathSegment(value: string): string {
  return value.replace(/[^A-Za-z0-9._-]+/g, "-").replace(/^-+|-+$/g, "") || "step";
}

function digest(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
