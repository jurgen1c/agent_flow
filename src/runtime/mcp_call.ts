import { createHash } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import {
  normalizeAgentFlowArtifactPath,
  type AgentFlowArtifactRecord,
  type AgentFlowRunStateStore,
  type AgentFlowRunStopStatus,
  type AgentFlowRunStateValue,
  type WriteAgentFlowArtifactInput
} from "./run_state";
import type { AgentFlowWorkflow, AgentFlowWorkflowStep, AgentFlowYamlMapping } from "./workflow";
import {
  AgentFlowSensitiveInputError,
  assertAgentFlowAdapterStringSafe,
  secureAgentFlowJsonInput,
  secureAgentFlowSensitiveJsonInputValue
} from "./execution_security";
import { agentFlowInputKeyLooksSensitive, redactAgentFlowSensitiveText } from "./failure_payload";

export const MAX_AGENT_FLOW_MCP_OUTPUT_BYTES = 10 * 1024 * 1024;
export const MAX_AGENT_FLOW_MCP_METADATA_BYTES = 1024 * 1024;
export const MAX_AGENT_FLOW_MCP_ARGUMENT_BYTES = 1024 * 1024;
export const MAX_AGENT_FLOW_MCP_CONTENT_TYPE_BYTES = 64 * 1024;

export interface AgentFlowMcpCallRequest {
  runId: string;
  stepId: string;
  server: string;
  tool: string;
  arguments: Record<string, AgentFlowRunStateValue>;
  outputs: string[];
  signal: AbortSignal;
}

export interface AgentFlowMcpCallResponse {
  outputs: Record<string, AgentFlowRunStateValue | Uint8Array>;
  contentTypes?: Record<string, string>;
  metadata?: Record<string, AgentFlowRunStateValue>;
}

export type AgentFlowMcpCallAdapter = (
  request: AgentFlowMcpCallRequest
) => AgentFlowMcpCallResponse | Promise<AgentFlowMcpCallResponse>;

export interface AgentFlowMcpCallExecutionResult {
  server: string;
  tool: string;
  requestArtifact: AgentFlowArtifactRecord;
  outputArtifacts: AgentFlowArtifactRecord[];
}

export interface ExecuteAgentFlowMcpCallOptions {
  attempt?: number;
  beforePublish?: () => void;
  stopStatus?: () => AgentFlowRunStopStatus | undefined;
}

export class AgentFlowMcpCallError extends Error {
  readonly code: string;

  constructor(message: string, code = "AGENT_FLOW_MCP_CALL", options?: ErrorOptions) {
    super(message, options);
    this.name = "AgentFlowMcpCallError";
    this.code = code;
  }
}

export class AgentFlowMcpCallInterruptedError extends AgentFlowMcpCallError {
  constructor(readonly status: AgentFlowRunStopStatus) {
    super(`MCP call was interrupted because the run was ${status}.`, "AGENT_FLOW_MCP_INTERRUPTED");
  }
}

export class AgentFlowMcpCallRegistry {
  private readonly servers = new Map<string, AgentFlowMcpCallAdapter>();

  register(server: string, adapter: AgentFlowMcpCallAdapter): this {
    const normalized = requiredName(server, "MCP server name");
    if (this.servers.has(normalized)) {
      throw new AgentFlowMcpCallError(
        `MCP server ${normalized} is already registered.`,
        "AGENT_FLOW_MCP_SERVER_COLLISION"
      );
    }
    this.servers.set(normalized, adapter);
    return this;
  }

  get(server: string): AgentFlowMcpCallAdapter | undefined {
    return this.servers.get(requiredName(server, "MCP server name"));
  }

  names(): string[] {
    return [...this.servers.keys()].sort();
  }
}

export function createAgentFlowMcpCallRegistry(): AgentFlowMcpCallRegistry {
  return new AgentFlowMcpCallRegistry();
}

export function createAgentFlowFixtureMcpAdapter(
  responses: Record<string, AgentFlowMcpCallResponse>
): AgentFlowMcpCallAdapter {
  const fixtures = new Map(Object.entries(responses));
  return (request) => {
    const response = fixtures.get(request.stepId);
    if (response === undefined) {
      throw new AgentFlowMcpCallError(
        `Fixture MCP adapter has no response for step ${request.stepId}.`,
        "AGENT_FLOW_MCP_FIXTURE_MISSING"
      );
    }
    return response;
  };
}

export async function executeAgentFlowMcpCall(
  store: AgentFlowRunStateStore,
  runId: string,
  workflow: AgentFlowWorkflow,
  step: AgentFlowWorkflowStep,
  registry: AgentFlowMcpCallRegistry,
  options: ExecuteAgentFlowMcpCallOptions = {}
): Promise<AgentFlowMcpCallExecutionResult> {
  const run = store.getRun(runId);
  if (run === null || run.status !== "running") {
    throw new AgentFlowMcpCallError(
      run === null
        ? `Agent Flow run ${runId} was not found.`
        : `Agent Flow run ${runId} must be running before an MCP tool can be invoked; current status is ${run.status}.`,
      "AGENT_FLOW_MCP_RUN_STATUS"
    );
  }
  const stepId = requiredName(step.id, "MCP call step ID");
  const declaredStep = findWorkflowStep(workflow.steps, stepId);
  if (requiredName(step.type, `MCP call ${stepId} type`) !== "mcp_call"
      || !isDeepStrictEqual(run.context.workflow, workflow)
      || declaredStep === undefined || !isDeepStrictEqual(declaredStep, step)) {
    throw new AgentFlowMcpCallError(
      `MCP call ${stepId} must match a step in the workflow persisted for run ${runId}.`,
      "AGENT_FLOW_MCP_WORKFLOW_MISMATCH"
    );
  }

  const server = requiredName(step.server, `MCP call ${stepId} server`);
  const tool = requiredName(step.tool, `MCP call ${stepId} tool`);
  if ([server, tool].some((value) => value.includes("{{") || value.includes("}}"))) {
    throw new AgentFlowMcpCallError(
      `MCP call ${stepId} server and tool must be static non-empty names.`,
      "AGENT_FLOW_MCP_CALL_INVALID"
    );
  }
  const outputs = validateAgentFlowMcpOutputPaths(step.outputs, stepId);
  try {
    for (const [label, value, path] of [
      ["MCP adapter run ID", runId, false],
      ["MCP adapter step ID", stepId, false],
      ["MCP adapter server", server, false],
      ["MCP adapter tool", tool, false],
      ...outputs.map((output): [string, string, boolean] => ["MCP adapter output path", output, false])
    ] as Array<[string, string, boolean]>) {
      assertAgentFlowAdapterStringSafe(workflow, label, value, { path });
    }
  } catch (error) {
    if (error instanceof AgentFlowSensitiveInputError) {
      throw new AgentFlowMcpCallError(error.message, error.code, { cause: error });
    }
    throw error;
  }
  const adapter = registry.get(server);
  if (adapter === undefined) {
    throw new AgentFlowMcpCallError(
      `No adapter is registered for MCP server ${server}; register it explicitly before running step ${stepId}.`,
      "AGENT_FLOW_MCP_SERVER_UNKNOWN"
    );
  }
  const declaredArguments = mapping(step.arguments);
  if (declaredArguments === undefined) {
    throw new AgentFlowMcpCallError(
      `MCP call ${stepId} arguments must be a mapping.`,
      "AGENT_FLOW_MCP_CALL_INVALID"
    );
  }
  let securedArguments: ReturnType<typeof prepareAgentFlowMcpArguments>;
  try {
    securedArguments = prepareAgentFlowMcpArguments(
      workflow,
      declaredArguments,
      run.inputs,
      stepId
    );
  } catch (error) {
    if (error instanceof AgentFlowSensitiveInputError) {
      throw new AgentFlowMcpCallError(error.message, error.code, { cause: error });
    }
    throw error;
  }
  const requestArguments = securedArguments.value;
  const auditArguments = structuredClone(securedArguments.value);
  assertAgentFlowMcpArgumentSize(auditArguments, stepId, "secured");
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

  const request: AgentFlowMcpCallRequest = {
    runId,
    stepId,
    server,
    tool,
    arguments: requestArguments,
    outputs: [...outputs],
    signal: new AbortController().signal
  };
  let response: AgentFlowMcpCallResponse;
  try {
    response = await invokeAdapter(adapter, request, options.stopStatus);
  } catch (error) {
    if (error instanceof AgentFlowMcpCallInterruptedError) throw error;
    if (error instanceof AgentFlowMcpCallError) {
      throw new AgentFlowMcpCallError(errorMessage(error), error.code, { cause: sanitizedErrorCause(error) });
    }
    throw new AgentFlowMcpCallError(
      `MCP server ${server} failed while invoking ${tool} for step ${stepId}: ${errorMessage(error)}`,
      "AGENT_FLOW_MCP_ADAPTER_FAILED",
      { cause: sanitizedErrorCause(error) }
    );
  }

  try {
    const returned = validateResponse(stepId, outputs, response);
    const responseMetadata = validateMetadata(stepId, response.metadata);
    options.beforePublish?.();
    const requestMetadata = {
    stepId,
    server,
    tool,
    arguments: auditArguments,
    outputs,
    ...(securedArguments.redacted ? { redacted: true } : {}),
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
      metadata: {
        server,
        tool,
        ...(options.attempt === undefined ? {} : { attempt: options.attempt })
      }
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
        metadata: {
          server,
          tool,
          requestArtifact: requestPath,
          ...(options.attempt === undefined ? {} : { attempt: options.attempt })
        }
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
  } catch (error) {
    if (error instanceof AgentFlowMcpCallInterruptedError) throw error;
    if (error instanceof AgentFlowMcpCallError) {
      throw new AgentFlowMcpCallError(errorMessage(error), error.code, { cause: sanitizedErrorCause(error) });
    }
    const code = agentFlowErrorCode(error);
    if (code !== undefined) {
      throw new AgentFlowMcpCallError(errorMessage(error), code, { cause: sanitizedErrorCause(error) });
    }
    throw new AgentFlowMcpCallError(
      `MCP server ${server} response processing failed for ${tool} at step ${stepId}: ${errorMessage(error)}`,
      "AGENT_FLOW_MCP_RESPONSE_FAILED",
      { cause: sanitizedErrorCause(error) }
    );
  }
}

function validateResponse(
  stepId: string,
  outputs: string[],
  response: AgentFlowMcpCallResponse
): Map<string, { content: AgentFlowRunStateValue | Uint8Array; contentType?: string }> {
  if (!plainObject(response) || !plainObject(response.outputs)) {
    throw new AgentFlowMcpCallError(
      `MCP adapter response for step ${stepId} must contain an outputs mapping.`,
      "AGENT_FLOW_MCP_OUTPUT_INVALID"
    );
  }
  const declared = new Set(outputs);
  const missing = outputs.find((output) => !Object.hasOwn(response.outputs, output));
  const extra = Object.keys(response.outputs).find((output) => !declared.has(output));
  if (missing !== undefined || extra !== undefined) {
    throw new AgentFlowMcpCallError(
      missing !== undefined
        ? `MCP adapter response for step ${stepId} is missing declared output ${missing}.`
        : `MCP adapter response for step ${stepId} returned undeclared output ${extra}.`,
      "AGENT_FLOW_MCP_OUTPUT_INVALID"
    );
  }
  const contentTypeEntries = response.contentTypes === undefined || !plainObject(response.contentTypes)
    ? undefined
    : Object.entries(response.contentTypes);
  if (response.contentTypes !== undefined && (contentTypeEntries === undefined
      || contentTypeEntries.some(([output, contentType]) =>
        !declared.has(output) || typeof contentType !== "string" || contentType.trim().length === 0)
      || contentTypeEntries.reduce((bytes, [output, contentType]) =>
        bytes + Buffer.byteLength(output) + Buffer.byteLength(String(contentType)), 0) > MAX_AGENT_FLOW_MCP_CONTENT_TYPE_BYTES)) {
    throw new AgentFlowMcpCallError(
      `MCP adapter response content types for step ${stepId} must map declared outputs to non-empty strings within the ${MAX_AGENT_FLOW_MCP_CONTENT_TYPE_BYTES}-byte limit.`,
      "AGENT_FLOW_MCP_OUTPUT_INVALID"
    );
  }

  const contentTypes = new Map(contentTypeEntries ?? []);
  let totalBytes = 0;
  return new Map(outputs.map((outputPath) => {
    const value = response.outputs[outputPath];
    let content: AgentFlowRunStateValue | Uint8Array;
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
      if (error instanceof AgentFlowMcpCallError) throw error;
      throw new AgentFlowMcpCallError(
        `MCP adapter output ${outputPath} for step ${stepId} must contain JSON-compatible, string, or binary content: ${errorMessage(error)}`,
        "AGENT_FLOW_MCP_OUTPUT_INVALID",
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
  if (size > MAX_AGENT_FLOW_MCP_OUTPUT_BYTES || totalBytes > MAX_AGENT_FLOW_MCP_OUTPUT_BYTES) {
    throw new AgentFlowMcpCallError(
      `MCP adapter outputs for step ${stepId} exceed the ${MAX_AGENT_FLOW_MCP_OUTPUT_BYTES}-byte limit.`,
      "AGENT_FLOW_MCP_OUTPUT_TOO_LARGE"
    );
  }
}

async function invokeAdapter(
  adapter: AgentFlowMcpCallAdapter,
  request: AgentFlowMcpCallRequest,
  stopStatus: ExecuteAgentFlowMcpCallOptions["stopStatus"]
): Promise<AgentFlowMcpCallResponse> {
  const initialStatus = stopStatus?.();
  if (initialStatus !== undefined) throw new AgentFlowMcpCallInterruptedError(initialStatus);
  if (stopStatus === undefined) return adapter(request);

  const controller = new AbortController();
  request.signal = controller.signal;
  let timer: ReturnType<typeof setInterval> | undefined;
  const interrupted = new Promise<never>((_resolve, reject) => {
    timer = setInterval(() => {
      const status = stopStatus();
      if (status === undefined) return;
      const error = new AgentFlowMcpCallInterruptedError(status);
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
  metadata: AgentFlowMcpCallResponse["metadata"]
): Record<string, AgentFlowRunStateValue> | undefined {
  if (metadata === undefined) return undefined;
  if (runStateMapping(metadata) === undefined) {
    throw new AgentFlowMcpCallError(
      `MCP adapter metadata for step ${stepId} must be a plain object.`,
      "AGENT_FLOW_MCP_METADATA_INVALID"
    );
  }
  let normalized: Record<string, AgentFlowRunStateValue>;
  try {
    normalized = normalizeJsonValue(metadata, new Set(), 0) as Record<string, AgentFlowRunStateValue>;
  } catch (error) {
    throw new AgentFlowMcpCallError(
      `MCP adapter metadata for step ${stepId} must contain only valid JSON values: ${errorMessage(error)}`,
      "AGENT_FLOW_MCP_METADATA_INVALID",
      { cause: error }
    );
  }
  if (Buffer.byteLength(stableJson(normalized)) > MAX_AGENT_FLOW_MCP_METADATA_BYTES) {
    throw new AgentFlowMcpCallError(
      `MCP adapter metadata for step ${stepId} exceeds the ${MAX_AGENT_FLOW_MCP_METADATA_BYTES}-byte limit.`,
      "AGENT_FLOW_MCP_METADATA_TOO_LARGE"
    );
  }
  return normalized;
}

function preflightCollisions(
  store: AgentFlowRunStateStore,
  runId: string,
  stepId: string,
  server: string,
  tool: string,
  overwrite: boolean,
  outputs: string[],
  requestPath: string
): { request: McpArtifactSnapshot; outputs: Map<string, McpArtifactSnapshot> } {
  if (outputs.includes(requestPath)) {
    throw new AgentFlowMcpCallError(
      `MCP output ${requestPath} conflicts with the runtime request metadata artifact.`,
      "AGENT_FLOW_MCP_OUTPUT_COLLISION"
    );
  }
  const requestArtifactId = `mcp-request:${digest(requestPath)}`;
  const requestIdOwner = store.getArtifactById(runId, requestArtifactId);
  if (requestIdOwner !== null && requestIdOwner.declaredPath !== requestPath) {
    throw new AgentFlowMcpCallError(
      `MCP request metadata ID ${requestArtifactId} is already registered at ${requestIdOwner.declaredPath}.`,
      "AGENT_FLOW_MCP_OUTPUT_COLLISION"
    );
  }
  const requestArtifact = store.getArtifact(runId, requestPath);
  if (requestArtifact !== null &&
      !ownedMcpRequest(requestArtifact, requestArtifactId, stepId, server, tool)) {
    throw new AgentFlowMcpCallError(
      `MCP request metadata path ${requestPath} is already owned by another artifact.`,
      "AGENT_FLOW_MCP_OUTPUT_COLLISION"
    );
  }
  const requestSnapshot = currentArtifactSnapshot(store, runId, requestPath, requestArtifact);
  const outputSnapshots = new Map(outputs.map((output) => {
    const artifact = store.getArtifact(runId, output);
    const generatedId = `mcp-output:${digest(output)}`;
    const idOwner = artifact === null ? store.getArtifactById(runId, generatedId) : null;
    if (idOwner !== null && idOwner.declaredPath !== output) {
      throw new AgentFlowMcpCallError(
        `MCP output ID ${generatedId} is already registered at ${idOwner.declaredPath}.`,
        "AGENT_FLOW_MCP_OUTPUT_COLLISION"
      );
    }
    return [output, currentArtifactSnapshot(store, runId, output, artifact)] as const;
  }));
  const collision = overwrite ? undefined : outputs.find((output) => {
    const snapshot = outputSnapshots.get(output)!;
    return snapshot.artifact !== null && !ownedMcpOutput(snapshot.artifact, stepId, server, tool);
  });
  if (collision !== undefined) {
    throw new AgentFlowMcpCallError(
      `MCP output ${collision} already exists; declare overwrite: true to replace it.`,
      "AGENT_FLOW_MCP_OUTPUT_COLLISION"
    );
  }
  return { request: requestSnapshot, outputs: outputSnapshots };
}

interface McpArtifactSnapshot {
  artifact: AgentFlowArtifactRecord | null;
  required: NonNullable<WriteAgentFlowArtifactInput["requiredCurrentArtifact"]>;
}

function ownedMcpRequest(
  artifact: AgentFlowArtifactRecord,
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
  store: AgentFlowRunStateStore,
  runId: string,
  artifactPath: string,
  artifact: AgentFlowArtifactRecord | null
): McpArtifactSnapshot {
  store.recoverArtifactBacking(runId, artifactPath);
  const backing = store.getArtifactBackingSnapshot(runId, artifactPath);
  const backingMatches = artifact?.checksum === null
    ? !backing.exists
    : artifact === null || (backing.exists && backing.checksum === artifact.checksum);
  if (!backingMatches) {
    throw new AgentFlowMcpCallError(
      `MCP artifact ${artifactPath} backing file does not match its registry record.`,
      "AGENT_FLOW_MCP_OUTPUT_COLLISION"
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
  artifact: AgentFlowArtifactRecord | null,
  stepId: string,
  server: string,
  tool: string
): boolean {
  return artifact?.kind === "mcp_output"
    && artifact.producerStepId === stepId
    && artifact.metadata.server === server
    && artifact.metadata.tool === tool;
}

export function validateAgentFlowMcpOutputPaths(value: unknown, stepId: string): string[] {
  if (!Array.isArray(value) || value.length === 0 ||
      !value.every((entry) => typeof entry === "string" && entry.trim().length > 0)) {
    throw new AgentFlowMcpCallError(
      `MCP call ${stepId} outputs must be a non-empty list of artifact paths.`,
      "AGENT_FLOW_MCP_CALL_INVALID"
    );
  }
  const outputs = value.map((entry) => {
    const declared = (entry as string).trim();
    let normalized: string;
    try {
      normalized = normalizeAgentFlowArtifactPath(declared);
    } catch (error) {
      throw new AgentFlowMcpCallError(
        `MCP call ${stepId} output ${declared} must be a normalized static repo-relative artifact path.`,
        "AGENT_FLOW_MCP_CALL_INVALID",
        { cause: error }
      );
    }
    if (declared.includes("{{") || declared.includes("}}") || normalized !== declared) {
      throw new AgentFlowMcpCallError(
        `MCP call ${stepId} output ${declared} must be a normalized static repo-relative artifact path.`,
        "AGENT_FLOW_MCP_CALL_INVALID"
      );
    }
    return normalized;
  });
  if (new Set(outputs).size !== outputs.length) {
    throw new AgentFlowMcpCallError(
      `MCP call ${stepId} outputs must not contain duplicate artifact paths.`,
      "AGENT_FLOW_MCP_CALL_INVALID"
    );
  }
  return outputs;
}

export function resolveAgentFlowMcpArguments(
  value: unknown,
  inputs: Record<string, AgentFlowRunStateValue>,
  stepId: string
): Record<string, AgentFlowRunStateValue> {
  const declaredArguments = mapping(value);
  if (declaredArguments === undefined) {
    throw new AgentFlowMcpCallError(
      `MCP call ${stepId} arguments must be a mapping.`,
      "AGENT_FLOW_MCP_CALL_INVALID"
    );
  }
  validateAgentFlowMcpArgumentExpressions(declaredArguments, stepId);
  const argumentsValue = resolveValue(
    declaredArguments as Record<string, AgentFlowRunStateValue>,
    inputs,
    stepId
  );
  const resolvedArguments = runStateMapping(argumentsValue);
  if (resolvedArguments === undefined) {
    throw new AgentFlowMcpCallError(
      `MCP call ${stepId} arguments must resolve to a mapping.`,
      "AGENT_FLOW_MCP_ARGUMENTS_INVALID"
    );
  }
  return normalizeJsonValue(resolvedArguments, new Set(), 0) as Record<string, AgentFlowRunStateValue>;
}

export function prepareAgentFlowMcpArguments(
  workflow: AgentFlowWorkflow,
  value: unknown,
  inputs: Record<string, AgentFlowRunStateValue>,
  stepId: string
): { value: Record<string, AgentFlowRunStateValue>; redacted: boolean } {
  const sourceArguments = resolveAgentFlowMcpArguments(value, inputs, stepId);
  assertAgentFlowMcpArgumentSize(sourceArguments, stepId, "source");

  const referencedInputNames = collectAgentFlowMcpInputReferences(value);
  const referencedInputs = Object.fromEntries([...referencedInputNames].sort().map((name) => [
    name,
    inputs[name]!
  ]));
  const securedInputs = secureAgentFlowJsonInput(
    workflow,
    `MCP call ${stepId} referenced inputs`,
    referencedInputs
  );
  const securedInputValues = { ...securedInputs.value };
  let referencedInputRedacted = securedInputs.redacted;
  for (const name of referencedInputNames) {
    if (!agentFlowInputKeyLooksSensitive(name)) continue;
    const secured = secureAgentFlowSensitiveJsonInputValue(
      workflow,
      `MCP call ${stepId} referenced input ${JSON.stringify(name)}`,
      inputs[name]!
    );
    securedInputValues[name] = secured.value;
    referencedInputRedacted ||= secured.redacted;
  }
  const argumentsWithSecuredInputs = referencedInputRedacted
    ? resolveAgentFlowMcpArguments(value, { ...inputs, ...securedInputValues }, stepId)
    : sourceArguments;
  const securedArguments = secureAgentFlowJsonInput(
    workflow,
    `MCP call ${stepId} arguments`,
    argumentsWithSecuredInputs
  );
  assertAgentFlowMcpArgumentSize(securedArguments.value, stepId, "secured");
  return {
    value: securedArguments.value,
    redacted: referencedInputRedacted || securedArguments.redacted
  };
}

export function assertAgentFlowMcpArgumentSize(
  value: AgentFlowRunStateValue,
  stepId: string,
  phase: "source" | "secured"
): void {
  if (Buffer.byteLength(stableJson(value)) <= MAX_AGENT_FLOW_MCP_ARGUMENT_BYTES) return;
  const qualifier = phase === "source" ? "source " : "";
  throw new AgentFlowMcpCallError(
    `MCP call ${stepId} ${qualifier}arguments exceed the ${MAX_AGENT_FLOW_MCP_ARGUMENT_BYTES}-byte limit.`,
    "AGENT_FLOW_MCP_ARGUMENTS_TOO_LARGE"
  );
}

export function validateAgentFlowMcpArgumentExpressions(value: unknown, stepId: string): void {
  if (Array.isArray(value)) {
    for (const entry of value) validateAgentFlowMcpArgumentExpressions(entry, stepId);
    return;
  }
  const record = runStateMapping(value);
  if (record !== undefined) {
    for (const entry of Object.values(record)) validateAgentFlowMcpArgumentExpressions(entry, stepId);
    return;
  }
  if (typeof value !== "string") return;
  const remainder = value.replace(/(?<!\{)\{\{\s*inputs\.([A-Za-z_][A-Za-z0-9_-]*)\s*}}(?!})/g, "");
  if (remainder.includes("{{") || remainder.includes("}}")) {
    throw new AgentFlowMcpCallError(
      `MCP call ${stepId} argument contains an unsupported input expression.`,
      "AGENT_FLOW_MCP_ARGUMENT_UNRESOLVED"
    );
  }
}

function collectAgentFlowMcpInputReferences(value: unknown, names: Set<string> = new Set()): Set<string> {
  if (Array.isArray(value)) {
    for (const entry of value) collectAgentFlowMcpInputReferences(entry, names);
    return names;
  }
  const record = runStateMapping(value);
  if (record !== undefined) {
    for (const entry of Object.values(record)) collectAgentFlowMcpInputReferences(entry, names);
    return names;
  }
  if (typeof value !== "string") return names;
  for (const match of value.matchAll(/(?<!\{)\{\{\s*inputs\.([A-Za-z_][A-Za-z0-9_-]*)\s*}}(?!})/g)) {
    names.add(match[1]!);
  }
  return names;
}

function resolveValue(
  value: AgentFlowRunStateValue,
  inputs: Record<string, AgentFlowRunStateValue>,
  stepId: string
): AgentFlowRunStateValue {
  if (Array.isArray(value)) return value.map((entry) => resolveValue(entry, inputs, stepId));
  const record = runStateMapping(value);
  if (record !== undefined) {
    return Object.fromEntries(Object.entries(record).map(([key, entry]) => [key, resolveValue(entry, inputs, stepId)]));
  }
  if (typeof value !== "string") return value;
  const exact = /^\{\{\s*inputs\.([A-Za-z_][A-Za-z0-9_-]*)\s*}}$/.exec(value);
  if (exact !== null) {
    if (!Object.hasOwn(inputs, exact[1]!)) {
      throw new AgentFlowMcpCallError(
        `MCP call ${stepId} argument ${value} references missing persisted input ${exact[1]}.`,
        "AGENT_FLOW_MCP_ARGUMENT_UNRESOLVED"
      );
    }
    return inputs[exact[1]!]!;
  }
  return value.replace(/(?<!\{)\{\{\s*inputs\.([A-Za-z_][A-Za-z0-9_-]*)\s*}}(?!})/g, (_match, name: string) => {
    if (!Object.hasOwn(inputs, name)) {
      throw new AgentFlowMcpCallError(
        `MCP call ${stepId} inline argument reference inputs.${name} is missing from persisted inputs.`,
        "AGENT_FLOW_MCP_ARGUMENT_UNRESOLVED"
      );
    }
    const resolved = inputs[name];
    if (resolved === undefined || typeof resolved === "object") {
      throw new AgentFlowMcpCallError(
        `MCP call ${stepId} inline argument reference inputs.${name} must resolve to a scalar value.`,
        "AGENT_FLOW_MCP_ARGUMENT_UNRESOLVED"
      );
    }
    return String(resolved);
  });
}

function findWorkflowStep(steps: AgentFlowWorkflowStep[], stepId: string): AgentFlowWorkflowStep | undefined {
  for (const step of steps) {
    if (typeof step.id === "string" && step.id.trim() === stepId) return step;
    for (const field of ["branches", "body", "steps"] as const) {
      const nested = step[field];
      if (!Array.isArray(nested)) continue;
      const found = findWorkflowStep(nested.filter(mapping) as AgentFlowWorkflowStep[], stepId);
      if (found !== undefined) return found;
    }
  }
  return undefined;
}

function serializeContent(value: AgentFlowRunStateValue | Uint8Array): Buffer {
  if (value instanceof Uint8Array) return Buffer.from(value);
  if (typeof value === "string") return Buffer.from(value);
  return Buffer.from(`${stableJson(value)}\n`);
}

function contentTypeFor(path: string, value: AgentFlowRunStateValue | Uint8Array): string {
  if (value instanceof Uint8Array) return "application/octet-stream";
  if (typeof value !== "string") return "application/json; charset=utf-8";
  if (path.endsWith(".json")) return "application/json; charset=utf-8";
  if (path.endsWith(".md")) return "text/markdown; charset=utf-8";
  return "text/plain; charset=utf-8";
}

function stableJson(value: AgentFlowRunStateValue): string {
  return JSON.stringify(value, (_key, entry) => {
    if (entry !== null && typeof entry === "object" && !Array.isArray(entry)) {
      return Object.fromEntries(Object.keys(entry).sort().map((key) => [key, entry[key]]));
    }
    return entry;
  });
}

function normalizeJsonValue(value: unknown, ancestors: Set<object>, depth: number): AgentFlowRunStateValue {
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

function mapping(value: unknown): AgentFlowYamlMapping | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value) && !(value instanceof Uint8Array)
    ? value as AgentFlowYamlMapping
    : undefined;
}

function plainObject(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value) || value instanceof Uint8Array) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function runStateMapping(value: unknown): Record<string, AgentFlowRunStateValue> | undefined {
  return plainObject(value) ? value as Record<string, AgentFlowRunStateValue> : undefined;
}

function requiredName(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new AgentFlowMcpCallError(`${label} must be a non-empty string.`, "AGENT_FLOW_MCP_CALL_INVALID");
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
  return redactAgentFlowSensitiveText(error instanceof Error ? error.message : String(error));
}

function sanitizedErrorCause(error: unknown): Error {
  return new Error(errorMessage(error));
}

function agentFlowErrorCode(error: unknown): string | undefined {
  try {
    if (typeof error !== "object" || error === null) return undefined;
    const code = Reflect.get(error, "code");
    return typeof code === "string" && code.startsWith("AGENT_FLOW_") ? code : undefined;
  } catch {
    return undefined;
  }
}
