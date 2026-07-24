import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import {
  AgentflowRunStateError,
  normalizeAgentflowArtifactPath,
  type AgentflowArtifactRecord,
  type AgentflowRunStateStore,
  type AgentflowRunStopStatus,
  type AgentflowRunStateValue
} from "./run_state";
import { evaluateAgentflowPolicy } from "./policy";
import type { AgentflowWorkflow, AgentflowWorkflowStep, AgentflowYamlMapping } from "./workflow";

export const MAX_AGENTFLOW_SESSION_PROMPT_BYTES = 1024 * 1024;
export const MAX_AGENTFLOW_SESSION_INPUT_BYTES = 10 * 1024 * 1024;
export const MAX_AGENTFLOW_SESSION_TOTAL_INPUT_BYTES = 10 * 1024 * 1024;
export const MAX_AGENTFLOW_SESSION_INPUTS = 64;
export const MAX_AGENTFLOW_SESSION_OUTPUT_BYTES = 10 * 1024 * 1024;
export const MAX_AGENTFLOW_SESSION_METADATA_BYTES = 1024 * 1024;

export interface AgentflowSessionRequestArtifact {
  path: string;
  content: Uint8Array;
  contentType: string;
  checksum: string;
}

export interface AgentflowSessionProviderRequest {
  runId: string;
  stepId: string;
  sessionId: string;
  provider: string;
  resume: boolean;
  externalSessionId?: string;
  prompt: { path: string; content: string; checksum: string };
  inputs: AgentflowSessionRequestArtifact[];
  outputs: string[];
  signal: AbortSignal;
}

export interface AgentflowSessionProviderOutput {
  content: string | Uint8Array;
  contentType?: string;
}

export interface AgentflowSessionProviderResponse {
  outputs: Record<string, string | Uint8Array | AgentflowSessionProviderOutput>;
  externalSessionId?: string;
  metadata?: Record<string, AgentflowRunStateValue>;
}

export type AgentflowSessionProviderAdapter = (
  request: AgentflowSessionProviderRequest
) => AgentflowSessionProviderResponse | Promise<AgentflowSessionProviderResponse>;

export interface AgentflowSessionRequestExecutionResult {
  sessionId: string;
  provider: string;
  requestArtifact: AgentflowArtifactRecord;
  outputArtifacts: AgentflowArtifactRecord[];
  externalSessionId?: string;
}

export interface ExecuteAgentflowSessionRequestOptions {
  beforePublish?: () => void;
  stopStatus?: () => AgentflowRunStopStatus | undefined;
}

export class AgentflowSessionRequestError extends Error {
  readonly code: string;

  constructor(message: string, code = "AGENTFLOW_SESSION_REQUEST", options?: ErrorOptions) {
    super(message, options);
    this.name = "AgentflowSessionRequestError";
    this.code = code;
  }
}

export class AgentflowSessionRequestInterruptedError extends AgentflowSessionRequestError {
  constructor(readonly status: AgentflowRunStopStatus) {
    super(`Session request was interrupted because the run was ${status}.`, "AGENTFLOW_SESSION_INTERRUPTED");
  }
}

export class AgentflowSessionPolicyError extends AgentflowSessionRequestError {
  constructor(
    message: string,
    code: string,
    readonly status: "pause" | "fail"
  ) {
    super(message, code);
    this.name = "AgentflowSessionPolicyError";
  }
}

export class AgentflowSessionProviderRegistry {
  private readonly providers = new Map<string, AgentflowSessionProviderAdapter>();

  register(name: string, adapter: AgentflowSessionProviderAdapter): this {
    const normalized = requiredName(name, "Session provider name");
    if (this.providers.has(normalized)) {
      throw new AgentflowSessionRequestError(
        `Session provider ${normalized} is already registered.`,
        "AGENTFLOW_SESSION_PROVIDER_COLLISION"
      );
    }
    this.providers.set(normalized, adapter);
    return this;
  }

  get(name: string): AgentflowSessionProviderAdapter | undefined {
    return this.providers.get(requiredName(name, "Session provider name"));
  }

  names(): string[] {
    return [...this.providers.keys()].sort();
  }
}

export function createAgentflowSessionProviderRegistry(): AgentflowSessionProviderRegistry {
  return new AgentflowSessionProviderRegistry();
}

export function createAgentflowFixtureSessionProvider(
  responses: Record<string, AgentflowSessionProviderResponse>,
  outcomes: Record<string, "succeeded" | "failed" | Array<"succeeded" | "failed">> = {}
): AgentflowSessionProviderAdapter {
  const fixtures = new Map(Object.entries(responses));
  const attempts = new Map<string, number>();
  return (request) => {
    const attemptKey = `${request.runId}\0${request.stepId}`;
    const attempt = attempts.get(attemptKey) ?? 0;
    attempts.set(attemptKey, attempt + 1);
    const declaredOutcome = outcomes[request.stepId];
    const outcome = Array.isArray(declaredOutcome)
      ? declaredOutcome[Math.min(attempt, declaredOutcome.length - 1)]
      : declaredOutcome;
    if (outcome === "failed") {
      throw new AgentflowSessionRequestError(
        `Fixture marks session request step ${request.stepId} as failed on attempt ${attempt + 1}.`,
        "AGENTFLOW_SESSION_FIXTURE_FAILED"
      );
    }
    const response = fixtures.get(request.stepId);
    if (response === undefined) {
      throw new AgentflowSessionRequestError(
        `Fixture session provider has no response for step ${request.stepId}.`,
        "AGENTFLOW_SESSION_FIXTURE_MISSING"
      );
    }
    return response;
  };
}

export async function executeAgentflowSessionRequest(
  store: AgentflowRunStateStore,
  runId: string,
  workflow: AgentflowWorkflow,
  step: AgentflowWorkflowStep,
  registry: AgentflowSessionProviderRegistry,
  options: ExecuteAgentflowSessionRequestOptions = {}
): Promise<AgentflowSessionRequestExecutionResult> {
  const run = store.getRun(runId);
  if (run === null || run.status !== "running") {
    throw new AgentflowSessionRequestError(
      run === null
        ? `Agentflow run ${runId} was not found.`
        : `Agentflow run ${runId} must be running before a session provider can be invoked; current status is ${run.status}.`,
      "AGENTFLOW_SESSION_RUN_STATUS"
    );
  }
  const stepId = requiredName(step.id, "Session request step ID");
  const declaredStep = findWorkflowStep(workflow.steps, stepId);
  if (requiredName(step.type, `Session request ${stepId} type`) !== "session_request"
      || !isDeepStrictEqual(run.context.workflow, workflow)
      || declaredStep === undefined || !isDeepStrictEqual(declaredStep, step)) {
    throw new AgentflowSessionRequestError(
      `Session request ${stepId} must match a step in the workflow persisted for run ${runId}.`,
      "AGENTFLOW_SESSION_WORKFLOW_MISMATCH"
    );
  }
  const sessionId = requiredName(step.session, `Session request ${stepId} session`);
  const session = mapping(workflow.sessions?.[sessionId]);
  if (session === undefined) {
    throw new AgentflowSessionRequestError(
      `Session request ${stepId} references undeclared session ${sessionId}.`,
      "AGENTFLOW_SESSION_UNDECLARED"
    );
  }
  const provider = requiredName(session.provider, `Session ${sessionId} provider`);
  const adapter = registry.get(provider);
  if (adapter === undefined) {
    throw new AgentflowSessionRequestError(
      `No adapter is registered for session provider ${provider}; register it explicitly before running step ${stepId}.`,
      "AGENTFLOW_SESSION_PROVIDER_UNKNOWN"
    );
  }
  const resume = session.resume === true;
  const previous = store.getSession(runId, sessionId);
  const priorExternalSessionId = resume ? previous?.externalSessionId ?? undefined : undefined;
  const prompt = readPrompt(store.repoRoot, requiredName(step.prompt, `Session request ${stepId} prompt`));
  const inputPaths = normalizedArtifactPaths(
    resolveSessionInputPaths(step.inputs, run.inputs, stepId),
    `Session request ${stepId} inputs`
  );
  if (inputPaths.length > MAX_AGENTFLOW_SESSION_INPUTS) {
    throw new AgentflowSessionRequestError(
      `Session request ${stepId} declares ${inputPaths.length} inputs; at most ${MAX_AGENTFLOW_SESSION_INPUTS} are allowed.`,
      "AGENTFLOW_SESSION_INPUT_LIMIT"
    );
  }
  const outputPaths = normalizedArtifactPaths(step.outputs, `Session request ${stepId} outputs`);
  const inputs: AgentflowSessionRequestArtifact[] = [];
  let totalInputBytes = 0;
  for (const inputPath of inputPaths) {
    const input = readInput(store, runId, stepId, inputPath);
    totalInputBytes += input.content.byteLength;
    if (totalInputBytes > MAX_AGENTFLOW_SESSION_TOTAL_INPUT_BYTES) {
      throw new AgentflowSessionRequestError(
        `Session request ${stepId} inputs exceed the ${MAX_AGENTFLOW_SESSION_TOTAL_INPUT_BYTES}-byte aggregate limit.`,
        "AGENTFLOW_SESSION_INPUT_LIMIT"
      );
    }
    inputs.push(input);
  }
  const requestPath = `session-requests/${safePathSegment(stepId).slice(0, 200)}-${digest(stepId).slice(0, 12)}.json`;
  preflightOutputCollisions(store, runId, step, outputPaths, requestPath);
  const request: AgentflowSessionProviderRequest = {
    runId,
    stepId,
    sessionId,
    provider,
    resume,
    ...(priorExternalSessionId === undefined
      ? {}
      : { externalSessionId: priorExternalSessionId }),
    prompt: { ...prompt },
    inputs: inputs.map((input) => ({ ...input, content: Uint8Array.from(input.content) })),
    outputs: [...outputPaths],
    signal: new AbortController().signal
  };

  store.claimSession({
    id: sessionId,
    runId,
    stepId,
    provider,
    status: "running",
    externalSessionId: priorExternalSessionId ?? null,
    state: { resume, lastStepId: stepId }
  });
  try {
    reserveModelCallBudgets(store, runId, workflow, stepId, sessionId, provider);
  } catch (error) {
    store.upsertSession({
      id: sessionId,
      runId,
      stepId,
      provider,
      status: "paused",
      externalSessionId: priorExternalSessionId ?? null,
      state: { resume, lastStepId: stepId, error: errorMessage(error) }
    });
    throw error;
  }

  let response: AgentflowSessionProviderResponse;
  let effectiveExternalSessionId = priorExternalSessionId;
  try {
    response = await invokeProvider(adapter, request, options.stopStatus);
  } catch (error) {
    const stopped = error instanceof AgentflowSessionRequestInterruptedError
      ? error.status
      : options.stopStatus?.();
    if (stopped !== undefined) {
      store.upsertSession({
        id: sessionId,
        runId,
        stepId,
        provider,
        status: stopped,
        externalSessionId: priorExternalSessionId ?? null,
        state: { resume, lastStepId: stepId, interrupted: stopped }
      });
      throw error instanceof AgentflowSessionRequestInterruptedError
        ? error
        : new AgentflowSessionRequestInterruptedError(stopped);
    }
    store.upsertSession({
      id: sessionId,
      runId,
      stepId,
      provider,
      status: "paused",
      externalSessionId: priorExternalSessionId ?? null,
      state: { resume, lastStepId: stepId, error: errorMessage(error) }
    });
    if (error instanceof AgentflowSessionRequestError) throw error;
    throw new AgentflowSessionRequestError(
      `Session provider ${provider} failed for step ${stepId}: ${errorMessage(error)}`,
      "AGENTFLOW_SESSION_PROVIDER_FAILED",
      { cause: error }
    );
  }

  try {
  const returnedExternalSessionId = optionalName(
    response.externalSessionId,
    `Session provider external session ID for step ${stepId}`
  );
  const externalSessionId = returnedExternalSessionId ?? priorExternalSessionId;
  effectiveExternalSessionId = externalSessionId;
  store.upsertSession({
    id: sessionId,
    runId,
    stepId,
    provider,
    status: "running",
    externalSessionId: externalSessionId ?? null,
    state: { resume, lastStepId: stepId, providerResponded: true }
  });
  const outputs = validateResponse(stepId, outputPaths, response);
  const providerMetadata = validateProviderMetadata(stepId, response.metadata);
  options.beforePublish?.();

  const requestMetadata = {
    stepId,
    sessionId,
    provider,
    resume,
    prompt: { path: prompt.path, checksum: prompt.checksum },
    inputs: inputs.map((input) => ({ path: input.path, checksum: input.checksum, contentType: input.contentType })),
    outputs: outputPaths,
    ...(externalSessionId === undefined ? {} : { externalSessionId }),
    ...(providerMetadata === undefined ? {} : { providerMetadata })
  };
  const requestArtifact = store.writeArtifact({
    id: `session-request:${digest(requestPath)}`,
    runId,
    stepId,
    path: requestPath,
    kind: "session_request",
    contentType: "application/json; charset=utf-8",
    content: `${stableJson(requestMetadata)}\n`,
    overwrite: store.getArtifact(runId, requestPath) !== null,
    requiredRunStatus: "running",
    requiredArtifacts: inputs.map((input) => ({ path: input.path, checksum: input.checksum })),
    metadata: { sessionId, provider, resume }
  });
  const inputPathSet = new Set(inputPaths);
  const publicationOrder = [
    ...outputPaths.filter((outputPath) => !inputPathSet.has(outputPath)),
    ...outputPaths.filter((outputPath) => inputPathSet.has(outputPath))
  ];
  const overwrittenInputs = new Set<string>();
  const publications = publicationOrder.map((outputPath) => {
    const output = outputs.get(outputPath)!;
    const existing = store.getArtifact(runId, outputPath);
    const publication = {
      id: existing?.id ?? `session-output:${digest(outputPath)}`,
      runId,
      stepId,
      path: outputPath,
      kind: "session_output",
      contentType: output.contentType ?? contentTypeFor(outputPath),
      content: output.content,
      overwrite: step.overwrite === true || ownedSessionOutput(existing, stepId, sessionId),
      requiredRunStatus: "running" as const,
      requiredArtifacts: inputs
        .filter((input) => !overwrittenInputs.has(input.path))
        .map((input) => ({ path: input.path, checksum: input.checksum })),
      metadata: { sessionId, provider, requestArtifact: requestPath }
    };
    if (inputPathSet.has(outputPath)) overwrittenInputs.add(outputPath);
    return publication;
  });
  const published = new Map(store.writeArtifactsAtomically(publications)
    .map((artifact) => [artifact.declaredPath, artifact]));
  const outputArtifacts = outputPaths.map((outputPath) => published.get(outputPath)!);
  options.beforePublish?.();
  store.upsertSession({
    id: sessionId,
    runId,
    stepId,
    provider,
    status: "waiting",
    externalSessionId: externalSessionId ?? null,
    state: {
      resume,
      lastStepId: stepId,
      requestArtifact: requestPath,
      outputArtifacts: outputPaths
    }
  });
  return {
    sessionId,
    provider,
    requestArtifact,
    outputArtifacts,
    ...(externalSessionId === undefined ? {} : { externalSessionId })
  };
  } catch (error) {
    const stopped = error instanceof AgentflowSessionRequestInterruptedError
      ? error.status
      : options.stopStatus?.();
    store.upsertSession({
      id: sessionId,
      runId,
      stepId,
      provider,
      status: stopped ?? "paused",
      externalSessionId: effectiveExternalSessionId ?? null,
      state: stopped === undefined
        ? { resume, lastStepId: stepId, error: errorMessage(error) }
        : { resume, lastStepId: stepId, interrupted: stopped }
    });
    if (stopped !== undefined && !(error instanceof AgentflowSessionRequestInterruptedError)) {
      throw new AgentflowSessionRequestInterruptedError(stopped);
    }
    throw error;
  }
}

async function invokeProvider(
  adapter: AgentflowSessionProviderAdapter,
  request: AgentflowSessionProviderRequest,
  stopStatus: ExecuteAgentflowSessionRequestOptions["stopStatus"]
): Promise<AgentflowSessionProviderResponse> {
  const initialStatus = stopStatus?.();
  if (initialStatus !== undefined) throw new AgentflowSessionRequestInterruptedError(initialStatus);
  if (stopStatus === undefined) return adapter(request);

  const controller = new AbortController();
  request.signal = controller.signal;
  let timer: ReturnType<typeof setInterval> | undefined;
  const interrupted = new Promise<never>((_resolve, reject) => {
    timer = setInterval(() => {
      const status = stopStatus();
      if (status === undefined) return;
      controller.abort(new AgentflowSessionRequestInterruptedError(status));
      reject(new AgentflowSessionRequestInterruptedError(status));
    }, 25);
  });
  try {
    return await Promise.race([Promise.resolve(adapter(request)), interrupted]);
  } finally {
    if (timer !== undefined) clearInterval(timer);
  }
}

function reserveModelCallBudgets(
  store: AgentflowRunStateStore,
  runId: string,
  workflow: AgentflowWorkflow,
  stepId: string,
  sessionId: string,
  provider: string
): void {
  const kinds = ["model_calls", ...(provider === "frontier" ? ["frontier_calls"] : [])];
  const usage = Object.fromEntries(kinds.map((kind) => [kind, store.getBudget(runId, `model:${kind}`)?.used ?? 0]));
  const decision = evaluateAgentflowPolicy(workflow, { kind: "model_usage", session: sessionId, usage });
  if (decision.status !== "allow") {
    throw new AgentflowSessionPolicyError(decision.message, decision.code, decision.status);
  }
  const limits = mapping(workflow.limits);
  store.reserveBudgets(kinds.flatMap((kind) => {
    const limit = limits?.[`max_${kind}`];
    if (typeof limit !== "number" || !Number.isFinite(limit) || limit <= 0) return [];
    return [{
      id: `model:${kind}`,
      runId,
      stepId,
      sessionId,
      scope: "workflow",
      kind,
      limit,
      amount: 1,
      unit: "calls"
    }];
  }));
}

function readPrompt(repoRoot: string, declaredPath: string): AgentflowSessionProviderRequest["prompt"] {
  const resolved = resolveRepoFile(repoRoot, declaredPath);
  let descriptor: number;
  try {
    descriptor = fs.openSync(resolved, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
  } catch (error) {
    throw new AgentflowSessionRequestError(
      `Could not read prompt ${declaredPath}: ${errorMessage(error)}`,
      "AGENTFLOW_SESSION_PROMPT_MISSING",
      { cause: error }
    );
  }
  let content: Buffer;
  try {
    const stat = fs.fstatSync(descriptor);
    if (!stat.isFile()) {
      throw new AgentflowSessionRequestError(
        `Prompt ${declaredPath} must be a regular file.`,
        "AGENTFLOW_SESSION_PROMPT_PATH"
      );
    }
    if (stat.size > MAX_AGENTFLOW_SESSION_PROMPT_BYTES) throw promptTooLarge(declaredPath);
    const bounded = Buffer.allocUnsafe(MAX_AGENTFLOW_SESSION_PROMPT_BYTES + 1);
    let size = 0;
    while (size < bounded.byteLength) {
      const read = fs.readSync(descriptor, bounded, size, bounded.byteLength - size, null);
      if (read === 0) break;
      size += read;
    }
    if (size > MAX_AGENTFLOW_SESSION_PROMPT_BYTES) throw promptTooLarge(declaredPath);
    content = bounded.subarray(0, size);
  } catch (error) {
    if (error instanceof AgentflowSessionRequestError) throw error;
    throw new AgentflowSessionRequestError(
      `Could not read prompt ${declaredPath}: ${errorMessage(error)}`,
      "AGENTFLOW_SESSION_PROMPT_MISSING",
      { cause: error }
    );
  } finally {
    fs.closeSync(descriptor);
  }
  return { path: declaredPath, content: content.toString("utf8"), checksum: `sha256:${digest(content)}` };
}

function promptTooLarge(declaredPath: string): AgentflowSessionRequestError {
  return new AgentflowSessionRequestError(
    `Prompt ${declaredPath} exceeds the ${MAX_AGENTFLOW_SESSION_PROMPT_BYTES}-byte session prompt limit.`,
    "AGENTFLOW_SESSION_PROMPT_TOO_LARGE"
  );
}

function readInput(
  store: AgentflowRunStateStore,
  runId: string,
  stepId: string,
  inputPath: string
): AgentflowSessionRequestArtifact {
  try {
    const input = store.readArtifact(runId, inputPath, { maxBytes: MAX_AGENTFLOW_SESSION_INPUT_BYTES });
    return {
      path: inputPath,
      content: input.content,
      contentType: input.artifact.contentType,
      checksum: input.artifact.checksum!
    };
  } catch (error) {
    if (error instanceof AgentflowRunStateError) {
      throw new AgentflowSessionRequestError(
        `Could not read bounded input ${inputPath} for session request ${stepId}: ${error.message}`,
        "AGENTFLOW_SESSION_INPUT",
        { cause: error }
      );
    }
    throw error;
  }
}

function validateResponse(
  stepId: string,
  outputPaths: string[],
  response: AgentflowSessionProviderResponse
): Map<string, AgentflowSessionProviderOutput> {
  if (response === null || typeof response !== "object" || Array.isArray(response) || !mapping(response.outputs)) {
    throw new AgentflowSessionRequestError(
      `Session provider response for step ${stepId} must contain an outputs mapping.`,
      "AGENTFLOW_SESSION_OUTPUT_INVALID"
    );
  }
  const declared = new Set(outputPaths);
  const actual = Object.keys(response.outputs);
  const missing = outputPaths.find((output) => !Object.hasOwn(response.outputs, output));
  const extra = actual.find((output) => !declared.has(output));
  if (missing !== undefined || extra !== undefined) {
    throw new AgentflowSessionRequestError(
      missing !== undefined
        ? `Session provider response for step ${stepId} is missing declared output ${missing}.`
        : `Session provider response for step ${stepId} returned undeclared output ${extra}.`,
      "AGENTFLOW_SESSION_OUTPUT_INVALID"
    );
  }
  let totalBytes = 0;
  return new Map(outputPaths.map((outputPath) => {
    const value = response.outputs[outputPath];
    if (typeof value === "string" || value instanceof Uint8Array) {
      const size = Buffer.byteLength(value);
      totalBytes += size;
      validateOutputSize(stepId, outputPath, size, totalBytes);
      return [outputPath, { content: value }];
    }
    if (!mapping(value) || !(typeof value.content === "string" || value.content instanceof Uint8Array)) {
      throw new AgentflowSessionRequestError(
        `Session provider output ${outputPath} for step ${stepId} must contain string or binary content.`,
        "AGENTFLOW_SESSION_OUTPUT_INVALID"
      );
    }
    const size = Buffer.byteLength(value.content);
    totalBytes += size;
    validateOutputSize(stepId, outputPath, size, totalBytes);
    return [outputPath, {
      content: value.content,
      ...(typeof value.contentType === "string" && value.contentType.trim().length > 0
        ? { contentType: value.contentType.trim() }
        : {})
    }];
  }));
}

function validateOutputSize(stepId: string, outputPath: string, size: number, totalBytes: number): void {
  if (size > MAX_AGENTFLOW_SESSION_OUTPUT_BYTES) {
    throw new AgentflowSessionRequestError(
      `Session provider output ${outputPath} for step ${stepId} exceeds the ${MAX_AGENTFLOW_SESSION_OUTPUT_BYTES}-byte limit.`,
      "AGENTFLOW_SESSION_OUTPUT_TOO_LARGE"
    );
  }
  if (totalBytes > MAX_AGENTFLOW_SESSION_OUTPUT_BYTES) {
    throw new AgentflowSessionRequestError(
      `Session provider outputs for step ${stepId} exceed the ${MAX_AGENTFLOW_SESSION_OUTPUT_BYTES}-byte aggregate limit.`,
      "AGENTFLOW_SESSION_OUTPUT_TOO_LARGE"
    );
  }
}

function preflightOutputCollisions(
  store: AgentflowRunStateStore,
  runId: string,
  step: AgentflowWorkflowStep,
  outputPaths: string[],
  requestPath: string
): void {
  if (outputPaths.includes(requestPath)) {
    throw new AgentflowSessionRequestError(
      `Session output ${requestPath} conflicts with the runtime request metadata artifact.`,
      "AGENTFLOW_SESSION_OUTPUT_COLLISION"
    );
  }
  const requestArtifact = store.getArtifact(runId, requestPath);
  if (requestArtifact !== null &&
      (requestArtifact.kind !== "session_request" || requestArtifact.id !== `session-request:${digest(requestPath)}`)) {
    throw new AgentflowSessionRequestError(
      `Session request metadata path ${requestPath} is already owned by another artifact.`,
      "AGENTFLOW_SESSION_OUTPUT_COLLISION"
    );
  }
  if (step.overwrite === true) return;
  const collision = outputPaths.find((outputPath) => {
    const existing = store.getArtifact(runId, outputPath);
    return existing !== null && !ownedSessionOutput(existing, requiredName(step.id, "Session request step ID"), requiredName(step.session, "Session request session ID"));
  });
  if (collision !== undefined) {
    throw new AgentflowSessionRequestError(
      `Session output ${collision} already exists; declare overwrite: true to replace it.`,
      "AGENTFLOW_SESSION_OUTPUT_COLLISION"
    );
  }
}

function ownedSessionOutput(
  artifact: AgentflowArtifactRecord | null,
  stepId: string,
  sessionId: string
): boolean {
  return artifact?.kind === "session_output"
    && artifact.producerStepId === stepId
    && artifact.metadata.sessionId === sessionId;
}

function resolveRepoFile(repoRoot: string, declaredPath: string): string {
  if (declaredPath.trim() !== declaredPath || declaredPath.includes("\\")
      || path.posix.isAbsolute(declaredPath) || path.win32.isAbsolute(declaredPath)) {
    throw new AgentflowSessionRequestError(
      `Prompt path ${JSON.stringify(declaredPath)} must be a normalized repo-relative path.`,
      "AGENTFLOW_SESSION_PROMPT_PATH"
    );
  }
  const normalized = path.posix.normalize(declaredPath);
  if (normalized !== declaredPath || normalized === ".." || normalized.startsWith("../")) {
    throw new AgentflowSessionRequestError(
      `Prompt path ${JSON.stringify(declaredPath)} must stay inside the repository.`,
      "AGENTFLOW_SESSION_PROMPT_PATH"
    );
  }
  const resolved = path.resolve(repoRoot, ...normalized.split("/"));
  let real: string;
  try {
    real = fs.realpathSync(resolved);
  } catch {
    real = resolved;
  }
  const relative = path.relative(repoRoot, real);
  if (relative === ".." || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative)) {
    throw new AgentflowSessionRequestError(
      `Prompt path ${JSON.stringify(declaredPath)} must stay inside the repository.`,
      "AGENTFLOW_SESSION_PROMPT_PATH"
    );
  }
  return real;
}

function requiredStringList(value: unknown, label: string): string[] {
  if (!Array.isArray(value) || value.length === 0 || !value.every((entry) => typeof entry === "string" && entry.trim().length > 0)) {
    throw new AgentflowSessionRequestError(`${label} must be a non-empty list of artifact paths.`, "AGENTFLOW_SESSION_REQUEST_INVALID");
  }
  const normalized = (value as string[]).map((entry) => entry.trim());
  if (new Set(normalized).size !== normalized.length) {
    throw new AgentflowSessionRequestError(`${label} must not contain duplicate artifact paths.`, "AGENTFLOW_SESSION_REQUEST_INVALID");
  }
  return normalized;
}

function normalizedArtifactPaths(value: unknown, label: string): string[] {
  const paths = requiredStringList(value, label).map((artifactPath) => {
    try {
      return normalizeAgentflowArtifactPath(artifactPath);
    } catch (error) {
      throw new AgentflowSessionRequestError(
        `${label} contains invalid artifact path ${JSON.stringify(artifactPath)}: ${errorMessage(error)}`,
        "AGENTFLOW_SESSION_REQUEST_INVALID",
        { cause: error }
      );
    }
  });
  if (new Set(paths).size !== paths.length) {
    throw new AgentflowSessionRequestError(
      `${label} must not contain paths that resolve to the same canonical artifact.`,
      "AGENTFLOW_SESSION_REQUEST_INVALID"
    );
  }
  return paths;
}

function resolveSessionInputPaths(
  value: unknown,
  runInputs: Record<string, AgentflowRunStateValue>,
  stepId: string
): unknown {
  if (!Array.isArray(value)) return value;
  return value.map((entry) => {
    if (typeof entry !== "string") return entry;
    const reference = /^\{\{\s*inputs\.([A-Za-z0-9_-]+)\s*}}$/.exec(entry.trim());
    if (reference === null) return entry;
    const resolved = runInputs[reference[1]!];
    if (typeof resolved !== "string" || resolved.trim().length === 0) {
      throw new AgentflowSessionRequestError(
        `Session request ${stepId} input ${entry.trim()} must resolve to a non-empty artifact path in persisted run inputs.`,
        "AGENTFLOW_SESSION_INPUT_UNRESOLVED"
      );
    }
    return resolved;
  });
}

function requiredName(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new AgentflowSessionRequestError(`${label} must be a non-empty string.`, "AGENTFLOW_SESSION_REQUEST_INVALID");
  }
  return value.trim();
}

function optionalName(value: unknown, label: string): string | undefined {
  if (value === undefined) return undefined;
  return requiredName(value, label);
}

function mapping(value: unknown): AgentflowYamlMapping | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as AgentflowYamlMapping
    : undefined;
}

function findWorkflowStep(steps: AgentflowWorkflowStep[], stepId: string): AgentflowWorkflowStep | undefined {
  for (const step of steps) {
    if (typeof step.id === "string" && step.id.trim() === stepId) return step;
    for (const field of ["body", "steps"] as const) {
      const nested = Array.isArray(step[field])
        ? (step[field] as unknown[]).filter((entry): entry is AgentflowWorkflowStep => mapping(entry) !== undefined)
        : [];
      const found = findWorkflowStep(nested, stepId);
      if (found !== undefined) return found;
    }
    if (Array.isArray(step.branches)) {
      for (const branch of step.branches) {
        const branchMapping = mapping(branch);
        if (branchMapping === undefined) continue;
        for (const field of ["body", "steps"] as const) {
          const nested = Array.isArray(branchMapping[field])
            ? (branchMapping[field] as unknown[]).filter((entry): entry is AgentflowWorkflowStep => mapping(entry) !== undefined)
            : [];
          const found = findWorkflowStep(nested, stepId);
          if (found !== undefined) return found;
        }
      }
    }
  }
  return undefined;
}

function contentTypeFor(outputPath: string): string {
  if (outputPath.endsWith(".json")) return "application/json; charset=utf-8";
  if (outputPath.endsWith(".md")) return "text/markdown; charset=utf-8";
  return "text/plain; charset=utf-8";
}

function safePathSegment(value: string): string {
  return value.replace(/[^A-Za-z0-9._-]/g, "_");
}

function digest(value: string | Uint8Array): string {
  return createHash("sha256").update(value).digest("hex");
}

function stableJson(value: unknown): string {
  return JSON.stringify(sortJson(value));
}

function validateProviderMetadata(
  stepId: string,
  metadata: Record<string, unknown> | undefined
): Record<string, AgentflowRunStateValue> | undefined {
  if (metadata === undefined) return undefined;
  const ancestors = new Set<object>();
  const validate = (value: unknown, depth: number): void => {
    if (depth > 50) throw new Error("metadata nesting exceeds 50 levels");
    if (value === null || typeof value === "string" || typeof value === "boolean") return;
    if (typeof value === "number" && Number.isFinite(value)) return;
    if (typeof value !== "object") throw new Error(`unsupported ${typeof value} value`);
    if (ancestors.has(value)) throw new Error("metadata contains a cycle");
    const prototype = Object.getPrototypeOf(value);
    if (!Array.isArray(value) && prototype !== Object.prototype && prototype !== null) {
      throw new Error("metadata objects must be plain objects");
    }
    ancestors.add(value);
    for (const entry of Array.isArray(value) ? value : Object.values(value)) validate(entry, depth + 1);
    ancestors.delete(value);
  };
  try {
    if (metadata === null || typeof metadata !== "object" || Array.isArray(metadata)
        || ![Object.prototype, null].includes(Object.getPrototypeOf(metadata))) {
      throw new Error("metadata must be a plain object");
    }
    validate(metadata, 0);
    const serialized = stableJson(metadata);
    if (Buffer.byteLength(serialized) > MAX_AGENTFLOW_SESSION_METADATA_BYTES) {
      throw new Error(`metadata exceeds ${MAX_AGENTFLOW_SESSION_METADATA_BYTES} bytes`);
    }
  } catch (error) {
    throw new AgentflowSessionRequestError(
      `Session provider metadata for step ${stepId} is invalid: ${errorMessage(error)}.`,
      "AGENTFLOW_SESSION_METADATA_INVALID",
      { cause: error }
    );
  }
  return metadata as Record<string, AgentflowRunStateValue>;
}

function sortJson(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sortJson);
  if (value === null || typeof value !== "object") return value;
  return Object.fromEntries(Object.keys(value).sort().map((key) => [key, sortJson((value as Record<string, unknown>)[key])]));
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
