import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import {
  AgentFlowRunStateError,
  normalizeAgentFlowArtifactPath,
  type AgentFlowArtifactRecord,
  type AgentFlowRunStateStore,
  type AgentFlowRunStopStatus,
  type AgentFlowRunStateValue
} from "./run_state";
import { evaluateAgentFlowPolicy } from "./policy";
import type { AgentFlowWorkflow, AgentFlowWorkflowStep, AgentFlowYamlMapping } from "./workflow";
import { createAgentFlowReviewPrompt, parseAgentFlowReviewResult } from "./review";

export const MAX_AGENT_FLOW_SESSION_PROMPT_BYTES = 1024 * 1024;
export const MAX_AGENT_FLOW_SESSION_INPUT_BYTES = 10 * 1024 * 1024;
export const MAX_AGENT_FLOW_SESSION_TOTAL_INPUT_BYTES = 10 * 1024 * 1024;
export const MAX_AGENT_FLOW_SESSION_INPUTS = 64;
export const MAX_AGENT_FLOW_SESSION_OUTPUT_BYTES = 10 * 1024 * 1024;
export const MAX_AGENT_FLOW_SESSION_METADATA_BYTES = 1024 * 1024;

export interface AgentFlowSessionRequestArtifact {
  path: string;
  content: Uint8Array;
  contentType: string;
  checksum: string;
}

export interface AgentFlowSessionProviderRequest {
  runId: string;
  stepId: string;
  sessionId: string;
  provider: string;
  resume: boolean;
  externalSessionId?: string;
  prompt: { path: string; content: string; checksum: string };
  inputs: AgentFlowSessionRequestArtifact[];
  outputs: string[];
  signal: AbortSignal;
}

export interface AgentFlowSessionProviderOutput {
  content: string | Uint8Array;
  contentType?: string;
}

export interface AgentFlowSessionProviderResponse {
  outputs: Record<string, string | Uint8Array | AgentFlowSessionProviderOutput>;
  externalSessionId?: string;
  metadata?: Record<string, AgentFlowRunStateValue>;
}

export type AgentFlowSessionProviderAdapter = (
  request: AgentFlowSessionProviderRequest
) => AgentFlowSessionProviderResponse | Promise<AgentFlowSessionProviderResponse>;

export interface AgentFlowSessionRequestExecutionResult {
  sessionId: string;
  provider: string;
  requestArtifact: AgentFlowArtifactRecord;
  outputArtifacts: AgentFlowArtifactRecord[];
  externalSessionId?: string;
}

export interface ExecuteAgentFlowSessionRequestOptions {
  attempt?: number;
  beforePublish?: () => void;
  stopStatus?: () => AgentFlowRunStopStatus | undefined;
}

interface ExecuteAgentFlowSessionStepOptions extends ExecuteAgentFlowSessionRequestOptions {
  review?: true;
}

export class AgentFlowSessionRequestError extends Error {
  readonly code: string;

  constructor(message: string, code = "AGENT_FLOW_SESSION_REQUEST", options?: ErrorOptions) {
    super(message, options);
    this.name = "AgentFlowSessionRequestError";
    this.code = code;
  }
}

export class AgentFlowSessionRequestInterruptedError extends AgentFlowSessionRequestError {
  constructor(readonly status: AgentFlowRunStopStatus) {
    super(`Session request was interrupted because the run was ${status}.`, "AGENT_FLOW_SESSION_INTERRUPTED");
  }
}

export class AgentFlowSessionPolicyError extends AgentFlowSessionRequestError {
  constructor(
    message: string,
    code: string,
    readonly status: "pause" | "fail"
  ) {
    super(message, code);
    this.name = "AgentFlowSessionPolicyError";
  }
}

export class AgentFlowSessionProviderRegistry {
  private readonly providers = new Map<string, AgentFlowSessionProviderAdapter>();

  register(name: string, adapter: AgentFlowSessionProviderAdapter): this {
    const normalized = requiredName(name, "Session provider name");
    if (this.providers.has(normalized)) {
      throw new AgentFlowSessionRequestError(
        `Session provider ${normalized} is already registered.`,
        "AGENT_FLOW_SESSION_PROVIDER_COLLISION"
      );
    }
    this.providers.set(normalized, adapter);
    return this;
  }

  get(name: string): AgentFlowSessionProviderAdapter | undefined {
    return this.providers.get(requiredName(name, "Session provider name"));
  }

  names(): string[] {
    return [...this.providers.keys()].sort();
  }
}

export function createAgentFlowSessionProviderRegistry(): AgentFlowSessionProviderRegistry {
  return new AgentFlowSessionProviderRegistry();
}

export function createAgentFlowFixtureSessionProvider(
  responses: Record<string, AgentFlowSessionProviderResponse>,
  outcomes: Record<string, "succeeded" | "failed" | Array<"succeeded" | "failed">> = {}
): AgentFlowSessionProviderAdapter {
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
      throw new AgentFlowSessionRequestError(
        `Fixture marks session request step ${request.stepId} as failed on attempt ${attempt + 1}.`,
        "AGENT_FLOW_SESSION_FIXTURE_FAILED"
      );
    }
    const response = fixtures.get(request.stepId);
    if (response === undefined) {
      throw new AgentFlowSessionRequestError(
        `Fixture session provider has no response for step ${request.stepId}.`,
        "AGENT_FLOW_SESSION_FIXTURE_MISSING"
      );
    }
    return response;
  };
}

export async function executeAgentFlowSessionRequest(
  store: AgentFlowRunStateStore,
  runId: string,
  workflow: AgentFlowWorkflow,
  step: AgentFlowWorkflowStep,
  registry: AgentFlowSessionProviderRegistry,
  options: ExecuteAgentFlowSessionRequestOptions = {}
): Promise<AgentFlowSessionRequestExecutionResult> {
  return executeAgentFlowSessionStep(store, runId, workflow, step, registry, options);
}

export async function executeAgentFlowReview(
  store: AgentFlowRunStateStore,
  runId: string,
  workflow: AgentFlowWorkflow,
  step: AgentFlowWorkflowStep,
  registry: AgentFlowSessionProviderRegistry,
  options: ExecuteAgentFlowSessionRequestOptions = {}
): Promise<AgentFlowSessionRequestExecutionResult> {
  return executeAgentFlowSessionStep(store, runId, workflow, step, registry, { ...options, review: true });
}

async function executeAgentFlowSessionStep(
  store: AgentFlowRunStateStore,
  runId: string,
  workflow: AgentFlowWorkflow,
  step: AgentFlowWorkflowStep,
  registry: AgentFlowSessionProviderRegistry,
  options: ExecuteAgentFlowSessionStepOptions
): Promise<AgentFlowSessionRequestExecutionResult> {
  const run = store.getRun(runId);
  if (run === null || run.status !== "running") {
    throw new AgentFlowSessionRequestError(
      run === null
        ? `Agent Flow run ${runId} was not found.`
        : `Agent Flow run ${runId} must be running before a session provider can be invoked; current status is ${run.status}.`,
      "AGENT_FLOW_SESSION_RUN_STATUS"
    );
  }
  const kind = options.review === true ? "review" : "session_request";
  const requestKind = options.review === true ? "review_request" : "session_request";
  const outputKind = options.review === true ? "review_output" : "session_output";
  const requestIdPrefix = options.review === true ? "review-request" : "session-request";
  const label = options.review === true ? "Review" : "Session request";
  const stepId = requiredName(step.id, `${label} step ID`);
  const declaredStep = findWorkflowStep(workflow.steps, stepId);
  if (requiredName(step.type, `${label} ${stepId} type`) !== kind
      || !isDeepStrictEqual(run.context.workflow, workflow)
      || declaredStep === undefined || !isDeepStrictEqual(declaredStep, step)) {
    throw new AgentFlowSessionRequestError(
      `${label} ${stepId} must match a step in the workflow persisted for run ${runId}.`,
      "AGENT_FLOW_SESSION_WORKFLOW_MISMATCH"
    );
  }
  const sessionId = requiredName(options.review === true ? step.reviewer : step.session, `${label} ${stepId} session`);
  const session = mapping(workflow.sessions?.[sessionId]);
  if (session === undefined) {
    throw new AgentFlowSessionRequestError(
      `${label} ${stepId} references undeclared session ${sessionId}.`,
      "AGENT_FLOW_SESSION_UNDECLARED"
    );
  }
  const provider = requiredName(session.provider, `Session ${sessionId} provider`);
  const adapter = registry.get(provider);
  if (adapter === undefined) {
    throw new AgentFlowSessionRequestError(
      `No adapter is registered for session provider ${provider}; register it explicitly before running step ${stepId}.`,
      "AGENT_FLOW_SESSION_PROVIDER_UNKNOWN"
    );
  }
  const resume = session.resume === true;
  const previous = store.getSession(runId, sessionId);
  const priorExternalSessionId = resume ? previous?.externalSessionId ?? undefined : undefined;
  const rawInputs = options.review === true ? step.artifacts : step.inputs;
  const rawOutputs = step.outputs;
  const outputPaths = normalizedArtifactPaths(rawOutputs, `${label} ${stepId} outputs`);
  const prompt = options.review === true
    ? createAgentFlowReviewPrompt(
      stepId,
      sessionId,
      requiredName(step.subject, `Review ${stepId} subject`),
      normalizedArtifactPaths(rawInputs, `Review ${stepId} artifacts`),
      outputPaths
    )
    : readAgentFlowSessionPrompt(store.repoRoot, requiredName(step.prompt, `Session request ${stepId} prompt`));
  const inputPaths = normalizedArtifactPaths(
    options.review === true ? rawInputs : resolveSessionInputPaths(rawInputs, run.inputs, stepId),
    `${label} ${stepId} ${options.review === true ? "artifacts" : "inputs"}`
  );
  if (inputPaths.length > MAX_AGENT_FLOW_SESSION_INPUTS) {
    throw new AgentFlowSessionRequestError(
      `${label} ${stepId} declares ${inputPaths.length} inputs; at most ${MAX_AGENT_FLOW_SESSION_INPUTS} are allowed.`,
      "AGENT_FLOW_SESSION_INPUT_LIMIT"
    );
  }
  const inputs: AgentFlowSessionRequestArtifact[] = [];
  let totalInputBytes = 0;
  for (const inputPath of inputPaths) {
    const input = readAgentFlowSessionInput(store, runId, stepId, inputPath);
    totalInputBytes += input.content.byteLength;
    if (totalInputBytes > MAX_AGENT_FLOW_SESSION_TOTAL_INPUT_BYTES) {
      throw new AgentFlowSessionRequestError(
        `Session request ${stepId} inputs exceed the ${MAX_AGENT_FLOW_SESSION_TOTAL_INPUT_BYTES}-byte aggregate limit.`,
        "AGENT_FLOW_SESSION_INPUT_LIMIT"
      );
    }
    inputs.push(input);
  }
  const requestDirectory = options.review === true ? "review-requests" : "session-requests";
  const requestPath = `${requestDirectory}/${safePathSegment(stepId).slice(0, 200)}-${digest(stepId).slice(0, 12)}.json`;
  preflightOutputCollisions(store, runId, step, sessionId, outputPaths, requestPath, requestKind, outputKind, requestIdPrefix);
  const request: AgentFlowSessionProviderRequest = {
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
    reserveAgentFlowSessionModelCallBudgets(store, runId, workflow, stepId, sessionId, provider);
  } catch (error) {
    const status = error instanceof AgentFlowSessionPolicyError && error.status === "fail" ? "failed" : "paused";
    store.upsertSession({
      id: sessionId,
      runId,
      stepId,
      provider,
      status,
      externalSessionId: priorExternalSessionId ?? null,
      state: { resume, lastStepId: stepId, error: errorMessage(error) }
    });
    throw error;
  }

  let response: AgentFlowSessionProviderResponse;
  let effectiveExternalSessionId = priorExternalSessionId;
  try {
    response = await invokeAgentFlowSessionProvider(adapter, request, options.stopStatus);
  } catch (error) {
    const stopped = error instanceof AgentFlowSessionRequestInterruptedError
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
      throw error instanceof AgentFlowSessionRequestInterruptedError
        ? error
        : new AgentFlowSessionRequestInterruptedError(stopped);
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
    if (error instanceof AgentFlowSessionRequestError) throw error;
    throw new AgentFlowSessionRequestError(
      `Session provider ${provider} failed for step ${stepId}: ${errorMessage(error)}`,
      "AGENT_FLOW_SESSION_PROVIDER_FAILED",
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
  const outputs = validateAgentFlowSessionProviderResponse(stepId, outputPaths, response);
  if (options.review === true) {
    for (const outputPath of outputPaths) parseAgentFlowReviewResult(outputs.get(outputPath)!, outputPath);
  }
  const providerMetadata = validateAgentFlowSessionProviderMetadata(stepId, response.metadata);
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
    id: `${requestIdPrefix}:${digest(requestPath)}`,
    runId,
    stepId,
    path: requestPath,
    kind: requestKind,
    contentType: "application/json; charset=utf-8",
    content: `${stableJson(requestMetadata)}\n`,
    overwrite: store.getArtifact(runId, requestPath) !== null,
    requiredRunStatus: "running",
    requiredArtifacts: inputs.map((input) => ({ path: input.path, checksum: input.checksum })),
    metadata: {
      sessionId,
      provider,
      resume,
      ...(options.attempt === undefined ? {} : { attempt: options.attempt })
    }
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
      kind: outputKind,
      contentType: output.contentType ?? contentTypeFor(outputPath),
      content: output.content,
      overwrite: step.overwrite === true || ownedSessionOutput(existing, stepId, sessionId, outputKind),
      requiredRunStatus: "running" as const,
      requiredArtifacts: inputs
        .filter((input) => !overwrittenInputs.has(input.path))
        .map((input) => ({ path: input.path, checksum: input.checksum })),
      metadata: {
        sessionId,
        provider,
        requestArtifact: requestPath,
        ...(options.attempt === undefined ? {} : { attempt: options.attempt })
      }
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
    const stopped = error instanceof AgentFlowSessionRequestInterruptedError
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
    if (stopped !== undefined && !(error instanceof AgentFlowSessionRequestInterruptedError)) {
      throw new AgentFlowSessionRequestInterruptedError(stopped);
    }
    throw error;
  }
}

export async function invokeAgentFlowSessionProvider(
  adapter: AgentFlowSessionProviderAdapter,
  request: AgentFlowSessionProviderRequest,
  stopStatus: ExecuteAgentFlowSessionRequestOptions["stopStatus"]
): Promise<AgentFlowSessionProviderResponse> {
  const initialStatus = stopStatus?.();
  if (initialStatus !== undefined) throw new AgentFlowSessionRequestInterruptedError(initialStatus);
  if (stopStatus === undefined) return adapter(request);

  const controller = new AbortController();
  request.signal = controller.signal;
  let timer: ReturnType<typeof setInterval> | undefined;
  const interrupted = new Promise<never>((_resolve, reject) => {
    timer = setInterval(() => {
      const status = stopStatus();
      if (status === undefined) return;
      controller.abort(new AgentFlowSessionRequestInterruptedError(status));
      reject(new AgentFlowSessionRequestInterruptedError(status));
    }, 25);
  });
  try {
    return await Promise.race([Promise.resolve(adapter(request)), interrupted]);
  } finally {
    if (timer !== undefined) clearInterval(timer);
  }
}

export function reserveAgentFlowSessionModelCallBudgets(
  store: AgentFlowRunStateStore,
  runId: string,
  workflow: AgentFlowWorkflow,
  stepId: string,
  sessionId: string,
  provider: string
): void {
  const kinds = ["model_calls", ...(provider === "frontier" ? ["frontier_calls"] : [])];
  const usage = Object.fromEntries(kinds.map((kind) => [kind, store.getBudget(runId, `model:${kind}`)?.used ?? 0]));
  const decision = evaluateAgentFlowPolicy(workflow, { kind: "model_usage", session: sessionId, usage });
  if (decision.status !== "allow") {
    throw new AgentFlowSessionPolicyError(decision.message, decision.code, decision.status);
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

export function readAgentFlowSessionPrompt(
  repoRoot: string,
  declaredPath: string
): AgentFlowSessionProviderRequest["prompt"] {
  const resolved = resolveRepoFile(repoRoot, declaredPath);
  let descriptor: number;
  try {
    descriptor = fs.openSync(resolved, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
  } catch (error) {
    throw new AgentFlowSessionRequestError(
      `Could not read prompt ${declaredPath}: ${errorMessage(error)}`,
      "AGENT_FLOW_SESSION_PROMPT_MISSING",
      { cause: error }
    );
  }
  let content: Buffer;
  try {
    const stat = fs.fstatSync(descriptor);
    if (!stat.isFile()) {
      throw new AgentFlowSessionRequestError(
        `Prompt ${declaredPath} must be a regular file.`,
        "AGENT_FLOW_SESSION_PROMPT_PATH"
      );
    }
    if (stat.size > MAX_AGENT_FLOW_SESSION_PROMPT_BYTES) throw promptTooLarge(declaredPath);
    const bounded = Buffer.allocUnsafe(MAX_AGENT_FLOW_SESSION_PROMPT_BYTES + 1);
    let size = 0;
    while (size < bounded.byteLength) {
      const read = fs.readSync(descriptor, bounded, size, bounded.byteLength - size, null);
      if (read === 0) break;
      size += read;
    }
    if (size > MAX_AGENT_FLOW_SESSION_PROMPT_BYTES) throw promptTooLarge(declaredPath);
    content = bounded.subarray(0, size);
  } catch (error) {
    if (error instanceof AgentFlowSessionRequestError) throw error;
    throw new AgentFlowSessionRequestError(
      `Could not read prompt ${declaredPath}: ${errorMessage(error)}`,
      "AGENT_FLOW_SESSION_PROMPT_MISSING",
      { cause: error }
    );
  } finally {
    fs.closeSync(descriptor);
  }
  return { path: declaredPath, content: content.toString("utf8"), checksum: `sha256:${digest(content)}` };
}

function promptTooLarge(declaredPath: string): AgentFlowSessionRequestError {
  return new AgentFlowSessionRequestError(
    `Prompt ${declaredPath} exceeds the ${MAX_AGENT_FLOW_SESSION_PROMPT_BYTES}-byte session prompt limit.`,
    "AGENT_FLOW_SESSION_PROMPT_TOO_LARGE"
  );
}

export function readAgentFlowSessionInput(
  store: AgentFlowRunStateStore,
  runId: string,
  stepId: string,
  inputPath: string
): AgentFlowSessionRequestArtifact {
  try {
    const input = store.readArtifact(runId, inputPath, { maxBytes: MAX_AGENT_FLOW_SESSION_INPUT_BYTES });
    return {
      path: inputPath,
      content: input.content,
      contentType: input.artifact.contentType,
      checksum: input.artifact.checksum!
    };
  } catch (error) {
    if (error instanceof AgentFlowRunStateError) {
      throw new AgentFlowSessionRequestError(
        `Could not read bounded input ${inputPath} for session request ${stepId}: ${error.message}`,
        "AGENT_FLOW_SESSION_INPUT",
        { cause: error }
      );
    }
    throw error;
  }
}

export function validateAgentFlowSessionProviderResponse(
  stepId: string,
  outputPaths: string[],
  response: AgentFlowSessionProviderResponse
): Map<string, AgentFlowSessionProviderOutput> {
  if (response === null || typeof response !== "object" || Array.isArray(response) || !mapping(response.outputs)) {
    throw new AgentFlowSessionRequestError(
      `Session provider response for step ${stepId} must contain an outputs mapping.`,
      "AGENT_FLOW_SESSION_OUTPUT_INVALID"
    );
  }
  const declared = new Set(outputPaths);
  const actual = Object.keys(response.outputs);
  const missing = outputPaths.find((output) => !Object.hasOwn(response.outputs, output));
  const extra = actual.find((output) => !declared.has(output));
  if (missing !== undefined || extra !== undefined) {
    throw new AgentFlowSessionRequestError(
      missing !== undefined
        ? `Session provider response for step ${stepId} is missing declared output ${missing}.`
        : `Session provider response for step ${stepId} returned undeclared output ${extra}.`,
      "AGENT_FLOW_SESSION_OUTPUT_INVALID"
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
      throw new AgentFlowSessionRequestError(
        `Session provider output ${outputPath} for step ${stepId} must contain string or binary content.`,
        "AGENT_FLOW_SESSION_OUTPUT_INVALID"
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
  if (size > MAX_AGENT_FLOW_SESSION_OUTPUT_BYTES) {
    throw new AgentFlowSessionRequestError(
      `Session provider output ${outputPath} for step ${stepId} exceeds the ${MAX_AGENT_FLOW_SESSION_OUTPUT_BYTES}-byte limit.`,
      "AGENT_FLOW_SESSION_OUTPUT_TOO_LARGE"
    );
  }
  if (totalBytes > MAX_AGENT_FLOW_SESSION_OUTPUT_BYTES) {
    throw new AgentFlowSessionRequestError(
      `Session provider outputs for step ${stepId} exceed the ${MAX_AGENT_FLOW_SESSION_OUTPUT_BYTES}-byte aggregate limit.`,
      "AGENT_FLOW_SESSION_OUTPUT_TOO_LARGE"
    );
  }
}

function preflightOutputCollisions(
  store: AgentFlowRunStateStore,
  runId: string,
  step: AgentFlowWorkflowStep,
  sessionId: string,
  outputPaths: string[],
  requestPath: string,
  requestKind: "review_request" | "session_request",
  outputKind: "review_output" | "session_output",
  requestIdPrefix: "review-request" | "session-request"
): void {
  const outputLabel = requestKind === "review_request" ? "Review output" : "Session output";
  const requestLabel = requestKind === "review_request" ? "Review request" : "Session request";
  if (outputPaths.includes(requestPath)) {
    throw new AgentFlowSessionRequestError(
      `${outputLabel} ${requestPath} conflicts with the runtime request metadata artifact.`,
      "AGENT_FLOW_SESSION_OUTPUT_COLLISION"
    );
  }
  const requestArtifact = store.getArtifact(runId, requestPath);
  if (requestArtifact !== null &&
      (requestArtifact.kind !== requestKind || requestArtifact.id !== `${requestIdPrefix}:${digest(requestPath)}`)) {
    throw new AgentFlowSessionRequestError(
      `${requestLabel} metadata path ${requestPath} is already owned by another artifact.`,
      "AGENT_FLOW_SESSION_OUTPUT_COLLISION"
    );
  }
  if (step.overwrite === true) return;
  const collision = outputPaths.find((outputPath) => {
    const existing = store.getArtifact(runId, outputPath);
    return existing !== null && !ownedSessionOutput(existing, requiredName(step.id, "Session request step ID"), sessionId, outputKind);
  });
  if (collision !== undefined) {
    throw new AgentFlowSessionRequestError(
      `${outputLabel} ${collision} already exists; declare overwrite: true to replace it.`,
      "AGENT_FLOW_SESSION_OUTPUT_COLLISION"
    );
  }
}

function ownedSessionOutput(
  artifact: AgentFlowArtifactRecord | null,
  stepId: string,
  sessionId: string,
  outputKind: "review_output" | "session_output"
): boolean {
  return artifact?.kind === outputKind
    && artifact.producerStepId === stepId
    && artifact.metadata.sessionId === sessionId;
}

function resolveRepoFile(repoRoot: string, declaredPath: string): string {
  if (declaredPath.trim() !== declaredPath || declaredPath.includes("\\")
      || path.posix.isAbsolute(declaredPath) || path.win32.isAbsolute(declaredPath)) {
    throw new AgentFlowSessionRequestError(
      `Prompt path ${JSON.stringify(declaredPath)} must be a normalized repo-relative path.`,
      "AGENT_FLOW_SESSION_PROMPT_PATH"
    );
  }
  const normalized = path.posix.normalize(declaredPath);
  if (normalized !== declaredPath || normalized === ".." || normalized.startsWith("../")) {
    throw new AgentFlowSessionRequestError(
      `Prompt path ${JSON.stringify(declaredPath)} must stay inside the repository.`,
      "AGENT_FLOW_SESSION_PROMPT_PATH"
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
    throw new AgentFlowSessionRequestError(
      `Prompt path ${JSON.stringify(declaredPath)} must stay inside the repository.`,
      "AGENT_FLOW_SESSION_PROMPT_PATH"
    );
  }
  return real;
}

function requiredStringList(value: unknown, label: string): string[] {
  if (!Array.isArray(value) || value.length === 0 || !value.every((entry) => typeof entry === "string" && entry.trim().length > 0)) {
    throw new AgentFlowSessionRequestError(`${label} must be a non-empty list of artifact paths.`, "AGENT_FLOW_SESSION_REQUEST_INVALID");
  }
  const normalized = (value as string[]).map((entry) => entry.trim());
  if (new Set(normalized).size !== normalized.length) {
    throw new AgentFlowSessionRequestError(`${label} must not contain duplicate artifact paths.`, "AGENT_FLOW_SESSION_REQUEST_INVALID");
  }
  return normalized;
}

function normalizedArtifactPaths(value: unknown, label: string): string[] {
  const paths = requiredStringList(value, label).map((artifactPath) => {
    try {
      return normalizeAgentFlowArtifactPath(artifactPath);
    } catch (error) {
      throw new AgentFlowSessionRequestError(
        `${label} contains invalid artifact path ${JSON.stringify(artifactPath)}: ${errorMessage(error)}`,
        "AGENT_FLOW_SESSION_REQUEST_INVALID",
        { cause: error }
      );
    }
  });
  if (new Set(paths).size !== paths.length) {
    throw new AgentFlowSessionRequestError(
      `${label} must not contain paths that resolve to the same canonical artifact.`,
      "AGENT_FLOW_SESSION_REQUEST_INVALID"
    );
  }
  return paths;
}

function resolveSessionInputPaths(
  value: unknown,
  runInputs: Record<string, AgentFlowRunStateValue>,
  stepId: string
): unknown {
  if (!Array.isArray(value)) return value;
  return value.map((entry) => {
    if (typeof entry !== "string") return entry;
    const reference = /^\{\{\s*inputs\.([A-Za-z0-9_-]+)\s*}}$/.exec(entry.trim());
    if (reference === null) return entry;
    const resolved = runInputs[reference[1]!];
    if (typeof resolved !== "string" || resolved.trim().length === 0) {
      throw new AgentFlowSessionRequestError(
        `Session request ${stepId} input ${entry.trim()} must resolve to a non-empty artifact path in persisted run inputs.`,
        "AGENT_FLOW_SESSION_INPUT_UNRESOLVED"
      );
    }
    return resolved;
  });
}

function requiredName(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new AgentFlowSessionRequestError(`${label} must be a non-empty string.`, "AGENT_FLOW_SESSION_REQUEST_INVALID");
  }
  return value.trim();
}

function optionalName(value: unknown, label: string): string | undefined {
  if (value === undefined) return undefined;
  return requiredName(value, label);
}

function mapping(value: unknown): AgentFlowYamlMapping | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as AgentFlowYamlMapping
    : undefined;
}

function findWorkflowStep(steps: AgentFlowWorkflowStep[], stepId: string): AgentFlowWorkflowStep | undefined {
  for (const step of steps) {
    if (typeof step.id === "string" && step.id.trim() === stepId) return step;
    for (const field of ["body", "steps"] as const) {
      const nested = Array.isArray(step[field])
        ? (step[field] as unknown[]).filter((entry): entry is AgentFlowWorkflowStep => mapping(entry) !== undefined)
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
            ? (branchMapping[field] as unknown[]).filter((entry): entry is AgentFlowWorkflowStep => mapping(entry) !== undefined)
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

export function validateAgentFlowSessionProviderMetadata(
  stepId: string,
  metadata: Record<string, unknown> | undefined
): Record<string, AgentFlowRunStateValue> | undefined {
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
    if (Buffer.byteLength(serialized) > MAX_AGENT_FLOW_SESSION_METADATA_BYTES) {
      throw new Error(`metadata exceeds ${MAX_AGENT_FLOW_SESSION_METADATA_BYTES} bytes`);
    }
  } catch (error) {
    throw new AgentFlowSessionRequestError(
      `Session provider metadata for step ${stepId} is invalid: ${errorMessage(error)}.`,
      "AGENT_FLOW_SESSION_METADATA_INVALID",
      { cause: error }
    );
  }
  return metadata as Record<string, AgentFlowRunStateValue>;
}

function sortJson(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sortJson);
  if (value === null || typeof value !== "object") return value;
  return Object.fromEntries(Object.keys(value).sort().map((key) => [key, sortJson((value as Record<string, unknown>)[key])]));
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
