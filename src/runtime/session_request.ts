import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import {
  AgentFlowRunStateError,
  isNormalizedStaticAgentFlowArtifactPath,
  normalizeAgentFlowArtifactPath,
  type AgentFlowArtifactRecord,
  type AgentFlowRunStateStore,
  type AgentFlowRunStopStatus,
  type AgentFlowRunStateValue
} from "./run_state";
import { evaluateAgentFlowPolicy } from "./policy";
import {
  isAgentFlowWorkspaceWriteLockManaged,
  withAgentFlowWorkspaceWriteLock
} from "./workspace_lock";
import type { AgentFlowWorkflow, AgentFlowWorkflowStep, AgentFlowYamlMapping } from "./workflow";
import { createAgentFlowReviewPrompt, parseAgentFlowReviewResult } from "./review";
import {
  createAgentFlowChallengePrompt,
  createAgentFlowConsultPrompt,
  parseAgentFlowChallengeResult,
  parseAgentFlowConsultResult
} from "./collaboration";
import type { AgentFlowConsultResult } from "./collaboration";
import {
  createAgentFlowDisagreementPrompt,
  parseAgentFlowDisagreementResult,
  type AgentFlowDisagreementDecision,
  type AgentFlowDisagreementResult
} from "./disagreement";
import {
  createAgentFlowApprovalPrompt,
  defaultAgentFlowApprovalOutputPath,
  parseAgentFlowApprovalResult,
  type AgentFlowApprovalResult
} from "./approval";
import {
  persistedStaleApprovalStepIdsAcrossLineage,
  staleApprovalMessage,
  staleApprovalStepIdsAcrossLineage
} from "./approval_state";
import { agentFlowInputKeyLooksSensitive, redactAgentFlowSensitiveText } from "./failure_payload";
import {
  AgentFlowSensitiveInputError,
  assertAgentFlowAdapterStringSafe,
  preflightAgentFlowTextInputPath,
  secureAgentFlowReferencedByteInput,
  secureAgentFlowTextInput
} from "./execution_security";
import { isAgentFlowFrontierProvider } from "./policy_utils";

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
  providerKind?: AgentFlowSessionProviderKind;
  providerProfile?: string;
  providerReasoningEffort?: string;
  providerTarget?: string;
  providerDriver?: string;
  providerModel?: string;
  providerFingerprint?: string;
  kind?: "review" | "consult" | "challenge" | "approval" | "disagreement" | "session_request" | "recovery";
  resume: boolean;
  externalSessionId?: string;
  prompt: { path: string; content: string; checksum: string };
  inputs: AgentFlowSessionRequestArtifact[];
  outputs: string[];
  repoRoot?: string;
  canModifyFiles?: boolean;
  fileScope?: {
    layers: Array<{ include: string[]; exclude: string[] }>;
  };
  signal: AbortSignal;
  reportExternalSessionId?: (externalSessionId: string) => void;
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

export type AgentFlowSessionProviderAdapter = ((
  request: AgentFlowSessionProviderRequest
) => AgentFlowSessionProviderResponse | Promise<AgentFlowSessionProviderResponse>) & {
  preflight?: (request: AgentFlowSessionProviderRequest) => void;
  waitForAbort?: boolean;
};

export type AgentFlowSessionProviderKind =
  | "fixture"
  | "local"
  | "frontier"
  | "codex_profile"
  | "custom";

interface AgentFlowSessionProviderRegistrationBase {
  adapter: AgentFlowSessionProviderAdapter;
}

export interface AgentFlowFixtureSessionProviderRegistration
  extends AgentFlowSessionProviderRegistrationBase {
  kind: "fixture";
}

export interface AgentFlowLocalSessionProviderRegistration
  extends AgentFlowSessionProviderRegistrationBase {
  kind: "local";
  enabled: boolean;
}

export interface AgentFlowFrontierSessionProviderRegistration
  extends AgentFlowSessionProviderRegistrationBase {
  kind: "frontier";
  enabled: boolean;
}

export interface AgentFlowCodexProfileSessionProviderRegistration
  extends AgentFlowSessionProviderRegistrationBase {
  kind: "codex_profile";
  enabled: boolean;
  profile: string;
}

export interface AgentFlowCustomSessionProviderRegistration
  extends AgentFlowSessionProviderRegistrationBase {
  kind: "custom";
  name: string;
}

export type AgentFlowSessionProviderRegistration =
  | AgentFlowFixtureSessionProviderRegistration
  | AgentFlowLocalSessionProviderRegistration
  | AgentFlowFrontierSessionProviderRegistration
  | AgentFlowCodexProfileSessionProviderRegistration
  | AgentFlowCustomSessionProviderRegistration;

export interface AgentFlowSessionProviderDescriptor {
  name: string;
  kind: AgentFlowSessionProviderKind;
  profile?: string;
  target?: string;
  driver?: string;
  model?: string;
  reasoningEffort?: string;
  fingerprint?: string;
}

export interface AgentFlowConfiguredSessionProviderDescriptor
  extends AgentFlowSessionProviderDescriptor {
  kind: "local" | "frontier";
  target: string;
  driver: string;
  model: string;
  fingerprint: string;
}

export interface PersistAgentFlowSessionProviderEvidenceInput {
  store: AgentFlowRunStateStore;
  runId: string;
  stepId: string;
  sessionId: string;
  provider: string;
  providerKind: AgentFlowSessionProviderKind;
  providerProfile?: string;
  providerReasoningEffort?: string;
  providerTarget?: string;
  providerDriver?: string;
  providerModel?: string;
  providerFingerprint?: string;
  resume: boolean;
  prompt: { path: string; checksum: string; providerChecksum?: string; redacted?: true };
  inputs: Array<{
    path: string;
    checksum: string;
    contentType: string;
    providerChecksum?: string;
    redacted?: true;
  }>;
  outputs: string[];
  externalSessionId?: string;
  providerMetadata?: Record<string, AgentFlowRunStateValue>;
  recoveryFailureId: string;
  recoveryContextRevision: number;
}

export interface AgentFlowSessionRequestExecutionResult {
  sessionId: string;
  provider: string;
  requestArtifact: AgentFlowArtifactRecord;
  outputArtifacts: AgentFlowArtifactRecord[];
  inputEvidence: Array<{ path: string; checksum: string }>;
  externalSessionId?: string;
  consultResult?: AgentFlowConsultResult;
  disagreementResult?: AgentFlowDisagreementResult;
  approvalResult?: AgentFlowApprovalResult;
}

export interface ExecuteAgentFlowSessionRequestOptions {
  attempt?: number;
  beforePublish?: () => void;
  finalize?: (result: AgentFlowSessionRequestExecutionResult) => void;
  requiredApprovalId?: string;
  stopStatus?: () => AgentFlowRunStopStatus | undefined;
  interruptError?: () => Error | undefined;
}

interface ExecuteAgentFlowSessionStepOptions extends ExecuteAgentFlowSessionRequestOptions {
  kind?: "review" | "consult" | "challenge" | "approval" | "disagreement";
  resolverSessionId?: string;
  disagreementRound?: number;
  disagreementOutput?: string;
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
  private readonly providers = new Map<string, {
    adapter: AgentFlowSessionProviderAdapter;
    descriptor: AgentFlowSessionProviderDescriptor;
  }>();

  register(
    name: string,
    adapter: AgentFlowSessionProviderAdapter,
    options: { enabled?: boolean } = {}
  ): this {
    const normalized = requiredName(name, "Session provider name");
    if (normalized === "local" || normalized === "frontier") {
      return this.registerProvider({
        kind: normalized,
        enabled: options.enabled === true,
        adapter
      });
    }
    if (normalized.startsWith("codex:")) {
      const profile = requiredCodexProfile(normalized.slice("codex:".length));
      return this.registerProvider({
        kind: "codex_profile",
        profile,
        enabled: options.enabled === true,
        adapter
      });
    }
    const descriptor = descriptorForExplicitProviderName(normalized);
    return this.add(normalized, adapter, descriptor);
  }

  registerProvider(registration: AgentFlowSessionProviderRegistration): this {
    if (registration === null || typeof registration !== "object") {
      throw invalidProviderRegistration("Session provider registration must be an object.");
    }
    if (typeof registration.adapter !== "function") {
      throw invalidProviderRegistration("Session provider registration must include an adapter function.");
    }
    if (registration.kind === "fixture") {
      return this.add("fixture", registration.adapter, { name: "fixture", kind: "fixture" });
    }
    if (registration.kind === "custom") {
      const name = requiredName(registration.name, "Custom session provider name");
      if (isReservedSessionProviderName(name)) {
        throw invalidProviderRegistration(
          `Custom session provider name ${name} is reserved; use a fixture registration or an enabled local, frontier, or Codex profile registration.`
        );
      }
      return this.add(name, registration.adapter, { name, kind: "custom" });
    }
    if (registration.kind !== "local" && registration.kind !== "frontier"
        && registration.kind !== "codex_profile") {
      throw invalidProviderRegistration(
        `Unsupported session provider kind ${JSON.stringify((registration as { kind?: unknown }).kind)}; `
          + "use fixture, local, frontier, codex_profile, or custom."
      );
    }
    if (registration.enabled !== true) {
      throw new AgentFlowSessionRequestError(
        `${providerKindLabel(registration.kind)} session providers are disabled unless enabled: true is explicitly configured.`,
        "AGENT_FLOW_SESSION_PROVIDER_DISABLED"
      );
    }
    if (registration.kind === "codex_profile") {
      const profile = requiredCodexProfile(registration.profile);
      const name = `codex:${profile}`;
      return this.add(name, registration.adapter, { name, kind: registration.kind, profile });
    }
    return this.add(registration.kind, registration.adapter, {
      name: registration.kind,
      kind: registration.kind
    });
  }

  get(name: string): AgentFlowSessionProviderAdapter | undefined {
    return this.providers.get(requiredName(name, "Session provider name"))?.adapter;
  }

  describe(name: string): AgentFlowSessionProviderDescriptor | undefined {
    const descriptor = this.providers.get(requiredName(name, "Session provider name"))?.descriptor;
    return descriptor === undefined ? undefined : { ...descriptor };
  }

  names(): string[] {
    return [...this.providers.keys()].sort();
  }

  registerConfigured(
    descriptor: AgentFlowConfiguredSessionProviderDescriptor,
    adapter: AgentFlowSessionProviderAdapter
  ): this {
    const name = requiredName(descriptor.name, "Configured session provider name");
    if (isReservedSessionProviderName(name)) {
      throw invalidProviderRegistration(`Configured session provider name ${name} is reserved.`);
    }
    if (descriptor.kind !== "local" && descriptor.kind !== "frontier") {
      throw invalidProviderRegistration("Configured session providers must declare local or frontier kind.");
    }
    const target = requiredConfiguredIdentity(descriptor.target, "Configured session provider target");
    const driver = requiredConfiguredIdentity(descriptor.driver, "Configured session provider driver");
    const fingerprint = requiredConfiguredIdentity(
      descriptor.fingerprint,
      "Configured session provider fingerprint"
    );
    const nativeCli = driver === "codex-cli" || driver === "claude-code";
    if (nativeCli && descriptor.kind !== "frontier") {
      throw invalidProviderRegistration("Native CLI configured session providers must declare frontier kind.");
    }
    const model = requiredName(descriptor.model, "Configured session provider model");
    if (model !== descriptor.model || /[\u0000-\u001F\u007F-\u009F]/u.test(model)) {
      throw invalidProviderRegistration(
        "Configured session provider model must not have surrounding whitespace or control characters."
      );
    }
    const profile = descriptor.profile === undefined
      ? undefined
      : requiredConfiguredIdentity(descriptor.profile, "Configured session provider profile");
    const reasoningEffort = descriptor.reasoningEffort === undefined
      ? undefined
      : requiredConfiguredIdentity(
          descriptor.reasoningEffort,
          "Configured session provider reasoning effort"
        );
    if ((profile !== undefined || reasoningEffort !== undefined) && driver !== "codex-cli") {
      throw invalidProviderRegistration(
        "Configured session provider profiles and reasoning effort require the codex-cli driver."
      );
    }
    return this.add(name, adapter, {
      ...descriptor,
      name,
      target,
      driver,
      model,
      ...(profile === undefined ? {} : { profile }),
      ...(reasoningEffort === undefined ? {} : { reasoningEffort }),
      fingerprint
    });
  }

  private add(
    name: string,
    adapter: AgentFlowSessionProviderAdapter,
    descriptor: AgentFlowSessionProviderDescriptor
  ): this {
    if (typeof adapter !== "function") {
      throw invalidProviderRegistration(`Session provider ${name} adapter must be a function.`);
    }
    if (this.providers.has(name)) {
      throw new AgentFlowSessionRequestError(
        `Session provider ${name} is already registered.`,
        "AGENT_FLOW_SESSION_PROVIDER_COLLISION"
      );
    }
    this.providers.set(name, { adapter, descriptor: Object.freeze({ ...descriptor }) });
    return this;
  }
}

export function createAgentFlowSessionProviderRegistry(
  registrations: readonly AgentFlowSessionProviderRegistration[] = []
): AgentFlowSessionProviderRegistry {
  const registry = new AgentFlowSessionProviderRegistry();
  for (const registration of registrations) registry.registerProvider(registration);
  return registry;
}

export function persistAgentFlowSessionProviderEvidence(
  input: PersistAgentFlowSessionProviderEvidenceInput
): AgentFlowArtifactRecord {
  const requestPath = preflightAgentFlowSessionProviderEvidence(
    input.store,
    input.runId,
    input.stepId,
    input.recoveryFailureId,
    input.recoveryContextRevision
  );
  const artifactId = `session-request:${digest(requestPath)}`;
  const existing = input.store.getArtifact(input.runId, requestPath);
  const requestMetadata = {
    stepId: input.stepId,
    sessionId: input.sessionId,
    provider: input.provider,
    providerKind: input.providerKind,
    ...(input.providerProfile === undefined ? {} : { providerProfile: input.providerProfile }),
    ...(input.providerReasoningEffort === undefined
      ? {}
      : { providerReasoningEffort: input.providerReasoningEffort }),
    ...(input.providerTarget === undefined ? {} : { providerTarget: input.providerTarget }),
    ...(input.providerDriver === undefined ? {} : { providerDriver: input.providerDriver }),
    ...(input.providerModel === undefined ? {} : { providerModel: `sha256:${digest(input.providerModel)}` }),
    ...(input.providerFingerprint === undefined ? {} : { providerFingerprint: input.providerFingerprint }),
    resume: input.resume,
    recoveryFailureId: input.recoveryFailureId,
    recoveryContextRevision: input.recoveryContextRevision,
    prompt: { ...input.prompt },
    inputs: input.inputs.map((artifact) => ({ ...artifact })),
    outputs: [...input.outputs],
    ...(input.externalSessionId === undefined ? {} : { externalSessionId: input.externalSessionId }),
    ...(input.providerMetadata === undefined ? {} : { providerMetadata: input.providerMetadata })
  };
  return input.store.writeArtifact({
    id: artifactId,
    runId: input.runId,
    stepId: input.stepId,
    path: requestPath,
    kind: "session_request",
    contentType: "application/json; charset=utf-8",
    content: `${stableJson(requestMetadata)}\n`,
    overwrite: existing !== null,
    requiredRunStatus: "running",
    metadata: {
      sessionId: input.sessionId,
      provider: input.provider,
      resume: input.resume
    }
  });
}

export function preflightAgentFlowSessionProviderEvidence(
  store: AgentFlowRunStateStore,
  runId: string,
  stepId: string,
  recoveryFailureId: string,
  recoveryContextRevision: number
): string {
  const evidenceKey = `${stepId}\0${recoveryFailureId}\0${recoveryContextRevision}`;
  const requestPath = `session-requests/recovery/${safePathSegment(stepId).slice(0, 180)}-${digest(evidenceKey).slice(0, 12)}.json`;
  const artifactId = `session-request:${digest(requestPath)}`;
  const idOwner = store.getArtifactById(runId, artifactId);
  if (idOwner !== null && idOwner.declaredPath !== requestPath) {
    throw new AgentFlowSessionRequestError(
      `Session request metadata ID ${artifactId} is already registered at ${idOwner.declaredPath}.`,
      "AGENT_FLOW_SESSION_REQUEST_COLLISION"
    );
  }
  const existing = store.getArtifact(runId, requestPath);
  if (existing !== null && (existing.kind !== "session_request" || existing.id !== artifactId)) {
    throw new AgentFlowSessionRequestError(
      `Session request metadata path ${requestPath} is already owned by another artifact.`,
      "AGENT_FLOW_SESSION_REQUEST_COLLISION"
    );
  }
  return requestPath;
}

export function createAgentFlowFixtureSessionProvider(
  responses: Record<string, AgentFlowSessionProviderResponse>,
  outcomes: Record<string, "succeeded" | "failed" | Array<"succeeded" | "failed">> = {},
  disagreements: Record<
    string,
    AgentFlowDisagreementDecision | "unresolved" | "failed"
      | Array<AgentFlowDisagreementDecision | "unresolved" | "failed">
  > = {}
): AgentFlowSessionProviderAdapter {
  const fixtures = new Map(Object.entries(responses));
  const attempts = new Map<string, number>();
  return (request) => {
    const attemptKey = `${request.runId}\0${request.stepId}\0${request.kind ?? "session_request"}`;
    const attempt = attempts.get(attemptKey) ?? 0;
    attempts.set(attemptKey, attempt + 1);
    if (request.kind === "disagreement") {
      const declaredDecision = disagreements[request.stepId];
      if (declaredDecision === undefined) {
        throw new AgentFlowSessionRequestError(
          `Fixture session provider has no disagreement response for step ${request.stepId}.`,
          "AGENT_FLOW_SESSION_FIXTURE_MISSING"
        );
      }
      const decision = Array.isArray(declaredDecision)
        ? declaredDecision[Math.min(attempt, declaredDecision.length - 1)]!
        : declaredDecision;
      if (decision === "failed") {
        throw new AgentFlowSessionRequestError(
          `Fixture marks disagreement round ${attempt + 1} for step ${request.stepId} as failed.`,
          "AGENT_FLOW_SESSION_FIXTURE_FAILED"
        );
      }
      const result = decision === "unresolved"
        ? { status: "unresolved", rationale: `Fixture left disagreement round ${attempt + 1} unresolved.` }
        : { status: "resolved", decision, rationale: `Fixture resolved disagreement round ${attempt + 1} as ${decision}.` };
      return {
        outputs: Object.fromEntries(request.outputs.map((output) => [output, `${JSON.stringify(result)}\n`]))
      };
    }
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
  return executeAgentFlowSessionStep(store, runId, workflow, step, registry, { ...options, kind: "review" });
}

export async function executeAgentFlowConsult(
  store: AgentFlowRunStateStore,
  runId: string,
  workflow: AgentFlowWorkflow,
  step: AgentFlowWorkflowStep,
  registry: AgentFlowSessionProviderRegistry,
  options: ExecuteAgentFlowSessionRequestOptions = {}
): Promise<AgentFlowSessionRequestExecutionResult> {
  return executeAgentFlowSessionStep(store, runId, workflow, step, registry, { ...options, kind: "consult" });
}

export async function executeAgentFlowChallenge(
  store: AgentFlowRunStateStore,
  runId: string,
  workflow: AgentFlowWorkflow,
  step: AgentFlowWorkflowStep,
  registry: AgentFlowSessionProviderRegistry,
  options: ExecuteAgentFlowSessionRequestOptions = {}
): Promise<AgentFlowSessionRequestExecutionResult> {
  return executeAgentFlowSessionStep(store, runId, workflow, step, registry, { ...options, kind: "challenge" });
}

export async function executeAgentFlowApproval(
  store: AgentFlowRunStateStore,
  runId: string,
  workflow: AgentFlowWorkflow,
  step: AgentFlowWorkflowStep,
  registry: AgentFlowSessionProviderRegistry,
  options: ExecuteAgentFlowSessionRequestOptions = {}
): Promise<AgentFlowSessionRequestExecutionResult> {
  return executeAgentFlowSessionStep(store, runId, workflow, step, registry, { ...options, kind: "approval" });
}

export async function executeAgentFlowDisagreementResolution(
  store: AgentFlowRunStateStore,
  runId: string,
  workflow: AgentFlowWorkflow,
  reviewStep: AgentFlowWorkflowStep,
  resolverSessionId: string,
  round: number,
  output: string,
  registry: AgentFlowSessionProviderRegistry,
  options: ExecuteAgentFlowSessionRequestOptions = {}
): Promise<AgentFlowSessionRequestExecutionResult> {
  return executeAgentFlowSessionStep(store, runId, workflow, reviewStep, registry, {
    ...options,
    kind: "disagreement",
    resolverSessionId,
    disagreementRound: round,
    disagreementOutput: output
  });
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
  const kind = options.kind ?? "session_request";
  const requestKind = kind === "session_request" ? "session_request" : `${kind}_request` as const;
  const outputKind = kind === "session_request" ? "session_output" : `${kind}_output` as const;
  const requestIdPrefix = kind === "session_request" ? "session-request" : `${kind}-request` as const;
  const label = kind === "session_request" ? "Session request" : `${kind[0]!.toUpperCase()}${kind.slice(1)}`;
  const stepId = requiredName(step.id, `${label} step ID`);
  const declaredStep = findWorkflowStep(workflow.steps, stepId);
  const expectedStepType = kind === "disagreement" ? "review" : kind;
  if (requiredName(step.type, `${label} ${stepId} type`) !== expectedStepType
      || !isDeepStrictEqual(run.context.workflow, workflow)
      || declaredStep === undefined || !isDeepStrictEqual(declaredStep, step)) {
    throw new AgentFlowSessionRequestError(
      `${label} ${stepId} must match a step in the workflow persisted for run ${runId}.`,
      "AGENT_FLOW_SESSION_WORKFLOW_MISMATCH"
    );
  }
  store.validateApprovalInvalidationConfiguration(runId);
  const sessionId = requiredName(
    kind === "disagreement" ? options.resolverSessionId
      : kind === "review" || kind === "approval" ? step.reviewer
        : kind === "session_request" ? step.session : step.to,
    `${label} ${stepId} session`
  );
  const session = mapping(workflow.sessions?.[sessionId]);
  if (session === undefined) {
    throw new AgentFlowSessionRequestError(
      `${label} ${stepId} references undeclared session ${sessionId}.`,
      "AGENT_FLOW_SESSION_UNDECLARED"
    );
  }
  if (kind === "approval") {
    if (!Array.isArray(step.artifacts)
        || step.artifacts.length === 0
        || !step.artifacts.every(isNormalizedStaticAgentFlowArtifactPath)) {
      throw new AgentFlowSessionRequestError(
        `Approval ${stepId} artifacts must use normalized static artifact paths.`,
        "AGENT_FLOW_SESSION_REQUEST_INVALID"
      );
    }
    if (step.output !== undefined && !isNormalizedStaticAgentFlowArtifactPath(step.output)) {
      throw new AgentFlowSessionRequestError(
        `Approval ${stepId} output must use a normalized static artifact path.`,
        "AGENT_FLOW_SESSION_REQUEST_INVALID"
      );
    }
    if (sessionId === "human") {
      throw new AgentFlowSessionRequestError(
        `Approval ${stepId} with reviewer human must use the interactive approval runtime.`,
        "AGENT_FLOW_SESSION_AUTHORITY"
      );
    }
    if (mapping(session.authority)?.can_approve !== true) {
      throw new AgentFlowSessionRequestError(
        `Approval reviewer ${sessionId} must explicitly declare can_approve authority.`,
        "AGENT_FLOW_SESSION_AUTHORITY"
      );
    }
  }
  const resolverAuthority = mapping(session.authority);
  if (kind === "disagreement" && (resolverAuthority?.can_approve !== true
      || resolverAuthority.can_request_changes !== true)) {
    throw new AgentFlowSessionRequestError(
      `Disagreement resolver ${sessionId} must explicitly declare can_approve and can_request_changes authority.`,
      "AGENT_FLOW_SESSION_AUTHORITY"
    );
  }
  const mergeCapable = kind !== "approval" && kind !== "review" && kind !== "disagreement"
    && mapping(session.authority)?.can_merge === true;
  const staleMergeApprovalError = (verifyArtifacts = true): AgentFlowSessionPolicyError | undefined => {
    if (!mergeCapable) return undefined;
    const staleApprovalIds = verifyArtifacts
      ? staleApprovalStepIdsAcrossLineage(store, runId)
      : persistedStaleApprovalStepIdsAcrossLineage(store, runId);
    return staleApprovalIds.length === 0 ? undefined : new AgentFlowSessionPolicyError(
      staleApprovalMessage(staleApprovalIds, `merge-capable session ${sessionId}`),
      "AGENT_FLOW_APPROVAL_STALE",
      "fail"
    );
  };
  const initialStaleApproval = staleMergeApprovalError();
  if (initialStaleApproval !== undefined) throw initialStaleApproval;
  const provider = requiredName(session.provider, `Session ${sessionId} provider`);
  const providerDescriptor = registry.describe(provider);
  try {
    for (const [label, value] of [
      ["Session adapter run ID", runId],
      ["Session adapter step ID", stepId],
      ["Session adapter session ID", sessionId],
      ["Session adapter provider", provider],
      ...(providerDescriptor?.profile === undefined
        ? []
        : [["Session adapter provider profile", providerDescriptor.profile]]),
      ...(providerDescriptor?.reasoningEffort === undefined
        ? []
        : [["Session adapter provider reasoning effort", providerDescriptor.reasoningEffort]]),
      ...(providerDescriptor?.target === undefined
        ? []
        : [["Session adapter provider target", providerDescriptor.target]]),
      ...(providerDescriptor?.driver === undefined
        ? []
        : [["Session adapter provider driver", providerDescriptor.driver]]),
      ...(providerDescriptor?.model === undefined
        ? []
        : [["Session adapter provider model", providerDescriptor.model]]),
      ...(providerDescriptor?.fingerprint === undefined
        ? []
        : [["Session adapter provider fingerprint", providerDescriptor.fingerprint]])
    ] as const) {
      assertAgentFlowAdapterStringSafe(workflow, label, value);
    }
  } catch (error) {
    if (error instanceof AgentFlowSensitiveInputError) {
      throw new AgentFlowSessionRequestError(errorMessage(error), error.code, { cause: sanitizedErrorCause(error) });
    }
    throw error;
  }
  const adapter = registry.get(provider);
  if (providerDescriptor === undefined || adapter === undefined) {
    throw new AgentFlowSessionRequestError(
      missingProviderAdapterMessage(provider, stepId),
      "AGENT_FLOW_SESSION_PROVIDER_UNKNOWN"
    );
  }
  const resume = session.resume === true;
  const previous = store.getSession(runId, sessionId);
  const priorExternalSessionId = resume ? previous?.externalSessionId ?? undefined : undefined;
  const rawInputs = kind === "session_request" ? step.inputs : step.artifacts;
  const disagreementInputs = kind === "disagreement"
    ? [...new Set([
        ...normalizedArtifactPaths(step.artifacts, `Disagreement ${stepId} artifacts`),
        ...normalizedArtifactPaths(step.outputs, `Disagreement ${stepId} review outputs`)
      ])]
    : undefined;
  const rawOutputs = kind === "consult" || kind === "challenge"
    ? [step.output]
    : kind === "disagreement" ? [options.disagreementOutput]
      : kind === "approval" ? [typeof step.output === "string" ? step.output : defaultAgentFlowApprovalOutputPath(stepId)] : step.outputs;
  const outputPaths = normalizedArtifactPaths(rawOutputs, `${label} ${stepId} outputs`);
  try {
    if (priorExternalSessionId !== undefined) {
      assertAgentFlowAdapterStringSafe(workflow, "Session adapter external session ID", priorExternalSessionId);
    }
    for (const outputPath of outputPaths) {
      assertAgentFlowAdapterStringSafe(workflow, "Session adapter output path", outputPath);
    }
  } catch (error) {
    if (error instanceof AgentFlowSensitiveInputError) {
      throw new AgentFlowSessionRequestError(errorMessage(error), error.code, { cause: sanitizedErrorCause(error) });
    }
    throw error;
  }
  let rawPrompt: AgentFlowSessionProviderRequest["prompt"];
  let prompt: AgentFlowSessionProviderRequest["prompt"];
  let sourcePromptChecksum: string;
  let promptWasRedacted = false;
  try {
    const secureGeneratedField = (value: string, field: string): string => {
      const secured = secureAgentFlowTextInput(workflow, `${label} ${stepId} ${field}`, value);
      if (secured.redacted) promptWasRedacted = true;
      return secured.value;
    };
    const createGeneratedPrompt = (
      transformField: (value: string, field: string) => string
    ): AgentFlowSessionProviderRequest["prompt"] => {
      if (kind === "approval") return createAgentFlowApprovalPrompt(
        stepId,
        sessionId,
        normalizedArtifactPaths(rawInputs, `Approval ${stepId} artifacts`),
        outputPaths[0]!,
        typeof step.message === "string"
          ? transformField(step.message.trim(), "message")
          : undefined
      );
      if (kind === "review") return createAgentFlowReviewPrompt(
        stepId,
        sessionId,
        transformField(requiredName(step.subject, `Review ${stepId} subject`), "subject"),
        normalizedArtifactPaths(rawInputs, `Review ${stepId} artifacts`),
        outputPaths
      );
      if (kind === "disagreement") return createAgentFlowDisagreementPrompt(
        stepId,
        sessionId,
        requiredName(step.reviewer, `Review ${stepId} reviewer`),
        transformField(requiredName(step.subject, `Review ${stepId} subject`), "subject"),
        disagreementInputs!,
        outputPaths[0]!,
        Number.isSafeInteger(options.disagreementRound) && Number(options.disagreementRound) > 0
          ? Number(options.disagreementRound)
          : 1
      );
      if (kind === "consult") return createAgentFlowConsultPrompt(
        stepId,
        requiredName(step.from, `Consult ${stepId} from`),
        sessionId,
        transformField(requiredName(step.question, `Consult ${stepId} question`), "question"),
        normalizedArtifactPaths(rawInputs, `Consult ${stepId} artifacts`),
        outputPaths[0]!,
        step.blocking === true
      );
      if (kind === "challenge") return createAgentFlowChallengePrompt(
        stepId,
        requiredName(step.from, `Challenge ${stepId} from`),
        sessionId,
        transformField(requiredName(step.question, `Challenge ${stepId} question`), "question"),
        normalizedArtifactPaths(rawInputs, `Challenge ${stepId} artifacts`),
        outputPaths[0]!
      );
      throw new AgentFlowSessionRequestError(
        `Session request ${stepId} does not use a generated prompt.`,
        "AGENT_FLOW_SESSION_PROMPT_PATH"
      );
    };
    const sourcePrompt = kind === "session_request"
      ? (() => {
        const promptPath = requiredName(step.prompt, `Session request ${stepId} prompt`);
        preflightAgentFlowTextInputPath(
          workflow,
          `${label} ${stepId} prompt`,
          promptPath
        );
        return readAgentFlowSessionPrompt(store.repoRoot, promptPath);
      })()
      : createGeneratedPrompt((value) => value);
    if (Buffer.byteLength(sourcePrompt.content, "utf8") > MAX_AGENT_FLOW_SESSION_PROMPT_BYTES) {
      throw promptTooLarge(sourcePrompt.path);
    }
    sourcePromptChecksum = sourcePrompt.checksum;
    rawPrompt = kind === "session_request" ? sourcePrompt : createGeneratedPrompt(secureGeneratedField);
    const securedPrompt = secureAgentFlowTextInput(
      workflow,
      `${label} ${stepId} prompt`,
      rawPrompt.content,
      kind === "session_request" ? rawPrompt.path : undefined
    );
    promptWasRedacted ||= securedPrompt.redacted;
    prompt = {
      ...rawPrompt,
      content: securedPrompt.value,
      checksum: `sha256:${digest(securedPrompt.value)}`
    };
  } catch (error) {
    if (error instanceof AgentFlowSensitiveInputError) {
      throw new AgentFlowSessionRequestError(errorMessage(error), error.code, { cause: sanitizedErrorCause(error) });
    }
    if (error instanceof AgentFlowSessionRequestError) {
      throw sanitizedSessionRequestError(error);
    }
    throw error;
  }
  if (Buffer.byteLength(prompt.content, "utf8") > MAX_AGENT_FLOW_SESSION_PROMPT_BYTES) {
    throw promptTooLarge(prompt.path);
  }
  const resolvedSessionInputs = kind === "session_request"
    ? resolveSessionInputPaths(rawInputs, run.inputs, stepId)
    : { value: rawInputs, sensitivePaths: new Set<string>() };
  const inputPaths = disagreementInputs ?? normalizedArtifactPaths(
    resolvedSessionInputs.value,
    `${label} ${stepId} ${kind === "session_request" ? "inputs" : "artifacts"}`
  );
  if (kind === "approval") {
    const evidenceCollision = outputPaths.find((outputPath) => inputPaths.includes(outputPath));
    if (evidenceCollision !== undefined) {
      throw new AgentFlowSessionRequestError(
        `Approval output must not overwrite evidence artifact ${evidenceCollision}.`,
        "AGENT_FLOW_SESSION_OUTPUT_COLLISION"
      );
    }
  }
  if (inputPaths.length > MAX_AGENT_FLOW_SESSION_INPUTS) {
    throw new AgentFlowSessionRequestError(
      `${label} ${stepId} declares ${inputPaths.length} inputs; at most ${MAX_AGENT_FLOW_SESSION_INPUTS} are allowed.`,
      "AGENT_FLOW_SESSION_INPUT_LIMIT"
    );
  }
  const inputs: AgentFlowSessionRequestArtifact[] = [];
  const sourceInputEvidence: Array<{ path: string; checksum: string }> = [];
  const redactedInputPaths = new Set<string>();
  let totalSourceInputBytes = 0;
  let totalProviderInputBytes = 0;
  for (const inputPath of inputPaths) {
    try {
      preflightAgentFlowTextInputPath(
        workflow,
        `${label} ${stepId} input`,
        inputPath
      );
    } catch (error) {
      if (error instanceof AgentFlowSensitiveInputError) {
        throw new AgentFlowSessionRequestError(errorMessage(error), error.code, { cause: sanitizedErrorCause(error) });
      }
      throw error;
    }
    const sourceInput = readAgentFlowSessionInput(store, runId, stepId, inputPath);
    totalSourceInputBytes += sourceInput.content.byteLength;
    if (totalSourceInputBytes > MAX_AGENT_FLOW_SESSION_TOTAL_INPUT_BYTES) {
      throw new AgentFlowSessionRequestError(
        `Session request ${stepId} inputs exceed the ${MAX_AGENT_FLOW_SESSION_TOTAL_INPUT_BYTES}-byte aggregate limit.`,
        "AGENT_FLOW_SESSION_INPUT_LIMIT"
      );
    }
    let securedInput: ReturnType<typeof secureAgentFlowReferencedByteInput>;
    try {
      securedInput = secureAgentFlowReferencedByteInput(
        workflow,
        `${label} ${stepId} input ${JSON.stringify(inputPath)}`,
        sourceInput.content,
        inputPath,
        sourceInput.contentType,
        resolvedSessionInputs.sensitivePaths.has(inputPath)
      );
    } catch (error) {
      if (error instanceof AgentFlowSensitiveInputError) {
        throw new AgentFlowSessionRequestError(errorMessage(error), error.code, { cause: sanitizedErrorCause(error) });
      }
      throw error;
    }
    const input = {
      ...sourceInput,
      content: securedInput.value,
      checksum: `sha256:${digest(securedInput.value)}`
    };
    if (input.content.byteLength > MAX_AGENT_FLOW_SESSION_INPUT_BYTES) {
      throw new AgentFlowSessionRequestError(
        `Session request ${stepId} input ${inputPath} exceeds the ${MAX_AGENT_FLOW_SESSION_INPUT_BYTES}-byte input limit after sensitive-data handling.`,
        "AGENT_FLOW_SESSION_INPUT_LIMIT"
      );
    }
    totalProviderInputBytes += input.content.byteLength;
    if (totalProviderInputBytes > MAX_AGENT_FLOW_SESSION_TOTAL_INPUT_BYTES) {
      throw new AgentFlowSessionRequestError(
        `Session request ${stepId} provider inputs exceed the ${MAX_AGENT_FLOW_SESSION_TOTAL_INPUT_BYTES}-byte aggregate limit after sensitive-data handling.`,
        "AGENT_FLOW_SESSION_INPUT_LIMIT"
      );
    }
    if (securedInput.redacted) redactedInputPaths.add(inputPath);
    sourceInputEvidence.push({ path: inputPath, checksum: sourceInput.checksum });
    inputs.push(input);
  }
  const requestDirectory = kind === "session_request" ? "session-requests" : `${kind}-requests`;
  const requestPath = `${requestDirectory}/${safePathSegment(stepId).slice(0, 200)}-${digest(stepId).slice(0, 12)}.json`;
  preflightOutputCollisions(store, runId, step, sessionId, outputPaths, requestPath, requestKind, outputKind, requestIdPrefix);
  try {
    if (kind === "session_request") {
      assertAgentFlowAdapterStringSafe(
        workflow,
        "Session adapter prompt path",
        prompt.path,
        { path: true }
      );
    }
    for (const input of inputs) {
      assertAgentFlowAdapterStringSafe(workflow, "Session adapter input path", input.path, { path: true });
      assertAgentFlowAdapterStringSafe(workflow, "Session adapter input content type", input.contentType);
    }
  } catch (error) {
    if (error instanceof AgentFlowSensitiveInputError) {
      throw new AgentFlowSessionRequestError(errorMessage(error), error.code, { cause: sanitizedErrorCause(error) });
    }
    throw error;
  }
  const request: AgentFlowSessionProviderRequest = {
    runId,
    stepId,
    sessionId,
    provider,
    providerKind: providerDescriptor.kind,
    ...(providerDescriptor.profile === undefined ? {} : { providerProfile: providerDescriptor.profile }),
    ...(providerDescriptor.reasoningEffort === undefined
      ? {}
      : { providerReasoningEffort: providerDescriptor.reasoningEffort }),
    ...(providerDescriptor.target === undefined ? {} : { providerTarget: providerDescriptor.target }),
    ...(providerDescriptor.driver === undefined ? {} : { providerDriver: providerDescriptor.driver }),
    ...(providerDescriptor.model === undefined
      ? {}
      : { providerModel: `sha256:${digest(providerDescriptor.model)}` }),
    ...(providerDescriptor.fingerprint === undefined ? {} : { providerFingerprint: providerDescriptor.fingerprint }),
    kind,
    resume,
    ...(priorExternalSessionId === undefined
      ? {}
      : { externalSessionId: priorExternalSessionId }),
    prompt: { ...prompt },
    inputs: inputs.map((input) => ({ ...input, content: Uint8Array.from(input.content) })),
    outputs: [...outputPaths],
    repoRoot: store.repoRoot,
    canModifyFiles: mapping(session.authority)?.can_modify_files === true,
    fileScope: effectiveSessionFileScope(workflow, session, stepId),
    signal: new AbortController().signal
  };

  preflightAgentFlowSessionProvider(adapter, request);
  store.claimSession({
    id: sessionId,
    runId,
    stepId,
    provider,
    status: "running",
    externalSessionId: priorExternalSessionId ?? null,
    state: { resume, lastStepId: stepId }
  });
  let effectiveExternalSessionId = priorExternalSessionId;
  let reportedExternalSessionId: string | undefined;
  request.reportExternalSessionId = (candidate) => {
    if (!resume) {
      throw new AgentFlowSessionRequestError(
        `Session provider ${provider} reported a persistent external session ID for non-resumable session ${sessionId}.`,
        "AGENT_FLOW_SESSION_RESPONSE_INVALID"
      );
    }
    const externalSessionId = requiredName(candidate, `Session provider external session ID for step ${stepId}`);
    assertAgentFlowAdapterStringSafe(workflow, "Session provider external session ID", externalSessionId);
    if (priorExternalSessionId !== undefined && externalSessionId !== priorExternalSessionId) {
      throw new AgentFlowSessionRequestError(
        `Session provider ${provider} reported an external session ID that differs from the persisted ID for resumable session ${sessionId}.`,
        "AGENT_FLOW_SESSION_RESPONSE_INVALID"
      );
    }
    if (reportedExternalSessionId !== undefined && reportedExternalSessionId !== externalSessionId) {
      throw new AgentFlowSessionRequestError(
        `Session provider ${provider} reported more than one external session ID for resumable session ${sessionId}.`,
        "AGENT_FLOW_SESSION_RESPONSE_INVALID"
      );
    }
    reportedExternalSessionId = externalSessionId;
    effectiveExternalSessionId = externalSessionId;
    store.upsertSession({
      id: sessionId,
      runId,
      stepId,
      provider,
      status: "running",
      externalSessionId,
      state: { resume, lastStepId: stepId, externalSessionEstablished: true }
    });
  };
  try {
    reserveAgentFlowSessionModelCallBudgets(
      store,
      runId,
      workflow,
      stepId,
      sessionId,
      provider,
      providerDescriptor.kind
    );
  } catch (error) {
    const status = error instanceof AgentFlowSessionPolicyError && error.status === "fail" ? "failed" : "paused";
    store.upsertSession({
      id: sessionId,
      runId,
      stepId,
      provider,
      status,
      externalSessionId: effectiveExternalSessionId ?? null,
      state: { resume, lastStepId: stepId, error: persistedErrorMessage(error) }
    });
    throw error;
  }

  let response: AgentFlowSessionProviderResponse;
  try {
    response = await invokeAgentFlowSessionProvider(
      adapter,
      request,
      options.stopStatus,
      () => options.interruptError?.() ?? staleMergeApprovalError(false)
    );
  } catch (error) {
    const interruption = options.interruptError?.();
    if (interruption !== undefined) throw interruption;
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
        externalSessionId: effectiveExternalSessionId ?? null,
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
      externalSessionId: effectiveExternalSessionId ?? null,
      state: { resume, lastStepId: stepId, error: persistedErrorMessage(error) }
    });
    if (error instanceof AgentFlowSessionRequestError) {
      throw sanitizedSessionRequestError(error);
    }
    if (error instanceof AgentFlowRunStateError) {
      throw new AgentFlowSessionRequestError(errorMessage(error), error.code, { cause: sanitizedErrorCause(error) });
    }
    throw new AgentFlowSessionRequestError(
      `Session provider ${provider} failed for step ${stepId}: ${errorMessage(error)}`,
      "AGENT_FLOW_SESSION_PROVIDER_FAILED",
      { cause: sanitizedErrorCause(error) }
    );
  }

  try {
  const returnedExternalSessionId = optionalName(
    response.externalSessionId,
    `Session provider external session ID for step ${stepId}`
  );
  if (returnedExternalSessionId !== undefined) {
    assertAgentFlowAdapterStringSafe(
      workflow,
      "Session provider external session ID",
      returnedExternalSessionId
    );
    if (!resume) {
      throw new AgentFlowSessionRequestError(
        `Session provider ${provider} returned a persistent external session ID for non-resumable session ${sessionId}.`,
        "AGENT_FLOW_SESSION_RESPONSE_INVALID"
      );
    }
    if (priorExternalSessionId !== undefined && returnedExternalSessionId !== priorExternalSessionId) {
      throw new AgentFlowSessionRequestError(
        `Session provider ${provider} returned an external session ID that differs from the persisted ID for resumable session ${sessionId}.`,
        "AGENT_FLOW_SESSION_RESPONSE_INVALID"
      );
    }
    if (reportedExternalSessionId !== undefined && returnedExternalSessionId !== reportedExternalSessionId) {
      throw new AgentFlowSessionRequestError(
        `Session provider ${provider} returned an external session ID that differs from the ID reported during execution.`,
        "AGENT_FLOW_SESSION_RESPONSE_INVALID"
      );
    }
  }
  const externalSessionId = returnedExternalSessionId ?? effectiveExternalSessionId;
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
  let consultResult: AgentFlowConsultResult | undefined;
  let disagreementResult: AgentFlowDisagreementResult | undefined;
  let approvalResult: AgentFlowApprovalResult | undefined;
  if (kind === "review") {
    for (const outputPath of outputPaths) parseAgentFlowReviewResult(outputs.get(outputPath)!, outputPath);
  } else if (kind === "consult") {
    consultResult = parseAgentFlowConsultResult(outputs.get(outputPaths[0]!)!, outputPaths[0]!, step.blocking === true);
  } else if (kind === "disagreement") {
    disagreementResult = parseAgentFlowDisagreementResult(outputs.get(outputPaths[0]!)!, outputPaths[0]!);
  } else if (kind === "challenge") {
    parseAgentFlowChallengeResult(outputs.get(outputPaths[0]!)!, outputPaths[0]!);
  } else if (kind === "approval") {
    approvalResult = parseAgentFlowApprovalResult(outputs.get(outputPaths[0]!)!, outputPaths[0]!);
  }
  const providerMetadata = validateAgentFlowSessionProviderMetadata(stepId, response.metadata);
  const staleApprovalBeforePublication = staleMergeApprovalError();
  if (staleApprovalBeforePublication !== undefined) throw staleApprovalBeforePublication;
  options.beforePublish?.();

  const requestMetadata = {
    stepId,
    sessionId,
    provider,
    providerKind: providerDescriptor.kind,
    ...(providerDescriptor.profile === undefined ? {} : { providerProfile: providerDescriptor.profile }),
    ...(providerDescriptor.reasoningEffort === undefined
      ? {}
      : { providerReasoningEffort: providerDescriptor.reasoningEffort }),
    ...(providerDescriptor.target === undefined ? {} : { providerTarget: providerDescriptor.target }),
    ...(providerDescriptor.driver === undefined ? {} : { providerDriver: providerDescriptor.driver }),
    ...(providerDescriptor.model === undefined
      ? {}
      : { providerModel: `sha256:${digest(providerDescriptor.model)}` }),
    ...(providerDescriptor.fingerprint === undefined ? {} : { providerFingerprint: providerDescriptor.fingerprint }),
    resume,
    prompt: {
      path: prompt.path,
      checksum: sourcePromptChecksum,
      ...(promptWasRedacted ? { providerChecksum: prompt.checksum, redacted: true } : {})
    },
    inputs: inputs.map((input) => ({
      path: input.path,
      checksum: sourceInputEvidence.find((evidence) => evidence.path === input.path)!.checksum,
      contentType: input.contentType,
      ...(redactedInputPaths.has(input.path) ? { providerChecksum: input.checksum, redacted: true } : {})
    })),
    outputs: outputPaths,
    ...(externalSessionId === undefined ? {} : { externalSessionId }),
    ...(providerMetadata === undefined ? {} : { providerMetadata })
  };
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
      ...(mergeCapable ? { requiredNoStaleApprovals: true } : {}),
      ...(kind === "approval" && options.requiredApprovalId !== undefined
        ? { requiredApproval: { id: options.requiredApprovalId, status: "requested" as const } }
        : {}),
      requiredArtifacts: sourceInputEvidence.filter((input) => !overwrittenInputs.has(input.path)),
      metadata: {
        sessionId,
        provider,
        requestArtifact: requestPath,
        ...(kind === "approval"
          ? { evidence: sourceInputEvidence }
          : {}),
        ...(options.attempt === undefined ? {} : { attempt: options.attempt })
      }
    };
    if (inputPathSet.has(outputPath)) overwrittenInputs.add(outputPath);
    return publication;
  });
  const staleApprovalBeforeOutputs = staleMergeApprovalError();
  if (staleApprovalBeforeOutputs !== undefined) throw staleApprovalBeforeOutputs;
  let committedInterruption: AgentFlowSessionRequestInterruptedError | undefined;
  const finalized = store.withRunFinalizationTransaction(runId, () => {
    try {
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
        requiredArtifacts: sourceInputEvidence,
        metadata: {
          sessionId,
          provider,
          resume,
          ...(options.attempt === undefined ? {} : { attempt: options.attempt })
        }
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
      const result = {
        sessionId,
        provider,
        requestArtifact,
        outputArtifacts,
        inputEvidence: sourceInputEvidence,
        ...(consultResult === undefined ? {} : { consultResult }),
        ...(disagreementResult === undefined ? {} : { disagreementResult }),
        ...(approvalResult === undefined ? {} : { approvalResult }),
        ...(externalSessionId === undefined ? {} : { externalSessionId })
      };
      options.finalize?.(result);
      return result;
    } catch (error) {
      const stopped = error instanceof AgentFlowSessionRequestInterruptedError
        ? error.status
        : options.stopStatus?.();
      if (stopped === undefined) throw error;
      store.upsertSession({
        id: sessionId,
        runId,
        stepId,
        provider,
        status: stopped,
        externalSessionId: effectiveExternalSessionId ?? null,
        state: { resume, lastStepId: stepId, interrupted: stopped }
      });
      committedInterruption = error instanceof AgentFlowSessionRequestInterruptedError
        ? error
        : new AgentFlowSessionRequestInterruptedError(stopped);
      return undefined;
    }
  });
  if (committedInterruption !== undefined) throw committedInterruption;
  return finalized!;
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
        ? { resume, lastStepId: stepId, error: persistedErrorMessage(error) }
        : { resume, lastStepId: stepId, interrupted: stopped }
    });
    if (stopped !== undefined && !(error instanceof AgentFlowSessionRequestInterruptedError)) {
      throw new AgentFlowSessionRequestInterruptedError(stopped);
    }
    if (error instanceof AgentFlowSessionRequestError) {
      throw sanitizedSessionRequestError(error);
    }
    if (error instanceof AgentFlowRunStateError) {
      throw new AgentFlowSessionRequestError(
        errorMessage(error),
        error.code,
        { cause: sanitizedErrorCause(error) }
      );
    }
    throw new AgentFlowSessionRequestError(
      `Session provider ${provider} response processing failed for step ${stepId}: ${errorMessage(error)}`,
      "AGENT_FLOW_SESSION_RESPONSE_FAILED",
      { cause: sanitizedErrorCause(error) }
    );
  }
}

export async function invokeAgentFlowSessionProvider(
  adapter: AgentFlowSessionProviderAdapter,
  request: AgentFlowSessionProviderRequest,
  stopStatus: ExecuteAgentFlowSessionRequestOptions["stopStatus"],
  interruptError?: () => Error | undefined
): Promise<AgentFlowSessionProviderResponse> {
  // Pipeline callers run adapter preflight before claiming a session or reserving budget.
  // Keep invocation side-effect free beyond the provider call so stateful preflights run once.
  const initialStatus = stopStatus?.();
  if (initialStatus !== undefined) throw new AgentFlowSessionRequestInterruptedError(initialStatus);
  const initialError = interruptError?.();
  if (initialError !== undefined) throw initialError;
  const invokeAdapter = (): Promise<AgentFlowSessionProviderResponse> => Promise.resolve(
    request.canModifyFiles === true && !isAgentFlowWorkspaceWriteLockManaged(adapter)
      ? withAgentFlowWorkspaceWriteLock(request.repoRoot!, request.signal, () => Promise.resolve(adapter(request)))
      : adapter(request)
  );
  if (stopStatus === undefined && interruptError === undefined) return invokeAdapter();

  const controller = new AbortController();
  request.signal = controller.signal;
  let timer: ReturnType<typeof setInterval> | undefined;
  let monitoring = true;
  const interrupted = new Promise<never>((_resolve, reject) => {
    timer = setInterval(() => {
      if (!monitoring) return;
      try {
        const status = stopStatus?.();
        const error = status === undefined ? interruptError?.() : new AgentFlowSessionRequestInterruptedError(status);
        if (error === undefined) return;
        controller.abort(error);
        reject(error);
      } catch (error) {
        controller.abort(error);
        reject(error);
      }
    }, 25);
  });
  const adapterResult = Promise.resolve().then(invokeAdapter);
  try {
    return await Promise.race([adapterResult, interrupted]);
  } catch (error) {
    if (controller.signal.aborted && adapter.waitForAbort === true) {
      try { await adapterResult; } catch { /* The interruption remains authoritative. */ }
    }
    throw error;
  } finally {
    monitoring = false;
    if (timer !== undefined) clearInterval(timer);
  }
}

export function preflightAgentFlowSessionProvider(
  adapter: AgentFlowSessionProviderAdapter,
  request: AgentFlowSessionProviderRequest
): void {
  adapter.preflight?.(request);
}

export function reserveAgentFlowSessionModelCallBudgets(
  store: AgentFlowRunStateStore,
  runId: string,
  workflow: AgentFlowWorkflow,
  stepId: string,
  sessionId: string,
  provider: string,
  providerKind?: AgentFlowSessionProviderKind
): void {
  const frontier = isAgentFlowFrontierProvider(provider)
    || providerKind === "frontier"
    || providerKind === "codex_profile";
  const kinds = ["model_calls", ...(frontier ? ["frontier_calls"] : [])];
  const usage = Object.fromEntries(kinds.map((kind) => [kind, store.getBudget(runId, `model:${kind}`)?.used ?? 0]));
  const decision = evaluateAgentFlowPolicy(workflow, {
    kind: "model_usage",
    session: sessionId,
    usage,
    ...(frontier ? { providerKind: "frontier" as const } : {})
  });
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
  let decoded: string;
  try {
    decoded = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(content);
  } catch (error) {
    throw new AgentFlowSessionRequestError(
      `Prompt ${declaredPath} is not valid UTF-8 text.`,
      "AGENT_FLOW_SESSION_PROMPT_ENCODING",
      { cause: error }
    );
  }
  return { path: declaredPath, content: decoded, checksum: `sha256:${digest(content)}` };
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
      validateAgentFlowSessionOutputSize(stepId, outputPath, size, totalBytes);
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
    validateAgentFlowSessionOutputSize(stepId, outputPath, size, totalBytes);
    return [outputPath, {
      content: value.content,
      ...(typeof value.contentType === "string" && value.contentType.trim().length > 0
        ? { contentType: value.contentType.trim() }
        : {})
    }];
  }));
}

export function validateAgentFlowSessionOutputSize(
  stepId: string,
  outputPath: string,
  size: number,
  totalBytes: number,
  label = "Session provider"
): void {
  if (size > MAX_AGENT_FLOW_SESSION_OUTPUT_BYTES) {
    throw new AgentFlowSessionRequestError(
      `${label} output ${outputPath} for step ${stepId} exceeds the ${MAX_AGENT_FLOW_SESSION_OUTPUT_BYTES}-byte limit.`,
      "AGENT_FLOW_SESSION_OUTPUT_TOO_LARGE"
    );
  }
  if (totalBytes > MAX_AGENT_FLOW_SESSION_OUTPUT_BYTES) {
    throw new AgentFlowSessionRequestError(
      `${label} outputs for step ${stepId} exceed the ${MAX_AGENT_FLOW_SESSION_OUTPUT_BYTES}-byte aggregate limit.`,
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
  requestKind: "approval_request" | "challenge_request" | "consult_request" | "disagreement_request" | "review_request" | "session_request",
  outputKind: "approval_output" | "challenge_output" | "consult_output" | "disagreement_output" | "review_output" | "session_output",
  requestIdPrefix: "approval-request" | "challenge-request" | "consult-request" | "disagreement-request" | "review-request" | "session-request"
): void {
  const label = requestKind.replace("_request", "").replace(/^./, (value) => value.toUpperCase());
  const outputLabel = `${label} output`;
  const requestLabel = `${label} request`;
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
  outputKind: "approval_output" | "challenge_output" | "consult_output" | "disagreement_output" | "review_output" | "session_output"
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
  const resolvedRepoRoot = fs.realpathSync(repoRoot);
  const resolved = path.resolve(resolvedRepoRoot, ...normalized.split("/"));
  let real: string;
  try {
    real = fs.realpathSync(resolved);
  } catch {
    real = resolved;
  }
  const relative = path.relative(resolvedRepoRoot, real);
  if (relative === ".." || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative)) {
    throw new AgentFlowSessionRequestError(
      `Prompt path ${JSON.stringify(declaredPath)} must stay inside the repository.`,
      "AGENT_FLOW_SESSION_PROMPT_PATH"
    );
  }
  if (real !== resolved) {
    throw new AgentFlowSessionRequestError(
      `Prompt path ${JSON.stringify(declaredPath)} must not traverse symbolic links.`,
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
): { value: unknown; sensitivePaths: Set<string> } {
  const sensitivePaths = new Set<string>();
  if (!Array.isArray(value)) return { value, sensitivePaths };
  const resolvedValue = value.map((entry) => {
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
    if (agentFlowInputKeyLooksSensitive(reference[1]!)) {
      try {
        sensitivePaths.add(normalizeAgentFlowArtifactPath(resolved));
      } catch {
        // normalizedArtifactPaths reports the canonical validation error below.
      }
    }
    return resolved;
  });
  return { value: resolvedValue, sensitivePaths };
}

function requiredName(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new AgentFlowSessionRequestError(`${label} must be a non-empty string.`, "AGENT_FLOW_SESSION_REQUEST_INVALID");
  }
  return value.trim();
}

function invalidProviderRegistration(message: string): AgentFlowSessionRequestError {
  return new AgentFlowSessionRequestError(message, "AGENT_FLOW_SESSION_PROVIDER_REGISTRATION_INVALID");
}

function requiredConfiguredIdentity(value: unknown, label: string): string {
  const normalized = requiredName(value, label);
  if (normalized !== value || /[\s=\u0000-\u001F\u007F-\u009F]/u.test(normalized)
      || redactAgentFlowSensitiveText(normalized) !== normalized) {
    throw invalidProviderRegistration(
      `${label} must be a non-secret identifier without whitespace, equals signs, or control characters.`
    );
  }
  return normalized;
}

function requiredCodexProfile(value: unknown): string {
  if (typeof value !== "string" || value.length === 0) {
    throw invalidProviderRegistration("Codex session provider profile must be a non-empty string.");
  }
  if (value !== value.trim()) {
    throw invalidProviderRegistration(
      "Codex session provider profile must not have leading or trailing whitespace."
    );
  }
  return value;
}

function descriptorForExplicitProviderName(name: string): AgentFlowSessionProviderDescriptor {
  if (name === "fixture") return { name, kind: "fixture" };
  return { name, kind: "custom" };
}

function isReservedSessionProviderName(name: string): boolean {
  return name === "fixture" || name === "local" || name === "frontier"
    || name.startsWith("codex:");
}

function providerKindLabel(kind: "local" | "frontier" | "codex_profile"): string {
  return kind === "codex_profile" ? "Codex profile" : `${kind[0]!.toUpperCase()}${kind.slice(1)}`;
}

function missingProviderAdapterMessage(provider: string, stepId: string): string {
  const liveGuidance = provider === "local" || provider === "frontier" || provider.startsWith("codex:")
    ? " Live providers are disabled by default; register an enabled provider configuration"
      + " with createAgentFlowSessionProviderRegistry or registerProvider."
    : " Register a fixture or named custom adapter explicitly.";
  return `No adapter is registered for session provider ${provider}; cannot run step ${stepId}.${liveGuidance}`;
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

function effectiveSessionFileScope(
  workflow: AgentFlowWorkflow,
  session: AgentFlowYamlMapping,
  stepId: string
): NonNullable<AgentFlowSessionProviderRequest["fileScope"]> {
  const scopes = [
    mapping(mapping(workflow.policies)?.file_scope),
    mapping(session.file_scope),
    ...(operationFileScopes(workflow.steps, stepId) ?? [])
  ].filter((scope): scope is AgentFlowYamlMapping => scope !== undefined);
  return { layers: scopes.map((scope) => ({
    include: Array.isArray(scope.include)
      ? scope.include.filter((value): value is string => typeof value === "string")
      : [],
    exclude: Array.isArray(scope.exclude)
      ? scope.exclude.filter((value): value is string => typeof value === "string")
      : []
  })) };
}

function operationFileScopes(
  steps: AgentFlowWorkflowStep[],
  targetStepId: string,
  inherited: AgentFlowYamlMapping[] = []
): AgentFlowYamlMapping[] | undefined {
  for (const step of steps) {
    const ownScope = mapping(step.file_scope);
    const scopes = ownScope === undefined ? inherited : [...inherited, ownScope];
    if (typeof step.id === "string" && step.id.trim() === targetStepId) return scopes;
    for (const field of ["body", "steps", "branches"] as const) {
      const nested = Array.isArray(step[field])
        ? (step[field] as unknown[]).filter((entry): entry is AgentFlowWorkflowStep => mapping(entry) !== undefined)
        : [];
      const found = operationFileScopes(nested, targetStepId, scopes);
      if (found !== undefined) return found;
    }
  }
  return undefined;
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
    const branches = Array.isArray(step.branches)
      ? (step.branches as unknown[]).filter((entry): entry is AgentFlowWorkflowStep => mapping(entry) !== undefined)
      : [];
    const found = findWorkflowStep(branches, stepId);
    if (found !== undefined) return found;
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
  return redactAgentFlowSensitiveText(error instanceof Error ? error.message : String(error));
}

function persistedErrorMessage(error: unknown): string {
  return errorMessage(error);
}

function sanitizedSessionRequestError(error: AgentFlowSessionRequestError): AgentFlowSessionRequestError {
  if (error instanceof AgentFlowSessionRequestInterruptedError) return error;
  const message = errorMessage(error);
  if (error instanceof AgentFlowSessionPolicyError) {
    return new AgentFlowSessionPolicyError(message, error.code, error.status);
  }
  return new AgentFlowSessionRequestError(message, error.code, { cause: sanitizedErrorCause(error) });
}

function sanitizedErrorCause(error: unknown): Error {
  return new Error(errorMessage(error));
}
