import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import { isDeepStrictEqual } from "node:util";
import {
  AgentFlowRunStateError,
  isNormalizedStaticAgentFlowArtifactPath,
  normalizeAgentFlowArtifactPath,
  type AgentFlowRunLockRecord,
  type AgentFlowRunRecord,
  type AgentFlowRunStateValue,
  type AgentFlowRunStateStore,
  type AgentFlowRunStopStatus,
  type AgentFlowRunStatus,
  type AgentFlowFailureOutcome,
  type WriteAgentFlowArtifactInput
} from "./run_state";
import type { AgentFlowWorkflow, AgentFlowWorkflowStep, AgentFlowYamlMapping, AgentFlowYamlValue } from "./workflow";
import { evaluateAgentFlowPolicy } from "./policy";
import { validateAgentFlowRetentionPolicy } from "./policy_validation";
import {
  AgentFlowArtifactTransformRegistry,
  createAgentFlowArtifactTransformRegistry,
  executeAgentFlowArtifactTransform
} from "./artifact_transform";
import {
  agentFlowCommandUnsafeReason,
  MAX_AGENT_FLOW_COMMAND_RETRIES,
  MAX_AGENT_FLOW_COMMAND_TIMEOUT_SECONDS
} from "./validation";
import {
  AgentFlowSessionProviderRegistry,
  AgentFlowSessionPolicyError,
  AgentFlowSessionRequestError,
  AgentFlowSessionRequestInterruptedError,
  MAX_AGENT_FLOW_SESSION_INPUT_BYTES,
  MAX_AGENT_FLOW_SESSION_INPUTS,
  MAX_AGENT_FLOW_SESSION_PROMPT_BYTES,
  MAX_AGENT_FLOW_SESSION_TOTAL_INPUT_BYTES,
  createAgentFlowSessionProviderRegistry,
  executeAgentFlowApproval,
  executeAgentFlowChallenge,
  executeAgentFlowConsult,
  executeAgentFlowDisagreementResolution,
  executeAgentFlowReview,
  executeAgentFlowSessionRequest,
  invokeAgentFlowSessionProvider,
  persistAgentFlowSessionProviderEvidence,
  preflightAgentFlowSessionProvider,
  preflightAgentFlowSessionProviderEvidence,
  readAgentFlowSessionInput,
  readAgentFlowSessionPrompt,
  reserveAgentFlowSessionModelCallBudgets,
  validateAgentFlowSessionOutputSize,
  validateAgentFlowSessionProviderMetadata,
  validateAgentFlowSessionProviderResponse,
  type AgentFlowSessionRequestArtifact
} from "./session_request";
import {
  AgentFlowMcpCallError,
  AgentFlowMcpCallRegistry,
  AgentFlowMcpCallInterruptedError,
  createAgentFlowMcpCallRegistry,
  executeAgentFlowMcpCall,
  validateAgentFlowMcpArgumentExpressions,
  validateAgentFlowMcpOutputPaths
} from "./mcp_call";
import {
  AgentFlowConditionError,
  agentFlowConditionArtifactAlias,
  agentFlowConditionDeclaredArtifactPaths,
  agentFlowConditionExpressionError,
  agentFlowConditionReferences,
  evaluateAgentFlowConditionWithResolver,
  preflightAgentFlowFailureClassificationReferences,
  resolveAgentFlowConditionReference,
  selectAgentFlowConditionTarget,
  type AgentFlowConditionReferenceResolver
} from "./condition";
import { assertAgentFlowSuccessTargetsAreUnambiguous } from "./success_routing";
import {
  createAgentFlowNotificationRegistry,
  deliverAgentFlowNotificationEvent,
  deliverAgentFlowNotifications,
  validateAgentFlowNotifications,
  type AgentFlowNotificationDeliveryResult,
  type AgentFlowNotificationRegistry
} from "./notifications";
import {
  AGENT_FLOW_FINAL_SUMMARY_PATH,
  agentFlowPipelineEffectsFinalized,
  applyAgentFlowRetention,
  markAgentFlowPipelineEffectsFinalized,
  writeAgentFlowFinalSummary
} from "./retention";
import { withAgentFlowPipelineFinalization } from "./finalization";
import {
  agentFlowInputKeyLooksSensitive,
  persistAgentFlowFailurePayload,
  redactAgentFlowSensitiveText,
  type PersistAgentFlowFailurePayloadResult
} from "./failure_payload";
import {
  AgentFlowSensitiveInputError,
  assertAgentFlowAdapterStringSafe,
  preflightAgentFlowTextInputPath,
  secureAgentFlowByteInput,
  secureAgentFlowJsonInput,
  secureAgentFlowReferencedByteInput,
  secureAgentFlowSensitiveJsonInputValue,
  secureAgentFlowTextInput
} from "./execution_security";
import {
  AgentFlowWorkflowRegistry,
  createAgentFlowWorkflowRegistry,
  type AgentFlowRecoveryStatus
} from "./recovery";
import {
  captureAgentFlowWorkspaceSnapshot,
  changedAgentFlowWorkspacePaths,
  type AgentFlowWorkspaceSnapshot
} from "./workspace";
import { AgentFlowFailureClassificationError } from "./failure_classification";
import { createAgentFlowLifecycleRun, transitionAgentFlowLifecycleRun } from "./lifecycle";
import { MAX_AGENT_FLOW_COLLABORATION_QUESTION_BYTES } from "./collaboration";
import { defaultAgentFlowApprovalOutputPath } from "./approval";
import {
  latestStaleApprovalStepIds,
  staleApprovalMessage,
  staleApprovalStepIdsAcrossLineage
} from "./approval_state";
import { defaultAgentFlowDecisionRecordPath, executeAgentFlowDecisionRecord } from "./decision_record";
import {
  collectAgentFlowReviewCyclePathReviewIds,
  collectAgentFlowReviewCycleStepIds,
  defaultAgentFlowDisagreementOutputPath,
  parseAgentFlowDisagreementPolicy,
  parseAgentFlowDisagreementResult,
  type AgentFlowDisagreementDecision,
  type AgentFlowDisagreementPolicy,
  type AgentFlowDisagreementResult
} from "./disagreement";
import { hashAgentFlowProviderModel } from "./provider_config";

const MAX_CAPTURE_BYTES = 10 * 1024 * 1024;
const MAX_RECOVERY_WORKSPACE_SNAPSHOT_BYTES = 64 * 1024 * 1024;
const MAX_RECOVERY_WORKSPACE_SNAPSHOT_PATHS = 200_000;
const RECOVERY_CONTEXT_INPUT_PATH = "recovery-context/injected.md";

export interface AgentFlowCommandPipelineResult {
  status: Extract<AgentFlowRunStatus, "completed" | "failed" | "paused" | "cancelled">;
  completedSteps: string[];
  failedStep?: string;
  failureOutcome?: Exclude<AgentFlowFailureOutcome, "retry" | "continue">;
  exitCode?: number | null;
  timedOut?: boolean;
  message?: string;
  resultStatus?: string;
  returnTo?: string;
}

export type AgentFlowPipelineResumeInput =
  | { outcome: string; decidedBy?: string }
  | { answer: AgentFlowRunStateValue };

interface AgentFlowPipelineWaitingState {
  kind: "approval" | "manual_gate" | "input_request" | "disagreement";
  stepId: string;
  attempt: number;
  reason: "approval" | "manual_approval" | "missing_input" | "disagreement";
  prompt: string;
  validOutcomes: string[];
  saveAs?: string;
  approvalId?: string;
  evidence?: AgentFlowWaitingEvidence[];
  completedSteps: string[];
  routing: SerializedSuccessfulRoutingBudget;
}

interface AgentFlowWaitingEvidence {
  path: string;
  checksum: string;
}

interface CommandAttemptResult {
  exitCode: number | null;
  signal: NodeJS.Signals | null;
  timedOut: boolean;
  stdout: Buffer;
  stderr: Buffer;
  message?: string;
}

interface CommandPreflightFailure {
  status: "failed" | "paused";
  message: string;
}

export async function executeAgentFlowCommandPipeline(
  store: AgentFlowRunStateStore,
  runId: string,
  workflow: AgentFlowWorkflow,
  transforms: AgentFlowArtifactTransformRegistry = createAgentFlowArtifactTransformRegistry(),
  sessionProviders: AgentFlowSessionProviderRegistry = createAgentFlowSessionProviderRegistry(),
  mcpCalls: AgentFlowMcpCallRegistry = createAgentFlowMcpCallRegistry(),
  notifications: AgentFlowNotificationRegistry = createAgentFlowNotificationRegistry(),
  workflows: AgentFlowWorkflowRegistry = createAgentFlowWorkflowRegistry(),
  prepareExecution?: () => void,
  beforeRecovery?: () => void
): Promise<AgentFlowCommandPipelineResult> {
  return store.withRunLock(runId, "run", (lock) => {
    beforeRecovery?.();
    const run = assertPersistedWorkflowIdentity(store, runId, workflow);
    assertOrPersistConfiguredProviderBindings(store, run, workflow, sessionProviders);
    const recoveredExecution = recoverInterruptedExecution(store, lock);
    prepareExecution?.();
    return runAgentFlowCommandPipeline(
      store, runId, workflow, undefined, transforms, sessionProviders, mcpCalls, notifications, workflows,
      undefined, recoveredExecution
    );
  });
}

export async function resumeAgentFlowCommandPipeline(
  store: AgentFlowRunStateStore,
  runId: string,
  workflow: AgentFlowWorkflow,
  response: AgentFlowPipelineResumeInput,
  transforms: AgentFlowArtifactTransformRegistry = createAgentFlowArtifactTransformRegistry(),
  sessionProviders: AgentFlowSessionProviderRegistry = createAgentFlowSessionProviderRegistry(),
  mcpCalls: AgentFlowMcpCallRegistry = createAgentFlowMcpCallRegistry(),
  notifications: AgentFlowNotificationRegistry = createAgentFlowNotificationRegistry(),
  workflows: AgentFlowWorkflowRegistry = createAgentFlowWorkflowRegistry(),
  prepareResume?: () => void
): Promise<AgentFlowCommandPipelineResult> {
  return store.withRunLock(runId, "resume", (lock) => {
    prepareResume?.();
    const run = assertPersistedWorkflowIdentity(store, runId, workflow);
    assertOrPersistConfiguredProviderBindings(store, run, workflow, sessionProviders);
    const recoveredExecution = recoverInterruptedExecution(store, lock);
    const effectiveResponse = recoveredExecution === undefined ? response : undefined;
    return runAgentFlowCommandPipeline(
      store, runId, workflow, effectiveResponse, transforms, sessionProviders, mcpCalls, notifications, workflows,
      undefined, recoveredExecution, prepareResume
    );
  });
}

function assertOrPersistConfiguredProviderBindings(
  store: AgentFlowRunStateStore,
  run: AgentFlowRunRecord,
  workflow: AgentFlowWorkflow,
  providers: AgentFlowSessionProviderRegistry
): void {
  const bindings: Record<string, AgentFlowRunStateValue> = {};
  for (const session of Object.values(workflow.sessions ?? {})) {
    if (session === null || typeof session !== "object" || Array.isArray(session)) continue;
    const provider = normalizedTarget((session as AgentFlowYamlMapping).provider);
    if (provider === undefined || Object.hasOwn(bindings, provider)) continue;
    const descriptor = providers.describe(provider);
    if (descriptor?.target === undefined || descriptor.driver === undefined
        || descriptor.model === undefined || descriptor.fingerprint === undefined) continue;
    bindings[provider] = {
      target: descriptor.target,
      kind: descriptor.kind,
      driver: descriptor.driver,
      modelHash: hashAgentFlowProviderModel(descriptor.model),
      fingerprint: descriptor.fingerprint
    };
  }

  const persisted = run.context.providerBindings;
  if (persisted === undefined) {
    if (Object.keys(bindings).length === 0) return;
    store.updateRun(run.id, { context: { ...run.context, providerBindings: bindings } });
    return;
  }
  if (!isDeepStrictEqual(persisted, bindings)) {
    throw new AgentFlowRunStateError(
      `Agent Flow run ${run.id} provider configuration changed after the run was created. Restore the pinned targets or start a new run ID.`,
      "AGENT_FLOW_PROVIDER_CONFIG_DRIFT"
    );
  }
}

function assertPersistedWorkflowIdentity(
  store: AgentFlowRunStateStore,
  runId: string,
  workflow: AgentFlowWorkflow
): AgentFlowRunRecord {
  const run = store.getRun(runId);
  if (run === null) throw new Error(`Agent Flow run ${runId} was not found.`);
  if (!isDeepStrictEqual(run.context.workflow, workflow)) {
    throw new AgentFlowRunStateError(
      `Agent Flow run ${runId} cannot execute a workflow that differs from its persisted definition.`,
      "AGENT_FLOW_RUN_COLLISION"
    );
  }
  return run;
}

function recoverInterruptedExecution(
  store: AgentFlowRunStateStore,
  lock: AgentFlowRunLockRecord
): RecoveredAgentFlowExecution | undefined {
  if (!lock.recoveredStaleLock) return undefined;
  const run = store.getRun(lock.runId);
  if (run === null) return undefined;
  const routing = persistedExecutionRouting(run.context);
  const cursorStepId = run.currentStepId ?? undefined;
  if (run.status !== "running"
      && !(run.status === "pending" && cursorStepId !== undefined && routing !== undefined)) {
    return undefined;
  }
  const latestCursorEvent = cursorStepId === undefined
    ? undefined
    : [...store.listEvents(lock.runId)].reverse().find((event) =>
        event.stepId === cursorStepId && event.type.startsWith("step.")
      );
  const latestCursorStep = cursorStepId === undefined
    ? null
    : store.latestStepRecoveryState(lock.runId, cursorStepId);
  const latestCursorFailure = cursorStepId === undefined || latestCursorStep === null
    ? undefined
    : [...store.listFailures(lock.runId)].reverse().find((entry) =>
      entry.stepId === cursorStepId && entry.attempt === latestCursorStep.attempt
    );
  const pendingCursorCheckpoint = cursorStepId !== undefined
    && isPendingExecutionCheckpoint(
      run.context.executionCheckpoint,
      cursorStepId,
      latestCursorStep?.attempt ?? 0
    );
  const checkpointCompletedAttempts = cursorStepId === undefined
    ? undefined
    : executionCheckpointCompletedAttempts(run.context.executionCheckpoint, cursorStepId);
  const checkpointedVisit = cursorStepId === undefined
    ? undefined
    : executionCheckpointVisit(run.context.executionCheckpoint, cursorStepId);
  const cursorHasPersistedFailure = cursorStepId !== undefined
    && (latestCursorStep?.status === "failed" || latestCursorStep?.status === "running")
    && latestCursorFailure !== undefined
    && hasUnroutedFailureOrRecoveryDecision(store, lock.runId, cursorStepId, latestCursorStep.attempt);
  const pendingRetryAttemptIndex = cursorHasPersistedFailure
      && latestCursorFailure?.retryable === true
      && mapping(latestCursorFailure.payload)?.outcome === "retry"
      && checkpointCompletedAttempts !== undefined
      && latestCursorStep!.attempt > checkpointCompletedAttempts
    ? latestCursorStep!.attempt - checkpointCompletedAttempts
    : undefined;
  const interruptedRecoveryCursor = cursorStepId !== undefined
    && cursorHasPersistedFailure
    && !pendingCursorCheckpoint
    && pendingRetryAttemptIndex === undefined;
  const pendingRecoveredCursor = cursorStepId !== undefined && (
    (run.status === "running"
      && latestCursorStep?.status === "running"
      && !cursorHasPersistedFailure
      && checkpointedVisit !== undefined
      && checkpointedVisit === routing?.visits[cursorStepId])
    || (run.status === "pending"
      && latestCursorStep?.status === "cancelled"
      && [...store.listEvents(lock.runId)].reverse().some((event) =>
        event.type === "run.execution_recovered"
          && mapping(event.payload)?.interruptedStepId === cursorStepId
      ))
  );
  const completedCursorPayload = latestCursorStep === null
    ? latestCursorEvent?.type === "step.completed" ? latestCursorEvent.payload : undefined
    : latestCursorStep.status === "completed" && !pendingCursorCheckpoint ? latestCursorStep.output : undefined;
  const latestCompletionEventAttempt = latestCursorEvent?.type === "step.completed"
    ? mapping(latestCursorEvent.payload)?.attempt
    : undefined;
  if (cursorStepId !== undefined
      && latestCursorStep?.status === "completed"
      && !pendingCursorCheckpoint
      && latestCompletionEventAttempt !== latestCursorStep.attempt) {
    store.appendRunEvent(lock.runId, {
      type: "step.completed",
      stepId: cursorStepId,
      payload: latestCursorStep.output
    });
  }
  const completedSteps = completedStepsFromExecutionEvents(store, lock.runId);
  return {
    attempts: store.recoverInterruptedRun(lock),
    routing,
    completedSteps,
    ...(cursorStepId === undefined ? {} : { cursorStepId }),
    ...(completedCursorPayload === undefined ? {} : { completedCursorPayload }),
    ...(interruptedRecoveryCursor ? { interruptedRecoveryCursor: true } : {}),
    ...(pendingCursorCheckpoint || pendingRecoveredCursor || pendingRetryAttemptIndex !== undefined
      ? { pendingExecutionCursor: true }
      : {}),
    ...(pendingRetryAttemptIndex === undefined ? {} : { pendingRetryAttemptIndex })
  };
}

async function runAgentFlowCommandPipeline(
  store: AgentFlowRunStateStore,
  runId: string,
  workflow: AgentFlowWorkflow,
  resumeInput: AgentFlowPipelineResumeInput | undefined,
  transforms: AgentFlowArtifactTransformRegistry,
  sessionProviders: AgentFlowSessionProviderRegistry,
  mcpCalls: AgentFlowMcpCallRegistry,
  notifications: AgentFlowNotificationRegistry,
  workflows: AgentFlowWorkflowRegistry,
  beforeRemediatedResult?: () => void,
  recoveredExecution?: RecoveredAgentFlowExecution,
  prepareResume?: () => void
): Promise<AgentFlowCommandPipelineResult> {
  const existing = store.getRun(runId);
  if (existing === null) throw new Error(`Agent Flow run ${runId} was not found.`);
  if (!isDeepStrictEqual(existing.context.workflow, workflow)) {
    throw new AgentFlowRunStateError(
      `Agent Flow run ${runId} cannot execute a workflow that differs from its persisted definition.`,
      "AGENT_FLOW_RUN_COLLISION"
    );
  }
  store.validateApprovalInvalidationConfiguration(runId);
  const notificationIssue = validateAgentFlowNotifications(workflow)[0];
  if (notificationIssue !== undefined) {
    throw new AgentFlowRunStateError(
      `Agent Flow run ${runId} cannot execute invalid notifications: ${notificationIssue.code} (${notificationIssue.path}): ${notificationIssue.message}`,
      "AGENT_FLOW_WORKFLOW_INVALID"
    );
  }
  const retentionIssue = validateAgentFlowRetentionPolicy(workflow.retention)[0];
  if (retentionIssue !== undefined) {
    throw new AgentFlowRunStateError(
      `Agent Flow run ${runId} cannot execute invalid retention: ${retentionIssue.code} (${retentionIssue.path}): ${retentionIssue.message}`,
      "AGENT_FLOW_WORKFLOW_INVALID"
    );
  }
  const recoveryLimitIssue = runtimeRecoveryLimitConfigurationIssue(workflow);
  if (recoveryLimitIssue !== undefined) {
    throw new AgentFlowRunStateError(
      `Agent Flow run ${runId} cannot execute invalid recovery limits: ${recoveryLimitIssue.code} (${recoveryLimitIssue.path}): ${recoveryLimitIssue.message}`,
      "AGENT_FLOW_WORKFLOW_INVALID"
    );
  }
  validateRuntimeInteractionSteps(workflow.steps, workflow.style === "pipeline");
  const stepLocations = collectRuntimeStepLocations(workflow.steps);
  validateRuntimeRecoveryTargets(
    workflow.steps,
    workflow,
    sessionProviders,
    workflows,
    new Set(stepLocations.keys())
  );
  if (resumeInput === undefined && existing.status !== "pending") {
    throw new Error(`Agent Flow run ${runId} cannot execute while its status is ${existing.status}.`);
  }
  if (resumeInput !== undefined && existing.status !== "paused") {
    throw new AgentFlowRunStateError(
      `Agent Flow run ${runId} cannot resume while its status is ${existing.status}.`,
      "AGENT_FLOW_RUN_TRANSITION"
    );
  }
  assertAgentFlowSuccessTargetsAreUnambiguous(workflow.steps);
  let completedSteps!: string[];
  let routingBudget!: SuccessfulRoutingBudget;
  let currentSteps = workflow.steps;
  let stepIndex = 0;
  let recoveredCompletedCursorPayload: AgentFlowRunStateValue | undefined;
  let pendingExecutionCursor = recoveredExecution?.pendingExecutionCursor === true;
  let pendingRetryAttemptIndex = recoveredExecution?.pendingRetryAttemptIndex;

  if (resumeInput === undefined) {
    store.withRunStateTransaction(runId, () => {
      store.transitionRunWithEvent(runId, {
        status: "running",
        allowedFrom: ["pending"],
        event: { type: "run.started", payload: { status: "running" } }
      });
      const persistedRouting = recoveredExecution?.routing ?? persistedExecutionRouting(existing.context);
      const cursorStepId = recoveredExecution?.cursorStepId
        ?? (persistedRouting === undefined ? undefined : existing.currentStepId ?? undefined);
      completedSteps = recoveredExecution?.completedSteps
        ?? (cursorStepId === undefined ? [] : completedStepsFromExecutionEvents(store, runId));
      if (cursorStepId !== undefined) {
        const cursor = stepLocations.get(cursorStepId);
        if (cursor === undefined) {
          throw new AgentFlowRunStateError(
            `Agent Flow run ${runId} cannot recover because interrupted step ${cursorStepId} is not in its workflow.`,
            "AGENT_FLOW_RUN_LOCK_RECOVERY"
          );
        }
        currentSteps = cursor.steps;
        stepIndex = cursor.index;
        recoveredCompletedCursorPayload = recoveredExecution?.completedCursorPayload;
      }
      routingBudget = persistedRouting === undefined
        ? createSuccessfulRoutingBudget(workflow, notifications, recoveredExecution?.attempts)
        : deserializeRoutingBudget(persistedRouting, workflow, notifications);
      for (const [stepId, attempt] of Object.entries(recoveredExecution?.attempts ?? {})) {
        routingBudget.attempts.set(stepId, Math.max(routingBudget.attempts.get(stepId) ?? 0, attempt));
      }
      checkpointExecutionRouting(store, runId, routingBudget);
    });
  } else {
    const resumed = resumeWaitingStep(
      store,
      runId,
      workflow,
      existing.context,
      resumeInput,
      stepLocations,
      notifications,
      prepareResume
    );
    if ("result" in resumed) return resumed.result;
    completedSteps = resumed.completedSteps;
    routingBudget = resumed.routingBudget;
    currentSteps = resumed.steps;
    stepIndex = resumed.nextIndex;
  }

  if (recoveredCompletedCursorPayload !== undefined) {
    const step = currentSteps[stepIndex]!;
    const stepId = requiredStepId(step);
    let routed: SuccessfulRoute;
    if (normalizedTarget(step.type) === "review"
        && (routingBudget.disagreementRounds.get(stepId) ?? 0) > 0
        && !isPersistedDisagreementResolution(recoveredCompletedCursorPayload)) {
      const disagreement = await resolveReviewDisagreement(
        store,
        runId,
        workflow,
        step,
        completedSteps,
        routingBudget,
        sessionProviders,
        true
      );
      if ("result" in disagreement) return disagreement.result;
      completedSteps.push(stepId);
      routed = routeAfterSuccessfulStep(
        store, runId, completedSteps, stepId, step, currentSteps, stepIndex, stepLocations, routingBudget
      );
    } else {
      if (isPersistedDisagreementResolution(recoveredCompletedCursorPayload)) {
        const output = mapping(recoveredCompletedCursorPayload)!;
        routingBudget.disagreementRounds.set(stepId, 0);
        routingBudget.attempts.set(stepId, Math.max(
          routingBudget.attempts.get(stepId) ?? 0,
          output.attempt as number
        ));
        checkpointExecutionRouting(store, runId, routingBudget);
      }
      routed = routeRecoveredCompletedStep(
        store,
        runId,
        completedSteps,
        stepId,
        step,
        currentSteps,
        stepIndex,
        stepLocations,
        routingBudget,
        recoveredCompletedCursorPayload,
        beforeRemediatedResult
      );
    }
    if ("result" in routed) return routed.result;
    currentSteps = routed.steps;
    stepIndex = routed.nextIndex;
  }

  if (recoveredExecution?.interruptedRecoveryCursor === true) {
    const step = currentSteps[stepIndex]!;
    const stepId = requiredStepId(step);
    const recoveryRoute = await routeAfterFailedStep(
      store, runId, workflow, completedSteps, stepId, step, currentSteps, stepIndex,
      stepLocations, routingBudget, transforms, sessionProviders, mcpCalls, notifications, workflows
    );
    if (recoveryRoute === undefined) {
      if (failureContinues(step)) {
        const routed = fallthroughAfterStep(store, runId, completedSteps, stepId, currentSteps, stepIndex, routingBudget);
        if ("result" in routed) return routed.result;
        currentSteps = routed.steps;
        stepIndex = routed.nextIndex;
      } else {
        const failure = latestPersistedStepFailure(store, runId, stepId);
        return finishFailure(
          store,
          runId,
          completedSteps,
          stepId,
          failure,
          failureStatus(step),
          routingBudget.terminalEffects
        );
      }
    } else {
      if ("result" in recoveryRoute) return recoveryRoute.result;
      currentSteps = recoveryRoute.steps;
      stepIndex = recoveryRoute.nextIndex;
    }
  }

  while (stepIndex < currentSteps.length) {
    const step = currentSteps[stepIndex]!;
    const stoppedBeforeStep = stoppedPipelineResult(store, runId, completedSteps);
    if (stoppedBeforeStep !== undefined) return stoppedBeforeStep;
    const stepId = requiredStepId(step);
    const recoveredRetryAttemptIndex = stepId === recoveredExecution?.cursorStepId
      ? pendingRetryAttemptIndex
      : undefined;
    pendingRetryAttemptIndex = undefined;
    const stepType = normalizedTarget(step.type);
    const staleApprovalIds = mergeContinuationStaleApprovals(store, runId, workflow, step);
    if (staleApprovalIds.length > 0) {
      return finishFailure(store, runId, completedSteps, stepId, {
        exitCode: null,
        timedOut: false,
        message: staleApprovalMessage(staleApprovalIds, `merge-capable step ${stepId}`)
      }, "failed", routingBudget.terminalEffects);
    }
    const recoveryGuard = recoveryGuardFailure(store, runId, workflow, completedSteps, stepId, step, routingBudget);
    if (recoveryGuard !== undefined) return recoveryGuard;
    if (recoveredRetryAttemptIndex === undefined
        && stepType === "review" && routingBudget.reviewCycleStepIds.has(stepId)
        && routingBudget.maxReviewCycles !== undefined
        && (routingBudget.attempts.get(stepId) ?? 0) >= routingBudget.maxReviewCycles) {
      const disagreement = await resolveReviewDisagreement(
        store,
        runId,
        workflow,
        step,
        completedSteps,
        routingBudget,
        sessionProviders
      );
      if ("result" in disagreement) return disagreement.result;
      completedSteps.push(stepId);
      const routed = routeAfterSuccessfulStep(
        store, runId, completedSteps, stepId, step, currentSteps, stepIndex, stepLocations, routingBudget
      );
      if ("result" in routed) return routed.result;
      currentSteps = routed.steps;
      stepIndex = routed.nextIndex;
      continue;
    }
    if (pendingExecutionCursor && stepId === recoveredExecution?.cursorStepId) {
      pendingExecutionCursor = false;
    } else {
      routingBudget.visits.set(stepId, (routingBudget.visits.get(stepId) ?? 0) + 1);
      checkpointExecutionRouting(store, runId, routingBudget, stepId);
    }
    if (stepType === "approval" && normalizedTarget(step.reviewer) === "human") {
      const attempt = allocateStepAttempt(routingBudget, stepId);
      if (attempt === undefined) return stepAttemptLimitResult(store, runId, completedSteps, stepId, routingBudget);
      const preflightError = validateApprovalStep(workflow, step);
      if (preflightError !== undefined) {
        persistSessionRequestFailure(store, runId, stepId, "human", preflightError, false, "fail", true, attempt);
        return finishFailure(store, runId, completedSteps, stepId, {
          exitCode: null,
          timedOut: false,
          message: preflightError
        }, "failed", routingBudget.terminalEffects);
      }
      try {
        return pauseForInteraction(store, runId, step, "approval", attempt, completedSteps, routingBudget);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        persistApprovalInteractionFailure(store, runId, stepId, message, attempt);
        return finishFailure(store, runId, completedSteps, stepId, { exitCode: null, timedOut: false, message }, "failed", routingBudget.terminalEffects);
      }
    }
    if (stepType === "decision_record") {
      const attempt = allocateStepAttempt(routingBudget, stepId);
      if (attempt === undefined) return stepAttemptLimitResult(store, runId, completedSteps, stepId, routingBudget);
      try {
        const preflightError = validateDecisionRecordStep(step);
        if (preflightError !== undefined) {
          throw new AgentFlowRunStateError(preflightError, "AGENT_FLOW_DECISION_RECORD_INVALID");
        }
        store.updateRun(runId, { currentStepId: stepId, error: null });
        store.upsertStep({
          runId,
          stepId,
          attempt,
          status: "running",
          input: { type: "decision_record", artifacts: step.artifacts as AgentFlowRunStateValue }
        });
        store.appendRunEvent(runId, { type: "step.started", stepId, payload: { attempt, type: "decision_record" } });
        store.withRunFinalizationTransaction(runId, () => {
          const result = executeAgentFlowDecisionRecord(store, runId, step, workflow);
          const output = { attempt, decisionId: result.record.decision_id, artifact: result.artifact.declaredPath };
          store.upsertStep({ runId, stepId, attempt, status: "completed", output });
          store.appendRunEvent(runId, { type: "step.completed", stepId, payload: output });
        });
        completedSteps.push(stepId);
        const routed = routeAfterSuccessfulStep(store, runId, completedSteps, stepId, step, currentSteps, stepIndex, stepLocations, routingBudget);
        if ("result" in routed) return routed.result;
        currentSteps = routed.steps;
        stepIndex = routed.nextIndex;
        continue;
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        const failure = { attempt, message, outcome: "fail" as const };
        const persisted = persistAgentFlowFailurePayload(store, {
          id: `decision-record:${safeId(stepId)}:attempt-${attempt}`,
          runId,
          stepId,
          stepType: "decision_record",
          attempt,
          exitCode: null,
          summary: message,
          classification: "decision_record_failure",
          retryable: false,
          outcome: "fail",
          indexPayload: failure
        });
        const indexedFailure = { ...persisted.indexPayload, ...failureReference(persisted) };
        store.upsertStep({ runId, stepId, attempt, status: "failed", error: indexedFailure });
        store.appendRunEvent(runId, { type: "step.failed", stepId, payload: indexedFailure });
        return finishFailure(store, runId, completedSteps, stepId, { exitCode: null, timedOut: false, message }, "failed", routingBudget.terminalEffects);
      }
    }
    if (stepType === "mcp_call") {
      const firstAttempt = allocateStepAttempt(routingBudget, stepId);
      if (firstAttempt === undefined) return stepAttemptLimitResult(store, runId, completedSteps, stepId, routingBudget);
      const preflightError = validateMcpCallStep(step);
      if (preflightError !== undefined) {
        persistMcpCallFailure(store, runId, stepId, preflightError, false, firstAttempt, "fail", true);
        return finishFailure(store, runId, completedSteps, stepId, {
          exitCode: null,
          timedOut: false,
          message: preflightError
        }, "failed", routingBudget.terminalEffects);
      }
      const retries = failureRetries(step);
      let failure: string | undefined;
      const firstAttemptIndex = nextRetryAttemptIndex(recoveredRetryAttemptIndex, retries, stepId);
      for (let attemptIndex = firstAttemptIndex; attemptIndex <= retries + 1; attemptIndex += 1) {
        const attempt = attemptIndex === firstAttemptIndex ? firstAttempt : allocateStepAttempt(routingBudget, stepId);
        if (attempt === undefined) return stepAttemptLimitResult(store, runId, completedSteps, stepId, routingBudget);
        const stopped = activeStopStatus(store, runId);
        if (stopped !== undefined) return stoppedPipelineResult(store, runId, completedSteps)!;
        const server = (step.server as string).trim();
        const tool = (step.tool as string).trim();
        const input = {
          attempt,
          server,
          tool,
          arguments: step.arguments as AgentFlowRunStateValue,
          outputs: step.outputs as AgentFlowRunStateValue
        };
        store.updateRun(runId, { currentStepId: stepId, error: null });
        store.upsertStep({ runId, stepId, attempt, status: "running", input });
        store.appendRunEvent(runId, { type: "step.started", stepId, payload: input });
        try {
          await executeAgentFlowMcpCall(store, runId, workflow, step, mcpCalls, {
            attempt,
            stopStatus: () => activeStopStatus(store, runId),
            interruptError: () => store.runLockInterruption(),
            beforePublish: () => {
              const lockError = store.runLockInterruption();
              if (lockError !== undefined) throw lockError;
              const status = activeStopStatus(store, runId);
              if (status !== undefined) throw new AgentFlowMcpCallInterruptedError(status);
            },
            finalize: (result) => {
              const output = {
                attempt,
                server: result.server,
                tool: result.tool,
                requestArtifact: result.requestArtifact.declaredPath,
                outputs: result.outputArtifacts.map((artifact) => artifact.declaredPath)
              };
              store.upsertStep({ runId, stepId, attempt, status: "completed", output });
              store.appendRunEvent(runId, { type: "step.completed", stepId, payload: output });
            }
          });
          const lockError = store.runLockInterruption();
          if (lockError !== undefined) throw lockError;
          completedSteps.push(stepId);
          failure = undefined;
          break;
        } catch (error) {
          const lockError = store.runLockInterruption();
          if (lockError !== undefined) throw lockError;
          if (error instanceof AgentFlowMcpCallInterruptedError) {
            persistMcpCallInterruption(store, runId, stepId, attempt, error.status);
            return interruptedPipelineResult(store, runId, completedSteps, error.status);
          }
          const stopped = activeStopStatus(store, runId);
          if (stopped !== undefined) {
            persistMcpCallInterruption(store, runId, stepId, attempt, stopped);
            return interruptedPipelineResult(store, runId, completedSteps, stopped);
          }
          failure = error instanceof Error ? error.message : String(error);
          const retryable = attemptIndex <= retries && mcpCallFailureIsRetryable(error);
          persistMcpCallFailure(store, runId, stepId, failure, retryable, attempt, failureOutcome(step, retryable));
          if (!retryable) break;
        }
      }
      if (failure === undefined) {
        const recoveryFailure = resolveReturnedRecoveryFailures(
          store, runId, completedSteps, stepId, routingBudget.attempts.get(stepId)!, routingBudget.terminalEffects
        );
        if (recoveryFailure !== undefined) return recoveryFailure;
        const routed = routeAfterSuccessfulStep(store, runId, completedSteps, stepId, step, currentSteps, stepIndex, stepLocations, routingBudget);
        if ("result" in routed) return routed.result;
        currentSteps = routed.steps;
        stepIndex = routed.nextIndex;
        continue;
      }
      const recoveryRoute = await routeAfterFailedStep(
        store, runId, workflow, completedSteps, stepId, step, currentSteps, stepIndex,
        stepLocations, routingBudget, transforms, sessionProviders, mcpCalls, notifications, workflows
      );
      if (recoveryRoute !== undefined) {
        if ("result" in recoveryRoute) return recoveryRoute.result;
        currentSteps = recoveryRoute.steps;
        stepIndex = recoveryRoute.nextIndex;
        continue;
      }
      if (failureContinues(step)) {
        const routed = fallthroughAfterStep(store, runId, completedSteps, stepId, currentSteps, stepIndex, routingBudget);
        if ("result" in routed) return routed.result;
        currentSteps = routed.steps;
        stepIndex = routed.nextIndex;
        continue;
      }
      return finishFailure(store, runId, completedSteps, stepId, {
        exitCode: null,
        timedOut: false,
        message: failure
      }, failureStatus(step), routingBudget.terminalEffects);
    }
    if (stepType === "artifact_transform") {
      const firstAttempt = allocateStepAttempt(routingBudget, stepId);
      if (firstAttempt === undefined) return stepAttemptLimitResult(store, runId, completedSteps, stepId, routingBudget);
      const preflightError = validateTransformStep(step);
      if (preflightError !== undefined) {
        persistTransformPreflightFailure(store, runId, stepId, firstAttempt, preflightError, "fail");
        return finishFailure(store, runId, completedSteps, stepId, {
          exitCode: null,
          timedOut: false,
          message: preflightError
        }, "failed", routingBudget.terminalEffects);
      }
      const retries = failureRetries(step);
      let failure: string | undefined;
      const firstAttemptIndex = nextRetryAttemptIndex(recoveredRetryAttemptIndex, retries, stepId);
      for (let attemptIndex = firstAttemptIndex; attemptIndex <= retries + 1; attemptIndex += 1) {
        const attempt = attemptIndex === firstAttemptIndex ? firstAttempt : allocateStepAttempt(routingBudget, stepId);
        if (attempt === undefined) return stepAttemptLimitResult(store, runId, completedSteps, stepId, routingBudget);
        const outcome = executeTransformStep(store, runId, stepId, step, transforms, attempt, attemptIndex <= retries);
        if (outcome.stopped !== undefined) {
          return stoppedPipelineResult(store, runId, completedSteps)!;
        }
        failure = outcome.failure;
        if (failure === undefined) break;
      }
      if (failure === undefined) {
        completedSteps.push(stepId);
        const recoveryFailure = resolveReturnedRecoveryFailures(
          store, runId, completedSteps, stepId, routingBudget.attempts.get(stepId)!, routingBudget.terminalEffects
        );
        if (recoveryFailure !== undefined) return recoveryFailure;
        const routed = routeAfterSuccessfulStep(store, runId, completedSteps, stepId, step, currentSteps, stepIndex, stepLocations, routingBudget);
        if ("result" in routed) return routed.result;
        currentSteps = routed.steps;
        stepIndex = routed.nextIndex;
        continue;
      }
      const recoveryRoute = await routeAfterFailedStep(
        store, runId, workflow, completedSteps, stepId, step, currentSteps, stepIndex,
        stepLocations, routingBudget, transforms, sessionProviders, mcpCalls, notifications, workflows
      );
      if (recoveryRoute !== undefined) {
        if ("result" in recoveryRoute) return recoveryRoute.result;
        currentSteps = recoveryRoute.steps;
        stepIndex = recoveryRoute.nextIndex;
        continue;
      }
      if (failureContinues(step)) {
        const routed = fallthroughAfterStep(store, runId, completedSteps, stepId, currentSteps, stepIndex, routingBudget);
        if ("result" in routed) return routed.result;
        currentSteps = routed.steps;
        stepIndex = routed.nextIndex;
        continue;
      }
      return finishFailure(store, runId, completedSteps, stepId, {
        exitCode: null,
        timedOut: false,
        message: failure
      }, failureStatus(step), routingBudget.terminalEffects);
    }
    if (stepType !== undefined && ["approval", "challenge", "consult", "review", "session_request"].includes(stepType)) {
      const isApproval = stepType === "approval";
      const isReview = stepType === "review";
      const isCollaborationExchange = stepType === "consult" || stepType === "challenge";
      const firstAttempt = allocateStepAttempt(routingBudget, stepId);
      if (firstAttempt === undefined) return stepAttemptLimitResult(store, runId, completedSteps, stepId, routingBudget);
      const preflightError = isApproval
        ? validateApprovalStep(workflow, step)
        : isReview
        ? validateReviewStep(workflow, step)
        : isCollaborationExchange ? validateCollaborationExchangeStep(workflow, step) : validateSessionRequestStep(step);
      if (preflightError !== undefined) {
        const actor = isApproval || isReview ? step.reviewer : isCollaborationExchange ? step.to : step.session;
        const sessionId = typeof actor === "string" && actor.trim().length > 0
          ? actor.trim()
          : undefined;
        persistSessionRequestFailure(store, runId, stepId, sessionId, preflightError, false, "fail", true, firstAttempt);
        return finishFailure(store, runId, completedSteps, stepId, {
          exitCode: null,
          timedOut: false,
          message: preflightError
        }, "failed", routingBudget.terminalEffects);
      }
      const retries = failureRetries(step);
      let failure: string | undefined;
      let sessionSelectedTarget: string | undefined;
      const firstAttemptIndex = nextRetryAttemptIndex(recoveredRetryAttemptIndex, retries, stepId);
      for (let attemptIndex = firstAttemptIndex; attemptIndex <= retries + 1; attemptIndex += 1) {
        const attempt = attemptIndex === firstAttemptIndex ? firstAttempt : allocateStepAttempt(routingBudget, stepId);
        if (attempt === undefined) return stepAttemptLimitResult(store, runId, completedSteps, stepId, routingBudget);
        const stopped = activeStopStatus(store, runId);
        if (stopped !== undefined) return stoppedPipelineResult(store, runId, completedSteps)!;
        store.updateRun(runId, { currentStepId: stepId, error: null });
        const sessionId = ((isApproval || isReview ? step.reviewer : isCollaborationExchange ? step.to : step.session) as string).trim();
        const input: Record<string, AgentFlowRunStateValue> = {
          attempt,
          session: sessionId,
          ...(isApproval
            ? { type: "approval", artifacts: step.artifacts as AgentFlowRunStateValue }
            : isReview
            ? { type: "review", subject: step.subject as string, artifacts: step.artifacts as AgentFlowRunStateValue }
            : isCollaborationExchange
              ? {
                type: stepType,
                from: step.from as string,
                question: step.question as string,
                artifacts: step.artifacts as AgentFlowRunStateValue,
                blocking: step.blocking === true
              }
            : { prompt: step.prompt as string, inputs: step.inputs as AgentFlowRunStateValue }),
          outputs: (isCollaborationExchange ? [step.output]
            : isApproval ? [typeof step.output === "string" ? step.output : defaultAgentFlowApprovalOutputPath(stepId)]
              : step.outputs) as AgentFlowRunStateValue
        };
        store.upsertStep({ runId, stepId, attempt, sessionId, status: "running", input });
        store.appendRunEvent(runId, { type: "step.started", stepId, payload: input });
        const approvalId = isApproval ? `approval:${safeId(stepId)}:attempt-${attempt}` : undefined;
        if (isApproval) {
          store.upsertApproval({
            id: approvalId!,
            runId,
            stepId,
            status: "requested",
            requestedBy: sessionId,
            context: { artifacts: step.artifacts as AgentFlowRunStateValue }
          });
        }
        try {
          const execute = isApproval
            ? executeAgentFlowApproval
            : isReview
            ? executeAgentFlowReview
            : stepType === "consult" ? executeAgentFlowConsult
              : stepType === "challenge" ? executeAgentFlowChallenge : executeAgentFlowSessionRequest;
          const result = await execute(store, runId, workflow, step, sessionProviders, {
            attempt,
            ...(approvalId === undefined ? {} : { requiredApprovalId: approvalId }),
            stopStatus: () => activeStopStatus(store, runId),
            interruptError: () => store.runLockInterruption(),
            beforePublish: () => {
              const lockError = store.runLockInterruption();
              if (lockError !== undefined) throw lockError;
              const status = activeStopStatus(store, runId);
              if (status !== undefined) throw new AgentFlowSessionRequestInterruptedError(status);
            },
            finalize: (result) => {
              const output = {
                attempt,
                session: result.sessionId,
                provider: result.provider,
                requestArtifact: result.requestArtifact.declaredPath,
                outputs: result.outputArtifacts.map((artifact) => artifact.declaredPath),
                externalSessionId: result.externalSessionId ?? null,
                ...(result.approvalResult === undefined ? {} : { approvalStatus: result.approvalResult.status }),
                ...(result.consultResult === undefined ? {} : { consultStatus: result.consultResult.status })
              };
              store.upsertStep({ runId, stepId, attempt, sessionId, status: "completed", output });
              store.appendRunEvent(runId, { type: "step.completed", stepId, payload: output });
              if (isApproval && result.approvalResult !== undefined) {
                store.upsertApproval({
                  id: approvalId!,
                  runId,
                  stepId,
                  status: result.approvalResult.status,
                  decidedBy: sessionId,
                  decision: result.approvalResult.decision,
                  context: {
                    artifacts: step.artifacts as AgentFlowRunStateValue,
                    evidence: result.inputEvidence as unknown as AgentFlowRunStateValue,
                    output: result.outputArtifacts[0]!.declaredPath
                  }
                });
              }
            }
          });
          completedSteps.push(stepId);
          if (result.approvalResult !== undefined) {
            sessionSelectedTarget = result.approvalResult.status === "approved"
              ? normalizedTarget(step.on_approve)
              : normalizedTarget(step.on_reject) ?? "cancel";
          }
          if (result.consultResult?.status === "blocked") {
            return finishBlockedConsult(store, runId, completedSteps, stepId, routingBudget.terminalEffects);
          }
          failure = undefined;
          break;
        } catch (error) {
          const lockError = store.runLockInterruption();
          if (lockError !== undefined) throw lockError;
          if (isApproval) {
            store.upsertApproval({
              id: approvalId!,
              runId,
              stepId,
              status: "cancelled",
              decision: error instanceof Error ? error.message : String(error)
            });
          }
          if (error instanceof AgentFlowSessionRequestInterruptedError) {
            const output = { attempt, status: error.status };
            persistSessionRequestInterruption(store, runId, workflow, stepId, sessionId, error.status);
            store.upsertStep({ runId, stepId, attempt, sessionId, status: error.status, output });
            store.appendRunEvent(runId, { type: "step.interrupted", stepId, payload: output });
            return stoppedPipelineResult(store, runId, completedSteps)!;
          }
          if (error instanceof AgentFlowSessionPolicyError) {
            failure = error.message;
            const outcome = error.status === "pause" ? "pause" : "fail";
            persistSessionRequestFailure(
              store, runId, stepId, sessionId, failure, false, outcome, true, attempt,
              modelLimitDetails(error)
            );
            recordModelLimitDecision(store, runId, stepId, workflow, error, false);
            return finishFailure(store, runId, completedSteps, stepId, {
              exitCode: null,
              timedOut: false,
              message: failure
            }, error.status === "pause" ? "paused" : "failed", routingBudget.terminalEffects);
          }
          if (error instanceof AgentFlowRunStateError && error.code === "AGENT_FLOW_ARTIFACT_RUN_STATUS") {
            const status = activeStopStatus(store, runId);
            if (status !== undefined) {
              const output = { attempt, status };
              persistSessionRequestInterruption(store, runId, workflow, stepId, sessionId, status);
              store.upsertStep({ runId, stepId, attempt, sessionId, status, output });
              store.appendRunEvent(runId, { type: "step.interrupted", stepId, payload: output });
              return stoppedPipelineResult(store, runId, completedSteps)!;
            }
          }
          const stopped = activeStopStatus(store, runId);
          if (stopped !== undefined) {
            const output = { attempt, status: stopped };
            persistSessionRequestInterruption(store, runId, workflow, stepId, sessionId, stopped);
            store.upsertStep({ runId, stepId, attempt, sessionId, status: stopped, output });
            store.appendRunEvent(runId, { type: "step.interrupted", stepId, payload: output });
            return stoppedPipelineResult(store, runId, completedSteps)!;
          }
          if (error instanceof AgentFlowRunStateError && error.code === "AGENT_FLOW_SESSION_ACTIVE") {
            throw error;
          }
          failure = error instanceof Error ? error.message : String(error);
          const sessionDefinition = mapping(workflow.sessions?.[sessionId]);
          const provider = typeof sessionDefinition?.provider === "string" ? sessionDefinition.provider.trim() : "unknown";
          const previousSession = store.getSession(runId, sessionId);
          store.upsertSession({
            id: sessionId,
            runId,
            stepId,
            provider,
            status: "paused",
            ...(previousSession?.externalSessionId === null || previousSession?.externalSessionId === undefined
              ? {}
              : { externalSessionId: previousSession.externalSessionId }),
            state: { resume: sessionDefinition?.resume === true, lastStepId: stepId, error: failure }
          });
          const retryable = attemptIndex <= retries;
          persistSessionRequestFailure(
            store,
            runId,
            stepId,
            sessionId,
            failure,
            retryable,
            failureOutcome(step, retryable),
            false,
            attempt
          );
        }
      }
      if (failure === undefined) {
        const recoveryFailure = resolveReturnedRecoveryFailures(
          store, runId, completedSteps, stepId, routingBudget.attempts.get(stepId)!, routingBudget.terminalEffects
        );
        if (recoveryFailure !== undefined) return recoveryFailure;
        const routed = routeAfterSuccessfulStep(store, runId, completedSteps, stepId, step, currentSteps, stepIndex, stepLocations, routingBudget, sessionSelectedTarget);
        if ("result" in routed) return routed.result;
        currentSteps = routed.steps;
        stepIndex = routed.nextIndex;
        continue;
      }
      const recoveryRoute = await routeAfterFailedStep(
        store, runId, workflow, completedSteps, stepId, step, currentSteps, stepIndex,
        stepLocations, routingBudget, transforms, sessionProviders, mcpCalls, notifications, workflows
      );
      if (recoveryRoute !== undefined) {
        if ("result" in recoveryRoute) return recoveryRoute.result;
        currentSteps = recoveryRoute.steps;
        stepIndex = recoveryRoute.nextIndex;
        continue;
      }
      if (failureContinues(step)) {
        const routed = fallthroughAfterStep(store, runId, completedSteps, stepId, currentSteps, stepIndex, routingBudget);
        if ("result" in routed) return routed.result;
        currentSteps = routed.steps;
        stepIndex = routed.nextIndex;
        continue;
      }
      return finishFailure(store, runId, completedSteps, stepId, {
        exitCode: null,
        timedOut: false,
        message: failure
      }, failureStatus(step), routingBudget.terminalEffects);
    }
    if (stepType === "condition") {
      const attempt = allocateStepAttempt(routingBudget, stepId);
      if (attempt === undefined) return stepAttemptLimitResult(store, runId, completedSteps, stepId, routingBudget);
      store.updateRun(runId, { currentStepId: stepId, error: null });
      store.upsertStep({ runId, stepId, attempt, status: "running", input: { type: "condition" } });
      store.appendRunEvent(runId, { type: "step.started", stepId, payload: { attempt, type: "condition" } });
      try {
        const selection = selectAgentFlowConditionTarget(store, runId, step);
        const stopped = activeStopStatus(store, runId);
        if (stopped !== undefined) {
          const output = { attempt, status: stopped, type: "condition" };
          store.upsertStep({ runId, stepId, attempt, status: stopped, output });
          store.appendRunEvent(runId, { type: "step.interrupted", stepId, payload: output });
          return interruptedPipelineResult(store, runId, completedSteps, stopped);
        }
        const output = {
          attempt,
          matched: selection.matched,
          expression: selection.expression ?? null,
          target: selection.target ?? null
        };
        store.upsertStep({ runId, stepId, attempt, status: "completed", output });
        store.appendRunEvent(runId, { type: "step.completed", stepId, payload: output });
        completedSteps.push(stepId);
        const routed = routeAfterSuccessfulStep(
          store,
          runId,
          completedSteps,
          stepId,
          step,
          currentSteps,
          stepIndex,
          stepLocations,
          routingBudget,
          selection.target
        );
        if ("result" in routed) return routed.result;
        currentSteps = routed.steps;
        stepIndex = routed.nextIndex;
        continue;
      } catch (error) {
        const stopped = activeStopStatus(store, runId);
        if (stopped !== undefined) {
          const output = { attempt, status: stopped, type: "condition" };
          store.upsertStep({ runId, stepId, attempt, status: stopped, output });
          store.appendRunEvent(runId, { type: "step.interrupted", stepId, payload: output });
          return interruptedPipelineResult(store, runId, completedSteps, stopped);
        }
        const message = error instanceof Error ? error.message : String(error);
        const classificationError = error instanceof AgentFlowFailureClassificationError ? error : undefined;
        const failureOutcome = classificationError === undefined ? "fail" as const : "pause" as const;
        const failureId = `condition:${safeId(stepId)}:evaluation`;
        const failure = { attempt, message, outcome: failureOutcome };
        const persisted = persistAgentFlowFailurePayload(store, {
          id: failureId,
          runId,
          stepId,
          stepType: "condition",
          attempt,
          summary: message,
          classification: classificationError?.code === "AGENT_FLOW_FAILURE_CLASSIFICATION_UNKNOWN"
            ? "failure_classification_unknown"
            : classificationError === undefined ? "condition_evaluation" : "failure_classification_invalid",
          retryable: false,
          outcome: failureOutcome,
          indexPayload: failure
        });
        const indexedFailure = { ...persisted.indexPayload, ...failureReference(persisted) };
        store.upsertStep({ runId, stepId, attempt, status: "failed", error: indexedFailure });
        store.appendRunEvent(runId, { type: "step.failed", stepId, payload: indexedFailure });
        return finishFailure(store, runId, completedSteps, stepId, {
          exitCode: null,
          timedOut: false,
          message
        }, classificationError === undefined ? "failed" : "paused", routingBudget.terminalEffects);
      }
    }
    if (stepType === "manual_gate" || stepType === "input_request") {
      const attempt = allocateStepAttempt(routingBudget, stepId);
      if (attempt === undefined) return stepAttemptLimitResult(store, runId, completedSteps, stepId, routingBudget);
      try {
        return pauseForInteraction(store, runId, step, stepType, attempt, completedSteps, routingBudget);
      } catch (error) {
        if (!(error instanceof AgentFlowRunStateError && error.code === "AGENT_FLOW_INTERACTION_INVALID")) {
          throw error;
        }
        persistAgentFlowFailurePayload(store, {
          id: `interaction:${safeId(stepId)}:attempt-${attempt}`,
          runId,
          stepId,
          stepType,
          attempt,
          exitCode: null,
          summary: error.message,
          classification: "interaction_failure",
          retryable: false,
          outcome: "fail",
          indexPayload: { attempt, message: error.message, outcome: "fail" }
        });
        return finishFailure(store, runId, completedSteps, stepId, {
          exitCode: null,
          timedOut: false,
          message: error.message
        }, "failed", routingBudget.terminalEffects);
      }
    }
    if (stepType === "result") {
      const attempt = allocateStepAttempt(routingBudget, stepId);
      if (attempt === undefined) return stepAttemptLimitResult(store, runId, completedSteps, stepId, routingBudget);
      return finishResultStep(
        store,
        runId,
        completedSteps,
        stepId,
        step,
        attempt,
        routingBudget.terminalEffects,
        beforeRemediatedResult
      );
    }
    if (stepType !== "command") {
      const attempt = (routingBudget.attempts.get(stepId) ?? 0) + 1;
      const message = `Step ${stepId} has unsupported type ${String(step.type)}; only command, artifact_transform, condition, input_request, manual_gate, mcp_call, and session_request steps can execute in this runtime phase.`;
      persistAgentFlowFailurePayload(store, {
        id: `runtime:${safeId(stepId)}:attempt-${attempt}`,
        runId,
        stepId,
        stepType: stepType ?? "unknown",
        attempt,
        exitCode: null,
        summary: message,
        classification: "unsupported_step",
        retryable: false,
        outcome: "pause",
        indexPayload: { attempt, message, outcome: "pause" }
      });
      return finishFailure(store, runId, completedSteps, stepId, {
        exitCode: null,
        timedOut: false,
        message
      }, "paused", routingBudget.terminalEffects);
    }

    const firstAttempt = allocateStepAttempt(routingBudget, stepId);
    if (firstAttempt === undefined) return stepAttemptLimitResult(store, runId, completedSteps, stepId, routingBudget);
    const preflightError = validateCommandStep(store.repoRoot, workflow, step);
    if (preflightError !== undefined) {
      persistPreflightFailure(
        store,
        runId,
        stepId,
        typeof step.command === "string" ? step.command : null,
        firstAttempt,
        preflightError.message,
        preflightError.status === "paused" ? "pause" : "fail"
      );
      return finishFailure(store, runId, completedSteps, stepId, {
        exitCode: null,
        timedOut: false,
        message: preflightError.message
      }, preflightError.status, routingBudget.terminalEffects);
    }

    const retries = failureRetries(step);
    let lastResult: CommandAttemptResult | undefined;
    const firstAttemptIndex = nextRetryAttemptIndex(recoveredRetryAttemptIndex, retries, stepId);
    for (let attemptIndex = firstAttemptIndex; attemptIndex <= retries + 1; attemptIndex += 1) {
      const attempt = attemptIndex === firstAttemptIndex ? firstAttempt : allocateStepAttempt(routingBudget, stepId);
      if (attempt === undefined) return stepAttemptLimitResult(store, runId, completedSteps, stepId, routingBudget);
      store.updateRun(runId, { currentStepId: stepId, error: null });
      store.upsertStep({ runId, stepId, attempt, status: "running", input: { command: step.command as string } });
      store.appendRunEvent(runId, { type: "step.started", stepId, payload: { attempt, command: step.command as string } });

      lastResult = await runCommand(
        store.repoRoot,
        step.command as string,
        timeoutMilliseconds(step),
        () => activeStopStatus(store, runId),
        () => store.runLockInterruption()
      );
      const stoppedAfterCommand = activeStopStatus(store, runId);
      if (stoppedAfterCommand !== undefined) {
        let logPersistenceError: string | undefined;
        try {
          persistCommandLog(store, runId, stepId, attempt, "stdout", lastResult.stdout);
          persistCommandLog(store, runId, stepId, attempt, "stderr", lastResult.stderr);
        } catch (error) {
          logPersistenceError = `Could not persist interrupted command logs: ${(error as Error).message}`;
        }
        store.upsertStep({
          runId,
          stepId,
          attempt,
          status: stoppedAfterCommand,
          output: { ...commandOutput(lastResult, attempt), logPersistenceError: logPersistenceError ?? null }
        });
        store.appendRunEvent(runId, {
          type: "step.interrupted",
          stepId,
          payload: { attempt, status: stoppedAfterCommand, logPersistenceError: logPersistenceError ?? null }
        });
        return stoppedPipelineResult(store, runId, completedSteps)!;
      }
      try {
        persistCommandLog(store, runId, stepId, attempt, "stdout", lastResult.stdout);
        persistCommandLog(store, runId, stepId, attempt, "stderr", lastResult.stderr);
      } catch (error) {
        lastResult.message = `Could not persist command logs: ${(error as Error).message}`;
      }

      if (lastResult.exitCode === 0 && !lastResult.timedOut && lastResult.message === undefined) {
        const artifactError = persistDeclaredOutputs(store, runId, stepId, step, attempt);
        if (artifactError === undefined) {
          const output = commandOutput(lastResult, attempt);
          store.upsertStep({ runId, stepId, attempt, status: "completed", output });
          store.appendRunEvent(runId, { type: "step.completed", stepId, payload: output });
          completedSteps.push(stepId);
          lastResult = undefined;
          break;
        }
        lastResult.message = artifactError;
      }

      const retryable = attemptIndex <= retries;
      const outcome = failureOutcome(step, retryable);
      const failureId = `command:${safeId(stepId)}:attempt-${attempt}`;
      const error = { ...commandError(lastResult, attempt), outcome };
      const persisted = persistAgentFlowFailurePayload(store, {
        id: failureId,
        runId,
        stepId,
        stepType: "command",
        attempt,
        exitCode: lastResult.exitCode,
        command: step.command as string,
        summary: error.message as string,
        classification: lastResult.timedOut ? "command_timeout" : "command_failure",
        retryable,
        outcome,
        logs: {
          stdout: commandLogPath(stepId, attempt, "stdout"),
          stderr: commandLogPath(stepId, attempt, "stderr")
        },
        indexPayload: error
      });
      const indexedError = { ...persisted.indexPayload, ...failureReference(persisted) };
      store.upsertStep({ runId, stepId, attempt, status: "failed", error: indexedError });
      store.appendRunEvent(runId, {
        type: lastResult.timedOut ? "step.timed_out" : "step.failed",
        stepId,
        payload: indexedError
      });
    }

    if (lastResult !== undefined) {
      const recoveryRoute = await routeAfterFailedStep(
        store, runId, workflow, completedSteps, stepId, step, currentSteps, stepIndex,
        stepLocations, routingBudget, transforms, sessionProviders, mcpCalls, notifications, workflows
      );
      if (recoveryRoute !== undefined) {
        if ("result" in recoveryRoute) return recoveryRoute.result;
        currentSteps = recoveryRoute.steps;
        stepIndex = recoveryRoute.nextIndex;
        continue;
      }
      if (failureContinues(step)) {
        const routed = fallthroughAfterStep(store, runId, completedSteps, stepId, currentSteps, stepIndex, routingBudget);
        if ("result" in routed) return routed.result;
        currentSteps = routed.steps;
        stepIndex = routed.nextIndex;
        continue;
      }
      return finishFailure(store, runId, completedSteps, stepId, {
        exitCode: lastResult.exitCode,
        timedOut: lastResult.timedOut,
        message: lastResult.message ?? failureMessage(lastResult)
      }, failureStatus(step), routingBudget.terminalEffects);
    }

    const recoveryFailure = resolveReturnedRecoveryFailures(
      store, runId, completedSteps, stepId, routingBudget.attempts.get(stepId)!, routingBudget.terminalEffects
    );
    if (recoveryFailure !== undefined) return recoveryFailure;
    const routed = routeAfterSuccessfulStep(store, runId, completedSteps, stepId, step, currentSteps, stepIndex, stepLocations, routingBudget);
    if ("result" in routed) return routed.result;
    currentSteps = routed.steps;
    stepIndex = routed.nextIndex;
  }

  return finishCompleted(store, runId, completedSteps, routingBudget.terminalEffects);
}

type SuccessfulRoute =
  | { steps: AgentFlowWorkflowStep[]; nextIndex: number }
  | { result: AgentFlowCommandPipelineResult };

interface RuntimeStepLocation {
  steps: AgentFlowWorkflowStep[];
  index: number;
}

interface SuccessfulRoutingBudget {
  terminalEffects: AgentFlowPipelineTerminalEffects;
  maxRecoveryCycles?: number;
  maxReviewCycles?: number;
  stepAttemptLimits: Map<string, number>;
  reviewCyclePathReviewIds: Map<string, Set<string>>;
  reviewCycleStepIds: Set<string>;
  visits: Map<string, number>;
  recoveryCycles: Map<string, number>;
  recoveryInvocations: Map<string, number>;
  disagreementEpisodes: Map<string, number>;
  disagreementRounds: Map<string, number>;
  attempts: Map<string, number>;
}

interface AgentFlowPipelineTerminalEffects {
  workflow: AgentFlowWorkflow;
  notifications: AgentFlowNotificationRegistry;
}

interface SerializedSuccessfulRoutingBudget {
  maxRecoveryCycles?: number;
  maxReviewCycles?: number;
  stepAttemptLimits: Record<string, number>;
  visits: Record<string, number>;
  recoveryCycles: Record<string, number>;
  recoveryInvocations: Record<string, number>;
  disagreementEpisodes: Record<string, number>;
  disagreementRounds: Record<string, number>;
  attempts: Record<string, number>;
}

interface RecoveredAgentFlowExecution {
  attempts: Record<string, number>;
  routing?: SerializedSuccessfulRoutingBudget;
  cursorStepId?: string;
  completedCursorPayload?: AgentFlowRunStateValue;
  interruptedRecoveryCursor?: true;
  pendingExecutionCursor?: true;
  pendingRetryAttemptIndex?: number;
  completedSteps: string[];
}

type ResumedWaitingStep =
  | { steps: AgentFlowWorkflowStep[]; nextIndex: number; completedSteps: string[]; routingBudget: SuccessfulRoutingBudget }
  | { result: AgentFlowCommandPipelineResult };

type ReviewDisagreementControl =
  | { resolved: true }
  | { result: AgentFlowCommandPipelineResult };

type PersistedDisagreementRoundOutcome =
  | { status: "failed" | "unresolved" }
  | {
      status: "resolved";
      result: AgentFlowDisagreementResult;
      evidence: AgentFlowWaitingEvidence[];
    };

async function resolveReviewDisagreement(
  store: AgentFlowRunStateStore,
  runId: string,
  workflow: AgentFlowWorkflow,
  reviewStep: AgentFlowWorkflowStep,
  completedSteps: string[],
  routingBudget: SuccessfulRoutingBudget,
  sessionProviders: AgentFlowSessionProviderRegistry,
  resumePersistedRound = false
): Promise<ReviewDisagreementControl> {
  const stepId = requiredStepId(reviewStep);
  const reviewer = requiredStaticString(reviewStep.reviewer, `Review ${stepId} reviewer`);
  const subject = requiredStaticString(reviewStep.subject, `Review ${stepId} subject`);
  const collaboration = mapping(workflow.collaboration);
  const policy = parseAgentFlowDisagreementPolicy(collaboration?.on_disagreement);
  const completedReviewCycles = routingBudget.attempts.get(stepId) ?? 0;
  const persistedRound = routingBudget.disagreementRounds.get(stepId) ?? 0;
  const resumingRound = resumePersistedRound || persistedRound > 0;
  if (!resumingRound) {
  const stoppedBeforeDisagreement = store.withRunFinalizationTransaction(runId, () => {
    const stopped = stoppedPipelineResult(store, runId, completedSteps);
    if (stopped !== undefined) return stopped;
    const payload = {
      reviewer,
      subject,
      completedReviewCycles,
      maxReviewCycles: routingBudget.maxReviewCycles!,
      strategy: policy.strategy
    };
    store.appendRunEvent(runId, {
      type: "collaboration.disagreement",
      stepId,
      payload
    });
    return undefined;
  });
  if (stoppedBeforeDisagreement !== undefined) return { result: stoppedBeforeDisagreement };
  const stoppedBeforeNotification = stoppedPipelineResult(store, runId, completedSteps);
  if (stoppedBeforeNotification !== undefined) return { result: stoppedBeforeNotification };
  const disagreementNotification = deliverAgentFlowNotificationEvent(
    store,
    runId,
    workflow,
    "collaboration.disagreement",
    routingBudget.terminalEffects.notifications,
    {
      stepId,
      payload: {
        reviewer,
        subject,
        completedReviewCycles,
        maxReviewCycles: routingBudget.maxReviewCycles!,
        strategy: policy.strategy
      },
      requiredRunStatus: "running"
    }
  );
  const stoppedAfterNotification = stoppedPipelineResult(store, runId, completedSteps);
  if (stoppedAfterNotification !== undefined) {
    if (stoppedAfterNotification.status === "paused" && policy.strategy === "ask_user") {
      return {
        result: pauseForReviewDisagreement(
          store,
          runId,
          reviewStep,
          completedSteps,
          routingBudget,
          policy,
          true
        )
      };
    }
    return { result: stoppedAfterNotification };
  }
  if (disagreementNotification.requiredFailure !== undefined) {
    return {
      result: finishRequiredStepNotificationFailure(
        store,
        runId,
        completedSteps,
        stepId,
        Math.max(1, completedReviewCycles + 1),
        "review",
        disagreementNotification.requiredFailure,
        routingBudget.terminalEffects
      )
    };
  }

  if (policy.strategy === "ask_user") {
    return { result: pauseForReviewDisagreement(store, runId, reviewStep, completedSteps, routingBudget, policy) };
  }
  if (policy.strategy === "fail") {
    return {
      result: finishReviewDisagreementFailure(
        store,
        runId,
        completedSteps,
        stepId,
        `Review ${stepId} exceeded collaboration.max_review_cycles ${routingBudget.maxReviewCycles}; disagreement strategy fail ended the workflow.`,
        routingBudget,
        policy
      )
    };
  }
  }

  const resolver = policy.strategy === "owner_decides" ? subject : policy.arbiter!;
  const maxRounds = policy.strategy === "owner_decides" ? 1 : policy.maxRounds!;
  const persistedEpisode = routingBudget.disagreementEpisodes.get(stepId) ?? 0;
  if (resumingRound && (persistedEpisode < 1 || persistedRound < 1 || persistedRound > maxRounds)) {
    throw new AgentFlowRunStateError(
      `Review ${stepId} cannot recover an invalid persisted disagreement round.`,
      "AGENT_FLOW_RUN_LOCK_RECOVERY"
    );
  }
  const episode = resumingRound ? persistedEpisode : persistedEpisode + 1;
  if (!resumingRound) {
    routingBudget.disagreementEpisodes.set(stepId, episode);
  }
  const persistedOutcome = resumingRound
    ? persistedDisagreementRoundOutcome(store, runId, stepId, policy.strategy, resolver, episode, persistedRound)
    : undefined;
  if (persistedOutcome?.status === "resolved") {
    const priorAttempt = routingBudget.attempts.get(stepId);
    try {
      completeReviewDisagreementResolution(
        store,
        runId,
        reviewStep,
        routingBudget,
        completedReviewCycles,
        persistedOutcome.result,
        persistedOutcome.evidence,
        resolver,
        policy.strategy,
        episode,
        persistedRound
      );
      return { resolved: true };
    } catch (error) {
      routingBudget.disagreementRounds.set(stepId, persistedRound);
      if (priorAttempt === undefined) routingBudget.attempts.delete(stepId);
      else routingBudget.attempts.set(stepId, priorAttempt);
      const lockError = store.runLockInterruption();
      if (lockError !== undefined) throw lockError;
      const message = redactAgentFlowSensitiveText(error instanceof Error ? error.message : String(error));
      store.appendRunEvent(runId, {
        type: "collaboration.disagreement.round_failed",
        stepId,
        payload: { strategy: policy.strategy, resolver, episode, round: persistedRound, message }
      });
      if (activeStopStatus(store, runId) !== undefined) {
        return { result: stoppedPipelineResult(store, runId, completedSteps)! };
      }
    }
  }
  let round = persistedRound;
  let resumeCurrentRound = resumingRound && persistedOutcome === undefined;
  while (resumeCurrentRound || round < maxRounds) {
    if (resumeCurrentRound) {
      resumeCurrentRound = false;
    } else {
      round += 1;
      routingBudget.disagreementRounds.set(stepId, round);
      checkpointExecutionRouting(store, runId, routingBudget, stepId);
    }
    const outputPath = defaultAgentFlowDisagreementOutputPath(stepId, round, episode);
    const priorAttempt = routingBudget.attempts.get(stepId);
    try {
      let evidence: AgentFlowWaitingEvidence[] | undefined;
      const resolved = await executeAgentFlowDisagreementResolution(
        store,
        runId,
        workflow,
        reviewStep,
        resolver,
        round,
        outputPath,
        sessionProviders,
        {
          stopStatus: () => activeStopStatus(store, runId),
          interruptError: () => store.runLockInterruption(),
          finalize: (finalized) => {
            const resolutionEvidence = finalized.outputArtifacts.map((artifact) => {
              if (artifact.checksum === null) {
                throw new AgentFlowRunStateError(
                  `Disagreement resolution artifact ${artifact.declaredPath} must have a persisted checksum.`,
                  "AGENT_FLOW_DISAGREEMENT_INVALID"
                );
              }
              return { path: artifact.declaredPath, checksum: artifact.checksum };
            });
            evidence = [...finalized.inputEvidence, ...resolutionEvidence];
            store.appendRunEvent(runId, {
              type: "collaboration.disagreement.round_completed",
              stepId,
              payload: {
                strategy: policy.strategy,
                resolver,
                episode,
                round,
                output: outputPath,
                status: finalized.disagreementResult!.status,
                evidence: evidence as unknown as AgentFlowRunStateValue
              }
            });
          }
        }
      );
      const result = resolved.disagreementResult!;
      if (evidence === undefined) {
        throw new AgentFlowRunStateError(
          `Disagreement resolution for ${stepId} did not persist its settlement evidence.`,
          "AGENT_FLOW_DISAGREEMENT_INVALID"
        );
      }
      if (result.status === "unresolved") continue;
      completeReviewDisagreementResolution(
        store,
        runId,
        reviewStep,
        routingBudget,
        completedReviewCycles,
        result,
        evidence,
        resolver,
        policy.strategy,
        episode,
        round
      );
      return { resolved: true };
    } catch (error) {
      routingBudget.disagreementRounds.set(stepId, round);
      if (priorAttempt === undefined) routingBudget.attempts.delete(stepId);
      else routingBudget.attempts.set(stepId, priorAttempt);
      const lockError = store.runLockInterruption();
      if (lockError !== undefined) throw lockError;
      const message = redactAgentFlowSensitiveText(error instanceof Error ? error.message : String(error));
      if (error instanceof AgentFlowSessionPolicyError) {
        const attempt = completedReviewCycles + round;
        const outcome = error.status === "pause" ? "pause" : "fail";
        persistSessionRequestFailure(
          store, runId, stepId, resolver, message, false, outcome, true, attempt,
          modelLimitDetails(error)
        );
        recordModelLimitDecision(store, runId, stepId, workflow, error, false);
        return {
          result: finishFailure(store, runId, completedSteps, stepId, {
            exitCode: null,
            timedOut: false,
            message
          }, error.status === "pause" ? "paused" : "failed", routingBudget.terminalEffects)
        };
      }
      store.appendRunEvent(runId, {
        type: "collaboration.disagreement.round_failed",
        stepId,
        payload: { strategy: policy.strategy, resolver, episode, round, message }
      });
      if (activeStopStatus(store, runId) !== undefined) {
        return { result: stoppedPipelineResult(store, runId, completedSteps)! };
      }
    }
  }

  if (policy.strategy === "arbiter_then_user") {
    return { result: pauseForReviewDisagreement(store, runId, reviewStep, completedSteps, routingBudget, policy) };
  }
  return {
    result: finishReviewDisagreementFailure(
      store,
      runId,
      completedSteps,
      stepId,
      `Review ${stepId} disagreement remained unresolved after ${maxRounds} ${policy.strategy === "owner_decides" ? "owner" : "arbiter"} round(s).`,
      routingBudget,
      policy
    )
  };
}

function persistedDisagreementRoundOutcome(
  store: AgentFlowRunStateStore,
  runId: string,
  stepId: string,
  strategy: AgentFlowDisagreementPolicy["strategy"],
  resolver: string,
  episode: number,
  round: number
): PersistedDisagreementRoundOutcome | undefined {
  const event = [...store.listEvents(runId)].reverse().find((candidate) => {
    if (candidate.stepId !== stepId || ![
      "collaboration.disagreement.round_completed",
      "collaboration.disagreement.round_failed"
    ].includes(candidate.type)) return false;
    const payload = mapping(candidate.payload);
    return payload?.strategy === strategy
      && payload.resolver === resolver
      && payload.episode === episode
      && payload.round === round;
  });
  if (event === undefined) return undefined;
  if (event.type === "collaboration.disagreement.round_failed") return { status: "failed" };
  const payload = mapping(event.payload);
  if (payload?.status === "unresolved") return { status: "unresolved" };
  const outputPath = defaultAgentFlowDisagreementOutputPath(stepId, round, episode);
  if (payload?.status !== "resolved" || payload.output !== outputPath) {
    throw invalidPersistedDisagreementRound(stepId);
  }
  const evidence = persistedDisagreementEvidence(payload.evidence, stepId);
  let result: AgentFlowDisagreementResult;
  try {
    result = parseAgentFlowDisagreementResult(store.readArtifact(runId, outputPath), outputPath);
  } catch (error) {
    throw new AgentFlowRunStateError(
      `Review ${stepId} cannot recover its settled disagreement round because the resolution evidence is unavailable: ${error instanceof Error ? error.message : String(error)}.`,
      "AGENT_FLOW_RUN_LOCK_RECOVERY",
      { cause: error }
    );
  }
  if (result.status !== "resolved") throw invalidPersistedDisagreementRound(stepId);
  return { status: "resolved", result, evidence };
}

function persistedDisagreementEvidence(value: unknown, stepId: string): AgentFlowWaitingEvidence[] {
  if (!Array.isArray(value) || value.length === 0) throw invalidPersistedDisagreementRound(stepId);
  const evidence = value.map((entry) => {
    const item = mapping(entry);
    if (typeof item?.path !== "string" || item.path.length === 0
        || typeof item.checksum !== "string" || item.checksum.length === 0) {
      throw invalidPersistedDisagreementRound(stepId);
    }
    return { path: item.path, checksum: item.checksum };
  });
  if (new Set(evidence.map(({ path }) => path)).size !== evidence.length) {
    throw invalidPersistedDisagreementRound(stepId);
  }
  return evidence;
}

function invalidPersistedDisagreementRound(stepId: string): AgentFlowRunStateError {
  return new AgentFlowRunStateError(
    `Review ${stepId} cannot recover an invalid persisted disagreement round outcome.`,
    "AGENT_FLOW_RUN_LOCK_RECOVERY"
  );
}

function completeReviewDisagreementResolution(
  store: AgentFlowRunStateStore,
  runId: string,
  reviewStep: AgentFlowWorkflowStep,
  routingBudget: SuccessfulRoutingBudget,
  completedReviewCycles: number,
  result: AgentFlowDisagreementResult,
  evidence: AgentFlowWaitingEvidence[],
  resolver: string,
  strategy: AgentFlowDisagreementPolicy["strategy"],
  episode: number,
  round: number
): void {
  const stepId = requiredStepId(reviewStep);
  const attempt = completedReviewCycles + round;
  const outputPath = defaultAgentFlowDisagreementOutputPath(stepId, round, episode);
  store.withRunFinalizationTransaction(runId, () => {
    publishReviewDisagreementDecision(
      store,
      runId,
      reviewStep,
      result.decision!,
      result.rationale,
      resolver,
      strategy,
      round,
      evidence
    );
    const output = {
      attempt,
      resolution: result.decision!,
      resolutionArtifact: outputPath,
      resolver,
      strategy,
      episode,
      round
    };
    store.upsertStep({ runId, stepId, attempt, sessionId: resolver, status: "completed", output });
    store.appendRunEvent(runId, { type: "step.completed", stepId, payload: output });
    store.appendRunEvent(runId, {
      type: "collaboration.disagreement.resolved",
      stepId,
      payload: {
        strategy,
        path: strategy === "owner_decides" ? "owner" : "arbiter",
        resolver,
        episode,
        round,
        decision: result.decision!,
        output: outputPath
      }
    });
    routingBudget.disagreementRounds.set(stepId, 0);
    routingBudget.attempts.set(stepId, attempt);
    checkpointExecutionRouting(store, runId, routingBudget);
  });
}

function publishReviewDisagreementDecision(
  store: AgentFlowRunStateStore,
  runId: string,
  reviewStep: AgentFlowWorkflowStep,
  decision: AgentFlowDisagreementDecision,
  rationale: string,
  resolver: string,
  strategy: AgentFlowDisagreementPolicy["strategy"],
  round: number,
  requiredArtifacts?: AgentFlowWaitingEvidence[]
): void {
  const stepId = requiredStepId(reviewStep);
  const outputs = normalizedStringList(reviewStep.outputs);
  const reviewResult = {
    status: decision,
    findings: decision === "changes_requested" ? [{ summary: rationale }] : [],
    summary: rationale
  };
  const content = `${JSON.stringify(reviewResult)}\n`;
  const contentBytes = Buffer.byteLength(content);
  let totalBytes = 0;
  for (const outputPath of outputs) {
    totalBytes += contentBytes;
    validateAgentFlowSessionOutputSize(stepId, outputPath, contentBytes, totalBytes, "Synthesized review");
  }
  store.writeArtifactsAtomically(outputs.map((outputPath, index) => {
    const existing = store.getArtifact(runId, outputPath);
    const mayReplaceExisting = reviewStep.overwrite === true || existing?.producerStepId === stepId;
    return {
      id: mayReplaceExisting && existing !== null
        ? existing.id
        : `review-output:${safeId(stepId)}:${safeId(outputPath)}`,
      runId,
      stepId,
      path: outputPath,
      kind: "review_output",
      contentType: "application/json; charset=utf-8",
      content,
      overwrite: mayReplaceExisting,
      requiredRunStatus: "running" as const,
      ...(index === 0 && requiredArtifacts !== undefined ? { requiredArtifacts } : {}),
      metadata: { sessionId: resolver, disagreementStrategy: strategy, disagreementRound: round }
    };
  }));
}

function reviewDisagreementEvidencePaths(reviewStep: AgentFlowWorkflowStep): string[] {
  return [...new Set([
    ...normalizedStringList(reviewStep.artifacts),
    ...normalizedStringList(reviewStep.outputs)
  ])];
}

function pauseForReviewDisagreement(
  store: AgentFlowRunStateStore,
  runId: string,
  reviewStep: AgentFlowWorkflowStep,
  completedSteps: string[],
  routingBudget: SuccessfulRoutingBudget,
  policy: AgentFlowDisagreementPolicy,
  acceptExistingPause = false
): AgentFlowCommandPipelineResult {
  const stepId = requiredStepId(reviewStep);
  const attempt = (routingBudget.attempts.get(stepId) ?? 0) + 1;
  routingBudget.attempts.set(stepId, attempt);
  const validOutcomes = ["approve", "request_changes", "fail", "cancel"];
  const prompt = `Review ${stepId} reached its disagreement limit. Choose one outcome: ${validOutcomes.join(", ")}.`;
  const evidence = reviewDisagreementEvidencePaths(reviewStep).map((artifactPath) => {
    const artifact = store.readArtifact(runId, artifactPath).artifact;
    if (artifact.checksum === null) {
      throw new AgentFlowRunStateError(
        `Review disagreement ${stepId} artifacts must have persisted checksums.`,
        "AGENT_FLOW_DISAGREEMENT_INVALID"
      );
    }
    return { path: artifact.declaredPath, checksum: artifact.checksum };
  });
  const waiting: AgentFlowPipelineWaitingState = {
    kind: "disagreement",
    stepId,
    attempt,
    reason: "disagreement",
    prompt,
    validOutcomes,
    evidence,
    completedSteps: [...completedSteps],
    routing: serializeRoutingBudget(routingBudget)
  };
  return store.withRunFinalizationTransaction(runId, () => {
    const stopped = stoppedPipelineResult(store, runId, completedSteps);
    if (stopped !== undefined && !(acceptExistingPause && stopped.status === "paused")) return stopped;
    const run = store.getRun(runId)!;
    store.updateRun(runId, {
      currentStepId: stepId,
      context: { ...run.context, waiting: waiting as unknown as AgentFlowRunStateValue },
      error: null
    });
    store.upsertStep({
      runId,
      stepId,
      attempt,
      status: "waiting",
      input: { attempt, type: "disagreement", question: prompt, options: validOutcomes }
    });
    store.appendRunEvent(runId, {
      type: "collaboration.disagreement.waiting",
      stepId,
      payload: { strategy: policy.strategy, path: "user", attempt, prompt, validOutcomes }
    });
    const message = `Review ${stepId} disagreement is waiting for user resolution.`;
    if (stopped?.status === "paused") {
      return { status: "paused", completedSteps, message };
    }
    const finalized = finalizePipelineRun(store, runId, routingBudget.terminalEffects, {
      intendedStatus: "paused",
      completedSteps,
      currentStepId: stepId,
      message,
      eventPayload: { stepId, reason: "disagreement", strategy: policy.strategy, prompt, validOutcomes },
      eventStepId: stepId,
      failureContext: run.context
    });
    return { status: finalized.status, completedSteps, message: finalized.message ?? message };
  });
}

function finishReviewDisagreementFailure(
  store: AgentFlowRunStateStore,
  runId: string,
  completedSteps: string[],
  stepId: string,
  message: string,
  routingBudget: SuccessfulRoutingBudget,
  policy: AgentFlowDisagreementPolicy
): AgentFlowCommandPipelineResult {
  const attempt = Math.max(1, routingBudget.attempts.get(stepId) ?? 0);
  const failure = { attempt, message, outcome: "fail" as const };
  persistAgentFlowFailurePayload(store, {
    id: `disagreement:${safeId(stepId)}:attempt-${attempt}`,
    runId,
    stepId,
    stepType: "review",
    attempt,
    exitCode: null,
    summary: message,
    classification: "collaboration_disagreement",
    retryable: false,
    outcome: "fail",
    indexPayload: failure
  });
  store.appendRunEvent(runId, {
    type: "collaboration.disagreement.resolved",
    stepId,
    payload: { strategy: policy.strategy, path: "fail", message }
  });
  return finishFailure(
    store,
    runId,
    completedSteps,
    stepId,
    { exitCode: null, timedOut: false, message },
    "failed",
    routingBudget.terminalEffects
  );
}

function pauseForInteraction(
  store: AgentFlowRunStateStore,
  runId: string,
  step: AgentFlowWorkflowStep,
  kind: "approval" | "manual_gate" | "input_request",
  attempt: number,
  completedSteps: string[],
  routingBudget: SuccessfulRoutingBudget
): AgentFlowCommandPipelineResult {
  const stepId = requiredStepId(step);
  const run = store.getRun(runId)!;
  const prompt = resolveInteractionPrompt(
    kind === "manual_gate" ? step.message : kind === "approval" ? step.message ?? `Approve artifacts for step ${stepId}?` : step.question,
    run.inputs,
    `${kind === "manual_gate" ? "Manual gate message" : kind === "approval" ? "Approval message" : "Input request question"} for step ${stepId}`
  );
  const validOutcomes = kind === "manual_gate" ? normalizedStringList(step.options) : kind === "approval" ? ["approve", "reject", "cancel"] : [];
  const saveAs = kind === "input_request"
    ? requiredStaticString(step.save_as, `Input request artifact for step ${stepId}`)
    : undefined;
  const approvalId = kind === "manual_gate" ? `manual-gate:${safeId(stepId)}:attempt-${attempt}`
    : kind === "approval" ? `approval:${safeId(stepId)}:attempt-${attempt}` : undefined;
  const evidence = kind === "approval"
    ? normalizedStringList(step.artifacts).map((artifactPath) => {
      const artifact = store.readArtifact(runId, artifactPath).artifact;
      if (artifact.checksum === null) {
        throw new AgentFlowRunStateError(
          `Approval ${stepId} artifacts must have persisted checksums.`,
          "AGENT_FLOW_APPROVAL_INVALID"
        );
      }
      return { path: artifact.declaredPath, checksum: artifact.checksum };
    })
    : undefined;
  const waiting: AgentFlowPipelineWaitingState = {
    kind,
    stepId,
    attempt,
    reason: kind === "manual_gate" ? "manual_approval" : kind === "approval" ? "approval" : "missing_input",
    prompt,
    validOutcomes,
    ...(saveAs === undefined ? {} : { saveAs }),
    ...(approvalId === undefined ? {} : { approvalId }),
    ...(evidence === undefined ? {} : { evidence }),
    completedSteps: [...completedSteps],
    routing: serializeRoutingBudget(routingBudget)
  };
  const input: Record<string, AgentFlowRunStateValue> = kind === "manual_gate" || kind === "approval"
    ? { attempt, type: kind, message: prompt, options: validOutcomes }
    : { attempt, type: kind, question: prompt, saveAs: saveAs! };

  const persistWaiting = (): AgentFlowCommandPipelineResult => {
    const stopped = stoppedPipelineResult(store, runId, completedSteps);
    if (stopped !== undefined) return stopped;
    store.updateRun(runId, {
      currentStepId: stepId,
      context: { ...run.context, waiting: waiting as unknown as AgentFlowRunStateValue },
      error: null
    });
    store.upsertStep({ runId, stepId, attempt, status: "waiting", input });
    store.appendRunEvent(runId, { type: "step.waiting", stepId, payload: input });
    if (approvalId !== undefined) {
      store.upsertApproval({
        id: approvalId,
        runId,
        stepId,
        status: "requested",
        ...(kind === "approval" ? { requestedBy: "human" } : {}),
        context: {
          message: prompt,
          options: validOutcomes,
          ...(evidence === undefined ? {} : {
            evidence: evidence as unknown as AgentFlowRunStateValue,
            output: typeof step.output === "string" && step.output.trim().length > 0
              ? step.output.trim()
              : defaultAgentFlowApprovalOutputPath(stepId)
          })
        }
      });
    }
    const failWaitingInteraction = (failureMessage: string): void => {
      const error = {
        attempt,
        message: failureMessage,
        outcome: "fail" as const
      };
      const persisted = persistAgentFlowFailurePayload(store, {
        id: `interaction:${safeId(stepId)}:attempt-${attempt}:notification`,
        runId,
        stepId,
        stepType: kind,
        attempt,
        exitCode: null,
        summary: failureMessage,
        classification: "notification_failure",
        retryable: false,
        outcome: "fail",
        indexPayload: error
      });
      const indexedError = { ...persisted.indexPayload, ...failureReference(persisted) };
      store.upsertStep({ runId, stepId, attempt, status: "failed", error: indexedError });
      store.appendRunEvent(runId, { type: "step.failed", stepId, payload: indexedError });
      if (approvalId !== undefined) {
        store.upsertApproval({
          id: approvalId,
          runId,
          stepId,
          status: "cancelled",
          decision: "notification_failure"
        });
      }
    };
    if (kind === "approval") {
      const approvalNotification = deliverAgentFlowNotificationEvent(
        store,
        runId,
        routingBudget.terminalEffects.workflow,
        "approval.waiting",
        routingBudget.terminalEffects.notifications,
        { stepId, payload: { attempt, prompt, validOutcomes }, requiredRunStatus: "running" }
      );
      if (approvalNotification.requiredFailure !== undefined) {
        return finishRequiredStepNotificationFailure(
          store,
          runId,
          completedSteps,
          stepId,
          attempt,
          kind,
          approvalNotification.requiredFailure,
          routingBudget.terminalEffects,
          {
            failureContext: run.context,
            beforeFailure: () => store.upsertApproval({
              id: approvalId!,
              runId,
              stepId,
              status: "cancelled",
              decision: "notification_failure"
            })
          }
        );
      }
    }
    const resultMessage = kind === "manual_gate" || kind === "approval"
      ? `${kind === "approval" ? "Approval" : "Manual gate"} ${stepId} is waiting for one of: ${validOutcomes.join(", ")}.`
      : `Input request ${stepId} is waiting for an answer to be saved as ${saveAs}.`;
    const finalized = finalizePipelineRun(store, runId, routingBudget.terminalEffects, {
      intendedStatus: "paused",
      completedSteps,
      currentStepId: stepId,
      message: resultMessage,
      eventPayload: {
        stepId,
        reason: waiting.reason,
        prompt,
        validOutcomes,
        ...(saveAs === undefined ? {} : { saveAs })
      },
      eventStepId: stepId,
      failureContext: run.context,
      beforeFinalTransition: (status, message) => {
        if (status !== "failed") return;
        failWaitingInteraction(message ?? "Required paused notification failed.");
      }
    });
    const finalizedRun = store.getRun(runId);
    if (finalized.status === "failed" && finalizedRun?.context.waiting !== undefined) {
      const failureMessage = finalized.message
        ?? (finalizedRun.error !== null && typeof finalizedRun.error === "object"
          && !Array.isArray(finalizedRun.error) && typeof finalizedRun.error.message === "string"
          ? finalizedRun.error.message
          : "Required paused notification failed.");
      failWaitingInteraction(failureMessage);
      store.updateRun(runId, { currentStepId: null, context: run.context });
    }
    return {
      status: finalized.status,
      completedSteps,
      message: finalized.message ?? resultMessage
    };
  };
  return kind === "approval"
    ? store.withRunFinalizationTransaction(runId, persistWaiting)
    : persistWaiting();
}

interface PersistedResumeDecision {
  persisted: true;
  location: RuntimeStepLocation;
  selectedTarget?: string;
  completedSteps: string[];
  routingBudget: SuccessfulRoutingBudget;
}

interface FailedResumeDecision {
  resumeError: unknown;
}

function resumeWaitingStep(
  store: AgentFlowRunStateStore,
  runId: string,
  workflow: AgentFlowWorkflow,
  context: Record<string, AgentFlowRunStateValue>,
  response: AgentFlowPipelineResumeInput,
  stepLocations: Map<string, RuntimeStepLocation>,
  notifications: AgentFlowNotificationRegistry,
  prepareResume?: () => void
): ResumedWaitingStep {
  const decision = store.withRunStateTransaction(runId, () => {
    prepareResume?.();
    const persisted = persistWaitingStepResume(
      store, runId, workflow, context, response, stepLocations, notifications
    );
    if (!("result" in persisted) && !("resumeError" in persisted)) {
      checkpointExecutionRouting(store, runId, persisted.routingBudget);
    }
    return persisted;
  });
  if ("resumeError" in decision) throw decision.resumeError;
  if (!("persisted" in decision)) return decision;

  const completedSteps = [...decision.completedSteps, requiredStepId(
    decision.location.steps[decision.location.index]!
  )];
  const routed = routeAfterSuccessfulStep(
    store,
    runId,
    completedSteps,
    completedSteps.at(-1)!,
    decision.location.steps[decision.location.index]!,
    decision.location.steps,
    decision.location.index,
    stepLocations,
    decision.routingBudget,
    decision.selectedTarget
  );
  if ("result" in routed) return routed;
  return { ...routed, completedSteps, routingBudget: decision.routingBudget };
}

function persistWaitingStepResume(
  store: AgentFlowRunStateStore,
  runId: string,
  workflow: AgentFlowWorkflow,
  context: Record<string, AgentFlowRunStateValue>,
  response: AgentFlowPipelineResumeInput,
  stepLocations: Map<string, RuntimeStepLocation>,
  notifications: AgentFlowNotificationRegistry
): ResumedWaitingStep | PersistedResumeDecision | FailedResumeDecision {
  const waiting = parseWaitingState(context.waiting);
  const location = stepLocations.get(waiting.stepId);
  if (location === undefined) {
    throw new AgentFlowRunStateError(
      `Agent Flow run ${runId} cannot resume because waiting step ${waiting.stepId} is not in its workflow.`,
      "AGENT_FLOW_RESUME_STATE"
    );
  }
  const step = location.steps[location.index]!;
  const waitingStepType = waiting.kind === "disagreement" ? "review" : waiting.kind;
  if (normalizedTarget(step.type) !== waitingStepType) {
    throw new AgentFlowRunStateError(
      `Agent Flow run ${runId} waiting state does not match workflow step ${waiting.stepId}.`,
      "AGENT_FLOW_RESUME_STATE"
    );
  }
  if (waiting.kind === "approval") {
    const declaredEvidence = Array.isArray(step.artifacts)
      ? step.artifacts.flatMap((value) => {
        if (typeof value !== "string") return [];
        try {
          return normalizeAgentFlowArtifactPath(value) === value ? [value] : [];
        } catch {
          return [];
        }
      })
      : [];
    const expectedApprovalId = `approval:${safeId(waiting.stepId)}:attempt-${waiting.attempt}`;
    if (!Array.isArray(step.artifacts)
        || declaredEvidence.length !== step.artifacts.length
        || !isDeepStrictEqual(waiting.evidence?.map(({ path }) => path), declaredEvidence)
        || waiting.approvalId !== expectedApprovalId) {
      throw new AgentFlowRunStateError(
        `Agent Flow run ${runId} approval waiting state does not match workflow step ${waiting.stepId}.`,
        "AGENT_FLOW_RESUME_STATE"
      );
    }
  } else if (waiting.kind === "disagreement") {
    const declaredEvidence = reviewDisagreementEvidencePaths(step);
    if (declaredEvidence.length === 0
        || !isDeepStrictEqual(waiting.evidence?.map(({ path }) => path), declaredEvidence)) {
      throw new AgentFlowRunStateError(
        `Agent Flow run ${runId} disagreement waiting state does not match workflow step ${waiting.stepId}.`,
        "AGENT_FLOW_RESUME_STATE"
      );
    }
  }

  const routingBudget = deserializeRoutingBudget(waiting.routing, workflow, notifications);
  const completedSteps = [...waiting.completedSteps];
  let selectedTarget: string | undefined;
  let output: Record<string, AgentFlowRunStateValue>;

  if (waiting.kind === "disagreement") {
    if (!("outcome" in response)) {
      throw new AgentFlowRunStateError(
        `Disagreement ${waiting.stepId} requires an explicit --outcome value.`,
        "AGENT_FLOW_GATE_OUTCOME_REQUIRED"
      );
    }
    const outcome = response.outcome.trim();
    if (!waiting.validOutcomes.includes(outcome)) {
      throw new AgentFlowRunStateError(
        `Disagreement ${waiting.stepId} rejected outcome ${JSON.stringify(outcome)}; valid outcomes are: ${waiting.validOutcomes.join(", ")}.`,
        "AGENT_FLOW_GATE_OUTCOME_INVALID"
      );
    }
    if (response.decidedBy !== undefined && response.decidedBy.trim().length === 0) {
      throw new AgentFlowRunStateError(
        `Disagreement ${waiting.stepId} decision actor must be non-empty text.`,
        "AGENT_FLOW_INTERACTION_INVALID"
      );
    }
    const decision = outcome === "approve" ? "approved"
      : outcome === "request_changes" ? "changes_requested" : undefined;
    const policy = parseAgentFlowDisagreementPolicy(mapping(workflow.collaboration)?.on_disagreement);
    let evidenceChanged = false;
    store.withRunFinalizationTransaction(runId, () => {
      if (decision !== undefined) {
        let currentEvidence: AgentFlowWaitingEvidence[] = [];
        let evidenceChange: string | undefined;
        try {
          currentEvidence = waiting.evidence!.map(({ path }) => {
            const artifact = store.readArtifact(runId, path).artifact;
            if (artifact.checksum === null) throw new Error(`Artifact ${path} does not have a persisted checksum.`);
            return { path: artifact.declaredPath, checksum: artifact.checksum };
          });
        } catch (error) {
          evidenceChange = `evidence is no longer available: ${error instanceof Error ? error.message : String(error)}`;
        }
        if (evidenceChange !== undefined || !isDeepStrictEqual(currentEvidence, waiting.evidence)) {
          const message = evidenceChange ?? "evidence changed while the run was paused";
          evidenceChanged = true;
          store.transitionRunWithEvent(runId, {
            status: "running",
            allowedFrom: ["paused"],
            event: { type: "collaboration.disagreement.evidence_changed", stepId: waiting.stepId, payload: { message } }
          });
          store.upsertStep({
            runId,
            stepId: waiting.stepId,
            attempt: waiting.attempt,
            status: "cancelled",
            output: { reason: "evidence_changed" }
          });
          const { waiting: _waiting, ...restartedContext } = store.getRun(runId)!.context;
          store.updateRun(runId, { context: restartedContext, error: null });
          return;
        }
      }
      store.transitionRunWithEvent(runId, {
        status: "running",
        allowedFrom: ["paused"],
        event: { type: "run.resume", stepId: waiting.stepId, payload: { outcome } }
      });
      if (decision !== undefined) {
        publishReviewDisagreementDecision(
          store,
          runId,
          step,
          decision,
          `User selected ${outcome} for review disagreement ${waiting.stepId}.`,
          response.decidedBy?.trim() || "human",
          policy.strategy,
          Math.max(1, routingBudget.disagreementRounds.get(waiting.stepId) ?? 0),
          waiting.evidence
        );
      }
      store.appendRunEvent(runId, {
        type: "collaboration.disagreement.resolved",
        stepId: waiting.stepId,
        payload: {
          strategy: policy.strategy,
          path: "user",
          decidedBy: response.decidedBy?.trim() || "human",
          outcome,
          ...(decision === undefined ? {} : { decision })
        }
      });
    });
    routingBudget.disagreementRounds.set(waiting.stepId, 0);
    if (evidenceChanged) {
      return {
        steps: location.steps,
        nextIndex: location.index,
        completedSteps,
        routingBudget
      };
    }
    output = { attempt: waiting.attempt, outcome, decidedBy: response.decidedBy?.trim() || "human" };
    selectedTarget = outcome === "fail" ? "fail" : outcome === "cancel" ? "cancel" : undefined;
  } else if (waiting.kind === "manual_gate" || waiting.kind === "approval") {
    if (!("outcome" in response)) {
      throw new AgentFlowRunStateError(
        `${waiting.kind === "approval" ? "Approval" : "Manual gate"} ${waiting.stepId} requires an explicit --outcome value.`,
        "AGENT_FLOW_GATE_OUTCOME_REQUIRED"
      );
    }
    const outcome = response.outcome.trim();
    if (!waiting.validOutcomes.includes(outcome)) {
      throw new AgentFlowRunStateError(
        `${waiting.kind === "approval" ? "Approval" : "Manual gate"} ${waiting.stepId} rejected outcome ${JSON.stringify(outcome)}; valid outcomes are: ${waiting.validOutcomes.join(", ")}.`,
        "AGENT_FLOW_GATE_OUTCOME_INVALID"
      );
    }
    if (outcome === "pause" || outcome === "paused") {
      store.updateRun(runId, { context: store.getRun(runId)!.context });
      store.appendRunEvent(runId, {
        type: "manual_gate.paused",
        stepId: waiting.stepId,
        payload: { outcome }
      });
      return {
        result: {
          status: "paused",
          completedSteps,
          message: `${waiting.kind === "approval" ? "Approval" : "Manual gate"} ${waiting.stepId} remains paused.`
        }
      };
    }
    if (response.decidedBy !== undefined && response.decidedBy.trim().length === 0) {
      throw new AgentFlowRunStateError(
        `Manual gate ${waiting.stepId} decision actor must be non-empty text.`,
        "AGENT_FLOW_INTERACTION_INVALID"
      );
    }

    const approvalStatus = outcome === "cancel" || outcome === "cancelled"
      ? "cancelled"
      : outcome === "reject"
        ? "rejected"
        : "approved";
    const decidedBy = response.decidedBy ?? (waiting.kind === "approval" ? "human" : undefined);
    let approvalArtifact: string | undefined;
    if (waiting.kind === "approval" && (approvalStatus === "approved" || approvalStatus === "rejected")) {
      const persistedApproval = store.listApprovals(runId)
        .find((approval) => approval.id === waiting.approvalId);
      let currentEvidence: AgentFlowWaitingEvidence[] = [];
      let evidenceChange = persistedApproval?.status === "stale"
        ? "a configured approval dependency changed while the run was paused"
        : undefined;
      if (evidenceChange === undefined) {
        try {
          currentEvidence = waiting.evidence!.map(({ path }) => {
            const artifact = store.readArtifact(runId, path).artifact;
            if (artifact.checksum === null) throw new Error(`Artifact ${path} does not have a persisted checksum.`);
            return { path: artifact.declaredPath, checksum: artifact.checksum };
          });
        } catch (error) {
          evidenceChange = `evidence is no longer available: ${error instanceof Error ? error.message : String(error)}`;
        }
      }
      if (evidenceChange !== undefined || !isDeepStrictEqual(currentEvidence, waiting.evidence)) {
        const message = evidenceChange ?? "evidence changed while the run was paused";
        store.withRunFinalizationTransaction(runId, () => {
          store.transitionRunWithEvent(runId, {
            status: "running",
            allowedFrom: ["paused"],
            event: { type: "approval.evidence_changed", stepId: waiting.stepId, payload: { message } }
          });
          store.upsertStep({
            runId,
            stepId: waiting.stepId,
            attempt: waiting.attempt,
            status: "cancelled",
            output: { reason: "evidence_changed" }
          });
          store.appendRunEvent(runId, {
            type: "step.cancelled",
            stepId: waiting.stepId,
            payload: { attempt: waiting.attempt, reason: "evidence_changed" }
          });
          store.upsertApproval({
            id: waiting.approvalId!,
            runId,
            stepId: waiting.stepId,
            status: "cancelled",
            decision: "evidence_changed"
          });
          const { waiting: _waiting, ...restartedContext } = store.getRun(runId)!.context;
          store.updateRun(runId, { context: restartedContext, error: null });
        });
        return {
          steps: location.steps,
          nextIndex: location.index,
          completedSteps,
          routingBudget
        };
      }
    }
    store.withRunFinalizationTransaction(runId, () => {
      store.transitionRunWithEvent(runId, {
        status: "running",
        allowedFrom: ["paused"],
        event: { type: "run.resume", stepId: waiting.stepId, payload: { outcome } }
      });
      if (waiting.kind === "approval" && (approvalStatus === "approved" || approvalStatus === "rejected")) {
        const outputPath = typeof step.output === "string" && step.output.trim().length > 0
          ? step.output.trim()
          : defaultAgentFlowApprovalOutputPath(waiting.stepId);
        const existing = store.getArtifact(runId, outputPath);
        const artifact = store.writeArtifact({
          id: existing?.id ?? `approval-output:${safeId(waiting.stepId)}`,
          runId,
          stepId: waiting.stepId,
          path: outputPath,
          kind: "approval_output",
          contentType: "application/json; charset=utf-8",
          content: `${JSON.stringify({ status: approvalStatus, decision: outcome })}\n`,
          overwrite: step.overwrite === true || existing?.producerStepId === waiting.stepId,
          requiredRunStatus: "running",
          requiredApproval: { id: waiting.approvalId!, status: "requested" },
          requiredArtifacts: waiting.evidence,
          metadata: {
            reviewer: "human",
            decidedBy: response.decidedBy ?? "human",
            attempt: waiting.attempt,
            evidence: waiting.evidence as unknown as AgentFlowRunStateValue
          }
        });
        approvalArtifact = artifact.declaredPath;
      }
      store.upsertApproval({
        id: waiting.approvalId!,
        runId,
        stepId: waiting.stepId,
        status: approvalStatus,
        ...(decidedBy === undefined ? {} : { decidedBy }),
        decision: outcome
      });
    });
    output = { attempt: waiting.attempt, outcome, ...(approvalArtifact === undefined ? {} : { approvalArtifact }) };
    selectedTarget = manualGateOutcomeTarget(step, outcome);
  } else {
    if (!("answer" in response)) {
      throw new AgentFlowRunStateError(
        `Input request ${waiting.stepId} requires an explicit --answer value.`,
        "AGENT_FLOW_INPUT_ANSWER_REQUIRED"
      );
    }
    store.transitionRunWithEvent(runId, {
      status: "running",
      allowedFrom: ["paused"],
      event: { type: "run.resume", stepId: waiting.stepId, payload: { answerProvided: true } }
    });
    const answer = response.answer;
    const textAnswer = typeof answer === "string" ? answer : `${JSON.stringify(answer)}\n`;
    const contentType = typeof answer === "string"
      ? "text/plain; charset=utf-8"
      : "application/json; charset=utf-8";
    try {
      const existingArtifact = store.getArtifact(runId, waiting.saveAs!);
      const mayReplaceExisting = step.overwrite === true
        || existingArtifact?.producerStepId === waiting.stepId;
      const artifact = store.writeArtifact({
        id: mayReplaceExisting && existingArtifact !== null
          ? existingArtifact.id
          : `input-request:${safeId(waiting.stepId)}:attempt-${waiting.attempt}`,
        runId,
        stepId: waiting.stepId,
        path: waiting.saveAs!,
        kind: "input_request",
        contentType,
        content: textAnswer,
        overwrite: mayReplaceExisting,
        requiredRunStatus: "running",
        metadata: { question: waiting.prompt }
      });
      output = {
        attempt: waiting.attempt,
        answerArtifact: artifact.declaredPath,
        checksum: artifact.checksum
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      const { waiting: _waiting, ...failureContext } = store.getRun(runId)!.context;
      finalizePipelineRun(store, runId, routingBudget.terminalEffects, {
        intendedStatus: "paused",
        completedSteps,
        currentStepId: waiting.stepId,
        output: { completedSteps },
        message,
        eventPayload: { reason: waiting.reason, error: message },
        eventStepId: waiting.stepId,
        failureContext
      });
      return { resumeError: error };
    }
  }

  const { waiting: _waiting, ...resumedContext } = store.getRun(runId)!.context;
  store.updateRun(runId, { context: resumedContext, error: null });
  store.upsertStep({
    runId,
    stepId: waiting.stepId,
    attempt: waiting.attempt,
    status: "completed",
    output
  });
  store.appendRunEvent(runId, {
    type: "step.completed",
    stepId: waiting.stepId,
    payload: output
  });
  return {
    persisted: true,
    location,
    ...(selectedTarget === undefined ? {} : { selectedTarget }),
    completedSteps,
    routingBudget
  };
}

function manualGateOutcomeTarget(step: AgentFlowWorkflowStep, outcome: string): string | undefined {
  if (outcome === "approve") return normalizedTarget(step.on_approve);
  if (outcome === "reject") return normalizedTarget(step.on_reject) ?? "cancel";
  if (outcome === "cancel" || outcome === "cancelled") return normalizedTarget(step.on_cancel) ?? "cancel";
  if (outcome === "fail" || outcome === "failed") return "fail";
  if (outcome === "complete" || outcome === "completed") return "complete";
  return undefined;
}

function serializeRoutingBudget(budget: SuccessfulRoutingBudget): SerializedSuccessfulRoutingBudget {
  return {
    ...(budget.maxRecoveryCycles === undefined ? {} : { maxRecoveryCycles: budget.maxRecoveryCycles }),
    ...(budget.maxReviewCycles === undefined ? {} : { maxReviewCycles: budget.maxReviewCycles }),
    stepAttemptLimits: Object.fromEntries([...budget.stepAttemptLimits].sort(([left], [right]) => left.localeCompare(right))),
    visits: Object.fromEntries([...budget.visits].sort(([left], [right]) => left.localeCompare(right))),
    recoveryCycles: Object.fromEntries([...budget.recoveryCycles].sort(([left], [right]) => left.localeCompare(right))),
    recoveryInvocations: Object.fromEntries([...budget.recoveryInvocations].sort(([left], [right]) => left.localeCompare(right))),
    disagreementEpisodes: Object.fromEntries([...budget.disagreementEpisodes].sort(([left], [right]) => left.localeCompare(right))),
    disagreementRounds: Object.fromEntries([...budget.disagreementRounds].sort(([left], [right]) => left.localeCompare(right))),
    attempts: Object.fromEntries([...budget.attempts].sort(([left], [right]) => left.localeCompare(right)))
  };
}

function persistedExecutionRouting(
  context: Record<string, AgentFlowRunStateValue>
): SerializedSuccessfulRoutingBudget | undefined {
  const persisted = mapping(context.executionRouting);
  return persisted === undefined ? undefined : parseSerializedRoutingBudget(persisted);
}

function completedStepsFromExecutionEvents(store: AgentFlowRunStateStore, runId: string): string[] {
  return store.listEvents(runId).flatMap((event) =>
    event.type === "step.completed" && event.stepId !== null ? [event.stepId] : []
  );
}

function incompleteRecoveryRoutePayload(
  store: AgentFlowRunStateStore,
  runId: string,
  stepId: string,
  failureId?: string
): AgentFlowYamlMapping | undefined {
  const latestRecoveryEvent = [...store.listEvents(runId)].reverse().find((event) =>
    event.stepId === stepId
      && (event.type === "recovery.routed" || event.type === "recovery.completed")
      && (failureId === undefined || mapping(event.payload)?.failureId === failureId)
  );
  return latestRecoveryEvent?.type === "recovery.routed"
    ? mapping(latestRecoveryEvent.payload)
    : undefined;
}

function recoveryWorkspaceSnapshotCoordinates(
  stepId: string,
  failureId: string
): { id: string; path: string } {
  const stepSegment = safeId(stepId);
  const failureSegment = safeId(failureId);
  return {
    id: `recovery-workspace:${stepSegment}:${failureSegment}`,
    path: `recovery-workspace/${stepSegment}/${failureSegment}.json`
  };
}

function serializeRecoveryWorkspaceSnapshot(snapshot: AgentFlowWorkspaceSnapshot): string {
  const serialized = `${JSON.stringify({
    version: 1,
    entries: [...snapshot.entries()].sort(([left], [right]) => left.localeCompare(right))
  })}\n`;
  if (Buffer.byteLength(serialized) > MAX_RECOVERY_WORKSPACE_SNAPSHOT_BYTES) {
    throw new AgentFlowRunStateError(
      `Recovery workspace snapshot exceeds the ${MAX_RECOVERY_WORKSPACE_SNAPSHOT_BYTES}-byte persistence limit.`,
      "AGENT_FLOW_RECOVERY_WORKSPACE_SNAPSHOT"
    );
  }
  return serialized;
}

function readRecoveryWorkspaceSnapshot(
  store: AgentFlowRunStateStore,
  runId: string,
  stepId: string,
  failureId: string,
  routeKind: "session" | "workflow",
  target: string,
  routePayload: AgentFlowYamlMapping
): AgentFlowWorkspaceSnapshot {
  const snapshotPath = normalizedTarget(routePayload.workspaceSnapshotPath);
  const snapshotChecksum = normalizedTarget(routePayload.workspaceSnapshotChecksum);
  const coordinates = recoveryWorkspaceSnapshotCoordinates(stepId, failureId);
  if (snapshotPath !== coordinates.path || snapshotChecksum === undefined
      || normalizedTarget(routePayload.route) !== routeKind
      || normalizedTarget(routePayload.target) !== target) {
    throw new AgentFlowRunStateError(
      `Interrupted recovery route for step ${stepId} has no persisted pre-route workspace snapshot.`,
      "AGENT_FLOW_RECOVERY_WORKSPACE_SNAPSHOT"
    );
  }
  const snapshot = store.readArtifact(runId, snapshotPath, { maxBytes: MAX_RECOVERY_WORKSPACE_SNAPSHOT_BYTES });
  if (snapshot.artifact.id !== coordinates.id
      || snapshot.artifact.kind !== "recovery_workspace_snapshot"
      || snapshot.artifact.checksum !== snapshotChecksum
      || snapshot.artifact.metadata.failureId !== failureId
      || snapshot.artifact.metadata.route !== routeKind
      || snapshot.artifact.metadata.target !== target) {
    throw new AgentFlowRunStateError(
      `Interrupted recovery route for step ${stepId} has an invalid pre-route workspace snapshot.`,
      "AGENT_FLOW_RECOVERY_WORKSPACE_SNAPSHOT"
    );
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(snapshot.content.toString("utf8"));
  } catch (error) {
    throw new AgentFlowRunStateError(
      `Interrupted recovery route for step ${stepId} has a malformed pre-route workspace snapshot.`,
      "AGENT_FLOW_RECOVERY_WORKSPACE_SNAPSHOT",
      { cause: error }
    );
  }
  const payload = mapping(parsed);
  const entries = payload?.entries;
  if (payload?.version !== 1 || !Array.isArray(entries) || entries.length > MAX_RECOVERY_WORKSPACE_SNAPSHOT_PATHS) {
    throw new AgentFlowRunStateError(
      `Interrupted recovery route for step ${stepId} has a malformed pre-route workspace snapshot.`,
      "AGENT_FLOW_RECOVERY_WORKSPACE_SNAPSHOT"
    );
  }
  const result: AgentFlowWorkspaceSnapshot = new Map();
  for (const entry of entries) {
    if (!Array.isArray(entry) || entry.length !== 2
        || typeof entry[0] !== "string" || entry[0].length === 0
        || typeof entry[1] !== "string" || entry[1].length === 0
        || result.has(entry[0])) {
      throw new AgentFlowRunStateError(
        `Interrupted recovery route for step ${stepId} has a malformed pre-route workspace snapshot.`,
        "AGENT_FLOW_RECOVERY_WORKSPACE_SNAPSHOT"
      );
    }
    result.set(entry[0], entry[1]);
  }
  return result;
}

function hasUnroutedFailureOrRecoveryDecision(
  store: AgentFlowRunStateStore,
  runId: string,
  stepId: string,
  attempt: number
): boolean {
  const failure = [...store.listFailures(runId)].reverse().find((entry) =>
    entry.stepId === stepId && entry.attempt === attempt
  );
  if (failure === undefined) return false;
  if (failure.resolvedAt === null) return true;
  return [...store.listEvents(runId)].reverse().some((event) => {
    if (event.stepId !== stepId || event.type !== "recovery.completed") return false;
    return mapping(event.payload)?.failureId === failure.id;
  });
}

function isPendingExecutionCheckpoint(
  value: AgentFlowRunStateValue | undefined,
  stepId: string,
  latestAttempt: number
): boolean {
  const checkpoint = mapping(value);
  return checkpoint?.stepId === stepId
    && typeof checkpoint.completedAttempts === "number"
    && Number.isSafeInteger(checkpoint.completedAttempts)
    && checkpoint.completedAttempts >= latestAttempt;
}

function executionCheckpointCompletedAttempts(
  value: AgentFlowRunStateValue | undefined,
  stepId: string
): number | undefined {
  const checkpoint = mapping(value);
  return checkpoint?.stepId === stepId
      && typeof checkpoint.completedAttempts === "number"
      && Number.isSafeInteger(checkpoint.completedAttempts)
      && checkpoint.completedAttempts >= 0
    ? checkpoint.completedAttempts
    : undefined;
}

function executionCheckpointVisit(
  value: AgentFlowRunStateValue | undefined,
  stepId: string
): number | undefined {
  const checkpoint = mapping(value);
  return checkpoint?.stepId === stepId
      && typeof checkpoint.visit === "number"
      && Number.isSafeInteger(checkpoint.visit)
      && checkpoint.visit > 0
    ? checkpoint.visit
    : undefined;
}

function nextRetryAttemptIndex(
  recoveredAttemptIndex: number | undefined,
  retries: number,
  stepId: string
): number {
  if (recoveredAttemptIndex === undefined) return 1;
  const next = recoveredAttemptIndex + 1;
  if (next <= retries + 1) return next;
  throw new AgentFlowRunStateError(
    `Agent Flow run cannot recover retryable step ${stepId} because its persisted retry index exceeds the configured retry bound.`,
    "AGENT_FLOW_RUN_LOCK_RECOVERY"
  );
}

function isPersistedDisagreementResolution(payload: AgentFlowRunStateValue): boolean {
  const output = mapping(payload);
  return typeof output?.attempt === "number"
    && Number.isSafeInteger(output.attempt)
    && output.attempt > 0
    && (output.resolution === "approved" || output.resolution === "changes_requested")
    && typeof output.resolver === "string"
    && typeof output.strategy === "string"
    && typeof output.episode === "number"
    && Number.isSafeInteger(output.episode)
    && output.episode > 0
    && typeof output.round === "number"
    && Number.isSafeInteger(output.round)
    && output.round > 0;
}

function latestPersistedStepFailure(
  store: AgentFlowRunStateStore,
  runId: string,
  stepId: string
): { exitCode: number | null; timedOut: boolean; message: string } {
  const failure = [...store.listFailures(runId)].reverse().find((entry) => entry.stepId === stepId);
  if (failure === undefined) {
    throw new AgentFlowRunStateError(
      `Agent Flow run ${runId} cannot recover failed step ${stepId} because its persisted failure was not found.`,
      "AGENT_FLOW_RUN_LOCK_RECOVERY"
    );
  }
  const payload = mapping(failure.payload);
  return {
    exitCode: payload?.exitCode === null || typeof payload?.exitCode === "number" ? payload.exitCode : null,
    timedOut: payload?.timedOut === true,
    message: failure.message
  };
}

function checkpointExecutionRouting(
  store: AgentFlowRunStateStore,
  runId: string,
  budget: SuccessfulRoutingBudget,
  currentStepId?: string
): void {
  const persist = (): void => {
    const run = store.getRun(runId)!;
    store.updateRun(runId, {
      ...(currentStepId === undefined ? {} : { currentStepId }),
      context: {
        ...run.context,
        executionRouting: serializeRoutingBudget(budget) as unknown as AgentFlowRunStateValue,
        ...(currentStepId === undefined ? {} : {
          executionCheckpoint: {
            stepId: currentStepId,
            visit: budget.visits.get(currentStepId) ?? 0,
            completedAttempts: budget.attempts.get(currentStepId) ?? 0
          }
        })
      }
    });
  };
  if (currentStepId === undefined) {
    persist();
  } else {
    store.withRunStateTransaction(runId, persist);
  }
}

function deserializeRoutingBudget(
  serialized: SerializedSuccessfulRoutingBudget,
  workflow: AgentFlowWorkflow,
  notifications: AgentFlowNotificationRegistry
): SuccessfulRoutingBudget {
  const configured = createSuccessfulRoutingBudget(workflow, notifications);
  return {
    terminalEffects: configured.terminalEffects,
    maxRecoveryCycles: configured.maxRecoveryCycles,
    maxReviewCycles: configured.maxReviewCycles,
    stepAttemptLimits: configured.stepAttemptLimits,
    reviewCyclePathReviewIds: configured.reviewCyclePathReviewIds,
    reviewCycleStepIds: configured.reviewCycleStepIds,
    visits: new Map(Object.entries(serialized.visits)),
    recoveryCycles: new Map(Object.entries(serialized.recoveryCycles)),
    recoveryInvocations: new Map(Object.entries(serialized.recoveryInvocations)),
    disagreementEpisodes: new Map(Object.entries(serialized.disagreementEpisodes)),
    disagreementRounds: new Map(Object.entries(serialized.disagreementRounds)),
    attempts: new Map(Object.entries(serialized.attempts))
  };
}

function parseWaitingState(value: AgentFlowRunStateValue | undefined): AgentFlowPipelineWaitingState {
  const record = mapping(value);
  if (record === undefined) {
    throw new AgentFlowRunStateError(
      "Paused Agent Flow run does not have a persisted approval, manual gate, or input request.",
      "AGENT_FLOW_RESUME_STATE"
    );
  }
  const kind = record.kind;
  const stepId = normalizedTarget(record.stepId);
  const attempt = record.attempt;
  const reason = record.reason;
  const prompt = typeof record.prompt === "string" ? record.prompt : undefined;
  const validOutcomes = normalizedStringList(record.validOutcomes);
  const completedSteps = normalizedStringList(record.completedSteps);
  const routing = mapping(record.routing);
  const reasonMatchesKind = kind === "approval" ? reason === "approval"
    : kind === "manual_gate" ? reason === "manual_approval"
      : kind === "input_request" ? reason === "missing_input"
        : kind === "disagreement" ? reason === "disagreement" : false;
  if ((kind !== "approval" && kind !== "manual_gate" && kind !== "input_request" && kind !== "disagreement")
      || stepId === undefined
      || !Number.isSafeInteger(attempt)
      || (attempt as number) < 1
      || !reasonMatchesKind
      || prompt === undefined
      || routing === undefined) {
    throw new AgentFlowRunStateError(
      "Paused Agent Flow run has invalid persisted interaction state.",
      "AGENT_FLOW_RESUME_STATE"
    );
  }
  const serialized = parseSerializedRoutingBudget(routing);
  const saveAs = typeof record.saveAs === "string" ? record.saveAs : undefined;
  const approvalId = typeof record.approvalId === "string" ? record.approvalId : undefined;
  const evidence = Array.isArray(record.evidence)
    ? record.evidence.flatMap((entry) => {
      const candidate = mapping(entry);
      const path = typeof candidate?.path === "string" ? candidate.path : undefined;
      const checksum = typeof candidate?.checksum === "string" ? candidate.checksum : undefined;
      if (path === undefined || checksum === undefined || !/^sha256:[a-f0-9]{64}$/.test(checksum)) return [];
      try {
        if (normalizeAgentFlowArtifactPath(path) !== path) return [];
      } catch {
        return [];
      }
      return [{ path, checksum }];
    })
    : [];
  if (((kind === "approval" || kind === "manual_gate") && (validOutcomes.length === 0 || approvalId === undefined))
      || (kind === "approval" && !isDeepStrictEqual(validOutcomes, ["approve", "reject", "cancel"]))
      || (kind === "approval" && (evidence.length === 0 || evidence.length !== (record.evidence as unknown[] | undefined)?.length))
      || (kind === "disagreement" && !isDeepStrictEqual(validOutcomes, ["approve", "request_changes", "fail", "cancel"]))
      || (kind === "disagreement" && (evidence.length === 0 || evidence.length !== (record.evidence as unknown[] | undefined)?.length))
      || (kind === "input_request" && saveAs === undefined)) {
    throw new AgentFlowRunStateError(
      "Paused Agent Flow run has incomplete persisted interaction state.",
      "AGENT_FLOW_RESUME_STATE"
    );
  }
  return {
    kind,
    stepId,
    attempt: attempt as number,
    reason: kind === "approval" ? "approval"
      : kind === "manual_gate" ? "manual_approval"
        : kind === "input_request" ? "missing_input" : "disagreement",
    prompt,
    validOutcomes,
    ...(saveAs === undefined ? {} : { saveAs }),
    ...(approvalId === undefined ? {} : { approvalId }),
    ...(kind === "approval" || kind === "disagreement" ? { evidence } : {}),
    completedSteps,
    routing: serialized
  };
}

export function validateAgentFlowPipelineWaitingState(value: AgentFlowRunStateValue | undefined): void {
  parseWaitingState(value);
}

function parseSerializedRoutingBudget(value: AgentFlowYamlMapping): SerializedSuccessfulRoutingBudget {
  const parseMap = (
    field: "stepAttemptLimits" | "visits" | "recoveryCycles" | "recoveryInvocations" | "disagreementEpisodes" | "disagreementRounds" | "attempts",
    valid: (value: unknown) => boolean
  ): Record<string, number> => {
    const candidate = mapping(value[field]);
    if (candidate === undefined) {
      throw new AgentFlowRunStateError(
        "Paused Agent Flow run has invalid persisted routing state.",
        "AGENT_FLOW_RESUME_STATE"
      );
    }
    const entries = Object.entries(candidate);
    if (entries.some(([, count]) => !valid(count))) {
      throw new AgentFlowRunStateError(
        "Paused Agent Flow run has invalid persisted routing counters.",
        "AGENT_FLOW_RESUME_STATE"
      );
    }
    return Object.fromEntries(entries) as Record<string, number>;
  };
  const parsed: Pick<
    SerializedSuccessfulRoutingBudget,
    "stepAttemptLimits" | "visits" | "recoveryCycles" | "recoveryInvocations" | "disagreementEpisodes" | "disagreementRounds" | "attempts"
  > = {
    stepAttemptLimits: parseMap(
      "stepAttemptLimits",
      (entry) => typeof entry === "number" && Number.isFinite(entry) && entry > 0
    ),
    visits: parseMap("visits", (entry) => Number.isSafeInteger(entry) && (entry as number) >= 0),
    recoveryCycles: parseMap("recoveryCycles", (entry) => Number.isSafeInteger(entry) && (entry as number) >= 0),
    recoveryInvocations: mapping(value.recoveryInvocations) === undefined
      ? {}
      : parseMap("recoveryInvocations", (entry) => Number.isSafeInteger(entry) && (entry as number) >= 0),
    disagreementEpisodes: mapping(value.disagreementEpisodes) === undefined
      ? {}
      : parseMap("disagreementEpisodes", (entry) => Number.isSafeInteger(entry) && (entry as number) >= 0),
    disagreementRounds: mapping(value.disagreementRounds) === undefined
      ? {}
      : parseMap("disagreementRounds", (entry) => Number.isSafeInteger(entry) && (entry as number) >= 0),
    attempts: parseMap("attempts", (entry) => Number.isSafeInteger(entry) && (entry as number) >= 0)
  };
  return {
    ...parsed,
    ...(Number.isSafeInteger(value.maxRecoveryCycles) && (value.maxRecoveryCycles as number) > 0
      ? { maxRecoveryCycles: value.maxRecoveryCycles as number }
      : {}),
    ...(Number.isSafeInteger(value.maxReviewCycles) && (value.maxReviewCycles as number) > 0
      ? { maxReviewCycles: value.maxReviewCycles as number }
      : {})
  };
}

function normalizedStringList(value: unknown): string[] {
  return Array.isArray(value)
    ? value.flatMap((entry) => typeof entry === "string" && entry.trim().length > 0 ? [entry.trim()] : [])
    : [];
}

function requiredStaticString(value: unknown, label: string): string {
  const normalized = normalizedTarget(value);
  if (normalized === undefined) {
    throw new AgentFlowRunStateError(`${label} must be a static non-empty string.`, "AGENT_FLOW_INTERACTION_INVALID");
  }
  return normalized;
}

function validateRuntimeInteractionSteps(
  steps: AgentFlowWorkflowStep[],
  reserveFinalSummary: boolean
): void {
  for (const step of steps) {
    const type = normalizedTarget(step.type);
    const stepId = requiredStepId(step);
    const outputPaths = [
      ...(typeof step.output === "string" ? [step.output] : []),
      ...(typeof step.save_as === "string" ? [step.save_as] : []),
      ...(Array.isArray(step.outputs) ? step.outputs.filter((value): value is string => typeof value === "string") : [])
    ];
    for (const outputPath of outputPaths) {
      let normalized: string | undefined;
      try {
        normalized = normalizeAgentFlowArtifactPath(outputPath);
      } catch {
        normalized = undefined;
      }
      if (reserveFinalSummary && normalized === AGENT_FLOW_FINAL_SUMMARY_PATH) {
        throw new AgentFlowRunStateError(
          `Agent Flow workflow step ${stepId} cannot publish reserved runtime summary ${AGENT_FLOW_FINAL_SUMMARY_PATH}.`,
          "AGENT_FLOW_WORKFLOW_INVALID"
        );
      }
    }
    if (type === "input_request") {
      const saveAs = requiredStaticString(step.save_as, `Input request artifact for step ${stepId}`);
      let normalized: string;
      try {
        normalized = normalizeAgentFlowArtifactPath(saveAs);
      } catch (error) {
        throw new AgentFlowRunStateError(
          `Input request artifact for step ${stepId} is invalid: ${error instanceof Error ? error.message : String(error)}`,
          "AGENT_FLOW_INTERACTION_INVALID",
          { cause: error }
        );
      }
      if (normalized !== saveAs) {
        throw new AgentFlowRunStateError(
          `Input request artifact for step ${stepId} must be normalized as ${normalized}.`,
          "AGENT_FLOW_INTERACTION_INVALID"
        );
      }
    }
    if (type === "result") {
      const status = normalizedTarget(step.status);
      if (status === undefined || ![
        "cancelled", "completed", "continue", "failed", "paused", "remediated", "unresolved"
      ].includes(status)) {
        throw new AgentFlowRunStateError(
          `Result step ${stepId} has unsupported status ${String(step.status)}.`,
          "AGENT_FLOW_RESULT_STATUS"
        );
      }
    }
    for (const field of ["body", "steps", ...(type === "parallel" ? ["branches"] : [])]) {
      const nested = step[field];
      if (Array.isArray(nested)) {
        validateRuntimeInteractionSteps(nested.filter(isWorkflowStep), reserveFinalSummary);
      }
    }
  }
}

function validateRuntimeRecoveryTargets(
  steps: AgentFlowWorkflowStep[],
  workflow: AgentFlowWorkflow,
  providers: AgentFlowSessionProviderRegistry,
  workflows: AgentFlowWorkflowRegistry,
  stepIds: ReadonlySet<string>
): void {
  for (const step of steps) {
    const stepId = requiredStepId(step);
    const onFailure = mapping(step.on_failure);
    const routeValue = onFailure?.route_to;
    if (routeValue === undefined && (onFailure?.on_remediated !== undefined || onFailure?.on_unresolved !== undefined)) {
      throw invalidRuntimeRecoveryRoute(stepId, "on_remediated and on_unresolved require route_to");
    }
    if (routeValue !== undefined) {
      if (!["artifact_transform", "command", "mcp_call", "session_request"].includes(normalizedTarget(step.type) ?? "")) {
        throw invalidRuntimeRecoveryRoute(stepId, `recovery is not supported for ${String(step.type)} steps`);
      }
      if (workflow.style !== "recovery_pipeline") {
        throw invalidRuntimeRecoveryRoute(stepId, "recovery routes require workflow style recovery_pipeline");
      }
      if (onFailure?.then !== undefined || onFailure?.goto !== undefined) {
        throw invalidRuntimeRecoveryRoute(stepId, "route_to cannot be combined with on_failure.then or on_failure.goto");
      }
      const route = mapping(routeValue);
      if (route === undefined) {
        throw invalidRuntimeRecoveryRoute(stepId, "route_to must be a mapping");
      }
      if (route.inputs !== undefined && mapping(route.inputs) === undefined) {
        throw invalidRuntimeRecoveryRoute(stepId, "route_to.inputs must be a mapping");
      }
      if (route.inputs !== undefined) {
        validateRuntimeRecoveryInputExpressions(
          route.inputs,
          stepId,
          new Set(Object.keys(mapping(workflow.inputs) ?? {}))
        );
      }
      if (dynamicRuntimeTarget(route.session) || dynamicRuntimeTarget(route.workflow)) {
        throw invalidRuntimeRecoveryRoute(stepId, "session and workflow targets must be static");
      }
      const sessionId = normalizedTarget(route.session);
      const workflowName = normalizedTarget(route.workflow);
      if ((sessionId === undefined) === (workflowName === undefined)) {
        throw new AgentFlowRunStateError(
          `Recovery route for step ${stepId} must declare exactly one session or workflow target.`,
          "AGENT_FLOW_WORKFLOW_INVALID"
        );
      }
      if (sessionId !== undefined) {
        const session = mapping(workflow.sessions?.[sessionId]);
        const provider = normalizedTarget(session?.provider);
        const prompt = normalizedTarget(route.prompt);
        if (prompt === undefined) {
          throw invalidRuntimeRecoveryRoute(stepId, "session routes require a static non-empty prompt path");
        }
        const normalizedPrompt = path.posix.normalize(prompt);
        if (prompt.includes("\\") || path.posix.isAbsolute(prompt) || path.win32.isAbsolute(prompt)
            || normalizedPrompt !== prompt || normalizedPrompt === "." || normalizedPrompt === ".."
            || normalizedPrompt.startsWith("../") || prompt.endsWith("/")) {
          throw invalidRuntimeRecoveryRoute(stepId, "session prompt must be a normalized repo-relative file path");
        }
        if (provider === undefined || providers.get(provider) === undefined) {
          throw new AgentFlowRunStateError(
            `Recovery session ${sessionId} for step ${stepId} does not have a registered provider adapter.`,
            "AGENT_FLOW_RECOVERY_SESSION_UNKNOWN"
          );
        }
      }
      if (workflowName !== undefined && workflows.get(workflowName) === undefined) {
        throw new AgentFlowRunStateError(
          `Recovery workflow ${workflowName} for step ${stepId} is not registered.`,
          "AGENT_FLOW_RECOVERY_WORKFLOW_UNKNOWN"
        );
      }
      for (const handlerName of ["on_remediated", "on_unresolved"] as const) {
        const handler = mapping(onFailure?.[handlerName]);
        if (dynamicRuntimeTarget(handler?.then) || dynamicRuntimeTarget(handler?.return_to)) {
          throw invalidRuntimeRecoveryRoute(stepId, `${handlerName} targets must be static`);
        }
        const then = normalizedTarget(handler?.then);
        const returnTo = normalizedTarget(handler?.return_to);
        if (handler === undefined || (then === undefined) === (returnTo === undefined)) {
          throw invalidRuntimeRecoveryRoute(
            stepId,
            `${handlerName} must declare exactly one static then or return_to target`
          );
        }
        if (handlerName === "on_unresolved" && returnTo !== undefined) {
          throw invalidRuntimeRecoveryRoute(stepId, "on_unresolved cannot return to the failed step");
        }
        if (handlerName === "on_remediated" && returnTo !== undefined && returnTo !== stepId) {
          throw invalidRuntimeRecoveryRoute(stepId, "on_remediated.return_to must name the failed step");
        }
        if (then !== undefined && !stepIds.has(then)
            && !["cancel", "complete", "completed", "continue", "fail", "ignore", "pause"].includes(then)) {
          throw invalidRuntimeRecoveryRoute(stepId, `${handlerName}.then target ${then} is unresolved`);
        }
      }
    }
    for (const field of ["body", "steps", ...(normalizedTarget(step.type) === "parallel" ? ["branches"] : [])]) {
      const nested = step[field];
      if (Array.isArray(nested)) {
        validateRuntimeRecoveryTargets(
          nested.filter(isWorkflowStep), workflow, providers, workflows, stepIds
        );
      }
    }
  }
}

function dynamicRuntimeTarget(value: unknown): boolean {
  return typeof value === "string" && (value.includes("{{") || value.includes("}}"));
}

function validateRuntimeRecoveryInputExpressions(
  value: unknown,
  stepId: string,
  declaredInputs: ReadonlySet<string>
): void {
  if (typeof value === "string") {
    if (!value.includes("{{") && !value.includes("}}")) return;
    const expression = /^\{\{\s*(?:failure\.path|step\.id|inputs\.([A-Za-z_][A-Za-z0-9_-]*))\s*}}$/.exec(value);
    if (expression !== null) {
      const inputName = expression[1];
      if (inputName === undefined || declaredInputs.has(inputName)) return;
      throw invalidRuntimeRecoveryRoute(stepId, `route_to.inputs references undeclared workflow input ${inputName}`);
    }
    throw invalidRuntimeRecoveryRoute(
      stepId,
      "route_to.inputs expressions must use failure.path, step.id, or inputs.<name>"
    );
  }
  if (Array.isArray(value)) {
    value.forEach((entry) => validateRuntimeRecoveryInputExpressions(entry, stepId, declaredInputs));
    return;
  }
  const record = mapping(value);
  if (record !== undefined) {
    Object.values(record).forEach((entry) => validateRuntimeRecoveryInputExpressions(entry, stepId, declaredInputs));
  }
}

function invalidRuntimeRecoveryRoute(stepId: string, reason: string): AgentFlowRunStateError {
  return new AgentFlowRunStateError(
    `Recovery route for step ${stepId} is invalid: ${reason}.`,
    "AGENT_FLOW_WORKFLOW_INVALID"
  );
}

function resolveInteractionPrompt(
  value: unknown,
  inputs: Record<string, AgentFlowRunStateValue>,
  label: string
): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new AgentFlowRunStateError(`${label} must be non-empty text.`, "AGENT_FLOW_INTERACTION_INVALID");
  }
  const template = value.trim();
  const unsupportedRemainder = template.replace(
    /(?<!\{)\{\{\s*inputs\.([A-Za-z_][A-Za-z0-9_-]*)\s*}}(?!})/g,
    ""
  );
  if (unsupportedRemainder.includes("{{") || unsupportedRemainder.includes("}}")) {
    throw new AgentFlowRunStateError(
      `${label} contains an unsupported input expression.`,
      "AGENT_FLOW_INTERACTION_INVALID"
    );
  }
  const resolved = template.replace(
    /(?<!\{)\{\{\s*inputs\.([A-Za-z_][A-Za-z0-9_-]*)\s*}}(?!})/g,
    (_match, name: string) => {
      if (!Object.hasOwn(inputs, name)) {
        throw new AgentFlowRunStateError(
          `${label} references missing run input ${name}.`,
          "AGENT_FLOW_INTERACTION_INVALID"
        );
      }
      const input = inputs[name]!;
      return typeof input === "string" ? input : JSON.stringify(input);
    }
  ).trim();
  if (resolved.length === 0) {
    throw new AgentFlowRunStateError(
      `${label} must resolve to non-empty text.`,
      "AGENT_FLOW_INTERACTION_INVALID"
    );
  }
  return resolved;
}

async function routeAfterFailedStep(
  store: AgentFlowRunStateStore,
  runId: string,
  workflow: AgentFlowWorkflow,
  completedSteps: string[],
  stepId: string,
  step: AgentFlowWorkflowStep,
  currentSteps: AgentFlowWorkflowStep[],
  stepIndex: number,
  stepLocations: Map<string, RuntimeStepLocation>,
  budget: SuccessfulRoutingBudget,
  transforms: AgentFlowArtifactTransformRegistry,
  sessionProviders: AgentFlowSessionProviderRegistry,
  mcpCalls: AgentFlowMcpCallRegistry,
  notifications: AgentFlowNotificationRegistry,
  workflows: AgentFlowWorkflowRegistry
): Promise<SuccessfulRoute | undefined> {
  const onFailure = mapping(step.on_failure);
  const route = mapping(onFailure?.route_to);
  if (route === undefined) return undefined;
  const failure = [...store.listFailures(runId)].reverse().find((entry) => entry.stepId === stepId);
  if (failure === undefined) {
    throw new AgentFlowRunStateError(
      `Recovery route for step ${stepId} cannot start because its persisted failure was not found.`,
      "AGENT_FLOW_RECOVERY_FAILURE_MISSING"
    );
  }
  const routeKind = normalizedTarget(route.session) === undefined ? "workflow" : "session";
  const target = normalizedTarget(route[routeKind])!;
  const persistedCompletion = persistedRecoveryCompletion(store, runId, stepId, failure.id);
  if (persistedCompletion !== undefined) {
    return routePersistedRecoveryCompletion(
      store,
      runId,
      completedSteps,
      stepId,
      step,
      currentSteps,
      stepIndex,
      stepLocations,
      budget,
      onFailure,
      routeKind,
      target,
      persistedCompletion
    );
  }
  const recoveryGuard = recoveryGuardFailure(store, runId, workflow, completedSteps, stepId, step, budget);
  if (recoveryGuard !== undefined) return { result: recoveryGuard };
  const interruptedRoute = incompleteRecoveryRoutePayload(store, runId, stepId, failure.id);
  const resumingInterruptedRoute = interruptedRoute !== undefined;
  if (routeKind === "session" && sessionCanMerge(workflow, target)) {
    const staleApprovalIds = staleApprovalStepIdsAcrossLineage(store, runId);
    if (staleApprovalIds.length > 0) {
      return {
        result: finishFailure(store, runId, completedSteps, stepId, {
          exitCode: null,
          timedOut: false,
          message: staleApprovalMessage(staleApprovalIds, `merge-capable recovery session ${target}`)
        }, "failed", budget.terminalEffects)
      };
    }
  }
  if (!resumingInterruptedRoute) {
    const routeBudgetFailure = recoveryInvocationBudgetFailure(
      store,
      runId,
      completedSteps,
      stepId,
      budget
    );
    if (routeBudgetFailure !== undefined) return { result: routeBudgetFailure };
  }
  let workspaceBefore: ReturnType<typeof captureAgentFlowWorkspaceSnapshot>;
  let workspaceSnapshotContent: string | undefined;
  try {
    workspaceBefore = interruptedRoute === undefined
      ? captureAgentFlowWorkspaceSnapshot(store.repoRoot)
      : readRecoveryWorkspaceSnapshot(store, runId, stepId, failure.id, routeKind, target, interruptedRoute);
    if (interruptedRoute === undefined) {
      workspaceSnapshotContent = serializeRecoveryWorkspaceSnapshot(workspaceBefore);
    }
  } catch (error) {
    const message = redactAgentFlowSensitiveText(error instanceof Error ? error.message : String(error));
    return {
      result: finishRecoveryGuardFailure(store, runId, workflow, completedSteps, stepId, budget.terminalEffects, {
        eventType: "recovery.workspace_snapshot_failed",
        classification: "recovery_workspace_snapshot",
        message,
        payload: {},
        forcePause: true
      })
    };
  }
  if (!resumingInterruptedRoute) {
    store.withRunStateTransaction(runId, () => {
      const coordinates = recoveryWorkspaceSnapshotCoordinates(stepId, failure.id);
      const snapshotArtifact = store.writeArtifact({
        id: coordinates.id,
        runId,
        path: coordinates.path,
        kind: "recovery_workspace_snapshot",
        contentType: "application/json; charset=utf-8",
        content: workspaceSnapshotContent!,
        requiredRunStatus: "running",
        metadata: { failureId: failure.id, route: routeKind, target }
      });
      checkpointExecutionRouting(store, runId, budget);
      store.appendRunEvent(runId, {
        type: "recovery.routed",
        stepId,
        payload: {
          failureId: failure.id,
          route: routeKind,
          target,
          workspaceSnapshotPath: snapshotArtifact.declaredPath,
          workspaceSnapshotChecksum: snapshotArtifact.checksum
        }
      });
    });
  }

  let status: AgentFlowRecoveryStatus = "unresolved";
  let recoveryRunId: string | undefined;
  let message: string | undefined;
  let recoveryPolicyError: AgentFlowSessionPolicyError | undefined;
  try {
    if (routeKind === "workflow") {
      const nested = await executeNestedRecoveryWorkflow(
        store, runId, workflow, stepId, step, failure.id, failure.attempt ?? 1, route,
        transforms, sessionProviders, mcpCalls, notifications, workflows
      );
      status = nested.status;
      recoveryRunId = nested.runId;
      message = nested.message;
    } else {
      const sessionResult = resumingInterruptedRoute
        ? recoveredRecoverySessionResult(store, runId, stepId, failure.id, target)
          ?? await executeRecoverySession(
            store, runId, workflow, stepId, step, failure.id, failure.payloadPath, route, sessionProviders
          )
        : await executeRecoverySession(
          store, runId, workflow, stepId, step, failure.id, failure.payloadPath, route, sessionProviders
        );
      status = sessionResult.status;
      message = sessionResult.message;
    }
    const routeLockError = store.runLockInterruption();
    if (routeLockError !== undefined) throw routeLockError;
  } catch (error) {
    const lockError = store.runLockInterruption();
    if (lockError !== undefined) throw lockError;
    if (["AGENT_FLOW_RUN_LOCKED", "AGENT_FLOW_RUN_LOCK_LOST"].includes(agentFlowErrorCode(error) ?? "")) {
      throw error;
    }
    if (error instanceof AgentFlowSessionPolicyError) {
      recoveryPolicyError = error;
    } else {
      status = "unresolved";
      message = redactAgentFlowSensitiveText(error instanceof Error ? error.message : String(error));
    }
  }

  let changedPaths: string[];
  try {
    changedPaths = changedAgentFlowWorkspacePaths(
      workspaceBefore,
      captureAgentFlowWorkspaceSnapshot(store.repoRoot)
    );
  } catch (error) {
    const snapshotMessage = redactAgentFlowSensitiveText(error instanceof Error ? error.message : String(error));
    return {
      result: finishRecoveryGuardFailure(store, runId, workflow, completedSteps, stepId, budget.terminalEffects, {
        eventType: "recovery.workspace_snapshot_failed",
        classification: "recovery_workspace_snapshot",
        message: snapshotMessage,
        payload: {},
        forcePause: true
      })
    };
  }
  const routeFileScope = mapping(route.file_scope);
  const operationScopes = [
    ...recoveryOperationFileScopes(workflow.steps, step),
    ...(routeFileScope === undefined ? [] : [routeFileScope])
  ];
  const operationExcludes = operationScopes.flatMap((scope) => stringList(scope.exclude));
  const includeScopes = operationScopes.filter((scope) => stringList(scope.include).length > 0);
  const deniedPaths = changedPaths.filter((changedPath) => {
    const scopeLayers = includeScopes.length > 0 ? includeScopes : [undefined];
    return scopeLayers.some((scope) => {
      const includes = stringList(scope?.include);
      return evaluateAgentFlowPolicy(workflow, {
        kind: "file_write",
        rootPath: store.repoRoot,
        ...(routeKind === "session" ? { session: target } : {}),
        path: changedPath,
        ...(operationScopes.length === 0 ? {} : {
          fileScope: {
            ...(includes.length === 0 ? {} : { include: includes }),
            ...(operationExcludes.length === 0 ? {} : { exclude: operationExcludes })
          }
        })
      }).status !== "allow";
    });
  });
  if (deniedPaths.length > 0) {
    const displayed = deniedPaths.slice(0, 20);
    const suffix = deniedPaths.length > displayed.length
      ? ` (and ${deniedPaths.length - displayed.length} more)`
      : "";
    const scopeMessage = `Recovery remediation for step ${stepId} changed files outside its authorized scope: ${displayed.join(", ")}${suffix}.`;
    return {
      result: finishRecoveryGuardFailure(store, runId, workflow, completedSteps, stepId, budget.terminalEffects, {
        eventType: "recovery.workspace_scope_violated",
        classification: "recovery_unrelated_files",
        message: scopeMessage,
        payload: { changedPaths, deniedPaths, route: routeKind, target },
        forcePause: true
      })
    };
  }
  if (recoveryPolicyError !== undefined) {
    recordModelLimitDecision(store, runId, stepId, workflow, recoveryPolicyError);
    return {
      result: finishFailure(store, runId, completedSteps, stepId, {
        exitCode: null,
        timedOut: false,
        message: recoveryPolicyError.message
      }, recoveryPolicyError.status === "pause" ? "paused" : "failed", budget.terminalEffects)
    };
  }

  const handler = mapping(onFailure?.[status === "remediated" ? "on_remediated" : "on_unresolved"]);
  const returnTo = normalizedTarget(handler?.return_to);
  if (returnTo !== undefined && returnTo !== stepId) {
    throw invalidRuntimeRecoveryRoute(stepId, "on_remediated.return_to must name the failed step");
  }

  let recoveryPersisted: boolean;
  try {
    recoveryPersisted = store.withRunFinalizationTransaction(runId, () => {
      if (store.getRun(runId)?.status !== "running") return false;
      store.updateFailureRecovery(runId, failure.id, {
        status,
        route: routeKind,
        target,
        ...(recoveryRunId === undefined ? {} : { recoveryRunId }),
        ...(returnTo === undefined ? {} : { deferResolution: true })
      });
      persistRecoveryDecision(store, runId, stepId, failure.id, {
        status,
        route: routeKind,
        target,
        ...(recoveryRunId === undefined ? {} : { recoveryRunId }),
        ...(message === undefined ? {} : { message })
      });
      store.appendRunEvent(runId, {
        type: "recovery.completed",
        stepId,
        payload: {
          failureId: failure.id,
          status,
          route: routeKind,
          target,
          ...(recoveryRunId === undefined ? {} : { recoveryRunId }),
          ...(message === undefined ? {} : { message })
        }
      });
      return true;
    });
  } catch (error) {
    const persistenceMessage = `Could not persist the recovery decision for step ${stepId}: ${error instanceof Error ? error.message : String(error)}`;
    return {
      result: finishFailure(store, runId, completedSteps, stepId, {
        exitCode: null,
        timedOut: false,
        message: persistenceMessage
      }, "failed", budget.terminalEffects)
    };
  }
  if (!recoveryPersisted) {
    return { result: stoppedPipelineResult(store, runId, completedSteps)! };
  }
  const handlerTarget = returnTo ?? normalizedTarget(handler?.then);
  return routeAfterSuccessfulStep(
    store,
    runId,
    completedSteps,
    stepId,
    step,
    currentSteps,
    stepIndex,
    stepLocations,
    budget,
    handlerTarget
  );
}

function persistedRecoveryCompletion(
  store: AgentFlowRunStateStore,
  runId: string,
  stepId: string,
  failureId: string
): AgentFlowRunStateValue | undefined {
  return [...store.listEvents(runId)].reverse().find((event) =>
    event.stepId === stepId
      && event.type === "recovery.completed"
      && mapping(event.payload)?.failureId === failureId
  )?.payload;
}

function routePersistedRecoveryCompletion(
  store: AgentFlowRunStateStore,
  runId: string,
  completedSteps: string[],
  stepId: string,
  step: AgentFlowWorkflowStep,
  currentSteps: AgentFlowWorkflowStep[],
  stepIndex: number,
  stepLocations: Map<string, RuntimeStepLocation>,
  budget: SuccessfulRoutingBudget,
  onFailure: AgentFlowYamlMapping | undefined,
  routeKind: "session" | "workflow",
  target: string,
  payload: AgentFlowRunStateValue
): SuccessfulRoute {
  const completion = mapping(payload);
  const status = normalizedTarget(completion?.status);
  if ((status !== "remediated" && status !== "unresolved")
      || normalizedTarget(completion?.route) !== routeKind
      || normalizedTarget(completion?.target) !== target) {
    throw new AgentFlowRunStateError(
      `Agent Flow run ${runId} cannot route the persisted recovery decision for step ${stepId} because it no longer matches the workflow.`,
      "AGENT_FLOW_RUN_LOCK_RECOVERY"
    );
  }
  const handler = mapping(onFailure?.[status === "remediated" ? "on_remediated" : "on_unresolved"]);
  const returnTo = normalizedTarget(handler?.return_to);
  if (returnTo !== undefined && returnTo !== stepId) {
    throw invalidRuntimeRecoveryRoute(stepId, "on_remediated.return_to must name the failed step");
  }
  return routeAfterSuccessfulStep(
    store,
    runId,
    completedSteps,
    stepId,
    step,
    currentSteps,
    stepIndex,
    stepLocations,
    budget,
    returnTo ?? normalizedTarget(handler?.then)
  );
}

function recoveryOperationFileScopes(
  steps: AgentFlowWorkflowStep[],
  target: AgentFlowWorkflowStep,
  inherited: AgentFlowYamlMapping[] = []
): AgentFlowYamlMapping[] {
  for (const step of steps) {
    const ownScope = mapping(step.file_scope);
    const scopes = ownScope === undefined ? inherited : [...inherited, ownScope];
    if (step === target) return scopes;
    const fields = normalizedTarget(step.type) === "parallel"
      ? ["branches", "body", "steps"]
      : ["body", "steps"];
    for (const field of fields) {
      const nested = step[field];
      if (!Array.isArray(nested)) continue;
      const found = recoveryOperationFileScopes(nested.filter(isWorkflowStep), target, scopes);
      if (found.length > 0) return found;
    }
  }
  return [];
}

function recoveryGuardFailure(
  store: AgentFlowRunStateStore,
  runId: string,
  workflow: AgentFlowWorkflow,
  completedSteps: string[],
  stepId: string,
  step: AgentFlowWorkflowStep,
  budget: SuccessfulRoutingBudget
): AgentFlowCommandPipelineResult | undefined {
  if (workflow.style !== "recovery_pipeline") return undefined;
  const run = store.getRun(runId)!;
  const limits = mapping(workflow.limits);
  const duration = typeof limits?.max_duration_seconds === "number"
    ? { name: "max_duration_seconds", value: limits.max_duration_seconds, milliseconds: limits.max_duration_seconds * 1_000 }
    : typeof limits?.max_duration_minutes === "number"
      ? { name: "max_duration_minutes", value: limits.max_duration_minutes, milliseconds: limits.max_duration_minutes * 60_000 }
      : undefined;
  if (duration !== undefined && run.startedAt !== null &&
      Date.parse(store.currentTimestamp()) - Date.parse(run.startedAt) >= duration.milliseconds) {
    return finishRecoveryGuardFailure(store, runId, workflow, completedSteps, stepId, budget.terminalEffects, {
      eventType: "recovery.limit_reached",
      classification: "recovery_duration_limit",
      message: `Recovery duration exceeded limits.${duration.name} ${duration.value}.`,
      payload: { limit: duration.name, configured: duration.value }
    });
  }

  const declarations = [workflow.short_circuit_if, step.short_circuit_if]
    .flatMap((value) => Array.isArray(value) ? value : [])
    .filter((value): value is string => typeof value === "string");
  for (const expression of declarations) {
    let matched = false;
    try {
      const artifactCache = new Map<string, AgentFlowYamlValue>();
      const resolve: AgentFlowConditionReferenceResolver = (scope, segments) => {
        if (scope === "artifacts" && segments[0] === "budget") {
          return recoveryBudgetReference(store, runId, workflow, segments.slice(1));
        }
        if (scope === "artifacts" && segments[0] === "failures") {
          return recoveryFailureReference(store, runId, segments.slice(1));
        }
        return resolveAgentFlowConditionReference(store, runId, scope, segments, artifactCache);
      };
      const artifactPaths = new Set([
        ...store.listArtifactMetadata(runId).map((artifact) => artifact.declaredPath),
        ...agentFlowConditionDeclaredArtifactPaths(workflow.steps)
      ]);
      preflightAgentFlowFailureClassificationReferences([expression], resolve, artifactPaths);
      matched = evaluateAgentFlowConditionWithResolver(expression, resolve, { missingReferences: "false" });
    } catch (error) {
      if (error instanceof AgentFlowConditionError &&
          (error.message.includes("does not match a published JSON artifact") ||
           error.message.includes("did not resolve to a value"))) {
        continue;
      }
      const classificationError = error instanceof AgentFlowFailureClassificationError ? error : undefined;
      const evaluationError = redactAgentFlowSensitiveText(error instanceof Error ? error.message : String(error));
      return finishRecoveryGuardFailure(store, runId, workflow, completedSteps, stepId, budget.terminalEffects, {
        eventType: "recovery.short_circuit_failed",
        classification: classificationError?.code === "AGENT_FLOW_FAILURE_CLASSIFICATION_UNKNOWN"
          ? "failure_classification_unknown"
          : classificationError === undefined ? "recovery_short_circuit_evaluation" : "failure_classification_invalid",
        message: `Recovery short circuit ${JSON.stringify(expression)} could not be evaluated: ${evaluationError}`,
        payload: { expression, error: evaluationError },
        forcePause: true
      });
    }
    if (matched) {
      return finishRecoveryGuardFailure(store, runId, workflow, completedSteps, stepId, budget.terminalEffects, {
        eventType: "recovery.short_circuited",
        classification: "recovery_short_circuit",
        message: `Recovery short circuit matched ${JSON.stringify(expression)}.`,
        payload: { expression },
        forcePause: true
      });
    }
  }
  return undefined;
}

function runtimeRecoveryLimitConfigurationIssue(
  workflow: AgentFlowWorkflow
): { code: string; path: string; message: string } | undefined {
  if (workflow.style !== "recovery_pipeline") {
    const invalidPath = workflow.short_circuit_if !== undefined
      ? "short_circuit_if"
      : runtimeRecoveryLimitSteps(workflow.steps)
        .find(({ step }) => step.short_circuit_if !== undefined)?.path;
    if (invalidPath !== undefined) {
      return {
        code: "workflow.recovery.short_circuit.style",
        path: invalidPath === "short_circuit_if" ? invalidPath : `${invalidPath}.short_circuit_if`,
        message: "Recovery short_circuit_if is only supported by recovery_pipeline workflows."
      };
    }
  }
  if (workflow.style !== "recovery_pipeline") return undefined;
  const limits = mapping(workflow.limits);
  if (limits?.max_duration_seconds !== undefined && limits.max_duration_minutes !== undefined) {
    return {
      code: "workflow.recovery.duration.ambiguous",
      path: "limits",
      message: "Recovery duration must use either limits.max_duration_seconds or limits.max_duration_minutes, not both."
    };
  }
  for (const field of ["max_duration_seconds", "max_duration_minutes"] as const) {
    const value = limits?.[field];
    if (value !== undefined && (typeof value !== "number" || !Number.isFinite(value) || value <= 0)) {
      return {
        code: "workflow.policy.budget.invalid",
        path: `limits.${field}`,
        message: `Budget limit limits.${field} must be a positive finite number.`
      };
    }
  }
  const recoveryLimitPolicy = mapping(workflow.policies)?.recovery_limits;
  if (recoveryLimitPolicy !== undefined && recoveryLimitPolicy !== "pause" && recoveryLimitPolicy !== "fail") {
    return {
      code: "workflow.policy.recovery_limits.invalid",
      path: "policies.recovery_limits",
      message: "Recovery-limit policy must be pause or fail."
    };
  }

  const declarations = [
    { value: workflow.short_circuit_if, path: "short_circuit_if" },
    ...runtimeRecoveryLimitSteps(workflow.steps)
      .map(({ step, path }) => ({ value: step.short_circuit_if, path: `${path}.short_circuit_if` }))
  ];
  for (const { step, path } of runtimeRecoveryLimitSteps(workflow.steps)) {
    const output = typeof step.output === "string" && step.output.trim().length > 0 ? step.output.trim() : undefined;
    const saveAs = normalizedTarget(step.type) === "input_request" && typeof step.save_as === "string" && step.save_as.trim().length > 0
      ? step.save_as.trim()
      : undefined;
    const outputs = [
      ...stringList(step.outputs).map((value) => ({ value, field: "outputs" })),
      ...(output === undefined ? [] : [{ value: output, field: "output" }]),
      ...(saveAs === undefined ? [] : [{ value: saveAs, field: "save_as" }])
    ];
    const reserved = outputs.find((candidate) => {
      try {
        const namespace = agentFlowConditionArtifactAlias(normalizeAgentFlowArtifactPath(candidate.value))[0];
        return namespace === "budget" || namespace === "failures";
      } catch {
        return false;
      }
    });
    if (reserved !== undefined) {
      const namespace = agentFlowConditionArtifactAlias(normalizeAgentFlowArtifactPath(reserved.value))[0];
      return {
        code: "workflow.recovery.short_circuit.namespace.reserved",
        path: `${path}.${reserved.field}`,
        message: `Artifact output ${JSON.stringify(reserved.value)} uses reserved recovery short-circuit namespace ${namespace}.`
      };
    }
  }
  for (const declaration of declarations) {
    if (declaration.value === undefined) continue;
    if (!Array.isArray(declaration.value) || declaration.value.length === 0 ||
        declaration.value.some((entry) => typeof entry !== "string" || entry.trim().length === 0)) {
      return {
        code: "workflow.recovery.short_circuit.invalid",
        path: declaration.path,
        message: "Recovery short_circuit_if must be a non-empty list of condition expressions."
      };
    }
    const unsupportedIndex = declaration.value.findIndex((entry) =>
      agentFlowConditionExpressionError(entry as string) !== undefined
    );
    if (unsupportedIndex >= 0) {
      return {
        code: "workflow.recovery.short_circuit.expression.unsupported",
        path: `${declaration.path}[${unsupportedIndex}]`,
        message: agentFlowConditionExpressionError(declaration.value[unsupportedIndex] as string)!
      };
    }
    const undeclaredInputIndex = declaration.value.findIndex((entry) =>
      agentFlowConditionReferences(entry as string).some((reference) =>
        reference.scope === "inputs" && !Object.hasOwn(workflow.inputs ?? {}, reference.segments[0]!)
      )
    );
    if (undeclaredInputIndex >= 0) {
      const reference = agentFlowConditionReferences(declaration.value[undeclaredInputIndex] as string)
        .find((candidate) => candidate.scope === "inputs" &&
          !Object.hasOwn(workflow.inputs ?? {}, candidate.segments[0]!))!;
      return {
        code: "workflow.input.undeclared",
        path: `${declaration.path}[${undeclaredInputIndex}]`,
        message: `Input ${JSON.stringify(reference.segments[0])} is referenced but not declared in workflow inputs.`
      };
    }
  }
  return undefined;
}

function runtimeRecoveryLimitSteps(
  steps: AgentFlowWorkflowStep[],
  basePath = "steps"
): Array<{ step: AgentFlowWorkflowStep; path: string }> {
  const result: Array<{ step: AgentFlowWorkflowStep; path: string }> = [];
  steps.forEach((step, index) => {
    const path = `${basePath}[${index}]`;
    result.push({ step, path });
    for (const field of ["body", "steps", "branches"] as const) {
      const nested = step[field];
      if (Array.isArray(nested)) {
        result.push(...runtimeRecoveryLimitSteps(nested.filter(isWorkflowStep), `${path}.${field}`));
      }
    }
  });
  return result;
}

function recoveryBudgetReference(
  store: AgentFlowRunStateStore,
  runId: string,
  workflow: AgentFlowWorkflow,
  segments: string[]
): AgentFlowRunStateValue | undefined {
  if (segments.length !== 1 || !segments[0]!.endsWith("_remaining")) return undefined;
  const kind = segments[0]!.slice(0, -"_remaining".length);
  const configured = mapping(workflow.limits)?.[`max_${kind}`];
  if (typeof configured !== "number" || !Number.isFinite(configured)) return undefined;
  return Math.max(0, configured - (store.getBudget(runId, `model:${kind}`)?.used ?? 0));
}

function recoveryFailureReference(
  store: AgentFlowRunStateStore,
  runId: string,
  segments: string[]
): AgentFlowRunStateValue | undefined {
  if (segments.length < 2 || segments.at(-1) !== "attempts") return undefined;
  const stepId = segments.slice(0, -1).join(".");
  const attempts = store.listFailures(runId)
    .filter((failure) => failure.stepId === stepId)
    .map((failure) => failure.attempt ?? 1);
  return attempts.length === 0 ? undefined : Math.max(...attempts);
}

function finishRecoveryGuardFailure(
  store: AgentFlowRunStateStore,
  runId: string,
  workflow: AgentFlowWorkflow,
  completedSteps: string[],
  stepId: string,
  terminalEffects: AgentFlowPipelineTerminalEffects,
  decision: {
    eventType:
      | "recovery.limit_reached"
      | "recovery.short_circuited"
      | "recovery.short_circuit_failed"
      | "recovery.workspace_scope_violated"
      | "recovery.workspace_snapshot_failed";
    classification:
      | "recovery_duration_limit"
      | "recovery_short_circuit"
      | "recovery_short_circuit_evaluation"
      | "recovery_unrelated_files"
      | "recovery_workspace_snapshot"
      | "failure_classification_invalid"
      | "failure_classification_unknown";
    message: string;
    payload: Record<string, AgentFlowRunStateValue>;
    forcePause?: boolean;
  }
): AgentFlowCommandPipelineResult {
  const outcome = decision.forcePause === true ? "pause" : recoveryLimitOutcome(workflow);
  const attempt = Math.max(1, store.listFailures(runId).filter((failure) => failure.stepId === stepId).length + 1);
  persistAgentFlowFailurePayload(store, {
    id: `recovery:${safeId(stepId)}:${safeId(decision.classification)}:attempt-${attempt}`,
    runId,
    stepId,
    stepType: "recovery_guard",
    attempt,
    exitCode: null,
    summary: decision.message,
    classification: decision.classification,
    retryable: false,
    outcome,
    indexPayload: { attempt, ...decision.payload, message: decision.message, outcome }
  });
  store.appendRunEvent(runId, {
    type: decision.eventType,
    stepId,
    payload: { ...decision.payload, message: decision.message, outcome }
  });
  return finishFailure(store, runId, completedSteps, stepId, {
    exitCode: null,
    timedOut: false,
    message: decision.message
  }, outcome === "fail" ? "failed" : "paused", terminalEffects);
}

function recoveryLimitOutcome(workflow: AgentFlowWorkflow): "pause" | "fail" {
  return workflow.style === "recovery_pipeline" && mapping(workflow.policies)?.recovery_limits === "fail"
    ? "fail"
    : "pause";
}

function recordModelLimitDecision(
  store: AgentFlowRunStateStore,
  runId: string,
  stepId: string,
  workflow: AgentFlowWorkflow,
  error: AgentFlowSessionPolicyError,
  persistFailure = true
): void {
  if (workflow.style !== "recovery_pipeline" || error.code !== "policy.budget.exhausted") return;
  const details = modelLimitDetails(error)!;
  if (persistFailure) {
    const attempt = Math.max(1, store.listFailures(runId).filter((failure) => failure.stepId === stepId).length + 1);
    persistAgentFlowFailurePayload(store, {
      id: `recovery:${safeId(stepId)}:recovery-model-limit:attempt-${attempt}`,
      runId,
      stepId,
      stepType: "recovery_guard",
      attempt,
      exitCode: null,
      summary: error.message,
      classification: "recovery_model_limit",
      retryable: false,
      outcome: details.outcome,
      indexPayload: { attempt, ...details }
    });
  }
  store.appendRunEvent(runId, {
    type: "recovery.limit_reached",
    stepId,
    payload: details
  });
}

function modelLimitDetails(error: AgentFlowSessionPolicyError): {
  limit: string;
  message: string;
  outcome: "pause" | "fail";
} | undefined {
  if (error.code !== "policy.budget.exhausted") return undefined;
  const budget = /Budget "([^"]+)"/.exec(error.message)?.[1] ?? "model_calls";
  return {
    limit: `max_${budget}`,
    message: error.message,
    outcome: error.status === "fail" ? "fail" : "pause"
  };
}

function resolveReturnedRecoveryFailures(
  store: AgentFlowRunStateStore,
  runId: string,
  completedSteps: string[],
  stepId: string,
  successfulAttempt: number,
  terminalEffects: AgentFlowPipelineTerminalEffects
): AgentFlowCommandPipelineResult | undefined {
  try {
    store.withRunFinalizationTransaction(runId, () => {
      for (const failure of store.listPendingReturnedRecoveryFailures(runId, stepId, successfulAttempt)) {
        store.resolveFailure(runId, failure.id);
        store.appendRunEvent(runId, {
          type: "recovery.returned",
          stepId,
          payload: { failureId: failure.id, failedAttempt: failure.attempt, successfulAttempt }
        });
      }
    });
    return undefined;
  } catch (error) {
    const message = `Could not persist return-to recovery completion for step ${stepId}: ${error instanceof Error ? error.message : String(error)}`;
    return finishFailure(store, runId, completedSteps, stepId, {
      exitCode: null,
      timedOut: false,
      message
    }, "failed", terminalEffects);
  }
}

async function executeNestedRecoveryWorkflow(
  store: AgentFlowRunStateStore,
  parentRunId: string,
  parentWorkflow: AgentFlowWorkflow,
  stepId: string,
  parentStep: AgentFlowWorkflowStep,
  failureId: string,
  attempt: number,
  route: AgentFlowYamlMapping,
  transforms: AgentFlowArtifactTransformRegistry,
  sessionProviders: AgentFlowSessionProviderRegistry,
  mcpCalls: AgentFlowMcpCallRegistry,
  notifications: AgentFlowNotificationRegistry,
  workflows: AgentFlowWorkflowRegistry
): Promise<{ status: AgentFlowRecoveryStatus; runId: string; message?: string }> {
  const workflowName = normalizedTarget(route.workflow)!;
  const nestedWorkflow = workflows.get(workflowName);
  if (nestedWorkflow === undefined) {
    throw new AgentFlowRunStateError(
      `Recovery workflow ${workflowName} is not registered.`,
      "AGENT_FLOW_RECOVERY_WORKFLOW_UNKNOWN"
    );
  }
  const parent = store.getRun(parentRunId)!;
  assertRecoveryWorkflowNotInLineage(store, parentRunId, nestedWorkflow);
  const failure = store.listFailures(parentRunId).find((entry) => entry.id === failureId)!;
  const resolvedInputs = resolveRecoveryInputs(route.inputs, parent.inputs, stepId, failure.payloadPath);
  const preparedInputs = prepareNestedRecoveryInputs(
    store,
    parentRunId,
    failureId,
    route.inputs,
    resolvedInputs,
    failure.payloadPath,
    parentWorkflow,
    nestedWorkflow
  );
  const inputs = preparedInputs.inputs;
  assertNestedRecoveryRequiredInputs(nestedWorkflow, inputs);
  const recoveryRunId = `${parentRunId}:recovery:${safeId(stepId)}:attempt-${attempt}`;
  const existing = store.getRun(recoveryRunId);
  if (existing !== null) {
    assertExistingRecoveryRunIdentity(existing, nestedWorkflow, inputs, parentRunId);
  }
  if (existing !== null && existing.status !== "pending" && existing.status !== "running") {
    const output = mapping(existing.output);
    const resultStatus = normalizedTarget(output?.resultStatus);
    const status = existing.status === "completed" && resultStatus === "remediated"
      ? "remediated"
      : "unresolved";
    if (status === "remediated") {
      promoteNestedRecoveryOutputs(store, parentRunId, recoveryRunId, parentStep, nestedWorkflow);
    }
    return {
      status,
      runId: recoveryRunId,
      ...(typeof output?.message === "string" ? { message: output.message } : {})
    };
  }
  if (existing === null) {
    store.withRunFinalizationTransaction(parentRunId, () => {
      const result = createAgentFlowLifecycleRun(store, {
        id: recoveryRunId,
        workflow: nestedWorkflow,
        inputs,
        parentRunId,
        recoveryOfRunId: parentRunId
      });
      if (result.changed) {
        copyRecoveryInputArtifacts(store, parentRunId, recoveryRunId, preparedInputs);
      }
      return result;
    });
  }
  const propagateParentStop = (): void => {
    const stopped = activeStopStatus(store, parentRunId);
    const child = store.getRun(recoveryRunId);
    if (stopped === undefined || child === null || ["completed", "failed", "cancelled"].includes(child.status)) return;
    try {
      transitionAgentFlowLifecycleRun(
        store,
        recoveryRunId,
        stopped === "paused" ? "pause" : "cancel",
        notifications
      );
    } catch {
      // The child may have reached a terminal state between the read and transition.
    }
  };
  propagateParentStop();
  const childBeforeStart = store.getRun(recoveryRunId);
  if (childBeforeStart !== null
      && childBeforeStart.status !== "pending"
      && childBeforeStart.status !== "running") {
    return {
      status: "unresolved",
      runId: recoveryRunId,
      message: `Recovery run ${recoveryRunId} was ${childBeforeStart.status} before execution started.`
    };
  }
  const stopMonitor = setInterval(propagateParentStop, 25);
  let result: AgentFlowCommandPipelineResult;
  let terminalizeChildOnError = false;
  try {
    let lockAttempt = 0;
    while (true) {
      lockAttempt += 1;
      const startedStepCount = store.listEvents(recoveryRunId)
        .filter((event) => event.type === "step.started").length;
      try {
        result = await store.withRunLock(recoveryRunId, "run", (lock) => {
          terminalizeChildOnError = true;
          const childRun = assertPersistedWorkflowIdentity(store, recoveryRunId, nestedWorkflow);
          assertOrPersistConfiguredProviderBindings(store, childRun, nestedWorkflow, sessionProviders);
          const recoveredAttempts = recoverInterruptedExecution(store, lock);
          return runAgentFlowCommandPipeline(
            store,
            recoveryRunId,
            nestedWorkflow,
            undefined,
            transforms,
            sessionProviders,
            mcpCalls,
            notifications,
            workflows,
            () => promoteNestedRecoveryOutputs(store, parentRunId, recoveryRunId, parentStep, nestedWorkflow),
            recoveredAttempts
          );
        });
        break;
      } catch (error) {
        if (agentFlowErrorCode(error) === "AGENT_FLOW_CONCURRENT_MUTATION") {
          const stepStarted = store.listEvents(recoveryRunId)
            .filter((event) => event.type === "step.started").length > startedStepCount;
          if (!stepStarted) {
            terminalizeChildOnError = false;
            if (lockAttempt < 3) {
              await Bun.sleep(lockAttempt * 25);
              continue;
            }
          }
        }
        throw error;
      }
    }
  } catch (error) {
    const message = redactAgentFlowSensitiveText(error instanceof Error ? error.message : String(error));
    if (["AGENT_FLOW_RUN_LOCKED", "AGENT_FLOW_RUN_LOCK_LOST"].includes(agentFlowErrorCode(error) ?? "")) {
      throw error;
    }
    if (!terminalizeChildOnError) {
      return { status: "unresolved", runId: recoveryRunId, message };
    }
    const child = store.getRun(recoveryRunId);
    if (child !== null && !["completed", "failed", "cancelled"].includes(child.status)) {
      store.withRunFinalizationTransaction(recoveryRunId, () => {
        const current = store.getRun(recoveryRunId);
        if (current === null || ["completed", "failed", "cancelled"].includes(current.status)) return;
        store.updateRun(recoveryRunId, {
          currentStepId: null,
          error: { code: "recovery.startup.failed", message }
        });
        store.transitionRunWithEvent(recoveryRunId, {
          status: "failed",
          allowedFrom: ["pending", "running", "waiting", "paused"],
          event: { type: "run.failed", payload: { code: "recovery.startup.failed", message } }
        });
      });
    }
    return { status: "unresolved", runId: recoveryRunId, message };
  } finally {
    clearInterval(stopMonitor);
  }
  const status = result.status === "completed" && result.resultStatus === "remediated"
    ? "remediated"
    : "unresolved";
  return {
    status,
    runId: recoveryRunId,
    ...(result.message === undefined ? {} : { message: result.message })
  };
}

function agentFlowErrorCode(error: unknown): string | undefined {
  if (typeof error !== "object" || error === null || !("code" in error)) return undefined;
  return typeof error.code === "string" ? error.code : undefined;
}

function assertNestedRecoveryRequiredInputs(
  workflow: AgentFlowWorkflow,
  inputs: Record<string, AgentFlowRunStateValue>
): void {
  const missing = Object.entries(workflow.inputs ?? {}).flatMap(([name, definition]) =>
    mapping(definition)?.required === true && !Object.hasOwn(inputs, name) ? [name] : []
  );
  if (missing.length === 0) return;
  throw new AgentFlowRunStateError(
    `Recovery workflow ${workflow.name} is missing required route inputs: ${missing.sort().join(", ")}.`,
    "AGENT_FLOW_RECOVERY_INPUT_REQUIRED"
  );
}

function promoteNestedRecoveryOutputs(
  store: AgentFlowRunStateStore,
  parentRunId: string,
  recoveryRunId: string,
  parentStep: AgentFlowWorkflowStep,
  workflow: AgentFlowWorkflow
): void {
  const parentStepId = normalizedTarget(parentStep.id)!;
  const writes: WriteAgentFlowArtifactInput[] = [];
  for (const declaredPath of nestedWorkflowOutputPaths(workflow.steps)) {
    const existing = store.getArtifact(parentRunId, declaredPath);
    if (existing?.metadata.recoveryRunId === recoveryRunId) continue;
    const childArtifact = store.getArtifact(recoveryRunId, declaredPath);
    if (childArtifact === null || childArtifact.writtenAt === null) continue;
    if (childArtifact.kind === "recovery_input") continue;
    const content = store.readArtifact(recoveryRunId, declaredPath).content;
    const parentPublication = parentOutputPublication(parentStep, declaredPath);
    writes.push({
      id: existing?.id
        ?? parentPublication?.id
        ?? `recovery-output:${createHash("sha256").update(`${recoveryRunId}:${declaredPath}`).digest("hex")}`,
      runId: parentRunId,
      stepId: existing?.producerStepId ?? parentStepId,
      path: declaredPath,
      kind: existing?.kind ?? parentPublication?.kind ?? childArtifact.kind,
      contentType: childArtifact.contentType,
      content,
      overwrite: existing !== null,
      requiredRunStatus: "running",
      metadata: {
        ...(existing?.metadata ?? {}),
        ...(existing === null ? parentPublication?.metadata ?? {} : {}),
        recoveryRunId,
        recoveryArtifactId: childArtifact.id,
        ...(childArtifact.producerStepId === null ? {} : { recoveryProducerStepId: childArtifact.producerStepId })
      }
    });
  }
  if (writes.length === 0) return;
  store.writeArtifactsAtomically(writes);
  store.appendRunEvent(parentRunId, {
    type: "recovery.outputs.promoted",
    stepId: parentStepId,
    payload: {
      recoveryRunId,
      artifacts: writes.map((write) => write.path)
    }
  });
}

function parentOutputPublication(
  step: AgentFlowWorkflowStep,
  declaredPath: string
): { id: string; kind: string; metadata: Record<string, AgentFlowRunStateValue> } | undefined {
  const normalizedPath = normalizeAgentFlowArtifactPath(declaredPath);
  const outputPaths = [step.output, step.save_as, ...stringList(step.outputs)]
    .flatMap((value) => typeof value === "string" ? [normalizeAgentFlowArtifactPath(value)] : []);
  if (!outputPaths.includes(normalizedPath)) return undefined;
  const digest = createHash("sha256").update(normalizedPath).digest("hex");
  const type = normalizedTarget(step.type);
  if (type === "command") return { id: `command-output:${digest}`, kind: "command_output", metadata: {} };
  if (type === "artifact_transform") {
    return {
      id: `artifact-transform:${digest}`,
      kind: "artifact_transform",
      metadata: {
        ...(typeof step.transform === "string" ? { transform: step.transform.trim() } : {}),
        ...(typeof step.input === "string" ? { input: normalizeAgentFlowArtifactPath(step.input) } : {})
      }
    };
  }
  if (type === "session_request") {
    return {
      id: `session-output:${digest}`,
      kind: "session_output",
      metadata: typeof step.session === "string" ? { sessionId: step.session.trim() } : {}
    };
  }
  if (type === "mcp_call") {
    return {
      id: `mcp-output:${digest}`,
      kind: "mcp_output",
      metadata: {
        ...(typeof step.server === "string" ? { server: step.server.trim() } : {}),
        ...(typeof step.tool === "string" ? { tool: step.tool.trim() } : {})
      }
    };
  }
  return undefined;
}

function nestedWorkflowOutputPaths(steps: AgentFlowWorkflowStep[]): string[] {
  const outputs = new Set<string>();
  const visit = (step: AgentFlowWorkflowStep): void => {
    const stepId = normalizedTarget(step.id);
    const type = normalizedTarget(step.type);
    if (stepId !== undefined && type === "approval" && step.output === undefined) {
      outputs.add(defaultAgentFlowApprovalOutputPath(stepId));
    }
    if (stepId !== undefined && type === "decision_record" && step.output === undefined) {
      outputs.add(defaultAgentFlowDecisionRecordPath(stepId));
    }
    for (const value of [step.output, step.save_as]) {
      if (typeof value === "string" && value.trim().length > 0) outputs.add(value.trim());
    }
    for (const value of normalizedStringList(step.outputs)) outputs.add(value);
    for (const field of ["body", "steps", "branches"]) {
      const nested = step[field];
      if (Array.isArray(nested)) nested.filter(isWorkflowStep).forEach(visit);
    }
  };
  steps.forEach(visit);
  return [...outputs].sort();
}

function assertExistingRecoveryRunIdentity(
  existing: NonNullable<ReturnType<AgentFlowRunStateStore["getRun"]>>,
  workflow: AgentFlowWorkflow,
  inputs: Record<string, AgentFlowRunStateValue>,
  parentRunId: string
): void {
  if (existing.workflowName === workflow.name
      && existing.workflowVersion === workflow.version
      && existing.workflowStyle === workflow.style
      && existing.workflowMaturity === workflow.maturity
      && existing.parentRunId === parentRunId
      && existing.recoveryOfRunId === parentRunId
      && isDeepStrictEqual(existing.context.workflow, workflow)
      && isDeepStrictEqual(existing.inputs, inputs)) return;
  throw new AgentFlowRunStateError(
    `Recovery run ${existing.id} already exists with different workflow, inputs, or lineage.`,
    "AGENT_FLOW_RUN_COLLISION"
  );
}

function assertRecoveryWorkflowNotInLineage(
  store: AgentFlowRunStateStore,
  parentRunId: string,
  nestedWorkflow: AgentFlowWorkflow
): void {
  const visited = new Set<string>();
  let current = store.getRun(parentRunId);
  while (current !== null && !visited.has(current.id)) {
    visited.add(current.id);
    if (isDeepStrictEqual(current.context.workflow, nestedWorkflow)) {
      throw new AgentFlowRunStateError(
        `Recovery workflow ${nestedWorkflow.name} is already present in run ${current.id}'s recovery lineage.`,
        "AGENT_FLOW_RECOVERY_WORKFLOW_RECURSIVE"
      );
    }
    current = current.parentRunId === null ? null : store.getRun(current.parentRunId);
  }
}

async function executeRecoverySession(
  store: AgentFlowRunStateStore,
  runId: string,
  workflow: AgentFlowWorkflow,
  stepId: string,
  failedStep: AgentFlowWorkflowStep,
  failureId: string,
  failurePath: string | null,
  route: AgentFlowYamlMapping,
  providers: AgentFlowSessionProviderRegistry
): Promise<{ status: AgentFlowRecoveryStatus; message?: string }> {
  const sessionId = normalizedTarget(route.session)!;
  const session = mapping(workflow.sessions?.[sessionId])!;
  const provider = normalizedTarget(session.provider)!;
  const adapter = providers.get(provider)!;
  const providerDescriptor = providers.describe(provider)!;
  const promptPath = normalizedTarget(route.prompt)!;
  let prompt: ReturnType<typeof readAgentFlowSessionPrompt>;
  let sourcePromptChecksum: string;
  let promptWasRedacted = false;
  try {
    preflightAgentFlowTextInputPath(workflow, `Recovery session ${stepId} prompt`, promptPath);
    const sourcePrompt = readAgentFlowSessionPrompt(store.repoRoot, promptPath);
    const securedPrompt = secureAgentFlowTextInput(
      workflow,
      `Recovery session ${stepId} prompt`,
      sourcePrompt.content,
      sourcePrompt.path
    );
    sourcePromptChecksum = sourcePrompt.checksum;
    promptWasRedacted = securedPrompt.redacted;
    prompt = {
      ...sourcePrompt,
      content: securedPrompt.value,
      checksum: `sha256:${createHash("sha256").update(securedPrompt.value).digest("hex")}`
    };
    if (Buffer.byteLength(prompt.content, "utf8") > MAX_AGENT_FLOW_SESSION_PROMPT_BYTES) {
      throw new AgentFlowSessionRequestError(
        `Recovery prompt ${prompt.path} exceeds the ${MAX_AGENT_FLOW_SESSION_PROMPT_BYTES}-byte session prompt limit after sensitive-data handling.`,
        "AGENT_FLOW_SESSION_PROMPT_TOO_LARGE"
      );
    }
  } catch (error) {
    if (error instanceof AgentFlowSensitiveInputError) {
      throw new AgentFlowSessionRequestError(error.message, error.code, { cause: error });
    }
    throw error;
  }
  const run = store.getRun(runId)!;
  const resolvedInputs = resolveRecoveryInputs(route.inputs, run.inputs, stepId, failurePath);
  const inputPaths = new Set<string>();
  const sensitiveInputPaths = new Set<string>();
  collectRecoveryArtifactPaths(store, runId, resolvedInputs, inputPaths, sensitiveInputPaths);
  collectRecoveryReferencedSensitiveArtifactPaths(
    store,
    runId,
    route.inputs,
    resolvedInputs,
    sensitiveInputPaths
  );
  const recoveryStepId = `${stepId}:recovery`;
  const hasInputManifest = Object.keys(resolvedInputs).length > 0;
  let inputManifestPath: string | undefined;
  let sourceInputManifestBytes = 0;
  if (failurePath !== null && recoveryValueReferencesPath(resolvedInputs, failurePath)) {
    const availableSlots = MAX_AGENT_FLOW_SESSION_INPUTS - inputPaths.size - (hasInputManifest ? 1 : 0);
    collectRecoveryFailureArtifactPaths(store, runId, failurePath, inputPaths, availableSlots);
  }
  if (hasInputManifest) {
    const sourceManifest = assertRecoverySessionInputManifestSize(
      resolvedInputs,
      recoveryStepId,
      "before sensitive-data handling"
    );
    sourceInputManifestBytes = Buffer.byteLength(sourceManifest, "utf8");
    let securedManifest: Record<string, AgentFlowRunStateValue>;
    try {
      const provenanceSecuredManifest = secureRecoveryReferencedInputValues(
        workflow,
        `Recovery session ${recoveryStepId} input manifest`,
        route.inputs,
        resolvedInputs
      );
      securedManifest = secureAgentFlowJsonInput(
        workflow,
        `Recovery session ${recoveryStepId} input manifest`,
        provenanceSecuredManifest
      ).value;
    } catch (error) {
      if (error instanceof AgentFlowSensitiveInputError) {
        throw new AgentFlowSessionRequestError(error.message, error.code, { cause: error });
      }
      throw error;
    }
    inputManifestPath = persistRecoverySessionInputs(
      store,
      runId,
      recoveryStepId,
      failureId,
      securedManifest
    );
    inputPaths.add(inputManifestPath);
  }
  if (inputPaths.size > MAX_AGENT_FLOW_SESSION_INPUTS) {
    throw new AgentFlowSessionRequestError(
      `Recovery session ${recoveryStepId} declares ${inputPaths.size} inputs; at most ${MAX_AGENT_FLOW_SESSION_INPUTS} are allowed.`,
      "AGENT_FLOW_SESSION_INPUT_LIMIT"
    );
  }
  const inputs: Array<ReturnType<typeof readAgentFlowSessionInput>> = [];
  const sourceInputChecksums = new Map<string, string>();
  const redactedInputPaths = new Set<string>();
  let totalSourceInputBytes = 0;
  let totalProviderInputBytes = 0;
  for (const artifactPath of [...inputPaths].sort()) {
    try {
      preflightAgentFlowTextInputPath(
        workflow,
        `Recovery session ${recoveryStepId} input`,
        artifactPath
      );
    } catch (error) {
      if (error instanceof AgentFlowSensitiveInputError) {
        throw new AgentFlowSessionRequestError(error.message, error.code, { cause: error });
      }
      throw error;
    }
    const sourceInput = readAgentFlowSessionInput(store, runId, recoveryStepId, artifactPath);
    sourceInputChecksums.set(artifactPath, sourceInput.checksum);
    totalSourceInputBytes += artifactPath === inputManifestPath
      ? sourceInputManifestBytes
      : sourceInput.content.byteLength;
    if (totalSourceInputBytes > MAX_AGENT_FLOW_SESSION_TOTAL_INPUT_BYTES) {
      throw new AgentFlowSessionRequestError(
        `Recovery session ${recoveryStepId} inputs exceed the ${MAX_AGENT_FLOW_SESSION_TOTAL_INPUT_BYTES}-byte aggregate limit.`,
        "AGENT_FLOW_SESSION_INPUT_LIMIT"
      );
    }
    try {
      const securedInput = secureAgentFlowReferencedByteInput(
        workflow,
        `Recovery session ${recoveryStepId} input ${JSON.stringify(artifactPath)}`,
        sourceInput.content,
        artifactPath,
        sourceInput.contentType,
        sensitiveInputPaths.has(artifactPath)
      );
      if (securedInput.value.byteLength > MAX_AGENT_FLOW_SESSION_INPUT_BYTES) {
        throw new AgentFlowSessionRequestError(
          `Recovery session ${recoveryStepId} input ${artifactPath} exceeds the ${MAX_AGENT_FLOW_SESSION_INPUT_BYTES}-byte input limit after sensitive-data handling.`,
          "AGENT_FLOW_SESSION_INPUT_LIMIT"
        );
      }
      totalProviderInputBytes += securedInput.value.byteLength;
      if (totalProviderInputBytes > MAX_AGENT_FLOW_SESSION_TOTAL_INPUT_BYTES) {
        throw new AgentFlowSessionRequestError(
          `Recovery session ${recoveryStepId} provider inputs exceed the ${MAX_AGENT_FLOW_SESSION_TOTAL_INPUT_BYTES}-byte aggregate limit after sensitive-data handling.`,
          "AGENT_FLOW_SESSION_INPUT_LIMIT"
        );
      }
      inputs.push({
        ...sourceInput,
        content: securedInput.value,
        checksum: `sha256:${createHash("sha256").update(securedInput.value).digest("hex")}`
      });
      if (securedInput.redacted) redactedInputPaths.add(artifactPath);
    } catch (error) {
      if (error instanceof AgentFlowSensitiveInputError) {
        throw new AgentFlowSessionRequestError(error.message, error.code, { cause: error });
      }
      throw error;
    }
  }
  if (inputs.some((input) => input.path === RECOVERY_CONTEXT_INPUT_PATH)) {
    throw new AgentFlowSessionRequestError(
      `Recovery input path ${RECOVERY_CONTEXT_INPUT_PATH} is reserved for injected context.`,
      "AGENT_FLOW_RECOVERY_CONTEXT_INVALID"
    );
  }
  const resume = session.resume === true;
  const previous = store.getSession(runId, sessionId);
  const priorExternalSessionId = resume ? previous?.externalSessionId ?? undefined : undefined;
  assertRecoveryAdapterStringsSafe(workflow, [
    ["Recovery adapter run ID", runId],
    ["Recovery adapter step ID", recoveryStepId],
    ["Recovery adapter session ID", sessionId],
    ["Recovery adapter provider", provider],
    ...(providerDescriptor.profile === undefined
      ? []
      : [["Recovery adapter provider profile", providerDescriptor.profile] as [string, string]]),
    ...(providerDescriptor.target === undefined
      ? []
      : [["Recovery adapter provider target", providerDescriptor.target] as [string, string]]),
    ...(providerDescriptor.driver === undefined
      ? []
      : [["Recovery adapter provider driver", providerDescriptor.driver] as [string, string]]),
    ...(providerDescriptor.model === undefined
      ? []
      : [["Recovery adapter provider model", providerDescriptor.model] as [string, string]]),
    ...(providerDescriptor.fingerprint === undefined
      ? []
      : [["Recovery adapter provider fingerprint", providerDescriptor.fingerprint] as [string, string]]),
    ["Recovery adapter prompt path", prompt.path, true],
    ...inputs.flatMap((input): Array<[string, string, boolean?]> => [
      ["Recovery adapter input path", input.path, true],
      ["Recovery adapter input content type", input.contentType]
    ]),
    ...(priorExternalSessionId === undefined
      ? []
      : [["Recovery adapter external session ID", priorExternalSessionId] as [string, string]])
  ]);
  const routeFileScope = mapping(route.file_scope);
  const authorityScopes = [
    mapping(mapping(workflow.policies)?.file_scope),
    mapping(session.file_scope),
    ...recoveryOperationFileScopes(workflow.steps, failedStep),
    routeFileScope
  ].filter((scope): scope is AgentFlowYamlMapping => scope !== undefined);
  preflightAgentFlowSessionProvider(adapter, {
    runId,
    stepId: recoveryStepId,
    sessionId,
    provider,
    providerKind: providerDescriptor.kind,
    kind: "recovery",
    ...(providerDescriptor.profile === undefined ? {} : { providerProfile: providerDescriptor.profile }),
    ...(providerDescriptor.target === undefined ? {} : { providerTarget: providerDescriptor.target }),
    ...(providerDescriptor.driver === undefined ? {} : { providerDriver: providerDescriptor.driver }),
    ...(providerDescriptor.model === undefined
      ? {}
      : { providerModel: hashAgentFlowProviderModel(providerDescriptor.model) }),
    ...(providerDescriptor.fingerprint === undefined ? {} : { providerFingerprint: providerDescriptor.fingerprint }),
    resume,
    ...(priorExternalSessionId === undefined ? {} : { externalSessionId: priorExternalSessionId }),
    prompt: { ...prompt },
    inputs: inputs.map((input) => ({ ...input, content: Uint8Array.from(input.content) })),
    outputs: [],
    repoRoot: store.repoRoot,
    canModifyFiles: mapping(session.authority)?.can_modify_files === true,
    fileScope: {
      layers: authorityScopes.map((scope) => ({
        include: Array.isArray(scope.include)
          ? scope.include.filter((value): value is string => typeof value === "string")
          : [],
        exclude: Array.isArray(scope.exclude)
          ? scope.exclude.filter((value): value is string => typeof value === "string")
          : []
      }))
    },
    signal: new AbortController().signal
  });
  store.claimSession({
    id: sessionId,
    runId,
    stepId: recoveryStepId,
    provider,
    status: "running",
    externalSessionId: priorExternalSessionId ?? null,
    state: { resume, recoveryOfStepId: stepId, failureId }
  });
  let response: Awaited<ReturnType<typeof adapter>>;
  let metadata: AgentFlowYamlMapping | undefined;
  let status: AgentFlowRecoveryStatus;
  let externalSessionId = priorExternalSessionId;
  let appliedContextRevision = 0;
  let contextInput: AgentFlowSessionRequestArtifact | undefined;
  let contextInputSourceChecksum: string | undefined;
  let contextInputWasRedacted = false;
  try {
    while (true) {
      const activeSession = store.getSession(runId, sessionId)!;
      const activeRevision = recoveryContextRevision(activeSession.state);
      let rerunRevision: number | undefined;
      if (activeRevision > appliedContextRevision) {
        appliedContextRevision = activeRevision;
        rerunRevision = appliedContextRevision;
        const sourceContextInput = recoveryContextInputArtifact(activeSession.state);
        if (inputs.length + 1 > MAX_AGENT_FLOW_SESSION_INPUTS
            || totalSourceInputBytes + sourceContextInput.content.byteLength > MAX_AGENT_FLOW_SESSION_TOTAL_INPUT_BYTES) {
          throw new AgentFlowSessionRequestError(
            `Recovery session ${recoveryStepId} inputs exceed their configured limits after context injection.`,
            "AGENT_FLOW_SESSION_INPUT_LIMIT"
          );
        }
        try {
          const securedContext = secureAgentFlowByteInput(
            workflow,
            `Recovery session ${recoveryStepId} injected context`,
            sourceContextInput.content,
            sourceContextInput.path,
            sourceContextInput.contentType
          );
          if (securedContext.value.byteLength > MAX_AGENT_FLOW_SESSION_INPUT_BYTES
              || totalProviderInputBytes + securedContext.value.byteLength > MAX_AGENT_FLOW_SESSION_TOTAL_INPUT_BYTES) {
            throw new AgentFlowSessionRequestError(
              `Recovery session ${recoveryStepId} provider inputs exceed their configured limits after sensitive-data handling.`,
              "AGENT_FLOW_SESSION_INPUT_LIMIT"
            );
          }
          contextInput = {
            ...sourceContextInput,
            content: securedContext.value,
            checksum: `sha256:${createHash("sha256").update(securedContext.value).digest("hex")}`
          };
          contextInputSourceChecksum = sourceContextInput.checksum;
          contextInputWasRedacted = securedContext.redacted;
        } catch (error) {
          if (error instanceof AgentFlowSensitiveInputError) {
            throw new AgentFlowSessionRequestError(error.message, error.code, { cause: error });
          }
          throw error;
        }
      }
      preflightAgentFlowSessionProviderEvidence(
        store,
        runId,
        recoveryStepId,
        failureId,
        appliedContextRevision
      );
      reserveAgentFlowSessionModelCallBudgets(
        store, runId, workflow, recoveryStepId, sessionId, provider, providerDescriptor.kind
      );
      if (rerunRevision !== undefined) {
        store.appendRunEvent(runId, {
          type: "recovery.context.rerun",
          stepId: recoveryStepId,
          payload: { sessionId, revision: rerunRevision }
        });
      }
      assertRecoveryAdapterStringsSafe(workflow, [
        ...(externalSessionId === undefined
          ? []
          : [["Recovery adapter external session ID", externalSessionId] as [string, string]]),
        ...(contextInput === undefined
          ? []
          : [
              ["Recovery adapter context path", contextInput.path, true] as [string, string, boolean],
              ["Recovery adapter context content type", contextInput.contentType] as [string, string]
            ])
      ]);
      const providerInputs = contextInput === undefined ? inputs : [...inputs, contextInput];
      const evidencePrompt = {
        path: prompt.path,
        checksum: sourcePromptChecksum,
        ...(promptWasRedacted ? { providerChecksum: prompt.checksum, redacted: true as const } : {})
      };
      const evidenceInputs = providerInputs.map((input) => ({
        path: input.path,
        checksum: input.path === RECOVERY_CONTEXT_INPUT_PATH
          ? contextInputSourceChecksum!
          : sourceInputChecksums.get(input.path)!,
        contentType: input.contentType,
        ...(input.path === RECOVERY_CONTEXT_INPUT_PATH
          ? contextInputWasRedacted
            ? { providerChecksum: input.checksum, redacted: true as const }
            : {}
          : redactedInputPaths.has(input.path)
            ? { providerChecksum: input.checksum, redacted: true as const }
            : {})
      }));
      try {
        response = await invokeAgentFlowSessionProvider(adapter, {
          runId,
          stepId: recoveryStepId,
          sessionId,
          provider,
          providerKind: providerDescriptor.kind,
          kind: "recovery",
          ...(providerDescriptor.profile === undefined
            ? {}
            : { providerProfile: providerDescriptor.profile }),
          ...(providerDescriptor.target === undefined ? {} : { providerTarget: providerDescriptor.target }),
          ...(providerDescriptor.driver === undefined ? {} : { providerDriver: providerDescriptor.driver }),
          ...(providerDescriptor.model === undefined
            ? {}
            : { providerModel: hashAgentFlowProviderModel(providerDescriptor.model) }),
          ...(providerDescriptor.fingerprint === undefined ? {} : { providerFingerprint: providerDescriptor.fingerprint }),
          resume,
          ...(externalSessionId === undefined ? {} : { externalSessionId }),
          prompt: { ...prompt },
          inputs: providerInputs.map((input) => ({
            ...input,
            content: Uint8Array.from(input.content)
          })),
          outputs: [],
          repoRoot: store.repoRoot,
          canModifyFiles: mapping(session.authority)?.can_modify_files === true,
          fileScope: {
            layers: authorityScopes.map((scope) => ({
              include: Array.isArray(scope.include)
                ? scope.include.filter((value): value is string => typeof value === "string")
                : [],
              exclude: Array.isArray(scope.exclude)
                ? scope.exclude.filter((value): value is string => typeof value === "string")
                : []
            }))
          },
          signal: new AbortController().signal
        }, () => activeStopStatus(store, runId), () => store.runLockInterruption());
      } catch (error) {
        if (activeStopStatus(store, runId) === undefined
            && recoveryContextRevision(store.getSession(runId, sessionId)?.state ?? {}) !== appliedContextRevision) {
          continue;
        }
        throw error;
      }
      const stoppedAfterResponse = activeStopStatus(store, runId);
      const returnedSessionId = response !== null && typeof response === "object"
        && typeof response.externalSessionId === "string" && response.externalSessionId.trim().length > 0
        ? response.externalSessionId.trim()
        : undefined;
      if (returnedSessionId !== undefined) {
        assertRecoveryAdapterStringsSafe(workflow, [[
          "Recovery provider external session ID",
          returnedSessionId
        ]]);
      }
      if (stoppedAfterResponse !== undefined) {
        externalSessionId = returnedSessionId ?? externalSessionId;
        throw new AgentFlowSessionRequestInterruptedError(stoppedAfterResponse);
      }
      if (recoveryContextRevision(store.getSession(runId, sessionId)?.state ?? {}) !== appliedContextRevision) {
        if (resume) externalSessionId = returnedSessionId ?? externalSessionId;
        continue;
      }
      const returnedExternalSessionId = response.externalSessionId;
      if (returnedExternalSessionId !== undefined &&
          (typeof returnedExternalSessionId !== "string" || returnedExternalSessionId.trim().length === 0)) {
        throw new AgentFlowSessionRequestError(
          `Session provider external session ID for step ${recoveryStepId} must be a non-empty string.`,
          "AGENT_FLOW_SESSION_RESPONSE_INVALID"
        );
      }
      externalSessionId = returnedExternalSessionId?.trim() ?? externalSessionId;
      validateAgentFlowSessionProviderResponse(recoveryStepId, [], response);
      metadata = validateAgentFlowSessionProviderMetadata(recoveryStepId, response.metadata);
      const declaredStatus = normalizedTarget(metadata?.recovery_status);
      if (declaredStatus !== "remediated" && declaredStatus !== "unresolved") {
        throw new AgentFlowRunStateError(
          "Recovery session metadata.recovery_status must be remediated or unresolved.",
          "AGENT_FLOW_RECOVERY_STATUS"
        );
      }
      status = declaredStatus;
      const settled = store.settleRecoverySessionForRunAtContextRevision({
        id: sessionId,
        runId,
        stepId: recoveryStepId,
        provider,
        externalSessionId: externalSessionId ?? null,
        waitingState: {
          resume,
          recoveryOfStepId: stepId,
          failureId,
          providerResponded: true,
          recoveryStatus: status
        },
        interruptedState: { resume, recoveryOfStepId: stepId, failureId }
      }, appliedContextRevision, () => {
        const requestArtifact = persistAgentFlowSessionProviderEvidence({
          store,
          runId,
          stepId: recoveryStepId,
          sessionId,
          provider,
          providerKind: providerDescriptor.kind,
          ...(providerDescriptor.profile === undefined
            ? {}
            : { providerProfile: providerDescriptor.profile }),
          ...(providerDescriptor.target === undefined ? {} : { providerTarget: providerDescriptor.target }),
          ...(providerDescriptor.driver === undefined ? {} : { providerDriver: providerDescriptor.driver }),
          ...(providerDescriptor.model === undefined ? {} : { providerModel: providerDescriptor.model }),
          ...(providerDescriptor.fingerprint === undefined ? {} : { providerFingerprint: providerDescriptor.fingerprint }),
          resume,
          prompt: evidencePrompt,
          inputs: evidenceInputs,
          outputs: [],
          ...(externalSessionId === undefined ? {} : { externalSessionId }),
          ...(metadata === undefined
            ? {}
            : { providerMetadata: metadata as Record<string, AgentFlowRunStateValue> }),
          recoveryFailureId: failureId,
          recoveryContextRevision: appliedContextRevision
        });
        return { requestArtifact: requestArtifact.declaredPath };
      });
      if (!settled.settled) continue;
      if (settled.stopped !== undefined) {
        throw new AgentFlowSessionRequestInterruptedError(settled.stopped);
      }
      break;
    }
  } catch (error) {
    const lockError = store.runLockInterruption();
    if (lockError !== undefined) throw lockError;
    const stopped = error instanceof AgentFlowSessionRequestInterruptedError
      ? error.status
      : activeStopStatus(store, runId);
    const failedByPolicy = error instanceof AgentFlowSessionPolicyError && error.status === "fail";
    store.upsertSession({
      id: sessionId,
      runId,
      stepId: recoveryStepId,
      provider,
      status: stopped ?? (failedByPolicy ? "failed" : "paused"),
      externalSessionId: externalSessionId ?? null,
      state: {
        ...(store.getSession(runId, sessionId)?.state ?? {}),
        resume,
        recoveryOfStepId: stepId,
        failureId,
        ...(stopped === undefined
          ? { error: redactAgentFlowSensitiveText(error instanceof Error ? error.message : String(error)) }
          : { interrupted: stopped })
      }
    });
    if (error instanceof AgentFlowSessionRequestInterruptedError) throw error;
    const message = redactAgentFlowSensitiveText(error instanceof Error ? error.message : String(error));
    if (error instanceof AgentFlowSessionPolicyError) {
      throw new AgentFlowSessionPolicyError(message, error.code, error.status);
    }
    if (error instanceof AgentFlowSessionRequestError) {
      throw new AgentFlowSessionRequestError(message, error.code, { cause: new Error(message) });
    }
    throw new AgentFlowSessionRequestError(
      `Session provider ${provider} failed for step ${recoveryStepId}: ${message}`,
      "AGENT_FLOW_SESSION_PROVIDER_FAILED",
      { cause: new Error(message) }
    );
  }
  return {
    status,
    ...(typeof metadata?.message === "string" ? { message: metadata.message } : {})
  };
}

function recoveredRecoverySessionResult(
  store: AgentFlowRunStateStore,
  runId: string,
  stepId: string,
  failureId: string,
  sessionId: string
): { status: AgentFlowRecoveryStatus; message?: string } | undefined {
  const recoveryStepId = `${stepId}:recovery`;
  const session = store.getSession(runId, sessionId);
  const state = session?.state;
  const status = normalizedTarget(state?.recoveryStatus);
  const requestPath = normalizedTarget(state?.requestArtifact);
  const appliedRevision = state?.appliedContextRevision;
  const currentRevision = recoveryContextRevision(state ?? {});
  if (session?.status !== "waiting"
      || session.stepId !== recoveryStepId
      || state?.recoveryOfStepId !== stepId
      || state.failureId !== failureId
      || state.providerResponded !== true
      || state.dirty === true) {
    return undefined;
  }
  const invalidEvidence = (cause?: unknown): AgentFlowRunStateError => new AgentFlowRunStateError(
    `Recovery session ${sessionId} cannot reuse its settled response because its persisted evidence is invalid.`,
    "AGENT_FLOW_RUN_LOCK_RECOVERY",
    cause === undefined ? undefined : { cause }
  );
  if (status === undefined || !["remediated", "unresolved"].includes(status)
      || requestPath === undefined
      || !Number.isSafeInteger(appliedRevision) || appliedRevision !== currentRevision) {
    throw invalidEvidence();
  }
  try {
    const evidence = store.readArtifact(runId, requestPath);
    const payload = mapping(JSON.parse(evidence.content.toString("utf8")));
    const metadata = mapping(payload?.providerMetadata);
    if (evidence.artifact.kind !== "session_request"
        || evidence.artifact.producerStepId !== recoveryStepId
        || payload?.stepId !== recoveryStepId
        || payload.sessionId !== sessionId
        || payload.recoveryFailureId !== failureId
        || payload.recoveryContextRevision !== appliedRevision
        || normalizedTarget(metadata?.recovery_status) !== status) {
      throw invalidEvidence();
    }
    return {
      status: status as AgentFlowRecoveryStatus,
      ...(typeof metadata?.message === "string" ? { message: metadata.message } : {})
    };
  } catch (error) {
    if (error instanceof AgentFlowRunStateError && error.code === "AGENT_FLOW_RUN_LOCK_RECOVERY") throw error;
    throw invalidEvidence(error);
  }
}

function recoveryContextRevision(state: Record<string, AgentFlowRunStateValue>): number {
  return typeof state.contextRevision === "number"
    && Number.isSafeInteger(state.contextRevision)
    && state.contextRevision >= 0
    ? state.contextRevision
    : 0;
}

function recoveryContextInputArtifact(
  state: Record<string, AgentFlowRunStateValue>
): AgentFlowSessionRequestArtifact {
  const injections = Array.isArray(state.contextInjections)
    ? state.contextInjections.flatMap((entry) => {
      const record = mapping(entry);
      return typeof record?.context === "string" && record.context.trim().length > 0
        ? [record.context]
        : [];
    })
    : [];
  if (injections.length === 0) {
    throw new AgentFlowRunStateError(
      "Recovery session context revision has no persisted injected context.",
      "AGENT_FLOW_RECOVERY_CONTEXT_INVALID"
    );
  }
  const content = Buffer.from(
    injections.map((entry, index) => `## Injected context ${index + 1}\n\n${entry}\n`).join("\n"),
    "utf8"
  );
  return {
    path: RECOVERY_CONTEXT_INPUT_PATH,
    content,
    contentType: "text/markdown; charset=utf-8",
    checksum: `sha256:${createHash("sha256").update(content).digest("hex")}`
  };
}

function resolveRecoveryInputs(
  value: unknown,
  runInputs: Record<string, AgentFlowRunStateValue>,
  stepId: string,
  failurePath: string | null
): Record<string, AgentFlowRunStateValue> {
  const input = mapping(value) ?? {};
  return Object.fromEntries(Object.entries(input).map(([name, entry]) => [
    name,
    resolveRecoveryInputValue(entry, runInputs, stepId, failurePath)
  ]));
}

function secureRecoveryReferencedInputValues(
  workflow: AgentFlowWorkflow,
  label: string,
  declared: unknown,
  resolved: Record<string, AgentFlowRunStateValue>
): Record<string, AgentFlowRunStateValue> {
  const declaredMapping = mapping(declared) ?? {};
  return Object.fromEntries(Object.entries(resolved).map(([name, value]) => [
    name,
    secureRecoveryReferencedInputValue(workflow, `${label}.${name}`, declaredMapping[name], value)
  ]));
}

function secureRecoveryReferencedInputValue(
  workflow: AgentFlowWorkflow,
  label: string,
  declared: unknown,
  resolved: AgentFlowRunStateValue
): AgentFlowRunStateValue {
  if (typeof declared === "string") {
    const reference = /^\{\{\s*inputs\.([A-Za-z_][A-Za-z0-9_-]*)\s*}}$/.exec(declared);
    if (reference !== null && agentFlowInputKeyLooksSensitive(reference[1]!)) {
      return secureAgentFlowSensitiveJsonInputValue(workflow, label, resolved).value;
    }
  }
  if (Array.isArray(declared) && Array.isArray(resolved)) {
    return resolved.map((value, index) =>
      secureRecoveryReferencedInputValue(workflow, `${label}[${index}]`, declared[index], value)
    );
  }
  const declaredMapping = mapping(declared);
  const resolvedMapping = mapping(resolved);
  if (declaredMapping !== undefined && resolvedMapping !== undefined) {
    return Object.fromEntries(Object.entries(resolvedMapping).map(([name, value]) => [
      name,
      secureRecoveryReferencedInputValue(
        workflow,
        `${label}.${name}`,
        declaredMapping[name],
        value as AgentFlowRunStateValue
      )
    ]));
  }
  return resolved;
}

function resolveRecoveryInputValue(
  value: unknown,
  runInputs: Record<string, AgentFlowRunStateValue>,
  stepId: string,
  failurePath: string | null
): AgentFlowRunStateValue {
  if (typeof value === "string") {
    const expression = /^\{\{\s*([^}]+?)\s*}}$/.exec(value);
    if (expression === null) return value;
    if (expression[1] === "failure.path") {
      if (failurePath === null) {
        throw new AgentFlowRunStateError(
          "Recovery route references failure.path, but the failure payload is unavailable.",
          "AGENT_FLOW_RECOVERY_FAILURE_PAYLOAD"
        );
      }
      return failurePath;
    }
    if (expression[1] === "step.id") return stepId;
    const inputReference = /^inputs\.([A-Za-z_][A-Za-z0-9_-]*)$/.exec(expression[1]!);
    if (inputReference !== null && Object.hasOwn(runInputs, inputReference[1]!)) {
      return runInputs[inputReference[1]!]!;
    }
    throw new AgentFlowRunStateError(
      `Unsupported recovery route input expression ${value}.`,
      "AGENT_FLOW_RECOVERY_INPUT_EXPRESSION"
    );
  }
  if (Array.isArray(value)) {
    return value.map((entry) => resolveRecoveryInputValue(entry, runInputs, stepId, failurePath));
  }
  const record = mapping(value);
  if (record !== undefined) {
    return Object.fromEntries(Object.entries(record).map(([name, entry]) => [
      name,
      resolveRecoveryInputValue(entry, runInputs, stepId, failurePath)
    ]));
  }
  return value === null || typeof value === "boolean" || typeof value === "number" ? value : null;
}

function persistRecoverySessionInputs(
  store: AgentFlowRunStateStore,
  runId: string,
  stepId: string,
  failureId: string,
  inputs: Record<string, AgentFlowRunStateValue>
): string {
  const artifactPath = `recoveries/${safeId(failureId)}/inputs.json`;
  const content = assertRecoverySessionInputManifestSize(inputs, stepId, "after sensitive-data handling");
  store.writeArtifact({
    id: `recovery:${createHash("sha256").update(failureId).digest("hex")}:inputs`,
    runId,
    stepId,
    path: artifactPath,
    kind: "recovery_input",
    contentType: "application/json",
    content,
    metadata: { failureId }
  });
  return artifactPath;
}

function assertRecoverySessionInputManifestSize(
  inputs: Record<string, AgentFlowRunStateValue>,
  stepId: string,
  phase: string
): string {
  const content = `${JSON.stringify(inputs, null, 2)}\n`;
  if (Buffer.byteLength(content) > MAX_AGENT_FLOW_SESSION_TOTAL_INPUT_BYTES) {
    throw new AgentFlowSessionRequestError(
      `Recovery session ${stepId} inputs exceed the ${MAX_AGENT_FLOW_SESSION_TOTAL_INPUT_BYTES}-byte aggregate limit ${phase}.`,
      "AGENT_FLOW_SESSION_INPUT_LIMIT"
    );
  }
  return content;
}

function assertRecoveryAdapterStringsSafe(
  workflow: AgentFlowWorkflow,
  values: Array<[label: string, value: string, path?: boolean]>
): void {
  try {
    for (const [label, value, path] of values) {
      assertAgentFlowAdapterStringSafe(workflow, label, value, { path: path === true });
    }
  } catch (error) {
    if (error instanceof AgentFlowSensitiveInputError) {
      throw new AgentFlowSessionRequestError(error.message, error.code, { cause: error });
    }
    throw error;
  }
}

interface PreparedNestedRecoveryInputs {
  inputs: Record<string, AgentFlowRunStateValue>;
  copies: Array<{ sourcePath: string; targetPath: string; sensitive: boolean }>;
  pathMap: Map<string, string>;
  securityWorkflow: AgentFlowWorkflow;
}

function prepareNestedRecoveryInputs(
  store: AgentFlowRunStateStore,
  parentRunId: string,
  failureId: string,
  declaredInputs: unknown,
  inputs: Record<string, AgentFlowRunStateValue>,
  failurePath: string | null,
  securityWorkflow: AgentFlowWorkflow,
  workflow: AgentFlowWorkflow
): PreparedNestedRecoveryInputs {
  const paths = new Set<string>();
  collectRecoveryArtifactPaths(store, parentRunId, inputs, paths);
  const sensitivePaths = new Set<string>();
  collectRecoveryReferencedSensitiveArtifactPaths(
    store,
    parentRunId,
    declaredInputs,
    inputs,
    sensitivePaths
  );
  const securedInputs = secureNestedRecoveryInputValues(
    store,
    parentRunId,
    securityWorkflow,
    "Nested recovery inputs",
    declaredInputs,
    inputs
  );
  const failurePaths = new Set<string>();
  if (failurePath !== null && recoveryValueReferencesPath(inputs, failurePath)) {
    collectRecoveryFailureArtifactPaths(store, parentRunId, failurePath, failurePaths);
    failurePaths.forEach((artifactPath) => paths.add(artifactPath));
  }
  const reservedOutputs = new Set(nestedWorkflowOutputPaths(workflow.steps).map(normalizeAgentFlowArtifactPath));
  const commandLogPrefixes = nestedWorkflowCommandLogPrefixes(workflow.steps);
  const assignedTargets = new Set<string>();
  const pathMap = new Map<string, string>();
  for (const sourcePath of [...paths].sort()) {
    let targetPath = sourcePath;
    if (failurePaths.has(sourcePath)
        || nestedWorkflowRuntimeArtifactCollision(workflow, sourcePath, commandLogPrefixes)) {
      const digest = createHash("sha256").update(`${failureId}\0${sourcePath}`).digest("hex");
      const basename = sourcePath.split("/").at(-1) ?? "input";
      let suffix = 0;
      do {
        targetPath = `recovery-inputs/${digest}${suffix === 0 ? "" : `-${suffix}`}/${basename}`;
        suffix += 1;
      } while (reservedOutputs.has(targetPath) || assignedTargets.has(targetPath) || paths.has(targetPath));
    }
    pathMap.set(sourcePath, targetPath);
    assignedTargets.add(targetPath);
  }
  return {
    inputs: remapRecoveryArtifactPaths(securedInputs, pathMap),
    copies: [...pathMap].map(([sourcePath, targetPath]) => ({
      sourcePath,
      targetPath,
      sensitive: sensitivePaths.has(sourcePath)
    })),
    pathMap,
    securityWorkflow
  };
}

function secureNestedRecoveryInputValues(
  store: AgentFlowRunStateStore,
  runId: string,
  workflow: AgentFlowWorkflow,
  label: string,
  declared: unknown,
  resolved: Record<string, AgentFlowRunStateValue>
): Record<string, AgentFlowRunStateValue> {
  const declaredMapping = mapping(declared) ?? {};
  return Object.fromEntries(Object.entries(resolved).map(([name, value]) => [
    name,
    secureNestedRecoveryInputValue(
      store,
      runId,
      workflow,
      `${label}.${name}`,
      declaredMapping[name],
      value,
      agentFlowInputKeyLooksSensitive(name)
    )
  ]));
}

function secureNestedRecoveryInputValue(
  store: AgentFlowRunStateStore,
  runId: string,
  workflow: AgentFlowWorkflow,
  label: string,
  declared: unknown,
  resolved: AgentFlowRunStateValue,
  sensitive: boolean
): AgentFlowRunStateValue {
  const reference = typeof declared === "string"
    ? /^\{\{\s*inputs\.([A-Za-z_][A-Za-z0-9_-]*)\s*}}$/.exec(declared)
    : null;
  const inheritedSensitive = sensitive
    || (reference !== null && agentFlowInputKeyLooksSensitive(reference[1]!));
  if (typeof resolved === "string") {
    try {
      if (store.getArtifact(runId, resolved) !== null) return resolved;
    } catch {
      // Literal nested-recovery inputs do not have to be artifact paths.
    }
    return secureNestedRecoveryScalarValue(workflow, label, resolved, inheritedSensitive);
  }
  if (Array.isArray(resolved)) {
    const declaredValues = Array.isArray(declared) ? declared : [];
    return resolved.map((value, index) => secureNestedRecoveryInputValue(
      store,
      runId,
      workflow,
      `${label}[${index}]`,
      declaredValues[index],
      value,
      inheritedSensitive
    ));
  }
  if (resolved !== null && typeof resolved === "object") {
    const declaredMapping = mapping(declared) ?? {};
    return Object.fromEntries(Object.entries(resolved).map(([name, value]) => [
      name,
      secureNestedRecoveryInputValue(
        store,
        runId,
        workflow,
        `${label}.${name}`,
        declaredMapping[name],
        value,
        inheritedSensitive || agentFlowInputKeyLooksSensitive(name)
      )
    ]));
  }
  return secureNestedRecoveryScalarValue(workflow, label, resolved, inheritedSensitive);
}

function secureNestedRecoveryScalarValue(
  workflow: AgentFlowWorkflow,
  label: string,
  value: AgentFlowRunStateValue,
  sensitive: boolean
): AgentFlowRunStateValue {
  if (sensitive) return secureAgentFlowSensitiveJsonInputValue(workflow, label, value).value;
  return secureAgentFlowJsonInput(workflow, label, { value }).value.value!;
}

function nestedWorkflowCommandLogPrefixes(steps: AgentFlowWorkflowStep[]): string[] {
  const prefixes: string[] = [];
  const visit = (step: AgentFlowWorkflowStep): void => {
    if (normalizedTarget(step.type) === "command" && typeof step.id === "string") {
      prefixes.push(`logs/${safeId(step.id.trim())}/`);
    }
    for (const field of ["body", "steps", "branches"]) {
      const nested = step[field];
      if (Array.isArray(nested)) nested.filter(isWorkflowStep).forEach(visit);
    }
  };
  steps.forEach(visit);
  return prefixes;
}

function nestedWorkflowRuntimeArtifactCollision(
  workflow: AgentFlowWorkflow,
  sourcePath: string,
  commandLogPrefixes: string[]
): boolean {
  if (["failures/", "recoveries/", "session-requests/", "mcp-calls/"]
    .some((prefix) => sourcePath.startsWith(prefix))) return true;
  if (workflow.style === "pipeline" && sourcePath === AGENT_FLOW_FINAL_SUMMARY_PATH) return true;
  return commandLogPrefixes.some((prefix) => sourcePath.startsWith(prefix));
}

function copyRecoveryInputArtifacts(
  store: AgentFlowRunStateStore,
  parentRunId: string,
  recoveryRunId: string,
  prepared: PreparedNestedRecoveryInputs
): void {
  for (const { sourcePath, targetPath, sensitive } of prepared.copies) {
    const source = store.readArtifact(parentRunId, sourcePath);
    const secured = secureAgentFlowReferencedByteInput(
      prepared.securityWorkflow,
      `Nested recovery input ${sourcePath}`,
      source.content,
      sourcePath,
      source.artifact.contentType,
      sensitive
    );
    const content = remapRecoveryArtifactContent(
      Buffer.from(secured.value),
      source.artifact.contentType,
      prepared.pathMap
    );
    store.writeArtifact({
      id: `recovery-input:${createHash("sha256").update(targetPath).digest("hex")}`,
      runId: recoveryRunId,
      path: targetPath,
      kind: "recovery_input",
      contentType: source.artifact.contentType,
      content,
      metadata: { parentRunId, sourceArtifactId: source.artifact.id, sourcePath }
    });
  }
}

function remapRecoveryArtifactContent(
  content: Buffer,
  contentType: string,
  pathMap: Map<string, string>
): Buffer {
  const mediaType = contentType.split(";", 1)[0]!.trim().toLowerCase();
  if (mediaType !== "application/json" && !mediaType.endsWith("+json")) return content;
  const source = content.toString("utf8");
  try {
    JSON.parse(source);
  } catch {
    return content;
  }
  let changed = false;
  let rewritten = "";
  let cursor = 0;
  for (let index = 0; index < source.length; index += 1) {
    if (source[index] !== "\"") continue;
    let closing = index + 1;
    for (; closing < source.length; closing += 1) {
      if (source[closing] === "\\") {
        closing += 1;
        continue;
      }
      if (source[closing] === "\"") break;
    }
    const token = source.slice(index, closing + 1);
    let next = closing + 1;
    while (/\s/.test(source[next] ?? "")) next += 1;
    const value = JSON.parse(token) as string;
    const mapped = source[next] === ":" ? undefined : remappedRecoveryArtifactPath(value, pathMap);
    if (mapped !== undefined && mapped !== value) {
      rewritten += source.slice(cursor, index) + JSON.stringify(mapped);
      cursor = closing + 1;
      changed = true;
    }
    index = closing;
  }
  return changed ? Buffer.from(rewritten + source.slice(cursor), "utf8") : content;
}

function remapRecoveryArtifactPaths<T extends AgentFlowRunStateValue>(
  value: T,
  pathMap: Map<string, string>
): T {
  if (typeof value === "string") {
    return (remappedRecoveryArtifactPath(value, pathMap) ?? value) as T;
  }
  if (Array.isArray(value)) {
    return value.map((entry) => remapRecoveryArtifactPaths(entry, pathMap)) as T;
  }
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(Object.entries(value).map(([key, entry]) => [
      key,
      remapRecoveryArtifactPaths(entry, pathMap)
    ])) as T;
  }
  return value;
}

function remappedRecoveryArtifactPath(value: string, pathMap: Map<string, string>): string | undefined {
  const direct = pathMap.get(value);
  if (direct !== undefined) return direct;
  try {
    return pathMap.get(normalizeAgentFlowArtifactPath(value));
  } catch {
    // Literal recovery input strings do not have to be artifact paths.
    return undefined;
  }
}

function collectRecoveryFailureArtifactPaths(
  store: AgentFlowRunStateStore,
  runId: string,
  failurePath: string | null,
  paths: Set<string>,
  maxAdditional = Number.POSITIVE_INFINITY
): void {
  if (failurePath === null || maxAdditional <= 0) return;
  const failure = readAgentFlowSessionInput(store, runId, "recovery-context", failurePath);
  const initialSize = paths.size;
  paths.add(failurePath);
  let payload: AgentFlowYamlMapping;
  try {
    payload = mapping(JSON.parse(Buffer.from(failure.content).toString("utf8"))) ?? {};
  } catch (error) {
    throw new AgentFlowRunStateError(
      `Recovery failure payload ${failurePath} is not valid JSON.`,
      "AGENT_FLOW_RECOVERY_FAILURE_PAYLOAD",
      { cause: error }
    );
  }
  const artifacts = mapping(payload.artifacts);
  const logs = mapping(payload.logs);
  const candidates = [
    ...(Array.isArray(artifacts?.available) ? artifacts.available : []),
    logs?.stdout,
    logs?.stderr
  ];
  for (const candidate of candidates) {
    if (paths.size - initialSize >= maxAdditional) break;
    if (typeof candidate === "string") collectRecoveryArtifactPaths(store, runId, candidate, paths);
  }
}

function recoveryValueReferencesPath(value: AgentFlowRunStateValue, target: string): boolean {
  if (typeof value === "string") return value === target;
  if (Array.isArray(value)) return value.some((entry) => recoveryValueReferencesPath(entry, target));
  return value !== null && typeof value === "object"
    && Object.values(value).some((entry) => recoveryValueReferencesPath(entry, target));
}

function collectRecoveryArtifactPaths(
  store: AgentFlowRunStateStore,
  runId: string,
  value: AgentFlowRunStateValue,
  paths: Set<string>,
  sensitivePaths?: Set<string>,
  sensitive = false
): void {
  if (typeof value === "string") {
    try {
      if (store.getArtifact(runId, value) !== null) {
        const normalized = normalizeAgentFlowArtifactPath(value);
        paths.add(normalized);
        if (sensitive) sensitivePaths?.add(normalized);
      }
    } catch {
      // Literal recovery inputs do not have to be artifact paths.
    }
    return;
  }
  if (Array.isArray(value)) {
    value.forEach((entry) => collectRecoveryArtifactPaths(store, runId, entry, paths, sensitivePaths, sensitive));
    return;
  }
  if (value !== null && typeof value === "object") {
    Object.entries(value).forEach(([key, entry]) => collectRecoveryArtifactPaths(
      store,
      runId,
      entry,
      paths,
      sensitivePaths,
      sensitive || agentFlowInputKeyLooksSensitive(key)
    ));
  }
}

function collectRecoveryReferencedSensitiveArtifactPaths(
  store: AgentFlowRunStateStore,
  runId: string,
  declared: unknown,
  resolved: AgentFlowRunStateValue,
  sensitivePaths: Set<string>,
  sensitive = false
): void {
  if (typeof declared === "string") {
    const expression = /^\{\{\s*inputs\.([A-Za-z_][A-Za-z0-9_-]*)\s*}}$/.exec(declared);
    const referencedSensitiveInput = expression !== null && agentFlowInputKeyLooksSensitive(expression[1]!);
    if (sensitive || referencedSensitiveInput) {
      collectRecoveryArtifactPaths(store, runId, resolved, new Set(), sensitivePaths, true);
    }
    return;
  }
  if (Array.isArray(declared) && Array.isArray(resolved)) {
    declared.forEach((entry, index) => collectRecoveryReferencedSensitiveArtifactPaths(
      store,
      runId,
      entry,
      resolved[index] ?? null,
      sensitivePaths,
      sensitive
    ));
    return;
  }
  const declaredRecord = mapping(declared);
  if (declaredRecord !== undefined && resolved !== null && typeof resolved === "object" && !Array.isArray(resolved)) {
    Object.entries(declaredRecord).forEach(([key, entry]) => collectRecoveryReferencedSensitiveArtifactPaths(
      store,
      runId,
      entry,
      resolved[key] ?? null,
      sensitivePaths,
      sensitive || agentFlowInputKeyLooksSensitive(key)
    ));
  }
}

function persistRecoveryDecision(
  store: AgentFlowRunStateStore,
  runId: string,
  stepId: string,
  failureId: string,
  decision: Record<string, AgentFlowRunStateValue>
): void {
  const artifactPath = `recoveries/${safeId(failureId)}/decision.json`;
  store.writeArtifact({
    id: `recovery:${createHash("sha256").update(failureId).digest("hex")}:decision`,
    runId,
    stepId,
    path: artifactPath,
    kind: "recovery_decision",
    contentType: "application/json; charset=utf-8",
    content: `${JSON.stringify({ failureId, stepId, ...decision }, null, 2)}\n`,
    overwrite: store.getArtifact(runId, artifactPath) !== null,
    requiredRunStatus: "running",
    metadata: { failureId, status: decision.status! }
  });
}

function finishResultStep(
  store: AgentFlowRunStateStore,
  runId: string,
  completedSteps: string[],
  stepId: string,
  step: AgentFlowWorkflowStep,
  attempt: number,
  terminalEffects: AgentFlowPipelineTerminalEffects,
  beforeRemediatedResult?: () => void
): AgentFlowCommandPipelineResult {
  const resultStatus = normalizedTarget(step.status);
  if (resultStatus === undefined || ![
    "cancelled", "completed", "continue", "failed", "paused", "remediated", "unresolved"
  ].includes(resultStatus)) {
    throw new AgentFlowRunStateError(
      `Result step ${stepId} has unsupported status ${String(step.status)}.`,
      "AGENT_FLOW_RESULT_STATUS"
    );
  }
  const run = store.getRun(runId)!;
  const returnTo = resolveResultReturnTarget(step.return_to, run.inputs);
  const output = {
    attempt,
    resultStatus,
    ...(returnTo === undefined ? {} : { returnTo })
  };
  store.updateRun(runId, { currentStepId: stepId, error: null });
  store.upsertStep({ runId, stepId, attempt, status: "completed", output });
  store.appendRunEvent(runId, { type: "step.completed", stepId, payload: output });
  completedSteps.push(stepId);
  return finalizeCompletedResultStep(
    store,
    runId,
    completedSteps,
    stepId,
    attempt,
    resultStatus,
    returnTo,
    terminalEffects,
    beforeRemediatedResult
  );
}

function finalizeCompletedResultStep(
  store: AgentFlowRunStateStore,
  runId: string,
  completedSteps: string[],
  stepId: string,
  attempt: number,
  resultStatus: string,
  returnTo: string | undefined,
  terminalEffects: AgentFlowPipelineTerminalEffects,
  beforeRemediatedResult?: () => void
): AgentFlowCommandPipelineResult {
  const intendedStatus = resultStatus === "cancelled"
    ? "cancelled"
    : resultStatus === "paused" ? "paused" : ["failed", "unresolved"].includes(resultStatus) ? "failed" : "completed";
  let finalized: ReturnType<typeof finalizePipelineRun>;
  try {
    finalized = finalizePipelineRun(store, runId, terminalEffects, {
      intendedStatus,
      completedSteps,
      currentStepId: null,
      output: { completedSteps, resultStatus, ...(returnTo === undefined ? {} : { returnTo }) },
      eventPayload: { completedSteps, resultStatus, ...(returnTo === undefined ? {} : { returnTo }) },
      ...(resultStatus === "remediated" && beforeRemediatedResult !== undefined
        ? {
            beforeTerminalEffects: beforeRemediatedResult,
            onFinalStatus: (
              status: AgentFlowCommandPipelineResult["status"],
              message: string | undefined,
              notificationFailure: AgentFlowNotificationDeliveryResult["requiredFailure"],
              notificationAttempts: AgentFlowNotificationDeliveryResult["attempts"]
            ) => {
              if (status !== "completed") {
                throw new NotificationPromotionRollbackError(
                  message ?? "Nested recovery finalization did not complete after output promotion.",
                  notificationFailure,
                  notificationAttempts ?? []
                );
              }
            }
          }
        : {})
    });
  } catch (error) {
    if (error instanceof NotificationPromotionRollbackError) {
      error.attempts.forEach((attempt) => store.appendRunEvent(runId, attempt));
      if (error.failure !== undefined) {
        return finishRequiredStepNotificationFailure(
          store,
          runId,
          completedSteps,
          stepId,
          attempt,
          "result",
          error.failure,
          terminalEffects
        );
      }
    }
    return finishFailure(store, runId, completedSteps, stepId, {
      exitCode: null,
      timedOut: false,
      message: `Could not promote nested recovery outputs: ${error instanceof Error ? error.message : String(error)}`
    }, "failed", terminalEffects);
  }
  return {
    status: finalized.status,
    completedSteps,
    resultStatus,
    ...(returnTo === undefined ? {} : { returnTo }),
    ...(finalized.message === undefined ? {} : { message: finalized.message })
  };
}

function resolveResultReturnTarget(
  value: unknown,
  inputs: Record<string, AgentFlowRunStateValue>
): string | undefined {
  const staticValue = normalizedTarget(value);
  if (staticValue !== undefined) return staticValue;
  if (typeof value !== "string") return undefined;
  const match = /^\{\{\s*inputs\.([A-Za-z_][A-Za-z0-9_-]*)\s*}}$/.exec(value);
  const resolved = match === null ? undefined : inputs[match[1]!];
  return typeof resolved === "string" && resolved.trim().length > 0 ? resolved.trim() : undefined;
}

function routeRecoveredCompletedStep(
  store: AgentFlowRunStateStore,
  runId: string,
  completedSteps: string[],
  stepId: string,
  step: AgentFlowWorkflowStep,
  currentSteps: AgentFlowWorkflowStep[],
  stepIndex: number,
  stepLocations: Map<string, RuntimeStepLocation>,
  budget: SuccessfulRoutingBudget,
  payload: AgentFlowRunStateValue,
  beforeRemediatedResult?: () => void
): SuccessfulRoute {
  const output = mapping(payload);
  const successfulAttempt = typeof output?.attempt === "number"
      && Number.isSafeInteger(output.attempt)
      && output.attempt > 0
    ? output.attempt
    : Math.max(1, budget.attempts.get(stepId) ?? 0);
  const recoveryFailure = resolveReturnedRecoveryFailures(
    store,
    runId,
    completedSteps,
    stepId,
    successfulAttempt,
    budget.terminalEffects
  );
  if (recoveryFailure !== undefined) return { result: recoveryFailure };
  const stepType = normalizedTarget(step.type);
  if (stepType === "result") {
    const resultStatus = normalizedTarget(output?.resultStatus);
    if (resultStatus === undefined || ![
      "cancelled", "completed", "continue", "failed", "paused", "remediated", "unresolved"
    ].includes(resultStatus)) {
      throw new AgentFlowRunStateError(
        `Result step ${stepId} cannot recover because its completed event has unsupported status ${String(output?.resultStatus)}.`,
        "AGENT_FLOW_RUN_LOCK_RECOVERY"
      );
    }
    const returnTo = normalizedTarget(output?.returnTo);
    return {
      result: finalizeCompletedResultStep(
        store,
        runId,
        completedSteps,
        stepId,
        successfulAttempt,
        resultStatus,
        returnTo,
        budget.terminalEffects,
        beforeRemediatedResult
      )
    };
  }
  if (stepType === "consult") {
    try {
      if (recoveredConsultWasBlocked(store, runId, output)) {
        return { result: finishBlockedConsult(store, runId, completedSteps, stepId, budget.terminalEffects) };
      }
    } catch (error) {
      return {
        result: finishFailure(store, runId, completedSteps, stepId, {
          exitCode: null,
          timedOut: false,
          message: error instanceof Error ? error.message : String(error)
        }, "paused", budget.terminalEffects)
      };
    }
  }

  let selectedTarget: string | undefined;
  if (stepType === "condition") {
    selectedTarget = normalizedTarget(output?.target);
  } else if (stepType === "manual_gate") {
    const outcome = normalizedTarget(output?.outcome);
    selectedTarget = outcome === undefined ? undefined : manualGateOutcomeTarget(step, outcome);
  } else if (stepType === "approval") {
    const outcome = normalizedTarget(output?.outcome);
    const approval = store.listApprovals(runId).filter((candidate) => candidate.stepId === stepId).at(-1);
    const approvalStatus = normalizedTarget(output?.approvalStatus) ?? approval?.status;
    selectedTarget = outcome === undefined
      ? approvalStatus === "approved"
        ? normalizedTarget(step.on_approve)
        : approvalStatus === "rejected"
          ? normalizedTarget(step.on_reject) ?? "cancel"
          : approvalStatus === "cancelled" ? normalizedTarget(step.on_cancel) ?? "cancel" : undefined
      : manualGateOutcomeTarget(step, outcome);
  } else if (stepType === "review") {
    const outcome = normalizedTarget(output?.outcome);
    selectedTarget = outcome === "fail" || outcome === "cancel" ? outcome : undefined;
  }
  return routeAfterSuccessfulStep(
    store,
    runId,
    completedSteps,
    stepId,
    step,
    currentSteps,
    stepIndex,
    stepLocations,
    budget,
    selectedTarget
  );
}

function recoveredConsultWasBlocked(
  store: AgentFlowRunStateStore,
  runId: string,
  output: AgentFlowYamlMapping | undefined
): boolean {
  const persistedStatus = normalizedTarget(output?.consultStatus);
  if (persistedStatus === "blocked") return true;
  if (persistedStatus === "advice") return false;
  const outputPath = Array.isArray(output?.outputs) && typeof output.outputs[0] === "string"
    ? output.outputs[0]
    : undefined;
  if (outputPath === undefined) {
    throw new AgentFlowRunStateError(
      `Completed consultation cannot recover because its blocking result was not persisted.`,
      "AGENT_FLOW_RUN_LOCK_RECOVERY"
    );
  }
  try {
    const result = mapping(JSON.parse(store.readArtifact(runId, outputPath).content.toString("utf8")));
    const status = normalizedTarget(result?.status);
    if (status === "blocked") return true;
    if (status === "advice") return false;
    throw new Error(`unsupported consultation status ${String(result?.status)}`);
  } catch (error) {
    throw new AgentFlowRunStateError(
      `Completed consultation cannot recover because its blocking evidence is unavailable: ${error instanceof Error ? error.message : String(error)}.`,
      "AGENT_FLOW_RUN_LOCK_RECOVERY",
      { cause: error }
    );
  }
}

function routeAfterSuccessfulStep(
  store: AgentFlowRunStateStore,
  runId: string,
  completedSteps: string[],
  stepId: string,
  step: AgentFlowWorkflowStep,
  currentSteps: AgentFlowWorkflowStep[],
  stepIndex: number,
  stepLocations: Map<string, RuntimeStepLocation>,
  budget: SuccessfulRoutingBudget,
  selectedTarget?: string
): SuccessfulRoute {
  const stopped = stoppedPipelineResult(store, runId, completedSteps);
  if (stopped !== undefined) return { result: stopped };
  const target = selectedTarget ?? (normalizedTarget(step.type) === "condition"
    ? undefined
    : normalizedTarget(step.then) ?? normalizedTarget(step.goto));
  if (target === undefined) {
    return fallthroughAfterStep(store, runId, completedSteps, stepId, currentSteps, stepIndex, budget);
  }
  const nextLocation = stepLocations.get(target);
  if (nextLocation !== undefined) {
    const failure = successfulTransitionFailure(store, runId, completedSteps, stepId, target, budget);
    if (failure !== undefined) return { result: failure };
    return { steps: nextLocation.steps, nextIndex: nextLocation.index };
  }
  if (target === "continue" || target === "ignore") {
    return fallthroughAfterStep(store, runId, completedSteps, stepId, currentSteps, stepIndex, budget);
  }
  if (target === "complete" || target === "completed") {
    return { result: finishCompleted(store, runId, completedSteps, budget.terminalEffects) };
  }
  if (target === "fail") {
    return {
      result: finishSuccessfulTerminalRoute(
        store,
        runId,
        completedSteps,
        stepId,
        "failed",
        budget.terminalEffects
      )
    };
  }
  if (target === "pause") {
    return {
      result: finishSuccessfulTerminalRoute(
        store,
        runId,
        completedSteps,
        stepId,
        "paused",
        budget.terminalEffects
      )
    };
  }
  if (target === "cancel") {
    return {
      result: finishSuccessfulTerminalRoute(
        store,
        runId,
        completedSteps,
        stepId,
        "cancelled",
        budget.terminalEffects
      )
    };
  }
  const attempt = Math.max(1, budget.attempts.get(stepId) ?? 0);
  const message = `Step ${stepId} routed to unresolved target ${target}.`;
  persistAgentFlowFailurePayload(store, {
    id: `routing:${safeId(stepId)}:to-${safeId(target)}:attempt-${attempt}`,
    runId,
    stepId,
    stepType: "routing",
    attempt,
    exitCode: null,
    summary: message,
    classification: "routing_target",
    retryable: false,
    outcome: "fail",
    indexPayload: { attempt, target, message, outcome: "fail" }
  });
  return { result: finishFailure(store, runId, completedSteps, stepId, {
    exitCode: null,
    timedOut: false,
    message
  }, "failed", budget.terminalEffects) };
}

function fallthroughAfterStep(
  store: AgentFlowRunStateStore,
  runId: string,
  completedSteps: string[],
  stepId: string,
  currentSteps: AgentFlowWorkflowStep[],
  stepIndex: number,
  budget: SuccessfulRoutingBudget
): SuccessfulRoute {
  const target = normalizedTarget(currentSteps[stepIndex + 1]?.id);
  if (target !== undefined) {
    const failure = successfulTransitionFailure(store, runId, completedSteps, stepId, target, budget);
    if (failure !== undefined) return { result: failure };
  }
  return { steps: currentSteps, nextIndex: stepIndex + 1 };
}

function successfulTransitionFailure(
  store: AgentFlowRunStateStore,
  runId: string,
  completedSteps: string[],
  stepId: string,
  target: string,
  budget: SuccessfulRoutingBudget
): AgentFlowCommandPipelineResult | undefined {
  const entersDisagreement = budget.reviewCycleStepIds.has(target)
    && budget.maxReviewCycles !== undefined
    && (budget.attempts.get(target) ?? 0) >= budget.maxReviewCycles;
  const attemptLimit = budget.stepAttemptLimits.get(target);
  if (!entersDisagreement && attemptLimit !== undefined && (budget.attempts.get(target) ?? 0) + 1 > attemptLimit) {
    return finishRoutingFailure(store, runId, completedSteps, stepId, target, {
      exitCode: null,
      timedOut: false,
      message: `Step ${stepId} cannot route to ${target} because limits.max_step_attempts allows ${attemptLimit} attempt(s).`
    }, budget, "max_step_attempts");
  }
  const staysWithinBoundedReviewCycle = budget.maxReviewCycles !== undefined
    && advancesBoundedReviewCycle(stepId, target, budget);
  if (staysWithinBoundedReviewCycle) return undefined;
  if ((budget.visits.get(target) ?? 0) === 0) return undefined;
  if (budget.maxRecoveryCycles === undefined) {
    return finishRoutingFailure(store, runId, completedSteps, stepId, target, {
      exitCode: null,
      timedOut: false,
      message: `Step ${stepId} repeated route target ${target} without a positive executable limits.max_recovery_cycles bound.`
    }, budget, "max_recovery_cycles");
  }
  const cycles = (budget.recoveryCycles.get(target) ?? 0) + 1;
  budget.recoveryCycles.set(target, cycles);
  if (cycles <= budget.maxRecoveryCycles) return undefined;
  return finishRoutingFailure(store, runId, completedSteps, stepId, target, {
    exitCode: null,
    timedOut: false,
    message: `Step ${stepId} exceeded limits.max_recovery_cycles ${budget.maxRecoveryCycles} while routing to ${target}.`
  }, budget, "max_recovery_cycles");
}

function advancesBoundedReviewCycle(
  stepId: string,
  target: string,
  budget: SuccessfulRoutingBudget
): boolean {
  const sourceReviewIds = budget.reviewCyclePathReviewIds.get(stepId);
  const targetReviewIds = budget.reviewCyclePathReviewIds.get(target);
  if (sourceReviewIds === undefined || targetReviewIds === undefined) return false;
  return [...sourceReviewIds].some((reviewId) => targetReviewIds.has(reviewId)
    && (target === reviewId
      || (budget.visits.get(target) ?? 0) < (budget.attempts.get(reviewId) ?? 0)));
}

function recoveryInvocationBudgetFailure(
  store: AgentFlowRunStateStore,
  runId: string,
  completedSteps: string[],
  stepId: string,
  budget: SuccessfulRoutingBudget
): AgentFlowCommandPipelineResult | undefined {
  const invocations = budget.recoveryInvocations.get(stepId) ?? 0;
  if (budget.maxRecoveryCycles !== undefined && invocations >= budget.maxRecoveryCycles) {
    return finishRoutingFailure(store, runId, completedSteps, stepId, stepId, {
      exitCode: null,
      timedOut: false,
      message: `Step ${stepId} cannot start recovery because it would exceed limits.max_recovery_cycles ${budget.maxRecoveryCycles}.`
    }, budget, "max_recovery_cycles");
  }
  budget.recoveryInvocations.set(stepId, invocations + 1);
  return undefined;
}

function finishRoutingFailure(
  store: AgentFlowRunStateStore,
  runId: string,
  completedSteps: string[],
  stepId: string,
  target: string,
  failure: { exitCode: null; timedOut: false; message: string },
  budget: SuccessfulRoutingBudget,
  limitName: "max_recovery_cycles" | "max_step_attempts"
): AgentFlowCommandPipelineResult {
  const attempt = Math.max(1, budget.attempts.get(stepId) ?? 0);
  const outcome = recoveryLimitOutcome(budget.terminalEffects.workflow);
  persistAgentFlowFailurePayload(store, {
    id: `routing:${safeId(stepId)}:to-${safeId(target)}:attempt-${attempt}`,
    runId,
    stepId,
    stepType: "routing",
    attempt,
    exitCode: null,
    summary: failure.message,
    classification: "routing_limit",
    retryable: false,
    outcome,
    indexPayload: { attempt, target, limit: limitName, message: failure.message, outcome }
  });
  store.appendRunEvent(runId, {
    type: "recovery.limit_reached",
    stepId,
    payload: { target, limit: limitName, message: failure.message, outcome }
  });
  return finishFailure(
    store,
    runId,
    completedSteps,
    stepId,
    failure,
    outcome === "fail" ? "failed" : "paused",
    budget.terminalEffects
  );
}

function finishSuccessfulTerminalRoute(
  store: AgentFlowRunStateStore,
  runId: string,
  completedSteps: string[],
  stepId: string,
  status: "failed" | "paused" | "cancelled",
  terminalEffects: AgentFlowPipelineTerminalEffects
): AgentFlowCommandPipelineResult {
  const target = status === "failed" ? "fail" : status === "paused" ? "pause" : "cancel";
  const message = `Step ${stepId} routed the pipeline to ${target}.`;
  const finalized = finalizePipelineRun(store, runId, terminalEffects, {
    intendedStatus: status,
    completedSteps,
    currentStepId: null,
    output: { completedSteps, terminalRoute: { status, stepId } },
    message,
    eventPayload: { routedByStepId: stepId, completedSteps, message }
  });
  return { status: finalized.status, completedSteps, message: finalized.message ?? message };
}

function createSuccessfulRoutingBudget(
  workflow: AgentFlowWorkflow,
  notifications: AgentFlowNotificationRegistry,
  initialAttempts: Record<string, number> = {}
): SuccessfulRoutingBudget {
  const limits = mapping(workflow.limits);
  const configuredStepAttempts = mapping(limits?.max_step_attempts);
  const stepAttemptLimits = new Map(Object.entries(configuredStepAttempts ?? {}).flatMap(([stepId, value]) =>
    typeof value === "number" && Number.isFinite(value) && value > 0 ? [[stepId, value] as const] : []
  ));
  const maxRecoveryCycles = workflow.style !== "pipeline" && typeof limits?.max_recovery_cycles === "number"
    && Number.isSafeInteger(limits.max_recovery_cycles) && limits.max_recovery_cycles > 0
    ? limits.max_recovery_cycles
    : undefined;
  const collaboration = mapping(workflow.collaboration);
  const maxReviewCycles = workflow.style === "collaborative"
    && typeof collaboration?.max_review_cycles === "number"
    && Number.isSafeInteger(collaboration.max_review_cycles)
    && collaboration.max_review_cycles > 0
    ? collaboration.max_review_cycles
    : undefined;
  const reviewCycleStepIds = collectAgentFlowReviewCycleStepIds(workflow.steps);
  const reviewCyclePathReviewIds = collectAgentFlowReviewCyclePathReviewIds(workflow.steps);
  return {
    terminalEffects: { workflow, notifications },
    maxRecoveryCycles,
    maxReviewCycles,
    stepAttemptLimits,
    reviewCyclePathReviewIds,
    reviewCycleStepIds,
    visits: new Map(),
    recoveryCycles: new Map(),
    recoveryInvocations: new Map(),
    disagreementEpisodes: new Map(),
    disagreementRounds: new Map(),
    attempts: new Map(Object.entries(initialAttempts))
  };
}

function collectRuntimeStepLocations(
  steps: AgentFlowWorkflowStep[],
  locations = new Map<string, RuntimeStepLocation>()
): Map<string, RuntimeStepLocation> {
  steps.forEach((step, index) => {
    if (normalizedTarget(step.type) !== undefined) {
      const stepId = requiredStepId(step);
      if (locations.has(stepId)) {
        throw new AgentFlowRunStateError(
          `Agent Flow workflow has multiple steps with ID ${JSON.stringify(stepId)}; runtime routing is ambiguous.`,
          "AGENT_FLOW_STEP_AMBIGUOUS"
        );
      }
      locations.set(stepId, { steps, index });
    }

    for (const field of ["body", "steps"] as const) {
      const nested = step[field];
      if (Array.isArray(nested)) {
        collectRuntimeStepLocations(nested.filter(isWorkflowStep), locations);
      }
    }

    if (normalizedTarget(step.type) === "parallel" && Array.isArray(step.branches)) {
      collectRuntimeStepLocations(step.branches.filter(isWorkflowStep), locations);
    }
  });

  return locations;
}

function allocateStepAttempt(budget: SuccessfulRoutingBudget, stepId: string): number | undefined {
  const attempt = (budget.attempts.get(stepId) ?? 0) + 1;
  const limit = budget.stepAttemptLimits.get(stepId);
  if (limit !== undefined && attempt > limit) return undefined;
  budget.attempts.set(stepId, attempt);
  return attempt;
}

function stepAttemptLimitResult(
  store: AgentFlowRunStateStore,
  runId: string,
  completedSteps: string[],
  stepId: string,
  budget: SuccessfulRoutingBudget
): AgentFlowCommandPipelineResult {
  const limit = budget.stepAttemptLimits.get(stepId)!;
  const attempt = (budget.attempts.get(stepId) ?? 0) + 1;
  const failure = {
    exitCode: null,
    timedOut: false,
    message: `Step ${stepId} cannot start because limits.max_step_attempts allows ${limit} attempt(s).`
  };
  const outcome = recoveryLimitOutcome(budget.terminalEffects.workflow);
  persistAgentFlowFailurePayload(store, {
    id: `routing:${safeId(stepId)}:attempt-${attempt}:limit`,
    runId,
    stepId,
    stepType: "routing",
    attempt,
    exitCode: null,
    summary: failure.message,
    classification: "step_attempt_limit",
    retryable: false,
    outcome,
    indexPayload: { attempt, limit, message: failure.message, outcome }
  });
  store.appendRunEvent(runId, {
    type: "recovery.limit_reached",
    stepId,
    payload: { limit: "max_step_attempts", configured: limit, message: failure.message, outcome }
  });
  return finishFailure(
    store, runId, completedSteps, stepId, failure,
    outcome === "fail" ? "failed" : "paused", budget.terminalEffects
  );
}

function finishCompleted(
  store: AgentFlowRunStateStore,
  runId: string,
  completedSteps: string[],
  terminalEffects: AgentFlowPipelineTerminalEffects
): AgentFlowCommandPipelineResult {
  const finalized = finalizePipelineRun(store, runId, terminalEffects, {
    intendedStatus: "completed",
    completedSteps,
    currentStepId: null,
    output: { completedSteps },
    eventPayload: { completedSteps }
  });
  return { status: finalized.status, completedSteps, ...(finalized.message === undefined ? {} : { message: finalized.message }) };
}

function finishBlockedConsult(
  store: AgentFlowRunStateStore,
  runId: string,
  completedSteps: string[],
  stepId: string,
  terminalEffects: AgentFlowPipelineTerminalEffects
): AgentFlowCommandPipelineResult {
  const message = `Consultation ${stepId} blocked workflow continuation.`;
  const finalized = finalizePipelineRun(store, runId, terminalEffects, {
    intendedStatus: "paused",
    completedSteps,
    currentStepId: stepId,
    output: { completedSteps, resultStatus: "blocked" },
    message,
    eventPayload: { completedSteps, resultStatus: "blocked", message },
    eventStepId: stepId
  });
  return {
    status: finalized.status,
    completedSteps,
    resultStatus: "blocked",
    message: finalized.message ?? message
  };
}

interface FinalizePipelineRunInput {
  intendedStatus: Extract<AgentFlowRunStatus, "completed" | "failed" | "paused" | "cancelled">;
  completedSteps: string[];
  currentStepId: string | null;
  output?: AgentFlowRunStateValue;
  error?: AgentFlowRunStateValue;
  message?: string;
  eventPayload: AgentFlowRunStateValue;
  eventStepId?: string;
  failureContext?: Record<string, AgentFlowRunStateValue>;
  terminalNotificationDelivered?: boolean;
  beforeTerminalEffects?: () => void;
  onFinalStatus?: (
    status: Extract<AgentFlowRunStatus, "completed" | "failed" | "paused" | "cancelled">,
    message: string | undefined,
    notificationFailure: AgentFlowNotificationDeliveryResult["requiredFailure"],
    notificationAttempts: AgentFlowNotificationDeliveryResult["attempts"]
  ) => void;
  beforeFinalTransition?: (
    status: Extract<AgentFlowRunStatus, "completed" | "failed" | "paused" | "cancelled">,
    message: string | undefined
  ) => void;
}

class NotificationPromotionRollbackError extends Error {
  constructor(
    message: string,
    readonly failure: AgentFlowNotificationDeliveryResult["requiredFailure"],
    readonly attempts: NonNullable<AgentFlowNotificationDeliveryResult["attempts"]>
  ) {
    super(message);
    this.name = "NotificationPromotionRollbackError";
  }
}

function finalizePipelineRun(
  store: AgentFlowRunStateStore,
  runId: string,
  terminalEffects: AgentFlowPipelineTerminalEffects,
  input: FinalizePipelineRunInput
): {
  status: Extract<AgentFlowRunStatus, "completed" | "failed" | "paused" | "cancelled">;
  message?: string;
} {
  if (terminalEffects.workflow.style !== "pipeline") {
    return store.withRunFinalizationTransaction(runId, () => {
      const current = store.getRun(runId);
      if (current?.status !== "running") {
        return finalizationResultForCurrentRun(store, runId, input);
      }
      input = staleApprovalFailureInput(store, runId, input);
      let status = input.intendedStatus;
      let message = input.message;
      let error = input.error;
      input.beforeTerminalEffects?.();
      const delivery = input.terminalNotificationDelivered === true
        ? {}
        : deliverAgentFlowNotifications(
            store,
            runId,
            terminalEffects.workflow,
            status,
            terminalEffects.notifications
          );
      const currentAfterDelivery = store.getRun(runId);
      if (currentAfterDelivery?.status !== "running") {
        const stopped = finalizationResultForCurrentRun(store, runId, input);
        input.onFinalStatus?.(
          stopped.status,
          stopped.message,
          delivery.requiredFailure,
          delivery.attempts
        );
        return stopped;
      }
      const fallbackFailure = delivery.requiredFailure !== undefined && status !== "failed"
        ? delivery.requiredFailure
        : undefined;
      if (fallbackFailure !== undefined) {
        status = "failed";
        message = `Required ${fallbackFailure.channel} notification for ${fallbackFailure.event} failed: ${fallbackFailure.message}`;
        error = {
          code: "notification.required.failed",
          channel: fallbackFailure.channel,
          event: fallbackFailure.event,
          message
        };
      }
      input.onFinalStatus?.(status, message, fallbackFailure, fallbackFailure === undefined ? undefined : delivery.attempts);
      if (fallbackFailure !== undefined) {
        deliverAgentFlowNotifications(
          store,
          runId,
          terminalEffects.workflow,
          "failed",
          terminalEffects.notifications
        );
        if (store.getRun(runId)?.status !== "running") {
          return finalizationResultForCurrentRun(store, runId, input);
        }
      }
      input.beforeFinalTransition?.(status, message);
      store.updateRun(runId, {
        currentStepId: input.currentStepId,
        ...(status === "failed" && input.failureContext !== undefined ? { context: input.failureContext } : {}),
        ...(input.output === undefined ? {} : { output: input.output }),
        ...(error === undefined ? {} : { error })
      });
      const eventPayload = status === input.intendedStatus
        ? input.eventPayload
        : {
            code: finalizationErrorCode(error),
            completedSteps: input.completedSteps,
            message: message ?? "Workflow finalization failed."
          };
      store.transitionRunWithEvent(runId, {
        status,
        allowedFrom: ["running"],
        event: {
          type: `run.${status}`,
          ...(input.eventStepId === undefined ? {} : { stepId: input.eventStepId }),
          payload: eventPayload
        }
      });
      return {
        status,
        ...(message === undefined ? {} : { message })
      };
    });
  }

  return withAgentFlowPipelineFinalization(
    store,
    runId,
    () => finalizationResultForCurrentRun(store, runId, input),
    () => finalizePipelineRunLocked(
      store,
      runId,
      terminalEffects,
      staleApprovalFailureInput(store, runId, input)
    )
  );
}

function staleApprovalFailureInput(
  store: AgentFlowRunStateStore,
  runId: string,
  input: FinalizePipelineRunInput
): FinalizePipelineRunInput {
  if (input.intendedStatus !== "completed") return input;
  const staleApprovalIds = latestStaleApprovalStepIds(store, runId);
  if (staleApprovalIds.length === 0) return input;
  const message = staleApprovalMessage(staleApprovalIds, "workflow completion");
  return {
    ...input,
    intendedStatus: "failed",
    message,
    error: { code: "approval.stale", approvalStepIds: staleApprovalIds, message },
    eventPayload: { completedSteps: input.completedSteps, approvalStepIds: staleApprovalIds, message }
  };
}

function mergeContinuationStaleApprovals(
  store: AgentFlowRunStateStore,
  runId: string,
  workflow: AgentFlowWorkflow,
  step: AgentFlowWorkflowStep
): string[] {
  const stepType = normalizedTarget(step.type);
  const actor = stepType === "session_request"
    ? step.session
    : stepType === "consult" || stepType === "challenge" ? step.to : undefined;
  const mergeCapable = typeof actor === "string" && sessionCanMerge(workflow, actor);
  return mergeCapable ? staleApprovalStepIdsAcrossLineage(store, runId) : [];
}

function sessionCanMerge(workflow: AgentFlowWorkflow, sessionId: string): boolean {
  return mapping(mapping(mapping(workflow.sessions)?.[sessionId.trim()])?.authority)?.can_merge === true;
}

function finalizePipelineRunLocked(
  store: AgentFlowRunStateStore,
  runId: string,
  terminalEffects: AgentFlowPipelineTerminalEffects,
  input: FinalizePipelineRunInput
): {
  status: Extract<AgentFlowRunStatus, "completed" | "failed" | "paused" | "cancelled">;
  message?: string;
} {
  const current = store.getRun(runId);
  if (current?.status !== "running") {
    return finalizationResultForCurrentRun(store, runId, input);
  }

  let status = input.intendedStatus;
  let message = input.message;
  let error = input.error;
  let summaryReady = status === "paused";
  let notificationFailure: AgentFlowNotificationDeliveryResult["requiredFailure"];
  let notificationAttempts: AgentFlowNotificationDeliveryResult["attempts"];

  input.beforeTerminalEffects?.();

  if (!summaryReady) {
    try {
      writeAgentFlowFinalSummary(store, runId, terminalEffects.workflow, {
        status,
        completedSteps: input.completedSteps,
        ...(message === undefined ? {} : { message })
      });
      summaryReady = true;
    } catch (summaryError) {
      ({ status, message, error } = recordSummaryFailure(store, runId, summaryError));
    }
  }

  if (input.terminalNotificationDelivered !== true && (summaryReady || input.intendedStatus === "failed")) {
    const delivery = deliverAgentFlowNotifications(
      store,
      runId,
      terminalEffects.workflow,
      input.intendedStatus,
      terminalEffects.notifications
    );
    if (delivery.requiredFailure !== undefined && input.intendedStatus !== "failed") {
      notificationFailure = delivery.requiredFailure;
      notificationAttempts = delivery.attempts;
      status = "failed";
      message = `Required ${delivery.requiredFailure.channel} notification for ${delivery.requiredFailure.event} failed: ${delivery.requiredFailure.message}`;
      error = {
        code: "notification.required.failed",
        channel: delivery.requiredFailure.channel,
        event: delivery.requiredFailure.event,
        message
      };
      try {
        writeAgentFlowFinalSummary(store, runId, terminalEffects.workflow, {
          status,
          completedSteps: input.completedSteps,
          message
        });
      } catch (summaryError) {
        ({ status, message, error } = recordSummaryFailure(store, runId, summaryError));
      }
    }
  }

  input.onFinalStatus?.(status, message, notificationFailure, notificationAttempts);

  if (input.intendedStatus !== "failed" && status === "failed") {
    deliverAgentFlowNotifications(
      store,
      runId,
      terminalEffects.workflow,
      "failed",
      terminalEffects.notifications
    );
    if (store.getRun(runId)?.status !== "running") {
      return finalizationResultForCurrentRun(store, runId, input);
    }
  }

  input.beforeFinalTransition?.(status, message);
  store.updateRun(runId, {
    currentStepId: input.currentStepId,
    ...(status === "failed" && input.failureContext !== undefined ? { context: input.failureContext } : {}),
    ...(input.output === undefined ? {} : { output: input.output }),
    ...(error === undefined ? {} : { error })
  });
  const eventPayload = status === input.intendedStatus
    ? input.eventPayload
    : {
        code: finalizationErrorCode(error),
        completedSteps: input.completedSteps,
        message: message ?? "Pipeline finalization failed."
      };
  store.transitionRunWithEvent(runId, {
    status,
    allowedFrom: ["running"],
    event: {
      type: `run.${status}`,
      ...(input.eventStepId === undefined ? {} : { stepId: input.eventStepId }),
      payload: eventPayload
    }
  });
  applyAgentFlowRetention(store, runId, terminalEffects.workflow, status);
  if (status === "paused" || status === "cancelled" || notificationFinalizationFailed(error)) {
    markAgentFlowPipelineEffectsFinalized(store, runId, status);
  }
  return { status, ...(message === undefined ? {} : { message }) };
}

function finalizationResultForCurrentRun(
  store: AgentFlowRunStateStore,
  runId: string,
  input: FinalizePipelineRunInput
): {
  status: Extract<AgentFlowRunStatus, "completed" | "failed" | "paused" | "cancelled">;
  message?: string;
} {
  const run = store.getRun(runId);
  if (run?.status === "completed") return { status: "completed" };
  const stopped = stoppedPipelineResult(store, runId, input.completedSteps);
  if (stopped !== undefined) {
    return {
      status: stopped.status,
      ...(stopped.message === undefined ? {} : { message: stopped.message })
    };
  }
  return {
    status: input.intendedStatus,
    ...(input.message === undefined ? {} : { message: input.message })
  };
}

function recordSummaryFailure(
  store: AgentFlowRunStateStore,
  runId: string,
  summaryError: unknown
): {
  status: "failed";
  message: string;
  error: AgentFlowRunStateValue;
} {
  const message = `Could not persist final pipeline summary: ${summaryError instanceof Error ? summaryError.message : String(summaryError)}`;
  store.appendRunEvent(runId, {
    type: "summary.failed",
    payload: { message }
  });
  return {
    status: "failed",
    message,
    error: {
      code: "summary.persist.failed",
      message
    }
  };
}

function finalizationErrorCode(error: AgentFlowRunStateValue | undefined): string {
  if (error === null || typeof error !== "object" || Array.isArray(error)) {
    return "pipeline.finalization.failed";
  }
  return typeof error.code === "string" ? error.code : "pipeline.finalization.failed";
}

function normalizedTarget(value: unknown): string | undefined {
  if (typeof value !== "string" || value.trim().length === 0) return undefined;
  const target = value.trim();
  return target.includes("{{") || target.includes("}}") ? undefined : target;
}

function persistMcpCallFailure(
  store: AgentFlowRunStateStore,
  runId: string,
  stepId: string,
  message: string,
  retryable: boolean,
  attempt: number,
  outcome: AgentFlowFailureOutcome,
  rejected = false
): void {
  const payload = { attempt, message, outcome };
  const failureId = rejected ? `mcp-call:${safeId(stepId)}:preflight` : `mcp-call:${safeId(stepId)}:attempt-${attempt}`;
  const persisted = persistAgentFlowFailurePayload(store, {
    id: failureId,
    runId,
    stepId,
    stepType: "mcp_call",
    attempt,
    summary: message,
    classification: rejected ? "mcp_call_policy" : "mcp_call_failure",
    retryable,
    outcome,
    indexPayload: payload
  });
  const indexedPayload = { ...persisted.indexPayload, ...failureReference(persisted) };
  store.upsertStep({ runId, stepId, attempt, status: "failed", error: indexedPayload });
  store.appendRunEvent(runId, {
    type: rejected ? "step.rejected" : "step.failed",
    stepId,
    payload: indexedPayload
  });
}

function mcpCallFailureIsRetryable(error: unknown): boolean {
  return error instanceof AgentFlowMcpCallError && error.code === "AGENT_FLOW_MCP_ADAPTER_FAILED";
}

function persistSessionRequestInterruption(
  store: AgentFlowRunStateStore,
  runId: string,
  workflow: AgentFlowWorkflow,
  stepId: string,
  sessionId: string,
  status: AgentFlowRunStopStatus
): void {
  const previousSession = store.getSession(runId, sessionId);
  const sessionDefinition = mapping(workflow.sessions?.[sessionId]);
  const provider = typeof sessionDefinition?.provider === "string" ? sessionDefinition.provider.trim() : "unknown";
  store.upsertSession({
    id: sessionId,
    runId,
    stepId,
    provider,
    status,
    ...(previousSession?.externalSessionId === null || previousSession?.externalSessionId === undefined
      ? {}
      : { externalSessionId: previousSession.externalSessionId }),
    state: { resume: sessionDefinition?.resume === true, lastStepId: stepId, interrupted: status }
  });
}

function executeTransformStep(
  store: AgentFlowRunStateStore,
  runId: string,
  stepId: string,
  step: AgentFlowWorkflowStep,
  transforms: AgentFlowArtifactTransformRegistry,
  attempt: number,
  retryable: boolean
): { failure?: string; stopped?: AgentFlowRunStopStatus } {
  const input = {
    transform: typeof step.transform === "string" ? step.transform : null,
    input: typeof step.input === "string" ? step.input : null,
    output: typeof step.output === "string" ? step.output : null
  };
  store.updateRun(runId, { currentStepId: stepId, error: null });
  store.upsertStep({ runId, stepId, attempt, status: "running", input });
  store.appendRunEvent(runId, { type: "step.started", stepId, payload: { attempt, ...input } });

  try {
    const outputPath = normalizedTarget(step.output);
    const existingOutput = outputPath === undefined ? null : store.getArtifact(runId, outputPath);
    const executableStep = attempt > 1 && existingOutput?.producerStepId === stepId
      ? { ...step, overwrite: true }
      : step;
    const result = executeAgentFlowArtifactTransform(store, runId, executableStep, transforms, {
      attempt,
      beforePublish: () => {
        const stopped = activeStopStatus(store, runId);
        if (stopped !== undefined) throw new TransformInterruptedError(stopped);
      }
    });
    const stoppedAfterPublish = activeStopStatus(store, runId);
    if (stoppedAfterPublish !== undefined) {
      const output = { attempt, status: stoppedAfterPublish, checksum: result.artifact.checksum };
      store.upsertStep({ runId, stepId, attempt, status: stoppedAfterPublish, output });
      store.appendRunEvent(runId, { type: "step.interrupted", stepId, payload: output });
      return { stopped: stoppedAfterPublish };
    }
    const output = {
      attempt,
      transform: result.transform,
      input: result.inputPath,
      output: result.outputPath,
      checksum: result.artifact.checksum
    };
    store.upsertStep({ runId, stepId, attempt, status: "completed", output });
    store.appendRunEvent(runId, { type: "step.completed", stepId, payload: output });
    return {};
  } catch (error) {
    if (error instanceof TransformInterruptedError) {
      const output = { attempt, status: error.status };
      store.upsertStep({ runId, stepId, attempt, status: error.status, output });
      store.appendRunEvent(runId, { type: "step.interrupted", stepId, payload: output });
      return { stopped: error.status };
    }
    if (error instanceof AgentFlowRunStateError && error.code === "AGENT_FLOW_ARTIFACT_RUN_STATUS") {
      const stopped = activeStopStatus(store, runId);
      if (stopped !== undefined) {
        const output = { attempt, status: stopped };
        store.upsertStep({ runId, stepId, attempt, status: stopped, output });
        store.appendRunEvent(runId, { type: "step.interrupted", stepId, payload: output });
        return { stopped };
      }
    }
    const message = error instanceof Error ? error.message : String(error);
    const outcome = failureOutcome(step, retryable);
    const payload = { attempt, message, outcome };
    const failureId = `artifact-transform:${safeId(stepId)}:attempt-${attempt}`;
    const persisted = persistAgentFlowFailurePayload(store, {
      id: failureId,
      runId,
      stepId,
      stepType: "artifact_transform",
      attempt,
      summary: message,
      classification: "artifact_transform_failure",
      retryable,
      outcome,
      indexPayload: payload
    });
    const indexedPayload = { ...persisted.indexPayload, ...failureReference(persisted) };
    store.upsertStep({ runId, stepId, attempt, status: "failed", error: indexedPayload });
    store.appendRunEvent(runId, { type: "step.failed", stepId, payload: indexedPayload });
    return { failure: message };
  }
}

class TransformInterruptedError extends Error {
  constructor(readonly status: AgentFlowRunStopStatus) {
    super(`Artifact transform was interrupted because the run was ${status}.`);
  }
}

function persistMcpCallInterruption(
  store: AgentFlowRunStateStore,
  runId: string,
  stepId: string,
  attempt: number,
  status: AgentFlowRunStopStatus
): void {
  const output = { attempt, status };
  store.upsertStep({ runId, stepId, attempt, status, output });
  store.appendRunEvent(runId, { type: "step.interrupted", stepId, payload: output });
}

function stoppedPipelineResult(
  store: AgentFlowRunStateStore,
  runId: string,
  completedSteps: string[]
): AgentFlowCommandPipelineResult | undefined {
  const run = store.getRun(runId);
  const status = run?.status;
  if (status === "running") return undefined;
  if (status === "failed") {
    const workflow = run!.context.workflow as unknown as AgentFlowWorkflow;
    if (workflow.style === "pipeline") {
      applyAgentFlowRetention(store, runId, workflow, status);
      markAgentFlowPipelineEffectsFinalized(store, runId, status);
    }
    return {
      status,
      completedSteps,
      message: terminalFailureMessage(run?.error)
    };
  }
  if (status === "paused" || status === "cancelled") {
    if (status === "cancelled") {
      const workflow = run!.context.workflow as unknown as AgentFlowWorkflow;
      if (workflow.style === "pipeline") {
        if (!agentFlowPipelineEffectsFinalized(store, runId, status)) {
          writeAgentFlowFinalSummary(store, runId, workflow, {
            status,
            completedSteps,
            message: `Agent Flow run ${runId} was cancelled.`
          });
        }
        applyAgentFlowRetention(store, runId, workflow, status);
        markAgentFlowPipelineEffectsFinalized(store, runId, status);
      }
    }
    return {
      status,
      completedSteps,
      message: `Agent Flow run ${runId} was ${status}; no additional commands were started.`
    };
  }
  throw new AgentFlowRunStateError(
    `Agent Flow run ${runId} cannot continue while its status is ${String(status)}.`,
    "AGENT_FLOW_RUN_TRANSITION"
  );
}

function interruptedPipelineResult(
  store: AgentFlowRunStateStore,
  runId: string,
  completedSteps: string[],
  interruptedStatus: AgentFlowRunStopStatus
): AgentFlowCommandPipelineResult {
  return stoppedPipelineResult(store, runId, completedSteps) ?? {
    status: interruptedStatus,
    completedSteps,
    message: `Agent Flow run ${runId} was interrupted as ${interruptedStatus}; no additional commands were started.`
  };
}

function activeStopStatus(store: AgentFlowRunStateStore, runId: string): AgentFlowRunStopStatus | undefined {
  const run = store.getRun(runId);
  const status = run?.status;
  return status === "paused" || status === "failed" || status === "cancelled" ? status : undefined;
}

function notificationFinalizationFailed(error: AgentFlowRunStateValue | null | undefined): boolean {
  return error !== null && typeof error === "object" && !Array.isArray(error)
    && error.code === "notification.required.failed";
}

function terminalFailureMessage(error: AgentFlowRunStateValue | null | undefined): string {
  return error !== null && typeof error === "object" && !Array.isArray(error)
    && typeof error.message === "string"
    ? error.message
    : "The pipeline failed while it was being finalized.";
}

function finishFailure(
  store: AgentFlowRunStateStore,
  runId: string,
  completedSteps: string[],
  stepId: string,
  failure: { exitCode: number | null; timedOut: boolean; message: string },
  status: "failed" | "paused",
  terminalEffects: AgentFlowPipelineTerminalEffects
): AgentFlowCommandPipelineResult {
  const failureOutcome = status === "failed" ? "fail" : "pause";
  const persistedFailure = { ...failure, outcome: failureOutcome };
  const finalized = finalizePipelineRun(store, runId, terminalEffects, {
    intendedStatus: status,
    completedSteps,
    currentStepId: stepId,
    output: { completedSteps },
    error: persistedFailure,
    message: failure.message,
    eventPayload: { stepId, ...persistedFailure },
    eventStepId: stepId
  });
  if (finalized.status === "cancelled") {
    const stopped = stoppedPipelineResult(store, runId, completedSteps);
    if (stopped !== undefined) return stopped;
  }
  const finalFailureOutcome = finalized.status === "failed" ? "fail" : "pause";
  return {
    status: finalized.status,
    completedSteps,
    failedStep: stepId,
    failureOutcome: finalFailureOutcome,
    ...failure,
    ...(finalized.message === undefined ? {} : { message: finalized.message })
  };
}

function finishRequiredStepNotificationFailure(
  store: AgentFlowRunStateStore,
  runId: string,
  completedSteps: string[],
  stepId: string,
  attempt: number,
  stepType: string,
  failure: NonNullable<AgentFlowNotificationDeliveryResult["requiredFailure"]>,
  terminalEffects: AgentFlowPipelineTerminalEffects,
  options?: {
    failureContext?: Record<string, AgentFlowRunStateValue>;
    beforeFailure?: () => void;
  }
): AgentFlowCommandPipelineResult {
  const message = `Required ${failure.channel} notification for ${failure.event} failed: ${failure.message}`;
  const error = {
    code: "notification.required.failed",
    channel: failure.channel,
    event: failure.event,
    attempt,
    message,
    outcome: "fail" as const
  };
  const persistFailure = (): void => {
    options?.beforeFailure?.();
    const persisted = persistAgentFlowFailurePayload(store, {
      id: `notification:${safeId(stepId)}:attempt-${attempt}:${safeId(failure.event)}`,
      runId,
      stepId,
      stepType,
      attempt,
      exitCode: null,
      summary: message,
      classification: "notification_failure",
      retryable: false,
      outcome: "fail",
      indexPayload: error
    });
    const indexedError = { ...persisted.indexPayload, ...failureReference(persisted) };
    store.upsertStep({ runId, stepId, attempt, status: "failed", error: indexedError });
    store.appendRunEvent(runId, { type: "step.failed", stepId, payload: indexedError });
  };
  const terminalNotificationDelivered = terminalEffects.workflow.style === "pipeline";
  if (terminalNotificationDelivered) {
    const stoppedBeforeFailureDelivery = stoppedPipelineResult(store, runId, completedSteps);
    if (stoppedBeforeFailureDelivery !== undefined) {
      if (stoppedBeforeFailureDelivery.status === "failed") {
        persistFailure();
        if (options?.failureContext !== undefined) {
          store.updateRun(runId, { currentStepId: null, context: options.failureContext });
        }
      }
      return stoppedBeforeFailureDelivery;
    }
    deliverAgentFlowNotifications(
      store,
      runId,
      terminalEffects.workflow,
      "failed",
      terminalEffects.notifications
    );
    const stopped = stoppedPipelineResult(store, runId, completedSteps);
    if (stopped !== undefined) {
      if (stopped.status === "failed") {
        persistFailure();
        if (options?.failureContext !== undefined) {
          store.updateRun(runId, { currentStepId: null, context: options.failureContext });
        }
      }
      return stopped;
    }
  }
  const finalized = finalizePipelineRun(store, runId, terminalEffects, {
    intendedStatus: "failed",
    completedSteps,
    currentStepId: stepId,
    output: { completedSteps },
    error,
    message,
    eventPayload: { stepId, ...error },
    eventStepId: stepId,
    failureContext: options?.failureContext,
    terminalNotificationDelivered,
    beforeFinalTransition: persistFailure
  });
  if (finalized.status !== "failed") {
    const stopped = stoppedPipelineResult(store, runId, completedSteps);
    if (stopped !== undefined) return stopped;
  }
  return {
    status: finalized.status,
    completedSteps,
    failedStep: stepId,
    failureOutcome: "fail",
    exitCode: null,
    timedOut: false,
    message: finalized.message ?? message
  };
}

function runCommand(
  repoRoot: string,
  command: string,
  timeoutMs: number | undefined,
  stopStatus: () => AgentFlowRunStopStatus | undefined,
  interruptError: () => Error | undefined
): Promise<CommandAttemptResult> {
  return new Promise((resolve, reject) => {
    const stdout: Buffer[] = [];
    const stderr: Buffer[] = [];
    let capturedBytes = 0;
    let timedOut = false;
    let settled = false;
    let killTimer: ReturnType<typeof setTimeout> | undefined;
    let terminationMessage: string | undefined;
    let interruption: Error | undefined;
    const child = spawn(command, {
      cwd: repoRoot,
      shell: true,
      detached: process.platform !== "win32",
      env: { ...process.env, AGENT_FLOW_REPO_ROOT: repoRoot },
      stdio: ["ignore", "pipe", "pipe"]
    });

    const requestTermination = (message: string, timeout: boolean): void => {
      if (terminationMessage !== undefined) return;
      terminationMessage = message;
      timedOut = timeout;
      terminateChild(child.pid, "SIGTERM");
      killTimer = setTimeout(() => terminateChild(child.pid, "SIGKILL"), 250);
    };
    const capture = (chunks: Buffer[], chunk: Buffer | string): void => {
      const content = Buffer.from(chunk);
      const remaining = MAX_CAPTURE_BYTES - capturedBytes;
      if (remaining > 0) chunks.push(content.subarray(0, remaining));
      capturedBytes += Math.min(content.byteLength, Math.max(remaining, 0));
      if (content.byteLength > remaining) {
        requestTermination(`Command output exceeded the ${MAX_CAPTURE_BYTES}-byte capture limit.`, false);
      }
    };
    child.stdout?.on("data", (chunk: Buffer | string) => capture(stdout, chunk));
    child.stderr?.on("data", (chunk: Buffer | string) => capture(stderr, chunk));
    const timer = timeoutMs === undefined ? undefined : setTimeout(() => {
      requestTermination("Command exceeded timeout_seconds and was terminated.", true);
    }, timeoutMs);
    const lifecycleTimer = setInterval(() => {
      interruption = interruptError();
      if (interruption !== undefined) {
        requestTermination(interruption.message, false);
        return;
      }
      const status = stopStatus();
      if (status !== undefined) requestTermination(`Agent Flow run was ${status}; command was terminated.`, false);
    }, 25);

    const finish = (result: Omit<CommandAttemptResult, "stdout" | "stderr" | "timedOut">): void => {
      if (settled) return;
      settled = true;
      if (timer !== undefined) clearTimeout(timer);
      if (killTimer !== undefined) clearTimeout(killTimer);
      if (lifecycleTimer !== undefined) clearInterval(lifecycleTimer);
      if (interruption !== undefined) {
        reject(interruption);
        return;
      }
      resolve({
        ...result,
        ...(result.message === undefined && terminationMessage !== undefined ? { message: terminationMessage } : {}),
        timedOut,
        stdout: Buffer.concat(stdout),
        stderr: Buffer.concat(stderr)
      });
    };
    child.on("error", (error) => finish({ exitCode: null, signal: null, message: `Could not start command: ${error.message}` }));
    child.on("close", (exitCode, signal) => finish({ exitCode, signal }));
  });
}

function terminateChild(pid: number | undefined, signal: NodeJS.Signals): void {
  if (pid === undefined) return;
  try {
    process.kill(process.platform === "win32" ? pid : -pid, signal);
  } catch (error) {
    if (!(["ESRCH", "EPERM"] as Array<string | undefined>).includes((error as NodeJS.ErrnoException).code)) throw error;
  }
}

function persistCommandLog(
  store: AgentFlowRunStateStore,
  runId: string,
  stepId: string,
  attempt: number,
  stream: "stdout" | "stderr",
  content: Buffer
): void {
  store.writeArtifact({
    id: `command:${safeId(stepId)}:attempt-${attempt}:${stream}`,
    runId,
    stepId,
    path: commandLogPath(stepId, attempt, stream),
    kind: "command_log",
    contentType: "text/plain; charset=utf-8",
    content,
    metadata: { attempt, stream }
  });
}

function commandLogPath(
  stepId: string,
  attempt: number,
  stream: "stdout" | "stderr"
): string {
  return `logs/${safeId(stepId)}/attempt-${attempt}/${stream}.log`;
}

function persistDeclaredOutputs(
  store: AgentFlowRunStateStore,
  runId: string,
  stepId: string,
  step: AgentFlowWorkflowStep,
  attempt: number
): string | undefined {
  for (const declaredPath of stringList(step.outputs)) {
    const source = resolveOutputPath(store.repoRoot, declaredPath);
    if (source === undefined) return `Declared output ${JSON.stringify(declaredPath)} must be repo-relative and stay inside the repository.`;
    let stat: fs.Stats;
    try {
      stat = fs.lstatSync(source);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return `Command completed without creating declared output ${declaredPath}.`;
      return `Could not inspect declared output ${declaredPath}: ${(error as Error).message}`;
    }
    if (stat.isSymbolicLink() || !stat.isFile()) return `Declared output ${declaredPath} must be a regular file, not a symlink or directory.`;
    const realSource = fs.realpathSync(source);
    if (!inside(store.repoRoot, realSource)) return `Declared output ${declaredPath} resolves outside the repository.`;
    try {
      const existing = store.getArtifact(runId, declaredPath);
      store.writeArtifact({
        id: `command-output:${createHash("sha256").update(declaredPath).digest("hex")}`,
        runId,
        stepId,
        path: declaredPath,
        kind: "command_output",
        contentType: "application/octet-stream",
        content: fs.readFileSync(realSource),
        overwrite: step.overwrite === true || (attempt > 1 && existing?.producerStepId === stepId),
        metadata: { attempt, source: declaredPath }
      });
    } catch (error) {
      return `Could not publish declared output ${declaredPath}: ${(error as Error).message}`;
    }
  }
  return undefined;
}

function validateCommandStep(
  repoRoot: string,
  workflow: AgentFlowWorkflow,
  step: AgentFlowWorkflowStep
): CommandPreflightFailure | undefined {
  if (typeof step.command !== "string" || step.command.trim().length === 0) {
    return { status: "failed", message: "Command steps require a non-empty command." };
  }
  if (step.timeout_seconds !== undefined &&
      (typeof step.timeout_seconds !== "number" || !Number.isFinite(step.timeout_seconds) || step.timeout_seconds <= 0)) {
    return { status: "failed", message: "Command timeout_seconds must be a positive finite number." };
  }
  if (typeof step.timeout_seconds === "number" && step.timeout_seconds > MAX_AGENT_FLOW_COMMAND_TIMEOUT_SECONDS) {
    return {
      status: "failed",
      message: `Command timeout_seconds cannot exceed ${MAX_AGENT_FLOW_COMMAND_TIMEOUT_SECONDS}.`
    };
  }

  const onFailure = mapping(step.on_failure);
  const retry = onFailure?.retry;
  if (retry !== undefined &&
      (!Number.isSafeInteger(retry) || Number(retry) < 0 || Number(retry) > MAX_AGENT_FLOW_COMMAND_RETRIES)) {
    return {
      status: "failed",
      message: `Command on_failure.retry must be an integer from 0 through ${MAX_AGENT_FLOW_COMMAND_RETRIES}.`
    };
  }
  if (["continue", "ignore"].includes(normalizedFailureThen(onFailure) ?? "") && onFailure?.allowed !== true) {
    return {
      status: "failed",
      message: "Command failures may continue or be ignored only when on_failure.allowed is true."
    };
  }

  const approval = evaluateAgentFlowPolicy(workflow, { kind: "approval", operation: "command" });
  if (approval.status !== "allow") return policyFailure(approval.status, approval.message);

  if (agentFlowCommandUnsafeReason(step.command) !== undefined) {
    const unsafe = evaluateAgentFlowPolicy(workflow, { kind: "unsafe_operation", operation: "command" });
    if (unsafe.status !== "allow") return policyFailure(unsafe.status, unsafe.message);
  }

  if (mapping(workflow.policies)?.file_scope !== undefined) {
    return {
      status: "failed",
      message: "Command steps cannot execute with policies.file_scope because this runtime cannot confine arbitrary shell writes."
    };
  }
  for (const output of stringList(step.outputs)) {
    if (resolveOutputPath(repoRoot, output) === undefined) {
      return {
        status: "failed",
        message: `Declared output ${JSON.stringify(output)} must be repo-relative and stay inside the repository.`
      };
    }
  }
  return undefined;
}

function validateTransformStep(step: AgentFlowWorkflowStep): string | undefined {
  const onFailure = mapping(step.on_failure);
  const retry = onFailure?.retry;
  if (retry !== undefined &&
      (!Number.isSafeInteger(retry) || Number(retry) < 0 || Number(retry) > MAX_AGENT_FLOW_COMMAND_RETRIES)) {
    return `Artifact transform on_failure.retry must be an integer from 0 through ${MAX_AGENT_FLOW_COMMAND_RETRIES}.`;
  }
  if (["continue", "ignore"].includes(normalizedFailureThen(onFailure) ?? "") && onFailure?.allowed !== true) {
    return "Artifact transform failures may continue or be ignored only when on_failure.allowed is true.";
  }
  if (onFailure !== undefined) {
    const then = normalizedFailureThen(onFailure);
    if ((then !== undefined && !["continue", "ignore", "fail", "pause"].includes(then))
        || ["goto", "return_to"].some((field) => onFailure[field] !== undefined)) {
      return "Artifact transform runtime supports only retry, recovery routes, and then: continue, ignore, fail, or pause.";
    }
  }
  return undefined;
}

function validateSessionRequestStep(step: AgentFlowWorkflowStep): string | undefined {
  if (typeof step.session !== "string" || step.session.trim().length === 0
      || typeof step.prompt !== "string" || step.prompt.trim().length === 0
      || !nonEmptyStringArray(step.inputs) || !nonEmptyStringArray(step.outputs)) {
    return "Session request requires a non-empty session, prompt, inputs list, and outputs list.";
  }
  const onFailure = mapping(step.on_failure);
  const retry = onFailure?.retry;
  if (retry !== undefined &&
      (!Number.isSafeInteger(retry) || Number(retry) < 0 || Number(retry) > MAX_AGENT_FLOW_COMMAND_RETRIES)) {
    return `Session request on_failure.retry must be an integer from 0 through ${MAX_AGENT_FLOW_COMMAND_RETRIES}.`;
  }
  if (["continue", "ignore"].includes(normalizedFailureThen(onFailure) ?? "") && onFailure?.allowed !== true) {
    return "Session request failures may continue or be ignored only when on_failure.allowed is true.";
  }
  if (onFailure !== undefined) {
    const then = normalizedFailureThen(onFailure);
    if ((then !== undefined && !["continue", "ignore", "fail", "pause"].includes(then))
        || ["goto", "return_to"].some((field) => onFailure[field] !== undefined)) {
      return "Session request runtime supports only retry, recovery routes, and then: continue, ignore, fail, or pause.";
    }
  }
  return undefined;
}

function validateApprovalStep(workflow: AgentFlowWorkflow, step: AgentFlowWorkflowStep): string | undefined {
  if (typeof step.reviewer !== "string" || step.reviewer.trim().length === 0
      || !nonEmptyStringArray(step.artifacts)) {
    return "Approval requires a non-empty reviewer and artifacts list.";
  }
  if (!(step.artifacts as string[]).every(isNormalizedStaticAgentFlowArtifactPath)) {
    return "Approval artifacts must use normalized static artifact paths.";
  }
  if (step.output !== undefined
      && (typeof step.output !== "string" || !isNormalizedStaticAgentFlowArtifactPath(step.output))) {
    return "Approval output must use a normalized static artifact path.";
  }
  const approvalOutput = typeof step.output === "string"
    ? step.output
    : defaultAgentFlowApprovalOutputPath(typeof step.id === "string" ? step.id.trim() : "");
  if ((step.artifacts as string[]).includes(approvalOutput)) {
    return `Approval output must not overwrite evidence artifact ${approvalOutput}.`;
  }
  if (step.outputs !== undefined) return "Approval steps do not support plural outputs.";
  if (step.message !== undefined && (typeof step.message !== "string" || step.message.trim().length === 0)) {
    return "Approval message must be non-empty text when declared.";
  }
  const reviewerId = step.reviewer.trim();
  if (reviewerId !== "human" && (step.artifacts as string[]).length > MAX_AGENT_FLOW_SESSION_INPUTS) {
    return `Session approvals may declare at most ${MAX_AGENT_FLOW_SESSION_INPUTS} artifact inputs.`;
  }
  if (reviewerId === "human") {
    return step.on_failure === undefined
      ? undefined
      : "Human approval steps do not support on_failure policies in this runtime phase.";
  }
  const failurePolicyError = validateSessionLikeFailurePolicy(step, "Approval");
  if (failurePolicyError !== undefined) return failurePolicyError;
  const reviewer = mapping(workflow.sessions?.[reviewerId]);
  if (reviewer === undefined) return `Approval references undeclared reviewer session ${reviewerId}.`;
  if (mapping(reviewer.authority)?.can_approve !== true) {
    return "Approval reviewers must explicitly declare can_approve authority.";
  }
  return undefined;
}

function validateDecisionRecordStep(step: AgentFlowWorkflowStep): string | undefined {
  if (typeof step.owner !== "string" || step.owner.trim().length === 0
      || typeof step.topic !== "string" || step.topic.trim().length === 0
      || !nonEmptyStringArray(step.artifacts)) {
    return "Decision record requires a non-empty owner, topic, and artifacts list.";
  }
  if (step.outputs !== undefined) return "Decision record steps do not support plural outputs.";
  if (step.on_failure !== undefined) {
    return "Decision record steps do not support on_failure policies in this runtime phase.";
  }
  if (step.rationale_summary !== undefined
      && (typeof step.rationale_summary !== "string" || step.rationale_summary.trim().length === 0)) {
    return "Decision record rationale_summary must be non-empty text when declared.";
  }
  const decisionOutput = typeof step.output === "string"
    ? step.output
    : defaultAgentFlowDecisionRecordPath(typeof step.id === "string" ? step.id.trim() : "");
  if (Array.isArray(step.artifacts) && step.artifacts.includes(decisionOutput)) {
    return `Decision record output must not overwrite evidence artifact ${decisionOutput}.`;
  }
  return undefined;
}

function validateSessionLikeFailurePolicy(step: AgentFlowWorkflowStep, label: string): string | undefined {
  if (step.on_failure !== undefined && mapping(step.on_failure) === undefined) {
    return `${label} on_failure must be a mapping.`;
  }
  const onFailure = mapping(step.on_failure);
  const retry = onFailure?.retry;
  if (retry !== undefined &&
      (!Number.isSafeInteger(retry) || Number(retry) < 0 || Number(retry) > MAX_AGENT_FLOW_COMMAND_RETRIES)) {
    return `${label} on_failure.retry must be an integer from 0 through ${MAX_AGENT_FLOW_COMMAND_RETRIES}.`;
  }
  if (["continue", "ignore"].includes(normalizedFailureThen(onFailure) ?? "") && onFailure?.allowed !== true) {
    return `${label} failures may continue or be ignored only when on_failure.allowed is true.`;
  }
  if (onFailure !== undefined) {
    const then = normalizedFailureThen(onFailure);
    if ((then !== undefined && !["continue", "ignore", "fail", "pause"].includes(then))
        || ["goto", "return_to"].some((field) => onFailure[field] !== undefined)) {
      return `${label} runtime supports only retry, recovery routes, and then: continue, ignore, fail, or pause.`;
    }
  }
  return undefined;
}

function validateReviewStep(workflow: AgentFlowWorkflow, step: AgentFlowWorkflowStep): string | undefined {
  if (typeof step.reviewer !== "string" || step.reviewer.trim().length === 0
      || typeof step.subject !== "string" || step.subject.trim().length === 0
      || !nonEmptyStringArray(step.artifacts) || !nonEmptyStringArray(step.outputs)) {
    return "Review requires a non-empty reviewer, subject, artifacts list, and outputs list.";
  }
  if ((step.outputs as string[]).some((output) => !output.trim().endsWith(".json"))) {
    return "Review outputs must use .json artifact paths.";
  }
  const reviewer = mapping(workflow.sessions?.[(step.reviewer as string).trim()]);
  const authority = mapping(reviewer?.authority);
  if (authority?.can_request_changes !== true || authority.can_approve !== true) {
    return "Reviewers must explicitly declare can_request_changes and can_approve authority.";
  }
  if (step.on_failure !== undefined) {
    return "Review steps do not support on_failure policies in this runtime phase.";
  }
  return undefined;
}

function validateCollaborationExchangeStep(
  workflow: AgentFlowWorkflow,
  step: AgentFlowWorkflowStep
): string | undefined {
  const kind = normalizedTarget(step.type);
  const label = kind === "challenge" ? "Challenge" : "Consult";
  if (!nonEmptyStringArray(step.artifacts)
      || typeof step.from !== "string" || step.from.trim().length === 0
      || typeof step.to !== "string" || step.to.trim().length === 0
      || typeof step.question !== "string" || step.question.trim().length === 0
      || typeof step.output !== "string" || step.output.trim().length === 0) {
    return `${label} requires non-empty from and to sessions, one bounded question, an artifacts list, and one output.`;
  }
  if (!isNormalizedStaticAgentFlowArtifactPath(step.output) || !step.output.endsWith(".json")) {
    return `${label} output must use a normalized static .json artifact path.`;
  }
  if (!(step.artifacts as string[]).every(isNormalizedStaticAgentFlowArtifactPath)) {
    return `${label} artifacts must use normalized static artifact paths.`;
  }
  if (kind === "consult" && typeof step.blocking !== "boolean") {
    return "Consult requires an explicit boolean blocking flag.";
  }
  const question = step.question.trim();
  const questionMarks = [...question].filter((character) => character === "?").length;
  const wordCount = question.split(/\s+/).filter(Boolean).length;
  if (Buffer.byteLength(question, "utf8") > MAX_AGENT_FLOW_COLLABORATION_QUESTION_BYTES
      || question.includes("{{") || question.includes("}}") || /[\r\n]/.test(question)
      || questionMarks !== 1 || !question.endsWith("?") || wordCount < 3) {
    return `${label} question must be one static, specific question ending in a single question mark within the ${MAX_AGENT_FLOW_COLLABORATION_QUESTION_BYTES}-byte limit.`;
  }
  const source = mapping(workflow.sessions?.[step.from.trim()]);
  if (source === undefined) return `${label} references undeclared source session ${step.from.trim()}.`;
  const target = mapping(workflow.sessions?.[step.to.trim()]);
  if (target === undefined) return `${label} references undeclared target session ${step.to.trim()}.`;
  const authority = mapping(target.authority);
  const hasEnabledAuthority = authority !== undefined && Object.values(authority).some((enabled) => enabled === true);
  const hasEffectiveAdvisoryAuthority = target.authority === undefined || authority?.can_advise === true
    || authority !== undefined && authority.can_advise !== false && !hasEnabledAuthority;
  if (kind === "consult" && step.blocking === false && !hasEffectiveAdvisoryAuthority) {
    return "Advisory consult targets must have effective can_advise authority.";
  }
  if (kind === "consult" && step.blocking === true && authority?.can_block !== true) {
    return "Blocking consult targets must explicitly declare can_block authority.";
  }
  if (step.on_failure !== undefined) return `${label} steps do not support on_failure policies in this runtime phase.`;
  return undefined;
}

function validateMcpCallStep(step: AgentFlowWorkflowStep): string | undefined {
  if (typeof step.server !== "string" || step.server.trim().length === 0
      || typeof step.tool !== "string" || step.tool.trim().length === 0
      || mapping(step.arguments) === undefined || !nonEmptyStringArray(step.outputs)) {
    return "MCP call requires a non-empty server, tool, arguments mapping, and outputs list.";
  }
  if ([step.server, step.tool].some((value) => value.includes("{{") || value.includes("}}"))) {
    return "MCP call server and tool must be static non-empty names.";
  }
  try {
    validateAgentFlowMcpArgumentExpressions(step.arguments, typeof step.id === "string" ? step.id.trim() : "(unnamed)");
    validateAgentFlowMcpOutputPaths(step.outputs, typeof step.id === "string" ? step.id.trim() : "(unnamed)");
  } catch (error) {
    return error instanceof Error ? error.message : String(error);
  }
  const onFailure = mapping(step.on_failure);
  const retry = onFailure?.retry;
  if (retry !== undefined &&
      (!Number.isSafeInteger(retry) || Number(retry) < 0 || Number(retry) > MAX_AGENT_FLOW_COMMAND_RETRIES)) {
    return `MCP call on_failure.retry must be an integer from 0 through ${MAX_AGENT_FLOW_COMMAND_RETRIES}.`;
  }
  if (["continue", "ignore"].includes(normalizedFailureThen(onFailure) ?? "") && onFailure?.allowed !== true) {
    return "MCP call failures may continue or be ignored only when on_failure.allowed is true.";
  }
  if (onFailure !== undefined) {
    const then = normalizedFailureThen(onFailure);
    if ((then !== undefined && !["continue", "ignore", "fail", "pause"].includes(then))
        || ["goto", "return_to"].some((field) => onFailure[field] !== undefined)) {
      return "MCP call runtime supports only retry, recovery routes, and then: continue, ignore, fail, or pause.";
    }
  }
  return undefined;
}

function nonEmptyStringArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.length > 0
    && value.every((entry) => typeof entry === "string" && entry.trim().length > 0);
}

function policyFailure(status: "pause" | "fail", message: string): CommandPreflightFailure {
  return { status: status === "pause" ? "paused" : "failed", message };
}

function persistPreflightFailure(
  store: AgentFlowRunStateStore,
  runId: string,
  stepId: string,
  command: string | null,
  attempt: number,
  message: string,
  outcome: Exclude<AgentFlowFailureOutcome, "retry" | "continue">
): void {
  const error = { attempt, exitCode: null, timedOut: false, message, outcome };
  const failureId = `command:${safeId(stepId)}:attempt-${attempt}:preflight`;
  const persisted = persistAgentFlowFailurePayload(store, {
    id: failureId,
    runId,
    stepId,
    stepType: "command",
    attempt,
    exitCode: null,
    command,
    summary: message,
    classification: "command_policy",
    retryable: false,
    outcome,
    indexPayload: error
  });
  const indexedError = { ...persisted.indexPayload, ...failureReference(persisted) };
  store.upsertStep({ runId, stepId, attempt, status: "failed", error: indexedError });
  store.appendRunEvent(runId, { type: "step.rejected", stepId, payload: indexedError });
}

function persistTransformPreflightFailure(
  store: AgentFlowRunStateStore,
  runId: string,
  stepId: string,
  attempt: number,
  message: string,
  outcome: Exclude<AgentFlowFailureOutcome, "retry" | "continue">
): void {
  const error = { attempt, message, outcome };
  const failureId = `artifact-transform:${safeId(stepId)}:attempt-${attempt}:preflight`;
  const persisted = persistAgentFlowFailurePayload(store, {
    id: failureId,
    runId,
    stepId,
    stepType: "artifact_transform",
    attempt,
    summary: message,
    classification: "artifact_transform_policy",
    retryable: false,
    outcome,
    indexPayload: error
  });
  const indexedError = { ...persisted.indexPayload, ...failureReference(persisted) };
  store.upsertStep({ runId, stepId, attempt, status: "failed", error: indexedError });
  store.appendRunEvent(runId, { type: "step.rejected", stepId, payload: indexedError });
}

function persistSessionRequestFailure(
  store: AgentFlowRunStateStore,
  runId: string,
  stepId: string,
  sessionId: string | undefined,
  message: string,
  retryable: boolean,
  outcome: AgentFlowFailureOutcome,
  rejected: boolean,
  attempt = 1,
  details?: { limit: string; message: string; outcome: "pause" | "fail" }
): void {
  const error = { attempt, message, outcome, ...details };
  const failureId = `session-request:${safeId(stepId)}:attempt-${attempt}`;
  const persisted = persistAgentFlowFailurePayload(store, {
    id: failureId,
    runId,
    stepId,
    ...(sessionId === undefined ? {} : { sessionId }),
    stepType: "session_request",
    attempt,
    summary: message,
    classification: rejected ? "session_request_policy" : "session_request_failure",
    retryable,
    outcome,
    indexPayload: error
  });
  const indexedError = { ...persisted.indexPayload, ...failureReference(persisted) };
  store.upsertStep({
    runId,
    stepId,
    attempt,
    ...(sessionId === undefined ? {} : { sessionId }),
    status: "failed",
    error: indexedError
  });
  store.appendRunEvent(runId, {
    type: rejected ? "step.rejected" : "step.failed",
    stepId,
    payload: indexedError
  });
}

function persistApprovalInteractionFailure(
  store: AgentFlowRunStateStore,
  runId: string,
  stepId: string,
  message: string,
  attempt: number
): void {
  const error = { attempt, message, outcome: "fail" as const };
  const persisted = persistAgentFlowFailurePayload(store, {
    id: `approval:${safeId(stepId)}:attempt-${attempt}:preflight`,
    runId,
    stepId,
    sessionId: "human",
    stepType: "approval",
    attempt,
    summary: message,
    classification: "approval_failure",
    retryable: false,
    outcome: "fail",
    indexPayload: error
  });
  const indexedError = { ...persisted.indexPayload, ...failureReference(persisted) };
  store.upsertStep({ runId, stepId, attempt, sessionId: "human", status: "failed", error: indexedError });
  store.appendRunEvent(runId, { type: "step.failed", stepId, payload: indexedError });
}

function resolveOutputPath(repoRoot: string, declaredPath: string): string | undefined {
  if (declaredPath.trim() !== declaredPath || declaredPath.length === 0 || declaredPath.includes("\\")) return undefined;
  const normalized = path.posix.normalize(declaredPath);
  if (path.posix.isAbsolute(normalized) || normalized === ".." || normalized.startsWith("../")) return undefined;
  const candidate = path.resolve(repoRoot, ...normalized.split("/"));
  if (!inside(repoRoot, candidate)) return undefined;
  let existingAncestor = candidate;
  while (!fs.existsSync(existingAncestor)) {
    const parent = path.dirname(existingAncestor);
    if (parent === existingAncestor) return undefined;
    existingAncestor = parent;
  }
  try {
    return inside(repoRoot, fs.realpathSync(existingAncestor)) ? candidate : undefined;
  } catch {
    return undefined;
  }
}

function inside(root: string, candidate: string): boolean {
  const relative = path.relative(root, candidate);
  return relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative);
}

function timeoutMilliseconds(step: AgentFlowWorkflowStep): number | undefined {
  return typeof step.timeout_seconds === "number" ? Math.ceil(step.timeout_seconds * 1_000) : undefined;
}

function failureRetries(step: AgentFlowWorkflowStep): number {
  const retry = mapping(step.on_failure)?.retry;
  return typeof retry === "number" && Number.isSafeInteger(retry) && retry > 0 ? retry : 0;
}

function failureThen(step: AgentFlowWorkflowStep): string | undefined {
  return normalizedFailureThen(mapping(step.on_failure));
}

function normalizedFailureThen(onFailure: AgentFlowYamlMapping | undefined): string | undefined {
  const then = onFailure?.then;
  return typeof then === "string" && then.trim().length > 0 ? then.trim() : undefined;
}

function failureContinues(step: AgentFlowWorkflowStep): boolean {
  return ["continue", "ignore"].includes(failureThen(step) ?? "");
}

function failureOutcome(step: AgentFlowWorkflowStep, retryable: boolean): AgentFlowFailureOutcome {
  if (retryable) return "retry";
  if (failureContinues(step)) return "continue";
  return failureStatus(step) === "failed" ? "fail" : "pause";
}

function failureStatus(step: AgentFlowWorkflowStep): "failed" | "paused" {
  return failureThen(step) === "fail" ? "failed" : "paused";
}

function commandOutput(result: CommandAttemptResult, attempt: number): Record<string, AgentFlowRunStateValue> {
  return { attempt, exitCode: result.exitCode, signal: result.signal, timedOut: result.timedOut };
}

function commandError(result: CommandAttemptResult, attempt: number): Record<string, AgentFlowRunStateValue> & { message: string } {
  return { ...commandOutput(result, attempt), message: result.message ?? failureMessage(result) };
}

function failureMessage(result: CommandAttemptResult): string {
  if (result.timedOut) return "Command exceeded timeout_seconds and was terminated.";
  if (result.message !== undefined) return result.message;
  if (result.signal !== null) return `Command terminated by signal ${result.signal}.`;
  return `Command exited with status ${String(result.exitCode)}.`;
}

function failureReference(
  result: PersistAgentFlowFailurePayloadResult
): Record<string, AgentFlowRunStateValue> {
  return {
    failurePath: result.path,
    ...(result.persistenceError === null
      ? {}
      : { failurePayloadPersistenceError: result.persistenceError })
  };
}

function requiredStepId(step: AgentFlowWorkflowStep): string {
  if (typeof step.id !== "string" || step.id.trim().length === 0) throw new Error("Executable workflow steps require an ID.");
  return step.id.trim();
}

function safeId(value: string): string {
  const slug = value.trim().replace(/[^A-Za-z0-9._-]+/g, "-").replace(/^-+|-+$/g, "") || "step";
  return `${slug}-${createHash("sha256").update(value).digest("hex").slice(0, 8)}`;
}

function stringList(value: unknown): string[] {
  return Array.isArray(value) ? value.filter((entry): entry is string => typeof entry === "string") : [];
}

function mapping(value: unknown): AgentFlowYamlMapping | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value) && !(value instanceof Uint8Array)
    ? value as AgentFlowYamlMapping
    : undefined;
}

function isWorkflowStep(value: unknown): value is AgentFlowWorkflowStep {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
