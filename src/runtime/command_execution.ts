import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import { isDeepStrictEqual } from "node:util";
import {
  AgentFlowRunStateError,
  normalizeAgentFlowArtifactPath,
  type AgentFlowRunStateValue,
  type AgentFlowRunStateStore,
  type AgentFlowRunStopStatus,
  type AgentFlowRunStatus,
  type AgentFlowFailureOutcome
} from "./run_state";
import type { AgentFlowWorkflow, AgentFlowWorkflowStep, AgentFlowYamlMapping } from "./workflow";
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
  AgentFlowSessionRequestInterruptedError,
  createAgentFlowSessionProviderRegistry,
  executeAgentFlowSessionRequest
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
import { selectAgentFlowConditionTarget } from "./condition";
import { assertAgentFlowSuccessTargetsAreUnambiguous } from "./success_routing";
import {
  createAgentFlowNotificationRegistry,
  deliverAgentFlowNotifications,
  validateAgentFlowNotifications,
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
  persistAgentFlowFailurePayload,
  type PersistAgentFlowFailurePayloadResult
} from "./failure_payload";

const MAX_CAPTURE_BYTES = 10 * 1024 * 1024;

export interface AgentFlowCommandPipelineResult {
  status: Extract<AgentFlowRunStatus, "completed" | "failed" | "paused" | "cancelled">;
  completedSteps: string[];
  failedStep?: string;
  failureOutcome?: Exclude<AgentFlowFailureOutcome, "retry" | "continue">;
  exitCode?: number | null;
  timedOut?: boolean;
  message?: string;
}

export type AgentFlowPipelineResumeInput =
  | { outcome: string; decidedBy?: string }
  | { answer: AgentFlowRunStateValue };

interface AgentFlowPipelineWaitingState {
  kind: "manual_gate" | "input_request";
  stepId: string;
  attempt: number;
  reason: "manual_approval" | "missing_input";
  prompt: string;
  validOutcomes: string[];
  saveAs?: string;
  approvalId?: string;
  completedSteps: string[];
  routing: SerializedSuccessfulRoutingBudget;
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
  notifications: AgentFlowNotificationRegistry = createAgentFlowNotificationRegistry()
): Promise<AgentFlowCommandPipelineResult> {
  return runAgentFlowCommandPipeline(
    store,
    runId,
    workflow,
    undefined,
    transforms,
    sessionProviders,
    mcpCalls,
    notifications
  );
}

export async function resumeAgentFlowCommandPipeline(
  store: AgentFlowRunStateStore,
  runId: string,
  workflow: AgentFlowWorkflow,
  response: AgentFlowPipelineResumeInput,
  transforms: AgentFlowArtifactTransformRegistry = createAgentFlowArtifactTransformRegistry(),
  sessionProviders: AgentFlowSessionProviderRegistry = createAgentFlowSessionProviderRegistry(),
  mcpCalls: AgentFlowMcpCallRegistry = createAgentFlowMcpCallRegistry(),
  notifications: AgentFlowNotificationRegistry = createAgentFlowNotificationRegistry()
): Promise<AgentFlowCommandPipelineResult> {
  return runAgentFlowCommandPipeline(
    store,
    runId,
    workflow,
    response,
    transforms,
    sessionProviders,
    mcpCalls,
    notifications
  );
}

async function runAgentFlowCommandPipeline(
  store: AgentFlowRunStateStore,
  runId: string,
  workflow: AgentFlowWorkflow,
  resumeInput: AgentFlowPipelineResumeInput | undefined,
  transforms: AgentFlowArtifactTransformRegistry,
  sessionProviders: AgentFlowSessionProviderRegistry,
  mcpCalls: AgentFlowMcpCallRegistry,
  notifications: AgentFlowNotificationRegistry
): Promise<AgentFlowCommandPipelineResult> {
  const existing = store.getRun(runId);
  if (existing === null) throw new Error(`Agent Flow run ${runId} was not found.`);
  if (!isDeepStrictEqual(existing.context.workflow, workflow)) {
    throw new AgentFlowRunStateError(
      `Agent Flow run ${runId} cannot execute a workflow that differs from its persisted definition.`,
      "AGENT_FLOW_RUN_COLLISION"
    );
  }
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
  validateRuntimeInteractionSteps(workflow.steps, workflow.style === "pipeline");
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
  const stepLocations = collectRuntimeStepLocations(workflow.steps);
  let completedSteps: string[];
  let routingBudget: SuccessfulRoutingBudget;
  let currentSteps = workflow.steps;
  let stepIndex = 0;

  if (resumeInput === undefined) {
    store.transitionRunWithEvent(runId, {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: { status: "running" } }
    });
    completedSteps = [];
    routingBudget = createSuccessfulRoutingBudget(workflow, notifications);
  } else {
    const resumed = resumeWaitingStep(
      store,
      runId,
      workflow,
      existing.context,
      resumeInput,
      stepLocations,
      notifications
    );
    if ("result" in resumed) return resumed.result;
    completedSteps = resumed.completedSteps;
    routingBudget = resumed.routingBudget;
    currentSteps = resumed.steps;
    stepIndex = resumed.nextIndex;
  }

  while (stepIndex < currentSteps.length) {
    const step = currentSteps[stepIndex]!;
    const stoppedBeforeStep = stoppedPipelineResult(store, runId, completedSteps);
    if (stoppedBeforeStep !== undefined) return stoppedBeforeStep;
    const stepId = requiredStepId(step);
    routingBudget.visits.set(stepId, (routingBudget.visits.get(stepId) ?? 0) + 1);
    const stepType = normalizedTarget(step.type);
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
      for (let attemptIndex = 1; attemptIndex <= retries + 1; attemptIndex += 1) {
        const attempt = attemptIndex === 1 ? firstAttempt : allocateStepAttempt(routingBudget, stepId);
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
          const result = await executeAgentFlowMcpCall(store, runId, workflow, step, mcpCalls, {
            attempt,
            stopStatus: () => activeStopStatus(store, runId),
            beforePublish: () => {
              const status = activeStopStatus(store, runId);
              if (status !== undefined) throw new AgentFlowMcpCallInterruptedError(status);
            }
          });
          const stoppedAfterPublish = activeStopStatus(store, runId);
          if (stoppedAfterPublish !== undefined) {
            persistMcpCallInterruption(store, runId, stepId, attempt, stoppedAfterPublish);
            return interruptedPipelineResult(store, runId, completedSteps, stoppedAfterPublish);
          }
          const output = {
            attempt,
            server: result.server,
            tool: result.tool,
            requestArtifact: result.requestArtifact.declaredPath,
            outputs: result.outputArtifacts.map((artifact) => artifact.declaredPath)
          };
          store.upsertStep({ runId, stepId, attempt, status: "completed", output });
          store.appendRunEvent(runId, { type: "step.completed", stepId, payload: output });
          completedSteps.push(stepId);
          failure = undefined;
          break;
        } catch (error) {
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
        const routed = routeAfterSuccessfulStep(store, runId, completedSteps, stepId, step, currentSteps, stepIndex, stepLocations, routingBudget);
        if ("result" in routed) return routed.result;
        currentSteps = routed.steps;
        stepIndex = routed.nextIndex;
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
      for (let attemptIndex = 1; attemptIndex <= retries + 1; attemptIndex += 1) {
        const attempt = attemptIndex === 1 ? firstAttempt : allocateStepAttempt(routingBudget, stepId);
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
        const routed = routeAfterSuccessfulStep(store, runId, completedSteps, stepId, step, currentSteps, stepIndex, stepLocations, routingBudget);
        if ("result" in routed) return routed.result;
        currentSteps = routed.steps;
        stepIndex = routed.nextIndex;
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
    if (stepType === "session_request") {
      const firstAttempt = allocateStepAttempt(routingBudget, stepId);
      if (firstAttempt === undefined) return stepAttemptLimitResult(store, runId, completedSteps, stepId, routingBudget);
      const preflightError = validateSessionRequestStep(step);
      if (preflightError !== undefined) {
        const sessionId = typeof step.session === "string" && step.session.trim().length > 0
          ? step.session.trim()
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
      for (let attemptIndex = 1; attemptIndex <= retries + 1; attemptIndex += 1) {
        const attempt = attemptIndex === 1 ? firstAttempt : allocateStepAttempt(routingBudget, stepId);
        if (attempt === undefined) return stepAttemptLimitResult(store, runId, completedSteps, stepId, routingBudget);
        const stopped = activeStopStatus(store, runId);
        if (stopped !== undefined) return stoppedPipelineResult(store, runId, completedSteps)!;
        store.updateRun(runId, { currentStepId: stepId, error: null });
        const sessionId = (step.session as string).trim();
        const input = {
          attempt,
          session: sessionId,
          prompt: step.prompt as string,
          inputs: step.inputs as AgentFlowRunStateValue,
          outputs: step.outputs as AgentFlowRunStateValue
        };
        store.upsertStep({ runId, stepId, attempt, sessionId, status: "running", input });
        store.appendRunEvent(runId, { type: "step.started", stepId, payload: input });
        try {
          const result = await executeAgentFlowSessionRequest(store, runId, workflow, step, sessionProviders, {
            attempt,
            stopStatus: () => activeStopStatus(store, runId),
            beforePublish: () => {
              const status = activeStopStatus(store, runId);
              if (status !== undefined) throw new AgentFlowSessionRequestInterruptedError(status);
            }
          });
          const output = {
            attempt,
            session: result.sessionId,
            provider: result.provider,
            requestArtifact: result.requestArtifact.declaredPath,
            outputs: result.outputArtifacts.map((artifact) => artifact.declaredPath),
            externalSessionId: result.externalSessionId ?? null
          };
          store.upsertStep({ runId, stepId, attempt, sessionId, status: "completed", output });
          store.appendRunEvent(runId, { type: "step.completed", stepId, payload: output });
          completedSteps.push(stepId);
          failure = undefined;
          break;
        } catch (error) {
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
            persistSessionRequestFailure(store, runId, stepId, sessionId, failure, false, outcome, true, attempt);
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
        const routed = routeAfterSuccessfulStep(store, runId, completedSteps, stepId, step, currentSteps, stepIndex, stepLocations, routingBudget);
        if ("result" in routed) return routed.result;
        currentSteps = routed.steps;
        stepIndex = routed.nextIndex;
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
        const failureId = `condition:${safeId(stepId)}:evaluation`;
        const failure = { attempt, message, outcome: "fail" as const };
        const persisted = persistAgentFlowFailurePayload(store, {
          id: failureId,
          runId,
          stepId,
          stepType: "condition",
          attempt,
          summary: message,
          classification: "condition_evaluation",
          retryable: false,
          outcome: "fail",
          indexPayload: failure
        });
        const indexedFailure = { ...persisted.indexPayload, ...failureReference(persisted) };
        store.upsertStep({ runId, stepId, attempt, status: "failed", error: indexedFailure });
        store.appendRunEvent(runId, { type: "step.failed", stepId, payload: indexedFailure });
        return finishFailure(store, runId, completedSteps, stepId, {
          exitCode: null,
          timedOut: false,
          message
        }, "failed", routingBudget.terminalEffects);
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
    for (let attemptIndex = 1; attemptIndex <= retries + 1; attemptIndex += 1) {
      const attempt = attemptIndex === 1 ? firstAttempt : allocateStepAttempt(routingBudget, stepId);
      if (attempt === undefined) return stepAttemptLimitResult(store, runId, completedSteps, stepId, routingBudget);
      store.updateRun(runId, { currentStepId: stepId, error: null });
      store.upsertStep({ runId, stepId, attempt, status: "running", input: { command: step.command as string } });
      store.appendRunEvent(runId, { type: "step.started", stepId, payload: { attempt, command: step.command as string } });

      lastResult = await runCommand(
        store.repoRoot,
        step.command as string,
        timeoutMilliseconds(step),
        () => activeStopStatus(store, runId)
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
  stepAttemptLimits: Map<string, number>;
  visits: Map<string, number>;
  recoveryCycles: Map<string, number>;
  attempts: Map<string, number>;
}

interface AgentFlowPipelineTerminalEffects {
  workflow: AgentFlowWorkflow;
  notifications: AgentFlowNotificationRegistry;
}

interface SerializedSuccessfulRoutingBudget {
  maxRecoveryCycles?: number;
  stepAttemptLimits: Record<string, number>;
  visits: Record<string, number>;
  recoveryCycles: Record<string, number>;
  attempts: Record<string, number>;
}

type ResumedWaitingStep =
  | { steps: AgentFlowWorkflowStep[]; nextIndex: number; completedSteps: string[]; routingBudget: SuccessfulRoutingBudget }
  | { result: AgentFlowCommandPipelineResult };

function pauseForInteraction(
  store: AgentFlowRunStateStore,
  runId: string,
  step: AgentFlowWorkflowStep,
  kind: "manual_gate" | "input_request",
  attempt: number,
  completedSteps: string[],
  routingBudget: SuccessfulRoutingBudget
): AgentFlowCommandPipelineResult {
  const stepId = requiredStepId(step);
  const run = store.getRun(runId)!;
  const prompt = resolveInteractionPrompt(
    kind === "manual_gate" ? step.message : step.question,
    run.inputs,
    `${kind === "manual_gate" ? "Manual gate message" : "Input request question"} for step ${stepId}`
  );
  const validOutcomes = kind === "manual_gate" ? normalizedStringList(step.options) : [];
  const saveAs = kind === "input_request"
    ? requiredStaticString(step.save_as, `Input request artifact for step ${stepId}`)
    : undefined;
  const approvalId = kind === "manual_gate" ? `manual-gate:${safeId(stepId)}:attempt-${attempt}` : undefined;
  const waiting: AgentFlowPipelineWaitingState = {
    kind,
    stepId,
    attempt,
    reason: kind === "manual_gate" ? "manual_approval" : "missing_input",
    prompt,
    validOutcomes,
    ...(saveAs === undefined ? {} : { saveAs }),
    ...(approvalId === undefined ? {} : { approvalId }),
    completedSteps: [...completedSteps],
    routing: serializeRoutingBudget(routingBudget)
  };
  const input: Record<string, AgentFlowRunStateValue> = kind === "manual_gate"
    ? { attempt, type: kind, message: prompt, options: validOutcomes }
    : { attempt, type: kind, question: prompt, saveAs: saveAs! };

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
      context: { message: prompt, options: validOutcomes }
    });
  }
  const resultMessage = kind === "manual_gate"
    ? `Manual gate ${stepId} is waiting for one of: ${validOutcomes.join(", ")}.`
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
    onFinalStatus: (status, message) => {
      if (status !== "failed") return;
      const failureMessage = message ?? "Required paused notification failed.";
      const error = {
        attempt,
        message: failureMessage,
        outcome: "fail"
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
    }
  });
  return {
    status: finalized.status,
    completedSteps,
    message: finalized.message ?? resultMessage
  };
}

function resumeWaitingStep(
  store: AgentFlowRunStateStore,
  runId: string,
  workflow: AgentFlowWorkflow,
  context: Record<string, AgentFlowRunStateValue>,
  response: AgentFlowPipelineResumeInput,
  stepLocations: Map<string, RuntimeStepLocation>,
  notifications: AgentFlowNotificationRegistry
): ResumedWaitingStep {
  const waiting = parseWaitingState(context.waiting);
  const location = stepLocations.get(waiting.stepId);
  if (location === undefined) {
    throw new AgentFlowRunStateError(
      `Agent Flow run ${runId} cannot resume because waiting step ${waiting.stepId} is not in its workflow.`,
      "AGENT_FLOW_RESUME_STATE"
    );
  }
  const step = location.steps[location.index]!;
  if (normalizedTarget(step.type) !== waiting.kind) {
    throw new AgentFlowRunStateError(
      `Agent Flow run ${runId} waiting state does not match workflow step ${waiting.stepId}.`,
      "AGENT_FLOW_RESUME_STATE"
    );
  }

  const routingBudget = deserializeRoutingBudget(waiting.routing, workflow, notifications);
  const completedSteps = [...waiting.completedSteps];
  let selectedTarget: string | undefined;
  let output: Record<string, AgentFlowRunStateValue>;

  if (waiting.kind === "manual_gate") {
    if (!("outcome" in response)) {
      throw new AgentFlowRunStateError(
        `Manual gate ${waiting.stepId} requires an explicit --outcome value.`,
        "AGENT_FLOW_GATE_OUTCOME_REQUIRED"
      );
    }
    const outcome = response.outcome.trim();
    if (!waiting.validOutcomes.includes(outcome)) {
      throw new AgentFlowRunStateError(
        `Manual gate ${waiting.stepId} rejected outcome ${JSON.stringify(outcome)}; valid outcomes are: ${waiting.validOutcomes.join(", ")}.`,
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
          message: `Manual gate ${waiting.stepId} remains paused.`
        }
      };
    }
    if (response.decidedBy !== undefined && response.decidedBy.trim().length === 0) {
      throw new AgentFlowRunStateError(
        `Manual gate ${waiting.stepId} decision actor must be non-empty text.`,
        "AGENT_FLOW_INTERACTION_INVALID"
      );
    }

    store.transitionRunWithEvent(runId, {
      status: "running",
      allowedFrom: ["paused"],
      event: { type: "run.resume", stepId: waiting.stepId, payload: { outcome } }
    });
    const approvalStatus = outcome === "cancel" || outcome === "cancelled"
      ? "cancelled"
      : outcome === "reject"
        ? "rejected"
        : "approved";
    store.upsertApproval({
      id: waiting.approvalId!,
      runId,
      stepId: waiting.stepId,
      status: approvalStatus,
      ...(response.decidedBy === undefined ? {} : { decidedBy: response.decidedBy }),
      decision: outcome
    });
    output = { attempt: waiting.attempt, outcome };
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
      throw error;
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
  completedSteps.push(waiting.stepId);

  const routed = routeAfterSuccessfulStep(
    store,
    runId,
    completedSteps,
    waiting.stepId,
    step,
    location.steps,
    location.index,
    stepLocations,
    routingBudget,
    selectedTarget
  );
  if ("result" in routed) return routed;
  return { ...routed, completedSteps, routingBudget };
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
    stepAttemptLimits: Object.fromEntries([...budget.stepAttemptLimits].sort(([left], [right]) => left.localeCompare(right))),
    visits: Object.fromEntries([...budget.visits].sort(([left], [right]) => left.localeCompare(right))),
    recoveryCycles: Object.fromEntries([...budget.recoveryCycles].sort(([left], [right]) => left.localeCompare(right))),
    attempts: Object.fromEntries([...budget.attempts].sort(([left], [right]) => left.localeCompare(right)))
  };
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
    stepAttemptLimits: configured.stepAttemptLimits,
    visits: new Map(Object.entries(serialized.visits)),
    recoveryCycles: new Map(Object.entries(serialized.recoveryCycles)),
    attempts: new Map(Object.entries(serialized.attempts))
  };
}

function parseWaitingState(value: AgentFlowRunStateValue | undefined): AgentFlowPipelineWaitingState {
  const record = mapping(value);
  if (record === undefined) {
    throw new AgentFlowRunStateError(
      "Paused Agent Flow run does not have a persisted manual gate or input request.",
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
  if ((kind !== "manual_gate" && kind !== "input_request")
      || stepId === undefined
      || !Number.isSafeInteger(attempt)
      || (attempt as number) < 1
      || (reason !== "manual_approval" && reason !== "missing_input")
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
  if ((kind === "manual_gate" && (validOutcomes.length === 0 || approvalId === undefined))
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
    reason,
    prompt,
    validOutcomes,
    ...(saveAs === undefined ? {} : { saveAs }),
    ...(approvalId === undefined ? {} : { approvalId }),
    completedSteps,
    routing: serialized
  };
}

function parseSerializedRoutingBudget(value: AgentFlowYamlMapping): SerializedSuccessfulRoutingBudget {
  const parseMap = (
    field: "stepAttemptLimits" | "visits" | "recoveryCycles" | "attempts",
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
    "stepAttemptLimits" | "visits" | "recoveryCycles" | "attempts"
  > = {
    stepAttemptLimits: parseMap(
      "stepAttemptLimits",
      (entry) => typeof entry === "number" && Number.isFinite(entry) && entry > 0
    ),
    visits: parseMap("visits", (entry) => Number.isSafeInteger(entry) && (entry as number) >= 0),
    recoveryCycles: parseMap("recoveryCycles", (entry) => Number.isSafeInteger(entry) && (entry as number) >= 0),
    attempts: parseMap("attempts", (entry) => Number.isSafeInteger(entry) && (entry as number) >= 0)
  };
  return {
    ...parsed,
    ...(Number.isSafeInteger(value.maxRecoveryCycles) && (value.maxRecoveryCycles as number) > 0
      ? { maxRecoveryCycles: value.maxRecoveryCycles as number }
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
    for (const field of ["body", "steps", ...(type === "parallel" ? ["branches"] : [])]) {
      const nested = step[field];
      if (Array.isArray(nested)) {
        validateRuntimeInteractionSteps(nested.filter(isWorkflowStep), reserveFinalSummary);
      }
    }
  }
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
  const attemptLimit = budget.stepAttemptLimits.get(target);
  if (attemptLimit !== undefined && (budget.attempts.get(target) ?? 0) + 1 > attemptLimit) {
    return finishRoutingFailure(store, runId, completedSteps, stepId, target, {
      exitCode: null,
      timedOut: false,
      message: `Step ${stepId} cannot route to ${target} because limits.max_step_attempts allows ${attemptLimit} attempt(s).`
    }, budget);
  }
  if ((budget.visits.get(target) ?? 0) === 0) return undefined;
  if (budget.maxRecoveryCycles === undefined) {
    return finishRoutingFailure(store, runId, completedSteps, stepId, target, {
      exitCode: null,
      timedOut: false,
      message: `Step ${stepId} repeated route target ${target} without a positive executable limits.max_recovery_cycles bound.`
    }, budget);
  }
  const cycles = (budget.recoveryCycles.get(target) ?? 0) + 1;
  budget.recoveryCycles.set(target, cycles);
  if (cycles <= budget.maxRecoveryCycles) return undefined;
  return finishRoutingFailure(store, runId, completedSteps, stepId, target, {
    exitCode: null,
    timedOut: false,
    message: `Step ${stepId} exceeded limits.max_recovery_cycles ${budget.maxRecoveryCycles} while routing to ${target}.`
  }, budget);
}

function finishRoutingFailure(
  store: AgentFlowRunStateStore,
  runId: string,
  completedSteps: string[],
  stepId: string,
  target: string,
  failure: { exitCode: null; timedOut: false; message: string },
  budget: SuccessfulRoutingBudget
): AgentFlowCommandPipelineResult {
  const attempt = Math.max(1, budget.attempts.get(stepId) ?? 0);
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
    outcome: "pause",
    indexPayload: { attempt, target, message: failure.message, outcome: "pause" }
  });
  return finishFailure(
    store,
    runId,
    completedSteps,
    stepId,
    failure,
    "paused",
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
  notifications: AgentFlowNotificationRegistry
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
  return {
    terminalEffects: { workflow, notifications },
    maxRecoveryCycles,
    stepAttemptLimits,
    visits: new Map(),
    recoveryCycles: new Map(),
    attempts: new Map()
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
    outcome: "pause",
    indexPayload: { attempt, limit, message: failure.message, outcome: "pause" }
  });
  return finishFailure(store, runId, completedSteps, stepId, failure, "paused", budget.terminalEffects);
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
  onFinalStatus?: (
    status: Extract<AgentFlowRunStatus, "completed" | "failed" | "paused" | "cancelled">,
    message: string | undefined
  ) => void;
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
    store.updateRun(runId, {
      currentStepId: input.currentStepId,
      ...(input.output === undefined ? {} : { output: input.output }),
      ...(input.error === undefined ? {} : { error: input.error })
    });
    store.transitionRunWithEvent(runId, {
      status: input.intendedStatus,
      allowedFrom: ["running"],
      event: {
        type: `run.${input.intendedStatus}`,
        ...(input.eventStepId === undefined ? {} : { stepId: input.eventStepId }),
        payload: input.eventPayload
      }
    });
    return {
      status: input.intendedStatus,
      ...(input.message === undefined ? {} : { message: input.message })
    };
  }

  return withAgentFlowPipelineFinalization(
    store,
    runId,
    () => finalizationResultForCurrentRun(store, runId, input),
    () => finalizePipelineRunLocked(store, runId, terminalEffects, input)
  );
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

  if (summaryReady || input.intendedStatus === "failed") {
    const delivery = deliverAgentFlowNotifications(
      store,
      runId,
      terminalEffects.workflow,
      input.intendedStatus,
      terminalEffects.notifications
    );
    if (delivery.requiredFailure !== undefined && input.intendedStatus !== "failed") {
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

  if (input.intendedStatus !== "failed" && status === "failed") {
    deliverAgentFlowNotifications(
      store,
      runId,
      terminalEffects.workflow,
      "failed",
      terminalEffects.notifications
    );
  }

  input.onFinalStatus?.(status, message);
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

function runCommand(
  repoRoot: string,
  command: string,
  timeoutMs: number | undefined,
  stopStatus: () => AgentFlowRunStopStatus | undefined
): Promise<CommandAttemptResult> {
  return new Promise((resolve) => {
    const stdout: Buffer[] = [];
    const stderr: Buffer[] = [];
    let capturedBytes = 0;
    let timedOut = false;
    let settled = false;
    let killTimer: ReturnType<typeof setTimeout> | undefined;
    let terminationMessage: string | undefined;
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
      const status = stopStatus();
      if (status !== undefined) requestTermination(`Agent Flow run was ${status}; command was terminated.`, false);
    }, 25);

    const finish = (result: Omit<CommandAttemptResult, "stdout" | "stderr" | "timedOut">): void => {
      if (settled) return;
      settled = true;
      if (timer !== undefined) clearTimeout(timer);
      if (killTimer !== undefined) clearTimeout(killTimer);
      if (lifecycleTimer !== undefined) clearInterval(lifecycleTimer);
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
        || ["goto", "route_to", "on_remediated", "on_unresolved", "return_to"].some((field) => onFailure[field] !== undefined)) {
      return "Artifact transform runtime supports only retry and then: continue, ignore, fail, or pause.";
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
        || ["goto", "route_to", "on_remediated", "on_unresolved", "return_to"].some((field) => onFailure[field] !== undefined)) {
      return "Session request runtime supports only retry and then: continue, ignore, fail, or pause.";
    }
  }
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
        || ["goto", "route_to", "on_remediated", "on_unresolved", "return_to"].some((field) => onFailure[field] !== undefined)) {
      return "MCP call runtime supports only retry and then: continue, ignore, fail, or pause.";
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
  attempt = 1
): void {
  const error = { attempt, message, outcome };
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
