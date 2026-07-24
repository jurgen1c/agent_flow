import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import { isDeepStrictEqual } from "node:util";
import {
  AgentflowRunStateError,
  normalizeAgentflowArtifactPath,
  type AgentflowRunStateValue,
  type AgentflowRunStateStore,
  type AgentflowRunStatus,
  type AgentflowFailureOutcome
} from "./run_state";
import type { AgentflowWorkflow, AgentflowWorkflowStep, AgentflowYamlMapping } from "./workflow";
import { evaluateAgentflowPolicy } from "./policy";
import {
  AgentflowArtifactTransformRegistry,
  createAgentflowArtifactTransformRegistry,
  executeAgentflowArtifactTransform
} from "./artifact_transform";
import {
  agentflowCommandUnsafeReason,
  MAX_AGENTFLOW_COMMAND_RETRIES,
  MAX_AGENTFLOW_COMMAND_TIMEOUT_SECONDS
} from "./validation";
import {
  AgentflowSessionProviderRegistry,
  AgentflowSessionPolicyError,
  AgentflowSessionRequestInterruptedError,
  createAgentflowSessionProviderRegistry,
  executeAgentflowSessionRequest
} from "./session_request";
import {
  AgentflowMcpCallError,
  AgentflowMcpCallRegistry,
  AgentflowMcpCallInterruptedError,
  createAgentflowMcpCallRegistry,
  executeAgentflowMcpCall,
  validateAgentflowMcpArgumentExpressions,
  validateAgentflowMcpOutputPaths
} from "./mcp_call";
import { selectAgentflowConditionTarget } from "./condition";
import { assertAgentflowSuccessTargetsAreUnambiguous } from "./success_routing";

const MAX_CAPTURE_BYTES = 10 * 1024 * 1024;

export interface AgentflowCommandPipelineResult {
  status: Extract<AgentflowRunStatus, "completed" | "failed" | "paused" | "cancelled">;
  completedSteps: string[];
  failedStep?: string;
  failureOutcome?: Exclude<AgentflowFailureOutcome, "retry" | "continue">;
  exitCode?: number | null;
  timedOut?: boolean;
  message?: string;
}

export type AgentflowPipelineResumeInput =
  | { outcome: string; decidedBy?: string }
  | { answer: AgentflowRunStateValue };

interface AgentflowPipelineWaitingState {
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

export async function executeAgentflowCommandPipeline(
  store: AgentflowRunStateStore,
  runId: string,
  workflow: AgentflowWorkflow,
  transforms: AgentflowArtifactTransformRegistry = createAgentflowArtifactTransformRegistry(),
  sessionProviders: AgentflowSessionProviderRegistry = createAgentflowSessionProviderRegistry(),
  mcpCalls: AgentflowMcpCallRegistry = createAgentflowMcpCallRegistry()
): Promise<AgentflowCommandPipelineResult> {
  return runAgentflowCommandPipeline(store, runId, workflow, undefined, transforms, sessionProviders, mcpCalls);
}

export async function resumeAgentflowCommandPipeline(
  store: AgentflowRunStateStore,
  runId: string,
  workflow: AgentflowWorkflow,
  response: AgentflowPipelineResumeInput,
  transforms: AgentflowArtifactTransformRegistry = createAgentflowArtifactTransformRegistry(),
  sessionProviders: AgentflowSessionProviderRegistry = createAgentflowSessionProviderRegistry(),
  mcpCalls: AgentflowMcpCallRegistry = createAgentflowMcpCallRegistry()
): Promise<AgentflowCommandPipelineResult> {
  return runAgentflowCommandPipeline(store, runId, workflow, response, transforms, sessionProviders, mcpCalls);
}

async function runAgentflowCommandPipeline(
  store: AgentflowRunStateStore,
  runId: string,
  workflow: AgentflowWorkflow,
  resumeInput: AgentflowPipelineResumeInput | undefined,
  transforms: AgentflowArtifactTransformRegistry,
  sessionProviders: AgentflowSessionProviderRegistry,
  mcpCalls: AgentflowMcpCallRegistry
): Promise<AgentflowCommandPipelineResult> {
  const existing = store.getRun(runId);
  if (existing === null) throw new Error(`Agentflow run ${runId} was not found.`);
  if (!isDeepStrictEqual(existing.context.workflow, workflow)) {
    throw new AgentflowRunStateError(
      `Agentflow run ${runId} cannot execute a workflow that differs from its persisted definition.`,
      "AGENTFLOW_RUN_COLLISION"
    );
  }
  validateRuntimeInteractionSteps(workflow.steps);
  if (resumeInput === undefined && existing.status !== "pending") {
    throw new Error(`Agentflow run ${runId} cannot execute while its status is ${existing.status}.`);
  }
  if (resumeInput !== undefined && existing.status !== "paused") {
    throw new AgentflowRunStateError(
      `Agentflow run ${runId} cannot resume while its status is ${existing.status}.`,
      "AGENTFLOW_RUN_TRANSITION"
    );
  }
  assertAgentflowSuccessTargetsAreUnambiguous(workflow.steps);
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
    routingBudget = createSuccessfulRoutingBudget(workflow);
  } else {
    const resumed = resumeWaitingStep(store, runId, workflow, existing.context, resumeInput, stepLocations);
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
        }, "failed");
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
          arguments: step.arguments as AgentflowRunStateValue,
          outputs: step.outputs as AgentflowRunStateValue
        };
        store.updateRun(runId, { currentStepId: stepId, error: null });
        store.upsertStep({ runId, stepId, attempt, status: "running", input });
        store.appendRunEvent(runId, { type: "step.started", stepId, payload: input });
        try {
          const result = await executeAgentflowMcpCall(store, runId, workflow, step, mcpCalls, {
            stopStatus: () => activeStopStatus(store, runId),
            beforePublish: () => {
              const status = activeStopStatus(store, runId);
              if (status !== undefined) throw new AgentflowMcpCallInterruptedError(status);
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
          if (error instanceof AgentflowMcpCallInterruptedError) {
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
      }, failureStatus(step));
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
        }, "failed");
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
      }, failureStatus(step));
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
        }, "failed");
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
          inputs: step.inputs as AgentflowRunStateValue,
          outputs: step.outputs as AgentflowRunStateValue
        };
        store.upsertStep({ runId, stepId, attempt, sessionId, status: "running", input });
        store.appendRunEvent(runId, { type: "step.started", stepId, payload: input });
        try {
          const result = await executeAgentflowSessionRequest(store, runId, workflow, step, sessionProviders, {
            stopStatus: () => activeStopStatus(store, runId),
            beforePublish: () => {
              const status = activeStopStatus(store, runId);
              if (status !== undefined) throw new AgentflowSessionRequestInterruptedError(status);
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
          if (error instanceof AgentflowSessionRequestInterruptedError) {
            const output = { attempt, status: error.status };
            persistSessionRequestInterruption(store, runId, workflow, stepId, sessionId, error.status);
            store.upsertStep({ runId, stepId, attempt, sessionId, status: error.status, output });
            store.appendRunEvent(runId, { type: "step.interrupted", stepId, payload: output });
            return stoppedPipelineResult(store, runId, completedSteps)!;
          }
          if (error instanceof AgentflowSessionPolicyError) {
            failure = error.message;
            const outcome = error.status === "pause" ? "pause" : "fail";
            persistSessionRequestFailure(store, runId, stepId, sessionId, failure, false, outcome, true, attempt);
            return finishFailure(store, runId, completedSteps, stepId, {
              exitCode: null,
              timedOut: false,
              message: failure
            }, error.status === "pause" ? "paused" : "failed");
          }
          if (error instanceof AgentflowRunStateError && error.code === "AGENTFLOW_ARTIFACT_RUN_STATUS") {
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
          if (error instanceof AgentflowRunStateError && error.code === "AGENTFLOW_SESSION_ACTIVE") {
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
      }, failureStatus(step));
    }
    if (stepType === "condition") {
      const attempt = allocateStepAttempt(routingBudget, stepId);
      if (attempt === undefined) return stepAttemptLimitResult(store, runId, completedSteps, stepId, routingBudget);
      store.updateRun(runId, { currentStepId: stepId, error: null });
      store.upsertStep({ runId, stepId, attempt, status: "running", input: { type: "condition" } });
      store.appendRunEvent(runId, { type: "step.started", stepId, payload: { attempt, type: "condition" } });
      try {
        const selection = selectAgentflowConditionTarget(store, runId, step);
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
        const failure = { attempt, message, outcome: "fail" };
        store.upsertStep({ runId, stepId, attempt, status: "failed", error: failure });
        store.recordFailure({
          id: `condition:${safeId(stepId)}:evaluation`,
          runId,
          stepId,
          classification: "condition_evaluation",
          message,
          retryable: false,
          payload: failure
        });
        store.appendRunEvent(runId, { type: "step.failed", stepId, payload: failure });
        return finishFailure(store, runId, completedSteps, stepId, {
          exitCode: null,
          timedOut: false,
          message
        }, "failed");
      }
    }
    if (stepType === "manual_gate" || stepType === "input_request") {
      const attempt = allocateStepAttempt(routingBudget, stepId);
      if (attempt === undefined) return stepAttemptLimitResult(store, runId, completedSteps, stepId, routingBudget);
      try {
        return pauseForInteraction(store, runId, step, stepType, attempt, completedSteps, routingBudget);
      } catch (error) {
        if (!(error instanceof AgentflowRunStateError && error.code === "AGENTFLOW_INTERACTION_INVALID")) {
          throw error;
        }
        return finishFailure(store, runId, completedSteps, stepId, {
          exitCode: null,
          timedOut: false,
          message: error.message
        }, "failed");
      }
    }
    if (stepType !== "command") {
      return finishFailure(store, runId, completedSteps, stepId, {
        exitCode: null,
        timedOut: false,
        message: `Step ${stepId} has unsupported type ${String(step.type)}; only command, artifact_transform, condition, input_request, manual_gate, mcp_call, and session_request steps can execute in this runtime phase.`
      }, "paused");
    }

    const firstAttempt = allocateStepAttempt(routingBudget, stepId);
    if (firstAttempt === undefined) return stepAttemptLimitResult(store, runId, completedSteps, stepId, routingBudget);
    const preflightError = validateCommandStep(store.repoRoot, workflow, step);
    if (preflightError !== undefined) {
      persistPreflightFailure(
        store,
        runId,
        stepId,
        firstAttempt,
        preflightError.message,
        preflightError.status === "paused" ? "pause" : "fail"
      );
      return finishFailure(store, runId, completedSteps, stepId, {
        exitCode: null,
        timedOut: false,
        message: preflightError.message
      }, preflightError.status);
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
      const stoppedAfterCommand = stoppedPipelineResult(store, runId, completedSteps);
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
          status: stoppedAfterCommand.status,
          output: { ...commandOutput(lastResult, attempt), logPersistenceError: logPersistenceError ?? null }
        });
        store.appendRunEvent(runId, {
          type: "step.interrupted",
          stepId,
          payload: { attempt, status: stoppedAfterCommand.status, logPersistenceError: logPersistenceError ?? null }
        });
        return stoppedAfterCommand;
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
      const error = { ...commandError(lastResult, attempt), outcome: failureOutcome(step, retryable) };
      store.upsertStep({ runId, stepId, attempt, status: "failed", error });
      store.recordFailure({
        id: `command:${safeId(stepId)}:attempt-${attempt}`,
        runId,
        stepId,
        classification: lastResult.timedOut ? "command_timeout" : "command_failure",
        message: error.message as string,
        retryable,
        payload: error
      });
      store.appendRunEvent(runId, {
        type: lastResult.timedOut ? "step.timed_out" : "step.failed",
        stepId,
        payload: error
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
      }, failureStatus(step));
    }

    const routed = routeAfterSuccessfulStep(store, runId, completedSteps, stepId, step, currentSteps, stepIndex, stepLocations, routingBudget);
    if ("result" in routed) return routed.result;
    currentSteps = routed.steps;
    stepIndex = routed.nextIndex;
  }

  return finishCompleted(store, runId, completedSteps);
}

type SuccessfulRoute =
  | { steps: AgentflowWorkflowStep[]; nextIndex: number }
  | { result: AgentflowCommandPipelineResult };

interface RuntimeStepLocation {
  steps: AgentflowWorkflowStep[];
  index: number;
}

interface SuccessfulRoutingBudget {
  maxRecoveryCycles?: number;
  stepAttemptLimits: Map<string, number>;
  visits: Map<string, number>;
  recoveryCycles: Map<string, number>;
  attempts: Map<string, number>;
}

interface SerializedSuccessfulRoutingBudget {
  maxRecoveryCycles?: number;
  stepAttemptLimits: Record<string, number>;
  visits: Record<string, number>;
  recoveryCycles: Record<string, number>;
  attempts: Record<string, number>;
}

type ResumedWaitingStep =
  | { steps: AgentflowWorkflowStep[]; nextIndex: number; completedSteps: string[]; routingBudget: SuccessfulRoutingBudget }
  | { result: AgentflowCommandPipelineResult };

function pauseForInteraction(
  store: AgentflowRunStateStore,
  runId: string,
  step: AgentflowWorkflowStep,
  kind: "manual_gate" | "input_request",
  attempt: number,
  completedSteps: string[],
  routingBudget: SuccessfulRoutingBudget
): AgentflowCommandPipelineResult {
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
  const waiting: AgentflowPipelineWaitingState = {
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
  const input: Record<string, AgentflowRunStateValue> = kind === "manual_gate"
    ? { attempt, type: kind, message: prompt, options: validOutcomes }
    : { attempt, type: kind, question: prompt, saveAs: saveAs! };

  store.updateRun(runId, {
    currentStepId: stepId,
    context: { ...run.context, waiting: waiting as unknown as AgentflowRunStateValue },
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
  store.transitionRunWithEvent(runId, {
    status: "paused",
    allowedFrom: ["running"],
    event: {
      type: "run.paused",
      stepId,
      payload: {
        stepId,
        reason: waiting.reason,
        prompt,
        validOutcomes,
        ...(saveAs === undefined ? {} : { saveAs })
      }
    }
  });
  return {
    status: "paused",
    completedSteps,
    message: kind === "manual_gate"
      ? `Manual gate ${stepId} is waiting for one of: ${validOutcomes.join(", ")}.`
      : `Input request ${stepId} is waiting for an answer to be saved as ${saveAs}.`
  };
}

function resumeWaitingStep(
  store: AgentflowRunStateStore,
  runId: string,
  workflow: AgentflowWorkflow,
  context: Record<string, AgentflowRunStateValue>,
  response: AgentflowPipelineResumeInput,
  stepLocations: Map<string, RuntimeStepLocation>
): ResumedWaitingStep {
  const waiting = parseWaitingState(context.waiting);
  const location = stepLocations.get(waiting.stepId);
  if (location === undefined) {
    throw new AgentflowRunStateError(
      `Agentflow run ${runId} cannot resume because waiting step ${waiting.stepId} is not in its workflow.`,
      "AGENTFLOW_RESUME_STATE"
    );
  }
  const step = location.steps[location.index]!;
  if (normalizedTarget(step.type) !== waiting.kind) {
    throw new AgentflowRunStateError(
      `Agentflow run ${runId} waiting state does not match workflow step ${waiting.stepId}.`,
      "AGENTFLOW_RESUME_STATE"
    );
  }

  const routingBudget = deserializeRoutingBudget(waiting.routing, workflow);
  const completedSteps = [...waiting.completedSteps];
  let selectedTarget: string | undefined;
  let output: Record<string, AgentflowRunStateValue>;

  if (waiting.kind === "manual_gate") {
    if (!("outcome" in response)) {
      throw new AgentflowRunStateError(
        `Manual gate ${waiting.stepId} requires an explicit --outcome value.`,
        "AGENTFLOW_GATE_OUTCOME_REQUIRED"
      );
    }
    const outcome = response.outcome.trim();
    if (!waiting.validOutcomes.includes(outcome)) {
      throw new AgentflowRunStateError(
        `Manual gate ${waiting.stepId} rejected outcome ${JSON.stringify(outcome)}; valid outcomes are: ${waiting.validOutcomes.join(", ")}.`,
        "AGENTFLOW_GATE_OUTCOME_INVALID"
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
      throw new AgentflowRunStateError(
        `Manual gate ${waiting.stepId} decision actor must be non-empty text.`,
        "AGENTFLOW_INTERACTION_INVALID"
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
      throw new AgentflowRunStateError(
        `Input request ${waiting.stepId} requires an explicit --answer value.`,
        "AGENTFLOW_INPUT_ANSWER_REQUIRED"
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
      store.transitionRunWithEvent(runId, {
        status: "paused",
        allowedFrom: ["running"],
        event: {
          type: "run.paused",
          stepId: waiting.stepId,
          payload: { reason: waiting.reason, error: error instanceof Error ? error.message : String(error) }
        }
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

function manualGateOutcomeTarget(step: AgentflowWorkflowStep, outcome: string): string | undefined {
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
  workflow: AgentflowWorkflow
): SuccessfulRoutingBudget {
  const configured = createSuccessfulRoutingBudget(workflow);
  return {
    maxRecoveryCycles: configured.maxRecoveryCycles,
    stepAttemptLimits: configured.stepAttemptLimits,
    visits: new Map(Object.entries(serialized.visits)),
    recoveryCycles: new Map(Object.entries(serialized.recoveryCycles)),
    attempts: new Map(Object.entries(serialized.attempts))
  };
}

function parseWaitingState(value: AgentflowRunStateValue | undefined): AgentflowPipelineWaitingState {
  const record = mapping(value);
  if (record === undefined) {
    throw new AgentflowRunStateError(
      "Paused Agentflow run does not have a persisted manual gate or input request.",
      "AGENTFLOW_RESUME_STATE"
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
    throw new AgentflowRunStateError(
      "Paused Agentflow run has invalid persisted interaction state.",
      "AGENTFLOW_RESUME_STATE"
    );
  }
  const serialized = parseSerializedRoutingBudget(routing);
  const saveAs = typeof record.saveAs === "string" ? record.saveAs : undefined;
  const approvalId = typeof record.approvalId === "string" ? record.approvalId : undefined;
  if ((kind === "manual_gate" && (validOutcomes.length === 0 || approvalId === undefined))
      || (kind === "input_request" && saveAs === undefined)) {
    throw new AgentflowRunStateError(
      "Paused Agentflow run has incomplete persisted interaction state.",
      "AGENTFLOW_RESUME_STATE"
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

function parseSerializedRoutingBudget(value: AgentflowYamlMapping): SerializedSuccessfulRoutingBudget {
  const parseMap = (
    field: "stepAttemptLimits" | "visits" | "recoveryCycles" | "attempts",
    valid: (value: unknown) => boolean
  ): Record<string, number> => {
    const candidate = mapping(value[field]);
    if (candidate === undefined) {
      throw new AgentflowRunStateError(
        "Paused Agentflow run has invalid persisted routing state.",
        "AGENTFLOW_RESUME_STATE"
      );
    }
    const entries = Object.entries(candidate);
    if (entries.some(([, count]) => !valid(count))) {
      throw new AgentflowRunStateError(
        "Paused Agentflow run has invalid persisted routing counters.",
        "AGENTFLOW_RESUME_STATE"
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
    throw new AgentflowRunStateError(`${label} must be a static non-empty string.`, "AGENTFLOW_INTERACTION_INVALID");
  }
  return normalized;
}

function validateRuntimeInteractionSteps(steps: AgentflowWorkflowStep[]): void {
  for (const step of steps) {
    const type = normalizedTarget(step.type);
    const stepId = requiredStepId(step);
    if (type === "input_request") {
      const saveAs = requiredStaticString(step.save_as, `Input request artifact for step ${stepId}`);
      let normalized: string;
      try {
        normalized = normalizeAgentflowArtifactPath(saveAs);
      } catch (error) {
        throw new AgentflowRunStateError(
          `Input request artifact for step ${stepId} is invalid: ${error instanceof Error ? error.message : String(error)}`,
          "AGENTFLOW_INTERACTION_INVALID",
          { cause: error }
        );
      }
      if (normalized !== saveAs) {
        throw new AgentflowRunStateError(
          `Input request artifact for step ${stepId} must be normalized as ${normalized}.`,
          "AGENTFLOW_INTERACTION_INVALID"
        );
      }
    }
    for (const field of ["body", "steps", ...(type === "parallel" ? ["branches"] : [])]) {
      const nested = step[field];
      if (Array.isArray(nested)) validateRuntimeInteractionSteps(nested.filter(isWorkflowStep));
    }
  }
}

function resolveInteractionPrompt(
  value: unknown,
  inputs: Record<string, AgentflowRunStateValue>,
  label: string
): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new AgentflowRunStateError(`${label} must be non-empty text.`, "AGENTFLOW_INTERACTION_INVALID");
  }
  const template = value.trim();
  const unsupportedRemainder = template.replace(
    /(?<!\{)\{\{\s*inputs\.([A-Za-z_][A-Za-z0-9_-]*)\s*}}(?!})/g,
    ""
  );
  if (unsupportedRemainder.includes("{{") || unsupportedRemainder.includes("}}")) {
    throw new AgentflowRunStateError(
      `${label} contains an unsupported input expression.`,
      "AGENTFLOW_INTERACTION_INVALID"
    );
  }
  const resolved = template.replace(
    /(?<!\{)\{\{\s*inputs\.([A-Za-z_][A-Za-z0-9_-]*)\s*}}(?!})/g,
    (_match, name: string) => {
      if (!Object.hasOwn(inputs, name)) {
        throw new AgentflowRunStateError(
          `${label} references missing run input ${name}.`,
          "AGENTFLOW_INTERACTION_INVALID"
        );
      }
      const input = inputs[name]!;
      return typeof input === "string" ? input : JSON.stringify(input);
    }
  ).trim();
  if (resolved.length === 0) {
    throw new AgentflowRunStateError(
      `${label} must resolve to non-empty text.`,
      "AGENTFLOW_INTERACTION_INVALID"
    );
  }
  return resolved;
}

function routeAfterSuccessfulStep(
  store: AgentflowRunStateStore,
  runId: string,
  completedSteps: string[],
  stepId: string,
  step: AgentflowWorkflowStep,
  currentSteps: AgentflowWorkflowStep[],
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
  if (target === "complete" || target === "completed") return { result: finishCompleted(store, runId, completedSteps) };
  if (target === "fail") {
    return { result: finishSuccessfulTerminalRoute(store, runId, completedSteps, stepId, "failed") };
  }
  if (target === "pause") {
    return { result: finishSuccessfulTerminalRoute(store, runId, completedSteps, stepId, "paused") };
  }
  if (target === "cancel") {
    return { result: finishSuccessfulTerminalRoute(store, runId, completedSteps, stepId, "cancelled") };
  }
  return { result: finishFailure(store, runId, completedSteps, stepId, {
    exitCode: null,
    timedOut: false,
    message: `Step ${stepId} routed to unresolved target ${target}.`
  }, "failed") };
}

function fallthroughAfterStep(
  store: AgentflowRunStateStore,
  runId: string,
  completedSteps: string[],
  stepId: string,
  currentSteps: AgentflowWorkflowStep[],
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
  store: AgentflowRunStateStore,
  runId: string,
  completedSteps: string[],
  stepId: string,
  target: string,
  budget: SuccessfulRoutingBudget
): AgentflowCommandPipelineResult | undefined {
  const attemptLimit = budget.stepAttemptLimits.get(target);
  if (attemptLimit !== undefined && (budget.attempts.get(target) ?? 0) + 1 > attemptLimit) {
    return finishFailure(store, runId, completedSteps, stepId, {
      exitCode: null,
      timedOut: false,
      message: `Step ${stepId} cannot route to ${target} because limits.max_step_attempts allows ${attemptLimit} attempt(s).`
    }, "paused");
  }
  if ((budget.visits.get(target) ?? 0) === 0) return undefined;
  if (budget.maxRecoveryCycles === undefined) {
    return finishFailure(store, runId, completedSteps, stepId, {
      exitCode: null,
      timedOut: false,
      message: `Step ${stepId} repeated route target ${target} without a positive executable limits.max_recovery_cycles bound.`
    }, "paused");
  }
  const cycles = (budget.recoveryCycles.get(target) ?? 0) + 1;
  budget.recoveryCycles.set(target, cycles);
  if (cycles <= budget.maxRecoveryCycles) return undefined;
  return finishFailure(store, runId, completedSteps, stepId, {
    exitCode: null,
    timedOut: false,
    message: `Step ${stepId} exceeded limits.max_recovery_cycles ${budget.maxRecoveryCycles} while routing to ${target}.`
  }, "paused");
}

function finishSuccessfulTerminalRoute(
  store: AgentflowRunStateStore,
  runId: string,
  completedSteps: string[],
  stepId: string,
  status: "failed" | "paused" | "cancelled"
): AgentflowCommandPipelineResult {
  const target = status === "failed" ? "fail" : status === "paused" ? "pause" : "cancel";
  const message = `Step ${stepId} routed the pipeline to ${target}.`;
  store.updateRun(runId, {
    currentStepId: null,
    output: { completedSteps, terminalRoute: { status, stepId } }
  });
  store.transitionRunWithEvent(runId, {
    status,
    allowedFrom: ["running"],
    event: { type: `run.${status}`, payload: { routedByStepId: stepId, completedSteps, message } }
  });
  return { status, completedSteps, message };
}

function createSuccessfulRoutingBudget(workflow: AgentflowWorkflow): SuccessfulRoutingBudget {
  const limits = mapping(workflow.limits);
  const configuredStepAttempts = mapping(limits?.max_step_attempts);
  const stepAttemptLimits = new Map(Object.entries(configuredStepAttempts ?? {}).flatMap(([stepId, value]) =>
    typeof value === "number" && Number.isFinite(value) && value > 0 ? [[stepId, value] as const] : []
  ));
  const maxRecoveryCycles = workflow.style !== "pipeline" && typeof limits?.max_recovery_cycles === "number"
    && Number.isSafeInteger(limits.max_recovery_cycles) && limits.max_recovery_cycles > 0
    ? limits.max_recovery_cycles
    : undefined;
  return { maxRecoveryCycles, stepAttemptLimits, visits: new Map(), recoveryCycles: new Map(), attempts: new Map() };
}

function collectRuntimeStepLocations(
  steps: AgentflowWorkflowStep[],
  locations = new Map<string, RuntimeStepLocation>()
): Map<string, RuntimeStepLocation> {
  steps.forEach((step, index) => {
    if (normalizedTarget(step.type) !== undefined) {
      const stepId = requiredStepId(step);
      if (locations.has(stepId)) {
        throw new AgentflowRunStateError(
          `Agentflow workflow has multiple steps with ID ${JSON.stringify(stepId)}; runtime routing is ambiguous.`,
          "AGENTFLOW_STEP_AMBIGUOUS"
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
  store: AgentflowRunStateStore,
  runId: string,
  completedSteps: string[],
  stepId: string,
  budget: SuccessfulRoutingBudget
): AgentflowCommandPipelineResult {
  const limit = budget.stepAttemptLimits.get(stepId)!;
  return finishFailure(store, runId, completedSteps, stepId, {
    exitCode: null,
    timedOut: false,
    message: `Step ${stepId} cannot start because limits.max_step_attempts allows ${limit} attempt(s).`
  }, "paused");
}

function finishCompleted(
  store: AgentflowRunStateStore,
  runId: string,
  completedSteps: string[]
): AgentflowCommandPipelineResult {
  store.updateRun(runId, { currentStepId: null, output: { completedSteps } });
  store.transitionRunWithEvent(runId, {
    status: "completed",
    allowedFrom: ["running"],
    event: { type: "run.completed", payload: { completedSteps } }
  });
  return { status: "completed", completedSteps };
}

function normalizedTarget(value: unknown): string | undefined {
  if (typeof value !== "string" || value.trim().length === 0) return undefined;
  const target = value.trim();
  return target.includes("{{") || target.includes("}}") ? undefined : target;
}

function persistMcpCallFailure(
  store: AgentflowRunStateStore,
  runId: string,
  stepId: string,
  message: string,
  retryable: boolean,
  attempt: number,
  outcome: AgentflowFailureOutcome,
  rejected = false
): void {
  const payload = { attempt, message, outcome };
  store.upsertStep({ runId, stepId, attempt, status: "failed", error: payload });
  store.recordFailure({
    id: rejected ? `mcp-call:${safeId(stepId)}:preflight` : `mcp-call:${safeId(stepId)}:attempt-${attempt}`,
    runId,
    stepId,
    classification: rejected ? "mcp_call_policy" : "mcp_call_failure",
    message,
    retryable,
    payload
  });
  store.appendRunEvent(runId, { type: rejected ? "step.rejected" : "step.failed", stepId, payload });
}

function mcpCallFailureIsRetryable(error: unknown): boolean {
  return error instanceof AgentflowMcpCallError && error.code === "AGENTFLOW_MCP_ADAPTER_FAILED";
}

function persistSessionRequestInterruption(
  store: AgentflowRunStateStore,
  runId: string,
  workflow: AgentflowWorkflow,
  stepId: string,
  sessionId: string,
  status: "paused" | "cancelled"
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
  store: AgentflowRunStateStore,
  runId: string,
  stepId: string,
  step: AgentflowWorkflowStep,
  transforms: AgentflowArtifactTransformRegistry,
  attempt: number,
  retryable: boolean
): { failure?: string; stopped?: "paused" | "cancelled" } {
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
    const result = executeAgentflowArtifactTransform(store, runId, executableStep, transforms, {
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
    if (error instanceof AgentflowRunStateError && error.code === "AGENTFLOW_ARTIFACT_RUN_STATUS") {
      const stopped = activeStopStatus(store, runId);
      if (stopped !== undefined) {
        const output = { attempt, status: stopped };
        store.upsertStep({ runId, stepId, attempt, status: stopped, output });
        store.appendRunEvent(runId, { type: "step.interrupted", stepId, payload: output });
        return { stopped };
      }
    }
    const message = error instanceof Error ? error.message : String(error);
    const payload = { attempt, message, outcome: failureOutcome(step, retryable) };
    store.upsertStep({ runId, stepId, attempt, status: "failed", error: payload });
    store.recordFailure({
      id: `artifact-transform:${safeId(stepId)}:attempt-${attempt}`,
      runId,
      stepId,
      classification: "artifact_transform_failure",
      message,
      retryable,
      payload
    });
    store.appendRunEvent(runId, { type: "step.failed", stepId, payload });
    return { failure: message };
  }
}

class TransformInterruptedError extends Error {
  constructor(readonly status: "paused" | "cancelled") {
    super(`Artifact transform was interrupted because the run was ${status}.`);
  }
}

function persistMcpCallInterruption(
  store: AgentflowRunStateStore,
  runId: string,
  stepId: string,
  attempt: number,
  status: "paused" | "cancelled"
): void {
  const output = { attempt, status };
  store.upsertStep({ runId, stepId, attempt, status, output });
  store.appendRunEvent(runId, { type: "step.interrupted", stepId, payload: output });
}

function stoppedPipelineResult(
  store: AgentflowRunStateStore,
  runId: string,
  completedSteps: string[]
): AgentflowCommandPipelineResult | undefined {
  const status = store.getRun(runId)?.status;
  if (status === "running") return undefined;
  if (status === "paused" || status === "cancelled") {
    return {
      status,
      completedSteps,
      message: `Agentflow run ${runId} was ${status}; no additional commands were started.`
    };
  }
  throw new AgentflowRunStateError(
    `Agentflow run ${runId} cannot continue while its status is ${String(status)}.`,
    "AGENTFLOW_RUN_TRANSITION"
  );
}

function interruptedPipelineResult(
  store: AgentflowRunStateStore,
  runId: string,
  completedSteps: string[],
  interruptedStatus: "paused" | "cancelled"
): AgentflowCommandPipelineResult {
  return stoppedPipelineResult(store, runId, completedSteps) ?? {
    status: interruptedStatus,
    completedSteps,
    message: `Agentflow run ${runId} was interrupted as ${interruptedStatus}; no additional commands were started.`
  };
}

function activeStopStatus(store: AgentflowRunStateStore, runId: string): "paused" | "cancelled" | undefined {
  const status = store.getRun(runId)?.status;
  return status === "paused" || status === "cancelled" ? status : undefined;
}

function finishFailure(
  store: AgentflowRunStateStore,
  runId: string,
  completedSteps: string[],
  stepId: string,
  failure: { exitCode: number | null; timedOut: boolean; message: string },
  status: "failed" | "paused"
): AgentflowCommandPipelineResult {
  const failureOutcome = status === "failed" ? "fail" : "pause";
  const persistedFailure = { ...failure, outcome: failureOutcome };
  store.updateRun(runId, { currentStepId: stepId, error: persistedFailure });
  store.transitionRunWithEvent(runId, {
    status,
    allowedFrom: ["running"],
    event: { type: `run.${status}`, payload: { stepId, ...persistedFailure } }
  });
  return { status, completedSteps, failedStep: stepId, failureOutcome, ...failure };
}

function runCommand(
  repoRoot: string,
  command: string,
  timeoutMs: number | undefined,
  stopStatus: () => "paused" | "cancelled" | undefined
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
      env: { ...process.env, AGENTFLOW_REPO_ROOT: repoRoot },
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
      if (status !== undefined) requestTermination(`Agentflow run was ${status}; command was terminated.`, false);
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
  store: AgentflowRunStateStore,
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
    path: `logs/${safeId(stepId)}/attempt-${attempt}/${stream}.log`,
    kind: "command_log",
    contentType: "text/plain; charset=utf-8",
    content,
    metadata: { attempt, stream }
  });
}

function persistDeclaredOutputs(
  store: AgentflowRunStateStore,
  runId: string,
  stepId: string,
  step: AgentflowWorkflowStep,
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
  workflow: AgentflowWorkflow,
  step: AgentflowWorkflowStep
): CommandPreflightFailure | undefined {
  if (typeof step.command !== "string" || step.command.trim().length === 0) {
    return { status: "failed", message: "Command steps require a non-empty command." };
  }
  if (step.timeout_seconds !== undefined &&
      (typeof step.timeout_seconds !== "number" || !Number.isFinite(step.timeout_seconds) || step.timeout_seconds <= 0)) {
    return { status: "failed", message: "Command timeout_seconds must be a positive finite number." };
  }
  if (typeof step.timeout_seconds === "number" && step.timeout_seconds > MAX_AGENTFLOW_COMMAND_TIMEOUT_SECONDS) {
    return {
      status: "failed",
      message: `Command timeout_seconds cannot exceed ${MAX_AGENTFLOW_COMMAND_TIMEOUT_SECONDS}.`
    };
  }

  const onFailure = mapping(step.on_failure);
  const retry = onFailure?.retry;
  if (retry !== undefined &&
      (!Number.isSafeInteger(retry) || Number(retry) < 0 || Number(retry) > MAX_AGENTFLOW_COMMAND_RETRIES)) {
    return {
      status: "failed",
      message: `Command on_failure.retry must be an integer from 0 through ${MAX_AGENTFLOW_COMMAND_RETRIES}.`
    };
  }
  if (["continue", "ignore"].includes(normalizedFailureThen(onFailure) ?? "") && onFailure?.allowed !== true) {
    return {
      status: "failed",
      message: "Command failures may continue or be ignored only when on_failure.allowed is true."
    };
  }

  const approval = evaluateAgentflowPolicy(workflow, { kind: "approval", operation: "command" });
  if (approval.status !== "allow") return policyFailure(approval.status, approval.message);

  if (agentflowCommandUnsafeReason(step.command) !== undefined) {
    const unsafe = evaluateAgentflowPolicy(workflow, { kind: "unsafe_operation", operation: "command" });
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

function validateTransformStep(step: AgentflowWorkflowStep): string | undefined {
  const onFailure = mapping(step.on_failure);
  const retry = onFailure?.retry;
  if (retry !== undefined &&
      (!Number.isSafeInteger(retry) || Number(retry) < 0 || Number(retry) > MAX_AGENTFLOW_COMMAND_RETRIES)) {
    return `Artifact transform on_failure.retry must be an integer from 0 through ${MAX_AGENTFLOW_COMMAND_RETRIES}.`;
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

function validateSessionRequestStep(step: AgentflowWorkflowStep): string | undefined {
  if (typeof step.session !== "string" || step.session.trim().length === 0
      || typeof step.prompt !== "string" || step.prompt.trim().length === 0
      || !nonEmptyStringArray(step.inputs) || !nonEmptyStringArray(step.outputs)) {
    return "Session request requires a non-empty session, prompt, inputs list, and outputs list.";
  }
  const onFailure = mapping(step.on_failure);
  const retry = onFailure?.retry;
  if (retry !== undefined &&
      (!Number.isSafeInteger(retry) || Number(retry) < 0 || Number(retry) > MAX_AGENTFLOW_COMMAND_RETRIES)) {
    return `Session request on_failure.retry must be an integer from 0 through ${MAX_AGENTFLOW_COMMAND_RETRIES}.`;
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

function validateMcpCallStep(step: AgentflowWorkflowStep): string | undefined {
  if (typeof step.server !== "string" || step.server.trim().length === 0
      || typeof step.tool !== "string" || step.tool.trim().length === 0
      || mapping(step.arguments) === undefined || !nonEmptyStringArray(step.outputs)) {
    return "MCP call requires a non-empty server, tool, arguments mapping, and outputs list.";
  }
  if ([step.server, step.tool].some((value) => value.includes("{{") || value.includes("}}"))) {
    return "MCP call server and tool must be static non-empty names.";
  }
  try {
    validateAgentflowMcpArgumentExpressions(step.arguments, typeof step.id === "string" ? step.id.trim() : "(unnamed)");
    validateAgentflowMcpOutputPaths(step.outputs, typeof step.id === "string" ? step.id.trim() : "(unnamed)");
  } catch (error) {
    return error instanceof Error ? error.message : String(error);
  }
  const onFailure = mapping(step.on_failure);
  const retry = onFailure?.retry;
  if (retry !== undefined &&
      (!Number.isSafeInteger(retry) || Number(retry) < 0 || Number(retry) > MAX_AGENTFLOW_COMMAND_RETRIES)) {
    return `MCP call on_failure.retry must be an integer from 0 through ${MAX_AGENTFLOW_COMMAND_RETRIES}.`;
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
  store: AgentflowRunStateStore,
  runId: string,
  stepId: string,
  attempt: number,
  message: string,
  outcome: Exclude<AgentflowFailureOutcome, "retry" | "continue">
): void {
  const error = { attempt, exitCode: null, timedOut: false, message, outcome };
  store.upsertStep({ runId, stepId, attempt, status: "failed", error });
  store.recordFailure({
    id: `command:${safeId(stepId)}:attempt-${attempt}:preflight`,
    runId,
    stepId,
    classification: "command_policy",
    message,
    retryable: false,
    payload: error
  });
  store.appendRunEvent(runId, { type: "step.rejected", stepId, payload: error });
}

function persistTransformPreflightFailure(
  store: AgentflowRunStateStore,
  runId: string,
  stepId: string,
  attempt: number,
  message: string,
  outcome: Exclude<AgentflowFailureOutcome, "retry" | "continue">
): void {
  const error = { attempt, message, outcome };
  store.upsertStep({ runId, stepId, attempt, status: "failed", error });
  store.recordFailure({
    id: `artifact-transform:${safeId(stepId)}:attempt-${attempt}:preflight`,
    runId,
    stepId,
    classification: "artifact_transform_policy",
    message,
    retryable: false,
    payload: error
  });
  store.appendRunEvent(runId, { type: "step.rejected", stepId, payload: error });
}

function persistSessionRequestFailure(
  store: AgentflowRunStateStore,
  runId: string,
  stepId: string,
  sessionId: string | undefined,
  message: string,
  retryable: boolean,
  outcome: AgentflowFailureOutcome,
  rejected: boolean,
  attempt = 1
): void {
  const error = { attempt, message, outcome };
  store.upsertStep({ runId, stepId, attempt, ...(sessionId === undefined ? {} : { sessionId }), status: "failed", error });
  store.recordFailure({
    id: `session-request:${safeId(stepId)}:attempt-${attempt}`,
    runId,
    stepId,
    ...(sessionId === undefined ? {} : { sessionId }),
    classification: rejected ? "session_request_policy" : "session_request_failure",
    message,
    retryable,
    payload: error
  });
  store.appendRunEvent(runId, { type: rejected ? "step.rejected" : "step.failed", stepId, payload: error });
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

function timeoutMilliseconds(step: AgentflowWorkflowStep): number | undefined {
  return typeof step.timeout_seconds === "number" ? Math.ceil(step.timeout_seconds * 1_000) : undefined;
}

function failureRetries(step: AgentflowWorkflowStep): number {
  const retry = mapping(step.on_failure)?.retry;
  return typeof retry === "number" && Number.isSafeInteger(retry) && retry > 0 ? retry : 0;
}

function failureThen(step: AgentflowWorkflowStep): string | undefined {
  return normalizedFailureThen(mapping(step.on_failure));
}

function normalizedFailureThen(onFailure: AgentflowYamlMapping | undefined): string | undefined {
  const then = onFailure?.then;
  return typeof then === "string" && then.trim().length > 0 ? then.trim() : undefined;
}

function failureContinues(step: AgentflowWorkflowStep): boolean {
  return ["continue", "ignore"].includes(failureThen(step) ?? "");
}

function failureOutcome(step: AgentflowWorkflowStep, retryable: boolean): AgentflowFailureOutcome {
  if (retryable) return "retry";
  if (failureContinues(step)) return "continue";
  return failureStatus(step) === "failed" ? "fail" : "pause";
}

function failureStatus(step: AgentflowWorkflowStep): "failed" | "paused" {
  return failureThen(step) === "fail" ? "failed" : "paused";
}

function commandOutput(result: CommandAttemptResult, attempt: number): Record<string, AgentflowRunStateValue> {
  return { attempt, exitCode: result.exitCode, signal: result.signal, timedOut: result.timedOut };
}

function commandError(result: CommandAttemptResult, attempt: number): Record<string, AgentflowRunStateValue> & { message: string } {
  return { ...commandOutput(result, attempt), message: result.message ?? failureMessage(result) };
}

function failureMessage(result: CommandAttemptResult): string {
  if (result.timedOut) return "Command exceeded timeout_seconds and was terminated.";
  if (result.message !== undefined) return result.message;
  if (result.signal !== null) return `Command terminated by signal ${result.signal}.`;
  return `Command exited with status ${String(result.exitCode)}.`;
}

function requiredStepId(step: AgentflowWorkflowStep): string {
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

function mapping(value: unknown): AgentflowYamlMapping | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value) && !(value instanceof Uint8Array)
    ? value as AgentflowYamlMapping
    : undefined;
}

function isWorkflowStep(value: unknown): value is AgentflowWorkflowStep {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
