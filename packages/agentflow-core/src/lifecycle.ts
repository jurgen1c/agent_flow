import { isDeepStrictEqual } from "node:util";
import type { AgentflowWorkflow } from "./workflow";
import { formatAgentflowWorkflowIssues, validateAgentflowWorkflow } from "./validation";
import {
  createAgentflowNotificationRegistry,
  deliverAgentflowNotifications,
  type AgentflowNotificationRegistry
} from "./notifications";
import {
  agentflowPipelineEffectsFinalized,
  applyAgentflowRetention,
  markAgentflowPipelineEffectsFinalized,
  writeAgentflowFinalSummary
} from "./retention";
import { withAgentflowPipelineFinalization } from "./finalization";
import {
  AgentflowRunStateError,
  type AgentflowRunMutationResult,
  type AgentflowRunRecord,
  type AgentflowRunStateValue,
  type AgentflowRunStateStore
} from "./run_state";

export type AgentflowLifecycleAction = "pause" | "resume" | "cancel";

export interface CreateAgentflowLifecycleRunInput {
  id: string;
  workflow: AgentflowWorkflow;
  inputs?: Record<string, AgentflowRunStateValue>;
}

export function createAgentflowLifecycleRun(
  store: AgentflowRunStateStore,
  input: CreateAgentflowLifecycleRunInput
): AgentflowRunMutationResult {
  const validation = validateAgentflowWorkflow(input.workflow);
  if (!validation.valid) {
    throw new AgentflowRunStateError(
      `Agentflow run ${input.id} cannot start because workflow validation failed:\n${formatAgentflowWorkflowIssues(validation.errors)}`,
      "AGENTFLOW_WORKFLOW_INVALID"
    );
  }

  const existing = store.getRun(input.id);

  if (existing !== null) {
    if (!matchesWorkflow(existing, input.workflow, input.inputs ?? {})) {
      throw new AgentflowRunStateError(
        `Agentflow run ${input.id} already exists for ${existing.workflowName} version ${existing.workflowVersion}. Choose a different run ID.`,
        "AGENTFLOW_RUN_COLLISION"
      );
    }
    if (existing.status !== "pending") {
      throw new AgentflowRunStateError(
        `Agentflow run ${input.id} already exists with status ${existing.status} and cannot be reopened. Choose a different run ID.`,
        "AGENTFLOW_RUN_COLLISION"
      );
    }
    return { changed: false, run: existing };
  }

  try {
    const run = store.createRunWithEvent({
      id: input.id,
      workflow: {
        name: input.workflow.name,
        version: input.workflow.version,
        style: input.workflow.style,
        maturity: input.workflow.maturity
      },
      context: { workflow: input.workflow as unknown as AgentflowRunStateValue },
      inputs: input.inputs
    }, { type: "run.created", payload: { status: "pending" } });
    return { changed: true, run };
  } catch (error) {
    if (error instanceof AgentflowRunStateError && error.code === "AGENTFLOW_RUN_COLLISION") {
      const raced = store.getRun(input.id);
      if (raced !== null && raced.status === "pending" && matchesWorkflow(raced, input.workflow, input.inputs ?? {})) {
        return { changed: false, run: raced };
      }
    }
    throw error;
  }
}

export function transitionAgentflowLifecycleRun(
  store: AgentflowRunStateStore,
  runId: string,
  action: AgentflowLifecycleAction,
  notifications: AgentflowNotificationRegistry = createAgentflowNotificationRegistry()
): AgentflowRunMutationResult {
  const current = store.getRun(runId);
  if (current === null) {
    throw new AgentflowRunStateError(`Agentflow run ${runId} was not found.`, "AGENTFLOW_RUN_NOT_FOUND");
  }
  const persistedWorkflow = current.context.workflow;
  if (persistedWorkflow === null || typeof persistedWorkflow !== "object" || Array.isArray(persistedWorkflow)) {
    throw new AgentflowRunStateError(
      `Agentflow run ${runId} cannot transition because its persisted context does not contain a workflow definition.`,
      "AGENTFLOW_WORKFLOW_INVALID"
    );
  }
  const workflow = persistedWorkflow as unknown as AgentflowWorkflow;
  let validation: ReturnType<typeof validateAgentflowWorkflow>;
  try {
    validation = validateAgentflowWorkflow(workflow);
  } catch (error) {
    throw new AgentflowRunStateError(
      `Agentflow run ${runId} cannot transition because its persisted workflow definition is malformed: ${error instanceof Error ? error.message : String(error)}`,
      "AGENTFLOW_WORKFLOW_INVALID",
      { cause: error }
    );
  }
  if (!validation.valid) {
    throw new AgentflowRunStateError(
      `Agentflow run ${runId} cannot transition because persisted workflow validation failed:\n${formatAgentflowWorkflowIssues(validation.errors)}`,
      "AGENTFLOW_WORKFLOW_INVALID"
    );
  }
  if (workflow.style === "pipeline" && (action === "pause" || action === "cancel")) {
    return withAgentflowPipelineFinalization(
      store,
      runId,
      () => ({ changed: false, run: store.getRun(runId)! }),
      () => transitionAgentflowLifecycleRunUnlocked(store, runId, action, notifications)
    );
  }
  return transitionAgentflowLifecycleRunUnlocked(store, runId, action, notifications);
}

function transitionAgentflowLifecycleRunUnlocked(
  store: AgentflowRunStateStore,
  runId: string,
  action: AgentflowLifecycleAction,
  notifications: AgentflowNotificationRegistry
): AgentflowRunMutationResult {
  const current = store.getRun(runId);
  if (current === null) {
    throw new AgentflowRunStateError(`Agentflow run ${runId} was not found.`, "AGENTFLOW_RUN_NOT_FOUND");
  }

  if (action === "pause" && current.status === "failed" && notificationFinalizationFailed(current.error)) {
    if (!agentflowPipelineEffectsFinalized(store, runId, "failed")) {
      const workflow = current.context.workflow as unknown as AgentflowWorkflow;
      if (workflow.style === "pipeline") {
        applyAgentflowRetention(store, runId, workflow, "failed");
        markAgentflowPipelineEffectsFinalized(store, runId, "failed");
        markLifecycleFinalized(store, runId, action, "failed");
      }
    }
    return { changed: false, run: store.getRun(runId)! };
  }

  const { targetStatus, allowedFrom } = transitionRule(current, action);
  const completedSteps = completedStepsFromRun(store, current);
  const workflow = current.context.workflow as unknown as AgentflowWorkflow;
  if (action === "cancel" && workflow.style === "pipeline") {
    return transitionPipelineCancellation(
      store,
      runId,
      current,
      targetStatus,
      allowedFrom,
      completedSteps,
      workflow,
      notifications
    );
  }
  if (action === "cancel" && current.context.waiting !== undefined) {
    closeCancelledInteraction(store, runId, current.context.waiting);
  }
  const result = store.transitionRunWithEvent(runId, {
    status: targetStatus,
    allowedFrom,
    event: { type: `run.${action}`, payload: { status: targetStatus } }
  });
  if (action !== "pause" && action !== "cancel") return result;

  if (workflow.style !== "pipeline") return result;
  if (!result.changed && agentflowPipelineEffectsFinalized(store, runId, "paused")) return result;
  return finalizeLifecycleSideEffects(
    store,
    runId,
    action,
    result,
    workflow,
    completedSteps,
    notifications
  );
}

function finalizeLifecycleSideEffects(
  store: AgentflowRunStateStore,
  runId: string,
  action: Extract<AgentflowLifecycleAction, "pause" | "cancel">,
  result: AgentflowRunMutationResult,
  workflow: AgentflowWorkflow,
  completedSteps: string[],
  notifications: AgentflowNotificationRegistry
): AgentflowRunMutationResult {
  if (action === "cancel") {
    throw new Error("Pipeline cancellation must be finalized before its terminal transition.");
  }

  const delivery = deliverAgentflowNotifications(store, runId, workflow, "paused", notifications);
  if (delivery.requiredFailure === undefined) {
    markAgentflowPipelineEffectsFinalized(store, runId, "paused");
    markLifecycleFinalized(store, runId, action, "paused");
    return { changed: result.changed, run: store.getRun(runId)! };
  }
  const message = `Required ${delivery.requiredFailure.channel} notification for ${delivery.requiredFailure.event} failed: ${delivery.requiredFailure.message}`;
  const error = {
    code: "notification.required.failed",
    channel: delivery.requiredFailure.channel,
    event: delivery.requiredFailure.event,
    message
  };
  try {
    writeAgentflowFinalSummary(store, runId, workflow, { status: "failed", completedSteps, message });
  } catch (summaryError) {
    return failLifecycleSummary(
      store,
      runId,
      action,
      workflow,
      completedSteps,
      notifications,
      summaryError
    );
  }
  deliverAgentflowNotifications(store, runId, workflow, "failed", notifications);
  store.updateRun(runId, { currentStepId: null, error });
  const failed = store.transitionRunWithEvent(runId, {
    status: "failed",
    allowedFrom: ["paused"],
    event: {
      type: "run.failed",
      payload: { code: "notification.required.failed", completedSteps, message }
    }
  });
  applyAgentflowRetention(store, runId, workflow, "failed");
  markAgentflowPipelineEffectsFinalized(store, runId, "failed");
  markLifecycleFinalized(store, runId, action, "failed");
  return failed;
}

function transitionPipelineCancellation(
  store: AgentflowRunStateStore,
  runId: string,
  current: AgentflowRunRecord,
  targetStatus: AgentflowRunRecord["status"],
  allowedFrom: AgentflowRunRecord["status"][],
  completedSteps: string[],
  workflow: AgentflowWorkflow,
  notifications: AgentflowNotificationRegistry
): AgentflowRunMutationResult {
  if (current.status === "cancelled" && agentflowPipelineEffectsFinalized(store, runId, "cancelled")) {
    return { changed: false, run: current };
  }
  if (current.context.waiting !== undefined) {
    closeCancelledInteraction(store, runId, current.context.waiting);
  }
  try {
    writeAgentflowFinalSummary(store, runId, workflow, {
      status: "cancelled",
      completedSteps,
      message: `Agentflow run ${runId} was cancelled.`
    });
  } catch (summaryError) {
    return failLifecycleSummary(
      store,
      runId,
      "cancel",
      workflow,
      completedSteps,
      notifications,
      summaryError
    );
  }
  const result = store.transitionRunWithEvent(runId, {
    status: targetStatus,
    allowedFrom,
    event: { type: "run.cancel", payload: { status: targetStatus } }
  });
  applyAgentflowRetention(store, runId, workflow, "cancelled");
  markAgentflowPipelineEffectsFinalized(store, runId, "cancelled");
  markLifecycleFinalized(store, runId, "cancel", "cancelled");
  return { changed: result.changed, run: store.getRun(runId)! };
}

function failLifecycleSummary(
  store: AgentflowRunStateStore,
  runId: string,
  action: Extract<AgentflowLifecycleAction, "pause" | "cancel">,
  workflow: AgentflowWorkflow,
  completedSteps: string[],
  notifications: AgentflowNotificationRegistry,
  summaryError: unknown
): AgentflowRunMutationResult {
  const message = `Could not persist final pipeline summary: ${summaryError instanceof Error ? summaryError.message : String(summaryError)}`;
  if (store.getRun(runId)?.status === "cancelled") {
    throw new AgentflowRunStateError(
      message,
      "AGENTFLOW_SUMMARY_PERSIST_FAILED",
      { cause: summaryError }
    );
  }
  const error = { code: "summary.persist.failed", message };
  store.appendRunEvent(runId, { type: "summary.failed", payload: { message } });
  deliverAgentflowNotifications(store, runId, workflow, "failed", notifications);
  store.updateRun(runId, { currentStepId: null, error });
  const failed = store.transitionRunWithEvent(runId, {
    status: "failed",
    allowedFrom: ["pending", "running", "waiting", "paused"],
    event: {
      type: "run.failed",
      payload: { code: "summary.persist.failed", completedSteps, message }
    }
  });
  applyAgentflowRetention(store, runId, workflow, "failed");
  markAgentflowPipelineEffectsFinalized(store, runId, "failed");
  markLifecycleFinalized(store, runId, action, "failed");
  return failed;
}

function markLifecycleFinalized(
  store: AgentflowRunStateStore,
  runId: string,
  action: Extract<AgentflowLifecycleAction, "pause" | "cancel">,
  status: AgentflowRunRecord["status"]
): void {
  store.appendRunEvent(runId, {
    type: `lifecycle.${action}.finalized`,
    payload: { status }
  });
}

function notificationFinalizationFailed(error: AgentflowRunStateValue | null): boolean {
  return error !== null && typeof error === "object" && !Array.isArray(error)
    && error.code === "notification.required.failed";
}

function completedStepsFromRun(store: AgentflowRunStateStore, run: AgentflowRunRecord): string[] {
  const output = run.output;
  if (output !== null && typeof output === "object" && !Array.isArray(output)
      && Array.isArray(output.completedSteps)) {
    return output.completedSteps.filter((step): step is string => typeof step === "string");
  }
  const waiting = run.context.waiting;
  if (waiting !== null && typeof waiting === "object" && !Array.isArray(waiting)
      && Array.isArray(waiting.completedSteps)) {
    return waiting.completedSteps.filter((step): step is string => typeof step === "string");
  }
  const completedSteps: string[] = [];
  const seen = new Set<string>();
  for (const event of store.listEvents(run.id)) {
    if (event.type !== "step.completed" || event.stepId === null || seen.has(event.stepId)) continue;
    seen.add(event.stepId);
    completedSteps.push(event.stepId);
  }
  return completedSteps;
}

function closeCancelledInteraction(
  store: AgentflowRunStateStore,
  runId: string,
  value: AgentflowRunStateValue
): void {
  const waiting = value !== null && typeof value === "object" && !Array.isArray(value)
    ? value
    : undefined;
  const run = store.getRun(runId)!;
  const { waiting: _waiting, ...context } = run.context;
  store.updateRun(runId, { currentStepId: null, context });

  const stepId = typeof waiting?.stepId === "string" && waiting.stepId.trim().length > 0
    ? waiting.stepId.trim()
    : undefined;
  const attempt = waiting?.attempt;
  if (stepId === undefined || !Number.isSafeInteger(attempt) || (attempt as number) < 1) return;

  const output = { attempt: attempt as number, reason: "run_cancelled" };
  store.upsertStep({
    runId,
    stepId,
    attempt: attempt as number,
    status: "cancelled",
    output
  });
  store.appendRunEvent(runId, { type: "step.interrupted", stepId, payload: output });

  const approvalId = typeof waiting?.approvalId === "string" && waiting.approvalId.trim().length > 0
    ? waiting.approvalId.trim()
    : undefined;
  if (waiting?.kind === "manual_gate" && approvalId !== undefined) {
    store.upsertApproval({
      id: approvalId,
      runId,
      stepId,
      status: "cancelled",
      decision: "cancel"
    });
  }
}

function transitionRule(
  run: AgentflowRunRecord,
  action: AgentflowLifecycleAction
): { targetStatus: AgentflowRunRecord["status"]; allowedFrom: AgentflowRunRecord["status"][] } {
  if (action === "pause") {
    if (["pending", "running", "waiting", "paused"].includes(run.status)) {
      return { targetStatus: "paused", allowedFrom: ["pending", "running", "waiting"] };
    }
  }

  if (action === "resume") {
    if (["pending", "running", "waiting", "paused"].includes(run.status)) {
      if (run.context.waiting !== undefined) {
        throw new AgentflowRunStateError(
          `Agentflow run ${run.id} is waiting for an explicit manual-gate outcome or input-request answer.`,
          "AGENTFLOW_INTERACTION_REQUIRED"
        );
      }
      return { targetStatus: "running", allowedFrom: ["pending", "waiting", "paused"] };
    }
  }

  if (action === "cancel") {
    if (!["completed", "failed"].includes(run.status)) {
      return { targetStatus: "cancelled", allowedFrom: ["pending", "running", "waiting", "paused"] };
    }
  }

  throw new AgentflowRunStateError(
    `Agentflow run ${run.id} cannot ${action} while its status is ${run.status}.`,
    "AGENTFLOW_RUN_TRANSITION"
  );
}

function matchesWorkflow(
  run: AgentflowRunRecord,
  workflow: AgentflowWorkflow,
  inputs: Record<string, AgentflowRunStateValue>
): boolean {
  return run.workflowName === workflow.name
    && run.workflowVersion === workflow.version
    && run.workflowStyle === workflow.style
    && run.workflowMaturity === workflow.maturity
    && isDeepStrictEqual(run.context.workflow, workflow)
    && isDeepStrictEqual(run.inputs, inputs);
}
