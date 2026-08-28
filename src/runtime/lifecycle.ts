import { isDeepStrictEqual } from "node:util";
import type { AgentFlowWorkflow } from "./workflow";
import { formatAgentFlowWorkflowIssues, validateAgentFlowWorkflow } from "./validation";
import type { AgentFlowProviderKindResolver } from "./policy_utils";
import {
  createAgentFlowNotificationRegistry,
  deliverAgentFlowNotifications,
  type AgentFlowNotificationRegistry
} from "./notifications";
import {
  agentFlowPipelineEffectsFinalized,
  applyAgentFlowRetention,
  markAgentFlowPipelineEffectsFinalized,
  writeAgentFlowFinalSummary
} from "./retention";
import { withAgentFlowPipelineFinalization } from "./finalization";
import {
  AgentFlowRunStateError,
  type AgentFlowRunMutationResult,
  type AgentFlowRunRecord,
  type AgentFlowRunStateValue,
  type AgentFlowRunStateStore
} from "./run_state";

export type AgentFlowLifecycleAction = "pause" | "resume" | "cancel";

const INITIAL_CONTEXT_KEY = "agentFlowInitialContext";

export interface CreateAgentFlowLifecycleRunInput {
  id: string;
  workflow: AgentFlowWorkflow;
  inputs?: Record<string, AgentFlowRunStateValue>;
  context?: Record<string, AgentFlowRunStateValue>;
  parentRunId?: string;
  recoveryOfRunId?: string;
  allowInterruptedRecovery?: boolean;
}

export function createAgentFlowLifecycleRun(
  store: AgentFlowRunStateStore,
  input: CreateAgentFlowLifecycleRunInput
): AgentFlowRunMutationResult {
  const initialContext = normalizedInitialContext(input.context);
  const validation = validateAgentFlowWorkflow(
    input.workflow,
    providerKindResolverFromBindings(initialContext.providerBindings)
  );
  if (!validation.valid) {
    throw new AgentFlowRunStateError(
      `Agent Flow run ${input.id} cannot start because workflow validation failed:\n${formatAgentFlowWorkflowIssues(validation.errors)}`,
      "AGENT_FLOW_WORKFLOW_INVALID"
    );
  }

  const existing = store.getRun(input.id);

  if (existing !== null) {
    if (!matchesWorkflow(
      existing,
      input.workflow,
      input.inputs ?? {},
      initialContext,
      input.parentRunId ?? null,
      input.recoveryOfRunId ?? null
    )) {
      throw new AgentFlowRunStateError(
        `Agent Flow run ${input.id} already exists for ${existing.workflowName} version ${existing.workflowVersion}. Choose a different run ID.`,
        "AGENT_FLOW_RUN_COLLISION"
      );
    }
    if (!canReuseLifecycleRun(existing, input.allowInterruptedRecovery === true)) {
      throw new AgentFlowRunStateError(
        `Agent Flow run ${input.id} already exists with status ${existing.status} and cannot be reopened. Choose a different run ID.`,
        "AGENT_FLOW_RUN_COLLISION"
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
      context: {
        ...input.context,
        workflow: input.workflow as unknown as AgentFlowRunStateValue,
        [INITIAL_CONTEXT_KEY]: initialContext
      },
      inputs: input.inputs,
      ...(input.parentRunId === undefined ? {} : { parentRunId: input.parentRunId }),
      ...(input.recoveryOfRunId === undefined ? {} : { recoveryOfRunId: input.recoveryOfRunId })
    }, { type: "run.created", payload: { status: "pending" } });
    return { changed: true, run };
  } catch (error) {
    if (error instanceof AgentFlowRunStateError && error.code === "AGENT_FLOW_RUN_COLLISION") {
      const raced = store.getRun(input.id);
      if (raced !== null && canReuseLifecycleRun(raced, input.allowInterruptedRecovery === true) && matchesWorkflow(
        raced,
        input.workflow,
        input.inputs ?? {},
        initialContext,
        input.parentRunId ?? null,
        input.recoveryOfRunId ?? null
      )) {
        return { changed: false, run: raced };
      }
    }
    throw error;
  }
}

function canReuseLifecycleRun(run: AgentFlowRunRecord, allowInterruptedRecovery: boolean): boolean {
  return run.status === "pending" || (allowInterruptedRecovery && run.status === "running");
}

export function transitionAgentFlowLifecycleRun(
  store: AgentFlowRunStateStore,
  runId: string,
  action: AgentFlowLifecycleAction,
  notifications: AgentFlowNotificationRegistry = createAgentFlowNotificationRegistry(),
  prepareTransition?: () => void
): AgentFlowRunMutationResult {
  const current = store.getRun(runId);
  if (current === null) {
    throw new AgentFlowRunStateError(`Agent Flow run ${runId} was not found.`, "AGENT_FLOW_RUN_NOT_FOUND");
  }
  const persistedWorkflow = current.context.workflow;
  if (persistedWorkflow === null || typeof persistedWorkflow !== "object" || Array.isArray(persistedWorkflow)) {
    throw new AgentFlowRunStateError(
      `Agent Flow run ${runId} cannot transition because its persisted context does not contain a workflow definition.`,
      "AGENT_FLOW_WORKFLOW_INVALID"
    );
  }
  const workflow = persistedWorkflow as unknown as AgentFlowWorkflow;
  let validation: ReturnType<typeof validateAgentFlowWorkflow>;
  try {
    validation = validateAgentFlowWorkflow(
      workflow,
      providerKindResolverFromBindings(current.context.providerBindings)
    );
  } catch (error) {
    throw new AgentFlowRunStateError(
      `Agent Flow run ${runId} cannot transition because its persisted workflow definition is malformed: ${error instanceof Error ? error.message : String(error)}`,
      "AGENT_FLOW_WORKFLOW_INVALID",
      { cause: error }
    );
  }
  if (!validation.valid) {
    throw new AgentFlowRunStateError(
      `Agent Flow run ${runId} cannot transition because persisted workflow validation failed:\n${formatAgentFlowWorkflowIssues(validation.errors)}`,
      "AGENT_FLOW_WORKFLOW_INVALID"
    );
  }
  let result: AgentFlowRunMutationResult;
  if (action === "pause" || (workflow.style === "pipeline" && action === "cancel")) {
    result = withAgentFlowPipelineFinalization(
      store,
      runId,
      () => ({ changed: false, run: store.getRun(runId)! }),
      () => {
        prepareTransition?.();
        return transitionAgentFlowLifecycleRunUnlocked(store, runId, action, notifications);
      }
    );
  } else {
    result = store.withRunStateTransaction(runId, () => {
      prepareTransition?.();
      return transitionAgentFlowLifecycleRunUnlocked(store, runId, action, notifications);
    });
  }
  if (action === "cancel") cancelNonterminalChildRuns(store, runId, notifications);
  return result;
}

function cancelNonterminalChildRuns(
  store: AgentFlowRunStateStore,
  parentRunId: string,
  notifications: AgentFlowNotificationRegistry
): void {
  const children = store.listRuns().filter((run) => run.parentRunId === parentRunId
    && ["pending", "running", "waiting", "paused"].includes(run.status));
  for (const child of children) {
    const current = store.getRun(child.id);
    if (current === null || ["completed", "failed", "cancelled"].includes(current.status)) {
      cancelNonterminalChildRuns(store, child.id, notifications);
      continue;
    }
    try {
      transitionAgentFlowLifecycleRun(store, child.id, "cancel", notifications);
    } catch (error) {
      const settled = store.getRun(child.id);
      if (!(error instanceof AgentFlowRunStateError)
          || error.code !== "AGENT_FLOW_RUN_TRANSITION"
          || settled === null
          || !["completed", "failed", "cancelled"].includes(settled.status)) throw error;
      cancelNonterminalChildRuns(store, child.id, notifications);
    }
  }
}

function transitionAgentFlowLifecycleRunUnlocked(
  store: AgentFlowRunStateStore,
  runId: string,
  action: AgentFlowLifecycleAction,
  notifications: AgentFlowNotificationRegistry
): AgentFlowRunMutationResult {
  const current = store.getRun(runId);
  if (current === null) {
    throw new AgentFlowRunStateError(`Agent Flow run ${runId} was not found.`, "AGENT_FLOW_RUN_NOT_FOUND");
  }

  if (action === "pause" && current.status === "failed" && notificationFinalizationFailed(current.error)) {
    if (!agentFlowPipelineEffectsFinalized(store, runId, "failed")) {
      const workflow = current.context.workflow as unknown as AgentFlowWorkflow;
      if (workflow.style === "pipeline") {
        applyAgentFlowRetention(store, runId, workflow, "failed");
        markAgentFlowPipelineEffectsFinalized(store, runId, "failed");
        markLifecycleFinalized(store, runId, action, "failed");
      }
    }
    return { changed: false, run: store.getRun(runId)! };
  }

  const { targetStatus, allowedFrom } = transitionRule(current, action);
  const completedSteps = completedStepsFromRun(store, current);
  const workflow = current.context.workflow as unknown as AgentFlowWorkflow;
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

  if (workflow.style !== "pipeline" && action !== "pause") return result;
  if (!result.changed && (workflow.style !== "pipeline"
      || agentFlowPipelineEffectsFinalized(store, runId, "paused"))) return result;
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
  store: AgentFlowRunStateStore,
  runId: string,
  action: Extract<AgentFlowLifecycleAction, "pause" | "cancel">,
  result: AgentFlowRunMutationResult,
  workflow: AgentFlowWorkflow,
  completedSteps: string[],
  notifications: AgentFlowNotificationRegistry
): AgentFlowRunMutationResult {
  if (action === "cancel") {
    throw new Error("Pipeline cancellation must be finalized before its terminal transition.");
  }

  const delivery = deliverAgentFlowNotifications(store, runId, workflow, "paused", notifications);
  const currentAfterDelivery = store.getRun(runId)!;
  if (currentAfterDelivery.status !== "paused") {
    return { changed: result.changed, run: currentAfterDelivery };
  }
  if (delivery.requiredFailure === undefined) {
    if (workflow.style === "pipeline") markAgentFlowPipelineEffectsFinalized(store, runId, "paused");
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
  if (workflow.style === "pipeline") {
    try {
      writeAgentFlowFinalSummary(store, runId, workflow, { status: "failed", completedSteps, message });
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
  }
  deliverAgentFlowNotifications(store, runId, workflow, "failed", notifications);
  const currentAfterFailureDelivery = store.getRun(runId)!;
  if (currentAfterFailureDelivery.status !== "paused") {
    return { changed: result.changed, run: currentAfterFailureDelivery };
  }
  store.updateRun(runId, { currentStepId: null, error });
  const failed = store.transitionRunWithEvent(runId, {
    status: "failed",
    allowedFrom: ["paused"],
    event: {
      type: "run.failed",
      payload: { code: "notification.required.failed", completedSteps, message }
    }
  });
  if (workflow.style === "pipeline") {
    applyAgentFlowRetention(store, runId, workflow, "failed");
    markAgentFlowPipelineEffectsFinalized(store, runId, "failed");
  }
  markLifecycleFinalized(store, runId, action, "failed");
  return failed;
}

function transitionPipelineCancellation(
  store: AgentFlowRunStateStore,
  runId: string,
  current: AgentFlowRunRecord,
  targetStatus: AgentFlowRunRecord["status"],
  allowedFrom: AgentFlowRunRecord["status"][],
  completedSteps: string[],
  workflow: AgentFlowWorkflow,
  notifications: AgentFlowNotificationRegistry
): AgentFlowRunMutationResult {
  if (current.status === "cancelled" && agentFlowPipelineEffectsFinalized(store, runId, "cancelled")) {
    return { changed: false, run: current };
  }
  if (current.context.waiting !== undefined) {
    closeCancelledInteraction(store, runId, current.context.waiting);
  }
  try {
    writeAgentFlowFinalSummary(store, runId, workflow, {
      status: "cancelled",
      completedSteps,
      message: `Agent Flow run ${runId} was cancelled.`
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
  applyAgentFlowRetention(store, runId, workflow, "cancelled");
  markAgentFlowPipelineEffectsFinalized(store, runId, "cancelled");
  markLifecycleFinalized(store, runId, "cancel", "cancelled");
  return { changed: result.changed, run: store.getRun(runId)! };
}

function failLifecycleSummary(
  store: AgentFlowRunStateStore,
  runId: string,
  action: Extract<AgentFlowLifecycleAction, "pause" | "cancel">,
  workflow: AgentFlowWorkflow,
  completedSteps: string[],
  notifications: AgentFlowNotificationRegistry,
  summaryError: unknown
): AgentFlowRunMutationResult {
  const message = `Could not persist final pipeline summary: ${summaryError instanceof Error ? summaryError.message : String(summaryError)}`;
  if (store.getRun(runId)?.status === "cancelled") {
    throw new AgentFlowRunStateError(
      message,
      "AGENT_FLOW_SUMMARY_PERSIST_FAILED",
      { cause: summaryError }
    );
  }
  const error = { code: "summary.persist.failed", message };
  store.appendRunEvent(runId, { type: "summary.failed", payload: { message } });
  deliverAgentFlowNotifications(store, runId, workflow, "failed", notifications);
  store.updateRun(runId, { currentStepId: null, error });
  const failed = store.transitionRunWithEvent(runId, {
    status: "failed",
    allowedFrom: ["pending", "running", "waiting", "paused"],
    event: {
      type: "run.failed",
      payload: { code: "summary.persist.failed", completedSteps, message }
    }
  });
  applyAgentFlowRetention(store, runId, workflow, "failed");
  markAgentFlowPipelineEffectsFinalized(store, runId, "failed");
  markLifecycleFinalized(store, runId, action, "failed");
  return failed;
}

function markLifecycleFinalized(
  store: AgentFlowRunStateStore,
  runId: string,
  action: Extract<AgentFlowLifecycleAction, "pause" | "cancel">,
  status: AgentFlowRunRecord["status"]
): void {
  store.appendRunEvent(runId, {
    type: `lifecycle.${action}.finalized`,
    payload: { status }
  });
}

function notificationFinalizationFailed(error: AgentFlowRunStateValue | null): boolean {
  return error !== null && typeof error === "object" && !Array.isArray(error)
    && error.code === "notification.required.failed";
}

function completedStepsFromRun(store: AgentFlowRunStateStore, run: AgentFlowRunRecord): string[] {
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
  store: AgentFlowRunStateStore,
  runId: string,
  value: AgentFlowRunStateValue
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
  if ((waiting?.kind === "manual_gate" || waiting?.kind === "approval") && approvalId !== undefined) {
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
  run: AgentFlowRunRecord,
  action: AgentFlowLifecycleAction
): { targetStatus: AgentFlowRunRecord["status"]; allowedFrom: AgentFlowRunRecord["status"][] } {
  if (action === "pause") {
    if (["pending", "running", "waiting", "paused"].includes(run.status)) {
      return { targetStatus: "paused", allowedFrom: ["pending", "running", "waiting"] };
    }
  }

  if (action === "resume") {
    if (["pending", "running", "waiting", "paused"].includes(run.status)) {
      if (run.context.waiting !== undefined) {
        throw new AgentFlowRunStateError(
          `Agent Flow run ${run.id} is waiting for an explicit manual-gate outcome or input-request answer.`,
          "AGENT_FLOW_INTERACTION_REQUIRED"
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

  throw new AgentFlowRunStateError(
    `Agent Flow run ${run.id} cannot ${action} while its status is ${run.status}.`,
    "AGENT_FLOW_RUN_TRANSITION"
  );
}

function matchesWorkflow(
  run: AgentFlowRunRecord,
  workflow: AgentFlowWorkflow,
  inputs: Record<string, AgentFlowRunStateValue>,
  initialContext: Record<string, AgentFlowRunStateValue>,
  parentRunId: string | null,
  recoveryOfRunId: string | null
): boolean {
  return run.workflowName === workflow.name
    && run.workflowVersion === workflow.version
    && run.workflowStyle === workflow.style
    && run.workflowMaturity === workflow.maturity
    && run.parentRunId === parentRunId
    && run.recoveryOfRunId === recoveryOfRunId
    && isDeepStrictEqual(run.context.workflow, workflow)
    && isDeepStrictEqual(run.context[INITIAL_CONTEXT_KEY] ?? {}, initialContext)
    && isDeepStrictEqual(run.inputs, inputs);
}

function normalizedInitialContext(
  context: Record<string, AgentFlowRunStateValue> | undefined
): Record<string, AgentFlowRunStateValue> {
  if (context !== undefined && Object.hasOwn(context, INITIAL_CONTEXT_KEY)) {
    throw new AgentFlowRunStateError(
      `Agent Flow lifecycle context key ${INITIAL_CONTEXT_KEY} is reserved.`,
      "AGENT_FLOW_RUN_CONTEXT_INVALID"
    );
  }
  return Object.fromEntries(Object.entries(context ?? {}).filter(([key]) => key !== "workflow"));
}

function providerKindResolverFromBindings(
  value: AgentFlowRunStateValue | undefined
): AgentFlowProviderKindResolver | undefined {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return undefined;
  return (provider) => {
    if (!Object.hasOwn(value, provider)) return undefined;
    const binding = value[provider];
    if (binding === null || typeof binding !== "object" || Array.isArray(binding)) return undefined;
    return binding.kind === "local" || binding.kind === "frontier"
      ? {
          kind: binding.kind,
          ...(typeof binding.driver === "string" ? { driver: binding.driver } : {})
        }
      : undefined;
  };
}
