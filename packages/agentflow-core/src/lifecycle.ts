import type { AgentflowWorkflow } from "./workflow";
import { formatAgentflowWorkflowIssues, validateAgentflowWorkflow } from "./validation";
import {
  AgentflowRunStateError,
  type AgentflowRunMutationResult,
  type AgentflowRunRecord,
  type AgentflowRunStateStore
} from "./run_state";

export type AgentflowLifecycleAction = "pause" | "resume" | "cancel";

export interface CreateAgentflowLifecycleRunInput {
  id: string;
  workflow: AgentflowWorkflow;
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
    if (!matchesWorkflow(existing, input.workflow)) {
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
      }
    }, { type: "run.created", payload: { status: "pending" } });
    return { changed: true, run };
  } catch (error) {
    if (error instanceof AgentflowRunStateError && error.code === "AGENTFLOW_RUN_COLLISION") {
      const raced = store.getRun(input.id);
      if (raced !== null && raced.status === "pending" && matchesWorkflow(raced, input.workflow)) {
        return { changed: false, run: raced };
      }
    }
    throw error;
  }
}

export function transitionAgentflowLifecycleRun(
  store: AgentflowRunStateStore,
  runId: string,
  action: AgentflowLifecycleAction
): AgentflowRunMutationResult {
  const current = store.getRun(runId);
  if (current === null) {
    throw new AgentflowRunStateError(`Agentflow run ${runId} was not found.`, "AGENTFLOW_RUN_NOT_FOUND");
  }

  const { targetStatus, allowedFrom } = transitionRule(current, action);
  return store.transitionRunWithEvent(runId, {
    status: targetStatus,
    allowedFrom,
    event: { type: `run.${action}`, payload: { status: targetStatus } }
  });
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

function matchesWorkflow(run: AgentflowRunRecord, workflow: AgentflowWorkflow): boolean {
  return run.workflowName === workflow.name
    && run.workflowVersion === workflow.version
    && run.workflowStyle === workflow.style
    && run.workflowMaturity === workflow.maturity;
}
