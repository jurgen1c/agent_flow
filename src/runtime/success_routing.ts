import type { AgentFlowWorkflowStep, AgentFlowYamlValue } from "./workflow";

export const AGENT_FLOW_AMBIGUOUS_SUCCESS_TARGET_CODE = "workflow.step.success_target.ambiguous";

export interface AgentFlowAmbiguousSuccessTarget {
  stepId: string | undefined;
}

export class AgentFlowAmbiguousSuccessTargetError extends Error {
  readonly code = AGENT_FLOW_AMBIGUOUS_SUCCESS_TARGET_CODE;
  readonly stepId: string | undefined;

  constructor(stepId?: string) {
    super(agentFlowAmbiguousSuccessTargetMessage(stepId));
    this.name = "AgentFlowAmbiguousSuccessTargetError";
    this.stepId = stepId;
  }
}

export function agentFlowStepHasAmbiguousSuccessTarget(step: AgentFlowWorkflowStep): boolean {
  return nonEmptyString(step.then) !== undefined && nonEmptyString(step.goto) !== undefined;
}

export function collectAgentFlowAmbiguousSuccessTargets(
  steps: AgentFlowWorkflowStep[],
  conflicts: AgentFlowAmbiguousSuccessTarget[] = []
): AgentFlowAmbiguousSuccessTarget[] {
  for (const step of steps) {
    if (agentFlowStepHasAmbiguousSuccessTarget(step)) {
      conflicts.push({ stepId: nonEmptyString(step.id) });
    }

    for (const field of ["body", "steps"] as const) {
      const nested = step[field];
      if (Array.isArray(nested)) {
        collectAgentFlowAmbiguousSuccessTargets(nested.filter(isWorkflowStep), conflicts);
      }
    }

    if (nonEmptyString(step.type) === "parallel" && Array.isArray(step.branches)) {
      collectAgentFlowAmbiguousSuccessTargets(step.branches.filter(isWorkflowStep), conflicts);
    }
  }

  return conflicts;
}

export function assertAgentFlowSuccessTargetsAreUnambiguous(steps: AgentFlowWorkflowStep[]): void {
  const conflict = collectAgentFlowAmbiguousSuccessTargets(steps)[0];
  if (conflict !== undefined) {
    throw new AgentFlowAmbiguousSuccessTargetError(conflict.stepId);
  }
}

export function agentFlowAmbiguousSuccessTargetMessage(stepId?: string): string {
  const subject = stepId === undefined ? "Workflow steps" : `Step ${JSON.stringify(stepId)}`;
  return `${subject} cannot declare both then and goto success targets.`;
}

function nonEmptyString(value: AgentFlowYamlValue | undefined): string | undefined {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : undefined;
}

function isWorkflowStep(value: unknown): value is AgentFlowWorkflowStep {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
