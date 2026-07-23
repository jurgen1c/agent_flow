import type { AgentflowWorkflowStep, AgentflowYamlValue } from "./workflow";

export const AGENTFLOW_AMBIGUOUS_SUCCESS_TARGET_CODE = "workflow.step.success_target.ambiguous";

export interface AgentflowAmbiguousSuccessTarget {
  stepId: string | undefined;
}

export class AgentflowAmbiguousSuccessTargetError extends Error {
  readonly code = AGENTFLOW_AMBIGUOUS_SUCCESS_TARGET_CODE;
  readonly stepId: string | undefined;

  constructor(stepId?: string) {
    super(agentflowAmbiguousSuccessTargetMessage(stepId));
    this.name = "AgentflowAmbiguousSuccessTargetError";
    this.stepId = stepId;
  }
}

export function agentflowStepHasAmbiguousSuccessTarget(step: AgentflowWorkflowStep): boolean {
  return nonEmptyString(step.then) !== undefined && nonEmptyString(step.goto) !== undefined;
}

export function collectAgentflowAmbiguousSuccessTargets(
  steps: AgentflowWorkflowStep[],
  conflicts: AgentflowAmbiguousSuccessTarget[] = []
): AgentflowAmbiguousSuccessTarget[] {
  for (const step of steps) {
    if (agentflowStepHasAmbiguousSuccessTarget(step)) {
      conflicts.push({ stepId: nonEmptyString(step.id) });
    }

    for (const field of ["body", "steps"] as const) {
      const nested = step[field];
      if (Array.isArray(nested)) {
        collectAgentflowAmbiguousSuccessTargets(nested.filter(isWorkflowStep), conflicts);
      }
    }

    if (nonEmptyString(step.type) === "parallel" && Array.isArray(step.branches)) {
      collectAgentflowAmbiguousSuccessTargets(step.branches.filter(isWorkflowStep), conflicts);
    }
  }

  return conflicts;
}

export function assertAgentflowSuccessTargetsAreUnambiguous(steps: AgentflowWorkflowStep[]): void {
  const conflict = collectAgentflowAmbiguousSuccessTargets(steps)[0];
  if (conflict !== undefined) {
    throw new AgentflowAmbiguousSuccessTargetError(conflict.stepId);
  }
}

export function agentflowAmbiguousSuccessTargetMessage(stepId?: string): string {
  const subject = stepId === undefined ? "Workflow steps" : `Step ${JSON.stringify(stepId)}`;
  return `${subject} cannot declare both then and goto success targets.`;
}

function nonEmptyString(value: AgentflowYamlValue | undefined): string | undefined {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : undefined;
}

function isWorkflowStep(value: unknown): value is AgentflowWorkflowStep {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
