import type { AgentFlowWorkflow } from "./workflow";

export type AgentFlowRecoveryStatus = "remediated" | "unresolved";

export class AgentFlowWorkflowRegistry {
  private readonly workflows = new Map<string, AgentFlowWorkflow>();

  register(name: string, workflow: AgentFlowWorkflow): this {
    const normalized = requiredName(name, "Recovery workflow name");
    if (this.workflows.has(normalized)) {
      throw new Error(`Recovery workflow ${normalized} is already registered.`);
    }
    this.workflows.set(normalized, workflow);
    return this;
  }

  get(name: string): AgentFlowWorkflow | undefined {
    return this.workflows.get(requiredName(name, "Recovery workflow name"));
  }

  names(): string[] {
    return [...this.workflows.keys()].sort();
  }
}

export function createAgentFlowWorkflowRegistry(): AgentFlowWorkflowRegistry {
  return new AgentFlowWorkflowRegistry();
}

function requiredName(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`${label} must be a non-empty string.`);
  }
  return value.trim();
}
