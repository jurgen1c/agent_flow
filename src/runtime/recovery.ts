import {
  AgentFlowRunStateError,
  MAX_AGENT_FLOW_RECOVERY_CONTEXT_BYTES,
  type AgentFlowRunStateStore,
  type AgentFlowSessionRecord
} from "./run_state";
import type { AgentFlowWorkflow } from "./workflow";

export type AgentFlowRecoveryStatus = "remediated" | "unresolved";
export { MAX_AGENT_FLOW_RECOVERY_CONTEXT_BYTES } from "./run_state";

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

export function injectAgentFlowRecoveryContext(
  store: AgentFlowRunStateStore,
  runId: string,
  sessionId: string,
  context: string
): AgentFlowSessionRecord {
  if (typeof context !== "string" || context.trim().length === 0) {
    throw new AgentFlowRunStateError(
      "Injected recovery context must be non-empty text.",
      "AGENT_FLOW_RECOVERY_CONTEXT_INVALID"
    );
  }
  if (Buffer.byteLength(context, "utf8") > MAX_AGENT_FLOW_RECOVERY_CONTEXT_BYTES) {
    throw new AgentFlowRunStateError(
      `Injected recovery context exceeds the ${MAX_AGENT_FLOW_RECOVERY_CONTEXT_BYTES}-byte limit.`,
      "AGENT_FLOW_RECOVERY_CONTEXT_INVALID"
    );
  }
  return store.injectRecoverySessionContext(runId, sessionId, context);
}

function requiredName(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`${label} must be a non-empty string.`);
  }
  return value.trim();
}
