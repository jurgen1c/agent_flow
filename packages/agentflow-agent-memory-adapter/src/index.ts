import type { AgentContext, ContextBudget } from "@jurgen1c/agent-memory-core";
import type { AgentflowCorePackageBoundary } from "@jurgen1c/agentflow-core";

export interface AgentflowAgentMemoryAdapterPackageBoundary {
  packageName: "@jurgen1c/agentflow-agent-memory-adapter";
  role: "agent-memory-adapter";
  status: "skeleton";
  corePackage: AgentflowCorePackageBoundary["packageName"];
  agentMemoryPackage: "@jurgen1c/agent-memory-core";
}

export interface AgentflowMemoryContextRequest {
  task?: string;
  changedFiles?: readonly string[];
  budget?: ContextBudget;
  includeInferred?: boolean;
}

export interface AgentflowAgentMemoryAdapter {
  buildContext(request: AgentflowMemoryContextRequest): Promise<AgentContext>;
}

export const agentflowAgentMemoryAdapterPackageBoundary: AgentflowAgentMemoryAdapterPackageBoundary = {
  packageName: "@jurgen1c/agentflow-agent-memory-adapter",
  role: "agent-memory-adapter",
  status: "skeleton",
  corePackage: "@jurgen1c/agentflow-core",
  agentMemoryPackage: "@jurgen1c/agent-memory-core"
};
