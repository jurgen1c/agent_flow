import type { AgentflowCorePackageBoundary } from "@jurgen1c/agentflow-core";

export { agentflowCorePackageBoundary, plannedAgentflowRuntimeCommands } from "@jurgen1c/agentflow-core";

export interface AgentflowPackageBoundary {
  packageName: "@jurgen1c/agentflow";
  role: "workflow-runtime";
  corePackage: AgentflowCorePackageBoundary["packageName"];
  status: "skeleton";
}

export const agentflowPackageBoundary: AgentflowPackageBoundary = {
  packageName: "@jurgen1c/agentflow",
  role: "workflow-runtime",
  corePackage: "@jurgen1c/agentflow-core",
  status: "skeleton"
};
