export interface AgentflowSchemaPackageBoundary {
  packageName: "@jurgen1c/agentflow-schemas";
  role: "workflow-schemas";
  status: "skeleton";
  exportedSchemas: readonly ["config", "workflow"];
}

export const agentflowSchemaPackageBoundary: AgentflowSchemaPackageBoundary = {
  packageName: "@jurgen1c/agentflow-schemas",
  role: "workflow-schemas",
  status: "skeleton",
  exportedSchemas: ["config", "workflow"]
};
