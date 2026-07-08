import type { AgentToolPackageBoundary } from "@jurgen1c/agent-tools";
import type { AgentflowSchemaPackageBoundary } from "@jurgen1c/agentflow-schemas";

export type AgentflowWorkflowStyle = "pipeline" | "recovery_pipeline" | "collaborative";
export type AgentflowMaturity = "draft" | "experimental" | "stable" | "trusted";
export type AgentflowRunStatus = "pending" | "running" | "waiting" | "paused" | "completed" | "failed" | "cancelled";

export const plannedAgentflowRuntimeCommands = [
  "init",
  "validate",
  "lint",
  "graph",
  "simulate",
  "run",
  "resume",
  "status",
  "logs",
  "artifacts",
  "cleanup"
] as const;

export type PlannedAgentflowRuntimeCommand = (typeof plannedAgentflowRuntimeCommands)[number];

export interface AgentflowCorePackageBoundary {
  packageName: "@jurgen1c/agentflow-core";
  role: "workflow-core";
  status: "skeleton";
  sharedToolsPackage: AgentToolPackageBoundary["packageName"];
  schemasPackage: AgentflowSchemaPackageBoundary["packageName"];
}

export interface AgentflowWorkflowReference {
  name: string;
  version: number;
  style: AgentflowWorkflowStyle;
  maturity: AgentflowMaturity;
}

export const agentflowCorePackageBoundary: AgentflowCorePackageBoundary = {
  packageName: "@jurgen1c/agentflow-core",
  role: "workflow-core",
  status: "skeleton",
  sharedToolsPackage: "@jurgen1c/agent-tools",
  schemasPackage: "@jurgen1c/agentflow-schemas"
};
