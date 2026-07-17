import type { AgentToolPackageBoundary } from "@jurgen1c/agent-tools";
import type { AgentflowSchemaPackageBoundary } from "@jurgen1c/agentflow-schemas";
import type { AgentflowMaturity, AgentflowWorkflowStyle } from "./workflow";
export {
  AGENTFLOW_RUN_STATE_SCHEMA_VERSION,
  AgentflowRunStateError,
  AgentflowRunStateStore,
  DEFAULT_AGENTFLOW_DATABASE_PATH,
  openAgentflowRunState
} from "./run_state";
export {
  AgentflowWorkflowParseError,
  formatWorkflowParseIssues,
  parseAgentflowWorkflow,
  parseAgentflowWorkflowOrThrow
} from "./workflow";
export {
  formatAgentflowWorkflowIssues,
  lintAgentflowWorkflow,
  validateAgentflowWorkflow
} from "./validation";
export {
  AgentflowWorkflowGraphError,
  buildAgentflowWorkflowGraph,
  explainAgentflowWorkflow,
  renderAgentflowWorkflowGraph
} from "./inspection";
export {
  parseAgentflowSimulationFixture,
  renderAgentflowSimulationSummary,
  simulateAgentflowWorkflow
} from "./simulation";
export {
  createAgentflowLifecycleRun,
  transitionAgentflowLifecycleRun
} from "./lifecycle";
export { executeAgentflowCommandPipeline } from "./command_execution";
export {
  evaluateAgentflowPolicy,
  validateAgentflowPolicyPrimitives
} from "./policy";
export type {
  AgentflowWorkflow,
  AgentflowWorkflowParseFailure,
  AgentflowWorkflowParseIssue,
  AgentflowWorkflowParseResult,
  AgentflowWorkflowParseSuccess,
  AgentflowMaturity,
  AgentflowWorkflowStyle,
  AgentflowWorkflowStep,
  AgentflowYamlMapping,
  AgentflowYamlValue
} from "./workflow";
export type {
  AgentflowWorkflowIssue,
  AgentflowWorkflowLintResult,
  AgentflowWorkflowValidationResult
} from "./validation";
export type {
  AgentflowWorkflowGraph,
  AgentflowWorkflowGraphEdge,
  AgentflowWorkflowGraphNode
} from "./inspection";
export type {
  AgentflowSimulationFixture,
  AgentflowSimulationFixtureParseResult,
  AgentflowSimulationMissingArtifact,
  AgentflowSimulationResult,
  AgentflowSimulationStatus,
  AgentflowSimulationStepFixture,
  AgentflowSimulationStepOutcome,
  AgentflowSimulationTerminalState,
  AgentflowSimulationUnresolvedBranch,
  AgentflowSimulationVisitedOutcome,
  AgentflowSimulationVisitedStep
} from "./simulation";
export type {
  AgentflowLifecycleAction,
  CreateAgentflowLifecycleRunInput
} from "./lifecycle";
export type { AgentflowCommandPipelineResult } from "./command_execution";
export type {
  AgentflowPolicyDecision,
  AgentflowPolicyIssue,
  AgentflowPolicyRequest,
  AgentflowPolicyStatus
} from "./policy";
export type {
  AgentflowApprovalStatus,
  AgentflowArtifactRecord,
  AgentflowArtifactStatus,
  AgentflowEventRecord,
  AgentflowRunEventInput,
  AgentflowRunMutationResult,
  AgentflowRunRecord,
  AgentflowRunStateValue,
  AgentflowRunStatus,
  AgentflowSessionStatus,
  AgentflowStepStatus,
  AppendAgentflowEventInput,
  CreateAgentflowRunInput,
  FindResumableAgentflowRunInput,
  OpenAgentflowRunStateOptions,
  RecordAgentflowFailureInput,
  TransitionAgentflowRunWithEventInput,
  UpdateAgentflowRunInput,
  UpsertAgentflowApprovalInput,
  UpsertAgentflowArtifactInput,
  UpsertAgentflowBudgetInput,
  UpsertAgentflowSessionInput,
  UpsertAgentflowStepInput,
  WriteAgentflowArtifactInput
} from "./run_state";

export const plannedAgentflowRuntimeCommands = [
  "init",
  "validate",
  "lint",
  "explain",
  "graph",
  "simulate",
  "run",
  "resume",
  "status",
  "logs",
  "artifacts",
  "pause",
  "cancel",
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
