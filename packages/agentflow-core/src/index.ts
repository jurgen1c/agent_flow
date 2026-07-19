import type { AgentToolPackageBoundary } from "@jurgen1c/agent-tools";
import type { AgentflowSchemaPackageBoundary } from "@jurgen1c/agentflow-schemas";
import type { AgentflowMaturity, AgentflowWorkflowStyle } from "./workflow";
export {
  AGENTFLOW_RUN_STATE_SCHEMA_VERSION,
  AgentflowRunStateError,
  AgentflowRunStateStore,
  DEFAULT_AGENTFLOW_DATABASE_PATH,
  normalizeAgentflowArtifactPath,
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
  AgentflowArtifactTransformError,
  AgentflowArtifactTransformRegistry,
  MAX_AGENTFLOW_TRANSFORM_INPUT_BYTES,
  createAgentflowArtifactTransformRegistry,
  executeAgentflowArtifactTransform,
  transformAgentflowFixtureArtifact
} from "./artifact_transform";
export {
  AgentflowSessionProviderRegistry,
  AgentflowSessionRequestError,
  AgentflowSessionRequestInterruptedError,
  MAX_AGENTFLOW_SESSION_INPUT_BYTES,
  MAX_AGENTFLOW_SESSION_INPUTS,
  MAX_AGENTFLOW_SESSION_METADATA_BYTES,
  MAX_AGENTFLOW_SESSION_OUTPUT_BYTES,
  MAX_AGENTFLOW_SESSION_PROMPT_BYTES,
  MAX_AGENTFLOW_SESSION_TOTAL_INPUT_BYTES,
  createAgentflowFixtureSessionProvider,
  createAgentflowSessionProviderRegistry,
  executeAgentflowSessionRequest
} from "./session_request";
export {
  AgentflowMcpCallError,
  AgentflowMcpCallInterruptedError,
  AgentflowMcpCallRegistry,
  MAX_AGENTFLOW_MCP_METADATA_BYTES,
  MAX_AGENTFLOW_MCP_ARGUMENT_BYTES,
  MAX_AGENTFLOW_MCP_CONTENT_TYPE_BYTES,
  MAX_AGENTFLOW_MCP_OUTPUT_BYTES,
  createAgentflowFixtureMcpAdapter,
  createAgentflowMcpCallRegistry,
  executeAgentflowMcpCall
} from "./mcp_call";
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
  AgentflowBinaryArtifactValue,
  AgentflowArtifactTransform,
  AgentflowArtifactTransformContext,
  AgentflowArtifactTransformExecutionResult,
  AgentflowArtifactTransformOutput
} from "./artifact_transform";
export type {
  AgentflowSessionProviderAdapter,
  AgentflowSessionProviderOutput,
  AgentflowSessionProviderRequest,
  AgentflowSessionProviderResponse,
  AgentflowSessionRequestArtifact,
  AgentflowSessionRequestExecutionResult,
  ExecuteAgentflowSessionRequestOptions
} from "./session_request";
export type {
  AgentflowMcpCallAdapter,
  AgentflowMcpCallExecutionResult,
  AgentflowMcpCallRequest,
  AgentflowMcpCallResponse,
  ExecuteAgentflowMcpCallOptions
} from "./mcp_call";
export type {
  AgentflowPolicyDecision,
  AgentflowPolicyIssue,
  AgentflowPolicyRequest,
  AgentflowPolicyStatus
} from "./policy";
export type {
  AgentflowApprovalStatus,
  AgentflowArtifactContent,
  AgentflowArtifactRecord,
  AgentflowArtifactStatus,
  AgentflowBudgetRecord,
  AgentflowEventRecord,
  AgentflowRunEventInput,
  AgentflowRunMutationResult,
  AgentflowRunRecord,
  AgentflowSessionRecord,
  AgentflowRunStateValue,
  AgentflowRunStatus,
  AgentflowSessionStatus,
  AgentflowStepStatus,
  AppendAgentflowEventInput,
  CreateAgentflowRunInput,
  FindResumableAgentflowRunInput,
  OpenAgentflowRunStateOptions,
  ReadAgentflowArtifactOptions,
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
