import {
  normalizeAgentFlowArtifactPath,
  type AgentFlowArtifactRecord,
  type AgentFlowRunStateStore
} from "./run_state";
import type { AgentFlowWorkflow, AgentFlowWorkflowStep } from "./workflow";

export interface AgentFlowDecisionRecord {
  decision_id: string;
  owner: string;
  topic: string;
  rationale_summary: string;
  consulted: string[];
  approved_by: string[];
  artifacts: string[];
  created_at: string;
}

export type AgentFlowDecisionRecordContract = Omit<AgentFlowDecisionRecord, "created_at"> & {
  step_id: string;
  output: string;
};

export function defaultAgentFlowDecisionRecordPath(stepId: string): string {
  return `decision-records/${safePathSegment(stepId)}.json`;
}

export function resolveAgentFlowDecisionRecordContract(
  workflow: AgentFlowWorkflow,
  step: AgentFlowWorkflowStep
): AgentFlowDecisionRecordContract {
  const stepId = requiredText(step.id, "Decision record step ID");
  const owner = requiredStaticName(step.owner, `Decision record ${stepId} owner`);
  const topic = requiredText(step.topic, `Decision record ${stepId} topic`);
  const consulted = optionalUniqueTextList(step.consulted, `Decision record ${stepId} consulted sessions`);
  const approvedBy = optionalUniqueTextList(step.approved_by, `Decision record ${stepId} approvers`);
  const artifacts = requiredArtifactPathList(step.artifacts, `Decision record ${stepId} artifacts`);
  const sessions = new Set(Object.keys(workflow.sessions ?? {}));
  for (const [label, actors] of [["owner", [owner]], ["consulted", consulted], ["approved_by", approvedBy]] as const) {
    const undeclared = actors.find((actor) => !sessions.has(actor));
    if (undeclared !== undefined) {
      throw new Error(`Decision record ${stepId} ${label} references undeclared session ${undeclared}.`);
    }
  }
  return {
    step_id: stepId,
    decision_id: `decision:${stepId}`,
    owner,
    topic,
    rationale_summary: step.rationale_summary === undefined
      ? topic
      : requiredText(step.rationale_summary, `Decision record ${stepId} rationale_summary`),
    consulted,
    approved_by: approvedBy,
    artifacts,
    output: step.output === undefined
      ? defaultAgentFlowDecisionRecordPath(stepId)
      : requiredArtifactPath(step.output, `Decision record ${stepId} output`)
  };
}

export function executeAgentFlowDecisionRecord(
  store: AgentFlowRunStateStore,
  runId: string,
  step: AgentFlowWorkflowStep,
  workflow?: AgentFlowWorkflow
): { record: AgentFlowDecisionRecord; artifact: AgentFlowArtifactRecord } {
  const persistedWorkflow = workflow ?? store.getRun(runId)?.context.workflow as unknown as AgentFlowWorkflow | undefined;
  if (persistedWorkflow === undefined) throw new Error(`Decision record workflow for run ${runId} is unavailable.`);
  const contract = resolveAgentFlowDecisionRecordContract(persistedWorkflow, step);
  const evidence = contract.artifacts.map((artifactPath) => store.readArtifact(runId, artifactPath).artifact);
  if (evidence.some((artifact) => artifact.checksum === null)) {
    throw new Error(`Decision record ${contract.step_id} artifacts must have persisted checksums.`);
  }
  const record: AgentFlowDecisionRecord = {
    decision_id: contract.decision_id,
    owner: contract.owner,
    topic: contract.topic,
    rationale_summary: contract.rationale_summary,
    consulted: contract.consulted,
    approved_by: contract.approved_by,
    artifacts: contract.artifacts,
    created_at: store.currentTimestamp()
  };
  const existing = store.getArtifact(runId, contract.output);
  const artifact = store.writeArtifact({
    id: existing?.id ?? `decision-record:${contract.step_id}`,
    runId,
    stepId: contract.step_id,
    path: contract.output,
    kind: "decision_record",
    contentType: "application/json; charset=utf-8",
    content: `${JSON.stringify(record)}\n`,
    overwrite: step.overwrite === true || existing?.producerStepId === contract.step_id,
    requiredRunStatus: "running",
    requiredArtifacts: evidence.map((artifact) => ({ path: artifact.declaredPath, checksum: artifact.checksum! })),
    metadata: { owner: contract.owner, topic: contract.topic, retainedByDefault: true }
  });
  return { record, artifact };
}

function requiredText(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim().length === 0) throw new Error(`${label} must be non-empty text.`);
  return value.trim();
}

function requiredStaticName(value: unknown, label: string): string {
  const name = requiredText(value, label);
  if (name.includes("{{") || name.includes("}}")) throw new Error(`${label} must be a static name.`);
  return name;
}

function optionalUniqueTextList(value: unknown, label: string): string[] {
  if (value === undefined) return [];
  if (!Array.isArray(value) || value.length === 0
      || value.some((entry) => typeof entry !== "string" || entry.trim().length === 0)) {
    throw new Error(`${label} must be a non-empty list of non-empty strings.`);
  }
  const values = value.map((entry) => (entry as string).trim());
  if (new Set(values).size !== values.length) throw new Error(`${label} must not contain duplicate values.`);
  return values;
}

function requiredArtifactPathList(value: unknown, label: string): string[] {
  if (!Array.isArray(value) || value.length === 0) throw new Error(`${label} must be a non-empty list.`);
  const paths = value.map((entry) => requiredArtifactPath(entry, label));
  if (new Set(paths).size !== paths.length) throw new Error(`${label} must not contain duplicate values.`);
  return paths;
}

function requiredArtifactPath(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim().length === 0) throw new Error(`${label} must be non-empty text.`);
  if (value.includes("{{") || value.includes("}}")) throw new Error(`${label} must use normalized static artifact paths.`);
  let normalized: string;
  try {
    normalized = normalizeAgentFlowArtifactPath(value);
  } catch {
    throw new Error(`${label} must use normalized static repo-relative artifact paths.`);
  }
  if (normalized !== value) throw new Error(`${label} must use normalized static repo-relative artifact paths.`);
  return normalized;
}

function safePathSegment(value: string): string {
  return value.replace(/[^A-Za-z0-9._-]+/g, "-").replace(/^-+|-+$/g, "") || "decision";
}
