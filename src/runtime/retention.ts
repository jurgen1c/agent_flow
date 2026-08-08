import { evaluateAgentFlowPolicy } from "./policy";
import { mapping, matchesPolicyGlob, stringList } from "./policy_utils";
import type {
  AgentFlowArtifactRecord,
  AgentFlowEventRecord,
  AgentFlowRunStateStore,
  AgentFlowRunStatus
} from "./run_state";
import { AGENT_FLOW_FINAL_SUMMARY_PATH } from "./run_state";
import type { AgentFlowWorkflow, AgentFlowYamlValue } from "./workflow";

export { AGENT_FLOW_FINAL_SUMMARY_PATH } from "./run_state";

export interface AgentFlowFinalSummaryInput {
  status: Extract<AgentFlowRunStatus, "completed" | "failed" | "paused" | "cancelled">;
  completedSteps: string[];
  message?: string;
}

export function writeAgentFlowFinalSummary(
  store: AgentFlowRunStateStore,
  runId: string,
  workflow: AgentFlowWorkflow,
  input: AgentFlowFinalSummaryInput
): AgentFlowArtifactRecord {
  const existing = store.getArtifact(runId, AGENT_FLOW_FINAL_SUMMARY_PATH);
  if (existing !== null && existing.kind !== "run_summary") {
    throw new Error(`Runtime summary path ${AGENT_FLOW_FINAL_SUMMARY_PATH} is already owned by ${existing.id}.`);
  }
  const completed = input.completedSteps.length === 0
    ? "- None"
    : input.completedSteps.map((stepId) => `- ${stepId}`).join("\n");
  const content = [
    "# Agent Flow run summary",
    "",
    `Workflow: ${workflow.name} (version ${workflow.version})`,
    `Run: ${runId}`,
    `Status: ${input.status}`,
    "",
    "Completed steps:",
    completed,
    ...(input.message === undefined ? [] : ["", `Message: ${input.message}`]),
    ""
  ].join("\n");

  if (existing !== null) {
    try {
      if (store.readArtifact(runId, AGENT_FLOW_FINAL_SUMMARY_PATH).content.equals(Buffer.from(content))) {
        return existing;
      }
    } catch {
      // Rewrite missing or stale summary backings through the normal artifact path.
    }
  }

  return store.writeArtifact({
    id: "run:final-summary",
    runId,
    path: AGENT_FLOW_FINAL_SUMMARY_PATH,
    kind: "run_summary",
    contentType: "text/markdown; charset=utf-8",
    content,
    overwrite: existing !== null,
    metadata: {
      status: input.status,
      completedSteps: input.completedSteps
    }
  });
}

export function applyAgentFlowRetention(
  store: AgentFlowRunStateStore,
  runId: string,
  workflow: AgentFlowWorkflow,
  status: AgentFlowRunStatus
): void {
  const ruleName = retentionRuleName(status);
  const rule = ruleName === undefined ? undefined : mapping(workflow.retention?.[ruleName]);
  if (ruleName === undefined || rule === undefined) return;

  const afterDays = number(rule.after_days);
  const keepAllForDays = number(rule.keep_all_for_days);
  if ((afterDays ?? 0) > 0 || (keepAllForDays ?? 0) > 0 || rule.ask_user === true) {
    if (hasRetentionEvent(store, runId, "retention.deferred", ruleName)) return;
    store.appendRunEvent(runId, {
      type: "retention.deferred",
      payload: {
        rule: ruleName,
        ...(afterDays === undefined ? {} : { afterDays }),
        ...(keepAllForDays === undefined ? {} : { keepAllForDays }),
        ...(rule.ask_user === true ? { approvalRequired: true } : {})
      }
    });
    return;
  }

  const deletions = stringList(rule.delete);
  if (deletions.length === 0) return;
  const keepPatterns = stringList(rule.keep);
  const protectedPaths = new Set([
    AGENT_FLOW_FINAL_SUMMARY_PATH,
    ...approvedArtifactPaths(store, runId, workflow)
  ]);
  const candidates = store.listArtifactMetadata(runId)
    .filter((artifact) =>
      artifact.status !== "missing"
      && artifact.kind !== "failure_payload"
      && artifact.kind !== "failure_attachment"
      && artifact.kind !== "decision_record"
      && deletions.some((pattern) => matchesPolicyGlob(artifact.declaredPath, pattern))
      && !protectedPaths.has(artifact.declaredPath)
      && !keepPatterns.some((pattern) => matchesPolicyGlob(artifact.declaredPath, pattern))
    )
    .sort((left, right) => left.declaredPath.localeCompare(right.declaredPath));
  const deleted: string[] = [];

  for (const artifact of candidates) {
    const decision = evaluateAgentFlowPolicy(workflow, {
      kind: "cleanup",
      rootPath: store.getArtifactPolicyRoot(runId),
      recursive: false,
      runStatus: status,
      paths: [artifact.declaredPath],
      ageDays: 0
    });
    if (decision.status !== "allow") {
      const eventType = decision.status === "pause" ? "retention.deferred" : "retention.skipped";
      if (!hasRetentionEvent(store, runId, eventType, ruleName, artifact.declaredPath)) {
        store.appendRunEvent(runId, {
          type: eventType,
          payload: {
            rule: ruleName,
            artifact: artifact.declaredPath,
            code: decision.code,
            message: decision.message
          }
        });
      }
      continue;
    }
    try {
      store.deleteArtifactBacking(runId, artifact.declaredPath);
      deleted.push(artifact.declaredPath);
    } catch (error) {
      store.appendRunEvent(runId, {
        type: "retention.failed",
        payload: {
          rule: ruleName,
          artifact: artifact.declaredPath,
          message: error instanceof Error ? error.message : String(error)
        }
      });
    }
  }

  if (deleted.length > 0) {
    store.appendRunEvent(runId, {
      type: "retention.deleted",
      payload: { rule: ruleName, artifacts: deleted }
    });
  }
}

function approvedArtifactPaths(
  store: AgentFlowRunStateStore,
  runId: string,
  workflow: AgentFlowWorkflow
): string[] {
  const paths = new Set<string>();
  for (const approval of store.listApprovals(runId)) {
    if (approval.status !== "approved") continue;
    const evidence = Array.isArray(approval.context.evidence) ? approval.context.evidence : [];
    for (const entry of evidence) {
      const path = mapping(entry)?.path;
      if (typeof path === "string") paths.add(path);
    }
    if (typeof approval.context.output === "string") paths.add(approval.context.output);
    if (approval.stepId !== null) {
      const invalidatedBy = stringList(mapping(workflow.approvals?.[approval.stepId])?.invalidated_by);
      for (const path of invalidatedBy) paths.add(path);
    }
  }
  return [...paths];
}

export function agentFlowPipelineEffectsFinalized(
  store: AgentFlowRunStateStore,
  runId: string,
  status: Extract<AgentFlowRunStatus, "completed" | "failed" | "paused" | "cancelled">
): boolean {
  const events = store.listEvents(runId);
  const transition = latestStatusTransition(events, status);
  if (transition === undefined) return false;
  return events.some((event) =>
    event.type === "pipeline.effects.finalized"
    && eventPayloadNumber(event, "transitionSequence") === transition.sequence
    && eventPayloadString(event, "status") === status
  );
}

export function markAgentFlowPipelineEffectsFinalized(
  store: AgentFlowRunStateStore,
  runId: string,
  status: Extract<AgentFlowRunStatus, "completed" | "failed" | "paused" | "cancelled">
): void {
  if (agentFlowPipelineEffectsFinalized(store, runId, status)) return;
  const transition = latestStatusTransition(store.listEvents(runId), status);
  if (transition === undefined) {
    throw new Error(`Cannot finalize pipeline effects for ${runId} without a ${status} transition event.`);
  }
  store.appendRunEvent(runId, {
    type: "pipeline.effects.finalized",
    payload: { status, transitionSequence: transition.sequence }
  });
}

function retentionRuleName(status: AgentFlowRunStatus): string | undefined {
  if (status === "completed") return "on_success";
  if (status === "failed") return "on_failure";
  if (status === "cancelled") return "on_cancelled";
  return undefined;
}

function number(value: AgentFlowYamlValue | undefined): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function latestStatusTransition(
  events: AgentFlowEventRecord[],
  status: Extract<AgentFlowRunStatus, "completed" | "failed" | "paused" | "cancelled">
): AgentFlowEventRecord | undefined {
  const types = status === "paused"
    ? new Set(["run.pause", "run.paused"])
    : status === "cancelled"
      ? new Set(["run.cancel", "run.cancelled"])
      : new Set([`run.${status}`]);
  return events.filter((event) => types.has(event.type)).at(-1);
}

function hasRetentionEvent(
  store: AgentFlowRunStateStore,
  runId: string,
  type: string,
  rule: string,
  artifact?: string
): boolean {
  return store.listEvents(runId).some((event) =>
    event.type === type
    && eventPayloadString(event, "rule") === rule
    && eventPayloadString(event, "artifact") === artifact
  );
}

function eventPayloadString(event: AgentFlowEventRecord, key: string): string | undefined {
  const payload = event.payload;
  if (payload === null || typeof payload !== "object" || Array.isArray(payload)) return undefined;
  return typeof payload[key] === "string" ? payload[key] : undefined;
}

function eventPayloadNumber(event: AgentFlowEventRecord, key: string): number | undefined {
  const payload = event.payload;
  if (payload === null || typeof payload !== "object" || Array.isArray(payload)) return undefined;
  return typeof payload[key] === "number" ? payload[key] : undefined;
}
