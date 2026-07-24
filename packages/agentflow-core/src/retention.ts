import { evaluateAgentflowPolicy } from "./policy";
import { mapping, matchesPolicyGlob, stringList } from "./policy_utils";
import type {
  AgentflowArtifactRecord,
  AgentflowEventRecord,
  AgentflowRunStateStore,
  AgentflowRunStatus
} from "./run_state";
import { AGENTFLOW_FINAL_SUMMARY_PATH } from "./run_state";
import type { AgentflowWorkflow, AgentflowYamlValue } from "./workflow";

export { AGENTFLOW_FINAL_SUMMARY_PATH } from "./run_state";

export interface AgentflowFinalSummaryInput {
  status: Extract<AgentflowRunStatus, "completed" | "failed" | "paused" | "cancelled">;
  completedSteps: string[];
  message?: string;
}

export function writeAgentflowFinalSummary(
  store: AgentflowRunStateStore,
  runId: string,
  workflow: AgentflowWorkflow,
  input: AgentflowFinalSummaryInput
): AgentflowArtifactRecord {
  const existing = store.getArtifact(runId, AGENTFLOW_FINAL_SUMMARY_PATH);
  if (existing !== null && existing.kind !== "run_summary") {
    throw new Error(`Runtime summary path ${AGENTFLOW_FINAL_SUMMARY_PATH} is already owned by ${existing.id}.`);
  }
  const completed = input.completedSteps.length === 0
    ? "- None"
    : input.completedSteps.map((stepId) => `- ${stepId}`).join("\n");
  const content = [
    "# Agentflow run summary",
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
      if (store.readArtifact(runId, AGENTFLOW_FINAL_SUMMARY_PATH).content.equals(Buffer.from(content))) {
        return existing;
      }
    } catch {
      // Rewrite missing or stale summary backings through the normal artifact path.
    }
  }

  return store.writeArtifact({
    id: "run:final-summary",
    runId,
    path: AGENTFLOW_FINAL_SUMMARY_PATH,
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

export function applyAgentflowRetention(
  store: AgentflowRunStateStore,
  runId: string,
  workflow: AgentflowWorkflow,
  status: AgentflowRunStatus
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
  const keep = [...stringList(rule.keep), AGENTFLOW_FINAL_SUMMARY_PATH];
  const candidates = store.listArtifactMetadata(runId)
    .filter((artifact) =>
      artifact.status !== "missing"
      && deletions.some((pattern) => matchesPolicyGlob(artifact.declaredPath, pattern))
      && !keep.some((pattern) => matchesPolicyGlob(artifact.declaredPath, pattern))
    )
    .sort((left, right) => left.declaredPath.localeCompare(right.declaredPath));
  const deleted: string[] = [];

  for (const artifact of candidates) {
    const decision = evaluateAgentflowPolicy(workflow, {
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

export function agentflowPipelineEffectsFinalized(
  store: AgentflowRunStateStore,
  runId: string,
  status: Extract<AgentflowRunStatus, "completed" | "failed" | "paused" | "cancelled">
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

export function markAgentflowPipelineEffectsFinalized(
  store: AgentflowRunStateStore,
  runId: string,
  status: Extract<AgentflowRunStatus, "completed" | "failed" | "paused" | "cancelled">
): void {
  if (agentflowPipelineEffectsFinalized(store, runId, status)) return;
  const transition = latestStatusTransition(store.listEvents(runId), status);
  if (transition === undefined) {
    throw new Error(`Cannot finalize pipeline effects for ${runId} without a ${status} transition event.`);
  }
  store.appendRunEvent(runId, {
    type: "pipeline.effects.finalized",
    payload: { status, transitionSequence: transition.sequence }
  });
}

function retentionRuleName(status: AgentflowRunStatus): string | undefined {
  if (status === "completed") return "on_success";
  if (status === "failed") return "on_failure";
  if (status === "cancelled") return "on_cancelled";
  return undefined;
}

function number(value: AgentflowYamlValue | undefined): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function latestStatusTransition(
  events: AgentflowEventRecord[],
  status: Extract<AgentflowRunStatus, "completed" | "failed" | "paused" | "cancelled">
): AgentflowEventRecord | undefined {
  const types = status === "paused"
    ? new Set(["run.pause", "run.paused"])
    : status === "cancelled"
      ? new Set(["run.cancel", "run.cancelled"])
      : new Set([`run.${status}`]);
  return events.filter((event) => types.has(event.type)).at(-1);
}

function hasRetentionEvent(
  store: AgentflowRunStateStore,
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

function eventPayloadString(event: AgentflowEventRecord, key: string): string | undefined {
  const payload = event.payload;
  if (payload === null || typeof payload !== "object" || Array.isArray(payload)) return undefined;
  return typeof payload[key] === "string" ? payload[key] : undefined;
}

function eventPayloadNumber(event: AgentflowEventRecord, key: string): number | undefined {
  const payload = event.payload;
  if (payload === null || typeof payload !== "object" || Array.isArray(payload)) return undefined;
  return typeof payload[key] === "number" ? payload[key] : undefined;
}
