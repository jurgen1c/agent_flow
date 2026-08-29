import crypto from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { lintAgentFlowWorkflow, validateAgentFlowWorkflow } from "./validation";
import {
  executeAgentFlowCommandPipeline,
  resumeAgentFlowCommandPipeline,
  validateAgentFlowPipelineWaitingState
} from "./command_execution";
import {
  createAgentFlowArtifactTransformRegistry,
  type AgentFlowArtifactTransformRegistry
} from "./artifact_transform";
import {
  createAgentFlowMcpCallRegistry,
  type AgentFlowMcpCallRegistry
} from "./mcp_call";
import {
  createAgentFlowNotificationRegistry,
  type AgentFlowNotificationRegistry
} from "./notifications";
import {
  createAgentFlowSessionProviderRegistry,
  type AgentFlowSessionProviderRegistry
} from "./session_request";
import {
  createAgentFlowWorkflowRegistryFromSnapshot,
  createAgentFlowWorkflowRegistry,
  type AgentFlowWorkflowRegistry
} from "./recovery";
import { transitionAgentFlowLifecycleRun } from "./lifecycle";
import {
  AgentFlowRunStateError,
  type AgentFlowApprovalRecord,
  type AgentFlowRunRecord,
  type AgentFlowRunStateStore,
  type AgentFlowRunStateValue
} from "./run_state";
import type { AgentFlowWorkflow } from "./workflow";

export const AGENT_FLOW_RUN_ACTIONS = [
  "approve", "reject", "provide_input", "resume", "pause", "cancel"
] as const;
export const MAX_AGENT_FLOW_RUN_ACTION_ANSWER_DEPTH = 50;

export type AgentFlowRunAction = typeof AGENT_FLOW_RUN_ACTIONS[number];

export interface AgentFlowRunActionRuntime {
  transforms?: AgentFlowArtifactTransformRegistry;
  sessionProviders?: AgentFlowSessionProviderRegistry;
  mcpCalls?: AgentFlowMcpCallRegistry;
  notifications?: AgentFlowNotificationRegistry;
  workflows?: AgentFlowWorkflowRegistry;
}

export interface AgentFlowRunActionWarning {
  code: string;
  message: string;
  severity: "warning" | "danger";
}

export interface AgentFlowRunActionAvailability {
  action: AgentFlowRunAction;
  label: string;
  enabled: boolean;
  reason: string | null;
  confirmation: string | null;
}

export interface AgentFlowRunActionWaitingState {
  kind: "approval" | "manual_gate" | "input_request" | "disagreement" | "provider_session" | "workflow";
  stepId: string;
  prompt: string;
  validOutcomes: string[];
  saveAs: string | null;
  approvalId: string | null;
  sessionId?: string;
  childRunId?: string;
  childStatus?: "paused" | "completed" | "failed" | "cancelled";
  nestedKind?: Exclude<AgentFlowRunActionWaitingState["kind"], "workflow">;
}

export interface AgentFlowRunActionSnapshot {
  runId: string;
  status: AgentFlowRunRecord["status"];
  updatedAt: string;
  guard: string;
  waiting: AgentFlowRunActionWaitingState | null;
  staleApprovals: Array<{ id: string; stepId: string | null; detected: boolean }>;
  warnings: AgentFlowRunActionWarning[];
  actions: AgentFlowRunActionAvailability[];
}

export interface ExecuteAgentFlowRunActionInput {
  action: AgentFlowRunAction;
  guard: string;
  answer?: AgentFlowRunStateValue;
}

export interface AgentFlowRunActionResult {
  action: AgentFlowRunAction;
  changed: boolean;
  status: AgentFlowRunRecord["status"];
  completedSteps: string[];
  message: string | null;
  snapshot: AgentFlowRunActionSnapshot;
}

export class AgentFlowRunActionError extends Error {
  constructor(
    message: string,
    readonly code: string,
    readonly status: number
  ) {
    super(message);
    this.name = "AgentFlowRunActionError";
  }
}

export function buildAgentFlowRunActionSnapshot(
  store: AgentFlowRunStateStore,
  runId: string
): AgentFlowRunActionSnapshot {
  const run = store.getRun(runId);
  if (run === null) {
    throw new AgentFlowRunStateError(`Agent Flow run ${runId} was not found.`, "AGENT_FLOW_RUN_NOT_FOUND");
  }
  let waitingResult = parseWaitingState(run.context.waiting);
  let waitingRun = run;
  if (waitingResult.error === null && run.context.waiting !== undefined) {
    try {
      validateAgentFlowPipelineWaitingState(run.context.waiting);
    } catch (error) {
      waitingResult = {
        waiting: null,
        error: `The persisted waiting state is incomplete or malformed, so interaction actions are disabled: ${error instanceof Error ? error.message : String(error)}`
      };
    }
  }
  if (waitingResult.error === null && waitingResult.waiting?.kind === "workflow") {
    const nested = resolveNestedActionWaitingState(store, waitingResult.waiting);
    waitingResult = { waiting: nested.waiting, error: nested.error };
    waitingRun = nested.run ?? run;
  }
  const approvals = store.listApprovals(waitingRun.id);
  let guardedLineage: Array<{
    run: AgentFlowRunRecord;
    approvals: AgentFlowApprovalRecord[];
  }> = [{
    run,
    approvals: run.id === waitingRun.id ? approvals : store.listApprovals(run.id)
  }];
  const warnings: AgentFlowRunActionWarning[] = [];
  let staleApprovals: AgentFlowRunActionSnapshot["staleApprovals"] = [];
  let approvalEvidenceValid = true;
  try {
    const guardedRuns = nestedActionApprovalLineage(store, run, waitingRun);
    if (waitingResult.waiting?.kind === "workflow" && waitingResult.waiting.childStatus === "completed") {
      guardedRuns.push(...completedPromotedDescendants(store, waitingRun, new Set(guardedRuns.map((entry) => entry.id))));
    }
    guardedLineage = guardedRuns.map((lineageRun) => ({
      run: lineageRun,
      approvals: lineageRun.id === waitingRun.id ? approvals : store.listApprovals(lineageRun.id)
    }));
    staleApprovals = guardedLineage.flatMap(({ run: lineageRun, approvals: lineageApprovals }) =>
      detectStaleApprovals(
        store,
        lineageRun.id,
        lineageApprovals,
        lineageRun.id === waitingRun.id ? waitingResult.waiting?.approvalId ?? undefined : undefined
      )
    );
  } catch (error) {
    approvalEvidenceValid = false;
    warnings.push({
      code: "action.approval.evidence_invalid",
      message: `Persisted approval evidence cannot be inspected safely, so run actions are disabled: ${error instanceof Error ? error.message : String(error)}`,
      severity: "danger"
    });
  }
  const workflow = persistedWorkflow(run);
  let workflowValid = false;

  if (workflow === null) {
    warnings.push({
      code: "action.workflow.invalid",
      message: "The persisted workflow is missing or malformed, so run actions are disabled.",
      severity: "danger"
    });
  } else {
    try {
      const validation = validateAgentFlowWorkflow(workflow);
      workflowValid = validation.valid;
      if (!validation.valid) {
        warnings.push({
          code: "action.workflow.invalid",
          message: "The persisted workflow no longer passes validation, so run actions are disabled.",
          severity: "danger"
        });
      } else {
        for (const warning of lintAgentFlowWorkflow(workflow).warnings) {
          warnings.push({
            code: warning.code,
            message: `${warning.path}: ${warning.message}`,
            severity: "warning"
          });
        }
      }
    } catch (error) {
      warnings.push({
        code: "action.workflow.invalid",
        message: `The persisted workflow cannot be interpreted safely: ${error instanceof Error ? error.message : String(error)}`,
        severity: "danger"
      });
    }
  }

  if (waitingResult.error !== null) {
    warnings.push({
      code: "action.waiting.invalid",
      message: waitingResult.error,
      severity: "danger"
    });
  }
  if (staleApprovals.length > 0) {
    warnings.push({
      code: "action.approval.stale",
      message: `Stale approval${staleApprovals.length === 1 ? "" : "s"} ${staleApprovals.map((approval) => approval.id).join(", ")} must be rerun before relying on a decision.`,
      severity: "danger"
    });
  }

  const waiting = waitingResult.waiting;
  let disagreementEvidence: Array<{
    path: string;
    expectedChecksum: string;
    currentChecksum: string | null;
    status: string;
  }> = [];
  let disagreementEvidenceValid = true;
  if (effectiveWaitingKind(waiting) === "disagreement") {
    try {
      const persistedWaiting = waitingRun.context.waiting as Record<string, AgentFlowRunStateValue>;
      disagreementEvidence = validatedApprovalEvidence(persistedWaiting.evidence, true).map((entry) => {
        const metadata = store.getArtifactMetadataForInspection(waitingRun.id, entry.path);
        if (metadata === null) {
          return { ...entry, expectedChecksum: entry.checksum, currentChecksum: null, status: "missing" };
        }
        const inspected = store.inspectArtifactRecordForActionGuard(metadata);
        return {
          path: entry.path,
          expectedChecksum: entry.checksum,
          currentChecksum: inspected.checksum,
          status: inspected.status
        };
      });
      const staleEvidence = disagreementEvidence.some((entry) =>
        !["available", "overwritten"].includes(entry.status)
          || entry.currentChecksum !== entry.expectedChecksum
      );
      if (staleEvidence) {
        disagreementEvidenceValid = false;
        warnings.push({
          code: "action.disagreement.evidence_stale",
          message: "Disagreement evidence changed or is unavailable. Refresh the run and rerun the review before deciding.",
          severity: "danger"
        });
      }
    } catch (error) {
      disagreementEvidenceValid = false;
      warnings.push({
        code: "action.disagreement.evidence_invalid",
        message: `Persisted disagreement evidence cannot be inspected safely, so run actions are disabled: ${error instanceof Error ? error.message : String(error)}`,
        severity: "danger"
      });
    }
  }
  const activeApproval = waiting?.approvalId === null || waiting?.approvalId === undefined
    ? null
    : approvals.find((approval) => approval.id === waiting.approvalId) ?? null;
  const interactionKind = effectiveWaitingKind(waiting);
  if (interactionKind === "approval" && activeApproval !== null) {
    try {
      const persistedWaiting = waitingRun.context.waiting as Record<string, AgentFlowRunStateValue>;
      const waitingEvidence = validatedApprovalEvidence(persistedWaiting.evidence, true);
      const approvalEvidence = validatedApprovalEvidence(activeApproval.context.evidence, true);
      if (!isDeepStrictEqual(approvalEvidence, waitingEvidence)) {
        throw new Error("the active approval evidence does not match the persisted waiting state");
      }
    } catch (error) {
      approvalEvidenceValid = false;
      if (!warnings.some((warning) => warning.code === "action.approval.evidence_invalid")) {
        warnings.push({
          code: "action.approval.evidence_invalid",
          message: `Persisted approval evidence cannot be inspected safely, so run actions are disabled: ${error instanceof Error ? error.message : String(error)}`,
          severity: "danger"
        });
      }
    }
  }
  const activeApprovalBlockReason = waiting?.approvalId === null || waiting?.approvalId === undefined
    ? null
    : activeApproval === null
      ? "The active approval record is missing. Refresh or recover the run before deciding."
      : activeApproval.status !== "requested"
        ? `The active approval was already finalized as ${activeApproval.status}. Refresh the run before deciding.`
        : null;
  if (activeApprovalBlockReason !== null) {
    warnings.push({
      code: "action.approval.finalized",
      message: activeApprovalBlockReason,
      severity: "danger"
    });
  }
  const actions = actionAvailability(
    run,
    waiting,
    workflowValid && approvalEvidenceValid && disagreementEvidenceValid && waitingResult.error === null,
    staleApprovals.length > 0,
    activeApprovalBlockReason
  );
  const guardPayload = {
    lineage: guardedLineage,
    waiting,
    disagreementEvidence,
    staleApprovals,
    warnings,
    actions
  };

  return {
    runId: run.id,
    status: run.status,
    updatedAt: run.updatedAt,
    guard: crypto.createHash("sha256").update(JSON.stringify(guardPayload)).digest("base64url"),
    waiting,
    staleApprovals,
    warnings,
    actions
  };
}

export async function executeAgentFlowRunAction(
  store: AgentFlowRunStateStore,
  runId: string,
  input: ExecuteAgentFlowRunActionInput,
  runtime: AgentFlowRunActionRuntime = {}
): Promise<AgentFlowRunActionResult> {
  assertRunAction(input.action);
  if (!/^[A-Za-z0-9_-]{43}$/.test(input.guard)) {
    throw new AgentFlowRunActionError(
      "Run action guard must come from the latest action snapshot.",
      "AGENT_FLOW_ACTION_GUARD_INVALID",
      400
    );
  }
  if (input.action === "provide_input" && !("answer" in input)) {
    throw new AgentFlowRunActionError(
      "Providing input requires an answer value.",
      "AGENT_FLOW_ACTION_ANSWER_REQUIRED",
      400
    );
  }
  if (input.action === "provide_input" && !isValidActionAnswer(input.answer)) {
    throw new AgentFlowRunActionError(
      `Input answers must contain only finite JSON values nested no more than ${MAX_AGENT_FLOW_RUN_ACTION_ANSWER_DEPTH} levels.`,
      "AGENT_FLOW_ACTION_BODY_INVALID",
      400
    );
  }
  const assertCurrent = (): AgentFlowRunActionSnapshot => {
    const snapshot = buildAgentFlowRunActionSnapshot(store, runId);
    if (!safeEqual(snapshot.guard, input.guard)) {
      throw new AgentFlowRunActionError(
        "Run state or approval evidence changed after this action was loaded. Refresh the run and review the latest warnings before trying again.",
        "AGENT_FLOW_ACTION_STALE",
        409
      );
    }
    const availability = snapshot.actions.find((candidate) => candidate.action === input.action)!;
    if (!availability.enabled) {
      throw new AgentFlowRunActionError(
        availability.reason ?? `Action ${input.action} is not available for this run.`,
        "AGENT_FLOW_ACTION_NOT_ALLOWED",
        409
      );
    }
    return snapshot;
  };
  const initialSnapshot = assertCurrent();
  const run = store.getRun(runId)!;
  const workflow = persistedWorkflow(run);
  if (workflow === null) {
    throw new AgentFlowRunActionError(
      "The persisted workflow is missing or malformed.",
      "AGENT_FLOW_ACTION_WORKFLOW_INVALID",
      409
    );
  }
  const workflows = runtime.workflows ?? persistedWorkflowRegistry(run, workflow);

  let changed = true;
  let completedSteps: string[] = [];
  let message: string | null = null;
  if (["approve", "reject", "provide_input"].includes(input.action)) {
    const response = input.action === "provide_input"
      ? { answer: input.answer! }
      : {
          outcome: input.action === "reject" && effectiveWaitingKind(initialSnapshot.waiting) === "disagreement"
            ? "request_changes"
            : input.action,
          decidedBy: "local-ui"
        };
    const execution = await resumeAgentFlowCommandPipeline(
      store,
      runId,
      workflow,
      response,
      runtime.transforms ?? createAgentFlowArtifactTransformRegistry(),
      runtime.sessionProviders ?? createAgentFlowSessionProviderRegistry(),
      runtime.mcpCalls ?? createAgentFlowMcpCallRegistry(),
      runtime.notifications ?? createAgentFlowNotificationRegistry(),
      workflows,
      assertCurrent
    );
    completedSteps = execution.completedSteps;
    message = execution.message ?? null;
  } else if (input.action === "resume") {
    const nestedTerminal = initialSnapshot.waiting?.kind === "workflow"
      && initialSnapshot.waiting.childStatus !== undefined;
    const execution = nestedTerminal
      ? await resumeAgentFlowCommandPipeline(
          store,
          runId,
          workflow,
          { outcome: "continue" },
          runtime.transforms ?? createAgentFlowArtifactTransformRegistry(),
          runtime.sessionProviders ?? createAgentFlowSessionProviderRegistry(),
          runtime.mcpCalls ?? createAgentFlowMcpCallRegistry(),
          runtime.notifications ?? createAgentFlowNotificationRegistry(),
          workflows,
          assertCurrent
        )
      : await executeAgentFlowCommandPipeline(
          store,
          runId,
          workflow,
          runtime.transforms ?? createAgentFlowArtifactTransformRegistry(),
          runtime.sessionProviders ?? createAgentFlowSessionProviderRegistry(),
          runtime.mcpCalls ?? createAgentFlowMcpCallRegistry(),
          runtime.notifications ?? createAgentFlowNotificationRegistry(),
          workflows,
          () => {
            store.withRunStateTransaction(runId, () => {
              assertCurrent();
              store.transitionRunWithEvent(runId, {
                status: "pending",
                allowedFrom: ["paused"],
                event: { type: "run.resume", payload: { status: "pending" } }
              });
            });
          },
          assertCurrent
        );
    completedSteps = execution.completedSteps;
    message = execution.message ?? null;
  } else {
    const result = transitionAgentFlowLifecycleRun(
      store,
      runId,
      input.action as "pause" | "cancel",
      runtime.notifications ?? createAgentFlowNotificationRegistry(),
      assertCurrent
    );
    changed = result.changed;
  }
  const current = store.getRun(runId)!;
  return {
    action: input.action,
    changed,
    status: current.status,
    completedSteps,
    message,
    snapshot: buildAgentFlowRunActionSnapshot(store, runId)
  };
}

function isValidActionAnswer(
  value: unknown,
  ancestors = new Set<object>(),
  depth = 0
): value is AgentFlowRunStateValue {
  if (depth > MAX_AGENT_FLOW_RUN_ACTION_ANSWER_DEPTH) return false;
  if (value === null || typeof value === "string" || typeof value === "boolean") return true;
  if (typeof value === "number") return Number.isFinite(value);
  if (typeof value !== "object" || ancestors.has(value)) return false;
  ancestors.add(value);
  let valid: boolean;
  if (Array.isArray(value)) {
    valid = Object.keys(value).every((key) => /^(0|[1-9]\d*)$/.test(key))
      && Object.keys(value).length === value.length
      && value.every((entry) => isValidActionAnswer(entry, ancestors, depth + 1));
  } else {
    const prototype = Object.getPrototypeOf(value);
    valid = (prototype === Object.prototype || prototype === null)
      && Object.values(value).every((entry) => isValidActionAnswer(entry, ancestors, depth + 1));
  }
  ancestors.delete(value);
  return valid;
}

function detectStaleApprovals(
  store: AgentFlowRunStateStore,
  runId: string,
  approvals: AgentFlowApprovalRecord[],
  activeApprovalId?: string
): AgentFlowRunActionSnapshot["staleApprovals"] {
  const stale = new Map<string, { id: string; stepId: string | null; detected: boolean }>();
  for (const approval of relevantApprovals(approvals, activeApprovalId)) {
    if (approval.status === "stale") {
      stale.set(approval.id, { id: approval.id, stepId: approval.stepId, detected: false });
      continue;
    }
    if (approval.status !== "requested" && approval.status !== "approved") continue;
    const evidence = approval.context.evidence === undefined
      ? []
      : validatedApprovalEvidence(approval.context.evidence, false);
    const paths = new Map<string, string | null>();
    for (const entry of evidence) {
      paths.set(entry.path, entry.checksum);
    }
    if (approval.stepId !== null) {
      for (const path of store.approvalInvalidationPaths(runId, approval.stepId)) {
        if (!paths.has(path)) paths.set(path, null);
      }
    }
    if (approval.status === "approved" && typeof approval.context.output === "string") {
      paths.set(approval.context.output, null);
    }
    const changed = [...paths].some(([path, expectedChecksum]) => {
      const metadata = store.getArtifactMetadataForInspection(runId, path);
      if (metadata === null) return true;
      const inspected = store.inspectArtifactRecordForActionGuard(metadata);
      return !["available", "overwritten"].includes(inspected.status)
        || (expectedChecksum !== null && inspected.checksum !== expectedChecksum);
    });
    if (changed) stale.set(approval.id, { id: approval.id, stepId: approval.stepId, detected: true });
  }
  return [...stale.values()].sort((left, right) => left.id.localeCompare(right.id));
}

function nestedActionApprovalLineage(
  store: AgentFlowRunStateStore,
  rootRun: AgentFlowRunRecord,
  waitingRun: AgentFlowRunRecord
): AgentFlowRunRecord[] {
  const lineage: AgentFlowRunRecord[] = [];
  const visited = new Set<string>();
  let current: AgentFlowRunRecord | null = waitingRun;
  while (current !== null && !visited.has(current.id)) {
    visited.add(current.id);
    lineage.push(current);
    if (current.id === rootRun.id) return lineage.reverse();
    current = current.parentRunId === null ? null : store.getRun(current.parentRunId);
  }
  throw new AgentFlowRunStateError(
    `Nested action run ${waitingRun.id} is not in run ${rootRun.id}'s parent lineage.`,
    "AGENT_FLOW_RUN_LINEAGE_INVALID"
  );
}

function completedPromotedDescendants(
  store: AgentFlowRunStateStore,
  rootRun: AgentFlowRunRecord,
  visited: Set<string>
): AgentFlowRunRecord[] {
  const descendants: AgentFlowRunRecord[] = [];
  const pending = [rootRun];
  while (pending.length > 0) {
    const parent = pending.pop()!;
    const latestChildByStep = new Map<string, string>();
    for (const event of store.listEvents(parent.id)) {
      if (event.type !== "workflow.outputs.promoted" || event.stepId === null
          || event.payload === null || typeof event.payload !== "object" || Array.isArray(event.payload)) continue;
      const childRunId = event.payload.childRunId;
      if (typeof childRunId === "string" && childRunId.trim().length > 0) {
        latestChildByStep.set(event.stepId, childRunId.trim());
      }
    }
    for (const childRunId of latestChildByStep.values()) {
      if (visited.has(childRunId)) continue;
      const child = store.getRun(childRunId);
      if (child?.parentRunId !== parent.id || child.status !== "completed") continue;
      visited.add(child.id);
      descendants.push(child);
      pending.push(child);
    }
  }
  return descendants;
}

function relevantApprovals(
  approvals: AgentFlowApprovalRecord[],
  activeApprovalId?: string
): AgentFlowApprovalRecord[] {
  const relevant = new Map<string, AgentFlowApprovalRecord>();
  const latestDecisionByStep = new Map<string, AgentFlowApprovalRecord>();
  for (const approval of approvals) {
    if (approval.stepId === null) {
      relevant.set(approval.id, approval);
      continue;
    }
    if (approval.status === "requested" || approval.status === "cancelled") continue;
    const previous = latestDecisionByStep.get(approval.stepId);
    if (previous === undefined || compareApprovalRecency(approval, previous) > 0) {
      latestDecisionByStep.set(approval.stepId, approval);
    }
  }
  for (const approval of latestDecisionByStep.values()) relevant.set(approval.id, approval);
  const active = activeApprovalId === undefined
    ? undefined
    : approvals.find((approval) => approval.id === activeApprovalId);
  if (active !== undefined) relevant.set(active.id, active);
  return [...relevant.values()];
}

function compareApprovalRecency(left: AgentFlowApprovalRecord, right: AgentFlowApprovalRecord): number {
  const attemptDifference = approvalAttempt(left.id) - approvalAttempt(right.id);
  if (attemptDifference !== 0) return attemptDifference;
  const updatedDifference = left.updatedAt.localeCompare(right.updatedAt);
  return updatedDifference !== 0 ? updatedDifference : left.id.localeCompare(right.id);
}

function approvalAttempt(id: string): number {
  return Number(/:attempt-(\d+)(?::|$)/.exec(id)?.[1] ?? 0);
}

function validatedApprovalEvidence(
  value: AgentFlowRunStateValue | undefined,
  required: boolean
): Array<{ path: string; checksum: string }> {
  if (!Array.isArray(value) || (required && value.length === 0)) {
    throw new Error(required
      ? "the active approval evidence must be a non-empty array"
      : "approval evidence must be an array");
  }
  return value.map((entry) => {
    if (entry === null || typeof entry !== "object" || Array.isArray(entry)) {
      throw new Error("approval evidence entries must be objects");
    }
    const record = entry as Record<string, AgentFlowRunStateValue>;
    if (typeof record.path !== "string" || typeof record.checksum !== "string"
        || !/^sha256:[a-f0-9]{64}$/.test(record.checksum)) {
      throw new Error("approval evidence entries require a path and SHA-256 checksum");
    }
    return { path: record.path, checksum: record.checksum };
  });
}

function actionAvailability(
  run: AgentFlowRunRecord,
  waiting: AgentFlowRunActionWaitingState | null,
  actionStateValid: boolean,
  hasStaleApprovals: boolean,
  activeApprovalBlockReason: string | null
): AgentFlowRunActionAvailability[] {
  const unavailable = actionStateValid ? null : "The persisted workflow, waiting state, and approval evidence must be valid before actions can run.";
  const outcomeReason = hasStaleApprovals
    ? "Relied-on approval evidence is stale. Refresh the run and rerun the affected approval before continuing."
    : waiting === null
      ? "This run is not waiting for a gate decision."
      : null;
  const interactionKind = effectiveWaitingKind(waiting);
  const canApprove = actionStateValid && !hasStaleApprovals && activeApprovalBlockReason === null
    && waiting !== null
    && (interactionKind === "approval" || interactionKind === "manual_gate" || interactionKind === "disagreement")
    && waiting.validOutcomes.includes("approve");
  const canReject = actionStateValid && !hasStaleApprovals && activeApprovalBlockReason === null
    && waiting !== null
    && (interactionKind === "approval" || interactionKind === "manual_gate" || interactionKind === "disagreement")
    && waiting.validOutcomes.includes(interactionKind === "disagreement" ? "request_changes" : "reject");
  const canInput = actionStateValid && !hasStaleApprovals && interactionKind === "input_request";
  const canContinueWorkflow = waiting?.kind === "workflow" && waiting.childStatus !== undefined;
  const staleApprovalBlocksResume = hasStaleApprovals
    && (!canContinueWorkflow || waiting.childStatus === "paused" || waiting.childStatus === "completed");
  const canResume = actionStateValid && !staleApprovalBlocksResume && run.status === "paused"
    && (waiting === null || canContinueWorkflow);
  const canPause = actionStateValid && ["pending", "running", "waiting"].includes(run.status);
  const canCancel = actionStateValid && ["pending", "running", "waiting", "paused"].includes(run.status);
  return [
    action("approve", "Approve", canApprove, unavailable ?? activeApprovalBlockReason ?? outcomeReason ?? "Approve is not a valid outcome for this gate.", "Confirm approval after reviewing the warnings and evidence shown above."),
    action("reject", "Reject", canReject, unavailable ?? activeApprovalBlockReason ?? outcomeReason ?? "Reject is not a valid outcome for this gate.", "Reject this gate and continue along its configured rejection path?"),
    action("provide_input", "Provide input", canInput, unavailable ?? "This run is not waiting for input.", null),
    action("resume", canContinueWorkflow
      ? waiting?.childStatus === "paused" ? "Resume child" : "Settle child"
      : "Resume", canResume, unavailable ?? (waiting === null
      ? "Only a paused run can resume."
      : canContinueWorkflow
        ? "The linked child workflow cannot be continued."
      : interactionKind === "provider_session"
        ? `Reset the unavailable provider session with agent-flow resume ${run.id} --reset-session ${waiting.sessionId}.`
        : "Respond to the waiting interaction instead of using plain resume."), null),
    action("pause", "Pause", canPause, unavailable ?? "Only a pending, running, or waiting run can pause.", null),
    action("cancel", "Cancel run", canCancel, unavailable ?? "This run is already terminal.", "Cancel this run? This stops further workflow execution."),
  ];
}

function action(
  actionName: AgentFlowRunAction,
  label: string,
  enabled: boolean,
  disabledReason: string,
  confirmation: string | null
): AgentFlowRunActionAvailability {
  return { action: actionName, label, enabled, reason: enabled ? null : disabledReason, confirmation };
}

function parseWaitingState(value: AgentFlowRunStateValue | undefined): {
  waiting: AgentFlowRunActionWaitingState | null;
  error: string | null;
} {
  if (value === undefined) return { waiting: null, error: null };
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    return { waiting: null, error: "The persisted waiting state is malformed, so interaction actions are disabled." };
  }
  const kind = value.kind;
  const stepId = value.stepId;
  const prompt = value.prompt;
  const validOutcomes = value.validOutcomes;
  if (!["approval", "manual_gate", "input_request", "disagreement", "provider_session", "workflow"].includes(String(kind))
      || typeof stepId !== "string" || stepId.length === 0
      || typeof prompt !== "string" || prompt.length === 0
      || !Array.isArray(validOutcomes) || validOutcomes.some((outcome) => typeof outcome !== "string")
      || (kind === "provider_session" && (typeof value.sessionId !== "string" || value.sessionId.length === 0))
      || (kind === "workflow" && (typeof value.childRunId !== "string" || value.childRunId.length === 0))) {
    return { waiting: null, error: "The persisted waiting state is malformed, so interaction actions are disabled." };
  }
  return {
    waiting: {
      kind: kind as AgentFlowRunActionWaitingState["kind"],
      stepId,
      prompt,
      validOutcomes: validOutcomes as string[],
      saveAs: typeof value.saveAs === "string" ? value.saveAs : null,
      approvalId: typeof value.approvalId === "string" ? value.approvalId : null,
      ...(kind === "provider_session" ? { sessionId: value.sessionId as string } : {}),
      ...(kind === "workflow" ? { childRunId: value.childRunId as string } : {})
    },
    error: null
  };
}

function resolveNestedActionWaitingState(
  store: AgentFlowRunStateStore,
  parentWaiting: AgentFlowRunActionWaitingState
): {
  waiting: AgentFlowRunActionWaitingState | null;
  error: string | null;
  run: AgentFlowRunRecord | null;
} {
  const visited = new Set<string>();
  let waiting = parentWaiting;
  let run: AgentFlowRunRecord | null = null;
  while (waiting.kind === "workflow") {
    const childRunId = waiting.childRunId!;
    if (visited.has(childRunId)) {
      return { waiting: null, error: "The nested workflow waiting lineage is recursive.", run };
    }
    visited.add(childRunId);
    run = store.getRun(childRunId);
    if (run === null) {
      return { waiting: null, error: "The nested child workflow is missing.", run };
    }
    if (["completed", "failed", "cancelled"].includes(run.status)) {
      return {
        waiting: {
          ...waiting,
          childRunId: run.id,
          childStatus: run.status as "completed" | "failed" | "cancelled"
        },
        error: null,
        run
      };
    }
    if (run.status !== "paused") {
      return { waiting: null, error: "The nested child workflow is no longer paused.", run };
    }
    const parsed = parseWaitingState(run.context.waiting);
    if (parsed.error !== null) {
      return { waiting: null, error: parsed.error, run };
    }
    if (parsed.waiting === null) {
      return {
        waiting: { ...waiting, childRunId: run.id, childStatus: "paused" },
        error: null,
        run
      };
    }
    try {
      validateAgentFlowPipelineWaitingState(run.context.waiting!);
    } catch (error) {
      return {
        waiting: null,
        error: `The nested child waiting state is incomplete or malformed: ${error instanceof Error ? error.message : String(error)}`,
        run
      };
    }
    waiting = parsed.waiting;
  }
  return {
    waiting: {
      ...waiting,
      kind: "workflow",
      nestedKind: waiting.kind,
      childRunId: run?.id
    },
    error: null,
    run
  };
}

function effectiveWaitingKind(
  waiting: AgentFlowRunActionWaitingState | null
): Exclude<AgentFlowRunActionWaitingState["kind"], "workflow"> | undefined {
  return waiting?.kind === "workflow" ? waiting.nestedKind : waiting?.kind;
}

function persistedWorkflow(run: AgentFlowRunRecord): AgentFlowWorkflow | null {
  const workflow = run.context.workflow;
  return workflow !== null && typeof workflow === "object" && !Array.isArray(workflow)
    ? workflow as unknown as AgentFlowWorkflow
    : null;
}

function persistedWorkflowRegistry(
  run: AgentFlowRunRecord,
  workflow: AgentFlowWorkflow
): AgentFlowWorkflowRegistry {
  return run.context.workflowRegistry === undefined
    ? createAgentFlowWorkflowRegistry().register(workflow.name, workflow)
    : createAgentFlowWorkflowRegistryFromSnapshot(run.context.workflowRegistry);
}

function assertRunAction(value: string): asserts value is AgentFlowRunAction {
  if (!(AGENT_FLOW_RUN_ACTIONS as readonly string[]).includes(value)) {
    throw new AgentFlowRunActionError(
      `Unknown Agent Flow run action ${JSON.stringify(value)}.`,
      "AGENT_FLOW_ACTION_INVALID",
      400
    );
  }
}

function safeEqual(left: string, right: string): boolean {
  const leftBytes = Buffer.from(left);
  const rightBytes = Buffer.from(right);
  return leftBytes.length === rightBytes.length && crypto.timingSafeEqual(leftBytes, rightBytes);
}
