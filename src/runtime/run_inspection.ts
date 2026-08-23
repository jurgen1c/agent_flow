import type { AgentFlowWorkflow } from "./workflow";
import { lintAgentFlowWorkflow, validateAgentFlowWorkflow } from "./validation";
import { AGENT_FLOW_FAILURE_REDACTION_MARKER } from "./failure_payload";
import { AgentFlowRunStateError, isNormalizedStaticAgentFlowArtifactPath } from "./run_state";
import type {
  AgentFlowApprovalRecord,
  AgentFlowArtifactRecord,
  AgentFlowEventRecord,
  AgentFlowFailureRecord,
  AgentFlowRunRecord,
  AgentFlowRunStateStore,
  AgentFlowRunStateValue,
  AgentFlowStepRecord
} from "./run_state";

export const MAX_AGENT_FLOW_INSPECTION_DOCUMENT_BYTES = 1024 * 1024;

export interface AgentFlowRunInspectionSummary {
  id: string;
  workflowName: string;
  workflowVersion: number;
  workflowStyle: AgentFlowRunRecord["workflowStyle"];
  workflowMaturity: AgentFlowRunRecord["workflowMaturity"];
  status: AgentFlowRunRecord["status"];
  parentRunId: string | null;
  recoveryOfRunId: string | null;
  currentStepId: string | null;
  createdAt: string;
  updatedAt: string;
  startedAt: string | null;
  finishedAt: string | null;
}

export interface AgentFlowRunInspectionWarning {
  code: string;
  message: string;
  source: "workflow" | "artifact" | "failure" | "approval" | "decision";
  path?: string;
}

export interface AgentFlowInspectionDocument {
  artifact: AgentFlowArtifactRecord;
  document: AgentFlowRunStateValue | null;
  error: string | null;
}

export interface AgentFlowFailureInspection extends AgentFlowFailureRecord {
  failurePayload: AgentFlowInspectionDocument | null;
}

export interface AgentFlowRunInspectionModel {
  run: AgentFlowRunInspectionSummary;
  state: {
    inputs: Record<string, AgentFlowRunStateValue>;
    output: AgentFlowRunStateValue | null;
    error: AgentFlowRunStateValue | null;
    waiting: AgentFlowRunStateValue | null;
  };
  steps: AgentFlowStepRecord[];
  events: AgentFlowEventRecord[];
  artifacts: AgentFlowArtifactRecord[];
  failures: AgentFlowFailureInspection[];
  approvals: AgentFlowApprovalRecord[];
  decisions: AgentFlowInspectionDocument[];
  warnings: AgentFlowRunInspectionWarning[];
}

export type AgentFlowRunInspectionSection =
  | "events"
  | "steps"
  | "artifacts"
  | "failures"
  | "approvals"
  | "decisions"
  | "warnings";

export interface AgentFlowRunInspectionOverview {
  run: AgentFlowRunInspectionSummary;
}

export interface AgentFlowRunInspectionPage {
  section: AgentFlowRunInspectionSection;
  items: Array<
    | AgentFlowEventRecord
    | AgentFlowStepRecord
    | AgentFlowArtifactRecord
    | AgentFlowFailureInspection
    | AgentFlowApprovalRecord
    | AgentFlowInspectionDocument
    | AgentFlowRunInspectionWarning
  >;
  offset: number;
  nextOffset: number | null;
}

export function listAgentFlowRunInspectionSummaries(
  store: AgentFlowRunStateStore
): AgentFlowRunInspectionSummary[] {
  return store.listRuns().map(agentFlowRunInspectionSummary);
}

export function buildAgentFlowRunInspectionOverview(
  store: AgentFlowRunStateStore,
  runId: string
): AgentFlowRunInspectionOverview {
  const run = requireInspectionRun(store, runId);
  return {
    run: agentFlowRunInspectionSummary(run)
  };
}

export function buildAgentFlowRunInspectionState(
  store: AgentFlowRunStateStore,
  runId: string
): AgentFlowRunInspectionModel["state"] {
  return inspectionRunState(requireInspectionRun(store, runId));
}

export function buildAgentFlowRunInspectionPage(
  store: AgentFlowRunStateStore,
  runId: string,
  section: AgentFlowRunInspectionSection,
  offset: number,
  limit: number
): AgentFlowRunInspectionPage {
  if (!Number.isSafeInteger(offset) || offset < 0
      || !Number.isSafeInteger(limit) || limit < 1 || limit > 200
      || offset > Number.MAX_SAFE_INTEGER - limit - 1) {
    throw new AgentFlowRunStateError(
      "Inspection pages require a non-negative offset and a limit between 1 and 200.",
      "AGENT_FLOW_RUN_STATE_PAGE"
    );
  }
  const page = { offset, limit: limit + 1 };
  switch (section) {
    case "events": {
      const items = store.withRunStateReadTransaction(() => store.listEvents(runId, page));
      return inspectionPage(section, items, offset, limit);
    }
    case "steps": {
      const items = store.withRunStateReadTransaction(() => store.listSteps(runId, page));
      return inspectionPage(section, items, offset, limit);
    }
    case "artifacts": {
      const metadata = store.withRunStateReadTransaction(() => store.listArtifactMetadata(runId, page));
      const items = metadata
        .map((artifact) => store.inspectArtifactRecordForInspection(artifact));
      return inspectionPage(section, items, offset, limit);
    }
    case "failures": {
      const snapshot = store.withRunStateReadTransaction(() => {
        const failures = store.listFailureMetadata(runId, page);
        const artifactMetadataByPath = new Map<string, AgentFlowArtifactRecord>();
        for (const failure of failures) {
          if (failure.payloadPath === null || artifactMetadataByPath.has(failure.payloadPath)) continue;
          const artifact = failurePayloadArtifactForInspection(store, runId, failure.payloadPath);
          if (artifact !== null) artifactMetadataByPath.set(failure.payloadPath, artifact);
        }
        return { failures, artifactMetadataByPath };
      });
      const artifactsByPath = new Map(
        [...snapshot.artifactMetadataByPath].map(([path, artifact]) => [
          path,
          store.inspectArtifactRecordForInspection(artifact)
        ])
      );
      const items = inspectFailureRecords(store, snapshot.failures, artifactsByPath, []);
      return inspectionPage(section, items, offset, limit);
    }
    case "approvals": {
      const items = store.withRunStateReadTransaction(() => store.listApprovals(runId, page));
      return inspectionPage(section, items, offset, limit);
    }
    case "decisions": {
      const metadata = store.withRunStateReadTransaction(() =>
        store.listArtifactMetadataByKind(runId, "decision_record", page)
      );
      const decisions = metadata
        .map((artifact) => store.inspectArtifactRecordForInspection(artifact));
      const items = decisions.map((artifact) => readInspectionDocument(store, artifact));
      return inspectionPage(section, items, offset, limit);
    }
    case "warnings":
      return buildWarningPage(store, runId, offset, limit);
  }
}

function inspectionPage(
  section: AgentFlowRunInspectionSection,
  items: AgentFlowRunInspectionPage["items"],
  offset: number,
  limit: number
): AgentFlowRunInspectionPage {
  const hasMore = items.length > limit;
  return {
    section,
    items: items.slice(0, limit),
    offset,
    nextOffset: hasMore ? offset + limit : null
  };
}

function buildWarningPage(
  store: AgentFlowRunStateStore,
  runId: string,
  offset: number,
  limit: number
): AgentFlowRunInspectionPage {
  const snapshot = store.withRunStateReadTransaction(() => {
    const workflow: AgentFlowRunInspectionWarning[] = [];
    const failures: Array<{ record: AgentFlowFailureRecord; artifact: AgentFlowArtifactRecord | null }> = [];
    const artifacts: AgentFlowArtifactRecord[] = [];
    const approvals: AgentFlowApprovalRecord[] = [];
    const workflowWarningsSnapshot = workflowWarnings(requireInspectionRun(store, runId));
    const counts = store.countInspectionWarningSources(runId);
    const totalSourceRecords = workflowWarningsSnapshot.length
      + counts.failures + counts.artifacts + counts.approvals;
    let sourceOffset = offset;
    let remaining = limit;
    let scanned = 0;
    const consume = <T>(
      total: number,
      load: (offset: number, limit: number) => T[],
      visit: (record: T) => void
    ): void => {
      if (remaining === 0) return;
      if (sourceOffset >= total) {
        sourceOffset -= total;
        return;
      }
      const count = Math.min(remaining, total - sourceOffset);
      for (const record of load(sourceOffset, count)) visit(record);
      sourceOffset = 0;
      remaining -= count;
      scanned += count;
    };

    consume(
      workflowWarningsSnapshot.length,
      (pageOffset, pageLimit) => workflowWarningsSnapshot.slice(pageOffset, pageOffset + pageLimit),
      (warning) => workflow.push(warning)
    );
    consume(
      counts.failures,
      (pageOffset, pageLimit) => store.listFailureMetadata(runId, { offset: pageOffset, limit: pageLimit }),
      (failure) => failures.push({
        record: failure,
        artifact: failure.payloadPath === null
          ? null
          : failurePayloadArtifactForInspection(store, runId, failure.payloadPath)
      })
    );
    consume(
      counts.artifacts,
      (pageOffset, pageLimit) => store.listArtifactMetadata(runId, { offset: pageOffset, limit: pageLimit }),
      (artifact) => artifacts.push(artifact)
    );
    consume(
      counts.approvals,
      (pageOffset, pageLimit) => store.listApprovals(runId, { offset: pageOffset, limit: pageLimit }),
      (approval) => approvals.push(approval)
    );
    return { workflow, failures, artifacts, approvals, scanned, totalSourceRecords };
  });

  const warnings: AgentFlowRunInspectionWarning[] = [];
  warnings.push(...snapshot.workflow);
  for (const failure of snapshot.failures) {
    const artifactsByPath = new Map<string, AgentFlowArtifactRecord>();
    if (failure.record.payloadPath !== null && failure.artifact !== null) {
      artifactsByPath.set(
        failure.record.payloadPath,
        store.inspectArtifactRecordForInspection(failure.artifact)
      );
    }
    inspectFailureRecords(store, [failure.record], artifactsByPath, warnings);
  }
  for (const metadataRecord of snapshot.artifacts) {
    const artifact = store.inspectArtifactRecordForInspection(metadataRecord);
    if (artifact.status === "missing" || artifact.status === "stale") warnings.push({
        code: `run.artifact.${artifact.status}`,
        message: `Artifact ${artifact.declaredPath} is ${artifact.status}.`,
        source: "artifact",
        path: artifact.declaredPath
    });
    if (artifact.kind === "decision_record") {
      const decision = readInspectionDocument(store, artifact);
      if (decision.error !== null) warnings.push({
          code: "run.decision.unavailable",
          message: `Decision record ${artifact.declaredPath} is unavailable: ${decision.error}`,
          source: "decision",
          path: artifact.declaredPath
      });
    }
  }
  for (const approval of snapshot.approvals) {
    if (approval.status === "requested" || approval.status === "stale") warnings.push({
        code: `run.approval.${approval.status}`,
        message: `Approval ${approval.id} is ${approval.status}.`,
        source: "approval"
    });
  }
  return {
    section: "warnings",
    items: warnings,
    offset,
    nextOffset: offset + snapshot.scanned < snapshot.totalSourceRecords
      ? offset + snapshot.scanned
      : null
  };
}

function failurePayloadArtifactForInspection(
  store: AgentFlowRunStateStore,
  runId: string,
  payloadPath: string
): AgentFlowArtifactRecord | null {
  if (!isNormalizedStaticAgentFlowArtifactPath(payloadPath)) return null;
  return store.getArtifactMetadataForInspection(runId, payloadPath);
}

export function buildAgentFlowRunInspectionModel(
  store: AgentFlowRunStateStore,
  runId: string
): AgentFlowRunInspectionModel {
  const snapshot = store.withRunStateReadTransaction(() => {
    const run = store.getRun(runId);
    if (run === null) {
      throw new AgentFlowRunStateError(
        `Agent Flow run ${runId} was not found.`,
        "AGENT_FLOW_RUN_NOT_FOUND"
      );
    }
    return {
      run,
      steps: store.listSteps(run.id),
      artifacts: store.listArtifactMetadata(run.id),
      failures: store.listFailureMetadata(run.id),
      approvals: store.listApprovals(run.id),
      events: store.listEvents(run.id)
    };
  });
  const { run, steps, failures: failureRecords, approvals, events } = snapshot;
  const artifacts = snapshot.artifacts.map((artifact) =>
    store.inspectArtifactRecordForInspection(artifact)
  );
  const artifactsByPath = new Map(artifacts.map((artifact) => [artifact.declaredPath, artifact]));
  const warnings = workflowWarnings(run);
  const failures = inspectFailureRecords(store, failureRecords, artifactsByPath, warnings);

  for (const artifact of artifacts) {
    if (artifact.status === "missing" || artifact.status === "stale") {
      warnings.push({
        code: `run.artifact.${artifact.status}`,
        message: `Artifact ${artifact.declaredPath} is ${artifact.status}.`,
        source: "artifact",
        path: artifact.declaredPath
      });
    }
  }

  const decisions = artifacts
    .filter((artifact) => artifact.kind === "decision_record")
    .map((artifact) => readInspectionDocument(store, artifact));
  for (const decision of decisions) {
    if (decision.error !== null) {
      warnings.push({
        code: "run.decision.unavailable",
        message: `Decision record ${decision.artifact.declaredPath} is unavailable: ${decision.error}`,
        source: "decision",
        path: decision.artifact.declaredPath
      });
    }
  }
  for (const approval of approvals) {
    if (approval.status === "requested" || approval.status === "stale") {
      warnings.push({
        code: `run.approval.${approval.status}`,
        message: `Approval ${approval.id} is ${approval.status}.`,
        source: "approval"
      });
    }
  }
  return {
    run: agentFlowRunInspectionSummary(run),
    state: inspectionRunState(run),
    steps,
    events,
    artifacts,
    failures,
    approvals,
    decisions,
    warnings
  };
}

function requireInspectionRun(store: AgentFlowRunStateStore, runId: string): AgentFlowRunRecord {
  const run = store.getRun(runId);
  if (run === null) {
    throw new AgentFlowRunStateError(
      `Agent Flow run ${runId} was not found.`,
      "AGENT_FLOW_RUN_NOT_FOUND"
    );
  }
  return run;
}

function inspectionRunState(run: AgentFlowRunRecord): AgentFlowRunInspectionModel["state"] {
  return {
    inputs: run.inputs,
    output: run.output,
    error: run.error,
    waiting: run.context.waiting ?? null
  };
}

function inspectFailureRecords(
  store: AgentFlowRunStateStore,
  failureRecords: AgentFlowFailureRecord[],
  artifactsByPath: Map<string, AgentFlowArtifactRecord>,
  warnings: AgentFlowRunInspectionWarning[]
): AgentFlowFailureInspection[] {
  return failureRecords.map((failure) => {
    const failureArtifact = failure.payloadPath === null
      ? null
      : matchingFailurePayloadArtifact(failure, artifactsByPath.get(failure.payloadPath));
    const failurePayload = failureArtifact === null
      ? null
      : readInspectionDocument(store, failureArtifact, failure);
    if (failure.resolvedAt === null) {
      warnings.push({
        code: "run.failure.unresolved",
        message: `Failure ${failure.id} remains unresolved.`,
        source: "failure"
      });
    }
    if (failurePayload === null) {
      warnings.push({
        code: "run.failure.payload_unavailable",
        message: `Failure ${failure.id} does not have a readable persisted payload.`,
        source: "failure"
      });
    } else if (failurePayload.error !== null) {
      warnings.push({
        code: "run.failure.payload_unavailable",
        message: `Failure ${failure.id} payload at ${failurePayload.artifact.declaredPath} is unavailable: ${failurePayload.error}`,
        source: "failure",
        path: failurePayload.artifact.declaredPath
      });
    }
    return { ...failure, failurePayload };
  });
}

function agentFlowRunInspectionSummary(run: AgentFlowRunRecord): AgentFlowRunInspectionSummary {
  return {
    id: run.id,
    workflowName: run.workflowName,
    workflowVersion: run.workflowVersion,
    workflowStyle: run.workflowStyle,
    workflowMaturity: run.workflowMaturity,
    status: run.status,
    parentRunId: run.parentRunId,
    recoveryOfRunId: run.recoveryOfRunId,
    currentStepId: run.currentStepId,
    createdAt: run.createdAt,
    updatedAt: run.updatedAt,
    startedAt: run.startedAt,
    finishedAt: run.finishedAt
  };
}

function workflowWarnings(run: AgentFlowRunRecord): AgentFlowRunInspectionWarning[] {
  const persisted = run.context.workflow;
  if (!isMapping(persisted)) {
    return [{
      code: "run.workflow.unavailable",
      message: "The persisted workflow definition is unavailable.",
      source: "workflow"
    }];
  }

  const workflow = persisted as unknown as AgentFlowWorkflow;
  let validation: ReturnType<typeof validateAgentFlowWorkflow>;
  try {
    validation = validateAgentFlowWorkflow(workflow);
  } catch (error) {
    return [invalidPersistedWorkflowWarning("validation", error)];
  }
  if (!validation.valid) {
    return validation.errors.map((issue) => ({
      code: issue.code,
      message: issue.message,
      source: "workflow" as const,
      path: issue.path
    }));
  }

  try {
    return lintAgentFlowWorkflow(workflow).warnings.map((issue) => ({
      code: issue.code,
      message: issue.message,
      source: "workflow" as const,
      path: issue.path
    }));
  } catch (error) {
    return [invalidPersistedWorkflowWarning("lint", error)];
  }
}

function invalidPersistedWorkflowWarning(
  phase: "validation" | "lint",
  error: unknown
): AgentFlowRunInspectionWarning {
  return {
    code: "run.workflow.invalid",
    message: `The persisted workflow definition failed ${phase}: ${error instanceof Error ? error.message : String(error)}`,
    source: "workflow"
  };
}

function readInspectionDocument(
  store: AgentFlowRunStateStore,
  artifact: AgentFlowArtifactRecord,
  expectedFailure?: AgentFlowFailureRecord
): AgentFlowInspectionDocument {
  if (artifact.status !== "available" && artifact.status !== "overwritten") {
    return { artifact, document: null, error: `artifact status is ${artifact.status}` };
  }

  try {
    const snapshot = store.readArtifactRecordForInspection(artifact, {
      maxBytes: MAX_AGENT_FLOW_INSPECTION_DOCUMENT_BYTES
    });
    const text = new TextDecoder("utf-8", { fatal: true }).decode(snapshot.content);
    const document = JSON.parse(text) as unknown;
    if (!isInspectionDocument(snapshot.artifact.kind, document)) {
      return {
        artifact: snapshot.artifact,
        document: null,
        error: `payload does not match the ${snapshot.artifact.kind} document shape`
      };
    }
    if (expectedFailure !== undefined
        && (!isMapping(document)
          || document.id !== expectedFailure.id
          || document.path !== snapshot.artifact.declaredPath)) {
      return {
        artifact: snapshot.artifact,
        document: null,
        error: "failure payload identity does not match its failure record and artifact path"
      };
    }
    return { artifact: snapshot.artifact, document, error: null };
  } catch (error) {
    return {
      artifact,
      document: null,
      error: error instanceof Error ? error.message : String(error)
    };
  }
}

function matchingFailurePayloadArtifact(
  failure: AgentFlowFailureRecord,
  artifact: AgentFlowArtifactRecord | undefined
): AgentFlowArtifactRecord | null {
  return artifact?.kind === "failure_payload"
      && artifact.metadata.failureId === failure.id
    ? artifact
    : null;
}

function isMapping(value: unknown): value is Record<string, AgentFlowRunStateValue> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function isRunStateValue(value: unknown): value is AgentFlowRunStateValue {
  if (value === null || typeof value === "string" || typeof value === "boolean") return true;
  if (typeof value === "number") return Number.isFinite(value);
  if (Array.isArray(value)) return value.every(isRunStateValue);
  return typeof value === "object"
    && Object.values(value as Record<string, unknown>).every(isRunStateValue);
}

function isInspectionDocument(kind: string, value: unknown): value is AgentFlowRunStateValue {
  if (!isMapping(value)) return false;
  if (kind === "decision_record") return isDecisionRecordDocument(value);
  if (kind === "failure_payload") return isFailurePayloadDocument(value);
  return isRunStateValue(value);
}

function isDecisionRecordDocument(value: Record<string, AgentFlowRunStateValue>): boolean {
  return hasExactKeys(value, [
    "decision_id",
    "owner",
    "topic",
    "rationale_summary",
    "consulted",
    "approved_by",
    "artifacts",
    "created_at"
  ])
    && nonEmptyText(value.decision_id)
    && nonEmptyText(value.owner)
    && nonEmptyText(value.topic)
    && nonEmptyText(value.rationale_summary)
    && uniqueNonEmptyTextList(value.consulted)
    && uniqueNonEmptyTextList(value.approved_by)
    && uniqueNonEmptyTextList(value.artifacts, true)
    && (value.artifacts as AgentFlowRunStateValue[]).every(isNormalizedStaticAgentFlowArtifactPath)
    && validDateTime(value.created_at);
}

function isFailurePayloadDocument(value: Record<string, AgentFlowRunStateValue>): boolean {
  const logs = mappingValue(value.logs);
  const artifacts = mappingValue(value.artifacts);
  const redactions = mappingValue(value.redactions);
  return hasExactKeys(value, [
    "id",
    "step_id",
    "step_type",
    "status",
    "attempt",
    "exit_code",
    "command",
    "summary",
    "logs",
    "artifacts",
    "classification",
    "remediation_status",
    "path",
    "redactions"
  ])
    && nonEmptyText(value.id)
    && nonEmptyText(value.step_id)
    && nonEmptyText(value.step_type)
    && value.status === "failed"
    && typeof value.attempt === "number"
    && Number.isSafeInteger(value.attempt)
    && value.attempt > 0
    && (value.exit_code === null || (typeof value.exit_code === "number" && Number.isSafeInteger(value.exit_code)))
    && (value.command === null || typeof value.command === "string")
    && nonEmptyText(value.summary)
    && logs !== null
    && hasExactKeys(logs, ["stdout", "stderr"])
    && nullableText(logs.stdout)
    && nullableText(logs.stderr)
    && artifacts !== null
    && hasExactKeys(artifacts, ["available", "withheld"])
    && uniqueNonEmptyTextList(artifacts.available)
    && uniqueNonEmptyTextList(artifacts.withheld)
    && nonEmptyText(value.classification)
    && (value.remediation_status === null || typeof value.remediation_status === "string")
    && nonEmptyText(value.path)
    && redactions !== null
    && hasExactKeys(redactions, ["applied", "marker", "fields", "unscanned_artifacts"])
    && typeof redactions.applied === "boolean"
    && redactions.marker === AGENT_FLOW_FAILURE_REDACTION_MARKER
    && uniqueNonEmptyTextList(redactions.fields)
    && uniqueNonEmptyTextList(redactions.unscanned_artifacts);
}

function mappingValue(value: AgentFlowRunStateValue | undefined): Record<string, AgentFlowRunStateValue> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value : null;
}

function nonEmptyText(value: AgentFlowRunStateValue | undefined): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

function nullableText(value: AgentFlowRunStateValue | undefined): boolean {
  return value === null || typeof value === "string";
}

function uniqueNonEmptyTextList(value: AgentFlowRunStateValue | undefined, requireEntry = false): boolean {
  return Array.isArray(value)
    && (!requireEntry || value.length > 0)
    && value.every(nonEmptyText)
    && new Set(value).size === value.length;
}

function hasExactKeys(value: Record<string, AgentFlowRunStateValue>, expected: string[]): boolean {
  const actual = Object.keys(value).sort();
  return actual.length === expected.length
    && [...expected].sort().every((key, index) => actual[index] === key);
}

function validDateTime(value: AgentFlowRunStateValue | undefined): boolean {
  return typeof value === "string"
    && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.test(value)
    && Number.isFinite(Date.parse(value));
}
