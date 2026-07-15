import fs from "node:fs";
import path from "node:path";
import type { AgentflowMaturity, AgentflowWorkflowStyle } from "./workflow";
import {
  AgentflowRunStateSchemaVersionError,
  initializeAgentflowRunStateSchema
} from "./run_state_schema";

export const AGENTFLOW_RUN_STATE_SCHEMA_VERSION = 1;
export const DEFAULT_AGENTFLOW_DATABASE_PATH = ".agentflow/agentflow.sqlite";

export type AgentflowRunStatus = "pending" | "running" | "waiting" | "paused" | "completed" | "failed" | "cancelled";
export type AgentflowStepStatus = AgentflowRunStatus | "skipped";
export type AgentflowSessionStatus = AgentflowRunStatus;
export type AgentflowApprovalStatus = "requested" | "approved" | "rejected" | "cancelled";
export type AgentflowRunStateValue = null | boolean | number | string | AgentflowRunStateValue[] | { [key: string]: AgentflowRunStateValue };

export interface OpenAgentflowRunStateOptions {
  cwd?: string;
  databasePath?: string;
  now?: () => string;
  busyTimeoutMs?: number;
}

export interface AgentflowRunRecord {
  id: string;
  workflowName: string;
  workflowVersion: number;
  workflowStyle: AgentflowWorkflowStyle;
  workflowMaturity: AgentflowMaturity;
  status: AgentflowRunStatus;
  parentRunId: string | null;
  recoveryOfRunId: string | null;
  currentStepId: string | null;
  inputs: Record<string, AgentflowRunStateValue>;
  context: Record<string, AgentflowRunStateValue>;
  output: AgentflowRunStateValue | null;
  error: AgentflowRunStateValue | null;
  createdAt: string;
  updatedAt: string;
  startedAt: string | null;
  finishedAt: string | null;
}

export interface CreateAgentflowRunInput {
  id: string;
  workflow: {
    name: string;
    version: number;
    style: AgentflowWorkflowStyle;
    maturity: AgentflowMaturity;
  };
  status?: AgentflowRunStatus;
  parentRunId?: string;
  recoveryOfRunId?: string;
  currentStepId?: string;
  inputs?: Record<string, AgentflowRunStateValue>;
  context?: Record<string, AgentflowRunStateValue>;
}

export interface UpdateAgentflowRunInput {
  status?: AgentflowRunStatus;
  currentStepId?: string | null;
  context?: Record<string, AgentflowRunStateValue>;
  output?: AgentflowRunStateValue | null;
  error?: AgentflowRunStateValue | null;
}

export interface FindResumableAgentflowRunInput {
  workflowName?: string;
  workflowVersion?: number;
}

export interface UpsertAgentflowStepInput {
  runId: string;
  stepId: string;
  attempt?: number;
  status: AgentflowStepStatus;
  parentStepId?: string;
  sessionId?: string;
  input?: AgentflowRunStateValue;
  output?: AgentflowRunStateValue;
  error?: AgentflowRunStateValue;
}

export interface UpsertAgentflowArtifactInput {
  id: string;
  runId: string;
  stepId?: string;
  path: string;
  kind: string;
  contentType: string;
  checksum?: string;
  sizeBytes?: number;
  metadata?: Record<string, AgentflowRunStateValue>;
}

export interface AppendAgentflowEventInput {
  id: string;
  runId: string;
  sequence: number;
  stepId?: string;
  sessionId?: string;
  type: string;
  payload?: AgentflowRunStateValue;
}

export interface UpsertAgentflowSessionInput {
  id: string;
  runId: string;
  stepId?: string;
  provider: string;
  status: AgentflowSessionStatus;
  externalSessionId?: string;
  state?: Record<string, AgentflowRunStateValue>;
}

export interface RecordAgentflowFailureInput {
  id: string;
  runId: string;
  stepId?: string;
  sessionId?: string;
  classification: string;
  message: string;
  retryable?: boolean;
  payload?: AgentflowRunStateValue;
  resolvedAt?: string;
}

export interface UpsertAgentflowApprovalInput {
  id: string;
  runId: string;
  stepId?: string;
  status: AgentflowApprovalStatus;
  requestedBy?: string;
  decidedBy?: string;
  decision?: string;
  context?: Record<string, AgentflowRunStateValue>;
  decidedAt?: string;
}

export interface UpsertAgentflowBudgetInput {
  id: string;
  runId: string;
  stepId?: string;
  sessionId?: string;
  scope: string;
  kind: string;
  limit: number;
  used: number;
  unit: string;
}

type SqliteValue = string | number | bigint | null | Uint8Array;

interface SqliteDatabase {
  exec(sql: string): void;
  run(sql: string, params?: SqliteValue[]): void;
  get<T>(sql: string, params?: SqliteValue[]): T | null;
  close(): void;
}

interface RunRow {
  id: string;
  workflow_name: string;
  workflow_version: number;
  workflow_style: AgentflowWorkflowStyle;
  workflow_maturity: AgentflowMaturity;
  status: AgentflowRunStatus;
  parent_run_id: string | null;
  recovery_of_run_id: string | null;
  current_step_id: string | null;
  inputs_json: string;
  context_json: string;
  output_json: string | null;
  error_json: string | null;
  created_at: string;
  updated_at: string;
  started_at: string | null;
  finished_at: string | null;
}

const TERMINAL_RUN_STATUSES = new Set<AgentflowRunStatus>(["completed", "failed", "cancelled"]);
const RUN_STATUSES = ["pending", "running", "waiting", "paused", "completed", "failed", "cancelled"] as const;
const STEP_STATUSES = [...RUN_STATUSES, "skipped"] as const;
const APPROVAL_STATUSES = ["requested", "approved", "rejected", "cancelled"] as const;
const TERMINAL_APPROVAL_STATUSES = new Set<AgentflowApprovalStatus>(["approved", "rejected", "cancelled"]);
const WORKFLOW_STYLES = ["pipeline", "recovery_pipeline", "collaborative"] as const;
const WORKFLOW_MATURITIES = ["draft", "experimental", "stable", "trusted"] as const;

export class AgentflowRunStateError extends Error {
  readonly code: string;

  constructor(message: string, code = "AGENTFLOW_RUN_STATE_ERROR", options?: ErrorOptions) {
    super(message, options);
    this.name = "AgentflowRunStateError";
    this.code = code;
  }
}

export class AgentflowRunStateStore {
  readonly repoRoot: string;
  readonly databasePath: string;
  private readonly database: SqliteDatabase;
  private readonly now: () => string;
  private closed = false;

  constructor(input: { repoRoot: string; databasePath: string; database: SqliteDatabase; now: () => string }) {
    this.repoRoot = input.repoRoot;
    this.databasePath = input.databasePath;
    this.database = input.database;
    this.now = input.now;
  }

  createRun(input: CreateAgentflowRunInput): AgentflowRunRecord {
    this.assertOpen();
    const id = requiredString(input.id, "Run ID");
    const workflowName = requiredString(input.workflow.name, "Workflow name");
    const status = input.status ?? "pending";
    assertOneOf(status, RUN_STATUSES, "run status");
    assertOneOf(input.workflow.style, WORKFLOW_STYLES, "workflow style");
    assertOneOf(input.workflow.maturity, WORKFLOW_MATURITIES, "workflow maturity");
    if (!Number.isSafeInteger(input.workflow.version) || input.workflow.version < 1) {
      throw new AgentflowRunStateError("Workflow version must be a positive integer.", "AGENTFLOW_RUN_INVALID");
    }
    const timestamp = currentTimestamp(this.now);
    const startedAt = status === "running" ? timestamp : null;
    const finishedAt = TERMINAL_RUN_STATUSES.has(status) ? timestamp : null;

    try {
      this.database.run(
        `INSERT INTO runs (
          id, workflow_name, workflow_version, workflow_style, workflow_maturity, status,
          parent_run_id, recovery_of_run_id, current_step_id, inputs_json, context_json,
          output_json, error_json, created_at, updated_at, started_at, finished_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, ?)`,
        [
          id,
          workflowName,
          input.workflow.version,
          input.workflow.style,
          input.workflow.maturity,
          status,
          optionalString(input.parentRunId, "Parent run ID"),
          optionalString(input.recoveryOfRunId, "Recovery run ID"),
          optionalString(input.currentStepId, "Current step ID"),
          stableJson(input.inputs ?? {}),
          stableJson(input.context ?? {}),
          timestamp,
          timestamp,
          startedAt,
          finishedAt
        ]
      );
    } catch (error) {
      if (error instanceof AgentflowRunStateError) throw error;
      if (isConstraintError(error)) {
        throw new AgentflowRunStateError(`Agentflow run ${id} already exists or references missing state.`, "AGENTFLOW_RUN_COLLISION", { cause: error });
      }
      throw runStateWriteError("create run", error);
    }

    return this.requireRun(id);
  }

  getRun(id: string): AgentflowRunRecord | null {
    this.assertOpen();
    const row = this.database.get<RunRow>("SELECT * FROM runs WHERE id = ?", [requiredString(id, "Run ID")]);
    return row === null ? null : hydrateRun(row);
  }

  updateRun(id: string, input: UpdateAgentflowRunInput): AgentflowRunRecord {
    this.assertOpen();
    const runId = requiredString(id, "Run ID");
    this.database.exec("BEGIN IMMEDIATE");

    try {
      const current = this.requireRun(runId);
      const status = input.status ?? current.status;
      assertOneOf(status, RUN_STATUSES, "run status");
      if (TERMINAL_RUN_STATUSES.has(current.status) && status !== current.status) {
        throw new AgentflowRunStateError(
          `Terminal Agentflow run ${runId} cannot transition from ${current.status} to ${status}.`,
          "AGENTFLOW_RUN_TERMINAL"
        );
      }
      if (TERMINAL_RUN_STATUSES.has(current.status)) {
        this.database.exec("COMMIT");
        return current;
      }

      const timestamp = currentTimestamp(this.now);
      const startedAt = current.startedAt ?? (status === "running" ? timestamp : null);
      const finishedAt = TERMINAL_RUN_STATUSES.has(status) ? current.finishedAt ?? timestamp : null;
      this.database.run(
        `UPDATE runs SET
          status = ?, current_step_id = ?, context_json = ?, output_json = ?, error_json = ?,
          updated_at = ?, started_at = ?, finished_at = ?
        WHERE id = ?`,
        [
          status,
          input.currentStepId === undefined ? current.currentStepId : optionalString(input.currentStepId ?? undefined, "Current step ID"),
          stableJson(input.context ?? current.context),
          nullableJson(input.output === undefined ? current.output : input.output),
          nullableJson(input.error === undefined ? current.error : input.error),
          timestamp,
          startedAt,
          finishedAt,
          runId
        ]
      );
      this.database.exec("COMMIT");
    } catch (error) {
      rollback(this.database);
      if (error instanceof AgentflowRunStateError) throw error;
      throw runStateWriteError("update run", error);
    }

    return this.requireRun(runId);
  }

  findResumableRun(input: FindResumableAgentflowRunInput = {}): AgentflowRunRecord | null {
    this.assertOpen();
    const conditions = ["status IN ('pending', 'running', 'waiting', 'paused')"];
    const params: SqliteValue[] = [];
    if (input.workflowName !== undefined) {
      conditions.push("workflow_name = ?");
      params.push(requiredString(input.workflowName, "Workflow name"));
    }
    if (input.workflowVersion !== undefined) {
      if (!Number.isSafeInteger(input.workflowVersion) || input.workflowVersion < 1) {
        throw new AgentflowRunStateError("Workflow version must be a positive integer.", "AGENTFLOW_RUN_INVALID");
      }
      conditions.push("workflow_version = ?");
      params.push(input.workflowVersion);
    }

    const row = this.database.get<RunRow>(
      `SELECT * FROM runs WHERE ${conditions.join(" AND ")} ORDER BY updated_at DESC, created_at DESC, id ASC LIMIT 1`,
      params
    );
    return row === null ? null : hydrateRun(row);
  }

  upsertStep(input: UpsertAgentflowStepInput): void {
    this.assertOpen();
    const attempt = input.attempt ?? 1;
    if (!Number.isSafeInteger(attempt) || attempt < 1) {
      throw new AgentflowRunStateError("Step attempt must be a positive integer.", "AGENTFLOW_STEP_INVALID");
    }
    assertOneOf(input.status, STEP_STATUSES, "step status");
    const timestamp = currentTimestamp(this.now);
    const startedAt = input.status === "running" ? timestamp : null;
    const finishedAt = TERMINAL_RUN_STATUSES.has(input.status as AgentflowRunStatus) || input.status === "skipped" ? timestamp : null;

    this.write("upsert step", `INSERT INTO run_steps (
      run_id, step_id, attempt, parent_step_id, session_id, status, input_json, output_json, error_json,
      created_at, updated_at, started_at, finished_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(run_id, step_id, attempt) DO UPDATE SET
      parent_step_id = CASE WHEN run_steps.finished_at IS NOT NULL OR ? = 1 THEN run_steps.parent_step_id ELSE excluded.parent_step_id END,
      session_id = CASE WHEN run_steps.finished_at IS NOT NULL OR ? = 1 THEN run_steps.session_id ELSE excluded.session_id END,
      status = CASE WHEN run_steps.finished_at IS NULL THEN excluded.status ELSE run_steps.status END,
      input_json = CASE WHEN run_steps.finished_at IS NOT NULL OR ? = 1 THEN run_steps.input_json ELSE excluded.input_json END,
      output_json = CASE WHEN run_steps.finished_at IS NOT NULL OR ? = 1 THEN run_steps.output_json ELSE excluded.output_json END,
      error_json = CASE WHEN run_steps.finished_at IS NOT NULL OR ? = 1 THEN run_steps.error_json ELSE excluded.error_json END,
      updated_at = CASE WHEN run_steps.finished_at IS NULL THEN excluded.updated_at ELSE run_steps.updated_at END,
      started_at = COALESCE(run_steps.started_at, excluded.started_at),
      finished_at = COALESCE(run_steps.finished_at, excluded.finished_at)`, [
      requiredString(input.runId, "Run ID"),
      requiredString(input.stepId, "Step ID"),
      attempt,
      optionalString(input.parentStepId, "Parent step ID"),
      optionalString(input.sessionId, "Session ID"),
      input.status,
      nullableJson(input.input),
      nullableJson(input.output),
      nullableJson(input.error),
      timestamp,
      timestamp,
      startedAt,
      finishedAt,
      input.parentStepId === undefined ? 1 : 0,
      input.sessionId === undefined ? 1 : 0,
      input.input === undefined ? 1 : 0,
      input.output === undefined ? 1 : 0,
      input.error === undefined ? 1 : 0
    ]);
  }

  upsertArtifact(input: UpsertAgentflowArtifactInput): void {
    this.assertOpen();
    const artifactPath = repoRelativeArtifactPath(input.path);
    if (input.sizeBytes !== undefined && (!Number.isSafeInteger(input.sizeBytes) || input.sizeBytes < 0)) {
      throw new AgentflowRunStateError("Artifact size must be a non-negative integer.", "AGENTFLOW_ARTIFACT_INVALID");
    }
    const timestamp = currentTimestamp(this.now);
    this.write("upsert artifact", `INSERT INTO artifacts (
      run_id, id, step_id, path, kind, content_type, checksum, size_bytes, metadata_json, created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(run_id, id) DO UPDATE SET
      step_id = excluded.step_id,
      path = excluded.path,
      kind = excluded.kind,
      content_type = excluded.content_type,
      checksum = excluded.checksum,
      size_bytes = excluded.size_bytes,
      metadata_json = excluded.metadata_json,
      updated_at = excluded.updated_at`, [
      requiredString(input.runId, "Run ID"),
      requiredString(input.id, "Artifact ID"),
      optionalString(input.stepId, "Step ID"),
      artifactPath,
      requiredString(input.kind, "Artifact kind"),
      requiredString(input.contentType, "Artifact content type"),
      optionalString(input.checksum, "Artifact checksum"),
      input.sizeBytes ?? null,
      stableJson(input.metadata ?? {}),
      timestamp,
      timestamp
    ]);
  }

  appendEvent(input: AppendAgentflowEventInput): void {
    this.assertOpen();
    if (!Number.isSafeInteger(input.sequence) || input.sequence < 1) {
      throw new AgentflowRunStateError("Event sequence must be a positive integer.", "AGENTFLOW_EVENT_INVALID");
    }
    const timestamp = currentTimestamp(this.now);
    this.write("append event", `INSERT INTO events (
      run_id, id, sequence, step_id, session_id, type, payload_json, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, [
      requiredString(input.runId, "Run ID"),
      requiredString(input.id, "Event ID"),
      input.sequence,
      optionalString(input.stepId, "Step ID"),
      optionalString(input.sessionId, "Session ID"),
      requiredString(input.type, "Event type"),
      nullableJson(input.payload),
      timestamp
    ]);
  }

  upsertSession(input: UpsertAgentflowSessionInput): void {
    this.assertOpen();
    assertOneOf(input.status, RUN_STATUSES, "session status");
    const timestamp = currentTimestamp(this.now);
    const startedAt = input.status === "running" ? timestamp : null;
    const finishedAt = TERMINAL_RUN_STATUSES.has(input.status) ? timestamp : null;
    this.write("upsert session", `INSERT INTO sessions (
      run_id, id, step_id, provider, external_session_id, status, state_json,
      created_at, updated_at, started_at, finished_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(run_id, id) DO UPDATE SET
      step_id = CASE WHEN sessions.finished_at IS NOT NULL OR ? = 1 THEN sessions.step_id ELSE excluded.step_id END,
      provider = CASE WHEN sessions.finished_at IS NULL THEN excluded.provider ELSE sessions.provider END,
      external_session_id = CASE WHEN sessions.finished_at IS NOT NULL OR ? = 1 THEN sessions.external_session_id ELSE excluded.external_session_id END,
      status = CASE WHEN sessions.finished_at IS NULL THEN excluded.status ELSE sessions.status END,
      state_json = CASE WHEN sessions.finished_at IS NOT NULL OR ? = 1 THEN sessions.state_json ELSE excluded.state_json END,
      updated_at = CASE WHEN sessions.finished_at IS NULL THEN excluded.updated_at ELSE sessions.updated_at END,
      started_at = COALESCE(sessions.started_at, excluded.started_at),
      finished_at = COALESCE(sessions.finished_at, excluded.finished_at)`, [
      requiredString(input.runId, "Run ID"),
      requiredString(input.id, "Session ID"),
      optionalString(input.stepId, "Step ID"),
      requiredString(input.provider, "Session provider"),
      optionalString(input.externalSessionId, "External session ID"),
      input.status,
      stableJson(input.state ?? {}),
      timestamp,
      timestamp,
      startedAt,
      finishedAt,
      input.stepId === undefined ? 1 : 0,
      input.externalSessionId === undefined ? 1 : 0,
      input.state === undefined ? 1 : 0
    ]);
  }

  recordFailure(input: RecordAgentflowFailureInput): void {
    this.assertOpen();
    const timestamp = currentTimestamp(this.now);
    this.write("record failure", `INSERT INTO failures (
      run_id, id, step_id, session_id, classification, message, retryable, payload_json, created_at, resolved_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, [
      requiredString(input.runId, "Run ID"),
      requiredString(input.id, "Failure ID"),
      optionalString(input.stepId, "Step ID"),
      optionalString(input.sessionId, "Session ID"),
      requiredString(input.classification, "Failure classification"),
      requiredString(input.message, "Failure message"),
      input.retryable === true ? 1 : 0,
      nullableJson(input.payload),
      timestamp,
      input.resolvedAt === undefined ? null : validTimestamp(input.resolvedAt)
    ]);
  }

  resolveFailure(runId: string, failureId: string, resolvedAt?: string): void {
    this.assertOpen();
    const normalizedRunId = requiredString(runId, "Run ID");
    const normalizedFailureId = requiredString(failureId, "Failure ID");
    const existing = this.database.get<{ id: string }>(
      "SELECT id FROM failures WHERE run_id = ? AND id = ?",
      [normalizedRunId, normalizedFailureId]
    );
    if (existing === null) {
      throw new AgentflowRunStateError(
        `Agentflow failure ${normalizedFailureId} was not found for run ${normalizedRunId}.`,
        "AGENTFLOW_FAILURE_NOT_FOUND"
      );
    }
    const timestamp = resolvedAt === undefined ? currentTimestamp(this.now) : validTimestamp(resolvedAt);
    this.write(
      "resolve failure",
      "UPDATE failures SET resolved_at = COALESCE(resolved_at, ?) WHERE run_id = ? AND id = ?",
      [timestamp, normalizedRunId, normalizedFailureId]
    );
  }

  upsertApproval(input: UpsertAgentflowApprovalInput): void {
    this.assertOpen();
    assertOneOf(input.status, APPROVAL_STATUSES, "approval status");
    if (input.status === "requested" && (input.decidedBy !== undefined || input.decision !== undefined || input.decidedAt !== undefined)) {
      throw new AgentflowRunStateError(
        "Requested approvals cannot include decision metadata.",
        "AGENTFLOW_APPROVAL_INVALID"
      );
    }
    const timestamp = currentTimestamp(this.now);
    const decidedAt = input.decidedAt === undefined
      ? TERMINAL_APPROVAL_STATUSES.has(input.status) ? timestamp : null
      : validTimestamp(input.decidedAt);
    this.write("upsert approval", `INSERT INTO approvals (
      run_id, id, step_id, status, requested_by, decided_by, decision, context_json, created_at, updated_at, decided_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(run_id, id) DO UPDATE SET
      step_id = CASE WHEN approvals.status IN ('approved', 'rejected', 'cancelled') OR ? = 1 THEN approvals.step_id ELSE excluded.step_id END,
      status = CASE WHEN approvals.status IN ('approved', 'rejected', 'cancelled') THEN approvals.status ELSE excluded.status END,
      requested_by = CASE WHEN approvals.status IN ('approved', 'rejected', 'cancelled') OR ? = 1 THEN approvals.requested_by ELSE excluded.requested_by END,
      decided_by = CASE WHEN approvals.status IN ('approved', 'rejected', 'cancelled') OR ? = 1 THEN approvals.decided_by ELSE excluded.decided_by END,
      decision = CASE WHEN approvals.status IN ('approved', 'rejected', 'cancelled') OR ? = 1 THEN approvals.decision ELSE excluded.decision END,
      context_json = CASE WHEN approvals.status IN ('approved', 'rejected', 'cancelled') OR ? = 1 THEN approvals.context_json ELSE excluded.context_json END,
      updated_at = CASE WHEN approvals.status IN ('approved', 'rejected', 'cancelled') THEN approvals.updated_at ELSE excluded.updated_at END,
      decided_at = CASE WHEN approvals.status IN ('approved', 'rejected', 'cancelled') OR ? = 1 THEN approvals.decided_at ELSE excluded.decided_at END`, [
      requiredString(input.runId, "Run ID"),
      requiredString(input.id, "Approval ID"),
      optionalString(input.stepId, "Step ID"),
      input.status,
      optionalString(input.requestedBy, "Approval requester"),
      optionalString(input.decidedBy, "Approval decider"),
      optionalString(input.decision, "Approval decision"),
      stableJson(input.context ?? {}),
      timestamp,
      timestamp,
      decidedAt,
      input.stepId === undefined ? 1 : 0,
      input.requestedBy === undefined ? 1 : 0,
      input.decidedBy === undefined ? 1 : 0,
      input.decision === undefined ? 1 : 0,
      input.context === undefined ? 1 : 0,
      decidedAt === null ? 1 : 0
    ]);
  }

  upsertBudget(input: UpsertAgentflowBudgetInput): void {
    this.assertOpen();
    if (!Number.isFinite(input.limit) || input.limit < 0 || !Number.isFinite(input.used) || input.used < 0) {
      throw new AgentflowRunStateError("Budget limit and usage must be non-negative finite numbers.", "AGENTFLOW_BUDGET_INVALID");
    }
    const timestamp = currentTimestamp(this.now);
    this.write("upsert budget", `INSERT INTO budgets (
      run_id, id, step_id, session_id, scope, kind, limit_value, used, unit, created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(run_id, id) DO UPDATE SET
      step_id = excluded.step_id,
      session_id = excluded.session_id,
      scope = excluded.scope,
      kind = excluded.kind,
      limit_value = excluded.limit_value,
      used = excluded.used,
      unit = excluded.unit,
      updated_at = excluded.updated_at`, [
      requiredString(input.runId, "Run ID"),
      requiredString(input.id, "Budget ID"),
      optionalString(input.stepId, "Step ID"),
      optionalString(input.sessionId, "Session ID"),
      requiredString(input.scope, "Budget scope"),
      requiredString(input.kind, "Budget kind"),
      input.limit,
      input.used,
      requiredString(input.unit, "Budget unit"),
      timestamp,
      timestamp
    ]);
  }

  close(): void {
    if (this.closed) return;
    this.database.close();
    this.closed = true;
  }

  private requireRun(id: string): AgentflowRunRecord {
    const row = this.database.get<RunRow>("SELECT * FROM runs WHERE id = ?", [id]);
    if (row === null) {
      throw new AgentflowRunStateError(`Agentflow run ${id} was not found.`, "AGENTFLOW_RUN_NOT_FOUND");
    }
    return hydrateRun(row);
  }

  private write(operation: string, sql: string, params: SqliteValue[]): void {
    try {
      this.database.run(sql, params);
    } catch (error) {
      throw runStateWriteError(operation, error);
    }
  }

  private assertOpen(): void {
    if (this.closed) {
      throw new AgentflowRunStateError("Agentflow run-state store is closed.", "AGENTFLOW_RUN_STATE_CLOSED");
    }
  }
}

export async function openAgentflowRunState(options: OpenAgentflowRunStateOptions = {}): Promise<AgentflowRunStateStore> {
  const repoRoot = findRepositoryRoot(options.cwd ?? process.cwd());
  const databasePath = resolveLocalDatabasePath(repoRoot, options.databasePath ?? DEFAULT_AGENTFLOW_DATABASE_PATH);
  const busyTimeoutMs = validBusyTimeout(options.busyTimeoutMs ?? 5_000);
  fs.mkdirSync(path.dirname(databasePath), { recursive: true });

  const database = await openSqliteDatabase(databasePath, busyTimeoutMs);
  try {
    database.exec("PRAGMA foreign_keys = ON");
    initializeAgentflowRunStateSchema(database, AGENTFLOW_RUN_STATE_SCHEMA_VERSION);
  } catch (error) {
    database.close();
    if (error instanceof AgentflowRunStateSchemaVersionError) {
      throw new AgentflowRunStateError(error.message, "AGENTFLOW_SCHEMA_VERSION", { cause: error });
    }
    if (error instanceof AgentflowRunStateError) throw error;
    throw new AgentflowRunStateError(`Could not initialize Agentflow run-state database: ${errorMessage(error)}`, "AGENTFLOW_SCHEMA_ERROR", { cause: error });
  }

  return new AgentflowRunStateStore({ repoRoot, databasePath, database, now: options.now ?? (() => new Date().toISOString()) });
}

async function openSqliteDatabase(databasePath: string, busyTimeoutMs: number): Promise<SqliteDatabase> {
  if (Boolean((globalThis as { Bun?: unknown }).Bun)) {
    const sqlite = await import("bun:sqlite");
    const database = new sqlite.Database(databasePath);
    database.exec(`PRAGMA busy_timeout = ${validBusyTimeout(busyTimeoutMs)}`);
    return {
      exec: (sql) => database.exec(sql),
      run: (sql, params = []) => { database.query(sql).run(...params); },
      get: <T>(sql: string, params: SqliteValue[] = []) => (database.query(sql).get(...params) as T | null) ?? null,
      close: () => database.close()
    };
  }

  const sqlite = await import("node:sqlite");
  const database = new sqlite.DatabaseSync(databasePath, { timeout: validBusyTimeout(busyTimeoutMs) });
  return {
    exec: (sql) => database.exec(sql),
    run: (sql, params = []) => { database.prepare(sql).run(...params); },
    get: <T>(sql: string, params: SqliteValue[] = []) => (database.prepare(sql).get(...params) as T | null) ?? null,
    close: () => database.close()
  };
}

function findRepositoryRoot(start: string): string {
  const resolvedStart = path.resolve(start);
  if (!fs.existsSync(resolvedStart)) {
    throw new AgentflowRunStateError(`Repository path does not exist: ${resolvedStart}`, "AGENTFLOW_REPOSITORY_NOT_FOUND");
  }
  const realStart = fs.realpathSync(resolvedStart);
  let current = fs.statSync(realStart).isDirectory() ? realStart : path.dirname(realStart);

  while (true) {
    if (fs.existsSync(path.join(current, ".git"))) return fs.realpathSync(current);
    const parent = path.dirname(current);
    if (parent === current) break;
    current = parent;
  }
  throw new AgentflowRunStateError(`Could not find a Git repository from ${start}.`, "AGENTFLOW_REPOSITORY_NOT_FOUND");
}

function resolveLocalDatabasePath(repoRoot: string, configuredPath: string): string {
  const databasePath = path.resolve(repoRoot, configuredPath);
  assertInsideRepository(repoRoot, databasePath);
  const existingParent = nearestExistingPath(path.dirname(databasePath));
  const realParent = fs.realpathSync(existingParent);
  assertInsideRepository(repoRoot, realParent);
  if (isSymbolicLink(databasePath)) {
    throw new AgentflowRunStateError(
      `Agentflow database path cannot be a symbolic link: ${databasePath}`,
      "AGENTFLOW_DATABASE_PATH"
    );
  }
  if (fs.existsSync(databasePath)) assertInsideRepository(repoRoot, fs.realpathSync(databasePath));
  return databasePath;
}

function isSymbolicLink(candidate: string): boolean {
  try {
    return fs.lstatSync(candidate).isSymbolicLink();
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
    throw error;
  }
}

function assertInsideRepository(repoRoot: string, candidate: string): void {
  const relative = path.relative(repoRoot, candidate);
  if (relative === ".." || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative)) {
    throw new AgentflowRunStateError(
      `Agentflow database path must stay inside the repository: ${candidate}`,
      "AGENTFLOW_DATABASE_PATH"
    );
  }
}

function nearestExistingPath(start: string): string {
  let current = start;
  while (!fs.existsSync(current)) {
    const parent = path.dirname(current);
    if (parent === current) return current;
    current = parent;
  }
  return current;
}

function hydrateRun(row: RunRow): AgentflowRunRecord {
  return {
    id: row.id,
    workflowName: row.workflow_name,
    workflowVersion: row.workflow_version,
    workflowStyle: row.workflow_style,
    workflowMaturity: row.workflow_maturity,
    status: row.status,
    parentRunId: row.parent_run_id,
    recoveryOfRunId: row.recovery_of_run_id,
    currentStepId: row.current_step_id,
    inputs: JSON.parse(row.inputs_json) as Record<string, AgentflowRunStateValue>,
    context: JSON.parse(row.context_json) as Record<string, AgentflowRunStateValue>,
    output: row.output_json === null ? null : JSON.parse(row.output_json) as AgentflowRunStateValue,
    error: row.error_json === null ? null : JSON.parse(row.error_json) as AgentflowRunStateValue,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    startedAt: row.started_at,
    finishedAt: row.finished_at
  };
}

function stableJson(value: unknown): string {
  try {
    return JSON.stringify(sortJsonValue(value, new Set()));
  } catch (error) {
    if (error instanceof AgentflowRunStateError) throw error;
    throw new AgentflowRunStateError(
      `Run-state JSON must contain only valid JSON values: ${errorMessage(error)}`,
      "AGENTFLOW_JSON_INVALID",
      { cause: error }
    );
  }
}

function nullableJson(value: AgentflowRunStateValue | undefined | null): string | null {
  return value === undefined || value === null ? null : stableJson(value);
}

function sortJsonValue(value: unknown, ancestors: Set<object>): AgentflowRunStateValue {
  if (typeof value === "number" && !Number.isFinite(value)) {
    throw new AgentflowRunStateError("Run-state JSON numbers must be finite.", "AGENTFLOW_JSON_INVALID");
  }
  if (value === null || typeof value === "string" || typeof value === "boolean" || typeof value === "number") return value;
  if (typeof value !== "object") {
    throw new AgentflowRunStateError("Run-state JSON must contain only valid JSON values.", "AGENTFLOW_JSON_INVALID");
  }
  if (ancestors.has(value)) throw new AgentflowRunStateError("Run-state JSON cannot contain cycles.", "AGENTFLOW_JSON_INVALID");
  ancestors.add(value);
  let sorted: AgentflowRunStateValue;
  if (Array.isArray(value)) {
    if (Object.keys(value).some((key) => !/^(0|[1-9]\d*)$/.test(key)) || Object.keys(value).length !== value.length) {
      throw new AgentflowRunStateError("Run-state JSON arrays cannot be sparse or have named properties.", "AGENTFLOW_JSON_INVALID");
    }
    sorted = value.map((item) => sortJsonValue(item, ancestors));
  } else {
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) {
      throw new AgentflowRunStateError("Run-state JSON objects must be plain objects.", "AGENTFLOW_JSON_INVALID");
    }
    const record = value as Record<string, unknown>;
    sorted = Object.fromEntries(Object.keys(record).sort().map((key) => [key, sortJsonValue(record[key], ancestors)]));
  }
  ancestors.delete(value);
  return sorted;
}

function repoRelativeArtifactPath(value: string): string {
  const candidate = requiredString(value, "Artifact path").replaceAll("\\", "/");
  if (path.posix.isAbsolute(candidate) || path.win32.isAbsolute(candidate)) {
    throw new AgentflowRunStateError("Artifact path must be repo-relative.", "AGENTFLOW_ARTIFACT_PATH");
  }
  const normalized = path.posix.normalize(candidate);
  if (normalized === "." || normalized === ".." || normalized.startsWith("../")) {
    throw new AgentflowRunStateError("Artifact path must be repo-relative and cannot escape the repository root.", "AGENTFLOW_ARTIFACT_PATH");
  }
  return normalized;
}

function requiredString(value: unknown, label: string): string {
  if (typeof value !== "string") {
    throw new AgentflowRunStateError(`${label} must be a non-empty string.`, "AGENTFLOW_RUN_STATE_INVALID");
  }
  const normalized = value.trim();
  if (normalized.length === 0) throw new AgentflowRunStateError(`${label} must be a non-empty string.`, "AGENTFLOW_RUN_STATE_INVALID");
  return normalized;
}

function optionalString(value: unknown, label: string): string | null {
  return value === undefined ? null : requiredString(value, label);
}

function assertOneOf<T extends string>(value: string, allowed: readonly T[], label: string): asserts value is T {
  if (!allowed.includes(value as T)) {
    throw new AgentflowRunStateError(`Invalid ${label}: ${value}.`, "AGENTFLOW_RUN_STATE_INVALID");
  }
}

function currentTimestamp(now: () => string): string {
  return validTimestamp(now());
}

function validTimestamp(value: string): string {
  const timestamp = requiredString(value, "Timestamp");
  if (!Number.isFinite(Date.parse(timestamp))) {
    throw new AgentflowRunStateError(`Invalid timestamp: ${timestamp}.`, "AGENTFLOW_TIMESTAMP_INVALID");
  }
  return new Date(timestamp).toISOString();
}

function validBusyTimeout(value: number): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new AgentflowRunStateError("SQLite busy timeout must be a non-negative integer.", "AGENTFLOW_SQLITE_OPTION");
  }
  return value;
}

function rollback(database: SqliteDatabase): void {
  try {
    database.exec("ROLLBACK");
  } catch {
    // Preserve the original transaction error.
  }
}

function isConstraintError(error: unknown): boolean {
  return /constraint|unique|foreign key/i.test(errorMessage(error));
}

function runStateWriteError(operation: string, error: unknown): AgentflowRunStateError {
  return new AgentflowRunStateError(`Could not ${operation}: ${errorMessage(error)}`, "AGENTFLOW_RUN_STATE_WRITE", { cause: error });
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
