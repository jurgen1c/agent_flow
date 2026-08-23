import fs from "node:fs";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import { AsyncLocalStorage } from "node:async_hooks";
import {
  findGitRepositoryRoot,
  PathContainmentError,
  resolveContainedPath
} from "@jurgen1c/agent-core/repository";
import {
  openSqliteDatabase,
  type SqliteDatabase,
  type SqliteValue
} from "@jurgen1c/agent-core/sqlite";
import type { AgentFlowMaturity, AgentFlowWorkflowStyle } from "./workflow";
import {
  AgentFlowRunStateSchemaVersionError,
  initializeAgentFlowRunStateSchema
} from "./run_state_schema";
import { defaultAgentFlowApprovalOutputPath } from "./approval";

export const AGENT_FLOW_RUN_STATE_SCHEMA_VERSION = 5;
export const DEFAULT_AGENT_FLOW_DATABASE_PATH = ".agent-flow/agent-flow.sqlite";
export const DEFAULT_AGENT_FLOW_RUN_LOCK_TTL_MS = 60_000;
export const AGENT_FLOW_FINAL_SUMMARY_PATH = "final-summary.md";
export const MAX_AGENT_FLOW_RECOVERY_CONTEXT_BYTES = 64 * 1024;
export const MAX_AGENT_FLOW_RECOVERY_CONTEXT_INJECTIONS = 128;

export type AgentFlowRunStatus = "pending" | "running" | "waiting" | "paused" | "completed" | "failed" | "cancelled";
export type AgentFlowRunStopStatus = Extract<AgentFlowRunStatus, "paused" | "failed" | "cancelled">;
export type AgentFlowStepStatus = AgentFlowRunStatus | "skipped";
export type AgentFlowSessionStatus = AgentFlowRunStatus;
export type AgentFlowApprovalStatus = "requested" | "approved" | "rejected" | "cancelled" | "stale";
export type AgentFlowArtifactStatus = "available" | "missing" | "stale" | "overwritten";
export type AgentFlowFailureOutcome = "retry" | "pause" | "fail" | "continue";
export type AgentFlowRunStateValue = null | boolean | number | string | AgentFlowRunStateValue[] | { [key: string]: AgentFlowRunStateValue };

export interface OpenAgentFlowRunStateOptions {
  cwd?: string;
  databasePath?: string;
  now?: () => string;
  busyTimeoutMs?: number;
}

export type AgentFlowRunLockOperation = "run" | "resume";

export interface AgentFlowRunLockRecord {
  runId: string;
  ownerToken: string;
  acquisitionId: string;
  ownerExecutorId: string;
  ownerProcessId: number;
  operation: AgentFlowRunLockOperation;
  acquiredAt: string;
  expiresAt: string;
  recoveredStaleLock: boolean;
}

export interface AcquireAgentFlowRunLockOptions {
  ownerToken?: string;
  ttlMs?: number;
}

export interface AgentFlowRecordPage {
  offset?: number;
  limit: number;
  after?: {
    sortValue: string | number;
    id?: string;
  };
}

export interface AgentFlowRunRecord {
  id: string;
  workflowName: string;
  workflowVersion: number;
  workflowStyle: AgentFlowWorkflowStyle;
  workflowMaturity: AgentFlowMaturity;
  status: AgentFlowRunStatus;
  parentRunId: string | null;
  recoveryOfRunId: string | null;
  currentStepId: string | null;
  inputs: Record<string, AgentFlowRunStateValue>;
  context: Record<string, AgentFlowRunStateValue>;
  output: AgentFlowRunStateValue | null;
  error: AgentFlowRunStateValue | null;
  createdAt: string;
  updatedAt: string;
  startedAt: string | null;
  finishedAt: string | null;
}

export interface CreateAgentFlowRunInput {
  id: string;
  workflow: {
    name: string;
    version: number;
    style: AgentFlowWorkflowStyle;
    maturity: AgentFlowMaturity;
  };
  status?: AgentFlowRunStatus;
  parentRunId?: string;
  recoveryOfRunId?: string;
  currentStepId?: string;
  inputs?: Record<string, AgentFlowRunStateValue>;
  context?: Record<string, AgentFlowRunStateValue>;
}

export interface UpdateAgentFlowRunInput {
  status?: AgentFlowRunStatus;
  currentStepId?: string | null;
  context?: Record<string, AgentFlowRunStateValue>;
  output?: AgentFlowRunStateValue | null;
  error?: AgentFlowRunStateValue | null;
}

export interface FindResumableAgentFlowRunInput {
  workflowName?: string;
  workflowVersion?: number;
}

export interface UpsertAgentFlowStepInput {
  runId: string;
  stepId: string;
  attempt?: number;
  status: AgentFlowStepStatus;
  parentStepId?: string;
  sessionId?: string;
  input?: AgentFlowRunStateValue;
  output?: AgentFlowRunStateValue;
  error?: AgentFlowRunStateValue;
}

export interface UpsertAgentFlowArtifactInput {
  id: string;
  runId: string;
  stepId?: string;
  path: string;
  kind: string;
  contentType: string;
  checksum?: string;
  sizeBytes?: number;
  metadata?: Record<string, AgentFlowRunStateValue>;
}

export interface WriteAgentFlowArtifactInput extends Omit<UpsertAgentFlowArtifactInput, "checksum" | "sizeBytes"> {
  content: string | Uint8Array;
  overwrite?: boolean;
  requiredRunStatus?: AgentFlowRunStatus;
  requiredArtifacts?: Array<{ path: string; checksum: string }>;
  requiredApproval?: { id: string; status: "requested" };
  requiredNoStaleApprovals?: boolean;
  requiredCurrentArtifact?: {
    artifact: null | {
      id: string;
      producerStepId: string | null;
      kind: string;
      contentType: string;
      checksum: string | null;
      generation: number;
      metadata: Record<string, AgentFlowRunStateValue>;
    };
    backingExists: boolean;
    backingChecksum: string | null;
  };
}

export interface AgentFlowArtifactRecord {
  id: string;
  runId: string;
  producerStepId: string | null;
  declaredPath: string;
  storagePath: string;
  kind: string;
  contentType: string;
  status: AgentFlowArtifactStatus;
  checksum: string | null;
  previousChecksum: string | null;
  generation: number;
  sizeBytes: number | null;
  metadata: Record<string, AgentFlowRunStateValue>;
  createdAt: string;
  updatedAt: string;
  writtenAt: string | null;
  checkedAt: string | null;
}

export interface AgentFlowArtifactContent {
  artifact: AgentFlowArtifactRecord;
  content: Buffer;
}

export interface ReadAgentFlowArtifactOptions {
  maxBytes?: number;
}

export interface AppendAgentFlowEventInput {
  id: string;
  runId: string;
  sequence: number;
  stepId?: string;
  sessionId?: string;
  type: string;
  payload?: AgentFlowRunStateValue;
}

export interface AgentFlowEventRecord {
  id: string;
  runId: string;
  sequence: number;
  stepId: string | null;
  sessionId: string | null;
  type: string;
  payload: AgentFlowRunStateValue | null;
  createdAt: string;
}

export interface AgentFlowRunEventInput {
  type: string;
  stepId?: string;
  payload?: AgentFlowRunStateValue;
}

export interface TransitionAgentFlowRunWithEventInput {
  status: AgentFlowRunStatus;
  allowedFrom: AgentFlowRunStatus[];
  event: AgentFlowRunEventInput;
}

export interface UpsertAgentFlowSessionInput {
  id: string;
  runId: string;
  stepId?: string;
  provider: string;
  status: AgentFlowSessionStatus;
  externalSessionId?: string | null;
  state?: Record<string, AgentFlowRunStateValue>;
}

interface SettleAgentFlowSessionInput extends Omit<UpsertAgentFlowSessionInput, "status" | "state"> {
  waitingState: Record<string, AgentFlowRunStateValue>;
  interruptedState: Record<string, AgentFlowRunStateValue>;
}

export interface AgentFlowSessionRecord {
  id: string;
  runId: string;
  stepId: string | null;
  provider: string;
  externalSessionId: string | null;
  status: AgentFlowSessionStatus;
  state: Record<string, AgentFlowRunStateValue>;
  createdAt: string;
  updatedAt: string;
  startedAt: string | null;
  finishedAt: string | null;
}

export interface RecordAgentFlowFailureInput {
  id: string;
  runId: string;
  stepId?: string;
  sessionId?: string;
  classification: string;
  message: string;
  retryable?: boolean;
  payload?: AgentFlowRunStateValue;
  resolvedAt?: string;
}

export interface UpdateAgentFlowFailureRecoveryInput {
  status: "remediated" | "unresolved";
  route: "session" | "workflow";
  target: string;
  recoveryRunId?: string;
  deferResolution?: boolean;
}

export interface AgentFlowFailureRecord {
  id: string;
  runId: string;
  stepId: string | null;
  sessionId: string | null;
  classification: string;
  message: string;
  retryable: boolean;
  attempt: number | null;
  outcome: AgentFlowFailureOutcome | null;
  payloadPath: string | null;
  payload: AgentFlowRunStateValue | null;
  createdAt: string;
  resolvedAt: string | null;
}

export interface UpsertAgentFlowApprovalInput {
  id: string;
  runId: string;
  stepId?: string;
  status: AgentFlowApprovalStatus;
  requestedBy?: string;
  decidedBy?: string;
  decision?: string;
  context?: Record<string, AgentFlowRunStateValue>;
  decidedAt?: string;
}

export interface AgentFlowApprovalRecord {
  id: string;
  runId: string;
  stepId: string | null;
  status: AgentFlowApprovalStatus;
  requestedBy: string | null;
  decidedBy: string | null;
  decision: string | null;
  context: Record<string, AgentFlowRunStateValue>;
  createdAt: string;
  updatedAt: string;
  decidedAt: string | null;
}

export interface UpsertAgentFlowBudgetInput {
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

export type AgentFlowBudgetRecord = UpsertAgentFlowBudgetInput;

interface ArtifactRow {
  run_id: string;
  id: string;
  step_id: string | null;
  path: string;
  kind: string;
  content_type: string;
  checksum: string | null;
  size_bytes: number | null;
  status: AgentFlowArtifactStatus;
  previous_checksum: string | null;
  metadata_json: string;
  created_at: string;
  updated_at: string;
  written_at: string | null;
  checked_at: string | null;
  generation: number;
}

interface EventRow {
  run_id: string;
  id: string;
  sequence: number;
  step_id: string | null;
  session_id: string | null;
  type: string;
  payload_json: string | null;
  created_at: string;
}

interface StepRecoveryRow {
  attempt: number;
  status: AgentFlowStepStatus;
  output_json: string | null;
}

interface RunRow {
  id: string;
  workflow_name: string;
  workflow_version: number;
  workflow_style: AgentFlowWorkflowStyle;
  workflow_maturity: AgentFlowMaturity;
  status: AgentFlowRunStatus;
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

interface RunLockRow {
  run_id: string;
  owner_token: string;
  acquisition_id: string;
  owner_executor_id: string;
  owner_process_id: number;
  operation: AgentFlowRunLockOperation;
  acquired_at: string;
  expires_at: string;
}

interface SessionRow {
  run_id: string;
  id: string;
  step_id: string | null;
  provider: string;
  external_session_id: string | null;
  status: AgentFlowSessionStatus;
  state_json: string;
  created_at: string;
  updated_at: string;
  started_at: string | null;
  finished_at: string | null;
}

interface FailureRow {
  run_id: string;
  id: string;
  step_id: string | null;
  session_id: string | null;
  classification: string;
  message: string;
  retryable: number;
  payload_json: string | null;
  created_at: string;
  resolved_at: string | null;
}

interface ApprovalRow {
  run_id: string;
  id: string;
  step_id: string | null;
  status: AgentFlowApprovalStatus;
  requested_by: string | null;
  decided_by: string | null;
  decision: string | null;
  context_json: string;
  created_at: string;
  updated_at: string;
  decided_at: string | null;
}

const TERMINAL_RUN_STATUSES = new Set<AgentFlowRunStatus>(["completed", "failed", "cancelled"]);
const RUN_STATUSES = ["pending", "running", "waiting", "paused", "completed", "failed", "cancelled"] as const;
const STEP_STATUSES = [...RUN_STATUSES, "skipped"] as const;
const APPROVAL_STATUSES = ["requested", "approved", "rejected", "cancelled", "stale"] as const;
const TERMINAL_APPROVAL_STATUSES = new Set<AgentFlowApprovalStatus>(["approved", "rejected", "cancelled", "stale"]);
const WORKFLOW_STYLES = ["pipeline", "recovery_pipeline", "collaborative"] as const;
const WORKFLOW_MATURITIES = ["draft", "experimental", "stable", "trusted"] as const;
const FAILURE_OUTCOMES = new Set<AgentFlowFailureOutcome>(["retry", "pause", "fail", "continue"]);
const agentFlowExecutorId = randomUUID();
const activeRunLockOwners = new Set<string>();
const RUN_LOCK_RELEASE_ATTEMPTS = 3;

export class AgentFlowRunStateError extends Error {
  readonly code: string;

  constructor(message: string, code = "AGENT_FLOW_RUN_STATE_ERROR", options?: ErrorOptions) {
    super(message, options);
    this.name = "AgentFlowRunStateError";
    this.code = code;
  }
}

export class AgentFlowRunStateStore {
  private artifactBatchActive = false;
  private finalizationTransactionActive = false;
  private finalizationCommitActions: Array<() => void> = [];
  private finalizationRollbackActions: Array<() => void> = [];
  private finalizationArtifactWrites = new Set<string>();
  private finalizationArtifactDeletions = new Set<string>();
  private readonly heldRunLockOwnerKeys = new Set<string>();
  private readonly runLockExecution = new AsyncLocalStorage<AbortSignal>();
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
    installAgentFlowRunLockFencing(this.database);
  }

  currentTimestamp(): string {
    this.assertOpen();
    return currentTimestamp(this.now);
  }

  hasActiveFinalizationTransaction(): boolean {
    this.assertOpen();
    return this.finalizationTransactionActive;
  }

  acquireRunLock(
    id: string,
    operation: AgentFlowRunLockOperation,
    options: AcquireAgentFlowRunLockOptions = {}
  ): AgentFlowRunLockRecord {
    this.assertOpen();
    const runId = requiredString(id, "Run ID");
    assertOneOf(operation, ["run", "resume"] as const, "run lock operation");
    const ownerToken = options.ownerToken === undefined
      ? randomUUID()
      : requiredString(options.ownerToken, "Run lock owner token");
    const acquisitionId = randomUUID();
    const ttlMs = validRunLockTtl(options.ttlMs ?? DEFAULT_AGENT_FLOW_RUN_LOCK_TTL_MS);
    // Lock leases coordinate independent executors, so they must share a real
    // wall-clock domain. The injected clock is reserved for deterministic
    // persisted business timestamps.
    const acquiredAt = new Date().toISOString();
    const expiresAt = new Date(Date.parse(acquiredAt) + ttlMs).toISOString();
    let recoveredStaleLock = false;

    try {
      this.database.exec("BEGIN IMMEDIATE");
      this.requireRun(runId);
      const existing = this.database.get<RunLockRow>("SELECT * FROM run_locks WHERE run_id = ?", [runId]);
      const existingOwnerIsActiveHere = existing !== null
        && existing.owner_executor_id === agentFlowExecutorId
        && activeRunLockOwners.has(runLockOwnerKey(
          this.databasePath,
          existing.run_id,
          existing.owner_token,
          existing.acquisition_id
        ));
      const existingOwnerWasAbandonedHere = existing !== null
        && existing.owner_executor_id === agentFlowExecutorId
        && !existingOwnerIsActiveHere;
      if (existing !== null && (
        (!existingOwnerWasAbandonedHere && Date.parse(existing.expires_at) > Date.parse(acquiredAt))
        || existingOwnerIsActiveHere
      )) {
        throw new AgentFlowRunStateError(
          `Agent Flow run ${runId} is locked for ${existing.operation} by process ${existing.owner_process_id} until ${existing.expires_at}. Retry after the active executor finishes; an expired lease or a lock abandoned by this executor is recovered deterministically.`,
          "AGENT_FLOW_RUN_LOCKED"
        );
      }
      recoveredStaleLock = existing !== null;
      this.database.run(
        `INSERT INTO run_locks (run_id, owner_token, acquisition_id, owner_executor_id, owner_process_id, operation, acquired_at, expires_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(run_id) DO UPDATE SET
           owner_token = excluded.owner_token,
           acquisition_id = excluded.acquisition_id,
           owner_executor_id = excluded.owner_executor_id,
           owner_process_id = excluded.owner_process_id,
           operation = excluded.operation,
           acquired_at = excluded.acquired_at,
           expires_at = excluded.expires_at`,
        [runId, ownerToken, acquisitionId, agentFlowExecutorId, process.pid, operation, acquiredAt, expiresAt]
      );
      this.database.run(
        "INSERT OR REPLACE INTO agent_flow_held_run_locks (run_id, owner_token, acquisition_id) VALUES (?, ?, ?)",
        [runId, ownerToken, acquisitionId]
      );
      this.database.exec("COMMIT");
      const lock = {
        runId,
        ownerToken,
        acquisitionId,
        ownerExecutorId: agentFlowExecutorId,
        ownerProcessId: process.pid,
        operation,
        acquiredAt,
        expiresAt,
        recoveredStaleLock
      };
      const ownerKey = runLockOwnerKey(this.databasePath, runId, ownerToken, acquisitionId);
      activeRunLockOwners.add(ownerKey);
      this.heldRunLockOwnerKeys.add(ownerKey);
      return lock;
    } catch (error) {
      rollback(this.database);
      if (error instanceof AgentFlowRunStateError) throw error;
      if (isSqliteContentionError(error)) throw concurrentMutationError(`acquire the ${operation} lock for run ${runId}`, error);
      throw runStateWriteError(`acquire the ${operation} lock for run ${runId}`, error);
    }
  }

  renewRunLock(lock: AgentFlowRunLockRecord, ttlMs = DEFAULT_AGENT_FLOW_RUN_LOCK_TTL_MS): AgentFlowRunLockRecord {
    this.assertOpen();
    const validatedTtlMs = validRunLockTtl(ttlMs);
    const renewedAt = new Date().toISOString();
    const expiresAt = new Date(Date.parse(renewedAt) + validatedTtlMs).toISOString();
    try {
      this.database.exec("BEGIN IMMEDIATE");
      const current = this.database.get<RunLockRow>(
        "SELECT * FROM run_locks WHERE run_id = ? AND owner_token = ? AND acquisition_id = ? AND owner_executor_id = ?",
        [lock.runId, lock.ownerToken, lock.acquisitionId, lock.ownerExecutorId]
      );
      if (current === null) {
        throw new AgentFlowRunStateError(
          `Agent Flow run ${lock.runId} execution lock was lost before it could be renewed. Stop this executor and retry the operation.`,
          "AGENT_FLOW_RUN_LOCK_LOST"
        );
      }
      this.database.run(
        "UPDATE run_locks SET expires_at = ? WHERE run_id = ? AND owner_token = ? AND acquisition_id = ? AND owner_executor_id = ?",
        [expiresAt, lock.runId, lock.ownerToken, lock.acquisitionId, lock.ownerExecutorId]
      );
      this.database.exec("COMMIT");
      return { ...lock, expiresAt };
    } catch (error) {
      rollback(this.database);
      if (error instanceof AgentFlowRunStateError) throw error;
      if (isSqliteContentionError(error)) throw concurrentMutationError(`renew the execution lock for run ${lock.runId}`, error);
      throw runStateWriteError(`renew the execution lock for run ${lock.runId}`, error);
    }
  }

  releaseRunLock(lock: AgentFlowRunLockRecord): void {
    this.assertOpen();
    try {
      this.database.exec("BEGIN IMMEDIATE");
      this.database.run(
        "DELETE FROM run_locks WHERE run_id = ? AND owner_token = ? AND acquisition_id = ? AND owner_executor_id = ?",
        [lock.runId, lock.ownerToken, lock.acquisitionId, lock.ownerExecutorId]
      );
      this.database.run(
        "DELETE FROM agent_flow_held_run_locks WHERE run_id = ? AND owner_token = ? AND acquisition_id = ?",
        [lock.runId, lock.ownerToken, lock.acquisitionId]
      );
      this.database.exec("COMMIT");
      const ownerKey = runLockOwnerKey(this.databasePath, lock.runId, lock.ownerToken, lock.acquisitionId);
      activeRunLockOwners.delete(ownerKey);
      this.heldRunLockOwnerKeys.delete(ownerKey);
    } catch (error) {
      rollback(this.database);
      if (isSqliteContentionError(error)) throw concurrentMutationError(`release the execution lock for run ${lock.runId}`, error);
      throw runStateWriteError(`release the execution lock for run ${lock.runId}`, error);
    }
  }

  private abandonRunLock(lock: AgentFlowRunLockRecord): void {
    const ownerKey = runLockOwnerKey(this.databasePath, lock.runId, lock.ownerToken, lock.acquisitionId);
    try {
      this.database.exec("BEGIN IMMEDIATE");
      this.database.run(
        `UPDATE run_locks SET expires_at = acquired_at
         WHERE run_id = ? AND owner_token = ? AND acquisition_id = ? AND owner_executor_id = ?`,
        [lock.runId, lock.ownerToken, lock.acquisitionId, lock.ownerExecutorId]
      );
      this.database.run(
        "DELETE FROM agent_flow_held_run_locks WHERE run_id = ? AND owner_token = ? AND acquisition_id = ?",
        [lock.runId, lock.ownerToken, lock.acquisitionId]
      );
      this.database.exec("COMMIT");
    } catch {
      rollback(this.database);
      // The original lease still expires at its configured TTL. Removing its
      // in-process ownership below also permits immediate same-executor recovery.
    } finally {
      activeRunLockOwners.delete(ownerKey);
      this.heldRunLockOwnerKeys.delete(ownerKey);
    }
  }

  async withRunLock<T>(
    runId: string,
    operation: AgentFlowRunLockOperation,
    callback: (lock: AgentFlowRunLockRecord) => Promise<T>,
    options: AcquireAgentFlowRunLockOptions = {}
  ): Promise<T> {
    const ttlMs = validRunLockTtl(options.ttlMs ?? DEFAULT_AGENT_FLOW_RUN_LOCK_TTL_MS);
    let lock = this.acquireRunLock(runId, operation, { ...options, ttlMs });
    let renewalError: unknown;
    const controller = new AbortController();
    let callbackCompleted = false;
    const parentSignal = this.runLockExecution.getStore();
    const abortFromParent = (): void => controller.abort(parentSignal?.reason);
    if (parentSignal?.aborted === true) abortFromParent();
    else parentSignal?.addEventListener("abort", abortFromParent, { once: true });
    const renewalTimer = setInterval(() => {
      try {
        lock = this.renewRunLock(lock, ttlMs);
        renewalError = undefined;
      } catch (error) {
        renewalError = error;
        if (runLockErrorCode(error) === "AGENT_FLOW_RUN_LOCK_LOST" && !controller.signal.aborted) {
          controller.abort(error);
        }
      }
    }, Math.max(1, Math.floor(ttlMs / 3)));
    renewalTimer.unref?.();
    try {
      const result = await this.runLockExecution.run(controller.signal, () => {
        const interruption = this.runLockInterruption();
        if (interruption !== undefined) throw interruption;
        return callback(lock);
      });
      callbackCompleted = true;
      if (runLockErrorCode(renewalError) === "AGENT_FLOW_RUN_LOCK_LOST") throw renewalError;
      let validationError: unknown;
      for (let attempt = 1; attempt <= RUN_LOCK_RELEASE_ATTEMPTS; attempt += 1) {
        try {
          lock = this.renewRunLock(lock, ttlMs);
          validationError = undefined;
          break;
        } catch (error) {
          validationError = error;
          if (runLockErrorCode(error) !== "AGENT_FLOW_CONCURRENT_MUTATION") break;
          if (attempt < RUN_LOCK_RELEASE_ATTEMPTS) await waitForRunLockRetry(attempt * 25);
        }
      }
      if (validationError !== undefined) throw validationError;
      return result;
    } catch (error) {
      if (runLockErrorCode(error) === "AGENT_FLOW_RUN_LOCK_LOST") throw error;
      if (isRunLockFenceError(error)) throw runLockLostError(runId, error);
      throw error;
    } finally {
      clearInterval(renewalTimer);
      parentSignal?.removeEventListener("abort", abortFromParent);
      if (!callbackCompleted) {
        this.abandonRunLock(lock);
      } else {
        let releaseError: unknown;
        for (let attempt = 1; attempt <= RUN_LOCK_RELEASE_ATTEMPTS; attempt += 1) {
          try {
            this.releaseRunLock(lock);
            releaseError = undefined;
            break;
          } catch (error) {
            releaseError = error;
            if (runLockErrorCode(error) !== "AGENT_FLOW_CONCURRENT_MUTATION") break;
            if (attempt < RUN_LOCK_RELEASE_ATTEMPTS) await waitForRunLockRetry(attempt * 25);
          }
        }
        if (releaseError !== undefined) {
          const ownerKey = runLockOwnerKey(this.databasePath, lock.runId, lock.ownerToken, lock.acquisitionId);
          activeRunLockOwners.delete(ownerKey);
          this.heldRunLockOwnerKeys.delete(ownerKey);
          throw releaseError;
        }
      }
    }
  }

  runLockInterruption(): AgentFlowRunStateError | undefined {
    const signal = this.runLockExecution.getStore();
    if (signal?.aborted !== true) return undefined;
    return signal.reason instanceof AgentFlowRunStateError
      ? signal.reason
      : new AgentFlowRunStateError(
        "The active Agent Flow execution lock was lost. Stop this executor and retry the operation.",
        "AGENT_FLOW_RUN_LOCK_LOST",
        { cause: signal.reason }
      );
  }

  recoverInterruptedRun(lock: AgentFlowRunLockRecord): Record<string, number> {
    this.assertOpen();
    if (!lock.recoveredStaleLock) {
      throw new AgentFlowRunStateError(
        `Agent Flow run ${lock.runId} does not have a recovered execution lease.`,
        "AGENT_FLOW_RUN_LOCK_RECOVERY"
      );
    }
    const timestamp = currentTimestamp(this.now);
    try {
      this.database.exec("BEGIN IMMEDIATE");
      const currentLock = this.database.get<Pick<RunLockRow, "owner_token" | "acquisition_id" | "owner_executor_id">>(
        "SELECT owner_token, acquisition_id, owner_executor_id FROM run_locks WHERE run_id = ?",
        [lock.runId]
      );
      if (currentLock?.owner_token !== lock.ownerToken
          || currentLock.acquisition_id !== lock.acquisitionId
          || currentLock.owner_executor_id !== lock.ownerExecutorId) {
        throw runLockLostError(lock.runId);
      }
      const run = this.requireRun(lock.runId);
      const attempts = Object.fromEntries(this.database.all<{ step_id: string; attempt: number }>(
        "SELECT step_id, MAX(attempt) AS attempt FROM run_steps WHERE run_id = ? GROUP BY step_id",
        [lock.runId]
      ).map((row) => [row.step_id, row.attempt]));
      if (run.status !== "running") {
        this.database.exec("COMMIT");
        return attempts;
      }
      this.database.run(
        `UPDATE run_steps SET status = 'cancelled', updated_at = ?, finished_at = COALESCE(finished_at, ?)
         WHERE run_id = ? AND finished_at IS NULL`,
        [timestamp, timestamp, lock.runId]
      );
      this.database.run(
        `UPDATE sessions SET status = 'waiting', updated_at = ?, finished_at = NULL
         WHERE run_id = ? AND status = 'running' AND finished_at IS NULL`,
        [timestamp, lock.runId]
      );
      this.database.run(
        `UPDATE approvals SET status = 'cancelled', decided_by = COALESCE(decided_by, 'system'),
           decision = COALESCE(decision, 'execution_recovered'), updated_at = ?, decided_at = COALESCE(decided_at, ?)
         WHERE run_id = ? AND status = 'requested'`,
        [timestamp, timestamp, lock.runId]
      );
      this.database.run(
        `UPDATE runs SET status = 'pending', error_json = NULL, updated_at = ?, finished_at = NULL
         WHERE id = ? AND status = 'running'`,
        [timestamp, lock.runId]
      );
      this.appendNextEvent(lock.runId, {
        type: "run.execution_recovered",
        payload: {
          priorStatus: "running",
          ...(run.currentStepId === null ? {} : { interruptedStepId: run.currentStepId })
        }
      });
      this.database.exec("COMMIT");
      return attempts;
    } catch (error) {
      rollback(this.database);
      if (error instanceof AgentFlowRunStateError) throw error;
      if (isRunLockFenceError(error)) throw runLockLostError(lock.runId, error);
      throw runStateWriteError(`recover interrupted run ${lock.runId}`, error);
    }
  }

  createRun(input: CreateAgentFlowRunInput): AgentFlowRunRecord {
    this.assertOpen();
    const id = requiredString(input.id, "Run ID");
    const workflowName = requiredString(input.workflow.name, "Workflow name");
    const status = input.status ?? "pending";
    assertOneOf(status, RUN_STATUSES, "run status");
    assertOneOf(input.workflow.style, WORKFLOW_STYLES, "workflow style");
    assertOneOf(input.workflow.maturity, WORKFLOW_MATURITIES, "workflow maturity");
    if (!Number.isSafeInteger(input.workflow.version) || input.workflow.version < 1) {
      throw new AgentFlowRunStateError("Workflow version must be a positive integer.", "AGENT_FLOW_RUN_INVALID");
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
      if (error instanceof AgentFlowRunStateError) throw error;
      if (isConstraintError(error)) {
        throw new AgentFlowRunStateError(`Agent Flow run ${id} already exists or references missing state.`, "AGENT_FLOW_RUN_COLLISION", { cause: error });
      }
      throw runStateWriteError("create run", error);
    }

    return this.requireRun(id);
  }

  createRunWithEvent(input: CreateAgentFlowRunInput, event: AgentFlowRunEventInput): AgentFlowRunRecord {
    this.assertOpen();
    const manageTransaction = !this.finalizationTransactionActive;
    if (manageTransaction) this.database.exec("BEGIN IMMEDIATE");
    try {
      const run = this.createRun(input);
      this.appendNextEvent(run.id, event);
      if (manageTransaction) this.database.exec("COMMIT");
      return run;
    } catch (error) {
      if (manageTransaction) rollback(this.database);
      if (error instanceof AgentFlowRunStateError) throw error;
      throw runStateWriteError("create run with event", error);
    }
  }

  getRun(id: string): AgentFlowRunRecord | null {
    this.assertOpen();
    const row = this.database.get<RunRow>("SELECT * FROM runs WHERE id = ?", [requiredString(id, "Run ID")]);
    return row === null ? null : hydrateRun(row);
  }

  listRuns(): AgentFlowRunRecord[] {
    this.assertOpen();
    return this.database.all<RunRow>(
      "SELECT * FROM runs ORDER BY created_at ASC, id ASC"
    ).map(hydrateRun);
  }

  withRunStateTransaction<T>(runId: string, callback: () => T): T {
    this.assertOpen();
    const normalizedRunId = requiredString(runId, "Run ID");
    if (this.finalizationTransactionActive) {
      this.requireRun(normalizedRunId);
      return callback();
    }
    let transactionStarted = false;
    let databaseCommitted = false;
    try {
      this.database.exec("BEGIN IMMEDIATE");
      transactionStarted = true;
      this.requireRun(normalizedRunId);
      this.finalizationTransactionActive = true;
      this.finalizationCommitActions = [];
      this.finalizationRollbackActions = [];
      this.finalizationArtifactWrites = new Set();
      this.finalizationArtifactDeletions = new Set();
      const result = callback();
      this.database.exec("COMMIT");
      databaseCommitted = true;
      this.finalizationCommitActions.forEach((action) => action());
      return result;
    } catch (error) {
      if (transactionStarted && !databaseCommitted) {
        rollback(this.database);
        [...this.finalizationRollbackActions].reverse().forEach((action) => action());
      }
      if (isSqliteContentionError(error)) {
        throw concurrentMutationError(`start a state transaction for run ${normalizedRunId}`, error);
      }
      throw error;
    } finally {
      if (transactionStarted) {
        this.finalizationTransactionActive = false;
        this.finalizationCommitActions = [];
        this.finalizationRollbackActions = [];
        this.finalizationArtifactWrites = new Set();
        this.finalizationArtifactDeletions = new Set();
      }
    }
  }

  withRunFinalizationTransaction<T>(runId: string, callback: () => T): T {
    return this.withRunStateTransaction(runId, callback);
  }

  updateRun(id: string, input: UpdateAgentFlowRunInput): AgentFlowRunRecord {
    this.assertOpen();
    const runId = requiredString(id, "Run ID");
    const manageTransaction = !this.finalizationTransactionActive;
    if (manageTransaction) this.database.exec("BEGIN IMMEDIATE");

    try {
      const current = this.requireRun(runId);
      const status = input.status ?? current.status;
      assertOneOf(status, RUN_STATUSES, "run status");
      if (TERMINAL_RUN_STATUSES.has(current.status) && status !== current.status) {
        throw new AgentFlowRunStateError(
          `Terminal Agent Flow run ${runId} cannot transition from ${current.status} to ${status}.`,
          "AGENT_FLOW_RUN_TERMINAL"
        );
      }
      if (TERMINAL_RUN_STATUSES.has(current.status)) {
        if (manageTransaction) this.database.exec("COMMIT");
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
      if (manageTransaction) this.database.exec("COMMIT");
    } catch (error) {
      if (manageTransaction) rollback(this.database);
      if (error instanceof AgentFlowRunStateError) throw error;
      throw runStateWriteError("update run", error);
    }

    return this.requireRun(runId);
  }

  transitionRunWithEvent(id: string, input: TransitionAgentFlowRunWithEventInput): AgentFlowRunMutationResult {
    this.assertOpen();
    const runId = requiredString(id, "Run ID");
    assertOneOf(input.status, RUN_STATUSES, "run status");
    for (const status of input.allowedFrom) assertOneOf(status, RUN_STATUSES, "allowed run status");
    const manageTransaction = !this.finalizationTransactionActive;
    if (manageTransaction) this.database.exec("BEGIN IMMEDIATE");

    try {
      const current = this.requireRun(runId);
      if (TERMINAL_RUN_STATUSES.has(current.status) && input.status !== current.status) {
        throw new AgentFlowRunStateError(
          `Terminal Agent Flow run ${runId} cannot transition from ${current.status} to ${input.status}.`,
          "AGENT_FLOW_RUN_TERMINAL"
        );
      }
      if (current.status === input.status) {
        if (manageTransaction) this.database.exec("COMMIT");
        return { changed: false, run: current };
      }
      if (!input.allowedFrom.includes(current.status)) {
        throw new AgentFlowRunStateError(
          `Agent Flow run ${runId} cannot transition from ${current.status} to ${input.status}.`,
          "AGENT_FLOW_RUN_TRANSITION"
        );
      }

      const timestamp = currentTimestamp(this.now);
      const startedAt = current.startedAt ?? (input.status === "running" ? timestamp : null);
      const finishedAt = TERMINAL_RUN_STATUSES.has(input.status) ? current.finishedAt ?? timestamp : null;
      this.database.run(
        "UPDATE runs SET status = ?, updated_at = ?, started_at = ?, finished_at = ? WHERE id = ?",
        [input.status, timestamp, startedAt, finishedAt, runId]
      );
      this.appendNextEvent(runId, input.event);
      const run = this.requireRun(runId);
      if (manageTransaction) this.database.exec("COMMIT");
      return { changed: true, run };
    } catch (error) {
      if (manageTransaction) rollback(this.database);
      if (error instanceof AgentFlowRunStateError) throw error;
      throw runStateWriteError("transition run", error);
    }
  }

  findResumableRun(input: FindResumableAgentFlowRunInput = {}): AgentFlowRunRecord | null {
    this.assertOpen();
    const conditions = ["status IN ('pending', 'running', 'waiting', 'paused')"];
    const params: SqliteValue[] = [];
    if (input.workflowName !== undefined) {
      conditions.push("workflow_name = ?");
      params.push(requiredString(input.workflowName, "Workflow name"));
    }
    if (input.workflowVersion !== undefined) {
      if (!Number.isSafeInteger(input.workflowVersion) || input.workflowVersion < 1) {
        throw new AgentFlowRunStateError("Workflow version must be a positive integer.", "AGENT_FLOW_RUN_INVALID");
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

  upsertStep(input: UpsertAgentFlowStepInput): void {
    this.assertOpen();
    const attempt = input.attempt ?? 1;
    if (!Number.isSafeInteger(attempt) || attempt < 1) {
      throw new AgentFlowRunStateError("Step attempt must be a positive integer.", "AGENT_FLOW_STEP_INVALID");
    }
    assertOneOf(input.status, STEP_STATUSES, "step status");
    const timestamp = currentTimestamp(this.now);
    const startedAt = input.status === "running" ? timestamp : null;
    const finishedAt = TERMINAL_RUN_STATUSES.has(input.status as AgentFlowRunStatus) || input.status === "skipped" ? timestamp : null;

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

  latestStepRecoveryState(
    runId: string,
    stepId: string
  ): { attempt: number; status: AgentFlowStepStatus; output: AgentFlowRunStateValue | null } | null {
    this.assertOpen();
    const row = this.database.get<StepRecoveryRow>(
      `SELECT attempt, status, output_json FROM run_steps
       WHERE run_id = ? AND step_id = ?
       ORDER BY attempt DESC LIMIT 1`,
      [requiredString(runId, "Run ID"), requiredString(stepId, "Step ID")]
    );
    return row === null ? null : {
      attempt: row.attempt,
      status: row.status,
      output: row.output_json === null ? null : JSON.parse(row.output_json) as AgentFlowRunStateValue
    };
  }

  upsertArtifact(input: UpsertAgentFlowArtifactInput): void {
    this.assertOpen();
    const runId = requiredString(input.runId, "Run ID");
    this.requireRun(runId);
    const id = requiredString(input.id, "Artifact ID");
    const artifactPath = normalizeAgentFlowArtifactPath(input.path);
    if (input.sizeBytes !== undefined && (!Number.isSafeInteger(input.sizeBytes) || input.sizeBytes < 0)) {
      throw new AgentFlowRunStateError("Artifact size must be a non-negative integer.", "AGENT_FLOW_ARTIFACT_INVALID");
    }
    const timestamp = currentTimestamp(this.now);
    this.database.exec("BEGIN IMMEDIATE");
    try {
      const existing = this.database.get<ArtifactRow>("SELECT * FROM artifacts WHERE run_id = ? AND id = ?", [runId, id]);
      const pathOwner = this.database.get<ArtifactRow>("SELECT * FROM artifacts WHERE run_id = ? AND path = ?", [runId, artifactPath]);
      if (pathOwner !== null && pathOwner.id !== id) {
        throw new AgentFlowRunStateError(
          `Artifact path ${artifactPath} is already registered as ${pathOwner.id} for run ${runId}.`,
          "AGENT_FLOW_ARTIFACT_COLLISION"
        );
      }
      if (existing !== null && existing.path !== artifactPath) {
        throw new AgentFlowRunStateError(
          `Artifact ${id} is already registered at ${existing.path}; artifact paths cannot be reassigned.`,
          "AGENT_FLOW_ARTIFACT_COLLISION"
        );
      }
      this.database.run(`INSERT INTO artifacts (
        run_id, id, step_id, path, kind, content_type, checksum, size_bytes, status, metadata_json, created_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'missing', ?, ?, ?)
      ON CONFLICT(run_id, id) DO UPDATE SET
        step_id = COALESCE(excluded.step_id, artifacts.step_id),
        kind = excluded.kind,
        content_type = excluded.content_type,
        checksum = CASE
          WHEN artifacts.written_at IS NULL THEN COALESCE(excluded.checksum, artifacts.checksum)
          ELSE artifacts.checksum
        END,
        size_bytes = CASE
          WHEN artifacts.written_at IS NULL THEN COALESCE(excluded.size_bytes, artifacts.size_bytes)
          ELSE artifacts.size_bytes
        END,
        metadata_json = excluded.metadata_json,
        updated_at = excluded.updated_at,
        generation = artifacts.generation + 1`, [
        runId,
        id,
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
      this.database.exec("COMMIT");
    } catch (error) {
      rollback(this.database);
      if (error instanceof AgentFlowRunStateError) throw error;
      throw runStateWriteError("upsert artifact", error);
    }
  }

  writeArtifact(input: WriteAgentFlowArtifactInput): AgentFlowArtifactRecord {
    return this.writeArtifactInternal(
      input,
      !this.artifactBatchActive && !this.finalizationTransactionActive
    );
  }

  private writeArtifactInternal(input: WriteAgentFlowArtifactInput, manageTransaction: boolean): AgentFlowArtifactRecord {
    this.assertOpen();
    const runId = artifactRunId(input.runId);
    const run = this.requireRun(runId);
    const id = requiredString(input.id, "Artifact ID");
    const declaredPath = normalizeAgentFlowArtifactPath(input.path);
    const kind = requiredString(input.kind, "Artifact kind");
    const contentType = requiredString(input.contentType, "Artifact content type");
    const stepId = optionalString(input.stepId, "Step ID");
    if (run.workflowStyle === "pipeline"
        && declaredPath === AGENT_FLOW_FINAL_SUMMARY_PATH
        && (id !== "run:final-summary" || kind !== "run_summary" || stepId !== null)) {
      throw new AgentFlowRunStateError(
        `Artifact path ${AGENT_FLOW_FINAL_SUMMARY_PATH} is reserved for the runtime's final pipeline summary.`,
        "AGENT_FLOW_ARTIFACT_RESERVED"
      );
    }
    const metadataJson = stableJson(input.metadata ?? {});
    const requiredArtifacts = (input.requiredArtifacts ?? []).map((artifact) => ({
      path: normalizeAgentFlowArtifactPath(artifact.path),
      checksum: requiredString(artifact.checksum, "Required artifact checksum")
    }));
    const content = typeof input.content === "string" ? Buffer.from(input.content, "utf8") : Buffer.from(input.content);
    const checksum = `sha256:${createHash("sha256").update(content).digest("hex")}`;
    const target = artifactStoragePath(this.repoRoot, runId, declaredPath, true);
    const timestamp = currentTimestamp(this.now);
    const { temporaryPath, backupPath, deletionBackupPath } = artifactStagingPaths(
      this.repoRoot,
      runId,
      declaredPath
    );
    const alreadyWrittenInFinalization = !manageTransaction
      && this.finalizationTransactionActive
      && this.finalizationArtifactWrites.has(target);
    let targetExistedBeforeWrite = false;
    let fileMutationStarted = false;
    let committed = false;

    try {
      if (manageTransaction) this.database.exec("BEGIN IMMEDIATE");
      if (input.requiredRunStatus !== undefined) {
        const status = this.database.get<{ status: AgentFlowRunStatus }>("SELECT status FROM runs WHERE id = ?", [runId])?.status;
        if (status !== input.requiredRunStatus) {
          throw new AgentFlowRunStateError(
            `Agent Flow run ${runId} must be ${input.requiredRunStatus} to publish ${declaredPath}; current status is ${String(status)}.`,
            "AGENT_FLOW_ARTIFACT_RUN_STATUS"
          );
        }
      }
      if (input.requiredApproval !== undefined) {
        const requiredApproval = input.requiredApproval;
        const current = this.database.get<Pick<ApprovalRow, "status">>(
          "SELECT status FROM approvals WHERE run_id = ? AND id = ?",
          [runId, requiredString(requiredApproval.id, "Required approval ID")]
        );
        if (current?.status !== requiredApproval.status) {
          throw new AgentFlowRunStateError(
            `Approval ${requiredApproval.id} changed before ${declaredPath} could be published.`,
            "AGENT_FLOW_APPROVAL_STALE"
          );
        }
      }
      if (input.requiredNoStaleApprovals === true) {
        const staleApprovalIds = persistedStaleApprovalStepIdsAcrossLineage(this.database, runId);
        if (staleApprovalIds.length > 0) {
          throw new AgentFlowRunStateError(
            `Stale approval${staleApprovalIds.length === 1 ? "" : "s"} ${staleApprovalIds.join(", ")} must be rerun before merge output publication.`,
            "AGENT_FLOW_APPROVAL_STALE"
          );
        }
      }
      for (const requiredArtifact of requiredArtifacts) {
        const current = this.database.get<Pick<ArtifactRow, "checksum">>(
          "SELECT checksum FROM artifacts WHERE run_id = ? AND path = ?",
          [runId, requiredArtifact.path]
        );
        let backingChecksum: string | undefined;
        try {
          const requiredTarget = artifactStoragePath(this.repoRoot, runId, requiredArtifact.path, false);
          backingChecksum = artifactChecksum(requiredTarget);
        } catch {
          backingChecksum = undefined;
        }
        if (current?.checksum !== requiredArtifact.checksum || backingChecksum !== requiredArtifact.checksum) {
          throw new AgentFlowRunStateError(
            `Required input artifact ${requiredArtifact.path} was overwritten before ${declaredPath} could be published.`,
            "AGENT_FLOW_ARTIFACT_STALE"
          );
        }
      }
      const existing = this.database.get<ArtifactRow>("SELECT * FROM artifacts WHERE run_id = ? AND id = ?", [runId, id]);
      const pathOwner = this.database.get<ArtifactRow>("SELECT * FROM artifacts WHERE run_id = ? AND path = ?", [runId, declaredPath]);
      const recoveryInputTakeover = pathOwner !== null
        && pathOwner.id !== id
        && pathOwner.kind === "recovery_input"
        && input.overwrite === true;
      if (pathOwner !== null && pathOwner.id !== id && !recoveryInputTakeover) {
        throw new AgentFlowRunStateError(
          `Artifact path ${declaredPath} is already registered as ${pathOwner.id} for run ${runId}.`,
          "AGENT_FLOW_ARTIFACT_COLLISION"
        );
      }
      if (existing !== null && existing.path !== declaredPath) {
        throw new AgentFlowRunStateError(
          `Artifact ${id} is already registered at ${existing.path}; artifact paths cannot be reassigned.`,
          "AGENT_FLOW_ARTIFACT_COLLISION"
        );
      }
      if (this.finalizationTransactionActive && this.finalizationArtifactDeletions.has(target)) {
        throw new AgentFlowRunStateError(
          `Artifact ${declaredPath} is pending retention deletion and cannot be republished in the same finalization.`,
          "AGENT_FLOW_ARTIFACT_STALE"
        );
      }
      if (!alreadyWrittenInFinalization) {
        recoverArtifactStaging(target, temporaryPath, backupPath, pathOwner?.checksum ?? null);
        recoverArtifactDeletionStaging(target, deletionBackupPath, pathOwner);
      }
      if (input.requiredCurrentArtifact !== undefined) {
        const required = input.requiredCurrentArtifact;
        const requiredArtifact = required?.artifact;
        const currentBackingExists = artifactTargetExists(target);
        const currentBackingChecksum = currentBackingExists ? artifactChecksum(target) : null;
        const rowMatches = requiredArtifact === null
          ? pathOwner === null
          : requiredArtifact !== undefined
            && pathOwner !== null
            && pathOwner.id === requiredArtifact.id
            && pathOwner.step_id === requiredArtifact.producerStepId
            && pathOwner.kind === requiredArtifact.kind
            && pathOwner.content_type === requiredArtifact.contentType
            && pathOwner.checksum === requiredArtifact.checksum
            && pathOwner.generation === requiredArtifact.generation
            && stableJson(JSON.parse(pathOwner.metadata_json)) === stableJson(requiredArtifact.metadata);
        const currentMatches = required !== undefined
          && rowMatches
          && currentBackingExists === required.backingExists
          && currentBackingChecksum === required.backingChecksum;
        if (!currentMatches) {
          throw new AgentFlowRunStateError(
            `Artifact ${declaredPath} changed ownership before it could be published.`,
            "AGENT_FLOW_ARTIFACT_STALE"
          );
        }
      }

      targetExistedBeforeWrite = fs.existsSync(target);
      if (targetExistedBeforeWrite && !fs.statSync(target).isFile()) {
        throw new AgentFlowRunStateError(`Artifact target is not a regular file: ${target}`, "AGENT_FLOW_ARTIFACT_PATH");
      }
      const targetChecksum = targetExistedBeforeWrite ? artifactChecksum(target) : null;
      const priorArtifact = existing ?? (recoveryInputTakeover ? pathOwner : null);
      const retryingPublishedContent = targetChecksum === checksum
        && (existing === null || existing.checksum === checksum || existing.written_at === null);
      const replacingPublishedContent = priorArtifact !== null
        && priorArtifact.written_at !== null
        && priorArtifact.checksum !== checksum;
      if (replacingPublishedContent && input.overwrite !== true) {
        throw new AgentFlowRunStateError(
          `Artifact ${declaredPath} was already published for run ${runId}; pass overwrite: true to replace it.`,
          "AGENT_FLOW_ARTIFACT_OVERWRITE"
        );
      }
      if (targetExistedBeforeWrite && input.overwrite !== true && !retryingPublishedContent) {
        throw new AgentFlowRunStateError(
          `Artifact ${declaredPath} already exists for run ${runId}; pass overwrite: true to replace it.`,
          "AGENT_FLOW_ARTIFACT_OVERWRITE"
        );
      }

      if (!retryingPublishedContent) {
        fs.writeFileSync(temporaryPath, content, { flag: "wx" });
        fileMutationStarted = true;
        if (targetExistedBeforeWrite) {
          if (alreadyWrittenInFinalization) {
            fs.unlinkSync(target);
          } else {
            fs.renameSync(target, backupPath);
          }
        }
        fs.renameSync(temporaryPath, target);
      }
      if (recoveryInputTakeover) {
        this.database.run("DELETE FROM artifacts WHERE run_id = ? AND id = ?", [runId, pathOwner!.id]);
      }
      this.database.run(`INSERT INTO artifacts (
        run_id, id, step_id, path, kind, content_type, checksum, size_bytes, status, previous_checksum,
        metadata_json, created_at, updated_at, written_at, checked_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(run_id, id) DO UPDATE SET
        step_id = COALESCE(excluded.step_id, artifacts.step_id),
        kind = excluded.kind,
        content_type = excluded.content_type,
        checksum = excluded.checksum,
        size_bytes = excluded.size_bytes,
        status = excluded.status,
        previous_checksum = excluded.previous_checksum,
        metadata_json = excluded.metadata_json,
        updated_at = excluded.updated_at,
        written_at = excluded.written_at,
        checked_at = excluded.checked_at,
        generation = artifacts.generation + 1`, [
        runId,
        id,
        stepId,
        declaredPath,
        kind,
        contentType,
        checksum,
        content.byteLength,
        (targetExistedBeforeWrite && !retryingPublishedContent) || replacingPublishedContent
          ? "overwritten"
          : existing?.status === "overwritten" ? "overwritten" : "available",
        (targetExistedBeforeWrite && !retryingPublishedContent) || replacingPublishedContent
          ? priorArtifact?.checksum ?? targetChecksum
          : existing?.previous_checksum ?? null,
        metadataJson,
        timestamp,
        timestamp,
        retryingPublishedContent ? existing?.written_at ?? timestamp : timestamp,
        timestamp
      ]);
      const backingAppeared = !targetExistedBeforeWrite || priorArtifact?.written_at === null;
      if (backingAppeared || priorArtifact?.checksum !== checksum) {
        this.invalidateApprovalsForArtifactChange(runId, declaredPath, checksum, timestamp);
      }
      if (manageTransaction) this.database.exec("COMMIT");
      if (!manageTransaction && this.finalizationTransactionActive
          && fileMutationStarted && !alreadyWrittenInFinalization) {
        this.finalizationArtifactWrites.add(target);
        this.finalizationCommitActions.push(() => {
          removeArtifactStagingEntry(temporaryPath);
          removeArtifactStagingEntry(backupPath);
        });
        this.finalizationRollbackActions.push(() => {
          restoreArtifactWrite(target, temporaryPath, backupPath, targetExistedBeforeWrite);
        });
      }
      committed = true;
    } catch (error) {
      if (manageTransaction) rollback(this.database);
      if (!committed && fileMutationStarted) restoreArtifactWrite(target, temporaryPath, backupPath, targetExistedBeforeWrite);
      else removeArtifactStagingEntry(temporaryPath);
      if (error instanceof AgentFlowRunStateError) throw error;
      if (isRunLockFenceError(error)) throw runLockLostError(runId, error);
      if (isSqliteContentionError(error)) {
        throw concurrentMutationError(`write artifact ${declaredPath} for run ${runId}`, error);
      }
      throw new AgentFlowRunStateError(
        `Could not write artifact ${declaredPath} for run ${runId}: ${errorMessage(error)}`,
        "AGENT_FLOW_ARTIFACT_WRITE",
        { cause: error }
      );
    }

    if (!this.artifactBatchActive && !this.finalizationTransactionActive) {
      removeArtifactStagingEntry(backupPath);
    }

    return this.requireArtifact(runId, id);
  }

  writeArtifactsAtomically(inputs: WriteAgentFlowArtifactInput[]): AgentFlowArtifactRecord[] {
    this.assertOpen();
    if (inputs.length === 0) return [];
    const runId = artifactRunId(inputs[0]!.runId);
    if (inputs.some((input) => artifactRunId(input.runId) !== runId)) {
      throw new AgentFlowRunStateError("Atomic artifact batches must belong to one run.", "AGENT_FLOW_ARTIFACT_INVALID");
    }
    const paths = inputs.map((input) => normalizeAgentFlowArtifactPath(input.path));
    if (new Set(paths).size !== paths.length) {
      throw new AgentFlowRunStateError("Atomic artifact batches must not contain duplicate paths.", "AGENT_FLOW_ARTIFACT_INVALID");
    }
    if (this.finalizationTransactionActive) {
      return inputs.map((input) => this.writeArtifact(input));
    }
    let snapshots: Array<{ declaredPath: string; row: ArtifactRow | null; targetExisted: boolean }> = [];
    let transactionStarted = false;
    try {
      this.database.exec("BEGIN IMMEDIATE");
      transactionStarted = true;
      snapshots = paths.map((declaredPath) => {
        const row = this.database.get<ArtifactRow>("SELECT * FROM artifacts WHERE run_id = ? AND path = ?", [runId, declaredPath]);
        const target = artifactStoragePath(this.repoRoot, runId, declaredPath, false);
        return { declaredPath, row, targetExisted: artifactTargetExists(target) };
      });
      this.artifactBatchActive = true;
      const artifacts = inputs.map((input) => this.writeArtifact(input));
      this.database.exec("COMMIT");
      for (const declaredPath of paths) {
        const { temporaryPath, backupPath } = artifactStagingPaths(this.repoRoot, runId, declaredPath);
        removeArtifactStagingEntry(temporaryPath);
        removeArtifactStagingEntry(backupPath);
      }
      return artifacts;
    } catch (error) {
      if (transactionStarted) {
        rollback(this.database);
        try {
          this.restoreArtifactBatch(runId, snapshots);
          for (const declaredPath of paths) {
            const { temporaryPath, backupPath } = artifactStagingPaths(this.repoRoot, runId, declaredPath);
            removeArtifactStagingEntry(temporaryPath);
            removeArtifactStagingEntry(backupPath);
          }
        } catch (restoreError) {
          throw new AgentFlowRunStateError(
            `Could not roll back atomic artifact batch for run ${runId}: ${errorMessage(restoreError)}`,
            "AGENT_FLOW_ARTIFACT_ROLLBACK",
            { cause: error }
          );
        }
      }
      if (isSqliteContentionError(error)) {
        throw concurrentMutationError(`write an atomic artifact batch for run ${runId}`, error);
      }
      throw error;
    } finally {
      this.artifactBatchActive = false;
    }
  }

  private restoreArtifactBatch(
    runId: string,
    snapshots: Array<{ declaredPath: string; row: ArtifactRow | null; targetExisted: boolean }>
  ): void {
    this.database.exec("BEGIN IMMEDIATE");
    try {
      for (const snapshot of snapshots) {
        const target = artifactStoragePath(this.repoRoot, runId, snapshot.declaredPath, true);
        const { backupPath } = artifactStagingPaths(this.repoRoot, runId, snapshot.declaredPath);
        if (snapshot.targetExisted) {
          if (fs.existsSync(backupPath)) {
            if (isSymbolicLink(backupPath) || !fs.statSync(backupPath).isFile()) {
              throw new AgentFlowRunStateError(
                `Artifact rollback backup is not a regular file: ${backupPath}`,
                "AGENT_FLOW_ARTIFACT_ROLLBACK"
              );
            }
            fs.rmSync(target, { force: true, recursive: true });
            fs.renameSync(backupPath, target);
          } else if (!fs.existsSync(target)) {
            throw new AgentFlowRunStateError(
              `Artifact rollback backup is missing for ${snapshot.declaredPath}.`,
              "AGENT_FLOW_ARTIFACT_ROLLBACK"
            );
          }
        } else {
          removeArtifactStagingEntry(target);
        }
        this.database.run("DELETE FROM artifacts WHERE run_id = ? AND path = ?", [runId, snapshot.declaredPath]);
        if (snapshot.row !== null) {
          const row = snapshot.row;
          this.database.run(`INSERT INTO artifacts (
            run_id, id, step_id, path, kind, content_type, checksum, size_bytes, status, previous_checksum,
            metadata_json, created_at, updated_at, written_at, checked_at, generation
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, [
            row.run_id, row.id, row.step_id, row.path, row.kind, row.content_type, row.checksum, row.size_bytes,
            row.status, row.previous_checksum, row.metadata_json, row.created_at, row.updated_at, row.written_at, row.checked_at,
            row.generation
          ]);
        }
      }
      this.database.exec("COMMIT");
    } catch (error) {
      rollback(this.database);
      throw error;
    }
  }

  listArtifacts(runId: string, page?: AgentFlowRecordPage): AgentFlowArtifactRecord[] {
    this.assertOpen();
    const normalizedRunId = artifactRunId(runId);
    this.requireRun(normalizedRunId);
    const pagination = recordPage(page, "path");
    const rows = this.database.all<ArtifactRow>(
      `SELECT * FROM artifacts WHERE run_id = ?${pagination.where} ORDER BY path ASC, id ASC${pagination.sql}`,
      [normalizedRunId, ...pagination.parameters]
    );
    return rows.map((row) => this.inspectArtifact(row));
  }

  listArtifactMetadata(runId: string): AgentFlowArtifactRecord[] {
    this.assertOpen();
    const normalizedRunId = artifactRunId(runId);
    this.requireRun(normalizedRunId);
    const rows = this.database.all<ArtifactRow>(
      "SELECT * FROM artifacts WHERE run_id = ? ORDER BY path ASC, id ASC",
      [normalizedRunId]
    );
    return rows.map((row) => hydrateArtifact(this.repoRoot, row));
  }

  getArtifact(runId: string, declaredPath: string): AgentFlowArtifactRecord | null {
    this.assertOpen();
    const normalizedRunId = artifactRunId(runId);
    this.requireRun(normalizedRunId);
    const normalizedPath = normalizeAgentFlowArtifactPath(declaredPath);
    const row = this.database.get<ArtifactRow>(
      "SELECT * FROM artifacts WHERE run_id = ? AND path = ?",
      [normalizedRunId, normalizedPath]
    );
    return row === null ? null : this.inspectArtifact(row);
  }

  getArtifactById(runId: string, artifactId: string): AgentFlowArtifactRecord | null {
    this.assertOpen();
    const normalizedRunId = artifactRunId(runId);
    this.requireRun(normalizedRunId);
    const normalizedId = requiredString(artifactId, "Artifact ID");
    const row = this.database.get<ArtifactRow>(
      "SELECT * FROM artifacts WHERE run_id = ? AND id = ?",
      [normalizedRunId, normalizedId]
    );
    return row === null ? null : this.inspectArtifact(row);
  }

  getArtifactBackingSnapshot(runId: string, declaredPath: string): { exists: boolean; checksum: string | null } {
    this.assertOpen();
    const normalizedRunId = artifactRunId(runId);
    this.requireRun(normalizedRunId);
    const normalizedPath = normalizeAgentFlowArtifactPath(declaredPath);
    const target = artifactStoragePath(this.repoRoot, normalizedRunId, normalizedPath, false);
    const exists = artifactTargetExists(target);
    return { exists, checksum: exists ? artifactChecksum(target) : null };
  }

  getArtifactPolicyRoot(runId: string): string {
    this.assertOpen();
    const normalizedRunId = artifactRunId(runId);
    this.requireRun(normalizedRunId);
    const root = artifactStorageRoot(this.repoRoot, normalizedRunId);
    verifyArtifactPath(this.repoRoot, root, true);
    return root;
  }

  deleteArtifactBacking(runId: string, declaredPath: string): AgentFlowArtifactRecord {
    this.assertOpen();
    const normalizedRunId = artifactRunId(runId);
    this.requireRun(normalizedRunId);
    const normalizedPath = normalizeAgentFlowArtifactPath(declaredPath);
    const manageTransaction = !this.finalizationTransactionActive;
    let targetMoved = false;
    let databaseCommitted = false;
    if (manageTransaction) this.database.exec("BEGIN IMMEDIATE");
    try {
      const row = this.database.get<ArtifactRow>(
        "SELECT * FROM artifacts WHERE run_id = ? AND path = ?",
        [normalizedRunId, normalizedPath]
      );
      if (row === null) {
        throw new AgentFlowRunStateError(
          `Declared artifact ${normalizedPath} was not found for run ${normalizedRunId}.`,
          "AGENT_FLOW_ARTIFACT_NOT_FOUND"
        );
      }

      const target = artifactStoragePath(this.repoRoot, normalizedRunId, normalizedPath, false);
      const { temporaryPath, backupPath, deletionBackupPath } = artifactStagingPaths(
        this.repoRoot,
        normalizedRunId,
        normalizedPath
      );
      if (!manageTransaction && this.finalizationArtifactDeletions.has(target)) {
        return this.requireArtifact(normalizedRunId, row.id);
      }
      if (manageTransaction || !this.finalizationArtifactWrites.has(target)) {
        recoverArtifactStaging(target, temporaryPath, backupPath, row.checksum);
      }
      recoverArtifactDeletionStaging(target, deletionBackupPath, row);
      if (artifactTargetExists(target)) {
        fs.renameSync(target, deletionBackupPath);
        targetMoved = true;
      }
      const timestamp = currentTimestamp(this.now);
      this.write(
        "mark artifact backing deleted",
        "UPDATE artifacts SET status = 'missing', checked_at = ?, updated_at = ? WHERE run_id = ? AND path = ?",
        [timestamp, timestamp, normalizedRunId, normalizedPath]
      );
      this.invalidateApprovalsForArtifactChange(normalizedRunId, normalizedPath, null, timestamp);
      const artifact = this.requireArtifact(normalizedRunId, row.id);
      if (manageTransaction) {
        this.database.exec("COMMIT");
        databaseCommitted = true;
        removeArtifactStagingEntry(deletionBackupPath);
      } else if (targetMoved) {
        this.finalizationArtifactDeletions.add(target);
        this.finalizationCommitActions.push(() => removeArtifactStagingEntry(deletionBackupPath));
        this.finalizationRollbackActions.push(() => {
          restoreArtifactWrite(target, temporaryPath, deletionBackupPath, true);
        });
      }
      return artifact;
    } catch (error) {
      if (manageTransaction && !databaseCommitted) rollback(this.database);
      if (targetMoved && !databaseCommitted) {
        const target = artifactStoragePath(this.repoRoot, normalizedRunId, normalizedPath, false);
        const { temporaryPath, deletionBackupPath } = artifactStagingPaths(
          this.repoRoot,
          normalizedRunId,
          normalizedPath
        );
        restoreArtifactWrite(target, temporaryPath, deletionBackupPath, true);
      }
      if (error instanceof AgentFlowRunStateError) throw error;
      throw new AgentFlowRunStateError(
        `Could not delete artifact backing ${normalizedPath} for run ${normalizedRunId}: ${errorMessage(error)}`,
        "AGENT_FLOW_ARTIFACT_DELETE",
        { cause: error }
      );
    }
  }

  recoverArtifactBacking(runId: string, declaredPath: string): void {
    this.assertOpen();
    const normalizedRunId = artifactRunId(runId);
    this.requireRun(normalizedRunId);
    const normalizedPath = normalizeAgentFlowArtifactPath(declaredPath);
    const manageTransaction = !this.finalizationTransactionActive;
    if (manageTransaction) this.database.exec("BEGIN IMMEDIATE");
    try {
      const row = this.database.get<Pick<ArtifactRow, "checksum" | "status">>(
        "SELECT checksum, status FROM artifacts WHERE run_id = ? AND path = ?",
        [normalizedRunId, normalizedPath]
      );
      const target = artifactStoragePath(this.repoRoot, normalizedRunId, normalizedPath, false);
      const { temporaryPath, backupPath, deletionBackupPath } = artifactStagingPaths(
        this.repoRoot,
        normalizedRunId,
        normalizedPath
      );
      recoverArtifactStaging(target, temporaryPath, backupPath, row?.checksum ?? null);
      recoverArtifactDeletionStaging(target, deletionBackupPath, row);
      if (manageTransaction) this.database.exec("COMMIT");
    } catch (error) {
      if (manageTransaction) rollback(this.database);
      throw error;
    }
  }

  readArtifact(
    runId: string,
    declaredPath: string,
    options: ReadAgentFlowArtifactOptions = {}
  ): AgentFlowArtifactContent {
    this.assertOpen();
    const normalizedRunId = artifactRunId(runId);
    this.requireRun(normalizedRunId);
    const normalizedPath = normalizeAgentFlowArtifactPath(declaredPath);
    const maxBytes = options.maxBytes;
    if (maxBytes !== undefined && (!Number.isSafeInteger(maxBytes) || maxBytes < 0)) {
      throw new AgentFlowRunStateError("Artifact read maxBytes must be a non-negative integer.", "AGENT_FLOW_ARTIFACT_INVALID");
    }
    const row = this.database.get<ArtifactRow>(
      "SELECT * FROM artifacts WHERE run_id = ? AND path = ?",
      [normalizedRunId, normalizedPath]
    );
    if (row === null) {
      throw new AgentFlowRunStateError(
        `Declared input artifact ${normalizedPath} was not found for run ${normalizedRunId}.`,
        "AGENT_FLOW_ARTIFACT_NOT_FOUND"
      );
    }
    const metadata = JSON.parse(row.metadata_json) as Record<string, AgentFlowRunStateValue>;
    if (metadata.approvalInvalidated === true) {
      throw new AgentFlowRunStateError(
        `Approval artifact ${normalizedPath} is stale because its evidence changed.`,
        "AGENT_FLOW_ARTIFACT_STALE"
      );
    }

    if (maxBytes !== undefined && row.size_bytes !== null && row.size_bytes > maxBytes) {
      throw new AgentFlowRunStateError(
        `Declared input artifact ${normalizedPath} exceeds the ${maxBytes}-byte read limit.`,
        "AGENT_FLOW_ARTIFACT_TOO_LARGE"
      );
    }

    const target = artifactStoragePath(this.repoRoot, normalizedRunId, normalizedPath, false);
    let descriptor: number;
    try {
      descriptor = fs.openSync(target, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
    } catch (error) {
      const code = (error as NodeJS.ErrnoException).code;
      if (["ENOENT", "ENOTDIR", "ELOOP"].includes(code ?? "")) {
        throw new AgentFlowRunStateError(
          `Declared input artifact ${normalizedPath} is unavailable for run ${normalizedRunId}; publish it before running the transform.`,
          "AGENT_FLOW_ARTIFACT_UNAVAILABLE",
          { cause: error }
        );
      }
      throw error;
    }
    try {
      const stat = fs.fstatSync(descriptor);
      if (!stat.isFile()) {
        throw new AgentFlowRunStateError(`Declared input artifact ${normalizedPath} is not a regular file.`, "AGENT_FLOW_ARTIFACT_STALE");
      }
      if (maxBytes !== undefined && stat.size > maxBytes) {
        throw new AgentFlowRunStateError(
          `Declared input artifact ${normalizedPath} exceeds the ${maxBytes}-byte read limit.`,
          "AGENT_FLOW_ARTIFACT_TOO_LARGE"
        );
      }
      if (row.checksum === null || row.size_bytes === null) {
        throw new AgentFlowRunStateError(
          `Declared input artifact ${normalizedPath} has not been published for run ${normalizedRunId}.`,
          "AGENT_FLOW_ARTIFACT_UNAVAILABLE"
        );
      }
      const buffer = Buffer.allocUnsafe(stat.size);
      let offset = 0;
      while (offset < buffer.byteLength) {
        const bytesRead = fs.readSync(descriptor, buffer, offset, buffer.byteLength - offset, offset);
        if (bytesRead === 0) break;
        offset += bytesRead;
      }
      const overflow = Buffer.allocUnsafe(1);
      const overflowBytes = fs.readSync(descriptor, overflow, 0, 1, offset);
      if (overflowBytes > 0) {
        throw new AgentFlowRunStateError(
          `Declared input artifact ${normalizedPath} changed while it was being read; retry after republishing it.`,
          "AGENT_FLOW_ARTIFACT_STALE"
        );
      }
      const content = buffer.subarray(0, offset);
      const checksum = `sha256:${createHash("sha256").update(content).digest("hex")}`;
      if (row.checksum !== checksum || row.size_bytes !== content.byteLength) {
        throw new AgentFlowRunStateError(
          `Declared input artifact ${normalizedPath} changed while it was being read; retry after republishing it.`,
          "AGENT_FLOW_ARTIFACT_STALE"
        );
      }
      const timestamp = currentTimestamp(this.now);
      const status: AgentFlowArtifactStatus = row.previous_checksum === null ? "available" : "overwritten";
      this.database.run(
        `UPDATE artifacts SET status = ?, checked_at = ?, updated_at = CASE WHEN status = ? THEN updated_at ELSE ? END
         WHERE run_id = ? AND id = ? AND checksum = ?`,
        [status, timestamp, status, timestamp, normalizedRunId, row.id, row.checksum]
      );
      const current = this.database.get<ArtifactRow>(
        "SELECT * FROM artifacts WHERE run_id = ? AND id = ?",
        [normalizedRunId, row.id]
      );
      if (current === null || current.checksum !== row.checksum || current.size_bytes !== row.size_bytes || current.written_at !== row.written_at) {
        throw new AgentFlowRunStateError(
          `Declared input artifact ${normalizedPath} was overwritten while it was being read; retry the transform.`,
          "AGENT_FLOW_ARTIFACT_STALE"
        );
      }
      const artifact = hydrateArtifact(this.repoRoot, current);
      return { artifact, content };
    } finally {
      fs.closeSync(descriptor);
    }
  }

  appendEvent(input: AppendAgentFlowEventInput): void {
    this.assertOpen();
    if (!Number.isSafeInteger(input.sequence) || input.sequence < 1) {
      throw new AgentFlowRunStateError("Event sequence must be a positive integer.", "AGENT_FLOW_EVENT_INVALID");
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

  appendRunEvent(runId: string, event: AgentFlowRunEventInput): void {
    this.assertOpen();
    const normalizedRunId = requiredString(runId, "Run ID");
    this.requireRun(normalizedRunId);
    const manageTransaction = !this.finalizationTransactionActive;
    if (manageTransaction) this.database.exec("BEGIN IMMEDIATE");
    try {
      this.appendNextEvent(normalizedRunId, event);
      if (manageTransaction) this.database.exec("COMMIT");
    } catch (error) {
      if (manageTransaction) rollback(this.database);
      if (error instanceof AgentFlowRunStateError) throw error;
      throw runStateWriteError("append run event", error);
    }
  }

  listEvents(runId: string, page?: AgentFlowRecordPage): AgentFlowEventRecord[] {
    this.assertOpen();
    const normalizedRunId = requiredString(runId, "Run ID");
    this.requireRun(normalizedRunId);
    const pagination = recordPage(page, "sequence");
    return this.database.all<EventRow>(
      `SELECT * FROM events WHERE run_id = ?${pagination.where} ORDER BY sequence ASC, id ASC${pagination.sql}`,
      [normalizedRunId, ...pagination.parameters]
    ).map(hydrateEvent);
  }

  upsertSession(input: UpsertAgentFlowSessionInput): void {
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
      input.externalSessionId === null ? null : optionalString(input.externalSessionId, "External session ID"),
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

  settleSessionForRun(input: SettleAgentFlowSessionInput): AgentFlowRunStopStatus | undefined {
    this.assertOpen();
    const runId = requiredString(input.runId, "Run ID");
    const manageTransaction = !this.finalizationTransactionActive;
    if (manageTransaction) this.database.exec("BEGIN IMMEDIATE");
    try {
      const runStatus = this.requireRun(runId).status;
      const stopped = runStatus === "paused" || runStatus === "failed" || runStatus === "cancelled"
        ? runStatus
        : undefined;
      this.upsertSession({
        id: input.id,
        runId,
        ...(input.stepId === undefined ? {} : { stepId: input.stepId }),
        provider: input.provider,
        ...(input.externalSessionId === undefined ? {} : { externalSessionId: input.externalSessionId }),
        status: stopped ?? "waiting",
        state: stopped === undefined ? input.waitingState : { ...input.interruptedState, interrupted: stopped }
      });
      if (manageTransaction) this.database.exec("COMMIT");
      return stopped;
    } catch (error) {
      if (manageTransaction) rollback(this.database);
      throw error;
    }
  }

  claimSession(input: UpsertAgentFlowSessionInput): void {
    this.assertOpen();
    const runId = requiredString(input.runId, "Run ID");
    const id = requiredString(input.id, "Session ID");
    const timestamp = currentTimestamp(this.now);
    this.database.exec("BEGIN IMMEDIATE");
    try {
      const existing = this.database.get<Pick<SessionRow, "status" | "finished_at">>(
        "SELECT status, finished_at FROM sessions WHERE run_id = ? AND id = ?",
        [runId, id]
      );
      if (existing?.status === "running" || existing?.finished_at !== null && existing?.finished_at !== undefined) {
        throw new AgentFlowRunStateError(
          `Agent Flow session ${id} for run ${runId} is already active or terminal.`,
          "AGENT_FLOW_SESSION_ACTIVE"
        );
      }
      this.database.run(`INSERT INTO sessions (
        run_id, id, step_id, provider, external_session_id, status, state_json,
        created_at, updated_at, started_at, finished_at
      ) VALUES (?, ?, ?, ?, ?, 'running', ?, ?, ?, ?, NULL)
      ON CONFLICT(run_id, id) DO UPDATE SET
        step_id = excluded.step_id, provider = excluded.provider,
        external_session_id = excluded.external_session_id, status = 'running',
        state_json = excluded.state_json, updated_at = excluded.updated_at,
        started_at = COALESCE(sessions.started_at, excluded.started_at), finished_at = NULL`, [
        runId, id, optionalString(input.stepId, "Step ID"), requiredString(input.provider, "Session provider"),
        optionalString(input.externalSessionId ?? undefined, "External session ID"), stableJson(input.state ?? {}),
        timestamp, timestamp, timestamp
      ]);
      this.database.exec("COMMIT");
    } catch (error) {
      rollback(this.database);
      throw error;
    }
  }

  getSession(runId: string, id: string): AgentFlowSessionRecord | null {
    this.assertOpen();
    const normalizedRunId = requiredString(runId, "Run ID");
    this.requireRun(normalizedRunId);
    const row = this.database.get<SessionRow>(
      "SELECT * FROM sessions WHERE run_id = ? AND id = ?",
      [normalizedRunId, requiredString(id, "Session ID")]
    );
    return row === null ? null : hydrateSession(row);
  }

  listSessions(runId: string, page?: AgentFlowRecordPage): AgentFlowSessionRecord[] {
    this.assertOpen();
    const normalizedRunId = requiredString(runId, "Run ID");
    this.requireRun(normalizedRunId);
    const pagination = recordPage(page, "id", null);
    return this.database.all<SessionRow>(
      `SELECT * FROM sessions WHERE run_id = ?${pagination.where} ORDER BY id ASC${pagination.sql}`,
      [normalizedRunId, ...pagination.parameters]
    ).map(hydrateSession);
  }

  injectRecoverySessionContext(runId: string, id: string, context: string): AgentFlowSessionRecord {
    this.assertOpen();
    const normalizedRunId = requiredString(runId, "Run ID");
    const sessionId = requiredString(id, "Session ID");
    if (typeof context !== "string" || context.trim().length === 0) {
      throw new AgentFlowRunStateError(
        "Injected recovery context must be non-empty text.",
        "AGENT_FLOW_RECOVERY_CONTEXT_INVALID"
      );
    }
    const injectedContext = context;
    this.database.exec("BEGIN IMMEDIATE");
    try {
      const run = this.database.get<Pick<RunRow, "status">>(
        "SELECT status FROM runs WHERE id = ?",
        [normalizedRunId]
      );
      if (run === null) {
        throw new AgentFlowRunStateError(
          `Agent Flow run ${normalizedRunId} was not found.`,
          "AGENT_FLOW_RUN_NOT_FOUND"
        );
      }
      if (run.status !== "running") {
        throw new AgentFlowRunStateError(
          `Agent Flow run ${normalizedRunId} must be running before recovery context can be injected; current status is ${run.status}.`,
          "AGENT_FLOW_RECOVERY_CONTEXT_STATUS"
        );
      }
      const row = this.database.get<SessionRow>(
        "SELECT * FROM sessions WHERE run_id = ? AND id = ?",
        [normalizedRunId, sessionId]
      );
      if (row === null || row.status !== "running") {
        throw new AgentFlowRunStateError(
          `Session ${sessionId} is not the active recovery remediation session for run ${normalizedRunId}.`,
          "AGENT_FLOW_RECOVERY_CONTEXT_SESSION"
        );
      }
      const session = hydrateSession(row);
      if (!isRecoveryRemediationState(session.state)) {
        throw new AgentFlowRunStateError(
          `Session ${sessionId} is not the active recovery remediation session for run ${normalizedRunId}.`,
          "AGENT_FLOW_RECOVERY_CONTEXT_SESSION"
        );
      }
      const priorInjections = Array.isArray(session.state.contextInjections)
        ? session.state.contextInjections
        : [];
      const priorContextBytes = priorInjections.reduce<number>((total, entry) => {
        if (entry === null || typeof entry !== "object" || Array.isArray(entry)) return total;
        return total + (typeof entry.context === "string" ? Buffer.byteLength(entry.context, "utf8") : 0);
      }, 0);
      if (priorInjections.length >= MAX_AGENT_FLOW_RECOVERY_CONTEXT_INJECTIONS
          || priorContextBytes + Buffer.byteLength(injectedContext, "utf8") > MAX_AGENT_FLOW_RECOVERY_CONTEXT_BYTES) {
        throw new AgentFlowRunStateError(
          `Injected recovery context exceeds the aggregate limit of ${MAX_AGENT_FLOW_RECOVERY_CONTEXT_INJECTIONS} injections or ${MAX_AGENT_FLOW_RECOVERY_CONTEXT_BYTES} bytes.`,
          "AGENT_FLOW_RECOVERY_CONTEXT_INVALID"
        );
      }
      const priorRevision = typeof session.state.contextRevision === "number"
        && Number.isSafeInteger(session.state.contextRevision)
        && session.state.contextRevision >= 0
        ? session.state.contextRevision
        : 0;
      const revision = priorRevision + 1;
      const timestamp = currentTimestamp(this.now);
      const state: Record<string, AgentFlowRunStateValue> = {
        ...session.state,
        dirty: true,
        contextRevision: revision,
        contextInjections: [
          ...priorInjections,
          { revision, context: injectedContext, injectedAt: timestamp }
        ]
      };
      this.database.run(`UPDATE sessions
        SET state_json = ?, updated_at = ?
        WHERE run_id = ? AND id = ?`, [
        stableJson(state), timestamp, normalizedRunId, sessionId
      ]);
      this.appendNextEvent(normalizedRunId, {
        type: "recovery.context.injected",
        stepId: session.stepId ?? undefined,
        payload: { sessionId, revision }
      });
      this.database.exec("COMMIT");
      return { ...session, state, updatedAt: timestamp };
    } catch (error) {
      rollback(this.database);
      throw error;
    }
  }

  settleRecoverySessionForRunAtContextRevision(
    input: SettleAgentFlowSessionInput,
    revision: number,
    settledState?: () => Record<string, AgentFlowRunStateValue>
  ): { settled: boolean; stopped?: AgentFlowRunStopStatus } {
    this.assertOpen();
    const runId = requiredString(input.runId, "Run ID");
    const sessionId = requiredString(input.id, "Session ID");
    if (!Number.isSafeInteger(revision) || revision < 0) {
      throw new AgentFlowRunStateError(
        "Recovery context revision must be a non-negative integer.",
        "AGENT_FLOW_RECOVERY_CONTEXT_INVALID"
      );
    }
    return this.withRunStateTransaction(runId, () => {
      const row = this.database.get<SessionRow>(
        "SELECT * FROM sessions WHERE run_id = ? AND id = ?",
        [runId, sessionId]
      );
      if (row === null || row.status !== "running") {
        return { settled: false };
      }
      const state = JSON.parse(row.state_json) as Record<string, AgentFlowRunStateValue>;
      if (!isRecoveryRemediationState(state)) {
        return { settled: false };
      }
      const currentRevision = typeof state.contextRevision === "number"
        && Number.isSafeInteger(state.contextRevision)
        && state.contextRevision >= 0
        ? state.contextRevision
        : 0;
      if (currentRevision !== revision) {
        return { settled: false };
      }
      const runStatus = this.requireRun(runId).status;
      const stopped = runStatus === "paused" || runStatus === "failed" || runStatus === "cancelled"
        ? runStatus
        : undefined;
      this.upsertSession({
        id: sessionId,
        runId,
        ...(input.stepId === undefined ? {} : { stepId: input.stepId }),
        provider: input.provider,
        ...(input.externalSessionId === undefined ? {} : { externalSessionId: input.externalSessionId }),
        status: stopped ?? "waiting",
        state: {
          ...state,
          ...(stopped === undefined
            ? { ...input.waitingState, ...(settledState?.() ?? {}) }
            : { ...input.interruptedState, interrupted: stopped }),
          dirty: false,
          appliedContextRevision: revision
        }
      });
      return { settled: true, ...(stopped === undefined ? {} : { stopped }) };
    });
  }

  recordFailure(input: RecordAgentFlowFailureInput): void {
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

  listFailures(runId: string, page?: AgentFlowRecordPage): AgentFlowFailureRecord[] {
    this.assertOpen();
    const normalizedRunId = requiredString(runId, "Run ID");
    this.requireRun(normalizedRunId);
    const pagination = recordPage(page, "created_at");
    const order = page === undefined ? "created_at ASC, rowid ASC" : "created_at ASC, id ASC";
    return this.database.all<FailureRow>(
      `SELECT * FROM failures WHERE run_id = ?${pagination.where} ORDER BY ${order}${pagination.sql}`,
      [normalizedRunId, ...pagination.parameters]
    ).map((row) => {
      const failure = hydrateFailure(row);
      if (failure.payloadPath === null) return failure;
      let artifact: AgentFlowArtifactRecord | null;
      try {
        artifact = this.getArtifact(normalizedRunId, failure.payloadPath);
      } catch {
        artifact = null;
      }
      const metadata = artifact?.metadata;
      return artifact?.kind === "failure_payload"
          && ["available", "overwritten"].includes(artifact.status)
          && metadata !== null
          && typeof metadata === "object"
          && !Array.isArray(metadata)
          && metadata.failureId === failure.id
        ? failure
        : { ...failure, payloadPath: null };
    });
  }

  listPendingReturnedRecoveryFailures(
    runId: string,
    stepId: string,
    successfulAttempt: number
  ): AgentFlowFailureRecord[] {
    this.assertOpen();
    const normalizedRunId = requiredString(runId, "Run ID");
    this.requireRun(normalizedRunId);
    const normalizedStepId = requiredString(stepId, "Step ID");
    if (!Number.isSafeInteger(successfulAttempt) || successfulAttempt < 1) {
      throw new AgentFlowRunStateError(
        "Successful recovery attempt must be a positive integer.",
        "AGENT_FLOW_FAILURE_INVALID"
      );
    }
    return this.database.all<FailureRow>(`SELECT * FROM failures
      WHERE run_id = ? AND step_id = ? AND resolved_at IS NULL
        AND json_extract(payload_json, '$.recovery.status') = 'remediated'
        AND json_extract(payload_json, '$.attempt') < ?
      ORDER BY created_at ASC, rowid ASC`, [normalizedRunId, normalizedStepId, successfulAttempt])
      .map(hydrateFailure);
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
      throw new AgentFlowRunStateError(
        `Agent Flow failure ${normalizedFailureId} was not found for run ${normalizedRunId}.`,
        "AGENT_FLOW_FAILURE_NOT_FOUND"
      );
    }
    const timestamp = resolvedAt === undefined ? currentTimestamp(this.now) : validTimestamp(resolvedAt);
    this.write(
      "resolve failure",
      "UPDATE failures SET resolved_at = COALESCE(resolved_at, ?) WHERE run_id = ? AND id = ?",
      [timestamp, normalizedRunId, normalizedFailureId]
    );
  }

  updateFailureRecovery(
    runId: string,
    failureId: string,
    input: UpdateAgentFlowFailureRecoveryInput
  ): void {
    this.assertOpen();
    const normalizedRunId = requiredString(runId, "Run ID");
    const normalizedFailureId = requiredString(failureId, "Failure ID");
    const existing = this.database.get<{ payload_json: string | null }>(
      "SELECT payload_json FROM failures WHERE run_id = ? AND id = ?",
      [normalizedRunId, normalizedFailureId]
    );
    if (existing === null) {
      throw new AgentFlowRunStateError(
        `Agent Flow failure ${normalizedFailureId} was not found for run ${normalizedRunId}.`,
        "AGENT_FLOW_FAILURE_NOT_FOUND"
      );
    }
    const parsedPayload = existing.payload_json === null
      ? {}
      : JSON.parse(existing.payload_json) as AgentFlowRunStateValue;
    const payload = parsedPayload !== null && typeof parsedPayload === "object" && !Array.isArray(parsedPayload)
      ? parsedPayload
      : {};
    const recovery: Record<string, AgentFlowRunStateValue> = {
      status: input.status,
      route: input.route,
      target: requiredString(input.target, "Recovery target"),
      ...(input.recoveryRunId === undefined
        ? {}
        : { recoveryRunId: requiredString(input.recoveryRunId, "Recovery run ID") })
    };
    this.write(
      "update failure recovery",
      "UPDATE failures SET payload_json = ? WHERE run_id = ? AND id = ?",
      [stableJson({ ...payload, recovery }), normalizedRunId, normalizedFailureId]
    );
    if (input.status === "remediated" && input.deferResolution !== true) {
      this.resolveFailure(normalizedRunId, normalizedFailureId);
    }
  }

  upsertApproval(input: UpsertAgentFlowApprovalInput): void {
    this.assertOpen();
    assertOneOf(input.status, APPROVAL_STATUSES, "approval status");
    if (input.status === "requested" && (input.decidedBy !== undefined || input.decision !== undefined || input.decidedAt !== undefined)) {
      throw new AgentFlowRunStateError(
        "Requested approvals cannot include decision metadata.",
        "AGENT_FLOW_APPROVAL_INVALID"
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
      step_id = CASE WHEN approvals.status IN ('approved', 'rejected', 'cancelled', 'stale') OR ? = 1 THEN approvals.step_id ELSE excluded.step_id END,
      status = CASE WHEN approvals.status IN ('approved', 'rejected', 'cancelled', 'stale') THEN approvals.status ELSE excluded.status END,
      requested_by = CASE WHEN approvals.status IN ('approved', 'rejected', 'cancelled', 'stale') OR ? = 1 THEN approvals.requested_by ELSE excluded.requested_by END,
      decided_by = CASE WHEN approvals.status IN ('approved', 'rejected', 'cancelled', 'stale') OR ? = 1 THEN approvals.decided_by ELSE excluded.decided_by END,
      decision = CASE WHEN approvals.status IN ('approved', 'rejected', 'cancelled', 'stale') OR ? = 1 THEN approvals.decision ELSE excluded.decision END,
      context_json = CASE WHEN approvals.status IN ('approved', 'rejected', 'cancelled', 'stale') OR ? = 1 THEN approvals.context_json ELSE excluded.context_json END,
      updated_at = CASE WHEN approvals.status IN ('approved', 'rejected', 'cancelled', 'stale') THEN approvals.updated_at ELSE excluded.updated_at END,
      decided_at = CASE WHEN approvals.status IN ('approved', 'rejected', 'cancelled', 'stale') OR ? = 1 THEN approvals.decided_at ELSE excluded.decided_at END`, [
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

  listApprovals(runId: string, page?: AgentFlowRecordPage): AgentFlowApprovalRecord[] {
    this.assertOpen();
    const normalizedRunId = requiredString(runId, "Run ID");
    this.requireRun(normalizedRunId);
    const pagination = recordPage(page, "created_at");
    return this.database.all<ApprovalRow>(
      `SELECT * FROM approvals WHERE run_id = ?${pagination.where} ORDER BY created_at ASC, id ASC${pagination.sql}`,
      [normalizedRunId, ...pagination.parameters]
    ).map(hydrateApproval);
  }

  runSnapshotStructuredBytes(runId: string): number {
    this.assertOpen();
    const normalizedRunId = requiredString(runId, "Run ID");
    this.requireRun(normalizedRunId);
    const row = this.database.get<{ bytes: number }>(`SELECT
      COALESCE((SELECT SUM(
        length(CAST(id AS BLOB)) + length(CAST(workflow_name AS BLOB))
        + length(CAST(workflow_style AS BLOB)) + length(CAST(workflow_maturity AS BLOB))
        + length(CAST(status AS BLOB)) + length(CAST(COALESCE(parent_run_id, '') AS BLOB))
        + length(CAST(COALESCE(recovery_of_run_id, '') AS BLOB))
        + length(CAST(COALESCE(current_step_id, '') AS BLOB))
        + length(CAST(inputs_json AS BLOB)) + length(CAST(context_json AS BLOB))
        + length(CAST(COALESCE(output_json, '') AS BLOB)) + length(CAST(COALESCE(error_json, '') AS BLOB))
        + length(CAST(created_at AS BLOB)) + length(CAST(updated_at AS BLOB))
        + length(CAST(COALESCE(started_at, '') AS BLOB)) + length(CAST(COALESCE(finished_at, '') AS BLOB))
      ) FROM runs WHERE id = ?), 0)
      + COALESCE((SELECT SUM(
        length(CAST(run_id AS BLOB)) + length(CAST(id AS BLOB))
        + length(CAST(COALESCE(step_id, '') AS BLOB)) + length(CAST(path AS BLOB))
        + length(CAST(kind AS BLOB)) + length(CAST(content_type AS BLOB))
        + length(CAST(COALESCE(checksum, '') AS BLOB)) + length(CAST(status AS BLOB))
        + length(CAST(COALESCE(previous_checksum, '') AS BLOB)) + length(CAST(metadata_json AS BLOB))
        + length(CAST(created_at AS BLOB)) + length(CAST(updated_at AS BLOB))
        + length(CAST(COALESCE(written_at, '') AS BLOB)) + length(CAST(COALESCE(checked_at, '') AS BLOB))
      ) FROM artifacts WHERE run_id = ?), 0)
      + COALESCE((SELECT SUM(
        length(CAST(run_id AS BLOB)) + length(CAST(id AS BLOB))
        + length(CAST(COALESCE(step_id, '') AS BLOB)) + length(CAST(COALESCE(session_id, '') AS BLOB))
        + length(CAST(type AS BLOB)) + length(CAST(COALESCE(payload_json, '') AS BLOB))
        + length(CAST(created_at AS BLOB))
      ) FROM events WHERE run_id = ?), 0)
      + COALESCE((SELECT SUM(
        length(CAST(run_id AS BLOB)) + length(CAST(id AS BLOB))
        + length(CAST(COALESCE(step_id, '') AS BLOB)) + length(CAST(provider AS BLOB))
        + length(CAST(COALESCE(external_session_id, '') AS BLOB)) + length(CAST(status AS BLOB))
        + length(CAST(state_json AS BLOB)) + length(CAST(created_at AS BLOB))
        + length(CAST(updated_at AS BLOB)) + length(CAST(COALESCE(started_at, '') AS BLOB))
        + length(CAST(COALESCE(finished_at, '') AS BLOB))
      ) FROM sessions WHERE run_id = ?), 0)
      + COALESCE((SELECT SUM(
        length(CAST(run_id AS BLOB)) + length(CAST(id AS BLOB))
        + length(CAST(COALESCE(step_id, '') AS BLOB)) + length(CAST(COALESCE(session_id, '') AS BLOB))
        + length(CAST(classification AS BLOB)) + length(CAST(message AS BLOB))
        + length(CAST(COALESCE(payload_json, '') AS BLOB)) + length(CAST(created_at AS BLOB))
        + length(CAST(COALESCE(resolved_at, '') AS BLOB))
      ) FROM failures WHERE run_id = ?), 0)
      + COALESCE((SELECT SUM(
        length(CAST(run_id AS BLOB)) + length(CAST(id AS BLOB))
        + length(CAST(COALESCE(step_id, '') AS BLOB)) + length(CAST(status AS BLOB))
        + length(CAST(COALESCE(requested_by, '') AS BLOB)) + length(CAST(COALESCE(decided_by, '') AS BLOB))
        + length(CAST(COALESCE(decision, '') AS BLOB)) + length(CAST(context_json AS BLOB))
        + length(CAST(created_at AS BLOB)) + length(CAST(updated_at AS BLOB))
        + length(CAST(COALESCE(decided_at, '') AS BLOB))
      ) FROM approvals WHERE run_id = ?), 0)
      AS bytes`, Array(6).fill(normalizedRunId));
    return row?.bytes ?? 0;
  }

  runSnapshotArtifactBytes(runId: string): number {
    this.assertOpen();
    const normalizedRunId = requiredString(runId, "Run ID");
    this.requireRun(normalizedRunId);
    const rows = this.database.all<ArtifactRow>(`SELECT * FROM artifacts
      WHERE run_id = ? AND status IN ('available', 'overwritten')`, [normalizedRunId]);
    return rows.reduce((bytes, row) => bytes + currentArtifactBackingSize(this.repoRoot, row), 0);
  }

  validateApprovalInvalidationConfiguration(runId: string): void {
    this.assertOpen();
    const normalizedRunId = requiredString(runId, "Run ID");
    this.requireRun(normalizedRunId);
    persistedApprovalInvalidationConfiguration(this.database, normalizedRunId);
  }

  approvalInvalidationPaths(runId: string, stepId: string): string[] {
    this.assertOpen();
    const normalizedRunId = requiredString(runId, "Run ID");
    this.requireRun(normalizedRunId);
    return persistedApprovalInvalidationConfiguration(this.database, normalizedRunId)
      .get(requiredString(stepId, "Approval step ID")) ?? [];
  }

  upsertBudget(input: UpsertAgentFlowBudgetInput): void {
    this.assertOpen();
    if (!Number.isFinite(input.limit) || input.limit < 0 || !Number.isFinite(input.used) || input.used < 0) {
      throw new AgentFlowRunStateError("Budget limit and usage must be non-negative finite numbers.", "AGENT_FLOW_BUDGET_INVALID");
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

  reserveBudgets(inputs: Array<Omit<UpsertAgentFlowBudgetInput, "used"> & { amount: number }>): AgentFlowBudgetRecord[] {
    this.assertOpen();
    if (inputs.length === 0) return [];
    const runId = requiredString(inputs[0]!.runId, "Run ID");
    if (inputs.some((input) => requiredString(input.runId, "Run ID") !== runId)) {
      throw new AgentFlowRunStateError("Budget reservations must belong to one run.", "AGENT_FLOW_BUDGET_INVALID");
    }
    const budgetIds = inputs.map((input) => requiredString(input.id, "Budget ID"));
    if (new Set(budgetIds).size !== budgetIds.length) {
      throw new AgentFlowRunStateError("Atomic budget reservations must not contain duplicate IDs.", "AGENT_FLOW_BUDGET_INVALID");
    }
    this.requireRun(runId);
    const timestamp = currentTimestamp(this.now);
    this.database.exec("BEGIN IMMEDIATE");
    try {
      const reservations = inputs.map((input) => {
        if (!Number.isFinite(input.limit) || input.limit < 0 || !Number.isFinite(input.amount) || input.amount <= 0) {
          throw new AgentFlowRunStateError("Budget limit must be a non-negative finite number and reservation amount must be a positive finite number.", "AGENT_FLOW_BUDGET_INVALID");
        }
        const id = requiredString(input.id, "Budget ID");
        const used = this.database.get<{ used: number }>(
          "SELECT used FROM budgets WHERE run_id = ? AND id = ?",
          [runId, id]
        )?.used ?? 0;
        if (used + input.amount > input.limit) {
          throw new AgentFlowRunStateError(
            `Budget "${input.kind}" would exceed its limit of ${input.limit} (${used} used, ${input.amount} requested).`,
            "AGENT_FLOW_BUDGET_EXCEEDED"
          );
        }
        return { input, id, used: used + input.amount };
      });
      for (const { input, id, used } of reservations) {
        this.database.run(`INSERT INTO budgets (
          run_id, id, step_id, session_id, scope, kind, limit_value, used, unit, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(run_id, id) DO UPDATE SET
          step_id = excluded.step_id, session_id = excluded.session_id, scope = excluded.scope,
          kind = excluded.kind, limit_value = excluded.limit_value, used = excluded.used,
          unit = excluded.unit, updated_at = excluded.updated_at`, [
          runId, id, optionalString(input.stepId, "Step ID"), optionalString(input.sessionId, "Session ID"),
          requiredString(input.scope, "Budget scope"), requiredString(input.kind, "Budget kind"),
          input.limit, used, requiredString(input.unit, "Budget unit"), timestamp, timestamp
        ]);
      }
      this.database.exec("COMMIT");
      return reservations.map(({ id }) => this.getBudget(runId, id)!);
    } catch (error) {
      rollback(this.database);
      throw error;
    }
  }

  getBudget(runId: string, id: string): AgentFlowBudgetRecord | null {
    this.assertOpen();
    const normalizedRunId = requiredString(runId, "Run ID");
    this.requireRun(normalizedRunId);
    const row = this.database.get<{
      id: string;
      run_id: string;
      step_id: string | null;
      session_id: string | null;
      scope: string;
      kind: string;
      limit_value: number;
      used: number;
      unit: string;
    }>("SELECT id, run_id, step_id, session_id, scope, kind, limit_value, used, unit FROM budgets WHERE run_id = ? AND id = ?", [
      normalizedRunId,
      requiredString(id, "Budget ID")
    ]);
    return row === null ? null : {
      id: row.id,
      runId: row.run_id,
      ...(row.step_id === null ? {} : { stepId: row.step_id }),
      ...(row.session_id === null ? {} : { sessionId: row.session_id }),
      scope: row.scope,
      kind: row.kind,
      limit: row.limit_value,
      used: row.used,
      unit: row.unit
    };
  }

  close(): void {
    if (this.closed) return;
    for (const ownerKey of this.heldRunLockOwnerKeys) activeRunLockOwners.delete(ownerKey);
    this.heldRunLockOwnerKeys.clear();
    this.database.close();
    this.closed = true;
  }

  private requireRun(id: string): AgentFlowRunRecord {
    const row = this.database.get<RunRow>("SELECT * FROM runs WHERE id = ?", [id]);
    if (row === null) {
      throw new AgentFlowRunStateError(`Agent Flow run ${id} was not found.`, "AGENT_FLOW_RUN_NOT_FOUND");
    }
    return hydrateRun(row);
  }

  private requireArtifact(runId: string, id: string): AgentFlowArtifactRecord {
    const row = this.database.get<ArtifactRow>("SELECT * FROM artifacts WHERE run_id = ? AND id = ?", [runId, id]);
    if (row === null) {
      throw new AgentFlowRunStateError(`Agent Flow artifact ${id} was not found for run ${runId}.`, "AGENT_FLOW_ARTIFACT_NOT_FOUND");
    }
    return hydrateArtifact(this.repoRoot, row);
  }

  private invalidateApprovalsForArtifactChange(
    runId: string,
    declaredPath: string,
    checksum: string | null,
    timestamp: string
  ): void {
    const approvals = this.database.all<ApprovalRow>(
      "SELECT * FROM approvals WHERE run_id = ? AND status IN ('requested', 'approved') ORDER BY id",
      [runId]
    );
    const invalidatedApprovalIds = new Set<string>();
    const pendingChanges: Array<{ path: string; actualChecksum: string | null }> = [
      { path: declaredPath, actualChecksum: checksum }
    ];
    for (let index = 0; index < pendingChanges.length; index += 1) {
      const changedEvidence = pendingChanges[index]!;
      for (const approval of approvals) {
        if (invalidatedApprovalIds.has(approval.id)) continue;
        const parsedContext = JSON.parse(approval.context_json) as unknown;
        if (parsedContext === null || typeof parsedContext !== "object" || Array.isArray(parsedContext)) continue;
        const context = parsedContext as Record<string, AgentFlowRunStateValue>;
        const evidence = Array.isArray(context.evidence) ? context.evidence : [];
        const requestedEvidenceChanged = approval.status === "requested"
          && Array.isArray(context.artifacts)
          && context.artifacts.includes(changedEvidence.path);
        const invalidatedEvidence = evidence.find((entry) => {
          if (entry === null || typeof entry !== "object" || Array.isArray(entry)) return false;
          const candidate = entry as Record<string, AgentFlowRunStateValue>;
          return candidate.path === changedEvidence.path
            && (changedEvidence.actualChecksum === null || candidate.checksum !== changedEvidence.actualChecksum);
        });
        const configuredInvalidation = approval.step_id !== null
          && approvalInvalidatedBy(this.database, runId, approval.step_id).includes(changedEvidence.path);
        let outputPath: string | undefined;
        if (typeof context.output === "string") {
          try {
            outputPath = normalizeAgentFlowArtifactPath(context.output);
          } catch {
            outputPath = undefined;
          }
        }
        let outcomeChanged = outputPath === changedEvidence.path;
        if (outcomeChanged && approval.status === "requested" && outputPath !== undefined) {
          const output = this.database.get<ArtifactRow>(
            "SELECT * FROM artifacts WHERE run_id = ? AND path = ? AND kind = 'approval_output'",
            [runId, outputPath]
          );
          const metadata = output === null
            ? undefined
            : JSON.parse(output.metadata_json) as Record<string, AgentFlowRunStateValue>;
          const approvalAttempt = /:attempt-(\d+)$/.exec(approval.id)?.[1];
          const outputAttempt = typeof metadata?.attempt === "number" && Number.isSafeInteger(metadata.attempt)
            ? String(metadata.attempt)
            : undefined;
          if (approvalAttempt !== undefined && outputAttempt === approvalAttempt) outcomeChanged = false;
        }
        if (
          invalidatedEvidence === undefined
          && !requestedEvidenceChanged
          && !outcomeChanged
          && !configuredInvalidation
        ) continue;
        invalidatedApprovalIds.add(approval.id);
        const expectedChecksum = invalidatedEvidence === undefined
          ? undefined
          : (invalidatedEvidence as Record<string, AgentFlowRunStateValue>).checksum;
        const invalidation: Record<string, AgentFlowRunStateValue> = {
          reason: "evidence_changed",
          path: changedEvidence.path,
          ...(outcomeChanged ? { source: "approval_output" } : {}),
          ...(configuredInvalidation ? { source: "configured_artifact" } : {}),
          ...(typeof expectedChecksum === "string" ? { expectedChecksum } : {}),
          actualChecksum: changedEvidence.actualChecksum,
          invalidatedAt: timestamp
        };
        this.database.run(
          `UPDATE approvals
           SET status = 'stale', context_json = ?, updated_at = ?, decided_at = COALESCE(decided_at, ?)
           WHERE run_id = ? AND id = ? AND status IN ('requested', 'approved')`,
          [stableJson({ ...context, invalidation }), timestamp, timestamp, runId, approval.id]
        );
        if (outputPath !== undefined) {
          const output = this.database.get<ArtifactRow>(
            "SELECT * FROM artifacts WHERE run_id = ? AND path = ? AND kind = 'approval_output'",
            [runId, outputPath]
          );
          if (output !== null) {
            const metadata = JSON.parse(output.metadata_json) as Record<string, AgentFlowRunStateValue>;
            const approvalAttempt = /:attempt-(\d+)$/.exec(approval.id)?.[1];
            const replacementAttempt = typeof metadata.attempt === "number" && Number.isSafeInteger(metadata.attempt)
              ? String(metadata.attempt)
              : undefined;
            const belongsToNewApprovalAttempt = outcomeChanged
              && approvalAttempt !== undefined
              && replacementAttempt !== undefined
              && replacementAttempt !== approvalAttempt;
            if (!belongsToNewApprovalAttempt) {
              this.database.run(
                `UPDATE artifacts
                 SET status = 'stale', metadata_json = ?, checked_at = ?, updated_at = ?
                 WHERE run_id = ? AND id = ?`,
                [
                  stableJson({ ...metadata, approvalInvalidated: true, approvalInvalidation: invalidation }),
                  timestamp,
                  timestamp,
                  runId,
                  output.id
                ]
              );
            }
          }
          pendingChanges.push({ path: outputPath, actualChecksum: null });
        }
        this.appendNextEvent(runId, {
          type: "approval.invalidated",
          ...(approval.step_id === null ? {} : { stepId: approval.step_id }),
          payload: { approvalId: approval.id, ...invalidation }
        });
      }
    }
  }

  private inspectArtifact(row: ArtifactRow): AgentFlowArtifactRecord {
    let status: AgentFlowArtifactStatus;
    let actualChecksum: string | null = null;
    const metadata = JSON.parse(row.metadata_json) as Record<string, AgentFlowRunStateValue>;
    if (metadata.approvalInvalidated === true) {
      status = "stale";
    } else {
      try {
        const target = artifactStoragePath(this.repoRoot, row.run_id, row.path, false, true);
        if (isSymbolicLink(target)) {
          status = "stale";
        } else {
          const stat = fs.statSync(target);
          if (!stat.isFile() || row.checksum === null || row.size_bytes === null || stat.size !== row.size_bytes) {
            status = "stale";
          } else {
            actualChecksum = artifactChecksum(target);
            status = actualChecksum !== row.checksum
              ? "stale"
              : row.previous_checksum !== null ? "overwritten" : "available";
          }
        }
      } catch (error) {
        const code = (error as NodeJS.ErrnoException).code ?? "";
        if ((error instanceof AgentFlowRunStateError && error.code === "AGENT_FLOW_ARTIFACT_PATH") || code === "ELOOP") {
          status = "stale";
        } else if (["ENOENT", "ENOTDIR"].includes(code)) {
          const { deletionBackupPath } = artifactStagingPaths(this.repoRoot, row.run_id, row.path);
          status = isSymbolicLink(deletionBackupPath)
            ? "stale"
            : fs.existsSync(deletionBackupPath) && fs.statSync(deletionBackupPath).isFile()
              ? row.status
              : "missing";
        } else {
          throw error;
        }
      }
    }
    const timestamp = currentTimestamp(this.now);
    const original = row;
    const updatedAt = status === row.status ? row.updated_at : timestamp;
    const inspected = { ...row, status, checked_at: timestamp, updated_at: updatedAt };
    let statusPersisted = false;
    try {
      this.database.run(
        `UPDATE artifacts SET status = ?, checked_at = ?, updated_at = ?
        WHERE run_id = ? AND id = ? AND checksum IS ? AND status = ? AND updated_at = ?`,
        [status, timestamp, updatedAt, row.run_id, row.id, row.checksum, row.status, row.updated_at]
      );
      statusPersisted = true;
    } catch (error) {
      if (!isSqliteContentionError(error)) throw error;
      row = inspected;
    }
    if (status === "stale" || status === "missing") {
      this.invalidateApprovalsForArtifactChange(row.run_id, row.path, actualChecksum, timestamp);
    }
    if (statusPersisted) {
      row = this.database.get<ArtifactRow>("SELECT * FROM artifacts WHERE run_id = ? AND id = ?", [row.run_id, row.id]) ?? original;
    }
    return hydrateArtifact(this.repoRoot, row);
  }

  private appendNextEvent(runId: string, event: AgentFlowRunEventInput): void {
    const type = requiredString(event.type, "Event type");
    const sequence = this.database.get<{ sequence: number }>(
      "SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence FROM events WHERE run_id = ?",
      [runId]
    )?.sequence ?? 1;
    const idPrefix = `lifecycle:${sequence}:${type}`;
    let id = idPrefix;
    let collision = 0;
    while (this.database.get<{ id: string }>("SELECT id FROM events WHERE run_id = ? AND id = ?", [runId, id]) !== null) {
      collision += 1;
      id = `${idPrefix}:${collision}`;
    }
    this.database.run(
      `INSERT INTO events (run_id, id, sequence, step_id, session_id, type, payload_json, created_at)
      VALUES (?, ?, ?, ?, NULL, ?, ?, ?)`,
      [runId, id, sequence, optionalString(event.stepId, "Step ID"), type, nullableJson(event.payload), currentTimestamp(this.now)]
    );
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
      throw new AgentFlowRunStateError("Agent Flow run-state store is closed.", "AGENT_FLOW_RUN_STATE_CLOSED");
    }
  }
}

function isRecoveryRemediationState(state: Record<string, AgentFlowRunStateValue>): boolean {
  return typeof state.recoveryOfStepId === "string" && state.recoveryOfStepId.trim().length > 0
    && typeof state.failureId === "string" && state.failureId.trim().length > 0;
}

export interface AgentFlowRunMutationResult {
  changed: boolean;
  run: AgentFlowRunRecord;
}

export async function openAgentFlowRunState(options: OpenAgentFlowRunStateOptions = {}): Promise<AgentFlowRunStateStore> {
  const repoRoot = findRepositoryRoot(options.cwd ?? process.cwd());
  const databasePath = resolveLocalDatabasePath(repoRoot, options.databasePath ?? DEFAULT_AGENT_FLOW_DATABASE_PATH);
  const busyTimeoutMs = validBusyTimeout(options.busyTimeoutMs ?? 5_000);
  fs.mkdirSync(path.dirname(databasePath), { recursive: true });

  const database = await openSqliteDatabase(databasePath, { busyTimeoutMs });
  try {
    database.exec("PRAGMA foreign_keys = ON");
    initializeAgentFlowRunStateSchema(database, AGENT_FLOW_RUN_STATE_SCHEMA_VERSION);
  } catch (error) {
    database.close();
    if (error instanceof AgentFlowRunStateSchemaVersionError) {
      throw new AgentFlowRunStateError(error.message, "AGENT_FLOW_SCHEMA_VERSION", { cause: error });
    }
    if (error instanceof AgentFlowRunStateError) throw error;
    throw new AgentFlowRunStateError(`Could not initialize Agent Flow run-state database: ${errorMessage(error)}`, "AGENT_FLOW_SCHEMA_ERROR", { cause: error });
  }

  return new AgentFlowRunStateStore({ repoRoot, databasePath, database, now: options.now ?? (() => new Date().toISOString()) });
}

function findRepositoryRoot(start: string): string {
  const resolvedStart = path.resolve(start);
  if (!fs.existsSync(resolvedStart)) {
    throw new AgentFlowRunStateError(`Repository path does not exist: ${resolvedStart}`, "AGENT_FLOW_REPOSITORY_NOT_FOUND");
  }
  const root = findGitRepositoryRoot(resolvedStart);
  if (root !== null) return root;
  throw new AgentFlowRunStateError(`Could not find a Git repository from ${start}.`, "AGENT_FLOW_REPOSITORY_NOT_FOUND");
}

function resolveLocalDatabasePath(repoRoot: string, configuredPath: string): string {
  try {
    return resolveContainedPath(repoRoot, configuredPath, {
      rejectFinalSymlink: true
    }).absolutePath;
  } catch (error) {
    if (!(error instanceof PathContainmentError)) throw error;
    throw new AgentFlowRunStateError(
      error.reason === "final_symlink"
        ? `Agent Flow database path cannot be a symbolic link: ${error.candidatePath}`
        : `Agent Flow database path must stay inside the repository: ${error.candidatePath}`,
      "AGENT_FLOW_DATABASE_PATH",
      { cause: error }
    );
  }
}

function isSymbolicLink(candidate: string): boolean {
  try {
    return fs.lstatSync(candidate).isSymbolicLink();
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
    throw error;
  }
}

function assertInsideRepository(
  repoRoot: string,
  candidate: string,
  message = `Agent Flow database path must stay inside the repository: ${candidate}`,
  code = "AGENT_FLOW_DATABASE_PATH"
): void {
  try {
    resolveContainedPath(repoRoot, candidate);
  } catch (error) {
    if (!(error instanceof PathContainmentError)) throw error;
    throw new AgentFlowRunStateError(message, code, { cause: error });
  }
}

function hydrateRun(row: RunRow): AgentFlowRunRecord {
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
    inputs: JSON.parse(row.inputs_json) as Record<string, AgentFlowRunStateValue>,
    context: JSON.parse(row.context_json) as Record<string, AgentFlowRunStateValue>,
    output: row.output_json === null ? null : JSON.parse(row.output_json) as AgentFlowRunStateValue,
    error: row.error_json === null ? null : JSON.parse(row.error_json) as AgentFlowRunStateValue,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    startedAt: row.started_at,
    finishedAt: row.finished_at
  };
}

function hydrateArtifact(repoRoot: string, row: ArtifactRow): AgentFlowArtifactRecord {
  return {
    id: row.id,
    runId: row.run_id,
    producerStepId: row.step_id,
    declaredPath: row.path,
    storagePath: path.relative(repoRoot, artifactStorageLocation(repoRoot, row.run_id, row.path)).replaceAll("\\", "/"),
    kind: row.kind,
    contentType: row.content_type,
    status: row.status,
    checksum: row.checksum,
    previousChecksum: row.previous_checksum,
    generation: row.generation,
    sizeBytes: row.size_bytes,
    metadata: JSON.parse(row.metadata_json) as Record<string, AgentFlowRunStateValue>,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    writtenAt: row.written_at,
    checkedAt: row.checked_at
  };
}

function hydrateEvent(row: EventRow): AgentFlowEventRecord {
  return {
    id: row.id,
    runId: row.run_id,
    sequence: row.sequence,
    stepId: row.step_id,
    sessionId: row.session_id,
    type: row.type,
    payload: row.payload_json === null ? null : JSON.parse(row.payload_json) as AgentFlowRunStateValue,
    createdAt: row.created_at
  };
}

function hydrateSession(row: SessionRow): AgentFlowSessionRecord {
  return {
    id: row.id,
    runId: row.run_id,
    stepId: row.step_id,
    provider: row.provider,
    externalSessionId: row.external_session_id,
    status: row.status,
    state: JSON.parse(row.state_json) as Record<string, AgentFlowRunStateValue>,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    startedAt: row.started_at,
    finishedAt: row.finished_at
  };
}

function hydrateApproval(row: ApprovalRow): AgentFlowApprovalRecord {
  return {
    id: row.id,
    runId: row.run_id,
    stepId: row.step_id,
    status: row.status,
    requestedBy: row.requested_by,
    decidedBy: row.decided_by,
    decision: row.decision,
    context: JSON.parse(row.context_json) as Record<string, AgentFlowRunStateValue>,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    decidedAt: row.decided_at
  };
}

function persistedStaleApprovalStepIdsAcrossLineage(
  database: Pick<SqliteDatabase, "all" | "get">,
  runId: string
): string[] {
  const stale = new Set<string>();
  const visited = new Set<string>();
  let currentRunId: string | null = runId;
  while (currentRunId !== null && !visited.has(currentRunId)) {
    visited.add(currentRunId);
    const latestByStep = new Map<string, ApprovalRow>();
    for (const approval of database.all<ApprovalRow>(
      "SELECT * FROM approvals WHERE run_id = ? ORDER BY id",
      [currentRunId]
    )) {
      if (approval.step_id === null || approval.status === "requested" || approval.status === "cancelled") continue;
      const previous = latestByStep.get(approval.step_id);
      if (previous === undefined || approvalAttempt(approval.id) > approvalAttempt(previous.id)
          || approvalAttempt(approval.id) === approvalAttempt(previous.id)
            && (approval.updated_at > previous.updated_at
              || approval.updated_at === previous.updated_at && approval.id > previous.id)) {
        latestByStep.set(approval.step_id, approval);
      }
    }
    for (const [stepId, approval] of latestByStep) {
      if (approval.status === "stale") stale.add(stepId);
    }
    currentRunId = database.get<Pick<RunRow, "parent_run_id">>(
      "SELECT parent_run_id FROM runs WHERE id = ?",
      [currentRunId]
    )?.parent_run_id ?? null;
  }
  return [...stale].sort();
}

function approvalAttempt(id: string): number {
  return Number(/:attempt-(\d+)(?::|$)/.exec(id)?.[1] ?? 0);
}

function approvalInvalidatedBy(
  database: Pick<SqliteDatabase, "get">,
  runId: string,
  stepId: string
): string[] {
  return persistedApprovalInvalidationConfiguration(database, runId).get(stepId) ?? [];
}

function persistedApprovalInvalidationConfiguration(
  database: Pick<SqliteDatabase, "get">,
  runId: string
): Map<string, string[]> {
  const row = database.get<Pick<RunRow, "context_json">>(
    "SELECT context_json FROM runs WHERE id = ?",
    [runId]
  );
  if (row === null) return new Map();
  try {
    const context = JSON.parse(row.context_json) as Record<string, unknown>;
    const workflow = recordValue(context.workflow);
    if (workflow === undefined || workflow.approvals === undefined) return new Map();
    const approvals = recordValue(workflow.approvals);
    if (approvals === undefined) throw invalidPersistedApprovalConfiguration("approvals must be a mapping");
    const approvalSteps = collectPersistedApprovalSteps(workflow.steps);
    const result = new Map<string, string[]>();
    for (const [approvalId, rawConfiguration] of Object.entries(approvals)) {
      const approvalStep = approvalSteps.get(approvalId);
      if (approvalStep === undefined) {
        throw invalidPersistedApprovalConfiguration(`approval key ${JSON.stringify(approvalId)} does not name a declared approval step`);
      }
      const configuration = recordValue(rawConfiguration);
      if (configuration === undefined) {
        throw invalidPersistedApprovalConfiguration(`approval ${JSON.stringify(approvalId)} must be a mapping`);
      }
      const unsupported = Object.keys(configuration).find((field) => field !== "invalidated_by");
      if (unsupported !== undefined) {
        throw invalidPersistedApprovalConfiguration(`approval ${JSON.stringify(approvalId)} has unsupported field ${JSON.stringify(unsupported)}`);
      }
      const invalidatedBy = configuration.invalidated_by;
      if (!Array.isArray(invalidatedBy) || invalidatedBy.length === 0) {
        throw invalidPersistedApprovalConfiguration(`approval ${JSON.stringify(approvalId)} invalidated_by must be a non-empty list`);
      }
      const paths: string[] = [];
      const seen = new Set<string>();
      for (const entry of invalidatedBy) {
        if (!isNormalizedStaticAgentFlowArtifactPath(entry)) {
          throw invalidPersistedApprovalConfiguration(
            `approval ${JSON.stringify(approvalId)} invalidated_by contains invalid path ${JSON.stringify(entry)}`
          );
        }
        if (seen.has(entry)) {
          throw invalidPersistedApprovalConfiguration(
            `approval ${JSON.stringify(approvalId)} invalidated_by contains duplicate path ${JSON.stringify(entry)}`
          );
        }
        if (entry === AGENT_FLOW_FINAL_SUMMARY_PATH) {
          throw invalidPersistedApprovalConfiguration(
            `approval ${JSON.stringify(approvalId)} cannot watch reserved artifact ${JSON.stringify(entry)}`
          );
        }
        seen.add(entry);
        paths.push(entry);
      }
      const output = typeof approvalStep.output === "string" && approvalStep.output.trim().length > 0
        ? approvalStep.output.trim()
        : defaultAgentFlowApprovalOutputPath(approvalId);
      if (seen.has(output)) {
        throw invalidPersistedApprovalConfiguration(
          `approval ${JSON.stringify(approvalId)} cannot be invalidated by its own output ${JSON.stringify(output)}`
        );
      }
      result.set(approvalId, paths);
    }
    return result;
  } catch (error) {
    if (error instanceof AgentFlowRunStateError) throw error;
    throw invalidPersistedApprovalConfiguration(error instanceof Error ? error.message : String(error));
  }
}

function collectPersistedApprovalSteps(
  value: unknown,
  result = new Map<string, Record<string, unknown>>()
): Map<string, Record<string, unknown>> {
  if (!Array.isArray(value)) return result;
  for (const entry of value) {
    const step = recordValue(entry);
    if (step === undefined) continue;
    const id = typeof step.id === "string" ? step.id.trim() : undefined;
    const type = typeof step.type === "string" ? step.type.trim() : undefined;
    if (id !== undefined && id.length > 0 && type === "approval") result.set(id, step);
    for (const field of ["body", "steps"] as const) {
      collectPersistedApprovalSteps(step[field], result);
    }
    if (type === "parallel") collectPersistedApprovalSteps(step.branches, result);
  }
  return result;
}

function invalidPersistedApprovalConfiguration(message: string): AgentFlowRunStateError {
  return new AgentFlowRunStateError(
    `Persisted approval invalidation configuration is invalid: ${message}.`,
    "AGENT_FLOW_WORKFLOW_INVALID"
  );
}

function recordValue(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined;
}

function hydrateFailure(row: FailureRow): AgentFlowFailureRecord {
  const payload = row.payload_json === null
    ? null
    : JSON.parse(row.payload_json) as AgentFlowRunStateValue;
  const payloadRecord = payload !== null && typeof payload === "object" && !Array.isArray(payload)
    ? payload
    : undefined;
  const attempt = payloadRecord?.attempt;
  const outcome = payloadRecord?.outcome;
  const payloadPath = payloadRecord?.failurePayloadPath;
  return {
    id: row.id,
    runId: row.run_id,
    stepId: row.step_id,
    sessionId: row.session_id,
    classification: row.classification,
    message: row.message,
    retryable: row.retryable === 1,
    attempt: typeof attempt === "number" && Number.isSafeInteger(attempt) && attempt > 0 ? attempt : null,
    outcome: typeof outcome === "string" && FAILURE_OUTCOMES.has(outcome as AgentFlowFailureOutcome)
      ? outcome as AgentFlowFailureOutcome
      : null,
    payloadPath: typeof payloadPath === "string" && payloadPath.length > 0 ? payloadPath : null,
    payload,
    createdAt: row.created_at,
    resolvedAt: row.resolved_at
  };
}

function stableJson(value: unknown): string {
  try {
    return JSON.stringify(sortJsonValue(value, new Set()));
  } catch (error) {
    if (error instanceof AgentFlowRunStateError) throw error;
    throw new AgentFlowRunStateError(
      `Run-state JSON must contain only valid JSON values: ${errorMessage(error)}`,
      "AGENT_FLOW_JSON_INVALID",
      { cause: error }
    );
  }
}

function nullableJson(value: AgentFlowRunStateValue | undefined | null): string | null {
  return value === undefined || value === null ? null : stableJson(value);
}

function sortJsonValue(value: unknown, ancestors: Set<object>): AgentFlowRunStateValue {
  if (typeof value === "number" && !Number.isFinite(value)) {
    throw new AgentFlowRunStateError("Run-state JSON numbers must be finite.", "AGENT_FLOW_JSON_INVALID");
  }
  if (value === null || typeof value === "string" || typeof value === "boolean" || typeof value === "number") return value;
  if (typeof value !== "object") {
    throw new AgentFlowRunStateError("Run-state JSON must contain only valid JSON values.", "AGENT_FLOW_JSON_INVALID");
  }
  if (ancestors.has(value)) throw new AgentFlowRunStateError("Run-state JSON cannot contain cycles.", "AGENT_FLOW_JSON_INVALID");
  ancestors.add(value);
  let sorted: AgentFlowRunStateValue;
  if (Array.isArray(value)) {
    if (Object.keys(value).some((key) => !/^(0|[1-9]\d*)$/.test(key)) || Object.keys(value).length !== value.length) {
      throw new AgentFlowRunStateError("Run-state JSON arrays cannot be sparse or have named properties.", "AGENT_FLOW_JSON_INVALID");
    }
    sorted = value.map((item) => sortJsonValue(item, ancestors));
  } else {
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) {
      throw new AgentFlowRunStateError("Run-state JSON objects must be plain objects.", "AGENT_FLOW_JSON_INVALID");
    }
    const record = value as Record<string, unknown>;
    sorted = Object.fromEntries(Object.keys(record).sort().map((key) => [key, sortJsonValue(record[key], ancestors)]));
  }
  ancestors.delete(value);
  return sorted;
}

export function normalizeAgentFlowArtifactPath(value: string): string {
  if (/[\u0000-\u001F\u007F-\u009F\u2028\u2029]/u.test(value)) {
    throw new AgentFlowRunStateError("Artifact path cannot contain control characters.", "AGENT_FLOW_ARTIFACT_PATH");
  }
  const candidate = requiredString(value, "Artifact path").replaceAll("\\", "/");
  if (path.posix.isAbsolute(candidate) || path.win32.isAbsolute(candidate)) {
    throw new AgentFlowRunStateError("Artifact path must be repo-relative.", "AGENT_FLOW_ARTIFACT_PATH");
  }
  if (candidate.endsWith("/")) {
    throw new AgentFlowRunStateError("Artifact path must name a file and cannot end with a separator.", "AGENT_FLOW_ARTIFACT_PATH");
  }
  const normalized = path.posix.normalize(candidate);
  if (normalized === "." || normalized === ".." || normalized.startsWith("../")) {
    throw new AgentFlowRunStateError("Artifact path must be repo-relative and cannot escape the repository root.", "AGENT_FLOW_ARTIFACT_PATH");
  }
  return normalized;
}

export function isNormalizedStaticAgentFlowArtifactPath(value: unknown): value is string {
  if (typeof value !== "string"
      || value.length === 0
      || value !== value.trim()
      || value.includes("{{")
      || value.includes("}}")) return false;
  try {
    return normalizeAgentFlowArtifactPath(value) === value;
  } catch {
    return false;
  }
}

function artifactRunId(value: string): string {
  return requiredString(value, "Run ID");
}

function artifactStoragePath(
  repoRoot: string,
  runId: string,
  declaredPath: string,
  createParent: boolean,
  allowTargetSymlink = false
): string {
  const target = artifactStorageLocation(repoRoot, runId, declaredPath);
  verifyArtifactPath(repoRoot, path.dirname(target), createParent);
  if (!allowTargetSymlink && isSymbolicLink(target)) {
    throw new AgentFlowRunStateError(`Artifact path cannot be a symbolic link: ${target}`, "AGENT_FLOW_ARTIFACT_PATH");
  }
  return target;
}

function artifactStorageLocation(repoRoot: string, runId: string, declaredPath: string): string {
  const artifactRoot = artifactStorageRoot(repoRoot, runId);
  const normalizedPath = normalizeAgentFlowArtifactPath(declaredPath);
  const target = path.join(artifactRoot, artifactFileName(normalizedPath));
  assertInsideRepository(
    repoRoot,
    target,
    `Artifact path must stay inside the repository: ${target}`,
    "AGENT_FLOW_ARTIFACT_PATH"
  );
  return target;
}

function artifactStorageRoot(repoRoot: string, runId: string): string {
  const normalizedRunId = artifactRunId(runId);
  const artifactRoot = path.join(repoRoot, ".agent-flow", "runs", artifactRunDirectory(normalizedRunId), "artifacts");
  assertInsideRepository(
    repoRoot,
    artifactRoot,
    `Artifact root must stay inside the repository: ${artifactRoot}`,
    "AGENT_FLOW_ARTIFACT_PATH"
  );
  return artifactRoot;
}

function artifactRunDirectory(runId: string): string {
  return `r-${createHash("sha256").update(runId).digest("hex")}`;
}

function artifactFileName(declaredPath: string): string {
  return `a-${createHash("sha256").update(declaredPath).digest("hex")}`;
}

function artifactStagingPaths(
  repoRoot: string,
  runId: string,
  declaredPath: string
): { temporaryPath: string; backupPath: string; deletionBackupPath: string } {
  const stagingDirectory = path.join(repoRoot, ".agent-flow", "runs", artifactRunDirectory(runId), ".staging");
  assertInsideRepository(
    repoRoot,
    stagingDirectory,
    `Artifact staging path must stay inside the repository: ${stagingDirectory}`,
    "AGENT_FLOW_ARTIFACT_PATH"
  );
  verifyArtifactPath(repoRoot, stagingDirectory, true);
  const key = createHash("sha256").update(declaredPath).digest("hex");
  return {
    temporaryPath: path.join(stagingDirectory, `${key}.new`),
    backupPath: path.join(stagingDirectory, `${key}.old`),
    deletionBackupPath: path.join(stagingDirectory, `${key}.deleted`)
  };
}

function currentArtifactBackingSize(repoRoot: string, row: ArtifactRow): number {
  if (row.size_bytes === null || row.checksum === null) return 0;
  const target = artifactStoragePath(repoRoot, row.run_id, row.path, false, true);
  const targetSize = regularFileSize(target);
  if (targetSize !== null) {
    return targetSize === row.size_bytes && artifactChecksum(target) === row.checksum ? row.size_bytes : 0;
  }
  const deletionBackupPath = artifactStagingPaths(repoRoot, row.run_id, row.path).deletionBackupPath;
  const backupSize = regularFileSize(deletionBackupPath);
  return backupSize === row.size_bytes && artifactChecksum(deletionBackupPath) === row.checksum ? row.size_bytes : 0;
}

function regularFileSize(candidate: string): number | null {
  try {
    const stat = fs.lstatSync(candidate);
    return stat.isFile() && !stat.isSymbolicLink() ? stat.size : null;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw error;
  }
}

function verifyArtifactPath(repoRoot: string, candidate: string, createDirectories: boolean): void {
  const relative = path.relative(repoRoot, candidate);
  const segments = relative.split(path.sep).filter(Boolean);
  let current = repoRoot;
  for (const segment of segments) {
    current = path.join(current, segment);
    if (!fs.existsSync(current)) {
      if (!createDirectories) return;
      try {
        fs.mkdirSync(current);
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
      }
    }
    const stat = fs.lstatSync(current);
    if (stat.isSymbolicLink()) {
      throw new AgentFlowRunStateError(`Artifact storage cannot traverse a symbolic link: ${current}`, "AGENT_FLOW_ARTIFACT_PATH");
    }
    if (!stat.isDirectory() && (createDirectories || current !== candidate)) {
      throw new AgentFlowRunStateError(`Artifact storage parent is not a directory: ${current}`, "AGENT_FLOW_ARTIFACT_PATH");
    }
  }
}

function restoreArtifactWrite(target: string, temporaryPath: string, backupPath: string, targetExisted: boolean): void {
  try {
    if (fs.existsSync(temporaryPath)) fs.unlinkSync(temporaryPath);
    if (targetExisted && fs.existsSync(backupPath)) {
      fs.rmSync(target, { force: true, recursive: true });
      fs.renameSync(backupPath, target);
    } else if (!targetExisted && fs.existsSync(target)) {
      fs.unlinkSync(target);
    }
  } catch {
    // Preserve the original write or database error.
  }
}

function recoverArtifactStaging(target: string, temporaryPath: string, backupPath: string, registeredChecksum: string | null): void {
  if (isSymbolicLink(backupPath)) {
    throw new AgentFlowRunStateError(`Artifact recovery backup cannot be a symbolic link: ${backupPath}`, "AGENT_FLOW_ARTIFACT_PATH");
  }
  if (fs.existsSync(backupPath)) {
    if (!fs.statSync(backupPath).isFile()) {
      removeArtifactStagingEntry(backupPath);
      removeArtifactStagingEntry(temporaryPath);
      return;
    }
    const targetMatchesRegistry = fs.existsSync(target)
      && !isSymbolicLink(target)
      && fs.statSync(target).isFile()
      && registeredChecksum !== null
      && artifactChecksum(target) === registeredChecksum;
    if (targetMatchesRegistry) {
      removeArtifactStagingEntry(backupPath);
    } else if (registeredChecksum === null || artifactChecksum(backupPath) === registeredChecksum) {
      fs.rmSync(target, { force: true, recursive: true });
      fs.renameSync(backupPath, target);
    } else {
      removeArtifactStagingEntry(backupPath);
    }
  }
  removeArtifactStagingEntry(temporaryPath);
}

function recoverArtifactDeletionStaging(
  target: string,
  deletionBackupPath: string,
  registeredArtifact: Pick<ArtifactRow, "checksum" | "status"> | null
): void {
  if (isSymbolicLink(deletionBackupPath)) {
    throw new AgentFlowRunStateError(
      `Artifact deletion recovery backup cannot be a symbolic link: ${deletionBackupPath}`,
      "AGENT_FLOW_ARTIFACT_PATH"
    );
  }
  if (!fs.existsSync(deletionBackupPath)) return;
  if (!fs.statSync(deletionBackupPath).isFile()
      || registeredArtifact === null
      || registeredArtifact.status === "missing") {
    removeArtifactStagingEntry(deletionBackupPath);
    return;
  }

  const targetMatchesRegistry = artifactTargetExists(target)
    && registeredArtifact.checksum !== null
    && artifactChecksum(target) === registeredArtifact.checksum;
  if (targetMatchesRegistry) {
    removeArtifactStagingEntry(deletionBackupPath);
    return;
  }
  if (registeredArtifact.checksum !== null
      && artifactChecksum(deletionBackupPath) !== registeredArtifact.checksum) {
    throw new AgentFlowRunStateError(
      `Artifact deletion recovery backup does not match the registered checksum: ${deletionBackupPath}`,
      "AGENT_FLOW_ARTIFACT_RECOVERY"
    );
  }

  fs.rmSync(target, { force: true, recursive: true });
  fs.renameSync(deletionBackupPath, target);
}

function removeArtifactStagingEntry(candidate: string): void {
  try {
    fs.rmSync(candidate, { force: true, recursive: true });
  } catch {
    // Staging cleanup is best-effort; the next registry write retries recovery.
  }
}

function artifactChecksum(candidate: string): string {
  if (isSymbolicLink(candidate)) {
    throw new AgentFlowRunStateError(`Artifact checksum path cannot be a symbolic link: ${candidate}`, "AGENT_FLOW_ARTIFACT_PATH");
  }
  const hash = createHash("sha256");
  const buffer = Buffer.allocUnsafe(64 * 1024);
  const descriptor = fs.openSync(candidate, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
  try {
    let bytesRead: number;
    do {
      bytesRead = fs.readSync(descriptor, buffer, 0, buffer.length, null);
      if (bytesRead > 0) hash.update(buffer.subarray(0, bytesRead));
    } while (bytesRead > 0);
  } finally {
    fs.closeSync(descriptor);
  }
  return `sha256:${hash.digest("hex")}`;
}

function artifactTargetExists(candidate: string): boolean {
  let stat: fs.Stats;
  try {
    stat = fs.lstatSync(candidate);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
    throw error;
  }
  if (stat.isSymbolicLink()) {
    throw new AgentFlowRunStateError(`Artifact target cannot be a symbolic link: ${candidate}`, "AGENT_FLOW_ARTIFACT_PATH");
  }
  if (!stat.isFile()) {
    throw new AgentFlowRunStateError(`Artifact target is not a regular file: ${candidate}`, "AGENT_FLOW_ARTIFACT_PATH");
  }
  return true;
}

function recordPage(
  page: AgentFlowRecordPage | undefined,
  sortColumn: "path" | "sequence" | "id" | "created_at",
  tieColumn: "id" | null = "id"
): { where: string; sql: string; parameters: SqliteValue[] } {
  if (page === undefined) return { where: "", sql: "", parameters: [] };
  const offset = page.offset ?? 0;
  if (!Number.isSafeInteger(offset) || offset < 0
      || !Number.isSafeInteger(page.limit) || page.limit < 1 || page.limit > 1_000
      || (page.after !== undefined && page.offset !== undefined)) {
    throw new AgentFlowRunStateError(
      "Record pages require either a non-negative offset or an after cursor, plus a limit between 1 and 1000.",
      "AGENT_FLOW_RUN_STATE_PAGE"
    );
  }
  if (page.after === undefined) {
    return { where: "", sql: " LIMIT ? OFFSET ?", parameters: [page.limit, offset] };
  }
  const sortValue = page.after.sortValue;
  if ((typeof sortValue !== "string" && typeof sortValue !== "number")
      || (typeof sortValue === "number" && !Number.isSafeInteger(sortValue))) {
    throw new AgentFlowRunStateError("Record page cursors require a valid sort value.", "AGENT_FLOW_RUN_STATE_PAGE");
  }
  if (tieColumn === null) {
    return { where: ` AND ${sortColumn} > ?`, sql: " LIMIT ?", parameters: [sortValue, page.limit] };
  }
  const id = page.after.id;
  if (typeof id !== "string") {
    throw new AgentFlowRunStateError("Record page cursors require an ID tie-breaker.", "AGENT_FLOW_RUN_STATE_PAGE");
  }
  return {
    where: ` AND (${sortColumn} > ? OR (${sortColumn} = ? AND ${tieColumn} > ?))`,
    sql: " LIMIT ?",
    parameters: [sortValue, sortValue, id, page.limit]
  };
}

function requiredString(value: unknown, label: string): string {
  if (typeof value !== "string") {
    throw new AgentFlowRunStateError(`${label} must be a non-empty string.`, "AGENT_FLOW_RUN_STATE_INVALID");
  }
  const normalized = value.trim();
  if (normalized.length === 0) throw new AgentFlowRunStateError(`${label} must be a non-empty string.`, "AGENT_FLOW_RUN_STATE_INVALID");
  return normalized;
}

function optionalString(value: unknown, label: string): string | null {
  return value === undefined ? null : requiredString(value, label);
}

function assertOneOf<T extends string>(value: string, allowed: readonly T[], label: string): asserts value is T {
  if (!allowed.includes(value as T)) {
    throw new AgentFlowRunStateError(`Invalid ${label}: ${value}.`, "AGENT_FLOW_RUN_STATE_INVALID");
  }
}

function currentTimestamp(now: () => string): string {
  return validTimestamp(now());
}

function validTimestamp(value: string): string {
  const timestamp = requiredString(value, "Timestamp");
  if (!Number.isFinite(Date.parse(timestamp))) {
    throw new AgentFlowRunStateError(`Invalid timestamp: ${timestamp}.`, "AGENT_FLOW_TIMESTAMP_INVALID");
  }
  return new Date(timestamp).toISOString();
}

function validBusyTimeout(value: number): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new AgentFlowRunStateError("SQLite busy timeout must be a non-negative integer.", "AGENT_FLOW_SQLITE_OPTION");
  }
  return value;
}

function validRunLockTtl(value: number): number {
  if (!Number.isSafeInteger(value) || value < 3) {
    throw new AgentFlowRunStateError(
      "Run lock TTL must be an integer of at least 3 milliseconds.",
      "AGENT_FLOW_RUN_LOCK_OPTION"
    );
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

function isSqliteContentionError(error: unknown): boolean {
  return /database is (?:locked|busy)|SQLITE_(?:BUSY|LOCKED)/i.test(errorMessage(error));
}

function runStateWriteError(operation: string, error: unknown): AgentFlowRunStateError {
  if (isRunLockFenceError(error)) {
    return new AgentFlowRunStateError(
      `The Agent Flow execution lock was replaced while attempting to ${operation}. Stop this executor and retry the operation.`,
      "AGENT_FLOW_RUN_LOCK_LOST",
      { cause: error }
    );
  }
  if (isSqliteContentionError(error)) return concurrentMutationError(operation, error);
  return new AgentFlowRunStateError(`Could not ${operation}: ${errorMessage(error)}`, "AGENT_FLOW_RUN_STATE_WRITE", { cause: error });
}

function isRunLockFenceError(error: unknown): boolean {
  const visited = new Set<unknown>();
  let current = error;
  while (typeof current === "object" && current !== null && !visited.has(current)) {
    visited.add(current);
    if (current instanceof Error && current.message.includes("agent_flow_run_lock_lost")) return true;
    current = "cause" in current ? current.cause : undefined;
  }
  return false;
}

function runLockLostError(runId: string, cause?: unknown): AgentFlowRunStateError {
  return new AgentFlowRunStateError(
    `Agent Flow run ${runId} execution lock was replaced. Stop this executor and retry the operation.`,
    "AGENT_FLOW_RUN_LOCK_LOST",
    cause === undefined ? undefined : { cause }
  );
}

function concurrentMutationError(operation: string, error: unknown): AgentFlowRunStateError {
  return new AgentFlowRunStateError(
    `Could not ${operation} because another Agent Flow state mutation is in progress. Retry after the active mutation completes.`,
    "AGENT_FLOW_CONCURRENT_MUTATION",
    { cause: error }
  );
}

function runLockOwnerKey(databasePath: string, runId: string, ownerToken: string, acquisitionId: string): string {
  return `${databasePath}\0${runId}\0${ownerToken}\0${acquisitionId}`;
}

function runLockErrorCode(error: unknown): string | undefined {
  return error instanceof AgentFlowRunStateError ? error.code : undefined;
}

async function waitForRunLockRetry(delayMs: number): Promise<void> {
  await new Promise<void>((resolve) => setTimeout(resolve, delayMs));
}

function installAgentFlowRunLockFencing(database: SqliteDatabase): void {
  const tables = [
    ["runs", "id"],
    ["run_steps", "run_id"],
    ["artifacts", "run_id"],
    ["events", "run_id"],
    ["sessions", "run_id"],
    ["failures", "run_id"],
    ["approvals", "run_id"],
    ["budgets", "run_id"]
  ] as const;
  const triggers = tables.flatMap(([table, runIdColumn]) => ["INSERT", "UPDATE", "DELETE"].map((operation) => {
    const row = operation === "DELETE" ? "OLD" : "NEW";
    const runId = `${row}.${runIdColumn}`;
    return `
CREATE TEMP TRIGGER IF NOT EXISTS agent_flow_fence_${table}_${operation.toLowerCase()}
BEFORE ${operation} ON ${table}
WHEN EXISTS (
  SELECT 1 FROM agent_flow_held_run_locks held WHERE held.run_id = ${runId}
) AND NOT EXISTS (
  SELECT 1 FROM run_locks locks
  JOIN agent_flow_held_run_locks held
    ON held.run_id = locks.run_id
      AND held.owner_token = locks.owner_token
      AND held.acquisition_id = locks.acquisition_id
  WHERE locks.run_id = ${runId}
)
BEGIN
  SELECT RAISE(ABORT, 'agent_flow_run_lock_lost');
END;`;
  })).join("\n");
  database.exec(`
CREATE TEMP TABLE IF NOT EXISTS agent_flow_held_run_locks (
  run_id TEXT PRIMARY KEY,
  owner_token TEXT NOT NULL,
  acquisition_id TEXT NOT NULL
);
${triggers}
  `);
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
