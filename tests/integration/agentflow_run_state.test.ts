import { Database } from "bun:sqlite";
import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  AgentflowRunStateError,
  openAgentflowRunState
} from "../../packages/agentflow-core/src";

const FIXED_TIME = "2026-07-15T12:00:00.000Z";

describe("Agentflow run-state SQLite store", () => {
  test("creates and updates resumable runs in a repository-local database", async () => {
    const repoRoot = temporaryRepo();
    const store = await openAgentflowRunState({ cwd: path.join(repoRoot, "nested"), now: () => FIXED_TIME });

    const created = store.createRun({
      id: "run-platform-1",
      workflow: { name: "ship-release", version: 3, style: "pipeline", maturity: "trusted" },
      inputs: { environment: "staging", nested: { b: 2, a: 1 } }
    });

    expect(store.databasePath).toBe(path.join(repoRoot, ".agentflow/agentflow.sqlite"));
    expect(created).toMatchObject({
      id: "run-platform-1",
      status: "pending",
      workflowName: "ship-release",
      workflowVersion: 3,
      currentStepId: null,
      createdAt: FIXED_TIME,
      updatedAt: FIXED_TIME
    });

    const updated = store.updateRun("run-platform-1", { status: "running", currentStepId: "build" });
    expect(updated).toMatchObject({ status: "running", currentStepId: "build", startedAt: FIXED_TIME, finishedAt: null });
    expect(store.findResumableRun({ workflowName: "ship-release" })?.id).toBe("run-platform-1");

    store.close();
    expect(fs.existsSync(path.join(repoRoot, ".agent-memory/memory.sqlite"))).toBe(false);
  });

  test("normalizes timestamps before selecting the most recently updated resumable run", async () => {
    const repoRoot = temporaryRepo();
    let now = "2026-07-15T13:00:00+01:00";
    const store = await openAgentflowRunState({ cwd: repoRoot, now: () => now });
    const workflow = { name: "ordered", version: 1, style: "pipeline", maturity: "stable" } as const;

    expect(store.createRun({ id: "older", workflow }).createdAt).toBe("2026-07-15T12:00:00.000Z");
    now = "2026-07-15T12:30:00Z";
    store.createRun({ id: "newer", workflow });

    expect(store.findResumableRun({ workflowName: "ordered" })?.id).toBe("newer");
    store.close();
  });

  test("stores pipeline, recovery, and collaboration state families", async () => {
    const repoRoot = temporaryRepo();
    let now = FIXED_TIME;
    const store = await openAgentflowRunState({ cwd: repoRoot, now: () => now });

    store.createRun({
      id: "run-parent",
      workflow: { name: "review", version: 1, style: "collaborative", maturity: "stable" }
    });
    store.createRun({
      id: "run-recovery",
      workflow: { name: "recover", version: 2, style: "recovery_pipeline", maturity: "experimental" },
      parentRunId: "run-parent",
      recoveryOfRunId: "run-parent"
    });
    store.upsertStep({
      runId: "run-recovery",
      stepId: "diagnose",
      attempt: 1,
      status: "running",
      parentStepId: "prepare",
      sessionId: "reviewer",
      input: { target: "spec" }
    });
    store.upsertArtifact({
      id: "failure-report",
      runId: "run-recovery",
      stepId: "diagnose",
      path: "failures/report.json",
      kind: "failure",
      contentType: "application/json",
      checksum: "sha256:fixture",
      metadata: { private: true }
    });
    store.appendEvent({
      id: "event-1",
      runId: "run-recovery",
      sequence: 1,
      stepId: "diagnose",
      type: "step.started",
      payload: { attempt: 1 }
    });
    store.upsertSession({
      id: "reviewer",
      runId: "run-recovery",
      provider: "codex",
      status: "running",
      externalSessionId: "session-external-1",
      state: { role: "reviewer" }
    });
    store.recordFailure({
      id: "failure-1",
      runId: "run-recovery",
      stepId: "diagnose",
      classification: "test_failure",
      message: "Focused test failed",
      retryable: true,
      payload: { exitCode: 1 }
    });
    store.upsertApproval({
      id: "approval-1",
      runId: "run-recovery",
      stepId: "diagnose",
      status: "requested",
      requestedBy: "reviewer",
      context: { reason: "publish" }
    });
    store.upsertBudget({
      id: "budget-1",
      runId: "run-recovery",
      scope: "run",
      kind: "tokens",
      limit: 10_000,
      used: 2_500,
      unit: "tokens"
    });

    store.upsertStep({ runId: "run-recovery", stepId: "diagnose", attempt: 1, status: "completed", output: { result: "fixed" } });
    store.upsertSession({ id: "reviewer", runId: "run-recovery", provider: "codex", status: "completed" });
    store.upsertApproval({
      id: "approval-1",
      runId: "run-recovery",
      status: "approved",
      decidedBy: "maintainer",
      decision: "ship"
    });
    now = "2026-07-15T12:05:00.000Z";
    store.resolveFailure("run-recovery", "failure-1");
    store.resolveFailure("run-recovery", "failure-1", "2026-07-15T12:10:00.000Z");
    store.upsertStep({ runId: "run-recovery", stepId: "diagnose", attempt: 1, status: "running", sessionId: "writer" });
    store.upsertSession({ id: "reviewer", runId: "run-recovery", provider: "other", status: "running", state: { role: "writer" } });
    store.upsertApproval({ id: "approval-1", runId: "run-recovery", status: "requested", requestedBy: "other" });
    store.upsertBudget({
      id: "budget-1",
      runId: "run-recovery",
      scope: "run",
      kind: "tokens",
      limit: 10_000,
      used: 3_000,
      unit: "tokens"
    });

    const database = new Database(store.databasePath, { readonly: true });
    for (const table of ["runs", "run_steps", "artifacts", "events", "sessions", "failures", "approvals", "budgets"]) {
      expect(database.query(`SELECT COUNT(*) AS count FROM ${table}`).get()).toEqual({ count: table === "runs" ? 2 : 1 });
    }
    expect(database.query("SELECT parent_step_id, status, session_id, input_json, output_json, finished_at FROM run_steps WHERE run_id = ? AND step_id = ?").get("run-recovery", "diagnose")).toEqual({
      parent_step_id: "prepare",
      status: "completed",
      session_id: "reviewer",
      input_json: '{"target":"spec"}',
      output_json: '{"result":"fixed"}',
      finished_at: FIXED_TIME
    });
    expect(database.query("SELECT status, provider, state_json, finished_at FROM sessions WHERE run_id = ? AND id = ?").get("run-recovery", "reviewer")).toEqual({
      status: "completed",
      provider: "codex",
      state_json: '{"role":"reviewer"}',
      finished_at: FIXED_TIME
    });
    expect(database.query("SELECT step_id, status, requested_by, decided_by, decision, context_json, decided_at FROM approvals WHERE run_id = ? AND id = ?").get("run-recovery", "approval-1")).toEqual({
      step_id: "diagnose",
      status: "approved",
      requested_by: "reviewer",
      decided_by: "maintainer",
      decision: "ship",
      context_json: '{"reason":"publish"}',
      decided_at: FIXED_TIME
    });
    expect(database.query("SELECT resolved_at FROM failures WHERE run_id = ? AND id = ?").get("run-recovery", "failure-1"))
      .toEqual({ resolved_at: now });
    expect(database.query("SELECT used FROM budgets WHERE run_id = ? AND id = ?").get("run-recovery", "budget-1")).toEqual({ used: 3000 });
    database.close();
    store.close();
  });

  test("excludes terminal runs from resume lookup and prevents terminal reopening", async () => {
    const repoRoot = temporaryRepo();
    const store = await openAgentflowRunState({ cwd: repoRoot, now: () => FIXED_TIME });

    for (const status of ["completed", "failed", "cancelled"] as const) {
      const id = `run-${status}`;
      store.createRun({ id, workflow: { name: "terminal", version: 1, style: "pipeline", maturity: "stable" } });
      const terminal = store.updateRun(id, { status, output: { result: status } });
      expect(terminal.finishedAt).toBe(FIXED_TIME);
      expect(store.findResumableRun({ workflowName: "terminal" })).toBeNull();
      expect(() => store.updateRun(id, { status: "running" })).toThrow(AgentflowRunStateError);
      expect(store.updateRun(id, { status, currentStepId: "later", output: { result: "changed" } })).toEqual(terminal);
    }

    store.close();
  });

  test("rejects collisions, invalid artifacts, and database paths outside the repository", async () => {
    const repoRoot = temporaryRepo();
    const store = await openAgentflowRunState({ cwd: repoRoot, now: () => FIXED_TIME });
    const run = { id: "run-1", workflow: { name: "safe", version: 1, style: "pipeline", maturity: "stable" } } as const;
    store.createRun(run);

    expect(() => store.createRun(run)).toThrow(/already exists/);
    expect(() => store.createRun({
      id: "invalid-style",
      workflow: { name: "safe", version: 1, style: "invalid" as never, maturity: "stable" }
    })).toThrow(/Invalid workflow style/);
    expect(() => store.createRun({
      id: "invalid-maturity",
      workflow: { name: "safe", version: 1, style: "pipeline", maturity: "invalid" as never }
    })).toThrow(/Invalid workflow maturity/);
    expect(() => store.createRun({
      id: null as never,
      workflow: { name: "safe", version: 1, style: "pipeline", maturity: "stable" }
    })).toThrow(AgentflowRunStateError);
    let invalidJsonError: unknown;
    try {
      store.createRun({
        id: "invalid-json",
        workflow: { name: "safe", version: 1, style: "pipeline", maturity: "stable" },
        inputs: { missing: undefined as never }
      });
    } catch (error) {
      invalidJsonError = error;
    }
    expect(invalidJsonError).toBeInstanceOf(AgentflowRunStateError);
    expect(invalidJsonError).toMatchObject({ code: "AGENTFLOW_JSON_INVALID" });
    expect(() => store.createRun({
      id: "invalid-object",
      workflow: { name: "safe", version: 1, style: "pipeline", maturity: "stable" },
      inputs: { createdAt: new Date() as never }
    })).toThrow(/plain objects/);
    expect(() => store.upsertApproval({
      id: "invalid-approval",
      runId: "run-1",
      status: "requested",
      decidedAt: FIXED_TIME
    })).toThrow(/cannot include decision metadata/);
    expect(() => store.upsertArtifact({
      id: "bad",
      runId: "run-1",
      path: "../outside.txt",
      kind: "output",
      contentType: "text/plain"
    })).toThrow(/repo-relative/);
    store.close();

    await expect(openAgentflowRunState({ cwd: repoRoot, databasePath: path.join(os.tmpdir(), "outside-agentflow.sqlite") }))
      .rejects.toThrow(/inside the repository/);
  });

  test("rejects database symlinks and unsupported schemas without mutating them", async () => {
    const repoRoot = temporaryRepo();
    const databaseDirectory = path.join(repoRoot, ".agentflow");
    const outsideDirectory = fs.mkdtempSync(path.join(os.tmpdir(), "agentflow-run-state-outside-"));
    const outsideDatabase = path.join(outsideDirectory, "escaped.sqlite");
    fs.mkdirSync(databaseDirectory);
    fs.symlinkSync(outsideDatabase, path.join(databaseDirectory, "agentflow.sqlite"));

    await expect(openAgentflowRunState({ cwd: repoRoot })).rejects.toThrow(/cannot be a symbolic link/);
    expect(fs.existsSync(outsideDatabase)).toBe(false);

    fs.unlinkSync(path.join(databaseDirectory, "agentflow.sqlite"));
    const unsupportedPath = path.join(databaseDirectory, "unsupported.sqlite");
    const unsupported = new Database(unsupportedPath);
    unsupported.exec("CREATE TABLE run_state_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)");
    unsupported.query("INSERT INTO run_state_metadata (key, value) VALUES ('schema_version', '999')").run();
    unsupported.close();

    await expect(openAgentflowRunState({ cwd: repoRoot, databasePath: unsupportedPath }))
      .rejects.toThrow(/schema version 999/);
    const inspected = new Database(unsupportedPath, { readonly: true });
    expect(inspected.query("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name").all())
      .toEqual([{ name: "run_state_metadata" }]);
    inspected.close();
  });

  test("rejects invalid SQLite options before creating generated state", async () => {
    const repoRoot = temporaryRepo();

    await expect(openAgentflowRunState({ cwd: repoRoot, busyTimeoutMs: -1 }))
      .rejects.toThrow(/non-negative integer/);
    expect(fs.existsSync(path.join(repoRoot, ".agentflow"))).toBe(false);
  });

  test("finds the owning repository from a symlinked working directory", async () => {
    const repoRoot = temporaryRepo();
    const linkedCwd = path.join(os.tmpdir(), `agentflow-run-state-link-${path.basename(repoRoot)}`);
    fs.symlinkSync(path.join(repoRoot, "nested"), linkedCwd);

    const store = await openAgentflowRunState({ cwd: linkedCwd });
    expect(store.repoRoot).toBe(repoRoot);
    expect(store.databasePath).toBe(path.join(repoRoot, ".agentflow/agentflow.sqlite"));
    store.close();
    fs.unlinkSync(linkedCwd);
  });
});

function temporaryRepo(): string {
  const repoRoot = fs.mkdtempSync(path.join(os.tmpdir(), "agentflow-run-state-"));
  fs.mkdirSync(path.join(repoRoot, ".git"));
  fs.mkdirSync(path.join(repoRoot, "nested"));
  return repoRoot;
}
