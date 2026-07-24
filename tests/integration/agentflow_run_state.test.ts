import { Database } from "bun:sqlite";
import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
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

    expect(store.listFailures("run-recovery")).toEqual([{
      id: "failure-1",
      runId: "run-recovery",
      stepId: "diagnose",
      sessionId: null,
      classification: "test_failure",
      message: "Focused test failed",
      retryable: true,
      attempt: null,
      outcome: null,
      payload: { exitCode: 1 },
      createdAt: FIXED_TIME,
      resolvedAt: now
    }]);

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

  test("validates atomic budget reservations and returns persisted record shapes", async () => {
    const repoRoot = temporaryRepo();
    const store = await openAgentflowRunState({ cwd: repoRoot, now: () => FIXED_TIME });
    store.createRun({ id: "budget-duplicates", workflow: { name: "budget", version: 1, style: "pipeline", maturity: "stable" } });
    const reservation = { id: "model:model_calls", runId: "budget-duplicates", scope: "workflow", kind: "model_calls", limit: 2, amount: 1, unit: "calls" };

    expect(() => store.reserveBudgets([reservation, reservation])).toThrow(/duplicate IDs/);
    expect(store.getBudget("budget-duplicates", reservation.id)).toBeNull();
    expect(() => store.reserveBudgets([{ ...reservation, id: "model:zero", amount: 0 }])).toThrow(/positive finite number/);
    expect(store.getBudget("budget-duplicates", "model:zero")).toBeNull();
    expect(store.reserveBudgets([reservation])).toEqual([{
      id: "model:model_calls",
      runId: "budget-duplicates",
      scope: "workflow",
      kind: "model_calls",
      limit: 2,
      used: 1,
      unit: "calls"
    }]);
    store.close();
  });

  test("lists ordered events and run-scoped artifacts after process restart", async () => {
    const repoRoot = temporaryRepo();
    const workflow = { name: "durable", version: 1, style: "pipeline", maturity: "stable" } as const;
    let store = await openAgentflowRunState({ cwd: repoRoot, now: () => FIXED_TIME });
    store.createRun({ id: "run-durable", workflow });
    store.appendEvent({ id: "event-2", runId: "run-durable", sequence: 2, type: "step.completed", payload: { step: "build" } });
    store.appendEvent({ id: "event-1", runId: "run-durable", sequence: 1, stepId: "build", type: "step.started" });
    const written = store.writeArtifact({
      id: "build-log",
      runId: "run-durable",
      stepId: "build",
      path: "logs/build.txt",
      kind: "log",
      contentType: "text/plain",
      content: "build passed\n",
      metadata: { retained: true }
    });
    expect(written).toMatchObject({
      producerStepId: "build",
      declaredPath: "logs/build.txt",
      storagePath: artifactStoragePath("run-durable", "logs/build.txt"),
      status: "available",
      sizeBytes: 13,
      metadata: { retained: true }
    });
    store.close();

    store = await openAgentflowRunState({ cwd: repoRoot, now: () => FIXED_TIME });
    expect(store.listEvents("run-durable")).toEqual([
      {
        id: "event-1",
        runId: "run-durable",
        sequence: 1,
        stepId: "build",
        sessionId: null,
        type: "step.started",
        payload: null,
        createdAt: FIXED_TIME
      },
      {
        id: "event-2",
        runId: "run-durable",
        sequence: 2,
        stepId: null,
        sessionId: null,
        type: "step.completed",
        payload: { step: "build" },
        createdAt: FIXED_TIME
      }
    ]);
    expect(store.listArtifacts("run-durable")[0]).toMatchObject({
      id: "build-log",
      status: "available",
      checksum: written.checksum,
      writtenAt: FIXED_TIME
    });
    expect(fs.readFileSync(path.join(repoRoot, written.storagePath), "utf8")).toBe("build passed\n");
    store.close();
  });

  test("reconciles missing and stale artifacts and protects explicit overwrites", async () => {
    const repoRoot = temporaryRepo();
    let now = FIXED_TIME;
    const store = await openAgentflowRunState({ cwd: repoRoot, now: () => now, busyTimeoutMs: 10 });
    store.createRun({
      id: "run-artifacts",
      workflow: { name: "artifacts", version: 1, style: "pipeline", maturity: "stable" }
    });
    store.upsertArtifact({
      id: "report",
      runId: "run-artifacts",
      stepId: "report",
      path: "reports/result.json",
      kind: "result",
      contentType: "application/json"
    });
    expect(store.listArtifacts("run-artifacts")[0]).toMatchObject({ status: "missing", checkedAt: FIXED_TIME });
    const unverifiedTarget = path.join(repoRoot, artifactStoragePath("run-artifacts", "reports/result.json"));
    fs.mkdirSync(path.dirname(unverifiedTarget), { recursive: true });
    fs.writeFileSync(unverifiedTarget, "published without metadata\n");
    expect(store.listArtifacts("run-artifacts")[0]?.status).toBe("stale");
    const replacedUnverified = store.writeArtifact({
      id: "report",
      runId: "run-artifacts",
      stepId: "report",
      path: "reports/result.json",
      kind: "result",
      contentType: "application/json",
      content: "{\"result\":1}\n",
      overwrite: true
    });
    expect(replacedUnverified).toMatchObject({
      status: "overwritten",
      previousChecksum: `sha256:${createHash("sha256").update("published without metadata\n").digest("hex")}`
    });
    expect(store.listArtifacts("run-artifacts")[0]?.status).toBe("overwritten");
    fs.unlinkSync(unverifiedTarget);

    const first = store.writeArtifact({
      id: "report",
      runId: "run-artifacts",
      stepId: "report",
      path: "reports/result.json",
      kind: "result",
      contentType: "application/json",
      content: "{\"result\":1}\n"
    });
    const target = path.join(repoRoot, first.storagePath);
    expect(first.status).toBe("overwritten");
    now = "2026-07-15T12:01:00.000Z";
    const identicalRetry = store.writeArtifact({
      id: "report",
      runId: "run-artifacts",
      path: "reports/result.json",
      kind: "result",
      contentType: "application/json",
      content: "{\"result\":1}\n"
    });
    expect(identicalRetry).toMatchObject({
      producerStepId: "report",
      writtenAt: first.writtenAt
    });
    const originalOpenSync = fs.openSync;
    const openSyncDescriptor = Object.getOwnPropertyDescriptor(fs, "openSync")!;
    Object.defineProperty(fs, "openSync", {
      ...openSyncDescriptor,
      value: (...args: unknown[]) => {
        if (path.resolve(String(args[0])) === target) {
          throw Object.assign(new Error("artifact disappeared during inspection"), { code: "ENOENT" });
        }
        return Reflect.apply(originalOpenSync, fs, args);
      }
    });
    try {
      expect(store.listArtifacts("run-artifacts")[0]?.status).toBe("missing");
    } finally {
      Object.defineProperty(fs, "openSync", openSyncDescriptor);
    }
    expect(store.listArtifacts("run-artifacts")[0]?.status).toBe("overwritten");
    const artifactDirectory = path.dirname(target);
    fs.rmSync(artifactDirectory, { recursive: true });
    fs.writeFileSync(artifactDirectory, "corrupted artifact directory");
    expect(store.listArtifacts("run-artifacts")[0]?.status).toBe("missing");
    fs.unlinkSync(artifactDirectory);
    fs.mkdirSync(artifactDirectory);
    fs.writeFileSync(target, "{\"result\":1}\n");
    expect(store.listArtifacts("run-artifacts")[0]?.status).toBe("overwritten");
    fs.writeFileSync(target, "short");
    Object.defineProperty(fs, "openSync", {
      ...openSyncDescriptor,
      value: (...args: unknown[]) => {
        if (path.resolve(String(args[0])) === target) throw new Error("size mismatch should not be hashed");
        return Reflect.apply(originalOpenSync, fs, args);
      }
    });
    try {
      expect(store.listArtifacts("run-artifacts")[0]?.status).toBe("stale");
    } finally {
      Object.defineProperty(fs, "openSync", openSyncDescriptor);
    }
    fs.writeFileSync(target, "{\"result\":1}\n");
    expect(store.listArtifacts("run-artifacts")[0]?.status).toBe("overwritten");
    store.upsertArtifact({
      id: "report",
      runId: "run-artifacts",
      path: "reports/result.json",
      kind: "result",
      contentType: "application/json",
      checksum: "sha256:untrusted-metadata-replacement",
      sizeBytes: 999,
      metadata: { reviewed: true }
    });
    expect(store.listArtifacts("run-artifacts")[0]).toMatchObject({
      producerStepId: "report",
      checksum: first.checksum,
      sizeBytes: first.sizeBytes,
      status: "overwritten",
      metadata: { reviewed: true }
    });
    expect(() => store.upsertArtifact({
      id: "report",
      runId: "run-artifacts",
      path: "reports/moved.json",
      kind: "result",
      contentType: "application/json"
    })).toThrow(/cannot be reassigned/);
    let pathCollisionError: unknown;
    try {
      store.upsertArtifact({
        id: "duplicate-report",
        runId: "run-artifacts",
        path: "reports/result.json",
        kind: "result",
        contentType: "application/json"
      });
    } catch (error) {
      pathCollisionError = error;
    }
    expect(pathCollisionError).toMatchObject({
      code: "AGENTFLOW_ARTIFACT_COLLISION",
      message: expect.stringContaining("already registered as report")
    });
    const collisionStagingDirectory = path.join(
      repoRoot,
      ".agentflow/runs",
      artifactRunDirectory("run-artifacts"),
      ".staging"
    );
    fs.mkdirSync(collisionStagingDirectory, { recursive: true });
    const collisionBackup = path.join(
      collisionStagingDirectory,
      `${createHash("sha256").update("reports/result.json").digest("hex")}.old`
    );
    fs.writeFileSync(collisionBackup, "must remain untouched");
    expect(() => store.writeArtifact({
      id: "duplicate-report",
      runId: "run-artifacts",
      path: "reports/result.json",
      kind: "result",
      contentType: "application/json",
      content: "collision"
    })).toThrow(/already registered as report/);
    expect(fs.readFileSync(collisionBackup, "utf8")).toBe("must remain untouched");
    fs.unlinkSync(collisionBackup);
    expect(() => store.writeArtifact({
      id: "report",
      runId: "run-artifacts",
      path: "reports/result.json",
      kind: "result",
      contentType: "application/json",
      content: "{\"result\":2}\n"
    })).toThrow(/overwrite: true/);
    expect(fs.readFileSync(target, "utf8")).toBe("{\"result\":1}\n");

    now = "2026-07-15T12:05:00.000Z";
    fs.writeFileSync(target, "externally changed\n");
    expect(store.listArtifacts("run-artifacts")[0]).toMatchObject({ status: "stale", checkedAt: now });

    now = "2026-07-15T12:10:00.000Z";
    const overwritten = store.writeArtifact({
      id: "report",
      runId: "run-artifacts",
      stepId: "report",
      path: "reports/result.json",
      kind: "result",
      contentType: "application/json",
      content: "{\"result\":2}\n",
      overwrite: true
    });
    expect(overwritten).toMatchObject({
      status: "overwritten",
      previousChecksum: first.checksum,
      writtenAt: now
    });
    expect(store.listArtifacts("run-artifacts")[0]?.status).toBe("overwritten");

    now = "2026-07-15T12:15:00.000Z";
    fs.unlinkSync(target);
    expect(store.listArtifacts("run-artifacts")[0]).toMatchObject({ status: "missing", checkedAt: now });
    fs.writeFileSync(target, "{\"result\":2}\n");
    expect(store.listArtifacts("run-artifacts")[0]?.status).toBe("overwritten");
    fs.unlinkSync(target);
    expect(() => store.writeArtifact({
      id: "report",
      runId: "run-artifacts",
      path: "reports/result.json",
      kind: "result",
      contentType: "application/json",
      content: "{\"result\":3}\n"
    })).toThrow(/overwrite: true/);
    expect(fs.existsSync(target)).toBe(false);
    const replacedMissing = store.writeArtifact({
      id: "report",
      runId: "run-artifacts",
      path: "reports/result.json",
      kind: "result",
      contentType: "application/json",
      content: "{\"result\":3}\n",
      overwrite: true
    });
    expect(replacedMissing).toMatchObject({ status: "overwritten", previousChecksum: overwritten.checksum });
    fs.unlinkSync(target);
    const replacedArtifactTarget = path.join(repoRoot, "agentflow-replaced-artifact");
    fs.writeFileSync(replacedArtifactTarget, "{\"result\":3}\n");
    fs.symlinkSync(replacedArtifactTarget, target);
    const originalLstatSync = fs.lstatSync;
    const lstatSyncDescriptor = Object.getOwnPropertyDescriptor(fs, "lstatSync")!;
    let targetChecks = 0;
    Object.defineProperty(fs, "lstatSync", {
      ...lstatSyncDescriptor,
      value: (...args: unknown[]) => {
        if (path.resolve(String(args[0])) === target && targetChecks++ === 0) {
          return { isSymbolicLink: () => false };
        }
        return Reflect.apply(originalLstatSync, fs, args);
      }
    });
    try {
      expect(store.listArtifacts("run-artifacts")[0]?.status).toBe("stale");
    } finally {
      Object.defineProperty(fs, "lstatSync", lstatSyncDescriptor);
    }
    const artifactRoot = path.dirname(target);
    const realArtifactRoot = `${artifactRoot}-real`;
    fs.renameSync(artifactRoot, realArtifactRoot);
    fs.symlinkSync(realArtifactRoot, artifactRoot);
    expect(store.listArtifacts("run-artifacts")[0]?.status).toBe("stale");
    fs.unlinkSync(artifactRoot);
    fs.renameSync(realArtifactRoot, artifactRoot);
    const locker = new Database(store.databasePath);
    locker.exec("BEGIN IMMEDIATE");
    expect(store.listArtifacts("run-artifacts")[0]?.status).toBe("stale");
    locker.exec("ROLLBACK");
    locker.close();
    store.close();
  });

  test("keeps artifact writes inside the run directory and rejects symlink escapes", async () => {
    const repoRoot = temporaryRepo();
    const outside = fs.mkdtempSync(path.join(os.tmpdir(), "agentflow-artifacts-outside-"));
    const store = await openAgentflowRunState({ cwd: repoRoot, now: () => FIXED_TIME });
    store.createRun({
      id: "run-safe",
      workflow: { name: "safe", version: 1, style: "pipeline", maturity: "stable" }
    });

    expect(() => store.writeArtifact({
      id: "escape",
      runId: "run-safe",
      path: "../escape.txt",
      kind: "output",
      contentType: "text/plain",
      content: "no"
    })).toThrow(/cannot escape/);
    const artifactRoot = path.join(repoRoot, ".agentflow/runs", artifactRunDirectory("run-safe"), "artifacts");
    fs.mkdirSync(artifactRoot, { recursive: true });
    fs.symlinkSync(path.join(outside, "escape.txt"), path.join(artifactRoot, artifactFileName("linked/escape.txt")));
    expect(() => store.writeArtifact({
      id: "linked",
      runId: "run-safe",
      path: "linked/escape.txt",
      kind: "output",
      contentType: "text/plain",
      content: "no"
    })).toThrow(/symbolic link/);
    expect(fs.existsSync(path.join(outside, "escape.txt"))).toBe(false);

    expect(() => store.writeArtifactsAtomically([{
      id: "linked-batch",
      runId: "run-safe",
      path: "linked/escape.txt",
      kind: "output",
      contentType: "text/plain",
      content: "no"
    }])).toThrow(/symbolic link/);

    store.createRun({
      id: "team/run\\safe",
      workflow: { name: "legacy-id", version: 1, style: "pipeline", maturity: "stable" }
    });
    const encoded = store.writeArtifact({
      id: "legacy-id",
      runId: "team/run\\safe",
      path: "result.txt",
      kind: "output",
      contentType: "text/plain",
      content: "safe"
    });
    expect(encoded.storagePath).toBe(artifactStoragePath("team/run\\safe", "result.txt"));
    expect(store.listArtifacts("team/run\\safe")[0]?.status).toBe("available");

    for (const runId of ["Run", "run"]) {
      store.createRun({
        id: runId,
        workflow: { name: "case-safe-id", version: 1, style: "pipeline", maturity: "stable" }
      });
    }
    const upperCaseRun = store.writeArtifact({
      id: "case",
      runId: "Run",
      path: "result.txt",
      kind: "output",
      contentType: "text/plain",
      content: "upper"
    });
    const lowerCaseRun = store.writeArtifact({
      id: "case",
      runId: "run",
      path: "result.txt",
      kind: "output",
      contentType: "text/plain",
      content: "lower"
    });
    expect(upperCaseRun.storagePath.toLowerCase()).not.toBe(lowerCaseRun.storagePath.toLowerCase());
    expect(fs.readFileSync(path.join(repoRoot, upperCaseRun.storagePath), "utf8")).toBe("upper");
    expect(fs.readFileSync(path.join(repoRoot, lowerCaseRun.storagePath), "utf8")).toBe("lower");
    expect(() => store.writeArtifact({
      id: "trailing",
      runId: "run-safe",
      path: "foo/",
      kind: "output",
      contentType: "text/plain",
      content: "no"
    })).toThrow(/cannot end with a separator/);
    const longRunId = "long".repeat(100);
    store.createRun({
      id: longRunId,
      workflow: { name: "long-id", version: 1, style: "pipeline", maturity: "stable" }
    });
    expect(store.writeArtifact({
      id: "long",
      runId: longRunId,
      path: "result.txt",
      kind: "output",
      contentType: "text/plain",
      content: "bounded"
    }).status).toBe("available");

    const suffix = store.writeArtifact({
      id: "suffix",
      runId: "run-safe",
      path: "foo.agentflow-new",
      kind: "output",
      contentType: "text/plain",
      content: "keep"
    });
    store.writeArtifact({
      id: "plain",
      runId: "run-safe",
      path: "foo",
      kind: "output",
      contentType: "text/plain",
      content: "plain"
    });
    expect(fs.readFileSync(path.join(repoRoot, suffix.storagePath), "utf8")).toBe("keep");
    expect(store.listArtifacts("run-safe").find((artifact) => artifact.id === "suffix")?.status).toBe("available");
    store.close();
  });

  test("recovers interrupted publications and finalizes matching orphan content", async () => {
    const repoRoot = temporaryRepo();
    const store = await openAgentflowRunState({ cwd: repoRoot, now: () => FIXED_TIME });
    store.createRun({
      id: "run-recovery",
      workflow: { name: "recovery", version: 1, style: "pipeline", maturity: "stable" }
    });
    const original = store.writeArtifact({
      id: "report",
      runId: "run-recovery",
      path: "report.txt",
      kind: "output",
      contentType: "text/plain",
      content: "original"
    });
    const target = path.join(repoRoot, original.storagePath);
    const stagingDirectory = path.join(repoRoot, ".agentflow/runs", artifactRunDirectory("run-recovery"), ".staging");
    fs.mkdirSync(stagingDirectory, { recursive: true });
    const backup = path.join(stagingDirectory, `${createHash("sha256").update("report.txt").digest("hex")}.old`);
    fs.renameSync(target, backup);
    fs.writeFileSync(target, "interrupted overwrite");

    const recovered = store.writeArtifact({
      id: "report",
      runId: "run-recovery",
      path: "report.txt",
      kind: "output",
      contentType: "text/plain",
      content: "replacement",
      overwrite: true
    });
    expect(recovered).toMatchObject({ status: "overwritten", previousChecksum: original.checksum });
    expect(fs.readFileSync(target, "utf8")).toBe("replacement");
    expect(fs.existsSync(backup)).toBe(false);

    fs.writeFileSync(backup, "original");
    fs.unlinkSync(target);
    const resumedAfterCleanupInterruption = store.writeArtifact({
      id: "report",
      runId: "run-recovery",
      path: "report.txt",
      kind: "output",
      contentType: "text/plain",
      content: "replacement"
    });
    expect(resumedAfterCleanupInterruption.status).toBe("overwritten");
    expect(fs.readFileSync(target, "utf8")).toBe("replacement");
    expect(fs.existsSync(backup)).toBe(false);

    const outsideBackup = path.join(repoRoot, "outside-backup.txt");
    fs.writeFileSync(outsideBackup, "replacement");
    fs.unlinkSync(target);
    fs.symlinkSync(outsideBackup, backup);
    expect(() => store.writeArtifact({
      id: "report",
      runId: "run-recovery",
      path: "report.txt",
      kind: "output",
      contentType: "text/plain",
      content: "replacement"
    })).toThrow(/backup cannot be a symbolic link/);
    expect(fs.existsSync(target)).toBe(false);
    expect(fs.readFileSync(outsideBackup, "utf8")).toBe("replacement");
    fs.unlinkSync(backup);
    store.writeArtifact({
      id: "report",
      runId: "run-recovery",
      path: "report.txt",
      kind: "output",
      contentType: "text/plain",
      content: "replacement"
    });

    fs.renameSync(target, backup);
    const outsideTarget = path.join(repoRoot, "outside-target.txt");
    fs.writeFileSync(outsideTarget, "outside must remain untouched");
    fs.symlinkSync(outsideTarget, target);
    const originalLstatSync = fs.lstatSync;
    const lstatSyncDescriptor = Object.getOwnPropertyDescriptor(fs, "lstatSync")!;
    let targetChecks = 0;
    Object.defineProperty(fs, "lstatSync", {
      ...lstatSyncDescriptor,
      value: (...args: unknown[]) => {
        if (path.resolve(String(args[0])) === target && targetChecks++ === 0) {
          return { isSymbolicLink: () => false };
        }
        return Reflect.apply(originalLstatSync, fs, args);
      }
    });
    let recoveredFromTargetSymlink;
    try {
      recoveredFromTargetSymlink = store.writeArtifact({
        id: "report",
        runId: "run-recovery",
        path: "report.txt",
        kind: "output",
        contentType: "text/plain",
        content: "replacement"
      });
    } finally {
      Object.defineProperty(fs, "lstatSync", lstatSyncDescriptor);
    }
    expect(recoveredFromTargetSymlink.status).toBe("overwritten");
    expect(fs.readFileSync(target, "utf8")).toBe("replacement");
    expect(fs.readFileSync(outsideTarget, "utf8")).toBe("outside must remain untouched");

    fs.renameSync(target, backup);
    fs.mkdirSync(target);
    fs.writeFileSync(path.join(target, "corrupted-entry"), "invalid");
    const recoveredFromTargetDirectory = store.writeArtifact({
      id: "report",
      runId: "run-recovery",
      path: "report.txt",
      kind: "output",
      contentType: "text/plain",
      content: "replacement"
    });
    expect(recoveredFromTargetDirectory.status).toBe("overwritten");
    expect(fs.readFileSync(target, "utf8")).toBe("replacement");

    fs.mkdirSync(backup);
    fs.writeFileSync(path.join(backup, "corrupted-entry"), "invalid");
    const recoveredFromDirectory = store.writeArtifact({
      id: "report",
      runId: "run-recovery",
      path: "report.txt",
      kind: "output",
      contentType: "text/plain",
      content: "replacement"
    });
    expect(recoveredFromDirectory.status).toBe("overwritten");
    expect(fs.existsSync(backup)).toBe(false);

    store.upsertArtifact({
      id: "pre-registered",
      runId: "run-recovery",
      path: "pre-registered.txt",
      kind: "output",
      contentType: "text/plain"
    });
    const preRegisteredTarget = path.join(repoRoot, artifactStoragePath("run-recovery", "pre-registered.txt"));
    fs.writeFileSync(preRegisteredTarget, "published before commit");
    const finalizedPreRegistered = store.writeArtifact({
      id: "pre-registered",
      runId: "run-recovery",
      path: "pre-registered.txt",
      kind: "output",
      contentType: "text/plain",
      content: "published before commit"
    });
    expect(finalizedPreRegistered).toMatchObject({ status: "available", previousChecksum: null, writtenAt: FIXED_TIME });

    const orphanTarget = path.join(repoRoot, artifactStoragePath("run-recovery", "orphan.txt"));
    fs.writeFileSync(orphanTarget, "published before commit");
    const finalized = store.writeArtifact({
      id: "orphan",
      runId: "run-recovery",
      path: "orphan.txt",
      kind: "output",
      contentType: "text/plain",
      content: "published before commit"
    });
    expect(finalized.status).toBe("available");
    expect(store.listArtifacts("run-recovery").map((artifact) => artifact.id)).toEqual(["orphan", "pre-registered", "report"]);
    store.close();
  });

  test("initializes the run-state schema safely across concurrent first opens", async () => {
    const repoRoot = temporaryRepo();
    const modulePath = path.resolve("packages/agentflow-core/src/run_state.ts");
    const script = `
      import { openAgentflowRunState } from ${JSON.stringify(modulePath)};
      const store = await openAgentflowRunState({ cwd: process.env.AF_ROOT });
      store.close();
    `;
    const children = Array.from({ length: 4 }, () =>
      Bun.spawn({ cmd: [process.execPath, "-e", script], env: { ...process.env, AF_ROOT: repoRoot } })
    );
    expect(await Promise.all(children.map((child) => child.exited))).toEqual([0, 0, 0, 0]);
  });

  test("serializes concurrent no-overwrite publication across processes", async () => {
    const repoRoot = temporaryRepo();
    const store = await openAgentflowRunState({ cwd: repoRoot, now: () => FIXED_TIME });
    store.createRun({
      id: "run-concurrent",
      workflow: { name: "concurrent", version: 1, style: "pipeline", maturity: "stable" }
    });
    store.createRun({
      id: "run-directories",
      workflow: { name: "concurrent", version: 1, style: "pipeline", maturity: "stable" }
    });
    store.close();

    const modulePath = path.resolve("packages/agentflow-core/src/run_state.ts");
    const script = `
      import { openAgentflowRunState } from ${JSON.stringify(modulePath)};
      const store = await openAgentflowRunState({ cwd: process.env.AF_ROOT });
      try {
        store.writeArtifact({
          id: process.env.AF_ID,
          runId: process.env.AF_RUN,
          path: process.env.AF_PATH,
          kind: "output",
          contentType: "text/plain",
          content: process.env.AF_CONTENT
        });
      } catch {
        process.exitCode = 2;
      } finally {
        store.close();
      }
    `;
    const children = [
      Bun.spawn({ cmd: [process.execPath, "-e", script], env: { ...process.env, AF_ROOT: repoRoot, AF_RUN: "run-concurrent", AF_PATH: "shared.txt", AF_ID: "alpha", AF_CONTENT: "alpha" } }),
      Bun.spawn({ cmd: [process.execPath, "-e", script], env: { ...process.env, AF_ROOT: repoRoot, AF_RUN: "run-concurrent", AF_PATH: "shared.txt", AF_ID: "beta", AF_CONTENT: "beta" } })
    ];
    const exitCodes = await Promise.all(children.map((child) => child.exited));
    expect(exitCodes.sort()).toEqual([0, 2]);

    const reopened = await openAgentflowRunState({ cwd: repoRoot });
    const artifacts = reopened.listArtifacts("run-concurrent");
    expect(artifacts).toHaveLength(1);
    expect(fs.readFileSync(path.join(repoRoot, artifacts[0]!.storagePath), "utf8")).toBe(artifacts[0]!.id);
    reopened.close();

    const directoryChildren = [
      Bun.spawn({ cmd: [process.execPath, "-e", script], env: { ...process.env, AF_ROOT: repoRoot, AF_RUN: "run-directories", AF_PATH: "alpha.txt", AF_ID: "alpha", AF_CONTENT: "alpha" } }),
      Bun.spawn({ cmd: [process.execPath, "-e", script], env: { ...process.env, AF_ROOT: repoRoot, AF_RUN: "run-directories", AF_PATH: "beta.txt", AF_ID: "beta", AF_CONTENT: "beta" } })
    ];
    expect(await Promise.all(directoryChildren.map((child) => child.exited))).toEqual([0, 0]);
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
      expect(() => store.transitionRunWithEvent(id, {
        status: "running",
        allowedFrom: [status],
        event: { type: "run.resume" }
      })).toThrow("cannot transition");
      expect(store.transitionRunWithEvent(id, {
        status,
        allowedFrom: [],
        event: { type: "run.noop" }
      })).toEqual({ changed: false, run: terminal });
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
    let missingRunError: unknown;
    try {
      store.upsertArtifact({
        id: "missing-run",
        runId: "missing-run",
        path: "result.txt",
        kind: "output",
        contentType: "text/plain"
      });
    } catch (error) {
      missingRunError = error;
    }
    expect(missingRunError).toMatchObject({ code: "AGENTFLOW_RUN_NOT_FOUND" });
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

  test("migrates version-one artifact metadata without discarding run state", async () => {
    const repoRoot = temporaryRepo();
    let store = await openAgentflowRunState({ cwd: repoRoot, now: () => FIXED_TIME });
    store.createRun({
      id: "run-version-one",
      workflow: { name: "migrate", version: 1, style: "pipeline", maturity: "stable" }
    });
    store.upsertArtifact({
      id: "legacy",
      runId: "run-version-one",
      path: "legacy.txt",
      kind: "output",
      contentType: "text/plain"
    });
    store.close();

    const legacy = new Database(path.join(repoRoot, ".agentflow/agentflow.sqlite"));
    for (const column of ["generation", "checked_at", "written_at", "previous_checksum", "status"]) {
      legacy.exec(`ALTER TABLE artifacts DROP COLUMN ${column}`);
    }
    legacy.query("UPDATE run_state_metadata SET value = '1' WHERE key = 'schema_version'").run();
    legacy.close();

    const modulePath = path.resolve("packages/agentflow-core/src/run_state.ts");
    const script = `
      import { openAgentflowRunState } from ${JSON.stringify(modulePath)};
      const store = await openAgentflowRunState({ cwd: process.env.AF_ROOT, busyTimeoutMs: 5000 });
      store.close();
    `;
    const children = [
      Bun.spawn({ cmd: [process.execPath, "-e", script], env: { ...process.env, AF_ROOT: repoRoot } }),
      Bun.spawn({ cmd: [process.execPath, "-e", script], env: { ...process.env, AF_ROOT: repoRoot } })
    ];
    expect(await Promise.all(children.map((child) => child.exited))).toEqual([0, 0]);

    store = await openAgentflowRunState({ cwd: repoRoot, now: () => FIXED_TIME });
    expect(store.getRun("run-version-one")?.workflowName).toBe("migrate");
    expect(store.listArtifacts("run-version-one")[0]).toMatchObject({
      id: "legacy",
      status: "missing",
      previousChecksum: null,
      writtenAt: null
    });
    const migrated = new Database(store.databasePath, { readonly: true });
    expect(migrated.query("SELECT value FROM run_state_metadata WHERE key = 'schema_version'").get()).toEqual({ value: "3" });
    migrated.close();
    store.close();

    const damaged = new Database(path.join(repoRoot, ".agentflow/agentflow.sqlite"));
    damaged.exec("DROP TABLE events");
    damaged.close();
    store = await openAgentflowRunState({ cwd: repoRoot, now: () => FIXED_TIME });
    store.appendEvent({ id: "repaired", runId: "run-version-one", sequence: 1, type: "schema.repaired" });
    expect(store.listEvents("run-version-one").map((event) => event.id)).toEqual(["repaired"]);
    store.close();

    const writer = new Database(path.join(repoRoot, ".agentflow/agentflow.sqlite"));
    writer.exec("BEGIN IMMEDIATE");
    store = await openAgentflowRunState({ cwd: repoRoot, busyTimeoutMs: 10 });
    expect(store.getRun("run-version-one")?.id).toBe("run-version-one");
    store.close();
    writer.exec("ROLLBACK");
    writer.close();
  });

  test("repairs a damaged version-two schema before migrating it", async () => {
    const repoRoot = temporaryRepo();
    let store = await openAgentflowRunState({ cwd: repoRoot });
    store.close();
    const databasePath = path.join(repoRoot, ".agentflow/agentflow.sqlite");
    const damaged = new Database(databasePath);
    damaged.exec("DROP TABLE artifacts");
    damaged.query("UPDATE run_state_metadata SET value = '2' WHERE key = 'schema_version'").run();
    damaged.close();

    store = await openAgentflowRunState({ cwd: repoRoot });

    expect(store.getRun("missing")).toBeNull();
    const repaired = new Database(databasePath, { readonly: true });
    expect(repaired.query("SELECT value FROM run_state_metadata WHERE key = 'schema_version'").get())
      .toEqual({ value: "3" });
    expect(repaired.query("SELECT name FROM pragma_table_info('artifacts') WHERE name = 'generation'").get())
      .toEqual({ name: "generation" });
    repaired.close();
    store.close();
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

function artifactRunDirectory(runId: string): string {
  return `r-${createHash("sha256").update(runId).digest("hex")}`;
}

function artifactFileName(declaredPath: string): string {
  return `a-${createHash("sha256").update(declaredPath).digest("hex")}`;
}

function artifactStoragePath(runId: string, declaredPath: string): string {
  return `.agentflow/runs/${artifactRunDirectory(runId)}/artifacts/${artifactFileName(declaredPath)}`;
}
