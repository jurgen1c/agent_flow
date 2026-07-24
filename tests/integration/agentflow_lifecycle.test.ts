import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  AgentflowRunStateError,
  createAgentflowLifecycleRun,
  openAgentflowRunState,
  parseAgentflowWorkflowOrThrow,
  transitionAgentflowLifecycleRun
} from "../../packages/agentflow-core/src";

const WORKFLOW_SOURCE = `
name: lifecycle-shell
version: 1
style: pipeline
maturity: experimental
steps:
  - id: build
    type: command
    command: bun run build
`;

describe("Agentflow run lifecycle", () => {
  test("persists idempotent lifecycle transitions and events across process restart", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(WORKFLOW_SOURCE);
    let now = "2026-07-15T20:00:00.000Z";
    let store = await openAgentflowRunState({ cwd: repoRoot, now: () => now });

    expect(createAgentflowLifecycleRun(store, { id: "run-shell", workflow })).toMatchObject({
      changed: true,
      run: { id: "run-shell", status: "pending" }
    });
    expect(createAgentflowLifecycleRun(store, { id: "run-shell", workflow }).changed).toBe(false);
    now = "2026-07-15T20:01:00.000Z";
    expect(transitionAgentflowLifecycleRun(store, "run-shell", "pause")).toMatchObject({
      changed: true,
      run: { status: "paused" }
    });
    expect(transitionAgentflowLifecycleRun(store, "run-shell", "pause").changed).toBe(false);
    store.close();

    now = "2026-07-15T20:02:00.000Z";
    store = await openAgentflowRunState({ cwd: repoRoot, now: () => now });
    expect(transitionAgentflowLifecycleRun(store, "run-shell", "resume")).toMatchObject({
      changed: true,
      run: { status: "running" }
    });
    now = "2026-07-15T20:03:00.000Z";
    expect(transitionAgentflowLifecycleRun(store, "run-shell", "cancel")).toMatchObject({
      changed: true,
      run: { status: "cancelled" }
    });
    expect(transitionAgentflowLifecycleRun(store, "run-shell", "cancel").changed).toBe(false);
    expect(store.listEvents("run-shell").map((event) => [event.sequence, event.type, event.payload])).toEqual([
      [1, "run.created", { status: "pending" }],
      [2, "run.pause", { status: "paused" }],
      [3, "pipeline.effects.finalized", { status: "paused", transitionSequence: 2 }],
      [4, "lifecycle.pause.finalized", { status: "paused" }],
      [5, "run.resume", { status: "running" }],
      [6, "run.cancel", { status: "cancelled" }],
      [7, "pipeline.effects.finalized", { status: "cancelled", transitionSequence: 6 }],
      [8, "lifecycle.cancel.finalized", { status: "cancelled" }]
    ]);
    store.close();
  });

  test("rejects run ID collisions, missing runs, and invalid terminal transitions", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(WORKFLOW_SOURCE);
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "run-shell", workflow });

    expect(() => createAgentflowLifecycleRun(store, {
      id: "run-shell",
      workflow: { ...workflow, name: "another-workflow" }
    })).toThrow(AgentflowRunStateError);
    expect(() => createAgentflowLifecycleRun(store, {
      id: "run-shell",
      workflow: {
        ...workflow,
        steps: workflow.steps.map((step) => ({ ...step, command: "bun test" }))
      }
    })).toThrow("already exists");
    expect(() => transitionAgentflowLifecycleRun(store, "missing", "pause")).toThrow("was not found");
    transitionAgentflowLifecycleRun(store, "run-shell", "cancel");
    expect(() => transitionAgentflowLifecycleRun(store, "run-shell", "resume")).toThrow("cannot resume");
    expect(() => createAgentflowLifecycleRun(store, { id: "run-shell", workflow })).toThrow("cannot be reopened");
    store.close();
  });

  test("rolls back lifecycle status when its ordered event cannot be written", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(WORKFLOW_SOURCE);
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "run-atomic", workflow });

    expect(() => store.transitionRunWithEvent("run-atomic", {
      status: "paused",
      allowedFrom: ["pending"],
      event: { type: "   " }
    })).toThrow("Event type must be a non-empty string");
    expect(store.getRun("run-atomic")?.status).toBe("pending");
    expect(store.listEvents("run-atomic").map((event) => event.type)).toEqual(["run.created"]);
    store.close();
  });

  test("wraps low-level event writes and rolls back transactional run creation", async () => {
    const repoRoot = temporaryRepo();
    const store = await openAgentflowRunState({ cwd: repoRoot });
    const database = (store as unknown as {
      database: { run: (sql: string, params?: unknown[]) => void };
    }).database;
    const originalRun = database.run;
    database.run = (sql, params) => {
      if (sql.includes("INSERT INTO events")) throw new Error("simulated SQLite write failure");
      originalRun(sql, params);
    };

    expect(() => store.createRunWithEvent({
      id: "run-create-atomic",
      workflow: { name: "atomic", version: 1, style: "pipeline", maturity: "experimental" }
    }, { type: "run.created" })).toThrow("Could not create run with event: simulated SQLite write failure");
    expect(store.getRun("run-create-atomic")).toBeNull();
    database.run = originalRun;
    store.close();
  });

  test("allocates lifecycle event IDs around caller-supplied collisions", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(WORKFLOW_SOURCE);
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "run-event-id", workflow });
    store.appendEvent({
      id: "lifecycle:3:run.pause",
      runId: "run-event-id",
      sequence: 2,
      type: "external.event"
    });

    expect(transitionAgentflowLifecycleRun(store, "run-event-id", "pause")).toMatchObject({
      changed: true,
      run: { status: "paused" }
    });
    expect(store.listEvents("run-event-id").map((event) => [event.sequence, event.id])).toEqual([
      [1, "lifecycle:1:run.created"],
      [2, "lifecycle:3:run.pause"],
      [3, "lifecycle:3:run.pause:1"],
      [4, "lifecycle:4:pipeline.effects.finalized"],
      [5, "lifecycle:5:lifecycle.pause.finalized"]
    ]);
    store.close();
  });
});

function temporaryRepo(): string {
  const repoRoot = fs.mkdtempSync(path.join(os.tmpdir(), "agentflow-lifecycle-"));
  fs.mkdirSync(path.join(repoRoot, ".git"));
  return repoRoot;
}
