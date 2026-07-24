import { describe, expect, spyOn, test } from "bun:test";
import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  createAgentflowLifecycleRun,
  createAgentflowNotificationRegistry,
  executeAgentflowCommandPipeline,
  openAgentflowRunState,
  parseAgentflowWorkflowOrThrow,
  resumeAgentflowCommandPipeline,
  transitionAgentflowLifecycleRun,
  type AgentflowNotificationAdapter,
  type AgentflowRunStateValue,
  validateAgentflowWorkflow,
  writeAgentflowFinalSummary
} from "../../packages/agentflow-core/src";

describe("Agentflow pipeline notifications and retention", () => {
  test("delivers configured completion channels and removes temporary artifact backings", async () => {
    const repoRoot = temporaryRepo();
    const unrelatedLogsTarget = fs.mkdtempSync(path.join(os.tmpdir(), "agentflow-workspace-logs-"));
    fs.symlinkSync(unrelatedLogsTarget, path.join(repoRoot, "logs"));
    const workflow = parseAgentflowWorkflowOrThrow(`
name: notified-cleanup
version: 1
style: pipeline
maturity: experimental
steps:
  - id: check
    type: command
    command: printf 'passed\\n'
notify:
  - on: workflow.completed
    channels: [terminal, system]
retention:
  on_success:
    keep: [final-summary.md]
    delete: [logs/**]
`);
    const delivered: Array<{ channel: string; event: string; message: string }> = [];
    const notifications = createAgentflowNotificationRegistry({
      terminal: (notification) => {
        delivered.push(notification);
      },
      system: (notification) => {
        delivered.push(notification);
      }
    });
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "notified-cleanup", workflow });

    const result = await executeAgentflowCommandPipeline(
      store,
      "notified-cleanup",
      workflow,
      undefined,
      undefined,
      undefined,
      notifications
    );

    expect(result.status).toBe("completed");
    expect(delivered.map(({ channel, event }) => [channel, event])).toEqual([
      ["terminal", "workflow.completed"],
      ["system", "workflow.completed"]
    ]);
    expect(delivered[0]!.message).toContain("notified-cleanup");
    expect(delivered[0]!.message).toContain("completed");
    expect(store.listEvents("notified-cleanup").map((event) => event.type)).toEqual([
      "run.created",
      "run.started",
      "step.started",
      "step.completed",
      "notification.delivered",
      "notification.delivered",
      "run.completed",
      "retention.deleted"
    ]);
    expect(store.listArtifacts("notified-cleanup").map((artifact) => [
      artifact.declaredPath,
      artifact.status,
      artifact.kind
    ])).toEqual([
      ["final-summary.md", "available", "run_summary"],
      [expect.stringMatching(/^logs\/check-[a-f0-9]{8}\/attempt-1\/stderr\.log$/), "missing", "command_log"],
      [expect.stringMatching(/^logs\/check-[a-f0-9]{8}\/attempt-1\/stdout\.log$/), "missing", "command_log"]
    ]);
    expect(store.readArtifact("notified-cleanup", "final-summary.md").content.toString("utf8")).toContain("Status: completed");
    store.close();
  });

  test("records optional delivery failures without failing the pipeline", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`
name: optional-notification
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: check, type: command, command: "printf ok" }
notify:
  - on: workflow.completed
    channels: [system]
`);
    const notifications = createAgentflowNotificationRegistry({
      system: () => {
        throw new Error("notification service unavailable");
      }
    });
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "optional-notification", workflow });

    const result = await executeAgentflowCommandPipeline(
      store,
      "optional-notification",
      workflow,
      undefined,
      undefined,
      undefined,
      notifications
    );

    expect(result.status).toBe("completed");
    expect(store.getRun("optional-notification")?.status).toBe("completed");
    expect(store.listEvents("optional-notification")).toContainEqual(expect.objectContaining({
      type: "notification.failed",
      payload: {
        channel: "system",
        event: "workflow.completed",
        message: "notification service unavailable",
        required: false
      }
    }));
    store.close();
  });

  test("rolls back finalization instead of misclassifying notification event-log failures", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`
name: notification-event-failure
version: 1
style: pipeline
maturity: experimental
steps: []
notify:
  - { on: workflow.completed, channels: [terminal], required: true }
`);
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "notification-event-failure", workflow });
    const database = new Database(store.databasePath);
    database.exec(`
      CREATE TRIGGER reject_delivered_event
      BEFORE INSERT ON events
      WHEN NEW.type = 'notification.delivered'
      BEGIN
        SELECT RAISE(ABORT, 'reject delivered event');
      END
    `);
    database.close();
    let deliveries = 0;
    const notifications = createAgentflowNotificationRegistry({
      terminal: () => {
        deliveries += 1;
      }
    });

    await expect(executeAgentflowCommandPipeline(
      store,
      "notification-event-failure",
      workflow,
      undefined,
      undefined,
      undefined,
      notifications
    )).rejects.toThrow("reject delivered event");
    expect(deliveries).toBe(1);
    expect(store.getRun("notification-event-failure")?.status).toBe("running");
    expect(store.getArtifact("notification-event-failure", "final-summary.md")).toBeNull();
    expect(store.listEvents("notification-event-failure").map((event) => event.type)).toEqual([
      "run.created",
      "run.started"
    ]);
    store.close();
  });

  test("rejects promise-returning adapters instead of recording an unresolved delivery", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`
name: asynchronous-notification
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: check, type: command, command: "printf ok" }
notify:
  - { on: workflow.completed, channels: [system], required: true }
`);
    const notifications = createAgentflowNotificationRegistry({
      system: (async () => {
        await Promise.resolve();
      }) as unknown as AgentflowNotificationAdapter
    });
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "asynchronous-notification", workflow });

    const result = await executeAgentflowCommandPipeline(
      store,
      "asynchronous-notification",
      workflow,
      undefined,
      undefined,
      undefined,
      notifications
    );

    expect(result.status).toBe("failed");
    expect(store.listEvents("asynchronous-notification")).toContainEqual(expect.objectContaining({
      type: "notification.failed",
      payload: expect.objectContaining({
        message: expect.stringContaining("asynchronous adapters are not supported"),
        required: true
      })
    }));
    store.close();
  });

  test("rejects callable thenables returned by unchecked notification adapters", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`
name: callable-thenable-notification
version: 1
style: pipeline
maturity: experimental
steps: []
notify:
  - { on: workflow.completed, channels: [system], required: true }
`);
    const callableThenable = Object.assign(() => {}, {
      then(resolve: () => void): void {
        queueMicrotask(resolve);
      }
    });
    const notifications = createAgentflowNotificationRegistry({
      system: (() => callableThenable) as unknown as AgentflowNotificationAdapter
    });
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "callable-thenable-notification", workflow });

    const result = await executeAgentflowCommandPipeline(
      store,
      "callable-thenable-notification",
      workflow,
      undefined,
      undefined,
      undefined,
      notifications
    );

    expect(result.status).toBe("failed");
    expect(store.listEvents("callable-thenable-notification")).toContainEqual(expect.objectContaining({
      type: "notification.failed",
      payload: expect.objectContaining({
        message: expect.stringContaining("asynchronous adapters are not supported"),
        required: true
      })
    }));
    store.close();
  });

  test("fails completed and paused outcomes when a required notification cannot be delivered", async () => {
    for (const [name, command, event] of [
      ["required-completed", "printf ok", "workflow.completed"],
      ["required-paused", "exit 9", "workflow.paused"]
    ] as const) {
      const repoRoot = temporaryRepo();
      const workflow = parseAgentflowWorkflowOrThrow(`
name: ${name}
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: check, type: command, command: "${command}" }
notify:
  - on: ${event}
    channels: [system]
    required: true
`);
      const notifications = createAgentflowNotificationRegistry({
        system: () => {
          throw new Error("required system notification failed");
        }
      });
      const store = await openAgentflowRunState({ cwd: repoRoot });
      createAgentflowLifecycleRun(store, { id: name, workflow });

      const result = await executeAgentflowCommandPipeline(
        store,
        name,
        workflow,
        undefined,
        undefined,
        undefined,
        notifications
      );

      expect(result.status).toBe("failed");
      expect(result.message).toContain("Required system notification for");
      expect(store.getRun(name)).toMatchObject({
        status: "failed",
        error: {
          code: "notification.required.failed",
          channel: "system",
          event
        }
      });
      expect(store.readArtifact(name, "final-summary.md").content.toString("utf8")).toContain("Status: failed");
      store.close();
    }
  });

  test("notifies paused pipelines and defers retention periods that have not elapsed", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`
name: paused-and-deferred
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: check, type: command, command: "exit 4" }
notify:
  - on: workflow.paused
    channels: [terminal]
retention:
  on_failure:
    delete: [logs/**]
    after_days: 7
`);
    const delivered: string[] = [];
    const notifications = createAgentflowNotificationRegistry({
      terminal: ({ event }) => {
        delivered.push(event);
      }
    });
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "paused-and-deferred", workflow });

    const paused = await executeAgentflowCommandPipeline(
      store,
      "paused-and-deferred",
      workflow,
      undefined,
      undefined,
      undefined,
      notifications
    );

    expect(paused.status).toBe("paused");
    expect(delivered).toEqual(["workflow.paused"]);
    expect(store.listEvents("paused-and-deferred").map((event) => event.type)).not.toContain("retention.deleted");
    expect(store.listArtifacts("paused-and-deferred").filter((artifact) =>
      artifact.kind === "command_log"
    ).every((artifact) => artifact.status === "available")).toBe(true);
    store.close();

    const failedWorkflow = parseAgentflowWorkflowOrThrow(`
name: failed-and-deferred
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: check, type: command, command: "exit 4", on_failure: { then: fail } }
retention:
  on_failure:
    delete: [logs/**]
    after_days: 7
`);
    const failedStore = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(failedStore, { id: "failed-and-deferred", workflow: failedWorkflow });
    expect((await executeAgentflowCommandPipeline(failedStore, "failed-and-deferred", failedWorkflow)).status).toBe("failed");
    expect(failedStore.listEvents("failed-and-deferred")).toContainEqual(expect.objectContaining({
      type: "retention.deferred",
      payload: expect.objectContaining({ rule: "on_failure", afterDays: 7 })
    }));
    expect(failedStore.listArtifacts("failed-and-deferred").filter((artifact) =>
      artifact.kind === "command_log"
    ).every((artifact) => artifact.status === "available")).toBe(true);
    failedStore.close();
  });

  test("applies terminal effects to operator lifecycle transitions", async () => {
    const repoRoot = temporaryRepo();
    const pauseWorkflow = parseAgentflowWorkflowOrThrow(`
name: lifecycle-pause
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: check, type: command, command: "printf ok" }
notify:
  - { on: workflow.paused, channels: [terminal] }
`);
    const delivered: string[] = [];
    const notifications = createAgentflowNotificationRegistry({
      terminal: ({ event }) => {
        delivered.push(event);
      }
    });
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "lifecycle-pause", workflow: pauseWorkflow });

    expect(transitionAgentflowLifecycleRun(
      store,
      "lifecycle-pause",
      "pause",
      notifications
    ).run.status).toBe("paused");
    expect(delivered).toEqual(["workflow.paused"]);
    expect(store.getArtifact("lifecycle-pause", "final-summary.md")).toBeNull();
    expect(store.listEvents("lifecycle-pause").map((event) => event.type))
      .toContain("lifecycle.pause.finalized");
    expect(transitionAgentflowLifecycleRun(store, "lifecycle-pause", "pause").run.status)
      .toBe("paused");
    expect(delivered).toEqual(["workflow.paused"]);

    const cancelWorkflow = parseAgentflowWorkflowOrThrow(`
name: lifecycle-cancel
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: check, type: command, command: "printf ok" }
retention:
  on_cancelled:
    delete: [temporary/**]
`);
    createAgentflowLifecycleRun(store, { id: "lifecycle-cancel", workflow: cancelWorkflow });
    store.writeArtifact({
      id: "temporary-log",
      runId: "lifecycle-cancel",
      path: "temporary/output.log",
      kind: "fixture",
      contentType: "text/plain",
      content: "temporary"
    });

    expect(transitionAgentflowLifecycleRun(store, "lifecycle-cancel", "cancel").run.status).toBe("cancelled");
    expect(store.readArtifact("lifecycle-cancel", "final-summary.md").content.toString("utf8"))
      .toContain("Status: cancelled");
    expect(store.getArtifact("lifecycle-cancel", "temporary/output.log")?.status).toBe("missing");
    expect(store.listEvents("lifecycle-cancel").map((event) => event.type))
      .toContain("lifecycle.cancel.finalized");

    const waitingWorkflow = parseAgentflowWorkflowOrThrow(`
name: lifecycle-cancel-waiting
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: first, type: command, command: "printf ok" }
  - { id: approve, type: manual_gate, message: Continue?, options: [approve, cancel] }
`);
    createAgentflowLifecycleRun(store, { id: "lifecycle-cancel-waiting", workflow: waitingWorkflow });
    expect((await executeAgentflowCommandPipeline(
      store,
      "lifecycle-cancel-waiting",
      waitingWorkflow
    )).status).toBe("paused");
    transitionAgentflowLifecycleRun(store, "lifecycle-cancel-waiting", "cancel");
    expect(store.readArtifact("lifecycle-cancel-waiting", "final-summary.md").content.toString("utf8"))
      .toContain("Completed steps:\n- first");

    const failedStepWorkflow = parseAgentflowWorkflowOrThrow(`
name: lifecycle-cancel-failed-step
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: first, type: command, command: "printf ok" }
  - { id: stop, type: command, command: "exit 9" }
`);
    createAgentflowLifecycleRun(store, {
      id: "lifecycle-cancel-failed-step",
      workflow: failedStepWorkflow
    });
    expect((await executeAgentflowCommandPipeline(
      store,
      "lifecycle-cancel-failed-step",
      failedStepWorkflow
    )).status).toBe("paused");
    transitionAgentflowLifecycleRun(store, "lifecycle-cancel-failed-step", "cancel");
    expect(store.readArtifact("lifecycle-cancel-failed-step", "final-summary.md").content.toString("utf8"))
      .toContain("Completed steps:\n- first");
    store.close();
  });

  test("recovers incomplete lifecycle side effects without repeating completed finalization", async () => {
    const repoRoot = temporaryRepo();
    const pauseWorkflow = parseAgentflowWorkflowOrThrow(`
name: recover-pause-finalization
version: 1
style: pipeline
maturity: experimental
steps: []
notify:
  - { on: workflow.paused, channels: [terminal] }
`);
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "recover-pause-finalization", workflow: pauseWorkflow });
    store.transitionRunWithEvent("recover-pause-finalization", {
      status: "paused",
      allowedFrom: ["pending"],
      event: { type: "run.pause", payload: { status: "paused" } }
    });
    let pauseDeliveries = 0;
    const notifications = createAgentflowNotificationRegistry({
      terminal: () => {
        pauseDeliveries += 1;
      }
    });

    expect(transitionAgentflowLifecycleRun(
      store,
      "recover-pause-finalization",
      "pause",
      notifications
    ).changed).toBe(false);
    expect(pauseDeliveries).toBe(1);
    transitionAgentflowLifecycleRun(store, "recover-pause-finalization", "resume", notifications);
    store.transitionRunWithEvent("recover-pause-finalization", {
      status: "paused",
      allowedFrom: ["running"],
      event: { type: "run.pause", payload: { status: "paused" } }
    });
    expect(transitionAgentflowLifecycleRun(
      store,
      "recover-pause-finalization",
      "pause",
      notifications
    ).changed).toBe(false);
    expect(pauseDeliveries).toBe(2);
    expect(transitionAgentflowLifecycleRun(
      store,
      "recover-pause-finalization",
      "pause",
      notifications
    ).changed).toBe(false);
    expect(pauseDeliveries).toBe(2);

    const cancelWorkflow = parseAgentflowWorkflowOrThrow(`
name: recover-cancel-finalization
version: 1
style: pipeline
maturity: experimental
steps: []
retention:
  on_cancelled:
    delete: [temporary/**]
`);
    createAgentflowLifecycleRun(store, { id: "recover-cancel-finalization", workflow: cancelWorkflow });
    store.writeArtifact({
      id: "temporary",
      runId: "recover-cancel-finalization",
      path: "temporary/output.log",
      kind: "fixture",
      contentType: "text/plain",
      content: "temporary"
    });
    store.transitionRunWithEvent("recover-cancel-finalization", {
      status: "cancelled",
      allowedFrom: ["pending"],
      event: { type: "run.cancel", payload: { status: "cancelled" } }
    });

    expect(transitionAgentflowLifecycleRun(
      store,
      "recover-cancel-finalization",
      "cancel"
    ).changed).toBe(false);
    expect(store.readArtifact("recover-cancel-finalization", "final-summary.md").content.toString("utf8"))
      .toContain("Status: cancelled");
    expect(store.getArtifact("recover-cancel-finalization", "temporary/output.log")?.status).toBe("missing");
    expect(transitionAgentflowLifecycleRun(
      store,
      "recover-cancel-finalization",
      "cancel"
    ).changed).toBe(false);
    expect(store.listEvents("recover-cancel-finalization")
      .filter((event) => event.type === "lifecycle.cancel.finalized")).toHaveLength(1);

    const failedPauseWorkflow = parseAgentflowWorkflowOrThrow(`
name: recover-required-pause-finalization
version: 1
style: pipeline
maturity: experimental
steps: []
retention:
  on_failure:
    delete: [temporary/**]
`);
    createAgentflowLifecycleRun(store, {
      id: "recover-required-pause-finalization",
      workflow: failedPauseWorkflow
    });
    store.writeArtifact({
      id: "temporary",
      runId: "recover-required-pause-finalization",
      path: "temporary/output.log",
      kind: "fixture",
      contentType: "text/plain",
      content: "temporary"
    });
    store.updateRun("recover-required-pause-finalization", {
      error: {
        code: "notification.required.failed",
        message: "paused notification failed"
      }
    });
    store.transitionRunWithEvent("recover-required-pause-finalization", {
      status: "failed",
      allowedFrom: ["pending"],
      event: {
        type: "run.failed",
        payload: { code: "notification.required.failed" }
      }
    });

    expect(transitionAgentflowLifecycleRun(
      store,
      "recover-required-pause-finalization",
      "pause"
    ).changed).toBe(false);
    expect(store.getArtifact("recover-required-pause-finalization", "temporary/output.log")?.status)
      .toBe("missing");
    expect(store.listEvents("recover-required-pause-finalization").map((event) => event.type))
      .toContain("lifecycle.pause.finalized");
    store.close();
  });

  test("keeps incomplete cancellation effects retryable when the summary cannot be recovered", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`
name: retry-cancel-finalization
version: 1
style: pipeline
maturity: experimental
steps: []
retention:
  on_cancelled:
    delete: [temporary/**]
`);
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "retry-cancel-finalization", workflow });
    store.writeArtifact({
      id: "temporary",
      runId: "retry-cancel-finalization",
      path: "temporary/output.log",
      kind: "fixture",
      contentType: "text/plain",
      content: "temporary"
    });
    store.upsertArtifact({
      id: "occupied-summary",
      runId: "retry-cancel-finalization",
      stepId: "fixture",
      path: "final-summary.md",
      kind: "fixture",
      contentType: "text/plain"
    });
    store.transitionRunWithEvent("retry-cancel-finalization", {
      status: "cancelled",
      allowedFrom: ["pending"],
      event: { type: "run.cancel", payload: { status: "cancelled" } }
    });

    expect(() => transitionAgentflowLifecycleRun(
      store,
      "retry-cancel-finalization",
      "cancel"
    )).toThrow("Could not persist final pipeline summary");
    expect(store.getArtifact("retry-cancel-finalization", "temporary/output.log")?.status)
      .toBe("available");
    expect(store.listEvents("retry-cancel-finalization").map((event) => event.type)).toEqual([
      "run.created",
      "run.cancel"
    ]);

    const database = new Database(store.databasePath);
    database.run(
      "DELETE FROM artifacts WHERE run_id = ? AND path = ?",
      ["retry-cancel-finalization", "final-summary.md"]
    );
    database.close();
    expect(transitionAgentflowLifecycleRun(
      store,
      "retry-cancel-finalization",
      "cancel"
    ).run.status).toBe("cancelled");
    expect(store.readArtifact(
      "retry-cancel-finalization",
      "final-summary.md"
    ).content.toString("utf8")).toContain("Status: cancelled");
    expect(store.getArtifact("retry-cancel-finalization", "temporary/output.log")?.status)
      .toBe("missing");
    store.close();
  });

  test("serializes lifecycle notification finalization across processes", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`
name: concurrent-pause-finalization
version: 1
style: pipeline
maturity: experimental
steps: []
notify:
  - { on: workflow.paused, channels: [terminal] }
`);
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, {
      id: "concurrent-pause-finalization",
      workflow
    });
    store.close();

    const modulePath = path.resolve("packages/agentflow-core/src/index.ts");
    const script = `
      import {
        createAgentflowNotificationRegistry,
        openAgentflowRunState,
        transitionAgentflowLifecycleRun
      } from ${JSON.stringify(modulePath)};
      import fs from "node:fs";
      const store = await openAgentflowRunState({
        cwd: process.env.AF_ROOT,
        busyTimeoutMs: 5000
      });
      const notifications = createAgentflowNotificationRegistry({
        terminal: () => {
          fs.appendFileSync(process.env.AF_DELIVERIES, "delivered\\n");
          Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 100);
        }
      });
      transitionAgentflowLifecycleRun(
        store,
        "concurrent-pause-finalization",
        "pause",
        notifications
      );
      store.close();
    `;
    const deliveriesPath = path.join(repoRoot, "deliveries.log");
    const children = Array.from({ length: 2 }, () =>
      Bun.spawn({
        cmd: [process.execPath, "-e", script],
        env: {
          ...process.env,
          AF_ROOT: repoRoot,
          AF_DELIVERIES: deliveriesPath
        }
      })
    );

    expect(await Promise.all(children.map((child) => child.exited))).toEqual([0, 0]);
    expect(fs.readFileSync(deliveriesPath, "utf8")).toBe("delivered\n");

    const reopened = await openAgentflowRunState({ cwd: repoRoot });
    const events = reopened.listEvents("concurrent-pause-finalization");
    expect(events.filter((event) => event.type === "notification.delivered")).toHaveLength(1);
    expect(events.filter((event) => event.type === "pipeline.effects.finalized")).toHaveLength(1);
    reopened.close();
  });

  test("stops an active executor when a required operator-pause notification fails", async () => {
    const repoRoot = temporaryRepo();
    const marker = path.join(repoRoot, "continued");
    const workflow = parseAgentflowWorkflowOrThrow(`
name: operator-pause-failure
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: first, type: command, command: "printf ok" }
  - { id: work, type: command, command: "sleep 0.25; touch continued" }
notify:
  - { on: workflow.paused, channels: [system], required: true }
retention:
  on_failure:
    delete: [logs/**]
`);
    const notifications = createAgentflowNotificationRegistry({
      system: () => {
        throw new Error("unavailable");
      }
    });
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "operator-pause-failure", workflow });
    const execution = executeAgentflowCommandPipeline(
      store,
      "operator-pause-failure",
      workflow,
      undefined,
      undefined,
      undefined,
      notifications
    );
    await new Promise((resolve) => setTimeout(resolve, 30));
    const operatorStore = await openAgentflowRunState({ cwd: repoRoot });

    expect(transitionAgentflowLifecycleRun(
      operatorStore,
      "operator-pause-failure",
      "pause",
      notifications
    ).run.status).toBe("failed");
    operatorStore.close();
    expect((await execution).status).toBe("failed");
    expect(fs.existsSync(marker)).toBe(false);
    expect(store.getRun("operator-pause-failure")?.status).toBe("failed");
    expect(store.readArtifact("operator-pause-failure", "final-summary.md").content.toString("utf8"))
      .toContain("Completed steps:\n- first");
    expect(store.listArtifacts("operator-pause-failure")
      .filter((artifact) => artifact.kind === "command_log")
      .every((artifact) => artifact.status === "missing")).toBe(true);
    store.close();
  });

  test("stops an active executor when operator cancellation cannot persist its summary", async () => {
    const repoRoot = temporaryRepo();
    const marker = path.join(repoRoot, "continued");
    const workflow = parseAgentflowWorkflowOrThrow(`
name: operator-cancel-summary-failure
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: work, type: command, command: "sleep 0.25; touch continued" }
retention:
  on_failure:
    delete: [logs/**]
`);
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, {
      id: "operator-cancel-summary-failure",
      workflow
    });
    store.upsertArtifact({
      id: "fixture-summary",
      runId: "operator-cancel-summary-failure",
      stepId: "fixture",
      path: "final-summary.md",
      kind: "fixture",
      contentType: "text/markdown"
    });

    const execution = executeAgentflowCommandPipeline(
      store,
      "operator-cancel-summary-failure",
      workflow
    );
    await new Promise((resolve) => setTimeout(resolve, 30));
    const operatorStore = await openAgentflowRunState({ cwd: repoRoot });
    expect(transitionAgentflowLifecycleRun(
      operatorStore,
      "operator-cancel-summary-failure",
      "cancel"
    ).run).toMatchObject({
      status: "failed",
      error: { code: "summary.persist.failed" }
    });
    operatorStore.close();

    await expect(execution).resolves.toMatchObject({
      status: "failed",
      message: expect.stringContaining("Could not persist final pipeline summary")
    });
    expect(fs.existsSync(marker)).toBe(false);
    expect(store.listEvents("operator-cancel-summary-failure")
      .filter((event) => event.type === "pipeline.effects.finalized")).toHaveLength(1);
    store.close();
  });

  test("keeps notification finalization and the summary reservation scoped to pipeline workflows", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`
name: recovery-with-notify-data
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - id: check
    type: command
    command: "printf recovered > final-summary.md"
    outputs: [final-summary.md]
notify:
  - { on: workflow.completed, channels: [system], required: yes }
`);
    let delivered = false;
    const notifications = createAgentflowNotificationRegistry({
      system: () => {
        delivered = true;
        throw new Error("unavailable");
      }
    });
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "recovery-with-notify-data", workflow });

    expect((await executeAgentflowCommandPipeline(
      store,
      "recovery-with-notify-data",
      workflow,
      undefined,
      undefined,
      undefined,
      notifications
    )).status).toBe("completed");
    expect(delivered).toBe(false);
    expect(store.readArtifact("recovery-with-notify-data", "final-summary.md").content.toString("utf8"))
      .toBe("recovered");
    store.close();
  });

  test("closes a manual-gate approval when its required paused notification fails", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`
name: required-gate-notification
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: approve, type: manual_gate, message: Continue?, options: [approve, cancel] }
notify:
  - { on: workflow.paused, channels: [system], required: true }
`);
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "required-gate-notification", workflow });
    const notifications = createAgentflowNotificationRegistry({
      system: () => {
        throw new Error("unavailable");
      }
    });

    expect((await executeAgentflowCommandPipeline(
      store,
      "required-gate-notification",
      workflow,
      undefined,
      undefined,
      undefined,
      notifications
    )).status).toBe("failed");
    const database = new Database(store.databasePath, { readonly: true });
    expect(database.query(
      "SELECT status, decision FROM approvals WHERE run_id = ?"
    ).get("required-gate-notification")).toEqual({
      status: "cancelled",
      decision: "notification_failure"
    });
    database.close();
    store.close();
  });

  test("rolls back terminal interaction state when closing a failed gate cannot commit", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`
name: atomic-gate-notification
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: approve, type: manual_gate, message: Continue?, options: [approve, cancel] }
notify:
  - { on: workflow.paused, channels: [system], required: true }
`);
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "atomic-gate-notification", workflow });
    const database = new Database(store.databasePath);
    database.exec(`
      CREATE TRIGGER reject_failed_gate
      BEFORE UPDATE OF status ON run_steps
      WHEN NEW.status = 'failed'
      BEGIN
        SELECT RAISE(ABORT, 'reject failed gate');
      END
    `);
    database.close();
    const notifications = createAgentflowNotificationRegistry({
      system: () => {
        throw new Error("unavailable");
      }
    });

    await expect(executeAgentflowCommandPipeline(
      store,
      "atomic-gate-notification",
      workflow,
      undefined,
      undefined,
      undefined,
      notifications
    )).rejects.toThrow("reject failed gate");
    expect(store.getRun("atomic-gate-notification")).toMatchObject({
      status: "running",
      currentStepId: "approve",
      context: { waiting: { kind: "manual_gate", stepId: "approve" } }
    });
    expect(store.getArtifact("atomic-gate-notification", "final-summary.md")).toBeNull();
    expect(store.listEvents("atomic-gate-notification").map((event) => event.type)).toEqual([
      "run.created",
      "run.started",
      "step.waiting"
    ]);
    const persisted = new Database(store.databasePath, { readonly: true });
    expect(persisted.query(
      "SELECT status FROM run_steps WHERE run_id = ? AND step_id = ?"
    ).get("atomic-gate-notification", "approve")).toEqual({ status: "waiting" });
    expect(persisted.query(
      "SELECT status, decision FROM approvals WHERE run_id = ?"
    ).get("atomic-gate-notification")).toEqual({ status: "requested", decision: null });
    persisted.close();
    store.close();
  });

  test("validates notification rules and reserves the runtime summary path in pipeline workflows", async () => {
    const workflow = parseAgentflowWorkflowOrThrow(`
name: invalid-notifications
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: check, type: command, command: "printf ok", outputs: [final-summary.md] }
notify:
  - on: workflow.cancelled
    channels: []
    required: yes
  - on: workflow.completed
    channels: [terminal, terminal]
`);

    expect(validateAgentflowWorkflow(workflow).errors.map((issue) => issue.code)).toEqual([
      "workflow.notification.event.unsupported",
      "workflow.notification.channels.invalid",
      "workflow.notification.required.invalid",
      "workflow.notification.channel.duplicate",
      "workflow.artifact.output.reserved"
    ]);

    const repoRoot = temporaryRepo();
    const validWorkflow = parseAgentflowWorkflowOrThrow(`
name: reserved-summary
version: 1
style: pipeline
maturity: experimental
steps: []
`);
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "reserved-summary", workflow: validWorkflow });
    expect(() => store.writeArtifact({
      id: "fixture-summary",
      runId: "reserved-summary",
      path: "final-summary.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "forged"
    })).toThrow("reserved for the runtime");
    store.close();
  });

  test("fails the terminal outcome when a mandatory final summary cannot be persisted", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`
name: blocked-summary
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: check, type: command, command: "printf ok" }
notify:
  - { on: workflow.completed, channels: [terminal] }
  - { on: workflow.failed, channels: [terminal] }
`);
    const delivered: string[] = [];
    const notifications = createAgentflowNotificationRegistry({
      terminal: ({ event }) => {
        delivered.push(event);
      }
    });
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "blocked-summary", workflow });
    store.upsertArtifact({
      id: "fixture-summary",
      runId: "blocked-summary",
      stepId: "fixture",
      path: "final-summary.md",
      kind: "fixture",
      contentType: "text/markdown"
    });

    const result = await executeAgentflowCommandPipeline(
      store,
      "blocked-summary",
      workflow,
      undefined,
      undefined,
      undefined,
      notifications
    );

    expect(result).toMatchObject({
      status: "failed",
      message: expect.stringContaining("Could not persist final pipeline summary")
    });
    expect(store.getRun("blocked-summary")).toMatchObject({
      status: "failed",
      error: {
        code: "summary.persist.failed"
      }
    });
    expect(store.listEvents("blocked-summary").map((event) => event.type))
      .toContain("summary.failed");
    expect(delivered).toEqual(["workflow.failed"]);
    store.close();
  });

  test("restores the mandatory summary backing when finalization rolls back", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`
name: rolled-back-summary
version: 1
style: pipeline
maturity: experimental
steps: []
`);
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "rolled-back-summary", workflow });
    const database = new Database(store.databasePath);
    database.exec(`
      CREATE TRIGGER reject_completion_event
      BEFORE INSERT ON events
      WHEN NEW.type = 'run.completed'
      BEGIN
        SELECT RAISE(ABORT, 'reject completion');
      END
    `);
    database.close();

    await expect(executeAgentflowCommandPipeline(
      store,
      "rolled-back-summary",
      workflow
    )).rejects.toThrow("reject completion");
    expect(store.getRun("rolled-back-summary")?.status).toBe("running");
    expect(store.getArtifact("rolled-back-summary", "final-summary.md")).toBeNull();
    expect(store.getArtifactBackingSnapshot("rolled-back-summary", "final-summary.md").exists)
      .toBe(false);

    expect(transitionAgentflowLifecycleRun(
      store,
      "rolled-back-summary",
      "cancel"
    ).run.status).toBe("cancelled");
    expect(store.readArtifact("rolled-back-summary", "final-summary.md").content.toString("utf8"))
      .toContain("Status: cancelled");
    store.close();
  });

  test("preserves the original summary when one transaction rewrites it twice and rolls back", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`
name: repeated-summary-rollback
version: 1
style: pipeline
maturity: experimental
steps: []
notify:
  - { on: workflow.completed, channels: [system], required: true }
`);
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "repeated-summary-rollback", workflow });
    writeAgentflowFinalSummary(store, "repeated-summary-rollback", workflow, {
      status: "failed",
      completedSteps: [],
      message: "original recovery summary"
    });
    const original = store.readArtifact(
      "repeated-summary-rollback",
      "final-summary.md"
    ).content.toString("utf8");
    const database = new Database(store.databasePath);
    database.exec(`
      CREATE TRIGGER reject_failed_finalization
      BEFORE INSERT ON events
      WHEN NEW.type = 'run.failed'
      BEGIN
        SELECT RAISE(ABORT, 'reject failed finalization');
      END
    `);
    database.close();
    const notifications = createAgentflowNotificationRegistry({
      system: () => {
        throw new Error("unavailable");
      }
    });

    await expect(executeAgentflowCommandPipeline(
      store,
      "repeated-summary-rollback",
      workflow,
      undefined,
      undefined,
      undefined,
      notifications
    )).rejects.toThrow("reject failed finalization");
    expect(store.getRun("repeated-summary-rollback")?.status).toBe("running");
    expect(store.getArtifact("repeated-summary-rollback", "final-summary.md")?.status)
      .toBe("available");
    expect(store.readArtifact(
      "repeated-summary-rollback",
      "final-summary.md"
    ).content.toString("utf8")).toBe(original);
    store.close();
  });

  test("restores retained artifact backings when finalization rolls back", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`
name: rolled-back-retention
version: 1
style: pipeline
maturity: experimental
steps: []
retention:
  on_success:
    delete: [temporary/**]
`);
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "rolled-back-retention", workflow });
    store.writeArtifact({
      id: "temporary-output",
      runId: "rolled-back-retention",
      path: "temporary/output.log",
      kind: "fixture",
      contentType: "text/plain",
      content: "temporary"
    });
    const database = new Database(store.databasePath);
    database.exec(`
      CREATE TRIGGER reject_retention_event
      BEFORE INSERT ON events
      WHEN NEW.type = 'retention.deleted'
      BEGIN
        SELECT RAISE(ABORT, 'reject retention');
      END
    `);
    database.close();

    await expect(executeAgentflowCommandPipeline(
      store,
      "rolled-back-retention",
      workflow
    )).rejects.toThrow("reject retention");
    expect(store.getRun("rolled-back-retention")?.status).toBe("running");
    expect(store.getArtifact("rolled-back-retention", "final-summary.md")).toBeNull();
    expect(store.getArtifact("rolled-back-retention", "temporary/output.log")?.status)
      .toBe("available");
    expect(store.readArtifact("rolled-back-retention", "temporary/output.log").content.toString("utf8"))
      .toBe("temporary");
    store.close();
  });

  test("keeps repeated retention deletion rollback idempotent", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`
name: repeated-retention-rollback
version: 1
style: pipeline
maturity: experimental
steps: []
`);
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "repeated-retention-rollback", workflow });
    store.writeArtifact({
      id: "temporary-output",
      runId: "repeated-retention-rollback",
      path: "temporary/output.log",
      kind: "fixture",
      contentType: "text/plain",
      content: "temporary"
    });

    expect(() => store.withRunFinalizationTransaction("repeated-retention-rollback", () => {
      store.deleteArtifactBacking("repeated-retention-rollback", "temporary/output.log");
      store.deleteArtifactBacking("repeated-retention-rollback", "temporary/output.log");
      throw new Error("roll back repeated retention");
    })).toThrow("roll back repeated retention");

    expect(store.getArtifact("repeated-retention-rollback", "temporary/output.log")?.status)
      .toBe("available");
    expect(store.readArtifact(
      "repeated-retention-rollback",
      "temporary/output.log"
    ).content.toString("utf8")).toBe("temporary");
    store.close();
  });

  test("restores the original backing when finalization writes and then deletes an artifact", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`
name: write-delete-retention-rollback
version: 1
style: pipeline
maturity: experimental
steps: []
`);
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "write-delete-retention-rollback", workflow });
    store.writeArtifact({
      id: "temporary-output",
      runId: "write-delete-retention-rollback",
      path: "temporary/output.log",
      kind: "fixture",
      contentType: "text/plain",
      content: "original"
    });

    expect(() => store.withRunFinalizationTransaction("write-delete-retention-rollback", () => {
      store.writeArtifact({
        id: "temporary-output",
        runId: "write-delete-retention-rollback",
        path: "temporary/output.log",
        kind: "fixture",
        contentType: "text/plain",
        content: "replacement",
        overwrite: true
      });
      store.deleteArtifactBacking("write-delete-retention-rollback", "temporary/output.log");
      throw new Error("roll back write and deletion");
    })).toThrow("roll back write and deletion");

    expect(store.getArtifact("write-delete-retention-rollback", "temporary/output.log")?.status)
      .toBe("available");
    expect(store.readArtifact(
      "write-delete-retention-rollback",
      "temporary/output.log"
    ).content.toString("utf8")).toBe("original");
    store.close();
  });

  test("does not recover deletion-specific retention staging as an interrupted write", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`
name: retained-deletion-recovery
version: 1
style: pipeline
maturity: experimental
steps: []
`);
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "retained-deletion-recovery", workflow });
    const artifact = store.writeArtifact({
      id: "temporary-output",
      runId: "retained-deletion-recovery",
      path: "temporary/output.log",
      kind: "fixture",
      contentType: "text/plain",
      content: "temporary"
    });
    const target = path.join(repoRoot, artifact.storagePath);
    const originalRemove = fs.rmSync.bind(fs);
    let deletionCleanupAttempts = 0;
    const remove = spyOn(fs, "rmSync").mockImplementation((candidate, options) => {
      if (String(candidate).endsWith(".deleted")) {
        deletionCleanupAttempts += 1;
        if (deletionCleanupAttempts === 1) return;
      }
      originalRemove(candidate, options);
    });

    try {
      expect(store.deleteArtifactBacking("retained-deletion-recovery", "temporary/output.log").status)
        .toBe("missing");
    } finally {
      remove.mockRestore();
    }

    const deletionBackup = path.join(
      repoRoot,
      ".agentflow/runs",
      `r-${createHash("sha256").update("retained-deletion-recovery").digest("hex")}`,
      ".staging",
      `${createHash("sha256").update("temporary/output.log").digest("hex")}.deleted`
    );
    expect(deletionCleanupAttempts).toBe(1);
    expect(fs.existsSync(target)).toBe(false);
    expect(fs.readFileSync(deletionBackup, "utf8")).toBe("temporary");

    store.recoverArtifactBacking("retained-deletion-recovery", "temporary/output.log");

    expect(fs.existsSync(target)).toBe(false);
    expect(fs.existsSync(deletionBackup)).toBe(false);
    expect(store.getArtifact("retained-deletion-recovery", "temporary/output.log")?.status)
      .toBe("missing");
    store.close();
  });

  test("restores deletion-specific staging when retention did not commit", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`
name: interrupted-retention-deletion
version: 1
style: pipeline
maturity: experimental
steps: []
`);
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "interrupted-retention-deletion", workflow });
    const artifact = store.writeArtifact({
      id: "temporary-output",
      runId: "interrupted-retention-deletion",
      path: "temporary/output.log",
      kind: "fixture",
      contentType: "text/plain",
      content: "temporary"
    });
    const target = path.join(repoRoot, artifact.storagePath);
    const deletionBackup = path.join(
      repoRoot,
      ".agentflow/runs",
      `r-${createHash("sha256").update("interrupted-retention-deletion").digest("hex")}`,
      ".staging",
      `${createHash("sha256").update("temporary/output.log").digest("hex")}.deleted`
    );
    fs.renameSync(target, deletionBackup);

    expect(store.getArtifact("interrupted-retention-deletion", "temporary/output.log")?.status)
      .toBe("available");
    store.recoverArtifactBacking("interrupted-retention-deletion", "temporary/output.log");

    expect(fs.readFileSync(target, "utf8")).toBe("temporary");
    expect(fs.existsSync(deletionBackup)).toBe(false);
    expect(store.getArtifact("interrupted-retention-deletion", "temporary/output.log")?.status)
      .toBe("available");
    store.close();
  });

  test("prevents concurrent inspection from committing a missing status during deletion", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`
name: concurrent-retention-inspection
version: 1
style: pipeline
maturity: experimental
steps: []
`);
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "concurrent-retention-inspection", workflow });
    const artifact = store.writeArtifact({
      id: "temporary-output",
      runId: "concurrent-retention-inspection",
      path: "temporary/output.log",
      kind: "fixture",
      contentType: "text/plain",
      content: "temporary"
    });
    const observer = await openAgentflowRunState({ cwd: repoRoot, busyTimeoutMs: 1 });
    const originalRename = fs.renameSync.bind(fs);
    const rename = spyOn(fs, "renameSync").mockImplementation((source, destination) => {
      originalRename(source, destination);
      if (String(destination).endsWith(".deleted")) {
        expect(observer.getArtifact("concurrent-retention-inspection", "temporary/output.log")?.status)
          .toBe("available");
        throw new Error("simulated crash after deletion rename");
      }
    });

    try {
      expect(() => store.deleteArtifactBacking(
        "concurrent-retention-inspection",
        "temporary/output.log"
      )).toThrow("simulated crash after deletion rename");
    } finally {
      rename.mockRestore();
    }

    const database = new Database(store.databasePath, { readonly: true });
    expect(database.query(
      "SELECT status FROM artifacts WHERE run_id = ? AND path = ?"
    ).get("concurrent-retention-inspection", "temporary/output.log")).toEqual({
      status: "available"
    });
    database.close();

    store.recoverArtifactBacking("concurrent-retention-inspection", "temporary/output.log");
    expect(fs.readFileSync(path.join(repoRoot, artifact.storagePath), "utf8")).toBe("temporary");
    observer.close();
    store.close();
  });

  test("fails lifecycle finalization explicitly when its mandatory summary cannot be persisted", async () => {
    for (const [runId, action, notify] of [
      ["blocked-cancel-summary", "cancel", ""],
      [
        "blocked-pause-summary",
        "pause",
        "notify:\n  - { on: workflow.paused, channels: [missing], required: true }"
      ]
    ] as const) {
      const repoRoot = temporaryRepo();
      const workflow = parseAgentflowWorkflowOrThrow(`
name: ${runId}
version: 1
style: pipeline
maturity: experimental
steps: []
${notify}
`);
      const store = await openAgentflowRunState({ cwd: repoRoot });
      createAgentflowLifecycleRun(store, { id: runId, workflow });
      store.upsertArtifact({
        id: "fixture-summary",
        runId,
        stepId: "fixture",
        path: "final-summary.md",
        kind: "fixture",
        contentType: "text/markdown"
      });

      const result = transitionAgentflowLifecycleRun(store, runId, action);

      expect(result.run).toMatchObject({
        status: "failed",
        error: { code: "summary.persist.failed" }
      });
      expect(store.listEvents(runId).map((event) => event.type)).toContain("summary.failed");
      store.close();
    }
  });

  test("fails closed before execution for malformed persisted notification rules", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`
name: persisted-invalid-notification
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: check, type: command, command: "touch command-started" }
notify:
  - on: workflow.completed
    channels: []
`);
    const store = await openAgentflowRunState({ cwd: repoRoot });
    store.createRunWithEvent({
      id: "persisted-invalid-notification",
      workflow: {
        name: workflow.name,
        version: workflow.version,
        style: workflow.style,
        maturity: workflow.maturity
      },
      context: { workflow: workflow as unknown as AgentflowRunStateValue }
    }, { type: "run.created", payload: { status: "pending" } });

    await expect(executeAgentflowCommandPipeline(
      store,
      "persisted-invalid-notification",
      workflow
    )).rejects.toThrow("cannot execute invalid notifications");
    expect(fs.existsSync(path.join(repoRoot, "command-started"))).toBe(false);
    expect(store.getRun("persisted-invalid-notification")?.status).toBe("pending");
    store.close();
  });

  test("fails closed with an actionable error for a malformed persisted notification root", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`
name: persisted-invalid-notification-root
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: check, type: command, command: "touch command-started" }
`);
    (workflow as unknown as { notify: unknown }).notify = {
      on: "workflow.completed",
      channels: ["terminal"]
    };
    const store = await openAgentflowRunState({ cwd: repoRoot });
    store.createRunWithEvent({
      id: "persisted-invalid-notification-root",
      workflow: {
        name: workflow.name,
        version: workflow.version,
        style: workflow.style,
        maturity: workflow.maturity
      },
      context: { workflow: workflow as unknown as AgentflowRunStateValue }
    }, { type: "run.created", payload: { status: "pending" } });

    await expect(executeAgentflowCommandPipeline(
      store,
      "persisted-invalid-notification-root",
      workflow
    )).rejects.toMatchObject({
      code: "AGENTFLOW_WORKFLOW_INVALID",
      message: expect.stringContaining("workflow.notification.rules.invalid (notify)")
    });
    expect(fs.existsSync(path.join(repoRoot, "command-started"))).toBe(false);
    store.close();
  });

  test("fails closed before lifecycle transitions for malformed persisted workflow effects", async () => {
    for (const [runId, source] of [
      [
        "lifecycle-invalid-notification",
        "notify:\n  - { on: workflow.paused, channels: [] }"
      ],
      [
        "lifecycle-invalid-retention",
        "retention:\n  on_cancelled:\n    delete: [temporary/**]\n    after_days: soon"
      ]
    ] as const) {
      const repoRoot = temporaryRepo();
      const workflow = parseAgentflowWorkflowOrThrow(`
name: ${runId}
version: 1
style: pipeline
maturity: experimental
steps: []
${source}
`);
      const store = await openAgentflowRunState({ cwd: repoRoot });
      store.createRunWithEvent({
        id: runId,
        workflow: {
          name: workflow.name,
          version: workflow.version,
          style: workflow.style,
          maturity: workflow.maturity
        },
        context: { workflow: workflow as unknown as AgentflowRunStateValue }
      }, { type: "run.created", payload: { status: "pending" } });

      expect(() => transitionAgentflowLifecycleRun(
        store,
        runId,
        runId.includes("notification") ? "pause" : "cancel"
      )).toThrow("persisted workflow validation failed");
      expect(store.getRun(runId)?.status).toBe("pending");
      expect(store.listEvents(runId).map((event) => event.type)).toEqual(["run.created"]);
      store.close();
    }
  });

  test("reports a missing persisted workflow definition as an actionable lifecycle error", async () => {
    const repoRoot = temporaryRepo();
    const store = await openAgentflowRunState({ cwd: repoRoot });
    store.createRunWithEvent({
      id: "missing-persisted-workflow",
      workflow: {
        name: "missing-persisted-workflow",
        version: 1,
        style: "pipeline",
        maturity: "experimental"
      }
    }, { type: "run.created", payload: { status: "pending" } });

    expect(() => transitionAgentflowLifecycleRun(
      store,
      "missing-persisted-workflow",
      "pause"
    )).toThrow("persisted context does not contain a workflow definition");
    expect(store.getRun("missing-persisted-workflow")?.status).toBe("pending");
    store.close();
  });

  test("routes input-answer publication failures through required paused notifications", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`
name: input-publication-notification
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: details, type: input_request, question: Target?, save_as: answer.md }
notify:
  - { on: workflow.paused, channels: [system], required: true }
`);
    let deliveries = 0;
    const notifications = createAgentflowNotificationRegistry({
      system: () => {
        deliveries += 1;
        if (deliveries === 2) throw new Error("second pause unavailable");
      }
    });
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "input-publication-notification", workflow });

    expect((await executeAgentflowCommandPipeline(
      store,
      "input-publication-notification",
      workflow,
      undefined,
      undefined,
      undefined,
      notifications
    )).status).toBe("paused");
    store.writeArtifact({
      id: "foreign-answer",
      runId: "input-publication-notification",
      stepId: "foreign",
      path: "answer.md",
      kind: "fixture",
      contentType: "text/plain",
      content: "occupied"
    });

    await expect(resumeAgentflowCommandPipeline(
      store,
      "input-publication-notification",
      workflow,
      { answer: "staging" },
      undefined,
      undefined,
      undefined,
      notifications
    )).rejects.toMatchObject({ code: "AGENTFLOW_ARTIFACT_COLLISION" });
    expect(deliveries).toBe(2);
    expect(store.getRun("input-publication-notification")).toMatchObject({
      status: "failed",
      error: {
        code: "notification.required.failed",
        event: "workflow.paused"
      }
    });
    store.close();
  });

  test("fails closed before execution for malformed persisted retention", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`
name: persisted-invalid-retention
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: check, type: command, command: "touch command-started" }
retention:
  on_success:
    delete: [logs/**]
    after_days: soon
`);
    const store = await openAgentflowRunState({ cwd: repoRoot });
    store.createRunWithEvent({
      id: "persisted-invalid-retention",
      workflow: {
        name: workflow.name,
        version: workflow.version,
        style: workflow.style,
        maturity: workflow.maturity
      },
      context: { workflow: workflow as unknown as AgentflowRunStateValue }
    }, { type: "run.created", payload: { status: "pending" } });

    await expect(executeAgentflowCommandPipeline(
      store,
      "persisted-invalid-retention",
      workflow
    )).rejects.toThrow("cannot execute invalid retention");
    expect(fs.existsSync(path.join(repoRoot, "command-started"))).toBe(false);
    expect(store.getRun("persisted-invalid-retention")?.status).toBe("pending");
    store.close();
  });
});

function temporaryRepo(): string {
  const repoRoot = fs.mkdtempSync(path.join(os.tmpdir(), "agentflow-notifications-"));
  fs.mkdirSync(path.join(repoRoot, ".git"));
  return repoRoot;
}
