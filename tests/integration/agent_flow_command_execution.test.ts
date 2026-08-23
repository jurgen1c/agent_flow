import { Database } from "bun:sqlite";
import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  AgentFlowRunStateError,
  MAX_AGENT_FLOW_FAILURE_ATTACHMENT_COUNT,
  MAX_AGENT_FLOW_FAILURE_ATTACHMENT_SCAN_BYTES,
  MAX_AGENT_FLOW_FAILURE_TOTAL_ATTACHMENT_BYTES,
  type AgentFlowRunStateValue,
  createAgentFlowSessionProviderRegistry,
  createAgentFlowWorkflowRegistry,
  createAgentFlowLifecycleRun,
  executeAgentFlowCommandPipeline,
  openAgentFlowRunState,
  parseAgentFlowWorkflowOrThrow,
  persistAgentFlowFailurePayload,
  resumeAgentFlowCommandPipeline,
  transitionAgentFlowLifecycleRun
} from "../../src/runtime";

describe("Agent Flow command step execution", () => {
  test("rejects a workflow that differs from the persisted run definition", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: immutable-run
version: 1
style: pipeline
maturity: experimental
steps:
  - id: write
    type: command
    command: printf original
`);
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(store, { id: "mismatched-workflow", workflow });

    await expect(executeAgentFlowCommandPipeline(store, "mismatched-workflow", {
      ...workflow,
      steps: [{ id: "write", type: "command", command: "printf replacement" }]
    })).rejects.toThrow("differs from its persisted definition");

    expect(store.getRun("mismatched-workflow")?.status).toBe("pending");
    store.close();
  });

  test("rejects a mismatched workflow before stale recovery mutates the run", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: persisted-workflow
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: work, type: command, command: echo persisted }
`);
    const mismatched = parseAgentFlowWorkflowOrThrow(`
name: mismatched-workflow
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: work, type: command, command: echo mismatched }
`);
    const interrupted = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(interrupted, { id: "stale-mismatched-workflow", workflow });
    interrupted.acquireRunLock("stale-mismatched-workflow", "run", { ttlMs: 60_000 });
    interrupted.transitionRunWithEvent("stale-mismatched-workflow", {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: { status: "running" } }
    });
    interrupted.updateRun("stale-mismatched-workflow", { currentStepId: "work" });
    interrupted.upsertStep({
      runId: "stale-mismatched-workflow",
      stepId: "work",
      attempt: 1,
      status: "running"
    });
    interrupted.close();

    const recovered = await openAgentFlowRunState({ cwd: repoRoot });
    await expect(executeAgentFlowCommandPipeline(recovered, "stale-mismatched-workflow", mismatched))
      .rejects.toMatchObject({ code: "AGENT_FLOW_RUN_COLLISION" });
    expect(recovered.getRun("stale-mismatched-workflow")).toMatchObject({
      status: "running",
      currentStepId: "work"
    });
    expect(recovered.latestStepRecoveryState("stale-mismatched-workflow", "work"))
      .toMatchObject({ attempt: 1, status: "running" });
    expect(recovered.listEvents("stale-mismatched-workflow").map((event) => event.type))
      .toEqual(["run.created", "run.started"]);
    recovered.close();
  });

  test("fails closed for invalid failure policies on externally persisted runs", async () => {
    for (const [runId, failurePolicy, expectedMessage] of [
      ["invalid-retry", "retry: 101\n      then: fail", "integer from 0 through 100"],
      ["unapproved-continue", "then: continue", "on_failure.allowed is true"],
      ["unapproved-ignore", "then: ignore", "on_failure.allowed is true"],
      ["unapproved-padded-continue", "then: ' continue '", "on_failure.allowed is true"],
      ["unapproved-padded-ignore", "then: ' ignore '", "on_failure.allowed is true"]
    ] as const) {
      const repoRoot = temporaryRepo();
      const workflow = parseAgentFlowWorkflowOrThrow(`
name: ${runId}
version: 1
style: pipeline
maturity: experimental
steps:
  - id: unsafe-policy
    type: command
    command: touch command-started
    on_failure:
      ${failurePolicy}
`);
      const store = await openAgentFlowRunState({ cwd: repoRoot });
      store.createRunWithEvent({
        id: runId,
        workflow: {
          name: workflow.name,
          version: workflow.version,
          style: workflow.style,
          maturity: workflow.maturity
        },
        context: { workflow: workflow as unknown as AgentFlowRunStateValue }
      }, { type: "run.created", payload: { status: "pending" } });

      const result = await executeAgentFlowCommandPipeline(store, runId, workflow);

      expect(result).toMatchObject({ status: "failed", failedStep: "unsafe-policy" });
      expect(result.message).toContain(expectedMessage);
      expect(fs.existsSync(path.join(repoRoot, "command-started"))).toBe(false);
      store.close();
    }
  });

  test("records a preflight failure for a malformed persisted command", async () => {
    const repoRoot = temporaryRepo();
    const parsed = parseAgentFlowWorkflowOrThrow(`
name: malformed-command
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: malformed, type: command, command: printf valid }
`);
    const workflow = {
      ...parsed,
      steps: [{ ...parsed.steps[0]!, command: 123 }]
    } as unknown as typeof parsed;
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    store.createRunWithEvent({
      id: "malformed-command",
      workflow: {
        name: workflow.name,
        version: workflow.version,
        style: workflow.style,
        maturity: workflow.maturity
      },
      context: { workflow: workflow as unknown as AgentFlowRunStateValue }
    }, { type: "run.created", payload: { status: "pending" } });

    const result = await executeAgentFlowCommandPipeline(store, "malformed-command", workflow);

    expect(result).toMatchObject({ status: "failed", failedStep: "malformed" });
    expect(store.getRun("malformed-command")?.status).toBe("failed");
    const failure = store.listFailures("malformed-command")[0]!;
    expect(JSON.parse(store.readArtifact("malformed-command", failure.payloadPath!).content.toString("utf8")))
      .toMatchObject({ command: null, summary: "Command steps require a non-empty command." });
    store.close();
  });

  test("does not allow an executor from another store to share a running run", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: single-owner
version: 1
style: pipeline
maturity: experimental
steps:
  - id: write
    type: command
    command: sleep 0.1
`);
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    const competitor = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(store, { id: "single-owner", workflow });

    const owner = executeAgentFlowCommandPipeline(store, "single-owner", workflow);
    await expect(executeAgentFlowCommandPipeline(competitor, "single-owner", workflow))
      .rejects.toMatchObject({ code: "AGENT_FLOW_RUN_LOCKED" });
    expect((await owner).status).toBe("completed");
    competitor.close();
    store.close();
  });

  test("rolls back run start when the initial routing checkpoint fails", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: atomic-run-start
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: check, type: command, command: "printf passed" }
`);
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(store, { id: "atomic-run-start", workflow });
    const updateRun = store.updateRun.bind(store);
    let rejectCheckpoint = true;
    store.updateRun = ((runId, input) => {
      if (rejectCheckpoint && input.context?.executionRouting !== undefined) {
        rejectCheckpoint = false;
        throw new Error("checkpoint unavailable");
      }
      return updateRun(runId, input);
    }) as typeof store.updateRun;

    await expect(executeAgentFlowCommandPipeline(store, "atomic-run-start", workflow))
      .rejects.toThrow("checkpoint unavailable");
    expect(store.getRun("atomic-run-start")?.status).toBe("pending");
    expect(store.listEvents("atomic-run-start").map((event) => event.type)).toEqual(["run.created"]);

    await expect(executeAgentFlowCommandPipeline(store, "atomic-run-start", workflow))
      .resolves.toMatchObject({ status: "completed" });
    store.close();
  });

  test("recovers a running execution after a later checkpoint failure", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: recover-failed-checkpoint
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: first, type: command, command: echo first >> effects.txt }
  - { id: second, type: command, command: echo second >> effects.txt }
`);
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(store, { id: "recover-failed-checkpoint", workflow });
    const updateRun = store.updateRun.bind(store);
    let rejectCheckpoint = true;
    store.updateRun = ((runId, input) => {
      const checkpoint = input.context?.executionCheckpoint;
      if (rejectCheckpoint
          && checkpoint !== null
          && typeof checkpoint === "object"
          && !Array.isArray(checkpoint)
          && checkpoint.stepId === "second") {
        rejectCheckpoint = false;
        throw new AgentFlowRunStateError("checkpoint contention", "AGENT_FLOW_CONCURRENT_MUTATION");
      }
      return updateRun(runId, input);
    }) as typeof store.updateRun;

    await expect(executeAgentFlowCommandPipeline(store, "recover-failed-checkpoint", workflow))
      .rejects.toMatchObject({ code: "AGENT_FLOW_CONCURRENT_MUTATION" });
    expect(store.getRun("recover-failed-checkpoint")?.status).toBe("running");
    store.updateRun = updateRun;

    await expect(executeAgentFlowCommandPipeline(store, "recover-failed-checkpoint", workflow))
      .resolves.toMatchObject({ status: "completed", completedSteps: ["first", "second"] });
    expect(fs.readFileSync(path.join(repoRoot, "effects.txt"), "utf8")).toBe("first\nsecond\n");
    store.close();
  });

  test("terminates an in-flight command when lease renewal loses ownership", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: lost-command-lease
version: 1
style: pipeline
maturity: experimental
steps:
  - id: slow
    type: command
    command: sleep 1; echo stale >> effects.txt
`);
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(store, { id: "lost-command-lease", workflow });
    const withRunLock = store.withRunLock.bind(store);
    store.withRunLock = ((runId, operation, callback, options) =>
      withRunLock(runId, operation, callback, { ...options, ttlMs: 30 })) as typeof store.withRunLock;
    store.renewRunLock = (() => {
      throw new AgentFlowRunStateError("lease replaced", "AGENT_FLOW_RUN_LOCK_LOST");
    }) as typeof store.renewRunLock;

    await expect(executeAgentFlowCommandPipeline(store, "lost-command-lease", workflow))
      .rejects.toMatchObject({ code: "AGENT_FLOW_RUN_LOCK_LOST" });
    await Bun.sleep(50);
    expect(fs.existsSync(path.join(repoRoot, "effects.txt"))).toBe(false);
    store.close();
  });

  test("propagates a lost nested recovery lease without settling the parent outcome", async () => {
    const repoRoot = temporaryRepo();
    const parent = parseAgentFlowWorkflowOrThrow(`
name: lost-child-lease-parent
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - id: check
    type: command
    command: exit 1
    on_failure:
      route_to: { workflow: lost-child-lease-child }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const child = parseAgentFlowWorkflowOrThrow(`
name: lost-child-lease-child
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - { id: repair, type: command, command: "sleep 1; echo stale >> effects.txt" }
  - { id: done, type: result, status: remediated }
`);
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(store, { id: "lost-child-lease-parent", workflow: parent });
    const workflows = createAgentFlowWorkflowRegistry().register(child.name, child);
    const childRunId = "lost-child-lease-parent:recovery:check-20f65c28:attempt-1";
    const withRunLock = store.withRunLock.bind(store);
    store.withRunLock = ((runId, operation, callback, options) => withRunLock(
      runId,
      operation,
      callback,
      runId === childRunId ? { ...options, ttlMs: 30 } : options
    )) as typeof store.withRunLock;
    const renewRunLock = store.renewRunLock.bind(store);
    let replaced = false;
    store.renewRunLock = ((lock, ttlMs) => {
      if (lock.runId === childRunId && !replaced) {
        replaced = true;
        const competitor = new Database(store.databasePath);
        competitor.run(
          "UPDATE run_locks SET owner_token = ?, owner_executor_id = ? WHERE run_id = ?",
          ["replacement-owner", "replacement-executor", childRunId]
        );
        competitor.close();
      }
      return renewRunLock(lock, ttlMs);
    }) as typeof store.renewRunLock;

    await expect(executeAgentFlowCommandPipeline(
      store, "lost-child-lease-parent", parent,
      undefined, undefined, undefined, undefined, workflows
    )).rejects.toMatchObject({ code: "AGENT_FLOW_RUN_LOCK_LOST" });

    expect(replaced).toBe(true);
    expect(store.getRun("lost-child-lease-parent")?.status).toBe("running");
    expect(store.getRun(childRunId)?.status).toBe("running");
    expect(store.listEvents("lost-child-lease-parent").some((event) => event.type === "recovery.completed"))
      .toBe(false);
    expect(store.listEvents(childRunId).some((event) => event.type === "run.failed")).toBe(false);
    await Bun.sleep(50);
    expect(fs.existsSync(path.join(repoRoot, "effects.txt"))).toBe(false);
    store.close();
  });

  test("locks nested recovery execution against a competing executor", async () => {
    const repoRoot = temporaryRepo();
    const parent = parseAgentFlowWorkflowOrThrow(`
name: locked-recovery-parent
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - id: check
    type: command
    command: exit 1
    on_failure:
      route_to: { workflow: locked-recovery-child }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const child = parseAgentFlowWorkflowOrThrow(`
name: locked-recovery-child
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - { id: repair, type: command, command: sleep 0.2 }
  - { id: done, type: result, status: remediated }
`);
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    const competitor = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(store, { id: "locked-recovery-parent", workflow: parent });
    const workflows = createAgentFlowWorkflowRegistry().register("locked-recovery-child", child);
    const owner = executeAgentFlowCommandPipeline(
      store, "locked-recovery-parent", parent, undefined, undefined, undefined, undefined, workflows
    );
    const childRunId = "locked-recovery-parent:recovery:check-20f65c28:attempt-1";
    for (let count = 0; count < 100 && store.getRun(childRunId)?.status !== "running"; count += 1) {
      await Bun.sleep(5);
    }

    await expect(executeAgentFlowCommandPipeline(competitor, childRunId, child))
      .rejects.toMatchObject({ code: "AGENT_FLOW_RUN_LOCKED" });
    expect((await owner).status).toBe("completed");
    competitor.close();
    store.close();
  });

  test("propagates an active nested recovery lock without settling the parent outcome", async () => {
    const repoRoot = temporaryRepo();
    const parent = parseAgentFlowWorkflowOrThrow(`
name: contended-recovery-parent
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - id: check
    type: command
    command: exit 1
    on_failure:
      route_to: { workflow: contended-recovery-child }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const child = parseAgentFlowWorkflowOrThrow(`
name: contended-recovery-child
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - { id: repair, type: command, command: sleep 0.1 }
  - { id: done, type: result, status: remediated }
`);
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    const competitor = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(store, { id: "contended-recovery-parent", workflow: parent });
    const workflows = createAgentFlowWorkflowRegistry().register("contended-recovery-child", child);
    const withRunLock = store.withRunLock.bind(store);
    let competitorLock: ReturnType<typeof competitor.acquireRunLock> | undefined;
    store.withRunLock = ((runId, operation, callback, options) => {
      if (runId.includes(":recovery:") && competitorLock === undefined) {
        competitorLock = competitor.acquireRunLock(runId, "run", { ttlMs: 1_000 });
      }
      return withRunLock(runId, operation, callback, options);
    }) as typeof store.withRunLock;

    await expect(executeAgentFlowCommandPipeline(
      store, "contended-recovery-parent", parent, undefined, undefined, undefined, undefined, workflows
    )).rejects.toMatchObject({ code: "AGENT_FLOW_RUN_LOCKED" });
    const childRunId = "contended-recovery-parent:recovery:check-20f65c28:attempt-1";
    expect(store.getRun("contended-recovery-parent")?.status).toBe("running");
    expect(store.getRun(childRunId)?.status).toBe("pending");
    expect(store.listEvents("contended-recovery-parent").some((event) => event.type === "recovery.completed"))
      .toBe(false);
    expect(competitorLock).toBeDefined();
    competitor.releaseRunLock(competitorLock!);
    await expect(executeAgentFlowCommandPipeline(
      store, "contended-recovery-parent", parent, undefined, undefined, undefined, undefined, workflows
    )).resolves.toMatchObject({ status: "completed" });
    expect(store.getRun(childRunId)?.status).toBe("completed");
    competitor.close();
    store.close();
  });

  test("retries nested recovery lock contention without replaying its source step", async () => {
    const repoRoot = temporaryRepo();
    const parent = parseAgentFlowWorkflowOrThrow(`
name: mutation-recovery-parent
version: 1
style: recovery_pipeline
maturity: experimental
limits:
  max_recovery_cycles: 1
  max_step_attempts: { check: 1 }
policies: { recovery_limits: fail }
steps:
  - id: check
    type: command
    command: echo attempted >> effects.txt; exit 1
    on_failure:
      route_to: { workflow: mutation-recovery-child }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const child = parseAgentFlowWorkflowOrThrow(`
name: mutation-recovery-child
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - { id: repair, type: command, command: echo repaired }
  - { id: done, type: result, status: remediated }
`);
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(store, { id: "mutation-parent", workflow: parent });
    const workflows = createAgentFlowWorkflowRegistry().register("mutation-recovery-child", child);
    const withRunLock = store.withRunLock.bind(store);
    let recoveryLockAttempts = 0;
    store.withRunLock = ((runId, operation, callback, options) => {
      if (runId.includes(":recovery:")) {
        recoveryLockAttempts += 1;
        if (recoveryLockAttempts === 1) {
          throw new AgentFlowRunStateError(
            "another state mutation is active",
            "AGENT_FLOW_CONCURRENT_MUTATION"
          );
        }
      }
      return withRunLock(runId, operation, callback, options);
    }) as typeof store.withRunLock;

    await expect(executeAgentFlowCommandPipeline(
      store, "mutation-parent", parent,
      undefined, undefined, undefined, undefined, workflows
    )).resolves.toMatchObject({
      status: "completed"
    });
    const childRunId = "mutation-parent:recovery:check-20f65c28:attempt-1";
    expect(recoveryLockAttempts).toBe(2);
    expect(store.getRun("mutation-parent")?.status).toBe("completed");
    expect(store.getRun(childRunId)?.status).toBe("completed");
    expect(fs.readFileSync(path.join(repoRoot, "effects.txt"), "utf8")).toBe("attempted\n");
    expect(store.listEvents("mutation-parent")).toContainEqual(expect.objectContaining({
      type: "recovery.completed",
      payload: expect.objectContaining({ status: "remediated" })
    }));
    store.close();
  });

  test("retries nested recovery startup contention before a child step begins", async () => {
    const repoRoot = temporaryRepo();
    const parent = parseAgentFlowWorkflowOrThrow(`
name: startup-contention-parent
version: 1
style: recovery_pipeline
maturity: experimental
limits:
  max_recovery_cycles: 1
  max_step_attempts: { check: 1 }
policies: { recovery_limits: fail }
steps:
  - id: check
    type: command
    command: exit 1
    on_failure:
      route_to: { workflow: startup-contention-child }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const child = parseAgentFlowWorkflowOrThrow(`
name: startup-contention-child
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - { id: repair, type: command, command: echo repaired }
  - { id: done, type: result, status: remediated }
`);
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(store, { id: "startup-contention-parent", workflow: parent });
    const workflows = createAgentFlowWorkflowRegistry().register("startup-contention-child", child);
    const withRunStateTransaction = store.withRunStateTransaction.bind(store);
    let startupContentions = 0;
    store.withRunStateTransaction = ((runId, callback) => {
      if (runId.includes(":recovery:") && startupContentions === 0) {
        startupContentions += 1;
        throw new AgentFlowRunStateError(
          "another state mutation is active",
          "AGENT_FLOW_CONCURRENT_MUTATION"
        );
      }
      return withRunStateTransaction(runId, callback);
    }) as typeof store.withRunStateTransaction;

    const result = await executeAgentFlowCommandPipeline(
      store,
      "startup-contention-parent",
      parent,
      undefined,
      undefined,
      undefined,
      undefined,
      workflows
    );

    const childRunId = "startup-contention-parent:recovery:check-20f65c28:attempt-1";
    expect(result).toMatchObject({ status: "completed" });
    expect(startupContentions).toBe(1);
    expect(store.getRun(childRunId)).toMatchObject({ status: "completed", error: null });
    expect(store.listEvents(childRunId).filter((event) => event.type === "step.started")).toHaveLength(1);
    store.close();
  });

  test("reuses the pre-route snapshot when an interrupted recovery changed files outside scope", async () => {
    const repoRoot = temporaryRepo();
    const parent = parseAgentFlowWorkflowOrThrow(`
name: interrupted-recovery-parent
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_recovery_cycles: 1 }
policies:
  recovery_limits: fail
  file_scope: { include: [allowed/**] }
steps:
  - id: check
    type: command
    command: echo source >> effects.txt; exit 1
    on_failure:
      route_to: { workflow: interrupted-recovery-child }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const child = parseAgentFlowWorkflowOrThrow(`
name: interrupted-recovery-child
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - { id: repair, type: command, command: printf child }
  - { id: done, type: result, status: remediated }
`);
    const parentRunId = "interrupted-recovery-parent";
    const childRunId = `${parentRunId}:recovery:check-20f65c28:attempt-1`;
    const interrupted = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(interrupted, { id: parentRunId, workflow: parent });
    interrupted.acquireRunLock(parentRunId, "run", { ttlMs: 60_000 });
    interrupted.transitionRunWithEvent(parentRunId, {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: { status: "running" } }
    });
    interrupted.updateRun(parentRunId, {
      currentStepId: "check",
      context: {
        ...interrupted.getRun(parentRunId)!.context,
        executionRouting: {
          maxRecoveryCycles: 1,
          stepAttemptLimits: {},
          visits: { check: 1 },
          recoveryCycles: {},
          recoveryInvocations: { check: 1 },
          disagreementEpisodes: {},
          disagreementRounds: {},
          attempts: { check: 1 }
        }
      }
    });
    persistAgentFlowFailurePayload(interrupted, {
      id: "command:check:attempt-1",
      runId: parentRunId,
      stepId: "check",
      stepType: "command",
      attempt: 1,
      summary: "failed",
      classification: "command_failure",
      retryable: false,
      outcome: "fail"
    });
    interrupted.upsertStep({
      runId: parentRunId,
      stepId: "check",
      attempt: 1,
      status: "failed",
      error: { attempt: 1, message: "failed", failureId: "command:check:attempt-1" }
    });
    interrupted.appendRunEvent(parentRunId, {
      type: "step.failed",
      stepId: "check",
      payload: { attempt: 1, message: "failed", failureId: "command:check:attempt-1" }
    });
    const workspaceSnapshot = interrupted.writeArtifact({
      id: "recovery-workspace:check-20f65c28:command-check-attempt-1-ee1428cf",
      runId: parentRunId,
      path: "recovery-workspace/check-20f65c28/command-check-attempt-1-ee1428cf.json",
      kind: "recovery_workspace_snapshot",
      contentType: "application/json; charset=utf-8",
      content: `${JSON.stringify({ version: 1, entries: [] })}\n`,
      metadata: { failureId: "command:check:attempt-1", route: "workflow", target: child.name }
    });
    interrupted.appendRunEvent(parentRunId, {
      type: "recovery.routed",
      stepId: "check",
      payload: {
        failureId: "command:check:attempt-1",
        route: "workflow",
        target: child.name,
        workspaceSnapshotPath: workspaceSnapshot.declaredPath,
        workspaceSnapshotChecksum: workspaceSnapshot.checksum
      }
    });
    createAgentFlowLifecycleRun(interrupted, {
      id: childRunId,
      workflow: child,
      inputs: {},
      parentRunId,
      recoveryOfRunId: parentRunId
    });
    interrupted.acquireRunLock(childRunId, "run", { ttlMs: 60_000 });
    interrupted.transitionRunWithEvent(childRunId, {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: { status: "running" } }
    });
    interrupted.updateRun(childRunId, { currentStepId: "repair" });
    interrupted.upsertStep({ runId: childRunId, stepId: "repair", attempt: 1, status: "running" });
    fs.writeFileSync(path.join(repoRoot, "effects.txt"), "source\n");
    interrupted.close();

    const recovered = await openAgentFlowRunState({ cwd: repoRoot });
    const workflows = createAgentFlowWorkflowRegistry().register(child.name, child);
    await expect(executeAgentFlowCommandPipeline(
      recovered, parentRunId, parent,
      undefined, undefined, undefined, undefined, workflows
    )).resolves.toMatchObject({ status: "paused" });
    expect(fs.readFileSync(path.join(repoRoot, "effects.txt"), "utf8")).toBe("source\n");
    expect(recovered.getRun(childRunId)?.status).toBe("completed");
    expect(recovered.latestStepRecoveryState(childRunId, "repair")).toMatchObject({
      attempt: 2,
      status: "completed"
    });
    expect(recovered.listRuns().filter((run) => run.parentRunId === parentRunId).map((run) => run.id))
      .toEqual([childRunId]);
    expect(recovered.listEvents(parentRunId)).toContainEqual(expect.objectContaining({
      type: "recovery.workspace_scope_violated",
      payload: expect.objectContaining({ deniedPaths: ["effects.txt"] })
    }));
    recovered.close();
  });

  test("routes a persisted failure left behind a running cursor before replaying its command", async () => {
    const repoRoot = temporaryRepo();
    const parent = parseAgentFlowWorkflowOrThrow(`
name: unrouted-recovery-parent
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_recovery_cycles: 1 }
policies: { recovery_limits: fail }
steps:
  - id: check
    type: command
    command: echo source >> effects.txt; exit 1
    on_failure:
      route_to: { workflow: unrouted-recovery-child }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const child = parseAgentFlowWorkflowOrThrow(`
name: unrouted-recovery-child
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - { id: repair, type: command, command: printf repaired }
  - { id: done, type: result, status: remediated }
`);
    const runId = "unrouted-recovery-parent";
    const interrupted = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(interrupted, { id: runId, workflow: parent });
    interrupted.acquireRunLock(runId, "run", { ttlMs: 60_000 });
    interrupted.transitionRunWithEvent(runId, {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: { status: "running" } }
    });
    interrupted.updateRun(runId, {
      currentStepId: "check",
      context: {
        ...interrupted.getRun(runId)!.context,
        executionRouting: {
          maxRecoveryCycles: 1,
          stepAttemptLimits: {},
          visits: { check: 1 },
          recoveryCycles: {},
          recoveryInvocations: {},
          disagreementEpisodes: {},
          disagreementRounds: {},
          attempts: { check: 1 }
        }
      }
    });
    persistAgentFlowFailurePayload(interrupted, {
      id: "command:check:attempt-1",
      runId,
      stepId: "check",
      stepType: "command",
      attempt: 1,
      summary: "failed",
      classification: "command_failure",
      retryable: false,
      outcome: "fail"
    });
    interrupted.upsertStep({
      runId,
      stepId: "check",
      attempt: 1,
      status: "running"
    });
    fs.writeFileSync(path.join(repoRoot, "effects.txt"), "source\n");
    interrupted.close();

    const recovered = await openAgentFlowRunState({ cwd: repoRoot });
    const workflows = createAgentFlowWorkflowRegistry().register(child.name, child);
    await expect(executeAgentFlowCommandPipeline(
      recovered, runId, parent, undefined, undefined, undefined, undefined, workflows
    )).resolves.toMatchObject({ status: "completed" });
    expect(fs.readFileSync(path.join(repoRoot, "effects.txt"), "utf8")).toBe("source\n");
    expect(recovered.listEvents(runId)).toContainEqual(expect.objectContaining({
      type: "recovery.routed",
      stepId: "check"
    }));
    recovered.close();
  });

  test("routes a persisted recovery completion without replaying its source step", async () => {
    const repoRoot = temporaryRepo();
    const parent = parseAgentFlowWorkflowOrThrow(`
name: completed-recovery-parent
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_recovery_cycles: 1, max_duration_seconds: 1 }
steps:
  - id: check
    type: command
    command: echo source >> effects.txt; exit 1
    on_failure:
      route_to: { workflow: completed-recovery-child }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const child = parseAgentFlowWorkflowOrThrow(`
name: completed-recovery-child
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - { id: done, type: result, status: remediated }
`);
    const runId = "completed-recovery-parent";
    const failureId = "command:check:attempt-1";
    const interrupted = await openAgentFlowRunState({
      cwd: repoRoot,
      now: () => "2026-08-22T12:00:00.000Z"
    });
    createAgentFlowLifecycleRun(interrupted, { id: runId, workflow: parent });
    interrupted.acquireRunLock(runId, "run", { ttlMs: 60_000 });
    interrupted.transitionRunWithEvent(runId, {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: { status: "running" } }
    });
    interrupted.updateRun(runId, {
      currentStepId: "check",
      context: {
        ...interrupted.getRun(runId)!.context,
        executionRouting: {
          maxRecoveryCycles: 1,
          stepAttemptLimits: {},
          visits: { check: 1 },
          recoveryCycles: {},
          recoveryInvocations: { check: 1 },
          disagreementEpisodes: {},
          disagreementRounds: {},
          attempts: { check: 1 }
        }
      }
    });
    persistAgentFlowFailurePayload(interrupted, {
      id: failureId,
      runId,
      stepId: "check",
      stepType: "command",
      attempt: 1,
      summary: "failed",
      classification: "command_failure",
      retryable: false,
      outcome: "fail"
    });
    interrupted.upsertStep({
      runId,
      stepId: "check",
      attempt: 1,
      status: "failed",
      error: { attempt: 1, message: "failed", failureId }
    });
    interrupted.appendRunEvent(runId, {
      type: "step.failed",
      stepId: "check",
      payload: { attempt: 1, message: "failed", failureId }
    });
    interrupted.updateFailureRecovery(runId, failureId, {
      status: "remediated",
      route: "workflow",
      target: child.name
    });
    interrupted.appendRunEvent(runId, {
      type: "recovery.completed",
      stepId: "check",
      payload: { failureId, status: "remediated", route: "workflow", target: child.name }
    });
    fs.writeFileSync(path.join(repoRoot, "effects.txt"), "source\n");
    interrupted.close();

    const recovered = await openAgentFlowRunState({
      cwd: repoRoot,
      now: () => "2026-08-22T12:00:02.000Z"
    });
    const workflows = createAgentFlowWorkflowRegistry().register(child.name, child);
    await expect(executeAgentFlowCommandPipeline(
      recovered, runId, parent, undefined, undefined, undefined, undefined, workflows
    )).resolves.toMatchObject({ status: "completed" });
    expect(fs.readFileSync(path.join(repoRoot, "effects.txt"), "utf8")).toBe("source\n");
    expect(recovered.listRuns().filter((run) => run.parentRunId === runId)).toHaveLength(0);
    expect(recovered.listEvents(runId).some((event) => event.type === "recovery.limit_reached")).toBe(false);
    recovered.close();
  });

  test("finalizes a persisted failed cursor that has no recovery route", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: failed-cursor-without-route
version: 1
style: pipeline
maturity: experimental
steps:
  - id: check
    type: command
    command: echo source >> effects.txt; exit 1
    on_failure: { then: fail }
`);
    const runId = "failed-cursor-without-route";
    const interrupted = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(interrupted, { id: runId, workflow });
    interrupted.acquireRunLock(runId, "run", { ttlMs: 60_000 });
    interrupted.transitionRunWithEvent(runId, {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: { status: "running" } }
    });
    interrupted.updateRun(runId, {
      currentStepId: "check",
      context: {
        ...interrupted.getRun(runId)!.context,
        executionRouting: {
          stepAttemptLimits: {},
          visits: { check: 1 },
          recoveryCycles: {},
          recoveryInvocations: {},
          disagreementEpisodes: {},
          disagreementRounds: {},
          attempts: { check: 1 }
        }
      }
    });
    persistAgentFlowFailurePayload(interrupted, {
      id: "command:check:attempt-1",
      runId,
      stepId: "check",
      stepType: "command",
      attempt: 1,
      summary: "failed",
      classification: "command_failure",
      retryable: false,
      outcome: "fail"
    });
    interrupted.upsertStep({
      runId,
      stepId: "check",
      attempt: 1,
      status: "failed",
      error: { attempt: 1, message: "failed" }
    });
    interrupted.appendRunEvent(runId, {
      type: "step.failed",
      stepId: "check",
      payload: { attempt: 1, message: "failed" }
    });
    fs.writeFileSync(path.join(repoRoot, "effects.txt"), "source\n");
    interrupted.close();

    const recovered = await openAgentFlowRunState({ cwd: repoRoot });
    await expect(executeAgentFlowCommandPipeline(recovered, runId, workflow)).resolves.toMatchObject({
      status: "failed",
      failedStep: "check",
      message: "failed"
    });
    expect(recovered.getRun(runId)?.status).toBe("failed");
    expect(fs.readFileSync(path.join(repoRoot, "effects.txt"), "utf8")).toBe("source\n");
    recovered.close();
  });

  test("continues an interrupted retry before applying failure routing", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: recovered-retry
version: 1
style: pipeline
maturity: experimental
steps:
  - id: flaky
    type: command
    command: echo recovered >> effects.txt
    on_failure: { retry: 1, then: fail }
`);
    const runId = "recovered-retry";
    const interrupted = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(interrupted, { id: runId, workflow });
    interrupted.acquireRunLock(runId, "run", { ttlMs: 60_000 });
    interrupted.transitionRunWithEvent(runId, {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: { status: "running" } }
    });
    interrupted.updateRun(runId, {
      currentStepId: "flaky",
      context: {
        ...interrupted.getRun(runId)!.context,
        executionRouting: {
          stepAttemptLimits: {},
          visits: { flaky: 1 },
          recoveryCycles: {},
          recoveryInvocations: {},
          disagreementEpisodes: {},
          disagreementRounds: {},
          attempts: { flaky: 1 }
        },
        executionCheckpoint: { stepId: "flaky", visit: 1, completedAttempts: 0 }
      }
    });
    interrupted.recordFailure({
      id: "command:flaky:attempt-1",
      runId,
      stepId: "flaky",
      classification: "command_failure",
      message: "first attempt failed",
      retryable: true,
      payload: { attempt: 1, exitCode: 9, timedOut: false, outcome: "retry" }
    });
    interrupted.upsertStep({
      runId,
      stepId: "flaky",
      attempt: 1,
      status: "running"
    });
    interrupted.close();

    const recovered = await openAgentFlowRunState({ cwd: repoRoot });
    await expect(executeAgentFlowCommandPipeline(recovered, runId, workflow))
      .resolves.toMatchObject({ status: "completed", completedSteps: ["flaky"] });
    expect(fs.readFileSync(path.join(repoRoot, "effects.txt"), "utf8")).toBe("recovered\n");
    expect(recovered.latestStepRecoveryState(runId, "flaky")).toMatchObject({
      attempt: 2,
      status: "completed"
    });
    expect(recovered.getRun(runId)?.context.executionRouting).toMatchObject({
      visits: { flaky: 1 }
    });
    recovered.close();
  });

  test("restarts an interrupted running execution after recovering its stale lease", async () => {
    for (const operation of ["run", "resume"] as const) {
      const repoRoot = temporaryRepo();
      const workflow = parseAgentFlowWorkflowOrThrow(`
name: recovered-${operation}
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: work, type: command, command: echo recovered }
`);
      const runId = `recovered-${operation}`;
      const interrupted = await openAgentFlowRunState({ cwd: repoRoot });
      createAgentFlowLifecycleRun(interrupted, { id: runId, workflow });
      interrupted.acquireRunLock(runId, operation, { ttlMs: 60_000 });
      interrupted.transitionRunWithEvent(runId, {
        status: "running",
        allowedFrom: ["pending"],
        event: { type: "run.started", payload: { status: "running" } }
      });
      interrupted.updateRun(runId, {
        currentStepId: "work",
        context: {
          ...interrupted.getRun(runId)!.context,
          executionRouting: {
            stepAttemptLimits: {},
            visits: { work: 1 },
            recoveryCycles: {},
            recoveryInvocations: {},
            disagreementEpisodes: {},
            disagreementRounds: {},
            attempts: {}
          },
          executionCheckpoint: { stepId: "work", visit: 1, completedAttempts: 0 }
        }
      });
      interrupted.upsertStep({ runId, stepId: "work", attempt: 1, status: "running" });
      interrupted.upsertApproval({
        id: "approval:work:attempt-1",
        runId,
        stepId: "work",
        status: "requested",
        requestedBy: "fixture"
      });
      interrupted.upsertSession({
        id: "reusable",
        runId,
        provider: "fixture",
        status: "waiting",
        externalSessionId: "external-reusable"
      });
      interrupted.upsertSession({
        id: "in-flight",
        runId,
        provider: "fixture",
        status: "running",
        externalSessionId: "external-in-flight"
      });
      interrupted.close();

      const recovered = await openAgentFlowRunState({ cwd: repoRoot });
      const result = operation === "run"
        ? await executeAgentFlowCommandPipeline(recovered, runId, workflow)
        : await resumeAgentFlowCommandPipeline(recovered, runId, workflow, { outcome: "approve" });
      expect(result.status).toBe("completed");
      expect(recovered.listEvents(runId).map((event) => event.type)).toContain("run.execution_recovered");
      expect(recovered.listApprovals(runId)).toEqual([
        expect.objectContaining({
          id: "approval:work:attempt-1",
          status: "cancelled",
          decision: "execution_recovered"
        })
      ]);
      expect(recovered.getSession(runId, "reusable")).toMatchObject({
        status: "waiting",
        externalSessionId: "external-reusable"
      });
      expect(recovered.getSession(runId, "in-flight")).toMatchObject({ status: "waiting" });
      expect(recovered.getRun(runId)?.context.executionRouting).toMatchObject({ visits: { work: 1 } });
      const database = new Database(recovered.databasePath, { readonly: true });
      expect(database.query(
        "SELECT attempt, status FROM run_steps WHERE run_id = ? AND step_id = ? ORDER BY attempt"
      ).all(runId, "work")).toEqual([
        { attempt: 1, status: "cancelled" },
        { attempt: 2, status: "completed" }
      ]);
      database.close();
      recovered.close();
    }
  });

  test("reclaims an interrupted provider session after stale-lock recovery", async () => {
    const repoRoot = temporaryRepo();
    fs.mkdirSync(path.join(repoRoot, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(repoRoot, "prompts", "draft.md"), "Draft a response.\n");
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: recovered-provider-session
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: fixture, resume: true }
steps:
  - id: draft
    type: session_request
    session: writer
    prompt: prompts/draft.md
    inputs: [request.md]
    outputs: [response.md]
`);
    const runId = "recovered-provider-session";
    const interrupted = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(interrupted, { id: runId, workflow });
    interrupted.writeArtifact({
      id: "request",
      runId,
      path: "request.md",
      kind: "fixture",
      contentType: "text/plain",
      content: "Request"
    });
    interrupted.acquireRunLock(runId, "run", { ttlMs: 60_000 });
    interrupted.transitionRunWithEvent(runId, {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: { status: "running" } }
    });
    interrupted.updateRun(runId, { currentStepId: "draft" });
    interrupted.upsertStep({ runId, stepId: "draft", attempt: 1, status: "running" });
    interrupted.upsertSession({
      id: "writer",
      runId,
      stepId: "draft",
      provider: "fixture",
      status: "running",
      externalSessionId: "provider-session",
      state: { resume: true, lastStepId: "draft" }
    });
    interrupted.close();

    const externalIds: Array<string | undefined> = [];
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => {
      externalIds.push(request.externalSessionId);
      return { externalSessionId: "provider-session", outputs: { "response.md": "Recovered" } };
    });
    const recovered = await openAgentFlowRunState({ cwd: repoRoot });
    await expect(executeAgentFlowCommandPipeline(
      recovered, runId, workflow, undefined, providers
    )).resolves.toMatchObject({ status: "completed", completedSteps: ["draft"] });
    expect(externalIds).toEqual(["provider-session"]);
    expect(recovered.getSession(runId, "writer")).toMatchObject({
      status: "waiting",
      externalSessionId: "provider-session"
    });
    expect(recovered.readArtifact(runId, "response.md").content.toString()).toBe("Recovered");
    recovered.close();
  });

  test("preserves routing-cycle counters when recovering an interrupted execution", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: recovered-routing-budget
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_recovery_cycles: 1 }
policies: { recovery_limits: fail }
steps:
  - { id: first, type: command, command: echo first, then: second }
  - { id: second, type: command, command: echo second, then: first }
`);
    const interrupted = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(interrupted, { id: "recovered-routing-budget", workflow });
    interrupted.acquireRunLock("recovered-routing-budget", "run", { ttlMs: 60_000 });
    interrupted.transitionRunWithEvent("recovered-routing-budget", {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: { status: "running" } }
    });
    const context = interrupted.getRun("recovered-routing-budget")!.context;
    interrupted.updateRun("recovered-routing-budget", {
      currentStepId: "first",
      context: {
        ...context,
        executionRouting: {
          maxRecoveryCycles: 1,
          stepAttemptLimits: {},
          visits: { first: 1, second: 1 },
          recoveryCycles: { first: 1 },
          recoveryInvocations: {},
          disagreementEpisodes: {},
          disagreementRounds: {},
          attempts: { first: 1, second: 1 }
        }
      }
    });
    interrupted.upsertStep({
      runId: "recovered-routing-budget",
      stepId: "first",
      attempt: 1,
      status: "running"
    });
    interrupted.close();

    const recovered = await openAgentFlowRunState({ cwd: repoRoot });
    const result = await executeAgentFlowCommandPipeline(recovered, "recovered-routing-budget", workflow);
    expect(result).toMatchObject({
      status: "failed",
      failedStep: "second",
      message: "Step second exceeded limits.max_recovery_cycles 1 while routing to first."
    });
    expect(recovered.listEvents("recovered-routing-budget").map((event) => event.type))
      .toContain("recovery.limit_reached");
    recovered.close();
  });

  test("resumes stale recovery at the interrupted step without replaying completed commands", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: recovered-cursor
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: first, type: command, command: echo first >> effects.txt }
  - { id: second, type: command, command: echo second >> effects.txt }
`);
    const interrupted = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(interrupted, { id: "recovered-cursor", workflow });
    interrupted.acquireRunLock("recovered-cursor", "run", { ttlMs: 60_000 });
    interrupted.transitionRunWithEvent("recovered-cursor", {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: { status: "running" } }
    });
    interrupted.updateRun("recovered-cursor", {
      currentStepId: "second",
      context: {
        ...interrupted.getRun("recovered-cursor")!.context,
        executionRouting: {
          stepAttemptLimits: {},
          visits: { first: 1, second: 1 },
          recoveryCycles: {},
          recoveryInvocations: {},
          disagreementEpisodes: {},
          disagreementRounds: {},
          attempts: { first: 1, second: 1 }
        }
      }
    });
    interrupted.upsertStep({ runId: "recovered-cursor", stepId: "first", attempt: 1, status: "completed" });
    interrupted.appendRunEvent("recovered-cursor", {
      type: "step.completed",
      stepId: "first",
      payload: { attempt: 1 }
    });
    fs.writeFileSync(path.join(repoRoot, "effects.txt"), "first\n");
    interrupted.upsertStep({ runId: "recovered-cursor", stepId: "second", attempt: 1, status: "running" });
    interrupted.close();

    const recovered = await openAgentFlowRunState({ cwd: repoRoot });
    expect(await executeAgentFlowCommandPipeline(recovered, "recovered-cursor", workflow)).toMatchObject({
      status: "completed",
      completedSteps: ["first", "second"]
    });
    expect(fs.readFileSync(path.join(repoRoot, "effects.txt"), "utf8")).toBe("first\nsecond\n");
    recovered.close();
  });

  test("keeps the recovery cursor and visit durable across a second interrupted executor", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: repeated-recovery-cursor
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: first, type: command, command: echo first >> effects.txt }
  - { id: second, type: command, command: echo second >> effects.txt }
`);
    const interrupted = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(interrupted, { id: "repeated-recovery-cursor", workflow });
    interrupted.acquireRunLock("repeated-recovery-cursor", "run", { ttlMs: 60_000 });
    interrupted.transitionRunWithEvent("repeated-recovery-cursor", {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: { status: "running" } }
    });
    interrupted.updateRun("repeated-recovery-cursor", {
      currentStepId: "second",
      context: {
        ...interrupted.getRun("repeated-recovery-cursor")!.context,
        executionRouting: {
          stepAttemptLimits: {},
          visits: { first: 1, second: 1 },
          recoveryCycles: {},
          recoveryInvocations: {},
          disagreementEpisodes: {},
          disagreementRounds: {},
          attempts: { first: 1, second: 1 }
        }
      }
    });
    interrupted.upsertStep({
      runId: "repeated-recovery-cursor",
      stepId: "first",
      attempt: 1,
      status: "completed",
      output: { attempt: 1 }
    });
    interrupted.appendRunEvent("repeated-recovery-cursor", {
      type: "step.completed",
      stepId: "first",
      payload: { attempt: 1 }
    });
    interrupted.upsertStep({
      runId: "repeated-recovery-cursor",
      stepId: "second",
      attempt: 1,
      status: "running"
    });
    fs.writeFileSync(path.join(repoRoot, "effects.txt"), "first\n");
    interrupted.close();

    const firstRecovery = await openAgentFlowRunState({ cwd: repoRoot });
    const recoveredLock = firstRecovery.acquireRunLock("repeated-recovery-cursor", "run", { ttlMs: 60_000 });
    expect(recoveredLock.recoveredStaleLock).toBe(true);
    firstRecovery.recoverInterruptedRun(recoveredLock);
    expect(firstRecovery.getRun("repeated-recovery-cursor")).toMatchObject({
      status: "pending",
      currentStepId: "second"
    });
    firstRecovery.close();

    const recovered = await openAgentFlowRunState({ cwd: repoRoot });
    expect(await executeAgentFlowCommandPipeline(recovered, "repeated-recovery-cursor", workflow)).toMatchObject({
      status: "completed",
      completedSteps: ["first", "second"]
    });
    expect(fs.readFileSync(path.join(repoRoot, "effects.txt"), "utf8")).toBe("first\nsecond\n");
    expect(recovered.getRun("repeated-recovery-cursor")?.context.executionRouting).toMatchObject({
      visits: { first: 1, second: 1 }
    });
    recovered.close();
  });

  test("routes past a completed recovery cursor without replaying its side effects", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: recovered-completed-cursor
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: first, type: command, command: echo first >> effects.txt }
  - { id: second, type: command, command: echo second >> effects.txt }
`);
    const interrupted = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(interrupted, { id: "recovered-completed-cursor", workflow });
    interrupted.acquireRunLock("recovered-completed-cursor", "run", { ttlMs: 60_000 });
    interrupted.transitionRunWithEvent("recovered-completed-cursor", {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: { status: "running" } }
    });
    interrupted.updateRun("recovered-completed-cursor", {
      currentStepId: "first",
      context: {
        ...interrupted.getRun("recovered-completed-cursor")!.context,
        executionRouting: {
          stepAttemptLimits: {},
          visits: { first: 1 },
          recoveryCycles: {},
          recoveryInvocations: {},
          disagreementEpisodes: {},
          disagreementRounds: {},
          attempts: { first: 1 }
        }
      }
    });
    interrupted.upsertStep({
      runId: "recovered-completed-cursor",
      stepId: "first",
      attempt: 1,
      status: "completed",
      output: { attempt: 1 }
    });
    interrupted.appendRunEvent("recovered-completed-cursor", {
      type: "step.completed",
      stepId: "first",
      payload: { attempt: 1 }
    });
    interrupted.appendRunEvent("recovered-completed-cursor", {
      type: "recovery.returned",
      stepId: "first",
      payload: { successfulAttempt: 1 }
    });
    fs.writeFileSync(path.join(repoRoot, "effects.txt"), "first\n");
    interrupted.close();

    const recovered = await openAgentFlowRunState({ cwd: repoRoot });
    expect(await executeAgentFlowCommandPipeline(recovered, "recovered-completed-cursor", workflow)).toMatchObject({
      status: "completed",
      completedSteps: ["first", "second"]
    });
    expect(fs.readFileSync(path.join(repoRoot, "effects.txt"), "utf8")).toBe("first\nsecond\n");
    expect(recovered.listEvents("recovered-completed-cursor").filter((event) =>
      event.type === "step.completed" && event.stepId === "first"
    )).toHaveLength(1);
    recovered.close();
  });

  test("executes a checkpointed revisit instead of routing an earlier completion", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: checkpointed-revisit
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - { id: work, type: command, command: echo work >> effects.txt }
  - { id: done, type: result, status: completed }
`);
    const runId = "checkpointed-revisit";
    const interrupted = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(interrupted, { id: runId, workflow });
    interrupted.acquireRunLock(runId, "run", { ttlMs: 60_000 });
    interrupted.transitionRunWithEvent(runId, {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: { status: "running" } }
    });
    interrupted.updateRun(runId, {
      currentStepId: "work",
      context: {
        ...interrupted.getRun(runId)!.context,
        executionRouting: {
          stepAttemptLimits: {},
          visits: { work: 2 },
          recoveryCycles: {},
          recoveryInvocations: {},
          disagreementEpisodes: {},
          disagreementRounds: {},
          attempts: { work: 1 }
        },
        executionCheckpoint: { stepId: "work", visit: 2, completedAttempts: 1 }
      }
    });
    interrupted.upsertStep({
      runId,
      stepId: "work",
      attempt: 1,
      status: "completed",
      output: { attempt: 1, exitCode: 0, timedOut: false, signal: null }
    });
    interrupted.appendRunEvent(runId, {
      type: "step.completed",
      stepId: "work",
      payload: { attempt: 1, exitCode: 0, timedOut: false, signal: null }
    });
    fs.writeFileSync(path.join(repoRoot, "effects.txt"), "work\n");
    interrupted.close();

    const recovered = await openAgentFlowRunState({ cwd: repoRoot });
    await expect(executeAgentFlowCommandPipeline(recovered, runId, workflow))
      .resolves.toMatchObject({ status: "completed" });
    expect(fs.readFileSync(path.join(repoRoot, "effects.txt"), "utf8")).toBe("work\nwork\n");
    expect(recovered.latestStepRecoveryState(runId, "work")).toMatchObject({
      attempt: 2,
      status: "completed"
    });
    expect(recovered.getRun(runId)?.context.executionRouting).toMatchObject({
      visits: { work: 2 }
    });
    recovered.close();
  });

  test("recovers a checkpoint committed before the first attempt row without recounting the visit", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: checkpoint-before-attempt
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: work, type: command, command: "true" }
`);
    const runId = "checkpoint-before-attempt";
    const interrupted = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(interrupted, { id: runId, workflow });
    interrupted.acquireRunLock(runId, "run", { ttlMs: 60_000 });
    interrupted.transitionRunWithEvent(runId, {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: { status: "running" } }
    });
    interrupted.updateRun(runId, {
      currentStepId: "work",
      context: {
        ...interrupted.getRun(runId)!.context,
        executionRouting: {
          stepAttemptLimits: {},
          visits: { work: 1 },
          recoveryCycles: {},
          recoveryInvocations: {},
          disagreementEpisodes: {},
          disagreementRounds: {},
          attempts: {}
        },
        executionCheckpoint: { stepId: "work", visit: 1, completedAttempts: 0 }
      }
    });
    interrupted.close();

    const recovered = await openAgentFlowRunState({ cwd: repoRoot });
    await expect(executeAgentFlowCommandPipeline(recovered, runId, workflow))
      .resolves.toMatchObject({ status: "completed", completedSteps: ["work"] });
    expect(recovered.latestStepRecoveryState(runId, "work")).toMatchObject({
      attempt: 1,
      status: "completed"
    });
    expect(recovered.getRun(runId)?.context.executionRouting).toMatchObject({
      visits: { work: 1 }
    });
    recovered.close();
  });

  test("repairs a missing completion event for the latest revisited attempt", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: repaired-latest-completion
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - { id: work, type: command, command: echo replay >> effects.txt }
  - { id: done, type: result, status: completed }
`);
    const runId = "repaired-latest-completion";
    const interrupted = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(interrupted, { id: runId, workflow });
    interrupted.acquireRunLock(runId, "run", { ttlMs: 60_000 });
    interrupted.transitionRunWithEvent(runId, {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: { status: "running" } }
    });
    interrupted.updateRun(runId, {
      currentStepId: "work",
      context: {
        ...interrupted.getRun(runId)!.context,
        executionRouting: {
          stepAttemptLimits: {},
          visits: { work: 2 },
          recoveryCycles: {},
          recoveryInvocations: {},
          disagreementEpisodes: {},
          disagreementRounds: {},
          attempts: { work: 2 }
        },
        executionCheckpoint: { stepId: "work", visit: 2, completedAttempts: 1 }
      }
    });
    for (const attempt of [1, 2]) {
      const output = { attempt, exitCode: 0, timedOut: false, signal: null };
      interrupted.upsertStep({ runId, stepId: "work", attempt, status: "completed", output });
      if (attempt === 1) {
        interrupted.appendRunEvent(runId, { type: "step.completed", stepId: "work", payload: output });
      }
    }
    fs.writeFileSync(path.join(repoRoot, "effects.txt"), "work\nwork\n");
    interrupted.close();

    const recovered = await openAgentFlowRunState({ cwd: repoRoot });
    await expect(executeAgentFlowCommandPipeline(recovered, runId, workflow))
      .resolves.toMatchObject({ status: "completed", completedSteps: ["work", "work", "done"] });
    expect(fs.readFileSync(path.join(repoRoot, "effects.txt"), "utf8")).toBe("work\nwork\n");
    expect(recovered.listEvents(runId).filter((event) =>
      event.type === "step.completed" && event.stepId === "work"
    ).map((event) => (event.payload as { attempt: number }).attempt)).toEqual([1, 2]);
    recovered.close();
  });

  test("executes a checkpointed revisit instead of routing an earlier failure", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: checkpointed-failed-revisit
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - id: work
    type: command
    command: echo work >> effects.txt
    on_failure: { then: continue, allowed: true }
  - { id: done, type: result, status: completed }
`);
    const runId = "checkpointed-failed-revisit";
    const interrupted = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(interrupted, { id: runId, workflow });
    interrupted.acquireRunLock(runId, "run", { ttlMs: 60_000 });
    interrupted.transitionRunWithEvent(runId, {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: { status: "running" } }
    });
    interrupted.updateRun(runId, {
      currentStepId: "work",
      context: {
        ...interrupted.getRun(runId)!.context,
        executionRouting: {
          stepAttemptLimits: {},
          visits: { work: 2 },
          recoveryCycles: {},
          recoveryInvocations: {},
          disagreementEpisodes: {},
          disagreementRounds: {},
          attempts: { work: 1 }
        },
        executionCheckpoint: { stepId: "work", visit: 2, completedAttempts: 1 }
      }
    });
    interrupted.recordFailure({
      id: "command:work:attempt-1",
      runId,
      stepId: "work",
      classification: "command_failure",
      message: "first attempt failed",
      retryable: false,
      payload: { attempt: 1, exitCode: 9, timedOut: false, outcome: "continue" }
    });
    interrupted.upsertStep({
      runId,
      stepId: "work",
      attempt: 1,
      status: "failed",
      error: { attempt: 1, exitCode: 9, timedOut: false, outcome: "continue" }
    });
    interrupted.appendRunEvent(runId, {
      type: "step.failed",
      stepId: "work",
      payload: { attempt: 1, exitCode: 9, timedOut: false, outcome: "continue" }
    });
    interrupted.close();

    const recovered = await openAgentFlowRunState({ cwd: repoRoot });
    await expect(executeAgentFlowCommandPipeline(recovered, runId, workflow))
      .resolves.toMatchObject({ status: "completed", completedSteps: ["work", "done"] });
    expect(fs.readFileSync(path.join(repoRoot, "effects.txt"), "utf8")).toBe("work\n");
    expect(recovered.latestStepRecoveryState(runId, "work")).toMatchObject({
      attempt: 2,
      status: "completed"
    });
    recovered.close();
  });

  test("settles returned recovery evidence before routing a completed cursor", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: recovered-returned-evidence
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - { id: work, type: command, command: echo work >> effects.txt }
  - { id: next, type: command, command: echo next >> effects.txt }
`);
    const runId = "recovered-returned-evidence";
    const interrupted = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(interrupted, { id: runId, workflow });
    interrupted.acquireRunLock(runId, "run", { ttlMs: 60_000 });
    interrupted.transitionRunWithEvent(runId, {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: { status: "running" } }
    });
    interrupted.updateRun(runId, {
      currentStepId: "work",
      context: {
        ...interrupted.getRun(runId)!.context,
        executionRouting: {
          stepAttemptLimits: {},
          visits: { work: 2 },
          recoveryCycles: {},
          recoveryInvocations: { work: 1 },
          disagreementEpisodes: {},
          disagreementRounds: {},
          attempts: { work: 2 }
        }
      }
    });
    interrupted.recordFailure({
      id: "failure-1",
      runId,
      stepId: "work",
      classification: "command_failure",
      message: "failed",
      payload: {
        attempt: 1,
        outcome: "retry",
        recovery: { status: "remediated", route: "session", target: "fix" }
      }
    });
    interrupted.upsertStep({ runId, stepId: "work", attempt: 1, status: "failed", error: { attempt: 1 } });
    interrupted.appendRunEvent(runId, { type: "step.failed", stepId: "work", payload: { attempt: 1 } });
    interrupted.upsertStep({
      runId,
      stepId: "work",
      attempt: 2,
      status: "completed",
      output: { attempt: 2, exitCode: 0, timedOut: false, signal: null }
    });
    interrupted.appendRunEvent(runId, {
      type: "step.completed",
      stepId: "work",
      payload: { attempt: 2, exitCode: 0, timedOut: false, signal: null }
    });
    fs.writeFileSync(path.join(repoRoot, "effects.txt"), "work\n");
    interrupted.close();

    const recovered = await openAgentFlowRunState({ cwd: repoRoot });
    await expect(executeAgentFlowCommandPipeline(recovered, runId, workflow))
      .resolves.toMatchObject({ status: "completed" });
    expect(recovered.listFailures(runId)[0]?.resolvedAt).not.toBeNull();
    expect(recovered.listEvents(runId)).toContainEqual(expect.objectContaining({
      type: "recovery.returned",
      stepId: "work",
      payload: expect.objectContaining({ failureId: "failure-1", successfulAttempt: 2 })
    }));
    expect(fs.readFileSync(path.join(repoRoot, "effects.txt"), "utf8")).toBe("work\nnext\n");
    recovered.close();
  });

  test("routes past a completed recovery row when its completion event was not persisted", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: recovered-completed-row
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: first, type: command, command: echo first >> effects.txt }
  - { id: second, type: command, command: echo second >> effects.txt }
`);
    const interrupted = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(interrupted, { id: "recovered-completed-row", workflow });
    interrupted.acquireRunLock("recovered-completed-row", "run", { ttlMs: 60_000 });
    interrupted.transitionRunWithEvent("recovered-completed-row", {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: { status: "running" } }
    });
    interrupted.updateRun("recovered-completed-row", {
      currentStepId: "first",
      context: {
        ...interrupted.getRun("recovered-completed-row")!.context,
        executionRouting: {
          stepAttemptLimits: {},
          visits: { first: 1 },
          recoveryCycles: {},
          recoveryInvocations: {},
          disagreementEpisodes: {},
          disagreementRounds: {},
          attempts: { first: 1 }
        }
      }
    });
    interrupted.upsertStep({
      runId: "recovered-completed-row",
      stepId: "first",
      attempt: 1,
      status: "completed",
      output: { attempt: 1 }
    });
    fs.writeFileSync(path.join(repoRoot, "effects.txt"), "first\n");
    interrupted.close();

    const recovered = await openAgentFlowRunState({ cwd: repoRoot });
    expect(await executeAgentFlowCommandPipeline(recovered, "recovered-completed-row", workflow)).toMatchObject({
      status: "completed",
      completedSteps: ["first", "second"]
    });
    expect(fs.readFileSync(path.join(repoRoot, "effects.txt"), "utf8")).toBe("first\nsecond\n");
    expect(recovered.listEvents("recovered-completed-row").filter((event) =>
      event.type === "step.completed" && event.stepId === "first"
    )).toHaveLength(1);
    recovered.close();
  });

  test("recovers an approved route from the atomic completion event", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: recovered-approval-route
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true }
sessions:
  reviewer: { provider: fixture, role: reviewer, authority: { can_approve: true } }
steps:
  - { id: approve, type: approval, reviewer: reviewer, artifacts: [evidence.md], on_approve: accepted, on_reject: rejected }
  - { id: accepted, type: command, command: echo accepted >> route.txt, then: complete }
  - { id: rejected, type: command, command: echo rejected >> route.txt, then: complete }
`);
    const interrupted = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(interrupted, { id: "recovered-approval-route", workflow });
    interrupted.writeArtifact({
      id: "evidence",
      runId: "recovered-approval-route",
      path: "evidence.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Evidence"
    });
    interrupted.acquireRunLock("recovered-approval-route", "run", { ttlMs: 60_000 });
    interrupted.transitionRunWithEvent("recovered-approval-route", {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: { status: "running" } }
    });
    interrupted.updateRun("recovered-approval-route", {
      currentStepId: "approve",
      context: {
        ...interrupted.getRun("recovered-approval-route")!.context,
        executionRouting: {
          maxReviewCycles: 1,
          stepAttemptLimits: {},
          visits: { approve: 1 },
          recoveryCycles: {},
          recoveryInvocations: {},
          disagreementEpisodes: {},
          disagreementRounds: {},
          attempts: { approve: 1 }
        }
      }
    });
    interrupted.upsertApproval({
      id: "approval:approve:attempt-1",
      runId: "recovered-approval-route",
      stepId: "approve",
      status: "requested",
      requestedBy: "reviewer"
    });
    interrupted.upsertStep({
      runId: "recovered-approval-route",
      stepId: "approve",
      attempt: 1,
      status: "completed",
      output: { attempt: 1, approvalStatus: "approved" }
    });
    interrupted.appendRunEvent("recovered-approval-route", {
      type: "step.completed",
      stepId: "approve",
      payload: { attempt: 1, approvalStatus: "approved" }
    });
    interrupted.close();

    const recovered = await openAgentFlowRunState({ cwd: repoRoot });
    expect(await executeAgentFlowCommandPipeline(recovered, "recovered-approval-route", workflow))
      .toMatchObject({ status: "completed", completedSteps: ["approve", "accepted"] });
    expect(fs.readFileSync(path.join(repoRoot, "route.txt"), "utf8")).toBe("accepted\n");
    recovered.close();
  });

  test("recovers a cancelled human approval through its declared cancellation route", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: recovered-human-cancellation
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: approve, type: approval, reviewer: human, artifacts: [evidence.md], on_reject: rejected, on_cancel: cancelled }
  - { id: rejected, type: command, command: echo rejected >> route.txt, then: complete }
  - { id: cancelled, type: command, command: echo cancelled >> route.txt, then: complete }
`);
    const interrupted = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(interrupted, { id: "recovered-human-cancellation", workflow });
    interrupted.writeArtifact({
      id: "evidence",
      runId: "recovered-human-cancellation",
      path: "evidence.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Evidence"
    });
    interrupted.acquireRunLock("recovered-human-cancellation", "resume", { ttlMs: 60_000 });
    interrupted.transitionRunWithEvent("recovered-human-cancellation", {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: { status: "running" } }
    });
    interrupted.updateRun("recovered-human-cancellation", {
      currentStepId: "approve",
      context: {
        ...interrupted.getRun("recovered-human-cancellation")!.context,
        executionRouting: {
          stepAttemptLimits: {},
          visits: { approve: 1 },
          recoveryCycles: {},
          recoveryInvocations: {},
          disagreementEpisodes: {},
          disagreementRounds: {},
          attempts: { approve: 1 }
        }
      }
    });
    interrupted.upsertApproval({
      id: "approval:approve:attempt-1",
      runId: "recovered-human-cancellation",
      stepId: "approve",
      status: "cancelled",
      decidedBy: "human",
      decision: "cancel"
    });
    interrupted.upsertStep({
      runId: "recovered-human-cancellation",
      stepId: "approve",
      attempt: 1,
      status: "completed",
      output: { attempt: 1, outcome: "cancel" }
    });
    interrupted.appendRunEvent("recovered-human-cancellation", {
      type: "step.completed",
      stepId: "approve",
      payload: { attempt: 1, outcome: "cancel" }
    });
    interrupted.close();

    const recovered = await openAgentFlowRunState({ cwd: repoRoot });
    expect(await executeAgentFlowCommandPipeline(recovered, "recovered-human-cancellation", workflow)).toMatchObject({
      status: "completed",
      completedSteps: ["approve", "cancelled"]
    });
    expect(fs.readFileSync(path.join(repoRoot, "route.txt"), "utf8")).toBe("cancelled\n");
    recovered.close();
  });

  test("fails closed when completed blocking-consult evidence cannot be recovered", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: recovered-blocking-consult
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true }
sessions:
  implementer: { provider: fixture, role: implementer }
  designer: { provider: fixture, role: designer, authority: { can_block: true } }
steps:
  - id: consult
    type: consult
    from: implementer
    to: designer
    question: Is this safe?
    artifacts: [evidence.md]
    output: consultations/design.json
    blocking: true
  - { id: continue, type: command, command: echo continued >> effects.txt }
`);
    const interrupted = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(interrupted, { id: "recovered-blocking-consult", workflow });
    interrupted.writeArtifact({
      id: "evidence",
      runId: "recovered-blocking-consult",
      path: "evidence.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Evidence"
    });
    interrupted.acquireRunLock("recovered-blocking-consult", "run", { ttlMs: 60_000 });
    interrupted.transitionRunWithEvent("recovered-blocking-consult", {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: { status: "running" } }
    });
    interrupted.updateRun("recovered-blocking-consult", {
      currentStepId: "consult",
      context: {
        ...interrupted.getRun("recovered-blocking-consult")!.context,
        executionRouting: {
          stepAttemptLimits: {},
          visits: { consult: 1 },
          recoveryCycles: {},
          recoveryInvocations: {},
          disagreementEpisodes: {},
          disagreementRounds: {},
          attempts: { consult: 1 }
        }
      }
    });
    interrupted.upsertStep({
      runId: "recovered-blocking-consult",
      stepId: "consult",
      attempt: 1,
      status: "completed",
      output: { attempt: 1, outputs: ["consultations/design.json"] }
    });
    interrupted.appendRunEvent("recovered-blocking-consult", {
      type: "step.completed",
      stepId: "consult",
      payload: { attempt: 1, outputs: ["consultations/design.json"] }
    });
    interrupted.close();

    const recovered = await openAgentFlowRunState({ cwd: repoRoot });
    await expect(executeAgentFlowCommandPipeline(recovered, "recovered-blocking-consult", workflow))
      .resolves.toMatchObject({
        status: "paused",
        message: expect.stringContaining("blocking evidence is unavailable")
      });
    expect(recovered.getRun("recovered-blocking-consult")?.status).toBe("paused");
    expect(fs.existsSync(path.join(repoRoot, "effects.txt"))).toBe(false);
    recovered.close();
  });

  test("runs a safe command pipeline and persists logs, declared artifacts, and completion state", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: safe-ci
version: 1
style: pipeline
maturity: experimental
steps:
  - id: check
    type: command
    command: mkdir -p ci && printf 'artifact output\\n' > ci/result.txt && printf 'standard output\\n' && printf 'standard error\\n' >&2
    timeout_seconds: 5
    outputs:
      - ci/result.txt
`);
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(store, { id: "run-safe", workflow });

    const result = await executeAgentFlowCommandPipeline(store, "run-safe", workflow);

    expect(result).toMatchObject({ status: "completed", completedSteps: ["check"] });
    expect(store.getRun("run-safe")).toMatchObject({ status: "completed", currentStepId: null });
    expect(store.listEvents("run-safe").map((event) => event.type)).toEqual([
      "run.created",
      "run.started",
      "step.started",
      "step.completed",
      "run.completed"
    ]);
    const artifacts = store.listArtifacts("run-safe");
    expect(artifacts.map((artifact) => artifact.declaredPath)).toHaveLength(4);
    expect(artifacts).toContainEqual(expect.objectContaining({
      declaredPath: "final-summary.md",
      kind: "run_summary",
      status: "available"
    }));
    expect(artifacts.map((artifact) => artifact.declaredPath)).toContain("ci/result.txt");
    expect(readArtifact(repoRoot, artifacts.find((artifact) => artifact.declaredPath === "ci/result.txt")!.storagePath))
      .toBe("artifact output\n");
    expect(readArtifact(repoRoot, artifacts.find((artifact) => artifact.declaredPath.endsWith("stdout.log"))!.storagePath))
      .toBe("standard output\n");
    expect(readArtifact(repoRoot, artifacts.find((artifact) => artifact.declaredPath.endsWith("stderr.log"))!.storagePath))
      .toBe("standard error\n");
    store.close();
  });

  test("records failed commands with exit status and captured logs", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: failing-ci
version: 1
style: pipeline
maturity: experimental
steps:
  - id: test
    type: command
    command: printf 'failure details\\n' >&2; exit 23
    on_failure:
      then: fail
`);
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(store, { id: "run-failed", workflow });

    const result = await executeAgentFlowCommandPipeline(store, "run-failed", workflow);

    expect(result).toMatchObject({
      status: "failed",
      failedStep: "test",
      failureOutcome: "fail",
      exitCode: 23,
      timedOut: false
    });
    expect(store.getRun("run-failed")).toMatchObject({
      status: "failed",
      currentStepId: "test",
      error: { exitCode: 23, timedOut: false, outcome: "fail" }
    });
    const failure = store.listFailures("run-failed")[0]!;
    const failurePath = failure.payloadPath;
    expect(failure).toMatchObject({
      stepId: "test",
      classification: "command_failure",
      retryable: false,
      payloadPath: expect.stringMatching(/^failures\/.+\.json$/),
      payload: { attempt: 1, exitCode: 23, timedOut: false, outcome: "fail" }
    });
    expect(failurePath).toMatch(/^failures\/.+\.json$/);
    const failurePayload = JSON.parse(store.readArtifact("run-failed", failurePath!).content.toString("utf8"));
    expect(failurePayload).toMatchObject({
      id: failure.id,
      step_id: "test",
      step_type: "command",
      status: "failed",
      attempt: 1,
      exit_code: 23,
      command: "printf 'failure details\\n' >&2; exit 23",
      summary: "Command exited with status 23.",
      logs: {
        stdout: expect.stringMatching(/stdout\.log$/),
        stderr: expect.stringMatching(/stderr\.log$/)
      },
      classification: "command_failure",
      remediation_status: null,
      path: failurePath,
      redactions: { applied: false, marker: "[REDACTED]" }
    });
    expect(store.listEvents("run-failed").map((event) => event.type)).toContain("step.failed");
    const stderr = store.listArtifacts("run-failed").find((artifact) => artifact.declaredPath.endsWith("stderr.log"))!;
    expect(readArtifact(repoRoot, stderr.storagePath)).toBe("failure details\n");
    store.close();
  });

  test("retries failed commands and persists each attempt", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: retry-ci
version: 1
style: pipeline
maturity: experimental
steps:
  - id: flaky
    type: command
    command: if [ -f .attempted ]; then printf 'recovered\\n'; else touch .attempted; printf 'try again\\n' >&2; exit 9; fi
    on_failure:
      retry: 1
      then: fail
`);
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(store, { id: "run-retry", workflow });

    const result = await executeAgentFlowCommandPipeline(store, "run-retry", workflow);

    expect(result).toMatchObject({ status: "completed", completedSteps: ["flaky"] });
    expect(store.listEvents("run-retry").map((event) => event.type)).toEqual([
      "run.created",
      "run.started",
      "step.started",
      "step.failed",
      "step.started",
      "step.completed",
      "run.completed"
    ]);
    expect(store.listArtifacts("run-retry").filter((artifact) => artifact.kind === "command_log")).toHaveLength(4);
    expect(store.listFailures("run-retry")).toMatchObject([{
      stepId: "flaky",
      retryable: true,
      payload: { attempt: 1, outcome: "retry" }
    }]);
    store.close();
  });

  test("persists retry exhaustion and allowed continuation as explicit outcomes", async () => {
    const repoRoot = temporaryRepo();
    const pausedWorkflow = parseAgentFlowWorkflowOrThrow(`
name: exhausted-retry
version: 1
style: pipeline
maturity: experimental
steps:
  - id: required
    type: command
    command: exit 17
    on_failure: { retry: 1 }
`);
    const pausedStore = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(pausedStore, { id: "exhausted-retry", workflow: pausedWorkflow });

    const paused = await executeAgentFlowCommandPipeline(pausedStore, "exhausted-retry", pausedWorkflow);

    expect(paused).toMatchObject({ status: "paused", failedStep: "required", failureOutcome: "pause" });
    expect(pausedStore.listFailures("exhausted-retry").map((failure) => ({
      retryable: failure.retryable,
      attempt: failure.attempt,
      outcome: failure.outcome
    }))).toEqual([
      { retryable: true, attempt: 1, outcome: "retry" },
      { retryable: false, attempt: 2, outcome: "pause" }
    ]);
    pausedStore.close();

    const continuedWorkflow = parseAgentFlowWorkflowOrThrow(`
name: optional-step
version: 1
style: pipeline
maturity: experimental
steps:
  - id: optional
    type: command
    command: exit 5
    on_failure: { then: continue, allowed: true, reason: Optional check }
  - id: after
    type: command
    command: printf done > done.txt
    outputs: [done.txt]
`);
    const continuedStore = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(continuedStore, { id: "continued-failure", workflow: continuedWorkflow });

    const continued = await executeAgentFlowCommandPipeline(
      continuedStore,
      "continued-failure",
      continuedWorkflow
    );

    expect(continued).toMatchObject({ status: "completed", completedSteps: ["after"] });
    expect(continuedStore.listFailures("continued-failure")).toMatchObject([{
      stepId: "optional",
      retryable: false,
      payload: { attempt: 1, outcome: "continue" }
    }]);
    continuedStore.close();
  });

  test("terminates commands after timeout_seconds and pauses by default", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: timed-ci
version: 1
style: pipeline
maturity: experimental
steps:
  - id: wait
    type: command
    command: trap '' TERM; sleep 2
    timeout_seconds: 0.05
`);
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(store, { id: "run-timeout", workflow });

    const result = await executeAgentFlowCommandPipeline(store, "run-timeout", workflow);

    expect(result).toMatchObject({
      status: "paused",
      failedStep: "wait",
      failureOutcome: "pause",
      timedOut: true
    });
    expect(store.getRun("run-timeout")).toMatchObject({
      status: "paused",
      error: { outcome: "pause" }
    });
    expect(store.listEvents("run-timeout").map((event) => event.type)).toContain("step.timed_out");
    store.close();
  });

  test("does not start later commands after concurrent cancellation", async () => {
    const repoRoot = temporaryRepo();
    const marker = path.join(repoRoot, "second-step-started");
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: cancelled-run
version: 1
style: pipeline
maturity: experimental
steps:
  - id: wait
    type: command
    command: printf 'before cancellation\\n'; sleep 2
  - id: mutate
    type: command
    command: touch second-step-started
retention:
  on_cancelled:
    delete: [logs/**]
    after_days: 7
`);
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(store, { id: "run-cancelled", workflow });

    const startedAt = Date.now();
    const execution = executeAgentFlowCommandPipeline(store, "run-cancelled", workflow);
    setTimeout(() => transitionAgentFlowLifecycleRun(store, "run-cancelled", "cancel"), 25);
    const result = await execution;

    expect(result.status).toBe("cancelled");
    expect(Date.now() - startedAt).toBeLessThan(1_000);
    expect(fs.existsSync(marker)).toBe(false);
    expect(store.listEvents("run-cancelled").map((event) => event.type)).toContain("step.interrupted");
    expect(store.listEvents("run-cancelled").filter((event) => event.type === "retention.deferred"))
      .toHaveLength(1);
    expect(store.getArtifact("run-cancelled", "final-summary.md")?.generation).toBe(1);
    const stdout = store.listArtifacts("run-cancelled").find((artifact) => artifact.declaredPath.endsWith("stdout.log"))!;
    expect(readArtifact(repoRoot, stdout.storagePath)).toBe("before cancellation\n");
    store.close();
  });

  test("does not start later commands after concurrent pause", async () => {
    const repoRoot = temporaryRepo();
    const marker = path.join(repoRoot, "second-step-started");
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: paused-run
version: 1
style: pipeline
maturity: experimental
steps:
  - id: wait
    type: command
    command: printf 'before pause\\n'; sleep 0.1
  - id: mutate
    type: command
    command: touch second-step-started
`);
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(store, { id: "run-paused", workflow });

    const execution = executeAgentFlowCommandPipeline(store, "run-paused", workflow);
    setTimeout(() => transitionAgentFlowLifecycleRun(store, "run-paused", "pause"), 25);
    const result = await execution;

    expect(result.status).toBe("paused");
    expect(fs.existsSync(marker)).toBe(false);
    const stdout = store.listArtifacts("run-paused").find((artifact) => artifact.declaredPath.endsWith("stdout.log"))!;
    expect(readArtifact(repoRoot, stdout.storagePath)).toBe("before pause\n");
    store.close();
  });

  test("preserves failed status when a running command is interrupted by terminal finalization", async () => {
    const repoRoot = temporaryRepo();
    const marker = path.join(repoRoot, "second-step-started");
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: failed-run
version: 1
style: pipeline
maturity: experimental
steps:
  - id: wait
    type: command
    command: sleep 2
  - id: mutate
    type: command
    command: touch second-step-started
`);
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(store, { id: "run-failed", workflow });

    const startedAt = Date.now();
    const execution = executeAgentFlowCommandPipeline(store, "run-failed", workflow);
    setTimeout(() => {
      store.updateRun("run-failed", {
        error: {
          code: "notification.required.failed",
          message: "Required lifecycle notification failed."
        }
      });
      store.transitionRunWithEvent("run-failed", {
        status: "failed",
        allowedFrom: ["running"],
        event: {
          type: "run.failed",
          payload: { code: "notification.required.failed" }
        }
      });
    }, 25);
    const result = await execution;

    expect(result).toMatchObject({
      status: "failed",
      message: "Required lifecycle notification failed."
    });
    expect(Date.now() - startedAt).toBeLessThan(1_000);
    expect(fs.existsSync(marker)).toBe(false);
    expect(store.listEvents("run-failed")).toContainEqual(expect.objectContaining({
      type: "step.interrupted",
      payload: expect.objectContaining({ status: "failed" })
    }));
    store.close();
  });

  test("pauses unsafe commands that require approval before starting them", async () => {
    const repoRoot = temporaryRepo();
    fs.mkdirSync(path.join(repoRoot, "protected"));
    fs.writeFileSync(path.join(repoRoot, "protected/keep.txt"), "keep\n");
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: approval-ci
version: 1
style: pipeline
maturity: experimental
policies:
  unsafe_operations: require_approval
steps:
  - id: erase
    type: command
    command: rm -rf .
`);
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(store, { id: "run-approval", workflow });

    const result = await executeAgentFlowCommandPipeline(store, "run-approval", workflow);

    expect(result).toMatchObject({ status: "paused", failedStep: "erase" });
    expect(result.message).toContain("Approval is required");
    expect(fs.existsSync(path.join(repoRoot, "protected/keep.txt"))).toBe(true);
    expect(store.listEvents("run-approval").map((event) => event.type)).toContain("step.rejected");
    store.close();
  });

  test("fails closed for configured file scopes before starting an unrestricted shell", async () => {
    const repoRoot = temporaryRepo();
    const marker = path.join(repoRoot, "command-started");
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: scoped-ci
version: 1
style: pipeline
maturity: experimental
policies:
  file_scope:
    include: [allowed/**]
steps:
  - id: denied
    type: command
    command: touch command-started
    outputs: [denied/result.txt]
`);
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(store, { id: "run-scope", workflow });

    const result = await executeAgentFlowCommandPipeline(store, "run-scope", workflow);

    expect(result).toMatchObject({ status: "failed", failedStep: "denied" });
    expect(result.message).toContain("cannot confine arbitrary shell writes");
    expect(fs.existsSync(marker)).toBe(false);
    store.close();
  });

  test("persists a terminal failure when declared artifact publication fails", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: colliding-artifact
version: 1
style: pipeline
maturity: experimental
steps:
  - id: check
    type: command
    command: mkdir -p ci && printf declared > ci/result.txt
    outputs: [ci/result.txt]
`);
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(store, { id: "run-collision", workflow });
    const writeArtifact = store.writeArtifact.bind(store);
    store.writeArtifact = (input) => {
      if (input.kind === "command_output") throw new Error("simulated artifact registry failure");
      return writeArtifact(input);
    };

    const result = await executeAgentFlowCommandPipeline(store, "run-collision", workflow);

    expect(result).toMatchObject({ status: "paused", failedStep: "check" });
    expect(result.message).toContain("Could not publish declared output");
    expect(store.getRun("run-collision")?.status).toBe("paused");
    expect(store.listEvents("run-collision").map((event) => event.type)).toContain("step.failed");
    store.close();
  });

  test("persists a terminal failure when command log publication fails", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: log-failure
version: 1
style: pipeline
maturity: experimental
steps:
  - id: check
    type: command
    command: printf output
`);
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(store, { id: "run-log-failure", workflow });
    const writeArtifact = store.writeArtifact.bind(store);
    store.writeArtifact = (input) => {
      if (input.kind === "command_log") throw new Error("simulated log registry failure");
      return writeArtifact(input);
    };

    const result = await executeAgentFlowCommandPipeline(store, "run-log-failure", workflow);

    expect(result).toMatchObject({ status: "paused", failedStep: "check" });
    expect(result.message).toContain("Could not persist command logs");
    expect(store.getRun("run-log-failure")?.status).toBe("paused");
    const failure = store.listFailures("run-log-failure")[0]!;
    expect(failure.payloadPath).toMatch(/^failures\/.+\.json$/);
    expect(JSON.parse(store.readArtifact("run-log-failure", failure.payloadPath!).content.toString("utf8")))
      .toMatchObject({
        logs: { stdout: null, stderr: null },
        artifacts: { available: [], withheld: [] }
      });
    store.close();
  });

  test("keeps the original step failure when attachment metadata cannot be scanned", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: metadata-scan-failure
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - { id: check, type: command, command: "exit 9", on_failure: { then: pause } }
`);
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(store, { id: "metadata-scan-failure", workflow });
    store.listArtifactMetadata = () => {
      throw new Error("simulated damaged artifact metadata");
    };

    const result = await executeAgentFlowCommandPipeline(store, "metadata-scan-failure", workflow);

    expect(result).toMatchObject({ status: "paused", failedStep: "check", exitCode: 9 });
    const failure = store.listFailures("metadata-scan-failure")[0]!;
    expect(failure.payloadPath).toMatch(/^failures\/.+\.json$/);
    expect(failure.payload).toMatchObject({
      failurePayloadPath: failure.payloadPath,
      payloadPersistenceError: "simulated damaged artifact metadata"
    });
    store.close();
  });

  test("withholds an attachment overwritten after attempt selection", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: raced-failure-attachment
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - { id: check, type: command, command: exit 1 }
`);
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(store, { id: "raced-failure-attachment", workflow });
    store.writeArtifact({
      id: "attempt-output",
      runId: "raced-failure-attachment",
      stepId: "check",
      path: "attempt.log",
      kind: "command_log",
      contentType: "text/plain",
      content: "first attempt",
      metadata: { attempt: 1 }
    });
    const readArtifact = store.readArtifact.bind(store);
    let raced = false;
    store.readArtifact = ((runId, artifactPath, options) => {
      if (!raced && artifactPath === "attempt.log") {
        raced = true;
        store.writeArtifact({
          id: "attempt-output",
          runId,
          stepId: "check",
          path: artifactPath,
          kind: "command_log",
          contentType: "text/plain",
          content: "second attempt",
          metadata: { attempt: 2 },
          overwrite: true
        });
      }
      return readArtifact(runId, artifactPath, options);
    }) as typeof store.readArtifact;

    const persisted = persistAgentFlowFailurePayload(store, {
      id: "command:check:attempt-1",
      runId: "raced-failure-attachment",
      stepId: "check",
      stepType: "command",
      attempt: 1,
      summary: "failed",
      classification: "command_failure",
      retryable: false,
      outcome: "pause"
    });
    const payload = JSON.parse(
      readArtifact("raced-failure-attachment", persisted.path!).content.toString("utf8")
    );

    expect(payload.artifacts).toEqual({ available: [], withheld: ["attempt.log"] });
    expect(payload.redactions.unscanned_artifacts).toEqual(["attempt.log"]);
    store.close();
  });

  test("indexes failure-attachment write errors as persistence diagnostics", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: failed-failure-attachment
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - { id: check, type: command, command: exit 1 }
`);
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(store, { id: "failed-failure-attachment", workflow });
    store.writeArtifact({
      id: "attempt-output",
      runId: "failed-failure-attachment",
      stepId: "check",
      path: "attempt.log",
      kind: "command_log",
      contentType: "text/plain",
      content: "safe evidence",
      metadata: { attempt: 1 }
    });
    const writeArtifact = store.writeArtifact.bind(store);
    store.writeArtifact = ((input) => {
      if (input.kind === "failure_attachment") throw new Error("simulated attachment write failure");
      return writeArtifact(input);
    }) as typeof store.writeArtifact;

    const persisted = persistAgentFlowFailurePayload(store, {
      id: "command:check:attempt-1",
      runId: "failed-failure-attachment",
      stepId: "check",
      stepType: "command",
      attempt: 1,
      summary: "failed",
      classification: "command_failure",
      retryable: false,
      outcome: "pause"
    });
    const failure = store.listFailures("failed-failure-attachment")[0]!;
    const payload = JSON.parse(
      store.readArtifact("failed-failure-attachment", persisted.path!).content.toString("utf8")
    );

    expect(persisted.persistenceError).toBe("simulated attachment write failure");
    expect(failure.payload).toMatchObject({
      payloadPersistenceError: "simulated attachment write failure"
    });
    expect(payload.artifacts).toEqual({ available: [], withheld: ["attempt.log"] });
    store.close();
  });

  test("rolls back failure attachments when payload persistence fails", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: failed-failure-payload
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - { id: check, type: command, command: exit 1 }
`);
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(store, { id: "failed-failure-payload", workflow });
    store.writeArtifact({
      id: "attempt-output",
      runId: "failed-failure-payload",
      stepId: "check",
      path: "attempt.log",
      kind: "command_log",
      contentType: "text/plain",
      content: "safe evidence",
      metadata: { attempt: 1 }
    });
    const writeArtifact = store.writeArtifact.bind(store);
    store.writeArtifact = ((input) => {
      if (input.kind === "failure_payload") throw new Error("simulated payload write failure");
      return writeArtifact(input);
    }) as typeof store.writeArtifact;

    const persisted = persistAgentFlowFailurePayload(store, {
      id: "command:check:attempt-1",
      runId: "failed-failure-payload",
      stepId: "check",
      stepType: "command",
      attempt: 1,
      summary: "failed",
      classification: "command_failure",
      retryable: false,
      outcome: "pause"
    });

    expect(persisted).toMatchObject({
      path: null,
      persistenceError: "simulated payload write failure"
    });
    expect(store.listArtifactMetadata("failed-failure-payload").map((artifact) => artifact.kind))
      .toEqual(["command_log"]);
    expect(store.listFailures("failed-failure-payload")[0]?.payloadPath).toBeNull();
    store.close();
  });

  test("persists repeated deterministic failures idempotently", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: idempotent-failure-payload
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - { id: check, type: command, command: exit 1 }
`);
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(store, { id: "idempotent-failure-payload", workflow });
    const input = {
      id: "routing:check:attempt-2:limit",
      runId: "idempotent-failure-payload",
      stepId: "check",
      stepType: "routing",
      attempt: 2,
      summary: "Step check cannot start because limits.max_step_attempts allows 1 attempt(s).",
      classification: "step_attempt_limit",
      retryable: false,
      outcome: "pause" as const,
      indexPayload: { attempt: 2, limit: 1, token: "replayed-secret" }
    };

    const first = persistAgentFlowFailurePayload(store, input);
    const replay = persistAgentFlowFailurePayload(store, input);

    expect(replay).toEqual(first);
    expect(store.listFailures(input.runId)).toHaveLength(1);
    expect(store.listArtifactMetadata(input.runId).filter((artifact) => artifact.kind === "failure_payload"))
      .toHaveLength(1);
    for (const changed of [
      { stepType: "command" },
      { exitCode: 1 },
      { command: "exit 9" },
      { logs: { stderr: "different.log" } },
      { indexPayload: { attempt: 2, limit: 2, token: "replayed-secret" } },
      { classification: "different_failure" }
    ]) {
      expect(() => persistAgentFlowFailurePayload(store, {
        ...input,
        ...changed
      })).toThrow("already exists with different failure data");
    }
    expect(store.listFailures(input.runId)).toHaveLength(1);
    store.close();
  });

  test("redacts secret-like command and log content in recovery-facing failure artifacts", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: redacted-failure
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - id: secret-check
    type: command
    command: |
      AWS_SECRET_ACCESS_KEY=aws-secret-value RAILS_MASTER_KEY=rails-master-value MY_API_TOKEN=super-secret-value sh -c "printf 'Authorization: Bearer abcdefghijklmnopqrstuvwxyz\\nProxy-Authorization: Basic dXNlcjpwYXNzd29yZA==\\nAuthorization: ApiKey opaque-api-key-value\\nMY_API_TOKEN=log-secret-value\\nJWT_SIGNING_KEY=jwt-signing-value\\n-----BEGIN PGP PRIVATE KEY BLOCK-----\\ncHJpdmF0ZS1rZXk=\\n-----END PGP PRIVATE KEY BLOCK-----\\n' >&2; exit 12" --password cli-password-value --api-token "quoted-cli-token" --verbose
    on_failure: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(store, { id: "redacted-failure", workflow });

    const result = await executeAgentFlowCommandPipeline(store, "redacted-failure", workflow);

    expect(result).toMatchObject({ status: "paused", failedStep: "secret-check" });
    const failure = store.listFailures("redacted-failure")[0]!;
    const serialized = store.readArtifact("redacted-failure", failure.payloadPath!).content.toString("utf8");
    expect(serialized).not.toContain("super-secret-value");
    expect(serialized).not.toContain("aws-secret-value");
    expect(serialized).not.toContain("rails-master-value");
    expect(serialized).not.toContain("log-secret-value");
    expect(serialized).not.toContain("abcdefghijklmnopqrstuvwxyz");
    expect(serialized).not.toContain("dXNlcjpwYXNzd29yZA");
    expect(serialized).not.toContain("opaque-api-key-value");
    expect(serialized).not.toContain("cHJpdmF0ZS1rZXk");
    expect(serialized).not.toContain("cli-password-value");
    expect(serialized).not.toContain("quoted-cli-token");
    expect(serialized).not.toContain("jwt-signing-value");
    const payload = JSON.parse(serialized);
    const stderrPath = payload.logs.stderr as string;
    const redactedFields = payload.redactions.fields as string[];
    expect(payload.command).toContain("AWS_SECRET_ACCESS_KEY=[REDACTED]");
    expect(payload.command).toContain("RAILS_MASTER_KEY=[REDACTED]");
    expect(payload.command).toContain("MY_API_TOKEN=[REDACTED]");
    expect(payload.command).toContain("--password [REDACTED]");
    expect(payload.command).toContain("--api-token [REDACTED]");
    expect(payload.command).toContain("--verbose");
    expect(stderrPath).toMatch(/^failures\/.+\/attachments\/.+\/stderr\.log$/);
    expect(payload.redactions).toMatchObject({ applied: true, marker: "[REDACTED]" });
    expect(redactedFields).toContain("command");
    expect(redactedFields.some((field) => /^artifacts\..+stderr\.log$/.test(field))).toBe(true);
    expect(store.readArtifact("redacted-failure", stderrPath).content.toString("utf8"))
      .toBe([
        "Authorization: Bearer [REDACTED]",
        "Proxy-Authorization: Basic [REDACTED]",
        "Authorization: ApiKey [REDACTED]",
        "MY_API_TOKEN=[REDACTED]",
        "JWT_SIGNING_KEY=[REDACTED]",
        "[REDACTED]",
        ""
      ].join("\n"));
    store.close();
  });

  test("redacts complete shell-substitution values in persisted failed commands", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: redacted-shell-substitution
version: 1
style: pipeline
maturity: experimental
steps:
  - id: secret-check
    type: command
    command: |
      API_KEYS=$(printf first-secret-part)opaque-suffix BACKUP_TOKEN=\`printf second-secret-part\`tail PREFIX_TOKEN=prefix$(printf third-secret-part) false
    on_failure: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(store, { id: "redacted-shell-substitution", workflow });

    expect((await executeAgentFlowCommandPipeline(
      store,
      "redacted-shell-substitution",
      workflow
    )).status).toBe("paused");
    const failure = store.listFailures("redacted-shell-substitution")[0]!;
    const payload = JSON.parse(
      store.readArtifact("redacted-shell-substitution", failure.payloadPath!).content.toString("utf8")
    );
    expect(payload.command).toBe(
      "API_KEYS=[REDACTED] BACKUP_TOKEN=[REDACTED] PREFIX_TOKEN=[REDACTED] false\n"
    );
    expect(JSON.stringify(payload)).not.toContain("first-secret-part");
    expect(JSON.stringify(payload)).not.toContain("second-secret-part");
    expect(JSON.stringify(payload)).not.toContain("third-secret-part");
    expect(JSON.stringify(payload)).not.toContain("opaque-suffix");
    store.close();
  });

  test("snapshots only artifacts explicitly associated with the failed attempt", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: attempt-scoped-failure
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: check, type: command, command: exit 1 }
`);
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(store, { id: "attempt-scoped-failure", workflow });
    transitionAgentFlowLifecycleRun(store, "attempt-scoped-failure", "resume");
    const stripeLikeToken = ["sk", "_live_", "123456789012345678901234"].join("");
    const slackLikeToken = ["xox", "b-", "1234567890-1234567890-abcdefghijklmnop"].join("");
    for (const [artifactPath, metadata] of [
      ["stale-unversioned.log", {}],
      ["stale-prior-attempt.log", { attempt: 1 }],
      ["current-attempt.log", { attempt: 2 }]
    ] as const) {
      store.writeArtifact({
        id: artifactPath,
        runId: "attempt-scoped-failure",
        stepId: "check",
        path: artifactPath,
        kind: "command_log",
        contentType: "text/plain",
        content: artifactPath === "current-attempt.log"
          ? [
              'Authorization: "Token quoted-authorization-secret"',
              "Cookie: session=cookie-secret",
              "password is phrase-secret",
              `Stripe ${stripeLikeToken}`,
              "GitLab glpat-12345678901234567890",
              `Slack ${slackLikeToken}`,
              "JWT eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.signature123456789",
              "aws configure set aws_secret_access_key attachment-aws-secret",
              "npm config set //registry.npmjs.org/:_authToken attachment-npm-secret",
              ""
            ].join("\n")
          : artifactPath,
        metadata
      });
    }
    store.writeArtifact({
      id: "structured-secret",
      runId: "attempt-scoped-failure",
      stepId: "check",
      path: "structured-secret.yaml",
      kind: "command_log",
      contentType: "application/yaml",
      content: "api_token: |\n  yaml-block-secret\n",
      metadata: { attempt: 2 }
    });
    store.writeArtifact({
      id: "private-key-secret",
      runId: "attempt-scoped-failure",
      stepId: "check",
      path: "private-key-secret.json",
      kind: "command_log",
      contentType: "application/json",
      content: "{\"private_key\":\"opaque-private-key-secret\"}\n",
      metadata: { attempt: 2 }
    });
    store.writeArtifact({
      id: "authorization-secret",
      runId: "attempt-scoped-failure",
      stepId: "check",
      path: "authorization-secret.json",
      kind: "command_log",
      contentType: "application/json",
      content: "{\"Authorization\":\"ApiKey structured-authorization-secret\"}\n",
      metadata: { attempt: 2 }
    });
    store.writeArtifact({
      id: "dotted-secret",
      runId: "attempt-scoped-failure",
      stepId: "check",
      path: "dotted-secret.json",
      kind: "command_log",
      contentType: "application/json",
      content: "{\"database.password\":\"dotted-structured-secret\"}\n",
      metadata: { attempt: 2 }
    });
    store.writeArtifact({
      id: "camel-secret",
      runId: "attempt-scoped-failure",
      stepId: "check",
      path: "camel-secret.json",
      kind: "command_log",
      contentType: "application/json",
      content: "{\"awsSecretAccessKey\":\"camel-structured-secret\"}\n",
      metadata: { attempt: 2 }
    });
    store.writeArtifact({
      id: "credentials-secret",
      runId: "attempt-scoped-failure",
      stepId: "check",
      path: "credentials-secret.json",
      kind: "command_log",
      contentType: "application/json",
      content: "{\"credentials\":\"structured-credentials-secret\",\"passphrase\":\"structured-passphrase-secret\"}\n",
      metadata: { attempt: 2 }
    });
    store.writeArtifact({
      id: "auth-secret",
      runId: "attempt-scoped-failure",
      stepId: "check",
      path: "auth-secret.json",
      kind: "command_log",
      contentType: "application/json",
      content: "{\"auth\":\"dXNlcjphdXRoLXNlY3JldA==\"}\n",
      metadata: { attempt: 2 }
    });
    store.writeArtifact({
      id: "plain-html-secret",
      runId: "attempt-scoped-failure",
      stepId: "check",
      path: "plain-html-secret.log",
      kind: "command_log",
      contentType: "text/plain",
      content: '<input type="password" value="plain-html-secret-value">',
      metadata: { attempt: 2 }
    });
    store.writeArtifact({
      id: "plain-block-secret",
      runId: "attempt-scoped-failure",
      stepId: "check",
      path: "plain-block-secret.log",
      kind: "command_log",
      contentType: "text/plain",
      content: "api_token: |\n  plain-block-secret-value\n",
      metadata: { attempt: 2 }
    });
    store.writeArtifact({
      id: "invalid-utf8",
      runId: "attempt-scoped-failure",
      stepId: "check",
      path: "invalid-utf8.log",
      kind: "command_log",
      contentType: "text/plain; charset=utf-8",
      content: Buffer.from([0xc3, 0x28]),
      metadata: { attempt: 2 }
    });
    store.writeArtifact({
      id: "unsupported-html-secret",
      runId: "attempt-scoped-failure",
      stepId: "check",
      path: "unsupported-secret.html",
      kind: "command_log",
      contentType: "text/html",
      content: '<input name="password" value="html-secret-value">',
      metadata: { attempt: 2 }
    });
    store.writeArtifact({
      id: "safe-markdown",
      runId: "attempt-scoped-failure",
      stepId: "check",
      path: "safe-evidence.md",
      kind: "session_output",
      contentType: "text/markdown; charset=utf-8",
      content: "# Safe evidence\n\nNo credentials here.\n",
      metadata: { attempt: 2 }
    });
    store.writeArtifact({
      id: "unsafe-markdown",
      runId: "attempt-scoped-failure",
      stepId: "check",
      path: "unsafe-evidence.md",
      kind: "session_output",
      contentType: "text/markdown",
      content: '<input name="password" value="markdown-html-secret">',
      metadata: { attempt: 2 }
    });

    const persisted = persistAgentFlowFailurePayload(store, {
      id: "command:check:attempt-2",
      runId: "attempt-scoped-failure",
      stepId: "check",
      stepType: "command",
      attempt: 2,
      command: "AUTHORIZATION=authorization-assignment-secret COOKIE=cookie-assignment-secret PASSPHRASE=passphrase-assignment-secret CREDENTIALS=credentials-assignment-secret PGPASSWORD=database-secret MYSQL_PWD=mysql-secret aws configure set aws_secret_access_key positional-secret && npm config set //registry.npmjs.org/:_authToken=npm_abcdefghijklmnopqrstuvwxyz1234567890 && tool --password \"abc\\\"def\" --api-token $'ansi-token-secret' --user alice:curl-secret --proxy-user \"bob:proxy-secret\" -ucarol:short-secret --password flag-prefix\\ flag-suffix-leak --mode safe && curl -u user:'mixed-secret-suffix' -H 'X-Api-Key: header-secret-suffix' && PASSWORD=plain'assignment-secret-suffix' tool",
      summary: "password: correct horse battery staple\nAPI key is spaced-api-secret\nBasic dTpw\nGitHub token github_pat_11ABCDEFGHijklmnopqrstuv1234567890\nnpm token npm_zyxwvutsrqponmlkjihgfedcba0987654321\nnext diagnostic line",
      classification: "command_failure",
      retryable: false,
      outcome: "pause",
      indexPayload: {
        api_token: "plain-index-secret",
        db_pass: "database-pass-index-secret",
        nested: {
          AWS_SECRET_ACCESS_KEY: "nested-index-secret",
          pwd: "nested-pwd-index-secret"
        }
      }
    });

    const serialized = store.readArtifact("attempt-scoped-failure", persisted.path!).content.toString("utf8");
    expect(serialized).not.toContain("plain-index-secret");
    expect(serialized).not.toContain("database-pass-index-secret");
    expect(serialized).not.toContain("nested-index-secret");
    expect(serialized).not.toContain("nested-pwd-index-secret");
    expect(serialized).not.toContain("yaml-block-secret");
    expect(serialized).not.toContain("opaque-private-key-secret");
    expect(serialized).not.toContain("plain-block-secret-value");
    expect(serialized).not.toContain("structured-authorization-secret");
    expect(serialized).not.toContain("dotted-structured-secret");
    expect(serialized).not.toContain("camel-structured-secret");
    expect(serialized).not.toContain("structured-credentials-secret");
    expect(serialized).not.toContain("structured-passphrase-secret");
    expect(serialized).not.toContain("dXNlcjphdXRoLXNlY3JldA==");
    expect(serialized).not.toContain("plain-html-secret-value");
    expect(serialized).not.toContain("quoted-authorization-secret");
    expect(serialized).not.toContain("cookie-secret");
    expect(serialized).not.toContain("phrase-secret");
    expect(serialized).not.toContain(stripeLikeToken);
    expect(serialized).not.toContain("glpat-12345678901234567890");
    expect(serialized).not.toContain(slackLikeToken);
    expect(serialized).not.toContain("eyJhbGciOiJIUzI1NiJ9");
    expect(serialized).not.toContain('abc\\"def');
    expect(serialized).not.toContain("database-secret");
    expect(serialized).not.toContain("mysql-secret");
    expect(serialized).not.toContain("ansi-token-secret");
    expect(serialized).not.toContain("curl-secret");
    expect(serialized).not.toContain("proxy-secret");
    expect(serialized).not.toContain("short-secret");
    expect(serialized).not.toContain("positional-secret");
    expect(serialized).not.toContain("html-secret-value");
    expect(serialized).not.toContain("markdown-html-secret");
    expect(serialized).not.toContain("authorization-assignment-secret");
    expect(serialized).not.toContain("cookie-assignment-secret");
    expect(serialized).not.toContain("passphrase-assignment-secret");
    expect(serialized).not.toContain("credentials-assignment-secret");
    expect(serialized).not.toContain("flag-suffix-leak");
    expect(serialized).not.toContain("mixed-secret-suffix");
    expect(serialized).not.toContain("header-secret-suffix");
    expect(serialized).not.toContain("assignment-secret-suffix");
    expect(serialized).not.toContain("github_pat_11ABCDEFGHijklmnopqrstuv1234567890");
    expect(serialized).not.toContain("npm_abcdefghijklmnopqrstuvwxyz1234567890");
    expect(serialized).not.toContain("npm_zyxwvutsrqponmlkjihgfedcba0987654321");
    expect(serialized).not.toContain("horse battery staple");
    expect(serialized).not.toContain("spaced-api-secret");
    expect(serialized).not.toContain("attachment-aws-secret");
    expect(serialized).not.toContain("attachment-npm-secret");
    expect(persisted.indexPayload).toEqual({
      api_token: "[REDACTED]",
      db_pass: "[REDACTED]",
      nested: {
        AWS_SECRET_ACCESS_KEY: "[REDACTED]",
        pwd: "[REDACTED]"
      }
    });
    const payload = JSON.parse(serialized);
    expect(payload.command).toBe(
      "AUTHORIZATION=[REDACTED] COOKIE=[REDACTED] PASSPHRASE=[REDACTED] CREDENTIALS=[REDACTED] PGPASSWORD=[REDACTED] MYSQL_PWD=[REDACTED] aws configure set aws_secret_access_key [REDACTED] && npm config set //registry.npmjs.org/:_authToken=[REDACTED] && tool --password [REDACTED] --api-token [REDACTED] --user [REDACTED] --proxy-user [REDACTED] -u[REDACTED] --password [REDACTED] --mode safe && curl -u [REDACTED] -H [REDACTED] && PASSWORD=[REDACTED] tool"
    );
    expect(payload.summary).toBe("password: [REDACTED]\nAPI key is [REDACTED]\nBasic [REDACTED]\nGitHub token [REDACTED]\nnpm token [REDACTED]\nnext diagnostic line");
    expect(payload.artifacts.available).toHaveLength(2);
    expect(payload.artifacts.withheld).toEqual([
      "auth-secret.json",
      "authorization-secret.json",
      "camel-secret.json",
      "credentials-secret.json",
      "dotted-secret.json",
      "invalid-utf8.log",
      "plain-block-secret.log",
      "plain-html-secret.log",
      "private-key-secret.json",
      "structured-secret.yaml",
      "unsafe-evidence.md",
      "unsupported-secret.html"
    ]);
    expect(payload.redactions.unscanned_artifacts).toEqual([
      "auth-secret.json",
      "authorization-secret.json",
      "camel-secret.json",
      "credentials-secret.json",
      "dotted-secret.json",
      "invalid-utf8.log",
      "plain-block-secret.log",
      "plain-html-secret.log",
      "private-key-secret.json",
      "structured-secret.yaml",
      "unsafe-evidence.md",
      "unsupported-secret.html"
    ]);
    const currentAttemptSnapshot = (payload.artifacts.available as string[])
      .find((artifactPath) => artifactPath.endsWith("/current-attempt.log"))!;
    const markdownSnapshot = (payload.artifacts.available as string[])
      .find((artifactPath) => artifactPath.endsWith("/safe-evidence.md"))!;
    expect(store.readArtifact("attempt-scoped-failure", currentAttemptSnapshot).content.toString())
      .toBe([
        'Authorization: "[REDACTED]"',
        "Cookie: [REDACTED]",
        "password is [REDACTED]",
        "Stripe [REDACTED]",
        "GitLab [REDACTED]",
        "Slack [REDACTED]",
        "JWT [REDACTED]",
        "aws configure set aws_secret_access_key [REDACTED]",
        "npm config set //registry.npmjs.org/:_authToken [REDACTED]",
        ""
      ].join("\n"));
    expect(store.readArtifact("attempt-scoped-failure", markdownSnapshot).content.toString())
      .toBe("# Safe evidence\n\nNo credentials here.\n");

    const malformed = persistAgentFlowFailurePayload(store, {
      id: "command:check:attempt-3",
      runId: "attempt-scoped-failure",
      stepId: "check",
      stepType: "command",
      attempt: 3,
      command: 'API_TOKEN="unterminated-secret',
      summary: "shell syntax failure",
      classification: "command_failure",
      retryable: false,
      outcome: "pause"
    });
    const malformedPayload = JSON.parse(
      store.readArtifact("attempt-scoped-failure", malformed.path!).content.toString("utf8")
    );
    expect(malformedPayload.command).toBe('API_TOKEN="[REDACTED]');
    expect(JSON.stringify(malformedPayload)).not.toContain("unterminated-secret");
    store.close();
  });

  test("bounds aggregate failure attachment scans by count and bytes", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: bounded-failure-attachments
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - { id: check, type: command, command: exit 1 }
`);
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(store, { id: "bounded-failure-attachments", workflow });
    for (let index = 0; index <= MAX_AGENT_FLOW_FAILURE_ATTACHMENT_COUNT; index += 1) {
      store.writeArtifact({
        id: `count-${index}`,
        runId: "bounded-failure-attachments",
        stepId: "check",
        path: `count/${String(index).padStart(3, "0")}.log`,
        kind: "command_log",
        contentType: "text/plain",
        content: "safe",
        metadata: { attempt: 1 }
      });
    }
    const countFailure = persistAgentFlowFailurePayload(store, {
      id: "count-bound",
      runId: "bounded-failure-attachments",
      stepId: "check",
      stepType: "command",
      attempt: 1,
      summary: "failed",
      classification: "command_failure",
      retryable: false,
      outcome: "pause"
    });
    const countPayload = JSON.parse(
      store.readArtifact("bounded-failure-attachments", countFailure.path!).content.toString("utf8")
    );
    expect(countPayload.artifacts.available).toHaveLength(MAX_AGENT_FLOW_FAILURE_ATTACHMENT_COUNT);
    expect(countPayload.artifacts.withheld).toEqual(["count/064.log"]);

    const aggregateArtifactCount = Math.floor(
      MAX_AGENT_FLOW_FAILURE_TOTAL_ATTACHMENT_BYTES / MAX_AGENT_FLOW_FAILURE_ATTACHMENT_SCAN_BYTES
    ) + 1;
    for (let index = 0; index < aggregateArtifactCount; index += 1) {
      store.writeArtifact({
        id: `bytes-${index}`,
        runId: "bounded-failure-attachments",
        stepId: "byte-check",
        path: `bytes/${index}.log`,
        kind: "command_log",
        contentType: "text/plain",
        content: Buffer.alloc(MAX_AGENT_FLOW_FAILURE_ATTACHMENT_SCAN_BYTES, 0x61),
        metadata: { attempt: 1 }
      });
    }
    const byteFailure = persistAgentFlowFailurePayload(store, {
      id: "byte-bound",
      runId: "bounded-failure-attachments",
      stepId: "byte-check",
      stepType: "command",
      attempt: 1,
      summary: "failed",
      classification: "command_failure",
      retryable: false,
      outcome: "pause"
    });
    const bytePayload = JSON.parse(
      store.readArtifact("bounded-failure-attachments", byteFailure.path!).content.toString("utf8")
    );
    expect(bytePayload.artifacts.available).toHaveLength(aggregateArtifactCount - 1);
    expect(bytePayload.artifacts.withheld).toEqual([`bytes/${aggregateArtifactCount - 1}.log`]);
    store.close();
  });

  test("rejects declared output traversal before starting a child process", async () => {
    const repoRoot = temporaryRepo();
    const marker = path.join(os.tmpdir(), `agent-flow-marker-${crypto.randomUUID()}`);
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: unsafe-output
version: 1
style: pipeline
maturity: experimental
steps:
  - id: escape
    type: command
    command: touch ${JSON.stringify(marker)}
    outputs:
      - ../outside.txt
`);
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(store, { id: "run-unsafe", workflow });

    const result = await executeAgentFlowCommandPipeline(store, "run-unsafe", workflow);

    expect(result).toMatchObject({ status: "failed", failedStep: "escape" });
    expect(result.message).toContain("repo-relative");
    expect(fs.existsSync(marker)).toBe(false);
    store.close();
  });

  test("rejects declared outputs through existing symlinked parent directories before execution", async () => {
    const repoRoot = temporaryRepo();
    const outside = fs.mkdtempSync(path.join(os.tmpdir(), "agent-flow-output-outside-"));
    fs.symlinkSync(outside, path.join(repoRoot, "linked-output"), "dir");
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: symlink-output
version: 1
style: pipeline
maturity: experimental
steps:
  - id: escape
    type: command
    command: touch linked-output/result.txt
    outputs: [linked-output/result.txt]
`);
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    createAgentFlowLifecycleRun(store, { id: "run-symlink-output", workflow });

    const result = await executeAgentFlowCommandPipeline(store, "run-symlink-output", workflow);

    expect(result).toMatchObject({ status: "failed", failedStep: "escape" });
    expect(result.message).toContain("stay inside the repository");
    expect(fs.existsSync(path.join(outside, "result.txt"))).toBe(false);
    store.close();
  });
});

function temporaryRepo(): string {
  const repoRoot = fs.mkdtempSync(path.join(os.tmpdir(), "agent-flow-command-"));
  fs.mkdirSync(path.join(repoRoot, ".git"));
  return repoRoot;
}

function readArtifact(repoRoot: string, storagePath: string): string {
  return fs.readFileSync(path.join(repoRoot, storagePath), "utf8");
}
