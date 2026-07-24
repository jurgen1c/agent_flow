import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  type AgentflowRunStateValue,
  createAgentflowLifecycleRun,
  executeAgentflowCommandPipeline,
  openAgentflowRunState,
  parseAgentflowWorkflowOrThrow,
  transitionAgentflowLifecycleRun
} from "../../packages/agentflow-core/src";

describe("Agentflow command step execution", () => {
  test("rejects a workflow that differs from the persisted run definition", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`
name: immutable-run
version: 1
style: pipeline
maturity: experimental
steps:
  - id: write
    type: command
    command: printf original
`);
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "mismatched-workflow", workflow });

    await expect(executeAgentflowCommandPipeline(store, "mismatched-workflow", {
      ...workflow,
      steps: [{ id: "write", type: "command", command: "printf replacement" }]
    })).rejects.toThrow("differs from its persisted definition");

    expect(store.getRun("mismatched-workflow")?.status).toBe("pending");
    store.close();
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
      const workflow = parseAgentflowWorkflowOrThrow(`
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

      const result = await executeAgentflowCommandPipeline(store, runId, workflow);

      expect(result).toMatchObject({ status: "failed", failedStep: "unsafe-policy" });
      expect(result.message).toContain(expectedMessage);
      expect(fs.existsSync(path.join(repoRoot, "command-started"))).toBe(false);
      store.close();
    }
  });

  test("does not allow a second executor to share a running run", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`
name: single-owner
version: 1
style: pipeline
maturity: experimental
steps:
  - id: write
    type: command
    command: sleep 0.1
`);
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "single-owner", workflow });

    const owner = executeAgentflowCommandPipeline(store, "single-owner", workflow);
    await expect(executeAgentflowCommandPipeline(store, "single-owner", workflow))
      .rejects.toThrow("status is running");
    expect((await owner).status).toBe("completed");
    store.close();
  });

  test("runs a safe command pipeline and persists logs, declared artifacts, and completion state", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`
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
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "run-safe", workflow });

    const result = await executeAgentflowCommandPipeline(store, "run-safe", workflow);

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
    expect(artifacts.map((artifact) => artifact.declaredPath)).toHaveLength(3);
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
    const workflow = parseAgentflowWorkflowOrThrow(`
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
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "run-failed", workflow });

    const result = await executeAgentflowCommandPipeline(store, "run-failed", workflow);

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
    expect(store.listFailures("run-failed")).toMatchObject([{
      stepId: "test",
      classification: "command_failure",
      retryable: false,
      payload: { attempt: 1, exitCode: 23, timedOut: false, outcome: "fail" }
    }]);
    expect(store.listEvents("run-failed").map((event) => event.type)).toContain("step.failed");
    const stderr = store.listArtifacts("run-failed").find((artifact) => artifact.declaredPath.endsWith("stderr.log"))!;
    expect(readArtifact(repoRoot, stderr.storagePath)).toBe("failure details\n");
    store.close();
  });

  test("retries failed commands and persists each attempt", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`
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
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "run-retry", workflow });

    const result = await executeAgentflowCommandPipeline(store, "run-retry", workflow);

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
    const pausedWorkflow = parseAgentflowWorkflowOrThrow(`
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
    const pausedStore = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(pausedStore, { id: "exhausted-retry", workflow: pausedWorkflow });

    const paused = await executeAgentflowCommandPipeline(pausedStore, "exhausted-retry", pausedWorkflow);

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

    const continuedWorkflow = parseAgentflowWorkflowOrThrow(`
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
    const continuedStore = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(continuedStore, { id: "continued-failure", workflow: continuedWorkflow });

    const continued = await executeAgentflowCommandPipeline(
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
    const workflow = parseAgentflowWorkflowOrThrow(`
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
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "run-timeout", workflow });

    const result = await executeAgentflowCommandPipeline(store, "run-timeout", workflow);

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
    const workflow = parseAgentflowWorkflowOrThrow(`
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
`);
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "run-cancelled", workflow });

    const startedAt = Date.now();
    const execution = executeAgentflowCommandPipeline(store, "run-cancelled", workflow);
    setTimeout(() => transitionAgentflowLifecycleRun(store, "run-cancelled", "cancel"), 25);
    const result = await execution;

    expect(result.status).toBe("cancelled");
    expect(Date.now() - startedAt).toBeLessThan(1_000);
    expect(fs.existsSync(marker)).toBe(false);
    expect(store.listEvents("run-cancelled").map((event) => event.type)).toContain("step.interrupted");
    const stdout = store.listArtifacts("run-cancelled").find((artifact) => artifact.declaredPath.endsWith("stdout.log"))!;
    expect(readArtifact(repoRoot, stdout.storagePath)).toBe("before cancellation\n");
    store.close();
  });

  test("does not start later commands after concurrent pause", async () => {
    const repoRoot = temporaryRepo();
    const marker = path.join(repoRoot, "second-step-started");
    const workflow = parseAgentflowWorkflowOrThrow(`
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
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "run-paused", workflow });

    const execution = executeAgentflowCommandPipeline(store, "run-paused", workflow);
    setTimeout(() => transitionAgentflowLifecycleRun(store, "run-paused", "pause"), 25);
    const result = await execution;

    expect(result.status).toBe("paused");
    expect(fs.existsSync(marker)).toBe(false);
    const stdout = store.listArtifacts("run-paused").find((artifact) => artifact.declaredPath.endsWith("stdout.log"))!;
    expect(readArtifact(repoRoot, stdout.storagePath)).toBe("before pause\n");
    store.close();
  });

  test("pauses unsafe commands that require approval before starting them", async () => {
    const repoRoot = temporaryRepo();
    fs.mkdirSync(path.join(repoRoot, "protected"));
    fs.writeFileSync(path.join(repoRoot, "protected/keep.txt"), "keep\n");
    const workflow = parseAgentflowWorkflowOrThrow(`
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
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "run-approval", workflow });

    const result = await executeAgentflowCommandPipeline(store, "run-approval", workflow);

    expect(result).toMatchObject({ status: "paused", failedStep: "erase" });
    expect(result.message).toContain("Approval is required");
    expect(fs.existsSync(path.join(repoRoot, "protected/keep.txt"))).toBe(true);
    expect(store.listEvents("run-approval").map((event) => event.type)).toContain("step.rejected");
    store.close();
  });

  test("fails closed for configured file scopes before starting an unrestricted shell", async () => {
    const repoRoot = temporaryRepo();
    const marker = path.join(repoRoot, "command-started");
    const workflow = parseAgentflowWorkflowOrThrow(`
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
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "run-scope", workflow });

    const result = await executeAgentflowCommandPipeline(store, "run-scope", workflow);

    expect(result).toMatchObject({ status: "failed", failedStep: "denied" });
    expect(result.message).toContain("cannot confine arbitrary shell writes");
    expect(fs.existsSync(marker)).toBe(false);
    store.close();
  });

  test("persists a terminal failure when declared artifact publication fails", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`
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
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "run-collision", workflow });
    const writeArtifact = store.writeArtifact.bind(store);
    store.writeArtifact = (input) => {
      if (input.kind === "command_output") throw new Error("simulated artifact registry failure");
      return writeArtifact(input);
    };

    const result = await executeAgentflowCommandPipeline(store, "run-collision", workflow);

    expect(result).toMatchObject({ status: "paused", failedStep: "check" });
    expect(result.message).toContain("Could not publish declared output");
    expect(store.getRun("run-collision")?.status).toBe("paused");
    expect(store.listEvents("run-collision").map((event) => event.type)).toContain("step.failed");
    store.close();
  });

  test("persists a terminal failure when command log publication fails", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`
name: log-failure
version: 1
style: pipeline
maturity: experimental
steps:
  - id: check
    type: command
    command: printf output
`);
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "run-log-failure", workflow });
    store.writeArtifact = () => {
      throw new Error("simulated log registry failure");
    };

    const result = await executeAgentflowCommandPipeline(store, "run-log-failure", workflow);

    expect(result).toMatchObject({ status: "paused", failedStep: "check" });
    expect(result.message).toContain("Could not persist command logs");
    expect(store.getRun("run-log-failure")?.status).toBe("paused");
    store.close();
  });

  test("rejects declared output traversal before starting a child process", async () => {
    const repoRoot = temporaryRepo();
    const marker = path.join(os.tmpdir(), `agentflow-marker-${crypto.randomUUID()}`);
    const workflow = parseAgentflowWorkflowOrThrow(`
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
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "run-unsafe", workflow });

    const result = await executeAgentflowCommandPipeline(store, "run-unsafe", workflow);

    expect(result).toMatchObject({ status: "failed", failedStep: "escape" });
    expect(result.message).toContain("repo-relative");
    expect(fs.existsSync(marker)).toBe(false);
    store.close();
  });

  test("rejects declared outputs through existing symlinked parent directories before execution", async () => {
    const repoRoot = temporaryRepo();
    const outside = fs.mkdtempSync(path.join(os.tmpdir(), "agentflow-output-outside-"));
    fs.symlinkSync(outside, path.join(repoRoot, "linked-output"), "dir");
    const workflow = parseAgentflowWorkflowOrThrow(`
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
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "run-symlink-output", workflow });

    const result = await executeAgentflowCommandPipeline(store, "run-symlink-output", workflow);

    expect(result).toMatchObject({ status: "failed", failedStep: "escape" });
    expect(result.message).toContain("stay inside the repository");
    expect(fs.existsSync(path.join(outside, "result.txt"))).toBe(false);
    store.close();
  });
});

function temporaryRepo(): string {
  const repoRoot = fs.mkdtempSync(path.join(os.tmpdir(), "agentflow-command-"));
  fs.mkdirSync(path.join(repoRoot, ".git"));
  return repoRoot;
}

function readArtifact(repoRoot: string, storagePath: string): string {
  return fs.readFileSync(path.join(repoRoot, storagePath), "utf8");
}
