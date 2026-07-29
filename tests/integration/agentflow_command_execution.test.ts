import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  MAX_AGENTFLOW_FAILURE_ATTACHMENT_COUNT,
  MAX_AGENTFLOW_FAILURE_ATTACHMENT_SCAN_BYTES,
  MAX_AGENTFLOW_FAILURE_TOTAL_ATTACHMENT_BYTES,
  type AgentflowRunStateValue,
  createAgentflowLifecycleRun,
  executeAgentflowCommandPipeline,
  openAgentflowRunState,
  parseAgentflowWorkflowOrThrow,
  persistAgentflowFailurePayload,
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

  test("records a preflight failure for a malformed persisted command", async () => {
    const repoRoot = temporaryRepo();
    const parsed = parseAgentflowWorkflowOrThrow(`
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
    const store = await openAgentflowRunState({ cwd: repoRoot });
    store.createRunWithEvent({
      id: "malformed-command",
      workflow: {
        name: workflow.name,
        version: workflow.version,
        style: workflow.style,
        maturity: workflow.maturity
      },
      context: { workflow: workflow as unknown as AgentflowRunStateValue }
    }, { type: "run.created", payload: { status: "pending" } });

    const result = await executeAgentflowCommandPipeline(store, "malformed-command", workflow);

    expect(result).toMatchObject({ status: "failed", failedStep: "malformed" });
    expect(store.getRun("malformed-command")?.status).toBe("failed");
    const failure = store.listFailures("malformed-command")[0]!;
    expect(JSON.parse(store.readArtifact("malformed-command", failure.payloadPath!).content.toString("utf8")))
      .toMatchObject({ command: null, summary: "Command steps require a non-empty command." });
    store.close();
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
retention:
  on_cancelled:
    delete: [logs/**]
    after_days: 7
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

  test("preserves failed status when a running command is interrupted by terminal finalization", async () => {
    const repoRoot = temporaryRepo();
    const marker = path.join(repoRoot, "second-step-started");
    const workflow = parseAgentflowWorkflowOrThrow(`
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
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "run-failed", workflow });

    const startedAt = Date.now();
    const execution = executeAgentflowCommandPipeline(store, "run-failed", workflow);
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
    const writeArtifact = store.writeArtifact.bind(store);
    store.writeArtifact = (input) => {
      if (input.kind === "command_log") throw new Error("simulated log registry failure");
      return writeArtifact(input);
    };

    const result = await executeAgentflowCommandPipeline(store, "run-log-failure", workflow);

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
    const workflow = parseAgentflowWorkflowOrThrow(`
name: metadata-scan-failure
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - { id: check, type: command, command: "exit 9", on_failure: { then: pause } }
`);
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "metadata-scan-failure", workflow });
    store.listArtifactMetadata = () => {
      throw new Error("simulated damaged artifact metadata");
    };

    const result = await executeAgentflowCommandPipeline(store, "metadata-scan-failure", workflow);

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
    const workflow = parseAgentflowWorkflowOrThrow(`
name: raced-failure-attachment
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - { id: check, type: command, command: exit 1 }
`);
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "raced-failure-attachment", workflow });
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

    const persisted = persistAgentflowFailurePayload(store, {
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
    const workflow = parseAgentflowWorkflowOrThrow(`
name: failed-failure-attachment
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - { id: check, type: command, command: exit 1 }
`);
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "failed-failure-attachment", workflow });
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

    const persisted = persistAgentflowFailurePayload(store, {
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
    const workflow = parseAgentflowWorkflowOrThrow(`
name: failed-failure-payload
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - { id: check, type: command, command: exit 1 }
`);
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "failed-failure-payload", workflow });
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

    const persisted = persistAgentflowFailurePayload(store, {
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
    const workflow = parseAgentflowWorkflowOrThrow(`
name: idempotent-failure-payload
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - { id: check, type: command, command: exit 1 }
`);
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "idempotent-failure-payload", workflow });
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

    const first = persistAgentflowFailurePayload(store, input);
    const replay = persistAgentflowFailurePayload(store, input);

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
      expect(() => persistAgentflowFailurePayload(store, {
        ...input,
        ...changed
      })).toThrow("already exists with different failure data");
    }
    expect(store.listFailures(input.runId)).toHaveLength(1);
    store.close();
  });

  test("redacts secret-like command and log content in recovery-facing failure artifacts", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`
name: redacted-failure
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - id: secret-check
    type: command
    command: |
      AWS_SECRET_ACCESS_KEY=aws-secret-value MY_API_TOKEN=super-secret-value sh -c "printf 'Authorization: Bearer abcdefghijklmnopqrstuvwxyz\\nProxy-Authorization: Basic dXNlcjpwYXNzd29yZA==\\nAuthorization: ApiKey opaque-api-key-value\\nMY_API_TOKEN=log-secret-value\\n-----BEGIN PGP PRIVATE KEY BLOCK-----\\ncHJpdmF0ZS1rZXk=\\n-----END PGP PRIVATE KEY BLOCK-----\\n' >&2; exit 12" --password cli-password-value --api-token "quoted-cli-token" --verbose
    on_failure: { then: pause }
`);
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "redacted-failure", workflow });

    const result = await executeAgentflowCommandPipeline(store, "redacted-failure", workflow);

    expect(result).toMatchObject({ status: "paused", failedStep: "secret-check" });
    const failure = store.listFailures("redacted-failure")[0]!;
    const serialized = store.readArtifact("redacted-failure", failure.payloadPath!).content.toString("utf8");
    expect(serialized).not.toContain("super-secret-value");
    expect(serialized).not.toContain("aws-secret-value");
    expect(serialized).not.toContain("log-secret-value");
    expect(serialized).not.toContain("abcdefghijklmnopqrstuvwxyz");
    expect(serialized).not.toContain("dXNlcjpwYXNzd29yZA");
    expect(serialized).not.toContain("opaque-api-key-value");
    expect(serialized).not.toContain("cHJpdmF0ZS1rZXk");
    expect(serialized).not.toContain("cli-password-value");
    expect(serialized).not.toContain("quoted-cli-token");
    const payload = JSON.parse(serialized);
    const stderrPath = payload.logs.stderr as string;
    const redactedFields = payload.redactions.fields as string[];
    expect(payload.command).toContain("AWS_SECRET_ACCESS_KEY=[REDACTED]");
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
        "[REDACTED]",
        ""
      ].join("\n"));
    store.close();
  });

  test("snapshots only artifacts explicitly associated with the failed attempt", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`
name: attempt-scoped-failure
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: check, type: command, command: exit 1 }
`);
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "attempt-scoped-failure", workflow });
    transitionAgentflowLifecycleRun(store, "attempt-scoped-failure", "resume");
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

    const persisted = persistAgentflowFailurePayload(store, {
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
        nested: { AWS_SECRET_ACCESS_KEY: "nested-index-secret" }
      }
    });

    const serialized = store.readArtifact("attempt-scoped-failure", persisted.path!).content.toString("utf8");
    expect(serialized).not.toContain("plain-index-secret");
    expect(serialized).not.toContain("nested-index-secret");
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
      nested: { AWS_SECRET_ACCESS_KEY: "[REDACTED]" }
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

    const malformed = persistAgentflowFailurePayload(store, {
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
    const workflow = parseAgentflowWorkflowOrThrow(`
name: bounded-failure-attachments
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - { id: check, type: command, command: exit 1 }
`);
    const store = await openAgentflowRunState({ cwd: repoRoot });
    createAgentflowLifecycleRun(store, { id: "bounded-failure-attachments", workflow });
    for (let index = 0; index <= MAX_AGENTFLOW_FAILURE_ATTACHMENT_COUNT; index += 1) {
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
    const countFailure = persistAgentflowFailurePayload(store, {
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
    expect(countPayload.artifacts.available).toHaveLength(MAX_AGENTFLOW_FAILURE_ATTACHMENT_COUNT);
    expect(countPayload.artifacts.withheld).toEqual(["count/064.log"]);

    const aggregateArtifactCount = Math.floor(
      MAX_AGENTFLOW_FAILURE_TOTAL_ATTACHMENT_BYTES / MAX_AGENTFLOW_FAILURE_ATTACHMENT_SCAN_BYTES
    ) + 1;
    for (let index = 0; index < aggregateArtifactCount; index += 1) {
      store.writeArtifact({
        id: `bytes-${index}`,
        runId: "bounded-failure-attachments",
        stepId: "byte-check",
        path: `bytes/${index}.log`,
        kind: "command_log",
        contentType: "text/plain",
        content: Buffer.alloc(MAX_AGENTFLOW_FAILURE_ATTACHMENT_SCAN_BYTES, 0x61),
        metadata: { attempt: 1 }
      });
    }
    const byteFailure = persistAgentflowFailurePayload(store, {
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
