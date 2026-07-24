import { Database } from "bun:sqlite";
import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import path from "node:path";
import {
  AgentflowRunStateError,
  createAgentflowLifecycleRun,
  executeAgentflowCommandPipeline,
  openAgentflowRunState,
  parseAgentflowWorkflowOrThrow,
  resumeAgentflowCommandPipeline,
  transitionAgentflowLifecycleRun,
  validateAgentflowWorkflow
} from "../../packages/agentflow-core/src";

describe("Agentflow manual gates and input requests", () => {
  test("pauses at a manual gate, rejects invalid outcomes, and resumes from that step", async () => {
    const repoRoot = temporaryRepo();
    let now = "2026-07-23T12:00:00.000Z";
    const store = await openAgentflowRunState({ cwd: repoRoot, now: () => now });
    const workflow = parseAgentflowWorkflowOrThrow(`
name: guarded-pipeline
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: before, type: command, command: "printf 'before\\n' >> trace.log" }
  - id: approve
    type: manual_gate
    message: Publish the result?
    options: [approve, pause, cancel]
  - { id: after, type: command, command: "printf 'after\\n' >> trace.log" }
`);
    createAgentflowLifecycleRun(store, { id: "manual-run", workflow });

    const paused = await executeAgentflowCommandPipeline(store, "manual-run", workflow);

    expect(paused).toMatchObject({ status: "paused", completedSteps: ["before"] });
    expect(fs.readFileSync(path.join(repoRoot, "trace.log"), "utf8")).toBe("before\n");
    expect(store.getRun("manual-run")).toMatchObject({
      status: "paused",
      currentStepId: "approve",
      context: {
        waiting: {
          kind: "manual_gate",
          reason: "manual_approval",
          prompt: "Publish the result?",
          validOutcomes: ["approve", "pause", "cancel"],
          completedSteps: ["before"]
        }
      }
    });
    expect(() => transitionAgentflowLifecycleRun(store, "manual-run", "resume")).toThrow(
      "waiting for an explicit manual-gate outcome"
    );

    await expect(resumeAgentflowCommandPipeline(
      store,
      "manual-run",
      workflow,
      { outcome: "ship" }
    )).rejects.toBeInstanceOf(AgentflowRunStateError);
    expect(store.getRun("manual-run")?.status).toBe("paused");
    expect(fs.readFileSync(path.join(repoRoot, "trace.log"), "utf8")).toBe("before\n");

    await expect(resumeAgentflowCommandPipeline(
      store,
      "manual-run",
      workflow,
      { outcome: "approve", decidedBy: " " }
    )).rejects.toMatchObject({ code: "AGENTFLOW_INTERACTION_INVALID" });
    expect(store.getRun("manual-run")).toMatchObject({
      status: "paused",
      context: { waiting: { stepId: "approve" } }
    });
    const pendingApprovalDatabase = new Database(store.databasePath, { readonly: true });
    expect(pendingApprovalDatabase.query(
      "SELECT status FROM approvals WHERE run_id = ?"
    ).get("manual-run")).toEqual({ status: "requested" });
    pendingApprovalDatabase.close();

    const initiallyPausedAt = store.getRun("manual-run")!.updatedAt;
    now = "2026-07-23T12:01:00.000Z";
    const stillPaused = await resumeAgentflowCommandPipeline(
      store,
      "manual-run",
      workflow,
      { outcome: "pause" }
    );
    expect(stillPaused.status).toBe("paused");
    expect(store.getRun("manual-run")?.context.waiting).toBeDefined();
    expect(store.getRun("manual-run")?.updatedAt).not.toBe(initiallyPausedAt);
    expect(store.getRun("manual-run")?.updatedAt).toBe(now);

    const completed = await resumeAgentflowCommandPipeline(
      store,
      "manual-run",
      workflow,
      { outcome: "approve", decidedBy: "maintainer" }
    );

    expect(completed).toEqual({
      status: "completed",
      completedSteps: ["before", "approve", "after"]
    });
    expect(fs.readFileSync(path.join(repoRoot, "trace.log"), "utf8")).toBe("before\nafter\n");
    expect(store.getRun("manual-run")).toMatchObject({
      status: "completed",
      currentStepId: null,
      context: { workflow }
    });
    expect(store.getRun("manual-run")?.context.waiting).toBeUndefined();
    expect(store.listEvents("manual-run").map((event) => event.type)).toContain("manual_gate.paused");

    const database = new Database(store.databasePath, { readonly: true });
    expect(database.query(
      "SELECT status, decided_by, decision FROM approvals WHERE run_id = ?"
    ).get("manual-run")).toEqual({
      status: "approved",
      decided_by: "maintainer",
      decision: "approve"
    });
    database.close();
    store.close();
  });

  test("honors a declared cancellation without starting later steps", async () => {
    const repoRoot = temporaryRepo();
    const store = await openAgentflowRunState({ cwd: repoRoot });
    const workflow = parseAgentflowWorkflowOrThrow(`
name: cancelled-gate
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: gate, type: manual_gate, message: Continue?, options: [approve, cancel] }
  - { id: never, type: command, command: "printf 'unexpected' > unexpected.txt" }
`);
    createAgentflowLifecycleRun(store, { id: "cancelled-run", workflow });
    expect((await executeAgentflowCommandPipeline(store, "cancelled-run", workflow)).status).toBe("paused");

    const cancelled = await resumeAgentflowCommandPipeline(
      store,
      "cancelled-run",
      workflow,
      { outcome: "cancel" }
    );

    expect(cancelled).toMatchObject({ status: "cancelled", completedSteps: ["gate"] });
    expect(store.getRun("cancelled-run")?.status).toBe("cancelled");
    expect(fs.existsSync(path.join(repoRoot, "unexpected.txt"))).toBe(false);
    store.close();
  });

  test("honors terminal failure outcomes instead of falling through", async () => {
    const repoRoot = temporaryRepo();
    const store = await openAgentflowRunState({ cwd: repoRoot });
    const workflow = parseAgentflowWorkflowOrThrow(`
name: failed-gate
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: gate, type: manual_gate, message: Continue?, options: [approve, fail, cancel] }
  - { id: never, type: command, command: "printf 'unexpected' > unexpected.txt" }
`);
    createAgentflowLifecycleRun(store, { id: "failed-run", workflow });
    await executeAgentflowCommandPipeline(store, "failed-run", workflow);

    const failed = await resumeAgentflowCommandPipeline(
      store,
      "failed-run",
      workflow,
      { outcome: "fail" }
    );

    expect(failed).toMatchObject({ status: "failed", completedSteps: ["gate"] });
    expect(fs.existsSync(path.join(repoRoot, "unexpected.txt"))).toBe(false);
    store.close();
  });

  test("restores fractional step-attempt limits when resuming", async () => {
    const repoRoot = temporaryRepo();
    const store = await openAgentflowRunState({ cwd: repoRoot });
    const workflow = parseAgentflowWorkflowOrThrow(`
name: fractional-resume-budget
version: 1
style: pipeline
maturity: experimental
limits: { max_step_attempts: { after: 0.5 } }
steps:
  - { id: gate, type: manual_gate, message: Continue?, options: [approve, cancel] }
  - { id: after, type: command, command: echo after }
`);
    createAgentflowLifecycleRun(store, { id: "fractional-resume-run", workflow });
    expect((await executeAgentflowCommandPipeline(store, "fractional-resume-run", workflow)).status).toBe("paused");

    const resumed = await resumeAgentflowCommandPipeline(
      store,
      "fractional-resume-run",
      workflow,
      { outcome: "approve" }
    );

    expect(resumed).toMatchObject({
      status: "paused",
      failedStep: "gate",
      message: "Step gate cannot route to after because limits.max_step_attempts allows 0.5 attempt(s)."
    });
    expect(store.getRun("fractional-resume-run")?.status).toBe("paused");
    store.close();
  });

  test("persists an input answer as the declared artifact and continues once", async () => {
    const repoRoot = temporaryRepo();
    const store = await openAgentflowRunState({ cwd: repoRoot });
    const workflow = parseAgentflowWorkflowOrThrow(`
name: requested-input
version: 1
style: pipeline
maturity: experimental
steps:
  - id: details
    type: input_request
    question: Which deployment target?
    save_as: answers/target.json
  - { id: finish, type: command, command: "printf 'done\\n' > finished.txt" }
`);
    createAgentflowLifecycleRun(store, { id: "input-run", workflow });

    const paused = await executeAgentflowCommandPipeline(store, "input-run", workflow);
    expect(paused).toMatchObject({ status: "paused", completedSteps: [] });
    expect(store.getArtifact("input-run", "answers/target.json")).toBeNull();

    await expect(resumeAgentflowCommandPipeline(
      store,
      "input-run",
      workflow,
      { outcome: "approve" }
    )).rejects.toMatchObject({ code: "AGENTFLOW_INPUT_ANSWER_REQUIRED" });
    const completed = await resumeAgentflowCommandPipeline(
      store,
      "input-run",
      workflow,
      { answer: { environment: "staging", region: "us-east-1" } }
    );

    expect(completed).toEqual({
      status: "completed",
      completedSteps: ["details", "finish"]
    });
    expect(store.readArtifact("input-run", "answers/target.json").content.toString()).toBe(
      '{"environment":"staging","region":"us-east-1"}\n'
    );
    expect(store.getArtifact("input-run", "answers/target.json")).toMatchObject({
      producerStepId: "details",
      kind: "input_request",
      contentType: "application/json; charset=utf-8"
    });
    expect(fs.readFileSync(path.join(repoRoot, "finished.txt"), "utf8")).toBe("done\n");
    store.close();
  });

  test("honors declared overwrite ownership for repeated answer paths", async () => {
    const repoRoot = temporaryRepo();
    const store = await openAgentflowRunState({ cwd: repoRoot });
    const workflow = parseAgentflowWorkflowOrThrow(`
name: overwritten-input
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: first, type: input_request, question: First?, save_as: answer.md }
  - { id: second, type: input_request, question: Second?, save_as: answer.md, overwrite: true }
`);
    createAgentflowLifecycleRun(store, { id: "overwrite-run", workflow });
    await executeAgentflowCommandPipeline(store, "overwrite-run", workflow);
    await resumeAgentflowCommandPipeline(store, "overwrite-run", workflow, { answer: "first" });
    const firstArtifact = store.getArtifact("overwrite-run", "answer.md");

    const completed = await resumeAgentflowCommandPipeline(
      store,
      "overwrite-run",
      workflow,
      { answer: "second" }
    );

    expect(completed).toMatchObject({ status: "completed", completedSteps: ["first", "second"] });
    expect(store.readArtifact("overwrite-run", "answer.md").content.toString()).toBe("second");
    expect(store.getArtifact("overwrite-run", "answer.md")).toMatchObject({
      id: firstArtifact?.id,
      producerStepId: "second",
      status: "overwritten"
    });
    store.close();
  });

  test("does not take ownership of an identical foreign artifact", async () => {
    const repoRoot = temporaryRepo();
    const store = await openAgentflowRunState({ cwd: repoRoot });
    const workflow = parseAgentflowWorkflowOrThrow(`
name: protected-input-artifact
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: details, type: input_request, question: Target?, save_as: answer.md }
`);
    createAgentflowLifecycleRun(store, { id: "protected-artifact-run", workflow });
    await executeAgentflowCommandPipeline(store, "protected-artifact-run", workflow);
    store.writeArtifact({
      id: "fixture-answer",
      runId: "protected-artifact-run",
      stepId: "fixture",
      path: "answer.md",
      kind: "fixture",
      contentType: "text/plain; charset=utf-8",
      content: "staging"
    });

    await expect(resumeAgentflowCommandPipeline(
      store,
      "protected-artifact-run",
      workflow,
      { answer: "staging" }
    )).rejects.toBeInstanceOf(AgentflowRunStateError);
    expect(store.getRun("protected-artifact-run")?.status).toBe("paused");
    expect(store.getArtifact("protected-artifact-run", "answer.md")).toMatchObject({
      id: "fixture-answer",
      producerStepId: "fixture",
      kind: "fixture"
    });
    store.close();
  });

  test("closes the waiting step and approval when the run is cancelled", async () => {
    const repoRoot = temporaryRepo();
    const store = await openAgentflowRunState({ cwd: repoRoot });
    const workflow = parseAgentflowWorkflowOrThrow(`
name: cancelled-waiting-gate
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: gate, type: manual_gate, message: Continue?, options: [approve, cancel] }
`);
    createAgentflowLifecycleRun(store, { id: "cancel-waiting-run", workflow });
    await executeAgentflowCommandPipeline(store, "cancel-waiting-run", workflow);

    const cancelled = transitionAgentflowLifecycleRun(store, "cancel-waiting-run", "cancel");

    expect(cancelled).toMatchObject({
      changed: true,
      run: { status: "cancelled", currentStepId: null }
    });
    expect(cancelled.run.context.waiting).toBeUndefined();
    const database = new Database(store.databasePath, { readonly: true });
    expect(database.query(
      "SELECT status FROM run_steps WHERE run_id = ? AND step_id = ?"
    ).get("cancel-waiting-run", "gate")).toEqual({ status: "cancelled" });
    expect(database.query(
      "SELECT status, decision FROM approvals WHERE run_id = ?"
    ).get("cancel-waiting-run")).toEqual({ status: "cancelled", decision: "cancel" });
    database.close();
    store.close();
  });

  test("resolves interaction prompts only when routing reaches their step", async () => {
    const repoRoot = temporaryRepo();
    const store = await openAgentflowRunState({ cwd: repoRoot });
    const workflow = parseAgentflowWorkflowOrThrow(`
name: skipped-gate
version: 1
style: pipeline
maturity: experimental
inputs:
  skip: { required: true }
  optional_prompt: { required: false }
steps:
  - { id: route, type: condition, if: inputs.skip == true, then: complete, else: gate }
  - { id: gate, type: manual_gate, message: "{{ inputs.optional_prompt }}", options: [approve, cancel] }
`);
    createAgentflowLifecycleRun(store, {
      id: "skipped-gate-run",
      workflow,
      inputs: { skip: true }
    });

    expect(await executeAgentflowCommandPipeline(store, "skipped-gate-run", workflow)).toEqual({
      status: "completed",
      completedSteps: ["route"]
    });
    expect(store.getRun("skipped-gate-run")?.status).toBe("completed");
    store.close();
  });

  test("resolves declared prompt inputs literally and rejects invalid interaction declarations", async () => {
    const repoRoot = temporaryRepo();
    const store = await openAgentflowRunState({ cwd: repoRoot });
    const dynamic = parseAgentflowWorkflowOrThrow(`
name: dynamic-gate
version: 1
style: pipeline
maturity: experimental
inputs: { prompt: { required: true } }
steps:
  - { id: gate, type: manual_gate, message: "{{ inputs.prompt }}", options: [approve, cancel] }
`);
    const invalidPath = parseAgentflowWorkflowOrThrow(`
name: invalid-answer-path
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: answer, type: input_request, question: Answer?, save_as: ../outside.md }
`);
    const unsupportedPrompt = parseAgentflowWorkflowOrThrow(`
name: invalid-interaction-prompt
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: gate, type: manual_gate, message: "Review {{ artifacts.report }}?", options: [approve, cancel] }
  - { id: answer, type: input_request, question: "Answer {{ sessions.writer }}?", save_as: answer.md }
`);

    expect(validateAgentflowWorkflow(dynamic)).toEqual({ valid: true, errors: [] });
    expect(validateAgentflowWorkflow(invalidPath).errors.map((issue) => issue.code)).toContain(
      "workflow.input_request.save_as.invalid"
    );
    expect(validateAgentflowWorkflow(unsupportedPrompt).errors.map((issue) => issue.code)).toEqual([
      "workflow.manual_gate.message.expression.unsupported",
      "workflow.input_request.question.expression.unsupported"
    ]);
    createAgentflowLifecycleRun(store, {
      id: "dynamic-run",
      workflow: dynamic,
      inputs: { prompt: "Deploy {{ user }} to staging?" }
    });
    expect(await executeAgentflowCommandPipeline(store, "dynamic-run", dynamic)).toMatchObject({
      status: "paused",
      message: "Manual gate gate is waiting for one of: approve, cancel."
    });
    expect(store.getRun("dynamic-run")?.context.waiting).toMatchObject({
      prompt: "Deploy {{ user }} to staging?"
    });
    expect(() => createAgentflowLifecycleRun(store, { id: "invalid-path-run", workflow: invalidPath })).toThrow(
      "workflow.input_request.save_as.invalid"
    );
    expect(store.getRun("dynamic-run")?.status).toBe("paused");
    expect(store.getRun("invalid-path-run")).toBeNull();
    store.close();
  });
});

function temporaryRepo(): string {
  const repoRoot = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agentflow-interaction-"));
  fs.mkdirSync(path.join(repoRoot, ".git"));
  return repoRoot;
}
