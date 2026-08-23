import { Database } from "bun:sqlite";
import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import path from "node:path";
import {
  AgentFlowRunStateError,
  createAgentFlowLifecycleRun,
  executeAgentFlowCommandPipeline,
  openAgentFlowRunState,
  parseAgentFlowWorkflowOrThrow,
  resumeAgentFlowCommandPipeline,
  transitionAgentFlowLifecycleRun,
  validateAgentFlowWorkflow
} from "../../src/runtime";

describe("Agent Flow manual gates and input requests", () => {
  test("captures malformed persisted interaction attempts as failure payloads", async () => {
    const repoRoot = temporaryRepo();
    const parsed = parseAgentFlowWorkflowOrThrow(`
name: malformed-interaction
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: gate, type: manual_gate, message: Review?, options: [approve] }
`);
    const workflow = {
      ...parsed,
      steps: [{ ...parsed.steps[0]!, message: 123 }]
    } as unknown as typeof parsed;
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    store.createRunWithEvent({
      id: "malformed-interaction",
      workflow: {
        name: workflow.name,
        version: workflow.version,
        style: workflow.style,
        maturity: workflow.maturity
      },
      context: { workflow: workflow as never }
    }, { type: "run.created", payload: { status: "pending" } });

    const result = await executeAgentFlowCommandPipeline(store, "malformed-interaction", workflow);

    expect(result).toMatchObject({ status: "failed", failedStep: "gate" });
    const failure = store.listFailures("malformed-interaction")[0]!;
    const failurePath = failure.payloadPath;
    expect(failure).toMatchObject({
      stepId: "gate",
      classification: "interaction_failure",
      attempt: 1,
      outcome: "fail",
      payloadPath: expect.any(String)
    });
    expect(JSON.parse(store.readArtifact(
      "malformed-interaction",
      failurePath!
    ).content.toString("utf8"))).toMatchObject({
      step_id: "gate",
      step_type: "manual_gate",
      classification: "interaction_failure"
    });
    store.close();
  });

  test("pauses at a manual gate, rejects invalid outcomes, and resumes from that step", async () => {
    const repoRoot = temporaryRepo();
    let now = "2026-07-23T12:00:00.000Z";
    const store = await openAgentFlowRunState({ cwd: repoRoot, now: () => now });
    const workflow = parseAgentFlowWorkflowOrThrow(`
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
    createAgentFlowLifecycleRun(store, { id: "manual-run", workflow });

    const paused = await executeAgentFlowCommandPipeline(store, "manual-run", workflow);

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
    expect(() => transitionAgentFlowLifecycleRun(store, "manual-run", "resume")).toThrow(
      "waiting for an explicit manual-gate outcome"
    );

    await expect(resumeAgentFlowCommandPipeline(
      store,
      "manual-run",
      workflow,
      { outcome: "ship" }
    )).rejects.toBeInstanceOf(AgentFlowRunStateError);
    expect(store.getRun("manual-run")?.status).toBe("paused");
    expect(fs.readFileSync(path.join(repoRoot, "trace.log"), "utf8")).toBe("before\n");

    await expect(resumeAgentFlowCommandPipeline(
      store,
      "manual-run",
      workflow,
      { outcome: "approve", decidedBy: " " }
    )).rejects.toMatchObject({ code: "AGENT_FLOW_INTERACTION_INVALID" });
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
    const stillPaused = await resumeAgentFlowCommandPipeline(
      store,
      "manual-run",
      workflow,
      { outcome: "pause" }
    );
    expect(stillPaused.status).toBe("paused");
    expect(store.getRun("manual-run")?.context.waiting).toBeDefined();
    expect(store.getRun("manual-run")?.updatedAt).not.toBe(initiallyPausedAt);
    expect(store.getRun("manual-run")?.updatedAt).toBe(now);

    const completed = await resumeAgentFlowCommandPipeline(
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

  test("rolls back a resumed decision when its routing checkpoint fails", async () => {
    const repoRoot = temporaryRepo();
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: resume-checkpoint-rollback
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: gate, type: manual_gate, message: Continue?, options: [approve, cancel] }
  - { id: after, type: command, command: echo after >> effects.txt }
`);
    const runId = "resume-checkpoint-rollback";
    createAgentFlowLifecycleRun(store, { id: runId, workflow });
    await expect(executeAgentFlowCommandPipeline(store, runId, workflow))
      .resolves.toMatchObject({ status: "paused" });

    const updateRun = store.updateRun.bind(store);
    let resumeUpdates = 0;
    store.updateRun = ((id, input) => {
      resumeUpdates += 1;
      if (resumeUpdates === 2) {
        throw new AgentFlowRunStateError(
          "checkpoint contention",
          "AGENT_FLOW_CONCURRENT_MUTATION"
        );
      }
      return updateRun(id, input);
    }) as typeof store.updateRun;

    await expect(resumeAgentFlowCommandPipeline(store, runId, workflow, { outcome: "approve" }))
      .rejects.toMatchObject({ code: "AGENT_FLOW_CONCURRENT_MUTATION" });
    expect(store.getRun(runId)).toMatchObject({
      status: "paused",
      context: { waiting: { stepId: "gate" } }
    });
    expect(store.latestStepRecoveryState(runId, "gate")).toMatchObject({ status: "waiting" });

    store.updateRun = updateRun;
    await expect(resumeAgentFlowCommandPipeline(store, runId, workflow, { outcome: "approve" }))
      .resolves.toMatchObject({ status: "completed" });
    expect(fs.readFileSync(path.join(repoRoot, "effects.txt"), "utf8")).toBe("after\n");
    store.close();
  });

  test("honors a declared cancellation without starting later steps", async () => {
    const repoRoot = temporaryRepo();
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: cancelled-gate
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: gate, type: manual_gate, message: Continue?, options: [approve, cancel] }
  - { id: never, type: command, command: "printf 'unexpected' > unexpected.txt" }
`);
    createAgentFlowLifecycleRun(store, { id: "cancelled-run", workflow });
    expect((await executeAgentFlowCommandPipeline(store, "cancelled-run", workflow)).status).toBe("paused");

    const cancelled = await resumeAgentFlowCommandPipeline(
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
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: failed-gate
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: gate, type: manual_gate, message: Continue?, options: [approve, fail, cancel] }
  - { id: never, type: command, command: "printf 'unexpected' > unexpected.txt" }
`);
    createAgentFlowLifecycleRun(store, { id: "failed-run", workflow });
    await executeAgentFlowCommandPipeline(store, "failed-run", workflow);

    const failed = await resumeAgentFlowCommandPipeline(
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
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: fractional-resume-budget
version: 1
style: pipeline
maturity: experimental
limits: { max_step_attempts: { after: 0.5 } }
steps:
  - { id: gate, type: manual_gate, message: Continue?, options: [approve, cancel] }
  - { id: after, type: command, command: echo after }
`);
    createAgentFlowLifecycleRun(store, { id: "fractional-resume-run", workflow });
    expect((await executeAgentFlowCommandPipeline(store, "fractional-resume-run", workflow)).status).toBe("paused");

    const resumed = await resumeAgentFlowCommandPipeline(
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
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    const workflow = parseAgentFlowWorkflowOrThrow(`
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
    createAgentFlowLifecycleRun(store, { id: "input-run", workflow });

    const paused = await executeAgentFlowCommandPipeline(store, "input-run", workflow);
    expect(paused).toMatchObject({ status: "paused", completedSteps: [] });
    expect(store.getArtifact("input-run", "answers/target.json")).toBeNull();

    await expect(resumeAgentFlowCommandPipeline(
      store,
      "input-run",
      workflow,
      { outcome: "approve" }
    )).rejects.toMatchObject({ code: "AGENT_FLOW_INPUT_ANSWER_REQUIRED" });
    const completed = await resumeAgentFlowCommandPipeline(
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
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: overwritten-input
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: first, type: input_request, question: First?, save_as: answer.md }
  - { id: second, type: input_request, question: Second?, save_as: answer.md, overwrite: true }
`);
    createAgentFlowLifecycleRun(store, { id: "overwrite-run", workflow });
    await executeAgentFlowCommandPipeline(store, "overwrite-run", workflow);
    await resumeAgentFlowCommandPipeline(store, "overwrite-run", workflow, { answer: "first" });
    const firstArtifact = store.getArtifact("overwrite-run", "answer.md");

    const completed = await resumeAgentFlowCommandPipeline(
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
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: protected-input-artifact
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: details, type: input_request, question: Target?, save_as: answer.md }
`);
    createAgentFlowLifecycleRun(store, { id: "protected-artifact-run", workflow });
    await executeAgentFlowCommandPipeline(store, "protected-artifact-run", workflow);
    store.writeArtifact({
      id: "fixture-answer",
      runId: "protected-artifact-run",
      stepId: "fixture",
      path: "answer.md",
      kind: "fixture",
      contentType: "text/plain; charset=utf-8",
      content: "staging"
    });

    await expect(resumeAgentFlowCommandPipeline(
      store,
      "protected-artifact-run",
      workflow,
      { answer: "staging" }
    )).rejects.toBeInstanceOf(AgentFlowRunStateError);
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
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: cancelled-waiting-gate
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: gate, type: manual_gate, message: Continue?, options: [approve, cancel] }
`);
    createAgentFlowLifecycleRun(store, { id: "cancel-waiting-run", workflow });
    await executeAgentFlowCommandPipeline(store, "cancel-waiting-run", workflow);
    const competitor = await openAgentFlowRunState({ cwd: repoRoot });

    const cancelled = transitionAgentFlowLifecycleRun(competitor, "cancel-waiting-run", "cancel");

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
    competitor.close();
    store.close();
  });

  test("resolves interaction prompts only when routing reaches their step", async () => {
    const repoRoot = temporaryRepo();
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    const workflow = parseAgentFlowWorkflowOrThrow(`
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
    createAgentFlowLifecycleRun(store, {
      id: "skipped-gate-run",
      workflow,
      inputs: { skip: true }
    });

    expect(await executeAgentFlowCommandPipeline(store, "skipped-gate-run", workflow)).toEqual({
      status: "completed",
      completedSteps: ["route"]
    });
    expect(store.getRun("skipped-gate-run")?.status).toBe("completed");
    store.close();
  });

  test("resolves declared prompt inputs literally and rejects invalid interaction declarations", async () => {
    const repoRoot = temporaryRepo();
    const store = await openAgentFlowRunState({ cwd: repoRoot });
    const dynamic = parseAgentFlowWorkflowOrThrow(`
name: dynamic-gate
version: 1
style: pipeline
maturity: experimental
inputs: { prompt: { required: true } }
steps:
  - { id: gate, type: manual_gate, message: "{{ inputs.prompt }}", options: [approve, cancel] }
`);
    const invalidPath = parseAgentFlowWorkflowOrThrow(`
name: invalid-answer-path
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: answer, type: input_request, question: Answer?, save_as: ../outside.md }
`);
    const unsupportedPrompt = parseAgentFlowWorkflowOrThrow(`
name: invalid-interaction-prompt
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: gate, type: manual_gate, message: "Review {{ artifacts.report }}?", options: [approve, cancel] }
  - { id: answer, type: input_request, question: "Answer {{ sessions.writer }}?", save_as: answer.md }
`);

    expect(validateAgentFlowWorkflow(dynamic)).toEqual({ valid: true, errors: [] });
    expect(validateAgentFlowWorkflow(invalidPath).errors.map((issue) => issue.code)).toContain(
      "workflow.input_request.save_as.invalid"
    );
    expect(validateAgentFlowWorkflow(unsupportedPrompt).errors.map((issue) => issue.code)).toEqual([
      "workflow.manual_gate.message.expression.unsupported",
      "workflow.input_request.question.expression.unsupported"
    ]);
    createAgentFlowLifecycleRun(store, {
      id: "dynamic-run",
      workflow: dynamic,
      inputs: { prompt: "Deploy {{ user }} to staging?" }
    });
    expect(await executeAgentFlowCommandPipeline(store, "dynamic-run", dynamic)).toMatchObject({
      status: "paused",
      message: "Manual gate gate is waiting for one of: approve, cancel."
    });
    expect(store.getRun("dynamic-run")?.context.waiting).toMatchObject({
      prompt: "Deploy {{ user }} to staging?"
    });
    expect(() => createAgentFlowLifecycleRun(store, { id: "invalid-path-run", workflow: invalidPath })).toThrow(
      "workflow.input_request.save_as.invalid"
    );
    expect(store.getRun("dynamic-run")?.status).toBe("paused");
    expect(store.getRun("invalid-path-run")).toBeNull();
    store.close();
  });
});

function temporaryRepo(): string {
  const repoRoot = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-interaction-"));
  fs.mkdirSync(path.join(repoRoot, ".git"));
  return repoRoot;
}
