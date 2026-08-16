import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  createAgentFlowLifecycleRun,
  createAgentFlowSessionProviderRegistry,
  executeAgentFlowCommandPipeline,
  lintAgentFlowWorkflow,
  openAgentFlowRunState,
  parseAgentFlowSimulationFixture,
  parseAgentFlowWorkflowOrThrow,
  resumeAgentFlowCommandPipeline,
  simulateAgentFlowWorkflow,
  validateAgentFlowWorkflow,
  type AgentFlowSessionProviderRequest
} from "../../src/runtime";
import {
  AgentFlowDisagreementError,
  collectAgentFlowReviewCyclePathStepIds,
  collectAgentFlowReviewCycleStepIds,
  defaultAgentFlowDisagreementOutputPath,
  parseAgentFlowDisagreementResult
} from "../../src/runtime/disagreement";

describe("Agent Flow collaborative disagreement handling", () => {
  test("caps reviewer loops, persists user escalation, and resumes from a human resolution", async () => {
    const { root, store, workflow } = await disagreementRun("ask_user", "ask-user");
    const providers = reviewProvider(() => reviewResult("changes_requested", "Needs another revision."));

    const paused = await executeAgentFlowCommandPipeline(store, "ask-user", workflow, undefined, providers);

    expect(paused).toMatchObject({ status: "paused" });
    expect(paused.message).toContain("waiting for user resolution");
    expect(store.getRun("ask-user")?.context.waiting).toMatchObject({
      prompt: expect.stringContaining("approve, request_changes, fail, cancel")
    });
    expect(store.listEvents("ask-user").filter((event) => event.type === "collaboration.disagreement"))
      .toHaveLength(1);
    expect(store.listEvents("ask-user")).toContainEqual(expect.objectContaining({
      type: "collaboration.disagreement.waiting",
      payload: expect.objectContaining({ path: "user", strategy: "ask_user" })
    }));

    const completed = await resumeAgentFlowCommandPipeline(
      store,
      "ask-user",
      workflow,
      { outcome: "approve", decidedBy: "operator" },
      undefined,
      providers
    );

    expect(completed).toMatchObject({ status: "completed" });
    expect(JSON.parse(store.readArtifact("ask-user", "reviews/review.json").content.toString()))
      .toMatchObject({ status: "approved", summary: expect.stringContaining("User selected approve") });
    expect(store.getArtifact("ask-user", "reviews/review.json")?.metadata)
      .toMatchObject({ disagreementStrategy: "ask_user", disagreementRound: 1 });
    expect(store.listEvents("ask-user")).toContainEqual(expect.objectContaining({
      type: "collaboration.disagreement.resolved",
      payload: expect.objectContaining({ path: "user", outcome: "approve", decidedBy: "operator" })
    }));
    store.close();
    fs.rmSync(root, { recursive: true, force: true });
  });

  test("runs bounded arbiter rounds and falls back to the user in runtime and simulation", async () => {
    const policy = `
    strategy: arbiter_then_user
    arbiter: arbiter
    max_rounds: 2`;
    const { root, store, workflow } = await disagreementRun(policy, "arbiter-fallback", true);
    const requests: AgentFlowSessionProviderRequest[] = [];
    const providers = reviewProvider((request) => {
      requests.push(request);
      return request.sessionId === "arbiter"
        ? { status: "unresolved", rationale: "The evidence remains ambiguous." }
        : reviewResult("changes_requested", "Needs another revision.");
    });

    const paused = await executeAgentFlowCommandPipeline(store, "arbiter-fallback", workflow, undefined, providers);

    expect(paused.status).toBe("paused");
    expect(requests.map((request) => request.sessionId)).toEqual(["reviewer", "arbiter", "arbiter"]);
    expect(requests[1]!.inputs.map((input) => input.path)).toEqual([
      "implementation.md",
      "reviews/review.json"
    ]);
    expect(JSON.parse(Buffer.from(requests[1]!.inputs[1]!.content).toString("utf8"))).toMatchObject({
      status: "changes_requested",
      findings: [{ summary: "Needs another revision." }]
    });
    expect(store.listArtifacts("arbiter-fallback").filter((artifact) => artifact.kind === "disagreement_output"))
      .toHaveLength(2);
    expect(store.listEvents("arbiter-fallback").filter((event) => event.type === "collaboration.disagreement.round_completed"))
      .toHaveLength(2);

    const simulation = simulateAgentFlowWorkflow(workflow, {
      artifacts: { "implementation.md": "Implementation" },
      steps: {
        review: {
          outputs: { "reviews/review.json": reviewResult("changes_requested", "Needs another revision.") },
          disagreement: ["failed", "unresolved"],
          input: "approve"
        }
      }
    });
    expect(simulation.status).toBe("completed");
    expect(simulation.visitedSteps.filter((step) => step.type === "disagreement")).toHaveLength(3);
    expect(simulation.visitedSteps.some((step) => step.id === "should_skip")).toBe(false);
    expect(simulation.availableArtifacts).not.toContain(defaultAgentFlowDisagreementOutputPath("review", 1));
    expect(simulation.artifactValues[defaultAgentFlowDisagreementOutputPath("review", 2)]).toEqual({
      status: "unresolved",
      rationale: "arbiter left round 2 unresolved."
    });

    const completed = await resumeAgentFlowCommandPipeline(
      store,
      "arbiter-fallback",
      workflow,
      { outcome: "approve", decidedBy: "operator" },
      undefined,
      providers
    );
    expect(completed.status).toBe("completed");
    expect(store.getArtifact("arbiter-fallback", "reviews/review.json")?.metadata)
      .toMatchObject({ disagreementStrategy: "arbiter_then_user" });
    store.close();
    fs.rmSync(root, { recursive: true, force: true });
  });

  test("restarts disagreement handling instead of accepting changed paused evidence", async () => {
    const { root, store, workflow } = await disagreementRun("ask_user", "changed-evidence");
    const providers = reviewProvider(() => reviewResult("changes_requested", "Needs another revision."));
    expect((await executeAgentFlowCommandPipeline(store, "changed-evidence", workflow, undefined, providers)).status)
      .toBe("paused");
    store.writeArtifact({
      id: "implementation",
      runId: "changed-evidence",
      path: "implementation.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Changed while paused",
      overwrite: true
    });

    const resumed = await resumeAgentFlowCommandPipeline(
      store,
      "changed-evidence",
      workflow,
      { outcome: "approve", decidedBy: "operator" },
      undefined,
      providers
    );

    expect(resumed.status).toBe("paused");
    expect(JSON.parse(store.readArtifact("changed-evidence", "reviews/review.json").content.toString()))
      .toMatchObject({ status: "changes_requested" });
    expect(store.listEvents("changed-evidence")).toContainEqual(expect.objectContaining({
      type: "collaboration.disagreement.evidence_changed"
    }));
    expect(store.listEvents("changed-evidence").filter((event) => event.type === "collaboration.disagreement.waiting"))
      .toHaveLength(2);
    store.close();
    fs.rmSync(root, { recursive: true, force: true });
  });

  test("redacts resolver failures before persisting disagreement state and events", async () => {
    const policy = `
    strategy: arbiter_then_user
    arbiter: arbiter
    max_rounds: 1`;
    const { root, store, workflow } = await disagreementRun(policy, "redacted-resolver", true);
    const providers = reviewProvider((request) => {
      if (request.sessionId === "arbiter") throw new Error("token=resolver-secret");
      return reviewResult("changes_requested", "Needs another revision.");
    });

    const result = await executeAgentFlowCommandPipeline(store, "redacted-resolver", workflow, undefined, providers);

    expect(result.status).toBe("paused");
    const failedRound = store.listEvents("redacted-resolver")
      .find((event) => event.type === "collaboration.disagreement.round_failed");
    expect(failedRound?.payload).toMatchObject({
      message: expect.stringContaining("token=[REDACTED]")
    });
    expect(JSON.stringify(store.listEvents("redacted-resolver"))).not.toContain("resolver-secret");
    expect(store.getSession("redacted-resolver", "arbiter")?.state).toMatchObject({
      error: expect.stringContaining("token=[REDACTED]")
    });
    expect(JSON.stringify(store.getSession("redacted-resolver", "arbiter"))).not.toContain("resolver-secret");
    store.close();
    fs.rmSync(root, { recursive: true, force: true });
  });

  test("honors a model-budget pause before invoking an arbiter", async () => {
    const policy = `
    strategy: arbiter_then_user
    arbiter: arbiter
    max_rounds: 2`;
    const { root, store, workflow } = await disagreementRun(policy, "arbiter-budget", true, 1);
    const calls: string[] = [];
    const providers = reviewProvider((request) => {
      calls.push(request.sessionId);
      return reviewResult("changes_requested", "Needs another revision.");
    });

    const result = await executeAgentFlowCommandPipeline(store, "arbiter-budget", workflow, undefined, providers);

    expect(result.status).toBe("paused");
    expect(result.message).toContain('Budget "model_calls" would exceed its limit of 1');
    expect(calls).toEqual(["reviewer"]);
    expect(store.listEvents("arbiter-budget").some((event) => event.type === "collaboration.disagreement.waiting"))
      .toBe(false);
    const simulation = simulateAgentFlowWorkflow(workflow, {
      artifacts: { "implementation.md": "Implementation" },
      steps: {
        review: {
          outputs: { "reviews/review.json": reviewResult("changes_requested", "Needs another revision.") },
          disagreement: "approved"
        }
      }
    });
    expect(simulation.status).toBe("paused");
    expect(simulation.visitedSteps).toContainEqual(expect.objectContaining({
      id: "review:disagreement:arbiter:episode-1:round-1",
      outcome: "failed"
    }));
    store.close();
    fs.rmSync(root, { recursive: true, force: true });
  });

  test("runs disagreement handling before an equal review step-attempt limit", async () => {
    const { root, store, workflow } = await disagreementRun(
      "ask_user",
      "review-attempt-limit",
      false,
      undefined,
      1
    );
    const providers = reviewProvider(() => reviewResult("changes_requested", "Needs another revision."));

    const result = await executeAgentFlowCommandPipeline(store, "review-attempt-limit", workflow, undefined, providers);

    expect(result.status).toBe("paused");
    expect(result.message).toContain("waiting for user resolution");
    expect(store.listEvents("review-attempt-limit").some((event) => event.type === "recovery.limit_reached"))
      .toBe(false);
    store.close();
    fs.rmSync(root, { recursive: true, force: true });

    const stricter = await disagreementRun(
      "ask_user",
      "stricter-review-attempt-limit",
      false,
      undefined,
      1,
      2
    );
    const stricterResult = await executeAgentFlowCommandPipeline(
      stricter.store,
      "stricter-review-attempt-limit",
      stricter.workflow,
      undefined,
      providers
    );
    expect(stricterResult.status).toBe("paused");
    expect(stricterResult.message).toContain("max_step_attempts allows 1 attempt");
    expect(stricter.store.listEvents("stricter-review-attempt-limit")
      .some((event) => event.type === "collaboration.disagreement.waiting")).toBe(false);
    stricter.store.close();
    fs.rmSync(stricter.root, { recursive: true, force: true });

    const twoRounds = await disagreementRun(
      "ask_user",
      "two-review-rounds",
      false,
      undefined,
      2,
      2
    );
    const twoRoundResult = await executeAgentFlowCommandPipeline(
      twoRounds.store,
      "two-review-rounds",
      twoRounds.workflow,
      undefined,
      providers
    );
    expect(twoRoundResult.status).toBe("paused");
    expect(twoRoundResult.message).toContain("waiting for user resolution");
    expect(twoRounds.store.listEvents("two-review-rounds")
      .some((event) => event.type === "recovery.limit_reached")).toBe(false);
    twoRounds.store.close();
    fs.rmSync(twoRounds.root, { recursive: true, force: true });
  });

  test("keeps non-review subcycles under the generic runtime bound", async () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: disagreement-inner-cycle
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true, max_review_cycles: 3, on_disagreement: fail }
inputs: { keep_looping: { required: true } }
sessions:
  implementer: { provider: fixture, role: implementer }
  reviewer: { provider: fixture, role: reviewer, authority: { can_request_changes: true, can_approve: true } }
steps:
  - { id: review, type: review, reviewer: reviewer, subject: implementer, artifacts: [implementation.md], outputs: [reviews/review.json], then: detour }
  - { id: detour, type: command, command: "true", then: branch }
  - id: branch
    type: condition
    branches:
      - { if: inputs.keep_looping == true, then: detour }
    else: review
`);
    expect(validateAgentFlowWorkflow(workflow)).toEqual({ valid: true, errors: [] });
    expect([...collectAgentFlowReviewCycleStepIds(workflow.steps)]).toEqual(["review"]);
    expect([...collectAgentFlowReviewCyclePathStepIds(workflow.steps)].sort()).toEqual([
      "branch", "detour", "review"
    ]);
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "agent-flow-disagreement-inner-cycle-"));
    fs.mkdirSync(path.join(root, ".git"));
    fs.writeFileSync(path.join(root, "package.json"), "{}\n");
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "inner-cycle", workflow, inputs: { keep_looping: true } });
    store.writeArtifact({
      id: "implementation",
      runId: "inner-cycle",
      path: "implementation.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Implementation"
    });
    const providers = reviewProvider(() => reviewResult("changes_requested", "Needs another revision."));

    const result = await executeAgentFlowCommandPipeline(store, "inner-cycle", workflow, undefined, providers);

    expect(result.status).toBe("paused");
    expect(result.message).toContain("repeated route target detour");
    expect(store.listEvents("inner-cycle")
      .filter((event) => event.type === "step.completed" && event.stepId === "detour")).toHaveLength(1);

    const simulation = simulateAgentFlowWorkflow(workflow, {
      inputs: { keep_looping: true },
      artifacts: { "implementation.md": "Implementation" },
      steps: {
        review: { outputs: { "reviews/review.json": reviewResult("changes_requested", "Needs another revision.") } }
      }
    });
    expect(simulation.status).toBe("paused");
    expect(simulation.visitedSteps.filter((step) => step.id === "detour")).toHaveLength(1);
    store.close();
    fs.rmSync(root, { recursive: true, force: true });
  });

  test("keeps recovery review cycles under the recovery-pipeline bound", async () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: bounded-recovery-review
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_recovery_cycles: 1 }
policies: { recovery_limits: pause }
sessions:
  implementer: { provider: fixture, role: implementer }
  reviewer: { provider: fixture, role: reviewer, authority: { can_request_changes: true, can_approve: true } }
steps:
  - { id: review, type: review, reviewer: reviewer, subject: implementer, artifacts: [implementation.md], outputs: [reviews/review.json], then: route }
  - id: route
    type: condition
    branches:
      - { if: 'artifacts.reviews.review.status == "approved"', then: done }
      - { if: 'artifacts.reviews.review.status == "changes_requested"', then: revise }
    else: fail
  - { id: revise, type: command, command: "true", then: review }
  - { id: done, type: result, status: completed }
`);
    expect(validateAgentFlowWorkflow(workflow)).toEqual({ valid: true, errors: [] });
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "agent-flow-recovery-review-cycle-"));
    fs.mkdirSync(path.join(root, ".git"));
    fs.writeFileSync(path.join(root, "package.json"), "{}\n");
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "recovery-review-cycle", workflow });
    store.writeArtifact({
      id: "implementation",
      runId: "recovery-review-cycle",
      path: "implementation.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Implementation"
    });

    const result = await executeAgentFlowCommandPipeline(
      store,
      "recovery-review-cycle",
      workflow,
      undefined,
      reviewProvider(() => reviewResult("changes_requested", "Needs another revision."))
    );

    expect(result.status).toBe("paused");
    expect(result.message).toContain("exceeded limits.max_recovery_cycles 1");
    store.close();
    fs.rmSync(root, { recursive: true, force: true });
  });

  test("preserves review-cycle fallthrough for terminal success aliases", async () => {
    for (const route of ["then: continue", "goto: ignore"]) {
      const workflow = parseAgentFlowWorkflowOrThrow(`name: disagreement-terminal-fallthrough
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true, max_review_cycles: 1, on_disagreement: fail }
sessions:
  implementer: { provider: fixture, role: implementer }
  reviewer: { provider: fixture, role: reviewer, authority: { can_request_changes: true, can_approve: true } }
steps:
  - { id: review, type: review, reviewer: reviewer, subject: implementer, artifacts: [implementation.md], outputs: [reviews/review.json], ${route} }
  - { id: revise, type: command, command: "true", then: review }
`);
      expect(validateAgentFlowWorkflow(workflow)).toEqual({ valid: true, errors: [] });
      expect([...collectAgentFlowReviewCycleStepIds(workflow.steps)]).toEqual(["review"]);

      const root = fs.mkdtempSync(path.join(os.tmpdir(), "agent-flow-disagreement-terminal-fallthrough-"));
      fs.mkdirSync(path.join(root, ".git"));
      fs.writeFileSync(path.join(root, "package.json"), "{}\n");
      const store = await openAgentFlowRunState({ cwd: root });
      createAgentFlowLifecycleRun(store, { id: "terminal-fallthrough", workflow });
      store.writeArtifact({
        id: "implementation",
        runId: "terminal-fallthrough",
        path: "implementation.md",
        kind: "fixture",
        contentType: "text/markdown",
        content: "Implementation"
      });

      const result = await executeAgentFlowCommandPipeline(
        store,
        "terminal-fallthrough",
        workflow,
        undefined,
        reviewProvider(() => reviewResult("changes_requested", "Needs another revision."))
      );

      expect(result.status).toBe("failed");
      expect(result.message).toContain("disagreement strategy fail");
      expect(store.listEvents("terminal-fallthrough").some((event) => event.type === "recovery.limit_reached"))
        .toBe(false);
      store.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  test("treats approval continue aliases as review-cycle fallthrough", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: disagreement-approval-fallthrough
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true, max_review_cycles: 1, on_disagreement: fail }
sessions:
  implementer: { provider: fixture, role: implementer }
  reviewer: { provider: fixture, role: reviewer, authority: { can_request_changes: true, can_approve: true } }
  approver: { provider: fixture, role: approver, authority: { can_approve: true } }
steps:
  - { id: review, type: review, reviewer: reviewer, subject: implementer, artifacts: [implementation.md], outputs: [reviews/review.json], then: gate }
  - { id: gate, type: approval, reviewer: approver, artifacts: [implementation.md], on_approve: continue }
  - { id: revise, type: command, command: "true", then: review }
`);

    expect(validateAgentFlowWorkflow(workflow)).toEqual({ valid: true, errors: [] });
    expect([...collectAgentFlowReviewCycleStepIds(workflow.steps)]).toEqual(["review"]);
    expect([...collectAgentFlowReviewCyclePathStepIds(workflow.steps)].sort()).toEqual([
      "gate", "review", "revise"
    ]);
  });

  test("does not treat bounded loop repetition as a review disagreement cycle", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: bounded-loop-review
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true, max_review_cycles: 1 }
sessions:
  implementer: { provider: fixture, role: implementer }
  reviewer: { provider: fixture, role: reviewer, authority: { can_request_changes: true, can_approve: true } }
steps:
  - id: bounded
    type: loop
    max_iterations: 2
    body:
      - { id: review, type: review, reviewer: reviewer, subject: implementer, artifacts: [implementation.md], outputs: [reviews/review.json] }
  - { id: done, type: result, status: completed }
`);
    expect(validateAgentFlowWorkflow(workflow)).toEqual({ valid: true, errors: [] });
    expect([...collectAgentFlowReviewCycleStepIds(workflow.steps)]).toEqual([]);

    const simulation = simulateAgentFlowWorkflow(workflow, {
      artifacts: { "implementation.md": "Implementation" },
      steps: {
        bounded: { iterations: 2 },
        review: {
          outputs: { "reviews/review.json": reviewResult("changes_requested", "Needs another revision.") }
        }
      }
    });

    expect(simulation.status).toBe("completed");
    expect(simulation.unresolvedBranches).toEqual([]);
    expect(simulation.visitedSteps.filter((step) => step.id === "review")).toHaveLength(2);
  });

  test("persists an arbiter decision and uses it to leave the review loop", async () => {
    const policy = `
    strategy: arbiter
    arbiter: arbiter
    max_rounds: 1`;
    const { root, store, workflow } = await disagreementRun(policy, "arbiter-resolved", true);
    const providers = reviewProvider((request) => request.sessionId === "arbiter"
      ? { status: "resolved", decision: "approved", rationale: "The implementation satisfies the requirement." }
      : reviewResult("changes_requested", "Needs another revision."));

    const result = await executeAgentFlowCommandPipeline(store, "arbiter-resolved", workflow, undefined, providers);

    expect(result).toMatchObject({ status: "completed" });
    expect(JSON.parse(store.readArtifact("arbiter-resolved", "reviews/review.json").content.toString()))
      .toMatchObject({ status: "approved", summary: "The implementation satisfies the requirement." });
    expect(store.listEvents("arbiter-resolved")).toContainEqual(expect.objectContaining({
      type: "collaboration.disagreement.resolved",
      payload: expect.objectContaining({ path: "arbiter", decision: "approved", round: 1 })
    }));
    expect(store.listEvents("arbiter-resolved")).toContainEqual(expect.objectContaining({
      type: "step.completed",
      stepId: "review",
      payload: expect.objectContaining({ resolution: "approved", resolver: "arbiter", round: 1 })
    }));
    store.close();
    fs.rmSync(root, { recursive: true, force: true });
  });

  test("enforces session output limits on synthesized review decisions", async () => {
    const policy = `
    strategy: arbiter
    arbiter: arbiter
    max_rounds: 1`;
    const rationale = "x".repeat(6 * 1024 * 1024);
    const cases = [
      {
        runId: "oversized-synthesized-output",
        decision: "changes_requested" as const,
        outputs: ["reviews/review.json"],
        expectedMessage: "byte limit"
      },
      {
        runId: "oversized-synthesized-aggregate",
        decision: "approved" as const,
        outputs: ["reviews/review.json", "reviews/secondary.json"],
        expectedMessage: "aggregate limit"
      }
    ];

    for (const testCase of cases) {
      const { root, store, workflow } = await disagreementRun(
        policy,
        testCase.runId,
        true,
        undefined,
        undefined,
        1,
        testCase.outputs
      );
      const providers = reviewProvider((request) => request.sessionId === "arbiter"
        ? { status: "resolved", decision: testCase.decision, rationale }
        : reviewResult("changes_requested", "Needs another revision."));

      const result = await executeAgentFlowCommandPipeline(
        store,
        testCase.runId,
        workflow,
        undefined,
        providers
      );

      expect(result).toMatchObject({ status: "failed", failedStep: "review" });
      expect(store.listEvents(testCase.runId)).toContainEqual(expect.objectContaining({
        type: "collaboration.disagreement.round_failed",
        payload: expect.objectContaining({ message: expect.stringContaining(testCase.expectedMessage) })
      }));
      expect(JSON.parse(store.readArtifact(testCase.runId, "reviews/review.json").content.toString()))
        .toMatchObject({ status: "changes_requested", summary: "Needs another revision." });
      store.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  test("starts a fresh bounded resolution episode after requested changes", async () => {
    const policy = `
    strategy: arbiter
    arbiter: arbiter
    max_rounds: 1`;
    const { root, store, workflow } = await disagreementRun(policy, "arbiter-new-episode", true);
    let arbiterCalls = 0;
    const providers = reviewProvider((request) => {
      if (request.sessionId !== "arbiter") return reviewResult("changes_requested", "Needs another revision.");
      arbiterCalls += 1;
      return arbiterCalls === 1
        ? { status: "resolved", decision: "changes_requested", rationale: "Revise once more." }
        : { status: "resolved", decision: "approved", rationale: "The revision is sufficient." };
    });

    const result = await executeAgentFlowCommandPipeline(
      store, "arbiter-new-episode", workflow, undefined, providers
    );

    expect(result.status).toBe("completed");
    expect(arbiterCalls).toBe(2);
    const resolutionEvents = store.listEvents("arbiter-new-episode").filter((event) =>
      event.type === "collaboration.disagreement.resolved" && event.payload.path === "arbiter"
    );
    const firstResolution = defaultAgentFlowDisagreementOutputPath("review", 1);
    const secondResolution = defaultAgentFlowDisagreementOutputPath("review", 1, 2);
    expect(resolutionEvents).toHaveLength(2);
    expect(resolutionEvents.map((event) => event.payload.output)).toEqual([
      firstResolution,
      secondResolution
    ]);
    expect(JSON.parse(store.readArtifact("arbiter-new-episode", firstResolution).content.toString()))
      .toMatchObject({ status: "resolved", decision: "changes_requested", rationale: "Revise once more." });
    expect(JSON.parse(store.readArtifact("arbiter-new-episode", secondResolution).content.toString()))
      .toMatchObject({ status: "resolved", decision: "approved", rationale: "The revision is sufficient." });
    expect(JSON.parse(store.readArtifact("arbiter-new-episode", "reviews/review.json").content.toString()))
      .toMatchObject({ status: "approved", summary: "The revision is sufficient." });

    const simulation = simulateAgentFlowWorkflow(workflow, {
      artifacts: { "implementation.md": "Implementation" },
      steps: {
        review: {
          outputs: {
            "reviews/review.json": reviewResult("changes_requested", "Needs another revision.")
          },
          disagreement: ["changes_requested", "approved"]
        }
      }
    });
    expect(simulation.status).toBe("completed");
    expect(simulation.visitedSteps.filter((step) => step.type === "disagreement")).toHaveLength(2);
    expect(simulation.artifactValues[firstResolution])
      .toMatchObject({ status: "resolved", decision: "changes_requested" });
    expect(simulation.artifactValues[secondResolution])
      .toMatchObject({ status: "resolved", decision: "approved" });
    store.close();
    fs.rmSync(root, { recursive: true, force: true });
  });

  test("enforces aggregate resolver input limits during simulation", async () => {
    const policy = `
    strategy: arbiter
    arbiter: arbiter
    max_rounds: 1`;
    const { root, store, workflow } = await disagreementRun(
      policy,
      "arbiter-aggregate-simulation-limit",
      true
    );
    const largeInput = "x".repeat(5.5 * 1024 * 1024);

    const simulation = simulateAgentFlowWorkflow(workflow, {
      artifacts: { "implementation.md": largeInput },
      steps: {
        review: {
          outputs: {
            "reviews/review.json": {
              status: "changes_requested",
              findings: [{ summary: largeInput }],
              summary: "Change requested."
            }
          },
          disagreement: "approved"
        }
      }
    });

    expect(simulation.status).toBe("failed");
    expect(simulation.visitedSteps.filter((step) => step.type === "disagreement"))
      .toEqual([expect.objectContaining({ outcome: "failed" })]);
    store.close();
    fs.rmSync(root, { recursive: true, force: true });
  });

  test("does not apply merge approval gating to a disagreement resolver", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "agent-flow-disagreement-arbiter-"));
    fs.mkdirSync(path.join(root, ".git"));
    fs.writeFileSync(path.join(root, "package.json"), "{}\n");
    const workflow = reviewLoopWorkflow(`
    strategy: arbiter
    arbiter: arbiter
    max_rounds: 1`, true);
    const arbiter = workflow.sessions!.arbiter as Record<string, unknown>;
    arbiter.authority = { ...(arbiter.authority as Record<string, unknown>), can_merge: true };
    expect(validateAgentFlowWorkflow(workflow)).toEqual({ valid: true, errors: [] });
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "arbiter-with-stale-approval", workflow });
    store.writeArtifact({
      id: "implementation",
      runId: "arbiter-with-stale-approval",
      path: "implementation.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Implementation"
    });
    store.upsertApproval({
      id: "approval:prior:attempt-1",
      runId: "arbiter-with-stale-approval",
      stepId: "prior",
      status: "stale",
      decision: "Prior evidence changed."
    });
    let arbiterInvoked = false;
    const providers = reviewProvider((request) => {
      if (request.sessionId !== "arbiter") return reviewResult("changes_requested", "Needs another revision.");
      arbiterInvoked = true;
      return { status: "resolved", decision: "approved", rationale: "The implementation satisfies the requirement." };
    });

    const result = await executeAgentFlowCommandPipeline(
      store, "arbiter-with-stale-approval", workflow, undefined, providers
    );

    expect(arbiterInvoked).toBe(true);
    expect(result).toMatchObject({
      status: "failed",
      resultStatus: "completed",
      message: "Stale approval prior must be rerun before workflow completion."
    });
    expect(store.listEvents("arbiter-with-stale-approval")).toContainEqual(expect.objectContaining({
      type: "collaboration.disagreement.resolved",
      payload: expect.objectContaining({ path: "arbiter", decision: "approved" })
    }));
    store.close();
    fs.rmSync(root, { recursive: true, force: true });
  });

  test("does not replace a review output taken over by another step during disagreement", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "agent-flow-disagreement-ownership-"));
    fs.mkdirSync(path.join(root, ".git"));
    fs.writeFileSync(path.join(root, "package.json"), "{}\n");
    const workflow = reviewLoopWorkflow(`
    strategy: arbiter
    arbiter: arbiter
    max_rounds: 1`, true);
    expect(validateAgentFlowWorkflow(workflow)).toEqual({ valid: true, errors: [] });
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "foreign-review-output", workflow });
    store.writeArtifact({
      id: "implementation",
      runId: "foreign-review-output",
      path: "implementation.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Implementation"
    });
    let arbiterInvoked = false;
    const providers = reviewProvider((request) => {
      if (request.sessionId !== "arbiter") return reviewResult("changes_requested", "Needs another revision.");
      arbiterInvoked = true;
      return { status: "resolved", decision: "approved", rationale: "Approve the disputed work." };
    });
    const originalFinalization = store.withRunFinalizationTransaction.bind(store);
    let outputTakenOver = false;
    store.withRunFinalizationTransaction = ((runId, callback) => {
      if (arbiterInvoked && !outputTakenOver) {
        outputTakenOver = true;
        const existing = store.getArtifact(runId, "reviews/review.json");
        store.writeArtifact({
          id: existing!.id,
          runId,
          stepId: "revise",
          path: "reviews/review.json",
          kind: "review_output",
          contentType: "application/json; charset=utf-8",
          content: `${JSON.stringify(reviewResult("changes_requested", "Foreign revision"))}\n`,
          overwrite: true
        });
      }
      return originalFinalization(runId, callback);
    }) as typeof store.withRunFinalizationTransaction;

    const result = await executeAgentFlowCommandPipeline(
      store, "foreign-review-output", workflow, undefined, providers
    );

    expect(result).toMatchObject({ status: "failed", failedStep: "review" });
    expect(store.getArtifact("foreign-review-output", "reviews/review.json")?.producerStepId).toBe("revise");
    expect(JSON.parse(store.readArtifact("foreign-review-output", "reviews/review.json").content.toString()))
      .toMatchObject({ status: "changes_requested", summary: "Foreign revision" });
    expect(store.listEvents("foreign-review-output")).toContainEqual(expect.objectContaining({
      type: "collaboration.disagreement.round_failed",
      payload: expect.objectContaining({ message: expect.stringContaining("was overwritten") })
    }));
    store.close();
    fs.rmSync(root, { recursive: true, force: true });
  });

  test("does not publish a decision after its resolution artifact is replaced", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "agent-flow-disagreement-resolution-race-"));
    fs.mkdirSync(path.join(root, ".git"));
    fs.writeFileSync(path.join(root, "package.json"), "{}\n");
    const workflow = reviewLoopWorkflow(`
    strategy: arbiter
    arbiter: arbiter
    max_rounds: 1`, true);
    expect(validateAgentFlowWorkflow(workflow)).toEqual({ valid: true, errors: [] });
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "replaced-resolution", workflow });
    store.writeArtifact({
      id: "implementation",
      runId: "replaced-resolution",
      path: "implementation.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Implementation"
    });
    let arbiterInvoked = false;
    const providers = reviewProvider((request) => {
      if (request.sessionId !== "arbiter") return reviewResult("changes_requested", "Needs another revision.");
      arbiterInvoked = true;
      return { status: "resolved", decision: "approved", rationale: "Approve the disputed work." };
    });
    const resolutionPath = defaultAgentFlowDisagreementOutputPath("review", 1);
    const originalFinalization = store.withRunFinalizationTransaction.bind(store);
    let resolutionTakenOver = false;
    store.withRunFinalizationTransaction = ((runId, callback) => {
      if (arbiterInvoked && !resolutionTakenOver) {
        resolutionTakenOver = true;
        const existing = store.getArtifact(runId, resolutionPath);
        store.writeArtifact({
          id: existing!.id,
          runId,
          stepId: "external",
          path: resolutionPath,
          kind: "disagreement_output",
          contentType: "application/json; charset=utf-8",
          content: `${JSON.stringify({
            status: "resolved",
            decision: "changes_requested",
            rationale: "Replacement decision."
          })}\n`,
          overwrite: true
        });
      }
      return originalFinalization(runId, callback);
    }) as typeof store.withRunFinalizationTransaction;

    const result = await executeAgentFlowCommandPipeline(
      store, "replaced-resolution", workflow, undefined, providers
    );

    expect(result).toMatchObject({ status: "failed", failedStep: "review" });
    expect(JSON.parse(store.readArtifact("replaced-resolution", "reviews/review.json").content.toString()))
      .toMatchObject({ status: "changes_requested", summary: "Needs another revision." });
    expect(store.listEvents("replaced-resolution")).toContainEqual(expect.objectContaining({
      type: "collaboration.disagreement.round_failed",
      payload: expect.objectContaining({ message: expect.stringContaining(`${resolutionPath} was overwritten`) })
    }));
    store.close();
    fs.rmSync(root, { recursive: true, force: true });
  });

  test("rejects missing and malformed disagreement strategies for review cycles", () => {
    expect(() => parseAgentFlowDisagreementResult(null as never)).toThrow(AgentFlowDisagreementError);

    const missing = reviewLoopWorkflow(undefined);
    expect(validateAgentFlowWorkflow(missing).errors).toContainEqual({
      code: "workflow.collaboration.on_disagreement.required",
      message: "Collaborative review cycles must declare collaboration.on_disagreement with a terminal strategy.",
      path: "collaboration.on_disagreement"
    });

    const ignoredReviewTarget = parseAgentFlowWorkflowOrThrow(`name: ignored-review-target
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true, max_review_cycles: 1 }
sessions:
  owner: { provider: fixture, role: owner }
  reviewer: { provider: fixture, role: reviewer, authority: { can_approve: true, can_request_changes: true } }
steps:
  - { id: review, type: review, reviewer: reviewer, subject: owner, artifacts: [implementation.md], outputs: [reviews/review.json], on_approve: complete }
  - { id: revise, type: command, command: "true", then: review }
`);
    expect([...collectAgentFlowReviewCycleStepIds(ignoredReviewTarget.steps)]).toEqual(["review"]);
    expect(validateAgentFlowWorkflow(ignoredReviewTarget).errors).toContainEqual(expect.objectContaining({
      code: "workflow.collaboration.on_disagreement.required"
    }));

    const malformed = reviewLoopWorkflow(`
    strategy: arbiter_then_user
    arbiter: missing
    max_rounds: 0`);
    expect(validateAgentFlowWorkflow(malformed).errors).toContainEqual(expect.objectContaining({
      code: "workflow.collaboration.on_disagreement.invalid",
      path: "collaboration.on_disagreement"
    }));

    const undeclared = reviewLoopWorkflow(`
    strategy: arbiter
    arbiter: missing
    max_rounds: 1`);
    expect(validateAgentFlowWorkflow(undeclared).errors).toContainEqual(expect.objectContaining({
      code: "workflow.collaboration.on_disagreement.arbiter.undeclared"
    }));

    const unauthorized = reviewLoopWorkflow(`
    strategy: arbiter
    arbiter: arbiter
    max_rounds: 1`, true);
    unauthorized.sessions!.arbiter = { provider: "fixture", role: "arbiter" };
    expect(validateAgentFlowWorkflow(unauthorized).errors).toContainEqual(expect.objectContaining({
      code: "workflow.collaboration.on_disagreement.arbiter.authority.required"
    }));

    const partiallyAuthorized = reviewLoopWorkflow(`
    strategy: arbiter
    arbiter: arbiter
    max_rounds: 1`, true);
    partiallyAuthorized.sessions!.arbiter = {
      provider: "fixture",
      role: "arbiter",
      authority: { can_approve: true }
    };
    expect(validateAgentFlowWorkflow(partiallyAuthorized).errors).toContainEqual(expect.objectContaining({
      code: "workflow.collaboration.on_disagreement.arbiter.authority.required"
    }));

    const unauthorizedOwner = reviewLoopWorkflow("owner_decides");
    expect(validateAgentFlowWorkflow(unauthorizedOwner).errors).toContainEqual(expect.objectContaining({
      code: "workflow.collaboration.on_disagreement.owner.authority.required",
      path: "steps[0].subject"
    }));

    const tooManyResolverInputs = reviewLoopWorkflow(`
    strategy: arbiter
    arbiter: arbiter
    max_rounds: 1`, true);
    tooManyResolverInputs.steps[0]!.artifacts = Array.from({ length: 64 }, (_, index) => `inputs/${index}.md`);
    expect(validateAgentFlowWorkflow(tooManyResolverInputs).errors).toContainEqual(expect.objectContaining({
      code: "workflow.review.disagreement.inputs.limit",
      path: "steps[0].artifacts"
    }));

    expect(parseAgentFlowSimulationFixture(JSON.stringify({
      steps: { review: { disagreement: "maybe" } }
    }))).toEqual({
      ok: false,
      error: "Simulation fixture step review.disagreement must be approved, changes_requested, unresolved, failed, or a non-empty list of those values."
    });

    const reservedOutput = defaultAgentFlowDisagreementOutputPath("review", 1);
    for (const producer of ["review", "revise"]) {
      const collision = reviewLoopWorkflow(`
    strategy: arbiter
    arbiter: arbiter
    max_rounds: 1`, true);
      const step = collision.steps.find((candidate) => candidate.id === producer)!;
      step.outputs = [...(Array.isArray(step.outputs) ? step.outputs : []), reservedOutput];
      step.overwrite = true;
      expect(validateAgentFlowWorkflow(collision).errors).toContainEqual(expect.objectContaining({
        code: "workflow.artifact.output.disagreement_reserved",
        stepId: producer
      }));
    }

    const excessiveRounds = reviewLoopWorkflow(`
    strategy: arbiter
    arbiter: arbiter
    max_rounds: 1000000000`, true);
    expect(validateAgentFlowWorkflow(excessiveRounds).errors).toContainEqual(expect.objectContaining({
      code: "workflow.collaboration.on_disagreement.invalid",
      message: expect.stringContaining("max_rounds from 1 through 100")
    }));

    const maximumRoundCollision = reviewLoopWorkflow(`
    strategy: arbiter
    arbiter: arbiter
    max_rounds: 100`, true);
    maximumRoundCollision.steps.find((candidate) => candidate.id === "revise")!.outputs = [
      defaultAgentFlowDisagreementOutputPath("review", 100)
    ];
    expect(validateAgentFlowWorkflow(maximumRoundCollision).errors).toContainEqual(expect.objectContaining({
      code: "workflow.artifact.output.disagreement_reserved",
      stepId: "revise"
    }));

    const laterEpisodeCollision = reviewLoopWorkflow(`
    strategy: arbiter
    arbiter: arbiter
    max_rounds: 1`, true);
    laterEpisodeCollision.steps.find((candidate) => candidate.id === "revise")!.outputs = [
      defaultAgentFlowDisagreementOutputPath("review", 1, 2)
    ];
    expect(validateAgentFlowWorkflow(laterEpisodeCollision).errors).toContainEqual(expect.objectContaining({
      code: "workflow.artifact.output.disagreement_reserved",
      stepId: "revise"
    }));

    const directParallelCycle = parseAgentFlowWorkflowOrThrow(`name: direct-parallel-review-cycle
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true, max_review_cycles: 1 }
sessions:
  owner: { provider: fixture, role: owner }
  reviewer: { provider: fixture, role: reviewer, authority: { can_approve: true, can_request_changes: true } }
steps:
  - id: parallel
    type: parallel
    strategy: fail_fast
    branches:
      - { id: review, type: review, session: owner, reviewer: reviewer, subject: owner, artifacts: [implementation.md], outputs: [reviews/review.json], then: review }
`);
    expect(validateAgentFlowWorkflow(directParallelCycle).errors).toContainEqual(expect.objectContaining({
      code: "workflow.collaboration.on_disagreement.required"
    }));

    const terminalFallthrough = parseAgentFlowWorkflowOrThrow(`name: terminal-review-fallthrough
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true }
sessions:
  owner: { provider: fixture, role: owner }
  reviewer: { provider: fixture, role: reviewer, authority: { can_approve: true, can_request_changes: true } }
steps:
  - { id: review, type: review, reviewer: reviewer, subject: owner, artifacts: [implementation.md], outputs: [reviews/review.json], then: done }
  - { id: done, type: result, status: completed }
  - { id: unreachable, type: command, command: "true", outputs: [disagreements/review-c97ace4c8fef/round-1.json], then: review }
`);
    expect([...collectAgentFlowReviewCycleStepIds(terminalFallthrough.steps)]).toEqual([]);
    expect(validateAgentFlowWorkflow(terminalFallthrough)).toEqual({ valid: true, errors: [] });
  });

  test("simulates generated resolver artifacts without treating conditional rounds as guaranteed lint producers", () => {
    const workflow = reviewLoopWorkflow(`
    strategy: arbiter
    arbiter: arbiter
    max_rounds: 1`, true);
    const resolverOutput = defaultAgentFlowDisagreementOutputPath("review", 1);
    workflow.sessions!.consumer = { provider: "fixture", role: "consumer" };
    const route = workflow.steps.find((step) => step.id === "route")!;
    const approved = (route.branches as Array<Record<string, unknown>>)
      .find((branch) => String(branch.if).includes("approved"))!;
    approved.then = "consume";
    const doneIndex = workflow.steps.findIndex((step) => step.id === "done");
    workflow.steps.splice(doneIndex, 0, {
      id: "consume",
      type: "session_request",
      session: "consumer",
      prompt: "consume.md",
      inputs: [resolverOutput],
      outputs: ["consumed.md"],
      then: "done"
    });

    expect(validateAgentFlowWorkflow(workflow)).toEqual({ valid: true, errors: [] });
    expect(lintAgentFlowWorkflow(workflow).warnings).toContainEqual(expect.objectContaining({
      code: "workflow.lint.artifact.read_before_write",
      stepId: "consume"
    }));

    const result = simulateAgentFlowWorkflow(workflow, {
      artifacts: { "implementation.md": "Implementation" },
      steps: {
        review: {
          outputs: {
            "reviews/review.json": reviewResult("changes_requested", "Needs another revision.")
          },
          disagreement: "approved"
        },
        consume: { outputs: { "consumed.md": "consumed" } }
      }
    });

    expect(result.status).toBe("completed");
    expect(result.availableArtifacts).toContain(resolverOutput);
    expect(result.artifactValues[resolverOutput]).toEqual({
      status: "resolved",
      decision: "approved",
      rationale: "arbiter resolved round 1."
    });

    const collision = simulateAgentFlowWorkflow(workflow, {
      artifacts: {
        "implementation.md": "Implementation",
        [resolverOutput]: { foreign: true }
      },
      steps: {
        review: {
          outputs: {
            "reviews/review.json": reviewResult("changes_requested", "Needs another revision.")
          },
          disagreement: "approved"
        }
      }
    });
    expect(collision.status).toBe("unresolved");
    expect(collision.artifactValues[resolverOutput]).toEqual({ foreign: true });
    expect(collision.unresolvedBranches).toContainEqual(expect.objectContaining({
      stepId: "review",
      reason: expect.stringContaining("already exists")
    }));

    const ownershipWorkflow = reviewLoopWorkflow(`
    strategy: arbiter
    arbiter: arbiter
    max_rounds: 1`, true);
    const ownershipRoute = ownershipWorkflow.steps.find((step) => step.id === "route")!;
    const changesRequested = (ownershipRoute.branches as Array<Record<string, unknown>>)
      .find((branch) => String(branch.if).includes("changes_requested"))!;
    changesRequested.then = "takeover";
    const ownershipDone = ownershipWorkflow.steps.findIndex((step) => step.id === "done");
    ownershipWorkflow.steps.splice(ownershipDone, 0, {
      id: "takeover",
      type: "command",
      command: "true",
      outputs: ["reviews/review.json"],
      overwrite: true,
      then: "review"
    });
    expect(validateAgentFlowWorkflow(ownershipWorkflow)).toEqual({ valid: true, errors: [] });

    const ownership = simulateAgentFlowWorkflow(ownershipWorkflow, {
      artifacts: { "implementation.md": "Implementation" },
      steps: {
        review: {
          outputs: {
            "reviews/review.json": reviewResult("changes_requested", "Needs another revision.")
          },
          disagreement: "approved"
        },
        takeover: {
          outputs: {
            "reviews/review.json": reviewResult("changes_requested", "Foreign revision")
          }
        }
      }
    });
    expect(ownership.status).toBe("unresolved");
    expect(ownership.artifactValues["reviews/review.json"])
      .toMatchObject({ status: "changes_requested", summary: "Foreign revision" });
    expect(ownership.unresolvedBranches).toContainEqual(expect.objectContaining({
      stepId: "review",
      reason: expect.stringContaining("already exists")
    }));
  });

  test("preflights automated disagreement inputs before selecting a fixture decision", () => {
    const workflow = reviewLoopWorkflow(`
    strategy: arbiter
    arbiter: arbiter
    max_rounds: 1`, true);
    workflow.policies = { sensitive_inputs: "deny" };

    const result = simulateAgentFlowWorkflow(workflow, {
      artifacts: { "implementation.md": "Implementation" },
      steps: {
        review: {
          outputs: {
            "reviews/review.json": reviewResult("changes_requested", "API_TOKEN=resolver-input-secret")
          },
          disagreement: "approved"
        }
      }
    });

    expect(result.status).toBe("failed");
    expect(result.visitedSteps).toContainEqual({
      id: "review:disagreement:arbiter:episode-1:round-1",
      type: "disagreement",
      outcome: "failed"
    });
    expect(result.availableArtifacts).not.toContain(defaultAgentFlowDisagreementOutputPath("review", 1));

    const unsafeResolver = structuredClone(workflow);
    unsafeResolver.sessions!["API_TOKEN=resolver-secret"] = unsafeResolver.sessions!.arbiter!;
    delete unsafeResolver.sessions!.arbiter;
    (unsafeResolver.collaboration!.on_disagreement as Record<string, unknown>).arbiter = "API_TOKEN=resolver-secret";
    expect(simulateAgentFlowWorkflow(unsafeResolver, {
      artifacts: { "implementation.md": "Implementation" },
      steps: {
        review: {
          outputs: {
            "reviews/review.json": reviewResult("changes_requested", "Needs another revision.")
          },
          disagreement: "approved"
        }
      }
    })).toMatchObject({ status: "failed" });
  });
});

async function disagreementRun(
  policy: string,
  runId: string,
  includeArbiter = false,
  maxModelCalls?: number,
  maxReviewAttempts?: number,
  maxReviewCycles = 1,
  reviewOutputs?: string[]
) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "agent-flow-disagreement-"));
  fs.mkdirSync(path.join(root, ".git"));
  fs.writeFileSync(path.join(root, "package.json"), "{}\n");
  const workflow = reviewLoopWorkflow(policy, includeArbiter, maxModelCalls, maxReviewAttempts, maxReviewCycles);
  if (reviewOutputs !== undefined) {
    workflow.steps.find((step) => step.id === "review")!.outputs = reviewOutputs;
  }
  expect(validateAgentFlowWorkflow(workflow)).toEqual({ valid: true, errors: [] });
  const store = await openAgentFlowRunState({ cwd: root });
  createAgentFlowLifecycleRun(store, { id: runId, workflow });
  store.writeArtifact({
    id: "implementation",
    runId,
    path: "implementation.md",
    kind: "fixture",
    contentType: "text/markdown",
    content: "Implementation"
  });
  return { root, store, workflow };
}

function reviewLoopWorkflow(
  policy: string | undefined,
  includeArbiter = false,
  maxModelCalls?: number,
  maxReviewAttempts?: number,
  maxReviewCycles = 1
) {
  const limitEntries = [
    ...(maxModelCalls === undefined ? [] : [`max_model_calls: ${maxModelCalls}`]),
    ...(maxReviewAttempts === undefined ? [] : [`max_step_attempts: { review: ${maxReviewAttempts} }`])
  ];
  return parseAgentFlowWorkflowOrThrow(`name: disagreement-loop
version: 1
style: collaborative
maturity: experimental
${limitEntries.length === 0 ? "" : `limits: { ${limitEntries.join(", ")} }`}
collaboration:
  enabled: true
  max_review_cycles: ${maxReviewCycles}
  ${policy === undefined ? "" : `on_disagreement:${policy.startsWith("\n") ? policy : ` ${policy}`}`}
sessions:
  implementer: { provider: fixture, role: implementer }
  reviewer:
    provider: fixture
    role: reviewer
    authority: { can_request_changes: true, can_approve: true }
  ${includeArbiter ? "arbiter: { provider: fixture, role: arbiter, authority: { can_approve: true, can_request_changes: true } }" : ""}
steps:
  - id: review
    type: review
    reviewer: reviewer
    subject: implementer
    artifacts: [implementation.md]
    outputs: [reviews/review.json]
    then: route
  - { id: should_skip, type: result, status: failed }
  - id: route
    type: condition
    branches:
      - { if: 'artifacts.reviews.review.status == "approved"', then: done }
      - { if: 'artifacts.reviews.review.status == "changes_requested"', then: revise }
    else: fail
  - { id: revise, type: command, command: "true", then: review }
  - { id: done, type: result, status: completed }
`);
}

function reviewProvider(
  response: (request: AgentFlowSessionProviderRequest) => Record<string, unknown>
) {
  return createAgentFlowSessionProviderRegistry().register("fixture", (request) => ({
    outputs: Object.fromEntries(request.outputs.map((output) => [output, JSON.stringify(response(request))]))
  }));
}

function reviewResult(status: "approved" | "changes_requested" | "unresolved", summary: string) {
  return { status, findings: status === "changes_requested" ? [{ summary }] : [], summary };
}
