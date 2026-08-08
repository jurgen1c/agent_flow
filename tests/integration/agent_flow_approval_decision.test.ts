import { describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  createAgentFlowLifecycleRun,
  createAgentFlowSessionProviderRegistry,
  createAgentFlowWorkflowRegistry,
  defaultAgentFlowApprovalOutputPath,
  defaultAgentFlowDecisionRecordPath,
  executeAgentFlowApproval,
  executeAgentFlowCommandPipeline,
  executeAgentFlowDecisionRecord,
  executeAgentFlowSessionRequest,
  lintAgentFlowWorkflow,
  openAgentFlowRunState,
  parseAgentFlowWorkflowOrThrow,
  resumeAgentFlowCommandPipeline,
  simulateAgentFlowWorkflow,
  transitionAgentFlowLifecycleRun,
  validateAgentFlowWorkflow,
  type AgentFlowRunStateValue,
  type AgentFlowSessionProviderRequest
} from "../../src/runtime";

describe("Agent Flow approval and decision record steps", () => {
  test("persists a session approval and an inspectable retained decision record", async () => {
    const root = temporaryRepo();
    const workflow = sessionApprovalWorkflow();
    expect(validateAgentFlowWorkflow(workflow)).toEqual({ valid: true, errors: [] });
    const store = await openAgentFlowRunState({ cwd: root, now: () => "2026-08-04T12:00:00.000Z" });
    createAgentFlowLifecycleRun(store, { id: "session-approval", workflow });
    store.writeArtifact({
      id: "spec",
      runId: "session-approval",
      path: "spec.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Ship the durable decision contract."
    });
    const requests: AgentFlowSessionProviderRequest[] = [];
    let approvalPromptContent: string | undefined;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => {
      requests.push(request);
      approvalPromptContent = request.prompt.content;
      return {
        outputs: {
          "approvals/approve.json": JSON.stringify({ status: "approved", decision: "The evidence satisfies the contract." })
        }
      };
    });

    const result = await executeAgentFlowCommandPipeline(store, "session-approval", workflow, undefined, providers);

    expect(result).toMatchObject({
      status: "completed",
      completedSteps: ["approve", "record_decision", "done"]
    });
    expect(requests[0]).toMatchObject({
      stepId: "approve",
      sessionId: "reviewer",
      prompt: expect.objectContaining({
        content: expect.stringContaining("Approval criteria: Approve only when the durable contract is satisfied."),
        checksum: expect.stringMatching(/^sha256:[a-f0-9]{64}$/)
      }),
      outputs: ["approvals/approve.json"],
      inputs: [expect.objectContaining({ path: "spec.md" })]
    });
    expect(store.listApprovals("session-approval")).toEqual([
      expect.objectContaining({
        id: expect.stringMatching(/^approval:approve-[a-f0-9]{8}:attempt-1$/),
        status: "approved",
        requestedBy: "reviewer",
        decidedBy: "reviewer",
        decision: "The evidence satisfies the contract."
      })
    ]);
    expect(approvalPromptContent).toContain('{"status":"approved","decision":"non-empty rationale summary"}');
    expect(approvalPromptContent).toContain('{"status":"rejected","decision":"non-empty rationale summary"}');
    expect(approvalPromptContent).not.toContain('"approved|rejected"');
    const decisionArtifact = store.getArtifact("session-approval", "decision-records/record_decision.json");
    expect(decisionArtifact).toMatchObject({ kind: "decision_record", status: "available" });
    expect(JSON.parse(store.readArtifact("session-approval", decisionArtifact!.declaredPath).content.toString())).toEqual({
      decision_id: "decision:record_decision",
      owner: "reviewer",
      topic: "Ship approval and decision records",
      rationale_summary: "The approval outcome and source specification form the durable rationale.",
      consulted: [],
      approved_by: ["reviewer"],
      artifacts: ["spec.md", "approvals/approve.json"],
      created_at: "2026-08-04T12:00:00.000Z"
    });
    store.close();
  });

  test("pauses for human authority and stores the resumed outcome in run state", async () => {
    const root = temporaryRepo();
    const workflow = humanApprovalWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "human-approval", workflow });
    store.writeArtifact({
      id: "release",
      runId: "human-approval",
      path: "release.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Release candidate"
    });

    expect(await executeAgentFlowCommandPipeline(store, "human-approval", workflow)).toMatchObject({
      status: "paused",
      completedSteps: []
    });
    expect(store.listApprovals("human-approval")).toEqual([
      expect.objectContaining({ status: "requested", requestedBy: "human" })
    ]);

    const resumed = await resumeAgentFlowCommandPipeline(store, "human-approval", workflow, {
      outcome: "approve",
      decidedBy: "release-manager"
    });
    expect(resumed).toMatchObject({ status: "completed", completedSteps: ["approve_release", "done"] });
    expect(store.listApprovals("human-approval")).toEqual([
      expect.objectContaining({ status: "approved", decidedBy: "release-manager", decision: "approve" })
    ]);
    store.close();
  });

  test("closes a requested human approval when its paused run is cancelled", async () => {
    const root = temporaryRepo();
    const workflow = humanApprovalWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "cancel-human-approval", workflow });
    store.writeArtifact({
      id: "release",
      runId: "cancel-human-approval",
      path: "release.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Release candidate"
    });
    expect(await executeAgentFlowCommandPipeline(store, "cancel-human-approval", workflow))
      .toMatchObject({ status: "paused" });

    const cancelled = transitionAgentFlowLifecycleRun(store, "cancel-human-approval", "cancel");

    expect(cancelled.run).toMatchObject({ status: "cancelled", currentStepId: null });
    expect(cancelled.run.context.waiting).toBeUndefined();
    expect(store.listApprovals("cancel-human-approval")).toEqual([
      expect.objectContaining({ status: "cancelled", decision: "cancel" })
    ]);
    store.close();
  });

  test("fails closed when a decision record references a missing artifact", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: missing-decision-evidence
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true }
sessions:
  owner: { provider: fixture, role: owner }
steps:
  - { id: record, type: decision_record, owner: owner, topic: Missing, artifacts: [missing.md] }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "missing-decision", workflow });

    const result = await executeAgentFlowCommandPipeline(store, "missing-decision", workflow);
    expect(result).toMatchObject({ status: "failed", failedStep: "record" });
    expect(store.getArtifact("missing-decision", "decision-records/record.json")).toBeNull();
    expect(store.listFailures("missing-decision")).toEqual([
      expect.objectContaining({ classification: "decision_record_failure", stepId: "record" })
    ]);
    expect(store.listEvents("missing-decision").map((event) => event.type)).toContain("step.failed");
    store.close();
  });

  test("fails a restored decision record before persisting malformed step input", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: malformed-decision-input
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true }
sessions:
  owner: { provider: fixture, role: owner }
steps:
  - { id: record, type: decision_record, owner: owner, topic: Missing artifacts, artifacts: [source.md] }
`);
    delete workflow.steps[0]!.artifacts;
    const store = await openAgentFlowRunState({ cwd: root });
    store.createRunWithEvent({
      id: "malformed-decision-input",
      workflow: {
        name: workflow.name,
        version: workflow.version,
        style: workflow.style,
        maturity: workflow.maturity
      },
      context: { workflow: workflow as unknown as AgentFlowRunStateValue }
    }, { type: "run.created", payload: { status: "pending" } });

    const result = await executeAgentFlowCommandPipeline(store, "malformed-decision-input", workflow);

    expect(result).toMatchObject({
      status: "failed",
      failedStep: "record",
      message: "Decision record requires a non-empty owner, topic, and artifacts list."
    });
    expect(store.getRun("malformed-decision-input")).toMatchObject({ status: "failed" });
    expect(store.listFailures("malformed-decision-input")).toEqual([
      expect.objectContaining({ classification: "decision_record_failure", stepId: "record" })
    ]);
    const eventTypes = store.listEvents("malformed-decision-input").map((event) => event.type);
    expect(eventTypes).toContain("step.failed");
    expect(eventTypes).not.toContain("step.started");
    store.close();
  });

  test("revalidates decision record actors when executing a restored malformed workflow", async () => {
    const base = parseAgentFlowWorkflowOrThrow(`name: malformed-decision-actors
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true }
sessions:
  owner: { provider: fixture, role: owner }
  advisor: { provider: fixture, role: advisor }
steps:
  - { id: record, type: decision_record, owner: owner, topic: Decision, consulted: [advisor], approved_by: [owner], artifacts: [source.md] }
`);
    const mutations = [
      (workflow: typeof base) => { workflow.steps[0]!.owner = "missing-owner"; },
      (workflow: typeof base) => { workflow.steps[0]!.consulted = ["missing-consulted"]; },
      (workflow: typeof base) => { workflow.steps[0]!.approved_by = ["missing-approver"]; },
      (workflow: typeof base) => {
        (workflow.sessions as unknown as Record<string, unknown>).owner = null;
      }
    ];

    for (const [index, mutate] of mutations.entries()) {
      const workflow = structuredClone(base);
      mutate(workflow);
      const root = temporaryRepo();
      const store = await openAgentFlowRunState({ cwd: root });
      store.createRunWithEvent({
        id: `malformed-decision-${index}`,
        workflow: {
          name: workflow.name,
          version: workflow.version,
          style: workflow.style,
          maturity: workflow.maturity
        },
        context: { workflow: workflow as unknown as AgentFlowRunStateValue }
      }, { type: "run.created", payload: { status: "pending" } });
      store.writeArtifact({
        id: "source",
        runId: `malformed-decision-${index}`,
        path: "source.md",
        kind: "fixture",
        contentType: "text/markdown",
        content: "Evidence"
      });

      expect(() => executeAgentFlowDecisionRecord(
        store,
        `malformed-decision-${index}`,
        workflow.steps[0]!,
        workflow
      )).toThrow("references undeclared session");
      expect(store.getArtifact(`malformed-decision-${index}`, "decision-records/record.json")).toBeNull();

      const result = await executeAgentFlowCommandPipeline(store, `malformed-decision-${index}`, workflow);

      expect(result).toMatchObject({ status: "failed", failedStep: "record" });
      expect(result.message).toContain("references undeclared session");
      expect(store.listFailures(`malformed-decision-${index}`)).toEqual([
        expect.objectContaining({ classification: "decision_record_failure", stepId: "record" })
      ]);
      store.close();
    }
  });

  test("indexes a human approval failure when its evidence cannot be prepared", async () => {
    const root = temporaryRepo();
    const workflow = humanApprovalWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "missing-human-evidence", workflow });

    const result = await executeAgentFlowCommandPipeline(store, "missing-human-evidence", workflow);

    expect(result).toMatchObject({ status: "failed", failedStep: "approve_release" });
    expect(store.listFailures("missing-human-evidence")).toEqual([
      expect.objectContaining({ classification: "approval_failure", stepId: "approve_release" })
    ]);
    expect(store.listEvents("missing-human-evidence").map((event) => event.type)).toContain("step.failed");
    store.close();
  });

  test("keeps a human approval paused when its evidence changes before the decision", async () => {
    const root = temporaryRepo();
    const workflow = humanApprovalWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "changed-human-evidence", workflow });
    const release = store.writeArtifact({
      id: "release",
      runId: "changed-human-evidence",
      path: "release.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Release candidate one"
    });
    expect(await executeAgentFlowCommandPipeline(store, "changed-human-evidence", workflow)).toMatchObject({ status: "paused" });

    store.writeArtifact({
      id: release.id,
      runId: "changed-human-evidence",
      path: release.declaredPath,
      kind: release.kind,
      contentType: release.contentType,
      content: "Release candidate two",
      overwrite: true
    });

    expect(await resumeAgentFlowCommandPipeline(store, "changed-human-evidence", workflow, { outcome: "approve" }))
      .toMatchObject({ status: "paused", completedSteps: [] });
    expect(store.getRun("changed-human-evidence")?.status).toBe("paused");
    expect(store.getArtifact("changed-human-evidence", "approvals/approve_release.json")).toBeNull();
    expect(store.listApprovals("changed-human-evidence")).toEqual([
      expect.objectContaining({
        status: "stale",
        context: expect.objectContaining({
          invalidation: expect.objectContaining({ reason: "evidence_changed", path: "release.md" })
        })
      }),
      expect.objectContaining({ status: "requested" })
    ]);
    expect(await resumeAgentFlowCommandPipeline(store, "changed-human-evidence", workflow, { outcome: "approve" }))
      .toMatchObject({ status: "completed" });
    store.close();
  });

  test("reruns a pending human approval when a configured dependency changes", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: pending-configured-invalidation
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: approve, type: approval, reviewer: human, artifacts: [spec.md] }
  - { id: done, type: result, status: completed }
approvals:
  approve: { invalidated_by: [watched.md] }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "pending-configured-invalidation", workflow });
    for (const [id, artifactPath, content] of [
      ["spec", "spec.md", "Specification"],
      ["watched", "watched.md", "Initial dependency"]
    ] as const) {
      store.writeArtifact({
        id,
        runId: "pending-configured-invalidation",
        path: artifactPath,
        kind: "fixture",
        contentType: "text/markdown",
        content
      });
    }

    expect(await executeAgentFlowCommandPipeline(store, "pending-configured-invalidation", workflow))
      .toMatchObject({ status: "paused" });
    store.writeArtifact({
      id: "watched",
      runId: "pending-configured-invalidation",
      path: "watched.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Changed dependency",
      overwrite: true
    });
    expect(store.listApprovals("pending-configured-invalidation"))
      .toEqual([expect.objectContaining({ status: "stale" })]);

    expect(await resumeAgentFlowCommandPipeline(
      store,
      "pending-configured-invalidation",
      workflow,
      { outcome: "approve" }
    )).toMatchObject({ status: "paused" });
    expect(store.listApprovals("pending-configured-invalidation")).toEqual([
      expect.objectContaining({ status: "stale" }),
      expect.objectContaining({ status: "requested" })
    ]);
    expect(await resumeAgentFlowCommandPipeline(
      store,
      "pending-configured-invalidation",
      workflow,
      { outcome: "approve" }
    )).toMatchObject({ status: "completed" });
    store.close();
  });

  test("rechecks human approval staleness after acquiring the resume transaction", async () => {
    const root = temporaryRepo();
    fs.writeFileSync(path.join(root, "consume.md"), "Consume the approval.\n");
    const workflow = parseAgentFlowWorkflowOrThrow(`name: concurrent-human-invalidation
version: 1
style: pipeline
maturity: experimental
sessions:
  consumer: { provider: fixture }
steps:
  - { id: approve, type: approval, reviewer: human, artifacts: [spec.md] }
  - { id: consume, type: session_request, session: consumer, prompt: consume.md, inputs: [approvals/approve.json], outputs: [result.md] }
  - { id: done, type: result, status: completed }
approvals:
  approve: { invalidated_by: [watched.md] }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    const competitor = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "concurrent-human-invalidation", workflow });
    for (const [id, artifactPath] of [["spec", "spec.md"], ["watched", "watched.md"]] as const) {
      store.writeArtifact({
        id,
        runId: "concurrent-human-invalidation",
        path: artifactPath,
        kind: "fixture",
        contentType: "text/markdown",
        content: "Initial content"
      });
    }
    expect(await executeAgentFlowCommandPipeline(store, "concurrent-human-invalidation", workflow))
      .toMatchObject({ status: "paused" });

    const originalFinalization = store.withRunFinalizationTransaction.bind(store);
    let invalidated = false;
    store.withRunFinalizationTransaction = ((runId, callback) => {
      if (!invalidated) {
        invalidated = true;
        competitor.writeArtifact({
          id: "watched",
          runId,
          path: "watched.md",
          kind: "fixture",
          contentType: "text/markdown",
          content: "Changed concurrently",
          overwrite: true
        });
      }
      return originalFinalization(runId, callback);
    }) as typeof store.withRunFinalizationTransaction;
    let consumerInvoked = false;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      consumerInvoked = true;
      return { outputs: { "result.md": "Consumed" } };
    });

    await expect(resumeAgentFlowCommandPipeline(
      store,
      "concurrent-human-invalidation",
      workflow,
      { outcome: "approve" },
      undefined,
      providers
    )).rejects.toMatchObject({ code: "AGENT_FLOW_APPROVAL_STALE" });
    expect(consumerInvoked).toBe(false);
    expect(store.getRun("concurrent-human-invalidation")).toMatchObject({ status: "paused" });
    expect(store.getArtifact("concurrent-human-invalidation", "approvals/approve.json")).toBeNull();
    expect(store.listApprovals("concurrent-human-invalidation"))
      .toEqual([expect.objectContaining({ status: "stale" })]);
    competitor.close();
    store.close();
  });

  test("invalidates a terminal approval and its output when approved evidence is overwritten", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: invalidate-approved-evidence
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true }
sessions:
  reviewer: { provider: fixture, role: release reviewer, authority: { can_approve: true } }
steps:
  - { id: approve, type: approval, reviewer: reviewer, artifacts: [evidence.md] }
  - { id: revise, type: command, command: "printf revised > evidence.md", outputs: [evidence.md], overwrite: true }
  - { id: done, type: result, status: completed }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "invalidate-approved-evidence", workflow });
    const evidence = store.writeArtifact({
      id: `command-output:${createHash("sha256").update("evidence.md").digest("hex")}`,
      runId: "invalidate-approved-evidence",
      path: "evidence.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Original evidence"
    });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => ({
      outputs: {
        "approvals/approve.json": JSON.stringify({ status: "approved", decision: "Approved original evidence." })
      }
    }));

    expect(await executeAgentFlowCommandPipeline(
      store,
      "invalidate-approved-evidence",
      workflow,
      undefined,
      providers
    )).toMatchObject({
      status: "failed",
      completedSteps: ["approve", "revise", "done"],
      message: "Stale approval approve must be rerun before workflow completion."
    });

    expect(store.listApprovals("invalidate-approved-evidence")).toEqual([
      expect.objectContaining({
        status: "stale",
        decision: "Approved original evidence.",
        context: expect.objectContaining({
          evidence: [{ path: "evidence.md", checksum: evidence.checksum }],
          output: "approvals/approve.json",
          invalidation: expect.objectContaining({ reason: "evidence_changed", path: "evidence.md" })
        })
      })
    ]);
    expect(store.getArtifact("invalidate-approved-evidence", "approvals/approve.json")).toMatchObject({
      status: "stale",
      metadata: expect.objectContaining({ approvalInvalidated: true })
    });
    expect(() => store.readArtifact("invalidate-approved-evidence", "approvals/approve.json"))
      .toThrow("is stale because its evidence changed");
    expect(store.listEvents("invalidate-approved-evidence").map((event) => event.type))
      .toContain("approval.invalidated");
    store.close();
  });

  test("invalidates approvals when recovering an interrupted first publication", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: recover-first-publication-invalidation
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: approve, type: approval, reviewer: human, artifacts: [spec.md] }
approvals:
  approve: { invalidated_by: [watched.md] }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "recover-first-publication-invalidation", workflow });
    store.upsertArtifact({
      id: "watched",
      runId: "recover-first-publication-invalidation",
      path: "watched.md",
      kind: "fixture",
      contentType: "text/markdown"
    });
    const watched = store.getArtifact("recover-first-publication-invalidation", "watched.md")!;
    store.upsertApproval({
      id: "approval:approve:attempt-1",
      runId: "recover-first-publication-invalidation",
      stepId: "approve",
      status: "approved",
      decision: "Approved before the watched artifact appeared.",
      context: { output: "approvals/approve.json" }
    });
    const target = path.join(root, watched.storagePath);
    fs.mkdirSync(path.dirname(target), { recursive: true });
    fs.writeFileSync(target, "interrupted publication");

    store.writeArtifact({
      id: "watched",
      runId: "recover-first-publication-invalidation",
      path: "watched.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "interrupted publication"
    });

    expect(store.listApprovals("recover-first-publication-invalidation"))
      .toEqual([expect.objectContaining({ status: "stale" })]);
    store.close();
  });

  test("invalidates approvals on first publication of a pre-registered watched artifact", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: preregistered-first-publication
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: approve, type: approval, reviewer: human, artifacts: [spec.md] }
approvals:
  approve: { invalidated_by: [watched.md] }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "preregistered-first-publication", workflow });
    const content = "Published after approval";
    store.upsertArtifact({
      id: "watched",
      runId: "preregistered-first-publication",
      path: "watched.md",
      kind: "fixture",
      contentType: "text/markdown",
      checksum: `sha256:${createHash("sha256").update(content).digest("hex")}`,
      sizeBytes: Buffer.byteLength(content)
    });
    store.upsertApproval({
      id: "approval:approve:attempt-1",
      runId: "preregistered-first-publication",
      stepId: "approve",
      status: "approved",
      decision: "Approved before publication.",
      context: { output: "approvals/approve.json" }
    });

    store.writeArtifact({
      id: "watched",
      runId: "preregistered-first-publication",
      path: "watched.md",
      kind: "fixture",
      contentType: "text/markdown",
      content
    });

    expect(store.listApprovals("preregistered-first-publication"))
      .toEqual([expect.objectContaining({ status: "stale" })]);
    store.close();
  });

  test("invalidates configured approval dependencies and blocks merge-capable continuation", async () => {
    const root = temporaryRepo();
    fs.writeFileSync(path.join(root, "merge-prompt.md"), "Merge the approved result.\n");
    const workflow = parseAgentFlowWorkflowOrThrow(`name: configured-approval-invalidation
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true }
sessions:
  reviewer: { provider: fixture, role: reviewer, authority: { can_approve: true } }
  merger: { provider: fixture, role: merger, authority: { can_merge: true } }
steps:
  - { id: approve, type: approval, reviewer: reviewer, artifacts: [spec.md] }
  - { id: revise, type: command, command: "printf revised > implementation-summary.md", outputs: [implementation-summary.md], overwrite: true }
  - { id: merge, type: session_request, session: " merger ", prompt: merge-prompt.md, inputs: [approvals/approve.json], outputs: [merge-result.md] }
  - { id: done, type: result, status: completed }
approvals:
  approve:
    invalidated_by: [implementation-summary.md]
`);
    expect(validateAgentFlowWorkflow(workflow)).toEqual({ valid: true, errors: [] });
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "configured-approval-invalidation", workflow });
    store.writeArtifact({
      id: "spec",
      runId: "configured-approval-invalidation",
      path: "spec.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Approved specification"
    });
    store.writeArtifact({
      id: `command-output:${createHash("sha256").update("implementation-summary.md").digest("hex")}`,
      runId: "configured-approval-invalidation",
      path: "implementation-summary.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Original summary"
    });
    let mergeInvoked = false;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => {
      if (request.stepId === "merge") {
        mergeInvoked = true;
        return { outputs: { "merge-result.md": "merged" } };
      }
      return {
        outputs: {
          "approvals/approve.json": JSON.stringify({ status: "approved", decision: "Approved before revision." })
        }
      };
    });

    const result = await executeAgentFlowCommandPipeline(
      store,
      "configured-approval-invalidation",
      workflow,
      undefined,
      providers
    );

    expect(result).toMatchObject({
      status: "failed",
      failedStep: "merge",
      completedSteps: ["approve", "revise"],
      message: "Stale approval approve must be rerun before merge-capable step merge."
    });
    expect(mergeInvoked).toBe(false);
    expect(store.listApprovals("configured-approval-invalidation")).toEqual([
      expect.objectContaining({
        stepId: "approve",
        status: "stale",
        decision: "Approved before revision.",
        context: expect.objectContaining({
          invalidation: expect.objectContaining({
            path: "implementation-summary.md",
            source: "configured_artifact"
          })
        })
      })
    ]);
    expect(store.getArtifact("configured-approval-invalidation", "approvals/approve.json"))
      .toMatchObject({ status: "stale" });
    store.close();
  });

  test("withholds an approval output invalidated while its provider is running", async () => {
    const root = temporaryRepo();
    fs.writeFileSync(path.join(root, "consume.md"), "Consume the approved result.\n");
    const workflow = parseAgentFlowWorkflowOrThrow(`name: in-flight-approval-invalidation
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true }
sessions:
  reviewer: { provider: fixture, role: reviewer, authority: { can_approve: true } }
  consumer: { provider: fixture, role: consumer }
steps:
  - id: approve
    type: approval
    reviewer: reviewer
    artifacts: [spec.md]
    on_failure: { then: continue, allowed: true }
  - { id: consume, type: session_request, session: consumer, prompt: consume.md, inputs: [approvals/approve.json], outputs: [result.md] }
  - { id: done, type: result, status: completed }
approvals:
  approve: { invalidated_by: [watched.md] }
`);
    expect(validateAgentFlowWorkflow(workflow)).toEqual({ valid: true, errors: [] });
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "in-flight-approval-invalidation", workflow });
    for (const [id, artifactPath, content] of [
      ["spec", "spec.md", "Specification"],
      ["watched", "watched.md", "Initial dependency"]
    ] as const) {
      store.writeArtifact({
        id,
        runId: "in-flight-approval-invalidation",
        path: artifactPath,
        kind: "fixture",
        contentType: "text/markdown",
        content
      });
    }
    let consumerInvoked = false;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => {
      if (request.stepId === "approve") {
        store.writeArtifact({
          id: "watched",
          runId: "in-flight-approval-invalidation",
          path: "watched.md",
          kind: "fixture",
          contentType: "text/markdown",
          content: "Changed dependency",
          overwrite: true
        });
        return {
          outputs: {
            "approvals/approve.json": JSON.stringify({ status: "approved", decision: "Approved stale evidence." })
          }
        };
      }
      consumerInvoked = true;
      return { outputs: { "result.md": "Consumed" } };
    });

    expect(await executeAgentFlowCommandPipeline(
      store,
      "in-flight-approval-invalidation",
      workflow,
      undefined,
      providers
    )).toMatchObject({ status: "paused", failedStep: "consume" });
    expect(consumerInvoked).toBe(false);
    expect(store.getArtifact("in-flight-approval-invalidation", "approvals/approve.json")).toBeNull();
    expect(store.listApprovals("in-flight-approval-invalidation"))
      .toEqual([expect.objectContaining({ status: "stale" })]);
    store.close();
  });

  test("keeps an approval stale when its declared evidence changes during provider execution", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: in-flight-approval-evidence-invalidation
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true }
sessions:
  reviewer: { provider: fixture, role: reviewer, authority: { can_approve: true } }
steps:
  - id: approve
    type: approval
    reviewer: reviewer
    artifacts: [spec.md]
    on_failure: { then: continue, allowed: true }
  - { id: done, type: result, status: completed }
`);
    expect(validateAgentFlowWorkflow(workflow)).toEqual({ valid: true, errors: [] });
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "in-flight-approval-evidence-invalidation", workflow });
    store.writeArtifact({
      id: "spec",
      runId: "in-flight-approval-evidence-invalidation",
      path: "spec.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Original specification"
    });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      store.writeArtifact({
        id: "spec",
        runId: "in-flight-approval-evidence-invalidation",
        path: "spec.md",
        kind: "fixture",
        contentType: "text/markdown",
        content: "Changed specification",
        overwrite: true
      });
      return {
        outputs: {
          "approvals/approve.json": JSON.stringify({
            status: "approved",
            decision: "Approved stale evidence."
          })
        }
      };
    });

    expect(await executeAgentFlowCommandPipeline(
      store,
      "in-flight-approval-evidence-invalidation",
      workflow,
      undefined,
      providers
    )).toMatchObject({
      status: "failed",
      message: "Stale approval approve must be rerun before workflow completion."
    });
    expect(store.getArtifact("in-flight-approval-evidence-invalidation", "approvals/approve.json")).toBeNull();
    expect(store.listApprovals("in-flight-approval-evidence-invalidation")).toEqual([
      expect.objectContaining({
        stepId: "approve",
        status: "stale",
        context: expect.objectContaining({
          invalidation: expect.objectContaining({
            reason: "evidence_changed",
            path: "spec.md"
          })
        })
      })
    ]);
    store.close();
  });

  test("allows a whitespace-normalized approval step to rerun after invalidation", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: normalized-approval-rerun
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true }
limits:
  max_recovery_cycles: 2
  max_step_attempts: { approve: 2 }
sessions:
  reviewer: { provider: fixture, role: reviewer, authority: { can_approve: true, can_merge: true } }
steps:
  - { id: approve, type: " approval ", reviewer: reviewer, artifacts: [spec.md], on_reject: done }
  - { id: revise, type: command, command: "printf revised > summary.md", outputs: [summary.md], goto: approve }
  - { id: done, type: result, status: completed }
approvals:
  approve: { invalidated_by: [summary.md] }
`);
    expect(validateAgentFlowWorkflow(workflow)).toEqual({ valid: true, errors: [] });
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "normalized-approval-rerun", workflow });
    store.writeArtifact({
      id: "spec",
      runId: "normalized-approval-rerun",
      path: "spec.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Specification"
    });
    let approvalInvocations = 0;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      approvalInvocations += 1;
      const approved = approvalInvocations === 1;
      return {
        outputs: {
          "approvals/approve.json": JSON.stringify({
            status: approved ? "approved" : "rejected",
            decision: approved ? "Revise once." : "Stop revising."
          })
        }
      };
    });

    expect(await executeAgentFlowCommandPipeline(
      store,
      "normalized-approval-rerun",
      workflow,
      undefined,
      providers
    )).toMatchObject({ status: "completed", completedSteps: ["approve", "revise", "approve", "done"] });
    expect(approvalInvocations).toBe(2);
    expect(store.listApprovals("normalized-approval-rerun")).toEqual([
      expect.objectContaining({ status: "stale" }),
      expect.objectContaining({ status: "rejected" })
    ]);
    store.close();
  });

  test("keeps an earlier stale approval outstanding when its rerun is cancelled", async () => {
    const root = temporaryRepo();
    fs.writeFileSync(path.join(root, "merge.md"), "Merge the approved result.\n");
    const workflow = parseAgentFlowWorkflowOrThrow(`name: cancelled-approval-rerun
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true }
sessions:
  reviewer: { provider: fixture, role: reviewer, authority: { can_approve: true } }
  merger: { provider: fixture, role: merger, authority: { can_merge: true } }
steps:
  - id: approve
    type: approval
    reviewer: reviewer
    artifacts: [spec.md]
    on_failure: { then: continue, allowed: true }
  - { id: merge, type: session_request, session: merger, prompt: merge.md, inputs: [spec.md], outputs: [merged.md] }
  - { id: done, type: result, status: completed }
`);
    expect(validateAgentFlowWorkflow(workflow)).toEqual({ valid: true, errors: [] });
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "cancelled-approval-rerun", workflow });
    store.writeArtifact({
      id: "spec",
      runId: "cancelled-approval-rerun",
      path: "spec.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Specification"
    });
    store.upsertStep({
      runId: "cancelled-approval-rerun",
      stepId: "approve",
      attempt: 1,
      status: "completed"
    });
    store.upsertApproval({
      id: "approval:approve:attempt-1",
      runId: "cancelled-approval-rerun",
      stepId: "approve",
      status: "stale",
      decision: "Evidence changed after approval.",
      context: { output: "approvals/approve.json" }
    });
    let mergeInvoked = false;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => {
      if (request.stepId === "merge") {
        mergeInvoked = true;
        return { outputs: { "merged.md": "Merged" } };
      }
      throw new Error("Reviewer unavailable");
    });

    expect(await executeAgentFlowCommandPipeline(
      store,
      "cancelled-approval-rerun",
      workflow,
      undefined,
      providers
    )).toMatchObject({
      status: "failed",
      failedStep: "merge",
      message: "Stale approval approve must be rerun before merge-capable step merge."
    });
    expect(mergeInvoked).toBe(false);
    expect(store.listApprovals("cancelled-approval-rerun")).toEqual([
      expect.objectContaining({ status: "stale" }),
      expect.objectContaining({ status: "cancelled" })
    ]);
    store.close();
  });

  test("rejects malformed approval invalidation declarations", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: invalid-approval-invalidation
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true }
steps:
  - { id: approve, type: approval, reviewer: human, artifacts: [spec.md] }
approvals:
  missing: { invalidated_by: [summary.md] }
  approve:
    invalidated_by: [summary.md, summary.md, ./noncanonical.md, approvals/approve.json, final-summary.md]
    unsupported: true
`);

    expect(validateAgentFlowWorkflow(workflow).errors).toEqual(expect.arrayContaining([
      expect.objectContaining({ code: "workflow.approvals.step.undeclared", path: "approvals.missing" }),
      expect.objectContaining({ code: "workflow.approvals.field.unsupported", path: "approvals.approve.unsupported" }),
      expect.objectContaining({
        code: "workflow.approvals.invalidated_by.duplicate",
        path: "approvals.approve.invalidated_by[1]"
      }),
      expect.objectContaining({
        code: "workflow.approvals.invalidated_by.invalid",
        path: "approvals.approve.invalidated_by[2]"
      }),
      expect.objectContaining({
        code: "workflow.approvals.invalidated_by.output_collision",
        path: "approvals.approve.invalidated_by"
      }),
      expect.objectContaining({
        code: "workflow.approvals.invalidated_by.reserved",
        path: "approvals.approve.invalidated_by[4]"
      })
    ]));
  });

  test("rejects malformed persisted approval invalidation declarations at runtime", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: malformed-persisted-invalidation
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: approve, type: approval, reviewer: human, artifacts: [spec.md] }
approvals:
  approve: { invalidated_by: watched.md }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    store.createRun({
      id: "malformed-persisted-invalidation",
      workflow: {
        name: workflow.name,
        version: workflow.version,
        style: workflow.style,
        maturity: workflow.maturity
      },
      context: { workflow }
    });

    await expect(executeAgentFlowCommandPipeline(
      store,
      "malformed-persisted-invalidation",
      workflow
    )).rejects.toMatchObject({ code: "AGENT_FLOW_WORKFLOW_INVALID" });
    expect(store.getRun("malformed-persisted-invalidation")).toMatchObject({ status: "pending" });
    store.close();
  });

  test("rejects approval configuration that targets condition branch metadata", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: condition-branch-metadata
version: 1
style: pipeline
maturity: experimental
steps:
  - id: route
    type: condition
    if: inputs.ready
    then: done
    branches:
      - { id: ghost, type: approval, if: inputs.ready, then: done }
  - { id: done, type: result, status: completed }
approvals:
  ghost: { invalidated_by: [watched.md] }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    store.createRun({
      id: "condition-branch-metadata",
      workflow: {
        name: workflow.name,
        version: workflow.version,
        style: workflow.style,
        maturity: workflow.maturity
      },
      context: { workflow }
    });

    expect(() => store.validateApprovalInvalidationConfiguration("condition-branch-metadata"))
      .toThrow("does not name a declared approval step");
    store.close();
  });

  test("blocks direct merge-capable session execution while an approval is stale", async () => {
    const root = temporaryRepo();
    fs.writeFileSync(path.join(root, "merge.md"), "Merge the approved result.\n");
    const workflow = parseAgentFlowWorkflowOrThrow(`name: direct-stale-approval-guard
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true }
sessions:
  merger: { provider: fixture, role: merger, authority: { can_merge: true } }
steps:
  - { id: approve, type: approval, reviewer: human, artifacts: [spec.md] }
  - { id: merge, type: session_request, session: merger, prompt: merge.md, inputs: [spec.md], outputs: [merged.md] }
approvals:
  approve: { invalidated_by: [watched.md] }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "direct-stale-approval-guard", workflow });
    for (const [id, artifactPath, content] of [
      ["spec", "spec.md", "Specification"],
      ["watched", "watched.md", "Initial dependency"]
    ] as const) {
      store.writeArtifact({
        id,
        runId: "direct-stale-approval-guard",
        path: artifactPath,
        kind: "fixture",
        contentType: "text/markdown",
        content
      });
    }
    store.upsertApproval({
      id: "approval:approve:attempt-1",
      runId: "direct-stale-approval-guard",
      stepId: "approve",
      status: "approved",
      decision: "Approved.",
      context: { output: "approvals/approve.json" }
    });
    store.writeArtifact({
      id: "watched",
      runId: "direct-stale-approval-guard",
      path: "watched.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Changed dependency",
      overwrite: true
    });
    store.transitionRunWithEvent("direct-stale-approval-guard", {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: {} }
    });
    let providerCalls = 0;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      providerCalls += 1;
      return { outputs: { "merged.md": "Merged" } };
    });

    await expect(executeAgentFlowSessionRequest(
      store,
      "direct-stale-approval-guard",
      workflow,
      workflow.steps[1]!,
      providers
    )).rejects.toMatchObject({ code: "AGENT_FLOW_APPROVAL_STALE" });
    expect(providerCalls).toBe(0);
    expect(store.getSession("direct-stale-approval-guard", "merger")).toBeNull();
    store.close();
  });

  test("aborts a merge-capable session when approval becomes stale during provider execution", async () => {
    const root = temporaryRepo();
    fs.writeFileSync(path.join(root, "merge.md"), "Merge the approved result.\n");
    const workflow = parseAgentFlowWorkflowOrThrow(`name: in-flight-merge-invalidation
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true }
sessions:
  merger: { provider: fixture, role: merger, authority: { can_merge: true } }
steps:
  - { id: approve, type: approval, reviewer: human, artifacts: [spec.md] }
  - { id: merge, type: session_request, session: merger, prompt: merge.md, inputs: [spec.md], outputs: [merged.md] }
approvals:
  approve: { invalidated_by: [watched.md] }
`);
    const runId = "in-flight-merge-invalidation";
    const store = await openAgentFlowRunState({ cwd: root });
    const competingStore = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: runId, workflow });
    for (const [id, artifactPath] of [["spec", "spec.md"], ["watched", "watched.md"]] as const) {
      store.writeArtifact({
        id,
        runId,
        path: artifactPath,
        kind: "fixture",
        contentType: "text/markdown",
        content: "Initial content"
      });
    }
    store.upsertApproval({
      id: "approval:approve:attempt-1",
      runId,
      stepId: "approve",
      status: "approved",
      decision: "Approved.",
      context: { output: "approvals/approve.json" }
    });
    store.transitionRunWithEvent(runId, {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: {} }
    });
    let providerAborted = false;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) =>
      new Promise((resolve, reject) => {
        const mutationTimer = setTimeout(() => {
          competingStore.writeArtifact({
            id: "watched",
            runId,
            path: "watched.md",
            kind: "fixture",
            contentType: "text/markdown",
            content: "Changed during merge",
            overwrite: true
          });
        }, 10);
        const completionTimer = setTimeout(() => resolve({ outputs: { "merged.md": "Merged" } }), 250);
        request.signal.addEventListener("abort", () => {
          providerAborted = true;
          clearTimeout(mutationTimer);
          clearTimeout(completionTimer);
          reject(request.signal.reason);
        }, { once: true });
      })
    );

    await expect(executeAgentFlowSessionRequest(
      store,
      runId,
      workflow,
      workflow.steps[1]!,
      providers
    )).rejects.toMatchObject({ code: "AGENT_FLOW_APPROVAL_STALE" });
    expect(providerAborted).toBe(true);
    expect(store.getArtifact(runId, "merged.md")).toBeNull();
    expect(store.listApprovals(runId)).toEqual([expect.objectContaining({ status: "stale" })]);
    competingStore.close();
    store.close();
  });

  test("ignores incidental merge-capable actor fields on non-session steps", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: incidental-merge-actor
version: 1
style: pipeline
maturity: experimental
sessions:
  merger: { provider: fixture, authority: { can_merge: true } }
steps:
  - { id: prepare, type: command, command: "true", owner: merger }
  - { id: approve, type: approval, reviewer: human, artifacts: [spec.md] }
`);
    expect(validateAgentFlowWorkflow(workflow)).toEqual({ valid: true, errors: [] });
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "incidental-merge-actor", workflow });
    store.writeArtifact({
      id: "spec",
      runId: "incidental-merge-actor",
      path: "spec.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Specification"
    });
    store.upsertApproval({
      id: "approval:approve:attempt-1",
      runId: "incidental-merge-actor",
      stepId: "approve",
      status: "stale",
      context: { output: "approvals/approve.json" }
    });

    expect(await executeAgentFlowCommandPipeline(store, "incidental-merge-actor", workflow))
      .toMatchObject({ status: "paused", completedSteps: ["prepare"] });
    store.close();
  });

  test("blocks merge-capable recovery sessions after configured approval invalidation", async () => {
    const root = temporaryRepo();
    fs.writeFileSync(path.join(root, "merge-prompt.md"), "Repair and merge the approved result.\n");
    const workflow = parseAgentFlowWorkflowOrThrow(`name: recovery-approval-invalidation
version: 1
style: recovery_pipeline
maturity: experimental
sessions:
  reviewer: { provider: fixture, authority: { can_approve: true } }
  merger: { provider: fixture, authority: { can_merge: true } }
steps:
  - { id: approve, type: approval, reviewer: reviewer, artifacts: [spec.md] }
  - { id: revise, type: command, command: "printf revised > implementation-summary.md", outputs: [implementation-summary.md], overwrite: true }
  - id: verify
    type: command
    command: "false"
    on_failure:
      route_to: { session: merger, prompt: merge-prompt.md }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
approvals:
  approve: { invalidated_by: [implementation-summary.md] }
limits: { max_recovery_cycles: 1 }
`);
    expect(validateAgentFlowWorkflow(workflow)).toEqual({ valid: true, errors: [] });
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "recovery-approval-invalidation", workflow });
    store.writeArtifact({
      id: "spec",
      runId: "recovery-approval-invalidation",
      path: "spec.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Approved specification"
    });
    let mergerInvoked = false;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => {
      if (request.stepId === "verify:recovery") {
        mergerInvoked = true;
        return { outputs: {}, metadata: { recovery_status: "remediated" } };
      }
      return {
        outputs: {
          "approvals/approve.json": JSON.stringify({ status: "approved", decision: "Approved before revision." })
        }
      };
    });

    expect(await executeAgentFlowCommandPipeline(
      store, "recovery-approval-invalidation", workflow, undefined, providers
    )).toMatchObject({
      status: "failed",
      failedStep: "verify",
      completedSteps: ["approve", "revise"],
      message: "Stale approval approve must be rerun before merge-capable recovery session merger."
    });
    expect(mergerInvoked).toBe(false);
    store.close();
  });

  test("blocks merge-capable sessions inside nested recovery workflows when a parent approval is stale", async () => {
    const root = temporaryRepo();
    fs.writeFileSync(path.join(root, "merge-prompt.md"), "Merge the repaired result.\n");
    const workflow = parseAgentFlowWorkflowOrThrow(`name: nested-recovery-approval-invalidation
version: 1
style: recovery_pipeline
maturity: experimental
sessions:
  reviewer: { provider: fixture, authority: { can_approve: true } }
steps:
  - { id: approve, type: approval, reviewer: reviewer, artifacts: [spec.md] }
  - { id: revise, type: command, command: "printf revised > implementation-summary.md", outputs: [implementation-summary.md] }
  - id: verify
    type: command
    command: "false"
    on_failure:
      route_to: { workflow: repair, file_scope: { include: [request.md, repaired.md] } }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
approvals:
  approve: { invalidated_by: [implementation-summary.md] }
limits: { max_recovery_cycles: 1 }
`);
    const recovery = parseAgentFlowWorkflowOrThrow(`name: repair
version: 1
style: recovery_pipeline
maturity: experimental
sessions:
  merger: { provider: fixture, authority: { can_merge: true } }
steps:
  - { id: prepare, type: command, command: "printf request > request.md", outputs: [request.md] }
  - { id: merge, type: session_request, session: merger, prompt: merge-prompt.md, inputs: [request.md], outputs: [repaired.md] }
  - { id: done, type: result, status: remediated }
`);
    expect(validateAgentFlowWorkflow(workflow)).toEqual({ valid: true, errors: [] });
    expect(validateAgentFlowWorkflow(recovery)).toEqual({ valid: true, errors: [] });
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "nested-recovery-approval-invalidation", workflow });
    store.writeArtifact({
      id: "spec",
      runId: "nested-recovery-approval-invalidation",
      path: "spec.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Approved specification"
    });
    let mergerInvoked = false;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => {
      if (request.stepId === "merge") {
        mergerInvoked = true;
        return { outputs: { "repaired.md": "repaired" } };
      }
      return {
        outputs: {
          "approvals/approve.json": JSON.stringify({ status: "approved", decision: "Approved before revision." })
        }
      };
    });
    const workflows = createAgentFlowWorkflowRegistry().register("repair", recovery);

    expect(await executeAgentFlowCommandPipeline(
      store,
      "nested-recovery-approval-invalidation",
      workflow,
      undefined,
      providers,
      undefined,
      undefined,
      workflows
    )).toMatchObject({ status: "paused", completedSteps: ["approve", "revise"] });
    expect(mergerInvoked).toBe(false);
    expect(store.getRun("nested-recovery-approval-invalidation:recovery:verify-a12dd3a7:attempt-1"))
      .toMatchObject({ status: "failed" });
    store.close();
  });

  test("propagates evidence invalidation through dependent approval outputs", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: invalidate-dependent-approvals
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true }
sessions:
  reviewer: { provider: fixture, role: release reviewer, authority: { can_approve: true } }
steps:
  - { id: approve_source, type: approval, reviewer: reviewer, artifacts: [source.md] }
  - { id: approve_release, type: approval, reviewer: reviewer, artifacts: [approvals/approve_source.json] }
  - { id: revise, type: command, command: "printf revised > source.md", outputs: [source.md], overwrite: true }
  - { id: done, type: result, status: completed }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "invalidate-dependent-approvals", workflow });
    store.writeArtifact({
      id: `command-output:${createHash("sha256").update("source.md").digest("hex")}`,
      runId: "invalidate-dependent-approvals",
      path: "source.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Original source"
    });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => ({
      outputs: Object.fromEntries(request.outputs.map((output) => [
        output,
        JSON.stringify({ status: "approved", decision: `Approved ${output}.` })
      ]))
    }));

    expect(await executeAgentFlowCommandPipeline(
      store,
      "invalidate-dependent-approvals",
      workflow,
      undefined,
      providers
    )).toMatchObject({
      status: "failed",
      completedSteps: ["approve_source", "approve_release", "revise", "done"]
    });

    expect(store.listApprovals("invalidate-dependent-approvals")).toEqual([
      expect.objectContaining({
        stepId: "approve_source",
        status: "stale",
        context: expect.objectContaining({
          invalidation: expect.objectContaining({ path: "source.md" })
        })
      }),
      expect.objectContaining({
        stepId: "approve_release",
        status: "stale",
        context: expect.objectContaining({
          invalidation: expect.objectContaining({
            path: "approvals/approve_source.json",
            actualChecksum: null
          })
        })
      })
    ]);
    for (const output of ["approvals/approve_source.json", "approvals/approve_release.json"]) {
      expect(store.getArtifact("invalidate-dependent-approvals", output)).toMatchObject({ status: "stale" });
      expect(() => store.readArtifact("invalidate-dependent-approvals", output))
        .toThrow("is stale because its evidence changed");
    }
    expect(store.listEvents("invalidate-dependent-approvals")
      .filter((event) => event.type === "approval.invalidated")).toHaveLength(2);
    store.close();
  });

  test("invalidates dependent approvals when their evidence backing is deleted", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: invalidate-deleted-approval-evidence
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true }
sessions:
  reviewer: { provider: fixture, role: release reviewer, authority: { can_approve: true } }
steps:
  - { id: approve_source, type: approval, reviewer: reviewer, artifacts: [source.md] }
  - { id: approve_release, type: approval, reviewer: reviewer, artifacts: [approvals/approve_source.json] }
  - { id: done, type: result, status: completed }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "invalidate-deleted-approval-evidence", workflow });
    store.writeArtifact({
      id: "source",
      runId: "invalidate-deleted-approval-evidence",
      path: "source.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Temporary source"
    });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => ({
      outputs: Object.fromEntries(request.outputs.map((output) => [
        output,
        JSON.stringify({ status: "approved", decision: `Approved ${output}.` })
      ]))
    }));

    expect(await executeAgentFlowCommandPipeline(
      store,
      "invalidate-deleted-approval-evidence",
      workflow,
      undefined,
      providers
    )).toMatchObject({ status: "completed", completedSteps: ["approve_source", "approve_release", "done"] });
    expect(store.listApprovals("invalidate-deleted-approval-evidence"))
      .toEqual([expect.objectContaining({ status: "approved" }), expect.objectContaining({ status: "approved" })]);

    store.deleteArtifactBacking("invalidate-deleted-approval-evidence", "source.md");

    expect(store.getArtifact("invalidate-deleted-approval-evidence", "source.md")).toMatchObject({ status: "missing" });
    expect(store.listApprovals("invalidate-deleted-approval-evidence")).toEqual([
      expect.objectContaining({
        stepId: "approve_source",
        status: "stale",
        context: expect.objectContaining({
          invalidation: expect.objectContaining({ path: "source.md", actualChecksum: null })
        })
      }),
      expect.objectContaining({
        stepId: "approve_release",
        status: "stale",
        context: expect.objectContaining({
          invalidation: expect.objectContaining({
            path: "approvals/approve_source.json",
            actualChecksum: null
          })
        })
      })
    ]);
    for (const output of ["approvals/approve_source.json", "approvals/approve_release.json"]) {
      expect(store.getArtifact("invalidate-deleted-approval-evidence", output)).toMatchObject({ status: "stale" });
      expect(() => store.readArtifact("invalidate-deleted-approval-evidence", output))
        .toThrow("is stale because its evidence changed");
    }
    const eventTypes = store.listEvents("invalidate-deleted-approval-evidence").map((event) => event.type);
    expect(eventTypes.filter((type) => type === "approval.invalidated")).toHaveLength(2);
    store.close();
  });

  test("invalidates approvals when inspection detects modified or deleted evidence backing", async () => {
    for (const drift of ["modified", "deleted"] as const) {
      const root = temporaryRepo();
      const workflow = parseAgentFlowWorkflowOrThrow(`name: inspect-approval-evidence-drift
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true }
sessions:
  reviewer: { provider: fixture, role: reviewer, authority: { can_approve: true } }
steps:
  - { id: approve, type: approval, reviewer: reviewer, artifacts: [source.md] }
  - { id: done, type: result, status: completed }
`);
      const runId = `inspect-approval-evidence-${drift}`;
      const store = await openAgentFlowRunState({ cwd: root });
      createAgentFlowLifecycleRun(store, { id: runId, workflow });
      const evidence = store.writeArtifact({
        id: "source",
        runId,
        path: "source.md",
        kind: "fixture",
        contentType: "text/markdown",
        content: "Source A"
      });
      const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => ({
        outputs: {
          "approvals/approve.json": JSON.stringify({ status: "approved", decision: "Approved." })
        }
      }));
      expect(await executeAgentFlowCommandPipeline(store, runId, workflow, undefined, providers))
        .toMatchObject({ status: "completed" });
      expect(store.listApprovals(runId)).toEqual([expect.objectContaining({ status: "approved" })]);

      const evidenceTarget = path.join(root, evidence.storagePath);
      const actualChecksum = drift === "modified"
        ? `sha256:${createHash("sha256").update("Source B").digest("hex")}`
        : null;
      if (drift === "modified") fs.writeFileSync(evidenceTarget, "Source B");
      else fs.unlinkSync(evidenceTarget);

      expect(store.getArtifact(runId, "source.md")).toMatchObject({
        status: drift === "modified" ? "stale" : "missing"
      });
      expect(store.listApprovals(runId)).toEqual([
        expect.objectContaining({
          status: "stale",
          decision: "Approved.",
          context: expect.objectContaining({
            invalidation: expect.objectContaining({ path: "source.md", actualChecksum })
          })
        })
      ]);
      expect(store.getArtifact(runId, "approvals/approve.json")).toMatchObject({ status: "stale" });
      expect(() => store.readArtifact(runId, "approvals/approve.json"))
        .toThrow("is stale because its evidence changed");
      expect(store.listEvents(runId).filter((event) => event.type === "approval.invalidated"))
        .toHaveLength(1);
      store.close();
    }
  });

  test("reconciles configured approval invalidation drift before successful completion", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: reconcile-approval-before-completion
version: 1
style: pipeline
maturity: experimental
sessions:
  reviewer: { provider: fixture, authority: { can_approve: true } }
steps:
  - { id: approve, type: approval, reviewer: reviewer, artifacts: [source.md] }
  - { id: wait, type: command, command: "sleep 0.2" }
  - { id: done, type: result, status: completed }
approvals:
  approve:
    invalidated_by: [release-notes.md]
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "reconcile-approval-before-completion", workflow });
    store.writeArtifact({
      id: "source",
      runId: "reconcile-approval-before-completion",
      path: "source.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Source A"
    });
    const releaseNotes = store.writeArtifact({
      id: "release-notes",
      runId: "reconcile-approval-before-completion",
      path: "release-notes.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Release A"
    });
    const releaseNotesTarget = path.join(root, releaseNotes.storagePath);
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      setTimeout(() => fs.writeFileSync(releaseNotesTarget, "Release B"), 25);
      return {
        outputs: {
          "approvals/approve.json": JSON.stringify({ status: "approved", decision: "Approved Source A." })
        }
      };
    });

    expect(await executeAgentFlowCommandPipeline(
      store, "reconcile-approval-before-completion", workflow, undefined, providers
    )).toMatchObject({
      status: "failed",
      message: "Stale approval approve must be rerun before workflow completion."
    });
    expect(store.listApprovals("reconcile-approval-before-completion"))
      .toEqual([expect.objectContaining({
        status: "stale",
        context: expect.objectContaining({
          invalidation: expect.objectContaining({ path: "release-notes.md" })
        })
      })]);
    store.close();
  });

  test("checks stale approvals after acquiring the finalization write lock", async () => {
    const root = temporaryRepo();
    const runId = "locked-approval-completion";
    const workflow = parseAgentFlowWorkflowOrThrow(`name: locked-approval-completion
version: 1
style: pipeline
maturity: experimental
sessions:
  reviewer: { provider: fixture, authority: { can_approve: true } }
steps:
  - { id: approve, type: approval, reviewer: reviewer, artifacts: [source.md] }
  - { id: done, type: result, status: completed }
approvals:
  approve:
    invalidated_by: [release-notes.md]
`);
    const store = await openAgentFlowRunState({ cwd: root });
    const competingStore = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: runId, workflow });
    store.writeArtifact({
      id: "source",
      runId,
      path: "source.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Source A"
    });
    store.writeArtifact({
      id: "release-notes",
      runId,
      path: "release-notes.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Release A"
    });
    const originalFinalization = store.withRunFinalizationTransaction.bind(store);
    let racedFinalization = false;
    store.withRunFinalizationTransaction = ((transactionRunId, callback) => {
      const approved = store.listApprovals(runId).some((approval) => approval.status === "approved");
      if (!racedFinalization && approved) {
        racedFinalization = true;
        competingStore.writeArtifact({
          id: "release-notes",
          runId,
          path: "release-notes.md",
          kind: "fixture",
          contentType: "text/markdown",
          content: "Release B",
          overwrite: true
        });
      }
      return originalFinalization(transactionRunId, callback);
    }) as typeof store.withRunFinalizationTransaction;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => ({
      outputs: {
        "approvals/approve.json": JSON.stringify({ status: "approved", decision: "Approved Release A." })
      }
    }));

    expect(await executeAgentFlowCommandPipeline(store, runId, workflow, undefined, providers)).toMatchObject({
      status: "failed",
      message: "Stale approval approve must be rerun before workflow completion."
    });
    expect(racedFinalization).toBe(true);
    expect(store.listApprovals(runId)).toEqual([expect.objectContaining({ status: "stale" })]);
    competingStore.close();
    store.close();
  });

  test("invalidates an approval when its own outcome artifact is replaced or deleted", async () => {
    for (const mutation of ["replace", "delete"] as const) {
      const root = temporaryRepo();
      const workflow = sessionApprovalWorkflow();
      const runId = `invalidate-${mutation}-approval-output`;
      const store = await openAgentFlowRunState({ cwd: root });
      createAgentFlowLifecycleRun(store, { id: runId, workflow });
      store.writeArtifact({
        id: "spec",
        runId,
        path: "spec.md",
        kind: "fixture",
        contentType: "text/markdown",
        content: "Original evidence"
      });
      const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => ({
        outputs: {
          "approvals/approve.json": JSON.stringify({ status: "approved", decision: "Approved." })
        }
      }));

      expect(await executeAgentFlowCommandPipeline(store, runId, workflow, undefined, providers))
        .toMatchObject({ status: "completed" });
      const outcome = store.getArtifact(runId, "approvals/approve.json");
      expect(outcome).not.toBeNull();

      if (mutation === "replace") {
        store.writeArtifact({
          id: outcome!.id,
          runId,
          path: outcome!.declaredPath,
          kind: "fixture",
          contentType: "application/json",
          content: JSON.stringify({ unrelated: true }),
          overwrite: true
        });
      } else {
        store.deleteArtifactBacking(runId, outcome!.declaredPath);
      }

      expect(store.listApprovals(runId)).toEqual([
        expect.objectContaining({
          status: "stale",
          decision: "Approved.",
          context: expect.objectContaining({
            invalidation: expect.objectContaining({
              path: "approvals/approve.json",
              source: "approval_output"
            })
          })
        })
      ]);
      expect(store.listEvents(runId).filter((event) => event.type === "approval.invalidated"))
        .toHaveLength(1);
      store.close();
    }
  });

  test("keeps implicit approval and decision-record paths unique after sanitizing step IDs", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: unique-specialized-default-paths
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true }
sessions:
  reviewer: { provider: fixture, role: release reviewer, authority: { can_approve: true } }
steps:
  - { id: release approval, type: approval, reviewer: reviewer, artifacts: [source.md] }
  - { id: release-approval, type: approval, reviewer: reviewer, artifacts: [source.md] }
  - { id: architecture decision, type: decision_record, owner: reviewer, topic: First, artifacts: [source.md] }
  - { id: architecture-decision, type: decision_record, owner: reviewer, topic: Second, artifacts: [source.md] }
  - { id: done, type: result, status: completed }
`);
    const lossyApprovalPath = defaultAgentFlowApprovalOutputPath("release approval");
    const safeApprovalPath = defaultAgentFlowApprovalOutputPath("release-approval");
    const lossyDecisionPath = defaultAgentFlowDecisionRecordPath("architecture decision");
    const safeDecisionPath = defaultAgentFlowDecisionRecordPath("architecture-decision");
    expect(lossyApprovalPath).toMatch(/^approvals\/release-approval-[a-f0-9]{12}\.json$/);
    expect(lossyApprovalPath).not.toBe(safeApprovalPath);
    expect(lossyDecisionPath).toMatch(/^decision-records\/architecture-decision-[a-f0-9]{12}\.json$/);
    expect(lossyDecisionPath).not.toBe(safeDecisionPath);
    expect(validateAgentFlowWorkflow(workflow)).toEqual({ valid: true, errors: [] });

    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "unique-specialized-default-paths", workflow });
    store.writeArtifact({
      id: "source",
      runId: "unique-specialized-default-paths",
      path: "source.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Source"
    });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => ({
      outputs: Object.fromEntries(request.outputs.map((output) => [
        output,
        JSON.stringify({ status: "approved", decision: `Approved ${output}.` })
      ]))
    }));

    expect(await executeAgentFlowCommandPipeline(
      store,
      "unique-specialized-default-paths",
      workflow,
      undefined,
      providers
    )).toMatchObject({
      status: "completed",
      completedSteps: [
        "release approval",
        "release-approval",
        "architecture decision",
        "architecture-decision",
        "done"
      ]
    });
    for (const output of [lossyApprovalPath, safeApprovalPath, lossyDecisionPath, safeDecisionPath]) {
      expect(store.getArtifact("unique-specialized-default-paths", output)).toMatchObject({ status: "available" });
    }
    store.close();
  });

  test("bounds implicit approval and decision-record filenames for long valid step IDs", async () => {
    const root = temporaryRepo();
    const approvalStepId = `approval-${"a".repeat(300)}`;
    const decisionStepId = `decision-${"d".repeat(300)}`;
    const approvalPath = defaultAgentFlowApprovalOutputPath(approvalStepId);
    const decisionPath = defaultAgentFlowDecisionRecordPath(decisionStepId);
    expect(Buffer.byteLength(path.basename(approvalPath), "utf8")).toBeLessThanOrEqual(255);
    expect(Buffer.byteLength(path.basename(decisionPath), "utf8")).toBeLessThanOrEqual(255);
    expect(approvalPath).toMatch(/-[a-f0-9]{12}\.json$/);
    expect(decisionPath).toMatch(/-[a-f0-9]{12}\.json$/);
    expect(defaultAgentFlowApprovalOutputPath(`${approvalStepId}-other`)).not.toBe(approvalPath);
    expect(defaultAgentFlowDecisionRecordPath(`${decisionStepId}-other`)).not.toBe(decisionPath);

    const workflow = parseAgentFlowWorkflowOrThrow(`name: bounded-specialized-default-paths
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true }
sessions:
  reviewer: { provider: fixture, role: release reviewer, authority: { can_approve: true } }
steps:
  - { id: "${approvalStepId}", type: approval, reviewer: reviewer, artifacts: [source.md] }
  - { id: "${decisionStepId}", type: decision_record, owner: reviewer, topic: Long decision, artifacts: [source.md] }
  - { id: done, type: result, status: completed }
`);
    expect(validateAgentFlowWorkflow(workflow)).toEqual({ valid: true, errors: [] });
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "bounded-specialized-default-paths", workflow });
    store.writeArtifact({
      id: "source",
      runId: "bounded-specialized-default-paths",
      path: "source.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Source"
    });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => ({
      outputs: {
        [approvalPath]: JSON.stringify({ status: "approved", decision: "Approved." })
      }
    }));

    expect(await executeAgentFlowCommandPipeline(
      store,
      "bounded-specialized-default-paths",
      workflow,
      undefined,
      providers
    )).toMatchObject({ status: "completed" });
    for (const output of [approvalPath, decisionPath]) {
      expect(store.getArtifact("bounded-specialized-default-paths", output)).toMatchObject({ status: "available" });
    }
    store.close();
  });

  test("rejects restored approval outcomes and evidence that differ from the workflow", async () => {
    const outcomeRoot = temporaryRepo();
    const outcomeWorkflow = humanApprovalWorkflow();
    const outcomeStore = await openAgentFlowRunState({ cwd: outcomeRoot });
    createAgentFlowLifecycleRun(outcomeStore, { id: "restored-approval-outcome", workflow: outcomeWorkflow });
    outcomeStore.writeArtifact({
      id: "release",
      runId: "restored-approval-outcome",
      path: "release.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Release"
    });
    expect(await executeAgentFlowCommandPipeline(
      outcomeStore,
      "restored-approval-outcome",
      outcomeWorkflow
    )).toMatchObject({ status: "paused" });
    const outcomeRun = outcomeStore.getRun("restored-approval-outcome")!;
    const outcomeWaiting = structuredClone(outcomeRun.context.waiting) as Record<string, AgentFlowRunStateValue>;
    outcomeWaiting.validOutcomes = ["complete"];
    outcomeStore.updateRun("restored-approval-outcome", {
      context: { ...outcomeRun.context, waiting: outcomeWaiting }
    });

    await expect(resumeAgentFlowCommandPipeline(
      outcomeStore,
      "restored-approval-outcome",
      outcomeWorkflow,
      { outcome: "complete" }
    )).rejects.toMatchObject({ code: "AGENT_FLOW_RESUME_STATE" });
    expect(outcomeStore.getRun("restored-approval-outcome")?.status).toBe("paused");
    expect(outcomeStore.listApprovals("restored-approval-outcome")).toEqual([
      expect.objectContaining({ status: "requested" })
    ]);
    outcomeStore.close();

    const evidenceRoot = temporaryRepo();
    const evidenceWorkflow = humanApprovalWorkflow();
    const evidenceStore = await openAgentFlowRunState({ cwd: evidenceRoot });
    createAgentFlowLifecycleRun(evidenceStore, { id: "restored-approval-evidence", workflow: evidenceWorkflow });
    const release = evidenceStore.writeArtifact({
      id: "release",
      runId: "restored-approval-evidence",
      path: "release.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Release one"
    });
    const unrelated = evidenceStore.writeArtifact({
      id: "unrelated",
      runId: "restored-approval-evidence",
      path: "unrelated.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Unrelated"
    });
    expect(await executeAgentFlowCommandPipeline(
      evidenceStore,
      "restored-approval-evidence",
      evidenceWorkflow
    )).toMatchObject({ status: "paused" });
    evidenceStore.writeArtifact({
      id: release.id,
      runId: "restored-approval-evidence",
      path: release.declaredPath,
      kind: release.kind,
      contentType: release.contentType,
      content: "Release two",
      overwrite: true
    });
    const evidenceRun = evidenceStore.getRun("restored-approval-evidence")!;
    const evidenceWaiting = structuredClone(evidenceRun.context.waiting) as Record<string, AgentFlowRunStateValue>;
    evidenceWaiting.evidence = [{ path: unrelated.declaredPath, checksum: unrelated.checksum! }];
    evidenceStore.updateRun("restored-approval-evidence", {
      context: { ...evidenceRun.context, waiting: evidenceWaiting }
    });

    await expect(resumeAgentFlowCommandPipeline(
      evidenceStore,
      "restored-approval-evidence",
      evidenceWorkflow,
      { outcome: "approve" }
    )).rejects.toMatchObject({ code: "AGENT_FLOW_RESUME_STATE" });
    expect(evidenceStore.getRun("restored-approval-evidence")?.status).toBe("paused");
    expect(evidenceStore.listApprovals("restored-approval-evidence")).toEqual([
      expect.objectContaining({ status: "stale" })
    ]);
    evidenceStore.close();
  });

  test("persists a preflight failure for a malformed approval reviewer", async () => {
    const root = temporaryRepo();
    const parsed = sessionApprovalWorkflow();
    const workflow = {
      ...parsed,
      steps: [{ ...parsed.steps[0]!, reviewer: 42 }, ...parsed.steps.slice(1)]
    } as unknown as typeof parsed;
    const store = await openAgentFlowRunState({ cwd: root });
    store.createRunWithEvent({
      id: "malformed-approval-reviewer",
      workflow: {
        name: workflow.name,
        version: workflow.version,
        style: workflow.style,
        maturity: workflow.maturity
      },
      context: { workflow: workflow as unknown as AgentFlowRunStateValue }
    }, { type: "run.created", payload: { status: "pending" } });

    const result = await executeAgentFlowCommandPipeline(store, "malformed-approval-reviewer", workflow);

    expect(result).toMatchObject({ status: "failed", failedStep: "approve" });
    expect(result.message).toContain("non-empty reviewer");
    expect(store.getRun("malformed-approval-reviewer")?.status).toBe("failed");
    expect(store.listFailures("malformed-approval-reviewer")).toEqual([
      expect.objectContaining({ classification: "session_request_policy", stepId: "approve" })
    ]);
    store.close();
  });

  test("enforces approval authority at the exported direct executor", async () => {
    const unauthorized = sessionApprovalWorkflow();
    (unauthorized.sessions!.reviewer as Record<string, unknown>).authority = { can_approve: false };
    const interactive = humanApprovalWorkflow();
    interactive.sessions = {
      human: { provider: "fixture", authority: { can_approve: true } }
    };
    const scenarios = [
      { runId: "direct-approval-unauthorized", workflow: unauthorized, message: "can_approve authority" },
      { runId: "direct-approval-human", workflow: interactive, message: "interactive approval runtime" }
    ];

    for (const scenario of scenarios) {
      const root = temporaryRepo();
      const store = await openAgentFlowRunState({ cwd: root });
      store.createRun({
        id: scenario.runId,
        status: "running",
        workflow: {
          name: scenario.workflow.name,
          version: scenario.workflow.version,
          style: scenario.workflow.style,
          maturity: scenario.workflow.maturity
        },
        context: { workflow: scenario.workflow as unknown as AgentFlowRunStateValue }
      });
      let called = false;
      const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
        called = true;
        return { outputs: {} };
      });

      await expect(executeAgentFlowApproval(
        store,
        scenario.runId,
        scenario.workflow,
        scenario.workflow.steps[0]!,
        providers
      )).rejects.toMatchObject({
        code: "AGENT_FLOW_SESSION_AUTHORITY",
        message: expect.stringContaining(scenario.message)
      });
      expect(called).toBe(false);
      expect(store.listApprovals(scenario.runId)).toEqual([]);
      store.close();
    }
  });

  test("rejects noncanonical approval paths at the exported direct executor", async () => {
    const scenarios = [
      {
        runId: "direct-approval-noncanonical-evidence",
        mutate: (workflow: ReturnType<typeof sessionApprovalWorkflow>) => {
          workflow.steps[0]!.artifacts = ["nested/../spec.md"];
        },
        message: "artifacts must use normalized static artifact paths"
      },
      {
        runId: "direct-approval-noncanonical-output",
        mutate: (workflow: ReturnType<typeof sessionApprovalWorkflow>) => {
          workflow.steps[0]!.output = "./approvals/approve.json";
        },
        message: "output must use a normalized static artifact path"
      },
      {
        runId: "direct-approval-control-character",
        mutate: (workflow: ReturnType<typeof sessionApprovalWorkflow>) => {
          workflow.steps[0]!.artifacts = ["reports/foo\nbar.json"];
        },
        message: "artifacts must use normalized static artifact paths"
      }
    ];

    for (const scenario of scenarios) {
      const root = temporaryRepo();
      const workflow = sessionApprovalWorkflow();
      scenario.mutate(workflow);
      const store = await openAgentFlowRunState({ cwd: root });
      store.createRun({
        id: scenario.runId,
        status: "running",
        workflow: {
          name: workflow.name,
          version: workflow.version,
          style: workflow.style,
          maturity: workflow.maturity
        },
        context: { workflow: workflow as unknown as AgentFlowRunStateValue }
      });
      store.writeArtifact({
        id: "spec",
        runId: scenario.runId,
        path: "spec.md",
        kind: "fixture",
        contentType: "text/markdown",
        content: "Spec"
      });
      let called = false;
      const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => {
        called = true;
        return {
          outputs: Object.fromEntries(request.outputs.map((output) => [
            output,
            JSON.stringify({ status: "approved", decision: "Approved." })
          ]))
        };
      });

      await expect(executeAgentFlowApproval(
        store,
        scenario.runId,
        workflow,
        workflow.steps[0]!,
        providers
      )).rejects.toMatchObject({
        code: "AGENT_FLOW_SESSION_REQUEST_INVALID",
        message: expect.stringContaining(scenario.message)
      });
      expect(called).toBe(false);
      expect(store.getArtifact(scenario.runId, "approvals/approve.json")).toBeNull();
      store.close();
    }
  });

  test("bounds session approval failure policies before invoking a provider", async () => {
    const root = temporaryRepo();
    const parsed = sessionApprovalWorkflow();
    const workflow = structuredClone(parsed);
    workflow.steps[0]!.on_failure = { retry: 1_000_000_000 };
    expect(validateAgentFlowWorkflow(workflow).errors).toContainEqual(expect.objectContaining({
      code: "workflow.approval.retry.invalid",
      path: "steps[0].on_failure.retry"
    }));
    const overLimit = structuredClone(parsed);
    overLimit.steps[0]!.artifacts = Array.from({ length: 65 }, (_, index) => `evidence-${index}.md`);
    expect(validateAgentFlowWorkflow(overLimit).errors).toContainEqual(expect.objectContaining({
      code: "workflow.approval.artifacts.limit",
      path: "steps[0].artifacts"
    }));
    const secretInput = structuredClone(parsed);
    secretInput.steps[0]!.artifacts = ["secrets/token.txt"];
    expect(lintAgentFlowWorkflow(secretInput).warnings).toContainEqual(expect.objectContaining({
      code: "workflow.lint.secret.input",
      path: "steps[0].artifacts"
    }));
    const store = await openAgentFlowRunState({ cwd: root });
    store.createRunWithEvent({
      id: "unbounded-approval",
      workflow: {
        name: workflow.name,
        version: workflow.version,
        style: workflow.style,
        maturity: workflow.maturity
      },
      context: { workflow: workflow as unknown as AgentFlowRunStateValue }
    }, { type: "run.created", payload: { status: "pending" } });
    let called = false;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      called = true;
      return { outputs: {} };
    });

    const result = await executeAgentFlowCommandPipeline(store, "unbounded-approval", workflow, undefined, providers);

    expect(result).toMatchObject({ status: "failed", failedStep: "approve" });
    expect(result.message).toContain("integer from 0 through");
    expect(called).toBe(false);
    store.close();
  });

  test("rejects restored approval recovery routes before invoking a provider", async () => {
    const root = temporaryRepo();
    const workflow = sessionApprovalWorkflow();
    workflow.steps[0]!.on_failure = {
      route_to: { session: "reviewer", prompt: "repair.md" },
      on_remediated: { then: "continue" },
      on_unresolved: { then: "fail" }
    };
    const store = await openAgentFlowRunState({ cwd: root });
    store.createRunWithEvent({
      id: "approval-recovery-route",
      workflow: {
        name: workflow.name,
        version: workflow.version,
        style: workflow.style,
        maturity: workflow.maturity
      },
      context: { workflow: workflow as unknown as AgentFlowRunStateValue }
    }, { type: "run.created", payload: { status: "pending" } });
    let called = false;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      called = true;
      return { outputs: {} };
    });

    await expect(executeAgentFlowCommandPipeline(
      store, "approval-recovery-route", workflow, undefined, providers
    )).rejects.toThrow("recovery is not supported for approval steps");
    expect(called).toBe(false);
    store.close();
  });

  test("rejects unsupported specialized failure policies and malformed optional text", async () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: invalid-specialized-contracts
version: 1
style: pipeline
maturity: experimental
sessions:
  reviewer: { provider: fixture, authority: { can_approve: false } }
  owner: { provider: fixture }
steps:
  - id: human
    type: approval
    reviewer: human
    message: Valid before mutation
    artifacts: [source.md]
    on_failure: { then: continue, allowed: true }
  - id: session
    type: approval
    reviewer: reviewer
    artifacts: [source.md]
    on_failure: { then: continue }
  - id: record
    type: decision_record
    owner: owner
    topic: Decision
    rationale_summary: Valid before mutation
    artifacts: [source.md]
    on_failure: { then: continue, allowed: true }
`);
    workflow.steps[0]!.message = 42;
    workflow.steps[1]!.outputs = ["ignored.json"];
    workflow.steps[2]!.rationale_summary = 42;
    workflow.steps[2]!.outputs = ["ignored.json"];

    expect(validateAgentFlowWorkflow(workflow).errors).toEqual(expect.arrayContaining([
      expect.objectContaining({ code: "workflow.approval.on_failure.unsupported", path: "steps[0].on_failure" }),
      expect.objectContaining({ code: "workflow.approval.message.invalid", path: "steps[0].message" }),
      expect.objectContaining({ code: "workflow.approval.continue.not_allowed", path: "steps[1].on_failure.allowed" }),
      expect.objectContaining({ code: "workflow.approval.outputs.unsupported", path: "steps[1].outputs" }),
      expect.objectContaining({ code: "workflow.collaboration.authority.can_approve.required", path: "steps[1].reviewer" }),
      expect.objectContaining({ code: "workflow.decision_record.on_failure.unsupported", path: "steps[2].on_failure" }),
      expect.objectContaining({ code: "workflow.decision_record.rationale_summary.invalid", path: "steps[2].rationale_summary" }),
      expect.objectContaining({ code: "workflow.decision_record.outputs.unsupported", path: "steps[2].outputs" })
    ]));

    const root = temporaryRepo();
    const runtimeWorkflow = structuredClone(workflow);
    runtimeWorkflow.steps[0]!.message = "Approve the source?";
    const store = await openAgentFlowRunState({ cwd: root });
    store.createRunWithEvent({
      id: "unsupported-human-policy",
      workflow: {
        name: runtimeWorkflow.name,
        version: runtimeWorkflow.version,
        style: runtimeWorkflow.style,
        maturity: runtimeWorkflow.maturity
      },
      context: { workflow: runtimeWorkflow as unknown as AgentFlowRunStateValue }
    }, { type: "run.created", payload: { status: "pending" } });
    const result = await executeAgentFlowCommandPipeline(store, "unsupported-human-policy", runtimeWorkflow);
    expect(result).toMatchObject({ status: "failed", failedStep: "human" });
    expect(result.message).toContain("do not support on_failure policies");
    store.close();
  });

  test("rejects non-normalized approval and decision artifact paths in validation and schema", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: invalid-collaboration-paths
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true }
sessions:
  owner: { provider: fixture, role: owner }
steps:
  - { id: approve, type: approval, reviewer: human, artifacts: [../secret], output: ./approval.json }
  - { id: record, type: decision_record, owner: owner, topic: Invalid, artifacts: [ok.md], output: ../decision.json }
`);
    workflow.steps[1]!.owner = "{{ inputs.owner }}";
    workflow.steps[1]!.approved_by = ["owner", "owner"];

    expect(validateAgentFlowWorkflow(workflow).errors).toEqual(expect.arrayContaining([
      expect.objectContaining({ code: "workflow.approval.artifact.invalid", path: "steps[0].artifacts[0]" }),
      expect.objectContaining({ code: "workflow.approval.output.invalid", path: "steps[0].output" }),
      expect.objectContaining({ code: "workflow.decision_record.output.invalid", path: "steps[1].output" }),
      expect.objectContaining({ code: "workflow.decision_record.owner.dynamic", path: "steps[1].owner" }),
      expect.objectContaining({ code: "workflow.decision_record.approved_by.duplicate", path: "steps[1].approved_by[1]" })
    ]));
    const schema = JSON.parse(fs.readFileSync(path.join(import.meta.dir, "../../schemas/workflow.schema.json"), "utf8")) as {
      properties: { steps: { items: unknown }; approvals: { additionalProperties: unknown } };
      $defs: {
        artifactPath: { pattern: string };
        step: { properties: Record<string, { items: unknown }> };
      };
    };
    const artifactPath = new RegExp(schema.$defs.artifactPath.pattern);
    expect(artifactPath.test("approvals/release.json")).toBe(true);
    const invalidArtifactPaths = [
      "../secret",
      "./approval.json",
      "/absolute",
      "nested/../secret",
      "{{ inputs.path }}",
      "approvals/",
      "reports/foo\nbar.json",
      "reports/foo\tbar.json",
      "reports/foo\u0000bar.json",
      "reports/foo\u0085bar.json",
      "reports/foo\u2028bar.json"
    ];
    expect(invalidArtifactPaths
      .some((candidate) => artifactPath.test(candidate))).toBe(false);
    expect(schema.properties.steps.items).toEqual({ $ref: "#/$defs/step" });
    expect(schema.properties.approvals.additionalProperties).toEqual({ $ref: "#/$defs/approvalInvalidation" });
    expect(schema.$defs.step.properties).toMatchObject({
      body: { items: { $ref: "#/$defs/step" } },
      steps: { items: { $ref: "#/$defs/step" } },
      branches: { items: { $ref: "#/$defs/step" } }
    });
    const decisionSchema = JSON.parse(
      fs.readFileSync(path.join(import.meta.dir, "../../schemas/decision-record.schema.json"), "utf8")
    ) as {
      $defs: { artifactPath: { pattern: string } };
      properties: Record<string, { uniqueItems?: boolean; items?: { $ref?: string } }>;
    };
    expect(decisionSchema.properties.consulted?.uniqueItems).toBe(true);
    expect(decisionSchema.properties.approved_by?.uniqueItems).toBe(true);
    expect(decisionSchema.properties.artifacts).toMatchObject({
      uniqueItems: true,
      items: { $ref: "#/$defs/artifactPath" }
    });
    const decisionArtifactPath = new RegExp(decisionSchema.$defs.artifactPath.pattern);
    expect(decisionArtifactPath.test("evidence/release.json")).toBe(true);
    expect(invalidArtifactPaths
      .some((candidate) => decisionArtifactPath.test(candidate))).toBe(false);
  });

  test("rejects approval and decision outputs that overwrite their own evidence", async () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: evidence-output-collisions
version: 1
style: pipeline
maturity: experimental
sessions:
  owner: { provider: fixture }
steps:
  - { id: approve, type: approval, reviewer: human, artifacts: [approvals/approve.json], overwrite: true }
  - { id: record, type: decision_record, owner: owner, topic: Preserve evidence, artifacts: [source.md], output: source.md, overwrite: true }
  - { id: record_default, type: decision_record, owner: owner, topic: Preserve default evidence, artifacts: [decision-records/record_default.json], overwrite: true }
`);

    expect(validateAgentFlowWorkflow(workflow).errors).toEqual(expect.arrayContaining([
      expect.objectContaining({
        code: "workflow.approval.output.evidence_collision",
        path: "steps[0].output"
      }),
      expect.objectContaining({
        code: "workflow.decision_record.output.evidence_collision",
        path: "steps[1].output"
      }),
      expect.objectContaining({
        code: "workflow.decision_record.output.evidence_collision",
        path: "steps[2].output"
      })
    ]));
    const simulated = simulateAgentFlowWorkflow(workflow, {
      artifacts: { "approvals/approve.json": "Evidence", "source.md": "Source" },
      steps: { approve: { input: "approve" } }
    });
    expect(simulated.status).toBe("unresolved");
    expect(simulated.visitedSteps[0]).toMatchObject({ id: "approve", outcome: "failed" });

    for (const [runId, step] of [
      ["approval-evidence-collision", workflow.steps[0]!],
      ["decision-evidence-collision", workflow.steps[1]!],
      ["decision-default-evidence-collision", workflow.steps[2]!]
    ] as const) {
      const root = temporaryRepo();
      const runtimeWorkflow = structuredClone(workflow);
      runtimeWorkflow.steps = [structuredClone(step)];
      const store = await openAgentFlowRunState({ cwd: root });
      store.createRunWithEvent({
        id: runId,
        workflow: {
          name: runtimeWorkflow.name,
          version: runtimeWorkflow.version,
          style: runtimeWorkflow.style,
          maturity: runtimeWorkflow.maturity
        },
        context: { workflow: runtimeWorkflow as unknown as AgentFlowRunStateValue }
      }, { type: "run.created", payload: { status: "pending" } });
      const evidencePath = String(runtimeWorkflow.steps[0]!.artifacts![0]);
      store.writeArtifact({
        id: `evidence:${runId}`,
        runId,
        path: evidencePath,
        kind: "fixture",
        contentType: "text/plain",
        content: "Original evidence"
      });

      const result = await executeAgentFlowCommandPipeline(store, runId, runtimeWorkflow);

      expect(result).toMatchObject({ status: "failed", failedStep: runtimeWorkflow.steps[0]!.id });
      expect(result.message).toContain("must not overwrite evidence artifact");
      expect(store.readArtifact(runId, evidencePath).content.toString()).toBe("Original evidence");
      store.close();
    }
  });

  test("fails runtime execution for noncanonical decision paths and duplicate participants", async () => {
    const scenarios = [
      {
        runId: "runtime-decision-paths",
        mutate: (step: Record<string, unknown>) => Object.assign(step, {
          artifacts: ["nested/../source.md"],
          output: "./decision.json"
        }),
        message: "normalized static repo-relative artifact paths"
      },
      {
        runId: "runtime-decision-participants",
        mutate: (step: Record<string, unknown>) => Object.assign(step, { approved_by: ["owner", "owner"] }),
        message: "duplicate values"
      },
      {
        runId: "runtime-decision-padded-artifact",
        mutate: (step: Record<string, unknown>) => Object.assign(step, { artifacts: [" source.md "] }),
        message: "normalized static repo-relative artifact paths"
      },
      {
        runId: "runtime-decision-padded-output",
        mutate: (step: Record<string, unknown>) => Object.assign(step, { output: " decision.json " }),
        message: "normalized static repo-relative artifact paths"
      },
      {
        runId: "runtime-decision-control-character",
        mutate: (step: Record<string, unknown>) => Object.assign(step, { output: "reports/foo\nbar.json" }),
        message: "normalized static repo-relative artifact paths"
      },
      {
        runId: "runtime-decision-empty-consulted",
        mutate: (step: Record<string, unknown>) => Object.assign(step, { consulted: [] }),
        message: "non-empty list"
      },
      {
        runId: "runtime-decision-empty-approvers",
        mutate: (step: Record<string, unknown>) => Object.assign(step, { approved_by: [] }),
        message: "non-empty list"
      },
      {
        runId: "runtime-decision-owner",
        mutate: (step: Record<string, unknown>) => Object.assign(step, { owner: "{{ inputs.owner }}" }),
        message: "static name"
      },
      {
        runId: "runtime-decision-policy",
        mutate: (step: Record<string, unknown>) => Object.assign(step, { on_failure: { then: "continue", allowed: true } }),
        message: "do not support on_failure policies"
      },
      {
        runId: "runtime-decision-rationale",
        mutate: (step: Record<string, unknown>) => Object.assign(step, { rationale_summary: 42 }),
        message: "rationale_summary must be non-empty text"
      },
      {
        runId: "runtime-decision-outputs",
        mutate: (step: Record<string, unknown>) => Object.assign(step, { outputs: ["ignored.json"] }),
        message: "do not support plural outputs"
      }
    ];
    for (const scenario of scenarios) {
      const root = temporaryRepo();
      const parsed = parseAgentFlowWorkflowOrThrow(`name: direct-decision
version: 1
style: pipeline
maturity: experimental
sessions:
  owner: { provider: fixture }
steps:
  - { id: record, type: decision_record, owner: owner, topic: Direct, artifacts: [source.md] }
`);
      const workflow = structuredClone(parsed);
      scenario.mutate(workflow.steps[0] as Record<string, unknown>);
      const store = await openAgentFlowRunState({ cwd: root });
      store.createRunWithEvent({
        id: scenario.runId,
        workflow: {
          name: workflow.name,
          version: workflow.version,
          style: workflow.style,
          maturity: workflow.maturity
        },
        context: { workflow: workflow as unknown as AgentFlowRunStateValue }
      }, { type: "run.created", payload: { status: "pending" } });
      store.writeArtifact({
        id: "source",
        runId: scenario.runId,
        path: "source.md",
        kind: "fixture",
        contentType: "text/markdown",
        content: "Source"
      });

      const result = await executeAgentFlowCommandPipeline(store, scenario.runId, workflow);

      expect(result).toMatchObject({ status: "failed", failedStep: "record" });
      expect(result.message).toContain(scenario.message);
      expect(store.getArtifact(scenario.runId, "decision-records/record.json")).toBeNull();
      store.close();
    }
  });

  test("honors explicit overwrite for a human approval output owned by another producer", async () => {
    const root = temporaryRepo();
    const workflow = humanApprovalWorkflow();
    workflow.steps[0]!.overwrite = true;
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "human-overwrite", workflow });
    store.writeArtifact({
      id: "release",
      runId: "human-overwrite",
      path: "release.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Release candidate"
    });
    store.writeArtifact({
      id: "foreign-approval",
      runId: "human-overwrite",
      path: "approvals/approve_release.json",
      kind: "fixture",
      contentType: "application/json",
      content: "{\"status\":\"rejected\",\"decision\":\"old\"}\n"
    });

    expect(await executeAgentFlowCommandPipeline(store, "human-overwrite", workflow)).toMatchObject({ status: "paused" });
    expect(await resumeAgentFlowCommandPipeline(store, "human-overwrite", workflow, { outcome: "approve" }))
      .toMatchObject({ status: "completed" });
    expect(JSON.parse(store.readArtifact("human-overwrite", "approvals/approve_release.json").content.toString()))
      .toEqual({ status: "approved", decision: "approve" });
    store.close();
  });

  test("honors explicit overwrite for a decision record output owned by another producer", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: decision-overwrite
version: 1
style: pipeline
maturity: experimental
sessions:
  owner: { provider: fixture }
steps:
  - id: record
    type: decision_record
    owner: owner
    topic: Replace the old record
    artifacts: [source.md]
    output: decisions/current.json
    overwrite: true
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "decision-overwrite", workflow });
    store.writeArtifact({
      id: "source",
      runId: "decision-overwrite",
      path: "source.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Source"
    });
    store.writeArtifact({
      id: "foreign-decision",
      runId: "decision-overwrite",
      path: "decisions/current.json",
      kind: "fixture",
      contentType: "application/json",
      content: "{\"old\":true}\n"
    });

    expect(await executeAgentFlowCommandPipeline(store, "decision-overwrite", workflow)).toMatchObject({ status: "completed" });
    expect(JSON.parse(store.readArtifact("decision-overwrite", "decisions/current.json").content.toString()))
      .toMatchObject({ decision_id: "decision:record", owner: "owner" });
    store.close();
  });

  test("binds the exported decision executor to its persisted workflow and step", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: direct-decision-contract
version: 1
style: pipeline
maturity: experimental
sessions:
  owner: { provider: fixture }
steps:
  - { id: record, type: decision_record, owner: owner, topic: Persisted decision, artifacts: [source.md] }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    store.createRun({
      id: "direct-decision-contract",
      status: "running",
      workflow: { name: workflow.name, version: workflow.version, style: workflow.style, maturity: workflow.maturity },
      context: { workflow: workflow as unknown as AgentFlowRunStateValue }
    });
    store.writeArtifact({
      id: "source",
      runId: "direct-decision-contract",
      path: "source.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Source"
    });
    const changedWorkflow = structuredClone(workflow);
    changedWorkflow.steps[0]!.topic = "Fabricated decision";
    const alteredStep = structuredClone(workflow.steps[0]!);
    alteredStep.overwrite = true;

    expect(() => executeAgentFlowDecisionRecord(
      store, "direct-decision-contract", changedWorkflow.steps[0]!, changedWorkflow
    )).toThrow("must match a step in the workflow persisted");
    expect(() => executeAgentFlowDecisionRecord(
      store, "direct-decision-contract", alteredStep, workflow
    )).toThrow("must match a step in the workflow persisted");
    expect(store.getArtifact("direct-decision-contract", "decision-records/record.json")).toBeNull();

    const result = executeAgentFlowDecisionRecord(
      store, "direct-decision-contract", workflow.steps[0]!, workflow
    );
    expect(result.record).toMatchObject({ decision_id: "decision:record", topic: "Persisted decision" });
    expect(result.artifact.declaredPath).toBe("decision-records/record.json");
    store.close();
  });

  test("executes specialized steps declared directly in parallel branches", async () => {
    const decisionRoot = temporaryRepo();
    const decisionWorkflow = parseAgentFlowWorkflowOrThrow(`name: direct-parallel-decision
version: 1
style: pipeline
maturity: experimental
sessions:
  owner: { provider: fixture }
steps:
  - { id: prepare, type: command, command: "printf Source > source.md", outputs: [source.md], then: record }
  - id: parallel_work
    type: parallel
    strategy: fail_fast
    branches:
      - { id: record, type: decision_record, session: owner, owner: owner, topic: Direct branch decision, artifacts: [source.md] }
`);
    expect(validateAgentFlowWorkflow(decisionWorkflow)).toEqual({ valid: true, errors: [] });
    const decisionStore = await openAgentFlowRunState({ cwd: decisionRoot });
    createAgentFlowLifecycleRun(decisionStore, { id: "direct-parallel-decision", workflow: decisionWorkflow });

    expect(await executeAgentFlowCommandPipeline(
      decisionStore,
      "direct-parallel-decision",
      decisionWorkflow
    )).toMatchObject({ status: "completed", completedSteps: ["prepare", "record"] });
    expect(decisionStore.getArtifact(
      "direct-parallel-decision",
      "decision-records/record.json"
    )).toMatchObject({ kind: "decision_record", status: "available" });
    decisionStore.close();

    const approvalRoot = temporaryRepo();
    const approvalWorkflow = parseAgentFlowWorkflowOrThrow(`name: direct-parallel-approval
version: 1
style: pipeline
maturity: experimental
sessions:
  reviewer: { provider: fixture, authority: { can_approve: true } }
steps:
  - { id: prepare, type: command, command: "printf Evidence > evidence.md", outputs: [evidence.md], then: approve }
  - id: parallel_work
    type: parallel
    strategy: fail_fast
    branches:
      - { id: approve, type: approval, session: reviewer, reviewer: reviewer, artifacts: [evidence.md] }
`);
    expect(validateAgentFlowWorkflow(approvalWorkflow)).toEqual({ valid: true, errors: [] });
    const approvalStore = await openAgentFlowRunState({ cwd: approvalRoot });
    createAgentFlowLifecycleRun(approvalStore, { id: "direct-parallel-approval", workflow: approvalWorkflow });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => ({
      outputs: {
        "approvals/approve.json": JSON.stringify({ status: "approved", decision: "Evidence accepted." })
      }
    }));

    expect(await executeAgentFlowCommandPipeline(
      approvalStore,
      "direct-parallel-approval",
      approvalWorkflow,
      undefined,
      providers
    )).toMatchObject({ status: "completed", completedSteps: ["prepare", "approve"] });
    expect(approvalStore.listApprovals("direct-parallel-approval")).toEqual([
      expect.objectContaining({ status: "approved", decidedBy: "reviewer" })
    ]);
    approvalStore.close();
  });

  test("replaces a human approval output when a bounded workflow revisits the step", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: repeated-human-approval
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true }
limits:
  max_recovery_cycles: 2
  max_step_attempts: { approve_release: 2 }
steps:
  - id: approve_release
    type: approval
    reviewer: human
    artifacts: [release.md]
    on_approve: approve_release
    on_reject: cancel
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "repeated-human-approval", workflow });
    store.writeArtifact({
      id: "release",
      runId: "repeated-human-approval",
      path: "release.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Release candidate"
    });

    expect(await executeAgentFlowCommandPipeline(store, "repeated-human-approval", workflow)).toMatchObject({ status: "paused" });
    expect(await resumeAgentFlowCommandPipeline(store, "repeated-human-approval", workflow, { outcome: "approve" }))
      .toMatchObject({ status: "paused" });
    expect(await resumeAgentFlowCommandPipeline(store, "repeated-human-approval", workflow, { outcome: "reject" }))
      .toMatchObject({ status: "cancelled" });
    expect(JSON.parse(store.readArtifact("repeated-human-approval", "approvals/approve_release.json").content.toString()))
      .toEqual({ status: "rejected", decision: "reject" });
    store.close();
  });

  test("retains decision record backing when terminal cleanup deletes other artifacts", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: retained-decision
version: 1
style: pipeline
maturity: experimental
sessions:
  owner: { provider: fixture }
steps:
  - { id: record, type: decision_record, owner: owner, topic: Retain this, artifacts: [source.md] }
retention:
  on_success:
    delete: ["**"]
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "retained-decision", workflow });
    store.writeArtifact({
      id: "source",
      runId: "retained-decision",
      path: "source.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Temporary source"
    });

    expect(await executeAgentFlowCommandPipeline(store, "retained-decision", workflow)).toMatchObject({ status: "completed" });
    expect(store.getArtifact("retained-decision", "source.md")?.status).toBe("missing");
    expect(store.readArtifact("retained-decision", "decision-records/record.json").artifact.status).toBe("available");
    store.close();
  });

  test("retains approved evidence and outcome backing during successful cleanup", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: retained-approval
version: 1
style: pipeline
maturity: experimental
sessions:
  reviewer: { provider: fixture, authority: { can_approve: true } }
steps:
  - { id: temporary, type: command, command: "printf temporary > temporary.md", outputs: [temporary.md] }
  - { id: approve, type: approval, reviewer: reviewer, artifacts: ["evidence/[draft].md"] }
  - { id: done, type: result, status: completed }
approvals:
  approve:
    invalidated_by: [release-notes.md]
retention:
  on_success:
    delete: ["**"]
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "retained-approval", workflow });
    store.writeArtifact({
      id: "spec",
      runId: "retained-approval",
      path: "evidence/[draft].md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Approved specification"
    });
    store.writeArtifact({
      id: "release-notes",
      runId: "retained-approval",
      path: "release-notes.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Watched release notes"
    });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => ({
      outputs: {
        "approvals/approve.json": JSON.stringify({ status: "approved", decision: "Retain this evidence." })
      }
    }));

    expect(await executeAgentFlowCommandPipeline(store, "retained-approval", workflow, undefined, providers))
      .toMatchObject({ status: "completed" });
    expect(store.listApprovals("retained-approval"))
      .toEqual([expect.objectContaining({ status: "approved" })]);
    expect(store.readArtifact("retained-approval", "evidence/[draft].md").artifact.status).toBe("available");
    expect(store.readArtifact("retained-approval", "approvals/approve.json").artifact.status).toBe("available");
    expect(store.readArtifact("retained-approval", "release-notes.md").artifact.status).toBe("available");
    expect(store.getArtifact("retained-approval", "temporary.md")?.status).toBe("missing");
    store.close();
  });

  test("simulates session and human approval routing plus generated decision artifacts", () => {
    const session = simulateAgentFlowWorkflow(sessionApprovalWorkflow(), {
      artifacts: { "spec.md": "Spec" },
      steps: {
        approve: { outputs: { "approvals/approve.json": { status: "approved", decision: "Approved." } } }
      }
    });
    expect(session.status).toBe("completed");
    expect(session.availableArtifacts).toContain("decision-records/record_decision.json");
    expect(session.artifactValues["decision-records/record_decision.json"]).toEqual({
      decision_id: "decision:record_decision",
      owner: "reviewer",
      topic: "Ship approval and decision records",
      rationale_summary: "The approval outcome and source specification form the durable rationale.",
      consulted: [],
      approved_by: ["reviewer"],
      artifacts: ["spec.md", "approvals/approve.json"],
      created_at: "1970-01-01T00:00:00.000Z"
    });

    const human = simulateAgentFlowWorkflow(humanApprovalWorkflow(), {
      artifacts: { "release.md": "Release" },
      steps: { approve_release: { input: "reject" } }
    });
    expect(human.status).toBe("cancelled");
    expect(human.artifactValues["approvals/approve_release.json"]).toEqual({
      status: "rejected",
      decision: "reject"
    });

    const targetless = simulateAgentFlowWorkflow(parseAgentFlowWorkflowOrThrow(`name: targetless-human-approval
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: approve, type: approval, reviewer: human, artifacts: [release.md] }
  - { id: done, type: result, status: completed }
`), {
      artifacts: { "release.md": "Release" },
      steps: { approve: { input: "approve" } }
    });
    expect(targetless.status).toBe("completed");

    const decisionCollision = simulateAgentFlowWorkflow(sessionApprovalWorkflow(), {
      artifacts: {
        "spec.md": "Spec",
        "decision-records/record_decision.json": { old: true }
      },
      steps: {
        approve: { outputs: { "approvals/approve.json": { status: "approved", decision: "Approved." } } }
      }
    });
    expect(decisionCollision.status).toBe("unresolved");

    const approvalCollision = simulateAgentFlowWorkflow(parseAgentFlowWorkflowOrThrow(`name: approval-collision
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: approve, type: approval, reviewer: human, artifacts: [release.md] }
`), {
      artifacts: {
        "release.md": "Release",
        "approvals/approve.json": { status: "rejected", decision: "Old" }
      },
      steps: { approve: { input: "approve" } }
    });
    expect(approvalCollision.status).toBe("unresolved");
  });

  test("keeps approval and decision simulation fail-closed with runtime contracts", () => {
    const budgeted = parseAgentFlowWorkflowOrThrow(`name: budgeted-approvals
version: 1
style: pipeline
maturity: experimental
limits: { max_frontier_calls: 1 }
sessions:
  reviewer: { provider: frontier, authority: { can_approve: true } }
steps:
  - { id: first, type: approval, reviewer: reviewer, artifacts: [spec.md] }
  - { id: second, type: approval, reviewer: reviewer, artifacts: [spec.md] }
`);
    const approvalFixture = (id: string) => ({
      outputs: { [`approvals/${id}.json`]: { status: "approved", decision: "Approved." } }
    });
    const budgetResult = simulateAgentFlowWorkflow(budgeted, {
      artifacts: { "spec.md": "Spec" },
      steps: { first: approvalFixture("first"), second: approvalFixture("second") }
    });
    expect(budgetResult.status).toBe("paused");
    expect(budgetResult.visitedSteps).toEqual([
      expect.objectContaining({ id: "first", outcome: "succeeded" }),
      expect.objectContaining({ id: "second", outcome: "failed" })
    ]);
    expect(budgetResult.availableArtifacts).toContain("approvals/first.json");
    expect(budgetResult.availableArtifacts).not.toContain("approvals/second.json");

    const unauthorized = structuredClone(sessionApprovalWorkflow());
    (unauthorized.sessions!.reviewer as Record<string, unknown>).authority = { can_approve: false };
    const unauthorizedResult = simulateAgentFlowWorkflow(unauthorized, {
      artifacts: { "spec.md": "Spec" },
      steps: { approve: approvalFixture("approve") }
    });
    expect(unauthorizedResult.status).not.toBe("completed");
    expect(unauthorizedResult.availableArtifacts).not.toContain("approvals/approve.json");

    const noncanonicalApprovals = [
      (workflow: ReturnType<typeof sessionApprovalWorkflow>) => {
        workflow.steps[0]!.artifacts = ["nested/../spec.md"];
      },
      (workflow: ReturnType<typeof sessionApprovalWorkflow>) => {
        workflow.steps[0]!.output = "./approvals/approve.json";
      }
    ];
    for (const mutate of noncanonicalApprovals) {
      const workflow = sessionApprovalWorkflow();
      mutate(workflow);
      const result = simulateAgentFlowWorkflow(workflow, {
        artifacts: { "spec.md": "Spec" },
        steps: { approve: approvalFixture("approve") }
      });
      expect(result.status).toBe("paused");
      expect(result.visitedSteps[0]).toMatchObject({ id: "approve", outcome: "failed" });
      expect(result.availableArtifacts).not.toContain("approvals/approve.json");
    }
    const noncanonicalHuman = humanApprovalWorkflow();
    noncanonicalHuman.steps[0]!.output = "./approvals/approve_release.json";
    const noncanonicalHumanResult = simulateAgentFlowWorkflow(noncanonicalHuman, {
      artifacts: { "release.md": "Release" },
      steps: { approve_release: { input: "approve" } }
    });
    expect(noncanonicalHumanResult.status).toBe("paused");
    expect(noncanonicalHumanResult.visitedSteps[0]).toMatchObject({ id: "approve_release", outcome: "failed" });
    expect(noncanonicalHumanResult.availableArtifacts).not.toContain("approvals/approve_release.json");

    const missingHuman = simulateAgentFlowWorkflow(humanApprovalWorkflow(), {
      steps: { approve_release: { input: "approve" } }
    });
    expect(missingHuman.status).toBe("unresolved");
    expect(missingHuman.visitedSteps[0]).toMatchObject({ id: "approve_release", outcome: "failed" });
    expect(missingHuman.availableArtifacts).not.toContain("approvals/approve_release.json");

    const missingDecision = simulateAgentFlowWorkflow(parseAgentFlowWorkflowOrThrow(`name: missing-decision-simulation
version: 1
style: pipeline
maturity: experimental
sessions:
  owner: { provider: fixture }
steps:
  - { id: record, type: decision_record, owner: owner, topic: Decision, artifacts: [missing.md] }
`), {});
    expect(missingDecision.status).toBe("unresolved");
    expect(missingDecision.visitedSteps[0]).toMatchObject({ id: "record", outcome: "failed" });
    expect(missingDecision.availableArtifacts).not.toContain("decision-records/record.json");

    const validDecision = parseAgentFlowWorkflowOrThrow(`name: malformed-decision-simulation
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true }
sessions:
  owner: { provider: fixture, role: owner }
  advisor: { provider: fixture, role: advisor }
steps:
  - { id: record, type: decision_record, owner: owner, topic: Decision, consulted: [advisor], approved_by: [owner], artifacts: [source.md] }
`);
    const malformedDecisions = [
      (workflow: typeof validDecision) => { workflow.steps[0]!.owner = "missing"; },
      (workflow: typeof validDecision) => { workflow.steps[0]!.consulted = ["advisor", "advisor"]; },
      (workflow: typeof validDecision) => { workflow.steps[0]!.approved_by = ["owner", "owner"]; }
    ];
    for (const mutate of malformedDecisions) {
      const workflow = structuredClone(validDecision);
      mutate(workflow);
      const result = simulateAgentFlowWorkflow(workflow, { artifacts: { "source.md": "Source" } });
      expect(result.status).toBe("failed");
      expect(result.visitedSteps[0]).toMatchObject({ id: "record", outcome: "failed" });
      expect(result.availableArtifacts).not.toContain("decision-records/record.json");
    }
  });
});

function sessionApprovalWorkflow() {
  return parseAgentFlowWorkflowOrThrow(`name: session-approval
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true }
sessions:
  reviewer:
    provider: fixture
    role: reviewer
    authority: { can_approve: true }
steps:
  - id: approve
    type: approval
    reviewer: reviewer
    message: Approve only when the durable contract is satisfied.
    artifacts: [spec.md]
  - id: record_decision
    type: decision_record
    owner: reviewer
    topic: Ship approval and decision records
    rationale_summary: The approval outcome and source specification form the durable rationale.
    approved_by: [reviewer]
    artifacts: [spec.md, approvals/approve.json]
  - { id: done, type: result, status: completed }
`);
}

function humanApprovalWorkflow() {
  return parseAgentFlowWorkflowOrThrow(`name: human-approval
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true }
steps:
  - id: approve_release
    type: approval
    reviewer: human
    message: Approve the release candidate?
    artifacts: [release.md]
    on_approve: done
  - { id: done, type: result, status: completed }
`);
}

function temporaryRepo(): string {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "agent-flow-approval-"));
  fs.mkdirSync(path.join(root, ".git"));
  return root;
}
