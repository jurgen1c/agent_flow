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
      (workflow: typeof base) => { workflow.steps[0]!.approved_by = ["missing-approver"]; }
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
      expect.objectContaining({ status: "cancelled", decision: "evidence_changed" }),
      expect.objectContaining({ status: "requested" })
    ]);
    expect(await resumeAgentFlowCommandPipeline(store, "changed-human-evidence", workflow, { outcome: "approve" }))
      .toMatchObject({ status: "completed" });
    store.close();
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
      properties: { steps: { items: unknown } };
      $defs: {
        artifactPath: { pattern: string };
        step: { properties: Record<string, { items: unknown }> };
      };
    };
    const artifactPath = new RegExp(schema.$defs.artifactPath.pattern);
    expect(artifactPath.test("approvals/release.json")).toBe(true);
    expect(["../secret", "./approval.json", "/absolute", "nested/../secret", "{{ inputs.path }}", "approvals/"]
      .some((candidate) => artifactPath.test(candidate))).toBe(false);
    expect(schema.properties.steps.items).toEqual({ $ref: "#/$defs/step" });
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
    expect(["../secret", "./evidence.json", "/absolute", "nested/../secret", "{{ inputs.path }}"]
      .some((candidate) => decisionArtifactPath.test(candidate))).toBe(false);
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
