import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import path from "node:path";
import {
  buildAgentFlowRunActionSnapshot,
  createAgentFlowLifecycleRun,
  createAgentFlowSessionProviderRegistry,
  createAgentFlowWorkflowRegistry,
  executeAgentFlowCommandPipeline,
  executeAgentFlowRunAction,
  openAgentFlowRunState,
  parseAgentFlowWorkflowOrThrow,
  resumeAgentFlowCommandPipeline,
  serializeAgentFlowWorkflowRegistry,
  transitionAgentFlowLifecycleRun
} from "../../src/runtime";

describe("Agent Flow nested workflow execution", () => {
  test("links a child run, forwards approval resume, and atomically promotes declared outputs", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-workflow-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const child = parseAgentFlowWorkflowOrThrow(`
name: child-workflow
version: 1
style: pipeline
maturity: experimental
inputs:
  ticket: { required: true }
steps:
  - id: approve
    type: manual_gate
    message: Approve child?
    options: [approve, pause, cancel]
  - id: write
    type: command
    command: printf 'child output\\n' > child.txt
    outputs: [child.txt]
retention:
  on_success:
    delete: [child.txt]
`);
    const parent = parseAgentFlowWorkflowOrThrow(`
name: parent-workflow
version: 1
style: pipeline
maturity: experimental
inputs:
  ticket: { required: true }
steps:
  - id: child
    type: workflow
    workflow: child-workflow
    inputs:
      ticket: "{{ inputs.ticket }}"
    outputs: [child.txt]
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register(parent.name, parent)
      .register(child.name, child);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "parent", workflow: parent, inputs: { ticket: "AF-1" } });

    const started = await executeAgentFlowCommandPipeline(
      store, "parent", parent, undefined, undefined, undefined, undefined, workflows
    );
    expect(started).toMatchObject({ status: "paused", failedStep: "child" });
    const waiting = store.getRun("parent")?.context.waiting as { childRunId?: string };
    const childRunId = waiting.childRunId!;
    expect(store.getRun(childRunId)).toMatchObject({ parentRunId: "parent", status: "paused" });

    const parentRun = store.getRun("parent")!;
    const { retryAttemptIndex: _retryAttemptIndex, ...legacyWaiting } = parentRun.context.waiting as Record<string, unknown>;
    store.updateRun("parent", {
      context: { ...parentRun.context, waiting: legacyWaiting as never }
    });

    const snapshot = buildAgentFlowRunActionSnapshot(store, "parent");
    expect(snapshot.waiting).toMatchObject({ kind: "workflow", nestedKind: "manual_gate" });
    expect(snapshot.actions.find((action) => action.action === "approve")?.enabled).toBe(true);
    const resumed = await executeAgentFlowRunAction(
      store,
      "parent",
      { action: "approve", guard: snapshot.guard },
      { workflows }
    );
    expect(resumed).toMatchObject({ status: "completed" });
    expect(store.getRun(childRunId)?.status).toBe("completed");
    expect(store.readArtifact("parent", "child.txt").content.toString("utf8")).toBe("child output\n");
    expect(store.getArtifact(childRunId, "child.txt")?.status).toBe("missing");
    expect(store.getArtifact("parent", "child.txt")?.metadata).toMatchObject({
      childRunId
    });
    store.close();
  });

  test("resumes a plain-paused child through its parent action", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-plain-pause-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const child = parseAgentFlowWorkflowOrThrow(`
name: plain-paused-child
version: 1
style: pipeline
maturity: experimental
steps:
  - id: publish
    type: command
    command: if test -f ready.txt; then printf done > child.txt; else exit 1; fi
    outputs: [child.txt]
`);
    const parent = parseAgentFlowWorkflowOrThrow(`
name: plain-paused-parent
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: child, type: workflow, workflow: plain-paused-child, inputs: {}, outputs: [child.txt] }
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register(parent.name, parent)
      .register(child.name, child);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, {
      id: "plain-paused-parent",
      workflow: parent,
      context: { workflowRegistry: serializeAgentFlowWorkflowRegistry(workflows) as never }
    });

    expect(await executeAgentFlowCommandPipeline(
      store, "plain-paused-parent", parent, undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "paused", failedStep: "child" });
    const childRunId = (store.getRun("plain-paused-parent")!.context.waiting as { childRunId: string }).childRunId;
    expect(store.getRun(childRunId)?.status).toBe("paused");
    expect(store.getRun(childRunId)?.context.waiting).toBeUndefined();

    const snapshot = buildAgentFlowRunActionSnapshot(store, "plain-paused-parent");
    expect(snapshot.waiting).toMatchObject({ kind: "workflow", childRunId, childStatus: "paused" });
    expect(snapshot.actions.find((action) => action.action === "resume")).toMatchObject({
      enabled: true,
      label: "Resume child"
    });

    fs.writeFileSync(path.join(repo, "ready.txt"), "ready\n");
    expect(await executeAgentFlowRunAction(
      store,
      "plain-paused-parent",
      { action: "resume", guard: snapshot.guard },
      { workflows }
    )).toMatchObject({ status: "completed" });
    expect(store.getRun(childRunId)?.status).toBe("completed");
    expect(store.readArtifact("plain-paused-parent", "child.txt").content.toString("utf8")).toBe("done");
    store.close();
  });

  test("keeps a plain-paused child paused when its resume lease is held", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-plain-pause-lock-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const child = parseAgentFlowWorkflowOrThrow(`
name: lease-paused-child
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: publish, type: command, command: "test -f ready.txt && printf done > child.txt", outputs: [child.txt] }
`);
    const parent = parseAgentFlowWorkflowOrThrow(`
name: lease-paused-parent
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: child, type: workflow, workflow: lease-paused-child, inputs: {}, outputs: [child.txt] }
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register(parent.name, parent)
      .register(child.name, child);
    const store = await openAgentFlowRunState({ cwd: repo });
    const competitor = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "lease-paused-parent", workflow: parent });
    expect(await executeAgentFlowCommandPipeline(
      store, "lease-paused-parent", parent, undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "paused" });
    const childRunId = (store.getRun("lease-paused-parent")!.context.waiting as { childRunId: string }).childRunId;
    fs.writeFileSync(path.join(repo, "ready.txt"), "ready\n");

    let entered!: () => void;
    let release!: () => void;
    const lockEntered = new Promise<void>((resolve) => { entered = resolve; });
    const lockRelease = new Promise<void>((resolve) => { release = resolve; });
    const heldLock = competitor.withRunLock(childRunId, "run", async () => {
      entered();
      await lockRelease;
    });
    await lockEntered;
    const snapshot = buildAgentFlowRunActionSnapshot(store, "lease-paused-parent");

    await expect(executeAgentFlowRunAction(
      store,
      "lease-paused-parent",
      { action: "resume", guard: snapshot.guard },
      { workflows }
    )).rejects.toMatchObject({ code: "AGENT_FLOW_RUN_LOCKED" });
    expect(store.getRun(childRunId)?.status).toBe("paused");
    expect(store.getRun("lease-paused-parent")?.status).toBe("paused");

    release();
    await heldLock;
    expect(await executeAgentFlowRunAction(
      store,
      "lease-paused-parent",
      { action: "resume", guard: buildAgentFlowRunActionSnapshot(store, "lease-paused-parent").guard },
      { workflows }
    )).toMatchObject({ status: "completed" });
    competitor.close();
    store.close();
  });

  test("promotes child outputs before immediate success retention", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-retention-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const child = parseAgentFlowWorkflowOrThrow(`
name: retained-child
version: 1
style: pipeline
maturity: experimental
steps:
  - id: write
    type: command
    command: printf retained > retained.txt
    outputs: [retained.txt]
retention:
  on_success:
    delete: [retained.txt]
`);
    const parent = parseAgentFlowWorkflowOrThrow(`
name: retention-parent
version: 1
style: pipeline
maturity: experimental
steps:
  - id: child
    type: workflow
    workflow: retained-child
    inputs: {}
    outputs: [retained.txt]
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register(parent.name, parent)
      .register(child.name, child);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "parent", workflow: parent });

    expect(await executeAgentFlowCommandPipeline(
      store, "parent", parent, undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "completed", completedSteps: ["child"] });
    const childRun = store.listRuns().find((run) => run.parentRunId === "parent")!;
    expect(childRun.status).toBe("completed");
    expect(store.getArtifact(childRun.id, "retained.txt")?.status).toBe("missing");
    expect(store.readArtifact("parent", "retained.txt").content.toString("utf8")).toBe("retained");
    store.close();
  });

  test("completes and promotes a child when the parent is externally paused during child execution", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-parent-pause-"));
    fs.mkdirSync(path.join(repo, ".git"));
    fs.writeFileSync(path.join(repo, "prompt.md"), "Write the result.\n");
    const child = parseAgentFlowWorkflowOrThrow(`
name: pause-race-child
version: 1
style: pipeline
maturity: experimental
inputs:
  source: { required: true }
sessions:
  writer: { provider: fixture }
limits: { max_model_calls: 1 }
steps:
  - id: write
    type: session_request
    session: writer
    prompt: prompt.md
    inputs: [input.txt]
    outputs: [result.txt]
retention:
  on_success:
    delete: [result.txt]
`);
    const parent = parseAgentFlowWorkflowOrThrow(`
name: pause-race-parent
version: 1
style: pipeline
maturity: experimental
inputs:
  source: { required: true }
steps:
  - id: child
    type: workflow
    workflow: pause-race-child
    inputs: { source: "{{ inputs.source }}" }
    outputs: [result.txt]
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register(parent.name, parent)
      .register(child.name, child);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, {
      id: "pause-race-parent",
      workflow: parent,
      inputs: { source: "input.txt" }
    });
    store.writeArtifact({
      id: "pause-race-input",
      runId: "pause-race-parent",
      path: "input.txt",
      kind: "fixture",
      contentType: "text/plain; charset=utf-8",
      content: "input\n"
    });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      transitionAgentFlowLifecycleRun(store, "pause-race-parent", "pause");
      return { outputs: { "result.txt": "completed before pause\n" } };
    });

    const result = await executeAgentFlowCommandPipeline(
      store, "pause-race-parent", parent, undefined, providers, undefined, undefined, workflows
    );
    expect(result).toMatchObject({ status: "paused" });
    const childRun = store.listRuns().find((run) => run.parentRunId === "pause-race-parent")!;
    expect(childRun.status).toBe("completed");
    expect(store.getArtifact(childRun.id, "result.txt")?.status).toBe("missing");
    expect(store.readArtifact("pause-race-parent", "result.txt").content.toString("utf8"))
      .toBe("completed before pause\n");
    store.close();
  });

  test("preserves sensitive provenance for artifact-backed child inputs", async () => {
    for (const mode of ["deny", "redact"] as const) {
      const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", `agent-flow-nested-sensitive-${mode}-`));
      fs.mkdirSync(path.join(repo, ".git"));
      const child = parseAgentFlowWorkflowOrThrow(`
name: sensitive-child
version: 1
style: pipeline
maturity: experimental
inputs:
  payload: { required: true }
steps:
  - id: publish
    type: command
    command: printf 'done\\n' > child.txt
    outputs: [child.txt]
`);
      const parent = parseAgentFlowWorkflowOrThrow(`
name: sensitive-parent-${mode}
version: 1
style: pipeline
maturity: experimental
policies: { sensitive_inputs: ${mode} }
steps:
  - id: child
    type: workflow
    workflow: sensitive-child
    inputs:
      payload: "{{ artifacts.secrets.api_token }}"
    outputs: [child.txt]
`);
      const workflows = createAgentFlowWorkflowRegistry()
        .register(parent.name, parent)
        .register(child.name, child);
      const store = await openAgentFlowRunState({ cwd: repo });
      const runId = `sensitive-${mode}`;
      createAgentFlowLifecycleRun(store, { id: runId, workflow: parent });
      store.writeArtifact({
        id: `secret-${mode}`,
        runId,
        stepId: "fixture",
        path: "secrets/api_token",
        kind: "fixture",
        contentType: "text/plain; charset=utf-8",
        content: "decoy.txt"
      });
      store.writeArtifact({
        id: `decoy-${mode}`,
        runId,
        stepId: "fixture",
        path: "decoy.txt",
        kind: "fixture",
        contentType: "text/plain; charset=utf-8",
        content: "must not be treated as the referenced artifact"
      });

      const result = await executeAgentFlowCommandPipeline(
        store, runId, parent, undefined, undefined, undefined, undefined, workflows
      );
      if (mode === "deny") {
        expect(result).toMatchObject({ status: "paused", failedStep: "child" });
        expect(result.message).toContain("denied by policies.sensitive_inputs");
        expect(store.listRuns().filter((run) => run.parentRunId === runId)).toHaveLength(0);
      } else {
        expect(result).toMatchObject({ status: "completed" });
        expect(store.listRuns().find((run) => run.parentRunId === runId)?.inputs)
          .toEqual({ payload: "[REDACTED]" });
      }
      store.close();
    }
  });

  test("keeps ancestor stale approvals in nested action guards", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-stale-approval-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const child = parseAgentFlowWorkflowOrThrow(`
name: stale-approval-child
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: ask, type: input_request, question: Give value, save_as: answer.json }
  - { id: done, type: result, status: completed }
`);
    const parent = parseAgentFlowWorkflowOrThrow(`
name: stale-approval-parent
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: produce, type: command, command: "printf before > evidence.txt", outputs: [evidence.txt] }
  - { id: approve, type: approval, reviewer: human, subject: Check, artifacts: [evidence.txt] }
  - { id: child, type: workflow, workflow: stale-approval-child, inputs: {}, outputs: [answer.json] }
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register(parent.name, parent)
      .register(child.name, child);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "stale-approval-parent", workflow: parent });

    expect(await executeAgentFlowCommandPipeline(
      store, "stale-approval-parent", parent, undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "paused" });
    expect(await resumeAgentFlowCommandPipeline(
      store, "stale-approval-parent", parent, { outcome: "approve" },
      undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "paused", failedStep: "child" });
    const evidence = store.getArtifact("stale-approval-parent", "evidence.txt")!;
    fs.writeFileSync(path.join(repo, evidence.storagePath), "changed");

    const snapshot = buildAgentFlowRunActionSnapshot(store, "stale-approval-parent");
    expect(snapshot.staleApprovals).toContainEqual(expect.objectContaining({
      id: expect.stringContaining("approval:approve"),
      detected: true
    }));
    expect(snapshot.warnings).toContainEqual(expect.objectContaining({ code: "action.approval.stale" }));
    expect(snapshot.actions).toContainEqual(expect.objectContaining({
      action: "provide_input",
      enabled: false
    }));
    await expect(executeAgentFlowRunAction(store, "stale-approval-parent", {
      action: "provide_input",
      guard: snapshot.guard,
      answer: "value"
    }, { workflows })).rejects.toMatchObject({ code: "AGENT_FLOW_ACTION_NOT_ALLOWED" });
    store.close();
  });

  test("invalidates the action guard when an intermediate child run changes", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-lineage-guard-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const leaf = parseAgentFlowWorkflowOrThrow(`
name: lineage-leaf
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: wait, type: manual_gate, message: Continue?, options: [approve, cancel] }
  - { id: publish, type: command, command: "printf done > leaf.txt", outputs: [leaf.txt] }
  - { id: done, type: result, status: completed }
`);
    const middle = parseAgentFlowWorkflowOrThrow(`
name: lineage-middle
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: leaf, type: workflow, workflow: lineage-leaf, inputs: {}, outputs: [leaf.txt] }
`);
    const root = parseAgentFlowWorkflowOrThrow(`
name: lineage-root
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: middle, type: workflow, workflow: lineage-middle, inputs: {}, outputs: [leaf.txt] }
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register(root.name, root)
      .register(middle.name, middle)
      .register(leaf.name, leaf);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "lineage-root", workflow: root });

    expect(await executeAgentFlowCommandPipeline(
      store, "lineage-root", root, undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "paused" });
    const middleRunId = (store.getRun("lineage-root")!.context.waiting as { childRunId: string }).childRunId;
    const middleRun = store.getRun(middleRunId)!;
    const leafRunId = (middleRun.context.waiting as { childRunId: string }).childRunId;
    const snapshot = buildAgentFlowRunActionSnapshot(store, "lineage-root");
    expect(snapshot.waiting).toMatchObject({
      kind: "workflow",
      nestedKind: "manual_gate",
      childRunId: leafRunId
    });

    store.updateRun(middleRunId, {
      context: { ...middleRun.context, routingRevision: "changed" }
    });

    await expect(executeAgentFlowRunAction(
      store,
      "lineage-root",
      { action: "approve", guard: snapshot.guard },
      { workflows }
    )).rejects.toMatchObject({ code: "AGENT_FLOW_ACTION_STALE" });
    expect(store.getRun(leafRunId)?.status).toBe("paused");
    store.close();
  });

  test("settles a paused parent when its child completed before the parent resumed", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-terminal-child-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const child = parseAgentFlowWorkflowOrThrow(`
name: terminal-child
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: approve, type: manual_gate, message: Continue?, options: [approve, cancel] }
  - { id: publish, type: command, command: "printf done > child.txt", outputs: [child.txt] }
retention:
  on_success: { delete: [child.txt] }
`);
    const parent = parseAgentFlowWorkflowOrThrow(`
name: terminal-parent
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: child, type: workflow, workflow: terminal-child, inputs: {}, outputs: [child.txt] }
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register(parent.name, parent)
      .register(child.name, child);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, {
      id: "terminal-parent",
      workflow: parent,
      context: { workflowRegistry: serializeAgentFlowWorkflowRegistry(workflows) as never }
    });

    expect(await executeAgentFlowCommandPipeline(
      store, "terminal-parent", parent, undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "paused" });
    const childRunId = (store.getRun("terminal-parent")!.context.waiting as { childRunId: string }).childRunId;
    expect(await resumeAgentFlowCommandPipeline(
      store, childRunId, child, { outcome: "approve" }, undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "completed" });
    expect(store.getArtifact(childRunId, "child.txt")?.status).toBe("missing");
    expect(store.readArtifact("terminal-parent", "child.txt").content.toString("utf8")).toBe("done");

    const snapshot = buildAgentFlowRunActionSnapshot(store, "terminal-parent");
    expect(snapshot.waiting).toMatchObject({ kind: "workflow", childStatus: "completed" });
    expect(snapshot.actions.find((action) => action.action === "resume")).toMatchObject({ enabled: true });
    expect(await executeAgentFlowRunAction(
      store, "terminal-parent", { action: "resume", guard: snapshot.guard }
    )).toMatchObject({ status: "completed" });
    expect(store.readArtifact("terminal-parent", "child.txt").content.toString("utf8")).toBe("done");
    store.close();
  });

  test("rejects an independently completed child after its approval evidence becomes stale", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-stale-child-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const child = parseAgentFlowWorkflowOrThrow(`
name: stale-child
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: evidence, type: command, command: "printf original > evidence.txt", outputs: [evidence.txt] }
  - { id: approve, type: approval, reviewer: human, subject: Check, artifacts: [evidence.txt] }
  - { id: publish, type: command, command: "printf result > result.txt", outputs: [result.txt] }
`);
    const parent = parseAgentFlowWorkflowOrThrow(`
name: stale-parent
version: 1
style: pipeline
maturity: experimental
steps:
  - id: child
    type: workflow
    workflow: stale-child
    inputs: {}
    outputs: [result.txt]
    on_failure: { then: fail }
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register(parent.name, parent)
      .register(child.name, child);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "stale-parent", workflow: parent });

    expect(await executeAgentFlowCommandPipeline(
      store, "stale-parent", parent, undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "paused" });
    const childRunId = (store.getRun("stale-parent")!.context.waiting as { childRunId: string }).childRunId;
    expect(await resumeAgentFlowCommandPipeline(
      store, childRunId, child, { outcome: "approve" }, undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "completed" });
    const evidence = store.getArtifact(childRunId, "evidence.txt")!;
    fs.writeFileSync(path.join(repo, evidence.storagePath), "changed");

    const result = await resumeAgentFlowCommandPipeline(
      store, "stale-parent", parent, { outcome: "continue" },
      undefined, undefined, undefined, undefined, workflows
    );
    expect(result).toMatchObject({ status: "failed", failedStep: "child" });
    expect(result.message).toContain("Stale approval approve must be rerun");
    expect(store.listApprovals(childRunId).find((approval) => approval.stepId === "approve")?.status)
      .toBe("stale");
    store.close();
  });

  test("rechecks promoted child approvals when the parent completes after a later pause", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-promoted-stale-child-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const child = parseAgentFlowWorkflowOrThrow(`
name: promoted-stale-child
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: evidence, type: command, command: "printf original > evidence.txt", outputs: [evidence.txt] }
  - { id: approve, type: approval, reviewer: human, subject: Check, artifacts: [evidence.txt] }
  - { id: publish, type: command, command: "printf result > result.txt", outputs: [result.txt] }
`);
    const parent = parseAgentFlowWorkflowOrThrow(`
name: promoted-stale-parent
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: child, type: workflow, workflow: promoted-stale-child, inputs: {}, outputs: [result.txt] }
  - { id: finish, type: manual_gate, message: Finish parent?, options: [approve, cancel] }
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register(parent.name, parent)
      .register(child.name, child);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "promoted-stale-parent", workflow: parent });

    expect(await executeAgentFlowCommandPipeline(
      store, "promoted-stale-parent", parent, undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "paused", failedStep: "child" });
    const childRunId = (store.getRun("promoted-stale-parent")!.context.waiting as { childRunId: string }).childRunId;
    expect(await resumeAgentFlowCommandPipeline(
      store, "promoted-stale-parent", parent, { outcome: "approve" },
      undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "paused" });
    expect(store.getRun("promoted-stale-parent")?.context.waiting).toMatchObject({
      kind: "manual_gate",
      stepId: "finish"
    });
    expect(store.getRun(childRunId)?.status).toBe("completed");
    expect(store.readArtifact("promoted-stale-parent", "result.txt").content.toString("utf8")).toBe("result");

    const evidence = store.getArtifact(childRunId, "evidence.txt")!;
    fs.writeFileSync(path.join(repo, evidence.storagePath), "changed");

    const snapshot = buildAgentFlowRunActionSnapshot(store, "promoted-stale-parent");
    expect(snapshot.staleApprovals).toContainEqual(expect.objectContaining({
      id: expect.stringContaining("approval:approve"),
      detected: true
    }));
    expect(snapshot.actions.find((action) => action.action === "approve"))
      .toMatchObject({ enabled: false });

    const result = await resumeAgentFlowCommandPipeline(
      store, "promoted-stale-parent", parent, { outcome: "approve" },
      undefined, undefined, undefined, undefined, workflows
    );
    expect(result).toMatchObject({ status: "failed", completedSteps: ["child", "finish"] });
    expect(result.message).toContain("Stale approval approve must be rerun");
    expect(store.listApprovals(childRunId).find((approval) => approval.stepId === "approve")?.status)
      .toBe("stale");
    store.close();
  });

  test("rejects a completed child when a descendant approval becomes stale", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-stale-descendant-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const grandchild = parseAgentFlowWorkflowOrThrow(`
name: stale-grandchild
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: evidence, type: command, command: "printf original > evidence.txt", outputs: [evidence.txt] }
  - { id: approve, type: approval, reviewer: human, subject: Check, artifacts: [evidence.txt] }
  - { id: publish, type: command, command: "printf result > result.txt", outputs: [result.txt] }
`);
    const child = parseAgentFlowWorkflowOrThrow(`
name: stale-middle
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: grandchild, type: workflow, workflow: stale-grandchild, inputs: {}, outputs: [result.txt] }
`);
    const parent = parseAgentFlowWorkflowOrThrow(`
name: stale-root
version: 1
style: pipeline
maturity: experimental
steps:
  - id: child
    type: workflow
    workflow: stale-middle
    inputs: {}
    outputs: [result.txt]
    on_failure: { then: fail }
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register(parent.name, parent)
      .register(child.name, child)
      .register(grandchild.name, grandchild);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "stale-root", workflow: parent });

    expect(await executeAgentFlowCommandPipeline(
      store, "stale-root", parent, undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "paused" });
    const childRunId = (store.getRun("stale-root")!.context.waiting as { childRunId: string }).childRunId;
    const grandchildRunId = (store.getRun(childRunId)!.context.waiting as { childRunId: string }).childRunId;
    expect(await resumeAgentFlowCommandPipeline(
      store, grandchildRunId, grandchild, { outcome: "approve" },
      undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "completed" });
    expect(await resumeAgentFlowCommandPipeline(
      store, childRunId, child, { outcome: "continue" },
      undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "completed" });
    const evidence = store.getArtifact(grandchildRunId, "evidence.txt")!;
    fs.writeFileSync(path.join(repo, evidence.storagePath), "changed");

    const snapshot = buildAgentFlowRunActionSnapshot(store, "stale-root");
    expect(snapshot.staleApprovals).toContainEqual(expect.objectContaining({
      id: expect.stringContaining("approval:approve"),
      detected: true
    }));
    expect(snapshot.actions.find((action) => action.action === "resume"))
      .toMatchObject({ enabled: false });

    const result = await resumeAgentFlowCommandPipeline(
      store, "stale-root", parent, { outcome: "continue" },
      undefined, undefined, undefined, undefined, workflows
    );
    expect(result).toMatchObject({ status: "failed", failedStep: "child" });
    expect(result.message).toContain("Stale approval approve must be rerun");
    expect(store.listApprovals(grandchildRunId).find((approval) => approval.stepId === "approve")?.status)
      .toBe("stale");
    store.close();
  });

  test("settles a failed child even when an earlier child approval becomes stale", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-stale-failed-child-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const child = parseAgentFlowWorkflowOrThrow(`
name: stale-failed-child
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: evidence, type: command, command: "printf original > evidence.txt", outputs: [evidence.txt] }
  - { id: approve, type: approval, reviewer: human, subject: Check, artifacts: [evidence.txt] }
  - { id: crash, type: command, command: "exit 1", outputs: [never.txt], on_failure: { then: fail } }
`);
    const parent = parseAgentFlowWorkflowOrThrow(`
name: stale-failed-parent
version: 1
style: pipeline
maturity: experimental
steps:
  - id: child
    type: workflow
    workflow: stale-failed-child
    inputs: {}
    outputs: [never.txt]
    on_failure: { then: fail }
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register(parent.name, parent)
      .register(child.name, child);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, {
      id: "stale-failed-parent",
      workflow: parent,
      context: { workflowRegistry: serializeAgentFlowWorkflowRegistry(workflows) as never }
    });

    expect(await executeAgentFlowCommandPipeline(
      store, "stale-failed-parent", parent, undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "paused" });
    const childRunId = (store.getRun("stale-failed-parent")!.context.waiting as { childRunId: string }).childRunId;
    expect(await resumeAgentFlowCommandPipeline(
      store, childRunId, child, { outcome: "approve" }, undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "failed" });
    const evidence = store.getArtifact(childRunId, "evidence.txt")!;
    fs.writeFileSync(path.join(repo, evidence.storagePath), "changed");

    const snapshot = buildAgentFlowRunActionSnapshot(store, "stale-failed-parent");
    expect(snapshot.staleApprovals).toHaveLength(1);
    expect(snapshot.waiting).toMatchObject({ kind: "workflow", childStatus: "failed" });
    expect(snapshot.actions.find((action) => action.action === "resume"))
      .toMatchObject({ enabled: true });
    expect(await executeAgentFlowRunAction(
      store, "stale-failed-parent", { action: "resume", guard: snapshot.guard }
    )).toMatchObject({ status: "failed" });
    expect(store.getRun("stale-failed-parent")?.status).toBe("failed");
    store.close();
  });

  test("ignores stale approvals from superseded completed descendant visits", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-stale-visit-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const grandchild = parseAgentFlowWorkflowOrThrow(`
name: visit-grandchild
version: 1
style: pipeline
maturity: experimental
sessions:
  reviewer: { provider: fixture, authority: { can_approve: true } }
steps:
  - { id: evidence, type: command, command: "printf original > evidence.txt", outputs: [evidence.txt] }
  - { id: approve, type: approval, reviewer: reviewer, artifacts: [evidence.txt] }
  - id: publish
    type: command
    command: >-
      if [ -f visit-sentinel ]; then printf '{"again":false}' > result.json;
      else touch visit-sentinel; printf '{"again":true}' > result.json; fi
    outputs: [result.json]
`);
    const child = parseAgentFlowWorkflowOrThrow(`
name: visit-middle
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_recovery_cycles: 3 }
steps:
  - id: grandchild
    type: workflow
    workflow: visit-grandchild
    inputs: {}
    outputs: [result.json]
    then: decide
  - { id: decide, type: condition, if: artifacts.result.again == true, then: grandchild, else: completed }
`);
    const parent = parseAgentFlowWorkflowOrThrow(`
name: visit-root
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: child, type: workflow, workflow: visit-middle, inputs: {}, outputs: [result.json], on_failure: { then: fail } }
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register(parent.name, parent)
      .register(child.name, child)
      .register(grandchild.name, grandchild);
    const store = await openAgentFlowRunState({ cwd: repo });
    let approvalCalls = 0;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => {
      approvalCalls += 1;
      if (approvalCalls === 2) {
        const superseded = store.listRuns().find((run) =>
          run.id.includes(":workflow:grandchild-") && run.id.endsWith(":attempt-1")
        )!;
        const evidence = store.getArtifact(superseded.id, "evidence.txt")!;
        fs.writeFileSync(path.join(repo, evidence.storagePath), "stale");
      }
      return { outputs: { [request.outputs[0]!]: JSON.stringify({ status: "approved", decision: "ok" }) } };
    });
    createAgentFlowLifecycleRun(store, { id: "visit-root", workflow: parent });

    expect(await executeAgentFlowCommandPipeline(
      store, "visit-root", parent, undefined, providers, undefined, undefined, workflows
    )).toMatchObject({ status: "completed", completedSteps: ["child"] });
    const superseded = store.listRuns().find((run) =>
      run.id.includes(":workflow:grandchild-") && run.id.endsWith(":attempt-1")
    )!;
    store.getArtifact(superseded.id, "evidence.txt");
    expect(store.listApprovals(superseded.id).find((approval) => approval.stepId === "approve")?.status)
      .toBe("stale");
    expect(JSON.parse(store.readArtifact("visit-root", "result.json").content.toString("utf8")))
      .toEqual({ again: false });
    store.close();
  });

  test("promotes retained outputs when a paused child is registered under an alias", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-alias-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const child = parseAgentFlowWorkflowOrThrow(`
name: internal-child
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: approve, type: manual_gate, message: Continue?, options: [approve, cancel] }
  - { id: publish, type: command, command: "printf done > child.txt", outputs: [child.txt] }
retention:
  on_success: { delete: [child.txt] }
`);
    const parent = parseAgentFlowWorkflowOrThrow(`
name: alias-parent
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: child, type: workflow, workflow: child-alias, inputs: {}, outputs: [child.txt] }
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register(parent.name, parent)
      .register("child-alias", child);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "alias-parent", workflow: parent });

    expect(await executeAgentFlowCommandPipeline(
      store, "alias-parent", parent, undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "paused" });
    const childRunId = (store.getRun("alias-parent")!.context.waiting as { childRunId: string }).childRunId;
    expect(await resumeAgentFlowCommandPipeline(
      store, childRunId, child, { outcome: "approve" }, undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "completed" });
    expect(store.getArtifact(childRunId, "child.txt")?.status).toBe("missing");
    expect(store.readArtifact("alias-parent", "child.txt").content.toString("utf8")).toBe("done");
    store.close();
  });

  test("rechecks a parent action guard after acquiring the nested child lock", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-action-race-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const child = parseAgentFlowWorkflowOrThrow(`
name: guarded-child
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: first, type: manual_gate, message: First?, options: [approve, cancel] }
  - { id: second, type: manual_gate, message: Second?, options: [approve, cancel] }
  - { id: publish, type: command, command: "printf unused > unused.txt", outputs: [unused.txt] }
`);
    const parent = parseAgentFlowWorkflowOrThrow(`
name: guarded-parent
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: child, type: workflow, workflow: guarded-child, inputs: {}, outputs: [unused.txt] }
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register(parent.name, parent)
      .register(child.name, child);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "guarded-parent", workflow: parent });
    expect(await executeAgentFlowCommandPipeline(
      store, "guarded-parent", parent, undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "paused" });
    const childRunId = (store.getRun("guarded-parent")!.context.waiting as { childRunId: string }).childRunId;
    const snapshot = buildAgentFlowRunActionSnapshot(store, "guarded-parent");
    const originalWithRunLock = store.withRunLock.bind(store);
    let advanced = false;
    store.withRunLock = (async (runId, operation, callback) => {
      if (runId === childRunId && operation === "resume" && !advanced) {
        advanced = true;
        expect(await resumeAgentFlowCommandPipeline(
          store, childRunId, child, { outcome: "approve" },
          undefined, undefined, undefined, undefined, workflows
        )).toMatchObject({ status: "paused" });
      }
      return originalWithRunLock(runId, operation, callback);
    }) as typeof store.withRunLock;

    await expect(executeAgentFlowRunAction(
      store,
      "guarded-parent",
      { action: "approve", guard: snapshot.guard },
      { workflows }
    )).rejects.toMatchObject({ code: "AGENT_FLOW_ACTION_STALE" });
    expect(store.getRun(childRunId)?.context.waiting).toMatchObject({ stepId: "second" });
    store.close();
  });

  test("invalidates the parent action guard when a child repeats the same input request", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-input-guard-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const child = parseAgentFlowWorkflowOrThrow(`
name: repeated-input-child
version: 1
style: recovery_pipeline
maturity: experimental
limits:
  max_recovery_cycles: 3
steps:
  - id: ask
    type: input_request
    question: Same prompt
    save_as: answer.txt
    goto: ask
  - { id: publish, type: command, command: "printf never > never.txt", outputs: [never.txt] }
`);
    const parent = parseAgentFlowWorkflowOrThrow(`
name: repeated-input-parent
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: child, type: workflow, workflow: repeated-input-child, inputs: {}, outputs: [never.txt] }
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register(parent.name, parent)
      .register(child.name, child);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "repeated-input-parent", workflow: parent });
    expect(await executeAgentFlowCommandPipeline(
      store, "repeated-input-parent", parent, undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "paused" });
    const childRunId = (store.getRun("repeated-input-parent")!.context.waiting as { childRunId: string }).childRunId;
    const staleSnapshot = buildAgentFlowRunActionSnapshot(store, "repeated-input-parent");

    expect(await resumeAgentFlowCommandPipeline(
      store, childRunId, child, { answer: "first" }, undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "paused" });
    expect(store.getRun(childRunId)?.context.waiting).toMatchObject({ stepId: "ask" });
    expect(buildAgentFlowRunActionSnapshot(store, "repeated-input-parent").guard)
      .not.toBe(staleSnapshot.guard);
    await expect(executeAgentFlowRunAction(
      store,
      "repeated-input-parent",
      { action: "provide_input", answer: "stale", guard: staleSnapshot.guard },
      { workflows }
    )).rejects.toMatchObject({ code: "AGENT_FLOW_ACTION_STALE" });
    store.close();
  });

  test("cancels a paused child when its parent is cancelled", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-cancel-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const child = parseAgentFlowWorkflowOrThrow(`
name: cancelled-child
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: approve, type: manual_gate, message: Continue?, options: [approve, cancel] }
  - { id: publish, type: command, command: "printf never > never.txt", outputs: [never.txt] }
`);
    const parent = parseAgentFlowWorkflowOrThrow(`
name: cancelled-parent
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: child, type: workflow, workflow: cancelled-child, inputs: {}, outputs: [never.txt] }
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register(parent.name, parent)
      .register(child.name, child);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "cancelled-parent", workflow: parent });

    expect(await executeAgentFlowCommandPipeline(
      store, "cancelled-parent", parent, undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "paused" });
    const childRunId = (store.getRun("cancelled-parent")!.context.waiting as { childRunId: string }).childRunId;
    expect(store.getRun(childRunId)?.status).toBe("paused");

    expect(transitionAgentFlowLifecycleRun(store, "cancelled-parent", "cancel").run.status)
      .toBe("cancelled");
    expect(store.getRun(childRunId)?.status).toBe("cancelled");
    expect(store.getRun(childRunId)?.context.waiting).toBeUndefined();
    store.close();
  });

  test("records an in-flight child cancellation as an interruption instead of a failure", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-running-cancel-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const child = parseAgentFlowWorkflowOrThrow(`
name: running-cancel-child
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: wait, type: command, command: sleep 2, outputs: [never.txt] }
`);
    const parent = parseAgentFlowWorkflowOrThrow(`
name: running-cancel-parent
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: child, type: workflow, workflow: running-cancel-child, inputs: {}, outputs: [never.txt] }
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register(parent.name, parent)
      .register(child.name, child);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "running-cancel-parent", workflow: parent });

    const execution = executeAgentFlowCommandPipeline(
      store, "running-cancel-parent", parent, undefined, undefined, undefined, undefined, workflows
    );
    setTimeout(() => transitionAgentFlowLifecycleRun(store, "running-cancel-parent", "cancel"), 25);
    expect(await execution).toMatchObject({ status: "cancelled" });

    const childRunId = store.listRuns().find((run) => run.parentRunId === "running-cancel-parent")!.id;
    expect(store.getRun(childRunId)?.status).toBe("cancelled");
    expect(store.listSteps("running-cancel-parent").find((step) => step.stepId === "child" && step.attempt === 1))
      .toMatchObject({ status: "cancelled" });
    const events = store.listEvents("running-cancel-parent");
    expect(events.some((event) => event.type === "step.interrupted" && event.stepId === "child")).toBe(true);
    expect(events.some((event) => event.type === "step.failed" && event.stepId === "child")).toBe(false);
    store.close();
  });

  test("pins configured providers only for workflows reachable from the active run", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-reachable-provider-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const parent = parseAgentFlowWorkflowOrThrow(`
name: reachable-parent
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: gate, type: manual_gate, message: Continue?, options: [approve, cancel] }
  - { id: done, type: result, status: completed }
`);
    const unrelated = parseAgentFlowWorkflowOrThrow(`
name: unrelated-provider-workflow
version: 1
style: pipeline
maturity: experimental
sessions:
  worker: { provider: unrelated }
steps:
  - { id: done, type: result, status: completed }
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register(parent.name, parent)
      .register(unrelated.name, unrelated);
    const providers = (fingerprint: string) => createAgentFlowSessionProviderRegistry().registerConfigured({
      name: "unrelated",
      kind: "local",
      target: "unrelated-target",
      driver: "test-driver",
      model: "test-model",
      fingerprint
    }, () => ({ outputs: {} }));
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "reachable-provider", workflow: parent });

    expect(await executeAgentFlowCommandPipeline(
      store, "reachable-provider", parent, undefined, providers("sha256:first"), undefined, undefined, workflows
    )).toMatchObject({ status: "paused" });
    expect(store.getRun("reachable-provider")?.context.providerBindings).toBeUndefined();
    expect(await resumeAgentFlowCommandPipeline(
      store, "reachable-provider", parent, { outcome: "approve" }, undefined,
      providers("sha256:changed"), undefined, undefined, workflows
    )).toMatchObject({ status: "completed" });
    store.close();
  });

  test("pins providers from distinct registry aliases that share an internal workflow name", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-provider-aliases-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const child = (provider: string, output: string) => parseAgentFlowWorkflowOrThrow(`
name: shared-internal-name
version: 1
style: pipeline
maturity: experimental
sessions:
  worker: { provider: ${provider} }
steps:
  - { id: publish, type: command, command: "printf done > ${output}", outputs: [${output}] }
`);
    const parent = parseAgentFlowWorkflowOrThrow(`
name: aliased-provider-parent
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: gate, type: manual_gate, message: Continue?, options: [approve, cancel] }
  - { id: first, type: workflow, workflow: first-alias, inputs: {}, outputs: [first.txt] }
  - { id: second, type: workflow, workflow: second-alias, inputs: {}, outputs: [second.txt] }
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register(parent.name, parent)
      .register("first-alias", child("first-provider", "first.txt"))
      .register("second-alias", child("second-provider", "second.txt"));
    const providers = (secondFingerprint: string) => createAgentFlowSessionProviderRegistry()
      .registerConfigured({
        name: "first-provider", kind: "local", target: "first-target", driver: "test-driver",
        model: "test-model", fingerprint: "sha256:first"
      }, () => ({ outputs: {} }))
      .registerConfigured({
        name: "second-provider", kind: "local", target: "second-target", driver: "test-driver",
        model: "test-model", fingerprint: secondFingerprint
      }, () => ({ outputs: {} }));
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "aliased-provider-parent", workflow: parent });

    expect(await executeAgentFlowCommandPipeline(
      store, "aliased-provider-parent", parent, undefined, providers("sha256:second"),
      undefined, undefined, workflows
    )).toMatchObject({ status: "paused" });
    expect(store.getRun("aliased-provider-parent")?.context.providerBindings).toMatchObject({
      "first-provider": { fingerprint: "sha256:first" },
      "second-provider": { fingerprint: "sha256:second" }
    });
    await expect(resumeAgentFlowCommandPipeline(
      store, "aliased-provider-parent", parent, { outcome: "approve" }, undefined,
      providers("sha256:changed"), undefined, undefined, workflows
    )).rejects.toMatchObject({ code: "AGENT_FLOW_PROVIDER_CONFIG_DRIFT" });
    store.close();
  });

  test("inherits only configured bindings reachable from a child workflow", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-child-bindings-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const child = parseAgentFlowWorkflowOrThrow(`
name: binding-child
version: 1
style: pipeline
maturity: experimental
steps:
  - id: write
    type: command
    command: printf 'done\\n' > done.txt
    outputs: [done.txt]
`);
    const parent = parseAgentFlowWorkflowOrThrow(`
name: binding-parent
version: 1
style: pipeline
maturity: experimental
sessions:
  parent_only: { provider: parent-provider }
steps:
  - id: child
    type: workflow
    workflow: binding-child
    inputs: {}
    outputs: [done.txt]
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register(parent.name, parent)
      .register(child.name, child);
    const providers = createAgentFlowSessionProviderRegistry().registerConfigured({
      name: "parent-provider",
      kind: "local",
      target: "parent-target",
      driver: "test-driver",
      model: "test-model",
      fingerprint: "sha256:parent"
    }, () => ({ outputs: {} }));
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "binding-parent", workflow: parent });

    expect(await executeAgentFlowCommandPipeline(
      store, "binding-parent", parent, undefined, providers, undefined, undefined, workflows
    )).toMatchObject({ status: "completed" });
    const childRun = store.listRuns().find((run) => run.parentRunId === "binding-parent")!;
    expect(store.getRun("binding-parent")?.context.providerBindings).toHaveProperty("parent-provider");
    expect(childRun.context.providerBindings).toBeUndefined();
    store.close();
  });

  test("requires explicit overwrite authorization when promoting child outputs", async () => {
    for (const overwrite of [false, true]) {
      const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", `agent-flow-nested-overwrite-${overwrite}-`));
      fs.mkdirSync(path.join(repo, ".git"));
      const child = parseAgentFlowWorkflowOrThrow(`
name: overwrite-child
version: 1
style: pipeline
maturity: experimental
steps:
  - id: write
    type: command
    command: printf child > shared.txt
    outputs: [shared.txt]
`);
      const parent = parseAgentFlowWorkflowOrThrow(`
name: overwrite-parent-${overwrite}
version: 1
style: pipeline
maturity: experimental
steps:
  - id: child
    type: workflow
    workflow: overwrite-child
    inputs: {}
    outputs: [shared.txt]
    overwrite: ${overwrite}
`);
      const workflows = createAgentFlowWorkflowRegistry()
        .register(parent.name, parent)
        .register(child.name, child);
      const store = await openAgentFlowRunState({ cwd: repo });
      const runId = `overwrite-${overwrite}`;
      createAgentFlowLifecycleRun(store, { id: runId, workflow: parent });
      store.writeArtifact({
        id: `existing-${overwrite}`,
        runId,
        stepId: "fixture",
        path: "shared.txt",
        kind: "fixture",
        contentType: "text/plain",
        content: "parent"
      });

      const result = await executeAgentFlowCommandPipeline(
        store, runId, parent, undefined, undefined, undefined, undefined, workflows
      );
      expect(result.status).toBe(overwrite ? "completed" : "paused");
      expect(store.readArtifact(runId, "shared.txt").content.toString())
        .toBe(overwrite ? "child" : "parent");
      expect(store.getArtifact(runId, "shared.txt")?.producerStepId)
        .toBe(overwrite ? "child" : "fixture");
      store.close();
    }
  });

  test("resolves published artifact values into child workflow inputs", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-artifact-input-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const child = parseAgentFlowWorkflowOrThrow(`
name: artifact-child
version: 1
style: pipeline
maturity: experimental
inputs:
  pr_url: { required: true }
steps:
  - id: publish
    type: command
    command: printf 'resolved\\n' > child.txt
    outputs: [child.txt]
`);
    const parent = parseAgentFlowWorkflowOrThrow(`
name: artifact-parent
version: 1
style: pipeline
maturity: experimental
steps:
  - id: child
    type: workflow
    workflow: artifact-child
    inputs:
      pr_url: "{{ artifacts.github.pr_url }}"
    outputs: [child.txt]
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register(parent.name, parent)
      .register(child.name, child);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "artifact-parent", workflow: parent });
    store.writeArtifact({
      id: "pr-url",
      runId: "artifact-parent",
      stepId: "fixture",
      path: "github/pr_url",
      kind: "fixture",
      contentType: "text/plain; charset=utf-8",
      content: "https://github.com/example/repo/pull/1\n"
    });

    expect(await executeAgentFlowCommandPipeline(
      store, "artifact-parent", parent, undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "completed" });
    expect(store.listRuns().find((run) => run.parentRunId === "artifact-parent")?.inputs)
      .toEqual({ pr_url: "https://github.com/example/repo/pull/1" });
    expect(store.readArtifact("artifact-parent", "child.txt").content.toString("utf8")).toBe("resolved\n");
    store.close();
  });

  test("does not copy artifacts named by resolved artifact contents into child runs", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-artifact-content-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const child = parseAgentFlowWorkflowOrThrow(`
name: artifact-content-child
version: 1
style: pipeline
maturity: experimental
inputs:
  label: { required: true }
steps:
  - { id: write, type: command, command: "printf child > shared.txt", outputs: [shared.txt] }
`);
    const parent = parseAgentFlowWorkflowOrThrow(`
name: artifact-content-parent
version: 1
style: pipeline
maturity: experimental
steps:
  - id: child
    type: workflow
    workflow: artifact-content-child
    inputs: { label: "{{ artifacts.value }}" }
    outputs: [shared.txt]
    overwrite: true
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register(parent.name, parent)
      .register(child.name, child);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "artifact-content-parent", workflow: parent });
    store.writeArtifact({
      id: "artifact-content-value",
      runId: "artifact-content-parent",
      stepId: "fixture",
      path: "value",
      kind: "fixture",
      contentType: "text/plain",
      content: "shared.txt"
    });
    store.writeArtifact({
      id: "artifact-content-decoy",
      runId: "artifact-content-parent",
      stepId: "fixture",
      path: "shared.txt",
      kind: "fixture",
      contentType: "text/plain",
      content: "parent"
    });

    expect(await executeAgentFlowCommandPipeline(
      store, "artifact-content-parent", parent, undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "completed" });
    const childRun = store.listRuns().find((run) => run.parentRunId === "artifact-content-parent")!;
    expect(childRun.inputs).toEqual({ label: "shared.txt" });
    expect(store.readArtifact(childRun.id, "shared.txt").content.toString()).toBe("child");
    expect(store.readArtifact("artifact-content-parent", "shared.txt").content.toString()).toBe("child");
    store.close();
  });

  test("copies artifact paths nested in aggregate workflow input values", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-aggregate-input-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const child = parseAgentFlowWorkflowOrThrow(`
name: aggregate-input-child
version: 1
style: pipeline
maturity: experimental
inputs:
  sources: { required: true }
steps:
  - { id: publish, type: command, command: "printf done > done.txt", outputs: [done.txt] }
`);
    const parent = parseAgentFlowWorkflowOrThrow(`
name: aggregate-input-parent
version: 1
style: pipeline
maturity: experimental
inputs:
  sources: { required: true }
steps:
  - id: child
    type: workflow
    workflow: aggregate-input-child
    inputs: { sources: "{{ inputs.sources }}" }
    outputs: [done.txt]
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register(parent.name, parent)
      .register(child.name, child);
    const store = await openAgentFlowRunState({ cwd: repo });
    const sources = { primary: "inputs/one.txt", more: ["inputs/two.txt"] };
    createAgentFlowLifecycleRun(store, { id: "aggregate-input-parent", workflow: parent, inputs: { sources } });
    store.writeArtifact({
      id: "aggregate-one", runId: "aggregate-input-parent", stepId: "fixture",
      path: "inputs/one.txt", kind: "fixture", contentType: "text/plain", content: "one"
    });
    store.writeArtifact({
      id: "aggregate-two", runId: "aggregate-input-parent", stepId: "fixture",
      path: "inputs/two.txt", kind: "fixture", contentType: "text/plain", content: "two"
    });

    expect(await executeAgentFlowCommandPipeline(
      store, "aggregate-input-parent", parent, undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "completed" });
    const childRun = store.listRuns().find((run) => run.parentRunId === "aggregate-input-parent")!;
    expect(childRun.inputs).toEqual({ sources });
    expect(store.readArtifact(childRun.id, "inputs/one.txt").content.toString()).toBe("one");
    expect(store.readArtifact(childRun.id, "inputs/two.txt").content.toString()).toBe("two");
    store.close();
  });

  test("preserves nested sensitive provenance in aggregate workflow input values", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-aggregate-sensitive-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const child = parseAgentFlowWorkflowOrThrow(`
name: aggregate-sensitive-child
version: 1
style: pipeline
maturity: experimental
inputs:
  sources: { required: true }
steps:
  - { id: publish, type: command, command: "printf done > done.txt", outputs: [done.txt] }
`);
    const parent = parseAgentFlowWorkflowOrThrow(`
name: aggregate-sensitive-parent
version: 1
style: pipeline
maturity: experimental
policies: { sensitive_inputs: redact }
inputs:
  sources: { required: true }
steps:
  - id: child
    type: workflow
    workflow: aggregate-sensitive-child
    inputs: { sources: "{{ inputs.sources }}" }
    outputs: [done.txt]
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register(parent.name, parent)
      .register(child.name, child);
    const store = await openAgentFlowRunState({ cwd: repo });
    const sources = { api_key: "inputs/private.txt", public: "inputs/public.txt" };
    createAgentFlowLifecycleRun(store, { id: "aggregate-sensitive-parent", workflow: parent, inputs: { sources } });
    store.writeArtifact({
      id: "aggregate-secret", runId: "aggregate-sensitive-parent", stepId: "fixture",
      path: "inputs/private.txt", kind: "fixture", contentType: "text/plain", content: "secret"
    });
    store.writeArtifact({
      id: "aggregate-public", runId: "aggregate-sensitive-parent", stepId: "fixture",
      path: "inputs/public.txt", kind: "fixture", contentType: "text/plain", content: "public"
    });

    expect(await executeAgentFlowCommandPipeline(
      store, "aggregate-sensitive-parent", parent, undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "completed" });
    const childRun = store.listRuns().find((run) => run.parentRunId === "aggregate-sensitive-parent")!;
    expect(childRun.inputs).toEqual({
      sources: { api_key: "[REDACTED]", public: "inputs/public.txt" }
    });
    expect(store.readArtifact(childRun.id, "inputs/private.txt").content.toString()).toBe("[REDACTED]");
    expect(store.readArtifact(childRun.id, "inputs/public.txt").content.toString()).toBe("public");
    store.close();
  });

  test("does not copy literal child input strings that happen to name parent artifacts", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-literal-input-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const child = parseAgentFlowWorkflowOrThrow(`
name: literal-input-child
version: 1
style: pipeline
maturity: experimental
inputs:
  label: { required: true }
steps:
  - { id: write, type: command, command: "printf child > shared.txt", outputs: [shared.txt] }
`);
    const parent = parseAgentFlowWorkflowOrThrow(`
name: literal-input-parent
version: 1
style: pipeline
maturity: experimental
steps:
  - id: child
    type: workflow
    workflow: literal-input-child
    inputs: { label: shared.txt }
    outputs: [shared.txt]
    overwrite: true
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register(parent.name, parent)
      .register(child.name, child);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "literal-input-parent", workflow: parent });
    store.writeArtifact({
      id: "existing-shared",
      runId: "literal-input-parent",
      stepId: "fixture",
      path: "shared.txt",
      kind: "fixture",
      contentType: "text/plain",
      content: "parent"
    });

    expect(await executeAgentFlowCommandPipeline(
      store, "literal-input-parent", parent, undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "completed" });
    const childRun = store.listRuns().find((run) => run.parentRunId === "literal-input-parent")!;
    expect(childRun.inputs).toEqual({ label: "shared.txt" });
    expect(store.readArtifact("literal-input-parent", "shared.txt").content.toString()).toBe("child");
    store.close();
  });

  test("rejects a copied child input that is not a declared child output", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-input-output-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const child = parseAgentFlowWorkflowOrThrow(`
name: input-output-child
version: 1
style: pipeline
maturity: experimental
inputs: { payload: { required: true } }
steps:
  - { id: done, type: result, status: completed }
`);
    const parent = parseAgentFlowWorkflowOrThrow(`
name: input-output-parent
version: 1
style: pipeline
maturity: experimental
inputs: { source: { required: true } }
steps:
  - id: child
    type: workflow
    workflow: input-output-child
    inputs: { payload: "{{ inputs.source }}" }
    outputs: [source.txt]
    overwrite: true
    on_failure: { then: fail }
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register(parent.name, parent)
      .register(child.name, child);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, {
      id: "input-output-parent",
      workflow: parent,
      inputs: { source: "source.txt" }
    });
    store.writeArtifact({
      id: "input-output-source",
      runId: "input-output-parent",
      stepId: "fixture",
      path: "source.txt",
      kind: "fixture",
      contentType: "text/plain",
      content: "parent input\n"
    });

    await expect(executeAgentFlowCommandPipeline(
      store, "input-output-parent", parent, undefined, undefined, undefined, undefined, workflows
    )).rejects.toMatchObject({
      code: "AGENT_FLOW_WORKFLOW_INVALID",
      message: expect.stringContaining("child workflow input-output-child does not declare")
    });
    expect(store.listRuns().filter((run) => run.parentRunId === "input-output-parent")).toHaveLength(0);
    expect(store.getRun("input-output-parent")?.status).toBe("pending");
    expect(store.readArtifact("input-output-parent", "source.txt").content.toString()).toBe("parent input\n");
    store.close();
  });

  test("persists child setup errors and finalizes the parent", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-setup-failure-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const child = parseAgentFlowWorkflowOrThrow(`
name: required-child
version: 1
style: pipeline
maturity: experimental
inputs:
  ticket: { required: true }
steps:
  - { id: done, type: result, status: completed }
  - { id: publish, type: command, command: "printf missing > missing.txt", outputs: [missing.txt] }
`);
    const parent = parseAgentFlowWorkflowOrThrow(`
name: setup-failure-parent
version: 1
style: pipeline
maturity: experimental
inputs:
  ticket: { required: false }
steps:
  - id: child
    type: workflow
    workflow: required-child
    inputs:
      ticket: "{{ inputs.ticket }}"
    outputs: [missing.txt]
    on_failure: { then: fail }
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register(parent.name, parent)
      .register(child.name, child);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "setup-failure", workflow: parent });

    expect(await executeAgentFlowCommandPipeline(
      store, "setup-failure", parent, undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "failed", failedStep: "child" });
    expect(store.getRun("setup-failure")?.status).toBe("failed");
    expect(store.listSteps("setup-failure").at(-1)).toMatchObject({ stepId: "child", status: "failed" });
    store.close();
  });

  test("terminalizes a created child when its runtime preflight fails", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-child-preflight-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const child = parseAgentFlowWorkflowOrThrow(`
name: preflight-failure-child
version: 1
style: recovery_pipeline
maturity: experimental
sessions:
  fixer: { provider: absent }
steps:
  - id: repair
    type: command
    command: exit 1
    outputs: [never.txt]
    on_failure:
      route_to: { session: fixer, prompt: fix.md }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const parent = parseAgentFlowWorkflowOrThrow(`
name: preflight-failure-parent
version: 1
style: pipeline
maturity: experimental
steps:
  - id: child
    type: workflow
    workflow: preflight-failure-child
    inputs: {}
    outputs: [never.txt]
    on_failure: { then: fail }
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register(parent.name, parent)
      .register(child.name, child);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "preflight-failure-parent", workflow: parent });

    expect(await executeAgentFlowCommandPipeline(
      store, "preflight-failure-parent", parent, undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "failed", failedStep: "child" });
    const childRuns = store.listRuns().filter((run) => run.parentRunId === "preflight-failure-parent");
    expect(childRuns).toHaveLength(1);
    expect(childRuns[0]).toMatchObject({
      status: "failed",
      error: expect.objectContaining({ code: "nested_workflow.execution_failed" })
    });
    store.close();
  });

  test("rolls back child creation when copying an artifact-backed input fails", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-copy-failure-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const child = parseAgentFlowWorkflowOrThrow(`
name: copy-failure-child
version: 1
style: pipeline
maturity: experimental
inputs: { payload: { required: true } }
steps:
  - { id: done, type: result, status: completed }
  - { id: publish, type: command, command: "printf never > never.txt", outputs: [never.txt] }
`);
    const parent = parseAgentFlowWorkflowOrThrow(`
name: copy-failure-parent
version: 1
style: pipeline
maturity: experimental
inputs: { source: { required: true } }
steps:
  - id: child
    type: workflow
    workflow: copy-failure-child
    inputs: { payload: "{{ inputs.source }}" }
    outputs: [never.txt]
    on_failure: { then: fail }
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register(parent.name, parent)
      .register(child.name, child);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, {
      id: "copy-failure-parent",
      workflow: parent,
      inputs: { source: "source.txt" }
    });
    store.writeArtifact({
      id: "copy-source",
      runId: "copy-failure-parent",
      stepId: "fixture",
      path: "source.txt",
      kind: "fixture",
      contentType: "text/plain",
      content: "source\n"
    });
    const writeArtifact = store.writeArtifact.bind(store);
    store.writeArtifact = (input) => {
      if (input.kind === "recovery_input") throw new Error("injected child input copy failure");
      return writeArtifact(input);
    };

    expect(await executeAgentFlowCommandPipeline(
      store, "copy-failure-parent", parent, undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({
      status: "failed",
      failedStep: "child",
      message: "injected child input copy failure"
    });
    expect(store.listRuns().filter((run) => run.parentRunId === "copy-failure-parent")).toHaveLength(0);
    store.close();
  });

  test("does not launch a child after its parent is cancelled during child setup", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-setup-cancel-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const child = parseAgentFlowWorkflowOrThrow(`
name: setup-cancel-child
version: 1
style: pipeline
maturity: experimental
steps:
  - id: side_effect
    type: command
    command: printf ran > marker.txt
    outputs: [marker.txt]
`);
    const parent = parseAgentFlowWorkflowOrThrow(`
name: setup-cancel-parent
version: 1
style: pipeline
maturity: experimental
steps:
  - id: child
    type: workflow
    workflow: setup-cancel-child
    inputs: {}
    outputs: [marker.txt]
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register(parent.name, parent)
      .register(child.name, child);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "setup-cancel-parent", workflow: parent });
    const finalize = store.withRunFinalizationTransaction.bind(store);
    let cancelDuringSetup = true;
    store.withRunFinalizationTransaction = ((runId, callback) => {
      if (runId === "setup-cancel-parent" && cancelDuringSetup) {
        cancelDuringSetup = false;
        transitionAgentFlowLifecycleRun(store, runId, "cancel");
      }
      return finalize(runId, callback);
    }) as typeof store.withRunFinalizationTransaction;

    expect(await executeAgentFlowCommandPipeline(
      store, "setup-cancel-parent", parent, undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "cancelled", completedSteps: [] });
    const childRun = store.listRuns().find((run) => run.parentRunId === "setup-cancel-parent")!;
    expect(childRun.status).toBe("cancelled");
    expect(fs.existsSync(path.join(repo, "marker.txt"))).toBe(false);
    expect(store.listEvents(childRun.id).map((event) => event.type)).not.toContain("step.started");
    store.close();
  });

  test("rejects recursive programmatic workflow registries before creating another lineage run", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-recursive-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const parent = parseAgentFlowWorkflowOrThrow(`
name: recursive-a
version: 1
style: pipeline
maturity: experimental
steps:
  - id: child_b
    type: workflow
    workflow: recursive-b
    inputs: {}
    outputs: [never.txt]
`);
    const child = parseAgentFlowWorkflowOrThrow(`
name: recursive-b
version: 1
style: pipeline
maturity: experimental
steps:
  - id: child_a
    type: workflow
    workflow: recursive-a
    inputs: {}
    outputs: [never.txt]
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register(parent.name, parent)
      .register(child.name, child);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "recursive-root", workflow: parent });

    await expect(executeAgentFlowCommandPipeline(
      store, "recursive-root", parent, undefined, undefined, undefined, undefined, workflows
    )).rejects.toMatchObject({
      code: "AGENT_FLOW_WORKFLOW_INVALID",
      message: expect.stringContaining("Recursive workflow reference detected")
    });
    expect(store.listRuns()).toHaveLength(1);
    expect(store.getRun("recursive-root")?.status).toBe("pending");
    expect(store.listEvents("recursive-root").map((event) => event.type)).toEqual(["run.created"]);
    store.close();
  });

  test("rejects an incomplete programmatic registry before earlier steps can run", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-incomplete-registry-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const parent = parseAgentFlowWorkflowOrThrow(`
name: incomplete-registry-parent
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: effect, type: command, command: "printf side-effect > marker.txt", outputs: [marker.txt] }
  - { id: child, type: workflow, workflow: missing-child, inputs: {}, outputs: [never.txt] }
`);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "incomplete-registry-parent", workflow: parent });

    await expect(executeAgentFlowCommandPipeline(
      store,
      "incomplete-registry-parent",
      parent,
      undefined,
      undefined,
      undefined,
      undefined,
      createAgentFlowWorkflowRegistry()
    )).rejects.toMatchObject({ code: "AGENT_FLOW_WORKFLOW_REGISTRY_INCOMPLETE" });
    expect(store.getRun("incomplete-registry-parent")?.status).toBe("pending");
    expect(store.listEvents("incomplete-registry-parent").map((event) => event.type))
      .toEqual(["run.created"]);
    expect(fs.existsSync(path.join(repo, "marker.txt"))).toBe(false);
    store.close();
  });

  test("rejects invalid child input contracts before earlier parent steps can run", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-invalid-contract-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const parent = parseAgentFlowWorkflowOrThrow(`
name: invalid-contract-parent
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: effect, type: command, command: "printf side-effect > marker.txt", outputs: [marker.txt] }
  - { id: child, type: workflow, workflow: required-input-child, inputs: {}, outputs: [result.txt] }
`);
    const child = parseAgentFlowWorkflowOrThrow(`
name: required-input-child
version: 1
style: pipeline
maturity: experimental
inputs:
  required: { required: true }
steps:
  - { id: done, type: result, status: completed }
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register(parent.name, parent)
      .register(child.name, child);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "invalid-contract-parent", workflow: parent });

    await expect(executeAgentFlowCommandPipeline(
      store, "invalid-contract-parent", parent, undefined, undefined, undefined, undefined, workflows
    )).rejects.toMatchObject({
      code: "AGENT_FLOW_WORKFLOW_INVALID",
      message: expect.stringContaining("omits required inputs for required-input-child: required")
    });
    expect(store.getRun("invalid-contract-parent")?.status).toBe("pending");
    expect(store.listEvents("invalid-contract-parent").map((event) => event.type)).toEqual(["run.created"]);
    expect(fs.existsSync(path.join(repo, "marker.txt"))).toBe(false);
    store.close();
  });

  test("rejects undeclared child outputs before earlier parent steps can run", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-undeclared-output-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const parent = parseAgentFlowWorkflowOrThrow(`
name: undeclared-output-parent
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: effect, type: command, command: "printf side-effect > marker.txt", outputs: [marker.txt] }
  - { id: child, type: workflow, workflow: declared-output-child, inputs: {}, outputs: [typo.txt] }
`);
    const child = parseAgentFlowWorkflowOrThrow(`
name: declared-output-child
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: publish, type: command, command: "touch child-side-effect && printf done > done.txt", outputs: [done.txt] }
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register(parent.name, parent)
      .register(child.name, child);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "undeclared-output-parent", workflow: parent });

    await expect(executeAgentFlowCommandPipeline(
      store, "undeclared-output-parent", parent, undefined, undefined, undefined, undefined, workflows
    )).rejects.toMatchObject({
      code: "AGENT_FLOW_WORKFLOW_INVALID",
      message: expect.stringContaining("child workflow declared-output-child does not declare")
    });
    expect(store.getRun("undeclared-output-parent")?.status).toBe("pending");
    expect(fs.existsSync(path.join(repo, "marker.txt"))).toBe(false);
    expect(fs.existsSync(path.join(repo, "child-side-effect"))).toBe(false);
    store.close();
  });

  test("revalidates persisted child workflows before earlier parent steps can run", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-invalid-snapshot-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const parent = parseAgentFlowWorkflowOrThrow(`
name: invalid-snapshot-parent
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: effect, type: command, command: "printf side-effect > marker.txt", outputs: [marker.txt] }
  - { id: child, type: workflow, workflow: invalid-snapshot-child, inputs: {}, outputs: [result.txt] }
`);
    const child = {
      name: "invalid-snapshot-child",
      version: 1,
      style: "pipeline",
      maturity: "experimental",
      steps: [
        { id: "unknown", type: "unsupported" },
        { id: "publish", type: "command", command: "printf result > result.txt", outputs: ["result.txt"] }
      ]
    } as const;
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, {
      id: "invalid-snapshot-parent",
      workflow: parent,
      context: {
        workflowRegistry: {
          [parent.name]: parent,
          [child.name]: child
        } as never
      }
    });

    await expect(executeAgentFlowCommandPipeline(
      store, "invalid-snapshot-parent", parent
    )).rejects.toMatchObject({
      code: "AGENT_FLOW_WORKFLOW_INVALID",
      message: expect.stringContaining("persisted workflow invalid-snapshot-child failed validation")
    });
    expect(store.getRun("invalid-snapshot-parent")?.status).toBe("pending");
    expect(fs.existsSync(path.join(repo, "marker.txt"))).toBe(false);
    store.close();
  });

  test("rejects unauthorized nested failure continuation before launching the child", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-invalid-failure-policy-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const child = parseAgentFlowWorkflowOrThrow(`
name: failing-policy-child
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: terminal, type: result, status: failed, output: never.txt }
`);
    const parent = parseAgentFlowWorkflowOrThrow(`
name: invalid-failure-policy-parent
version: 1
style: pipeline
maturity: experimental
steps:
  - id: child
    type: workflow
    workflow: failing-policy-child
    inputs: {}
    outputs: [never.txt]
    on_failure: { then: continue }
  - { id: after, type: command, command: "touch continued" }
`);
    const store = await openAgentFlowRunState({ cwd: repo });
    store.createRunWithEvent({
      id: "invalid-failure-policy-parent",
      workflow: { name: parent.name, version: parent.version, style: parent.style, maturity: parent.maturity },
      context: { workflow: parent as never }
    }, { type: "run.created", payload: { status: "pending" } });

    expect(await executeAgentFlowCommandPipeline(
      store,
      "invalid-failure-policy-parent",
      parent,
      undefined,
      undefined,
      undefined,
      undefined,
      createAgentFlowWorkflowRegistry().register(child.name, child)
    )).toMatchObject({
      status: "failed",
      failedStep: "child",
      message: expect.stringContaining("on_failure.allowed is true")
    });
    expect(store.listRuns()).toHaveLength(1);
    expect(fs.existsSync(path.join(repo, "continued"))).toBe(false);
    store.close();
  });

  test("rejects noncanonical and duplicate nested outputs before launching the child", async () => {
    for (const [suffix, outputs, message] of [
      ["noncanonical", "[a/../done.txt]", "must use its normalized path"],
      ["duplicate", "[done.txt, done.txt]", "must not contain duplicate artifact path"]
    ] as const) {
      const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", `agent-flow-nested-${suffix}-outputs-`));
      fs.mkdirSync(path.join(repo, ".git"));
      const child = parseAgentFlowWorkflowOrThrow(`
name: output-contract-child
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: publish, type: command, command: "touch child-side-effect && printf done > done.txt", outputs: [done.txt] }
`);
      const parent = parseAgentFlowWorkflowOrThrow(`
name: ${suffix}-outputs-parent
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: child, type: workflow, workflow: output-contract-child, inputs: {}, outputs: ${outputs}, on_failure: { then: fail } }
`);
      const store = await openAgentFlowRunState({ cwd: repo });
      store.createRunWithEvent({
        id: `${suffix}-outputs-parent`,
        workflow: { name: parent.name, version: parent.version, style: parent.style, maturity: parent.maturity },
        context: { workflow: parent as never }
      }, { type: "run.created", payload: { status: "pending" } });

      expect(await executeAgentFlowCommandPipeline(
        store,
        `${suffix}-outputs-parent`,
        parent,
        undefined,
        undefined,
        undefined,
        undefined,
        createAgentFlowWorkflowRegistry().register(child.name, child)
      )).toMatchObject({ status: "failed", failedStep: "child", message: expect.stringContaining(message) });
      expect(store.listRuns()).toHaveLength(1);
      expect(fs.existsSync(path.join(repo, "child-side-effect"))).toBe(false);
      store.close();
    }
  });

  test("pins the reachable workflow registry across programmatic resumes", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-registry-drift-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const parent = parseAgentFlowWorkflowOrThrow(`
name: registry-drift-parent
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: gate, type: manual_gate, message: Continue?, options: [approve, cancel] }
  - { id: child, type: workflow, workflow: child-alias, inputs: {}, outputs: [result.txt] }
`);
    const child = (value: string) => parseAgentFlowWorkflowOrThrow(`
name: registry-drift-child
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: publish, type: command, command: "printf ${value} > result.txt", outputs: [result.txt] }
`);
    const original = createAgentFlowWorkflowRegistry()
      .register("parent-alias", parent)
      .register("child-alias", child("original"));
    const changed = createAgentFlowWorkflowRegistry()
      .register("parent-alias", parent)
      .register("child-alias", child("changed"));
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, {
      id: "registry-drift-parent",
      workflow: parent,
      context: { workflowRegistryName: "parent-alias" }
    });

    expect(await executeAgentFlowCommandPipeline(
      store, "registry-drift-parent", parent, undefined, undefined, undefined, undefined, original
    )).toMatchObject({ status: "paused" });
    expect(await resumeAgentFlowCommandPipeline(
      store, "registry-drift-parent", parent, { outcome: "approve" },
      undefined, undefined, undefined, undefined, changed
    )).toMatchObject({ status: "completed" });
    expect(store.readArtifact("registry-drift-parent", "result.txt").content.toString()).toBe("original");
    expect(store.getRun("registry-drift-parent")?.context.workflowRegistry)
      .toMatchObject({ "child-alias": expect.objectContaining({ name: "registry-drift-child" }) });
    store.close();
  });

  test("propagates a live nested child lease without failing or retrying the parent step", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-child-lock-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const child = parseAgentFlowWorkflowOrThrow(`
name: locked-child
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: publish, type: command, command: "printf done > done.txt", outputs: [done.txt] }
`);
    const parent = parseAgentFlowWorkflowOrThrow(`
name: locked-child-parent
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: child, type: workflow, workflow: locked-child, inputs: {}, outputs: [done.txt], on_failure: { retry: 1 } }
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register(parent.name, parent)
      .register(child.name, child);
    const store = await openAgentFlowRunState({ cwd: repo });
    const competitor = await openAgentFlowRunState({ cwd: repo });
    const parentRunId = "locked-child-parent";
    const childRunId = `${parentRunId}:workflow:child-ddc9e669:attempt-1`;
    createAgentFlowLifecycleRun(store, { id: parentRunId, workflow: parent });
    createAgentFlowLifecycleRun(store, {
      id: childRunId,
      workflow: child,
      inputs: {},
      parentRunId
    });
    let entered!: () => void;
    let release!: () => void;
    const lockEntered = new Promise<void>((resolve) => { entered = resolve; });
    const lockRelease = new Promise<void>((resolve) => { release = resolve; });
    const heldLock = competitor.withRunLock(childRunId, "run", async () => {
      entered();
      await lockRelease;
    });
    await lockEntered;

    await expect(executeAgentFlowCommandPipeline(
      store, parentRunId, parent, undefined, undefined, undefined, undefined, workflows
    )).rejects.toMatchObject({ code: "AGENT_FLOW_RUN_LOCKED" });
    expect(store.getRun(parentRunId)?.status).toBe("running");
    expect(store.listSteps(parentRunId)).toEqual([
      expect.objectContaining({ stepId: "child", attempt: 1, status: "running" })
    ]);
    expect(store.listEvents(parentRunId).some((event) => event.type === "step.failed")).toBe(false);
    expect(store.listRuns().filter((run) => run.parentRunId === parentRunId)).toHaveLength(1);

    release();
    await heldLock;
    competitor.close();
    store.close();
  });

  test("uses registry aliases rather than internal workflow names for recursion identity", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-alias-identity-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const parent = parseAgentFlowWorkflowOrThrow(`
name: shared-internal-name
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: child, type: workflow, workflow: child-alias, inputs: {}, outputs: [done.txt] }
`);
    const child = parseAgentFlowWorkflowOrThrow(`
name: shared-internal-name
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: done, type: command, command: "printf done > done.txt", outputs: [done.txt] }
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register("parent-alias", parent)
      .register("child-alias", child);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "alias-identity-parent", workflow: parent });

    expect(await executeAgentFlowCommandPipeline(
      store, "alias-identity-parent", parent, undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "completed", completedSteps: ["child"] });
    expect(store.listRuns().find((run) => run.parentRunId === "alias-identity-parent")?.context)
      .toMatchObject({ workflowRegistryName: "child-alias" });
    store.close();
  });

  test("retries failed children and honors explicit continuation", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-retry-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const child = parseAgentFlowWorkflowOrThrow(`
name: failing-child
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: break, type: command, command: exit 1, outputs: [never.txt], on_failure: { then: fail } }
`);
    const parent = parseAgentFlowWorkflowOrThrow(`
name: retry-parent
version: 1
style: pipeline
maturity: experimental
steps:
  - id: child
    type: workflow
    workflow: failing-child
    inputs: {}
    outputs: [never.txt]
    on_failure: { retry: 1, then: continue, allowed: true }
  - id: later
    type: command
    command: printf 'continued\\n' > later.txt
    outputs: [later.txt]
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register(parent.name, parent)
      .register(child.name, child);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "retry-parent", workflow: parent });

    expect(await executeAgentFlowCommandPipeline(
      store, "retry-parent", parent, undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "completed" });
    expect(store.listRuns().filter((run) => run.parentRunId === "retry-parent"))
      .toHaveLength(2);
    expect(store.readArtifact("retry-parent", "later.txt").content.toString("utf8")).toBe("continued\n");
    store.close();
  });

  test("applies retry and continuation after a paused child later fails", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-resumed-failure-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const child = parseAgentFlowWorkflowOrThrow(`
name: paused-failing-child
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: approve, type: manual_gate, message: Continue?, options: [approve, cancel] }
  - { id: break, type: command, command: exit 1, outputs: [never.txt], on_failure: { then: fail } }
`);
    const parent = parseAgentFlowWorkflowOrThrow(`
name: resumed-retry-parent
version: 1
style: pipeline
maturity: experimental
steps:
  - id: child
    type: workflow
    workflow: paused-failing-child
    inputs: {}
    outputs: [never.txt]
    on_failure: { retry: 1, then: continue, allowed: true }
  - { id: done, type: result, status: completed }
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register(parent.name, parent)
      .register(child.name, child);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "resumed-retry", workflow: parent });

    expect(await executeAgentFlowCommandPipeline(
      store, "resumed-retry", parent, undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "paused" });
    expect(await resumeAgentFlowCommandPipeline(
      store, "resumed-retry", parent, { outcome: "approve" }, undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "paused" });
    expect(await resumeAgentFlowCommandPipeline(
      store, "resumed-retry", parent, { outcome: "approve" }, undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "completed" });
    expect(store.listRuns().filter((run) => run.parentRunId === "resumed-retry"))
      .toHaveLength(2);
    store.close();
  });

  test("resets resumed child retry accounting for each routed visit", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-routed-retry-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const child = parseAgentFlowWorkflowOrThrow(`
name: routed-retry-child
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: approve, type: manual_gate, message: Continue?, options: [approve, cancel] }
  - { id: publish, type: command, command: "printf done > child.txt", outputs: [child.txt] }
`);
    const parent = parseAgentFlowWorkflowOrThrow(`
name: routed-retry-parent
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_recovery_cycles: 3 }
steps:
  - id: child
    type: workflow
    workflow: routed-retry-child
    inputs: {}
    outputs: [child.txt]
    goto: child
    on_failure: { retry: 1, then: continue, allowed: true }
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register(parent.name, parent)
      .register(child.name, child);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "routed-retry", workflow: parent });

    expect(await executeAgentFlowCommandPipeline(
      store, "routed-retry", parent, undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "paused" });
    expect(await resumeAgentFlowCommandPipeline(
      store, "routed-retry", parent, { outcome: "approve" }, undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "paused" });
    expect(await resumeAgentFlowCommandPipeline(
      store, "routed-retry", parent, { outcome: "cancel" }, undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "paused" });
    expect(store.listRuns().filter((run) => run.parentRunId === "routed-retry"))
      .toHaveLength(3);
    store.close();
  });
});
