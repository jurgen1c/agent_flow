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
    const child = (provider: string) => parseAgentFlowWorkflowOrThrow(`
name: shared-internal-name
version: 1
style: pipeline
maturity: experimental
sessions:
  worker: { provider: ${provider} }
steps:
  - { id: done, type: result, status: completed }
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
      .register("first-alias", child("first-provider"))
      .register("second-alias", child("second-provider"));
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
`);
    const parent = parseAgentFlowWorkflowOrThrow(`
name: setup-failure-parent
version: 1
style: pipeline
maturity: experimental
steps:
  - id: child
    type: workflow
    workflow: required-child
    inputs: {}
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

    expect(await executeAgentFlowCommandPipeline(
      store, "recursive-root", parent, undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "paused", failedStep: "child_b" });
    const runs = store.listRuns();
    expect(runs).toHaveLength(2);
    const childRun = runs.find((run) => run.parentRunId === "recursive-root")!;
    expect(JSON.stringify(store.listSteps(childRun.id).at(-1)?.error))
      .toContain("already present in run recursive-root's parent lineage");
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
  - { id: break, type: command, command: exit 1, on_failure: { then: fail } }
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
  - { id: break, type: command, command: exit 1, on_failure: { then: fail } }
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
