import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import path from "node:path";
import {
  createAgentFlowLifecycleRun,
  createAgentFlowSessionProviderRegistry,
  createAgentFlowWorkflowRegistry,
  executeAgentFlowCommandPipeline,
  openAgentFlowRunState,
  parseAgentFlowWorkflowOrThrow,
  resumeAgentFlowCommandPipeline
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

    const resumed = await resumeAgentFlowCommandPipeline(
      store, "parent", parent, { outcome: "approve" }, undefined, undefined, undefined, undefined, workflows
    );
    expect(resumed).toMatchObject({ status: "completed" });
    expect(store.getRun(childRunId)?.status).toBe("completed");
    expect(store.readArtifact("parent", "child.txt").content.toString("utf8")).toBe("child output\n");
    expect(store.getArtifact("parent", "child.txt")?.metadata).toMatchObject({
      childRunId
    });
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
        content: "opaque-value"
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
    createAgentFlowLifecycleRun(store, { id: "terminal-parent", workflow: parent });

    expect(await executeAgentFlowCommandPipeline(
      store, "terminal-parent", parent, undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "paused" });
    const childRunId = (store.getRun("terminal-parent")!.context.waiting as { childRunId: string }).childRunId;
    expect(await resumeAgentFlowCommandPipeline(
      store, childRunId, child, { outcome: "approve" }, undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "completed" });

    expect(await resumeAgentFlowCommandPipeline(
      store, "terminal-parent", parent, { outcome: "approve" },
      undefined, undefined, undefined, undefined, workflows
    )).toMatchObject({ status: "completed" });
    expect(store.readArtifact("terminal-parent", "child.txt").content.toString("utf8")).toBe("done");
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
