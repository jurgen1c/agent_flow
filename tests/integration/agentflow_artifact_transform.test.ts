import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  type AgentflowRunStateValue,
  AgentflowArtifactTransformRegistry,
  MAX_AGENTFLOW_TRANSFORM_INPUT_BYTES,
  createAgentflowLifecycleRun,
  executeAgentflowCommandPipeline,
  openAgentflowRunState,
  parseAgentflowSimulationFixture,
  parseAgentflowWorkflowOrThrow,
  simulateAgentflowWorkflow,
  transitionAgentflowLifecycleRun
} from "../../packages/agentflow-core/src";
import { runCli } from "../../packages/agentflow-cli/src/router";

const repoRoot = path.resolve(".");
const examplePath = path.join(repoRoot, "agentflow-examples/agentflow-examples/workflows/jira-ticket-spec.yml");
const fixturePath = path.join(repoRoot, "tests/fixtures/agentflow/simulation/jira-ticket.json");

describe("Agentflow artifact transform steps", () => {
  test("simulates jira-ticket-spec with a fixture-derived Markdown artifact", () => {
    const workflow = parseAgentflowWorkflowOrThrow(fs.readFileSync(examplePath, "utf8"));
    const fixtureResult = parseAgentflowSimulationFixture(fs.readFileSync(fixturePath, "utf8"));
    expect(fixtureResult.ok).toBe(true);
    if (!fixtureResult.ok) throw new Error(fixtureResult.error);

    const result = simulateAgentflowWorkflow(workflow, fixtureResult.fixture);

    expect(result.status).toBe("completed");
    expect(result.availableArtifacts).toEqual(["spec.md", "ticket.json", "ticket.md"]);
    expect(result.artifactValues["ticket.md"]).toContain("# AM-24: Implement Agentflow artifact transform steps for pipelines");
    expect(result.artifactValues["ticket.md"]).toContain("- Status: In Progress");
  });

  test("runs the jira-ticket-spec transform against a declared fixture artifact", async () => {
    const fullWorkflow = parseAgentflowWorkflowOrThrow(fs.readFileSync(examplePath, "utf8"));
    const transformStep = fullWorkflow.steps.find((step) => step.type === "artifact_transform")!;
    const workflow = { ...fullWorkflow, steps: [transformStep] };
    const fixtureResult = parseAgentflowSimulationFixture(fs.readFileSync(fixturePath, "utf8"));
    if (!fixtureResult.ok) throw new Error(fixtureResult.error);
    const ticket = fixtureResult.fixture.steps?.fetch_ticket?.outputs;
    if (Array.isArray(ticket) || ticket?.["ticket.json"] === undefined) throw new Error("Fixture ticket is missing.");

    const temporaryRoot = temporaryRepo();
    const store = await openAgentflowRunState({ cwd: temporaryRoot });
    createAgentflowLifecycleRun(store, { id: "jira-transform", workflow });
    store.writeArtifact({
      id: "fixture-ticket",
      runId: "jira-transform",
      stepId: "fixture",
      path: "ticket.json",
      kind: "fixture",
      contentType: "application/json",
      content: JSON.stringify(ticket["ticket.json"])
    });

    const result = await executeAgentflowCommandPipeline(store, "jira-transform", workflow);

    expect(result).toMatchObject({ status: "completed", completedSteps: ["render_ticket"] });
    expect(store.listArtifacts("jira-transform").map((artifact) => artifact.declaredPath)).toEqual(["ticket.json", "ticket.md"]);
    expect(store.readArtifact("jira-transform", "ticket.md").content.toString("utf8"))
      .toContain("## Description\n\nNormalize structured artifacts between pipeline steps.");
    expect(store.listEvents("jira-transform").map((event) => event.type)).toEqual([
      "run.created",
      "run.started",
      "step.started",
      "step.completed",
      "run.completed"
    ]);
    store.close();
  });

  test("routes command output through a transform in the CLI pipeline", async () => {
    const temporaryRoot = temporaryRepo();
    const workflowPath = path.join(temporaryRoot, "pipeline.yml");
    fs.writeFileSync(workflowPath, `name: transform-pipeline
version: 1
style: pipeline
maturity: experimental
steps:
  - id: fixture
    type: command
    command: printf '\\173"key":"AM-24","fields":\\173"summary":"Transform pipeline"\\175\\175' > ticket.json
    outputs: [ticket.json]
  - id: render
    type: artifact_transform
    input: ticket.json
    output: ticket.md
    transform: jira_ticket_to_markdown
`);
    const stdout: string[] = [];
    const stderr: string[] = [];

    const exitCode = await runCli(
      ["run", "pipeline.yml", "--id", "cli-transform"],
      { stdout: { write: (value) => stdout.push(String(value)) }, stderr: { write: (value) => stderr.push(String(value)) } },
      { cwd: temporaryRoot }
    );

    expect(stderr).toEqual([]);
    expect(exitCode).toBe(0);
    expect(stdout.join("")).toContain("Completed steps: fixture, render");
    const store = await openAgentflowRunState({ cwd: temporaryRoot });
    expect(store.readArtifact("cli-transform", "ticket.md").content.toString("utf8"))
      .toBe("# AM-24: Transform pipeline\n");
    store.close();
  });

  test("fails with an actionable error when the declared input artifact is missing", async () => {
    const workflow = transformWorkflow("jira_ticket_to_markdown");
    const temporaryRoot = temporaryRepo();
    const store = await openAgentflowRunState({ cwd: temporaryRoot });
    createAgentflowLifecycleRun(store, { id: "missing-input", workflow });

    const result = await executeAgentflowCommandPipeline(store, "missing-input", workflow);

    expect(result).toMatchObject({ status: "paused", failedStep: "render" });
    expect(result.message).toContain("Could not read declared input ticket.json");
    expect(result.message).toContain("publish it before running the transform");
    expect(store.listArtifacts("missing-input")).toEqual([]);
    store.close();
  });

  test("rejects unknown transforms unless they are explicitly registered", async () => {
    const workflow = transformWorkflow("uppercase");
    const temporaryRoot = temporaryRepo();

    const rejectedStore = await openAgentflowRunState({ cwd: temporaryRoot });
    createAgentflowLifecycleRun(rejectedStore, { id: "unknown-transform", workflow });
    seedInput(rejectedStore, "unknown-transform", "hello");
    const rejected = await executeAgentflowCommandPipeline(rejectedStore, "unknown-transform", workflow);
    expect(rejected.status).toBe("paused");
    expect(rejected.message).toContain("Unknown artifact transform uppercase");
    rejectedStore.close();

    const registeredStore = await openAgentflowRunState({ cwd: temporaryRoot });
    createAgentflowLifecycleRun(registeredStore, { id: "registered-transform", workflow });
    seedInput(registeredStore, "registered-transform", "hello");
    const registry = new AgentflowArtifactTransformRegistry().register("uppercase", (input) => ({
      content: Buffer.from(input).toString("utf8").toUpperCase(),
      contentType: "text/plain; charset=utf-8"
    }));

    const completed = await executeAgentflowCommandPipeline(registeredStore, "registered-transform", workflow, registry);

    expect(completed.status).toBe("completed");
    expect(registeredStore.readArtifact("registered-transform", "ticket.md").content.toString("utf8")).toBe("HELLO");
    registeredStore.close();
  });

  test("reuses the path owner when a transform explicitly overwrites another producer", async () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: overwrite-transform
version: 1
style: pipeline
maturity: experimental
steps:
  - id: fixture
    type: command
    command: printf '\\173"key":"AM-24","fields":\\173"summary":"Overwrite"\\175\\175' > ticket.json
    outputs: [ticket.json]
  - id: render
    type: artifact_transform
    input: ticket.json
    output: ticket.json
    transform: jira_ticket_to_markdown
    overwrite: true
`);
    const temporaryRoot = temporaryRepo();
    const store = await openAgentflowRunState({ cwd: temporaryRoot });
    createAgentflowLifecycleRun(store, { id: "overwrite-transform", workflow });

    const result = await executeAgentflowCommandPipeline(store, "overwrite-transform", workflow);

    expect(result.status).toBe("completed");
    const artifacts = store.listArtifacts("overwrite-transform").filter((artifact) => artifact.declaredPath === "ticket.json");
    expect(artifacts).toHaveLength(1);
    expect(artifacts[0]).toMatchObject({ kind: "artifact_transform", status: "overwritten" });
    expect(store.readArtifact("overwrite-transform", "ticket.json").content.toString("utf8"))
      .toBe("# AM-24: Overwrite\n");
    store.close();
  });

  test("retries registered transforms using distinct persisted attempts", async () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: retry-transform
version: 1
style: pipeline
maturity: experimental
steps:
  - id: render
    type: artifact_transform
    input: ticket.json
    output: ticket.md
    transform: flaky
    on_failure:
      retry: 1
      then: fail
`);
    const temporaryRoot = temporaryRepo();
    const store = await openAgentflowRunState({ cwd: temporaryRoot });
    createAgentflowLifecycleRun(store, { id: "retry-transform", workflow });
    seedInput(store, "retry-transform", "hello");
    let calls = 0;
    const registry = new AgentflowArtifactTransformRegistry().register("flaky", (input) => {
      calls += 1;
      if (calls === 1) throw new Error("transient transform failure");
      return { content: input, contentType: "text/plain" };
    });

    const result = await executeAgentflowCommandPipeline(store, "retry-transform", workflow, registry);

    expect(result.status).toBe("completed");
    expect(calls).toBe(2);
    expect(store.listEvents("retry-transform").map((event) => event.type)).toEqual([
      "run.created",
      "run.started",
      "step.started",
      "step.failed",
      "step.started",
      "step.completed",
      "run.completed"
    ]);
    expect(store.readArtifact("retry-transform", "ticket.md").content.toString("utf8")).toBe("hello");
    store.close();
  });

  test("fails closed for excessive retries on an externally persisted workflow", async () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: invalid-transform-retry
version: 1
style: pipeline
maturity: experimental
steps:
  - id: render
    type: artifact_transform
    input: ticket.json
    output: ticket.md
    transform: never_run
    on_failure:
      retry: 101
      then: fail
`);
    const temporaryRoot = temporaryRepo();
    const store = await openAgentflowRunState({ cwd: temporaryRoot });
    store.createRunWithEvent({
      id: "invalid-transform-retry",
      workflow: { name: workflow.name, version: workflow.version, style: workflow.style, maturity: workflow.maturity },
      context: { workflow: workflow as unknown as AgentflowRunStateValue }
    }, { type: "run.created", payload: { status: "pending" } });
    seedInput(store, "invalid-transform-retry", "hello");
    let calls = 0;
    const registry = new AgentflowArtifactTransformRegistry().register("never_run", () => {
      calls += 1;
      throw new Error("must not run");
    });

    const result = await executeAgentflowCommandPipeline(store, "invalid-transform-retry", workflow, registry);

    expect(result).toMatchObject({ status: "failed", failedStep: "render" });
    expect(result.message).toContain("integer from 0 through 100");
    expect(calls).toBe(0);
    store.close();
  });

  test("fails closed for an unapproved ignored transform failure on an externally persisted workflow", async () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: invalid-transform-ignore
version: 1
style: pipeline
maturity: experimental
steps:
  - id: render
    type: artifact_transform
    input: ticket.json
    output: ticket.md
    transform: never_run
    on_failure: { then: " ignore " }
`);
    const temporaryRoot = temporaryRepo();
    const store = await openAgentflowRunState({ cwd: temporaryRoot });
    store.createRunWithEvent({
      id: "invalid-transform-ignore",
      workflow: { name: workflow.name, version: workflow.version, style: workflow.style, maturity: workflow.maturity },
      context: { workflow: workflow as unknown as AgentflowRunStateValue }
    }, { type: "run.created", payload: { status: "pending" } });
    seedInput(store, "invalid-transform-ignore", "hello");
    let calls = 0;
    const registry = new AgentflowArtifactTransformRegistry().register("never_run", () => {
      calls += 1;
      return { content: "unexpected", contentType: "text/plain" };
    });

    const result = await executeAgentflowCommandPipeline(store, "invalid-transform-ignore", workflow, registry);

    expect(result).toMatchObject({ status: "failed", failedStep: "render" });
    expect(result.message).toContain("on_failure.allowed is true");
    expect(calls).toBe(0);
    store.close();
  });

  test("fails closed for unsupported transform failure targets on externally persisted workflows", async () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: invalid-transform-target
version: 1
style: pipeline
maturity: experimental
steps:
  - id: render
    type: artifact_transform
    input: ticket.json
    output: ticket.md
    transform: never_run
    on_failure: { then: complete }
`);
    const temporaryRoot = temporaryRepo();
    const store = await openAgentflowRunState({ cwd: temporaryRoot });
    store.createRunWithEvent({
      id: "invalid-transform-target",
      workflow: { name: workflow.name, version: workflow.version, style: workflow.style, maturity: workflow.maturity },
      context: { workflow: workflow as unknown as AgentflowRunStateValue }
    }, { type: "run.created", payload: { status: "pending" } });
    seedInput(store, "invalid-transform-target", "hello");
    let calls = 0;
    const registry = new AgentflowArtifactTransformRegistry().register("never_run", () => {
      calls += 1;
      return { content: "unexpected", contentType: "text/plain" };
    });

    const result = await executeAgentflowCommandPipeline(store, "invalid-transform-target", workflow, registry);

    expect(result).toMatchObject({ status: "failed", failedStep: "render" });
    expect(result.message).toContain("supports only retry");
    expect(calls).toBe(0);
    store.close();
  });

  test("does not publish transform output after concurrent cancellation", async () => {
    const workflow = transformWorkflow("cancel_during_transform");
    const temporaryRoot = temporaryRepo();
    const store = await openAgentflowRunState({ cwd: temporaryRoot });
    createAgentflowLifecycleRun(store, { id: "cancel-transform", workflow });
    seedInput(store, "cancel-transform", "hello");
    const registry = new AgentflowArtifactTransformRegistry().register("cancel_during_transform", (input) => {
      transitionAgentflowLifecycleRun(store, "cancel-transform", "cancel");
      return { content: input, contentType: "text/plain" };
    });

    const result = await executeAgentflowCommandPipeline(store, "cancel-transform", workflow, registry);

    expect(result).toMatchObject({ status: "cancelled", completedSteps: [] });
    expect(store.getArtifact("cancel-transform", "ticket.md")).toBeNull();
    expect(store.listEvents("cancel-transform").map((event) => event.type)).toEqual([
      "run.created",
      "run.started",
      "step.started",
      "run.cancel",
      "step.interrupted"
    ]);
    store.close();
  });

  test("checks cancellation atomically at the artifact publication boundary", async () => {
    const workflow = transformWorkflow("copy");
    const temporaryRoot = temporaryRepo();
    const store = await openAgentflowRunState({ cwd: temporaryRoot });
    createAgentflowLifecycleRun(store, { id: "cancel-at-publish", workflow });
    seedInput(store, "cancel-at-publish", "hello");
    const registry = new AgentflowArtifactTransformRegistry().register("copy", (input) => ({
      content: input,
      contentType: "text/plain"
    }));
    const writeArtifact = store.writeArtifact.bind(store);
    store.writeArtifact = (input) => {
      transitionAgentflowLifecycleRun(store, "cancel-at-publish", "cancel");
      return writeArtifact(input);
    };

    const result = await executeAgentflowCommandPipeline(store, "cancel-at-publish", workflow, registry);

    expect(result).toMatchObject({ status: "cancelled", completedSteps: [] });
    expect(store.getArtifact("cancel-at-publish", "ticket.md")).toBeNull();
    expect(store.listEvents("cancel-at-publish").map((event) => event.type)).toContain("step.interrupted");
    store.close();
  });

  test("returns cancellation that lands immediately after artifact publication", async () => {
    const workflow = transformWorkflow("copy");
    const temporaryRoot = temporaryRepo();
    const store = await openAgentflowRunState({ cwd: temporaryRoot });
    createAgentflowLifecycleRun(store, { id: "cancel-after-publish", workflow });
    seedInput(store, "cancel-after-publish", "hello");
    const registry = new AgentflowArtifactTransformRegistry().register("copy", (input) => ({
      content: input,
      contentType: "text/plain"
    }));
    const writeArtifact = store.writeArtifact.bind(store);
    store.writeArtifact = (input) => {
      const artifact = writeArtifact(input);
      if (input.path === "ticket.md") transitionAgentflowLifecycleRun(store, "cancel-after-publish", "cancel");
      return artifact;
    };

    const result = await executeAgentflowCommandPipeline(store, "cancel-after-publish", workflow, registry);

    expect(result).toMatchObject({ status: "cancelled", completedSteps: [] });
    expect(store.getArtifact("cancel-after-publish", "ticket.md")).not.toBeNull();
    expect(store.listEvents("cancel-after-publish").map((event) => event.type)).toEqual([
      "run.created",
      "run.started",
      "step.started",
      "run.cancel",
      "step.interrupted"
    ]);
    store.close();
  });

  test("rejects output publication when the transform input is concurrently overwritten", async () => {
    const workflow = transformWorkflow("overwrite_input");
    const temporaryRoot = temporaryRepo();
    const firstStore = await openAgentflowRunState({ cwd: temporaryRoot });
    const secondStore = await openAgentflowRunState({ cwd: temporaryRoot });
    createAgentflowLifecycleRun(firstStore, { id: "concurrent-input", workflow });
    firstStore.writeArtifact({
      id: "fixture-ticket",
      runId: "concurrent-input",
      path: "ticket.json",
      kind: "fixture",
      contentType: "text/plain",
      content: "old"
    });
    const registry = new AgentflowArtifactTransformRegistry().register("overwrite_input", (input) => {
      secondStore.writeArtifact({
        id: "fixture-ticket",
        runId: "concurrent-input",
        path: "ticket.json",
        kind: "fixture",
        contentType: "text/plain",
        content: "new",
        overwrite: true
      });
      return { content: input, contentType: "text/plain" };
    });

    const result = await executeAgentflowCommandPipeline(firstStore, "concurrent-input", workflow, registry);

    expect(result).toMatchObject({ status: "paused", failedStep: "render" });
    expect(result.message).toContain("was overwritten before ticket.md could be published");
    expect(firstStore.getArtifact("concurrent-input", "ticket.md")).toBeNull();
    firstStore.close();
    secondStore.close();
  });

  test("rejects output publication when input backing bytes change without a registry update", async () => {
    const workflow = transformWorkflow("mutate_backing_file");
    const temporaryRoot = temporaryRepo();
    const store = await openAgentflowRunState({ cwd: temporaryRoot });
    createAgentflowLifecycleRun(store, { id: "stale-backing-input", workflow });
    seedInput(store, "stale-backing-input", "old");
    const input = store.getArtifact("stale-backing-input", "ticket.json");
    if (input === null) throw new Error("Expected seeded input artifact.");
    const registry = new AgentflowArtifactTransformRegistry().register("mutate_backing_file", (content) => {
      fs.writeFileSync(path.join(temporaryRoot, input.storagePath), "new");
      return { content, contentType: "text/plain" };
    });

    const result = await executeAgentflowCommandPipeline(store, "stale-backing-input", workflow, registry);

    expect(result).toMatchObject({ status: "paused", failedStep: "render" });
    expect(result.message).toContain("was overwritten before ticket.md could be published");
    expect(store.getArtifact("stale-backing-input", "ticket.md")).toBeNull();
    store.close();
  });

  test("rejects oversized inputs through a bounded artifact read", async () => {
    const workflow = transformWorkflow("jira_ticket_to_markdown");
    const temporaryRoot = temporaryRepo();
    const store = await openAgentflowRunState({ cwd: temporaryRoot });
    createAgentflowLifecycleRun(store, { id: "large-transform", workflow });
    store.writeArtifact({
      id: "large-input",
      runId: "large-transform",
      path: "ticket.json",
      kind: "fixture",
      contentType: "application/json",
      content: Buffer.alloc(MAX_AGENTFLOW_TRANSFORM_INPUT_BYTES + 1, 32)
    });

    const originalReadSync = fs.readSync;
    let bytesRead = 0;
    Object.defineProperty(fs, "readSync", {
      configurable: true,
      value: (...args: Parameters<typeof fs.readSync>) => {
        const read = originalReadSync(...args);
        bytesRead += read;
        return read;
      }
    });
    let result;
    try {
      result = await executeAgentflowCommandPipeline(store, "large-transform", workflow);
    } finally {
      Object.defineProperty(fs, "readSync", { configurable: true, value: originalReadSync });
    }

    expect(result.status).toBe("paused");
    expect(result.message).toContain(`exceeds the ${MAX_AGENTFLOW_TRANSFORM_INPUT_BYTES}-byte read limit`);
    expect(result.message).toContain("Reduce the artifact size");
    expect(bytesRead).toBe(0);
    store.close();
  });

  test("rejects an oversized stale replacement before checksumming it", async () => {
    const workflow = transformWorkflow("jira_ticket_to_markdown");
    const temporaryRoot = temporaryRepo();
    const store = await openAgentflowRunState({ cwd: temporaryRoot });
    createAgentflowLifecycleRun(store, { id: "stale-large-transform", workflow });
    seedInput(store, "stale-large-transform", "{}");
    const input = store.getArtifact("stale-large-transform", "ticket.json")!;
    fs.writeFileSync(path.join(temporaryRoot, input.storagePath), Buffer.alloc(MAX_AGENTFLOW_TRANSFORM_INPUT_BYTES + 1, 32));
    const originalReadSync = fs.readSync;
    let bytesRead = 0;
    Object.defineProperty(fs, "readSync", {
      configurable: true,
      value: (...args: Parameters<typeof fs.readSync>) => {
        const read = originalReadSync(...args);
        bytesRead += read;
        return read;
      }
    });
    let result;
    try {
      result = await executeAgentflowCommandPipeline(store, "stale-large-transform", workflow);
    } finally {
      Object.defineProperty(fs, "readSync", { configurable: true, value: originalReadSync });
    }

    expect(result.status).toBe("paused");
    expect(result.message).toContain(`exceeds the ${MAX_AGENTFLOW_TRANSFORM_INPUT_BYTES}-byte read limit`);
    expect(bytesRead).toBe(0);
    store.close();
  });

  test("rejects artifact bytes from a registry entry overwritten during the read", async () => {
    const temporaryRoot = temporaryRepo();
    const firstStore = await openAgentflowRunState({ cwd: temporaryRoot });
    const workflow = transformWorkflow("jira_ticket_to_markdown");
    createAgentflowLifecycleRun(firstStore, { id: "concurrent-read", workflow });
    seedInput(firstStore, "concurrent-read", "old");
    const secondStore = await openAgentflowRunState({ cwd: temporaryRoot });
    const openSync = fs.openSync;
    let overwrite = true;
    Object.defineProperty(fs, "openSync", {
      configurable: true,
      value: (...args: Parameters<typeof fs.openSync>) => {
        const descriptor = openSync(...args);
        if (overwrite && String(args[0]).includes("/artifacts/")) {
          overwrite = false;
          secondStore.writeArtifact({
            id: "fixture-ticket",
            runId: "concurrent-read",
            path: "ticket.json",
            kind: "fixture",
            contentType: "text/plain",
            content: "new",
            overwrite: true
          });
        }
        return descriptor;
      }
    });

    try {
      expect(() => firstStore.readArtifact("concurrent-read", "ticket.json"))
        .toThrow("was overwritten while it was being read");
    } finally {
      Object.defineProperty(fs, "openSync", { configurable: true, value: openSync });
      firstStore.close();
      secondStore.close();
    }
  });

  test("reports missing and unknown transform inputs during simulation", () => {
    const missing = simulateAgentflowWorkflow(transformWorkflow("jira_ticket_to_markdown"), {});
    expect(missing.status).toBe("unresolved");
    expect(missing.missingArtifacts).toEqual([{ stepId: "render", artifact: "ticket.json", kind: "input" }]);
    expect(missing.availableArtifacts).toEqual([]);

    const unknown = simulateAgentflowWorkflow(transformWorkflow("unknown"), {
      artifacts: { "ticket.json": { key: "AM-24", fields: { summary: "Transform" } } }
    });
    expect(unknown.status).toBe("unresolved");
    expect(unknown.unresolvedBranches[0]?.reason).toContain("Unknown artifact transform unknown");
    expect(unknown.availableArtifacts).toEqual(["ticket.json"]);
    expect(unknown.visitedSteps.map((step) => step.id)).toEqual(["render"]);
  });

  test("pauses fixture-declared transform failures without an explicit policy", () => {
    const result = simulateAgentflowWorkflow(transformWorkflow("jira_ticket_to_markdown"), {
      artifacts: { "ticket.json": { key: "AM-29", fields: { summary: "Failure behavior" } } },
      steps: { render: { outcome: "failed" } }
    });

    expect(result.status).toBe("paused");
    expect(result.visitedSteps).toEqual([{ id: "render", type: "artifact_transform", outcome: "failed" }]);
    expect(result.terminalStates).toEqual([{ stepId: "render", status: "paused" }]);
  });

  test("normalizes transform paths during simulation like runtime artifact storage", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: normalized-transform-paths
version: 1
style: pipeline
maturity: experimental
steps:
  - id: render
    type: artifact_transform
    input: inputs/../ticket.json
    output: generated/../ticket.md
    transform: jira_ticket_to_markdown
`);

    const result = simulateAgentflowWorkflow(workflow, {
      artifacts: { "ticket.json": { key: "AM-24", fields: { summary: "Normalized" } } }
    });

    expect(result.status).toBe("completed");
    expect(result.missingArtifacts).toEqual([]);
    expect(result.availableArtifacts).toEqual(["ticket.json", "ticket.md"]);
    expect(result.artifactValues["ticket.md"]).toBe("# AM-24: Normalized\n");
  });

  test("normalizes fixture and producer artifacts before transform simulation", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: normalized-transform-producer
version: 1
style: pipeline
maturity: experimental
steps:
  - id: fixture
    type: command
    command: echo ticket
    outputs: [inputs/../ticket.json]
  - id: render
    type: artifact_transform
    input: ticket.json
    output: ticket.md
    transform: jira_ticket_to_markdown
`);

    const result = simulateAgentflowWorkflow(workflow, {
      artifacts: { "seed/../seed.json": true },
      steps: { fixture: { outputs: { "inputs/../ticket.json": { key: "AM-24", fields: { summary: "Producer" } } } } }
    });

    expect(result.status).toBe("completed");
    expect(result.availableArtifacts).toEqual(["seed.json", "ticket.json", "ticket.md"]);
    expect(result.artifactValues["ticket.md"]).toBe("# AM-24: Producer\n");
  });

  test("rejects fixture artifact keys that collide after path normalization", () => {
    const result = simulateAgentflowWorkflow(transformWorkflow("jira_ticket_to_markdown"), {
      artifacts: {
        "ticket.json": { key: "AM-24", fields: { summary: "First" } },
        "inputs/../ticket.json": { key: "AM-24", fields: { summary: "Second" } }
      }
    });

    expect(result.status).toBe("unresolved");
    expect(result.artifactValues["ticket.json"]).toBeUndefined();
    expect(result.unresolvedBranches).toContainEqual({
      stepId: "(fixture)",
      reason: "Fixture artifact keys collide at canonical path ticket.json."
    });
  });

  test("rejects per-step fixture output keys that collide after path normalization", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: colliding-step-output-fixture
version: 1
style: pipeline
maturity: experimental
steps:
  - id: fixture
    type: command
    command: echo ticket
    outputs: [ticket.json]
  - id: render
    type: artifact_transform
    input: ticket.json
    output: ticket.md
    transform: jira_ticket_to_markdown
`);

    const result = simulateAgentflowWorkflow(workflow, {
      steps: {
        fixture: {
          outputs: {
            "inputs/../ticket.json": { key: "AM-24", fields: { summary: "First" } },
            "ticket.json": { key: "AM-24", fields: { summary: "Second" } }
          }
        }
      }
    });

    expect(result.status).toBe("unresolved");
    expect(result.artifactValues["ticket.json"]).toBeUndefined();
    expect(result.unresolvedBranches).toContainEqual({
      stepId: "fixture",
      reason: "Fixture output keys collide at canonical path ticket.json."
    });
  });

  test("rejects oversized transform fixture inputs during simulation", () => {
    const result = simulateAgentflowWorkflow(transformWorkflow("jira_ticket_to_markdown"), {
      artifacts: { "ticket.json": "x".repeat(MAX_AGENTFLOW_TRANSFORM_INPUT_BYTES + 1) }
    });

    expect(result.status).toBe("unresolved");
    expect(result.availableArtifacts).toEqual(["ticket.json"]);
    expect(result.unresolvedBranches[0]?.reason)
      .toContain(`exceeds the ${MAX_AGENTFLOW_TRANSFORM_INPUT_BYTES}-byte transform input limit`);
  });

  test("halts simulation on transform failure and enforces overwrite protection", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: transform-simulation-failure
version: 1
style: pipeline
maturity: experimental
steps:
  - id: render
    type: artifact_transform
    input: ticket.json
    output: ticket.md
    transform: jira_ticket_to_markdown
  - id: downstream
    type: command
    command: echo impossible
    outputs: [downstream.txt]
`);

    const invalid = simulateAgentflowWorkflow(workflow, {
      artifacts: { "ticket.json": "not json" },
      steps: { downstream: { outputs: { "downstream.txt": "impossible" } } }
    });
    expect(invalid.status).toBe("unresolved");
    expect(invalid.visitedSteps.map((step) => step.id)).toEqual(["render"]);
    expect(invalid.availableArtifacts).toEqual(["ticket.json"]);

    const collision = simulateAgentflowWorkflow(transformWorkflow("jira_ticket_to_markdown"), {
      artifacts: {
        "ticket.json": { key: "AM-24", fields: { summary: "New" } },
        "ticket.md": "existing"
      }
    });
    expect(collision.status).toBe("unresolved");
    expect(collision.unresolvedBranches[0]?.reason).toContain("declare overwrite: true");
    expect(collision.artifactValues["ticket.md"]).toBe("existing");
  });

  test("pauses simulation for a targetless transform failure policy", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: targetless-transform-failure
version: 1
style: pipeline
maturity: experimental
steps:
  - id: render
    type: artifact_transform
    input: ticket.json
    output: ticket.md
    transform: jira_ticket_to_markdown
    on_failure: {}
`);

    const result = simulateAgentflowWorkflow(workflow, { artifacts: { "ticket.json": "not-json" } });

    expect(result.status).toBe("paused");
    expect(result.terminalStates).toEqual([{ stepId: "render", status: "paused" }]);

    const fixtureFailure = simulateAgentflowWorkflow(workflow, {
      artifacts: { "ticket.json": { key: "AM-24", fields: { summary: "Valid" } } },
      steps: { render: { outcome: "failed" } }
    });
    expect(fixtureFailure.status).toBe("paused");
    expect(fixtureFailure.terminalStates).toEqual([{ stepId: "render", status: "paused" }]);
  });

  test("normalizes padded transform failure targets during execution", async () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: padded-transform-failure-target
version: 1
style: pipeline
maturity: experimental
steps:
  - id: render
    type: artifact_transform
    input: ticket.json
    output: ticket.md
    transform: jira_ticket_to_markdown
    on_failure: { then: " fail " }
`);
    const temporaryRoot = temporaryRepo();
    const store = await openAgentflowRunState({ cwd: temporaryRoot });
    createAgentflowLifecycleRun(store, { id: "padded-transform-target", workflow });
    seedInput(store, "padded-transform-target", "invalid");

    const result = await executeAgentflowCommandPipeline(store, "padded-transform-target", workflow);

    expect(result).toMatchObject({ status: "failed", failedStep: "render" });
    store.close();
  });

  test("applies retry and continue policies to transform failures during simulation", () => {
    const retryWorkflow = parseAgentflowWorkflowOrThrow(`name: retry-transform-simulation
version: 1
style: pipeline
maturity: experimental
steps:
  - id: render
    type: artifact_transform
    input: ticket.json
    output: ticket.md
    transform: flaky
    on_failure: { retry: 1, then: fail }
`);
    let calls = 0;
    const retryRegistry = new AgentflowArtifactTransformRegistry().register("flaky", () => {
      calls += 1;
      if (calls === 1) throw new Error("transient");
      return { content: "recovered", contentType: "text/plain" };
    });
    const retried = simulateAgentflowWorkflow(retryWorkflow, {
      artifacts: { "ticket.json": "input" }
    }, retryRegistry);
    expect(retried.status).toBe("completed");
    expect(retried.visitedSteps.map((step) => step.id)).toEqual(["render", "render"]);
    expect(retried.artifactValues["ticket.md"]).toBe("recovered");

    const exhaustedWorkflow = parseAgentflowWorkflowOrThrow(`name: exhausted-transform-simulation
version: 1
style: pipeline
maturity: experimental
steps:
  - id: render
    type: artifact_transform
    input: ticket.json
    output: ticket.md
    transform: always_fail
    on_failure: { retry: 1 }
`);
    const exhaustedRegistry = new AgentflowArtifactTransformRegistry().register("always_fail", () => {
      throw new Error("still failing");
    });
    const exhausted = simulateAgentflowWorkflow(exhaustedWorkflow, {
      artifacts: { "ticket.json": "input" }
    }, exhaustedRegistry);
    expect(exhausted.status).toBe("paused");
    expect(exhausted.terminalStates).toEqual([{ stepId: "render", status: "paused" }]);

    const continueWorkflow = parseAgentflowWorkflowOrThrow(`name: continue-transform-simulation
version: 1
style: pipeline
maturity: experimental
steps:
  - id: render
    type: artifact_transform
    input: ticket.json
    output: ticket.md
    transform: jira_ticket_to_markdown
    on_failure: { then: continue, allowed: true }
  - id: downstream
    type: command
    command: echo continued
    outputs: [downstream.txt]
`);
    const continued = simulateAgentflowWorkflow(continueWorkflow, {
      artifacts: { "ticket.json": "not json" },
      steps: { downstream: { outputs: { "downstream.txt": "continued" } } }
    });
    expect(continued.status).toBe("completed");
    expect(continued.visitedSteps).toEqual([
      { id: "render", type: "artifact_transform", outcome: "failed" },
      { id: "downstream", type: "command", outcome: "succeeded" }
    ]);
    expect(continued.availableArtifacts).toEqual(["downstream.txt", "ticket.json"]);

    const missingContinued = simulateAgentflowWorkflow(continueWorkflow, {
      steps: { downstream: { outputs: { "downstream.txt": "continued" } } }
    });
    expect(missingContinued.status).toBe("completed");
    expect(missingContinued.missingArtifacts).toEqual([
      { stepId: "render", artifact: "ticket.json", kind: "input" }
    ]);
    expect(missingContinued.visitedSteps.map((step) => step.id)).toEqual(["render", "downstream"]);
  });

  test("continues runtime execution for an ignored transform failure", async () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: ignored-transform-failure
version: 1
style: pipeline
maturity: experimental
steps:
  - id: render
    type: artifact_transform
    input: ticket.json
    output: ticket.md
    transform: jira_ticket_to_markdown
    on_failure: { then: ignore, allowed: true }
  - id: after
    type: command
    command: printf done > done.txt
    outputs: [done.txt]
`);
    const temporaryRoot = temporaryRepo();
    const store = await openAgentflowRunState({ cwd: temporaryRoot });
    createAgentflowLifecycleRun(store, { id: "ignored-transform", workflow });
    seedInput(store, "ignored-transform", "not-json");

    const result = await executeAgentflowCommandPipeline(store, "ignored-transform", workflow);

    expect(result).toMatchObject({ status: "completed", completedSteps: ["after"] });
    expect(store.listFailures("ignored-transform")).toMatchObject([{
      stepId: "render",
      retryable: false,
      payload: { attempt: 1, outcome: "continue" }
    }]);
    expect(store.readArtifact("ignored-transform", "done.txt").content.toString()).toBe("done");
    store.close();
  });

  test("does not fabricate transform content for array-form fixture outputs", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: availability-only-transform-input
version: 1
style: pipeline
maturity: experimental
steps:
  - id: fixture
    type: command
    command: printf ticket > ticket.json
    outputs: [ticket.json]
  - id: render
    type: artifact_transform
    input: ticket.json
    output: ticket.md
    transform: jira_ticket_to_markdown
`);

    const result = simulateAgentflowWorkflow(workflow, {
      steps: { fixture: { outputs: ["ticket.json"] } }
    });

    expect(result.status).toBe("unresolved");
    expect(result.availableArtifacts).toEqual(["ticket.json"]);
    expect(result.artifactValues["ticket.json"]).toBeUndefined();
    expect(result.unresolvedBranches[0]?.reason).toContain("must include a value");
  });

  test("clears stale content for availability-only overwritten fixture outputs", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: availability-only-overwrite
version: 1
style: pipeline
maturity: experimental
steps:
  - id: fixture
    type: command
    command: printf ticket > ticket.json
    outputs: [ticket.json]
    overwrite: true
  - id: render
    type: artifact_transform
    input: ticket.json
    output: ticket.md
    transform: jira_ticket_to_markdown
`);

    const result = simulateAgentflowWorkflow(workflow, {
      artifacts: { "ticket.json": { key: "STALE", fields: { summary: "Old" } } },
      steps: { fixture: { outputs: ["ticket.json"] } }
    });

    expect(result.status).toBe("unresolved");
    expect(result.artifactValues["ticket.json"]).toBeUndefined();
    expect(result.artifactValues["ticket.md"]).toBeUndefined();
    expect(result.unresolvedBranches[0]?.reason).toContain("must include a value");
  });

  test("resolves dynamic artifact inputs and preserves binary transform values losslessly", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: binary-transform
version: 1
style: pipeline
maturity: experimental
inputs:
  source: { required: true }
steps:
  - id: encode
    type: artifact_transform
    input: "{{ inputs.source }}"
    output: encoded.bin
    transform: binary
  - id: inspect
    type: artifact_transform
    input: encoded.bin
    output: encoded.hex
    transform: hex
`);
    const registry = new AgentflowArtifactTransformRegistry()
      .register("binary", () => ({ content: Uint8Array.from([0xff, 0x00, 0x80]), contentType: "application/octet-stream" }))
      .register("hex", (input) => ({ content: Buffer.from(input).toString("hex"), contentType: "text/plain" }));

    const result = simulateAgentflowWorkflow(workflow, {
      inputs: { source: "source.txt" },
      artifacts: { "source.txt": "ignored" }
    }, registry);

    expect(result.status).toBe("completed");
    expect(result.artifactValues["encoded.bin"]).toEqual({ __agentflow_binary__: "base64", data: "/wCA" });
    expect(result.artifactValues["encoded.hex"]).toBe("ff0080");
  });

  test("compares simulated artifact objects independently of key insertion order", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: equivalent-binary-artifact
version: 1
style: pipeline
maturity: experimental
steps:
  - id: encode
    type: artifact_transform
    input: source.txt
    output: encoded.bin
    transform: binary
`);
    const registry = new AgentflowArtifactTransformRegistry().register("binary", () => ({
      content: Uint8Array.from([0xff, 0x00, 0x80]),
      contentType: "application/octet-stream"
    }));

    const result = simulateAgentflowWorkflow(workflow, {
      artifacts: {
        "source.txt": "ignored",
        "encoded.bin": { data: "/wCA", __agentflow_binary__: "base64" }
      }
    }, registry);

    expect(result.status).toBe("completed");
    expect(result.unresolvedBranches).toEqual([]);
  });

  test("rejects malformed binary fixture encodings", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: malformed-binary-fixture
version: 1
style: pipeline
maturity: experimental
steps:
  - id: inspect
    type: artifact_transform
    input: encoded.bin
    output: encoded.hex
    transform: hex
`);
    const registry = new AgentflowArtifactTransformRegistry().register("hex", (input) => ({
      content: Buffer.from(input).toString("hex"),
      contentType: "text/plain"
    }));

    const result = simulateAgentflowWorkflow(workflow, {
      artifacts: { "encoded.bin": { __agentflow_binary__: "base64", data: "not base64!" } }
    }, registry);

    expect(result.status).toBe("unresolved");
    expect(result.unresolvedBranches[0]?.reason).toContain("invalid base64 binary data");
    expect(result.artifactValues["encoded.hex"]).toBeUndefined();
  });

  test("rejects invalid custom transform output contracts during simulation", () => {
    const registry = new AgentflowArtifactTransformRegistry().register("invalid", () => ({
      content: "output",
      contentType: ""
    }));

    const result = simulateAgentflowWorkflow(transformWorkflow("invalid"), {
      artifacts: { "ticket.json": "input" }
    }, registry);

    expect(result.status).toBe("unresolved");
    expect(result.unresolvedBranches[0]?.reason).toContain("invalid content type");
    expect(result.availableArtifacts).toEqual(["ticket.json"]);
  });
});

function transformWorkflow(transform: string) {
  return parseAgentflowWorkflowOrThrow(`name: transform
version: 1
style: pipeline
maturity: experimental
steps:
  - id: render
    type: artifact_transform
    input: ticket.json
    output: ticket.md
    transform: ${transform}
`);
}

function temporaryRepo(): string {
  const temporaryRoot = fs.mkdtempSync(path.join(os.tmpdir(), "agentflow-transform-"));
  fs.mkdirSync(path.join(temporaryRoot, ".git"));
  return temporaryRoot;
}

function seedInput(store: Awaited<ReturnType<typeof openAgentflowRunState>>, runId: string, content: string): void {
  store.writeArtifact({
    id: "fixture-ticket",
    runId,
    path: "ticket.json",
    kind: "fixture",
    contentType: "text/plain; charset=utf-8",
    content
  });
}
