import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  createAgentflowFixtureSessionProvider,
  createAgentflowLifecycleRun,
  createAgentflowSessionProviderRegistry,
  executeAgentflowCommandPipeline,
  executeAgentflowSessionRequest,
  MAX_AGENTFLOW_SESSION_METADATA_BYTES,
  MAX_AGENTFLOW_SESSION_PROMPT_BYTES,
  MAX_AGENTFLOW_SESSION_TOTAL_INPUT_BYTES,
  MAX_AGENTFLOW_SESSION_OUTPUT_BYTES,
  openAgentflowRunState,
  parseAgentflowWorkflowOrThrow,
  simulateAgentflowWorkflow,
  transitionAgentflowLifecycleRun,
  validateAgentflowWorkflow,
  type AgentflowSessionProviderRequest
} from "../../packages/agentflow-core/src";

describe("Agentflow session request steps", () => {
  test("validates static provider, resume, prompt, bounded inputs, and outputs", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: invalid-session-contract
version: 1
style: pipeline
maturity: experimental
sessions:
  dynamic: { provider: "{{ inputs.provider }}", resume: sometimes }
steps:
  - { id: ask, type: session_request, session: dynamic, prompt: "" }
`);

    expect(validateAgentflowWorkflow(workflow).errors.map((issue) => issue.code)).toEqual([
      "workflow.session.provider.dynamic",
      "workflow.session.resume.invalid",
      "workflow.policy.model_usage.provider.dynamic",
      "workflow.step.field.required",
      "workflow.step.field.required",
      "workflow.step.field.required",
      "workflow.input.undeclared"
    ]);
  });

  test("rejects prompt paths and failure routes that the runtime cannot execute", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: invalid-session-runtime-contract
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: fixture, resume: true }
steps:
  - id: ask
    type: session_request
    session: writer
    prompt: ../outside.md
    inputs: [../request.md]
    outputs: [/tmp/response.md]
    on_failure: { then: recover }
  - { id: recover, type: command, command: echo recover }
`);

    expect(validateAgentflowWorkflow(workflow).errors.map((issue) => issue.code)).toEqual([
      "workflow.session_request.target.unsupported",
      "workflow.session_request.prompt.invalid",
      "workflow.session_request.artifact.invalid",
      "workflow.session_request.artifact.invalid"
    ]);
  });

  test("rejects duplicate declared session artifacts during validation", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: duplicate-session-artifacts
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: fixture }
steps:
  - { id: ask, type: session_request, session: writer, prompt: prompt.md, inputs: [request.md, request.md], outputs: [response.md, response.md] }
`);

    expect(validateAgentflowWorkflow(workflow).errors.map((issue) => issue.code).filter((code) =>
      code === "workflow.session_request.artifact.duplicate"
    )).toEqual([
      "workflow.session_request.artifact.duplicate",
      "workflow.session_request.artifact.duplicate"
    ]);
  });

  test("rejects noncanonical session artifact aliases during validation", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: aliased-session-artifacts
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: fixture }
steps:
  - { id: ask, type: session_request, session: writer, prompt: prompt.md, inputs: [dir/../request.md], outputs: [response.md] }
`);

    expect(validateAgentflowWorkflow(workflow).errors).toContainEqual(expect.objectContaining({
      code: "workflow.session_request.artifact.invalid",
      path: "steps[0].inputs[0]"
    }));
  });

  test("rejects dynamic paths that the session runtime cannot resolve", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: unsupported-session-paths
version: 1
style: pipeline
maturity: experimental
inputs:
  target: { required: true }
sessions:
  writer: { provider: fixture }
steps:
  - { id: ask, type: session_request, session: writer, prompt: "{{ inputs.target }}", inputs: ["prefix/{{ inputs.target }}"], outputs: ["{{ inputs.target }}"] }
`);

    expect(validateAgentflowWorkflow(workflow).errors).toEqual(expect.arrayContaining([
      expect.objectContaining({ code: "workflow.session_request.prompt.invalid", path: "steps[0].prompt" }),
      expect.objectContaining({ code: "workflow.session_request.artifact.invalid", path: "steps[0].inputs[0]" }),
      expect.objectContaining({ code: "workflow.session_request.artifact.invalid", path: "steps[0].outputs[0]" })
    ]));
  });

  test("simulates declared outputs deterministically from a fixture", () => {
    const workflow = sessionWorkflow();
    const result = simulateAgentflowWorkflow(workflow, {
      artifacts: { "request.md": "Bounded request" },
      steps: { draft: { outputs: { "response.md": "Fixture response" } } }
    });

    expect(result).toMatchObject({ status: "completed", availableArtifacts: ["request.md", "response.md"] });
    expect(result.artifactValues["response.md"]).toBe("Fixture response");

    const repeatedWorkflow = parseAgentflowWorkflowOrThrow(`name: repeated-session-simulation
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: fixture, resume: true }
steps:
  - id: repeat
    type: loop
    max_iterations: 2
    body:
      - { id: draft, type: session_request, session: writer, prompt: prompt.md, inputs: [request.md], outputs: [response.md] }
`);
    const repeated = simulateAgentflowWorkflow(repeatedWorkflow, {
      artifacts: { "request.md": "Request" },
      steps: {
        repeat: { iterations: 2 },
        draft: { outputs: { "response.md": "Repeated response" } }
      }
    });
    expect(repeated).toMatchObject({ status: "completed", availableArtifacts: ["request.md", "response.md"] });
    expect(repeated.visitedSteps.filter((step) => step.id === "draft")).toHaveLength(2);
  });

  test("simulates output collisions with runtime overwrite semantics", () => {
    const result = simulateAgentflowWorkflow(sessionWorkflow(), {
      artifacts: { "request.md": "Request", "response.md": "Existing" },
      steps: { draft: { outputs: { "response.md": "Replacement" } } }
    });

    expect(result.status).toBe("paused");
    expect(result.artifactValues["response.md"]).toBe("Existing");
  });

  test("routes missing inputs and invalid fixture responses as session failures without publishing outputs", () => {
    for (const fixture of [
      { steps: { draft: { outputs: { "response.md": "Response" } } } },
      { artifacts: { "request.md": "Request" }, steps: { draft: { outputs: { "other.md": "Response" } } } }
    ]) {
      const result = simulateAgentflowWorkflow(sessionWorkflow(), fixture);

      expect(result.status).toBe("paused");
      expect(result.visitedSteps).toContainEqual(expect.objectContaining({ id: "draft", outcome: "failed" }));
      expect(result.availableArtifacts).not.toContain("response.md");
      expect(result.availableArtifacts).not.toContain("other.md");
    }

    const multipleMissingWorkflow = parseAgentflowWorkflowOrThrow(`name: missing-session-inputs
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: fixture }
steps:
  - { id: draft, type: session_request, session: writer, prompt: prompt.md, inputs: [first.md, second.md], outputs: [response.md], on_failure: { then: pause } }
`);
    const multipleMissing = simulateAgentflowWorkflow(multipleMissingWorkflow, {
      steps: { draft: { outputs: { "response.md": "Response" } } }
    });
    expect(multipleMissing.status).toBe("paused");
    expect(multipleMissing.missingArtifacts).toEqual([
      { stepId: "draft", artifact: "first.md", kind: "input" },
      { stepId: "draft", artifact: "second.md", kind: "input" }
    ]);

    const unresolvedReferenceWorkflow = parseAgentflowWorkflowOrThrow(`name: unresolved-session-input
version: 1
style: pipeline
maturity: experimental
inputs:
  failure_payload: { required: true }
sessions:
  writer: { provider: fixture }
steps:
  - { id: draft, type: session_request, session: writer, prompt: prompt.md, inputs: ["{{ inputs.failure_payload }}"], outputs: [response.md], on_failure: { then: pause } }
`);
    for (const failurePayload of ["", { invalid: true }]) {
      const unresolvedReference = simulateAgentflowWorkflow(unresolvedReferenceWorkflow, {
        inputs: { failure_payload: failurePayload },
        steps: { draft: { outputs: { "response.md": "Response" } } }
      });
      expect(unresolvedReference.status).toBe("paused");
      expect(unresolvedReference.visitedSteps).toContainEqual(expect.objectContaining({ id: "draft", outcome: "failed" }));
      expect(unresolvedReference.availableArtifacts).not.toContain("response.md");
    }
  });

  test("simulates an unhandled session failure as paused", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: failed-session-simulation
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: fixture }
steps:
  - { id: draft, type: session_request, session: writer, prompt: prompt.md, inputs: [request.md], outputs: [response.md] }
`);
    const result = simulateAgentflowWorkflow(workflow, {
      artifacts: { "request.md": "Request" },
      steps: { draft: { outcome: "failed" } }
    });

    expect(result).toMatchObject({ status: "paused" });
    expect(result.visitedSteps).toContainEqual(expect.objectContaining({ id: "draft", outcome: "failed" }));
  });

  test("runs through a provider adapter and persists inspectable request, output, and resumable session state", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft a bounded response.\n");
    const workflow = sessionWorkflow();
    const store = await openAgentflowRunState({ cwd: root });
    createAgentflowLifecycleRun(store, { id: "session-run", workflow });
    store.writeArtifact({
      id: "request",
      runId: "session-run",
      stepId: "fixture",
      path: "request.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Bounded request\n"
    });
    const requests: AgentflowSessionProviderRequest[] = [];
    const providers = createAgentflowSessionProviderRegistry().register("fixture", (request) => {
      requests.push(request);
      return {
        externalSessionId: "fixture-session-1",
        outputs: { "response.md": { content: "Deterministic response\n", contentType: "text/markdown" } },
        metadata: { fixture: true }
      };
    });

    const result = await executeAgentflowCommandPipeline(store, "session-run", workflow, undefined, providers);

    expect(result).toMatchObject({ status: "completed", completedSteps: ["draft"] });
    expect(requests).toHaveLength(1);
    expect(requests[0]).toMatchObject({
      stepId: "draft",
      sessionId: "writer",
      provider: "fixture",
      resume: true,
      outputs: ["response.md"]
    });
    expect(requests[0]?.prompt.content).toBe("Draft a bounded response.\n");
    expect(Buffer.from(requests[0]!.inputs[0]!.content).toString("utf8")).toBe("Bounded request\n");
    expect(store.readArtifact("session-run", "response.md").content.toString("utf8")).toBe("Deterministic response\n");
    const requestPath = store.listArtifacts("session-run")
      .find((artifact) => artifact.kind === "session_request")!.declaredPath;
    const metadata = JSON.parse(store.readArtifact("session-run", requestPath).content.toString("utf8"));
    expect(metadata).toMatchObject({
      stepId: "draft",
      sessionId: "writer",
      provider: "fixture",
      resume: true,
      outputs: ["response.md"],
      providerMetadata: { fixture: true }
    });
    expect(store.getSession("session-run", "writer")).toMatchObject({
      status: "waiting",
      externalSessionId: "fixture-session-1",
      state: {
        resume: true,
        lastStepId: "draft",
        requestArtifact: requestPath,
        outputArtifacts: ["response.md"]
      }
    });
    expect(store.listSessions("session-run").map((session) => session.id)).toEqual(["writer"]);
    expect(store.listEvents("session-run").map((event) => event.type)).toEqual([
      "run.created",
      "run.started",
      "step.started",
      "step.completed",
      "run.completed"
    ]);
    store.close();
  });

  test("executes session steps with validator-normalized IDs and types", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = parseAgentflowWorkflowOrThrow(`name: normalized-session-step
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: fixture }
steps:
  - { id: " draft ", type: " session_request ", session: writer, prompt: prompts/draft.md, inputs: [request.md], outputs: [response.md] }
`);
    expect(validateAgentflowWorkflow(workflow)).toEqual({ valid: true, errors: [] });
    const store = await openAgentflowRunState({ cwd: root });
    createAgentflowLifecycleRun(store, { id: "normalized-session-step", workflow });
    store.writeArtifact({ id: "request", runId: "normalized-session-step", path: "request.md", kind: "fixture", contentType: "text/plain", content: "Request" });
    const providers = createAgentflowSessionProviderRegistry().register("fixture", () => ({
      outputs: { "response.md": "Response" }
    }));

    const result = await executeAgentflowCommandPipeline(store, "normalized-session-step", workflow, undefined, providers);

    expect(result).toMatchObject({ status: "completed", completedSteps: ["draft"] });
    expect(store.readArtifact("normalized-session-step", "response.md").content.toString()).toBe("Response");
    store.close();
  });

  test("preserves provider state when a padded session ID fails", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = parseAgentflowWorkflowOrThrow(`name: normalized-failing-session
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: fixture }
steps:
  - { id: draft, type: session_request, session: " writer ", prompt: prompts/draft.md, inputs: [request.md], outputs: [response.md], on_failure: { then: pause } }
`);
    expect(validateAgentflowWorkflow(workflow)).toEqual({ valid: true, errors: [] });
    const store = await openAgentflowRunState({ cwd: root });
    createAgentflowLifecycleRun(store, { id: "normalized-failing-session", workflow });
    store.writeArtifact({ id: "request", runId: "normalized-failing-session", path: "request.md", kind: "fixture", contentType: "text/plain", content: "Request" });
    const providers = createAgentflowSessionProviderRegistry().register("fixture", () => {
      throw new Error("provider failed");
    });

    const result = await executeAgentflowCommandPipeline(store, "normalized-failing-session", workflow, undefined, providers);

    expect(result).toMatchObject({ status: "paused", failedStep: "draft" });
    expect(store.getSession("normalized-failing-session", "writer")).toMatchObject({
      provider: "fixture",
      status: "paused"
    });
    expect(store.listSessions("normalized-failing-session")).toHaveLength(1);
    store.close();
  });

  test("bounds request metadata filenames for long valid step IDs", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const longStepId = `draft-${"x".repeat(300)}`;
    const workflow = parseAgentflowWorkflowOrThrow(`name: long-session-step-id
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: fixture }
steps:
  - { id: "${longStepId}", type: session_request, session: writer, prompt: prompts/draft.md, inputs: [request.md], outputs: [response.md] }
`);
    const store = await openAgentflowRunState({ cwd: root });
    createAgentflowLifecycleRun(store, { id: "long-session-step-id", workflow });
    store.writeArtifact({ id: "request", runId: "long-session-step-id", path: "request.md", kind: "fixture", contentType: "text/plain", content: "Request" });
    const providers = createAgentflowSessionProviderRegistry().register("fixture", () => ({ outputs: { "response.md": "Response" } }));

    await expect(executeAgentflowCommandPipeline(store, "long-session-step-id", workflow, undefined, providers))
      .resolves.toMatchObject({ status: "completed" });
    const requestArtifact = store.listArtifacts("long-session-step-id").find((artifact) => artifact.kind === "session_request");
    expect(path.basename(requestArtifact!.declaredPath).length).toBeLessThanOrEqual(255);
    store.close();
  });

  test("supports an explicit fixture provider and fails closed on missing fixture output", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = sessionWorkflow();
    const store = await openAgentflowRunState({ cwd: root });
    createAgentflowLifecycleRun(store, { id: "missing-fixture", workflow });
    store.writeArtifact({
      id: "request",
      runId: "missing-fixture",
      path: "request.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Request"
    });
    const providers = createAgentflowSessionProviderRegistry().register(
      "fixture",
      createAgentflowFixtureSessionProvider({})
    );

    const result = await executeAgentflowCommandPipeline(store, "missing-fixture", workflow, undefined, providers);

    expect(result).toMatchObject({ status: "paused", failedStep: "draft" });
    expect(result.message).toContain("no response for step draft");
    expect(store.listArtifacts("missing-fixture").map((artifact) => artifact.declaredPath)).toEqual(["request.md"]);
    store.close();
  });

  test("rejects oversized provider outputs before publication", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = sessionWorkflow();
    const store = await openAgentflowRunState({ cwd: root });
    createAgentflowLifecycleRun(store, { id: "oversized-output", workflow });
    store.writeArtifact({ id: "request", runId: "oversized-output", path: "request.md", kind: "fixture", contentType: "text/plain", content: "Request" });
    const providers = createAgentflowSessionProviderRegistry().register("fixture", () => ({
      outputs: { "response.md": "x".repeat(MAX_AGENTFLOW_SESSION_OUTPUT_BYTES + 1) }
    }));

    const result = await executeAgentflowCommandPipeline(store, "oversized-output", workflow, undefined, providers);

    expect(result).toMatchObject({ status: "paused", failedStep: "draft" });
    expect(result.message).toContain("exceeds the");
    expect(store.getArtifact("oversized-output", "response.md")).toBeNull();
    store.close();
  });

  test("rejects oversized prompts before invoking a provider", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), Buffer.alloc(MAX_AGENTFLOW_SESSION_PROMPT_BYTES + 1, "x"));
    const workflow = sessionWorkflow();
    const store = await openAgentflowRunState({ cwd: root });
    createAgentflowLifecycleRun(store, { id: "oversized-prompt", workflow });
    store.writeArtifact({ id: "request", runId: "oversized-prompt", path: "request.md", kind: "fixture", contentType: "text/plain", content: "Request" });
    let calls = 0;
    const providers = createAgentflowSessionProviderRegistry().register("fixture", () => {
      calls += 1;
      return { outputs: { "response.md": "Response" } };
    });

    const result = await executeAgentflowCommandPipeline(store, "oversized-prompt", workflow, undefined, providers);

    expect(result).toMatchObject({ status: "paused", failedStep: "draft" });
    expect(result.message).toContain("session prompt limit");
    expect(calls).toBe(0);
    expect(store.getSession("oversized-prompt", "writer")).toMatchObject({ status: "paused" });
    store.close();
  });

  test("rejects oversized provider metadata before persisting request artifacts", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = sessionWorkflow();
    const store = await openAgentflowRunState({ cwd: root });
    createAgentflowLifecycleRun(store, { id: "oversized-metadata", workflow });
    store.writeArtifact({ id: "request", runId: "oversized-metadata", path: "request.md", kind: "fixture", contentType: "text/plain", content: "Request" });
    const providers = createAgentflowSessionProviderRegistry().register("fixture", () => ({
      outputs: { "response.md": "Response" },
      metadata: { value: "x".repeat(MAX_AGENTFLOW_SESSION_METADATA_BYTES + 1) }
    }));

    const result = await executeAgentflowCommandPipeline(store, "oversized-metadata", workflow, undefined, providers);

    expect(result).toMatchObject({ status: "paused", failedStep: "draft" });
    expect(result.message).toContain("metadata exceeds");
    expect(store.listArtifacts("oversized-metadata").some((artifact) => artifact.kind === "session_request")).toBe(false);
    store.close();
  });

  test("rejects provider metadata whose top level is not a plain object", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = sessionWorkflow();
    const store = await openAgentflowRunState({ cwd: root });
    createAgentflowLifecycleRun(store, { id: "invalid-metadata-shape", workflow });
    store.writeArtifact({ id: "request", runId: "invalid-metadata-shape", path: "request.md", kind: "fixture", contentType: "text/plain", content: "Request" });
    const providers = createAgentflowSessionProviderRegistry().register("fixture", () => ({
      outputs: { "response.md": "Response" },
      metadata: [] as never
    }));

    const result = await executeAgentflowCommandPipeline(store, "invalid-metadata-shape", workflow, undefined, providers);

    expect(result).toMatchObject({ status: "paused", failedStep: "draft" });
    expect(result.message).toContain("metadata must be a plain object");
    expect(store.listArtifacts("invalid-metadata-shape").some((artifact) => artifact.kind === "session_request")).toBe(false);
    store.close();
  });

  test("rejects session inputs whose aggregate bytes exceed the request bound", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = parseAgentflowWorkflowOrThrow(`name: aggregate-input-bound
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: fixture }
steps:
  - { id: draft, type: session_request, session: writer, prompt: prompts/draft.md, inputs: [first.bin, second.bin], outputs: [response.md] }
`);
    const store = await openAgentflowRunState({ cwd: root });
    createAgentflowLifecycleRun(store, { id: "aggregate-input-bound", workflow });
    const half = Buffer.alloc(Math.floor(MAX_AGENTFLOW_SESSION_TOTAL_INPUT_BYTES / 2) + 1);
    store.writeArtifact({ id: "first", runId: "aggregate-input-bound", path: "first.bin", kind: "fixture", contentType: "application/octet-stream", content: half });
    store.writeArtifact({ id: "second", runId: "aggregate-input-bound", path: "second.bin", kind: "fixture", contentType: "application/octet-stream", content: half });
    let calls = 0;
    const providers = createAgentflowSessionProviderRegistry().register("fixture", () => {
      calls += 1;
      return { outputs: { "response.md": "Response" } };
    });

    const result = await executeAgentflowCommandPipeline(store, "aggregate-input-bound", workflow, undefined, providers);

    expect(result).toMatchObject({ status: "paused", failedStep: "draft" });
    expect(result.message).toContain("aggregate limit");
    expect(calls).toBe(0);
    store.close();
  });

  test("fails malformed direct-API session steps before persisting running state", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`name: malformed-direct-session
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: fixture }
steps:
  - { id: draft, type: session_request, session: writer, prompt: prompt.md }
`);
    const store = await openAgentflowRunState({ cwd: root });
    store.createRun({
      id: "malformed-direct",
      workflow: { name: workflow.name, version: workflow.version, style: workflow.style, maturity: workflow.maturity },
      context: { workflow }
    });

    const result = await executeAgentflowCommandPipeline(store, "malformed-direct", workflow);

    expect(result).toMatchObject({ status: "failed", failedStep: "draft" });
    expect(result.message).toContain("requires a non-empty session, prompt, inputs list, and outputs list");
    expect(store.getRun("malformed-direct")).toMatchObject({ status: "failed" });
    store.close();
  });

  test("rejects direct session execution for inactive runs before invoking the provider", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = sessionWorkflow();
    const store = await openAgentflowRunState({ cwd: root });
    createAgentflowLifecycleRun(store, { id: "inactive-session", workflow });
    let calls = 0;
    const providers = createAgentflowSessionProviderRegistry().register("fixture", () => {
      calls += 1;
      return { outputs: { "response.md": "Response" } };
    });

    await expect(executeAgentflowSessionRequest(
      store,
      "inactive-session",
      workflow,
      workflow.steps[0]!,
      providers
    )).rejects.toMatchObject({ code: "AGENTFLOW_SESSION_RUN_STATUS" });
    expect(calls).toBe(0);
    expect(store.getSession("inactive-session", "writer")).toBeNull();
    store.close();
  });

  test("rejects Windows-absolute prompt paths at the direct runtime boundary", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`name: windows-absolute-prompt
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: fixture }
steps:
  - { id: draft, type: session_request, session: writer, prompt: C:/tmp/prompt.md, inputs: [request.md], outputs: [response.md] }
`);
    const store = await openAgentflowRunState({ cwd: root });
    store.createRun({
      id: "windows-absolute-prompt",
      status: "running",
      workflow: { name: workflow.name, version: workflow.version, style: workflow.style, maturity: workflow.maturity },
      context: { workflow }
    });
    let calls = 0;
    const providers = createAgentflowSessionProviderRegistry().register("fixture", () => {
      calls += 1;
      return { outputs: { "response.md": "Response" } };
    });

    await expect(executeAgentflowSessionRequest(
      store,
      "windows-absolute-prompt",
      workflow,
      workflow.steps[0]!,
      providers
    )).rejects.toMatchObject({ code: "AGENTFLOW_SESSION_PROMPT_PATH" });
    expect(calls).toBe(0);
    expect(store.getSession("windows-absolute-prompt", "writer")).toBeNull();
    store.close();
  });

  test("rejects canonical output collisions before reserving budget or invoking a direct provider", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = parseAgentflowWorkflowOrThrow(`name: direct-output-alias
version: 1
style: pipeline
maturity: experimental
limits: { max_model_calls: 1 }
sessions:
  writer: { provider: fixture }
steps:
  - { id: draft, type: session_request, session: writer, prompt: prompts/draft.md, inputs: [request.md], outputs: [answer.md, dir/../answer.md] }
`);
    const store = await openAgentflowRunState({ cwd: root });
    store.createRun({
      id: "direct-output-alias",
      status: "running",
      workflow: { name: workflow.name, version: workflow.version, style: workflow.style, maturity: workflow.maturity },
      context: { workflow }
    });
    store.writeArtifact({ id: "request", runId: "direct-output-alias", path: "request.md", kind: "fixture", contentType: "text/plain", content: "Request" });
    let calls = 0;
    const providers = createAgentflowSessionProviderRegistry().register("fixture", () => {
      calls += 1;
      return { outputs: { "answer.md": "Answer" } };
    });

    await expect(executeAgentflowSessionRequest(store, "direct-output-alias", workflow, workflow.steps[0]!, providers))
      .rejects.toMatchObject({ code: "AGENTFLOW_SESSION_REQUEST_INVALID" });
    expect(calls).toBe(0);
    expect(store.getBudget("direct-output-alias", "model:model_calls")).toBeNull();
    store.close();
  });

  test("binds direct session requests to the persisted workflow definition", async () => {
    const root = temporaryRepo();
    const workflow = sessionWorkflow();
    const store = await openAgentflowRunState({ cwd: root });
    store.createRun({
      id: "persisted-workflow",
      status: "running",
      workflow: { name: workflow.name, version: workflow.version, style: workflow.style, maturity: workflow.maturity },
      context: { workflow }
    });
    let calls = 0;
    const providers = createAgentflowSessionProviderRegistry().register("fixture", () => {
      calls += 1;
      return { outputs: { "response.md": "Response" } };
    });
    const changed = structuredClone(workflow);
    changed.limits = { max_model_calls: 999 };

    await expect(executeAgentflowSessionRequest(store, "persisted-workflow", changed, changed.steps[0]!, providers))
      .rejects.toMatchObject({ code: "AGENTFLOW_SESSION_WORKFLOW_MISMATCH" });
    expect(calls).toBe(0);
    store.close();
  });

  test("does not let providers mutate persisted input and output declarations", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = sessionWorkflow();
    const store = await openAgentflowRunState({ cwd: root });
    createAgentflowLifecycleRun(store, { id: "mutating-provider", workflow });
    store.writeArtifact({ id: "request", runId: "mutating-provider", path: "request.md", kind: "fixture", contentType: "text/plain", content: "Request" });
    const providers = createAgentflowSessionProviderRegistry().register("fixture", (request) => {
      request.outputs.push("extra.md");
      request.inputs.splice(0, request.inputs.length);
      return { outputs: { "response.md": "Response", "extra.md": "Extra" } };
    });

    const result = await executeAgentflowCommandPipeline(store, "mutating-provider", workflow, undefined, providers);

    expect(result).toMatchObject({ status: "paused", failedStep: "draft" });
    expect(store.getArtifact("mutating-provider", "extra.md")).toBeNull();
    expect(store.getArtifact("mutating-provider", "response.md")).toBeNull();
    store.close();
  });

  test("persists paused session state when a direct provider response is invalid", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = sessionWorkflow();
    const store = await openAgentflowRunState({ cwd: root });
    createAgentflowLifecycleRun(store, { id: "invalid-direct-response", workflow });
    store.writeArtifact({ id: "request", runId: "invalid-direct-response", path: "request.md", kind: "fixture", contentType: "text/plain", content: "Request" });
    store.transitionRunWithEvent("invalid-direct-response", {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: {} }
    });
    const providers = createAgentflowSessionProviderRegistry().register("fixture", () => ({ outputs: {} }));

    await expect(executeAgentflowSessionRequest(store, "invalid-direct-response", workflow, workflow.steps[0]!, providers))
      .rejects.toMatchObject({ code: "AGENTFLOW_SESSION_OUTPUT_INVALID" });
    expect(store.getSession("invalid-direct-response", "writer")).toMatchObject({ status: "paused" });
    store.close();
  });

  test("scopes fixture outcome attempts to each run", () => {
    const adapter = createAgentflowFixtureSessionProvider(
      { draft: { outputs: { "response.md": "Response" } } },
      { draft: ["failed", "succeeded"] }
    );
    const request = {
      stepId: "draft",
      sessionId: "writer",
      provider: "fixture",
      resume: false,
      prompt: { path: "prompt.md", content: "Prompt", checksum: "sha256:prompt" },
      inputs: [],
      outputs: ["response.md"],
      signal: new AbortController().signal
    };

    expect(() => adapter({ ...request, runId: "first-run" })).toThrow(/attempt 1/);
    expect(() => adapter({ ...request, runId: "second-run" })).toThrow(/attempt 1/);
  });

  test("retries partial multi-output publication and resumes with the provider session ID", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = parseAgentflowWorkflowOrThrow(`name: retry-session
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: fixture, resume: true }
steps:
  - id: draft
    type: session_request
    session: writer
    prompt: prompts/draft.md
    inputs: [request.md]
    outputs: [first.md, second.md]
    on_failure: { retry: 1, then: pause }
`);
    const store = await openAgentflowRunState({ cwd: root });
    createAgentflowLifecycleRun(store, { id: "retry-session", workflow });
    store.writeArtifact({ id: "request", runId: "retry-session", path: "request.md", kind: "fixture", contentType: "text/plain", content: "Request" });
    const externalIds: Array<string | undefined> = [];
    const providers = createAgentflowSessionProviderRegistry().register("fixture", (request) => {
      externalIds.push(request.externalSessionId);
      if (request.externalSessionId === undefined) {
        return { externalSessionId: "provider-session", outputs: { "first.md": "First", "second.md": "Second" } };
      }
      request.externalSessionId = "mutated-session";
      return { outputs: { "first.md": "First", "second.md": "Second" } };
    });
    const writeArtifact = store.writeArtifact.bind(store);
    let failedSecondOutput = false;
    store.writeArtifact = ((input) => {
      if (input.path === "second.md" && !failedSecondOutput) {
        failedSecondOutput = true;
        throw new Error("simulated second output failure");
      }
      return writeArtifact(input);
    }) as typeof store.writeArtifact;

    const result = await executeAgentflowCommandPipeline(store, "retry-session", workflow, undefined, providers);

    expect(result).toMatchObject({ status: "completed", completedSteps: ["draft"] });
    expect(externalIds).toEqual([undefined, "provider-session"]);
    expect(store.readArtifact("retry-session", "first.md").content.toString()).toBe("First");
    expect(store.readArtifact("retry-session", "second.md").content.toString()).toBe("Second");
    const requestArtifact = store.listArtifacts("retry-session").find((artifact) => artifact.kind === "session_request")!;
    expect(JSON.parse(store.readArtifact("retry-session", requestArtifact.declaredPath).content.toString()))
      .toMatchObject({ externalSessionId: "provider-session" });
    store.close();
  });

  test("rolls back a partially published response before continuing", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = parseAgentflowWorkflowOrThrow(`name: atomic-session
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: fixture }
steps:
  - id: draft
    type: session_request
    session: writer
    prompt: prompts/draft.md
    inputs: [request.md]
    outputs: [first.md, second.md]
    on_failure: { then: continue, allowed: true }
`);
    const store = await openAgentflowRunState({ cwd: root });
    createAgentflowLifecycleRun(store, { id: "atomic-session", workflow });
    store.writeArtifact({ id: "request", runId: "atomic-session", path: "request.md", kind: "fixture", contentType: "text/plain", content: "Request" });
    const providers = createAgentflowSessionProviderRegistry().register("fixture", () => ({
      outputs: { "first.md": "First", "second.md": "Second" }
    }));
    const writeArtifact = store.writeArtifact.bind(store);
    store.writeArtifact = ((input) => {
      if (input.path === "second.md") throw new Error("simulated publication failure");
      return writeArtifact(input);
    }) as typeof store.writeArtifact;

    const result = await executeAgentflowCommandPipeline(store, "atomic-session", workflow, undefined, providers);

    expect(result).toMatchObject({ status: "completed", completedSteps: [] });
    expect(store.listArtifacts("atomic-session").map((artifact) => artifact.declaredPath)).not.toContain("first.md");
    expect(store.listArtifacts("atomic-session").map((artifact) => artifact.declaredPath)).not.toContain("second.md");
    store.close();
  });

  test("returns cancellation immediately before atomic publication as an interruption", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = sessionWorkflow();
    const store = await openAgentflowRunState({ cwd: root });
    createAgentflowLifecycleRun(store, { id: "cancel-session", workflow });
    store.writeArtifact({ id: "request", runId: "cancel-session", path: "request.md", kind: "fixture", contentType: "text/plain", content: "Request" });
    const providers = createAgentflowSessionProviderRegistry().register("fixture", () => ({ outputs: { "response.md": "Response" } }));
    const writeArtifactsAtomically = store.writeArtifactsAtomically.bind(store);
    store.writeArtifactsAtomically = ((inputs) => {
      transitionAgentflowLifecycleRun(store, "cancel-session", "cancel");
      return writeArtifactsAtomically(inputs);
    }) as typeof store.writeArtifactsAtomically;

    const result = await executeAgentflowCommandPipeline(store, "cancel-session", workflow, undefined, providers);

    expect(result).toMatchObject({ status: "cancelled", completedSteps: [] });
    expect(store.getSession("cancel-session", "writer")).toMatchObject({ status: "cancelled" });
    store.close();
  });

  test("returns cancellation immediately after atomic output publication as an interruption", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = sessionWorkflow();
    const store = await openAgentflowRunState({ cwd: root });
    createAgentflowLifecycleRun(store, { id: "cancel-after-batch", workflow });
    store.writeArtifact({ id: "request", runId: "cancel-after-batch", path: "request.md", kind: "fixture", contentType: "text/plain", content: "Request" });
    const providers = createAgentflowSessionProviderRegistry().register("fixture", () => ({ outputs: { "response.md": "Response" } }));
    const writeArtifactsAtomically = store.writeArtifactsAtomically.bind(store);
    store.writeArtifactsAtomically = ((inputs) => {
      const result = writeArtifactsAtomically(inputs);
      transitionAgentflowLifecycleRun(store, "cancel-after-batch", "cancel");
      return result;
    }) as typeof store.writeArtifactsAtomically;

    const result = await executeAgentflowCommandPipeline(store, "cancel-after-batch", workflow, undefined, providers);

    expect(result).toMatchObject({ status: "cancelled", completedSteps: [] });
    expect(store.getSession("cancel-after-batch", "writer")).toMatchObject({ status: "cancelled" });
    expect(store.readArtifact("cancel-after-batch", "response.md").content.toString()).toBe("Response");
    store.close();
  });

  test("persists cancellation when a pending provider resolves", async () => {
    const { store, workflow } = await pendingProviderRun("resolve-after-cancel");
    let resolveProvider!: (response: { outputs: { "response.md": string } }) => void;
    const providers = createAgentflowSessionProviderRegistry().register("fixture", () =>
      new Promise((resolve) => { resolveProvider = resolve; })
    );
    const execution = executeAgentflowCommandPipeline(store, "resolve-after-cancel", workflow, undefined, providers);
    await waitForProviderStart();
    transitionAgentflowLifecycleRun(store, "resolve-after-cancel", "cancel");
    resolveProvider({ outputs: { "response.md": "Response" } });

    await expect(execution).resolves.toMatchObject({ status: "cancelled", completedSteps: [] });
    expect(store.getSession("resolve-after-cancel", "writer")).toMatchObject({
      status: "cancelled",
      state: { interrupted: "cancelled" }
    });
    store.close();
  });

  test("honors cancellation when a pending provider rejects", async () => {
    const { store, workflow } = await pendingProviderRun("reject-after-cancel");
    let rejectProvider!: (error: Error) => void;
    const providers = createAgentflowSessionProviderRegistry().register("fixture", () =>
      new Promise((_resolve, reject) => { rejectProvider = reject; })
    );
    const execution = executeAgentflowCommandPipeline(store, "reject-after-cancel", workflow, undefined, providers);
    await waitForProviderStart();
    transitionAgentflowLifecycleRun(store, "reject-after-cancel", "cancel");
    rejectProvider(new Error("provider stopped"));

    await expect(execution).resolves.toMatchObject({ status: "cancelled", completedSteps: [] });
    expect(store.getSession("reject-after-cancel", "writer")).toMatchObject({
      status: "cancelled",
      state: { interrupted: "cancelled" }
    });
    expect(store.listEvents("reject-after-cancel").map((event) => event.type)).not.toContain("step.failed");
    store.close();
  });

  test("aborts a pending provider when the run is cancelled", async () => {
    const { store, workflow } = await pendingProviderRun("abort-on-cancel");
    let signal!: AbortSignal;
    const providers = createAgentflowSessionProviderRegistry().register("fixture", (request) => {
      signal = request.signal;
      return new Promise(() => {});
    });
    const execution = executeAgentflowCommandPipeline(store, "abort-on-cancel", workflow, undefined, providers);
    await waitForProviderStart();
    transitionAgentflowLifecycleRun(store, "abort-on-cancel", "cancel");

    await expect(execution).resolves.toMatchObject({ status: "cancelled", completedSteps: [] });
    expect(signal.aborted).toBe(true);
    expect(store.getSession("abort-on-cancel", "writer")).toMatchObject({ status: "cancelled" });
    store.close();
  });

  test("reserves model-call budgets before invoking providers", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = parseAgentflowWorkflowOrThrow(`name: bounded-session
version: 1
style: pipeline
maturity: experimental
limits: { max_model_calls: 1 }
sessions:
  writer: { provider: fixture, resume: true }
steps:
  - { id: first, type: session_request, session: writer, prompt: prompts/draft.md, inputs: [request.md], outputs: [first.md] }
  - { id: second, type: session_request, session: writer, prompt: prompts/draft.md, inputs: [request.md], outputs: [second.md], on_failure: { then: pause } }
`);
    const store = await openAgentflowRunState({ cwd: root });
    createAgentflowLifecycleRun(store, { id: "bounded-session", workflow });
    store.writeArtifact({ id: "request", runId: "bounded-session", path: "request.md", kind: "fixture", contentType: "text/plain", content: "Request" });
    const calls: string[] = [];
    const providers = createAgentflowSessionProviderRegistry().register("fixture", (request) => {
      calls.push(request.stepId);
      return { outputs: { [`${request.stepId}.md`]: request.stepId } };
    });

    const result = await executeAgentflowCommandPipeline(store, "bounded-session", workflow, undefined, providers);

    expect(result).toMatchObject({ status: "paused", completedSteps: ["first"], failedStep: "second" });
    expect(result.message).toContain('Budget "model_calls" would exceed its limit of 1');
    expect(calls).toEqual(["first"]);
    expect(store.getBudget("bounded-session", "model:model_calls")).toMatchObject({ limit: 1, used: 1 });
    store.close();
  });

  test("does not let failure routing override a model-budget pause", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = parseAgentflowWorkflowOrThrow(`name: policy-pause-session
version: 1
style: pipeline
maturity: experimental
limits: { max_model_calls: 1 }
sessions:
  writer: { provider: fixture }
steps:
  - { id: first, type: session_request, session: writer, prompt: prompts/draft.md, inputs: [request.md], outputs: [first.md] }
  - { id: second, type: session_request, session: writer, prompt: prompts/draft.md, inputs: [request.md], outputs: [second.md], on_failure: { then: continue, allowed: true } }
`);
    const store = await openAgentflowRunState({ cwd: root });
    createAgentflowLifecycleRun(store, { id: "policy-pause-session", workflow });
    store.writeArtifact({ id: "request", runId: "policy-pause-session", path: "request.md", kind: "fixture", contentType: "text/plain", content: "Request" });
    const calls: string[] = [];
    const providers = createAgentflowSessionProviderRegistry().register("fixture", (request) => {
      calls.push(request.stepId);
      return { outputs: { [`${request.stepId}.md`]: request.stepId } };
    });

    const result = await executeAgentflowCommandPipeline(store, "policy-pause-session", workflow, undefined, providers);

    expect(result).toMatchObject({ status: "paused", completedSteps: ["first"], failedStep: "second" });
    expect(result.message).toContain('Budget "model_calls" would exceed its limit of 1');
    expect(calls).toEqual(["first"]);
    store.close();
  });

  test("atomically rejects concurrent execution of the same session", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = parseAgentflowWorkflowOrThrow(`name: concurrent-budget
version: 1
style: pipeline
maturity: experimental
limits: { max_model_calls: 1 }
sessions:
  writer: { provider: fixture }
steps:
  - { id: draft, type: session_request, session: writer, prompt: prompts/draft.md, inputs: [request.md], outputs: [response.md] }
`);
    const store = await openAgentflowRunState({ cwd: root });
    createAgentflowLifecycleRun(store, { id: "concurrent-budget", workflow });
    store.writeArtifact({ id: "request", runId: "concurrent-budget", path: "request.md", kind: "fixture", contentType: "text/plain", content: "Request" });
    store.transitionRunWithEvent("concurrent-budget", {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: {} }
    });
    const other = await openAgentflowRunState({ cwd: root });
    let resolveProvider!: (response: { outputs: { "response.md": string } }) => void;
    let calls = 0;
    const providers = createAgentflowSessionProviderRegistry().register("fixture", () => {
      calls += 1;
      return new Promise((resolve) => { resolveProvider = resolve; });
    });
    const first = executeAgentflowSessionRequest(store, "concurrent-budget", workflow, workflow.steps[0]!, providers);
    await waitForProviderStart();

    await expect(executeAgentflowSessionRequest(other, "concurrent-budget", workflow, workflow.steps[0]!, providers))
      .rejects.toMatchObject({ code: "AGENTFLOW_SESSION_ACTIVE" });
    expect(calls).toBe(1);
    resolveProvider({ outputs: { "response.md": "Response" } });
    await first;
    expect(store.getBudget("concurrent-budget", "model:model_calls")?.used).toBe(1);
    other.close();
    store.close();
  });

  test("does not pause an active session when pipeline claiming conflicts", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = sessionWorkflow();
    const store = await openAgentflowRunState({ cwd: root });
    createAgentflowLifecycleRun(store, { id: "active-claim-conflict", workflow });
    store.writeArtifact({ id: "request", runId: "active-claim-conflict", path: "request.md", kind: "fixture", contentType: "text/plain", content: "Request" });
    store.claimSession({
      id: "writer",
      runId: "active-claim-conflict",
      stepId: "other-step",
      provider: "fixture",
      status: "running",
      state: { owner: "other-executor" }
    });
    let calls = 0;
    const providers = createAgentflowSessionProviderRegistry().register("fixture", () => {
      calls += 1;
      return { outputs: { "response.md": "Response" } };
    });

    await expect(executeAgentflowCommandPipeline(store, "active-claim-conflict", workflow, undefined, providers))
      .rejects.toMatchObject({ code: "AGENTFLOW_SESSION_ACTIVE" });
    expect(calls).toBe(0);
    expect(store.getSession("active-claim-conflict", "writer")).toMatchObject({
      status: "running",
      stepId: "other-step",
      state: { owner: "other-executor" }
    });
    store.close();
  });

  test("resolves exact workflow input references to persisted artifact paths", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = parseAgentflowWorkflowOrThrow(`name: dynamic-session-input
version: 1
style: pipeline
maturity: experimental
inputs:
  failure_payload: { required: true }
sessions:
  writer: { provider: fixture }
steps:
  - { id: draft, type: session_request, session: writer, prompt: prompts/draft.md, inputs: ["{{ inputs.failure_payload }}"], outputs: [response.md] }
`);
    const store = await openAgentflowRunState({ cwd: root });
    createAgentflowLifecycleRun(store, {
      id: "dynamic-input",
      workflow,
      inputs: { failure_payload: "ci/../failure.json" }
    });
    store.writeArtifact({ id: "failure", runId: "dynamic-input", path: "failure.json", kind: "fixture", contentType: "application/json", content: "{}" });
    let requestedPath: string | undefined;
    const providers = createAgentflowSessionProviderRegistry().register("fixture", (request) => {
      requestedPath = request.inputs[0]?.path;
      return { outputs: { "response.md": "Response" } };
    });

    await expect(executeAgentflowCommandPipeline(store, "dynamic-input", workflow, undefined, providers))
      .resolves.toMatchObject({ status: "completed" });
    expect(requestedPath).toBe("failure.json");
    store.close();
  });

  test("publishes independent outputs before outputs that overwrite inputs", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = parseAgentflowWorkflowOrThrow(`name: in-place-session-output
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: fixture }
steps:
  - { id: draft, type: session_request, session: writer, prompt: prompts/draft.md, inputs: [state.md], outputs: [state.md, summary.md], overwrite: true }
`);
    const store = await openAgentflowRunState({ cwd: root });
    createAgentflowLifecycleRun(store, { id: "in-place-output", workflow });
    store.writeArtifact({ id: "state", runId: "in-place-output", path: "state.md", kind: "fixture", contentType: "text/markdown", content: "Old" });
    const providers = createAgentflowSessionProviderRegistry().register("fixture", () => ({
      outputs: { "state.md": "New", "summary.md": "Summary" }
    }));

    await expect(executeAgentflowCommandPipeline(store, "in-place-output", workflow, undefined, providers))
      .resolves.toMatchObject({ status: "completed", completedSteps: ["draft"] });
    expect(store.readArtifact("in-place-output", "state.md").content.toString()).toBe("New");
    expect(store.readArtifact("in-place-output", "summary.md").content.toString()).toBe("Summary");
    store.close();
  });

  test("clears stale external IDs for non-resumable sessions", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = parseAgentflowWorkflowOrThrow(`name: non-resumable-session
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: fixture, resume: false }
steps:
  - { id: first, type: session_request, session: writer, prompt: prompts/draft.md, inputs: [request.md], outputs: [first.md] }
  - { id: second, type: session_request, session: writer, prompt: prompts/draft.md, inputs: [request.md], outputs: [second.md] }
`);
    const store = await openAgentflowRunState({ cwd: root });
    createAgentflowLifecycleRun(store, { id: "non-resumable", workflow });
    store.writeArtifact({ id: "request", runId: "non-resumable", path: "request.md", kind: "fixture", contentType: "text/plain", content: "Request" });
    const providers = createAgentflowSessionProviderRegistry().register("fixture", (request) => request.stepId === "first"
      ? { externalSessionId: "stale-id", outputs: { "first.md": "First" } }
      : { outputs: { "second.md": "Second" } });

    await expect(executeAgentflowCommandPipeline(store, "non-resumable", workflow, undefined, providers))
      .resolves.toMatchObject({ status: "completed" });
    expect(store.getSession("non-resumable", "writer")).toMatchObject({
      status: "waiting",
      externalSessionId: null
    });
    store.close();
  });
});

async function pendingProviderRun(runId: string) {
  const root = temporaryRepo();
  fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
  fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
  const workflow = sessionWorkflow();
  const store = await openAgentflowRunState({ cwd: root });
  createAgentflowLifecycleRun(store, { id: runId, workflow });
  store.writeArtifact({ id: "request", runId, path: "request.md", kind: "fixture", contentType: "text/plain", content: "Request" });
  return { store, workflow };
}

async function waitForProviderStart(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 0));
}

function sessionWorkflow() {
  return parseAgentflowWorkflowOrThrow(`name: fixture-session
version: 1
style: pipeline
maturity: experimental
sessions:
  writer:
    provider: fixture
    resume: true
steps:
  - id: draft
    type: session_request
    session: writer
    prompt: prompts/draft.md
    inputs: [request.md]
    outputs: [response.md]
    on_failure: { then: pause }
`);
}

function temporaryRepo(): string {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "agentflow-session-request-"));
  fs.mkdirSync(path.join(root, ".git"));
  return root;
}
