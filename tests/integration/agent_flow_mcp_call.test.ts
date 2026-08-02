import { describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  createAgentFlowFixtureMcpAdapter,
  createAgentFlowLifecycleRun,
  createAgentFlowMcpCallRegistry,
  executeAgentFlowCommandPipeline,
  executeAgentFlowMcpCall,
  MAX_AGENT_FLOW_MCP_ARGUMENT_BYTES,
  MAX_AGENT_FLOW_MCP_CONTENT_TYPE_BYTES,
  MAX_AGENT_FLOW_MCP_METADATA_BYTES,
  MAX_AGENT_FLOW_MCP_OUTPUT_BYTES,
  openAgentFlowRunState,
  parseAgentFlowSimulationFixture,
  parseAgentFlowWorkflowOrThrow,
  simulateAgentFlowWorkflow,
  transitionAgentFlowLifecycleRun,
  validateAgentFlowWorkflow,
  type AgentFlowMcpCallRequest
} from "../../src/runtime";
import { resolveAgentFlowMcpArguments } from "../../src/runtime/mcp_call";

const repoRoot = path.resolve(".");
const examplePath = path.join(repoRoot, "examples/workflows/jira-ticket-spec.yml");
const fixturePath = path.join(repoRoot, "tests/fixtures/agent-flow/simulation/jira-ticket.json");

describe("Agent Flow MCP call steps", () => {
  test("requires server, tool, arguments, and declared outputs", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: invalid-mcp-contract
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: missing, type: mcp_call }
`);

    expect(validateAgentFlowWorkflow(workflow).errors.map((issue) => issue.code)).toEqual([
      "workflow.step.field.required",
      "workflow.step.field.required",
      "workflow.step.field.required",
      "workflow.step.field.required"
    ]);
  });

  test("rejects binary MCP arguments at static and pipeline preflight boundaries", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: binary-mcp-arguments
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: fetch, type: mcp_call, server: fixture, tool: fetch, arguments: {}, outputs: [ticket.json] }
`);
    workflow.steps[0]!.arguments = new Uint8Array([1, 2, 3]) as never;

    expect(validateAgentFlowWorkflow(workflow).errors).toContainEqual(expect.objectContaining({
      code: "workflow.step.field.required",
      path: "steps[0].arguments"
    }));

    workflow.steps[0]!.arguments = {};
    const store = await openAgentFlowRunState({ cwd: root });
    store.createRunWithEvent({
      id: "binary-arguments",
      workflow: { name: workflow.name, version: workflow.version, style: workflow.style, maturity: workflow.maturity },
      context: { workflow: workflow as never }
    }, { type: "run.created", payload: { status: "pending" } });
    workflow.steps[0]!.arguments = new Uint8Array([1, 2, 3]) as never;
    const persistedRun = store.getRun("binary-arguments")!;
    const getRun = store.getRun.bind(store);
    let initialRead = true;
    store.getRun = (runId) => {
      if (!initialRead) return getRun(runId);
      initialRead = false;
      return { ...persistedRun, context: { ...persistedRun.context, workflow: workflow as never } };
    };

    const result = await executeAgentFlowCommandPipeline(store, "binary-arguments", workflow);

    expect(result).toMatchObject({ status: "failed", failedStep: "fetch" });
    expect(store.listEvents("binary-arguments").map((event) => event.type)).toEqual([
      "run.created",
      "run.started",
      "step.rejected",
      "run.failed"
    ]);
    store.close();
  });

  test("classifies externally persisted contract failures as rejected MCP policy", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: rejected-mcp-contract
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: fetch, type: mcp_call, server: fixture, tool: fetch, arguments: {} }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    store.createRunWithEvent({
      id: "rejected-contract",
      workflow: {
        name: workflow.name,
        version: workflow.version,
        style: workflow.style,
        maturity: workflow.maturity
      },
      context: { workflow: workflow as never }
    }, { type: "run.created", payload: { status: "pending" } });
    const classifications: string[] = [];
    const recordFailure = store.recordFailure.bind(store);
    store.recordFailure = (input) => {
      classifications.push(input.classification);
      recordFailure(input);
    };

    const result = await executeAgentFlowCommandPipeline(store, "rejected-contract", workflow);

    expect(result).toMatchObject({ status: "failed", failedStep: "fetch" });
    expect(classifications).toEqual(["mcp_call_policy"]);
    expect(store.listEvents("rejected-contract").map((event) => event.type)).toEqual([
      "run.created",
      "run.started",
      "step.rejected",
      "run.failed"
    ]);
    store.close();
  });

  test("rejects unsupported persisted argument templates before failure policies", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: rejected-mcp-arguments
version: 1
style: pipeline
maturity: experimental
steps:
  - id: fetch
    type: mcp_call
    server: fixture
    tool: fetch
    arguments: { key: "{{ inputs.ticket.key }}" }
    outputs: [ticket.json]
    on_failure: { then: continue, allowed: true }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    store.createRunWithEvent({
      id: "rejected-arguments",
      workflow: { name: workflow.name, version: workflow.version, style: workflow.style, maturity: workflow.maturity },
      context: { workflow: workflow as never },
      inputs: { ticket: { key: "AM-26" } }
    }, { type: "run.created", payload: { status: "pending" } });

    const result = await executeAgentFlowCommandPipeline(store, "rejected-arguments", workflow);

    expect(result).toMatchObject({ status: "failed", failedStep: "fetch" });
    expect(store.listEvents("rejected-arguments").map((event) => event.type)).toEqual([
      "run.created",
      "run.started",
      "step.rejected",
      "run.failed"
    ]);
    store.close();
  });

  test("rejects malformed, duplicate, and dynamic output declarations", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: invalid-mcp-outputs
version: 1
style: pipeline
maturity: experimental
steps:
  - id: fetch
    type: mcp_call
    server: fixture
    tool: fetch
    arguments: {}
    outputs: [ticket.json, ticket.json, ../outside.json, "{{ inputs.output }}"]
`);

    expect(validateAgentFlowWorkflow(workflow).errors).toEqual(expect.arrayContaining([
      expect.objectContaining({ code: "workflow.mcp_call.output.duplicate", path: "steps[0].outputs[1]" }),
      expect.objectContaining({ code: "workflow.mcp_call.output.invalid", path: "steps[0].outputs[2]" }),
      expect.objectContaining({ code: "workflow.mcp_call.output.invalid", path: "steps[0].outputs[3]" })
    ]));

    const indexPreserving = parseAgentFlowWorkflowOrThrow(`name: indexed-invalid-mcp-outputs
version: 1
style: pipeline
maturity: experimental
steps:
  - id: fetch
    type: mcp_call
    server: fixture
    tool: fetch
    arguments: {}
    outputs: [ticket.json, null, ticket.json, ""]
`);
    expect(validateAgentFlowWorkflow(indexPreserving).errors).toEqual(expect.arrayContaining([
      expect.objectContaining({ code: "workflow.mcp_call.output.invalid", path: "steps[0].outputs[1]" }),
      expect.objectContaining({ code: "workflow.mcp_call.output.duplicate", path: "steps[0].outputs[2]" }),
      expect.objectContaining({ code: "workflow.mcp_call.output.invalid", path: "steps[0].outputs[3]" })
    ]));
  });

  test("rejects dynamic MCP server and tool declarations", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: dynamic-mcp-adapter
version: 1
style: pipeline
maturity: experimental
inputs:
  server: { required: true }
  tool: { required: true }
steps:
  - id: fetch
    type: mcp_call
    server: "{{ inputs.server }}"
    tool: "{{ inputs.tool }}"
    arguments: {}
    outputs: [ticket.json]
`);

    expect(validateAgentFlowWorkflow(workflow).errors).toEqual(expect.arrayContaining([
      expect.objectContaining({ code: "workflow.mcp_call.server.invalid", path: "steps[0].server" }),
      expect.objectContaining({ code: "workflow.mcp_call.tool.invalid", path: "steps[0].tool" })
    ]));
  });

  test("rejects MCP argument expressions unsupported by the runtime resolver", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: unsupported-mcp-expression
version: 1
style: pipeline
maturity: experimental
inputs: { ticket: { required: true } }
steps:
  - id: fetch
    type: mcp_call
    server: fixture
    tool: get_issue
    arguments: { key: "{{ inputs.ticket.key }}" }
    outputs: [ticket.json]
`);

    expect(validateAgentFlowWorkflow(workflow).errors).toContainEqual(expect.objectContaining({
      code: "workflow.mcp_call.arguments.expression.unsupported",
      path: "steps[0].arguments.key"
    }));

    for (const reference of ["{{ inputs.123 }}", "{{ inputs.-key }}"]) {
      const numericOrHyphenated = parseAgentFlowWorkflowOrThrow(`name: invalid-mcp-input-name
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: fetch, type: mcp_call, server: fixture, tool: get_issue, arguments: { key: "${reference}" }, outputs: [ticket.json] }
`);
      expect(validateAgentFlowWorkflow(numericOrHyphenated).errors).toContainEqual(expect.objectContaining({
        code: "workflow.mcp_call.arguments.expression.unsupported"
      }));
    }
  });

  test("rejects dynamic adapter names at the direct MCP executor boundary", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: direct-dynamic-mcp
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: fetch, type: mcp_call, server: "{{ inputs.server }}", tool: get_issue, arguments: {}, outputs: [ticket.json] }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    store.createRunWithEvent({
      id: "direct-dynamic",
      workflow: { name: workflow.name, version: workflow.version, style: workflow.style, maturity: workflow.maturity },
      context: { workflow: workflow as never }
    }, { type: "run.created", payload: { status: "pending" } });
    store.transitionRunWithEvent("direct-dynamic", {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: {} }
    });
    let invoked = false;
    const calls = createAgentFlowMcpCallRegistry().register("{{ inputs.server }}", () => {
      invoked = true;
      return { outputs: { "ticket.json": {} } };
    });

    await expect(executeAgentFlowMcpCall(store, "direct-dynamic", workflow, workflow.steps[0]!, calls))
      .rejects.toMatchObject({ code: "AGENT_FLOW_MCP_CALL_INVALID" });
    expect(invoked).toBe(false);
    store.close();
  });

  test("validates and simulates jira-ticket-spec with fixture-only MCP output", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(fs.readFileSync(examplePath, "utf8"));
    const fixtureResult = parseAgentFlowSimulationFixture(fs.readFileSync(fixturePath, "utf8"));
    if (!fixtureResult.ok) throw new Error(fixtureResult.error);

    expect(validateAgentFlowWorkflow(workflow)).toEqual({ valid: true, errors: [] });
    const result = simulateAgentFlowWorkflow(workflow, fixtureResult.fixture);

    expect(result).toMatchObject({ status: "completed" });
    expect(result.availableArtifacts).toEqual(["spec.md", "ticket.json", "ticket.md"]);
    expect(result.artifactValues["ticket.json"]).toMatchObject({ key: "AM-24" });
  });

  test("fails simulation when MCP arguments cannot resolve fixture inputs", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: unresolved-mcp-arguments
version: 1
style: pipeline
maturity: experimental
inputs:
  ticket_key: { required: false }
steps:
  - id: fetch
    type: mcp_call
    server: fixture
    tool: get_issue
    arguments: { key: "{{ inputs.ticket_key }}" }
    outputs: [ticket.json]
    on_failure: { then: pause }
`);

    const result = simulateAgentFlowWorkflow(workflow, {
      steps: { fetch: { outputs: { "ticket.json": { key: "AM-26" } } } }
    });

    expect(result.status).toBe("paused");
    expect(result.visitedSteps).toEqual([{ id: "fetch", type: "mcp_call", outcome: "failed" }]);
    expect(result.availableArtifacts).toEqual([]);
  });

  test("does not retry deterministic MCP simulation failures", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: deterministic-mcp-simulation
version: 1
style: pipeline
maturity: experimental
steps:
  - id: fetch
    type: mcp_call
    server: fixture
    tool: get
    arguments: { key: "{{ inputs.missing }}" }
    outputs: [ticket.json]
    on_failure: { retry: 2, then: pause }
`);

    const result = simulateAgentFlowWorkflow(workflow, {
      steps: { fetch: { outputs: { "ticket.json": { ok: true } } } }
    });

    expect(result.status).toBe("paused");
    expect(result.visitedSteps).toEqual([{ id: "fetch", type: "mcp_call", outcome: "failed" }]);
  });

  test("pauses unhandled deterministic MCP simulation failures", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: unhandled-mcp-simulation
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: fetch, type: mcp_call, server: fixture, tool: get, arguments: { key: "{{ inputs.missing }}" }, outputs: [ticket.json] }
`);

    const result = simulateAgentFlowWorkflow(workflow, {
      steps: { fetch: { outputs: { "ticket.json": { ok: true } } } }
    });

    expect(result.status).toBe("paused");
    expect(result.terminalStates).toEqual([{ stepId: "fetch", status: "paused" }]);
  });

  test("pauses deterministic MCP simulation failures after suppressing retry-only policies", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: retry-only-mcp-simulation
version: 1
style: pipeline
maturity: experimental
steps:
  - id: fetch
    type: mcp_call
    server: fixture
    tool: get
    arguments: { key: "{{ inputs.missing }}" }
    outputs: [ticket.json]
    on_failure: { retry: 2 }
`);

    const result = simulateAgentFlowWorkflow(workflow, {
      steps: { fetch: { outputs: { "ticket.json": { ok: true } } } }
    });

    expect(result.status).toBe("paused");
    expect(result.visitedSteps).toEqual([{ id: "fetch", type: "mcp_call", outcome: "failed" }]);
  });

  test("rejects aliased MCP fixture output paths", () => {
    const workflow = mcpWorkflow();

    const result = simulateAgentFlowWorkflow(workflow, {
      inputs: { ticket_key: "AM-26" },
      steps: { fetch: { outputs: { "dir/../ticket.json": { key: "AM-26" } } } }
    });

    expect(result.status).toBe("paused");
    expect(result.visitedSteps).toEqual([{ id: "fetch", type: "mcp_call", outcome: "failed" }]);
  });

  test("routes resolved arguments through an adapter and persists request metadata plus outputs", async () => {
    const root = temporaryRepo();
    const workflow = mcpWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, {
      id: "mcp-run",
      workflow,
      inputs: { ticket_key: "AM-26" }
    });
    const requests: AgentFlowMcpCallRequest[] = [];
    const calls = createAgentFlowMcpCallRegistry().register("fixture", (request) => {
      requests.push(request);
      return {
        outputs: { "ticket.json": { key: request.arguments.key, fields: { summary: "MCP contract" } } },
        metadata: { fixture: true, requestId: "fixture-1" }
      };
    });

    const result = await executeAgentFlowCommandPipeline(store, "mcp-run", workflow, undefined, undefined, calls);

    expect(result).toMatchObject({ status: "completed", completedSteps: ["fetch"] });
    expect(requests).toHaveLength(1);
    expect(requests[0]).toMatchObject({
      stepId: "fetch",
      server: "fixture",
      tool: "get_issue",
      arguments: { key: "AM-26" },
      outputs: ["ticket.json"]
    });
    expect(JSON.parse(store.readArtifact("mcp-run", "ticket.json").content.toString())).toMatchObject({
      key: "AM-26",
      fields: { summary: "MCP contract" }
    });
    const requestArtifact = store.listArtifacts("mcp-run").find((artifact) => artifact.kind === "mcp_request")!;
    expect(JSON.parse(store.readArtifact("mcp-run", requestArtifact.declaredPath).content.toString())).toMatchObject({
      stepId: "fetch",
      server: "fixture",
      tool: "get_issue",
      arguments: { key: "AM-26" },
      outputs: ["ticket.json"],
      responseMetadata: { fixture: true, requestId: "fixture-1" }
    });
    expect(store.listEvents("mcp-run").map((event) => event.type)).toEqual([
      "run.created",
      "run.started",
      "step.started",
      "step.completed",
      "run.completed"
    ]);
    store.close();
  });

  test("rejects unsupported MCP argument templates in the runtime resolver", () => {
    expect(() => resolveAgentFlowMcpArguments(
      { key: "{{ inputs.ticket.key }}" },
      { ticket: { key: "AM-26" } },
      "fetch"
    )).toThrow("unsupported input expression");
    expect(() => resolveAgentFlowMcpArguments(
      { key: "prefix {{ inputs.toString }}" },
      {},
      "fetch"
    )).toThrow("missing from persisted inputs");
    expect(resolveAgentFlowMcpArguments(
      { key: "prefix {{ inputs.text }}" },
      { text: "literal {{ brace }}" },
      "fetch"
    )).toEqual({ key: "prefix literal {{ brace }}" });
  });

  test("snapshots resolved arguments before giving the adapter a mutable request", async () => {
    const root = temporaryRepo();
    const workflow = mcpWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "mutable-request", workflow, inputs: { ticket_key: "AM-26" } });
    const calls = createAgentFlowMcpCallRegistry().register("fixture", (request) => {
      request.arguments.key = "rewritten";
      return { outputs: { "ticket.json": { key: "AM-26" } } };
    });

    const result = await executeAgentFlowCommandPipeline(store, "mutable-request", workflow, undefined, undefined, calls);

    expect(result.status).toBe("completed");
    const requestArtifact = store.listArtifacts("mutable-request").find((artifact) => artifact.kind === "mcp_request")!;
    expect(JSON.parse(store.readArtifact("mutable-request", requestArtifact.declaredPath).content.toString()).arguments)
      .toEqual({ key: "AM-26" });
    store.close();
  });

  test("preserves JSON objects shaped like content metadata without treating them as wrappers", async () => {
    const root = temporaryRepo();
    const workflow = mcpWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "content-shaped-json", workflow, inputs: { ticket_key: "AM-26" } });
    const calls = createAgentFlowMcpCallRegistry().register("fixture", () => ({
      outputs: { "ticket.json": { content: "body", contentType: "record-type" } },
      contentTypes: { "ticket.json": "application/vnd.agent-flow.test+json" }
    }));

    const result = await executeAgentFlowCommandPipeline(store, "content-shaped-json", workflow, undefined, undefined, calls);

    expect(result.status).toBe("completed");
    expect(JSON.parse(store.readArtifact("content-shaped-json", "ticket.json").content.toString()))
      .toEqual({ content: "body", contentType: "record-type" });
    expect(store.getArtifact("content-shaped-json", "ticket.json")?.contentType)
      .toBe("application/vnd.agent-flow.test+json");
    store.close();
  });

  test("uses an explicit fixture adapter and fails closed when the response is absent", async () => {
    const root = temporaryRepo();
    const workflow = mcpWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "missing-fixture", workflow, inputs: { ticket_key: "AM-26" } });
    const calls = createAgentFlowMcpCallRegistry().register("fixture", createAgentFlowFixtureMcpAdapter({}));

    const result = await executeAgentFlowCommandPipeline(store, "missing-fixture", workflow, undefined, undefined, calls);

    expect(result).toMatchObject({ status: "paused", failedStep: "fetch" });
    expect(result.message).toContain("no response for step fetch");
    expect(store.listArtifacts("missing-fixture").filter((artifact) => artifact.kind !== "failure_payload")).toEqual([]);
    const failure = store.listFailures("missing-fixture")[0]!;
    expect(failure.payloadPath).toMatch(/^failures\/.+\.json$/);
    expect(JSON.parse(store.readArtifact("missing-fixture", failure.payloadPath!).content.toString("utf8")))
      .toMatchObject({
        id: failure.id,
        step_id: "fetch",
        step_type: "mcp_call",
        status: "failed",
        attempt: 1,
        exit_code: null,
        command: null,
        logs: { stdout: null, stderr: null },
        classification: "mcp_call_failure",
        remediation_status: null,
        path: failure.payloadPath
      });
    store.close();
  });

  test("rejects missing and undeclared adapter outputs without partial publication", async () => {
    for (const outputs of [{}, { "ticket.json": { key: "AM-26" }, "extra.json": {} }]) {
      const root = temporaryRepo();
      const workflow = mcpWorkflow();
      const store = await openAgentFlowRunState({ cwd: root });
      createAgentFlowLifecycleRun(store, { id: "invalid-output", workflow, inputs: { ticket_key: "AM-26" } });
      const calls = createAgentFlowMcpCallRegistry().register("fixture", () => ({ outputs }));

      const result = await executeAgentFlowCommandPipeline(store, "invalid-output", workflow, undefined, undefined, calls);

      expect(result).toMatchObject({ status: "paused", failedStep: "fetch" });
      expect(store.listArtifacts("invalid-output").filter((artifact) => artifact.kind !== "failure_payload")).toEqual([]);
      store.close();
    }
  });

  test("checks output collisions before invoking the adapter", async () => {
    const root = temporaryRepo();
    const workflow = mcpWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "collision", workflow, inputs: { ticket_key: "AM-26" } });
    store.writeArtifact({
      id: "existing",
      runId: "collision",
      path: "ticket.json",
      kind: "fixture",
      contentType: "application/json",
      content: "{}"
    });
    let invoked = false;
    const calls = createAgentFlowMcpCallRegistry().register("fixture", () => {
      invoked = true;
      return { outputs: { "ticket.json": {} } };
    });

    const result = await executeAgentFlowCommandPipeline(store, "collision", workflow, undefined, undefined, calls);

    expect(result).toMatchObject({ status: "paused", failedStep: "fetch" });
    expect(result.message).toContain("already exists");
    expect(invoked).toBe(false);
    expect(store.readArtifact("collision", "ticket.json").content.toString()).toBe("{}");
    store.close();
  });

  test("checks deterministic request and output ID collisions before invoking an adapter", async () => {
    for (const [id, seededPath] of [
      [`mcp-request:${createHash("sha256").update("mcp-calls/fetch-e7d3799ecc09.json").digest("hex")}`, "other-request.json"],
      [`mcp-output:${createHash("sha256").update("ticket.json").digest("hex")}`, "other-output.json"]
    ] as const) {
      const root = temporaryRepo();
      const workflow = mcpWorkflow();
      const store = await openAgentFlowRunState({ cwd: root });
      createAgentFlowLifecycleRun(store, { id: "id-collision", workflow, inputs: { ticket_key: "AM-26" } });
      store.writeArtifact({
        id,
        runId: "id-collision",
        stepId: "other",
        path: seededPath,
        kind: "fixture",
        contentType: "application/json",
        content: "{}"
      });
      let invoked = false;
      const calls = createAgentFlowMcpCallRegistry().register("fixture", () => {
        invoked = true;
        return { outputs: { "ticket.json": {} } };
      });

      const result = await executeAgentFlowCommandPipeline(store, "id-collision", workflow, undefined, undefined, calls);

      expect(result).toMatchObject({ status: "paused", failedStep: "fetch" });
      expect(result.message).toContain("already registered");
      expect(invoked).toBe(false);
      store.close();
    }
  });

  test("rejects non-static output paths before invoking the adapter", async () => {
    for (const output of ["a/../ticket.json", "{{ inputs.output }}"]) {
      const root = temporaryRepo();
      const workflow = parseAgentFlowWorkflowOrThrow(`name: invalid-runtime-output
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: fetch, type: mcp_call, server: fixture, tool: fetch, arguments: {}, outputs: ["${output}"], on_failure: { then: pause } }
`);
      const store = await openAgentFlowRunState({ cwd: root });
      store.createRunWithEvent({
        id: "invalid-runtime-output",
        workflow: {
          name: workflow.name,
          version: workflow.version,
          style: workflow.style,
          maturity: workflow.maturity
        },
        context: { workflow: workflow as never }
      }, { type: "run.created", payload: { status: "pending" } });
      let invoked = false;
      const calls = createAgentFlowMcpCallRegistry().register("fixture", () => {
        invoked = true;
        return { outputs: { [output]: {} } };
      });

      const result = await executeAgentFlowCommandPipeline(store, "invalid-runtime-output", workflow, undefined, undefined, calls);

      expect(result).toMatchObject({ status: "failed", failedStep: "fetch" });
      expect(result.message).toContain("normalized static repo-relative artifact path");
      expect(invoked).toBe(false);
      expect(store.listArtifacts("invalid-runtime-output")
        .filter((artifact) => artifact.kind !== "failure_payload")
        .map((artifact) => artifact.declaredPath))
        .toEqual(["final-summary.md"]);
      store.close();
    }
  });

  test("classifies malformed persisted MCP output declarations as preflight rejections", async () => {
    for (const outputs of ["[ticket.json, ticket.json]", "[a/../ticket.json]", "[\"{{ inputs.output }}\"]"]) {
      const root = temporaryRepo();
      const workflow = parseAgentFlowWorkflowOrThrow(`name: malformed-persisted-output
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: fetch, type: mcp_call, server: fixture, tool: fetch, arguments: {}, outputs: ${outputs} }
`);
      const store = await openAgentFlowRunState({ cwd: root });
      store.createRunWithEvent({
        id: "malformed-output",
        workflow: {
          name: workflow.name,
          version: workflow.version,
          style: workflow.style,
          maturity: workflow.maturity
        },
        context: { workflow: workflow as never }
      }, { type: "run.created", payload: { status: "pending" } });
      const classifications: string[] = [];
      const recordFailure = store.recordFailure.bind(store);
      store.recordFailure = (input) => {
        classifications.push(input.classification);
        recordFailure(input);
      };

      const result = await executeAgentFlowCommandPipeline(store, "malformed-output", workflow);

      expect(result).toMatchObject({ status: "failed", failedStep: "fetch" });
      expect(classifications).toEqual(["mcp_call_policy"]);
      expect(store.listEvents("malformed-output").map((event) => event.type)).toEqual([
        "run.created",
        "run.started",
        "step.rejected",
        "run.failed"
      ]);
      store.close();
    }
  });

  test("enforces step-attempt limits before MCP preflight rejection", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: bounded-mcp-preflight
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_step_attempts: { fetch: 0.5 } }
steps:
  - { id: fetch, type: mcp_call, server: fixture, tool: fetch, arguments: {}, outputs: [ticket.json, ticket.json] }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    store.createRunWithEvent({
      id: "bounded-mcp-preflight",
      workflow: {
        name: workflow.name,
        version: workflow.version,
        style: workflow.style,
        maturity: workflow.maturity
      },
      context: { workflow: workflow as never }
    }, { type: "run.created", payload: { status: "pending" } });

    const result = await executeAgentFlowCommandPipeline(store, "bounded-mcp-preflight", workflow);

    expect(result).toMatchObject({
      status: "paused",
      failedStep: "fetch",
      message: "Step fetch cannot start because limits.max_step_attempts allows 0.5 attempt(s)."
    });
    expect(store.listEvents("bounded-mcp-preflight").map((event) => event.type)).toEqual([
      "run.created", "run.started", "recovery.limit_reached", "run.paused"
    ]);
    store.close();
  });

  test("checks every output collision before invoking a multi-output adapter", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: multi-output-collision
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: fetch, type: mcp_call, server: fixture, tool: fetch, arguments: {}, outputs: [new.json, occupied.json], on_failure: { then: pause } }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "multi-collision", workflow });
    store.writeArtifact({ id: "occupied", runId: "multi-collision", path: "occupied.json", kind: "fixture", contentType: "application/json", content: "{}" });
    let invoked = false;
    const calls = createAgentFlowMcpCallRegistry().register("fixture", () => {
      invoked = true;
      return { outputs: { "new.json": {}, "occupied.json": {} } };
    });

    const result = await executeAgentFlowCommandPipeline(store, "multi-collision", workflow, undefined, undefined, calls);

    expect(result).toMatchObject({ status: "paused", failedStep: "fetch" });
    expect(result.message).toContain("occupied.json already exists");
    expect(invoked).toBe(false);
    expect(store.getArtifact("multi-collision", "new.json")).toBeNull();
    store.close();
  });

  test("does not overwrite request metadata artifacts owned by another producer", async () => {
    const root = temporaryRepo();
    const workflow = mcpWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "request-owner", workflow, inputs: { ticket_key: "AM-26" } });
    const stepDigest = createHash("sha256").update("fetch").digest("hex").slice(0, 12);
    const requestPath = `mcp-calls/fetch-${stepDigest}.json`;
    const requestId = `mcp-request:${createHash("sha256").update(requestPath).digest("hex")}`;
    store.writeArtifact({
      id: requestId,
      runId: "request-owner",
      stepId: "other",
      path: requestPath,
      kind: "mcp_request",
      contentType: "application/json",
      content: "seeded",
      metadata: { server: "fixture", tool: "get_issue" }
    });
    let invoked = false;
    const calls = createAgentFlowMcpCallRegistry().register("fixture", () => {
      invoked = true;
      return { outputs: { "ticket.json": { key: "AM-26" } } };
    });

    const result = await executeAgentFlowCommandPipeline(store, "request-owner", workflow, undefined, undefined, calls);

    expect(result).toMatchObject({ status: "paused", failedStep: "fetch" });
    expect(result.message).toContain("already owned by another artifact");
    expect(invoked).toBe(false);
    expect(store.readArtifact("request-owner", requestPath).content.toString()).toBe("seeded");
    store.close();
  });

  test("publishes into an owned pre-registered output artifact", async () => {
    const root = temporaryRepo();
    const workflow = mcpWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "reserved-output", workflow, inputs: { ticket_key: "AM-26" } });
    store.upsertArtifact({
      id: `mcp-output:${createHash("sha256").update("ticket.json").digest("hex")}`,
      runId: "reserved-output",
      stepId: "fetch",
      path: "ticket.json",
      kind: "mcp_output",
      contentType: "application/json",
      metadata: { server: "fixture", tool: "get_issue" }
    });
    const calls = createAgentFlowMcpCallRegistry().register("fixture", () => ({
      outputs: { "ticket.json": { key: "AM-26" } }
    }));

    const result = await executeAgentFlowCommandPipeline(store, "reserved-output", workflow, undefined, undefined, calls);

    expect(result.status).toBe("completed");
    expect(JSON.parse(store.readArtifact("reserved-output", "ticket.json").content.toString()))
      .toEqual({ key: "AM-26" });
    store.close();
  });

  test("does not claim a pre-registered output owned by another step", async () => {
    const root = temporaryRepo();
    const workflow = mcpWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "foreign-reservation", workflow, inputs: { ticket_key: "AM-26" } });
    store.upsertArtifact({
      id: `mcp-output:${createHash("sha256").update("ticket.json").digest("hex")}`,
      runId: "foreign-reservation",
      stepId: "other",
      path: "ticket.json",
      kind: "mcp_output",
      contentType: "application/json",
      metadata: { server: "fixture", tool: "get_issue" }
    });
    let invoked = false;
    const calls = createAgentFlowMcpCallRegistry().register("fixture", () => {
      invoked = true;
      return { outputs: { "ticket.json": { key: "AM-26" } } };
    });

    const result = await executeAgentFlowCommandPipeline(store, "foreign-reservation", workflow, undefined, undefined, calls);

    expect(result).toMatchObject({ status: "paused", failedStep: "fetch" });
    expect(result.message).toContain("already exists");
    expect(invoked).toBe(false);
    expect(store.getArtifact("foreign-reservation", "ticket.json")?.producerStepId).toBe("other");
    store.close();
  });

  test("recovers an interrupted staged output before collision preflight", async () => {
    const root = temporaryRepo();
    const workflow = mcpWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "staged-recovery", workflow, inputs: { ticket_key: "AM-26" } });
    store.transitionRunWithEvent("staged-recovery", {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: {} }
    });
    const seedCalls = createAgentFlowMcpCallRegistry().register("fixture", () => ({
      outputs: { "ticket.json": { version: "initial" } }
    }));
    await executeAgentFlowMcpCall(store, "staged-recovery", workflow, workflow.steps[0]!, seedCalls);
    const artifact = store.getArtifact("staged-recovery", "ticket.json")!;
    const target = path.join(root, artifact.storagePath);
    const staging = path.join(
      root,
      ".agent-flow",
      "runs",
      `r-${createHash("sha256").update("staged-recovery").digest("hex")}`,
      ".staging"
    );
    fs.mkdirSync(staging, { recursive: true });
    const backup = path.join(staging, `${createHash("sha256").update("ticket.json").digest("hex")}.old`);
    fs.renameSync(target, backup);
    const retryCalls = createAgentFlowMcpCallRegistry().register("fixture", () => ({
      outputs: { "ticket.json": { version: "retried" } }
    }));

    const result = await executeAgentFlowMcpCall(store, "staged-recovery", workflow, workflow.steps[0]!, retryCalls);

    expect(result.outputArtifacts).toHaveLength(1);
    expect(JSON.parse(store.readArtifact("staged-recovery", "ticket.json").content.toString()))
      .toEqual({ version: "retried" });
    expect(fs.existsSync(backup)).toBe(false);
    store.close();
  });

  test("finalizes matching orphaned output content from an interrupted publication", async () => {
    const root = temporaryRepo();
    const workflow = mcpWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "orphan-output", workflow, inputs: { ticket_key: "AM-26" } });
    store.transitionRunWithEvent("orphan-output", {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: {} }
    });
    const target = path.join(
      root,
      ".agent-flow",
      "runs",
      `r-${createHash("sha256").update("orphan-output").digest("hex")}`,
      "artifacts",
      `a-${createHash("sha256").update("ticket.json").digest("hex")}`
    );
    fs.mkdirSync(path.dirname(target), { recursive: true });
    fs.writeFileSync(target, '{"key":"AM-26"}\n');
    const calls = createAgentFlowMcpCallRegistry().register("fixture", () => ({
      outputs: { "ticket.json": { key: "AM-26" } }
    }));

    const result = await executeAgentFlowMcpCall(store, "orphan-output", workflow, workflow.steps[0]!, calls);

    expect(result.outputArtifacts[0]).toMatchObject({ declaredPath: "ticket.json", status: "available" });
    expect(store.readArtifact("orphan-output", "ticket.json").content.toString()).toBe('{"key":"AM-26"}\n');
    store.close();
  });

  test("does not replace mismatched orphaned output content", async () => {
    const root = temporaryRepo();
    const workflow = mcpWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "foreign-orphan", workflow, inputs: { ticket_key: "AM-26" } });
    store.transitionRunWithEvent("foreign-orphan", {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: {} }
    });
    const target = path.join(
      root,
      ".agent-flow",
      "runs",
      `r-${createHash("sha256").update("foreign-orphan").digest("hex")}`,
      "artifacts",
      `a-${createHash("sha256").update("ticket.json").digest("hex")}`
    );
    fs.mkdirSync(path.dirname(target), { recursive: true });
    fs.writeFileSync(target, "foreign\n");
    const calls = createAgentFlowMcpCallRegistry().register("fixture", () => ({
      outputs: { "ticket.json": { key: "AM-26" } }
    }));

    await expect(executeAgentFlowMcpCall(store, "foreign-orphan", workflow, workflow.steps[0]!, calls))
      .rejects.toMatchObject({ code: "AGENT_FLOW_ARTIFACT_OVERWRITE" });
    expect(fs.readFileSync(target, "utf8")).toBe("foreign\n");
    expect(store.listArtifacts("foreign-orphan")).toEqual([]);
    store.close();
  });

  test("rechecks request metadata ownership after the adapter returns", async () => {
    const root = temporaryRepo();
    const workflow = mcpWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "request-race", workflow, inputs: { ticket_key: "AM-26" } });
    const stepDigest = createHash("sha256").update("fetch").digest("hex").slice(0, 12);
    const requestPath = `mcp-calls/fetch-${stepDigest}.json`;
    const requestId = `mcp-request:${createHash("sha256").update(requestPath).digest("hex")}`;
    const calls = createAgentFlowMcpCallRegistry().register("fixture", () => {
      store.writeArtifact({
        id: requestId,
        runId: "request-race",
        stepId: "other",
        path: requestPath,
        kind: "mcp_request",
        contentType: "application/json",
        content: "racing writer",
        metadata: { server: "fixture", tool: "get_issue" }
      });
      return { outputs: { "ticket.json": { key: "AM-26" } } };
    });

    const result = await executeAgentFlowCommandPipeline(store, "request-race", workflow, undefined, undefined, calls);

    expect(result).toMatchObject({ status: "paused", failedStep: "fetch" });
    expect(result.message).toContain("changed ownership");
    expect(store.readArtifact("request-race", requestPath).content.toString()).toBe("racing writer");
    expect(store.getArtifact("request-race", "ticket.json")).toBeNull();
    store.close();
  });

  test("does not overwrite MCP outputs owned by a different tool", async () => {
    const root = temporaryRepo();
    const workflow = mcpWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "output-owner", workflow, inputs: { ticket_key: "AM-26" } });
    store.writeArtifact({
      id: "seeded-output",
      runId: "output-owner",
      stepId: "fetch",
      path: "ticket.json",
      kind: "mcp_output",
      contentType: "application/json",
      content: "seeded",
      metadata: { server: "fixture", tool: "different_tool" }
    });
    let invoked = false;
    const calls = createAgentFlowMcpCallRegistry().register("fixture", () => {
      invoked = true;
      return { outputs: { "ticket.json": { key: "AM-26" } } };
    });

    const result = await executeAgentFlowCommandPipeline(store, "output-owner", workflow, undefined, undefined, calls);

    expect(result).toMatchObject({ status: "paused", failedStep: "fetch" });
    expect(result.message).toContain("already exists");
    expect(invoked).toBe(false);
    expect(store.readArtifact("output-owner", "ticket.json").content.toString()).toBe("seeded");
    store.close();
  });

  test("rejects output ownership changes inside atomic publication", async () => {
    const root = temporaryRepo();
    const workflow = mcpWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "output-race", workflow, inputs: { ticket_key: "AM-26" } });
    const calls = createAgentFlowMcpCallRegistry().register("fixture", () => ({
      outputs: { "ticket.json": { key: "AM-26" } }
    }));
    const writeArtifactsAtomically = store.writeArtifactsAtomically.bind(store);
    store.writeArtifactsAtomically = (inputs) => {
      store.writeArtifact({
        id: `mcp-output:${createHash("sha256").update("ticket.json").digest("hex")}`,
        runId: "output-race",
        stepId: "other",
        path: "ticket.json",
        kind: "mcp_output",
        contentType: "application/json",
        content: '{"key":"AM-26"}\n',
        metadata: { server: "fixture", tool: "get_issue" }
      });
      return writeArtifactsAtomically(inputs);
    };

    const result = await executeAgentFlowCommandPipeline(store, "output-race", workflow, undefined, undefined, calls);

    expect(result).toMatchObject({ status: "paused", failedStep: "fetch" });
    expect(result.message).toContain("changed ownership");
    expect(store.getArtifact("output-race", "ticket.json")?.producerStepId).toBe("other");
    expect(store.getArtifact("output-race", "mcp-calls/fetch-e7d3799ecc09.json")).toBeNull();
    store.close();
  });

  test("serializes MCP output object keys with locale-independent ordering", async () => {
    const root = temporaryRepo();
    const workflow = mcpWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "stable-json", workflow, inputs: { ticket_key: "AM-26" } });
    const calls = createAgentFlowMcpCallRegistry().register("fixture", () => ({
      outputs: { "ticket.json": { "ä": 1, z: 2, a: 3 } }
    }));

    await executeAgentFlowCommandPipeline(store, "stable-json", workflow, undefined, undefined, calls);

    expect(store.readArtifact("stable-json", "ticket.json").content.toString()).toBe('{"a":3,"z":2,"ä":1}\n');
    store.close();
  });

  test("prevents a stale overlapping MCP call from overwriting newer output", async () => {
    const root = temporaryRepo();
    const workflow = mcpWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "overlapping-call", workflow, inputs: { ticket_key: "AM-26" } });
    store.transitionRunWithEvent("overlapping-call", {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: {} }
    });
    const seedCalls = createAgentFlowMcpCallRegistry().register("fixture", () => ({
      outputs: { "ticket.json": { version: "initial" } }
    }));
    await executeAgentFlowMcpCall(store, "overlapping-call", workflow, workflow.steps[0]!, seedCalls);
    let invocation = 0;
    let releaseOlder!: () => void;
    let olderStarted!: () => void;
    const didStartOlder = new Promise<void>((resolve) => { olderStarted = resolve; });
    const calls = createAgentFlowMcpCallRegistry().register("fixture", () => {
      invocation += 1;
      if (invocation === 1) {
        olderStarted();
        return new Promise((resolve) => {
          releaseOlder = () => resolve({ outputs: { "ticket.json": { version: "older" } } });
        });
      }
      return { outputs: { "ticket.json": { version: "newer" } } };
    });

    const older = executeAgentFlowMcpCall(store, "overlapping-call", workflow, workflow.steps[0]!, calls);
    await didStartOlder;
    await executeAgentFlowMcpCall(store, "overlapping-call", workflow, workflow.steps[0]!, calls);
    await executeAgentFlowMcpCall(store, "overlapping-call", workflow, workflow.steps[0]!, seedCalls);
    releaseOlder();

    await expect(older).rejects.toMatchObject({ code: "AGENT_FLOW_ARTIFACT_STALE" });
    expect(JSON.parse(store.readArtifact("overlapping-call", "ticket.json").content.toString()))
      .toEqual({ version: "initial" });
    store.close();
  });

  test("rejects backing-file changes while an MCP adapter is running", async () => {
    const root = temporaryRepo();
    const workflow = mcpWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "backing-race", workflow, inputs: { ticket_key: "AM-26" } });
    store.transitionRunWithEvent("backing-race", {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: {} }
    });
    const seedCalls = createAgentFlowMcpCallRegistry().register("fixture", () => ({
      outputs: { "ticket.json": { version: "initial" } }
    }));
    await executeAgentFlowMcpCall(store, "backing-race", workflow, workflow.steps[0]!, seedCalls);
    const output = store.getArtifact("backing-race", "ticket.json")!;
    const calls = createAgentFlowMcpCallRegistry().register("fixture", () => {
      fs.writeFileSync(path.join(root, output.storagePath), '{"version":"foreign"}\n');
      return { outputs: { "ticket.json": { version: "adapter" } } };
    });

    await expect(executeAgentFlowMcpCall(store, "backing-race", workflow, workflow.steps[0]!, calls))
      .rejects.toMatchObject({ code: "AGENT_FLOW_ARTIFACT_STALE" });
    expect(fs.readFileSync(path.join(root, output.storagePath), "utf8")).toBe('{"version":"foreign"}\n');
    store.close();
  });

  test("treats content-type changes as stale artifact versions", async () => {
    const root = temporaryRepo();
    const workflow = mcpWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "content-type-version", workflow, inputs: { ticket_key: "AM-26" } });
    const initial = store.writeArtifact({
      id: "versioned",
      runId: "content-type-version",
      stepId: "fetch",
      path: "ticket.json",
      kind: "mcp_output",
      contentType: "application/initial",
      content: "same",
      metadata: { server: "fixture", tool: "get_issue" }
    });
    const backing = store.getArtifactBackingSnapshot("content-type-version", "ticket.json");
    store.writeArtifact({
      id: initial.id,
      runId: "content-type-version",
      stepId: "fetch",
      path: "ticket.json",
      kind: "mcp_output",
      contentType: "application/newer",
      content: "same",
      overwrite: true,
      metadata: initial.metadata
    });

    expect(() => store.writeArtifact({
      id: initial.id,
      runId: "content-type-version",
      stepId: "fetch",
      path: "ticket.json",
      kind: "mcp_output",
      contentType: "application/older",
      content: "same",
      overwrite: true,
      requiredCurrentArtifact: {
        artifact: {
          id: initial.id,
          producerStepId: initial.producerStepId,
          kind: initial.kind,
          contentType: initial.contentType,
          checksum: initial.checksum,
          generation: initial.generation,
          metadata: initial.metadata
        },
        backingExists: backing.exists,
        backingChecksum: backing.checksum
      },
      metadata: initial.metadata
    })).toThrow("changed ownership");
    expect(store.getArtifact("content-type-version", "ticket.json")?.contentType).toBe("application/newer");
    store.close();
  });

  test("rejects oversized resolved arguments before invoking an adapter", async () => {
    const root = temporaryRepo();
    const workflow = mcpWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, {
      id: "large-arguments",
      workflow,
      inputs: { ticket_key: "x".repeat(MAX_AGENT_FLOW_MCP_ARGUMENT_BYTES + 1) }
    });
    let invoked = false;
    const calls = createAgentFlowMcpCallRegistry().register("fixture", () => {
      invoked = true;
      return { outputs: { "ticket.json": {} } };
    });

    const result = await executeAgentFlowCommandPipeline(store, "large-arguments", workflow, undefined, undefined, calls);

    expect(result).toMatchObject({ status: "paused", failedStep: "fetch" });
    expect(result.message).toContain("arguments exceed");
    expect(invoked).toBe(false);
    store.close();
  });

  test("retries adapter failures with the declared failure policy", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: retry-mcp
version: 1
style: pipeline
maturity: experimental
inputs: { ticket_key: { required: true } }
steps:
  - id: fetch
    type: mcp_call
    server: fixture
    tool: get_issue
    arguments: { key: "{{ inputs.ticket_key }}" }
    outputs: [ticket.json]
    on_failure: { retry: 1, then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "retry", workflow, inputs: { ticket_key: "AM-26" } });
    let attempts = 0;
    const calls = createAgentFlowMcpCallRegistry().register("fixture", () => {
      attempts += 1;
      if (attempts === 1) throw new Error("temporary failure");
      return { outputs: { "ticket.json": { key: "AM-26" } } };
    });

    const result = await executeAgentFlowCommandPipeline(store, "retry", workflow, undefined, undefined, calls);

    expect(result).toMatchObject({ status: "completed", completedSteps: ["fetch"] });
    expect(attempts).toBe(2);
    expect(store.listEvents("retry").filter((event) => event.type === "step.failed")).toHaveLength(1);
    expect(store.listFailures("retry")).toMatchObject([{
      stepId: "fetch",
      retryable: true,
      payload: { attempt: 1, outcome: "retry" }
    }]);
    store.close();
  });

  test("does not retry deterministic MCP contract failures", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: deterministic-mcp-failure
version: 1
style: pipeline
maturity: experimental
steps:
  - id: fetch
    type: mcp_call
    server: missing
    tool: get_issue
    arguments: {}
    outputs: [ticket.json]
    on_failure: { retry: 3, then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "deterministic-failure", workflow });
    const retryableValues: boolean[] = [];
    const recordFailure = store.recordFailure.bind(store);
    store.recordFailure = (input) => {
      retryableValues.push(input.retryable);
      recordFailure(input);
    };

    const result = await executeAgentFlowCommandPipeline(store, "deterministic-failure", workflow);

    expect(result).toMatchObject({ status: "paused", failedStep: "fetch", failureOutcome: "pause" });
    expect(result.message).toContain("No adapter is registered for MCP server missing");
    expect(store.listEvents("deterministic-failure").filter((event) => event.type === "step.failed"))
      .toHaveLength(1);
    expect(retryableValues).toEqual([false]);
    expect(store.listFailures("deterministic-failure")).toMatchObject([{
      stepId: "fetch",
      retryable: false,
      payload: { attempt: 1, outcome: "pause" }
    }]);
    store.close();
  });

  test("aborts an in-flight adapter and publishes nothing after the run is paused", async () => {
    const root = temporaryRepo();
    const workflow = mcpWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "paused-call", workflow, inputs: { ticket_key: "AM-26" } });
    let request: AgentFlowMcpCallRequest | undefined;
    let started!: () => void;
    const didStart = new Promise<void>((resolve) => { started = resolve; });
    const calls = createAgentFlowMcpCallRegistry().register("fixture", (value) => {
      request = value;
      started();
      return new Promise(() => undefined);
    });

    const execution = executeAgentFlowCommandPipeline(store, "paused-call", workflow, undefined, undefined, calls);
    await didStart;
    transitionAgentFlowLifecycleRun(store, "paused-call", "pause");
    const result = await execution;

    expect(result).toMatchObject({ status: "paused" });
    expect(request?.signal.aborted).toBe(true);
    expect(store.listArtifacts("paused-call")).toEqual([]);
    store.close();
  });

  test("returns the captured interruption result if the run resumes during adapter abort", async () => {
    const root = temporaryRepo();
    const workflow = mcpWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "resumed-call", workflow, inputs: { ticket_key: "AM-26" } });
    let started!: () => void;
    const didStart = new Promise<void>((resolve) => { started = resolve; });
    const calls = createAgentFlowMcpCallRegistry().register("fixture", (request) => {
      request.signal.addEventListener("abort", () => {
        transitionAgentFlowLifecycleRun(store, "resumed-call", "resume");
      });
      started();
      return new Promise(() => undefined);
    });

    const execution = executeAgentFlowCommandPipeline(store, "resumed-call", workflow, undefined, undefined, calls);
    await didStart;
    transitionAgentFlowLifecycleRun(store, "resumed-call", "pause");
    const result = await execution;

    expect(result).toMatchObject({ status: "paused", completedSteps: [] });
    expect(store.getRun("resumed-call")?.status).toBe("running");
    expect(store.listEvents("resumed-call").map((event) => event.type)).toContain("step.interrupted");
    store.close();
  });

  test("records interruption when cancellation lands immediately after atomic publication", async () => {
    const root = temporaryRepo();
    const workflow = mcpWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "cancel-after-publish", workflow, inputs: { ticket_key: "AM-26" } });
    const calls = createAgentFlowMcpCallRegistry().register("fixture", () => ({ outputs: { "ticket.json": {} } }));
    const writeArtifactsAtomically = store.writeArtifactsAtomically.bind(store);
    store.writeArtifactsAtomically = (inputs) => {
      const artifacts = writeArtifactsAtomically(inputs);
      transitionAgentFlowLifecycleRun(store, "cancel-after-publish", "cancel");
      return artifacts;
    };

    const result = await executeAgentFlowCommandPipeline(store, "cancel-after-publish", workflow, undefined, undefined, calls);

    expect(result).toMatchObject({ status: "cancelled", completedSteps: [] });
    expect(store.getArtifact("cancel-after-publish", "ticket.json")).not.toBeNull();
    expect(store.listEvents("cancel-after-publish").map((event) => event.type)).toContain("step.interrupted");
    expect(store.listEvents("cancel-after-publish").map((event) => event.type)).not.toContain("step.completed");
    store.close();
  });

  test("records interruption when cancellation races with an atomic publication error", async () => {
    const root = temporaryRepo();
    const workflow = mcpWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "cancel-with-error", workflow, inputs: { ticket_key: "AM-26" } });
    const calls = createAgentFlowMcpCallRegistry().register("fixture", () => ({ outputs: { "ticket.json": {} } }));
    store.writeArtifactsAtomically = () => {
      transitionAgentFlowLifecycleRun(store, "cancel-with-error", "cancel");
      throw new Error("publication raced with cancellation");
    };

    const result = await executeAgentFlowCommandPipeline(store, "cancel-with-error", workflow, undefined, undefined, calls);

    expect(result).toMatchObject({ status: "cancelled", completedSteps: [] });
    expect(store.listEvents("cancel-with-error").map((event) => event.type)).toContain("step.interrupted");
    expect(store.listEvents("cancel-with-error").find((event) => event.type === "step.interrupted")?.payload)
      .toMatchObject({ status: "cancelled" });
    store.close();
  });

  test("rejects oversized adapter metadata before atomic publication", async () => {
    const root = temporaryRepo();
    const workflow = mcpWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "oversized-metadata", workflow, inputs: { ticket_key: "AM-26" } });
    const calls = createAgentFlowMcpCallRegistry().register("fixture", () => ({
      outputs: { "ticket.json": { key: "AM-26" } },
      metadata: { value: "x".repeat(MAX_AGENT_FLOW_MCP_METADATA_BYTES + 1) }
    }));

    const result = await executeAgentFlowCommandPipeline(store, "oversized-metadata", workflow, undefined, undefined, calls);

    expect(result).toMatchObject({ status: "paused", failedStep: "fetch" });
    expect(result.message).toContain("metadata");
    expect(store.listArtifacts("oversized-metadata").filter((artifact) => artifact.kind !== "failure_payload")).toEqual([]);
    store.close();
  });

  test("rejects oversized string and binary outputs", async () => {
    for (const output of [
      "x".repeat(MAX_AGENT_FLOW_MCP_OUTPUT_BYTES + 1),
      new Uint8Array(MAX_AGENT_FLOW_MCP_OUTPUT_BYTES + 1)
    ]) {
      const root = temporaryRepo();
      const workflow = mcpWorkflow();
      const store = await openAgentFlowRunState({ cwd: root });
      createAgentFlowLifecycleRun(store, { id: "oversized-output", workflow, inputs: { ticket_key: "AM-26" } });
      const calls = createAgentFlowMcpCallRegistry().register("fixture", () => ({
        outputs: { "ticket.json": output }
      }));

      const result = await executeAgentFlowCommandPipeline(store, "oversized-output", workflow, undefined, undefined, calls);

      expect(result).toMatchObject({ status: "paused", failedStep: "fetch" });
      expect(result.message).toContain("outputs for step fetch exceed");
      expect(store.listArtifacts("oversized-output").filter((artifact) => artifact.kind !== "failure_payload")).toEqual([]);
      store.close();
    }
  });

  test("rejects oversized adapter content types before publication", async () => {
    const root = temporaryRepo();
    const workflow = mcpWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "oversized-content-type", workflow, inputs: { ticket_key: "AM-26" } });
    const calls = createAgentFlowMcpCallRegistry().register("fixture", () => ({
      outputs: { "ticket.json": {} },
      contentTypes: { "ticket.json": `application/x-${"x".repeat(MAX_AGENT_FLOW_MCP_CONTENT_TYPE_BYTES)}` }
    }));

    const result = await executeAgentFlowCommandPipeline(
      store,
      "oversized-content-type",
      workflow,
      undefined,
      undefined,
      calls
    );

    expect(result).toMatchObject({ status: "paused", failedStep: "fetch" });
    expect(result.message).toContain("content types");
    expect(store.listArtifacts("oversized-content-type").filter((artifact) => artifact.kind !== "failure_payload")).toEqual([]);
    store.close();
  });

  test("supports prototype-named outputs without reading inherited content types", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: prototype-output
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: fetch, type: mcp_call, server: fixture, tool: get, arguments: {}, outputs: [constructor] }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "prototype-output", workflow });
    const outputs = Object.create(null) as Record<string, unknown>;
    outputs.constructor = { ok: true };
    const calls = createAgentFlowMcpCallRegistry().register("fixture", () => ({ outputs, contentTypes: {} }) as never);

    const result = await executeAgentFlowCommandPipeline(store, "prototype-output", workflow, undefined, undefined, calls);

    expect(result.status).toBe("completed");
    expect(JSON.parse(store.readArtifact("prototype-output", "constructor").content.toString())).toEqual({ ok: true });
    store.close();
  });

  test("rejects non-plain adapter response mappings", async () => {
    for (const response of [
      { outputs: new Map([["ticket.json", {}]]) },
      { outputs: { "ticket.json": {} }, contentTypes: new Map([["ticket.json", "application/json"]]) }
    ]) {
      const root = temporaryRepo();
      const workflow = mcpWorkflow();
      const store = await openAgentFlowRunState({ cwd: root });
      createAgentFlowLifecycleRun(store, { id: "non-plain-response", workflow, inputs: { ticket_key: "AM-26" } });
      const calls = createAgentFlowMcpCallRegistry().register("fixture", () => response as never);

      const result = await executeAgentFlowCommandPipeline(store, "non-plain-response", workflow, undefined, undefined, calls);

      expect(result).toMatchObject({ status: "paused", failedStep: "fetch" });
      expect(store.listArtifacts("non-plain-response").filter((artifact) => artifact.kind !== "failure_payload")).toEqual([]);
      store.close();
    }
  });

  test("rejects non-JSON output and metadata values instead of coercing them", async () => {
    for (const response of [
      { outputs: { "ticket.json": { invalid: Number.NaN } } },
      { outputs: { "ticket.json": { key: "AM-26" } }, metadata: { invalid: new Date() } }
    ]) {
      const root = temporaryRepo();
      const workflow = mcpWorkflow();
      const store = await openAgentFlowRunState({ cwd: root });
      createAgentFlowLifecycleRun(store, { id: "invalid-json", workflow, inputs: { ticket_key: "AM-26" } });
      const calls = createAgentFlowMcpCallRegistry().register("fixture", () => response as never);

      const result = await executeAgentFlowCommandPipeline(store, "invalid-json", workflow, undefined, undefined, calls);

      expect(result).toMatchObject({ status: "paused", failedStep: "fetch" });
      expect(store.listArtifacts("invalid-json").filter((artifact) => artifact.kind !== "failure_payload")).toEqual([]);
      store.close();
    }
  });
});

function mcpWorkflow() {
  return parseAgentFlowWorkflowOrThrow(`name: fixture-mcp
version: 1
style: pipeline
maturity: experimental
inputs:
  ticket_key: { required: true }
steps:
  - id: fetch
    type: mcp_call
    server: fixture
    tool: get_issue
    arguments:
      key: "{{ inputs.ticket_key }}"
    outputs: [ticket.json]
    on_failure: { then: pause }
`);
}

function temporaryRepo(): string {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "agent-flow-mcp-call-"));
  fs.mkdirSync(path.join(root, ".git"));
  return root;
}
