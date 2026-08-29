import { describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  createAgentFlowFixtureMcpAdapter,
  createAgentFlowLifecycleRun,
  createAgentFlowMcpCallRegistry,
  createAgentFlowSessionProviderRegistry,
  AgentFlowMcpCallError,
  AgentFlowRunStateError,
  AgentFlowSessionRequestError,
  executeAgentFlowCommandPipeline,
  executeAgentFlowMcpCall,
  MAX_AGENT_FLOW_MCP_ARGUMENT_BYTES,
  MAX_AGENT_FLOW_MCP_CONTENT_TYPE_BYTES,
  MAX_AGENT_FLOW_MCP_METADATA_BYTES,
  MAX_AGENT_FLOW_MCP_OUTPUT_BYTES,
  openAgentFlowRunState,
  parseAgentFlowSimulationFixture,
  parseAgentFlowWorkflowOrThrow,
  resumeAgentFlowCommandPipeline,
  simulateAgentFlowWorkflow,
  transitionAgentFlowLifecycleRun,
  validateAgentFlowWorkflow,
  type AgentFlowMcpCallRequest
} from "../../src/runtime";
import { resolveAgentFlowMcpArguments } from "../../src/runtime/mcp_call";
import { AgentFlowSessionPolicyError } from "../../src/runtime/session_request";

const repoRoot = path.resolve(".");
const examplePath = path.join(repoRoot, "examples/workflows/jira-ticket-spec.yml");
const fixturePath = path.join(repoRoot, "tests/fixtures/agent-flow/simulation/jira-ticket.json");

function completedCodexMcpCall(
  server: string,
  tool: string,
  arguments_: Record<string, unknown>,
  result: string
) {
  return {
    server,
    tool,
    arguments: arguments_,
    status: "completed",
    resultHash: `sha256:${createHash("sha256").update(result, "utf8").digest("hex")}`
  };
}

describe("Agent Flow MCP call steps", () => {
  test("routes a Codex-mediated call through a named resumable session and requires matching JSONL evidence", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: codex-mcp
version: 1
style: pipeline
maturity: experimental
sessions:
  agent: { provider: codex, resume: true }
limits:
  max_frontier_calls: 1
steps:
  - id: fetch
    type: mcp_call
    via: codex
    session: agent
    server: atlassian
    tool: get_issue
    arguments: { key: AF-1 }
    outputs: [ticket.json]
`);
    const prompts: string[] = [];
    const providers = createAgentFlowSessionProviderRegistry();
    providers.register("codex", async (request) => {
      prompts.push(request.prompt.content);
      request.reportExternalSessionId?.("codex-thread-1");
      return {
        externalSessionId: "codex-thread-1",
        outputs: { "ticket.json": '{"key":"AF-1"}\n' },
        metadata: { mcpCalls: [completedCodexMcpCall("atlassian", "get_issue", { key: "AF-1" }, '{"key":"AF-1"}\n')] }
      };
    });
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "codex-mcp", workflow });
    expect(await executeAgentFlowCommandPipeline(
      store, "codex-mcp", workflow, undefined, providers
    )).toMatchObject({ status: "completed" });
    expect(prompts[0]).toContain('server "atlassian" tool "get_issue" exactly once');
    expect(store.getSession("codex-mcp", "agent")).toMatchObject({
      status: "waiting", externalSessionId: "codex-thread-1"
    });
    expect(store.readArtifact("codex-mcp", "ticket.json").content.toString("utf8"))
      .toBe('{"key":"AF-1"}\n');
    store.close();
  });

  test("routes a Codex-mediated call through a registered Codex profile", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: codex-profile-mcp
version: 1
style: pipeline
maturity: experimental
sessions:
  agent: { provider: "codex:reviewer", resume: true }
limits:
  max_frontier_calls: 1
steps:
  - { id: fetch, type: mcp_call, via: codex, session: agent, server: jira, tool: get, arguments: { key: AF-1 }, outputs: [ticket.json] }
`);
    const providers = createAgentFlowSessionProviderRegistry([{
      kind: "codex_profile",
      profile: "reviewer",
      enabled: true,
      adapter: async (request) => {
        expect(request).toMatchObject({
          providerKind: "codex_profile",
          providerProfile: "reviewer"
        });
        request.reportExternalSessionId?.("profile-thread");
        return {
          externalSessionId: "profile-thread",
          outputs: {
            "ticket.json": {
              content: '{"key":"AF-1"}\n',
              contentType: "application/vnd.agent-flow.ticket+json"
            }
          },
          metadata: {
            mcpCalls: [completedCodexMcpCall("jira", "get", { key: "AF-1" }, '{"key":"AF-1"}\n')]
          }
        };
      }
    }]);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "codex-profile-mcp", workflow });

    expect(await executeAgentFlowCommandPipeline(
      store, "codex-profile-mcp", workflow, undefined, providers
    )).toMatchObject({ status: "completed" });
    expect(store.getSession("codex-profile-mcp", "agent")).toMatchObject({
      status: "waiting", externalSessionId: "profile-thread"
    });
    expect(store.getArtifact("codex-profile-mcp", "ticket.json")?.contentType)
      .toBe("application/vnd.agent-flow.ticket+json");
    store.close();
  });

  test("preserves configured Codex provider descriptors for mediated calls", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: configured-codex-mcp
version: 1
style: pipeline
maturity: experimental
sessions:
  agent: { provider: codex-target, resume: true }
limits: { max_frontier_calls: 1 }
steps:
  - { id: fetch, type: mcp_call, via: codex, session: agent, server: jira, tool: get, arguments: {}, outputs: [ticket.json] }
`);
    const model = "gpt-test";
    const providers = createAgentFlowSessionProviderRegistry().registerConfigured({
      name: "codex-target",
      kind: "frontier",
      target: "codex-host",
      driver: "codex-cli",
      model,
      profile: "reviewer",
      reasoningEffort: "high",
      fingerprint: "configured-fingerprint"
    }, async (request) => {
      expect(request).toMatchObject({
        providerKind: "frontier",
        providerProfile: "reviewer",
        providerReasoningEffort: "high",
        providerTarget: "codex-host",
        providerDriver: "codex-cli",
        providerModel: `sha256:${createHash("sha256").update(model).digest("hex")}`,
        providerFingerprint: "configured-fingerprint"
      });
      request.reportExternalSessionId?.("configured-thread");
      return {
        externalSessionId: "configured-thread",
        outputs: { "ticket.json": "{}\n" },
        metadata: { mcpCalls: [completedCodexMcpCall("jira", "get", {}, "{}\n")] }
      };
    });
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "configured-codex-mcp", workflow });

    expect(await executeAgentFlowCommandPipeline(
      store, "configured-codex-mcp", workflow, undefined, providers
    )).toMatchObject({ status: "completed" });
    expect(store.getRun("configured-codex-mcp")?.context.providerBindings)
      .toHaveProperty("codex-target");
    store.close();
  });

  test("cancels a Codex MCP session when its in-flight provider is aborted", async () => {
    const root = temporaryRepo();
    const workflow = codexMcpWorkflow("codex-mcp-abort");
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "codex-mcp-abort", workflow });
    let started!: () => void;
    const didStart = new Promise<void>((resolve) => { started = resolve; });
    let aborted!: () => void;
    const didAbort = new Promise<void>((resolve) => { aborted = resolve; });
    let release!: () => void;
    const maySettle = new Promise<void>((resolve) => { release = resolve; });
    let providerSettled = false;
    const provider = Object.assign((request: Parameters<NonNullable<ReturnType<typeof createAgentFlowSessionProviderRegistry>["get"]>>[0]) => {
      request.reportExternalSessionId?.("abort-thread");
      started();
      return new Promise((_resolve, reject) => {
        request.signal.addEventListener("abort", () => {
          aborted();
          void maySettle.then(() => {
            providerSettled = true;
            reject(request.signal.reason);
          });
        }, { once: true });
      });
    }, { waitForAbort: true });
    const providers = createAgentFlowSessionProviderRegistry().register("codex", provider);

    const execution = executeAgentFlowCommandPipeline(
      store, "codex-mcp-abort", workflow, undefined, providers
    );
    await didStart;
    transitionAgentFlowLifecycleRun(store, "codex-mcp-abort", "cancel");
    await didAbort;
    expect(await Promise.race([
      execution.then(() => "settled"),
      new Promise<string>((resolve) => setTimeout(() => resolve("pending"), 50))
    ])).toBe("pending");
    release();

    await expect(execution).resolves.toMatchObject({ status: "cancelled", completedSteps: [] });
    expect(providerSettled).toBe(true);
    expect(store.getSession("codex-mcp-abort", "agent")).toMatchObject({
      status: "cancelled",
      externalSessionId: "abort-thread",
      state: { interrupted: "cancelled" }
    });
    store.close();
  });

  test("cancels a Codex MCP session when cancellation races with its provider response", async () => {
    const root = temporaryRepo();
    const workflow = codexMcpWorkflow("codex-mcp-return-race");
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "codex-mcp-return-race", workflow });
    const providers = createAgentFlowSessionProviderRegistry().register("codex", (request) => {
      request.reportExternalSessionId?.("return-thread");
      transitionAgentFlowLifecycleRun(store, "codex-mcp-return-race", "cancel");
      return {
        externalSessionId: "return-thread",
        outputs: { "ticket.json": "{}\n" },
        metadata: { mcpCalls: [completedCodexMcpCall("jira", "get", {}, "{}\n")] }
      };
    });

    await expect(executeAgentFlowCommandPipeline(
      store, "codex-mcp-return-race", workflow, undefined, providers
    )).resolves.toMatchObject({ status: "cancelled", completedSteps: [] });
    expect(store.getSession("codex-mcp-return-race", "agent")).toMatchObject({
      status: "cancelled",
      externalSessionId: "return-thread",
      state: { interrupted: "cancelled" }
    });
    expect(store.getArtifact("codex-mcp-return-race", "ticket.json")).toBeNull();
    store.close();
  });

  test("rejects secret-like Codex MCP session IDs before persistence", async () => {
    for (const source of ["reported", "returned"] as const) {
      const root = temporaryRepo();
      const runId = `codex-mcp-sensitive-session-${source}`;
      const workflow = parseAgentFlowWorkflowOrThrow(`name: ${runId}
version: 1
style: pipeline
maturity: experimental
sessions:
  agent: { provider: codex, resume: true }
limits:
  max_frontier_calls: 1
steps:
  - { id: fetch, type: mcp_call, via: codex, session: agent, server: jira, tool: get, arguments: {}, outputs: [ticket.json] }
`);
      const providers = createAgentFlowSessionProviderRegistry().register("codex", async (request) => {
        if (source === "reported") {
          request.reportExternalSessionId?.("Authorization: Bearer codex-mcp-session-secret");
        }
        return {
          ...(source === "returned"
            ? { externalSessionId: "Authorization: Bearer codex-mcp-session-secret" }
            : {}),
          outputs: { "ticket.json": "{}\n" },
          metadata: { mcpCalls: [completedCodexMcpCall("jira", "get", {}, "{}\n")] }
        };
      });
      const store = await openAgentFlowRunState({ cwd: root });
      createAgentFlowLifecycleRun(store, { id: runId, workflow });

      expect(await executeAgentFlowCommandPipeline(
        store, runId, workflow, undefined, providers
      )).toMatchObject({ status: "paused", failedStep: "fetch" });
      expect(store.getSession(runId, "agent")?.externalSessionId).toBeNull();
      expect(JSON.stringify(store.getSession(runId, "agent")))
        .not.toContain("codex-mcp-session-secret");
      expect(JSON.stringify(store.listEvents(runId)))
        .not.toContain("codex-mcp-session-secret");
      store.close();
    }
  });

  test("rejects Codex-mediated outputs without matching completed MCP evidence", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: codex-mcp-no-evidence
version: 1
style: pipeline
maturity: experimental
sessions:
  agent: { provider: codex, resume: true }
limits:
  max_frontier_calls: 1
steps:
  - id: fetch
    type: mcp_call
    via: codex
    session: agent
    server: atlassian
    tool: get_issue
    arguments: { key: AF-1 }
    outputs: [ticket.json]
`);
    const providers = createAgentFlowSessionProviderRegistry();
    providers.register("codex", async (request) => {
      request.reportExternalSessionId?.("codex-thread-1");
      return {
        externalSessionId: "codex-thread-1",
        outputs: { "ticket.json": '{"key":"AF-1"}\n' }
      };
    });
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "codex-mcp-no-evidence", workflow });

    expect(await executeAgentFlowCommandPipeline(
      store, "codex-mcp-no-evidence", workflow, undefined, providers
    )).toMatchObject({ status: "paused", failedStep: "fetch" });
    expect(store.getSession("codex-mcp-no-evidence", "agent")?.status).toBe("failed");
    expect(store.getArtifact("codex-mcp-no-evidence", "ticket.json")).toBeNull();
    store.close();
  });

  test("rejects Codex-mediated output that does not match the completed tool result", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: codex-mcp-result-mismatch
version: 1
style: pipeline
maturity: experimental
sessions:
  agent: { provider: codex, resume: true }
limits: { max_frontier_calls: 1 }
steps:
  - { id: fetch, type: mcp_call, via: codex, session: agent, server: jira, tool: get, arguments: { key: AF-1 }, outputs: [ticket.json] }
`);
    const providers = createAgentFlowSessionProviderRegistry().register("codex", async (request) => {
      request.reportExternalSessionId?.("mismatch-thread");
      return {
        externalSessionId: "mismatch-thread",
        outputs: { "ticket.json": '{"key":"FABRICATED"}\n' },
        metadata: {
          mcpCalls: [completedCodexMcpCall("jira", "get", { key: "AF-1" }, '{"key":"AF-1"}\n')]
        }
      };
    });
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "codex-mcp-result-mismatch", workflow });

    expect(await executeAgentFlowCommandPipeline(
      store, "codex-mcp-result-mismatch", workflow, undefined, providers
    )).toMatchObject({ status: "paused", failedStep: "fetch" });
    expect(store.getSession("codex-mcp-result-mismatch", "agent")).toMatchObject({
      status: "failed",
      state: { error: "mcp_result_mismatch" }
    });
    expect(store.getArtifact("codex-mcp-result-mismatch", "ticket.json")).toBeNull();
    store.close();
  });

  test("leaves a Codex MCP session reclaimable when its output contract is malformed", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: codex-mcp-invalid-output
version: 1
style: pipeline
maturity: experimental
sessions:
  agent: { provider: codex, resume: true }
limits: { max_frontier_calls: 1 }
steps:
  - { id: fetch, type: mcp_call, via: codex, session: agent, server: jira, tool: get, arguments: {}, outputs: [ticket.json] }
`);
    const providers = createAgentFlowSessionProviderRegistry().register("codex", async (request) => {
      request.reportExternalSessionId?.("invalid-output-thread");
      return {
        externalSessionId: "invalid-output-thread",
        outputs: { "unexpected.json": "{}\n" },
        metadata: { mcpCalls: [completedCodexMcpCall("jira", "get", {}, "{}\n")] }
      };
    });
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "codex-mcp-invalid-output", workflow });

    expect(await executeAgentFlowCommandPipeline(
      store, "codex-mcp-invalid-output", workflow, undefined, providers
    )).toMatchObject({ status: "paused", failedStep: "fetch" });
    expect(store.getSession("codex-mcp-invalid-output", "agent")).toMatchObject({
      status: "paused", externalSessionId: "invalid-output-thread"
    });
    expect(store.getArtifact("codex-mcp-invalid-output", "ticket.json")).toBeNull();
    store.close();
  });

  test("reclaims a Codex MCP session after a retryable response failure", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: codex-mcp-response-retry
version: 1
style: pipeline
maturity: experimental
sessions:
  agent: { provider: codex, resume: true }
limits: { max_frontier_calls: 2 }
steps:
  - id: fetch
    type: mcp_call
    via: codex
    session: agent
    server: jira
    tool: get
    arguments: { key: AF-1 }
    outputs: [ticket.json]
    on_failure: { retry: 1 }
`);
    let calls = 0;
    const providers = createAgentFlowSessionProviderRegistry().register("codex", async (request) => {
      calls += 1;
      request.reportExternalSessionId?.("retry-thread");
      return {
        externalSessionId: "retry-thread",
        outputs: calls === 1 ? {} : { "ticket.json": '{"key":"AF-1"}\n' },
        metadata: {
          mcpCalls: [completedCodexMcpCall("jira", "get", { key: "AF-1" }, '{"key":"AF-1"}\n')]
        }
      };
    });
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "codex-mcp-response-retry", workflow });

    expect(await executeAgentFlowCommandPipeline(
      store, "codex-mcp-response-retry", workflow, undefined, providers
    )).toMatchObject({ status: "completed" });
    expect(calls).toBe(2);
    expect(store.getSession("codex-mcp-response-retry", "agent")).toMatchObject({
      status: "waiting", externalSessionId: "retry-thread"
    });
    expect(store.readArtifact("codex-mcp-response-retry", "ticket.json").content.toString("utf8"))
      .toBe('{"key":"AF-1"}\n');
    store.close();
  });

  test("rejects wrong arguments, duplicate calls, and additional Codex MCP operations", async () => {
    const result = '{"key":"AF-1"}\n';
    const evidenceCases = [
      [completedCodexMcpCall("atlassian", "get_issue", { key: "AF-2" }, result)],
      [
        completedCodexMcpCall("atlassian", "get_issue", { key: "AF-1" }, result),
        completedCodexMcpCall("atlassian", "get_issue", { key: "AF-1" }, result)
      ],
      [
        completedCodexMcpCall("atlassian", "search", { query: "AF-1" }, result),
        completedCodexMcpCall("atlassian", "get_issue", { key: "AF-1" }, result)
      ]
    ];
    for (const [index, mcpCalls] of evidenceCases.entries()) {
      const root = temporaryRepo();
      const runId = `codex-mcp-contract-${index}`;
      const workflow = parseAgentFlowWorkflowOrThrow(`name: ${runId}
version: 1
style: pipeline
maturity: experimental
sessions:
  agent: { provider: codex, resume: true }
limits:
  max_frontier_calls: 1
steps:
  - id: fetch
    type: mcp_call
    via: codex
    session: agent
    server: atlassian
    tool: get_issue
    arguments: { key: AF-1 }
    outputs: [ticket.json]
`);
      const providers = createAgentFlowSessionProviderRegistry().register("codex", async (request) => {
        request.reportExternalSessionId?.(`thread-${index}`);
        return {
          externalSessionId: `thread-${index}`,
          outputs: { "ticket.json": result },
          metadata: { mcpCalls }
        };
      });
      const store = await openAgentFlowRunState({ cwd: root });
      createAgentFlowLifecycleRun(store, { id: runId, workflow });

      expect(await executeAgentFlowCommandPipeline(
        store, runId, workflow, undefined, providers
      )).toMatchObject({ status: "paused", failedStep: "fetch" });
      expect(store.getArtifact(runId, "ticket.json")).toBeNull();
      store.close();
    }
  });

  test("persists a Codex MCP thread as soon as the provider reports it", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: codex-mcp-thread-durability
version: 1
style: pipeline
maturity: experimental
sessions:
  agent: { provider: codex, resume: true }
limits:
  max_frontier_calls: 1
steps:
  - id: fetch
    type: mcp_call
    via: codex
    session: agent
    server: atlassian
    tool: get_issue
    arguments: { key: AF-1 }
    outputs: [ticket.json]
`);
    let releaseProvider!: () => void;
    let providerEntered!: () => void;
    const entered = new Promise<void>((resolve) => { providerEntered = resolve; });
    const release = new Promise<void>((resolve) => { releaseProvider = resolve; });
    const providers = createAgentFlowSessionProviderRegistry().register("codex", async (request) => {
      request.reportExternalSessionId?.("durable-thread");
      providerEntered();
      await release;
      return {
        externalSessionId: "durable-thread",
        outputs: { "ticket.json": '{"key":"AF-1"}\n' },
        metadata: {
          mcpCalls: [completedCodexMcpCall("atlassian", "get_issue", { key: "AF-1" }, '{"key":"AF-1"}\n')]
        }
      };
    });
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "codex-mcp-thread-durability", workflow });
    const execution = executeAgentFlowCommandPipeline(
      store, "codex-mcp-thread-durability", workflow, undefined, providers
    );
    await entered;

    expect(store.getSession("codex-mcp-thread-durability", "agent")).toMatchObject({
      status: "running",
      externalSessionId: "durable-thread"
    });
    releaseProvider();
    expect(await execution).toMatchObject({ status: "completed" });
    store.close();
  });

  test("preserves model-limit policy and simulation budgets for Codex-mediated MCP calls", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: codex-mcp-budget
version: 1
style: recovery_pipeline
maturity: experimental
policies: { recovery_limits: fail }
sessions:
  agent: { provider: codex, resume: true }
limits:
  max_frontier_calls: 1
steps:
  - { id: first, type: mcp_call, via: codex, session: agent, server: jira, tool: get, arguments: { key: AF-1 }, outputs: [first.json] }
  - { id: second, type: mcp_call, via: codex, session: agent, server: jira, tool: get, arguments: { key: AF-2 }, outputs: [second.json] }
`);
    let invocations = 0;
    const providers = createAgentFlowSessionProviderRegistry().register("codex", async (request) => {
      invocations += 1;
      request.reportExternalSessionId?.("budget-thread");
      const key = request.stepId === "first" ? "AF-1" : "AF-2";
      const output = request.outputs[0]!;
      return {
        externalSessionId: "budget-thread",
        outputs: { [output]: JSON.stringify({ key }) },
        metadata: { mcpCalls: [completedCodexMcpCall("jira", "get", { key }, JSON.stringify({ key }))] }
      };
    });
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "codex-mcp-budget", workflow });

    expect(await executeAgentFlowCommandPipeline(
      store, "codex-mcp-budget", workflow, undefined, providers
    )).toMatchObject({ status: "failed", failedStep: "second" });
    expect(invocations).toBe(1);
    expect(store.listEvents("codex-mcp-budget").map((event) => event.type))
      .toContain("recovery.limit_reached");
    store.close();

    expect(simulateAgentFlowWorkflow(workflow, {
      steps: {
        first: { outputs: { "first.json": { key: "AF-1" } } },
        second: { outputs: { "second.json": { key: "AF-2" } } }
      }
    })).toMatchObject({
      status: "failed",
      visitedSteps: [
        { id: "first", outcome: "succeeded" },
        { id: "second", outcome: "failed" }
      ]
    });

    const failedFirst = structuredClone(workflow);
    failedFirst.steps[0]!.on_failure = { then: "continue" };
    expect(simulateAgentFlowWorkflow(failedFirst, {
      steps: {
        first: { outcome: "failed" },
        second: { outputs: { "second.json": { key: "AF-2" } } }
      }
    })).toMatchObject({
      status: "failed",
      visitedSteps: [
        { id: "first", outcome: "failed" },
        { id: "second", outcome: "failed" }
      ]
    });

    const invalidFirst = structuredClone(workflow);
    invalidFirst.steps[0]!.arguments = { key: "{{ inputs.missing }}" };
    invalidFirst.steps[0]!.on_failure = { retry: 1, allowed: true, then: "continue" };
    expect(simulateAgentFlowWorkflow(invalidFirst, {
      steps: {
        first: { outcome: "failed" },
        second: { outputs: { "second.json": { key: "AF-2" } } }
      }
    })).toMatchObject({
      status: "completed",
      visitedSteps: [
        { id: "first", outcome: "failed" },
        { id: "second", outcome: "succeeded" }
      ]
    });
  });

  test("does not reserve Codex MCP budgets when the session claim conflicts", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: codex-mcp-claim-conflict
version: 1
style: pipeline
maturity: experimental
sessions:
  agent: { provider: codex, resume: true }
limits: { max_model_calls: 1, max_frontier_calls: 1 }
steps:
  - id: fetch
    type: mcp_call
    via: codex
    session: agent
    server: jira
    tool: get
    arguments: { key: AF-1 }
    outputs: [ticket.json]
    on_failure: { then: fail }
`);
    let invocations = 0;
    const providers = createAgentFlowSessionProviderRegistry().register("codex", async () => {
      invocations += 1;
      return { outputs: {} };
    });
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "codex-mcp-claim-conflict", workflow });
    store.claimSession({
      id: "agent",
      runId: "codex-mcp-claim-conflict",
      stepId: "other",
      provider: "codex",
      status: "running",
      state: {}
    });

    expect(await executeAgentFlowCommandPipeline(
      store, "codex-mcp-claim-conflict", workflow, undefined, providers
    )).toMatchObject({ status: "failed", failedStep: "fetch" });
    expect(invocations).toBe(0);
    expect(store.getBudget("codex-mcp-claim-conflict", "model:model_calls")).toBeNull();
    expect(store.getBudget("codex-mcp-claim-conflict", "model:frontier_calls")).toBeNull();
    store.close();
  });

  test("pauses Codex MCP for an unavailable thread and resumes after explicit reset", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: codex-mcp-reset
version: 1
style: pipeline
maturity: experimental
sessions:
  agent: { provider: codex, resume: true }
limits:
  max_frontier_calls: 3
steps:
  - { id: first, type: mcp_call, via: codex, session: agent, server: jira, tool: get, arguments: { key: AF-1 }, outputs: [first.json] }
  - { id: second, type: mcp_call, via: codex, session: agent, server: jira, tool: get, arguments: { key: AF-2 }, outputs: [second.json] }
`);
    const providers = createAgentFlowSessionProviderRegistry().register("codex", async (request) => {
      if (request.stepId === "second" && request.externalSessionId === "thread-1") {
        throw new AgentFlowSessionRequestError(
          "Codex thread not found",
          "AGENT_FLOW_PROVIDER_SESSION_UNAVAILABLE"
        );
      }
      const thread = request.stepId === "first" ? "thread-1" : "thread-2";
      const key = request.stepId === "first" ? "AF-1" : "AF-2";
      request.reportExternalSessionId?.(thread);
      return {
        externalSessionId: thread,
        outputs: { [request.outputs[0]!]: JSON.stringify({ key }) },
        metadata: { mcpCalls: [completedCodexMcpCall("jira", "get", { key }, JSON.stringify({ key }))] }
      };
    });
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "codex-mcp-reset", workflow });

    expect(await executeAgentFlowCommandPipeline(
      store, "codex-mcp-reset", workflow, undefined, providers
    )).toMatchObject({ status: "paused", failedStep: "second" });
    expect(store.getRun("codex-mcp-reset")?.context.waiting).toMatchObject({
      kind: "provider_session", sessionId: "agent"
    });
    expect(await resumeAgentFlowCommandPipeline(
      store, "codex-mcp-reset", workflow, { resetSession: "agent" }, undefined, providers
    )).toMatchObject({ status: "completed" });
    expect(store.getSession("codex-mcp-reset", "agent")?.externalSessionId).toBe("thread-2");
    store.close();
  });

  test("redacts Codex MCP provider failures before persisting session state", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: codex-mcp-redacted-error
version: 1
style: pipeline
maturity: experimental
sessions:
  agent: { provider: codex, resume: true }
limits: { max_frontier_calls: 1 }
steps:
  - { id: fetch, type: mcp_call, via: codex, session: agent, server: jira, tool: get, arguments: {}, outputs: [ticket.json] }
`);
    const providers = createAgentFlowSessionProviderRegistry().register("codex", async () => {
      throw new Error("Authorization: Bearer codex-mcp-secret");
    });
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "codex-mcp-redacted-error", workflow });

    expect(await executeAgentFlowCommandPipeline(
      store, "codex-mcp-redacted-error", workflow, undefined, providers
    )).toMatchObject({ status: "paused" });
    expect(JSON.stringify(store.getSession("codex-mcp-redacted-error", "agent")))
      .not.toContain("codex-mcp-secret");
    expect(JSON.stringify(store.listEvents("codex-mcp-redacted-error")))
      .not.toContain("codex-mcp-secret");
    store.close();
  });

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
    let identityReadsRemaining = 2;
    store.getRun = (runId) => {
      if (runId !== "binary-arguments" || identityReadsRemaining <= 0) return getRun(runId);
      identityReadsRemaining -= 1;
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

  test("redacts secret-like MCP arguments before adapter invocation and audit persistence", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: redacted-mcp-arguments
version: 1
style: pipeline
maturity: experimental
steps:
  - id: fetch
    type: mcp_call
    server: fixture
    tool: get_issue
    arguments:
      api_token: mcp-secret-value
      key: AF-44
      issue_key: AF-44
      project_key: AF
      sort_key: created_at
    outputs: [ticket.json]
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "redacted-mcp", workflow });
    let captured: AgentFlowMcpCallRequest | undefined;
    const calls = createAgentFlowMcpCallRegistry().register("fixture", (request) => {
      captured = request;
      return { outputs: { "ticket.json": { key: "AF-44" } } };
    });

    expect((await executeAgentFlowCommandPipeline(
      store,
      "redacted-mcp",
      workflow,
      undefined,
      undefined,
      calls
    )).status).toBe("completed");
    expect(captured?.arguments).toEqual({
      api_token: "[REDACTED]",
      key: "AF-44",
      issue_key: "AF-44",
      project_key: "AF",
      sort_key: "created_at"
    });
    const requestArtifact = store.listArtifacts("redacted-mcp")
      .find((artifact) => artifact.kind === "mcp_request")!;
    const persisted = store.readArtifact("redacted-mcp", requestArtifact.declaredPath).content.toString("utf8");
    expect(persisted).toContain('\"redacted\":true');
    expect(persisted).not.toContain("mcp-secret-value");
    store.close();
  });

  test("preserves sensitive workflow-input provenance through MCP interpolation", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: interpolated-mcp-credential
version: 1
style: pipeline
maturity: experimental
inputs: { key: { required: true } }
steps:
  - id: fetch
    type: mcp_call
    server: fixture
    tool: search
    arguments: { query: "prefix {{ inputs.key }} suffix" }
    outputs: [result.json]
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, {
      id: "interpolated-mcp-credential",
      workflow,
      inputs: { key: "hunter2abc" }
    });
    let captured: AgentFlowMcpCallRequest | undefined;
    const calls = createAgentFlowMcpCallRegistry().register("fixture", (request) => {
      captured = request;
      return { outputs: { "result.json": { ok: true } } };
    });

    expect((await executeAgentFlowCommandPipeline(
      store,
      "interpolated-mcp-credential",
      workflow,
      undefined,
      undefined,
      calls
    )).status).toBe("completed");
    expect(captured?.arguments).toEqual({ query: "prefix [REDACTED] suffix" });
    const requestArtifact = store.listArtifacts("interpolated-mcp-credential")
      .find((artifact) => artifact.kind === "mcp_request")!;
    const persisted = store.readArtifact(
      "interpolated-mcp-credential",
      requestArtifact.declaredPath
    ).content.toString("utf8");
    expect(persisted).toContain('"redacted":true');
    expect(persisted).not.toContain("hunter2abc");
    store.close();
  });

  test("accepts ordinary structured-format names for MCP output artifacts", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: csv-mcp-output
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: fetch, type: mcp_call, server: fixture, tool: export, arguments: {}, outputs: [report.csv] }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "csv-mcp-output", workflow });
    const calls = createAgentFlowMcpCallRegistry().register("fixture", () => ({
      outputs: { "report.csv": "key,value\nAF-44,complete\n" }
    }));

    expect((await executeAgentFlowCommandPipeline(
      store,
      "csv-mcp-output",
      workflow,
      undefined,
      undefined,
      calls
    )).status).toBe("completed");
    expect(store.readArtifact("csv-mcp-output", "report.csv").content.toString("utf8"))
      .toBe("key,value\nAF-44,complete\n");
    store.close();
  });

  test("mirrors sensitive MCP argument rejection during simulation", () => {
    const secretPathWorkflow = parseAgentFlowWorkflowOrThrow(`name: simulated-secret-path
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: fetch, type: mcp_call, server: fixture, tool: read_file, arguments: { path: 'C:\\repo\\.env' }, outputs: [ticket.json] }
`);
    expect(simulateAgentFlowWorkflow(secretPathWorkflow, {
      steps: { fetch: { outputs: { "ticket.json": { ok: true } } } }
    })).toMatchObject({ status: "paused", availableArtifacts: [] });

    const denyWorkflow = parseAgentFlowWorkflowOrThrow(`name: simulated-denied-secret
version: 1
style: pipeline
maturity: experimental
policies:
  sensitive_inputs: deny
steps:
  - { id: fetch, type: mcp_call, server: fixture, tool: get, arguments: { api_token: secret-value }, outputs: [ticket.json] }
`);
    expect(simulateAgentFlowWorkflow(denyWorkflow, {
      steps: { fetch: { outputs: { "ticket.json": { ok: true } } } }
    })).toMatchObject({ status: "paused", availableArtifacts: [] });

    const interpolatedCredentialWorkflow = parseAgentFlowWorkflowOrThrow(`name: simulated-interpolated-credential
version: 1
style: pipeline
maturity: experimental
policies: { sensitive_inputs: deny }
inputs: { key: { required: true } }
steps:
  - { id: fetch, type: mcp_call, server: fixture, tool: search, arguments: { query: "prefix {{ inputs.key }} suffix" }, outputs: [ticket.json] }
`);
    expect(simulateAgentFlowWorkflow(interpolatedCredentialWorkflow, {
      inputs: { key: "hunter2abc" },
      steps: { fetch: { outputs: { "ticket.json": { ok: true } } } }
    })).toMatchObject({ status: "paused", availableArtifacts: [] });

    for (const argumentsYaml of [
      "{ filepath: .env }",
      "{ access_key: opaque-value }",
      "{ aws_access_key_id: opaque-value }",
      "{ encryption_key: opaque-value }"
    ]) {
      const workflow = parseAgentFlowWorkflowOrThrow(`name: simulated-sensitive-arguments
version: 1
style: pipeline
maturity: experimental
policies:
  sensitive_inputs: deny
steps:
  - { id: fetch, type: mcp_call, server: fixture, tool: get, arguments: ${argumentsYaml}, outputs: [ticket.json] }
`);
      expect(simulateAgentFlowWorkflow(workflow, {
        steps: { fetch: { outputs: { "ticket.json": { ok: true } } } }
      })).toMatchObject({ status: "paused", availableArtifacts: [] });
    }
  });

  test("mirrors MCP adapter metadata checks during simulation without treating output names as inputs", () => {
    const safeWorkflow = parseAgentFlowWorkflowOrThrow(`name: simulated-csv-mcp-output
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: fetch, type: mcp_call, server: fixture, tool: export, arguments: {}, outputs: [report.csv] }
`);
    expect(simulateAgentFlowWorkflow(safeWorkflow, {
      steps: { fetch: { outputs: { "report.csv": "key,value\nAF-44,complete\n" } } }
    })).toMatchObject({ status: "completed", availableArtifacts: ["report.csv"] });

    const unsafeStepWorkflow = structuredClone(safeWorkflow);
    unsafeStepWorkflow.steps[0]!.id = "api_token=opaque";
    expect(simulateAgentFlowWorkflow(unsafeStepWorkflow, {
      steps: { "api_token=opaque": { outputs: { "report.csv": "unsafe" } } }
    })).toMatchObject({ status: "paused", availableArtifacts: [] });

    const unsafeOutputWorkflow = structuredClone(safeWorkflow);
    unsafeOutputWorkflow.steps[0]!.outputs = ["secrets/result.json"];
    expect(simulateAgentFlowWorkflow(unsafeOutputWorkflow, {
      steps: { fetch: { outputs: { "secrets/result.json": { unsafe: true } } } }
    })).toMatchObject({ status: "paused", availableArtifacts: [] });
  });

  test("redacts adapter-native MCP errors before pipeline persistence", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: redacted-native-mcp-error
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: fetch, type: mcp_call, server: fixture, tool: get_issue, arguments: {}, outputs: [ticket.json] }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "redacted-native-mcp-error", workflow });
    const calls = createAgentFlowMcpCallRegistry().register("fixture", () => {
      throw new AgentFlowMcpCallError(
        "server rejected Authorization: Bearer mcp-adapter-secret; passwords: plural-mcp-secret",
        "FIXTURE_REJECTED"
      );
    });

    const result = await executeAgentFlowCommandPipeline(
      store,
      "redacted-native-mcp-error",
      workflow,
      undefined,
      undefined,
      calls
    );

    expect(result.message).toContain("Authorization: Bearer [REDACTED]");
    expect(JSON.stringify(result)).not.toContain("mcp-adapter-secret");
    expect(JSON.stringify(store.listEvents("redacted-native-mcp-error"))).not.toContain("mcp-adapter-secret");
    expect(JSON.stringify(result)).not.toContain("plural-mcp-secret");
    expect(JSON.stringify(store.listEvents("redacted-native-mcp-error"))).not.toContain("plural-mcp-secret");
    store.close();
  });

  test("sanitizes adapter error causes at the exported MCP boundary", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: sanitized-direct-mcp-error
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: fetch, type: mcp_call, server: fixture, tool: get_issue, arguments: {}, outputs: [ticket.json] }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    store.createRun({
      id: "sanitized-direct-mcp-error",
      status: "running",
      workflow: { name: workflow.name, version: workflow.version, style: workflow.style, maturity: workflow.maturity },
      context: { workflow }
    });
    const calls = createAgentFlowMcpCallRegistry().register("fixture", () => {
      throw new AgentFlowMcpCallError("Authorization: Bearer direct-mcp-secret-value", "FIXTURE_REJECTED");
    });

    try {
      await executeAgentFlowMcpCall(store, "sanitized-direct-mcp-error", workflow, workflow.steps[0]!, calls);
      throw new Error("Expected direct MCP execution to fail.");
    } catch (error) {
      expect(error).toBeInstanceOf(AgentFlowMcpCallError);
      expect((error as Error).message).toContain("Authorization: Bearer [REDACTED]");
      expect(((error as Error).cause as Error | undefined)?.message).toContain("Authorization: Bearer [REDACTED]");
      expect(JSON.stringify({
        message: (error as Error).message,
        cause: ((error as Error).cause as Error | undefined)?.message
      })).not.toContain("direct-mcp-secret-value");
    }
    store.close();
  });

  test("sanitizes response-processing errors at the exported MCP boundary", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: sanitized-mcp-response-error
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: fetch, type: mcp_call, server: fixture, tool: get_issue, arguments: {}, outputs: [ticket.json] }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    store.createRun({
      id: "sanitized-mcp-response-error",
      status: "running",
      workflow: { name: workflow.name, version: workflow.version, style: workflow.style, maturity: workflow.maturity },
      context: { workflow }
    });
    const calls = createAgentFlowMcpCallRegistry().register("fixture", () =>
      new Proxy({ outputs: { "ticket.json": { key: "AF-44" } } }, {
        get(target, property, receiver) {
          if (property === "outputs") {
            throw Object.assign(new Error("Authorization: Bearer mcp-response-secret"), {
              code: "AGENT_FLOW_RUN_COLLISION"
            });
          }
          return Reflect.get(target, property, receiver);
        }
      })
    );

    try {
      await executeAgentFlowMcpCall(store, "sanitized-mcp-response-error", workflow, workflow.steps[0]!, calls);
      throw new Error("Expected MCP response processing to fail.");
    } catch (error) {
      expect(error).toBeInstanceOf(AgentFlowMcpCallError);
      expect((error as AgentFlowMcpCallError).code).toBe("AGENT_FLOW_MCP_RESPONSE_FAILED");
      expect((error as Error).message).toContain("Authorization: Bearer [REDACTED]");
      expect(((error as Error).cause as Error | undefined)?.message).toContain("Authorization: Bearer [REDACTED]");
      expect(JSON.stringify({
        message: (error as Error).message,
        cause: ((error as Error).cause as Error | undefined)?.message
      })).not.toContain("mcp-response-secret");
    }
    store.close();
  });

  test("blocks secret-like MCP path arguments before adapter invocation", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: blocked-mcp-path
version: 1
style: pipeline
maturity: experimental
steps:
  - id: fetch
    type: mcp_call
    server: fixture
    tool: read_file
    arguments: { source: .env, arbitrary: [README.md, credentials.json] }
    outputs: [result.json]
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "blocked-mcp-path", workflow });
    let calls = 0;
    const adapters = createAgentFlowMcpCallRegistry().register("fixture", () => {
      calls += 1;
      return { outputs: { "result.json": {} } };
    });

    const result = await executeAgentFlowCommandPipeline(
      store,
      "blocked-mcp-path",
      workflow,
      undefined,
      undefined,
      adapters
    );

    expect(result).toMatchObject({ status: "paused", failedStep: "fetch" });
    expect(result.message).toContain("secret-like path");
    expect(calls).toBe(0);
    store.close();
  });

  test("rejects secret-bearing MCP adapter identity metadata before invocation", async () => {
    const root = temporaryRepo();
    const secretStepId = "API_TOKEN=adapter-identity-secret";
    const workflow = parseAgentFlowWorkflowOrThrow(`name: blocked-mcp-adapter-identity
version: 1
style: pipeline
maturity: experimental
policies: { sensitive_inputs: deny }
steps:
  - id: ${JSON.stringify(secretStepId)}
    type: mcp_call
    server: fixture
    tool: get_issue
    arguments: {}
    outputs: [ticket.json]
`);
    const store = await openAgentFlowRunState({ cwd: root });
    store.createRun({
      id: "blocked-mcp-adapter-identity",
      status: "running",
      workflow: { name: workflow.name, version: workflow.version, style: workflow.style, maturity: workflow.maturity },
      context: { workflow }
    });
    let calls = 0;
    const adapters = createAgentFlowMcpCallRegistry().register("fixture", () => {
      calls += 1;
      return { outputs: { "ticket.json": {} } };
    });

    await expect(executeAgentFlowMcpCall(
      store,
      "blocked-mcp-adapter-identity",
      workflow,
      workflow.steps[0]!,
      adapters
    )).rejects.toMatchObject({ code: "AGENT_FLOW_SENSITIVE_INPUT" });
    expect(calls).toBe(0);
    store.close();
  });

  test("blocks host credential and process-introspection paths before MCP invocation", async () => {
    for (const [index, hostPath] of ["/etc/shadow", "/proc/self/mem", "/proc/self/fd/3"].entries()) {
      const root = temporaryRepo();
      const workflow = parseAgentFlowWorkflowOrThrow(`name: blocked-host-path-${index}
version: 1
style: pipeline
maturity: experimental
steps:
  - id: fetch
    type: mcp_call
    server: fixture
    tool: read_file
    arguments: { path: ${JSON.stringify(hostPath)} }
    outputs: [result.json]
`);
      const store = await openAgentFlowRunState({ cwd: root });
      createAgentFlowLifecycleRun(store, { id: `blocked-host-path-${index}`, workflow });
      let calls = 0;
      const adapters = createAgentFlowMcpCallRegistry().register("fixture", () => {
        calls += 1;
        return { outputs: { "result.json": {} } };
      });

      const result = await executeAgentFlowCommandPipeline(
        store,
        `blocked-host-path-${index}`,
        workflow,
        undefined,
        undefined,
        adapters
      );

      expect(result).toMatchObject({ status: "paused", failedStep: "fetch" });
      expect(result.message).toContain("secret-like path");
      expect(calls).toBe(0);
      store.close();
    }
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

  test("rejects malformed sensitive-input policy at the direct MCP executor boundary", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: direct-invalid-sensitive-policy
version: 1
style: pipeline
maturity: experimental
policies: { sensitive_inputs: deny_typo }
steps:
  - { id: fetch, type: mcp_call, server: fixture, tool: get_issue, arguments: { api_token: secret }, outputs: [ticket.json] }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    store.createRunWithEvent({
      id: "direct-invalid-sensitive-policy",
      workflow: { name: workflow.name, version: workflow.version, style: workflow.style, maturity: workflow.maturity },
      context: { workflow: workflow as never }
    }, { type: "run.created", payload: { status: "pending" } });
    store.transitionRunWithEvent("direct-invalid-sensitive-policy", {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: {} }
    });
    let invoked = false;
    const calls = createAgentFlowMcpCallRegistry().register("fixture", () => {
      invoked = true;
      return { outputs: { "ticket.json": {} } };
    });

    await expect(executeAgentFlowMcpCall(
      store,
      "direct-invalid-sensitive-policy",
      workflow,
      workflow.steps[0]!,
      calls
    )).rejects.toMatchObject({ code: "AGENT_FLOW_SENSITIVE_INPUT" });
    expect(invoked).toBe(false);
    store.close();
  });

  test("invokes a direct MCP pre-publication hook exactly once", async () => {
    const root = temporaryRepo();
    const workflow = mcpWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, {
      id: "single-pre-publication-hook",
      workflow,
      inputs: { ticket_key: "AF-69" }
    });
    store.transitionRunWithEvent("single-pre-publication-hook", {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: {} }
    });
    const calls = createAgentFlowMcpCallRegistry().register("fixture", () => ({
      outputs: { "ticket.json": { key: "AF-69" } }
    }));
    let hookCalls = 0;

    await expect(executeAgentFlowMcpCall(
      store,
      "single-pre-publication-hook",
      workflow,
      workflow.steps[0]!,
      calls,
      { beforePublish: () => { hookCalls += 1; } }
    )).resolves.toMatchObject({ server: "fixture", tool: "get_issue" });
    expect(hookCalls).toBe(1);
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

  test("enforces runtime MCP argument size limits during simulation", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: oversized-simulated-mcp-arguments
version: 1
style: pipeline
maturity: experimental
inputs:
  credential: { required: true }
steps:
  - id: fetch
    type: mcp_call
    server: fixture
    tool: get_issue
    arguments: { api_token: "{{ inputs.credential }}" }
    outputs: [ticket.json]
    on_failure: { then: pause }
`);

    const result = simulateAgentFlowWorkflow(workflow, {
      inputs: { credential: "x".repeat(MAX_AGENT_FLOW_MCP_ARGUMENT_BYTES + 1) },
      steps: { fetch: { outputs: { "ticket.json": { key: "AF-44" } } } }
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

  test("rolls back output ownership changes injected inside atomic finalization", async () => {
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
    expect(store.getArtifact("output-race", "ticket.json")).toBeNull();
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

  test("rejects oversized source arguments before sensitive-value redaction", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: oversized-secret-source-argument
version: 1
style: pipeline
maturity: experimental
inputs: { credential: { required: true } }
steps:
  - id: fetch
    type: mcp_call
    server: fixture
    tool: get_issue
    arguments: { api_token: "{{ inputs.credential }}" }
    outputs: [ticket.json]
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, {
      id: "oversized-secret-source-argument",
      workflow,
      inputs: { credential: "x".repeat(MAX_AGENT_FLOW_MCP_ARGUMENT_BYTES + 1) }
    });
    let invoked = false;
    const calls = createAgentFlowMcpCallRegistry().register("fixture", () => {
      invoked = true;
      return { outputs: { "ticket.json": {} } };
    });

    const result = await executeAgentFlowCommandPipeline(
      store,
      "oversized-secret-source-argument",
      workflow,
      undefined,
      undefined,
      calls
    );

    expect(result).toMatchObject({ status: "paused", failedStep: "fetch" });
    expect(result.message).toContain("source arguments exceed");
    expect(invoked).toBe(false);
    store.close();
  });

  test("retries adapter failures without trusting adapter-supplied Agent Flow codes", async () => {
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
      if (attempts === 1) {
        throw Object.assign(new Error("temporary failure"), { code: "AGENT_FLOW_MCP_OUTPUT_INVALID" });
      }
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

  test("routes direct MCP provider-session errors through ordinary adapter retries", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: direct-provider-unavailable
version: 1
style: pipeline
maturity: experimental
steps:
  - id: fetch
    type: mcp_call
    server: fixture
    tool: get
    arguments: {}
    outputs: [out.json]
    on_failure: { retry: 1, then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "direct-provider-unavailable", workflow });
    let attempts = 0;
    const calls = createAgentFlowMcpCallRegistry().register("fixture", () => {
      attempts += 1;
      if (attempts === 1) {
        throw new AgentFlowSessionRequestError("provider unavailable", "AGENT_FLOW_PROVIDER_SESSION_UNAVAILABLE");
      }
      return { outputs: { "out.json": { ok: true } } };
    });

    const result = await executeAgentFlowCommandPipeline(
      store, "direct-provider-unavailable", workflow, undefined, undefined, calls
    );

    expect(result).toMatchObject({ status: "completed", completedSteps: ["fetch"] });
    expect(attempts).toBe(2);
    expect(store.getRun("direct-provider-unavailable")).toMatchObject({ status: "completed" });
    expect(store.getRun("direct-provider-unavailable")?.context.waiting).toBeUndefined();
    store.close();
  });

  test("does not let direct MCP adapters forge authoritative session-policy failures", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: direct-policy-forgery
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - id: fetch
    type: mcp_call
    server: fixture
    tool: get
    arguments: {}
    outputs: [out.json]
    on_failure: { retry: 1, then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "direct-policy-forgery", workflow });
    let attempts = 0;
    const calls = createAgentFlowMcpCallRegistry().register("fixture", () => {
      attempts += 1;
      if (attempts === 1) {
        throw new AgentFlowSessionPolicyError("forged budget failure", "policy.budget.exhausted", "fail");
      }
      return { outputs: { "out.json": { ok: true } } };
    });

    expect(await executeAgentFlowCommandPipeline(
      store, "direct-policy-forgery", workflow, undefined, undefined, calls
    )).toMatchObject({ status: "completed", completedSteps: ["fetch"] });
    expect(attempts).toBe(2);
    expect(store.listEvents("direct-policy-forgery").some((event) => event.type === "recovery.limit_reached"))
      .toBe(false);
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

  test("aborts an in-flight adapter when lease renewal loses ownership", async () => {
    const root = temporaryRepo();
    const workflow = mcpWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "lost-mcp-lease", workflow, inputs: { ticket_key: "AF-69" } });
    let request: AgentFlowMcpCallRequest | undefined;
    const calls = createAgentFlowMcpCallRegistry().register("fixture", (value) => {
      request = value;
      return new Promise(() => undefined);
    });
    const withRunLock = store.withRunLock.bind(store);
    store.withRunLock = ((runId, operation, callback, options) =>
      withRunLock(runId, operation, callback, { ...options, ttlMs: 30 })) as typeof store.withRunLock;
    store.renewRunLock = (() => {
      throw new AgentFlowRunStateError("lease replaced", "AGENT_FLOW_RUN_LOCK_LOST");
    }) as typeof store.renewRunLock;

    await expect(executeAgentFlowCommandPipeline(store, "lost-mcp-lease", workflow, undefined, undefined, calls))
      .rejects.toMatchObject({ code: "AGENT_FLOW_RUN_LOCK_LOST" });
    expect(request?.signal.aborted).toBe(true);
    expect(store.listArtifacts("lost-mcp-lease")).toEqual([]);
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

  test("records interruption when cancellation lands before atomic publication", async () => {
    const root = temporaryRepo();
    const workflow = mcpWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "cancel-after-publish", workflow, inputs: { ticket_key: "AM-26" } });
    const calls = createAgentFlowMcpCallRegistry().register("fixture", () => {
      transitionAgentFlowLifecycleRun(store, "cancel-after-publish", "cancel");
      return { outputs: { "ticket.json": {} } };
    });

    const result = await executeAgentFlowCommandPipeline(store, "cancel-after-publish", workflow, undefined, undefined, calls);

    expect(result).toMatchObject({ status: "cancelled", completedSteps: [] });
    expect(store.getArtifact("cancel-after-publish", "ticket.json")).toBeNull();
    expect(store.listEvents("cancel-after-publish").map((event) => event.type)).toContain("step.interrupted");
    expect(store.listEvents("cancel-after-publish").map((event) => event.type)).not.toContain("step.completed");
    store.close();
  });

  test("keeps committed MCP completion when cancellation lands after atomic finalization", async () => {
    const root = temporaryRepo();
    const workflow = mcpWorkflow();
    const runId = "cancel-after-mcp-finalization";
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: runId, workflow, inputs: { ticket_key: "AF-69" } });
    const calls = createAgentFlowMcpCallRegistry().register("fixture", () => ({
      outputs: { "ticket.json": { key: "AF-69" } }
    }));
    const finalize = store.withRunFinalizationTransaction.bind(store);
    let cancelAfterCommit = true;
    let cancelling = false;
    store.withRunFinalizationTransaction = ((id, callback) => {
      const result = finalize(id, callback);
      if (id === runId && cancelAfterCommit && !cancelling
          && store.listEvents(runId).some((event) => event.type === "step.completed")) {
        cancelAfterCommit = false;
        cancelling = true;
        try {
          transitionAgentFlowLifecycleRun(store, runId, "cancel");
        } finally {
          cancelling = false;
        }
      }
      return result;
    }) as typeof store.withRunFinalizationTransaction;

    const result = await executeAgentFlowCommandPipeline(store, runId, workflow, undefined, undefined, calls);

    expect(result).toMatchObject({ status: "cancelled", completedSteps: ["fetch"] });
    expect(store.latestStepRecoveryState(runId, "fetch")).toMatchObject({ status: "completed" });
    expect(store.getArtifact(runId, "ticket.json")).not.toBeNull();
    expect(store.listEvents(runId).map((event) => event.type)).toContain("step.completed");
    expect(store.listEvents(runId).map((event) => event.type)).not.toContain("step.interrupted");
    store.close();
  });

  test("rolls back MCP artifacts when step finalization fails", async () => {
    const root = temporaryRepo();
    const workflow = mcpWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "finalization-error", workflow, inputs: { ticket_key: "AM-26" } });
    const calls = createAgentFlowMcpCallRegistry().register("fixture", () => ({ outputs: { "ticket.json": {} } }));
    const appendRunEvent = store.appendRunEvent.bind(store);
    store.appendRunEvent = ((runId, input) => {
      if (input.type === "step.completed") throw new Error("step completion failed");
      return appendRunEvent(runId, input);
    }) as typeof store.appendRunEvent;

    const result = await executeAgentFlowCommandPipeline(store, "finalization-error", workflow, undefined, undefined, calls);

    expect(result).toMatchObject({ status: "paused", completedSteps: [], failedStep: "fetch" });
    expect(result.message).toContain("step completion failed");
    expect(store.getArtifact("finalization-error", "ticket.json")).toBeNull();
    expect(store.getArtifact("finalization-error", "mcp-calls/fetch-e7d3799ecc09.json")).toBeNull();
    expect(store.listEvents("finalization-error").map((event) => event.type)).not.toContain("step.completed");
    store.close();
  });

  test("recovers committed MCP completion without invoking the adapter again", async () => {
    const root = temporaryRepo();
    const workflow = mcpWorkflow();
    const runId = "committed-mcp-completion";
    const interrupted = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(interrupted, { id: runId, workflow, inputs: { ticket_key: "AF-69" } });
    interrupted.transitionRunWithEvent(runId, {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: { status: "running" } }
    });
    interrupted.updateRun(runId, {
      currentStepId: "fetch",
      context: {
        ...interrupted.getRun(runId)!.context,
        executionRouting: {
          stepAttemptLimits: {},
          visits: { fetch: 1 },
          recoveryCycles: {},
          recoveryInvocations: {},
          disagreementEpisodes: {},
          disagreementRounds: {},
          attempts: { fetch: 1 }
        },
        executionCheckpoint: { stepId: "fetch", visit: 1, completedAttempts: 0 }
      }
    });
    let invocations = 0;
    const calls = createAgentFlowMcpCallRegistry().register("fixture", () => {
      invocations += 1;
      return { outputs: { "ticket.json": { key: "AF-69" } } };
    });
    const finalize = interrupted.withRunFinalizationTransaction.bind(interrupted);
    let crashAfterCommit = true;
    interrupted.withRunFinalizationTransaction = ((id, callback) => {
      const result = finalize(id, callback);
      if (crashAfterCommit) {
        crashAfterCommit = false;
        throw new Error("executor exited after commit");
      }
      return result;
    }) as typeof interrupted.withRunFinalizationTransaction;

    await expect(executeAgentFlowMcpCall(
      interrupted,
      runId,
      workflow,
      workflow.steps[0]!,
      calls,
      {
        attempt: 1,
        finalize: (result) => {
          const output = {
            attempt: 1,
            server: result.server,
            tool: result.tool,
            requestArtifact: result.requestArtifact.declaredPath,
            outputs: result.outputArtifacts.map((artifact) => artifact.declaredPath)
          };
          interrupted.upsertStep({ runId, stepId: "fetch", attempt: 1, status: "completed", output });
          interrupted.appendRunEvent(runId, { type: "step.completed", stepId: "fetch", payload: output });
        }
      }
    )).rejects.toThrow("executor exited after commit");
    expect(interrupted.getArtifact(runId, "ticket.json")).not.toBeNull();
    expect(interrupted.latestStepRecoveryState(runId, "fetch")).toMatchObject({ status: "completed" });
    interrupted.acquireRunLock(runId, "run", { ttlMs: 60_000 });
    interrupted.close();

    const recovered = await openAgentFlowRunState({ cwd: root });
    await expect(executeAgentFlowCommandPipeline(recovered, runId, workflow, undefined, undefined, calls))
      .resolves.toMatchObject({ status: "completed", completedSteps: ["fetch"] });
    expect(invocations).toBe(1);
    recovered.close();
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

function codexMcpWorkflow(name: string) {
  return parseAgentFlowWorkflowOrThrow(`name: ${name}
version: 1
style: pipeline
maturity: experimental
sessions:
  agent: { provider: codex, resume: true }
limits: { max_frontier_calls: 1 }
steps:
  - { id: fetch, type: mcp_call, via: codex, session: agent, server: jira, tool: get, arguments: {}, outputs: [ticket.json] }
`);
}

function temporaryRepo(): string {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "agent-flow-mcp-call-"));
  fs.mkdirSync(path.join(root, ".git"));
  return root;
}
