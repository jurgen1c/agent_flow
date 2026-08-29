import { describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  createAgentFlowFixtureSessionProvider,
  createAgentFlowApprovalPrompt,
  createAgentFlowLifecycleRun,
  createAgentFlowSessionProviderRegistry,
  AgentFlowRunStateError,
  AgentFlowSessionRequestError,
  executeAgentFlowCommandPipeline,
  executeAgentFlowSessionRequest,
  MAX_AGENT_FLOW_SESSION_METADATA_BYTES,
  MAX_AGENT_FLOW_SESSION_PROMPT_BYTES,
  MAX_AGENT_FLOW_SESSION_TOTAL_INPUT_BYTES,
  MAX_AGENT_FLOW_SESSION_OUTPUT_BYTES,
  openAgentFlowRunState,
  parseAgentFlowWorkflowOrThrow,
  simulateAgentFlowWorkflow,
  transitionAgentFlowLifecycleRun,
  validateAgentFlowWorkflow,
  type AgentFlowSessionProviderRequest
} from "../../src/runtime";
import {
  invokeAgentFlowSessionProvider,
  readAgentFlowSessionPrompt,
  reserveAgentFlowSessionModelCallBudgets
} from "../../src/runtime/session_request";
import {
  assertAgentFlowAdapterStringSafe,
  secureAgentFlowByteInput,
  secureAgentFlowJsonInput,
  secureAgentFlowTextInput
} from "../../src/runtime/execution_security";
import { redactAgentFlowSensitiveText } from "../../src/runtime/failure_payload";

describe("Agent Flow session request steps", () => {
  test("registers typed provider boundaries while live providers require explicit opt in", () => {
    const adapter = () => ({ outputs: {} });
    const registry = createAgentFlowSessionProviderRegistry([
      { kind: "fixture", adapter },
      { kind: "local", enabled: true, adapter },
      { kind: "frontier", enabled: true, adapter },
      { kind: "codex_profile", profile: "reviewer", enabled: true, adapter },
      { kind: "custom", name: "paseo", adapter }
    ]);

    expect(registry.names()).toEqual(["codex:reviewer", "fixture", "frontier", "local", "paseo"]);
    expect(registry.describe("codex:reviewer")).toEqual({
      name: "codex:reviewer",
      kind: "codex_profile",
      profile: "reviewer"
    });
    expect(() => createAgentFlowSessionProviderRegistry([
      { kind: "frontier", enabled: false, adapter }
    ])).toThrow("disabled unless enabled: true");
    expect(() => createAgentFlowSessionProviderRegistry().register("local", adapter))
      .toThrow("disabled unless enabled: true");
    expect(createAgentFlowSessionProviderRegistry().register("local", adapter, { enabled: true }).describe("local"))
      .toEqual({ name: "local", kind: "local" });
    expect(() => createAgentFlowSessionProviderRegistry([
      { kind: "custom", name: "codex:reviewer", adapter }
    ])).toThrow("is reserved");
    expect(() => createAgentFlowSessionProviderRegistry([
      { kind: "custom", name: "codex", adapter }
    ])).toThrow("is reserved");
    expect(() => createAgentFlowSessionProviderRegistry([
      { kind: "custom", name: "fixture", adapter }
    ])).toThrow("is reserved");
    expect(() => createAgentFlowSessionProviderRegistry().register(
      "codex: reviewer", adapter, { enabled: true }
    )).toThrow("must not have leading or trailing whitespace");
    expect(() => createAgentFlowSessionProviderRegistry([
      { kind: "codex_profile", profile: "reviewer ", enabled: true, adapter }
    ])).toThrow("must not have leading or trailing whitespace");
    expect(() => createAgentFlowSessionProviderRegistry([
      { kind: "custom", name: "codex:", adapter }
    ])).toThrow("is reserved");
  });

  test("fails unsupported providers with registration guidance before invocation", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = parseAgentFlowWorkflowOrThrow(`name: unsupported-provider
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: unavailable }
steps:
  - { id: draft, type: session_request, session: writer, prompt: prompts/draft.md, inputs: [request.md], outputs: [response.md] }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "unsupported-provider", workflow });
    store.writeArtifact({
      id: "request",
      runId: "unsupported-provider",
      path: "request.md",
      kind: "fixture",
      contentType: "text/plain",
      content: "Request"
    });

    const result = await executeAgentFlowCommandPipeline(
      store,
      "unsupported-provider",
      workflow,
      undefined,
      createAgentFlowSessionProviderRegistry()
    );

    expect(result).toMatchObject({ status: "paused", failedStep: "draft" });
    expect(result.message).toContain("Register a fixture or named custom adapter explicitly.");
    store.close();
  });

  test("rejects restored Codex option values before provider invocation or budget reservation", async () => {
    for (const codexOptions of [
      { profile: "../bad" },
      { reasoningEffort: "extreme" }
    ]) {
      const root = temporaryRepo();
      fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
      fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
      const workflow = parseAgentFlowWorkflowOrThrow(`name: invalid-restored-codex-options
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: codex, resume: true }
limits: { max_frontier_calls: 1 }
steps:
  - { id: draft, type: session_request, session: writer, prompt: prompts/draft.md, inputs: [request.md], outputs: [response.md] }
`);
      const runId = `invalid-restored-codex-${Object.keys(codexOptions)[0]}`;
      const store = await openAgentFlowRunState({ cwd: root });
      createAgentFlowLifecycleRun(store, { id: runId, workflow, context: { codexOptions } });
      store.writeArtifact({
        id: "request",
        runId,
        path: "request.md",
        kind: "fixture",
        contentType: "text/plain",
        content: "Request"
      });
      let invocations = 0;
      const providers = createAgentFlowSessionProviderRegistry().register("codex", () => {
        invocations += 1;
        return { outputs: { "response.md": "Response" } };
      });

      const result = await executeAgentFlowCommandPipeline(store, runId, workflow, undefined, providers);
      expect(result).toMatchObject({ status: "paused", failedStep: "draft" });
      expect(result.message).toContain(Object.hasOwn(codexOptions, "profile") ? "Codex profile" : "reasoning effort");
      expect(invocations).toBe(0);
      expect(store.getSession(runId, "writer")).toMatchObject({ status: "paused", startedAt: null });
      expect(store.getBudget(runId, "model:model_calls")).toBeNull();
      expect(store.getBudget(runId, "model:frontier_calls")).toBeNull();
      store.close();
    }
  });

  test("rejects and aborts when an in-flight interrupt check throws", async () => {
    let interruptChecks = 0;
    let aborted = false;
    const request: AgentFlowSessionProviderRequest = {
      runId: "polling-error",
      stepId: "ask",
      sessionId: "worker",
      provider: "fixture",
      resume: false,
      prompt: { path: "prompt.md", content: "Prompt", checksum: "sha256:prompt" },
      inputs: [],
      outputs: ["response.md"],
      signal: new AbortController().signal
    };
    const execution = invokeAgentFlowSessionProvider(
      (providerRequest) => new Promise((_resolve, reject) => {
        providerRequest.signal.addEventListener("abort", () => {
          aborted = true;
          reject(providerRequest.signal.reason);
        }, { once: true });
      }),
      request,
      undefined,
      () => {
        interruptChecks += 1;
        if (interruptChecks > 1) throw new Error("Approval polling failed.");
        return undefined;
      }
    );

    await expect(execution).rejects.toThrow("Approval polling failed.");
    expect(aborted).toBe(true);
  });

  test("validates static provider, resume, prompt, bounded inputs, and outputs", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: invalid-session-contract
version: 1
style: pipeline
maturity: experimental
sessions:
  dynamic: { provider: "{{ inputs.provider }}", resume: sometimes }
steps:
  - { id: ask, type: session_request, session: dynamic, prompt: "" }
`);

    expect(validateAgentFlowWorkflow(workflow).errors.map((issue) => issue.code)).toEqual([
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
    const workflow = parseAgentFlowWorkflowOrThrow(`name: invalid-session-runtime-contract
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

    expect(validateAgentFlowWorkflow(workflow).errors.map((issue) => issue.code)).toEqual([
      "workflow.session_request.target.unsupported",
      "workflow.session_request.prompt.invalid",
      "workflow.session_request.artifact.invalid",
      "workflow.session_request.artifact.invalid"
    ]);
  });

  test("rejects duplicate declared session artifacts during validation", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: duplicate-session-artifacts
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: fixture }
steps:
  - { id: ask, type: session_request, session: writer, prompt: prompt.md, inputs: [request.md, request.md], outputs: [response.md, response.md] }
`);

    expect(validateAgentFlowWorkflow(workflow).errors.map((issue) => issue.code).filter((code) =>
      code === "workflow.session_request.artifact.duplicate"
    )).toEqual([
      "workflow.session_request.artifact.duplicate",
      "workflow.session_request.artifact.duplicate"
    ]);
  });

  test("rejects noncanonical session artifact aliases during validation", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: aliased-session-artifacts
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: fixture }
steps:
  - { id: ask, type: session_request, session: writer, prompt: prompt.md, inputs: [dir/../request.md], outputs: [response.md] }
`);

    expect(validateAgentFlowWorkflow(workflow).errors).toContainEqual(expect.objectContaining({
      code: "workflow.session_request.artifact.invalid",
      path: "steps[0].inputs[0]"
    }));
  });

  test("rejects dynamic paths that the session runtime cannot resolve", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: unsupported-session-paths
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

    expect(validateAgentFlowWorkflow(workflow).errors).toEqual(expect.arrayContaining([
      expect.objectContaining({ code: "workflow.session_request.prompt.invalid", path: "steps[0].prompt" }),
      expect.objectContaining({ code: "workflow.session_request.artifact.invalid", path: "steps[0].inputs[0]" }),
      expect.objectContaining({ code: "workflow.session_request.artifact.invalid", path: "steps[0].outputs[0]" })
    ]));
  });

  test("simulates declared outputs deterministically from a fixture", () => {
    const workflow = sessionWorkflow();
    const result = simulateAgentFlowWorkflow(workflow, {
      artifacts: { "request.md": "Bounded request" },
      steps: { draft: { outputs: { "response.md": "Fixture response" } } }
    });

    expect(result).toMatchObject({ status: "completed", availableArtifacts: ["request.md", "response.md"] });
    expect(result.artifactValues["response.md"]).toBe("Fixture response");

    const repeatedWorkflow = parseAgentFlowWorkflowOrThrow(`name: repeated-session-simulation
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
    const repeated = simulateAgentFlowWorkflow(repeatedWorkflow, {
      artifacts: { "request.md": "Request" },
      steps: {
        repeat: { iterations: 2 },
        draft: { outputs: { "response.md": "Repeated response" } }
      }
    });
    expect(repeated).toMatchObject({ status: "completed", availableArtifacts: ["request.md", "response.md"] });
    expect(repeated.visitedSteps.filter((step) => step.id === "draft")).toHaveLength(2);
  });

  test("redacts secret-like prompt and artifact content before invoking a session provider", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(
      path.join(root, "prompts", "draft.md"),
      "Use Authorization: Bearer prompt-secret-value to investigate.\n"
    );
    const sourcePrompt = readAgentFlowSessionPrompt(root, "prompts/draft.md");
    const workflow = sessionWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "redacted-session-input", workflow });
    const source = store.writeArtifact({
      id: "request",
      runId: "redacted-session-input",
      path: "request.md",
      kind: "command_log",
      contentType: "text/markdown",
      content: "api_token: artifact-secret-value\n"
    });
    let captured: AgentFlowSessionProviderRequest | undefined;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => {
      captured = request;
      return { outputs: { "response.md": "Safe response" } };
    });

    expect((await executeAgentFlowCommandPipeline(
      store,
      "redacted-session-input",
      workflow,
      undefined,
      providers
    )).status).toBe("completed");
    expect(captured?.prompt.content).toContain("Authorization: Bearer [REDACTED]");
    expect(captured?.prompt.content).not.toContain("prompt-secret-value");
    expect(Buffer.from(captured!.inputs[0]!.content).toString("utf8")).toBe("api_token: [REDACTED]\n");
    expect(captured!.inputs[0]!.checksum).not.toBe(source.checksum);
    const requestArtifact = store.listArtifacts("redacted-session-input")
      .find((artifact) => artifact.kind === "session_request")!;
    const requestMetadata = JSON.parse(
      store.readArtifact("redacted-session-input", requestArtifact.declaredPath).content.toString("utf8")
    );
    expect(requestMetadata.prompt).toEqual({
      path: "prompts/draft.md",
      checksum: sourcePrompt.checksum,
      providerChecksum: captured!.prompt.checksum,
      redacted: true
    });
    expect(requestMetadata.inputs).toEqual([{
      path: "request.md",
      checksum: source.checksum,
      contentType: "text/markdown",
      providerChecksum: captured!.inputs[0]!.checksum,
      redacted: true
    }]);
    store.close();
  });

  test("preserves sensitive input provenance for opaque referenced artifacts and accepts CSV output names", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft a response.\n");
    const workflow = parseAgentFlowWorkflowOrThrow(`name: referenced-session-credential
version: 1
style: pipeline
maturity: experimental
inputs: { credential: { required: true } }
sessions: { writer: { provider: fixture } }
steps:
  - { id: draft, type: session_request, session: writer, prompt: prompts/draft.md, inputs: ["{{ inputs.credential }}"], outputs: [response.csv] }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, {
      id: "referenced-session-credential",
      workflow,
      inputs: { credential: "payload.txt" }
    });
    store.writeArtifact({
      id: "credential-payload",
      runId: "referenced-session-credential",
      path: "payload.txt",
      kind: "fixture",
      contentType: "text/plain",
      content: "hunter2abc"
    });
    let providerInput = "";
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => {
      providerInput = Buffer.from(request.inputs[0]!.content).toString("utf8");
      return { outputs: { "response.csv": "status\ncomplete\n" } };
    });

    expect((await executeAgentFlowCommandPipeline(
      store,
      "referenced-session-credential",
      workflow,
      undefined,
      providers
    )).status).toBe("completed");
    expect(providerInput).toBe("[REDACTED]");
    expect(providerInput).not.toContain("hunter2abc");
    expect(store.readArtifact("referenced-session-credential", "response.csv").content.toString("utf8"))
      .toBe("status\ncomplete\n");
    store.close();
  });

  test("denies opaque artifacts referenced by sensitive workflow inputs", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft a response.\n");
    const workflow = parseAgentFlowWorkflowOrThrow(`name: denied-referenced-session-credential
version: 1
style: pipeline
maturity: experimental
policies: { sensitive_inputs: deny }
inputs: { credential: { required: true } }
sessions: { writer: { provider: fixture } }
steps:
  - { id: draft, type: session_request, session: writer, prompt: prompts/draft.md, inputs: ["{{ inputs.credential }}"], outputs: [response.md] }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, {
      id: "denied-referenced-session-credential",
      workflow,
      inputs: { credential: "payload.txt" }
    });
    store.writeArtifact({
      id: "credential-payload",
      runId: "denied-referenced-session-credential",
      path: "payload.txt",
      kind: "fixture",
      contentType: "text/plain",
      content: "hunter2abc"
    });
    let invoked = false;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      invoked = true;
      return { outputs: { "response.md": "unsafe" } };
    });

    expect((await executeAgentFlowCommandPipeline(
      store,
      "denied-referenced-session-credential",
      workflow,
      undefined,
      providers
    ))).toMatchObject({ status: "paused", failedStep: "draft" });
    expect(invoked).toBe(false);
    store.close();
  });

  test("preserves ordinary keys and safely handles structured or secret-path inputs", () => {
    const workflow = sessionWorkflow();
    const deniedWorkflow = parseAgentFlowWorkflowOrThrow(`name: denied-sensitive-input
version: 1
style: pipeline
maturity: experimental
policies:
  sensitive_inputs: deny
steps: []
`);
    expect(secureAgentFlowTextInput(workflow, "ticket", "key: AF-44\n").value).toBe("key: AF-44\n");
    expect(secureAgentFlowTextInput(workflow, "ticket", "ticket_key: AF-44\n").value)
      .toBe("ticket_key: AF-44\n");
    expect(secureAgentFlowJsonInput(workflow, "ticket", { issue_key: "AF-44" }))
      .toEqual({ value: { issue_key: "AF-44" }, redacted: false });
    for (const credentialKey of [
      "PASS",
      "PWD",
      "DB_PASS",
      "DB_PWD",
      "REDIS_PASS",
      "SSH_KEY",
      "SSH_KEY_B64",
      "SSH_KEY_PATH",
      "DEPLOY_KEY",
      "DEPLOY_KEY_FILE",
      "SERVICE_KEY",
      "SERVICE_KEY_PATH",
      "SERVICE_ACCOUNT_KEY",
      "SERVICE_ACCOUNT_KEY_JSON"
    ]) {
      expect(secureAgentFlowTextInput(workflow, "credential", `${credentialKey}=opaque-value\n`).value)
        .toBe(`${credentialKey}=[REDACTED]\n`);
      expect(secureAgentFlowJsonInput(workflow, "credential", { [credentialKey]: "opaque-value" }))
        .toEqual({ value: { [credentialKey]: "[REDACTED]" }, redacted: true });
      expect(() => secureAgentFlowJsonInput(deniedWorkflow, "credential", { [credentialKey]: "opaque-value" }))
        .toThrow("denied by policies.sensitive_inputs");
    }
    expect(secureAgentFlowTextInput(workflow, "credential", "KEY=secret-value\n").value)
      .toBe("KEY=[REDACTED]\n");
    expect(secureAgentFlowTextInput(workflow, "credential", "tool --key secret-value\n").value)
      .toBe("tool --key [REDACTED]\n");
    for (const [source, expected] of [
      ['export "API_TOKEN"=hunter2-value\n', 'export "API_TOKEN"=[REDACTED]\n'],
      ["export 'API_TOKEN'=hunter2-value\n", "export 'API_TOKEN'=[REDACTED]\n"]
    ] as const) {
      expect(secureAgentFlowTextInput(workflow, "quoted shell credential", source).value).toBe(expected);
      expect(() => secureAgentFlowTextInput(deniedWorkflow, "quoted shell credential", source))
        .toThrow("denied by policies.sensitive_inputs");
    }
    for (const [source, expected] of [
      ["PASSWORD+=opaque-secret\n", "PASSWORD+=[REDACTED]\n"],
      ["API_TOKEN ?= opaque-secret\n", "API_TOKEN ?= [REDACTED]\n"],
      ["PASSWORD := opaque-secret\n", "PASSWORD := [REDACTED]\n"]
    ] as const) {
      expect(secureAgentFlowTextInput(workflow, "compound credential", source).value).toBe(expected);
      expect(() => secureAgentFlowTextInput(deniedWorkflow, "compound credential", source))
        .toThrow("denied by policies.sensitive_inputs");
    }
    expect(secureAgentFlowTextInput(workflow, "credential", "api_token: first;second;third\n").value)
      .toBe("api_token: [REDACTED]\n");
    expect(secureAgentFlowTextInput(
      workflow,
      "password-only connection URI",
      "REDIS_URL=redis://:opaque-password@localhost/0\n"
    ).value).toBe("REDIS_URL=redis://:[REDACTED]@localhost/0\n");
    expect(secureAgentFlowTextInput(
      workflow,
      "username-only connection URI",
      "https://opaque-credential@example.test/repo"
    ).value).toBe("https://[REDACTED]@example.test/repo");
    expect(() => secureAgentFlowTextInput(
      deniedWorkflow,
      "username-only connection URI",
      "https://opaque-credential@example.test/repo"
    )).toThrow("denied by policies.sensitive_inputs");
    for (const [source, redactedFragment] of [
      ["setenv API_TOKEN hunter2abc\n", "API_TOKEN [REDACTED]"],
      ["set -gx API_TOKEN hunter2abc\n", "API_TOKEN [REDACTED]"],
      ["set --global --export API_TOKEN hunter2abc\n", "API_TOKEN [REDACTED]"],
      ["printf -v API_TOKEN %s hunter2abc\n", "API_TOKEN [REDACTED]"],
      ["ENV API_TOKEN hunter2abc\n", "API_TOKEN [REDACTED]"],
      ["ENV API_TOKEN first-part\\\n  hunter2abc\n", "API_TOKEN [REDACTED]"],
      ["setx API_TOKEN hunter2abc\n", "API_TOKEN [REDACTED]"],
      ["setx /M \"API_TOKEN\" \"hunter2abc\"\n", "API_TOKEN [REDACTED]"],
      ["setx API_TOKEN first-part^\n  hunter2abc\n", "API_TOKEN [REDACTED]"],
      ["Set-Variable -Name API_TOKEN -Value hunter2abc\n", "API_TOKEN -Value [REDACTED]"],
      ["Set-Variable -Value hunter2abc -Name 'API_TOKEN'\n", "API_TOKEN -Value [REDACTED]"],
      ["Set-Variable API_TOKEN hunter2abc\n", "API_TOKEN -Value [REDACTED]"],
      [
        "[Environment]::SetEnvironmentVariable('API_TOKEN', 'hunter2abc')\n",
        "'API_TOKEN', '[REDACTED]')"
      ]
    ] as const) {
      expect(secureAgentFlowTextInput(workflow, "positional credential", source).value)
        .toContain(redactedFragment);
      expect(() => secureAgentFlowTextInput(deniedWorkflow, "positional credential", source))
        .toThrow("denied by policies.sensitive_inputs");
      expect(redactAgentFlowSensitiveText(source)).not.toContain("hunter2abc");
    }
    for (const ordinaryPositionalAssignment of [
      "ENV RELEASE_CHANNEL stable\n",
      "setx RELEASE_CHANNEL stable\n"
    ]) {
      expect(secureAgentFlowTextInput(workflow, "ordinary positional value", ordinaryPositionalAssignment))
        .toEqual({ value: ordinaryPositionalAssignment, redacted: false });
    }
    for (const ordinaryCredentialProse of [
      "Never expose secrets in logs.",
      "Review how credentials are stored."
    ]) {
      expect(secureAgentFlowTextInput(workflow, "ordinary security guidance", ordinaryCredentialProse))
        .toEqual({ value: ordinaryCredentialProse, redacted: false });
    }
    expect(() => assertAgentFlowAdapterStringSafe(workflow, "adapter identifier", "credentials"))
      .not.toThrow();
    for (const [source, expected] of [
      [
        'Authorization: Digest username="Mufasa", realm="test", nonce="abc", response="opaque-response"',
        "Authorization: Digest [REDACTED]"
      ],
      [
        "Authorization: AWS4-HMAC-SHA256 Credential=AKID/20260814/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=opaque-signature",
        "Authorization: AWS4-HMAC-SHA256 [REDACTED]"
      ],
      ["Proxy-Authorization: Negotiate first-token trailing-token", "Proxy-Authorization: Negotiate [REDACTED]"],
      [
        'Authorization: Digest username="foo\\\"bar", response="opaque-secret"',
        "Authorization: Digest [REDACTED]"
      ]
    ] as const) {
      expect(secureAgentFlowTextInput(workflow, "authorization header", source).value).toBe(expected);
    }
    for (const ambiguousHeader of [
      'Authorization: "opaque-authorization-secret',
      "Authorization: 'opaque-authorization-secret",
      "Authorization:\\r\\n opaque-authorization-secret",
      "Proxy-Authorization:\\n opaque-proxy-secret",
      "Cookie:\\t session=opaque-cookie-secret",
      'Authorization: "opaque-authorization-secret" trailing-secret'
    ]) {
      expect(() => secureAgentFlowTextInput(workflow, "ambiguous credential header", ambiguousHeader))
        .toThrow("ambiguous credential header");
      expect(() => secureAgentFlowTextInput(deniedWorkflow, "ambiguous credential header", ambiguousHeader))
        .toThrow("ambiguous credential header");
      expect(() => secureAgentFlowJsonInput(workflow, "nested ambiguous credential header", {
        body: ambiguousHeader
      })).toThrow("ambiguous credential header");
    }
    expect(secureAgentFlowTextInput(
      workflow,
      "inline JSON",
      '{"api_token":"opaquevalue","query":"keep"}\n'
    ).value).toBe('{"api_token":"[REDACTED]","query":"keep"}\n');
    for (const key of ["RAILS_MASTER_KEY", "JWT_SIGNING_KEY"]) {
      expect(secureAgentFlowTextInput(workflow, "credential", `${key}=opaquevalue\n`).value)
        .toBe(`${key}=[REDACTED]\n`);
      expect(secureAgentFlowJsonInput(workflow, "credential", { [key]: "opaquevalue" }))
        .toEqual({ value: { [key]: "[REDACTED]" }, redacted: true });
    }
    expect(secureAgentFlowTextInput(
      workflow,
      "quoted credentials",
      '{"authorization":"opaque-value","cookie":"session-value"}\n'
    ).value).not.toContain("opaque-value");

    const securedJson = secureAgentFlowByteInput(
      workflow,
      "ticket JSON",
      Buffer.from('{"key":"AF-44","api_token":"secret-value"}\n'),
      "ticket.json",
      "application/json"
    );
    expect(JSON.parse(Buffer.from(securedJson.value).toString("utf8"))).toEqual({
      key: "AF-44",
      api_token: "[REDACTED]"
    });
    expect(() => secureAgentFlowByteInput(
      workflow,
      "ticket JSON",
      Buffer.from('{"id":9007199254740993,"api_token":"secret-value"}\n'),
      "ticket.json",
      "application/json"
    )).toThrow("outside the safe lossless redaction range");
    for (const unsafeNumber of ["9007199254740993.0", "9.007199254740993e15"]) {
      expect(() => secureAgentFlowByteInput(
        workflow,
        "ticket JSON",
        Buffer.from(`{"id":${unsafeNumber},"api_token":"secret-value"}\n`),
        "ticket.json",
        "application/json"
      )).toThrow("outside the safe lossless redaction range");
    }
    for (const lossyNumber of ["0.1234567890123456789", "1e-400", "-0"]) {
      expect(() => secureAgentFlowByteInput(
        workflow,
        "ticket JSON",
        Buffer.from(`{"amount":${lossyNumber},"api_token":"secret-value"}\n`),
        "ticket.json",
        "application/json"
      )).toThrow("outside the safe lossless redaction range");
    }
    const losslessJson = '{"id":9007199254740993,"key":"AF-44"}\n';
    expect(Buffer.from(secureAgentFlowByteInput(
      workflow,
      "ticket JSON",
      Buffer.from(losslessJson),
      "ticket.json",
      "application/json"
    ).value).toString("utf8")).toBe(losslessJson);
    const sniffedJson = secureAgentFlowByteInput(
      workflow,
      "untyped JSON artifact",
      Buffer.from('{"api\\u005ftoken":"opaque-secret-value","query":"keep"}\n'),
      "request.txt",
      "application/octet-stream"
    );
    expect(JSON.parse(Buffer.from(sniffedJson.value).toString("utf8"))).toEqual({
      api_token: "[REDACTED]",
      query: "keep"
    });
    for (const markdown of [
      "[Review instructions]\nCheck the diff.\n",
      "{project} should remain literal\n",
      "[link](https://example.com)\n"
    ]) {
      expect(secureAgentFlowTextInput(workflow, "Markdown prompt", markdown, "prompt.md").value)
        .toBe(markdown);
    }
    const sniffedJsonArray = secureAgentFlowTextInput(
      workflow,
      "untyped JSON array",
      '[{"api_token":"opaque-array-secret"}]\n',
      "request.txt",
      "application/octet-stream"
    );
    expect(JSON.parse(sniffedJsonArray.value)).toEqual([{ api_token: "[REDACTED]" }]);
    expect(() => secureAgentFlowByteInput(
      workflow,
      "malformed untyped JSON artifact",
      Buffer.from('{"api\\u005ftoken":"opaque-secret-value"'),
      "request.txt",
      "application/octet-stream"
    )).toThrow("could not be parsed and sanitized safely");
    for (const malformedArray of [
      '[,{"api\\u005ftoken":"opaque-secret-value"}]',
      '[/*comment*/{"api\\u005ftoken":"opaque-secret-value"}]'
    ]) {
      expect(() => secureAgentFlowByteInput(
        workflow,
        "malformed untyped JSON array",
        Buffer.from(malformedArray),
        "request.txt",
        "application/octet-stream"
      )).toThrow("could not be parsed and sanitized safely");
    }
    for (const [path, contentType, content] of [
      [
        "duplicate.json",
        "application/json",
        '{"note":"Authorization: Bearer leaked-value","note":"safe"}'
      ],
      [
        "comment.yaml",
        "application/yaml",
        "note: safe\n# Authorization: Bearer leaked-value\n"
      ]
    ] as const) {
      expect(() => secureAgentFlowByteInput(
        workflow,
        "lossy structured artifact",
        Buffer.from(content),
        path,
        contentType
      )).toThrow("structured source text that cannot be sanitized safely");
    }

    expect(() => secureAgentFlowTextInput(deniedWorkflow, "credential", "KEY=secret-value\n"))
      .toThrow("denied by policies.sensitive_inputs");
    expect(() => secureAgentFlowByteInput(
      deniedWorkflow,
      "untyped JSON artifact",
      Buffer.from('{"api\\u005ftoken":"opaque-secret-value"}\n'),
      "request.txt",
      "application/octet-stream"
    )).toThrow("denied by policies.sensitive_inputs");
    expect(() => secureAgentFlowByteInput(
      deniedWorkflow,
      "malformed untyped JSON artifact",
      Buffer.from('{"api\\u005ftoken":"opaque-secret-value"'),
      "request.txt",
      "application/octet-stream"
    )).toThrow("could not be parsed and sanitized safely");
    for (const malformedJson of ['{"password', '{"api\\u005ftoken']) {
      expect(() => secureAgentFlowTextInput(
        workflow,
        "malformed declared JSON",
        malformedJson,
        "request.json",
        "application/json"
      )).toThrow("could not be parsed and sanitized safely");
    }
    for (const [contentType, content] of [
      ["text/plain", "<password>opaque-xml-secret</password>"],
      ["application/octet-stream", '"password" = "opaque-toml-secret"'],
      ["text/plain", '<input type="password" value="opaque-html-secret">'],
      ["application/octet-stream", "<input type=password value=opaque-html-secret>"],
      ["text/plain", "<input name=password value=opaque-html-secret>"],
      [
        "text/plain",
        [
          "--credential-boundary",
          'Content-Disposition: form-data; name="api_token"',
          "",
          "opaque-multipart-secret",
          "--credential-boundary--"
        ].join("\r\n")
      ],
      [
        "application/octet-stream",
        [
          "--credential-boundary",
          "Content-Disposition: form-data; name=api_token",
          "",
          "opaque-multipart-secret",
          "--credential-boundary--"
        ].join("\r\n")
      ]
    ] as const) {
      for (const sensitiveWorkflow of [workflow, deniedWorkflow]) {
        expect(() => secureAgentFlowByteInput(
          sensitiveWorkflow,
          "disguised structured artifact",
          Buffer.from(content),
          "notes.txt",
          contentType
        )).toThrow("no supported safe sanitizer");
      }
    }
    expect(() => secureAgentFlowTextInput(
      deniedWorkflow,
      "password-only connection URI",
      "REDIS_URL=redis://:opaque-password@localhost/0\n"
    )).toThrow("denied by policies.sensitive_inputs");
    expect(() => secureAgentFlowByteInput(
      workflow,
      "duplicate-key JSON artifact",
      Buffer.from('{"policy":"first","policy":"second","api_token":"opaque"}\n'),
      "request.json",
      "application/json"
    )).toThrow("duplicate object keys");
    const duplicateOrdinaryJson = '{"policy":"first","policy":"second"}\n';
    expect(Buffer.from(secureAgentFlowByteInput(
      workflow,
      "ordinary duplicate-key JSON artifact",
      Buffer.from(duplicateOrdinaryJson),
      "request.json",
      "application/json"
    ).value).toString("utf8")).toBe(duplicateOrdinaryJson);
    expect(() => secureAgentFlowTextInput(
      workflow,
      "bare carriage-return credential",
      "password: first-secret-part\r  second-secret-part\r"
    )).toThrow("multiline secret-like assignment");
    for (const blockScalar of [
      "password: |\n\n  leaked-value\n",
      "password: >-\n# comment\n  leaked-value\n"
    ]) {
      expect(() => secureAgentFlowTextInput(workflow, "YAML block credential", blockScalar))
        .toThrow("multiline secret-like assignment");
      expect(() => secureAgentFlowJsonInput(workflow, "nested YAML block credential", { body: blockScalar }))
        .toThrow("multiline secret-like assignment");
    }
    const structuredMultilineSecret = "password: first-secret-part\n  second-secret-part";
    expect(() => secureAgentFlowJsonInput(workflow, "multiline structured credential", {
      nested: { note: structuredMultilineSecret }
    })).toThrow("multiline secret-like assignment");
    expect(() => secureAgentFlowByteInput(
      workflow,
      "multiline JSON credential",
      Buffer.from(JSON.stringify({ nested: { note: structuredMultilineSecret } })),
      "request.json",
      "application/json"
    )).toThrow("multiline secret-like assignment");
    for (const shellQuotedSecret of [
      "PASSWORD=$'first-secret-part\nsecond-secret-part",
      'API_TOKEN=$"first-secret-part\nsecond-secret-part',
      '$env:API_TOKEN = @"\nfirst-secret-part\n"@'
    ]) {
      expect(() => secureAgentFlowTextInput(workflow, "shell-quoted multiline credential", shellQuotedSecret))
        .toThrow("multiline secret-like assignment");
      expect(() => secureAgentFlowTextInput(deniedWorkflow, "shell-quoted multiline credential", shellQuotedSecret))
        .toThrow("multiline secret-like assignment");
      expect(() => secureAgentFlowJsonInput(workflow, "nested shell-quoted multiline credential", {
        body: shellQuotedSecret
      })).toThrow("multiline secret-like assignment");
    }
    for (const [nestedJson, expected] of [
      ['{"api\\u005ftoken":"hunter2-value"}', '{"api_token":"[REDACTED]"}'],
      ['[{"client\\u005fsecret":"opaque-value"}]', '[{"client_secret":"[REDACTED]"}]']
    ] as const) {
      expect(secureAgentFlowJsonInput(workflow, "JSON-encoded structured value", { body: nestedJson }))
        .toEqual({ value: { body: expected }, redacted: true });
      expect(() => secureAgentFlowJsonInput(deniedWorkflow, "JSON-encoded structured value", { body: nestedJson }))
        .toThrow("denied by policies.sensitive_inputs");
    }
    const ordinaryNestedJson = '{ "ticket_key": "AF-44" }';
    expect(secureAgentFlowJsonInput(workflow, "ordinary JSON-encoded value", { body: ordinaryNestedJson }))
      .toEqual({ value: { body: ordinaryNestedJson }, redacted: false });
    expect(() => secureAgentFlowJsonInput(workflow, "malformed JSON-encoded value", {
      body: '{"api\\u005ftoken":"hunter2-value"'
    })).toThrow("could not be parsed and sanitized safely");
    expect(() => secureAgentFlowJsonInput(workflow, "lossy JSON-encoded value", {
      body: '{"id":9007199254740993,"api_token":"hunter2-value"}'
    })).toThrow("outside the safe lossless redaction range");
    expect(() => secureAgentFlowJsonInput(workflow, "duplicate-key JSON-encoded value", {
      body: '{"api_token":"first","api_token":"second"}'
    })).toThrow("duplicate object keys");
    for (const hiddenSecret of [
      '{"note":"Authorization: Bearer abcdefghijklmnop","note":"safe"}',
      '{"note":"password: leaked-value","note":"safe"}'
    ]) {
      expect(() => secureAgentFlowJsonInput(workflow, "duplicate-key hidden JSON credential", {
        body: hiddenSecret
      })).toThrow("secret-like JSON source text");
      expect(() => secureAgentFlowJsonInput(deniedWorkflow, "duplicate-key hidden JSON credential", {
        body: hiddenSecret
      })).toThrow("secret-like JSON source text");
    }
    for (const embeddedStructuredSecret of [
      "<password>opaque-xml-secret</password>",
      "<input type=password value=opaque-html-secret>",
      "<input name=password value=opaque-html-secret>",
      '"database"."password" = "opaque-toml-secret"',
      'database."client_secret" = "opaque-toml-secret"',
      '"api\\u005ftoken" = "opaque-toml-secret"',
      'database."client\\u005fsecret" = "opaque-toml-secret"'
    ]) {
      expect(() => secureAgentFlowJsonInput(workflow, "nested structured credential", {
        nested: { content: embeddedStructuredSecret }
      })).toThrow("no supported safe sanitizer");
      expect(() => secureAgentFlowJsonInput(deniedWorkflow, "nested structured credential", {
        nested: { content: embeddedStructuredSecret }
      })).toThrow("no supported safe sanitizer");
    }
    for (const pluralKey of ["api_tokens", "api_keys", "passwords", "client_secrets"]) {
      expect(secureAgentFlowTextInput(workflow, "plural credential", `${pluralKey}: opaque-value\n`).value)
        .toBe(`${pluralKey}: [REDACTED]\n`);
      expect(secureAgentFlowJsonInput(workflow, "plural credential", { [pluralKey]: "opaque-value" }))
        .toEqual({ value: { [pluralKey]: "[REDACTED]" }, redacted: true });
      expect(() => secureAgentFlowJsonInput(deniedWorkflow, "plural credential", { [pluralKey]: "opaque-value" }))
        .toThrow("denied by policies.sensitive_inputs");
    }
    for (const nestedAssignment of [
      "Note: API_TOKEN=hunter2",
      "prefix: password=opaque",
      "env: API_TOKEN=hunter2",
      "$env:API_TOKEN = hunter2"
    ]) {
      expect(secureAgentFlowTextInput(workflow, "nested credential assignment", nestedAssignment).value)
        .not.toContain("hunter2");
      expect(secureAgentFlowTextInput(workflow, "nested credential assignment", nestedAssignment).value)
        .not.toContain("opaque");
      expect(() => secureAgentFlowTextInput(deniedWorkflow, "nested credential assignment", nestedAssignment))
        .toThrow("denied by policies.sensitive_inputs");
    }
    const longOrdinaryAssignmentChain = `${"A=".repeat(5_000)}safe`;
    expect(secureAgentFlowTextInput(workflow, "bounded ordinary assignment chain", longOrdinaryAssignmentChain))
      .toEqual({ value: longOrdinaryAssignmentChain, redacted: false });
    const longSensitiveAssignmentChain = `${"A=".repeat(5_000)}API_TOKEN=hunter2`;
    const securedLongChain = secureAgentFlowTextInput(workflow, "bounded sensitive assignment chain", longSensitiveAssignmentChain);
    expect(securedLongChain.redacted).toBe(true);
    expect(securedLongChain.value).not.toContain("hunter2");
    expect(() => secureAgentFlowTextInput(deniedWorkflow, "bounded sensitive assignment chain", longSensitiveAssignmentChain))
      .toThrow("denied by policies.sensitive_inputs");
    for (const secretBearingKey of [
      "api_token=opaque-secret",
      "Authorization: Bearer abcdefghijklmnop",
      "ghp_abcdefghijklmnopqrstuvwxyz",
      "https://user:password@example.com"
    ]) {
      expect(() => secureAgentFlowJsonInput(workflow, "secret-bearing object key", {
        nested: { [secretBearingKey]: true }
      })).toThrow("secret material in an object key");
      expect(() => secureAgentFlowJsonInput(deniedWorkflow, "secret-bearing object key", {
        nested: { [secretBearingKey]: true }
      })).toThrow("secret material in an object key");
    }
    for (const structuredPath of [
      { files: { primary: "credentials.json" } },
      { path: { value: ".env" } },
      { url: "file:///repo/.env" },
      { urls: { primary: "file:///repo/credentials.json" } },
      { source: ".env" },
      { source: ".netrc" },
      { source: ".pgpass" },
      { path: "C:.netrc" },
      { path: "D:.pgpass" },
      { path: "/home/user/.ssh/id_ecdsa" },
      { path: "/home/user/.ssh/id_dsa" },
      { path: "keys/id_ecdsa" },
      { path: "keys/id_dsa" },
      { path: "/home/user/.ssh/deploy_key" },
      { path: "/home/user/.ssh/github" },
      { files: { ".env": true } },
      { urls: { "file:///repo/.netrc": true } },
      { files: { primary: "inputs/ghp_abcdefghijklmnopqrstuvwxyz.txt" } },
      { path: "inputs/api_token=opaque-secret.txt" },
      { url: "https://example.test/%2Eenv" },
      { url: "https://example.test/%252Eenv" },
      { url: "https://example.test/%2Edocker/config.json" },
      { url: "https://example.test/call?api%5Ftoken=hunter2" },
      { url: "https://example.test/call?api%255Ftoken=hunter2" },
      { arbitrary: ["credentials.json"] }
    ]) {
      expect(() => secureAgentFlowJsonInput(workflow, "nested path", structuredPath))
        .toThrow("secret-like path");
      expect(() => secureAgentFlowJsonInput(deniedWorkflow, "nested path", structuredPath))
        .toThrow("secret-like path");
    }
    for (const embeddedPath of [
      "cat .env && echo done",
      "cat /repo/.env | grep TOKEN",
      "cat credentials.json && true",
      "Read file:///repo/.env now",
      "Inspect config/master.key",
      "Read /etc/shadow.",
      "Inspect /proc/self/environ?",
      "Open .env.",
      "Read credentials.json.",
      "https://example.test/?%70assword=hunter2",
      "https://example.test/?api%5Ftoken=hunter2",
      "filename=.netrc",
      "filename=%2enetrc",
      "Content-Disposition: form-data; name=file; filename=.netrc\r\n\r\nmachine example.test login bob password hunter2"
    ]) {
      expect(() => secureAgentFlowTextInput(workflow, "embedded sensitive path", embeddedPath))
        .toThrow("secret-like path");
      expect(() => secureAgentFlowTextInput(deniedWorkflow, "embedded sensitive path", embeddedPath))
        .toThrow("secret-like path");
      expect(() => secureAgentFlowJsonInput(workflow, "embedded sensitive path", { command: embeddedPath }))
        .toThrow("secret-like path");
      expect(() => secureAgentFlowJsonInput(deniedWorkflow, "embedded sensitive path", { command: embeddedPath }))
        .toThrow("secret-like path");
    }
    for (const fileUrl of [
      "file:///repo/%2Eenv",
      "file:///repo/.env?raw=1",
      "file:///home/user/.docker/config.json#auth",
      "file:///repo/%252Eenv",
      "file:///repo/%2525252Eenv",
      "file:///repo/.netrc~",
      "file:///repo/.docker/config.json.swp",
      "file:///proc/self/environ",
      "file:///proc/1/task/1/cmdline",
      "https://callback.test/#api%5Ftoken=hunter2",
      "file:///safe#/%2Eenv",
      "https://example.test/call?file=.netrc",
      "https://example.test/call?file=%252Epgpass",
      "https://example.test/call#file=.npmrc",
      "https://example.test/call#.pypirc"
    ]) {
      expect(() => secureAgentFlowJsonInput(workflow, "encoded file URL", { url: fileUrl }))
        .toThrow("secret-like path");
      expect(() => secureAgentFlowJsonInput(deniedWorkflow, "encoded file URL", { url: fileUrl }))
        .toThrow("secret-like path");
    }
    for (const pseudoFile of [
      "/proc/self/environ",
      "/proc/thread-self/cmdline",
      "/proc/1/environ",
      "/proc/1/task/1/cmdline",
      "/etc/./shadow",
      "/etc//shadow",
      "/dev/fd/3",
      "/dev/stdin",
      "/dev/stdout",
      "/dev/stderr"
    ]) {
      expect(() => secureAgentFlowJsonInput(workflow, "process pseudo-file", { path: pseudoFile }))
        .toThrow("secret-like path");
      expect(() => secureAgentFlowJsonInput(deniedWorkflow, "process pseudo-file", { path: pseudoFile }))
        .toThrow("secret-like path");
    }

    expect(() => secureAgentFlowTextInput(
      workflow,
      "environment",
      "API_TOKEN=known\nCUSTOM_VALUE=still-secret\n",
      ".env"
    )).toThrow("complete redaction cannot be verified");
    expect(() => secureAgentFlowTextInput(
      workflow,
      "Windows environment",
      "ignored",
      String.raw`C:\repo\.env`
    )).toThrow("complete redaction cannot be verified");
    for (const dotenvPath of ["production.env", "config/staging.env.local", ".envrc"]) {
      expect(() => secureAgentFlowTextInput(
        workflow,
        "suffix-style environment",
        "RAILS_MASTER_KEY=opaque-secret\n",
        dotenvPath
      )).toThrow("secret-like path");
      expect(() => secureAgentFlowTextInput(
        deniedWorkflow,
        "suffix-style environment",
        "RAILS_MASTER_KEY=opaque-secret\n",
        dotenvPath
      )).toThrow("secret-like path");
    }
    for (const credentialPath of [
      ".env~",
      ".htpasswd",
      ".htpasswd.bak",
      "config/.htpasswd",
      ".netrc",
      ".netrc~",
      ".pgpass",
      ".my.cnf",
      ".npmrc",
      ".pypirc",
      ".docker/config.json",
      ".docker/config.json.backup",
      ".kube/config",
      ".kube/config.old",
      "/home/user/.gnupg/private-keys-v1.d/0123456789ABCDEF.key",
      "/home/user/.aws/credentials",
      "/home/user/.azure/accessTokens.json",
      "/home/user/.config/containers/auth.json"
    ]) {
      expect(() => secureAgentFlowTextInput(workflow, "credential file", "opaque", credentialPath))
        .toThrow("secret-like path");
      expect(() => secureAgentFlowTextInput(deniedWorkflow, "credential file", "opaque", credentialPath))
        .toThrow("secret-like path");
    }
    for (const railsKeyPath of ["config/master.key", "config/credentials/production.key"]) {
      expect(() => secureAgentFlowTextInput(workflow, "Rails credential key", "opaque", railsKeyPath))
        .toThrow("secret-like path");
      expect(() => secureAgentFlowTextInput(deniedWorkflow, "Rails credential key", "opaque", railsKeyPath))
        .toThrow("secret-like path");
    }
    expect(() => secureAgentFlowByteInput(
      workflow,
      "malformed text",
      Buffer.from([0xff, ...Buffer.from("API_TOKEN=secret-value")]),
      "notes.txt",
      "text/plain"
    )).toThrow("cannot be inspected or redacted safely");
    expect(() => secureAgentFlowByteInput(
      workflow,
      "YAML artifact",
      Buffer.from("api_token: |\n  still-secret\n"),
      "config.yaml",
      "application/yaml"
    )).toThrow("cannot be reserialized safely");
    for (const content of [
      "api_token: |\n  still-secret\n",
      "api_token: first-secret-part\n  second-secret-part\n",
      "password:\n  still-secret\n",
      "'client_secret': >-\n  still-secret\n",
      "api_token=first-secret-part\n  second-secret-part\n",
      "api_token = \"\"\"first-secret-part\nsecond-secret-part\"\"\"\n",
      "client_secret = '''first-secret-part\nsecond-secret-part'''\n",
      "password=first-secret-part\\\nsecond-secret-part\n",
      "api_token = [\n\"array-secret\"\n]\n",
      "api_token={\nvalue = \"object-secret\"\n}\n",
      "password=<<EOF\nheredoc-secret\nEOF\n",
      "client_secret=<<-'SECRET'\nheredoc-secret\nSECRET\n",
      "password = <<~RUBY\nsquiggly-heredoc-secret\nRUBY\n",
      "PASSWORD=$(cat <<EOF\nshell-substitution-secret\nEOF\n)\n",
      "PASSWORD=$(cat <<EOF)\ninline-heredoc-secret\nEOF\n",
      "PASSWORD=`cat <<EOF\nbacktick-substitution-secret\nEOF\n`\n",
      "export PASSWORD=$(cat <<EOF\nprefixed-shell-secret\nEOF\n)\n",
      "readonly PASSWORD=<<EOF\nprefixed-heredoc-secret\nEOF\n",
      "env PASSWORD=`cat <<EOF\nprefixed-backtick-secret\nEOF\n`\n",
      "API_TOKEN=${TOKEN:-\nparameter-expansion-secret\n}\n",
      "CLIENT_SECRET=$((1 +\narithmetic-expansion-secret\n))\n",
      "api_token=\"first-secret-part\nsecond-secret-part\"\n",
      "api_token=\nsecret-on-next-line\n",
      "PASSWORD+=first-secret-part\n  second-secret-part\n",
      "API_TOKEN ?= first-secret-part\n  second-secret-part\n",
      "API_TOKEN=prefix\"first-secret-part\nsecond-secret-part\"\n",
      "CLIENT_SECRET=prefix'first-secret-part\nsecond-secret-part'\n",
      "PASSWORD=prefix`first-secret-part\nsecond-secret-part`\n"
    ]) {
      expect(() => secureAgentFlowTextInput(workflow, "unstructured input", content, "notes.md"))
        .toThrow("multiline secret-like assignment");
    }
    for (const prefixedMultilineSecret of [
      'Note: API_TOKEN="first-secret-part\nsecond-secret-part',
      'A=1 API_TOKEN="first-secret-part\nsecond-secret-part'
    ]) {
      expect(() => secureAgentFlowTextInput(workflow, "prefixed multiline credential", prefixedMultilineSecret))
        .toThrow("multiline secret-like assignment");
      expect(redactAgentFlowSensitiveText(prefixedMultilineSecret)).not.toContain("first-secret-part");
      expect(redactAgentFlowSensitiveText(prefixedMultilineSecret)).not.toContain("second-secret-part");
    }
    for (const indexedAssignment of [
      "PASSWORD[0]=indexed-secret",
      "API_TOKENS[prod]=indexed-secret",
      "export CLIENT_SECRET[primary]=indexed-secret",
      'config["api_token"] = "indexed-secret"',
      'os.environ["PASSWORD"] = "indexed-secret"',
      "PASSWORD[0]=first-part\\\nsecond-part"
    ]) {
      expect(() => secureAgentFlowTextInput(workflow, "indexed credential", indexedAssignment, "notes.md"))
        .toThrow("indexed secret-like assignment");
      expect(() => secureAgentFlowJsonInput(deniedWorkflow, "indexed credential", { body: indexedAssignment }))
        .toThrow("indexed secret-like assignment");
    }
    expect(secureAgentFlowTextInput(workflow, "ordinary indexed assignment", "VALUES[0]=ordinary", "notes.md"))
      .toEqual({ value: "VALUES[0]=ordinary", redacted: false });
    for (const [content, secured] of [
      ["PASSWORD=$(printf ordinary)\nnext line\n", "PASSWORD=[REDACTED]\nnext line\n"],
      ["PASSWORD=`printf ordinary`\nnext line\n", "PASSWORD=[REDACTED]\nnext line\n"],
      ["API_TOKEN=${TOKEN:-ordinary}\nnext line\n", "API_TOKEN=[REDACTED]\nnext line\n"],
      ["CLIENT_SECRET=$((1 + 1))\nnext line\n", "CLIENT_SECRET=[REDACTED]\nnext line\n"]
    ] as const) {
      expect(secureAgentFlowTextInput(workflow, "single-line shell assignment", content, "notes.md").value)
        .toBe(secured);
    }
    for (const content of [
      "-----BEGIN PRIVATE KEY-----\nopaque-private-key-material\n",
      "-----BEGIN PGP PRIVATE KEY BLOCK-----\nopaque-private-key-material\n-----END PRIVATE KEY-----\n"
    ]) {
      expect(() => secureAgentFlowTextInput(workflow, "private key", content, "notes.md"))
        .toThrow("private-key material");
      expect(() => secureAgentFlowJsonInput(workflow, "private key", { source: content }))
        .toThrow("private-key material");
    }
    for (const [path, contentType, content] of [
      ["config.json", "application/json", '{"api_token" /*comment*/: "leaked-secret-value"}'],
      ["config.yaml", "application/yaml", "api_token: [unterminated\n"]
    ] as const) {
      expect(() => secureAgentFlowByteInput(
        workflow,
        "malformed structured artifact",
        Buffer.from(content),
        path,
        contentType
      )).toThrow("could not be parsed and sanitized safely");
    }
    for (const [path, contentType, content] of [
      ["credential.xml", "application/xml", "<password>opaquevalue</password>"],
      ["credential.toml", "application/toml", '"password" = "opaquevalue"'],
      [
        "upload.bin",
        "multipart/form-data; boundary=credential",
        '--credential\r\nContent-Disposition: form-data; name="password"\r\n\r\nopaquevalue\r\n--credential--\r\n'
      ],
      ["credential.eml", "message/rfc822", "Password: opaquevalue\r\n"]
    ] as const) {
      expect(() => secureAgentFlowByteInput(
        workflow,
        "unsupported structured artifact",
        Buffer.from(content),
        path,
        contentType
      )).toThrow("no supported safe sanitizer");
      expect(() => secureAgentFlowByteInput(
        deniedWorkflow,
        "unsupported structured artifact",
        Buffer.from(content),
        path,
        contentType
      )).toThrow("no supported safe sanitizer");
    }
  });

  test("allows reviewed sensitive model inputs only through explicit policy", async () => {
    const root = temporaryRepo();
    fs.writeFileSync(path.join(root, ".env"), "API_TOKEN=prompt-secret-value\n");
    const workflow = parseAgentFlowWorkflowOrThrow(`name: explicitly-allowed-sensitive-input
version: 1
style: pipeline
maturity: experimental
policies:
  sensitive_inputs: allow
sessions:
  writer: { provider: fixture }
steps:
  - { id: draft, type: session_request, session: writer, prompt: .env, inputs: [credentials.json], outputs: [response.md] }
`);
    expect(Buffer.from(secureAgentFlowByteInput(
      workflow,
      "reviewed XML artifact",
      Buffer.from("<password>reviewed-value</password>"),
      "credential.xml",
      "application/xml"
    ).value).toString("utf8")).toBe("<password>reviewed-value</password>");
    expect(Buffer.from(secureAgentFlowByteInput(
      workflow,
      "reviewed untyped JSON artifact",
      Buffer.from('{"api\\u005ftoken":"reviewed-value"}\n'),
      "request.txt",
      "application/octet-stream"
    ).value).toString("utf8")).toBe('{"api\\u005ftoken":"reviewed-value"}\n');
    expect(secureAgentFlowJsonInput(workflow, "reviewed file URL", { url: "file:///repo/%2Eenv" }))
      .toEqual({ value: { url: "file:///repo/%2Eenv" }, redacted: false });
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "allowed-sensitive-input", workflow });
    store.writeArtifact({
      id: "credentials",
      runId: "allowed-sensitive-input",
      path: "credentials.json",
      kind: "fixture",
      contentType: "application/json",
      content: '{"api_token":"artifact-secret-value"}\n'
    });
    let prompt = "";
    let input = "";
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => {
      prompt = request.prompt.content;
      input = Buffer.from(request.inputs[0]!.content).toString("utf8");
      return { outputs: { "response.md": "Reviewed response" } };
    });

    expect((await executeAgentFlowCommandPipeline(
      store,
      "allowed-sensitive-input",
      workflow,
      undefined,
      providers
    )).status).toBe("completed");
    expect(prompt).toContain("prompt-secret-value");
    expect(input).toContain("artifact-secret-value");
    store.close();
  });

  test("does not scan synthetic generated-prompt paths as external input paths", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: generated-prompt-path
version: 1
style: pipeline
maturity: experimental
sessions:
  reviewer: { provider: fixture, authority: { can_approve: true } }
steps:
  - { id: approve-secrets, type: approval, reviewer: reviewer, artifacts: [spec.md], output: approvals/result.json }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "generated-prompt-path", workflow });
    store.writeArtifact({
      id: "spec",
      runId: "generated-prompt-path",
      path: "spec.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Review this ordinary specification."
    });
    let promptPath = "";
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => {
      promptPath = request.prompt.path;
      return {
        outputs: {
          "approvals/result.json": JSON.stringify({ status: "approved", decision: "The specification is safe." })
        }
      };
    });

    expect((await executeAgentFlowCommandPipeline(
      store,
      "generated-prompt-path",
      workflow,
      undefined,
      providers
    )).status).toBe("completed");
    expect(promptPath).toContain("approve-secrets");
    store.close();
  });

  test("preserves ancestor file scopes for sessions reached through direct routing", async () => {
    const root = temporaryRepo();
    fs.writeFileSync(path.join(root, "prompt.md"), "Draft the response.\n");
    const workflow = parseAgentFlowWorkflowOrThrow(`name: routed-nested-session-scope
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true }
inputs: { ready: {} }
sessions:
  writer:
    provider: custom
    role: writer
    authority: { can_modify_files: true }
    file_scope: { include: ["**"] }
steps:
  - { id: route, type: condition, if: ready, then: write, else: fail }
  - id: container
    type: loop
    max_iterations: 1
    file_scope: { include: [src/**] }
    body:
      - id: write
        type: session_request
        session: writer
        prompt: prompt.md
        inputs: [request.md]
        outputs: [response.md]
        then: completed
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, {
      id: "routed-nested-session-scope",
      workflow,
      inputs: { ready: true }
    });
    store.writeArtifact({
      id: "request",
      runId: "routed-nested-session-scope",
      path: "request.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Please draft this."
    });
    let fileScope: AgentFlowSessionProviderRequest["fileScope"];
    const providers = createAgentFlowSessionProviderRegistry().register("custom", (request) => {
      fileScope = request.fileScope;
      return { outputs: { "response.md": "Drafted." } };
    });

    expect((await executeAgentFlowCommandPipeline(
      store,
      "routed-nested-session-scope",
      workflow,
      undefined,
      providers
    )).status).toBe("completed");
    expect(fileScope).toEqual({
      layers: [
        { include: ["**"], exclude: [] },
        { include: ["src/**"], exclude: [] }
      ]
    });
    store.close();
  });

  test("preserves generated prompt source provenance when a field is redacted", async () => {
    const root = temporaryRepo();
    const message = "Approve with API_TOKEN=generated-prompt-secret";
    const workflow = parseAgentFlowWorkflowOrThrow(`name: generated-prompt-provenance
version: 1
style: pipeline
maturity: experimental
policies: { sensitive_inputs: redact }
sessions:
  reviewer: { provider: fixture, authority: { can_approve: true } }
steps:
  - { id: approve, type: approval, reviewer: reviewer, artifacts: [spec.md], output: approvals/result.json }
`);
    workflow.steps[0]!.message = message;
    const sourcePrompt = createAgentFlowApprovalPrompt(
      "approve",
      "reviewer",
      ["spec.md"],
      "approvals/result.json",
      message
    );
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "generated-prompt-provenance", workflow });
    store.writeArtifact({
      id: "spec",
      runId: "generated-prompt-provenance",
      path: "spec.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Review this ordinary specification."
    });
    let captured: AgentFlowSessionProviderRequest | undefined;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => {
      captured = request;
      return {
        outputs: {
          "approvals/result.json": JSON.stringify({ status: "approved", decision: "Safe" })
        }
      };
    });

    expect((await executeAgentFlowCommandPipeline(
      store,
      "generated-prompt-provenance",
      workflow,
      undefined,
      providers
    )).status).toBe("completed");
    expect(captured!.prompt.content).toContain("API_TOKEN=[REDACTED]");
    expect(captured!.prompt.checksum).not.toBe(sourcePrompt.checksum);
    const requestArtifact = store.listArtifacts("generated-prompt-provenance")
      .find((artifact) => artifact.kind === "approval_request")!;
    const requestMetadata = JSON.parse(
      store.readArtifact("generated-prompt-provenance", requestArtifact.declaredPath).content.toString("utf8")
    );
    expect(requestMetadata.prompt).toEqual({
      path: sourcePrompt.path,
      checksum: sourcePrompt.checksum,
      providerChecksum: captured!.prompt.checksum,
      redacted: true
    });
    store.close();
  });

  test("enforces generated prompt source limits before sensitive-data redaction", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: generated-prompt-source-limit
version: 1
style: pipeline
maturity: experimental
policies: { sensitive_inputs: redact }
sessions:
  reviewer: { provider: fixture, authority: { can_approve: true } }
steps:
  - { id: approve, type: approval, reviewer: reviewer, artifacts: [spec.md], output: approvals/result.json }
`);
    workflow.steps[0]!.message = `API_TOKEN=${"x".repeat(MAX_AGENT_FLOW_SESSION_PROMPT_BYTES)}`;
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "generated-prompt-source-limit", workflow });
    store.writeArtifact({
      id: "spec",
      runId: "generated-prompt-source-limit",
      path: "spec.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Review this ordinary specification."
    });
    let invoked = false;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      invoked = true;
      return {
        outputs: {
          "approvals/result.json": JSON.stringify({ status: "approved", decision: "Safe" })
        }
      };
    });

    const result = await executeAgentFlowCommandPipeline(
      store,
      "generated-prompt-source-limit",
      workflow,
      undefined,
      providers
    );

    expect(result).toMatchObject({ status: "paused" });
    expect(result.message).toContain("session prompt limit");
    expect(invoked).toBe(false);
    store.close();
  });

  test("preflights generated prompt content during simulation", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: simulated-sensitive-generated-prompt
version: 1
style: pipeline
maturity: experimental
policies: { sensitive_inputs: deny }
sessions:
  reviewer: { provider: fixture, authority: { can_approve: true } }
steps:
  - id: approve
    type: approval
    reviewer: reviewer
    message: "API_TOKEN=generated-prompt-secret"
    artifacts: [spec.md]
    output: approvals/result.json
`);
    const fixture = {
      artifacts: { "spec.md": "Ordinary specification" },
      steps: {
        approve: {
          outputs: {
            "approvals/result.json": { status: "approved", decision: "Safe" }
          }
        }
      }
    };

    expect(simulateAgentFlowWorkflow(workflow, fixture)).toMatchObject({
      status: "paused",
      availableArtifacts: ["spec.md"],
      terminalStates: [{ stepId: "approve", status: "paused" }]
    });

    const allowedWorkflow = structuredClone(workflow);
    allowedWorkflow.policies = { sensitive_inputs: "allow" };
    expect(simulateAgentFlowWorkflow(allowedWorkflow, fixture)).toMatchObject({
      status: "completed",
      availableArtifacts: ["approvals/result.json", "spec.md"]
    });

    const oversizedWorkflow = structuredClone(allowedWorkflow);
    oversizedWorkflow.policies = { sensitive_inputs: "redact" };
    oversizedWorkflow.steps[0]!.message = "token=x\n".repeat(100_000);
    expect(simulateAgentFlowWorkflow(oversizedWorkflow, fixture)).toMatchObject({
      status: "paused",
      availableArtifacts: ["spec.md"],
      terminalStates: [{ stepId: "approve", status: "paused" }]
    });

    const oversizedSourceWorkflow = structuredClone(allowedWorkflow);
    oversizedSourceWorkflow.policies = { sensitive_inputs: "redact" };
    oversizedSourceWorkflow.steps[0]!.message =
      `API_TOKEN=${"x".repeat(MAX_AGENT_FLOW_SESSION_PROMPT_BYTES)}`;
    expect(simulateAgentFlowWorkflow(oversizedSourceWorkflow, fixture)).toMatchObject({
      status: "paused",
      availableArtifacts: ["spec.md"],
      terminalStates: [{ stepId: "approve", status: "paused" }]
    });
  });

  test("preflights sensitive session paths during simulation", () => {
    const defaultWorkflow = parseAgentFlowWorkflowOrThrow(`name: simulated-sensitive-session
version: 1
style: pipeline
maturity: experimental
sessions: { writer: { provider: fixture } }
steps:
  - { id: draft, type: session_request, session: writer, prompt: .env, inputs: [request.md], outputs: [response.md] }
`);
    const fixture = {
      artifacts: { "request.md": "Request" },
      steps: { draft: { outputs: { "response.md": "Response" } } }
    };

    expect(simulateAgentFlowWorkflow(defaultWorkflow, fixture)).toMatchObject({
      status: "paused",
      availableArtifacts: ["request.md"],
      terminalStates: [{ stepId: "draft", status: "paused" }]
    });

    const sensitiveInputWorkflow = parseAgentFlowWorkflowOrThrow(`name: simulated-sensitive-session-input
version: 1
style: pipeline
maturity: experimental
sessions: { writer: { provider: fixture } }
steps:
  - { id: draft, type: session_request, session: writer, prompt: prompt.md, inputs: [credentials.json], outputs: [response.md] }
`);
    expect(simulateAgentFlowWorkflow(sensitiveInputWorkflow, {
      artifacts: { "credentials.json": { key: "ordinary-value" } },
      steps: { draft: { outputs: { "response.md": "Response" } } }
    })).toMatchObject({
      status: "paused",
      availableArtifacts: ["credentials.json"],
      terminalStates: [{ stepId: "draft", status: "paused" }]
    });

    const allowedWorkflow = parseAgentFlowWorkflowOrThrow(`name: simulated-allowed-sensitive-session
version: 1
style: pipeline
maturity: experimental
policies: { sensitive_inputs: allow }
sessions: { writer: { provider: fixture } }
steps:
  - { id: draft, type: session_request, session: writer, prompt: .env, inputs: [request.md], outputs: [response.md] }
`);
    expect(simulateAgentFlowWorkflow(allowedWorkflow, fixture)).toMatchObject({
      status: "completed",
      availableArtifacts: ["request.md", "response.md"]
    });
  });

  test("mirrors referenced-input provenance and output-name checks during session simulation", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: simulated-referenced-session-credential
version: 1
style: pipeline
maturity: experimental
inputs: { credential: { required: true } }
sessions: { writer: { provider: fixture } }
steps:
  - { id: draft, type: session_request, session: writer, prompt: prompt.md, inputs: ["{{ inputs.credential }}"], outputs: [response.csv] }
`);
    const fixture = {
      inputs: { credential: "payload.txt" },
      artifacts: { "payload.txt": "hunter2abc" },
      steps: { draft: { outputs: { "response.csv": "status\ncomplete\n" } } }
    };

    expect(simulateAgentFlowWorkflow(workflow, fixture)).toMatchObject({
      status: "completed",
      availableArtifacts: ["payload.txt", "response.csv"]
    });

    const deniedWorkflow = structuredClone(workflow);
    deniedWorkflow.policies = { sensitive_inputs: "deny" };
    expect(simulateAgentFlowWorkflow(deniedWorkflow, fixture)).toMatchObject({
      status: "paused",
      availableArtifacts: ["payload.txt"]
    });

    const unsafeIdentityWorkflow = structuredClone(workflow);
    unsafeIdentityWorkflow.steps[0]!.id = "api_token=opaque";
    expect(simulateAgentFlowWorkflow(unsafeIdentityWorkflow, {
      ...fixture,
      steps: { "api_token=opaque": fixture.steps.draft }
    })).toMatchObject({ status: "paused", availableArtifacts: ["payload.txt"] });
  });

  test("preflights unsupported availability-only session inputs during simulation", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: simulated-unsupported-session-input
version: 1
style: pipeline
maturity: experimental
sessions: { writer: { provider: fixture } }
steps:
  - { id: prepare, type: command, command: echo, outputs: [data.csv] }
  - { id: draft, type: session_request, session: writer, prompt: prompt.md, inputs: [data.csv], outputs: [response.md] }
`);
    const fixture = {
      steps: {
        prepare: { outputs: ["data.csv"] },
        draft: { outputs: { "response.md": "Response" } }
      }
    };

    expect(simulateAgentFlowWorkflow(workflow, fixture)).toMatchObject({
      status: "paused",
      availableArtifacts: ["data.csv"],
      terminalStates: [{ stepId: "draft", status: "paused" }]
    });

    const allowedWorkflow = structuredClone(workflow);
    allowedWorkflow.policies = { sensitive_inputs: "allow" };
    expect(simulateAgentFlowWorkflow(allowedWorkflow, fixture)).toMatchObject({
      status: "completed",
      availableArtifacts: ["data.csv", "response.md"]
    });
  });

  test("simulates output collisions with runtime overwrite semantics", () => {
    const result = simulateAgentFlowWorkflow(sessionWorkflow(), {
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
      const result = simulateAgentFlowWorkflow(sessionWorkflow(), fixture);

      expect(result.status).toBe("paused");
      expect(result.visitedSteps).toContainEqual(expect.objectContaining({ id: "draft", outcome: "failed" }));
      expect(result.availableArtifacts).not.toContain("response.md");
      expect(result.availableArtifacts).not.toContain("other.md");
    }

    const multipleMissingWorkflow = parseAgentFlowWorkflowOrThrow(`name: missing-session-inputs
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: fixture }
steps:
  - { id: draft, type: session_request, session: writer, prompt: prompt.md, inputs: [first.md, second.md], outputs: [response.md], on_failure: { then: pause } }
`);
    const multipleMissing = simulateAgentFlowWorkflow(multipleMissingWorkflow, {
      steps: { draft: { outputs: { "response.md": "Response" } } }
    });
    expect(multipleMissing.status).toBe("paused");
    expect(multipleMissing.missingArtifacts).toEqual([
      { stepId: "draft", artifact: "first.md", kind: "input" },
      { stepId: "draft", artifact: "second.md", kind: "input" }
    ]);

    const unresolvedReferenceWorkflow = parseAgentFlowWorkflowOrThrow(`name: unresolved-session-input
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
      const unresolvedReference = simulateAgentFlowWorkflow(unresolvedReferenceWorkflow, {
        inputs: { failure_payload: failurePayload },
        steps: { draft: { outputs: { "response.md": "Response" } } }
      });
      expect(unresolvedReference.status).toBe("paused");
      expect(unresolvedReference.visitedSteps).toContainEqual(expect.objectContaining({ id: "draft", outcome: "failed" }));
      expect(unresolvedReference.availableArtifacts).not.toContain("response.md");
    }
  });

  test("simulates an unhandled session failure as paused", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: failed-session-simulation
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: fixture }
steps:
  - { id: draft, type: session_request, session: writer, prompt: prompt.md, inputs: [request.md], outputs: [response.md] }
`);
    const result = simulateAgentFlowWorkflow(workflow, {
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
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "session-run", workflow });
    store.writeArtifact({
      id: "request",
      runId: "session-run",
      stepId: "fixture",
      path: "request.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Bounded request\n"
    });
    const requests: AgentFlowSessionProviderRequest[] = [];
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => {
      requests.push(request);
      return {
        externalSessionId: "fixture-session-1",
        outputs: { "response.md": { content: "Deterministic response\n", contentType: "text/markdown" } },
        metadata: { fixture: true }
      };
    });

    const result = await executeAgentFlowCommandPipeline(store, "session-run", workflow, undefined, providers);

    expect(result).toMatchObject({ status: "completed", completedSteps: ["draft"] });
    expect(requests).toHaveLength(1);
    expect(requests[0]).toMatchObject({
      stepId: "draft",
      sessionId: "writer",
      provider: "fixture",
      providerKind: "fixture",
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
      providerKind: "fixture",
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
    const workflow = parseAgentFlowWorkflowOrThrow(`name: normalized-session-step
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: fixture }
steps:
  - { id: " draft ", type: " session_request ", session: writer, prompt: prompts/draft.md, inputs: [request.md], outputs: [response.md] }
`);
    expect(validateAgentFlowWorkflow(workflow)).toEqual({ valid: true, errors: [] });
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "normalized-session-step", workflow });
    store.writeArtifact({ id: "request", runId: "normalized-session-step", path: "request.md", kind: "fixture", contentType: "text/plain", content: "Request" });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => ({
      outputs: { "response.md": "Response" }
    }));

    const result = await executeAgentFlowCommandPipeline(store, "normalized-session-step", workflow, undefined, providers);

    expect(result).toMatchObject({ status: "completed", completedSteps: ["draft"] });
    expect(store.readArtifact("normalized-session-step", "response.md").content.toString()).toBe("Response");
    store.close();
  });

  test("preserves provider state when a padded session ID fails", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = parseAgentFlowWorkflowOrThrow(`name: normalized-failing-session
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: fixture }
steps:
  - { id: draft, type: session_request, session: " writer ", prompt: prompts/draft.md, inputs: [request.md], outputs: [response.md], on_failure: { then: pause } }
`);
    expect(validateAgentFlowWorkflow(workflow)).toEqual({ valid: true, errors: [] });
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "normalized-failing-session", workflow });
    store.writeArtifact({ id: "request", runId: "normalized-failing-session", path: "request.md", kind: "fixture", contentType: "text/plain", content: "Request" });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      throw new Error("provider failed");
    });

    const result = await executeAgentFlowCommandPipeline(store, "normalized-failing-session", workflow, undefined, providers);

    expect(result).toMatchObject({ status: "paused", failedStep: "draft" });
    expect(store.getSession("normalized-failing-session", "writer")).toMatchObject({
      provider: "fixture",
      status: "paused"
    });
    expect(store.listSessions("normalized-failing-session")).toHaveLength(1);
    store.close();
  });

  test("redacts adapter-native session errors before pipeline persistence", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = sessionWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "redacted-native-session-error", workflow });
    store.writeArtifact({
      id: "request",
      runId: "redacted-native-session-error",
      path: "request.md",
      kind: "fixture",
      contentType: "text/plain",
      content: "Request"
    });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      throw new AgentFlowSessionRequestError(
        "provider rejected Authorization: Bearer adapter-secret-value; api_tokens: plural-session-secret",
        "FIXTURE_REJECTED"
      );
    });

    const result = await executeAgentFlowCommandPipeline(
      store,
      "redacted-native-session-error",
      workflow,
      undefined,
      providers
    );

    expect(result.message).toContain("Authorization: Bearer [REDACTED]");
    expect(JSON.stringify(result)).not.toContain("adapter-secret-value");
    expect(JSON.stringify(store.getSession("redacted-native-session-error", "writer"))).not.toContain("adapter-secret-value");
    expect(JSON.stringify(store.listEvents("redacted-native-session-error"))).not.toContain("adapter-secret-value");
    expect(JSON.stringify(result)).not.toContain("plural-session-secret");
    expect(JSON.stringify(store.getSession("redacted-native-session-error", "writer"))).not.toContain("plural-session-secret");
    expect(JSON.stringify(store.listEvents("redacted-native-session-error"))).not.toContain("plural-session-secret");
    store.close();
  });

  test("sanitizes adapter error causes at the exported session boundary", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = sessionWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    store.createRun({
      id: "sanitized-direct-session-error",
      status: "running",
      workflow: { name: workflow.name, version: workflow.version, style: workflow.style, maturity: workflow.maturity },
      context: { workflow }
    });
    store.writeArtifact({
      id: "request",
      runId: "sanitized-direct-session-error",
      path: "request.md",
      kind: "fixture",
      contentType: "text/plain",
      content: "Request"
    });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      throw new AgentFlowSessionRequestError(
        "Authorization: Bearer direct-session-secret-value",
        "FIXTURE_REJECTED"
      );
    });

    try {
      await executeAgentFlowSessionRequest(
        store,
        "sanitized-direct-session-error",
        workflow,
        workflow.steps[0]!,
        providers
      );
      throw new Error("Expected direct session execution to fail.");
    } catch (error) {
      expect(error).toBeInstanceOf(AgentFlowSessionRequestError);
      expect((error as Error).message).toContain("Authorization: Bearer [REDACTED]");
      expect(((error as Error).cause as Error | undefined)?.message).toContain("Authorization: Bearer [REDACTED]");
      expect(JSON.stringify({
        message: (error as Error).message,
        cause: ((error as Error).cause as Error | undefined)?.message
      })).not.toContain("direct-session-secret-value");
    }
    store.close();
  });

  test("sanitizes response-processing errors at the exported session boundary", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = sessionWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    store.createRun({
      id: "sanitized-session-response-error",
      status: "running",
      workflow: { name: workflow.name, version: workflow.version, style: workflow.style, maturity: workflow.maturity },
      context: { workflow }
    });
    store.writeArtifact({
      id: "request",
      runId: "sanitized-session-response-error",
      path: "request.md",
      kind: "fixture",
      contentType: "text/plain",
      content: "Request"
    });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () =>
      new Proxy({ outputs: { "response.md": "unused" } }, {
        get(target, property, receiver) {
          if (property === "outputs") throw new Error("Authorization: Bearer session-response-secret");
          return Reflect.get(target, property, receiver);
        }
      })
    );

    try {
      await executeAgentFlowSessionRequest(
        store,
        "sanitized-session-response-error",
        workflow,
        workflow.steps[0]!,
        providers
      );
      throw new Error("Expected session response processing to fail.");
    } catch (error) {
      expect(error).toBeInstanceOf(AgentFlowSessionRequestError);
      expect((error as Error).message).toContain("Authorization: Bearer [REDACTED]");
      expect(((error as Error).cause as Error | undefined)?.message).toContain("Authorization: Bearer [REDACTED]");
      expect(JSON.stringify(store.getSession("sanitized-session-response-error", "writer")))
        .not.toContain("session-response-secret");
    }
    store.close();
  });

  test("preserves sanitized run-state codes from session response publication", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = sessionWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    store.createRun({
      id: "session-publication-race",
      status: "running",
      workflow: { name: workflow.name, version: workflow.version, style: workflow.style, maturity: workflow.maturity },
      context: { workflow }
    });
    store.writeArtifact({
      id: "request",
      runId: "session-publication-race",
      path: "request.md",
      kind: "fixture",
      contentType: "text/plain",
      content: "Request"
    });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => ({
      outputs: { "response.md": "Response" }
    }));
    store.writeArtifactsAtomically = (() => {
      throw new AgentFlowRunStateError(
        "Artifact publication raced with run state; Authorization: Bearer publication-secret",
        "AGENT_FLOW_ARTIFACT_RUN_STATUS"
      );
    }) as typeof store.writeArtifactsAtomically;

    try {
      await executeAgentFlowSessionRequest(
        store,
        "session-publication-race",
        workflow,
        workflow.steps[0]!,
        providers
      );
      throw new Error("Expected session publication to fail.");
    } catch (error) {
      expect(error).toBeInstanceOf(AgentFlowSessionRequestError);
      expect(error).toMatchObject({ code: "AGENT_FLOW_ARTIFACT_RUN_STATUS" });
      expect((error as Error).message).toContain("Authorization: Bearer [REDACTED]");
      expect(((error as Error).cause as Error | undefined)?.message)
        .toContain("Authorization: Bearer [REDACTED]");
      expect(JSON.stringify(error)).not.toContain("publication-secret");
    }
    store.close();
  });

  test("sanitizes credential-bearing artifact paths at the exported session boundary", async () => {
    const root = temporaryRepo();
    fs.writeFileSync(path.join(root, "prompt.md"), "Draft.\n");
    const credentialPath = "inputs/api_token=direct-path-secret.txt";
    const workflow = parseAgentFlowWorkflowOrThrow(`name: sanitized-direct-session-path
version: 1
style: pipeline
maturity: experimental
sessions: { writer: { provider: fixture } }
steps:
  - { id: draft, type: session_request, session: writer, prompt: prompt.md, inputs: [${credentialPath}], outputs: [out.md] }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    store.createRun({
      id: "sanitized-direct-session-path",
      status: "running",
      workflow: { name: workflow.name, version: workflow.version, style: workflow.style, maturity: workflow.maturity },
      context: { workflow }
    });
    store.writeArtifact({
      id: "credential-path-input",
      runId: "sanitized-direct-session-path",
      path: credentialPath,
      kind: "fixture",
      contentType: "text/plain",
      content: "ordinary content"
    });
    let calls = 0;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      calls += 1;
      return { outputs: { "out.md": "unexpected" } };
    });

    try {
      await executeAgentFlowSessionRequest(
        store,
        "sanitized-direct-session-path",
        workflow,
        workflow.steps[0]!,
        providers
      );
      throw new Error("Expected credential-bearing path preflight to fail.");
    } catch (error) {
      expect(error).toBeInstanceOf(AgentFlowSessionRequestError);
      expect((error as Error).message).toContain("secret-like path");
      expect(((error as Error).cause as Error | undefined)?.message).toContain("secret-like path");
      expect(JSON.stringify({
        message: (error as Error).message,
        cause: ((error as Error).cause as Error | undefined)?.message
      })).not.toContain("direct-path-secret");
    }
    expect(calls).toBe(0);
    store.close();
  });

  test("rejects secret-bearing session adapter identity metadata before invocation", async () => {
    const root = temporaryRepo();
    fs.writeFileSync(path.join(root, "prompt.md"), "Draft.\n");
    const secretStepId = "API_TOKEN=session-adapter-identity-secret";
    const workflow = parseAgentFlowWorkflowOrThrow(`name: blocked-session-adapter-identity
version: 1
style: pipeline
maturity: experimental
policies: { sensitive_inputs: deny }
sessions: { writer: { provider: fixture } }
steps:
  - id: ${JSON.stringify(secretStepId)}
    type: session_request
    session: writer
    prompt: prompt.md
    inputs: []
    outputs: [out.md]
`);
    const store = await openAgentFlowRunState({ cwd: root });
    store.createRun({
      id: "blocked-session-adapter-identity",
      status: "running",
      workflow: { name: workflow.name, version: workflow.version, style: workflow.style, maturity: workflow.maturity },
      context: { workflow }
    });
    let calls = 0;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      calls += 1;
      return { outputs: { "out.md": "unexpected" } };
    });

    await expect(executeAgentFlowSessionRequest(
      store,
      "blocked-session-adapter-identity",
      workflow,
      workflow.steps[0]!,
      providers
    )).rejects.toMatchObject({ code: "AGENT_FLOW_SENSITIVE_INPUT" });
    expect(calls).toBe(0);
    store.close();
  });

  test("rejects secret-bearing configured provider metadata before invocation", async () => {
    const root = temporaryRepo();
    fs.writeFileSync(path.join(root, "prompt.md"), "Draft.\n");
    const workflow = parseAgentFlowWorkflowOrThrow(`name: blocked-provider-metadata
version: 1
style: pipeline
maturity: experimental
policies: { sensitive_inputs: deny }
sessions: { writer: { provider: configured } }
steps:
  - { id: draft, type: session_request, session: writer, prompt: prompt.md, inputs: [in.md], outputs: [out.md] }
limits: { max_model_calls: 1 }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    store.createRun({
      id: "blocked-provider-metadata",
      status: "running",
      workflow: { name: workflow.name, version: workflow.version, style: workflow.style, maturity: workflow.maturity },
      context: { workflow }
    });
    store.writeArtifact({
      id: "input",
      runId: "blocked-provider-metadata",
      path: "in.md",
      kind: "fixture",
      contentType: "text/plain",
      content: "Input"
    });
    let calls = 0;
    const providers = createAgentFlowSessionProviderRegistry().registerConfigured({
      name: "configured",
      kind: "local",
      target: "local-target",
      driver: "openai-compatible",
      model: "API_TOKEN=configured-provider-secret",
      fingerprint: "sha256:test"
    }, () => {
      calls += 1;
      return { outputs: { "out.md": "unexpected" } };
    });

    await expect(executeAgentFlowSessionRequest(
      store,
      "blocked-provider-metadata",
      workflow,
      workflow.steps[0]!,
      providers
    )).rejects.toMatchObject({ code: "AGENT_FLOW_SENSITIVE_INPUT" });
    expect(calls).toBe(0);
    expect(store.getBudget("blocked-provider-metadata", "model:model_calls")).toBeNull();
    store.close();
  });

  test("keeps reserved frontier provider names on the frontier budget", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: reserved-frontier-budget
version: 1
style: pipeline
maturity: experimental
sessions: { writer: { provider: frontier } }
steps: []
limits: { max_model_calls: 4, max_frontier_calls: 1 }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "reserved-frontier-budget", workflow });

    reserveAgentFlowSessionModelCallBudgets(
      store, "reserved-frontier-budget", workflow, "first", "writer", "frontier", "local"
    );
    expect(store.getBudget("reserved-frontier-budget", "model:frontier_calls")).toMatchObject({ used: 1, limit: 1 });
    expect(() => reserveAgentFlowSessionModelCallBudgets(
      store, "reserved-frontier-budget", workflow, "second", "writer", "frontier", "local"
    )).toThrow('Budget "frontier_calls" would exceed its limit of 1');
    store.close();
  });

  test("sanitizes missing credential-bearing paths before session artifact reads", async () => {
    const root = temporaryRepo();
    fs.writeFileSync(path.join(root, "prompt.md"), "Draft.\n");
    const credentialPath = "inputs/api_token=missing-path-secret.txt";
    const workflow = parseAgentFlowWorkflowOrThrow(`name: sanitized-missing-session-path
version: 1
style: pipeline
maturity: experimental
sessions: { writer: { provider: fixture } }
steps:
  - { id: draft, type: session_request, session: writer, prompt: prompt.md, inputs: [${credentialPath}], outputs: [out.md] }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    store.createRun({
      id: "sanitized-missing-session-path",
      status: "running",
      workflow: { name: workflow.name, version: workflow.version, style: workflow.style, maturity: workflow.maturity },
      context: { workflow }
    });
    let calls = 0;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      calls += 1;
      return { outputs: { "out.md": "unexpected" } };
    });

    try {
      await executeAgentFlowSessionRequest(
        store,
        "sanitized-missing-session-path",
        workflow,
        workflow.steps[0]!,
        providers
      );
      throw new Error("Expected credential-bearing path preflight to fail.");
    } catch (error) {
      expect(error).toBeInstanceOf(AgentFlowSessionRequestError);
      expect((error as Error).message).not.toContain("missing-path-secret");
      expect(((error as Error).cause as Error | undefined)?.message).not.toContain("missing-path-secret");
    }
    expect(calls).toBe(0);
    store.close();
  });

  test("sanitizes credential-bearing prompt paths before session filesystem reads", async () => {
    const root = temporaryRepo();
    const credentialPath = "prompts/api_token=missing-prompt-secret.txt";
    const workflow = parseAgentFlowWorkflowOrThrow(`name: sanitized-missing-prompt-path
version: 1
style: pipeline
maturity: experimental
sessions: { writer: { provider: fixture } }
steps:
  - { id: draft, type: session_request, session: writer, prompt: ${credentialPath}, outputs: [out.md] }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    store.createRun({
      id: "sanitized-missing-prompt-path",
      status: "running",
      workflow: { name: workflow.name, version: workflow.version, style: workflow.style, maturity: workflow.maturity },
      context: { workflow }
    });
    let calls = 0;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      calls += 1;
      return { outputs: { "out.md": "unexpected" } };
    });

    try {
      await executeAgentFlowSessionRequest(
        store,
        "sanitized-missing-prompt-path",
        workflow,
        workflow.steps[0]!,
        providers
      );
      throw new Error("Expected credential-bearing prompt preflight to fail.");
    } catch (error) {
      expect(error).toBeInstanceOf(AgentFlowSessionRequestError);
      expect((error as Error).message).toContain("secret-like path");
      expect((error as Error).message).not.toContain("missing-prompt-secret");
      expect(((error as Error).cause as Error | undefined)?.message).not.toContain("missing-prompt-secret");
    }
    expect(calls).toBe(0);
    store.close();
  });

  test("bounds request metadata filenames for long valid step IDs", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const longStepId = `draft-${"x".repeat(300)}`;
    const workflow = parseAgentFlowWorkflowOrThrow(`name: long-session-step-id
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: fixture }
steps:
  - { id: "${longStepId}", type: session_request, session: writer, prompt: prompts/draft.md, inputs: [request.md], outputs: [response.md] }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "long-session-step-id", workflow });
    store.writeArtifact({ id: "request", runId: "long-session-step-id", path: "request.md", kind: "fixture", contentType: "text/plain", content: "Request" });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => ({ outputs: { "response.md": "Response" } }));

    await expect(executeAgentFlowCommandPipeline(store, "long-session-step-id", workflow, undefined, providers))
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
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "missing-fixture", workflow });
    store.writeArtifact({
      id: "request",
      runId: "missing-fixture",
      path: "request.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Request"
    });
    const providers = createAgentFlowSessionProviderRegistry().register(
      "fixture",
      createAgentFlowFixtureSessionProvider({})
    );

    const result = await executeAgentFlowCommandPipeline(store, "missing-fixture", workflow, undefined, providers);

    expect(result).toMatchObject({ status: "paused", failedStep: "draft" });
    expect(result.message).toContain("no response for step draft");
    expect(store.listArtifacts("missing-fixture")
      .filter((artifact) => artifact.kind !== "failure_payload")
      .map((artifact) => artifact.declaredPath)).toEqual(["request.md"]);
    const failure = store.listFailures("missing-fixture")[0]!;
    expect(failure.payloadPath).toMatch(/^failures\/.+\.json$/);
    expect(JSON.parse(store.readArtifact("missing-fixture", failure.payloadPath!).content.toString("utf8")))
      .toMatchObject({
        id: failure.id,
        step_id: "draft",
        step_type: "session_request",
        status: "failed",
        attempt: 1,
        exit_code: null,
        command: null,
        logs: { stdout: null, stderr: null },
        classification: "session_request_failure",
        remediation_status: null,
        path: failure.payloadPath
      });
    store.close();
  });

  test("rejects oversized provider outputs before publication", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = sessionWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "oversized-output", workflow });
    store.writeArtifact({ id: "request", runId: "oversized-output", path: "request.md", kind: "fixture", contentType: "text/plain", content: "Request" });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => ({
      outputs: { "response.md": "x".repeat(MAX_AGENT_FLOW_SESSION_OUTPUT_BYTES + 1) }
    }));

    const result = await executeAgentFlowCommandPipeline(store, "oversized-output", workflow, undefined, providers);

    expect(result).toMatchObject({ status: "paused", failedStep: "draft" });
    expect(result.message).toContain("exceeds the");
    expect(store.getArtifact("oversized-output", "response.md")).toBeNull();
    store.close();
  });

  test("rejects oversized prompts before invoking a provider", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), Buffer.alloc(MAX_AGENT_FLOW_SESSION_PROMPT_BYTES + 1, "x"));
    const workflow = sessionWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "oversized-prompt", workflow });
    store.writeArtifact({ id: "request", runId: "oversized-prompt", path: "request.md", kind: "fixture", contentType: "text/plain", content: "Request" });
    let calls = 0;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      calls += 1;
      return { outputs: { "response.md": "Response" } };
    });

    const result = await executeAgentFlowCommandPipeline(store, "oversized-prompt", workflow, undefined, providers);

    expect(result).toMatchObject({ status: "paused", failedStep: "draft" });
    expect(result.message).toContain("session prompt limit");
    expect(calls).toBe(0);
    expect(store.getSession("oversized-prompt", "writer")).toMatchObject({ status: "paused" });
    store.close();
  });

  test("rejects malformed UTF-8 prompts before invoking a provider", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), Buffer.from([0xff, ...Buffer.from("Draft")]));
    const workflow = sessionWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "malformed-prompt", workflow });
    store.writeArtifact({
      id: "request",
      runId: "malformed-prompt",
      path: "request.md",
      kind: "fixture",
      contentType: "text/plain",
      content: "Request"
    });
    let calls = 0;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      calls += 1;
      return { outputs: { "response.md": "Response" } };
    });

    const result = await executeAgentFlowCommandPipeline(store, "malformed-prompt", workflow, undefined, providers);

    expect(result).toMatchObject({ status: "paused", failedStep: "draft" });
    expect(result.message).toContain("not valid UTF-8 text");
    expect(result.message).not.toContain("�");
    expect(calls).toBe(0);
    store.close();
  });

  test("rejects oversized provider metadata before persisting request artifacts", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = sessionWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "oversized-metadata", workflow });
    store.writeArtifact({ id: "request", runId: "oversized-metadata", path: "request.md", kind: "fixture", contentType: "text/plain", content: "Request" });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => ({
      outputs: { "response.md": "Response" },
      metadata: { value: "x".repeat(MAX_AGENT_FLOW_SESSION_METADATA_BYTES + 1) }
    }));

    const result = await executeAgentFlowCommandPipeline(store, "oversized-metadata", workflow, undefined, providers);

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
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "invalid-metadata-shape", workflow });
    store.writeArtifact({ id: "request", runId: "invalid-metadata-shape", path: "request.md", kind: "fixture", contentType: "text/plain", content: "Request" });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => ({
      outputs: { "response.md": "Response" },
      metadata: [] as never
    }));

    const result = await executeAgentFlowCommandPipeline(store, "invalid-metadata-shape", workflow, undefined, providers);

    expect(result).toMatchObject({ status: "paused", failedStep: "draft" });
    expect(result.message).toContain("metadata must be a plain object");
    expect(store.listArtifacts("invalid-metadata-shape").some((artifact) => artifact.kind === "session_request")).toBe(false);
    store.close();
  });

  test("rejects session inputs whose aggregate bytes exceed the request bound", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = parseAgentFlowWorkflowOrThrow(`name: aggregate-input-bound
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: fixture }
steps:
  - { id: draft, type: session_request, session: writer, prompt: prompts/draft.md, inputs: [first.bin, second.bin], outputs: [response.md] }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "aggregate-input-bound", workflow });
    const half = `api_token: ${"x".repeat(Math.floor(MAX_AGENT_FLOW_SESSION_TOTAL_INPUT_BYTES / 2))}`;
    store.writeArtifact({ id: "first", runId: "aggregate-input-bound", path: "first.bin", kind: "fixture", contentType: "text/plain", content: half });
    store.writeArtifact({ id: "second", runId: "aggregate-input-bound", path: "second.bin", kind: "fixture", contentType: "text/plain", content: half });
    let calls = 0;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      calls += 1;
      return { outputs: { "response.md": "Response" } };
    });

    const result = await executeAgentFlowCommandPipeline(store, "aggregate-input-bound", workflow, undefined, providers);

    expect(result).toMatchObject({ status: "paused", failedStep: "draft" });
    expect(result.message).toContain("aggregate limit");
    expect(calls).toBe(0);
    store.close();
  });

  test("rejects structured inputs that exceed provider limits after redaction", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = parseAgentFlowWorkflowOrThrow(`name: post-redaction-input-bound
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: fixture }
steps:
  - { id: draft, type: session_request, session: writer, prompt: prompts/draft.md, inputs: [request.json], outputs: [response.md] }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "post-redaction-input-bound", workflow });
    const repeated = '{"token":""},'.repeat(480_000);
    const oversizedAfterRedaction = `[${repeated}{"token":""}]`;
    expect(simulateAgentFlowWorkflow(workflow, {
      artifacts: { "request.json": oversizedAfterRedaction },
      steps: { draft: { outputs: { "response.md": "Response" } } }
    })).toMatchObject({ status: "paused" });

    const aggregateWorkflow = structuredClone(workflow);
    aggregateWorkflow.steps[0]!.inputs = ["first.json", "second.json"];
    const halfRepeated = '{"token":""},'.repeat(240_000);
    const aggregatePart = `[${halfRepeated}{"token":""}]`;
    expect(simulateAgentFlowWorkflow(aggregateWorkflow, {
      artifacts: { "first.json": aggregatePart, "second.json": aggregatePart },
      steps: { draft: { outputs: { "response.md": "Response" } } }
    })).toMatchObject({ status: "paused" });
    store.writeArtifact({
      id: "request",
      runId: "post-redaction-input-bound",
      path: "request.json",
      kind: "fixture",
      contentType: "application/json",
      content: oversizedAfterRedaction
    });
    let calls = 0;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      calls += 1;
      return { outputs: { "response.md": "Response" } };
    });

    const result = await executeAgentFlowCommandPipeline(
      store,
      "post-redaction-input-bound",
      workflow,
      undefined,
      providers
    );

    expect(result).toMatchObject({ status: "paused", failedStep: "draft" });
    expect(result.message).toContain("after sensitive-data handling");
    expect(calls).toBe(0);
    store.close();
  }, 30_000);

  test("fails malformed direct-API session steps before persisting running state", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: malformed-direct-session
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: fixture }
steps:
  - { id: draft, type: session_request, session: writer, prompt: prompt.md }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    store.createRun({
      id: "malformed-direct",
      workflow: { name: workflow.name, version: workflow.version, style: workflow.style, maturity: workflow.maturity },
      context: { workflow }
    });

    const result = await executeAgentFlowCommandPipeline(store, "malformed-direct", workflow);

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
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "inactive-session", workflow });
    let calls = 0;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      calls += 1;
      return { outputs: { "response.md": "Response" } };
    });

    await expect(executeAgentFlowSessionRequest(
      store,
      "inactive-session",
      workflow,
      workflow.steps[0]!,
      providers
    )).rejects.toMatchObject({ code: "AGENT_FLOW_SESSION_RUN_STATUS" });
    expect(calls).toBe(0);
    expect(store.getSession("inactive-session", "writer")).toBeNull();
    store.close();
  });

  test("rejects Windows-absolute prompt paths at the direct runtime boundary", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: windows-absolute-prompt
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: fixture }
steps:
  - { id: draft, type: session_request, session: writer, prompt: C:/tmp/prompt.md, inputs: [request.md], outputs: [response.md] }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    store.createRun({
      id: "windows-absolute-prompt",
      status: "running",
      workflow: { name: workflow.name, version: workflow.version, style: workflow.style, maturity: workflow.maturity },
      context: { workflow }
    });
    let calls = 0;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      calls += 1;
      return { outputs: { "response.md": "Response" } };
    });

    await expect(executeAgentFlowSessionRequest(
      store,
      "windows-absolute-prompt",
      workflow,
      workflow.steps[0]!,
      providers
    )).rejects.toMatchObject({ code: "AGENT_FLOW_SESSION_PROMPT_PATH" });
    expect(calls).toBe(0);
    expect(store.getSession("windows-absolute-prompt", "writer")).toBeNull();
    store.close();
  });

  test("rejects prompt paths that traverse in-repository symbolic links", () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "config"), { recursive: true });
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "config", "master.key"), "0123456789abcdef0123456789abcdef\n");
    fs.symlinkSync(path.join("..", "config", "master.key"), path.join(root, "prompts", "draft.md"));

    expect(() => readAgentFlowSessionPrompt(root, "prompts/draft.md"))
      .toThrow("must not traverse symbolic links");
  });

  test("preserves a UTF-8 BOM so prompt content matches its checksum", () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    const bytes = Buffer.from("\uFEFFDraft.\n", "utf8");
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), bytes);

    const prompt = readAgentFlowSessionPrompt(root, "prompts/draft.md");

    expect(prompt.content).toBe("\uFEFFDraft.\n");
    expect(prompt.checksum).toBe(`sha256:${createHash("sha256").update(Buffer.from(prompt.content, "utf8")).digest("hex")}`);

    const workflow = sessionWorkflow();
    const safeJson = "\uFEFF{\"safe\":true}";
    expect(secureAgentFlowTextInput(workflow, "BOM-prefixed JSON prompt", safeJson, "prompt.json"))
      .toEqual({ value: safeJson, redacted: false });
    expect(secureAgentFlowTextInput(
      workflow,
      "BOM-prefixed JSON prompt",
      "\uFEFF{\"api_token\":\"opaque-value\"}",
      "prompt.json"
    )).toEqual({ value: '{"api_token":"[REDACTED]"}\n', redacted: true });
  });

  test("rejects canonical output collisions before reserving budget or invoking a direct provider", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = parseAgentFlowWorkflowOrThrow(`name: direct-output-alias
version: 1
style: pipeline
maturity: experimental
limits: { max_model_calls: 1 }
sessions:
  writer: { provider: fixture }
steps:
  - { id: draft, type: session_request, session: writer, prompt: prompts/draft.md, inputs: [request.md], outputs: [answer.md, dir/../answer.md] }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    store.createRun({
      id: "direct-output-alias",
      status: "running",
      workflow: { name: workflow.name, version: workflow.version, style: workflow.style, maturity: workflow.maturity },
      context: { workflow }
    });
    store.writeArtifact({ id: "request", runId: "direct-output-alias", path: "request.md", kind: "fixture", contentType: "text/plain", content: "Request" });
    let calls = 0;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      calls += 1;
      return { outputs: { "answer.md": "Answer" } };
    });

    await expect(executeAgentFlowSessionRequest(store, "direct-output-alias", workflow, workflow.steps[0]!, providers))
      .rejects.toMatchObject({ code: "AGENT_FLOW_SESSION_REQUEST_INVALID" });
    expect(calls).toBe(0);
    expect(store.getBudget("direct-output-alias", "model:model_calls")).toBeNull();
    store.close();
  });

  test("binds direct session requests to the persisted workflow definition", async () => {
    const root = temporaryRepo();
    const workflow = sessionWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    store.createRun({
      id: "persisted-workflow",
      status: "running",
      workflow: { name: workflow.name, version: workflow.version, style: workflow.style, maturity: workflow.maturity },
      context: { workflow }
    });
    let calls = 0;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      calls += 1;
      return { outputs: { "response.md": "Response" } };
    });
    const changed = structuredClone(workflow);
    changed.limits = { max_model_calls: 999 };

    await expect(executeAgentFlowSessionRequest(store, "persisted-workflow", changed, changed.steps[0]!, providers))
      .rejects.toMatchObject({ code: "AGENT_FLOW_SESSION_WORKFLOW_MISMATCH" });
    expect(calls).toBe(0);
    store.close();
  });

  test("does not let providers mutate persisted input and output declarations", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = sessionWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "mutating-provider", workflow });
    store.writeArtifact({ id: "request", runId: "mutating-provider", path: "request.md", kind: "fixture", contentType: "text/plain", content: "Request" });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => {
      request.outputs.push("extra.md");
      request.inputs.splice(0, request.inputs.length);
      return { outputs: { "response.md": "Response", "extra.md": "Extra" } };
    });

    const result = await executeAgentFlowCommandPipeline(store, "mutating-provider", workflow, undefined, providers);

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
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "invalid-direct-response", workflow });
    store.writeArtifact({ id: "request", runId: "invalid-direct-response", path: "request.md", kind: "fixture", contentType: "text/plain", content: "Request" });
    store.transitionRunWithEvent("invalid-direct-response", {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: {} }
    });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => ({ outputs: {} }));

    await expect(executeAgentFlowSessionRequest(store, "invalid-direct-response", workflow, workflow.steps[0]!, providers))
      .rejects.toMatchObject({ code: "AGENT_FLOW_SESSION_OUTPUT_INVALID" });
    expect(store.getSession("invalid-direct-response", "writer")).toMatchObject({ status: "paused" });
    store.close();
  });

  test("scopes fixture outcome attempts to each run", () => {
    const adapter = createAgentFlowFixtureSessionProvider(
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
    const workflow = parseAgentFlowWorkflowOrThrow(`name: retry-session
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
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "retry-session", workflow });
    store.writeArtifact({ id: "request", runId: "retry-session", path: "request.md", kind: "fixture", contentType: "text/plain", content: "Request" });
    const externalIds: Array<string | undefined> = [];
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => {
      externalIds.push(request.externalSessionId);
      if (request.externalSessionId === undefined) {
        return {
          externalSessionId: "provider-session",
          outputs: { "first.md": "First", "second.md": "Second" },
          metadata: { stage: "safe-first-attempt" }
        };
      }
      request.externalSessionId = "mutated-session";
      return {
        outputs: { "first.md": "First", "second.md": "Second" },
        metadata: { api_token: "secret-from-second-attempt" }
      };
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

    const result = await executeAgentFlowCommandPipeline(store, "retry-session", workflow, undefined, providers);

    expect(result).toMatchObject({ status: "completed", completedSteps: ["draft"] });
    expect(externalIds).toEqual([undefined, "provider-session"]);
    expect(store.readArtifact("retry-session", "first.md").content.toString()).toBe("First");
    expect(store.readArtifact("retry-session", "second.md").content.toString()).toBe("Second");
    const requestArtifact = store.listArtifacts("retry-session").find((artifact) => artifact.kind === "session_request")!;
    expect(JSON.parse(store.readArtifact("retry-session", requestArtifact.declaredPath).content.toString()))
      .toMatchObject({
        externalSessionId: "provider-session",
        providerMetadata: { api_token: "secret-from-second-attempt" }
      });
    const failure = store.listFailures("retry-session")[0]!;
    const failurePayload = JSON.parse(
      store.readArtifact("retry-session", failure.payloadPath!).content.toString("utf8")
    );
    expect((failurePayload.artifacts.available as string[])
      .some((artifactPath) => artifactPath.endsWith(path.posix.basename(requestArtifact.declaredPath))))
      .toBe(false);
    store.close();
  });

  test("rejects reported and returned identity switches for resumable provider sessions", async () => {
    for (const mode of ["report", "return"] as const) {
      const root = temporaryRepo();
      fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
      fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
      const workflow = parseAgentFlowWorkflowOrThrow(`name: reject-${mode}-session-switch
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: fixture, resume: true }
steps:
  - { id: first, type: session_request, session: writer, prompt: prompts/draft.md, inputs: [request.md], outputs: [first.md] }
  - { id: second, type: session_request, session: writer, prompt: prompts/draft.md, inputs: [request.md], outputs: [second.md] }
limits: { max_model_calls: 2 }
`);
      const runId = `reject-${mode}-session-switch`;
      const store = await openAgentFlowRunState({ cwd: root });
      createAgentFlowLifecycleRun(store, { id: runId, workflow });
      store.writeArtifact({
        id: "request",
        runId,
        path: "request.md",
        kind: "fixture",
        contentType: "text/plain",
        content: "Request"
      });
      const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => {
        if (request.stepId === "first") {
          return { externalSessionId: "provider-session", outputs: { "first.md": "First" } };
        }
        expect(request.externalSessionId).toBe("provider-session");
        if (mode === "report") request.reportExternalSessionId!("switched-session");
        return {
          ...(mode === "return" ? { externalSessionId: "switched-session" } : {}),
          outputs: { "second.md": "Second" }
        };
      });

      const result = await executeAgentFlowCommandPipeline(store, runId, workflow, undefined, providers);

      expect(result).toMatchObject({
        status: "paused",
        completedSteps: ["first"],
        message: expect.stringContaining("differs from the persisted ID")
      });
      expect(store.getSession(runId, "writer")).toMatchObject({
        status: "paused",
        externalSessionId: "provider-session"
      });
      store.close();
    }
  });

  test("rolls back a partially published response before continuing", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = parseAgentFlowWorkflowOrThrow(`name: atomic-session
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
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "atomic-session", workflow });
    store.writeArtifact({ id: "request", runId: "atomic-session", path: "request.md", kind: "fixture", contentType: "text/plain", content: "Request" });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => ({
      outputs: { "first.md": "First", "second.md": "Second" }
    }));
    const writeArtifact = store.writeArtifact.bind(store);
    store.writeArtifact = ((input) => {
      if (input.path === "second.md") throw new Error("simulated publication failure");
      return writeArtifact(input);
    }) as typeof store.writeArtifact;

    const result = await executeAgentFlowCommandPipeline(store, "atomic-session", workflow, undefined, providers);

    expect(result).toMatchObject({ status: "completed", completedSteps: [] });
    expect(store.listFailures("atomic-session")).toMatchObject([{
      stepId: "draft",
      retryable: false,
      payload: { attempt: 1, outcome: "continue" }
    }]);
    expect(store.listArtifacts("atomic-session").map((artifact) => artifact.declaredPath)).not.toContain("first.md");
    expect(store.listArtifacts("atomic-session").map((artifact) => artifact.declaredPath)).not.toContain("second.md");
    store.close();
  });

  test("returns cancellation immediately before atomic publication as an interruption", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = sessionWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "cancel-session", workflow });
    store.writeArtifact({ id: "request", runId: "cancel-session", path: "request.md", kind: "fixture", contentType: "text/plain", content: "Request" });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => ({ outputs: { "response.md": "Response" } }));
    const writeArtifactsAtomically = store.writeArtifactsAtomically.bind(store);
    store.writeArtifactsAtomically = ((inputs) => {
      transitionAgentFlowLifecycleRun(store, "cancel-session", "cancel");
      return writeArtifactsAtomically(inputs);
    }) as typeof store.writeArtifactsAtomically;

    const result = await executeAgentFlowCommandPipeline(store, "cancel-session", workflow, undefined, providers);

    expect(result).toMatchObject({ status: "cancelled", completedSteps: [] });
    expect(store.getSession("cancel-session", "writer")).toMatchObject({ status: "cancelled" });
    store.close();
  });

  test("returns cancellation immediately after atomic output publication as an interruption", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = sessionWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "cancel-after-batch", workflow });
    store.writeArtifact({ id: "request", runId: "cancel-after-batch", path: "request.md", kind: "fixture", contentType: "text/plain", content: "Request" });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => ({ outputs: { "response.md": "Response" } }));
    const writeArtifactsAtomically = store.writeArtifactsAtomically.bind(store);
    store.writeArtifactsAtomically = ((inputs) => {
      const result = writeArtifactsAtomically(inputs);
      transitionAgentFlowLifecycleRun(store, "cancel-after-batch", "cancel");
      return result;
    }) as typeof store.writeArtifactsAtomically;

    const result = await executeAgentFlowCommandPipeline(store, "cancel-after-batch", workflow, undefined, providers);

    expect(result).toMatchObject({ status: "cancelled", completedSteps: [] });
    expect(store.getSession("cancel-after-batch", "writer")).toMatchObject({ status: "cancelled" });
    expect(store.readArtifact("cancel-after-batch", "response.md").content.toString()).toBe("Response");
    store.close();
  });

  test("persists cancellation when a pending provider resolves", async () => {
    const { store, workflow } = await pendingProviderRun("resolve-after-cancel");
    let resolveProvider!: (response: { outputs: { "response.md": string } }) => void;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () =>
      new Promise((resolve) => { resolveProvider = resolve; })
    );
    const execution = executeAgentFlowCommandPipeline(store, "resolve-after-cancel", workflow, undefined, providers);
    await waitForProviderStart();
    transitionAgentFlowLifecycleRun(store, "resolve-after-cancel", "cancel");
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
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () =>
      new Promise((_resolve, reject) => { rejectProvider = reject; })
    );
    const execution = executeAgentFlowCommandPipeline(store, "reject-after-cancel", workflow, undefined, providers);
    await waitForProviderStart();
    transitionAgentFlowLifecycleRun(store, "reject-after-cancel", "cancel");
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
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => {
      signal = request.signal;
      return new Promise(() => {});
    });
    const execution = executeAgentFlowCommandPipeline(store, "abort-on-cancel", workflow, undefined, providers);
    await waitForProviderStart();
    transitionAgentFlowLifecycleRun(store, "abort-on-cancel", "cancel");

    await expect(execution).resolves.toMatchObject({ status: "cancelled", completedSteps: [] });
    expect(signal.aborted).toBe(true);
    expect(store.getSession("abort-on-cancel", "writer")).toMatchObject({ status: "cancelled" });
    store.close();
  });

  test("reserves model-call budgets before invoking providers", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = parseAgentFlowWorkflowOrThrow(`name: bounded-session
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
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "bounded-session", workflow });
    store.writeArtifact({ id: "request", runId: "bounded-session", path: "request.md", kind: "fixture", contentType: "text/plain", content: "Request" });
    const calls: string[] = [];
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => {
      calls.push(request.stepId);
      return { outputs: { [`${request.stepId}.md`]: request.stepId } };
    });

    const result = await executeAgentFlowCommandPipeline(store, "bounded-session", workflow, undefined, providers);

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
    const workflow = parseAgentFlowWorkflowOrThrow(`name: policy-pause-session
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
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "policy-pause-session", workflow });
    store.writeArtifact({ id: "request", runId: "policy-pause-session", path: "request.md", kind: "fixture", contentType: "text/plain", content: "Request" });
    const calls: string[] = [];
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => {
      calls.push(request.stepId);
      return { outputs: { [`${request.stepId}.md`]: request.stepId } };
    });

    const result = await executeAgentFlowCommandPipeline(store, "policy-pause-session", workflow, undefined, providers);

    expect(result).toMatchObject({ status: "paused", completedSteps: ["first"], failedStep: "second" });
    expect(result.message).toContain('Budget "model_calls" would exceed its limit of 1');
    expect(calls).toEqual(["first"]);
    store.close();
  });

  test("atomically rejects concurrent execution of the same session", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = parseAgentFlowWorkflowOrThrow(`name: concurrent-budget
version: 1
style: pipeline
maturity: experimental
limits: { max_model_calls: 1 }
sessions:
  writer: { provider: fixture }
steps:
  - { id: draft, type: session_request, session: writer, prompt: prompts/draft.md, inputs: [request.md], outputs: [response.md] }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "concurrent-budget", workflow });
    store.writeArtifact({ id: "request", runId: "concurrent-budget", path: "request.md", kind: "fixture", contentType: "text/plain", content: "Request" });
    store.transitionRunWithEvent("concurrent-budget", {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: {} }
    });
    const other = await openAgentFlowRunState({ cwd: root });
    let resolveProvider!: (response: { outputs: { "response.md": string } }) => void;
    let calls = 0;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      calls += 1;
      return new Promise((resolve) => { resolveProvider = resolve; });
    });
    const first = executeAgentFlowSessionRequest(store, "concurrent-budget", workflow, workflow.steps[0]!, providers);
    await waitForProviderStart();

    await expect(executeAgentFlowSessionRequest(other, "concurrent-budget", workflow, workflow.steps[0]!, providers))
      .rejects.toMatchObject({ code: "AGENT_FLOW_SESSION_ACTIVE" });
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
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "active-claim-conflict", workflow });
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
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      calls += 1;
      return { outputs: { "response.md": "Response" } };
    });

    await expect(executeAgentFlowCommandPipeline(store, "active-claim-conflict", workflow, undefined, providers))
      .rejects.toMatchObject({ code: "AGENT_FLOW_SESSION_ACTIVE" });
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
    const workflow = parseAgentFlowWorkflowOrThrow(`name: dynamic-session-input
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
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, {
      id: "dynamic-input",
      workflow,
      inputs: { failure_payload: "ci/../failure.json" }
    });
    store.writeArtifact({ id: "failure", runId: "dynamic-input", path: "failure.json", kind: "fixture", contentType: "application/json", content: "{}" });
    let requestedPath: string | undefined;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => {
      requestedPath = request.inputs[0]?.path;
      return { outputs: { "response.md": "Response" } };
    });

    await expect(executeAgentFlowCommandPipeline(store, "dynamic-input", workflow, undefined, providers))
      .resolves.toMatchObject({ status: "completed" });
    expect(requestedPath).toBe("failure.json");
    store.close();
  });

  test("publishes independent outputs before outputs that overwrite inputs", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = parseAgentFlowWorkflowOrThrow(`name: in-place-session-output
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: fixture }
steps:
  - { id: draft, type: session_request, session: writer, prompt: prompts/draft.md, inputs: [state.md], outputs: [state.md, summary.md], overwrite: true }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "in-place-output", workflow });
    store.writeArtifact({ id: "state", runId: "in-place-output", path: "state.md", kind: "fixture", contentType: "text/markdown", content: "Old" });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => ({
      outputs: { "state.md": "New", "summary.md": "Summary" }
    }));

    await expect(executeAgentFlowCommandPipeline(store, "in-place-output", workflow, undefined, providers))
      .resolves.toMatchObject({ status: "completed", completedSteps: ["draft"] });
    expect(store.readArtifact("in-place-output", "state.md").content.toString()).toBe("New");
    expect(store.readArtifact("in-place-output", "summary.md").content.toString()).toBe("Summary");
    store.close();
  });

  test("rejects persistent external IDs from non-resumable sessions", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
    const workflow = parseAgentFlowWorkflowOrThrow(`name: non-resumable-session
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: fixture, resume: false }
steps:
  - { id: first, type: session_request, session: writer, prompt: prompts/draft.md, inputs: [request.md], outputs: [first.md] }
  - { id: second, type: session_request, session: writer, prompt: prompts/draft.md, inputs: [request.md], outputs: [second.md] }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "non-resumable", workflow });
    store.writeArtifact({ id: "request", runId: "non-resumable", path: "request.md", kind: "fixture", contentType: "text/plain", content: "Request" });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => request.stepId === "first"
      ? { externalSessionId: "stale-id", outputs: { "first.md": "First" } }
      : { outputs: { "second.md": "Second" } });

    await expect(executeAgentFlowCommandPipeline(store, "non-resumable", workflow, undefined, providers))
      .resolves.toMatchObject({
        status: "paused",
        completedSteps: [],
        message: expect.stringContaining("non-resumable session writer")
      });
    expect(store.getSession("non-resumable", "writer")).toMatchObject({
      status: "paused",
      externalSessionId: null
    });
    store.close();
  });

  test("rolls back provider artifacts when step finalization does not commit", async () => {
    const { store, workflow } = await pendingProviderRun("atomic-session-finalization");
    store.transitionRunWithEvent("atomic-session-finalization", {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: { status: "running" } }
    });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => ({
      outputs: { "response.md": "Response" }
    }));

    await expect(executeAgentFlowSessionRequest(
      store,
      "atomic-session-finalization",
      workflow,
      workflow.steps[0]!,
      providers,
      { finalize: () => { throw new Error("simulated finalization crash"); } }
    )).rejects.toThrow("simulated finalization crash");

    expect(store.listArtifacts("atomic-session-finalization").map((artifact) => artifact.declaredPath))
      .toEqual(["request.md"]);
    expect(store.getSession("atomic-session-finalization", "writer")).toMatchObject({ status: "paused" });
    store.close();
  });
});

async function pendingProviderRun(runId: string) {
  const root = temporaryRepo();
  fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
  fs.writeFileSync(path.join(root, "prompts", "draft.md"), "Draft.\n");
  const workflow = sessionWorkflow();
  const store = await openAgentFlowRunState({ cwd: root });
  createAgentFlowLifecycleRun(store, { id: runId, workflow });
  store.writeArtifact({ id: "request", runId, path: "request.md", kind: "fixture", contentType: "text/plain", content: "Request" });
  return { store, workflow };
}

async function waitForProviderStart(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 0));
}

function sessionWorkflow() {
  return parseAgentFlowWorkflowOrThrow(`name: fixture-session
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
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "agent-flow-session-request-"));
  fs.mkdirSync(path.join(root, ".git"));
  return root;
}
