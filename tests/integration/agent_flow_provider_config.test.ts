import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import path from "node:path";
import { runCli } from "../../src/cli/router";
import {
  createAgentFlowConfiguredProviderRegistry,
  createAgentFlowLifecycleRun,
  createAgentFlowWorkflowRegistry,
  doctorAgentFlowProviderCatalog,
  executeAgentFlowCommandPipeline,
  hashAgentFlowProviderModel,
  loadAgentFlowProviderCatalog,
  providerBindingsForWorkflow,
  renderAgentFlowProviderCatalog,
  openAgentFlowRunState,
  parseAgentFlowWorkflowOrThrow,
  resumeAgentFlowCommandPipeline,
  serializeAgentFlowProviderBindings,
  validateAgentFlowWorkflow,
  type AgentFlowSessionProviderRequest
} from "../../src/runtime";

describe("Agent Flow configured providers", () => {
  test("loads global targets, resolves repo aliases, and applies kind-safe overrides", () => {
    const { repo, home, globalConfig } = configuredRepo();
    const catalog = loadAgentFlowProviderCatalog({ cwd: repo, homeDir: home, env: {} });

    expect(catalog.bindings.planner).toMatchObject({ target: "claude-main", kind: "frontier" });
    expect(catalog.bindings.drafter).toMatchObject({ target: "qwen-local", kind: "local" });
    expect(renderAgentFlowProviderCatalog(catalog)).toContain("drafter\tqwen-local\tlocal\topenai-compatible\tqwen3");
    const nested = path.join(repo, "nested", "directory");
    fs.mkdirSync(nested, { recursive: true });
    expect(loadAgentFlowProviderCatalog({ cwd: nested, homeDir: home, env: {} }).bindings.drafter)
      .toMatchObject({ target: "qwen-local" });
    expect(loadAgentFlowProviderCatalog({
      cwd: repo,
      configPath: path.relative(repo, globalConfig),
      env: {}
    }).bindings.drafter).toMatchObject({ target: "qwen-local" });
    expect(() => loadAgentFlowProviderCatalog({
      cwd: repo,
      configPath: "missing-config.yml",
      env: {}
    })).toThrow("Could not read global config");

    const repositoryControlledConfig = path.join(repo, "agent-flow", "config.yml");
    fs.mkdirSync(path.dirname(repositoryControlledConfig), { recursive: true });
    fs.writeFileSync(repositoryControlledConfig, `version: 1
targets:
  claude-main: { kind: frontier, driver: anthropic-messages, model: repository-controlled, api_key_env: ANTHROPIC_TEST_KEY, enabled: true }
`);
    expect(loadAgentFlowProviderCatalog({ cwd: repo, homeDir: home, env: { XDG_CONFIG_HOME: "" } })
      .bindings.planner.config.model).toBe("claude-test");
    expect(loadAgentFlowProviderCatalog({ cwd: repo, homeDir: home, env: { XDG_CONFIG_HOME: "." } })
      .bindings.planner.config.model).toBe("claude-test");

    const overridden = loadAgentFlowProviderCatalog({
      cwd: repo,
      homeDir: home,
      env: {},
      overrides: ["drafter=gemma-local"]
    });
    expect(overridden.bindings.drafter.target).toBe("gemma-local");
    expect(overridden.bindings.drafter.fingerprint).not.toBe(catalog.bindings.drafter.fingerprint);
    expect(() => loadAgentFlowProviderCatalog({
      cwd: repo,
      homeDir: home,
      env: {},
      overrides: ["drafter=claude-main"]
    })).toThrow("cannot resolve to frontier target");
    expect(() => loadAgentFlowProviderCatalog({
      cwd: repo,
      configPath: globalConfig,
      env: {},
      overrides: ["missing=qwen-local"]
    })).toThrow("undeclared repository alias");

    fs.writeFileSync(path.join(repo, ".agent-flow.yml"), `version: 1
providers:
  toString: { kind: local, target: qwen-local }
`);
    const prototypeNamed = loadAgentFlowProviderCatalog({
      cwd: repo,
      configPath: globalConfig,
      env: {},
      overrides: ["toString=gemma-local"]
    });
    expect(Object.hasOwn(prototypeNamed.bindings, "toString")).toBe(true);
    expect(prototypeNamed.bindings.toString.target).toBe("gemma-local");
    expect(Object.hasOwn(prototypeNamed.bindings, "hasOwnProperty")).toBe(false);
  });

  test("requires complete safe identities for programmatic configured providers", () => {
    const registry = createAgentFlowConfiguredProviderRegistry({
      globalConfigPath: "global.yml",
      repoConfigPath: ".agent-flow.yml",
      targets: {},
      providers: {},
      bindings: {}
    });
    expect(() => registry.registerConfigured({
      name: "incomplete",
      kind: "local"
    } as never, async () => ({ outputs: {} }))).toThrow("target must be a non-empty string");
    expect(() => registry.registerConfigured({
      name: "unsafe",
      kind: "local",
      target: "API_TOKEN=durable-secret",
      driver: "openai-compatible",
      model: "model",
      fingerprint: "sha256:test"
    }, async () => ({ outputs: {} }))).toThrow("non-secret identifier");
  });

  test("exposes config validation and redacted provider inspection through the CLI", async () => {
    const { repo, globalConfig } = configuredRepo();
    const validated = await captureCli(["config", "validate", "--config", globalConfig], repo);
    expect(validated).toMatchObject({ exitCode: 0 });
    expect(validated.stdout).toContain("Targets: 3");
    expect(validated.stdout).toContain("Aliases: 2");
    const listed = await captureCli(["providers", "list", "--config", globalConfig], repo);
    expect(listed).toMatchObject({ exitCode: 0 });
    expect(listed.stdout).toContain("planner\tclaude-main\tfrontier\tanthropic-messages");
    expect(listed.stdout).not.toContain("secret");
    const doctor = await captureCli(["providers", "doctor", "--config", globalConfig], repo);
    expect(doctor.exitCode).toBe(2);
    expect(doctor.stderr).toContain("planner: missing credential environment variable ANTHROPIC_TEST_KEY");

    fs.writeFileSync(globalConfig, fs.readFileSync(globalConfig, "utf8")
      .replace("model: qwen3", "model: API_TOKEN=diagnostic-secret"));
    const secretList = await captureCli(["providers", "list", "--config", globalConfig], repo);
    const secretDoctor = await captureCli(["providers", "doctor", "--config", globalConfig], repo);
    expect(secretList.stdout).toContain("[REDACTED]");
    expect(secretDoctor.stderr).toContain("[REDACTED]");
    expect(`${secretList.stdout}\n${secretDoctor.stderr}`).not.toContain("diagnostic-secret");
  });

  test("keeps offline authoring independent of machine-local target configuration", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-offline-authoring-"));
    fs.mkdirSync(path.join(repo, ".git"));
    fs.writeFileSync(path.join(repo, ".agent-flow.yml"), `version: 1
providers:
  planner: { kind: frontier, target: missing-on-this-machine }
`);
    fs.writeFileSync(path.join(repo, "workflow.yml"), `name: offline-authoring
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: check, type: command, command: "printf ok", timeout_seconds: 1 }
`);
    fs.writeFileSync(path.join(repo, "fixture.json"), JSON.stringify({
      steps: { check: { outcome: "succeeded" } }
    }));

    expect(await captureCli(["validate", path.join(repo, "workflow.yml")], repo)).toMatchObject({ exitCode: 0 });
    expect(await captureCli(["lint", path.join(repo, "workflow.yml")], repo)).toMatchObject({ exitCode: 0 });
    expect(await captureCli([
      "simulate", path.join(repo, "workflow.yml"), "--fixture", path.join(repo, "fixture.json")
    ], repo)).toMatchObject({ exitCode: 0 });

    fs.writeFileSync(path.join(repo, "prompt.md"), "Draft.\n");
    fs.writeFileSync(path.join(repo, "fixture-run.json"), JSON.stringify({
      artifacts: { "request.md": "Request" },
      steps: { draft: { outputs: { "draft.md": "Draft" } } }
    }));
    fs.writeFileSync(path.join(repo, "fixture-workflow.yml"), `name: fixture-only
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: fixture }
steps:
  - { id: draft, type: session_request, session: writer, prompt: prompt.md, inputs: [request.md], outputs: [draft.md] }
limits: { max_model_calls: 1 }
`);
    expect(await captureCli([
      "run", "fixture-workflow.yml", "--id", "fixture-only", "--fixture", "fixture-run.json"
    ], repo)).toMatchObject({ exitCode: 0 });
  });

  test("validates secret indirection, endpoint safety, strict fields, and readiness without network calls", async () => {
    const { repo, home, globalConfig } = configuredRepo();
    const catalog = loadAgentFlowProviderCatalog({ cwd: repo, homeDir: home, env: {} });
    const doctor = doctorAgentFlowProviderCatalog(catalog, { PATH: "" });
    expect(doctor.ok).toBe(false);
    expect(doctor.lines).toContain("planner: missing credential environment variable ANTHROPIC_TEST_KEY");

    fs.writeFileSync(globalConfig, `version: 1
targets:
  inherited-env:
    kind: frontier
    driver: openai-responses
    model: model
    api_key_env: toString
    enabled: true
`);
    fs.writeFileSync(path.join(repo, ".agent-flow.yml"), `version: 1
providers:
  planner: { kind: frontier, target: inherited-env }
`);
    const inheritedEnvCatalog = loadAgentFlowProviderCatalog({ cwd: repo, homeDir: home, env: {} });
    expect(doctorAgentFlowProviderCatalog(inheritedEnvCatalog, {})).toEqual({
      ok: false,
      lines: ["planner: missing credential environment variable toString"]
    });
    await expect(createAgentFlowConfiguredProviderRegistry(inheritedEnvCatalog, { env: {} })
      .get("planner")!(providerRequest(repo)))
      .rejects.toThrow("Credential environment variable toString is not set");

    fs.writeFileSync(globalConfig, `version: 1
targets:
  unsafe-model:
    kind: local
    driver: openai-compatible
    model: "safe\\u001b[2J"
    base_url: http://127.0.0.1:11434/v1
    enabled: true
`);
    expect(() => loadAgentFlowProviderCatalog({ cwd: repo, homeDir: home, env: {} }))
      .toThrow("Model identifiers must not contain control characters");

    fs.writeFileSync(globalConfig, `version: 1
targets:
  unsafe:
    kind: frontier
    driver: openai-compatible
    model: model
    base_url: http://example.com/v1
    enabled: true
`);
    expect(() => loadAgentFlowProviderCatalog({ cwd: repo, homeDir: home, env: {} }))
      .toThrow("Frontier targets require HTTPS");

    fs.writeFileSync(globalConfig, `version: 1
targets:
  api:
    kind: frontier
    driver: openai-responses
    model: model
    api_key: literal-secret
    enabled: true
`);
    expect(() => loadAgentFlowProviderCatalog({ cwd: repo, homeDir: home, env: {} }))
      .toThrow("Unknown field: api_key");

    fs.writeFileSync(globalConfig, `version: 1
targets:
  codex:
    kind: frontier
    driver: codex-cli
    model: gpt-test
    enabled: true
`);
    expect(() => loadAgentFlowProviderCatalog({ cwd: repo, homeDir: home, env: {} }))
      .toThrow("Expected one of: openai-responses, anthropic-messages, openai-compatible");

    fs.writeFileSync(globalConfig, `version: 1
targets:
  codex-local:
    kind: local
    driver: codex-cli
    model: qwen3
    enabled: true
`);
    fs.writeFileSync(path.join(repo, ".agent-flow.yml"), `version: 1
providers:
  local-coder: { kind: local, target: codex-local }
`);
    expect(() => loadAgentFlowProviderCatalog({ cwd: repo, homeDir: home, env: {} }))
      .toThrow("Expected one of: openai-responses, anthropic-messages, openai-compatible");

    fs.writeFileSync(globalConfig, `version: 1
targets:
  __proto__: { kind: local, driver: openai-compatible, base_url: http://127.0.0.1:11434/v1, model: model, enabled: true }
`);
    expect(() => loadAgentFlowProviderCatalog({ cwd: repo, homeDir: home, env: {} }))
      .toThrow("Name \"__proto__\" is reserved");

    fs.writeFileSync(globalConfig, `version: 1
targets:
  "bad\\u001b[2J": { kind: local, driver: openai-compatible, base_url: http://127.0.0.1:11434/v1, model: model, enabled: true }
`);
    expect(() => loadAgentFlowProviderCatalog({ cwd: repo, homeDir: home, env: {} }))
      .toThrow("control characters");

    fs.writeFileSync(globalConfig, "version: 1\ntargtes: {}\napi_key: literal-secret\n");
    expect(() => loadAgentFlowProviderCatalog({ cwd: repo, homeDir: home, env: {} }))
      .toThrow("Unknown fields: targtes, api_key");

    fs.writeFileSync(globalConfig, "version: 1\nworkflows: 42\n");
    expect(() => loadAgentFlowProviderCatalog({ cwd: repo, homeDir: home, env: {} }))
      .toThrow("Config workflows must be a string");
  });

  test("classifies configured frontier aliases for static budget validation", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: configured-frontier
version: 1
style: pipeline
maturity: experimental
sessions:
  planner: { provider: planning }
steps:
  - { id: plan, type: session_request, session: planner, prompt: prompt.md, inputs: [request.md], outputs: [plan.md] }
`);
    expect(validateAgentFlowWorkflow(workflow).valid).toBe(true);
    const resolved = validateAgentFlowWorkflow(workflow, (provider) => provider === "planning" ? "frontier" : undefined);
    expect(resolved.errors.map((issue) => issue.code)).toContain("workflow.policy.budget.frontier.required");
  });

  test("applies configured frontier alias budgets during simulation", async () => {
    const { repo } = configuredRepo();
    fs.writeFileSync(path.join(repo, "workflow.yml"), `name: configured-frontier-simulation
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: planner }
steps:
  - { id: first, type: session_request, session: writer, prompt: prompt.md, inputs: [request.md], outputs: [first.md] }
  - { id: second, type: session_request, session: writer, prompt: prompt.md, inputs: [request.md], outputs: [second.md] }
limits: { max_model_calls: 2, max_frontier_calls: 1 }
`);
    fs.writeFileSync(path.join(repo, "fixture.json"), JSON.stringify({
      artifacts: { "request.md": "Request" },
      steps: {
        first: { outputs: { "first.md": "First" } },
        second: { outputs: { "second.md": "Second" } }
      }
    }));

    const result = await captureCli([
      "simulate",
      path.join(repo, "workflow.yml"),
      "--fixture",
      path.join(repo, "fixture.json")
    ], repo);
    expect(result).toMatchObject({ exitCode: 0 });
    expect(result.stdout).toContain("Status: paused");
    expect(result.stdout).toContain("second: paused");
  });

  test("publishes endpoint safety constraints in the exported config schema", () => {
    const schema = JSON.parse(fs.readFileSync(
      path.join(import.meta.dir, "../../schemas/config.schema.json"),
      "utf8"
    )) as {
      $defs: { target: { allOf: Array<Record<string, unknown>> } };
    };
    const constraints = schema.$defs.target.allOf.filter((entry) => {
      const condition = entry.if as { properties?: { kind?: { const?: string } } } | undefined;
      return condition?.properties?.kind?.const === "local" || condition?.properties?.kind?.const === "frontier";
    }) as Array<{
      if: { properties: { kind: { const: string } } };
      then: { properties: { base_url: { pattern: string } } };
    }>;
    const patterns = Object.fromEntries(constraints.map((entry) => [
      entry.if.properties.kind.const,
      new RegExp(entry.then.properties.base_url.pattern)
    ]));

    expect(patterns.local?.test("http://127.0.0.1:11434/v1")).toBe(true);
    expect(patterns.local?.test("https://[::1]/v1")).toBe(true);
    expect(patterns.local?.test("http://example.com/v1")).toBe(false);
    expect(patterns.frontier?.test("https://api.example.com/v1")).toBe(true);
    expect(patterns.frontier?.test("http://api.example.com/v1")).toBe(false);
    expect(patterns.frontier?.test("https://user:secret@api.example.com/v1")).toBe(false);
    expect(patterns.frontier?.test("https://api.example.com/v1?token=secret")).toBe(false);
    expect(patterns.frontier?.test("https://api.example.com/v1#secret")).toBe(false);
  });

  test("invokes OpenAI-compatible local targets with exact structured outputs", async () => {
    let received: Record<string, unknown> | undefined;
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (_input: string | URL | Request, init?: RequestInit) => {
      received = JSON.parse(String(init?.body)) as Record<string, unknown>;
      return Response.json({
        choices: [{ finish_reason: "stop", message: { content: JSON.stringify({ outputs: { "draft.md": "Local draft\n" } }) } }]
      });
    }) as typeof fetch;
    try {
      const { repo, home, globalConfig } = configuredRepo();
      fs.writeFileSync(globalConfig, `version: 1
targets:
  qwen-local:
    kind: local
    driver: openai-compatible
    base_url: http://127.0.0.1:11434/v1
    model: qwen3
    enabled: true
`);
      fs.writeFileSync(path.join(repo, ".agent-flow.yml"), `version: 1
providers:
  drafter: { kind: local, target: qwen-local }
`);
      const registry = createAgentFlowConfiguredProviderRegistry(
        loadAgentFlowProviderCatalog({ cwd: repo, homeDir: home, env: {} })
      );
      const response = await registry.get("drafter")!(providerRequest(repo));

      expect(response.outputs).toEqual({ "draft.md": "Local draft\n" });
      expect(response.metadata).toMatchObject({
        target: "qwen-local",
        driver: "openai-compatible",
        modelHash: expect.stringMatching(/^sha256:/)
      });
      expect(received).toMatchObject({ model: "qwen3" });
      expect(received?.response_format).toMatchObject({ type: "json_schema" });

      globalThis.fetch = (async () => Response.json({
        choices: [{ finish_reason: "stop", message: { content: '{"outputs":{"__proto__":"Preserved\\n"}}' } }]
      })) as typeof fetch;
      const prototypeNamed = await registry.get("drafter")!({
        ...providerRequest(repo),
        outputs: ["__proto__"]
      });
      expect(Object.hasOwn(prototypeNamed.outputs, "__proto__")).toBe(true);
      expect(prototypeNamed.outputs.__proto__).toBe("Preserved\n");

      globalThis.fetch = (async () => Response.json({
        choices: [{ finish_reason: "length", message: { content: '{"outputs":{"draft.md":"Partial"}}' } }]
      })) as typeof fetch;
      await expect(registry.get("drafter")!(providerRequest(repo)))
        .rejects.toThrow("stopped before completing the declared outputs");

      globalThis.fetch = (async () => Response.json({
        choices: [{ finish_reason: "stop", message: { content: '{"outputs":{"draft.md":"\\ud800"}}' } }]
      })) as typeof fetch;
      await expect(registry.get("drafter")!(providerRequest(repo)))
        .rejects.toThrow("invalid Unicode scalar sequence");

      globalThis.fetch = (async () => Response.json({
        choices: [{ message: { content: '{"outputs":{"draft.md":"Apparently complete"}}' } }]
      })) as typeof fetch;
      await expect(registry.get("drafter")!(providerRequest(repo)))
        .rejects.toThrow("stopped before completing the declared outputs");
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("invokes OpenAI Responses and Anthropic Messages with env-backed credentials", async () => {
    const calls: Array<{ url: string; headers: Headers; body: Record<string, unknown> }> = [];
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (input: string | URL | Request, init?: RequestInit) => {
      const url = String(input);
      calls.push({
        url,
        headers: new Headers(init?.headers),
        body: JSON.parse(String(init?.body)) as Record<string, unknown>
      });
      if (url.includes("anthropic")) {
        return Response.json({ id: "msg_test", stop_reason: "end_turn", content: [{ type: "text", text: '{"outputs":{"draft.md":"Anthropic draft\\n"}}' }] });
      }
      return Response.json({ id: "resp_test", status: "completed", output: [{ content: [{ type: "output_text", text: '{"outputs":{"draft.md":"OpenAI draft\\n"}}' }] }] });
    }) as typeof fetch;
    try {
      const { repo, home, globalConfig } = configuredRepo();
      fs.writeFileSync(globalConfig, `version: 1
targets:
  openai: { kind: frontier, driver: openai-responses, model: gpt-test, api_key_env: OPENAI_TEST_KEY, enabled: true }
  anthropic: { kind: frontier, driver: anthropic-messages, model: claude-test, api_key_env: ANTHROPIC_TEST_KEY, enabled: true }
`);
      fs.writeFileSync(path.join(repo, ".agent-flow.yml"), `version: 1
providers:
  writer: { kind: frontier, target: openai }
  reviewer: { kind: frontier, target: anthropic }
`);
      const env = { OPENAI_TEST_KEY: "openai-secret", ANTHROPIC_TEST_KEY: "anthropic-secret" };
      const registry = createAgentFlowConfiguredProviderRegistry(
        loadAgentFlowProviderCatalog({ cwd: repo, homeDir: home, env: {} }),
        { env }
      );
      expect(await registry.get("writer")!(providerRequest(repo))).toMatchObject({ outputs: { "draft.md": "OpenAI draft\n" } });
      expect(await registry.get("reviewer")!(providerRequest(repo))).toMatchObject({ outputs: { "draft.md": "Anthropic draft\n" } });
      expect(calls[0].body).toMatchObject({ model: "gpt-test", store: false });
      expect(calls[0].headers.get("authorization")).toBe("Bearer openai-secret");
      expect(calls[1].body).toMatchObject({ model: "claude-test", max_tokens: 4096 });
      expect(calls[1].headers.get("x-api-key")).toBe("anthropic-secret");
      await expect(registry.get("writer")!({ ...providerRequest(repo), canModifyFiles: true }))
        .rejects.toThrow("cannot receive file-modification authority");

      globalThis.fetch = (async (input: string | URL | Request) => String(input).includes("anthropic")
        ? Response.json({
            stop_reason: "max_tokens",
            content: [{ type: "text", text: '{"outputs":{"draft.md":"Partial"}}' }]
          })
        : Response.json({
            status: "incomplete",
            output: [{ content: [{ type: "output_text", text: '{"outputs":{"draft.md":"Partial"}}' }] }]
          })) as typeof fetch;
      await expect(registry.get("writer")!(providerRequest(repo)))
        .rejects.toThrow("stopped before completing the declared outputs");
      await expect(registry.get("reviewer")!(providerRequest(repo)))
        .rejects.toThrow("stopped before completing the declared outputs");

      globalThis.fetch = (async (input: string | URL | Request) => String(input).includes("anthropic")
        ? Response.json({ content: [{ type: "text", text: '{"outputs":{"draft.md":"No marker"}}' }] })
        : Response.json({ output: [{ content: [{ type: "output_text", text: '{"outputs":{"draft.md":"No marker"}}' }] }] })) as typeof fetch;
      await expect(registry.get("writer")!(providerRequest(repo)))
        .rejects.toThrow("stopped before completing the declared outputs");
      await expect(registry.get("reviewer")!(providerRequest(repo)))
        .rejects.toThrow("stopped before completing the declared outputs");
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("resolves aliases for whitespace-normalized review step types", async () => {
    const { repo, globalConfig } = configuredRepo();
    fs.writeFileSync(path.join(repo, "inputs.json"), JSON.stringify({
      artifacts: { "implementation-summary.md": "Implemented.\n" }
    }));
    fs.writeFileSync(path.join(repo, "workflow.yml"), `name: padded-review
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true }
sessions:
  implementer: { provider: reviewer, role: implementer }
  reviewer:
    provider: reviewer
    role: reviewer
    authority: { can_approve: true, can_request_changes: true, can_modify_files: false }
steps:
  - id: review
    type: " review "
    reviewer: reviewer
    subject: implementer
    artifacts: [implementation-summary.md]
    outputs: [reviews/code-review.json]
policies:
  model_usage: { allowed_providers: [reviewer] }
limits: { max_model_calls: 1 }
`);
    fs.writeFileSync(path.join(repo, ".agent-flow.yml"), `version: 1
providers:
  reviewer: { kind: local, target: qwen-local }
`);
    const originalFetch = globalThis.fetch;
    let calls = 0;
    globalThis.fetch = (async () => {
      calls += 1;
      return Response.json({ choices: [{ finish_reason: "stop", message: { content: JSON.stringify({
        outputs: {
          "reviews/code-review.json": JSON.stringify({
            status: "approved",
            findings: [],
            summary: "The implementation is ready."
          })
        }
      }) } }] });
    }) as typeof fetch;
    try {
      const result = await captureCli([
        "run", "workflow.yml", "--id", "padded-review", "--config", globalConfig,
        "--fixture", "inputs.json"
      ], repo);
      expect(result).toMatchObject({ exitCode: 0 });
      expect(result.stdout).toContain("Status: completed");
      expect(calls).toBe(1);
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("runs different workflow steps on Qwen and Gemma and swaps a target per run", async () => {
    const { repo, globalConfig } = configuredRepo();
    fs.writeFileSync(path.join(repo, "draft.md"), "Draft the request.\n");
    fs.writeFileSync(path.join(repo, "review.md"), "Review the draft.\n");
    fs.writeFileSync(path.join(repo, "inputs.json"), JSON.stringify({ artifacts: { "request.md": "Write a draft." } }));
    fs.writeFileSync(path.join(repo, "workflow.yml"), `name: multi-local
version: 1
style: pipeline
maturity: experimental
sessions:
  drafter: { provider: local-drafter }
  reviewer: { provider: local-reviewer }
steps:
  - { id: draft, type: session_request, session: drafter, prompt: draft.md, inputs: [request.md], outputs: [draft-output.md] }
  - { id: review, type: session_request, session: reviewer, prompt: review.md, inputs: [draft-output.md], outputs: [review-output.md] }
`);
    fs.writeFileSync(path.join(repo, ".agent-flow.yml"), `version: 1
providers:
  local-drafter: { kind: local, target: qwen-local }
  local-reviewer: { kind: local, target: gemma-local }
`);
    const models: string[] = [];
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (_input: string | URL | Request, init?: RequestInit) => {
      const body = JSON.parse(String(init?.body)) as { model: string; messages: Array<{ content: string }> };
      models.push(body.model);
      const output = body.messages[0].content.includes("review-output.md") ? "review-output.md" : "draft-output.md";
      return Response.json({ choices: [{ finish_reason: "stop", message: { content: JSON.stringify({ outputs: { [output]: `${body.model} output\n` } }) } }] });
    }) as typeof fetch;
    try {
      const first = await captureCli([
        "run", "workflow.yml", "--id", "multi-local-default", "--config", globalConfig,
        "--fixture", "inputs.json"
      ], repo);
      expect(first).toMatchObject({ exitCode: 0 });
      expect(first.stdout).toContain("Status: completed");
      expect(models).toEqual(["qwen3", "gemma4"]);
      const store = await openAgentFlowRunState({ cwd: repo });
      const requestEvidence = store.listArtifacts("multi-local-default")
        .filter((artifact) => artifact.kind === "session_request")
        .map((artifact) => Buffer.from(store.readArtifact("multi-local-default", artifact.declaredPath).content).toString("utf8"))
        .join("\n");
      expect(requestEvidence).not.toContain("qwen3");
      expect(requestEvidence).not.toContain("gemma4");
      expect(requestEvidence).toContain("modelHash");
      expect(requestEvidence).toContain("sha256:");
      store.close();

      models.length = 0;
      const swapped = await captureCli([
        "run", "workflow.yml", "--id", "multi-local-swapped", "--config", globalConfig,
        "--provider", "local-drafter=gemma-local", "--fixture", "inputs.json"
      ], repo);
      expect(swapped).toMatchObject({ exitCode: 0 });
      expect(models).toEqual(["gemma4", "gemma4"]);
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("pins configured targets and rejects resume after model drift", async () => {
    const { repo, globalConfig } = configuredRepo();
    fs.writeFileSync(path.join(repo, "prompt.md"), "Draft.\n");
    fs.writeFileSync(path.join(repo, "inputs.json"), JSON.stringify({ artifacts: { "request.md": "Request" } }));
    fs.writeFileSync(path.join(repo, "workflow.yml"), `name: pinned-provider
version: 1
style: pipeline
maturity: experimental
sessions:
  drafter: { provider: local-drafter }
steps:
  - { id: draft, type: session_request, session: drafter, prompt: prompt.md, inputs: [request.md], outputs: [draft.md] }
  - { id: confirm, type: manual_gate, message: Continue?, options: [proceed, pause], on_proceed: done }
  - { id: done, type: command, command: "printf done" }
`);
    fs.writeFileSync(path.join(repo, ".agent-flow.yml"), `version: 1
providers:
  local-drafter: { kind: local, target: qwen-local }
`);
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async () => Response.json({
      choices: [{ finish_reason: "stop", message: { content: '{"outputs":{"draft.md":"Draft\\n"}}' } }]
    })) as typeof fetch;
    try {
      const overridden = await captureCli([
        "run", "workflow.yml", "--id", "pinned-override", "--config", globalConfig,
        "--provider", "local-drafter=gemma-local", "--fixture", "inputs.json"
      ], repo);
      expect(overridden.exitCode).toBe(3);
      const resumedOverride = await captureCli([
        "resume", "pinned-override", "--outcome", "proceed", "--config", globalConfig
      ], repo);
      expect(resumedOverride).toMatchObject({ exitCode: 0 });

      const started = await captureCli([
        "run", "workflow.yml", "--id", "pinned", "--config", globalConfig, "--fixture", "inputs.json"
      ], repo);
      expect(started.exitCode).toBe(3);
      fs.unlinkSync(path.join(repo, "inputs.json"));
      const config = fs.readFileSync(globalConfig, "utf8").replace("model: qwen3", "model: qwen3:changed");
      fs.writeFileSync(globalConfig, config);
      const resumed = await captureCli([
        "resume", "pinned", "--outcome", "proceed", "--config", globalConfig
      ], repo);
      expect(resumed).toMatchObject({ exitCode: 2 });
      expect(resumed.stderr).toContain("provider configuration changed");
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("persists only a hash when a configured model identifier contains sensitive text", async () => {
    const { repo, globalConfig } = configuredRepo();
    fs.writeFileSync(globalConfig, fs.readFileSync(globalConfig, "utf8")
      .replace("model: qwen3", "model: API_TOKEN=pinned-binding-secret"));
    fs.writeFileSync(path.join(repo, "prompt.md"), "Draft.\n");
    fs.writeFileSync(path.join(repo, "inputs.json"), JSON.stringify({ artifacts: { "request.md": "Request" } }));
    fs.writeFileSync(path.join(repo, "workflow.yml"), `name: private-provider-binding
version: 1
style: pipeline
maturity: experimental
policies: { sensitive_inputs: deny }
sessions: { writer: { provider: drafter } }
steps:
  - { id: draft, type: session_request, session: writer, prompt: prompt.md, inputs: [request.md], outputs: [draft.md] }
limits: { max_model_calls: 1 }
`);

    const result = await captureCli([
      "run", "workflow.yml", "--id", "private-provider-binding", "--config", globalConfig,
      "--fixture", "inputs.json"
    ], repo);
    expect(result).toMatchObject({ exitCode: 3 });
    expect(result.stderr).toContain("sensitive");
    const store = await openAgentFlowRunState({ cwd: repo });
    const persisted = JSON.stringify(store.getRun("private-provider-binding")?.context.providerBindings);
    expect(persisted).not.toContain("pinned-binding-secret");
    expect(persisted).toContain("modelHash");
    expect(persisted).toContain("sha256:");
    expect(store.getBudget("private-provider-binding", "model:model_calls")).toBeNull();
    store.close();
  });

  test("pins configured providers for programmatic execution and rejects resume drift", async () => {
    const { repo, home, globalConfig } = configuredRepo();
    fs.writeFileSync(path.join(repo, "prompt.md"), "Draft.\n");
    const workflow = parseAgentFlowWorkflowOrThrow(`name: programmatic-provider-pinning
version: 1
style: pipeline
maturity: experimental
sessions: { writer: { provider: drafter } }
steps:
  - { id: draft, type: session_request, session: writer, prompt: prompt.md, inputs: [request.md], outputs: [draft.md] }
  - { id: confirm, type: manual_gate, message: Continue?, options: [proceed, pause] }
limits: { max_model_calls: 1 }
`);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "programmatic-provider-pinning", workflow });
    store.writeArtifact({
      id: "programmatic-provider-input",
      runId: "programmatic-provider-pinning",
      stepId: "fixture",
      path: "request.md",
      kind: "fixture",
      contentType: "text/markdown; charset=utf-8",
      content: "Request\n"
    });
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async () => Response.json({
      choices: [{ finish_reason: "stop", message: { content: '{"outputs":{"draft.md":"Draft\\n"}}' } }]
    })) as typeof fetch;
    try {
      const providers = createAgentFlowConfiguredProviderRegistry(
        loadAgentFlowProviderCatalog({ cwd: repo, homeDir: home, env: {} })
      );
      expect(await executeAgentFlowCommandPipeline(
        store, "programmatic-provider-pinning", workflow, undefined, providers
      )).toMatchObject({ status: "paused", message: expect.stringContaining("Manual gate confirm") });
      const persisted = JSON.stringify(store.getRun("programmatic-provider-pinning")?.context.providerBindings);
      expect(persisted).toContain("modelHash");
      expect(persisted).not.toContain("qwen3");

      fs.writeFileSync(globalConfig, fs.readFileSync(globalConfig, "utf8")
        .replace("model: qwen3", "model: gemma4"));
      const changedProviders = createAgentFlowConfiguredProviderRegistry(
        loadAgentFlowProviderCatalog({ cwd: repo, homeDir: home, env: {} })
      );
      await expect(resumeAgentFlowCommandPipeline(
        store,
        "programmatic-provider-pinning",
        workflow,
        { outcome: "proceed" },
        undefined,
        changedProviders
      )).rejects.toMatchObject({ code: "AGENT_FLOW_PROVIDER_CONFIG_DRIFT" });
      expect(store.getRun("programmatic-provider-pinning")?.status).toBe("paused");
    } finally {
      globalThis.fetch = originalFetch;
      store.close();
    }
  });

  test("pins configured providers used only by a nested recovery workflow", async () => {
    const { repo, home } = configuredRepo();
    fs.writeFileSync(path.join(repo, "prompt.md"), "Repair the failure.\n");
    const child = parseAgentFlowWorkflowOrThrow(`name: configured-nested-recovery
version: 1
style: recovery_pipeline
maturity: experimental
inputs: { request: { required: true } }
sessions: { writer: { provider: drafter } }
steps:
  - { id: repair, type: session_request, session: writer, prompt: prompt.md, inputs: ["{{ inputs.request }}"], outputs: [repair.md] }
  - { id: remediated, type: result, status: remediated }
limits: { max_model_calls: 1 }
`);
    const parent = parseAgentFlowWorkflowOrThrow(`name: configured-nested-parent
version: 1
style: recovery_pipeline
maturity: experimental
inputs: { request: { required: true } }
steps:
  - id: check
    type: command
    command: "false"
    on_failure:
      route_to:
        workflow: nested
        inputs: { request: "{{ inputs.request }}" }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
  - { id: complete, type: result, status: completed }
  - { id: pause, type: result, status: paused }
`);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, {
      id: "configured-nested-parent",
      workflow: parent,
      inputs: { request: "request.md" }
    });
    store.writeArtifact({
      id: "configured-nested-input",
      runId: "configured-nested-parent",
      stepId: "fixture",
      path: "request.md",
      kind: "fixture",
      contentType: "text/plain; charset=utf-8",
      content: "Repair this request."
    });
    const providers = createAgentFlowConfiguredProviderRegistry(
      loadAgentFlowProviderCatalog({ cwd: repo, homeDir: home, env: {} })
    );
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async () => Response.json({
      choices: [{ finish_reason: "stop", message: { content: '{"outputs":{"repair.md":"Repaired\\n"}}' } }]
    })) as typeof fetch;
    try {
      const nestedResult = await executeAgentFlowCommandPipeline(
        store,
        "configured-nested-parent",
        parent,
        undefined,
        providers,
        undefined,
        undefined,
        createAgentFlowWorkflowRegistry().register("nested", child)
      );
      expect(nestedResult).toMatchObject({ status: "completed" });
      const recoveryRunId = store.listEvents("configured-nested-parent")
        .find((event) => event.type === "recovery.completed")?.payload.recoveryRunId;
      expect(typeof recoveryRunId).toBe("string");
      const bindings = store.getRun(recoveryRunId as string)?.context.providerBindings;
      expect(bindings).toMatchObject({
        drafter: {
          target: "qwen-local",
          kind: "local",
          driver: "openai-compatible",
          modelHash: hashAgentFlowProviderModel("qwen3")
        }
      });
      expect(JSON.stringify(bindings)).not.toContain("qwen3");
    } finally {
      globalThis.fetch = originalFetch;
      store.close();
    }
  });

  test("recovers an interrupted CLI run with its pinned provider override", async () => {
    const { repo, home, globalConfig } = configuredRepo();
    fs.writeFileSync(path.join(repo, "prompt.md"), "Draft.\n");
    fs.writeFileSync(path.join(repo, "inputs.json"), JSON.stringify({
      artifacts: { "request.md": "Request" }
    }));
    fs.writeFileSync(path.join(repo, "workflow.yml"), `name: interrupted-provider-override
version: 1
style: pipeline
maturity: experimental
sessions: { writer: { provider: drafter } }
steps:
  - { id: draft, type: session_request, session: writer, prompt: prompt.md, inputs: [request.md], outputs: [draft.md] }
limits: { max_model_calls: 1 }
`);
    const workflow = parseAgentFlowWorkflowOrThrow(fs.readFileSync(path.join(repo, "workflow.yml"), "utf8"));
    const overriddenCatalog = loadAgentFlowProviderCatalog({
      cwd: repo,
      homeDir: home,
      env: {},
      overrides: ["drafter=gemma-local"]
    });
    const providerBindings = serializeAgentFlowProviderBindings(
      providerBindingsForWorkflow(workflow, overriddenCatalog)
    );
    const interrupted = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(interrupted, {
      id: "interrupted-provider-override",
      workflow,
      context: { providerBindings }
    });
    interrupted.writeArtifact({
      id: "interrupted-provider-input",
      runId: "interrupted-provider-override",
      stepId: "fixture",
      path: "request.md",
      kind: "fixture",
      contentType: "text/plain; charset=utf-8",
      content: "Request"
    });
    interrupted.acquireRunLock("interrupted-provider-override", "run", { ttlMs: 60_000 });
    interrupted.transitionRunWithEvent("interrupted-provider-override", {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: { status: "running" } }
    });
    interrupted.close();

    const models: string[] = [];
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (_input: string | URL | Request, init?: RequestInit) => {
      models.push((JSON.parse(String(init?.body)) as { model: string }).model);
      return Response.json({
        choices: [{ finish_reason: "stop", message: { content: '{"outputs":{"draft.md":"Draft\\n"}}' } }]
      });
    }) as typeof fetch;
    try {
      const conflicting = await captureCli([
        "run", "workflow.yml", "--id", "interrupted-provider-override", "--config", globalConfig,
        "--provider", "drafter=qwen-local", "--fixture", "inputs.json"
      ], repo);
      expect(conflicting).toMatchObject({ exitCode: 2 });
      expect(conflicting.stderr).toContain("cannot replace pinned provider");
      expect(models).toEqual([]);

      const recovered = await captureCli([
        "run", "workflow.yml", "--id", "interrupted-provider-override", "--config", globalConfig,
        "--fixture", "inputs.json"
      ], repo);
      expect(recovered).toMatchObject({ exitCode: 0 });
      expect(recovered.stdout).toContain("Reused Agent Flow run interrupted-provider-override");
      expect(models).toEqual(["gemma4"]);
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("ignores fixture response shapes for configured-provider steps on run and resume", async () => {
    const { repo, globalConfig } = configuredRepo();
    fs.writeFileSync(path.join(repo, "prompt.md"), "Draft.\n");
    fs.writeFileSync(path.join(repo, "inputs.json"), JSON.stringify({
      artifacts: { "request.md": "Request" },
      steps: { draft: { outputs: ["draft.md"] } }
    }));
    fs.writeFileSync(path.join(repo, "workflow.yml"), `name: configured-fixture-inputs
version: 1
style: pipeline
maturity: experimental
sessions: { writer: { provider: drafter } }
steps:
  - { id: draft, type: session_request, session: writer, prompt: prompt.md, inputs: [request.md], outputs: [draft.md] }
  - { id: confirm, type: manual_gate, message: Continue?, options: [proceed, pause] }
`);
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async () => Response.json({
      choices: [{ finish_reason: "stop", message: { content: '{"outputs":{"draft.md":"Draft\\n"}}' } }]
    })) as typeof fetch;
    try {
      expect(await captureCli([
        "run", "workflow.yml", "--id", "configured-fixture-inputs", "--config", globalConfig,
        "--fixture", "inputs.json"
      ], repo)).toMatchObject({ exitCode: 3 });
      expect(await captureCli([
        "resume", "configured-fixture-inputs", "--outcome", "proceed", "--config", globalConfig,
        "--fixture", "inputs.json"
      ], repo)).toMatchObject({ exitCode: 0 });
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("preflights configured HTTP capabilities before reserving model budget", async () => {
    const { repo, globalConfig } = configuredRepo();
    fs.writeFileSync(path.join(repo, "prompt.md"), "Draft.\n");
    fs.writeFileSync(path.join(repo, "inputs.json"), JSON.stringify({ artifacts: { "request.md": "Request" } }));
    fs.writeFileSync(path.join(repo, "workflow.yml"), `name: configured-capability-preflight
version: 1
style: pipeline
maturity: experimental
sessions: { writer: { provider: drafter, resume: true } }
steps:
  - { id: draft, type: session_request, session: writer, prompt: prompt.md, inputs: [request.md], outputs: [draft.md] }
limits: { max_model_calls: 1 }
`);
    let calls = 0;
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async () => {
      calls += 1;
      return Response.json({ choices: [] });
    }) as typeof fetch;
    try {
      const result = await captureCli([
        "run", "workflow.yml", "--id", "configured-capability-preflight", "--config", globalConfig,
        "--fixture", "inputs.json"
      ], repo);
      expect(result).toMatchObject({ exitCode: 3 });
      expect(result.stderr).toContain("does not support conversational resume");
      expect(calls).toBe(0);
      const store = await openAgentFlowRunState({ cwd: repo });
      expect(store.getBudget("configured-capability-preflight", "model:model_calls")).toBeNull();
      expect(store.getSession("configured-capability-preflight", "writer")).toMatchObject({
        status: "paused",
        startedAt: null
      });
      store.close();
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("preflights configured credentials before reserving frontier budget", async () => {
    const { repo, globalConfig } = configuredRepo();
    fs.writeFileSync(path.join(repo, "prompt.md"), "Plan.\n");
    fs.writeFileSync(path.join(repo, "inputs.json"), JSON.stringify({ artifacts: { "request.md": "Request" } }));
    fs.writeFileSync(path.join(repo, "workflow.yml"), `name: configured-credential-preflight
version: 1
style: pipeline
maturity: experimental
sessions: { writer: { provider: planner } }
steps:
  - { id: plan, type: session_request, session: writer, prompt: prompt.md, inputs: [request.md], outputs: [plan.md] }
limits: { max_model_calls: 1, max_frontier_calls: 1 }
`);
    let calls = 0;
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async () => {
      calls += 1;
      return Response.json({ content: [] });
    }) as typeof fetch;
    try {
      const result = await captureCli([
        "run", "workflow.yml", "--id", "configured-credential-preflight", "--config", globalConfig,
        "--fixture", "inputs.json"
      ], repo);
      expect(result).toMatchObject({ exitCode: 3 });
      expect(result.stderr).toContain("ANTHROPIC_TEST_KEY");
      expect(calls).toBe(0);
      const store = await openAgentFlowRunState({ cwd: repo });
      expect(store.getBudget("configured-credential-preflight", "model:model_calls")).toBeNull();
      expect(store.getBudget("configured-credential-preflight", "model:frontier_calls")).toBeNull();
      expect(store.getSession("configured-credential-preflight", "writer")).toMatchObject({
        status: "paused",
        startedAt: null
      });
      store.close();
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("preflights configured provider text encoding before reserving model budget", async () => {
    const { repo, home } = configuredRepo();
    fs.writeFileSync(path.join(repo, "prompt.md"), "Draft.\n");
    const workflow = parseAgentFlowWorkflowOrThrow(`name: configured-encoding-preflight
version: 1
style: pipeline
maturity: experimental
policies: { sensitive_inputs: allow }
sessions: { writer: { provider: drafter } }
steps:
  - { id: draft, type: session_request, session: writer, prompt: prompt.md, inputs: [request.bin], outputs: [draft.md] }
limits: { max_model_calls: 1 }
`);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "configured-encoding-preflight", workflow });
    store.writeArtifact({
      id: "binary-input",
      runId: "configured-encoding-preflight",
      path: "request.bin",
      kind: "fixture",
      contentType: "application/octet-stream",
      content: Uint8Array.from([0xff, 0xfe])
    });
    let calls = 0;
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async () => {
      calls += 1;
      return Response.json({ choices: [] });
    }) as typeof fetch;
    try {
      const providers = createAgentFlowConfiguredProviderRegistry(
        loadAgentFlowProviderCatalog({ cwd: repo, homeDir: home, env: {} })
      );
      const result = await executeAgentFlowCommandPipeline(
        store,
        "configured-encoding-preflight",
        workflow,
        undefined,
        providers
      );
      expect(result).toMatchObject({ status: "paused", failedStep: "draft" });
      expect(result.message).toContain("not valid UTF-8");
      expect(calls).toBe(0);
      expect(store.getBudget("configured-encoding-preflight", "model:model_calls")).toBeNull();
    } finally {
      globalThis.fetch = originalFetch;
      store.close();
    }
  });

  test("preflights configured providers used only by recovery routes before creating a run", async () => {
    const { repo, globalConfig } = configuredRepo();
    fs.writeFileSync(path.join(repo, "prompt.md"), "Repair the failure.\n");
    fs.writeFileSync(path.join(repo, "workflow.yml"), `name: recovery-provider-preflight
version: 1
style: recovery_pipeline
maturity: experimental
sessions:
  fixer: { provider: missing-repairer }
steps:
  - id: fail
    type: command
    command: "false"
    timeout_seconds: 1
    on_failure:
      route_to: { session: fixer, prompt: prompt.md }
      on_remediated: { return_to: fail }
      on_unresolved: { then: fail }
limits: { max_model_calls: 1, max_recovery_cycles: 1 }
`);

    const result = await captureCli([
      "run", "workflow.yml", "--id", "recovery-preflight", "--config", globalConfig
    ], repo);
    expect(result).toMatchObject({ exitCode: 1 });
    expect(result.stderr).toContain("missing-repairer");
    const store = await openAgentFlowRunState({ cwd: repo });
    expect(store.getRun("recovery-preflight")).toBeNull();
    store.close();
  });
});

function configuredRepo(): { repo: string; home: string; globalConfig: string } {
  const root = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-providers-"));
  const repo = path.join(root, "repo");
  const home = path.join(root, "home");
  const globalConfig = path.join(home, ".config", "agent-flow", "config.yml");
  fs.mkdirSync(repo, { recursive: true });
  fs.mkdirSync(path.join(repo, ".git"));
  fs.mkdirSync(path.dirname(globalConfig), { recursive: true });
  fs.writeFileSync(globalConfig, `version: 1
targets:
  claude-main: { kind: frontier, driver: anthropic-messages, model: claude-test, api_key_env: ANTHROPIC_TEST_KEY, enabled: true }
  qwen-local: { kind: local, driver: openai-compatible, base_url: http://127.0.0.1:11434/v1, model: qwen3, enabled: true }
  gemma-local: { kind: local, driver: openai-compatible, base_url: http://127.0.0.1:11434/v1, model: gemma4, enabled: true }
`);
  fs.writeFileSync(path.join(repo, ".agent-flow.yml"), `version: 1
providers:
  planner: { kind: frontier, target: claude-main }
  drafter: { kind: local, target: qwen-local }
`);
  return { repo, home, globalConfig };
}

function providerRequest(repoRoot: string): AgentFlowSessionProviderRequest {
  return {
    runId: "run",
    stepId: "draft",
    sessionId: "writer",
    provider: "drafter",
    providerKind: "local",
    resume: false,
    prompt: { path: "prompt.md", content: "Draft the requested text.", checksum: "sha256:prompt" },
    inputs: [],
    outputs: ["draft.md"],
    repoRoot,
    canModifyFiles: false,
    fileScope: { layers: [] },
    signal: new AbortController().signal
  };
}

async function captureCli(args: string[], cwd: string): Promise<{ exitCode: number; stdout: string; stderr: string }> {
  let stdout = "";
  let stderr = "";
  const exitCode = await runCli(args, {
    stdout: { write: (value) => { stdout += String(value); return true; } },
    stderr: { write: (value) => { stderr += String(value); return true; } }
  }, { cwd, env: {} });
  return { exitCode, stdout, stderr };
}
