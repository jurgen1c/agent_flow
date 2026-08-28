import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { runCli } from "../../src/cli/router";
import {
  buildAgentFlowRunActionSnapshot,
  createAgentFlowConfiguredProviderAdapter,
  createAgentFlowConfiguredProviderRegistry,
  createAgentFlowLifecycleRun,
  createAgentFlowSessionProviderRegistry,
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
import {
  runAgentFlowNativeProviderDoctorProbe,
  selectedCodexProviderEnvironment
} from "../../src/runtime/provider_config";
import { nativeExecutableMountPaths } from "../../src/runtime/provider_drivers";

describe("Agent Flow configured providers", () => {
  test("fails closed when programmatic bindings contain an unsupported driver", () => {
    expect(() => createAgentFlowConfiguredProviderAdapter({
      alias: "future",
      target: "future",
      kind: "frontier",
      fingerprint: "sha256:test",
      config: {
        kind: "frontier",
        driver: "future-driver",
        model: "future-model",
        enabled: true
      }
    } as never)).toThrow("Unsupported configured provider driver");
  });

  test("copies selected Codex credentials without prototype-bearing names", () => {
    const source = Object.create(null) as Record<string, string>;
    source.SAFE_PROVIDER_KEY = "  provider-secret\n";
    source.EMPTY_PROVIDER_KEY = " \t\n";
    source.__proto__ = "prototype-secret";
    const selected = selectedCodexProviderEnvironment(source, ["SAFE_PROVIDER_KEY", "EMPTY_PROVIDER_KEY"]);

    expect(selected).toEqual({ SAFE_PROVIDER_KEY: "provider-secret" });
    expect(Object.getPrototypeOf(selected)).toBeNull();
    expect(() => selectedCodexProviderEnvironment(source, ["__proto__"]))
      .toThrow("credential references must use canonical environment variable names");
  });

  test("bounds native CLI doctor probes", () => {
    if (process.platform !== "linux") return;

    const root = fs.mkdtempSync(path.join(os.tmpdir(), "agent-flow-doctor-timeout-"));
    try {
      const bin = path.join(root, "bin");
      fs.mkdirSync(bin);
      const codex = path.join(bin, "codex");
      fs.writeFileSync(codex, "#!/bin/sh\nexec /bin/sleep 30\n");
      fs.chmodSync(codex, 0o755);
      const startedAt = Date.now();
      const result = runAgentFlowNativeProviderDoctorProbe(
        codex,
        ["--version"],
        { PATH: `${bin}:/usr/bin:/bin` },
        50
      );

      const elapsed = Date.now() - startedAt;
      expect(elapsed).toBeGreaterThanOrEqual(40);
      expect(elapsed).toBeLessThan(2_000);
      expect((result.error as NodeJS.ErrnoException | undefined)?.code).toBe("ETIMEDOUT");
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  test("uses the configured HOME for asdf tool-version mounts", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "agent-flow-asdf-home-"));
    try {
      const home = path.join(root, "configured-home");
      const bin = path.join(root, "bin");
      const shims = path.join(home, ".asdf", "shims");
      fs.mkdirSync(bin);
      fs.mkdirSync(shims, { recursive: true });
      const executable = path.join(bin, "codex");
      const interpreter = path.join(shims, "node");
      const asdf = path.join(bin, "asdf");
      const toolVersions = path.join(home, ".tool-versions");
      fs.writeFileSync(executable, "#!/usr/bin/env node\n");
      fs.writeFileSync(interpreter, "#!/bin/sh\n");
      fs.writeFileSync(asdf, "#!/bin/sh\n");
      fs.writeFileSync(toolVersions, "nodejs 24.0.0\n");
      for (const candidate of [executable, interpreter, asdf]) fs.chmodSync(candidate, 0o755);

      expect(nativeExecutableMountPaths(
        "codex",
        executable,
        `${shims}${path.delimiter}${bin}`,
        home
      )).toContain(toolVersions);
      expect(nativeExecutableMountPaths(
        "codex",
        executable,
        shims,
        home
      )).toEqual([executable, path.join(home, ".asdf"), toolVersions]);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  test("loads global targets, resolves repo aliases, and applies kind-safe overrides", () => {
    const { repo, home, globalConfig } = configuredRepo();
    const catalog = loadAgentFlowProviderCatalog({ cwd: repo, homeDir: home, env: {} });

    expect(catalog.bindings.planner).toMatchObject({ target: "claude-main", kind: "frontier" });
    expect(catalog.bindings.drafter).toMatchObject({ target: "qwen-local", kind: "local" });
    const legacyTargetIdentity = JSON.stringify({
      kind: "local",
      driver: "openai-compatible",
      model: "qwen3",
      api_key_env: null,
      endpoint: createHash("sha256").update("http://127.0.0.1:11434/v1").digest("hex"),
      max_output_tokens: null
    });
    expect(catalog.bindings.drafter.fingerprint)
      .toBe(`sha256:${createHash("sha256").update(legacyTargetIdentity).digest("hex")}`);
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
    expect(() => registry.registerConfigured({
      name: "local-native",
      kind: "local",
      target: "codex",
      driver: "codex-cli",
      fingerprint: "sha256:test"
    }, async () => ({ outputs: {} }))).toThrow("must declare frontier kind");
    expect(() => registry.registerConfigured({
      name: "native-without-model",
      kind: "frontier",
      target: "codex",
      driver: "codex-cli",
      fingerprint: "sha256:test"
    } as never, async () => ({ outputs: {} }))).toThrow("Configured session provider model must be a non-empty string");
    expect(() => registry.registerConfigured({
      name: "unsafe-profile",
      kind: "frontier",
      target: "codex",
      driver: "codex-cli",
      model: "codex-test",
      profile: "../unsafe",
      fingerprint: "sha256:test"
    }, async () => ({ outputs: {} }))).toThrow("profile may contain only letters");
    expect(() => registry.registerConfigured({
      name: "unsupported-effort",
      kind: "frontier",
      target: "codex",
      driver: "codex-cli",
      model: "codex-test",
      reasoningEffort: "extreme",
      fingerprint: "sha256:test"
    }, async () => ({ outputs: {} }))).toThrow("reasoning effort must be one of");
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

    const secretIdentifier = "ghp_abcdefghijklmnopqrstuvwxyz1234567890";
    fs.writeFileSync(globalConfig, `version: 1
targets:
  ${secretIdentifier}: { kind: local, driver: openai-compatible, model: model, base_url: http://127.0.0.1:11434/v1, enabled: true }
`);
    let secretIdentifierError: unknown;
    try {
      loadAgentFlowProviderCatalog({ cwd: repo, homeDir: home, env: {} });
    } catch (error) {
      secretIdentifierError = error;
    }
    expect(String(secretIdentifierError)).toContain("Names must be non-secret");
    expect(String(secretIdentifierError)).not.toContain(secretIdentifier);

    fs.writeFileSync(globalConfig, `version: 1
targets:
  qwen-local: { kind: local, driver: openai-compatible, model: model, base_url: http://127.0.0.1:11434/v1, enabled: true }
`);
    fs.writeFileSync(path.join(repo, ".agent-flow.yml"), `version: 1
providers:
  ${secretIdentifier}: { kind: local, target: qwen-local }
`);
    secretIdentifierError = undefined;
    try {
      loadAgentFlowProviderCatalog({ cwd: repo, homeDir: home, env: {} });
    } catch (error) {
      secretIdentifierError = error;
    }
    expect(String(secretIdentifierError)).toContain("Names must be non-secret");
    expect(String(secretIdentifierError)).not.toContain(secretIdentifier);

    fs.writeFileSync(path.join(repo, ".agent-flow.yml"), `version: 1
providers:
  drafter: { kind: local, target: qwen-local }
`);
    for (const baseUrl of ["http://127.0.0.1:11434/v1?", "http://127.0.0.1:11434/v1#"]) {
      fs.writeFileSync(globalConfig, `version: 1
targets:
  qwen-local: { kind: local, driver: openai-compatible, model: model, base_url: "${baseUrl}", enabled: true }
`);
      expect(() => loadAgentFlowProviderCatalog({ cwd: repo, homeDir: home, env: {} }))
        .toThrow("base_url must not contain credentials, a query, or a fragment");
    }

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
    enabled: true
`);
    fs.writeFileSync(path.join(repo, ".agent-flow.yml"), `version: 1
providers:
  coder: { kind: frontier, target: codex }
`);
    expect(() => loadAgentFlowProviderCatalog({ cwd: repo, homeDir: home, env: {} }))
      .toThrow("Value must be a canonical non-empty string");

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
      .toThrow("codex-cli targets must be frontier");

    fs.writeFileSync(globalConfig, `version: 1
targets:
  claude:
    kind: frontier
    driver: claude-code
    model: claude-test
    profile: unsupported
    enabled: true
`);
    fs.writeFileSync(path.join(repo, ".agent-flow.yml"), `version: 1
providers:
  reviewer: { kind: frontier, target: claude }
`);
    expect(() => loadAgentFlowProviderCatalog({ cwd: repo, homeDir: home, env: {} }))
      .toThrow("profile is supported only by codex-cli targets");

    fs.writeFileSync(globalConfig, `version: 1
targets:
  claude:
    kind: frontier
    driver: claude-code
    model: claude-test
    reasoning_effort: high
    enabled: true
`);
    expect(() => loadAgentFlowProviderCatalog({ cwd: repo, homeDir: home, env: {} }))
      .toThrow("reasoning_effort is supported only by codex-cli targets");

    fs.writeFileSync(globalConfig, `version: 1
targets:
  codex:
    kind: frontier
    driver: codex-cli
    model: codex-test
    profile: ../unsafe
    enabled: true
`);
    fs.writeFileSync(path.join(repo, ".agent-flow.yml"), `version: 1
providers:
  coder: { kind: frontier, target: codex }
`);
    expect(() => loadAgentFlowProviderCatalog({ cwd: repo, homeDir: home, env: {} }))
      .toThrow("Codex profile names must be non-secret");

    fs.writeFileSync(globalConfig, `version: 1
targets:
  codex:
    kind: frontier
    driver: codex-cli
    model: codex-test
    reasoning_effort: extreme
    enabled: true
`);
    expect(() => loadAgentFlowProviderCatalog({ cwd: repo, homeDir: home, env: {} }))
      .toThrow("Expected one of: minimal, low, medium, high, xhigh");

    fs.writeFileSync(globalConfig, `version: 1
targets:
  __proto__: { kind: local, driver: openai-compatible, base_url: http://127.0.0.1:11434/v1, model: model, enabled: true }
`);
    expect(() => loadAgentFlowProviderCatalog({ cwd: repo, homeDir: home, env: {} }))
      .toThrow("Name \"__proto__\" is reserved");

    fs.writeFileSync(globalConfig, `version: 1
targets:
  codex-target: { kind: frontier, driver: codex-cli, model: codex-test, enabled: true }
`);
    fs.writeFileSync(path.join(repo, ".agent-flow.yml"), `version: 1
providers:
  codex: { kind: frontier, target: codex-target }
`);
    expect(() => loadAgentFlowProviderCatalog({ cwd: repo, homeDir: home, env: {} }))
      .toThrow('Alias "codex" is reserved');
    fs.writeFileSync(path.join(repo, ".agent-flow.yml"), "version: 1\n");

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
      properties: {
        providers: { propertyNames: { pattern: string } };
      };
      $defs: {
        target: {
          required: string[];
          properties: {
            profile: { pattern: string };
            reasoning_effort: { enum: string[] };
          };
          allOf: Array<Record<string, unknown>>;
        };
      };
    };
    const providerNamePattern = new RegExp(schema.properties.providers.propertyNames.pattern);
    expect(providerNamePattern.test("codex")).toBe(false);
    expect(providerNamePattern.test("codex:reviewer")).toBe(false);
    expect(providerNamePattern.test("reviewer")).toBe(true);
    expect(schema.$defs.target.required).toEqual(["kind", "driver", "model", "enabled"]);
    expect(schema.$defs.target.properties.profile.pattern).toBe("^[A-Za-z0-9_-]+$");
    expect(schema.$defs.target.properties.reasoning_effort.enum)
      .toEqual(["minimal", "low", "medium", "high", "xhigh"]);
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
    const nativeConstraint = schema.$defs.target.allOf.find((entry) => {
      const condition = entry.if as { properties?: { driver?: { enum?: string[] } } } | undefined;
      return condition?.properties?.driver?.enum?.includes("codex-cli") === true;
    }) as { then?: { not?: { required?: string[] } } } | undefined;
    expect(nativeConstraint?.then?.not?.required).toEqual(["api_key_env"]);
    const codexConstraint = schema.$defs.target.allOf.find((entry) => {
      const condition = entry.if as { properties?: { driver?: { const?: string } } } | undefined;
      return condition?.properties?.driver?.const === "codex-cli";
    }) as { else?: { not?: { anyOf?: Array<{ required?: string[] }> } } } | undefined;
    expect(codexConstraint?.else?.not?.anyOf?.map((entry) => entry.required))
      .toEqual([["profile"], ["reasoning_effort"]]);
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

  test("passes normal Codex config, environment, overrides, and durable thread IDs through", async () => {
    const { repo, home, globalConfig } = configuredRepo();
    const fake = installFakeAgentClis(path.dirname(repo));
    fs.writeFileSync(globalConfig, `version: 1
targets:
  codex: { kind: frontier, driver: codex-cli, model: configured-model, enabled: true }
`);
    fs.writeFileSync(path.join(repo, ".agent-flow.yml"), `version: 1
providers:
  coder: { kind: frontier, target: codex }
`);
    const ambientConfig = path.join(fake.root, "config.toml");
    const ambientSkill = path.join(fake.root, "skills", "ambient", "SKILL.md");
    fs.mkdirSync(path.dirname(ambientSkill), { recursive: true });
    fs.writeFileSync(ambientConfig, "[mcp_servers.atlassian]\ncommand = 'ambient-mcp'\n");
    fs.writeFileSync(ambientSkill, "ambient skill\n");
    fs.writeFileSync(path.join(fake.root, "read-path"), `${ambientConfig}\n${ambientSkill}\n`);
    const env = {
      PATH: `${fake.bin}:${process.env.PATH ?? ""}`,
      CODEX_HOME: fake.root,
      UNRELATED_SECRET: "available-to-codex"
    };
    const catalog = loadAgentFlowProviderCatalog({ cwd: repo, homeDir: home, env });
    expect(doctorAgentFlowProviderCatalog(catalog, env)).toMatchObject({ ok: true });
    const registry = createAgentFlowConfiguredProviderRegistry(catalog, { env });
    const ids: string[] = [];
    const request = {
      ...providerRequest(repo),
      provider: "coder",
      providerKind: "frontier" as const,
      resume: true,
      codexOptions: { profile: "run-profile", model: "run-model", reasoningEffort: "high" },
      reportExternalSessionId: (id: string) => ids.push(id)
    };
    const first = await registry.get("coder")!(request);
    await registry.get("coder")!({ ...request, externalSessionId: first.externalSessionId });

    expect(first).toMatchObject({ externalSessionId: "codex-thread-1", outputs: { "draft.md": "codex output\n" } });
    expect(ids).toEqual(["codex-thread-1", "codex-thread-1"]);
    const invocations = fs.readFileSync(fake.log, "utf8").trim().split("\n")
      .map((line) => JSON.parse(line) as { args: string[]; unrelatedSecret?: string; hostReads?: Array<string | null> });
    expect(invocations[0]!.args).toContain("--profile");
    expect(invocations[0]!.args).toContain("run-profile");
    expect(invocations[0]!.args).toContain("run-model");
    expect(invocations[0]!.args).toContain('model_reasoning_effort="high"');
    expect(invocations[0]!.args).not.toContain("--ignore-user-config");
    expect(invocations[0]!.args).not.toContain("--ignore-rules");
    expect(invocations[0]!.args).not.toContain("--disable");
    expect(invocations[0]!.unrelatedSecret).toBe("available-to-codex");
    expect(invocations[0]!.hostReads).toEqual([
      "[mcp_servers.atlassian]\ncommand = 'ambient-mcp'\n",
      "ambient skill\n"
    ]);
    expect(invocations[1]!.args).toContain("resume");
  });

  test("preserves Claude Code's default login file while isolating it from model tools", async () => {
    const { repo, home, globalConfig } = configuredRepo();
    const fake = installFakeAgentClis(path.dirname(repo));
    const claudeHome = path.join(path.dirname(repo), "claude-home");
    const claudeState = path.join(claudeHome, ".claude");
    const claudeLogin = path.join(claudeHome, ".claude.json");
    fs.mkdirSync(claudeState, { recursive: true });
    fs.writeFileSync(claudeLogin, '{"login":"existing"}\n', { mode: 0o600 });
    fs.writeFileSync(globalConfig, `version: 1
targets:
  claude: { kind: frontier, driver: claude-code, model: claude-test, enabled: true }
`);
    fs.writeFileSync(path.join(repo, ".agent-flow.yml"), `version: 1
providers:
  reviewer: { kind: frontier, target: claude }
`);
    const env = {
      PATH: `${fake.bin}:${process.env.PATH ?? ""}`,
      HOME: claudeHome
    };
    const registry = createAgentFlowConfiguredProviderRegistry(
      loadAgentFlowProviderCatalog({ cwd: repo, homeDir: home, env }),
      { env }
    );

    const response = await registry.get("reviewer")!({
      ...providerRequest(repo),
      provider: "reviewer",
      providerKind: "frontier"
    });
    expect(response).toMatchObject({ outputs: { "draft.md": "claude output\n" } });

    const invocation = JSON.parse(fs.readFileSync(path.join(claudeState, "invocations.jsonl"), "utf8")) as {
      args: string[];
      configDir?: string;
      defaultConfigContent?: string;
    };
    expect(invocation.configDir).toBeUndefined();
    expect(invocation.defaultConfigContent).toBe('{"login":"existing"}\n');
    const settings = JSON.parse(invocation.args[invocation.args.indexOf("--settings") + 1]!);
    expect(settings.sandbox.filesystem).toEqual({
      denyRead: [fs.realpathSync(claudeState), fs.realpathSync(claudeLogin)],
      denyWrite: [fs.realpathSync(claudeState), fs.realpathSync(claudeLogin)]
    });
  });

  test("supports linked Git worktrees and symlinked native CLI state", async () => {
    const parent = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-native-paths-"));
    const main = path.join(parent, "main");
    const repo = path.join(parent, "worktree");
    const home = path.join(parent, "home");
    const globalConfig = path.join(home, ".config", "agent-flow", "config.yml");
    fs.mkdirSync(main, { recursive: true });
    fs.mkdirSync(path.dirname(globalConfig), { recursive: true });
    execFileSync("git", ["init", "--quiet"], { cwd: main });
    execFileSync("git", ["config", "user.email", "agent-flow@example.test"], { cwd: main });
    execFileSync("git", ["config", "user.name", "Agent Flow Test"], { cwd: main });
    fs.writeFileSync(path.join(main, "tracked.txt"), "tracked\n");
    execFileSync("git", ["add", "tracked.txt"], { cwd: main });
    execFileSync("git", ["commit", "--quiet", "-m", "Initial"], { cwd: main });
    execFileSync("git", ["worktree", "add", "--quiet", "-b", "native-test", repo], { cwd: main });

    const fake = installFakeAgentClis(parent);
    const stateLink = path.join(parent, "codex-state");
    fs.symlinkSync(fake.root, stateLink, "dir");
    fs.writeFileSync(path.join(fake.root, "require-git-status"), "required\n");
    fs.writeFileSync(globalConfig, `version: 1\ntargets:\n  codex: { kind: frontier, driver: codex-cli, model: codex-test, enabled: true }\n`);
    fs.writeFileSync(path.join(repo, ".agent-flow.yml"), `version: 1\nproviders:\n  coder: { kind: frontier, target: codex }\n`);
    const env = {
      PATH: `${fake.bin}:${process.env.PATH ?? ""}`,
      CODEX_HOME: stateLink
    };
    const registry = createAgentFlowConfiguredProviderRegistry(
      loadAgentFlowProviderCatalog({ cwd: repo, homeDir: home, env }),
      { env }
    );

    await expect(registry.get("coder")!({
      ...providerRequest(repo),
      provider: "coder",
      providerKind: "frontier"
    })).resolves.toMatchObject({ outputs: { "draft.md": "codex output\n" } });
    expect(fs.readFileSync(fake.log, "utf8")).toContain('"cli":"codex"');
  });

  test("lets Codex and Agent Flow writers rely on their own concurrency boundaries", async () => {
    const { repo, home, globalConfig } = configuredRepo();
    const fake = installFakeAgentClis(path.dirname(repo));
    fs.writeFileSync(globalConfig, `version: 1
targets:
  codex: { kind: frontier, driver: codex-cli, model: codex-test, enabled: true }
`);
    fs.writeFileSync(path.join(repo, ".agent-flow.yml"), `version: 1
providers:
  coder: { kind: frontier, target: codex }
`);
    const target = path.join(repo, "concurrent-change.txt");
    fs.writeFileSync(path.join(fake.root, "concurrency-path"), `${target}\n`);
    fs.writeFileSync(path.join(repo, "custom.md"), "Write another file.\n");
    const env = { PATH: `${fake.bin}:${process.env.PATH ?? ""}`, CODEX_HOME: fake.root };
    const registry = createAgentFlowConfiguredProviderRegistry(
      loadAgentFlowProviderCatalog({ cwd: repo, homeDir: home, env }),
      { env }
    );
    const writable = registry.get("coder")!({
      ...providerRequest(repo),
      provider: "coder",
      providerKind: "frontier",
      canModifyFiles: true,
      fileScope: { layers: [{ include: ["concurrent-change.txt"], exclude: [] }] }
    });
    await waitForPath(path.join(fake.root, "concurrency-started"));
    const commandWorkflow = parseAgentFlowWorkflowOrThrow(`name: concurrent-command
version: 1
style: pipeline
maturity: experimental
steps:
  - id: write
    type: command
    command: "printf 'command output\\n' > command-change.txt"
    timeout_seconds: 2
    on_failure: { then: fail }
`);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "concurrent-command", workflow: commandWorkflow });
    const command = executeAgentFlowCommandPipeline(store, "concurrent-command", commandWorkflow);
    const customWorkflow = parseAgentFlowWorkflowOrThrow(`name: concurrent-custom
version: 1
style: pipeline
maturity: experimental
sessions:
  writer:
    provider: custom
    authority: { can_modify_files: true }
    file_scope: { include: [custom-change.txt] }
steps:
  - id: write
    type: session_request
    session: writer
    prompt: custom.md
    inputs: [request.md]
    outputs: [custom-summary.md]
limits: { max_model_calls: 1 }
`);
    createAgentFlowLifecycleRun(store, { id: "concurrent-custom", workflow: customWorkflow });
    store.writeArtifact({
      id: "concurrent-custom-request",
      runId: "concurrent-custom",
      path: "request.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Write it.\n"
    });
    const customProviders = createAgentFlowSessionProviderRegistry().register("custom", async () => {
      fs.writeFileSync(path.join(repo, "custom-change.txt"), "custom output\n");
      return { outputs: { "custom-summary.md": "done\n" } };
    });
    const custom = executeAgentFlowCommandPipeline(
      store, "concurrent-custom", customWorkflow, undefined, customProviders
    );
    const [, commandResult, customResult] = await Promise.all([writable, command, custom]);
    expect(commandResult.status).toBe("completed");
    expect(customResult.status).toBe("completed");
    expect(fs.readFileSync(target, "utf8")).toBe("changed by first fake CLI\n");
    expect(fs.readFileSync(path.join(repo, "command-change.txt"), "utf8")).toBe("command output\n");
    expect(fs.readFileSync(path.join(repo, "custom-change.txt"), "utf8")).toBe("custom output\n");
    store.close();
  });

  test("pauses for a missing Codex thread and resumes only after an explicit session reset", async () => {
    const { repo, globalConfig } = configuredRepo();
    const fake = installFakeAgentClis(path.dirname(repo));
    fs.writeFileSync(globalConfig, `version: 1
targets:
  codex: { kind: frontier, driver: codex-cli, model: codex-test, enabled: true }
`);
    fs.writeFileSync(path.join(repo, ".agent-flow.yml"), `version: 1
providers:
  coder: { kind: frontier, target: codex }
`);
    fs.writeFileSync(path.join(repo, "prompt.md"), "Work on the current task.\n");
    fs.writeFileSync(path.join(repo, "inputs.json"), JSON.stringify({ artifacts: { "request.md": "Implement this." } }));
    fs.writeFileSync(path.join(repo, "workflow.yml"), `name: native-session-reset
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: coder, resume: true }
steps:
  - { id: implement, type: session_request, session: writer, prompt: prompt.md, inputs: [request.md], outputs: [implementation.md] }
  - { id: review, type: session_request, session: writer, prompt: prompt.md, inputs: [implementation.md], outputs: [review.md] }
limits: { max_model_calls: 3, max_frontier_calls: 3 }
`);
    fs.writeFileSync(fake.failResumeOnce, "fail\n");
    const env = { PATH: `${fake.bin}:${process.env.PATH ?? ""}`, CODEX_HOME: fake.root };
    const started = await captureCli([
      "run", "workflow.yml", "--id", "native-reset", "--config", globalConfig, "--fixture", "inputs.json"
    ], repo, env);
    expect(started).toMatchObject({ exitCode: 3 });
    expect(started.stdout).toContain("Status: paused");

    const pausedStore = await openAgentFlowRunState({ cwd: repo });
    expect(pausedStore.getSession("native-reset", "writer")?.externalSessionId).toBe("codex-thread-1");
    expect(pausedStore.getRun("native-reset")?.context.waiting).toMatchObject({
      kind: "provider_session",
      sessionId: "writer",
      reason: "external_session_unavailable"
    });
    expect(pausedStore.listEvents("native-reset").map((event) => event.type)).toContain("session.external_unavailable");
    const snapshot = buildAgentFlowRunActionSnapshot(pausedStore, "native-reset");
    expect(snapshot.waiting).toMatchObject({ kind: "provider_session", sessionId: "writer" });
    expect(snapshot.actions.find((action) => action.action === "resume")).toMatchObject({
      enabled: false,
      reason: expect.stringContaining("--reset-session writer")
    });
    pausedStore.close();

    const reset = await captureCli([
      "resume", "native-reset", "--reset-session", "writer", "--config", globalConfig
    ], repo, env);
    expect(reset).toMatchObject({ exitCode: 0 });
    expect(reset.stdout).toContain("Status: completed");
    const completedStore = await openAgentFlowRunState({ cwd: repo });
    expect(completedStore.getSession("native-reset", "writer")?.externalSessionId).toBe("codex-thread-2");
    expect(completedStore.listEvents("native-reset").map((event) => event.type)).toContain("session.external_reset");
    completedStore.close();
  });

  test("routes a missing native recovery session through explicit reset", async () => {
    const { repo, globalConfig } = configuredRepo();
    const fake = installFakeAgentClis(path.dirname(repo));
    fs.writeFileSync(path.join(fake.root, "recovery.config.toml"), 'model_verbosity = "low"\n');
    fs.writeFileSync(globalConfig, `version: 1
targets:
  codex: { kind: frontier, driver: codex-cli, model: codex-test, profile: recovery, reasoning_effort: high, enabled: true }
`);
    fs.writeFileSync(path.join(repo, ".agent-flow.yml"), `version: 1
providers:
  coder: { kind: frontier, target: codex }
`);
    fs.writeFileSync(path.join(repo, "fix.md"), "Repair the failed check.\n");
    fs.writeFileSync(path.join(repo, "workflow.yml"), `name: native-recovery-session-reset
version: 1
style: recovery_pipeline
maturity: experimental
sessions:
  fixer: { provider: coder, resume: true }
steps:
  - id: check
    type: command
    command: "false"
    on_failure:
      route_to: { session: fixer, prompt: fix.md }
      on_remediated: { return_to: check }
      on_unresolved: { then: pause }
limits:
  max_model_calls: 3
  max_frontier_calls: 3
  max_recovery_cycles: 3
  max_step_attempts: { check: 3 }
`);
    fs.writeFileSync(fake.failResumeOnce, "fail\n");
    const env = { PATH: `${fake.bin}:${process.env.PATH ?? ""}`, CODEX_HOME: fake.root };

    const started = await captureCli([
      "run", "workflow.yml", "--id", "native-recovery-reset", "--config", globalConfig
    ], repo, env);
    expect(started).toMatchObject({ exitCode: 3 });
    const pausedStore = await openAgentFlowRunState({ cwd: repo });
    expect(pausedStore.getRun("native-recovery-reset")?.context.waiting).toMatchObject({
      kind: "provider_session",
      stepId: "check",
      sessionId: "fixer"
    });
    const recoveryRequestPath = pausedStore.listArtifacts("native-recovery-reset")
      .find((artifact) => artifact.kind === "session_request"
        && artifact.declaredPath.startsWith("session-requests/recovery/"))!.declaredPath;
    expect(JSON.parse(pausedStore.readArtifact(
      "native-recovery-reset",
      recoveryRequestPath
    ).content.toString("utf8"))).toMatchObject({
      providerProfile: "recovery",
      providerReasoningEffort: "high"
    });
    pausedStore.close();

    const reset = await captureCli([
      "resume", "native-recovery-reset", "--reset-session", "fixer", "--config", globalConfig
    ], repo, env);
    expect(reset.stderr).not.toContain("waiting state does not match");
    const resetStore = await openAgentFlowRunState({ cwd: repo });
    expect(resetStore.listEvents("native-recovery-reset").map((event) => event.type))
      .toContain("session.external_reset");
    resetStore.close();
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

function installFakeAgentClis(parent: string): {
  root: string;
  bin: string;
  log: string;
  failResumeOnce: string;
  failClaudeResumeOnce: string;
  failClaudeCreateOnce: string;
} {
  const root = fs.mkdtempSync(path.join(parent, "fake-agent-clis-"));
  const bin = path.join(root, "bin");
  const log = path.join(root, "invocations.jsonl");
  const failResumeOnce = path.join(root, "fail-resume-once");
  const failClaudeResumeOnce = path.join(root, "fail-claude-resume-once");
  const failClaudeCreateOnce = path.join(root, "fail-claude-create-once");
  fs.mkdirSync(bin);
  fs.writeFileSync(path.join(bin, "codex"), String.raw`#!/usr/bin/env node
const fs = require("node:fs");
const path = require("node:path");
const args = process.argv.slice(2);
if (args[0] === "--version") { console.log("codex-cli 0.test"); process.exit(0); }
if (args[0] === "login" && args[1] === "status") {
  const expectedHomeMarker = path.join(__dirname, "expected-codex-home");
  if (fs.existsSync(expectedHomeMarker)
      && process.env.CODEX_HOME !== fs.readFileSync(expectedHomeMarker, "utf8").trim()) {
    process.exit(1);
  }
  console.log("Logged in");
  process.exit(0);
}
const root = process.env.CODEX_HOME;
if (args[0] === "exec" && args.includes("--strict-config") && !args.includes("--output-schema")) {
  const profileIndex = args.indexOf("--profile");
  const profile = profileIndex < 0 ? undefined : args[profileIndex + 1];
  const profilePath = profile === undefined ? undefined : path.join(root, profile + ".config.toml");
  if (profilePath === undefined || !fs.existsSync(profilePath)
      || /model_verbosity\s*=\s*42/.test(fs.readFileSync(profilePath, "utf8"))) {
    console.error("Error loading config.toml");
    process.exit(1);
  }
  console.log("{}\n");
  process.exit(0);
}
const projectCodexDirectory = path.join(process.cwd(), ".codex");
if (fs.existsSync(projectCodexDirectory)) {
  try {
    fs.accessSync(projectCodexDirectory, fs.constants.R_OK | fs.constants.X_OK);
  } catch {
    console.error("Repository .codex directory is not traversable");
    process.exit(1);
  }
}
let certificateContent;
if (process.env.NODE_EXTRA_CA_CERTS) {
  try { certificateContent = fs.readFileSync(process.env.NODE_EXTRA_CA_CERTS, "utf8"); } catch {}
}
const gitMarker = path.join(root, "require-git-status");
if (fs.existsSync(gitMarker)) {
  const git = require("node:child_process").spawnSync("git", ["status", "--short"], {
    cwd: process.cwd(),
    encoding: "utf8"
  });
  if (git.status !== 0) {
    fs.writeSync(2, git.stderr || "git status failed\n");
    process.exit(1);
  }
}
const readMarker = path.join(root, "read-path");
let hostRead;
let hostReads;
if (fs.existsSync(readMarker)) {
  const readPaths = fs.readFileSync(readMarker, "utf8").split(/\r?\n/).filter(Boolean);
  hostReads = readPaths.map((readPath) => {
    try { return fs.readFileSync(readPath, "utf8"); } catch { return undefined; }
  });
  hostRead = hostReads[0];
}
fs.appendFileSync(path.join(root, "invocations.jsonl"), JSON.stringify({
  cli: "codex",
  args,
  unrelatedSecret: process.env.UNRELATED_SECRET,
  providerKey: process.env.MISTRAL_API_KEY,
  providerOrg: process.env.MISTRAL_ORG,
  openAiKey: process.env.OPENAI_API_KEY || null,
  hostRead,
  hostReads,
  certificateContent
}) + "\n");
const concurrencyMarker = path.join(root, "concurrency-path");
if (fs.existsSync(concurrencyMarker)) {
  const started = path.join(root, "concurrency-started");
  if (!fs.existsSync(started)) {
    fs.writeFileSync(started, "started\n");
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 200);
    fs.writeFileSync(fs.readFileSync(concurrencyMarker, "utf8").trim(), "changed by first fake CLI\n");
  } else {
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 500);
  }
}
const resumeIndex = args.indexOf("resume");
const resume = args[0] === "exec" && resumeIndex > 0;
const splitUtf8 = fs.existsSync(path.join(root, "split-utf8-thread"));
const failureMarker = path.join(root, "fail-resume-once");
if (resume && fs.existsSync(failureMarker)) {
  fs.unlinkSync(failureMarker);
  fs.writeSync(2, "thread was not found\n");
  process.exit(1);
}
let threadId;
if (resume) {
  threadId = args[args.length - 2];
} else if (splitUtf8) {
  threadId = "codex-thread-🚀";
} else {
  const counterPath = path.join(root, "counter");
  const count = fs.existsSync(counterPath) ? Number(fs.readFileSync(counterPath, "utf8")) + 1 : 1;
  fs.writeFileSync(counterPath, String(count));
  threadId = "codex-thread-" + String(count);
}
const writeMarker = path.join(root, "write-path");
if (fs.existsSync(writeMarker)) fs.writeFileSync(fs.readFileSync(writeMarker, "utf8").trim(), "changed by fake CLI\n");
const schemaPath = args[args.indexOf("--output-schema") + 1];
const outputPath = args[args.indexOf("--output-last-message") + 1];
const schema = JSON.parse(fs.readFileSync(schemaPath, "utf8"));
const outputs = {};
for (const name of schema.properties.outputs.required) outputs[name] = "codex output\n";
const result = { outputs };
if (schema.required.includes("recovery_status")) result.recovery_status = "remediated";
fs.writeFileSync(outputPath, JSON.stringify(result));
const threadEvent = Buffer.from(JSON.stringify({ type: "thread.started", thread_id: threadId }) + "\n");
if (splitUtf8) {
  const emojiOffset = threadEvent.indexOf(Buffer.from("🚀"));
  fs.writeSync(1, threadEvent.subarray(0, emojiOffset + 2));
  setTimeout(() => {
    fs.writeSync(1, threadEvent.subarray(emojiOffset + 2));
    fs.writeSync(1, JSON.stringify({ type: "turn.completed" }) + "\n");
  }, 20);
} else {
  fs.writeSync(1, threadEvent);
  fs.writeSync(1, JSON.stringify({ type: "turn.completed" }) + "\n");
}
`, { mode: 0o755 });
  fs.writeFileSync(path.join(bin, "claude"), String.raw`#!/usr/bin/env bun
const fs = require("node:fs");
const path = require("node:path");
const args = process.argv.slice(2);
if (args[0] === "--version") { console.log("2.test"); process.exit(0); }
if (args[0] === "auth" && args[1] === "status") { console.log("Authenticated"); process.exit(0); }
const root = process.env.CLAUDE_CONFIG_DIR || path.join(process.env.HOME, ".claude");
let defaultConfigContent;
if (!process.env.CLAUDE_CONFIG_DIR) {
  try { defaultConfigContent = fs.readFileSync(path.join(process.env.HOME, ".claude.json"), "utf8"); } catch {}
}
fs.appendFileSync(path.join(root, "invocations.jsonl"), JSON.stringify({
  cli: "claude",
  args,
  unrelatedSecret: process.env.UNRELATED_SECRET,
  configDir: process.env.CLAUDE_CONFIG_DIR,
  defaultConfigContent
}) + "\n");
const sessionFlag = args.includes("--resume") ? "--resume" : "--session-id";
const sessionId = args.includes(sessionFlag) ? args[args.indexOf(sessionFlag) + 1] : undefined;
const failureMarker = path.join(root, "fail-claude-resume-once");
const createFailureMarker = path.join(root, "fail-claude-create-once");
if (!args.includes("--resume") && fs.existsSync(createFailureMarker)) {
  fs.unlinkSync(createFailureMarker);
  fs.writeSync(2, "authentication failed\n");
  process.exit(1);
}
if (args.includes("--resume") && fs.existsSync(failureMarker)) {
  fs.unlinkSync(failureMarker);
  fs.writeSync(2, "No conversation found with session ID: " + sessionId + "\n");
  process.exit(1);
}
const schema = JSON.parse(args[args.indexOf("--json-schema") + 1]);
const outputs = {};
for (const name of schema.properties.outputs.required) outputs[name] = "claude output\n";
const structured_output = { outputs };
if (schema.required.includes("recovery_status")) structured_output.recovery_status = "remediated";
fs.writeSync(1, JSON.stringify({ session_id: sessionId, structured_output }) + "\n");
`, { mode: 0o755 });
  return { root, bin, log, failResumeOnce, failClaudeResumeOnce, failClaudeCreateOnce };
}


async function captureCli(
  args: string[],
  cwd: string,
  env: Readonly<Record<string, string | undefined>> = {}
): Promise<{ exitCode: number; stdout: string; stderr: string }> {
  let stdout = "";
  let stderr = "";
  const exitCode = await runCli(args, {
    stdout: { write: (value) => { stdout += String(value); return true; } },
    stderr: { write: (value) => { stderr += String(value); return true; } }
  }, { cwd, env });
  return { exitCode, stdout, stderr };
}

async function waitForPath(target: string): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (fs.existsSync(target)) return;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`Timed out waiting for ${target}.`);
}
