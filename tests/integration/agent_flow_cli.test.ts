import { describe, expect, spyOn, test } from "bun:test";
import { Database } from "bun:sqlite";
import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { dispatch, runCli } from "../../src/cli/router";
import {
  AgentFlowRunStateStore,
  MAX_AGENT_FLOW_PORTABLE_ARCHIVE_CONTENT_BYTES,
  applyAgentFlowRetention,
  createAgentFlowLifecycleRun,
  createAgentFlowWorkflowRegistry,
  defaultAgentFlowArchivePath,
  defaultAgentFlowExportPath,
  openAgentFlowRunState,
  parseAgentFlowWorkflowOrThrow,
  plannedAgentFlowRuntimeCommands,
  serializeAgentFlowWorkflowRegistry,
  writeAgentFlowPortableArchive,
  writeAgentFlowFinalSummary
} from "../../src/runtime";

const repoRoot = path.resolve(".");
const rootPackage = JSON.parse(fs.readFileSync(path.join(repoRoot, "package.json"), "utf8")) as { version: string };

describe("Agent Flow CLI", () => {
  test("renders help with validation authoring commands active", () => {
    const result = dispatch(["help"]);

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain("Agent Flow");
    expect(result.stdout).toContain("Available now");
    expect(result.stdout).toContain("skills install --destination <agents|codex>");
    expect(result.stdout).toContain("validate <workflow>");
    expect(result.stdout).toContain("lint <workflow>");
    expect(result.stdout).toContain("explain <workflow>");
    expect(result.stdout).toContain("graph <workflow>");
    expect(result.stdout).toContain("simulate <workflow> --fixture <file>");
    expect(result.stdout).toContain("run <workflow> --id <run-id>");
    expect(result.stdout).toContain("resume <run-id> --reset-session <session-name> [--fixture <file>] [--config <file>]");
    expect(result.stdout).toContain("pause <run-id>");
    expect(result.stdout).toContain("cleanup <run-id>");
    expect(result.stdout).toContain("archive <run-id>");
    expect(result.stdout).toContain("export <run-id> --format zip");
    expect(result.stdout).toContain("Command and artifact-transform pipeline execution");
  });

  test("renders version from root package metadata", () => {
    const result = dispatch(["--version"]);

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toBe(`agent-flow ${rootPackage.version}`);
  });

  test("keeps remaining execution commands reserved but inactive", () => {
    for (const command of plannedAgentFlowRuntimeCommands.filter((candidate) => !["validate", "lint", "explain", "graph", "simulate", "run", "resume", "status", "logs", "artifacts", "pause", "cancel", "cleanup", "archive", "export"].includes(candidate))) {
      const result = dispatch([command]);

      expect(result.exitCode).toBe(7);
      expect(result.stderr).toContain("reserved but not active yet");
      expect(result.stderr).toContain("run, resume, status, logs, artifacts, pause, and cancel");
    }
  });

  test("distinguishes active, reserved, and unknown help topics", () => {
    expect(dispatch(["help", "validate"])).toEqual({
      exitCode: 0,
      stdout: "agent-flow validate\n\nUsage: agent-flow validate <workflow>"
    });
    expect(dispatch(["help", "explain"])).toEqual({
      exitCode: 0,
      stdout: "agent-flow explain\n\nUsage: agent-flow explain <workflow>"
    });
    expect(dispatch(["help", "simulate"])).toEqual({
      exitCode: 0,
      stdout: "agent-flow simulate\n\nUsage: agent-flow simulate <workflow> --fixture <file>"
    });
    expect(dispatch(["help", "run"])).toEqual({
      exitCode: 0,
      stdout: "agent-flow run\n\nUsage: agent-flow run <workflow> --id <run-id> [--fixture <file>] [--input <key=value>] [--input-file <json>] [--profile <name>] [--model <name>] [--reasoning-effort <level>]"
    });
    expect(dispatch(["help", "inject"])).toEqual({
      exitCode: 0,
      stdout: "agent-flow inject\n\nUsage: agent-flow inject <run-id> <session-name> <context>"
    });
    expect(dispatch(["help", "cleanup"])).toEqual({
      exitCode: 0,
      stdout: "agent-flow cleanup\n\nUsage: agent-flow cleanup ([--] <run-id> | --older-than <duration> [--status <status>]) [--approve]"
    });
    expect(dispatch(["help", "export"])).toEqual({
      exitCode: 0,
      stdout: "agent-flow export\n\nUsage: agent-flow export [--] <run-id> --format zip [--output <file>]"
    });
    expect(dispatch(["help", "missing"])).toEqual({
      exitCode: 7,
      stderr: "Unknown Agent Flow help topic: missing\nRun `agent-flow help` to see available commands."
    });
  });

  test("validates workflows from the CLI", () => {
    const validPath = path.join(repoRoot, "tests/fixtures/agent-flow/workflows/simple-ci.yml");
    const invalidPath = path.join(repoRoot, "tests/fixtures/agent-flow/invalid/unsafe-workflow.yml");

    expect(dispatch(["validate", validPath])).toMatchObject({
      exitCode: 0,
      stdout: `Agent Flow validation passed: ${validPath}`
    });
    const invalid = dispatch(["validate", invalidPath]);
    expect(invalid.exitCode).toBe(2);
    expect(invalid.stderr).toContain("workflow.command.unsafe");
    expect(invalid.stderr).toContain("workflow.loop.unbounded");
  });

  test("lints workflows without rewriting them", () => {
    const fixturePath = path.join(repoRoot, "tests/fixtures/agent-flow/workflows/content-review-collab.yml");
    const before = fs.readFileSync(fixturePath, "utf8");
    const result = dispatch(["lint", fixturePath]);

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain("workflow.lint.artifact.read_before_write");
    expect(fs.readFileSync(fixturePath, "utf8")).toBe(before);
  });

  test("explains and graphs workflows without executing or rewriting them", () => {
    const fixturePath = path.join(repoRoot, "tests/fixtures/agent-flow/workflows/pr-feedback-loop.yml");
    const before = fs.readFileSync(fixturePath, "utf8");

    const explanation = dispatch(["explain", fixturePath]);
    expect(explanation.exitCode).toBe(0);
    expect(explanation.stdout).toContain("Workflow: pr-feedback-loop (version 1)");
    expect(explanation.stdout).toContain("wait_for_review [loop]");

    const graph = dispatch(["graph", fixturePath]);
    expect(graph.exitCode).toBe(0);
    expect(graph.stdout).toContain("Workflow graph: pr-feedback-loop (version 1)");
    expect(graph.stdout).toContain("wait_for_review -> collect_pr_state [loop body]");
    expect(fs.readFileSync(fixturePath, "utf8")).toBe(before);
  });

  test("simulates workflows from JSON fixtures without executing or rewriting them", () => {
    const workflowPath = path.join(repoRoot, "examples/workflows/simple-ci.yml");
    const fixturePath = path.join(repoRoot, "tests/fixtures/agent-flow/simulation/simple-ci.json");
    const workflowBefore = fs.readFileSync(workflowPath, "utf8");
    const fixtureBefore = fs.readFileSync(fixturePath, "utf8");
    const result = dispatch(["simulate", workflowPath, "--fixture", fixturePath]);

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain("Agent Flow simulation: simple-ci (version 1)");
    expect(result.stdout).toContain("Status: completed");
    expect(result.stdout).toContain("install [command]: succeeded");
    expect(fs.readFileSync(workflowPath, "utf8")).toBe(workflowBefore);
    expect(fs.readFileSync(fixturePath, "utf8")).toBe(fixtureBefore);
  });

  test("reports invalid simulation fixtures and usage", () => {
    const workflowPath = path.join(repoRoot, "examples/workflows/simple-ci.yml");
    const invalidFixture = path.join(repoRoot, "tests/fixtures/agent-flow/workflows/simple-ci.yml");

    expect(dispatch(["simulate", workflowPath])).toEqual({
      exitCode: 1,
      stderr: "Usage: agent-flow simulate <workflow> --fixture <file>"
    });
    const invalid = dispatch(["simulate", workflowPath, "--fixture", invalidFixture]);
    expect(invalid.exitCode).toBe(2);
    expect(invalid.stderr).toContain("Could not parse Agent Flow simulation fixture");
  });

  test("reports generated graph node collisions without crashing", () => {
    const fixturePath = path.join(repoRoot, "tests/fixtures/agent-flow/workflows/graph-node-collision.yml");
    const result = dispatch(["graph", fixturePath]);

    expect(result.exitCode).toBe(2);
    expect(result.stderr).toContain("workflow.graph.node_id_collision");
    expect(result.stderr).toContain('Graph node id "terminal:pause" collides');
  });

  test("manages persistent lifecycle state through the asynchronous CLI runner", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-cli-lifecycle-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const workflowPath = path.join(repo, "workflow.yml");
    fs.writeFileSync(workflowPath, `
name: simple-ci
version: 1
style: pipeline
maturity: experimental
steps:
  - id: check
    type: command
    command: printf 'check passed\\n'
`);

    const run = await captureCli(["run", path.basename(workflowPath), "--id", "run-cli"], repo);
    expect(run).toMatchObject({ exitCode: 0 });
    expect(run.stdout).toContain("Created Agent Flow run run-cli");
    expect(run.stdout).toContain("Status: completed");
    const status = await captureCli(["status", "run-cli"], repo);
    expect(status.stdout).toContain("Workflow: simple-ci (version 1)");
    expect(status.stdout).toContain("Status: completed");
    const logs = await captureCli(["logs", "run-cli"], repo);
    expect(logs.stdout).toContain("run.created");
    expect(logs.stdout).toContain("step.completed");
    expect((await captureCli(["artifacts", "run-cli"], repo)).stdout).toContain("stdout.log");

    const restartedStatus = await captureCli(["status", "run-cli"], repo);
    expect(restartedStatus.stdout).toContain("Status: completed");
    expect(await captureCli(["pause", "run-cli"], repo)).toMatchObject({ exitCode: 2 });
    expect(await captureCli(["pause", "missing"], repo)).toMatchObject({ exitCode: 4 });
  });

  test("merges fixture, JSON-file, and repeatable CLI inputs before creating a run", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-cli-inputs-"));
    fs.mkdirSync(path.join(repo, ".git"));
    fs.writeFileSync(path.join(repo, "workflow.yml"), `
name: cli-inputs
version: 1
style: pipeline
maturity: experimental
inputs:
  ticket: { required: true }
  count: {}
  enabled: {}
steps:
  - { id: done, type: result, status: completed }
`);
    fs.writeFileSync(path.join(repo, "fixture.json"), JSON.stringify({ inputs: { ticket: "fixture", count: 1 } }));
    fs.writeFileSync(path.join(repo, "inputs.json"), JSON.stringify({ ticket: "file", count: 2, enabled: false }));

    expect(await captureCli([
      "run", "workflow.yml", "--id", "input-run", "--fixture", "fixture.json",
      "--input-file", "inputs.json", "--input", "ticket=CLI-7", "--input", "enabled=true"
    ], repo)).toMatchObject({ exitCode: 0 });
    const store = await openAgentFlowRunState({ cwd: repo });
    expect(store.getRun("input-run")?.inputs).toEqual({ ticket: "CLI-7", count: 2, enabled: true });
    store.close();

    const duplicate = await captureCli([
      "run", "workflow.yml", "--id", "duplicate", "--input", "ticket=A", "--input", "ticket=B"
    ], repo);
    expect(duplicate).toMatchObject({ exitCode: 2 });
    expect(duplicate.stderr).toContain("provided more than once");
    const unknown = await captureCli([
      "run", "workflow.yml", "--id", "unknown", "--input", "ticket=A", "--input", "extra=1"
    ], repo);
    expect(unknown).toMatchObject({ exitCode: 2 });
    expect(unknown.stderr).toContain("unknown inputs: extra");
    const missing = await captureCli(["run", "workflow.yml", "--id", "missing"], repo);
    expect(missing).toMatchObject({ exitCode: 2 });
    expect(missing.stderr).toContain("missing required inputs: ticket");
  });

  test("rejects direct MCP in the stock CLI before creating run state", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-cli-direct-mcp-"));
    fs.mkdirSync(path.join(repo, ".git"));
    fs.writeFileSync(path.join(repo, "workflow.yml"), `
name: direct-mcp
version: 1
style: pipeline
maturity: experimental
steps:
  - id: fetch
    type: mcp_call
    server: atlassian
    tool: get_issue
    arguments: { key: AF-1 }
    outputs: [ticket.json]
`);
    const result = await captureCli(["run", "workflow.yml", "--id", "direct"], repo);
    expect(result).toMatchObject({ exitCode: 1 });
    expect(result.stderr).toContain("use via: codex");
    expect(fs.existsSync(path.join(repo, ".agent-flow"))).toBe(false);
  });

  test("rejects direct MCP in reachable child workflows before creating a run", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-cli-child-direct-mcp-"));
    fs.mkdirSync(path.join(repo, ".git"));
    fs.writeFileSync(path.join(repo, "parent.yml"), `
name: direct-mcp-parent
version: 1
style: pipeline
maturity: experimental
steps:
  - id: child
    type: workflow
    workflow: direct-mcp-child
    inputs: {}
    outputs: [ticket.json]
`);
    fs.writeFileSync(path.join(repo, "child.yml"), `
name: direct-mcp-child
version: 1
style: pipeline
maturity: experimental
steps:
  - id: fetch
    type: mcp_call
    server: atlassian
    tool: get_issue
    arguments: { key: AF-1 }
    outputs: [ticket.json]
`);

    const result = await captureCli(["run", "parent.yml", "--id", "nested-direct"], repo);
    expect(result).toMatchObject({ exitCode: 1 });
    expect(result.stderr).toContain("direct MCP step fetch in workflow direct-mcp-child");
    const store = await openAgentFlowRunState({ cwd: repo });
    expect(store.getRun("nested-direct")).toBeNull();
    store.close();
  });

  test("runs input through Codex MCP, a nested workflow, and approval resume end to end", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-cli-e2e-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const bin = path.join(repo, "bin");
    const codexHome = path.join(repo, "codex-home");
    fs.mkdirSync(bin);
    fs.mkdirSync(codexHome);
    fs.writeFileSync(path.join(bin, "codex"), `#!/usr/bin/env node
const fs = require("node:fs");
const args = process.argv.slice(2);
const schema = JSON.parse(fs.readFileSync(args[args.indexOf("--output-schema") + 1], "utf8"));
const outputPath = args[args.indexOf("--output-last-message") + 1];
const outputs = {};
for (const name of schema.properties.outputs.required) outputs[name] = name.endsWith(".json") ? '{"key":"AF-9"}\\n' : 'codex output\\n';
fs.writeFileSync(outputPath, JSON.stringify({ outputs }));
const resume = args.indexOf("resume");
const thread = resume > 0 ? args[args.length - 2] : "thread-e2e";
fs.writeSync(1, JSON.stringify({ type: "thread.started", thread_id: thread }) + "\\n");
fs.writeSync(1, JSON.stringify({ type: "item.completed", item: { type: "mcp_tool_call", server: "atlassian", tool: "get_issue", arguments: { key: "AF-9" }, status: "completed" } }) + "\\n");
fs.writeSync(1, JSON.stringify({ type: "turn.completed" }) + "\\n");
`, { mode: 0o755 });
    fs.writeFileSync(path.join(repo, "parent.yml"), `
name: parent-e2e
version: 1
style: pipeline
maturity: experimental
inputs:
  ticket: { required: true }
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
    arguments: { key: "{{ inputs.ticket }}" }
    outputs: [ticket.json]
  - id: child
    type: workflow
    workflow: child-e2e
    inputs: { ticket: "{{ inputs.ticket }}" }
    outputs: [child.txt]
`);
    fs.writeFileSync(path.join(repo, "child.yml"), `
name: child-e2e
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
  - id: publish
    type: command
    command: printf 'nested complete\\n' > child.txt
    outputs: [child.txt]
`);
    const env = { PATH: `${bin}:${process.env.PATH ?? ""}`, CODEX_HOME: codexHome };
    const started = await captureCli([
      "run", "parent.yml", "--id", "e2e", "--input", "ticket=AF-9", "--model", "test-model"
    ], repo, env);
    expect(started).toMatchObject({ exitCode: 3 });
    expect(started.stdout).toContain("Status: paused");
    const resumed = await captureCli(["resume", "e2e", "--outcome", "approve"], repo, env);
    expect(resumed).toMatchObject({ exitCode: 0 });
    const store = await openAgentFlowRunState({ cwd: repo });
    expect(store.getRun("e2e")?.inputs).toEqual({ ticket: "AF-9" });
    expect(store.getSession("e2e", "agent")?.externalSessionId).toBe("thread-e2e");
    expect(store.readArtifact("e2e", "child.txt").content.toString("utf8")).toBe("nested complete\n");
    store.close();
  });

  test("recovers an interrupted running execution through the documented run command", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-cli-recovery-"));
    fs.mkdirSync(path.join(repo, ".git"));
    fs.writeFileSync(path.join(repo, "workflow.yml"), `
name: cli-recovery
version: 1
style: pipeline
maturity: experimental
inputs:
  ticket: { required: true }
steps:
  - { id: check, type: command, command: "echo recovered >> effects.txt" }
`);
    const workflow = parseAgentFlowWorkflowOrThrow(fs.readFileSync(path.join(repo, "workflow.yml"), "utf8"));
    const interrupted = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(interrupted, { id: "cli-recovery", workflow, inputs: { ticket: "AF-1" } });
    interrupted.acquireRunLock("cli-recovery", "run", { ttlMs: 60_000 });
    interrupted.transitionRunWithEvent("cli-recovery", {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: { status: "running" } }
    });
    interrupted.close();

    const changed = await captureCli([
      "run", "workflow.yml", "--id", "cli-recovery", "--input", "ticket=AF-2"
    ], repo);
    expect(changed).toMatchObject({ exitCode: 2 });
    expect(changed.stderr).toContain("differs from its persisted value");

    const recovered = await captureCli(["run", "workflow.yml", "--id", "cli-recovery"], repo);

    expect(recovered).toMatchObject({ exitCode: 0, stderr: "" });
    expect(recovered.stdout).toContain("Reused Agent Flow run cli-recovery");
    expect(recovered.stdout).toContain("Status: completed");
    const recoveredStore = await openAgentFlowRunState({ cwd: repo });
    expect(recoveredStore.getRun("cli-recovery")?.inputs).toEqual({ ticket: "AF-1" });
    recoveredStore.close();
    expect(fs.readFileSync(path.join(repo, "effects.txt"), "utf8")).toBe("recovered\n");
    expect((await captureCli(["logs", "cli-recovery"], repo)).stdout).toContain("run.execution_recovered");
  });

  test("recovers child providers and workflow definitions from the persisted registry", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-cli-child-recovery-"));
    fs.mkdirSync(path.join(repo, ".git"));
    fs.mkdirSync(path.join(repo, "prompts"));
    fs.writeFileSync(path.join(repo, "prompts", "write.md"), "Write the response.\n");
    fs.writeFileSync(path.join(repo, "parent.yml"), `
name: recovered-parent
version: 1
style: pipeline
maturity: experimental
steps:
  - id: child
    type: workflow
    workflow: recovered-child
    inputs: { request: request.md }
    outputs: [response.md]
`);
    const originalChildSource = `
name: recovered-child
version: 1
style: pipeline
maturity: experimental
inputs:
  request: { required: true }
sessions:
  writer: { provider: fixture }
limits: { max_model_calls: 1 }
steps:
  - id: write
    type: session_request
    session: writer
    prompt: prompts/write.md
    inputs: [request.md]
    outputs: [response.md]
`;
    fs.writeFileSync(path.join(repo, "child.yml"), originalChildSource);
    fs.writeFileSync(path.join(repo, "fixture.json"), JSON.stringify({
      artifacts: { "request.md": "request" },
      steps: { write: { outputs: { "response.md": "persisted child response\n" } } }
    }));
    const parent = parseAgentFlowWorkflowOrThrow(fs.readFileSync(path.join(repo, "parent.yml"), "utf8"));
    const child = parseAgentFlowWorkflowOrThrow(originalChildSource);
    const registry = createAgentFlowWorkflowRegistry().register(parent.name, parent).register(child.name, child);
    const interrupted = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(interrupted, {
      id: "child-recovery",
      workflow: parent,
      context: { workflowRegistry: serializeAgentFlowWorkflowRegistry(registry) as never }
    });
    interrupted.acquireRunLock("child-recovery", "run", { ttlMs: 60_000 });
    interrupted.transitionRunWithEvent("child-recovery", {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: { status: "running" } }
    });
    interrupted.writeArtifact({
      id: "request",
      runId: "child-recovery",
      stepId: "fixture",
      path: "request.md",
      kind: "fixture",
      contentType: "text/plain; charset=utf-8",
      content: "request"
    });
    interrupted.close();
    fs.writeFileSync(path.join(repo, "child.yml"), `
name: recovered-child
version: 1
style: pipeline
maturity: experimental
steps:
  - id: changed
    type: command
    command: printf 'changed child\\n' > response.md
    outputs: [response.md]
`);

    const recovered = await captureCli([
      "run", "parent.yml", "--id", "child-recovery", "--fixture", "fixture.json"
    ], repo);

    expect(recovered).toMatchObject({ exitCode: 0 });
    const store = await openAgentFlowRunState({ cwd: repo });
    expect(store.readArtifact("child-recovery", "response.md").content.toString("utf8"))
      .toBe("persisted child response\n");
    store.close();
  });

  test("pins configured providers used only by a reachable child workflow", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-cli-child-provider-"));
    fs.mkdirSync(path.join(repo, ".git"));
    fs.mkdirSync(path.join(repo, "workflows"));
    fs.writeFileSync(path.join(repo, "config.yml"), `version: 1
targets:
  child-local:
    kind: local
    driver: openai-compatible
    base_url: http://127.0.0.1:11434/v1
    model: child-model
    enabled: true
`);
    fs.writeFileSync(path.join(repo, ".agent-flow.yml"), `version: 1
workflows: workflows
providers:
  child-writer: { kind: local, target: child-local }
`);
    fs.writeFileSync(path.join(repo, "workflows", "parent.yml"), `
name: child-provider-parent
version: 1
style: pipeline
maturity: experimental
steps:
  - id: child
    type: workflow
    workflow: child-provider-child
    inputs: {}
    outputs: [response.md]
`);
    fs.writeFileSync(path.join(repo, "workflows", "child.yml"), `
name: child-provider-child
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: child-writer }
limits: { max_model_calls: 1 }
steps:
  - { id: approve, type: manual_gate, message: Continue?, options: [approve, cancel] }
  - { id: write, type: session_request, session: writer, prompt: prompt.md, inputs: [input.md], outputs: [response.md] }
`);

    expect(await captureCli([
      "run", "workflows/parent.yml", "--id", "child-provider", "--config", "config.yml"
    ], repo)).toMatchObject({ exitCode: 3 });
    const store = await openAgentFlowRunState({ cwd: repo });
    expect(store.getRun("child-provider")?.context.providerBindings).toMatchObject({
      "child-writer": { target: "child-local", kind: "local", driver: "openai-compatible" }
    });
    store.close();
  });

  test("ignores providers and validation in unrelated workflow registry entries", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-cli-reachable-registry-"));
    fs.mkdirSync(path.join(repo, ".git"));
    fs.mkdirSync(path.join(repo, "workflows"));
    fs.writeFileSync(path.join(repo, ".agent-flow.yml"), "version: 1\nworkflows: workflows\n");
    fs.writeFileSync(path.join(repo, "workflows", "entry.yml"), `
name: reachable-entry
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: done, type: result, status: completed }
`);
    fs.writeFileSync(path.join(repo, "workflows", "unrelated.yml"), `
name: unrelated-entry
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: fixture }
steps:
  - { id: write, type: session_request, session: writer, prompt: prompt.md, inputs: [input.md], outputs: [output.md] }
`);

    expect(await captureCli([
      "run", "workflows/entry.yml", "--id", "reachable-only"
    ], repo)).toMatchObject({ exitCode: 0 });
    const store = await openAgentFlowRunState({ cwd: repo });
    expect(Object.keys(store.getRun("reachable-only")?.context.workflowRegistry as object))
      .toEqual(["reachable-entry"]);
    store.close();
  });

  test("does not rewrite fixture state before acquiring an active run lease", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-cli-fixture-lock-"));
    fs.mkdirSync(path.join(repo, ".git"));
    fs.writeFileSync(path.join(repo, "workflow.yml"), `
name: cli-fixture-lock
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: fixture }
steps:
  - { id: write, type: session_request, session: writer, prompt: prompts/write.md, inputs: [input.txt], outputs: [response.md] }
`);
    fs.writeFileSync(path.join(repo, "replacement.json"), JSON.stringify({
      inputs: {},
      artifacts: { "input.txt": "replacement" },
      steps: { write: { outputs: { "response.md": "replacement" } } }
    }));
    const workflow = parseAgentFlowWorkflowOrThrow(fs.readFileSync(path.join(repo, "workflow.yml"), "utf8"));
    const owner = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(owner, { id: "cli-fixture-lock", workflow, inputs: {} });
    owner.updateRun("cli-fixture-lock", {
      context: { ...owner.getRun("cli-fixture-lock")!.context, cliFixturePath: path.join(repo, "original.json") }
    });
    owner.writeArtifact({
      id: "fixture:1",
      runId: "cli-fixture-lock",
      stepId: "fixture",
      path: "input.txt",
      kind: "fixture",
      contentType: "text/plain; charset=utf-8",
      content: "original"
    });
    const originalGeneration = owner.getArtifact("cli-fixture-lock", "input.txt")!.generation;
    const lock = owner.acquireRunLock("cli-fixture-lock", "run", { ttlMs: 60_000 });
    owner.transitionRunWithEvent("cli-fixture-lock", {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: { status: "running" } }
    });

    const contender = await captureCli([
      "run", "workflow.yml", "--id", "cli-fixture-lock", "--fixture", "replacement.json"
    ], repo);

    expect(contender.exitCode).not.toBe(0);
    expect(contender.stderr).toContain("is locked for run");
    expect(owner.getRun("cli-fixture-lock")?.context.cliFixturePath).toBe(path.join(repo, "original.json"));
    expect(owner.readArtifact("cli-fixture-lock", "input.txt").content.toString()).toBe("original");
    expect(owner.getArtifact("cli-fixture-lock", "input.txt")?.generation).toBe(originalGeneration);
    owner.releaseRunLock(lock);
    owner.close();
  });

  test("retains a replacement fixture after a stale recovered run pauses", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-cli-recovered-fixture-"));
    fs.mkdirSync(path.join(repo, ".git"));
    fs.mkdirSync(path.join(repo, "prompts"));
    fs.writeFileSync(path.join(repo, "prompts", "draft.md"), "Draft.\n");
    fs.writeFileSync(path.join(repo, "workflow.yml"), `
name: recovered-fixture
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: fixture }
steps:
  - { id: first, type: session_request, session: writer, prompt: prompts/draft.md, inputs: [request.md], outputs: [first.md] }
  - { id: approve, type: manual_gate, message: Continue?, options: [approve, cancel] }
  - { id: second, type: session_request, session: writer, prompt: prompts/draft.md, inputs: [request.md], outputs: [second.md] }
`);
    const originalFixturePath = path.join(repo, "original.json");
    const replacementFixturePath = path.join(repo, "replacement.json");
    fs.writeFileSync(originalFixturePath, JSON.stringify({
      steps: {
        first: { outputs: { "first.md": "original first" } },
        second: { outputs: { "second.md": "original second" } }
      }
    }));
    fs.writeFileSync(replacementFixturePath, JSON.stringify({
      steps: {
        first: { outputs: { "first.md": "replacement first" } },
        second: { outputs: { "second.md": "replacement second" } }
      }
    }));
    const workflow = parseAgentFlowWorkflowOrThrow(fs.readFileSync(path.join(repo, "workflow.yml"), "utf8"));
    const interrupted = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(interrupted, { id: "recovered-fixture", workflow });
    interrupted.updateRun("recovered-fixture", {
      context: { ...interrupted.getRun("recovered-fixture")!.context, cliFixturePath: originalFixturePath }
    });
    interrupted.writeArtifact({
      id: "request",
      runId: "recovered-fixture",
      path: "request.md",
      kind: "fixture",
      contentType: "text/plain; charset=utf-8",
      content: "Request"
    });
    interrupted.acquireRunLock("recovered-fixture", "run", { ttlMs: 60_000 });
    interrupted.transitionRunWithEvent("recovered-fixture", {
      status: "running",
      allowedFrom: ["pending"],
      event: { type: "run.started", payload: { status: "running" } }
    });
    interrupted.close();

    expect(await captureCli([
      "run", "workflow.yml", "--id", "recovered-fixture", "--fixture", "replacement.json"
    ], repo)).toMatchObject({ exitCode: 3 });
    let store = await openAgentFlowRunState({ cwd: repo });
    expect(store.getRun("recovered-fixture")?.context.cliFixturePath).toBe(replacementFixturePath);
    expect(store.readArtifact("recovered-fixture", "first.md").content.toString()).toBe("replacement first");
    store.close();

    expect(await captureCli([
      "resume", "recovered-fixture", "--outcome", "approve"
    ], repo)).toMatchObject({ exitCode: 0 });
    store = await openAgentFlowRunState({ cwd: repo });
    expect(store.readArtifact("recovered-fixture", "second.md").content.toString()).toBe("replacement second");
    store.close();
  });

  test("renders terminal notifications and retained artifact status through the CLI", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-cli-notification-"));
    fs.mkdirSync(path.join(repo, ".git"));
    fs.writeFileSync(path.join(repo, "workflow.yml"), `
name: cli-notification
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: check, type: command, command: "printf 'passed\\n'" }
notify:
  - { on: workflow.completed, channels: [terminal] }
retention:
  on_success:
    delete: [logs/**]
`);

    const run = await captureCli(["run", "workflow.yml", "--id", "cli-notification"], repo);

    expect(run).toMatchObject({ exitCode: 0, stderr: "" });
    expect(run.stdout).toContain("Notification: Agent Flow workflow cli-notification run cli-notification completed.");
    const artifacts = await captureCli(["artifacts", "cli-notification"], repo);
    expect(artifacts.stdout).toContain("final-summary.md\tavailable\trun_summary");
    expect(artifacts.stdout).toMatch(/logs\/check-[a-f0-9]{8}\/attempt-1\/stdout\.log\tmissing\tcommand_log/);
  });

  test("cleans runs by age and status while retaining required evidence", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-cli-cleanup-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: retained-runs
version: 1
style: pipeline
maturity: experimental
steps: []
retention:
  on_success: { delete: ["**"], after_days: 30 }
  on_failure: { keep_all_for_days: 30 }
  on_cancelled: { ask_user: true }
`);
    const store = await openAgentFlowRunState({ cwd: repo, now: () => "2026-01-01T00:00:00.000Z" });
    for (const status of ["completed", "failed", "cancelled", "paused"] as const) {
      const runId = `retained-${status}`;
      createAgentFlowLifecycleRun(store, { id: runId, workflow });
      store.updateRun(runId, { status });
      store.writeArtifact({
        id: `temporary-${status}`,
        runId,
        path: "temp/output.txt",
        kind: "temporary",
        contentType: "text/plain",
        content: status
      });
      if (status === "completed") {
        writeAgentFlowFinalSummary(store, runId, workflow, { status, completedSteps: [] });
      }
      if (status === "failed") {
        store.writeArtifact({
          id: "failure-evidence",
          runId,
          path: "failures/evidence.json",
          kind: "failure_payload",
          contentType: "application/json",
          content: "{}\n"
        });
        store.writeArtifact({
          id: "decision-evidence",
          runId,
          path: "decision-records/failure.json",
          kind: "decision_record",
          contentType: "application/json",
          content: "{}\n"
        });
      }
    }
    store.close();

    expect(await captureCli(["cleanup", "--older-than", "30d", "--status", "completed"], repo))
      .toMatchObject({ exitCode: 0, stdout: expect.stringContaining("retained-completed") });
    expect(await captureCli(["cleanup", "--older-than", "30d", "--status", "failed"], repo))
      .toMatchObject({ exitCode: 0, stdout: expect.stringContaining("retained-failed") });
    expect(await captureCli(["cleanup", "retained-cancelled"], repo))
      .toMatchObject({ exitCode: 0, stdout: expect.stringContaining("skipped=1") });
    let inspected = await openAgentFlowRunState({ cwd: repo });
    expect(inspected.getArtifact("retained-cancelled", "temp/output.txt")?.status).toBe("available");
    inspected.close();
    expect(await captureCli(["cleanup", "retained-cancelled", "--approve"], repo))
      .toMatchObject({ exitCode: 0, stdout: expect.stringContaining("deleted=1") });
    expect(await captureCli(["cleanup", "retained-paused"], repo))
      .toMatchObject({ exitCode: 0, stdout: expect.stringContaining("not_configured") });

    inspected = await openAgentFlowRunState({ cwd: repo });
    expect(inspected.getArtifact("retained-completed", "temp/output.txt")?.status).toBe("missing");
    expect(inspected.getArtifact("retained-completed", "final-summary.md")?.status).toBe("available");
    expect(inspected.getArtifact("retained-failed", "temp/output.txt")?.status).toBe("missing");
    expect(inspected.getArtifact("retained-failed", "failures/evidence.json")?.status).toBe("available");
    expect(inspected.getArtifact("retained-failed", "decision-records/failure.json")?.status).toBe("available");
    expect(inspected.getArtifact("retained-cancelled", "temp/output.txt")?.status).toBe("missing");
    expect(inspected.getArtifact("retained-paused", "temp/output.txt")?.status).toBe("available");
    expect(inspected.listEvents("retained-completed").some((event) => event.type === "retention.deleted")).toBe(true);
    inspected.close();
  });

  test("continues batch cleanup when a matching run lacks its persisted workflow", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-cli-cleanup-workflow-error-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: cleanup-workflow-error
version: 1
style: pipeline
maturity: experimental
steps: []
retention:
  on_success: { delete: [temp/**] }
`);
    const store = await openAgentFlowRunState({ cwd: repo, now: () => "2026-01-01T00:00:00.000Z" });
    store.createRun({
      id: "missing-workflow",
      workflow: { name: "legacy", version: 1, style: "pipeline", maturity: "experimental" },
      status: "completed"
    });
    createAgentFlowLifecycleRun(store, { id: "cleanup-candidate", workflow });
    store.updateRun("cleanup-candidate", { status: "completed" });
    store.writeArtifact({
      id: "cleanup-candidate-temporary",
      runId: "cleanup-candidate",
      path: "temp/output.txt",
      kind: "temporary",
      contentType: "text/plain",
      content: "cleanup candidate"
    });
    store.close();

    const result = await captureCli(["cleanup", "--older-than", "30d", "--status", "completed"], repo);

    expect(result).toMatchObject({
      exitCode: 2,
      stdout: expect.stringContaining("missing-workflow\tcompleted\tworkflow_error"),
      stderr: expect.stringContaining("could not process 1 run")
    });
    const inspected = await openAgentFlowRunState({ cwd: repo });
    expect(inspected.getArtifact("cleanup-candidate", "temp/output.txt")?.status).toBe("missing");
    inspected.close();
  });

  test("continues batch cleanup after a run retention transaction fails", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-cli-cleanup-run-error-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: cleanup-run-error
version: 1
style: pipeline
maturity: experimental
steps: []
retention:
  on_success: { delete: [temp/**] }
`);
    const store = await openAgentFlowRunState({ cwd: repo, now: () => "2026-01-01T00:00:00.000Z" });
    for (const runId of ["a-broken", "z-cleanup-candidate"]) {
      createAgentFlowLifecycleRun(store, { id: runId, workflow });
      store.updateRun(runId, { status: "completed" });
      store.writeArtifact({
        id: `${runId}-temporary`,
        runId,
        path: "temp/output.txt",
        kind: "temporary",
        contentType: "text/plain",
        content: "cleanup candidate"
      });
    }
    const databasePath = store.databasePath;
    store.close();
    const database = new Database(databasePath);
    database.exec(`
      CREATE TRIGGER reject_first_run_cleanup_audit
      BEFORE INSERT ON events
      WHEN NEW.type = 'retention.deleted' AND NEW.run_id = 'a-broken'
      BEGIN
        SELECT RAISE(ABORT, 'reject first run cleanup audit');
      END
    `);
    database.close();

    const result = await captureCli(["cleanup", "--older-than", "30d", "--status", "completed"], repo);

    expect(result.exitCode).toBe(2);
    expect(result.stdout.includes("a-broken\tcompleted\trun_error")).toBe(true);
    expect(result.stdout.includes("z-cleanup-candidate\tcompleted\tapplied\tdeleted=1")).toBe(true);
    expect(result.stderr.includes("could not process 1 run")).toBe(true);
    const inspected = await openAgentFlowRunState({ cwd: repo });
    expect(inspected.getArtifact("a-broken", "temp/output.txt")?.status).toBe("available");
    expect(inspected.getArtifact("z-cleanup-candidate", "temp/output.txt")?.status).toBe("missing");
    inspected.close();
  });

  test("fails cleanup when an artifact backing cannot be deleted", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-cli-cleanup-failure-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: cleanup-failure
version: 1
style: pipeline
maturity: experimental
steps: []
retention:
  on_success: { delete: ["**"] }
`);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "cleanup-failure", workflow });
    store.updateRun("cleanup-failure", { status: "completed" });
    store.writeArtifact({
      id: "temporary",
      runId: "cleanup-failure",
      path: "temp/output.txt",
      kind: "temporary",
      contentType: "text/plain",
      content: "retained after failure"
    });
    store.close();

    const originalDelete = AgentFlowRunStateStore.prototype.deleteArtifactBacking;
    const deleteSpy = spyOn(AgentFlowRunStateStore.prototype, "deleteArtifactBacking")
      .mockImplementation(function (runId, declaredPath) {
        if (runId === "cleanup-failure") throw new Error("simulated deletion failure");
        return originalDelete.call(this, runId, declaredPath);
      });
    let result: Awaited<ReturnType<typeof captureCli>>;
    try {
      result = await captureCli(["cleanup", "cleanup-failure"], repo);
    } finally {
      deleteSpy.mockRestore();
    }

    expect(result).toMatchObject({
      exitCode: 2,
      stdout: expect.stringContaining("failed=1"),
      stderr: expect.stringContaining("could not delete 1 artifact")
    });
    const inspected = await openAgentFlowRunState({ cwd: repo });
    expect(inspected.getArtifact("cleanup-failure", "temp/output.txt")?.status).toBe("available");
    expect(inspected.listEvents("cleanup-failure").some((event) => event.type === "retention.failed")).toBe(true);
    inspected.close();
  });

  test("rolls back explicit cleanup when its deletion audit event cannot be persisted", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-cli-cleanup-audit-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: cleanup-audit
version: 1
style: pipeline
maturity: experimental
steps: []
retention:
  on_success: { delete: [temp/**] }
`);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "cleanup-audit", workflow });
    store.updateRun("cleanup-audit", { status: "completed" });
    store.writeArtifact({
      id: "temporary",
      runId: "cleanup-audit",
      path: "temp/output.txt",
      kind: "temporary",
      contentType: "text/plain",
      content: "retain when auditing fails"
    });
    const databasePath = store.databasePath;
    store.close();

    const database = new Database(databasePath);
    database.exec(`
      CREATE TRIGGER reject_cleanup_audit
      BEFORE INSERT ON events
      WHEN NEW.type = 'retention.deleted'
      BEGIN
        SELECT RAISE(ABORT, 'reject cleanup audit');
      END
    `);
    database.close();

    expect(await captureCli(["cleanup", "cleanup-audit"], repo))
      .toMatchObject({ exitCode: 2, stderr: expect.stringContaining("reject cleanup audit") });
    const inspected = await openAgentFlowRunState({ cwd: repo });
    expect(inspected.getArtifact("cleanup-audit", "temp/output.txt")?.status).toBe("available");
    expect(inspected.readArtifact("cleanup-audit", "temp/output.txt").content.toString("utf8"))
      .toBe("retain when auditing fails");
    expect(inspected.listEvents("cleanup-audit").some((event) => event.type === "retention.deleted"))
      .toBe(false);
    inspected.close();
  });

  test("applies after-days-only retention once its explicit cleanup threshold elapses", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-cli-cleanup-after-days-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: cleanup-after-days
version: 1
style: pipeline
maturity: experimental
steps: []
retention:
  on_success: { after_days: 30 }
`);
    const store = await openAgentFlowRunState({ cwd: repo, now: () => "2026-01-01T00:00:00.000Z" });
    createAgentFlowLifecycleRun(store, { id: "cleanup-after-days", workflow });
    store.updateRun("cleanup-after-days", { status: "completed" });
    store.writeArtifact({
      id: "temporary",
      runId: "cleanup-after-days",
      path: "temp/output.txt",
      kind: "temporary",
      contentType: "text/plain",
      content: "delete after threshold"
    });
    writeAgentFlowFinalSummary(store, "cleanup-after-days", workflow, {
      status: "completed",
      completedSteps: []
    });
    expect(applyAgentFlowRetention(store, "cleanup-after-days", workflow, "completed").status).toBe("deferred");
    store.close();

    expect(await captureCli(["cleanup", "--older-than", "30d", "--status", "completed"], repo))
      .toMatchObject({ exitCode: 0, stdout: expect.stringContaining("deleted=1") });
    const inspected = await openAgentFlowRunState({ cwd: repo });
    expect(inspected.getArtifact("cleanup-after-days", "temp/output.txt")?.status).toBe("missing");
    expect(inspected.getArtifact("cleanup-after-days", "final-summary.md")?.status).toBe("available");
    inspected.close();
  });

  test("archives and exports inspectable portable ZIP artifacts", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-cli-export-"));
    fs.mkdirSync(path.join(repo, ".git"));
    fs.writeFileSync(path.join(repo, "workflow.yml"), `
name: portable-run
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: check, type: command, command: "printf portable-output" }
`);
    fs.writeFileSync(path.join(repo, "fixture.json"), '{"inputs":{},"artifacts":{},"steps":{}}\n');
    expect(await captureCli([
      "run", "workflow.yml", "--id", "portable-run", "--fixture", "fixture.json"
    ], repo))
      .toMatchObject({ exitCode: 0 });

    const archived = await captureCli(["archive", "portable-run", "--output", "portable/archive.zip"], repo);
    const exported = await captureCli(["export", "portable-run", "--format", "zip", "--output", "portable/export.zip"], repo);
    expect(archived).toMatchObject({ exitCode: 0, stdout: expect.stringContaining("Archived Agent Flow run portable-run") });
    expect(exported).toMatchObject({ exitCode: 0, stdout: expect.stringContaining("Exported Agent Flow run portable-run") });

    for (const filename of ["archive.zip", "export.zip"]) {
      const entries = readStoredZip(fs.readFileSync(path.join(repo, "portable", filename)));
      expect([...entries.keys()]).toContain("manifest.json");
      expect([...entries.keys()]).toContain("state.json");
      expect([...entries.keys()]).toContain("events.jsonl");
      expect([...entries.keys()]).toContain("artifacts/final-summary.md");
      expect([...entries.keys()].some((entry) => entry.endsWith("/stdout.log"))).toBe(true);
      const state = entries.get("state.json")!.toString("utf8");
      expect(state).toContain('"status": "completed"');
      expect(state).not.toContain("cliFixturePath");
      expect(state).not.toContain(repo);
      expect(entries.get("events.jsonl")!.toString("utf8")).toContain('"type":"run.completed"');
      expect(entries.get("manifest.json")!.toString("utf8")).not.toContain(repo);
    }
    expect(await captureCli(["archive", "portable-run", "--output", "portable/archive.zip"], repo))
      .toMatchObject({ exitCode: 2, stderr: expect.stringContaining("already exists") });
    const stagingEntries = fs.readdirSync(path.join(repo, "portable"))
      .filter((entry) => entry.startsWith(".agent-flow-archive-"));
    expect(await captureCli(["archive", "portable-run", "--output", "portable/archive.zip"], repo))
      .toMatchObject({ exitCode: 2, stderr: expect.stringContaining("already exists") });
    expect(fs.readdirSync(path.join(repo, "portable"))
      .filter((entry) => entry.startsWith(".agent-flow-archive-"))).toEqual(stagingEntries);
  });

  test("accepts flag-prefixed run IDs in maintenance commands", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-cli-maintenance-run-id-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: maintenance-run-id
version: 1
style: pipeline
maturity: experimental
steps: []
retention:
  on_success: { delete: [temp/**] }
`);
    const store = await openAgentFlowRunState({ cwd: repo });
    for (const runId of ["--nightly", "--older-than"]) {
      createAgentFlowLifecycleRun(store, { id: runId, workflow });
      store.updateRun(runId, { status: "completed" });
      store.writeArtifact({
        id: `${runId}-temporary`,
        runId,
        path: "temp/output.txt",
        kind: "temporary",
        contentType: "text/plain",
        content: "cleanup candidate"
      });
    }
    store.close();

    expect(await captureCli(["cleanup", "--nightly"], repo))
      .toMatchObject({ exitCode: 0, stdout: expect.stringContaining("--nightly") });
    expect(await captureCli(["archive", "--nightly", "--output", "portable/nightly.zip"], repo))
      .toMatchObject({ exitCode: 0 });
    expect(await captureCli(["export", "--nightly", "--format", "zip", "--output", "portable/nightly-export.zip"], repo))
      .toMatchObject({ exitCode: 0 });
    expect(await captureCli(["cleanup", "--", "--older-than"], repo))
      .toMatchObject({ exitCode: 0, stdout: expect.stringContaining("--older-than") });
  });

  test("reconciles approval staleness before enforcing the portable archive size limit", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-cli-archive-reconcile-size-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: archive-reconcile-size
version: 1
style: pipeline
maturity: experimental
steps: []
`);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "archive-reconcile-size", workflow });
    const approvalOutput = store.writeArtifact({
      id: "approval-output",
      runId: "archive-reconcile-size",
      path: "approval/a-output.bin",
      kind: "approval_output",
      contentType: "application/octet-stream",
      content: Buffer.alloc(MAX_AGENT_FLOW_PORTABLE_ARCHIVE_CONTENT_BYTES)
    });
    const approvalEvidence = store.writeArtifact({
      id: "approval-evidence",
      runId: "archive-reconcile-size",
      path: "approval/z-evidence.txt",
      kind: "fixture",
      contentType: "text/plain",
      content: "ok"
    });
    store.upsertApproval({
      runId: "archive-reconcile-size",
      id: "approval",
      status: "approved",
      context: {
        evidence: [{ path: approvalEvidence.declaredPath, checksum: approvalEvidence.checksum! }],
        output: approvalOutput.declaredPath
      }
    });
    fs.writeFileSync(path.join(repo, approvalEvidence.storagePath), "no");

    writeAgentFlowPortableArchive(store, "archive-reconcile-size", "portable/reconciled-size.zip");

    const entries = readStoredZip(fs.readFileSync(path.join(repo, "portable", "reconciled-size.zip")));
    const manifest = JSON.parse(entries.get("manifest.json")!.toString("utf8")) as {
      artifacts: Array<{ declaredPath: string; status: string; archivePath?: string }>;
    };
    expect(manifest.artifacts.find((artifact) => artifact.declaredPath === approvalOutput.declaredPath))
      .toMatchObject({ status: "stale" });
    expect(entries.has(`artifacts/${approvalOutput.declaredPath}`)).toBe(false);
    store.close();
  });

  test("excludes legacy artifact content whose registered size is missing", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-cli-archive-missing-size-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: archive-missing-size
version: 1
style: pipeline
maturity: experimental
steps: []
`);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "archive-missing-size", workflow });
    store.writeArtifact({
      id: "legacy-artifact",
      runId: "archive-missing-size",
      path: "legacy/output.txt",
      kind: "fixture",
      contentType: "text/plain",
      content: "legacy content"
    });
    const database = new Database(store.databasePath);
    database.run(
      "UPDATE artifacts SET size_bytes = NULL WHERE run_id = ? AND id = ?",
      ["archive-missing-size", "legacy-artifact"]
    );
    database.close();

    writeAgentFlowPortableArchive(store, "archive-missing-size", "portable/missing-size.zip");

    const entries = readStoredZip(fs.readFileSync(path.join(repo, "portable", "missing-size.zip")));
    const manifest = JSON.parse(entries.get("manifest.json")!.toString("utf8")) as {
      artifacts: Array<{ id: string; status: string; archivePath?: string }>;
    };
    expect(manifest.artifacts.find((artifact) => artifact.id === "legacy-artifact"))
      .toMatchObject({ status: "stale" });
    expect(manifest.artifacts.find((artifact) => artifact.id === "legacy-artifact")?.archivePath)
      .toBeUndefined();
    expect(entries.has("artifacts/legacy/output.txt")).toBe(false);
    store.close();
  });

  test("bounds archive names, snapshots state, and never replaces raced destinations", async () => {
    expect(defaultAgentFlowArchivePath("a/b")).not.toBe(defaultAgentFlowArchivePath("a b"));
    expect(defaultAgentFlowArchivePath(" archive-safety ")).toBe(defaultAgentFlowArchivePath("archive-safety"));
    expect(defaultAgentFlowExportPath("日本語")).not.toBe(defaultAgentFlowExportPath("한국어"));
    expect(Buffer.byteLength(path.basename(defaultAgentFlowArchivePath("long".repeat(100))), "utf8"))
      .toBeLessThan(128);

    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-cli-archive-safety-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: archive-safety
version: 1
style: pipeline
maturity: experimental
steps: []
`);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "archive-safety", workflow });
    store.updateRun("archive-safety", { status: "completed" });
    writeAgentFlowFinalSummary(store, "archive-safety", workflow, { status: "completed", completedSteps: [] });
    const originalSnapshot = store.withRunFinalizationTransaction.bind(store);
    let snapshotUsed = false;
    store.withRunFinalizationTransaction = ((runId, callback) => {
      snapshotUsed = true;
      return originalSnapshot(runId, callback);
    }) as typeof store.withRunFinalizationTransaction;
    writeAgentFlowPortableArchive(store, "archive-safety", "portable/snapshot.zip");
    expect(snapshotUsed).toBe(true);
    store.withRunFinalizationTransaction("archive-safety", () => {
      expect(() => writeAgentFlowPortableArchive(store, "archive-safety", "portable/nested-transaction.zip"))
        .toThrow(/active finalization transaction/);
    });
    expect(fs.existsSync(path.join(repo, "portable", "nested-transaction.zip"))).toBe(false);
    const maximumLengthOutputName = "x".repeat(255);
    writeAgentFlowPortableArchive(store, "archive-safety", `portable/${maximumLengthOutputName}`);
    expect(fs.existsSync(path.join(repo, "portable", maximumLengthOutputName))).toBe(true);
    writeAgentFlowPortableArchive(store, " archive-safety ", "portable/padded.zip");
    expect(JSON.parse(readStoredZip(fs.readFileSync(path.join(repo, "portable", "padded.zip")))
      .get("manifest.json")!.toString("utf8"))).toMatchObject({ runId: "archive-safety" });

    const originalFsync = fs.fsyncSync.bind(fs);
    let directorySyncs = 0;
    const fsyncSpy = spyOn(fs, "fsyncSync").mockImplementation((descriptor) => {
      if (fs.fstatSync(descriptor).isDirectory()) directorySyncs += 1;
      originalFsync(descriptor);
    });
    try {
      writeAgentFlowPortableArchive(store, "archive-safety", "durable/nested/archive.zip");
    } finally {
      fsyncSpy.mockRestore();
    }
    expect(directorySyncs).toBeGreaterThanOrEqual(3);
    const escapedJson = '"\\\u0001'.repeat(100_000);
    const manyJsonScalars = Array.from({ length: 100_000 }, (_, index) => index % 2 === 0);
    createAgentFlowLifecycleRun(store, {
      id: "archive-escaped-json",
      workflow,
      inputs: { escapedJson, manyJsonScalars }
    });
    writeAgentFlowPortableArchive(store, "archive-escaped-json", "portable/escaped-json.zip");
    const escapedState = JSON.parse(readStoredZip(fs.readFileSync(path.join(repo, "portable", "escaped-json.zip")))
      .get("state.json")!.toString("utf8"));
    expect(escapedState.inputs.escapedJson).toBe(escapedJson);
    expect(escapedState.inputs.manyJsonScalars).toEqual(manyJsonScalars);
    const repositoryEntries = fs.readdirSync(repo);
    expect(() => writeAgentFlowPortableArchive(store, "archive-safety", "."))
      .toThrow(/must name a file beneath the repository root/);
    expect(fs.readdirSync(repo)).toEqual(repositoryEntries);

    const racedTarget = path.join(repo, "portable", "raced.zip");
    const originalLink = fs.linkSync.bind(fs);
    const linkSpy = spyOn(fs, "linkSync").mockImplementationOnce((source, target) => {
      fs.writeFileSync(target, "raced destination", { flag: "wx" });
      originalLink(source, target);
    });
    try {
      expect(() => writeAgentFlowPortableArchive(store, "archive-safety", "portable/raced.zip"))
        .toThrow(/already exists/);
    } finally {
      linkSpy.mockRestore();
    }
    expect(fs.readFileSync(racedTarget, "utf8")).toBe("raced destination");

    const replacedStagingTarget = path.join(repo, "portable", "replaced-staging.zip");
    let replacementPath: string | undefined;
    const stagingRaceSpy = spyOn(fs, "linkSync").mockImplementationOnce((source, target) => {
      replacementPath = source.toString();
      fs.unlinkSync(source);
      fs.writeFileSync(source, "replacement bytes", { flag: "wx" });
      originalLink(source, target);
    });
    try {
      expect(() => writeAgentFlowPortableArchive(store, "archive-safety", "portable/replaced-staging.zip"))
        .toThrow(/staged archive changed during publication/);
    } finally {
      stagingRaceSpy.mockRestore();
      if (replacementPath !== undefined && fs.existsSync(replacementPath)) fs.unlinkSync(replacementPath);
    }
    expect(fs.readFileSync(replacedStagingTarget, "utf8")).toBe("replacement bytes");
    fs.unlinkSync(replacedStagingTarget);

    const concurrentlyReplacedTarget = path.join(repo, "portable", "concurrently-replaced.zip");
    const targetReplacementSpy = spyOn(fs, "linkSync").mockImplementationOnce((source, target) => {
      originalLink(source, target);
      fs.unlinkSync(target);
      fs.writeFileSync(target, "concurrent destination", { flag: "wx" });
    });
    try {
      expect(() => writeAgentFlowPortableArchive(store, "archive-safety", "portable/concurrently-replaced.zip"))
        .toThrow(/staged archive changed during publication/);
    } finally {
      targetReplacementSpy.mockRestore();
    }
    expect(fs.readFileSync(concurrentlyReplacedTarget, "utf8")).toBe("concurrent destination");

    const outside = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-cli-archive-outside-"));
    const portableParent = path.join(repo, "portable");
    const movedParent = path.join(repo, "portable-before-race");
    const directoryRaceSpy = spyOn(fs, "linkSync").mockImplementationOnce((source, target) => {
      fs.renameSync(portableParent, movedParent);
      fs.symlinkSync(outside, portableParent, "dir");
      originalLink(source, target);
    });
    try {
      expect(() => writeAgentFlowPortableArchive(store, "archive-safety", "portable/redirected.zip"))
        .toThrow(/directory changed during publication/);
    } finally {
      directoryRaceSpy.mockRestore();
      fs.unlinkSync(portableParent);
      fs.renameSync(movedParent, portableParent);
    }
    expect(fs.readdirSync(outside)).toEqual([]);
    expect(fs.existsSync(path.join(portableParent, "redirected.zip"))).toBe(true);

    const retainedOnSyncFailure = path.join(repo, "portable", "retained-on-sync-failure.zip");
    const syncFailureSpy = spyOn(fs, "fsyncSync").mockImplementation((descriptor) => {
      if (fs.fstatSync(descriptor).isDirectory()) throw new Error("simulated directory sync failure");
      originalFsync(descriptor);
    });
    try {
      expect(() => writeAgentFlowPortableArchive(store, "archive-safety", "portable/retained-on-sync-failure.zip"))
        .toThrow(/simulated directory sync failure/);
    } finally {
      syncFailureSpy.mockRestore();
    }
    expect(readStoredZip(fs.readFileSync(retainedOnSyncFailure)).has("manifest.json")).toBe(true);

    const changingArtifact = store.writeArtifact({
      id: "changing-during-archive",
      runId: "archive-safety",
      path: "changing/output.txt",
      kind: "fixture",
      contentType: "text/plain",
      content: "changing archive content"
    });
    const originalReadArtifact = store.readArtifact.bind(store);
    const changingReadSpy = spyOn(store, "readArtifact").mockImplementationOnce((runId, declaredPath, options) => {
      fs.unlinkSync(path.join(repo, changingArtifact.storagePath));
      return originalReadArtifact(runId, declaredPath, options);
    });
    try {
      expect(() => writeAgentFlowPortableArchive(store, "archive-safety", "portable/changing.zip"))
        .toThrow(/unavailable/);
    } finally {
      changingReadSpy.mockRestore();
    }
    expect(fs.existsSync(path.join(portableParent, "changing.zip"))).toBe(false);

    const recoverable = store.writeArtifact({
      id: "recoverable",
      runId: "archive-safety",
      path: "recoverable/output.txt",
      kind: "fixture",
      contentType: "text/plain",
      content: "recoverable archive content"
    });
    const recoverableTarget = path.join(repo, recoverable.storagePath);
    const recoverableStaging = path.join(path.dirname(path.dirname(recoverableTarget)), ".staging");
    const recoverableBackup = path.join(
      recoverableStaging,
      `${createHash("sha256").update(recoverable.declaredPath).digest("hex")}.deleted`
    );
    fs.mkdirSync(recoverableStaging, { recursive: true });
    fs.renameSync(recoverableTarget, recoverableBackup);
    writeAgentFlowPortableArchive(store, "archive-safety", "portable/recovered.zip");
    expect(fs.readFileSync(recoverableTarget, "utf8")).toBe("recoverable archive content");
    expect(readStoredZip(fs.readFileSync(path.join(portableParent, "recovered.zip")))
      .get("artifacts/recoverable/output.txt")?.toString("utf8")).toBe("recoverable archive content");

    const approvalOutput = store.writeArtifact({
      id: "archive-approval-output",
      runId: "archive-safety",
      path: "approval/a-output.json",
      kind: "approval_output",
      contentType: "application/json",
      content: "{}"
    });
    const approvalEvidence = store.writeArtifact({
      id: "archive-approval-evidence",
      runId: "archive-safety",
      path: "approval/z-evidence.txt",
      kind: "fixture",
      contentType: "text/plain",
      content: "ok"
    });
    store.upsertApproval({
      runId: "archive-safety",
      id: "archive-approval",
      status: "approved",
      context: {
        evidence: [{ path: approvalEvidence.declaredPath, checksum: approvalEvidence.checksum! }],
        output: approvalOutput.declaredPath
      }
    });
    fs.writeFileSync(path.join(repo, approvalEvidence.storagePath), "no");
    writeAgentFlowPortableArchive(store, "archive-safety", "portable/reconciled-approval.zip");
    const reconciledEntries = readStoredZip(fs.readFileSync(path.join(portableParent, "reconciled-approval.zip")));
    const reconciledManifest = JSON.parse(reconciledEntries.get("manifest.json")!.toString("utf8")) as {
      artifacts: Array<{ declaredPath: string; status: string; archivePath?: string }>;
    };
    expect(reconciledManifest.artifacts.find((artifact) => artifact.declaredPath === approvalOutput.declaredPath))
      .toMatchObject({ status: "stale" });
    expect(reconciledManifest.artifacts.find((artifact) => artifact.declaredPath === approvalOutput.declaredPath)?.archivePath)
      .toBeUndefined();
    expect(reconciledEntries.has(`artifacts/${approvalOutput.declaredPath}`)).toBe(false);

    const snapshotBytesBeforeFailure = store.runSnapshotStructuredBytes("archive-safety");
    const largeFailureMessage = "failure scalar ".repeat(256);
    store.recordFailure({
      id: "archive-size-fixture",
      runId: "archive-safety",
      classification: "implementation_error",
      message: largeFailureMessage,
      retryable: false
    });
    expect(store.runSnapshotStructuredBytes("archive-safety") - snapshotBytesBeforeFailure)
      .toBeGreaterThanOrEqual(Buffer.byteLength(largeFailureMessage, "utf8"));

    const eventPages: Array<{
      offset?: number;
      limit: number;
      after?: { sortValue: string | number; id?: string };
    } | undefined> = [];
    for (let sequence = 0; sequence < 130; sequence += 1) {
      store.appendRunEvent("archive-safety", { type: "archive.page.fixture", payload: { sequence } });
    }
    const originalListEvents = store.listEvents.bind(store);
    const eventPageSpy = spyOn(store, "listEvents").mockImplementation((runId, page) => {
      eventPages.push(page);
      return originalListEvents(runId, page);
    });
    try {
      writeAgentFlowPortableArchive(store, "archive-safety", "portable/paged.zip");
    } finally {
      eventPageSpy.mockRestore();
    }
    expect(eventPages[0]).toEqual({ limit: 128, after: { sortValue: 0, id: "" } });
    expect(eventPages[1]).toMatchObject({
      limit: 128,
      after: { sortValue: expect.any(Number), id: expect.any(String) }
    });
    expect(eventPages.every((page) => page?.offset === undefined)).toBe(true);

    const structuredBytesSpy = spyOn(store, "runSnapshotStructuredBytes")
      .mockReturnValue(MAX_AGENT_FLOW_PORTABLE_ARCHIVE_CONTENT_BYTES + 1);
    const unboundedEventSpy = spyOn(store, "listEvents");
    try {
      expect(() => writeAgentFlowPortableArchive(store, "archive-safety", "portable/oversized-history.zip"))
        .toThrow(/archive content exceeds/);
      expect(unboundedEventSpy).not.toHaveBeenCalled();
    } finally {
      unboundedEventSpy.mockRestore();
      structuredBytesSpy.mockRestore();
    }

    store.writeArtifact({
      id: "oversized",
      runId: "archive-safety",
      path: "oversized.bin",
      kind: "fixture",
      contentType: "application/octet-stream",
      content: Buffer.alloc(MAX_AGENT_FLOW_PORTABLE_ARCHIVE_CONTENT_BYTES)
    });
    const artifactInspectionSpy = spyOn(store, "listArtifacts");
    try {
      expect(() => writeAgentFlowPortableArchive(store, "archive-safety", "portable/oversized.zip"))
        .toThrow(/archive content exceeds/);
      expect(artifactInspectionSpy).toHaveBeenCalled();
    } finally {
      artifactInspectionSpy.mockRestore();
    }
    expect(fs.existsSync(path.join(repo, "portable", "oversized.zip"))).toBe(false);
    const oversizedBacking = path.join(repo, store.getArtifact("archive-safety", "oversized.bin")!.storagePath);
    const oversizedDescriptor = fs.openSync(oversizedBacking, "r+");
    try {
      fs.writeSync(oversizedDescriptor, Buffer.from([1]), 0, 1, 0);
    } finally {
      fs.closeSync(oversizedDescriptor);
    }
    writeAgentFlowPortableArchive(store, "archive-safety", "portable/stale-excluded.zip");
    expect(fs.existsSync(path.join(repo, "portable", "stale-excluded.zip"))).toBe(true);
    expect(store.getArtifact("archive-safety", "oversized.bin")?.status).toBe("stale");
    store.close();
  });

  test("returns failure when a required lifecycle notification cannot be delivered", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-cli-pause-notification-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: cli-pause-notification
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: check, type: command, command: "printf ok" }
notify:
  - { on: workflow.paused, channels: [unregistered], required: true }
`);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "cli-pause-notification", workflow });
    store.close();

    const paused = await captureCli(["pause", "cli-pause-notification"], repo);

    expect(paused).toMatchObject({
      exitCode: 1,
      stderr: expect.stringContaining(
        "Required unregistered notification for workflow.paused failed"
      )
    });
    expect(paused.stdout).toContain("Failed to pause Agent Flow run cli-pause-notification.");
    expect(paused.stdout).toContain("Status: failed");
    expect(paused.stdout).not.toContain("Paused Agent Flow run");
  });

  test("resumes manual gates and input requests with explicit CLI values", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-cli-interaction-"));
    fs.mkdirSync(path.join(repo, ".git"));
    fs.writeFileSync(path.join(repo, "workflow.yml"), `
name: interactive
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: approve, type: manual_gate, message: Continue?, options: [approve, pause, cancel] }
  - { id: details, type: input_request, question: Target?, save_as: answers/target.json }
  - { id: finish, type: command, command: "printf 'done\\n' > finished.txt" }
`);

    const started = await captureCli(["run", "workflow.yml", "--id", "interactive-run"], repo);
    expect(started).toMatchObject({ exitCode: 3 });
    expect(started.stdout).toContain("Status: paused");
    const status = await captureCli(["status", "interactive-run"], repo);
    expect(status.stdout).toContain("Waiting reason: manual_approval");
    expect(status.stdout).toContain("Valid outcomes: approve, pause, cancel");

    const invalid = await captureCli(["resume", "interactive-run", "--outcome", "ship"], repo);
    expect(invalid).toMatchObject({ exitCode: 2 });
    expect(invalid.stderr).toContain("valid outcomes are: approve, pause, cancel");
    const approved = await captureCli(["resume", "interactive-run", "--outcome", "approve"], repo);
    expect(approved).toMatchObject({ exitCode: 3 });
    expect(approved.stdout).toContain("Completed steps: approve");
    const inputStatus = await captureCli(["status", "interactive-run"], repo);
    expect(inputStatus.stdout).toContain("Waiting reason: missing_input");
    expect(inputStatus.stdout).toContain("Answer artifact: answers/target.json");
    expect(inputStatus.stdout).not.toContain("Valid outcomes:");

    const answered = await captureCli([
      "resume",
      "interactive-run",
      "--answer",
      '{"environment":"staging"}'
    ], repo);
    expect(answered).toMatchObject({ exitCode: 0 });
    expect(answered.stdout).toContain("Status: completed");
    expect((await captureCli(["artifacts", "interactive-run"], repo)).stdout).toContain("answers/target.json");
    expect(fs.readFileSync(path.join(repo, "finished.txt"), "utf8")).toBe("done\n");
  });

  test("attributes CLI human approval decisions to the human actor by default", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-cli-human-approval-"));
    fs.mkdirSync(path.join(repo, ".git"));
    fs.writeFileSync(path.join(repo, "workflow.yml"), `
name: cli-human-approval
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: prepare, type: command, command: "printf 'Release candidate\\n' > release.md", outputs: [release.md] }
  - { id: approve, type: approval, reviewer: human, artifacts: [release.md] }
`);

    expect(await captureCli(["run", "workflow.yml", "--id", "cli-human-approval"], repo)).toMatchObject({
      exitCode: 3
    });
    expect(await captureCli([
      "resume",
      "cli-human-approval",
      "--outcome",
      "approve"
    ], repo)).toMatchObject({ exitCode: 0 });

    const store = await openAgentFlowRunState({ cwd: repo });
    expect(store.listApprovals("cli-human-approval")).toEqual([
      expect.objectContaining({ status: "approved", decidedBy: "human", decision: "approve" })
    ]);
    store.close();
  });

  test("restores the CLI fixture provider after a manual gate", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-cli-fixture-resume-"));
    fs.mkdirSync(path.join(repo, ".git"));
    fs.mkdirSync(path.join(repo, "prompts"));
    fs.writeFileSync(path.join(repo, "prompts", "draft.md"), "Draft.\n");
    fs.writeFileSync(path.join(repo, "workflow.yml"), `
name: interactive-fixture
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: fixture }
steps:
  - { id: approve, type: manual_gate, message: Continue?, options: [approve, cancel] }
  - { id: confirm, type: manual_gate, message: Really continue?, options: [approve, cancel] }
  - { id: draft, type: session_request, session: writer, prompt: prompts/draft.md, inputs: [request.md], outputs: [response.md] }
`);
    fs.writeFileSync(path.join(repo, "fixture.json"), JSON.stringify({
      artifacts: { "request.md": "Request" },
      steps: { draft: { outputs: { "response.md": "Resumed response" } } }
    }));

    expect(await captureCli([
      "run",
      "workflow.yml",
      "--id",
      "fixture-interaction",
      "--fixture",
      "fixture.json"
    ], repo)).toMatchObject({ exitCode: 3 });
    const resumed = await captureCli([
      "resume",
      "fixture-interaction",
      "--outcome",
      "approve"
    ], repo);

    expect(resumed).toMatchObject({ exitCode: 3 });
    const confirmed = await captureCli([
      "resume",
      "fixture-interaction",
      "--outcome",
      "approve"
    ], repo);
    expect(confirmed).toMatchObject({ exitCode: 0 });
    expect(confirmed.stdout).toContain("Status: completed");
    expect((await captureCli(["artifacts", "fixture-interaction"], repo)).stdout).toContain("response.md");

    expect(await captureCli([
      "run",
      "workflow.yml",
      "--id",
      "fixture-invalid-resume",
      "--fixture",
      "fixture.json"
    ], repo)).toMatchObject({ exitCode: 3 });
    fs.writeFileSync(path.join(repo, "fixture.json"), JSON.stringify({
      artifacts: { "request.md": "Request" },
      steps: { draft: { outputs: ["response.md"] } }
    }));
    const invalidResumeFixture = await captureCli([
      "resume",
      "fixture-invalid-resume",
      "--outcome",
      "approve"
    ], repo);
    expect(invalidResumeFixture).toMatchObject({ exitCode: 2 });
    expect(invalidResumeFixture.stderr).toContain("array-form outputs are simulation-only");
    expect((await captureCli(["status", "fixture-invalid-resume"], repo)).stdout).toContain("Status: paused");

    fs.writeFileSync(path.join(repo, "fixture.json"), JSON.stringify({
      artifacts: { "request.md": "Request" },
      steps: { draft: { outputs: { "response.md": "Resumed response" } } }
    }));
    fs.writeFileSync(path.join(repo, "replacement.json"), JSON.stringify({
      artifacts: { "request.md": "Request" },
      steps: { draft: { outcome: "failed", outputs: { "response.md": "Replacement response" } } }
    }));
    const pausedWithReplacement = await captureCli([
      "resume",
      "fixture-invalid-resume",
      "--outcome",
      "approve",
      "--fixture",
      "replacement.json"
    ], repo);
    expect(pausedWithReplacement).toMatchObject({ exitCode: 3 });
    fs.writeFileSync(path.join(repo, "replacement.json"), JSON.stringify({
      artifacts: { "request.md": "Request" },
      steps: { draft: { outputs: { "response.md": "Replacement response" } } }
    }));
    const resumedWithPersistedReplacement = await captureCli([
      "resume",
      "fixture-invalid-resume",
      "--outcome",
      "approve"
    ], repo);
    expect(resumedWithPersistedReplacement).toMatchObject({ exitCode: 0 });

    expect(await captureCli([
      "run",
      "workflow.yml",
      "--id",
      "fixture-subdirectory-resume",
      "--fixture",
      "fixture.json"
    ], repo)).toMatchObject({ exitCode: 3 });
    fs.mkdirSync(path.join(repo, "nested"));
    const resumedFromSubdirectory = await captureCli([
      "resume",
      "fixture-subdirectory-resume",
      "--outcome",
      "approve"
    ], path.join(repo, "nested"));
    expect(resumedFromSubdirectory).toMatchObject({ exitCode: 3 });
    const confirmedFromSubdirectory = await captureCli([
      "resume",
      "fixture-subdirectory-resume",
      "--outcome",
      "approve"
    ], path.join(repo, "nested"));
    expect(confirmedFromSubdirectory).toMatchObject({ exitCode: 0 });
    expect(confirmedFromSubdirectory.stdout).toContain("Status: completed");
  });

  test("runs session requests through an explicit CLI fixture provider", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-cli-session-"));
    fs.mkdirSync(path.join(repo, ".git"));
    fs.mkdirSync(path.join(repo, "prompts"));
    fs.writeFileSync(path.join(repo, "prompts", "draft.md"), "Draft.\n");
    fs.writeFileSync(path.join(repo, "workflow.yml"), `name: fixture-session
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: fixture }
steps:
  - { id: draft, type: session_request, session: writer, prompt: prompts/draft.md, inputs: [request.md], outputs: [response.md], on_failure: { retry: 1, then: pause } }
`);
    fs.writeFileSync(path.join(repo, "fixture.json"), JSON.stringify({
      artifacts: { "request.md": "Request" },
      steps: { draft: { outputs: { "nested/../response.md": "Fixture response" } } }
    }));

    const withoutFixture = await captureCli(["run", "workflow.yml", "--id", "missing-fixture"], repo);
    expect(withoutFixture).toMatchObject({ exitCode: 1 });
    expect(withoutFixture.stderr).toContain("require --fixture");
    const run = await captureCli(["run", "workflow.yml", "--id", "fixture-run", "--fixture", "fixture.json"], repo);

    expect(run).toMatchObject({ exitCode: 0 });
    expect(run.stdout).toContain("Status: completed");
    expect((await captureCli(["artifacts", "fixture-run"], repo)).stdout).toContain("response.md");

    fs.writeFileSync(path.join(repo, "fixture.json"), JSON.stringify({
      artifacts: { "request.md": "Request" },
      steps: { draft: { outcome: ["failed", "succeeded"], outputs: { "response.md": "Retried response" } } }
    }));
    const retried = await captureCli(["run", "workflow.yml", "--id", "fixture-retry", "--fixture", "fixture.json"], repo);
    expect(retried).toMatchObject({ exitCode: 0 });
    expect(retried.stdout).toContain("Status: completed");

    fs.writeFileSync(path.join(repo, "fixture.json"), JSON.stringify({
      artifacts: { "request.md": "Request" },
      steps: { draft: { outputs: ["response.md"] } }
    }));
    const arrayOutputs = await captureCli(["run", "workflow.yml", "--id", "array-outputs", "--fixture", "fixture.json"], repo);
    expect(arrayOutputs).toMatchObject({ exitCode: 2 });
    expect(arrayOutputs.stderr).toContain("array-form outputs are simulation-only");
    expect(await captureCli(["status", "array-outputs"], repo)).toMatchObject({ exitCode: 4 });

    fs.writeFileSync(path.join(repo, "nested.yml"), `name: nested-fixture-session
version: 1
style: pipeline
maturity: experimental
sessions:
  writer: { provider: fixture }
steps:
  - id: bounded
    type: loop
    max_iterations: 1
    body:
      - { id: " nested-draft ", type: " session_request ", session: writer, prompt: prompts/draft.md, inputs: [request.md], outputs: [response.md] }
`);
    const nestedWithoutFixture = await captureCli(["run", "nested.yml", "--id", "nested-missing-fixture"], repo);
    expect(nestedWithoutFixture).toMatchObject({ exitCode: 1 });
    expect(nestedWithoutFixture.stderr).toContain("require --fixture");

    fs.writeFileSync(path.join(repo, "fixture.json"), JSON.stringify({
      artifacts: { "request.md": "Request" },
      steps: { "nested-draft": { outputs: ["response.md"] } }
    }));
    const nestedArrayOutputs = await captureCli(["run", "nested.yml", "--id", "nested-array-outputs", "--fixture", "fixture.json"], repo);
    expect(nestedArrayOutputs).toMatchObject({ exitCode: 2 });
    expect(nestedArrayOutputs.stderr).toContain("array-form outputs are simulation-only");
    expect(await captureCli(["status", "nested-array-outputs"], repo)).toMatchObject({ exitCode: 4 });

    fs.writeFileSync(path.join(repo, "fixture.json"), JSON.stringify({
      artifacts: { "request.md": "Request" },
      steps: { "nested-draft": { outputs: { "response.md": "Response" } } }
    }));
    fs.writeFileSync(path.join(repo, "nested-unsupported.yml"), fs.readFileSync(path.join(repo, "nested.yml"), "utf8")
      .replace("provider: fixture", "provider: local"));
    const nestedUnsupported = await captureCli(["run", "nested-unsupported.yml", "--id", "nested-unsupported", "--fixture", "fixture.json"], repo);
    expect(nestedUnsupported).toMatchObject({ exitCode: 1 });
    expect(nestedUnsupported.stderr).toContain('supports only provider "fixture"');

    fs.writeFileSync(path.join(repo, "fixture.json"), JSON.stringify({
      artifacts: { "request.md": "First", "inputs/../request.md": "Second" },
      steps: { draft: { outputs: { "response.md": "Response" } } }
    }));
    const collidingArtifacts = await captureCli(["run", "workflow.yml", "--id", "colliding-artifacts", "--fixture", "fixture.json"], repo);
    expect(collidingArtifacts).toMatchObject({ exitCode: 2 });
    expect(collidingArtifacts.stderr).toContain("collide at canonical path request.md");
    expect(await captureCli(["status", "colliding-artifacts"], repo)).toMatchObject({ exitCode: 4 });

    fs.writeFileSync(path.join(repo, "fixture.json"), JSON.stringify({
      artifacts: { "request.md": "Request" },
      steps: { draft: { outputs: { "response.md": "Response" } } }
    }));
    fs.writeFileSync(path.join(repo, "unsupported.yml"), fs.readFileSync(path.join(repo, "workflow.yml"), "utf8")
      .replace("provider: fixture", "provider: local")
      .replace("steps:\n", 'steps:\n  - { id: side_effect, type: command, command: "printf side-effect > should-not-exist.txt" }\n'));
    const unsupported = await captureCli(["run", "unsupported.yml", "--id", "unsupported", "--fixture", "fixture.json"], repo);
    expect(unsupported).toMatchObject({ exitCode: 1 });
    expect(unsupported.stderr).toContain('supports only provider "fixture"');
    expect(fs.existsSync(path.join(repo, "should-not-exist.txt"))).toBe(false);
  });

  test("runs automated disagreement rounds from CLI fixtures", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-cli-disagreement-"));
    fs.mkdirSync(path.join(repo, ".git"));
    fs.writeFileSync(path.join(repo, "workflow.yml"), `name: fixture-disagreement
version: 1
style: collaborative
maturity: experimental
collaboration:
  enabled: true
  max_review_cycles: 1
  on_disagreement: { strategy: arbiter, arbiter: arbiter, max_rounds: 1 }
sessions:
  implementer: { provider: fixture, role: implementer }
  reviewer: { provider: fixture, role: reviewer, authority: { can_request_changes: true, can_approve: true } }
  arbiter: { provider: fixture, role: arbiter, authority: { can_request_changes: true, can_approve: true } }
steps:
  - { id: review, type: review, reviewer: reviewer, subject: implementer, artifacts: [implementation.md], outputs: [review.json], then: route }
  - id: route
    type: condition
    branches:
      - { if: 'artifacts.review.status == "approved"', then: done }
      - { if: 'artifacts.review.status == "changes_requested"', then: revise }
    else: fail
  - { id: revise, type: command, command: "true", then: review }
  - { id: done, type: result, status: completed }
`);
    fs.writeFileSync(path.join(repo, "fixture.json"), JSON.stringify({
      artifacts: { "implementation.md": "Implementation" },
      steps: {
        review: {
          outputs: {
            "review.json": {
              status: "changes_requested",
              findings: [{ summary: "Needs another revision." }]
            }
          },
          disagreement: "approved"
        }
      }
    }));

    const run = await captureCli([
      "run", "workflow.yml", "--id", "fixture-disagreement", "--fixture", "fixture.json"
    ], repo);

    expect(run).toMatchObject({ exitCode: 0 });
    expect(run.stdout).toContain("Status: completed");
    expect((await captureCli(["artifacts", "fixture-disagreement"], repo)).stdout)
      .toContain("disagreements/review-c97ace4c8fef/round-1.json");

    fs.writeFileSync(path.join(repo, "unsupported-resolver.yml"), fs.readFileSync(path.join(repo, "workflow.yml"), "utf8")
      .replace("arbiter: { provider: fixture", "arbiter: { provider: local")
      .replace("steps:\n", 'steps:\n  - { id: side_effect, type: command, command: "printf side-effect > should-not-exist.txt" }\n'));
    const unsupportedResolver = await captureCli([
      "run", "unsupported-resolver.yml", "--id", "unsupported-resolver", "--fixture", "fixture.json"
    ], repo);
    expect(unsupportedResolver).toMatchObject({ exitCode: 1 });
    expect(unsupportedResolver.stderr).toContain('unsupported providers: local');
    expect(fs.existsSync(path.join(repo, "should-not-exist.txt"))).toBe(false);

    fs.writeFileSync(path.join(repo, "unsupported-owner.yml"), fs.readFileSync(path.join(repo, "workflow.yml"), "utf8")
      .replace("on_disagreement: { strategy: arbiter, arbiter: arbiter, max_rounds: 1 }", "on_disagreement: { strategy: owner_decides }")
      .replace(
        "implementer: { provider: fixture, role: implementer }",
        "implementer: { provider: local, role: implementer, authority: { can_request_changes: true, can_approve: true } }"
      )
      .replace("steps:\n", 'steps:\n  - { id: owner_side_effect, type: command, command: "printf side-effect > owner-side-effect.txt" }\n'));
    const unsupportedOwner = await captureCli([
      "run", "unsupported-owner.yml", "--id", "unsupported-owner", "--fixture", "fixture.json"
    ], repo);
    expect(unsupportedOwner).toMatchObject({ exitCode: 1 });
    expect(unsupportedOwner.stderr).toContain('unsupported providers: local');
    expect(fs.existsSync(path.join(repo, "owner-side-effect.txt"))).toBe(false);
  });

  test("surfaces validation warnings while preserving a successful exit", () => {
    const fixturePath = path.join(repoRoot, "tests/fixtures/agent-flow/invalid/missing-artifact.yml");
    const result = dispatch(["validate", fixturePath]);

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain("passed with 1 warning");
    expect(result.stdout).toContain("workflow.lint.artifact.read_before_write");
  });

  test("reports missing workflow paths and parse failures", () => {
    expect(dispatch(["validate"])).toEqual({
      exitCode: 1,
      stderr: "Usage: agent-flow validate <workflow>"
    });
    expect(dispatch(["graph"])).toEqual({
      exitCode: 1,
      stderr: "Usage: agent-flow graph <workflow>"
    });
    const missing = dispatch(["lint", path.join(repoRoot, "missing.yml")]);
    expect(missing.exitCode).toBe(1);
    expect(missing.stderr).toContain("Could not read Agent Flow workflow");
  });

  test("unknown commands return not found", async () => {
    let stderr = "";
    const exitCode = await runCli(["missing"], {
      stdout: { write: () => true },
      stderr: {
        write: (chunk: string) => {
          stderr += chunk;
          return true;
        }
      }
    });

    expect(exitCode).toBe(7);
    expect(stderr).toContain("Unknown Agent Flow command");
  });

});

async function captureCli(
  args: string[],
  cwd: string,
  env?: Readonly<Record<string, string | undefined>>
): Promise<{ exitCode: number; stdout: string; stderr: string }> {
  let stdout = "";
  let stderr = "";
  const exitCode = await runCli(args, {
    stdout: { write: (chunk: string) => { stdout += chunk; return true; } },
    stderr: { write: (chunk: string) => { stderr += chunk; return true; } }
  }, { cwd, ...(env === undefined ? {} : { env }) });
  return { exitCode, stdout, stderr };
}

function readStoredZip(archive: Buffer): Map<string, Buffer> {
  const entries = new Map<string, Buffer>();
  let offset = 0;
  while (offset + 4 <= archive.byteLength && archive.readUInt32LE(offset) === 0x04034b50) {
    const method = archive.readUInt16LE(offset + 8);
    if (method !== 0) throw new Error(`Expected a stored ZIP entry, received compression method ${method}.`);
    const size = archive.readUInt32LE(offset + 18);
    const nameLength = archive.readUInt16LE(offset + 26);
    const extraLength = archive.readUInt16LE(offset + 28);
    const nameStart = offset + 30;
    const contentStart = nameStart + nameLength + extraLength;
    const contentEnd = contentStart + size;
    entries.set(archive.subarray(nameStart, nameStart + nameLength).toString("utf8"), archive.subarray(contentStart, contentEnd));
    offset = contentEnd;
  }
  return entries;
}
