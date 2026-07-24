import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import path from "node:path";
import { agentflowAgentMemoryAdapterPackageBoundary } from "../../packages/agentflow-agent-memory-adapter/src";
import { dispatch, runCli } from "../../packages/agentflow-cli/src/router";
import { agentflowCorePackageBoundary, plannedAgentflowRuntimeCommands } from "../../packages/agentflow-core/src";
import { agentflowSchemaPackageBoundary } from "../../packages/agentflow-schemas/src";

const repoRoot = path.resolve(".");
const rootPackage = JSON.parse(fs.readFileSync(path.join(repoRoot, "package.json"), "utf8")) as { version: string };

describe("Agentflow CLI", () => {
  test("renders help with validation authoring commands active", () => {
    const result = dispatch(["help"]);

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain("Agentflow");
    expect(result.stdout).toContain("Available now");
    expect(result.stdout).toContain("validate <workflow>");
    expect(result.stdout).toContain("lint <workflow>");
    expect(result.stdout).toContain("explain <workflow>");
    expect(result.stdout).toContain("graph <workflow>");
    expect(result.stdout).toContain("simulate <workflow> --fixture <file>");
    expect(result.stdout).toContain("run <workflow> --id <run-id>");
    expect(result.stdout).toContain("pause <run-id>");
    expect(result.stdout).toContain("Command and artifact-transform pipeline execution");
  });

  test("renders version from root package metadata", () => {
    const result = dispatch(["--version"]);

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toBe(`agentflow ${rootPackage.version}`);
  });

  test("keeps remaining execution commands reserved but inactive", () => {
    for (const command of plannedAgentflowRuntimeCommands.filter((candidate) => !["validate", "lint", "explain", "graph", "simulate", "run", "resume", "status", "logs", "artifacts", "pause", "cancel"].includes(candidate))) {
      const result = dispatch([command]);

      expect(result.exitCode).toBe(7);
      expect(result.stderr).toContain("reserved but not active yet");
      expect(result.stderr).toContain("run, resume, status, logs, artifacts, pause, and cancel");
    }
  });

  test("distinguishes active, reserved, and unknown help topics", () => {
    expect(dispatch(["help", "validate"])).toEqual({
      exitCode: 0,
      stdout: "agentflow validate\n\nUsage: agentflow validate <workflow>"
    });
    expect(dispatch(["help", "explain"])).toEqual({
      exitCode: 0,
      stdout: "agentflow explain\n\nUsage: agentflow explain <workflow>"
    });
    expect(dispatch(["help", "simulate"])).toEqual({
      exitCode: 0,
      stdout: "agentflow simulate\n\nUsage: agentflow simulate <workflow> --fixture <file>"
    });
    expect(dispatch(["help", "run"])).toEqual({
      exitCode: 0,
      stdout: "agentflow run\n\nUsage: agentflow run <workflow> --id <run-id> [--fixture <file>]"
    });
    expect(dispatch(["help", "missing"])).toEqual({
      exitCode: 7,
      stderr: "Unknown Agentflow help topic: missing\nRun `agentflow help` to see available commands."
    });
  });

  test("validates workflows from the CLI", () => {
    const validPath = path.join(repoRoot, "tests/fixtures/agentflow/workflows/simple-ci.yml");
    const invalidPath = path.join(repoRoot, "tests/fixtures/agentflow/invalid/unsafe-workflow.yml");

    expect(dispatch(["validate", validPath])).toMatchObject({
      exitCode: 0,
      stdout: `Agentflow validation passed: ${validPath}`
    });
    const invalid = dispatch(["validate", invalidPath]);
    expect(invalid.exitCode).toBe(2);
    expect(invalid.stderr).toContain("workflow.command.unsafe");
    expect(invalid.stderr).toContain("workflow.loop.unbounded");
  });

  test("lints workflows without rewriting them", () => {
    const fixturePath = path.join(repoRoot, "tests/fixtures/agentflow/workflows/content-review-collab.yml");
    const before = fs.readFileSync(fixturePath, "utf8");
    const result = dispatch(["lint", fixturePath]);

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain("workflow.lint.artifact.read_before_write");
    expect(fs.readFileSync(fixturePath, "utf8")).toBe(before);
  });

  test("explains and graphs workflows without executing or rewriting them", () => {
    const fixturePath = path.join(repoRoot, "tests/fixtures/agentflow/workflows/pr-feedback-loop.yml");
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
    const workflowPath = path.join(repoRoot, "agentflow-examples/agentflow-examples/workflows/simple-ci.yml");
    const fixturePath = path.join(repoRoot, "tests/fixtures/agentflow/simulation/simple-ci.json");
    const workflowBefore = fs.readFileSync(workflowPath, "utf8");
    const fixtureBefore = fs.readFileSync(fixturePath, "utf8");
    const result = dispatch(["simulate", workflowPath, "--fixture", fixturePath]);

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain("Agentflow simulation: simple-ci (version 1)");
    expect(result.stdout).toContain("Status: completed");
    expect(result.stdout).toContain("install [command]: succeeded");
    expect(fs.readFileSync(workflowPath, "utf8")).toBe(workflowBefore);
    expect(fs.readFileSync(fixturePath, "utf8")).toBe(fixtureBefore);
  });

  test("reports invalid simulation fixtures and usage", () => {
    const workflowPath = path.join(repoRoot, "agentflow-examples/agentflow-examples/workflows/simple-ci.yml");
    const invalidFixture = path.join(repoRoot, "tests/fixtures/agentflow/workflows/simple-ci.yml");

    expect(dispatch(["simulate", workflowPath])).toEqual({
      exitCode: 1,
      stderr: "Usage: agentflow simulate <workflow> --fixture <file>"
    });
    const invalid = dispatch(["simulate", workflowPath, "--fixture", invalidFixture]);
    expect(invalid.exitCode).toBe(2);
    expect(invalid.stderr).toContain("Could not parse Agentflow simulation fixture");
  });

  test("reports generated graph node collisions without crashing", () => {
    const fixturePath = path.join(repoRoot, "tests/fixtures/agentflow/workflows/graph-node-collision.yml");
    const result = dispatch(["graph", fixturePath]);

    expect(result.exitCode).toBe(2);
    expect(result.stderr).toContain("workflow.graph.node_id_collision");
    expect(result.stderr).toContain('Graph node id "terminal:pause" collides');
  });

  test("manages persistent lifecycle state through the asynchronous CLI runner", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agentflow-cli-lifecycle-"));
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
    expect(run.stdout).toContain("Created Agentflow run run-cli");
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

  test("resumes manual gates and input requests with explicit CLI values", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agentflow-cli-interaction-"));
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

  test("restores the CLI fixture provider after a manual gate", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agentflow-cli-fixture-resume-"));
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
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agentflow-cli-session-"));
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

  test("surfaces validation warnings while preserving a successful exit", () => {
    const fixturePath = path.join(repoRoot, "tests/fixtures/agentflow/invalid/missing-artifact.yml");
    const result = dispatch(["validate", fixturePath]);

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain("passed with 1 warning");
    expect(result.stdout).toContain("workflow.lint.artifact.read_before_write");
  });

  test("reports missing workflow paths and parse failures", () => {
    expect(dispatch(["validate"])).toEqual({
      exitCode: 1,
      stderr: "Usage: agentflow validate <workflow>"
    });
    expect(dispatch(["graph"])).toEqual({
      exitCode: 1,
      stderr: "Usage: agentflow graph <workflow>"
    });
    const missing = dispatch(["lint", path.join(repoRoot, "missing.yml")]);
    expect(missing.exitCode).toBe(1);
    expect(missing.stderr).toContain("Could not read Agentflow workflow");
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
    expect(stderr).toContain("Unknown Agentflow command");
  });

  test("exports typed package boundary metadata without enabling runtime behavior", () => {
    expect(agentflowCorePackageBoundary).toEqual({
      packageName: "@jurgen1c/agentflow-core",
      role: "workflow-core",
      status: "skeleton",
      sharedToolsPackage: "@jurgen1c/agent-tools",
      schemasPackage: "@jurgen1c/agentflow-schemas"
    });
    expect(agentflowSchemaPackageBoundary.exportedSchemas).toEqual(["config", "workflow"]);
    expect(agentflowAgentMemoryAdapterPackageBoundary.status).toBe("active");
    expect(agentflowAgentMemoryAdapterPackageBoundary.agentMemoryPackage).toBe("@jurgen1c/agent-memory-core");
  });
});

async function captureCli(args: string[], cwd: string): Promise<{ exitCode: number; stdout: string; stderr: string }> {
  let stdout = "";
  let stderr = "";
  const exitCode = await runCli(args, {
    stdout: { write: (chunk: string) => { stdout += chunk; return true; } },
    stderr: { write: (chunk: string) => { stderr += chunk; return true; } }
  }, { cwd });
  return { exitCode, stdout, stderr };
}
