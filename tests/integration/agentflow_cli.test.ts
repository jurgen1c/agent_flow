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
    expect(result.stdout).toContain("Lifecycle state management is active");
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
      stdout: "agentflow run\n\nUsage: agentflow run <workflow> --id <run-id>"
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
    expect(result.stdout).toContain("workflow.lint.frontier.unbounded");
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
    fs.copyFileSync(path.join(repoRoot, "tests/fixtures/agentflow/workflows/simple-ci.yml"), workflowPath);

    const run = await captureCli(["run", path.basename(workflowPath), "--id", "run-cli"], repo);
    expect(run).toMatchObject({ exitCode: 7 });
    expect(run.stdout).toContain("Created Agentflow run run-cli");
    expect(run.stderr).toContain("Workflow step execution is not available yet");
    expect(await captureCli(["pause", "run-cli"], repo)).toMatchObject({ exitCode: 0 });
    const resumed = await captureCli(["resume", "run-cli"], repo);
    expect(resumed.exitCode).toBe(7);
    expect(resumed.stdout).toContain("Status: running");
    expect(resumed.stderr).toContain("no workflow steps were executed");
    const status = await captureCli(["status", "run-cli"], repo);
    expect(status.stdout).toContain("Workflow: simple-ci (version 1)");
    expect(status.stdout).toContain("Status: running");
    const logs = await captureCli(["logs", "run-cli"], repo);
    expect(logs.stdout).toContain("run.created");
    expect(logs.stdout).toContain("run.resume");
    expect(await captureCli(["artifacts", "run-cli"], repo)).toMatchObject({
      exitCode: 0,
      stdout: "No artifacts registered for Agentflow run run-cli.\n"
    });
    expect((await captureCli(["cancel", "run-cli"], repo)).stdout).toContain("Status: cancelled");

    const restartedStatus = await captureCli(["status", "run-cli"], repo);
    expect(restartedStatus.stdout).toContain("Status: cancelled");
    expect(await captureCli(["pause", "missing"], repo)).toMatchObject({ exitCode: 4 });
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
