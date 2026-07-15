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
    expect(result.stdout).toContain("No workflow execution commands are active yet.");
  });

  test("renders version from root package metadata", () => {
    const result = dispatch(["--version"]);

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toBe(`agentflow ${rootPackage.version}`);
  });

  test("keeps execution commands reserved but inactive", () => {
    for (const command of plannedAgentflowRuntimeCommands.filter((candidate) => !["validate", "lint"].includes(candidate))) {
      const result = dispatch([command]);

      expect(result.exitCode).toBe(7);
      expect(result.stderr).toContain("reserved but not active yet");
      expect(result.stderr).toContain("Available now: help, version, validate, and lint.");
    }
  });

  test("distinguishes active, reserved, and unknown help topics", () => {
    expect(dispatch(["help", "validate"])).toEqual({
      exitCode: 0,
      stdout: "agentflow validate\n\nUsage: agentflow validate <workflow>"
    });
    expect(dispatch(["help", "run"])).toEqual({
      exitCode: 0,
      stdout: "agentflow run\n\nThis command name is reserved for a future Agentflow runtime surface."
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
