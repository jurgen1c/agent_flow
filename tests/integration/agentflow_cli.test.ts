import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import path from "node:path";
import { agentflowAgentMemoryAdapterPackageBoundary } from "../../packages/agentflow-agent-memory-adapter/src";
import { dispatch, runCli } from "../../packages/agentflow-cli/src/router";
import { agentflowCorePackageBoundary, plannedAgentflowRuntimeCommands } from "../../packages/agentflow-core/src";
import { agentflowSchemaPackageBoundary } from "../../packages/agentflow-schemas/src";

const repoRoot = path.resolve(".");
const rootPackage = JSON.parse(fs.readFileSync(path.join(repoRoot, "package.json"), "utf8")) as { version: string };

describe("Agentflow CLI skeleton", () => {
  test("renders help with only help and version active", () => {
    const result = dispatch(["help"]);

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain("Agentflow");
    expect(result.stdout).toContain("Available now");
    expect(result.stdout).toContain("No workflow execution commands are active yet.");
  });

  test("renders version from root package metadata", () => {
    const result = dispatch(["--version"]);

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toBe(`agentflow ${rootPackage.version}`);
  });

  test("keeps planned runtime commands reserved but inactive", () => {
    for (const command of plannedAgentflowRuntimeCommands) {
      const result = dispatch([command]);

      expect(result.exitCode).toBe(7);
      expect(result.stderr).toContain("reserved but not active yet");
    }
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
