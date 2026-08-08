import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const repositoryRoot = path.resolve(".");
const builtCli = path.join(repositoryRoot, "dist", "agent-flow.js");
const nodeExecutable = process.env.AGENT_TEST_NODE ?? resolveNodeExecutable();

describe("built Node CLI", () => {
  test("runs persistent workflows without exposing Node SQLite warnings", () => {
    const build = run(["bun", "run", "build"], repositoryRoot);
    expect(build.exitCode).toBe(0);
    expect(fs.readFileSync(builtCli, "utf8").split(/\r?\n/, 1)[0]).toBe(
      "#!/usr/bin/env node"
    );

    const workingDirectory = fs.mkdtempSync(
      path.join(os.tmpdir(), "agent-flow-node-runtime-")
    );
    fs.mkdirSync(path.join(workingDirectory, ".git"));
    fs.writeFileSync(
      path.join(workingDirectory, "workflow.yml"),
      [
        "name: node-runtime",
        "version: 1",
        "style: pipeline",
        "maturity: stable",
        "steps:",
        "  - id: node-check",
        "    type: command",
        "    command: printf 'node runtime passed\\n'",
        ""
      ].join("\n")
    );

    const execution = run(
      [nodeExecutable, builtCli, "run", "workflow.yml", "--id", "node-runtime"],
      workingDirectory
    );

    expect(execution.exitCode).toBe(0);
    expect(execution.stdout).toContain("Status: completed");
    expect(execution.stderr).not.toContain("ExperimentalWarning");

    const status = run(
      [nodeExecutable, builtCli, "status", "node-runtime"],
      workingDirectory
    );
    expect(status.exitCode).toBe(0);
    expect(status.stdout).toContain("Status: completed");
    expect(status.stderr).not.toContain("ExperimentalWarning");
  }, 120_000);
});

function run(
  command: string[],
  cwd: string,
  env: Bun.Env = process.env
): { exitCode: number; stdout: string; stderr: string } {
  const result = Bun.spawnSync(command, {
    cwd,
    env
  });

  return {
    exitCode: result.exitCode,
    stdout: new TextDecoder().decode(result.stdout),
    stderr: new TextDecoder().decode(result.stderr)
  };
}

function resolveNodeExecutable(): string {
  const result = Bun.spawnSync(["node", "-p", "process.execPath"], {
    cwd: repositoryRoot,
    env: process.env
  });
  if (result.exitCode !== 0) return "node";
  const executable = new TextDecoder().decode(result.stdout).trim();
  return executable.length > 0 ? executable : "node";
}
