import fs from "node:fs";
import {
  formatAgentflowWorkflowIssues,
  formatWorkflowParseIssues,
  lintAgentflowWorkflow,
  parseAgentflowWorkflow,
  plannedAgentflowRuntimeCommands,
  validateAgentflowWorkflow
} from "@jurgen1c/agentflow-core";

export interface AgentflowCliStreams {
  stdout: Pick<NodeJS.WriteStream, "write">;
  stderr: Pick<NodeJS.WriteStream, "write">;
}

export interface AgentflowCliResult {
  exitCode: number;
  stdout?: string;
  stderr?: string;
}

export async function runCli(args: string[], streams: AgentflowCliStreams = process): Promise<number> {
  const result = dispatch(args);

  if (result.stdout) {
    streams.stdout.write(result.stdout.endsWith("\n") ? result.stdout : `${result.stdout}\n`);
  }

  if (result.stderr) {
    streams.stderr.write(result.stderr.endsWith("\n") ? result.stderr : `${result.stderr}\n`);
  }

  return result.exitCode;
}

export function dispatch(args: string[]): AgentflowCliResult {
  const [command, ...rest] = args;

  if (!command || command === "--help" || command === "-h") {
    return {
      exitCode: 0,
      stdout: renderHelp()
    };
  }

  if (command === "help") {
    const topic = rest[0];

    if (topic && !["help", "version", "validate", "lint"].includes(topic) && !isPlannedRuntimeCommand(topic)) {
      return {
        exitCode: 7,
        stderr: `Unknown Agentflow help topic: ${topic}\nRun \`agentflow help\` to see available commands.`
      };
    }

    return {
      exitCode: 0,
      stdout: renderHelp(topic)
    };
  }

  if (command === "--version" || command === "-v" || command === "version") {
    return {
      exitCode: 0,
      stdout: `agentflow ${readRootPackageVersion()}`
    };
  }

  if (command === "validate" || command === "lint") {
    return checkWorkflow(command, rest);
  }

  if (isPlannedRuntimeCommand(command)) {
    return {
      exitCode: 7,
      stderr: `Agentflow command "${command}" is reserved but not active yet.\nAvailable now: help, version, validate, and lint.`
    };
  }

  return {
    exitCode: 7,
    stderr: `Unknown Agentflow command: ${command}\nRun \`agentflow help\` to see available commands.`
  };
}

function renderHelp(topic?: string): string {
  if (topic && topic !== "help" && topic !== "version") {
    return [
      `agentflow ${topic}`,
      "",
      topic === "validate" || topic === "lint"
        ? `Usage: agentflow ${topic} <workflow>`
        : "This command name is reserved for a future Agentflow runtime surface."
    ].join("\n");
  }

  return [
    "Agentflow",
    "",
    "Usage:",
    "  agentflow help",
    "  agentflow --version",
    "  agentflow validate <workflow>",
    "  agentflow lint <workflow>",
    "",
    "Available now:",
    "  help       Show this help output.",
    "  version    Print the Agentflow package version.",
    "  validate <workflow>  Validate workflow structure, references, and safety.",
    "  lint <workflow>      Warn about complexity and risky authoring patterns.",
    "",
    "Reserved placeholders:",
    `  ${plannedAgentflowRuntimeCommands.filter((command) => command !== "validate" && command !== "lint").join(", ")}`,
    "",
    "No workflow execution commands are active yet."
  ].join("\n");
}

function checkWorkflow(command: "validate" | "lint", args: string[]): AgentflowCliResult {
  const workflowPath = args[0];

  if (!workflowPath || args.length !== 1) {
    return { exitCode: 1, stderr: `Usage: agentflow ${command} <workflow>` };
  }

  let source: string;

  try {
    source = fs.readFileSync(workflowPath, "utf8");
  } catch (error) {
    return {
      exitCode: 1,
      stderr: `Could not read Agentflow workflow ${workflowPath}: ${error instanceof Error ? error.message : String(error)}`
    };
  }

  const parsed = parseAgentflowWorkflow(source);

  if (!parsed.ok) {
    return {
      exitCode: 2,
      stderr: `Agentflow ${command} failed: ${workflowPath}\n${formatWorkflowParseIssues(parsed.errors)}`
    };
  }

  const validation = validateAgentflowWorkflow(parsed.workflow);

  if (!validation.valid) {
    return {
      exitCode: 2,
      stderr: `Agentflow ${command} failed: ${workflowPath}\n${formatAgentflowWorkflowIssues(validation.errors)}`
    };
  }

  if (command === "validate") {
    const warnings = lintAgentflowWorkflow(parsed.workflow).warnings;

    return warnings.length === 0
      ? { exitCode: 0, stdout: `Agentflow validation passed: ${workflowPath}` }
      : {
          exitCode: 0,
          stdout: `Agentflow validation passed with ${warnings.length} warning${warnings.length === 1 ? "" : "s"}: ${workflowPath}\n${formatAgentflowWorkflowIssues(warnings)}`
        };
  }

  const lint = lintAgentflowWorkflow(parsed.workflow);

  if (lint.warnings.length === 0) {
    return { exitCode: 0, stdout: `Agentflow lint passed with no warnings: ${workflowPath}` };
  }

  return {
    exitCode: 0,
    stdout: `Agentflow lint found ${lint.warnings.length} warning${lint.warnings.length === 1 ? "" : "s"}: ${workflowPath}\n${formatAgentflowWorkflowIssues(lint.warnings)}`
  };
}

function isPlannedRuntimeCommand(command: string): boolean {
  return plannedAgentflowRuntimeCommands.includes(command as (typeof plannedAgentflowRuntimeCommands)[number]);
}

function readRootPackageVersion(): string {
  const candidates = [
    new URL("../package.json", import.meta.url),
    new URL("../../../package.json", import.meta.url)
  ];

  for (const packageUrl of candidates) {
    try {
      const packageJson = JSON.parse(fs.readFileSync(packageUrl, "utf8")) as { name?: unknown; version?: unknown };

      const packageNames = ["@jurgen1c/agentflow-cli", "@jurgen1c/agent-memory-cli"];

      if (packageNames.includes(String(packageJson.name)) && typeof packageJson.version === "string" && packageJson.version.length > 0) {
        return packageJson.version;
      }
    } catch {
      // Try the next source/bundled package.json location.
    }
  }

  return "0.0.0";
}
