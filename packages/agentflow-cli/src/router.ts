import fs from "node:fs";
import {
  AgentflowWorkflowGraphError,
  explainAgentflowWorkflow,
  formatAgentflowWorkflowIssues,
  formatWorkflowParseIssues,
  lintAgentflowWorkflow,
  parseAgentflowWorkflow,
  parseAgentflowSimulationFixture,
  plannedAgentflowRuntimeCommands,
  renderAgentflowSimulationSummary,
  renderAgentflowWorkflowGraph,
  simulateAgentflowWorkflow,
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

    if (topic && !["help", "version", "validate", "lint", "explain", "graph", "simulate"].includes(topic) && !isPlannedRuntimeCommand(topic)) {
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

  if (command === "validate" || command === "lint" || command === "explain" || command === "graph") {
    return checkWorkflow(command, rest);
  }

  if (command === "simulate") {
    return simulateWorkflow(rest);
  }

  if (isPlannedRuntimeCommand(command)) {
    return {
      exitCode: 7,
      stderr: `Agentflow command "${command}" is reserved but not active yet.\nAvailable now: help, version, validate, lint, explain, graph, and simulate.`
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
      ["validate", "lint", "explain", "graph", "simulate"].includes(topic)
        ? topic === "simulate"
          ? "Usage: agentflow simulate <workflow> --fixture <file>"
          : `Usage: agentflow ${topic} <workflow>`
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
    "  agentflow explain <workflow>",
    "  agentflow graph <workflow>",
    "  agentflow simulate <workflow> --fixture <file>",
    "",
    "Available now:",
    "  help       Show this help output.",
    "  version    Print the Agentflow package version.",
    "  validate <workflow>  Validate workflow structure, references, and safety.",
    "  lint <workflow>      Warn about complexity and risky authoring patterns.",
    "  explain <workflow>   Explain steps, artifacts, policies, and warnings.",
    "  graph <workflow>     Print a deterministic workflow graph.",
    "  simulate <workflow> --fixture <file>  Traverse a workflow from fixture data without executing steps.",
    "",
    "Reserved placeholders:",
    `  ${plannedAgentflowRuntimeCommands.filter((command) => !["validate", "lint", "explain", "graph", "simulate"].includes(command)).join(", ")}`,
    "",
    "No workflow execution commands are active yet."
  ].join("\n");
}

function simulateWorkflow(args: string[]): AgentflowCliResult {
  if (args.length !== 3 || args[1] !== "--fixture") {
    return { exitCode: 1, stderr: "Usage: agentflow simulate <workflow> --fixture <file>" };
  }

  const [workflowPath, , fixturePath] = args;
  const workflowResult = readWorkflow(workflowPath, "simulate");
  if ("exitCode" in workflowResult) return workflowResult;

  let fixtureSource: string;
  try {
    fixtureSource = fs.readFileSync(fixturePath, "utf8");
  } catch (error) {
    return {
      exitCode: 1,
      stderr: `Could not read Agentflow simulation fixture ${fixturePath}: ${error instanceof Error ? error.message : String(error)}`
    };
  }

  const fixture = parseAgentflowSimulationFixture(fixtureSource);
  if (!fixture.ok) {
    return {
      exitCode: 2,
      stderr: `Could not parse Agentflow simulation fixture ${fixturePath}: ${fixture.error}`
    };
  }

  const result = simulateAgentflowWorkflow(workflowResult.workflow, fixture.fixture);
  return {
    exitCode: result.status === "unresolved" ? 2 : 0,
    stdout: renderAgentflowSimulationSummary(result)
  };
}

function checkWorkflow(command: "validate" | "lint" | "explain" | "graph", args: string[]): AgentflowCliResult {
  const workflowPath = args[0];

  if (!workflowPath || args.length !== 1) {
    return { exitCode: 1, stderr: `Usage: agentflow ${command} <workflow>` };
  }

  const workflowResult = readWorkflow(workflowPath, command);
  if ("exitCode" in workflowResult) return workflowResult;
  const workflow = workflowResult.workflow;

  if (command === "explain") {
    return { exitCode: 0, stdout: explainAgentflowWorkflow(workflow) };
  }

  if (command === "graph") {
    try {
      return { exitCode: 0, stdout: renderAgentflowWorkflowGraph(workflow) };
    } catch (error) {
      if (error instanceof AgentflowWorkflowGraphError) {
        return {
          exitCode: 2,
          stderr: `Agentflow graph failed: ${workflowPath}\n${error.code}: ${error.message}`
        };
      }
      return {
        exitCode: 2,
        stderr: `Agentflow graph failed: ${workflowPath}\nworkflow.graph.internal: ${error instanceof Error ? error.message : String(error)}`
      };
    }
  }

  if (command === "validate") {
    const warnings = lintAgentflowWorkflow(workflow).warnings;

    return warnings.length === 0
      ? { exitCode: 0, stdout: `Agentflow validation passed: ${workflowPath}` }
      : {
          exitCode: 0,
          stdout: `Agentflow validation passed with ${warnings.length} warning${warnings.length === 1 ? "" : "s"}: ${workflowPath}\n${formatAgentflowWorkflowIssues(warnings)}`
        };
  }

  const lint = lintAgentflowWorkflow(workflow);

  if (lint.warnings.length === 0) {
    return { exitCode: 0, stdout: `Agentflow lint passed with no warnings: ${workflowPath}` };
  }

  return {
    exitCode: 0,
    stdout: `Agentflow lint found ${lint.warnings.length} warning${lint.warnings.length === 1 ? "" : "s"}: ${workflowPath}\n${formatAgentflowWorkflowIssues(lint.warnings)}`
  };
}

function readWorkflow(
  workflowPath: string,
  command: "validate" | "lint" | "explain" | "graph" | "simulate"
): { workflow: import("@jurgen1c/agentflow-core").AgentflowWorkflow } | AgentflowCliResult {
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

  return { workflow: parsed.workflow };
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
