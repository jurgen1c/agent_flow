import fs from "node:fs";
import path from "node:path";
import {
  AgentflowWorkflowGraphError,
  AgentflowRunStateError,
  createAgentflowLifecycleRun,
  executeAgentflowCommandPipeline,
  explainAgentflowWorkflow,
  formatAgentflowWorkflowIssues,
  formatWorkflowParseIssues,
  lintAgentflowWorkflow,
  parseAgentflowWorkflow,
  parseAgentflowSimulationFixture,
  openAgentflowRunState,
  plannedAgentflowRuntimeCommands,
  renderAgentflowSimulationSummary,
  renderAgentflowWorkflowGraph,
  simulateAgentflowWorkflow,
  transitionAgentflowLifecycleRun,
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

export interface AgentflowCliOptions {
  cwd?: string;
}

const ACTIVE_LIFECYCLE_COMMANDS = ["run", "resume", "status", "logs", "artifacts", "pause", "cancel"] as const;
type ActiveLifecycleCommand = (typeof ACTIVE_LIFECYCLE_COMMANDS)[number];

export async function runCli(
  args: string[],
  streams: AgentflowCliStreams = process,
  options: AgentflowCliOptions = {}
): Promise<number> {
  const result = isActiveLifecycleCommand(args[0])
    ? await runLifecycleCommand(args[0], args.slice(1), options)
    : dispatch(args);

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

  if (isActiveLifecycleCommand(command)) {
    return {
      exitCode: 1,
      stderr: `Agentflow ${command} uses persistent run state and must be invoked through the CLI runner.`
    };
  }

  if (isPlannedRuntimeCommand(command)) {
    return {
      exitCode: 7,
      stderr: `Agentflow command "${command}" is reserved but not active yet.\nAvailable now: help, version, validate, lint, explain, graph, simulate, run, resume, status, logs, artifacts, pause, and cancel.`
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
      ["validate", "lint", "explain", "graph", "simulate", ...ACTIVE_LIFECYCLE_COMMANDS].includes(topic as ActiveLifecycleCommand)
        ? lifecycleUsage(topic) ?? (topic === "simulate"
          ? "Usage: agentflow simulate <workflow> --fixture <file>"
          : `Usage: agentflow ${topic} <workflow>`)
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
    "  agentflow run <workflow> --id <run-id>",
    "  agentflow resume <run-id>",
    "  agentflow status <run-id>",
    "  agentflow logs <run-id>",
    "  agentflow artifacts <run-id>",
    "  agentflow pause <run-id>",
    "  agentflow cancel <run-id>",
    "",
    "Available now:",
    "  help       Show this help output.",
    "  version    Print the Agentflow package version.",
    "  validate <workflow>  Validate workflow structure, references, and safety.",
    "  lint <workflow>      Warn about complexity and risky authoring patterns.",
    "  explain <workflow>   Explain steps, artifacts, policies, and warnings.",
    "  graph <workflow>     Print a deterministic workflow graph.",
    "  simulate <workflow> --fixture <file>  Traverse a workflow from fixture data without executing steps.",
    "  run <workflow> --id <run-id>  Execute a command-only pipeline and persist its run state.",
    "  resume <run-id>       Resume a paused, waiting, or pending run.",
    "  status <run-id>       Inspect persistent run state.",
    "  logs <run-id>         List ordered lifecycle events.",
    "  artifacts <run-id>    List registered run artifacts.",
    "  pause <run-id>        Pause an active run.",
    "  cancel <run-id>       Cancel a non-terminal run.",
    "",
    "Reserved placeholders:",
    `  ${plannedAgentflowRuntimeCommands.filter((command) => !["validate", "lint", "explain", "graph", "simulate", ...ACTIVE_LIFECYCLE_COMMANDS].includes(command as ActiveLifecycleCommand)).join(", ")}`,
    "",
    "Command-only pipeline execution and persistent lifecycle state are active."
  ].join("\n");
}

async function runLifecycleCommand(
  command: ActiveLifecycleCommand,
  args: string[],
  options: AgentflowCliOptions
): Promise<AgentflowCliResult> {
  const usage = lifecycleUsage(command);
  if (!validLifecycleArgs(command, args)) return { exitCode: 1, stderr: usage! };

  const workflowPath = command === "run" && options.cwd ? path.resolve(options.cwd, args[0]) : args[0];
  const workflowResult = command === "run" ? readWorkflow(workflowPath, "run") : null;
  if (workflowResult && "exitCode" in workflowResult) return workflowResult;

  let store: Awaited<ReturnType<typeof openAgentflowRunState>> | undefined;
  try {
    store = await openAgentflowRunState({ cwd: options.cwd });

    if (command === "run") {
      const result = createAgentflowLifecycleRun(store, { id: args[2], workflow: workflowResult!.workflow });
      const execution = await executeAgentflowCommandPipeline(store, result.run.id, workflowResult!.workflow);
      const lines = [
        `${result.changed ? "Created" : "Reused"} Agentflow run ${result.run.id} for ${result.run.workflowName} (version ${result.run.workflowVersion}).`,
        `Status: ${execution.status}`,
        `Completed steps: ${execution.completedSteps.length === 0 ? "none" : execution.completedSteps.join(", ")}`
      ];
      if (execution.failedStep !== undefined) lines.push(`Failed step: ${execution.failedStep}`);
      return {
        exitCode: execution.status === "completed" ? 0 : execution.status === "paused" ? 3 : 1,
        stdout: lines.join("\n"),
        stderr: execution.message
      };
    }

    const runId = args[0];
    if (command === "status") {
      const run = requireRun(store, runId);
      return { exitCode: 0, stdout: renderRunStatus(run) };
    }
    if (command === "logs") {
      requireRun(store, runId);
      const events = store.listEvents(runId);
      return {
        exitCode: 0,
        stdout: events.length === 0
          ? `No events recorded for Agentflow run ${runId}.`
          : events.map((event) => `${event.sequence}\t${event.createdAt}\t${event.type}\t${JSON.stringify(event.payload)}`).join("\n")
      };
    }
    if (command === "artifacts") {
      requireRun(store, runId);
      const artifacts = store.listArtifacts(runId);
      return {
        exitCode: 0,
        stdout: artifacts.length === 0
          ? `No artifacts registered for Agentflow run ${runId}.`
          : artifacts.map((artifact) => `${artifact.declaredPath}\t${artifact.status}\t${artifact.kind}\t${artifact.contentType}`).join("\n")
      };
    }

    const result = transitionAgentflowLifecycleRun(store, runId, command);
    const verb = command === "pause" ? "Paused" : command === "resume" ? "Resumed" : "Cancelled";
    const lines = [
      `${result.changed ? verb : "No change for"} Agentflow run ${runId}.`,
      `Status: ${result.run.status}`
    ];
    if (command === "resume") {
      return {
        exitCode: 7,
        stdout: lines.join("\n"),
        stderr: "Resuming command execution is not available yet; no additional workflow steps were executed. Pause or cancel the run explicitly."
      };
    }
    return { exitCode: 0, stdout: lines.join("\n") };
  } catch (error) {
    if (error instanceof AgentflowRunStateError) {
      return { exitCode: error.code === "AGENTFLOW_RUN_NOT_FOUND" ? 4 : 2, stderr: error.message };
    }
    return { exitCode: 1, stderr: error instanceof Error ? error.message : String(error) };
  } finally {
    store?.close();
  }
}

function validLifecycleArgs(command: ActiveLifecycleCommand, args: string[]): boolean {
  return command === "run"
    ? args.length === 3 && args[1] === "--id" && args[0].length > 0 && args[2].length > 0
    : args.length === 1 && args[0].length > 0;
}

function lifecycleUsage(topic: string): string | null {
  if (topic === "run") return "Usage: agentflow run <workflow> --id <run-id>";
  if (isActiveLifecycleCommand(topic)) return `Usage: agentflow ${topic} <run-id>`;
  return null;
}

function requireRun(
  store: Awaited<ReturnType<typeof openAgentflowRunState>>,
  runId: string
): NonNullable<ReturnType<typeof store.getRun>> {
  const run = store.getRun(runId);
  if (run === null) throw new AgentflowRunStateError(`Agentflow run ${runId} was not found.`, "AGENTFLOW_RUN_NOT_FOUND");
  return run;
}

function renderRunStatus(run: NonNullable<ReturnType<Awaited<ReturnType<typeof openAgentflowRunState>>["getRun"]>>): string {
  return [
    `Run: ${run.id}`,
    `Workflow: ${run.workflowName} (version ${run.workflowVersion})`,
    `Status: ${run.status}`,
    `Current step: ${run.currentStepId ?? "none"}`,
    `Created: ${run.createdAt}`,
    `Updated: ${run.updatedAt}`
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
  command: "validate" | "lint" | "explain" | "graph" | "simulate" | "run"
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

function isActiveLifecycleCommand(command: string | undefined): command is ActiveLifecycleCommand {
  return command !== undefined && ACTIVE_LIFECYCLE_COMMANDS.includes(command as ActiveLifecycleCommand);
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
