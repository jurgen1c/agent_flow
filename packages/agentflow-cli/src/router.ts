import fs from "node:fs";
import path from "node:path";
import {
  AgentflowWorkflowGraphError,
  AgentflowRunStateError,
  createAgentflowLifecycleRun,
  createAgentflowFixtureSessionProvider,
  createAgentflowSessionProviderRegistry,
  executeAgentflowCommandPipeline,
  explainAgentflowWorkflow,
  formatAgentflowWorkflowIssues,
  formatWorkflowParseIssues,
  lintAgentflowWorkflow,
  normalizeAgentflowArtifactPath,
  parseAgentflowWorkflow,
  parseAgentflowSimulationFixture,
  openAgentflowRunState,
  plannedAgentflowRuntimeCommands,
  renderAgentflowSimulationSummary,
  renderAgentflowWorkflowGraph,
  resumeAgentflowCommandPipeline,
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
    "  agentflow run <workflow> --id <run-id> --fixture <file>",
    "  agentflow resume <run-id> --outcome <choice> [--fixture <file>]",
    "  agentflow resume <run-id> --answer <value> [--fixture <file>]",
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
    "  run <workflow> --id <run-id> [--fixture <file>]  Execute command, artifact-transform, and fixture-backed session-request steps.",
    "  resume <run-id> (--outcome <choice> | --answer <value>) [--fixture <file>]  Resume a paused interaction.",
    "  status <run-id>       Inspect persistent run state.",
    "  logs <run-id>         List ordered lifecycle events.",
    "  artifacts <run-id>    List registered run artifacts.",
    "  pause <run-id>        Pause an active run.",
    "  cancel <run-id>       Cancel a non-terminal run.",
    "",
    "Reserved placeholders:",
    `  ${plannedAgentflowRuntimeCommands.filter((command) => !["validate", "lint", "explain", "graph", "simulate", ...ACTIVE_LIFECYCLE_COMMANDS].includes(command as ActiveLifecycleCommand)).join(", ")}`,
    "",
    "Command and artifact-transform pipeline execution, including session-request steps, plus persistent lifecycle state are active."
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
      const fixture = args.length === 5 ? readRunFixture(args[4], options.cwd) : null;
      if (fixture !== null && "exitCode" in fixture) return fixture;
      const sessionRequestSteps = collectSessionRequestSteps(workflowResult!.workflow.steps);
      if (sessionRequestSteps.length > 0 && fixture === null) {
        return {
          exitCode: 1,
          stderr: "Session-request workflows require --fixture <file> until a non-fixture provider adapter is configured."
        };
      }
      const unsupportedProviders = sessionRequestSteps
        .filter((step) => typeof step.session === "string")
        .map((step) => workflowResult!.workflow.sessions?.[String(step.session).trim()])
        .flatMap((session) => session !== null && typeof session === "object" && !Array.isArray(session)
          ? [String((session as Record<string, unknown>).provider ?? "").trim()]
          : [])
        .filter((provider) => provider !== "fixture");
      if (unsupportedProviders.length > 0) {
        return {
          exitCode: 1,
          stderr: `CLI fixture mode supports only provider "fixture"; unsupported providers: ${[...new Set(unsupportedProviders)].sort().join(", ")}.`
        };
      }
      if (fixture !== null) {
        const unsupportedOutputStep = sessionRequestSteps.find((step) =>
          fixture.arrayOutputSteps.has(String(step.id ?? "").trim())
        );
        if (unsupportedOutputStep !== undefined) {
          return {
            exitCode: 2,
            stderr: `Run fixture step ${String(unsupportedOutputStep.id).trim()}.outputs must be an object with materializable output values; array-form outputs are simulation-only.`
          };
        }
      }
      const result = createAgentflowLifecycleRun(store, {
        id: args[2],
        workflow: workflowResult!.workflow,
        ...(fixture === null ? {} : { inputs: fixture.inputs })
      });
      if (fixture !== null) {
        store.updateRun(result.run.id, {
          context: {
            ...result.run.context,
            cliFixturePath: path.resolve(options.cwd ?? process.cwd(), args[4])
          }
        });
      }
      if (fixture !== null) {
        for (const [index, [artifactPath, value]] of Object.entries(fixture.artifacts)
          .sort(([left], [right]) => left.localeCompare(right)).entries()) {
          store.writeArtifact({
            id: `fixture:${index + 1}`,
            runId: result.run.id,
            stepId: "fixture",
            path: artifactPath,
            kind: "fixture",
            contentType: artifactPath.endsWith(".json") ? "application/json; charset=utf-8" : "text/plain; charset=utf-8",
            content: typeof value === "string" ? value : `${JSON.stringify(value)}\n`
          });
        }
      }
      const providers = createAgentflowSessionProviderRegistry();
      if (fixture !== null) providers.register("fixture", createAgentflowFixtureSessionProvider(fixture.responses, fixture.outcomes));
      const execution = await executeAgentflowCommandPipeline(
        store,
        result.run.id,
        workflowResult!.workflow,
        undefined,
        providers
      );
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

    if (command === "resume") {
      const run = requireRun(store, runId);
      const workflow = run.context.workflow;
      if (workflow === null || typeof workflow !== "object" || Array.isArray(workflow)) {
        throw new AgentflowRunStateError(
          `Agentflow run ${runId} does not contain its persisted workflow definition.`,
          "AGENTFLOW_RESUME_STATE"
        );
      }
      const response = args[1] === "--outcome"
        ? { outcome: args[2] }
        : { answer: parseCliAnswer(args[2]) };
      const persistedFixturePath = typeof run.context.cliFixturePath === "string"
        ? run.context.cliFixturePath
        : undefined;
      const fixturePath = args.length === 5 ? args[4] : persistedFixturePath;
      const fixture = fixturePath === undefined ? null : readRunFixture(fixturePath, options.cwd);
      if (fixture !== null && "exitCode" in fixture) return fixture;
      if (fixture !== null) {
        const unsupportedOutputStep = collectSessionRequestSteps(
          (workflow as unknown as import("@jurgen1c/agentflow-core").AgentflowWorkflow).steps
        ).find((step) => fixture.arrayOutputSteps.has(String(step.id ?? "").trim()));
        if (unsupportedOutputStep !== undefined) {
          return {
            exitCode: 2,
            stderr: `Run fixture step ${String(unsupportedOutputStep.id).trim()}.outputs must be an object with materializable output values; array-form outputs are simulation-only.`
          };
        }
      }
      const providers = createAgentflowSessionProviderRegistry();
      if (fixture !== null) {
        providers.register("fixture", createAgentflowFixtureSessionProvider(fixture.responses, fixture.outcomes));
      }
      const execution = await resumeAgentflowCommandPipeline(
        store,
        runId,
        workflow as unknown as import("@jurgen1c/agentflow-core").AgentflowWorkflow,
        response,
        undefined,
        providers
      );
      if (args.length === 5 && execution.status === "paused") {
        const resumedRun = requireRun(store, runId);
        store.updateRun(runId, {
          context: {
            ...resumedRun.context,
            cliFixturePath: path.resolve(options.cwd ?? process.cwd(), args[4])
          }
        });
      }
      const lines = [
        `Resumed Agentflow run ${runId}.`,
        `Status: ${execution.status}`,
        `Completed steps: ${execution.completedSteps.length === 0 ? "none" : execution.completedSteps.join(", ")}`
      ];
      return {
        exitCode: execution.status === "completed" ? 0 : execution.status === "paused" ? 3 : 1,
        stdout: lines.join("\n"),
        stderr: execution.message
      };
    }

    const result = transitionAgentflowLifecycleRun(store, runId, command);
    const verb = command === "pause" ? "Paused" : "Cancelled";
    const lines = [
      `${result.changed ? verb : "No change for"} Agentflow run ${runId}.`,
      `Status: ${result.run.status}`
    ];
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
  if (command === "run") {
    return (args.length === 3 || (args.length === 5 && args[3] === "--fixture" && args[4].length > 0))
      && args[1] === "--id" && args[0].length > 0 && args[2].length > 0
  }
  if (command === "resume") {
    return (args.length === 3 || (args.length === 5 && args[3] === "--fixture" && args[4].length > 0))
      && args[0].length > 0
      && ["--outcome", "--answer"].includes(args[1])
      && (args[1] === "--answer" || args[2].length > 0);
  }
  return args.length === 1 && args[0].length > 0;
}

function lifecycleUsage(topic: string): string | null {
  if (topic === "run") return "Usage: agentflow run <workflow> --id <run-id> [--fixture <file>]";
  if (topic === "resume") return "Usage: agentflow resume <run-id> (--outcome <choice> | --answer <value>) [--fixture <file>]";
  if (isActiveLifecycleCommand(topic)) return `Usage: agentflow ${topic} <run-id>`;
  return null;
}

function parseCliAnswer(value: string): import("@jurgen1c/agentflow-core").AgentflowRunStateValue {
  try {
    const parsed = JSON.parse(value) as unknown;
    if (isRunStateValue(parsed)) return parsed;
  } catch {
    // Plain text answers are valid input-request values.
  }
  return value;
}

function isRunStateValue(value: unknown): value is import("@jurgen1c/agentflow-core").AgentflowRunStateValue {
  if (value === null || typeof value === "boolean" || typeof value === "string") return true;
  if (typeof value === "number") return Number.isFinite(value);
  if (Array.isArray(value)) return value.every(isRunStateValue);
  return typeof value === "object"
    && Object.values(value as Record<string, unknown>).every(isRunStateValue);
}

function readRunFixture(
  fixturePath: string,
  cwd?: string
): {
  inputs: Record<string, import("@jurgen1c/agentflow-core").AgentflowRunStateValue>;
  artifacts: Record<string, import("@jurgen1c/agentflow-core").AgentflowRunStateValue>;
  responses: Record<string, import("@jurgen1c/agentflow-core").AgentflowSessionProviderResponse>;
  outcomes: Record<string, "succeeded" | "failed" | Array<"succeeded" | "failed">>;
  arrayOutputSteps: Set<string>;
} | AgentflowCliResult {
  const resolvedPath = cwd === undefined ? fixturePath : path.resolve(cwd, fixturePath);
  let source: string;
  try {
    source = fs.readFileSync(resolvedPath, "utf8");
  } catch (error) {
    return { exitCode: 1, stderr: `Could not read Agentflow run fixture ${fixturePath}: ${error instanceof Error ? error.message : String(error)}` };
  }
  const parsed = parseAgentflowSimulationFixture(source);
  if (!parsed.ok) return { exitCode: 2, stderr: `Could not parse Agentflow run fixture ${fixturePath}: ${parsed.error}` };
  const responses: Record<string, import("@jurgen1c/agentflow-core").AgentflowSessionProviderResponse> = {};
  const outcomes: Record<string, "succeeded" | "failed" | Array<"succeeded" | "failed">> = {};
  const arrayOutputSteps = new Set<string>();
  for (const [stepId, fixture] of Object.entries(parsed.fixture.steps ?? {})) {
    if (fixture.outcome !== undefined) outcomes[stepId] = fixture.outcome;
    if (Array.isArray(fixture.outputs)) {
      arrayOutputSteps.add(stepId);
      continue;
    }
    if (fixture.outputs === undefined) continue;
    const outputs: Record<string, string> = {};
    for (const [declaredPath, value] of Object.entries(fixture.outputs)) {
      let canonicalPath: string;
      try {
        canonicalPath = normalizeAgentflowArtifactPath(declaredPath);
      } catch (error) {
        return { exitCode: 2, stderr: `Run fixture step ${stepId} output ${JSON.stringify(declaredPath)} is invalid: ${error instanceof Error ? error.message : String(error)}` };
      }
      if (Object.hasOwn(outputs, canonicalPath)) {
        return { exitCode: 2, stderr: `Run fixture step ${stepId} output keys collide at canonical path ${canonicalPath}.` };
      }
      outputs[canonicalPath] = typeof value === "string" ? value : `${JSON.stringify(value)}\n`;
    }
    responses[stepId] = {
      outputs
    };
  }
  const artifacts: Record<string, import("@jurgen1c/agentflow-core").AgentflowRunStateValue> = {};
  for (const [declaredPath, value] of Object.entries(parsed.fixture.artifacts ?? {})) {
    let canonicalPath: string;
    try {
      canonicalPath = normalizeAgentflowArtifactPath(declaredPath);
    } catch (error) {
      return { exitCode: 2, stderr: `Run fixture artifact ${JSON.stringify(declaredPath)} is invalid: ${error instanceof Error ? error.message : String(error)}` };
    }
    if (Object.hasOwn(artifacts, canonicalPath)) {
      return { exitCode: 2, stderr: `Run fixture artifact keys collide at canonical path ${canonicalPath}.` };
    }
    artifacts[canonicalPath] = value as import("@jurgen1c/agentflow-core").AgentflowRunStateValue;
  }
  return {
    inputs: (parsed.fixture.inputs ?? {}) as unknown as Record<string, import("@jurgen1c/agentflow-core").AgentflowRunStateValue>,
    artifacts,
    responses,
    outcomes,
    arrayOutputSteps
  };
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
  const lines = [
    `Run: ${run.id}`,
    `Workflow: ${run.workflowName} (version ${run.workflowVersion})`,
    `Status: ${run.status}`,
    `Current step: ${run.currentStepId ?? "none"}`
  ];
  const waiting = run.context.waiting;
  if (waiting !== null && typeof waiting === "object" && !Array.isArray(waiting)) {
    const reason = waiting.reason;
    const prompt = waiting.prompt;
    if (typeof reason === "string") lines.push(`Waiting reason: ${reason}`);
    if (typeof prompt === "string") lines.push(`Prompt: ${prompt}`);
    if (waiting.kind === "manual_gate" && Array.isArray(waiting.validOutcomes)) {
      lines.push(`Valid outcomes: ${waiting.validOutcomes.join(", ") || "none"}`);
    }
    if (waiting.kind === "input_request" && typeof waiting.saveAs === "string") {
      lines.push(`Answer artifact: ${waiting.saveAs}`);
    }
  }
  lines.push(
    `Created: ${run.createdAt}`,
    `Updated: ${run.updatedAt}`
  );
  return lines.join("\n");
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

function collectSessionRequestSteps(
  steps: import("@jurgen1c/agentflow-core").AgentflowWorkflowStep[]
): import("@jurgen1c/agentflow-core").AgentflowWorkflowStep[] {
  const requests: import("@jurgen1c/agentflow-core").AgentflowWorkflowStep[] = [];
  const visit = (step: import("@jurgen1c/agentflow-core").AgentflowWorkflowStep): void => {
    if (typeof step.type === "string" && step.type.trim() === "session_request") requests.push(step);
    for (const field of ["body", "steps", "branches"] as const) {
      const nested = step[field];
      if (!Array.isArray(nested)) continue;
      for (const entry of nested) {
        if (entry !== null && typeof entry === "object" && !Array.isArray(entry)) {
          visit(entry as import("@jurgen1c/agentflow-core").AgentflowWorkflowStep);
        }
      }
    }
  };
  steps.forEach(visit);
  return requests;
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
