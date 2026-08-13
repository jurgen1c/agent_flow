import fs from "node:fs";
import path from "node:path";
import {
  bundledAgentFlowSkillNames,
  installAgentFlowSkills,
  type AgentFlowSkillDestination
} from "../skills";
import {
  AgentFlowWorkflowGraphError,
  AgentFlowRunStateError,
  applyAgentFlowRetention,
  createAgentFlowLifecycleRun,
  createAgentFlowNotificationRegistry,
  createAgentFlowFixtureSessionProvider,
  createAgentFlowSessionProviderRegistry,
  collectAgentFlowReviewCycleStepIds,
  executeAgentFlowCommandPipeline,
  defaultAgentFlowArchivePath,
  defaultAgentFlowExportPath,
  explainAgentFlowWorkflow,
  formatAgentFlowWorkflowIssues,
  formatWorkflowParseIssues,
  injectAgentFlowRecoveryContext,
  lintAgentFlowWorkflow,
  normalizeAgentFlowArtifactPath,
  parseAgentFlowWorkflow,
  parseAgentFlowDisagreementPolicy,
  parseAgentFlowSimulationFixture,
  openAgentFlowRunState,
  plannedAgentFlowRuntimeCommands,
  renderAgentFlowSimulationSummary,
  renderAgentFlowWorkflowGraph,
  resumeAgentFlowCommandPipeline,
  simulateAgentFlowWorkflow,
  transitionAgentFlowLifecycleRun,
  validateAgentFlowWorkflow,
  writeAgentFlowPortableArchive
} from "../runtime/index";

export interface AgentFlowCliStreams {
  stdout: Pick<NodeJS.WriteStream, "write">;
  stderr: Pick<NodeJS.WriteStream, "write">;
}

export interface AgentFlowCliResult {
  exitCode: number;
  stdout?: string;
  stderr?: string;
}

export interface AgentFlowCliOptions {
  cwd?: string;
  env?: Readonly<Record<string, string | undefined>>;
  homeDir?: string;
  skillsSourceRoot?: string;
}

const ACTIVE_LIFECYCLE_COMMANDS = [
  "run", "resume", "inject", "status", "logs", "artifacts", "pause", "cancel", "cleanup", "archive", "export"
] as const;
type ActiveLifecycleCommand = (typeof ACTIVE_LIFECYCLE_COMMANDS)[number];

export async function runCli(
  args: string[],
  streams: AgentFlowCliStreams = process,
  options: AgentFlowCliOptions = {}
): Promise<number> {
  const result = isActiveLifecycleCommand(args[0])
    ? await runLifecycleCommand(args[0], args.slice(1), options)
    : dispatch(args, options);

  if (result.stdout) {
    streams.stdout.write(result.stdout.endsWith("\n") ? result.stdout : `${result.stdout}\n`);
  }

  if (result.stderr) {
    streams.stderr.write(result.stderr.endsWith("\n") ? result.stderr : `${result.stderr}\n`);
  }

  return result.exitCode;
}

export function dispatch(args: string[], options: AgentFlowCliOptions = {}): AgentFlowCliResult {
  const [command, ...rest] = args;

  if (!command || command === "--help" || command === "-h") {
    return {
      exitCode: 0,
      stdout: renderHelp()
    };
  }

  if (command === "help") {
    const topic = rest[0];

    if (topic && !["help", "version", "skills", "validate", "lint", "explain", "graph", "simulate"].includes(topic)
        && !isActiveLifecycleCommand(topic) && !isPlannedRuntimeCommand(topic)) {
      return {
        exitCode: 7,
        stderr: `Unknown Agent Flow help topic: ${topic}\nRun \`agent-flow help\` to see available commands.`
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
      stdout: `agent-flow ${readRootPackageVersion()}`
    };
  }

  if (command === "skills") {
    return manageSkills(rest, options);
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
      stderr: `Agent Flow ${command} uses persistent run state and must be invoked through the CLI runner.`
    };
  }

  if (isPlannedRuntimeCommand(command)) {
    return {
      exitCode: 7,
      stderr: `Agent Flow command "${command}" is reserved but not active yet.\nAvailable now: help, version, validate, lint, explain, graph, simulate, run, resume, status, logs, artifacts, pause, and cancel.`
    };
  }

  return {
    exitCode: 7,
    stderr: `Unknown Agent Flow command: ${command}\nRun \`agent-flow help\` to see available commands.`
  };
}

function renderHelp(topic?: string): string {
  if (topic === "skills") {
    return [
      "agent-flow skills",
      "",
      "Usage:",
      "  agent-flow skills list",
      "  agent-flow skills install --destination <agents|codex>"
    ].join("\n");
  }

  if (topic && topic !== "help" && topic !== "version") {
    return [
      `agent-flow ${topic}`,
      "",
      ["validate", "lint", "explain", "graph", "simulate", ...ACTIVE_LIFECYCLE_COMMANDS].includes(topic as ActiveLifecycleCommand)
        ? lifecycleUsage(topic) ?? (topic === "simulate"
          ? "Usage: agent-flow simulate <workflow> --fixture <file>"
          : `Usage: agent-flow ${topic} <workflow>`)
        : "This command name is reserved for a future Agent Flow runtime surface."
    ].join("\n");
  }

  return [
    "Agent Flow",
    "",
    "Usage:",
    "  agent-flow help",
    "  agent-flow --version",
    "  agent-flow skills list",
    "  agent-flow skills install --destination <agents|codex>",
    "  agent-flow validate <workflow>",
    "  agent-flow lint <workflow>",
    "  agent-flow explain <workflow>",
    "  agent-flow graph <workflow>",
    "  agent-flow simulate <workflow> --fixture <file>",
    "  agent-flow run <workflow> --id <run-id>",
    "  agent-flow run <workflow> --id <run-id> --fixture <file>",
    "  agent-flow resume <run-id> --outcome <choice> [--fixture <file>]",
    "  agent-flow resume <run-id> --answer <value> [--fixture <file>]",
    "  agent-flow inject <run-id> <session-name> <context>",
    "  agent-flow status <run-id>",
    "  agent-flow logs <run-id>",
    "  agent-flow artifacts <run-id>",
    "  agent-flow pause <run-id>",
    "  agent-flow cancel <run-id>",
    "  agent-flow cleanup <run-id>",
    "  agent-flow cleanup --older-than <duration> [--status <status>]",
    "  agent-flow archive <run-id>",
    "  agent-flow export <run-id> --format zip",
    "",
    "Available now:",
    "  help       Show this help output.",
    "  version    Print the Agent Flow package version.",
    "  skills     List or install the bundled authoring and review skills.",
    "  validate <workflow>  Validate workflow structure, references, and safety.",
    "  lint <workflow>      Warn about complexity and risky authoring patterns.",
    "  explain <workflow>   Explain steps, artifacts, policies, and warnings.",
    "  graph <workflow>     Print a deterministic workflow graph.",
    "  simulate <workflow> --fixture <file>  Traverse a workflow from fixture data without executing steps.",
    "  run <workflow> --id <run-id> [--fixture <file>]  Execute command, artifact-transform, session-request, and review steps.",
    "  resume <run-id> (--outcome <choice> | --answer <value>) [--fixture <file>]  Resume a paused interaction.",
    "  inject <run-id> <session-name> <context>  Inject context into active recovery remediation.",
    "  status <run-id>       Inspect persistent run state.",
    "  logs <run-id>         List ordered lifecycle events.",
    "  artifacts <run-id>    List registered run artifacts.",
    "  pause <run-id>        Pause an active run.",
    "  cancel <run-id>       Cancel a non-terminal run.",
    "  cleanup <run-id>      Apply the run's declared retention policy.",
    "  cleanup --older-than <duration> [--status <status>]  Clean matching runs.",
    "  archive <run-id>      Write a portable ZIP under .agent-flow/archives.",
    "  export <run-id> --format zip  Export a portable ZIP in the repository root.",
    "",
    "Reserved placeholders:",
    `  ${plannedAgentFlowRuntimeCommands.filter((command) => !["validate", "lint", "explain", "graph", "simulate", ...ACTIVE_LIFECYCLE_COMMANDS].includes(command as ActiveLifecycleCommand)).join(", ")}`,
    "",
    "Command and artifact-transform pipeline execution, including session-request, review, approval, decision-record, retention, archive, and export operations, plus persistent lifecycle state are active."
  ].join("\n");
}

function manageSkills(args: string[], options: AgentFlowCliOptions): AgentFlowCliResult {
  if (args.length === 1 && args[0] === "list") {
    return {
      exitCode: 0,
      stdout: bundledAgentFlowSkillNames.join("\n")
    };
  }

  if (args.length !== 3 || args[0] !== "install" || args[1] !== "--destination") {
    return {
      exitCode: 1,
      stderr: "Usage: agent-flow skills install --destination <agents|codex>"
    };
  }

  const destination = args[2];
  if (destination !== "agents" && destination !== "codex") {
    return {
      exitCode: 1,
      stderr: `Unknown skill destination: ${destination}\nExpected one of: agents, codex.`
    };
  }

  try {
    const result = installAgentFlowSkills({
      destination: destination as AgentFlowSkillDestination,
      cwd: options.cwd,
      env: options.env,
      homeDir: options.homeDir,
      sourceRoot: options.skillsSourceRoot
    });
    return {
      exitCode: 0,
      stdout: `Installed ${result.skills.length} Agent Flow skills to ${result.destinationRoot}.`
    };
  } catch (error) {
    return {
      exitCode: 2,
      stderr: error instanceof Error ? error.message : String(error)
    };
  }
}

async function runLifecycleCommand(
  command: ActiveLifecycleCommand,
  args: string[],
  options: AgentFlowCliOptions
): Promise<AgentFlowCliResult> {
  const usage = lifecycleUsage(command);
  if (!validLifecycleArgs(command, args)) return { exitCode: 1, stderr: usage! };

  const workflowPath = command === "run" && options.cwd ? path.resolve(options.cwd, args[0]) : args[0];
  const workflowResult = command === "run" ? readWorkflow(workflowPath, "run") : null;
  if (workflowResult && "exitCode" in workflowResult) return workflowResult;

  let store: Awaited<ReturnType<typeof openAgentFlowRunState>> | undefined;
  try {
    store = await openAgentFlowRunState({ cwd: options.cwd });

    if (command === "cleanup") {
      return runCleanupCommand(store, args);
    }

    if (command === "archive" || command === "export") {
      const portable = parsePortableArgs(command, args)!;
      requireRun(store, portable.runId);
      const outputPath = portable.outputPath ?? (command === "archive"
        ? defaultAgentFlowArchivePath(portable.runId)
        : defaultAgentFlowExportPath(portable.runId));
      const result = writeAgentFlowPortableArchive(store, portable.runId, outputPath);
      return {
        exitCode: 0,
        stdout: `${command === "archive" ? "Archived" : "Exported"} Agent Flow run ${portable.runId} to ${result.outputPath}.\nEntries: ${result.entryCount}\nSize: ${result.sizeBytes} bytes`
      };
    }

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
      const resolverSessions = collectDisagreementResolverSessions(workflowResult!.workflow, sessionRequestSteps);
      const unsupportedProviders = [
        ...sessionRequestSteps.map((step) => step.type === "review" || step.type === "approval" ? step.reviewer
          : step.type === "consult" || step.type === "challenge" ? step.to : step.session)
          .filter((session): session is string => typeof session === "string"),
        ...resolverSessions
      ]
        .map((session) => workflowResult!.workflow.sessions?.[session.trim()])
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
      const result = createAgentFlowLifecycleRun(store, {
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
      const providers = createAgentFlowSessionProviderRegistry();
      if (fixture !== null) providers.register("fixture", createAgentFlowFixtureSessionProvider(
        fixture.responses,
        fixture.outcomes,
        fixture.disagreements
      ));
      const terminalNotifications: string[] = [];
      const notifications = createAgentFlowNotificationRegistry({
        terminal: (notification) => {
          terminalNotifications.push(notification.message);
        }
      });
      const execution = await executeAgentFlowCommandPipeline(
        store,
        result.run.id,
        workflowResult!.workflow,
        undefined,
        providers,
        undefined,
        notifications
      );
      const lines = [
        `${result.changed ? "Created" : "Reused"} Agent Flow run ${result.run.id} for ${result.run.workflowName} (version ${result.run.workflowVersion}).`,
        `Status: ${execution.status}`,
        `Completed steps: ${execution.completedSteps.length === 0 ? "none" : execution.completedSteps.join(", ")}`,
        ...terminalNotifications.map((message) => `Notification: ${message}`)
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
          ? `No events recorded for Agent Flow run ${runId}.`
          : events.map((event) => `${event.sequence}\t${event.createdAt}\t${event.type}\t${JSON.stringify(event.payload)}`).join("\n")
      };
    }
    if (command === "artifacts") {
      requireRun(store, runId);
      const artifacts = store.listArtifacts(runId);
      return {
        exitCode: 0,
        stdout: artifacts.length === 0
          ? `No artifacts registered for Agent Flow run ${runId}.`
          : artifacts.map((artifact) => `${artifact.declaredPath}\t${artifact.status}\t${artifact.kind}\t${artifact.contentType}`).join("\n")
      };
    }

    if (command === "inject") {
      requireRun(store, runId);
      const session = injectAgentFlowRecoveryContext(store, runId, args[1], args[2]);
      return {
        exitCode: 0,
        stdout: `Injected recovery context into session ${session.id} for Agent Flow run ${runId}.\nSession dirty: ${session.state.dirty === true ? "yes" : "no"}`
      };
    }

    if (command === "resume") {
      const run = requireRun(store, runId);
      const workflow = run.context.workflow;
      if (workflow === null || typeof workflow !== "object" || Array.isArray(workflow)) {
        throw new AgentFlowRunStateError(
          `Agent Flow run ${runId} does not contain its persisted workflow definition.`,
          "AGENT_FLOW_RESUME_STATE"
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
          (workflow as unknown as import("../runtime/index").AgentFlowWorkflow).steps
        ).find((step) => fixture.arrayOutputSteps.has(String(step.id ?? "").trim()));
        if (unsupportedOutputStep !== undefined) {
          return {
            exitCode: 2,
            stderr: `Run fixture step ${String(unsupportedOutputStep.id).trim()}.outputs must be an object with materializable output values; array-form outputs are simulation-only.`
          };
        }
      }
      const providers = createAgentFlowSessionProviderRegistry();
      if (fixture !== null) {
        providers.register("fixture", createAgentFlowFixtureSessionProvider(
          fixture.responses,
          fixture.outcomes,
          fixture.disagreements
        ));
      }
      const terminalNotifications: string[] = [];
      const notifications = createAgentFlowNotificationRegistry({
        terminal: (notification) => {
          terminalNotifications.push(notification.message);
        }
      });
      const execution = await resumeAgentFlowCommandPipeline(
        store,
        runId,
        workflow as unknown as import("../runtime/index").AgentFlowWorkflow,
        response,
        undefined,
        providers,
        undefined,
        notifications
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
        `Resumed Agent Flow run ${runId}.`,
        `Status: ${execution.status}`,
        `Completed steps: ${execution.completedSteps.length === 0 ? "none" : execution.completedSteps.join(", ")}`,
        ...terminalNotifications.map((message) => `Notification: ${message}`)
      ];
      return {
        exitCode: execution.status === "completed" ? 0 : execution.status === "paused" ? 3 : 1,
        stdout: lines.join("\n"),
        stderr: execution.message
      };
    }

    const terminalNotifications: string[] = [];
    const result = transitionAgentFlowLifecycleRun(
      store,
      runId,
      command,
      createAgentFlowNotificationRegistry({
        terminal: (notification) => {
          terminalNotifications.push(notification.message);
        }
      })
    );
    const verb = command === "pause" ? "Paused" : "Cancelled";
    const lines = [
      result.run.status === "failed"
        ? `Failed to ${command} Agent Flow run ${runId}.`
        : `${result.changed ? verb : "No change for"} Agent Flow run ${runId}.`,
      `Status: ${result.run.status}`,
      ...terminalNotifications.map((message) => `Notification: ${message}`)
    ];
    return {
      exitCode: result.run.status === "failed" ? 1 : 0,
      stdout: lines.join("\n"),
      ...(result.run.status === "failed" && result.run.error !== null
        && typeof result.run.error === "object" && !Array.isArray(result.run.error)
        && typeof result.run.error.message === "string"
        ? { stderr: result.run.error.message }
        : {})
    };
  } catch (error) {
    if (error instanceof AgentFlowRunStateError) {
      return { exitCode: error.code === "AGENT_FLOW_RUN_NOT_FOUND" ? 4 : 2, stderr: error.message };
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
  if (command === "inject") {
    return args.length === 3 && args.every((entry) => entry.length > 0);
  }
  if (command === "cleanup") return parseCleanupArgs(args) !== null;
  if (command === "archive" || command === "export") return parsePortableArgs(command, args) !== null;
  return args.length === 1 && args[0].length > 0;
}

function lifecycleUsage(topic: string): string | null {
  if (topic === "run") return "Usage: agent-flow run <workflow> --id <run-id> [--fixture <file>]";
  if (topic === "resume") return "Usage: agent-flow resume <run-id> (--outcome <choice> | --answer <value>) [--fixture <file>]";
  if (topic === "inject") return "Usage: agent-flow inject <run-id> <session-name> <context>";
  if (topic === "cleanup") return "Usage: agent-flow cleanup ([--] <run-id> | --older-than <duration> [--status <status>]) [--approve]";
  if (topic === "archive") return "Usage: agent-flow archive [--] <run-id> [--output <file>]";
  if (topic === "export") return "Usage: agent-flow export [--] <run-id> --format zip [--output <file>]";
  if (isActiveLifecycleCommand(topic)) return `Usage: agent-flow ${topic} <run-id>`;
  return null;
}

function runCleanupCommand(
  store: Awaited<ReturnType<typeof openAgentFlowRunState>>,
  args: string[]
): AgentFlowCliResult {
  const input = parseCleanupArgs(args)!;
  const now = Date.parse(store.currentTimestamp());
  const runs = input.runId === undefined
    ? store.listRuns().filter((run) => {
        if (input.status !== undefined && run.status !== input.status) return false;
        return ageDays(run.finishedAt ?? run.updatedAt, now) >= input.olderThanDays!;
      })
    : [requireRun(store, input.runId)];
  if (runs.length === 0) {
    return { exitCode: 0, stdout: "No Agent Flow runs matched the cleanup filters." };
  }

  const lines: string[] = [];
  let totalDeleted = 0;
  let totalSkipped = 0;
  let totalFailed = 0;
  let totalRunErrors = 0;
  for (const run of runs) {
    try {
      const workflow = persistedWorkflow(run);
      const result = store.withRunFinalizationTransaction(run.id, () => applyAgentFlowRetention(
        store,
        run.id,
        workflow,
        run.status,
        {
          explicit: true,
          ageDays: ageDays(run.finishedAt ?? run.updatedAt, now),
          ...(input.approved ? { approvalStatus: "approved" as const } : {})
        }
      ));
      totalDeleted += result.deleted.length;
      totalSkipped += result.skipped.length;
      totalFailed += result.failed.length;
      lines.push(
        `${run.id}\t${run.status}\t${result.status}\tdeleted=${result.deleted.length}`
        + `\tskipped=${result.skipped.length}\tfailed=${result.failed.length}`
      );
    } catch (error) {
      if (input.runId !== undefined) throw error;
      totalRunErrors += 1;
      const status = error instanceof AgentFlowRunStateError && error.code === "AGENT_FLOW_RETENTION_STATE"
        ? "workflow_error"
        : "run_error";
      lines.push(`${run.id}\t${run.status}\t${status}\tdeleted=0\tskipped=0\tfailed=0`);
    }
  }
  const errors: string[] = [];
  if (totalFailed > 0) {
    errors.push(`Cleanup could not delete ${totalFailed} artifact${totalFailed === 1 ? "" : "s"}.`);
  }
  if (totalRunErrors > 0) {
    errors.push(`Cleanup could not process ${totalRunErrors} run${totalRunErrors === 1 ? "" : "s"}.`);
  }
  return {
    exitCode: errors.length === 0 ? 0 : 2,
    stdout: [
      `Cleanup processed ${runs.length} Agent Flow run${runs.length === 1 ? "" : "s"}.`,
      `Deleted artifacts: ${totalDeleted}`,
      `Skipped artifacts: ${totalSkipped}`,
      `Failed deletions: ${totalFailed}`,
      `Run errors: ${totalRunErrors}`,
      ...lines
    ].join("\n"),
    ...(errors.length === 0 ? {} : { stderr: errors.join("\n") })
  };
}

function persistedWorkflow(
  run: import("../runtime/index").AgentFlowRunRecord
): import("../runtime/index").AgentFlowWorkflow {
  const workflow = run.context.workflow;
  if (workflow === null || typeof workflow !== "object" || Array.isArray(workflow)) {
    throw new AgentFlowRunStateError(
      `Agent Flow run ${run.id} does not contain its persisted workflow definition.`,
      "AGENT_FLOW_RETENTION_STATE"
    );
  }
  const typed = workflow as unknown as import("../runtime/index").AgentFlowWorkflow;
  const validation = validateAgentFlowWorkflow(typed);
  if (!validation.valid) {
    throw new AgentFlowRunStateError(
      `Agent Flow run ${run.id} contains an invalid persisted workflow definition.\n${formatAgentFlowWorkflowIssues(validation.errors)}`,
      "AGENT_FLOW_RETENTION_STATE"
    );
  }
  return typed;
}

function ageDays(timestamp: string, now: number): number {
  const then = Date.parse(timestamp);
  if (!Number.isFinite(then) || !Number.isFinite(now)) return 0;
  return Math.max(0, (now - then) / 86_400_000);
}

function parseCleanupArgs(args: string[]): {
  runId?: string;
  olderThanDays?: number;
  status?: import("../runtime/index").AgentFlowRunStatus;
  approved: boolean;
} | null {
  const escapedRunId = args[0] === "--";
  const values = escapedRunId ? args.slice(1) : args.slice();
  if (escapedRunId || values[0] !== "--older-than") {
    if (values.length === 1 && values[0]!.length > 0) {
      return { runId: values[0], approved: false };
    }
    if (values.length === 2 && values[0]!.length > 0 && values[1] === "--approve") {
      return { runId: values[0], approved: true };
    }
    return null;
  }
  const approved = values.at(-1) === "--approve";
  if (approved) values.pop();
  if (values.length !== 2 && values.length !== 4) return null;
  if (values[0] !== "--older-than") return null;
  const olderThanDays = parseDurationDays(values[1]!);
  if (olderThanDays === null) return null;
  if (values.length === 2) return { olderThanDays, approved };
  if (values[2] !== "--status" || !isRunStatus(values[3]!)) return null;
  return { olderThanDays, status: values[3], approved };
}

function parsePortableArgs(
  command: "archive" | "export",
  args: string[]
): { runId: string; outputPath?: string } | null {
  const values = args[0] === "--" ? args.slice(1) : args;
  if (values.length === 0 || values[0]!.length === 0) return null;
  const runId = values[0]!;
  let index = 1;
  if (command === "export") {
    if (values[index] !== "--format" || values[index + 1] !== "zip") return null;
    index += 2;
  }
  if (index === values.length) return { runId };
  if (values.length === index + 2 && values[index] === "--output" && values[index + 1]!.length > 0) {
    return { runId, outputPath: values[index + 1] };
  }
  return null;
}

function parseDurationDays(value: string): number | null {
  const match = /^(\d+)([mhd])$/.exec(value);
  if (match === null) return null;
  const amount = Number(match[1]);
  if (!Number.isSafeInteger(amount)) return null;
  if (match[2] === "m") return amount / 1_440;
  if (match[2] === "h") return amount / 24;
  return amount;
}

function isRunStatus(value: string): value is import("../runtime/index").AgentFlowRunStatus {
  return ["pending", "running", "waiting", "paused", "completed", "failed", "cancelled"].includes(value);
}

function parseCliAnswer(value: string): import("../runtime/index").AgentFlowRunStateValue {
  try {
    const parsed = JSON.parse(value) as unknown;
    if (isRunStateValue(parsed)) return parsed;
  } catch {
    // Plain text answers are valid input-request values.
  }
  return value;
}

function isRunStateValue(value: unknown): value is import("../runtime/index").AgentFlowRunStateValue {
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
  inputs: Record<string, import("../runtime/index").AgentFlowRunStateValue>;
  artifacts: Record<string, import("../runtime/index").AgentFlowRunStateValue>;
  responses: Record<string, import("../runtime/index").AgentFlowSessionProviderResponse>;
  outcomes: Record<string, "succeeded" | "failed" | Array<"succeeded" | "failed">>;
  disagreements: Record<
    string,
    import("../runtime/index").AgentFlowDisagreementDecision | "unresolved" | "failed"
      | Array<import("../runtime/index").AgentFlowDisagreementDecision | "unresolved" | "failed">
  >;
  arrayOutputSteps: Set<string>;
} | AgentFlowCliResult {
  const resolvedPath = cwd === undefined ? fixturePath : path.resolve(cwd, fixturePath);
  let source: string;
  try {
    source = fs.readFileSync(resolvedPath, "utf8");
  } catch (error) {
    return { exitCode: 1, stderr: `Could not read Agent Flow run fixture ${fixturePath}: ${error instanceof Error ? error.message : String(error)}` };
  }
  const parsed = parseAgentFlowSimulationFixture(source);
  if (!parsed.ok) return { exitCode: 2, stderr: `Could not parse Agent Flow run fixture ${fixturePath}: ${parsed.error}` };
  const responses: Record<string, import("../runtime/index").AgentFlowSessionProviderResponse> = {};
  const outcomes: Record<string, "succeeded" | "failed" | Array<"succeeded" | "failed">> = {};
  const disagreements: Record<
    string,
    import("../runtime/index").AgentFlowDisagreementDecision | "unresolved" | "failed"
      | Array<import("../runtime/index").AgentFlowDisagreementDecision | "unresolved" | "failed">
  > = {};
  const arrayOutputSteps = new Set<string>();
  for (const [stepId, fixture] of Object.entries(parsed.fixture.steps ?? {})) {
    if (fixture.outcome !== undefined) outcomes[stepId] = fixture.outcome;
    if (fixture.disagreement !== undefined) disagreements[stepId] = fixture.disagreement;
    if (Array.isArray(fixture.outputs)) {
      arrayOutputSteps.add(stepId);
      continue;
    }
    if (fixture.outputs === undefined) continue;
    const outputs: Record<string, string> = {};
    for (const [declaredPath, value] of Object.entries(fixture.outputs)) {
      let canonicalPath: string;
      try {
        canonicalPath = normalizeAgentFlowArtifactPath(declaredPath);
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
  const artifacts: Record<string, import("../runtime/index").AgentFlowRunStateValue> = {};
  for (const [declaredPath, value] of Object.entries(parsed.fixture.artifacts ?? {})) {
    let canonicalPath: string;
    try {
      canonicalPath = normalizeAgentFlowArtifactPath(declaredPath);
    } catch (error) {
      return { exitCode: 2, stderr: `Run fixture artifact ${JSON.stringify(declaredPath)} is invalid: ${error instanceof Error ? error.message : String(error)}` };
    }
    if (Object.hasOwn(artifacts, canonicalPath)) {
      return { exitCode: 2, stderr: `Run fixture artifact keys collide at canonical path ${canonicalPath}.` };
    }
    artifacts[canonicalPath] = value as import("../runtime/index").AgentFlowRunStateValue;
  }
  return {
    inputs: (parsed.fixture.inputs ?? {}) as unknown as Record<string, import("../runtime/index").AgentFlowRunStateValue>,
    artifacts,
    responses,
    outcomes,
    disagreements,
    arrayOutputSteps
  };
}

function requireRun(
  store: Awaited<ReturnType<typeof openAgentFlowRunState>>,
  runId: string
): NonNullable<ReturnType<typeof store.getRun>> {
  const run = store.getRun(runId);
  if (run === null) throw new AgentFlowRunStateError(`Agent Flow run ${runId} was not found.`, "AGENT_FLOW_RUN_NOT_FOUND");
  return run;
}

function renderRunStatus(run: NonNullable<ReturnType<Awaited<ReturnType<typeof openAgentFlowRunState>>["getRun"]>>): string {
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

function simulateWorkflow(args: string[]): AgentFlowCliResult {
  if (args.length !== 3 || args[1] !== "--fixture") {
    return { exitCode: 1, stderr: "Usage: agent-flow simulate <workflow> --fixture <file>" };
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
      stderr: `Could not read Agent Flow simulation fixture ${fixturePath}: ${error instanceof Error ? error.message : String(error)}`
    };
  }

  const fixture = parseAgentFlowSimulationFixture(fixtureSource);
  if (!fixture.ok) {
    return {
      exitCode: 2,
      stderr: `Could not parse Agent Flow simulation fixture ${fixturePath}: ${fixture.error}`
    };
  }

  const result = simulateAgentFlowWorkflow(workflowResult.workflow, fixture.fixture);
  return {
    exitCode: result.status === "unresolved" ? 2 : 0,
    stdout: renderAgentFlowSimulationSummary(result)
  };
}

function checkWorkflow(command: "validate" | "lint" | "explain" | "graph", args: string[]): AgentFlowCliResult {
  const workflowPath = args[0];

  if (!workflowPath || args.length !== 1) {
    return { exitCode: 1, stderr: `Usage: agent-flow ${command} <workflow>` };
  }

  const workflowResult = readWorkflow(workflowPath, command);
  if ("exitCode" in workflowResult) return workflowResult;
  const workflow = workflowResult.workflow;

  if (command === "explain") {
    return { exitCode: 0, stdout: explainAgentFlowWorkflow(workflow) };
  }

  if (command === "graph") {
    try {
      return { exitCode: 0, stdout: renderAgentFlowWorkflowGraph(workflow) };
    } catch (error) {
      if (error instanceof AgentFlowWorkflowGraphError) {
        return {
          exitCode: 2,
          stderr: `Agent Flow graph failed: ${workflowPath}\n${error.code}: ${error.message}`
        };
      }
      return {
        exitCode: 2,
        stderr: `Agent Flow graph failed: ${workflowPath}\nworkflow.graph.internal: ${error instanceof Error ? error.message : String(error)}`
      };
    }
  }

  if (command === "validate") {
    const warnings = lintAgentFlowWorkflow(workflow).warnings;

    return warnings.length === 0
      ? { exitCode: 0, stdout: `Agent Flow validation passed: ${workflowPath}` }
      : {
          exitCode: 0,
          stdout: `Agent Flow validation passed with ${warnings.length} warning${warnings.length === 1 ? "" : "s"}: ${workflowPath}\n${formatAgentFlowWorkflowIssues(warnings)}`
        };
  }

  const lint = lintAgentFlowWorkflow(workflow);

  if (lint.warnings.length === 0) {
    return { exitCode: 0, stdout: `Agent Flow lint passed with no warnings: ${workflowPath}` };
  }

  return {
    exitCode: 0,
    stdout: `Agent Flow lint found ${lint.warnings.length} warning${lint.warnings.length === 1 ? "" : "s"}: ${workflowPath}\n${formatAgentFlowWorkflowIssues(lint.warnings)}`
  };
}

function readWorkflow(
  workflowPath: string,
  command: "validate" | "lint" | "explain" | "graph" | "simulate" | "run"
): { workflow: import("../runtime/index").AgentFlowWorkflow } | AgentFlowCliResult {
  let source: string;

  try {
    source = fs.readFileSync(workflowPath, "utf8");
  } catch (error) {
    return {
      exitCode: 1,
      stderr: `Could not read Agent Flow workflow ${workflowPath}: ${error instanceof Error ? error.message : String(error)}`
    };
  }

  const parsed = parseAgentFlowWorkflow(source);
  if (!parsed.ok) {
    return {
      exitCode: 2,
      stderr: `Agent Flow ${command} failed: ${workflowPath}\n${formatWorkflowParseIssues(parsed.errors)}`
    };
  }

  const validation = validateAgentFlowWorkflow(parsed.workflow);
  if (!validation.valid) {
    return {
      exitCode: 2,
      stderr: `Agent Flow ${command} failed: ${workflowPath}\n${formatAgentFlowWorkflowIssues(validation.errors)}`
    };
  }

  return { workflow: parsed.workflow };
}

function collectSessionRequestSteps(
  steps: import("../runtime/index").AgentFlowWorkflowStep[]
): import("../runtime/index").AgentFlowWorkflowStep[] {
  const requests: import("../runtime/index").AgentFlowWorkflowStep[] = [];
  const visit = (step: import("../runtime/index").AgentFlowWorkflowStep): void => {
    if (typeof step.type === "string" && (
      ["challenge", "consult", "review", "session_request"].includes(step.type.trim())
      || (step.type.trim() === "approval" && String(step.reviewer ?? "").trim() !== "human")
    )) requests.push(step);
    for (const field of ["body", "steps", "branches"] as const) {
      const nested = step[field];
      if (!Array.isArray(nested)) continue;
      for (const entry of nested) {
        if (entry !== null && typeof entry === "object" && !Array.isArray(entry)) {
          visit(entry as import("../runtime/index").AgentFlowWorkflowStep);
        }
      }
    }
  };
  steps.forEach(visit);
  return requests;
}

function collectDisagreementResolverSessions(
  workflow: import("../runtime/index").AgentFlowWorkflow,
  sessionRequestSteps: import("../runtime/index").AgentFlowWorkflowStep[]
): string[] {
  const collaboration = workflow.collaboration;
  if (collaboration === null || typeof collaboration !== "object" || Array.isArray(collaboration)
      || collaboration.on_disagreement === undefined) return [];
  const policy = parseAgentFlowDisagreementPolicy(collaboration.on_disagreement);
  if (policy.arbiter !== undefined) return [policy.arbiter];
  if (policy.strategy !== "owner_decides") return [];
  const reviewCycleIds = collectAgentFlowReviewCycleStepIds(workflow.steps);
  return sessionRequestSteps
    .filter((step) => step.type === "review" && reviewCycleIds.has(String(step.id ?? "").trim()))
    .map((step) => String(step.subject ?? "").trim())
    .filter((session) => session.length > 0);
}

function isPlannedRuntimeCommand(command: string): boolean {
  return plannedAgentFlowRuntimeCommands.includes(command as (typeof plannedAgentFlowRuntimeCommands)[number]);
}

function isActiveLifecycleCommand(command: string | undefined): command is ActiveLifecycleCommand {
  return command !== undefined && ACTIVE_LIFECYCLE_COMMANDS.includes(command as ActiveLifecycleCommand);
}

function readRootPackageVersion(): string {
  const candidates = [
    new URL("../../package.json", import.meta.url),
    new URL("../package.json", import.meta.url)
  ];

  for (const packageUrl of candidates) {
    try {
      const packageJson = JSON.parse(fs.readFileSync(packageUrl, "utf8")) as { name?: unknown; version?: unknown };

      if (packageJson.name === "@jurgen1c/agent-flow" && typeof packageJson.version === "string" && packageJson.version.length > 0) {
        return packageJson.version;
      }
    } catch {
      // Try the next source/bundled package.json location.
    }
  }

  return "0.0.0";
}
