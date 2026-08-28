import fs from "node:fs";
import path from "node:path";
import { isDeepStrictEqual } from "node:util";
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
  createAgentFlowConfiguredProviderRegistry,
  collectAgentFlowReviewCycleStepIds,
  executeAgentFlowCommandPipeline,
  defaultAgentFlowArchivePath,
  defaultAgentFlowExportPath,
  explainAgentFlowWorkflow,
  formatAgentFlowWorkflowIssues,
  formatWorkflowParseIssues,
  injectAgentFlowRecoveryContext,
  loadAgentFlowProviderCatalog,
  loadAgentFlowRepositoryProviderAliases,
  lintAgentFlowWorkflow,
  loadAgentFlowWorkflowRegistry,
  normalizeAgentFlowArtifactPath,
  providerBindingsForWorkflow,
  parseAgentFlowWorkflow,
  parseAgentFlowDisagreementPolicy,
  parseAgentFlowSimulationFixture,
  openAgentFlowRunState,
  plannedAgentFlowRuntimeCommands,
  renderAgentFlowSimulationSummary,
  renderAgentFlowProviderCatalog,
  renderAgentFlowWorkflowGraph,
  resumeAgentFlowCommandPipeline,
  createAgentFlowWorkflowRegistry,
  createAgentFlowWorkflowRegistryFromSnapshot,
  serializeAgentFlowWorkflowRegistry,
  simulateAgentFlowWorkflow,
  transitionAgentFlowLifecycleRun,
  doctorAgentFlowProviderCatalog,
  serializeAgentFlowProviderBindings,
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
  skillsCopyDirectory?: (source: string, destination: string) => void;
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

    if (topic && !["help", "version", "skills", "config", "providers", "validate", "lint", "explain", "graph", "simulate"].includes(topic)
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

  if (command === "config") return manageConfig(rest, options);
  if (command === "providers") return manageProviders(rest, options);

  if (command === "validate" || command === "lint" || command === "explain" || command === "graph") {
    return checkWorkflow(command, rest, options);
  }

  if (command === "simulate") {
    return simulateWorkflow(rest, options);
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

  if (topic === "config") {
    return "agent-flow config\n\nUsage: agent-flow config validate [--config <file>]";
  }

  if (topic === "providers") {
    return "agent-flow providers\n\nUsage: agent-flow providers <list|doctor> [--config <file>]";
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
    "  agent-flow config validate [--config <file>]",
    "  agent-flow providers list [--config <file>]",
    "  agent-flow providers doctor [--config <file>]",
    "  agent-flow validate <workflow>",
    "  agent-flow lint <workflow>",
    "  agent-flow explain <workflow>",
    "  agent-flow graph <workflow>",
    "  agent-flow simulate <workflow> --fixture <file>",
    "  agent-flow run <workflow> --id <run-id> [--input <key=value>] [--input-file <json>] [--provider <alias=target>]",
    "  agent-flow run <workflow> --id <run-id> [--profile <name>] [--model <name>] [--reasoning-effort <level>]",
    "  agent-flow run <workflow> --id <run-id> --fixture <file>",
    "  agent-flow resume <run-id> --outcome <choice> [--fixture <file>]",
    "  agent-flow resume <run-id> --answer <value> [--fixture <file>]",
    "  agent-flow resume <run-id> --reset-session <session-name> [--fixture <file>] [--config <file>]",
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
    "  config     Validate global targets and repository provider aliases.",
    "  providers  List configured aliases or check local readiness.",
    "  validate <workflow>  Validate workflow structure, references, and safety.",
    "  lint <workflow>      Warn about complexity and risky authoring patterns.",
    "  explain <workflow>   Explain steps, artifacts, policies, and warnings.",
    "  graph <workflow>     Print a deterministic workflow graph.",
    "  simulate <workflow> --fixture <file>  Traverse a workflow from fixture data without executing steps.",
    "  run <workflow> --id <run-id> [--fixture <file>]  Execute command, artifact-transform, session-request, and review steps.",
    "  resume <run-id> (--outcome <choice> | --answer <value> | --reset-session <name>) [--fixture <file>]  Resume a paused interaction.",
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
      sourceRoot: options.skillsSourceRoot,
      copyDirectory: options.skillsCopyDirectory
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

function manageConfig(args: string[], options: AgentFlowCliOptions): AgentFlowCliResult {
  const parsed = parseCatalogCommandArgs(args, "validate", "agent-flow config validate [--config <file>]");
  if ("exitCode" in parsed) return parsed;
  try {
    const catalog = loadAgentFlowProviderCatalog({
      cwd: options.cwd,
      env: options.env,
      homeDir: options.homeDir,
      ...(parsed.configPath === undefined ? {} : { configPath: parsed.configPath })
    });
    return {
      exitCode: 0,
      stdout: `Agent Flow provider configuration is valid.\nGlobal: ${catalog.globalConfigPath}\nRepository: ${catalog.repoConfigPath}\nTargets: ${Object.keys(catalog.targets).length}\nAliases: ${Object.keys(catalog.providers).length}`
    };
  } catch (error) {
    return { exitCode: 2, stderr: error instanceof Error ? error.message : String(error) };
  }
}

function manageProviders(args: string[], options: AgentFlowCliOptions): AgentFlowCliResult {
  const action = args[0];
  if (action !== "list" && action !== "doctor") {
    return { exitCode: 1, stderr: "Usage: agent-flow providers <list|doctor> [--config <file>]" };
  }
  const parsed = parseCatalogCommandArgs(args, action, `agent-flow providers ${action} [--config <file>]`);
  if ("exitCode" in parsed) return parsed;
  try {
    const catalog = loadAgentFlowProviderCatalog({
      cwd: options.cwd,
      env: options.env,
      homeDir: options.homeDir,
      ...(parsed.configPath === undefined ? {} : { configPath: parsed.configPath })
    });
    if (action === "list") return { exitCode: 0, stdout: renderAgentFlowProviderCatalog(catalog) };
    const result = doctorAgentFlowProviderCatalog(catalog, options.env ?? process.env);
    return { exitCode: result.ok ? 0 : 2, [result.ok ? "stdout" : "stderr"]: result.lines.join("\n") };
  } catch (error) {
    return { exitCode: 2, stderr: error instanceof Error ? error.message : String(error) };
  }
}

function parseCatalogCommandArgs(
  args: string[],
  action: string,
  usage: string
): { configPath?: string } | AgentFlowCliResult {
  if (args[0] !== action || (args.length !== 1 && !(args.length === 3 && args[1] === "--config" && args[2]))) {
    return { exitCode: 1, stderr: `Usage: ${usage}` };
  }
  return args.length === 3 ? { configPath: args[2] } : {};
}

async function runLifecycleCommand(
  command: ActiveLifecycleCommand,
  args: string[],
  options: AgentFlowCliOptions
): Promise<AgentFlowCliResult> {
  const usage = lifecycleUsage(command);
  const parsedRun = command === "run" ? parseRunLifecycleArgs(args) : undefined;
  const parsedResume = command === "resume" ? parseResumeLifecycleArgs(args) : undefined;
  if ((parsedRun !== undefined && "exitCode" in parsedRun)
      || (parsedResume !== undefined && "exitCode" in parsedResume)
      || ((command !== "run" && command !== "resume") && !validLifecycleArgs(command, args))) {
    return { exitCode: 1, stderr: usage! };
  }

  const workflowPath = command === "run"
    ? path.resolve(options.cwd ?? process.cwd(), (parsedRun as ParsedRunLifecycleArgs).workflowPath)
    : args[0];
  const workflowResult = command === "run" ? readWorkflow(workflowPath, "run") : null;
  if (workflowResult && "exitCode" in workflowResult) return workflowResult;
  if (command === "run") {
    const directMcp = collectWorkflowSteps(workflowResult!.workflow.steps)
      .filter((step) => String(step.type ?? "").trim() === "mcp_call"
        && (step.via === undefined || String(step.via).trim() === "direct"));
    if (directMcp.length > 0) {
      return {
        exitCode: 1,
        stderr: `Stock Agent Flow CLI cannot execute direct MCP step ${String(directMcp[0]!.id)}; use via: codex with a named Codex session or run through a host that registers MCP adapters.`
      };
    }
  }

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
      const runArgs = parsedRun as ParsedRunLifecycleArgs;
      const existingRun = store.getRun(runArgs.runId);
      const availableWorkflows = existingRun === null
        ? loadAgentFlowWorkflowRegistry(workflowPath, { cwd: options.cwd })
        : createAgentFlowWorkflowRegistryFromSnapshot(
            existingRun.context.workflowRegistry
              ?? { [workflowResult!.workflow.name]: workflowResult!.workflow } as unknown as import("../runtime/index").AgentFlowRunStateValue
          );
      const workflows = reachableWorkflowRegistry(
        availableWorkflows,
        existingRun?.workflowName ?? workflowResult!.workflow.name
      );
      const registeredWorkflows = workflows.names().map((name) => workflows.get(name)!);
      const directMcp = registeredWorkflows.flatMap((registeredWorkflow) =>
        collectWorkflowSteps(registeredWorkflow.steps).map((step) => ({ workflow: registeredWorkflow, step }))
      ).find(({ step }) => String(step.type ?? "").trim() === "mcp_call"
        && (step.via === undefined || String(step.via).trim() === "direct"));
      if (directMcp !== undefined) {
        return {
          exitCode: 1,
          stderr: `Stock Agent Flow CLI cannot execute direct MCP step ${String(directMcp.step.id)} in workflow ${directMcp.workflow.name}; use via: codex with a named Codex session or run through a host that registers MCP adapters.`
        };
      }
      const workflowSnapshot = serializeAgentFlowWorkflowRegistry(workflows);
      const fixture = runArgs.fixturePath === undefined ? null : readRunFixture(runArgs.fixturePath, options.cwd);
      if (fixture !== null && "exitCode" in fixture) return fixture;
      const inputFile = runArgs.inputFilePath === undefined
        ? { ok: true as const, inputs: {} }
        : readRunInputFile(runArgs.inputFilePath, options.cwd);
      if (!inputFile.ok) return inputFile.result;
      const explicitInputs = parseRunInputs(runArgs.inputValues);
      if (!explicitInputs.ok) return explicitInputs.result;
      const providedInputs = {
        ...(fixture === null ? {} : fixture.inputs),
        ...inputFile.inputs,
        ...explicitInputs.inputs
      };
      if (existingRun !== null) {
        const changedInput = Object.entries(providedInputs).find(([name, value]) =>
          !Object.hasOwn(existingRun.inputs, name) || !isDeepStrictEqual(existingRun.inputs[name], value)
        );
        if (changedInput !== undefined) {
          return {
            exitCode: 2,
            stderr: `Agent Flow run ${runArgs.runId} input ${JSON.stringify(changedInput[0])} differs from its persisted value; start a new run ID to change inputs.`
          };
        }
      }
      const inputs = existingRun?.inputs ?? providedInputs;
      const inputError = validateRunInputs(workflowResult!.workflow, inputs);
      if (inputError !== undefined) return { exitCode: 2, stderr: inputError };
      const sessionRequestSteps = registeredWorkflows.flatMap((registeredWorkflow) =>
        collectSessionRequestSteps(registeredWorkflow.steps).map((step) => ({ workflow: registeredWorkflow, step }))
      );
      const requiredProviders = registeredWorkflows.flatMap((registeredWorkflow) =>
        collectRequiredWorkflowProviders(registeredWorkflow)
      );
      const usesFixtureProvider = requiredProviders.includes("fixture");
      const repositoryAliases = loadAgentFlowRepositoryProviderAliases({ cwd: options.cwd });
      const providerOverrides = existingRun === null
        ? runArgs.providerOverrides
        : reconcilePinnedProviderOverrides(
            persistedProviderOverrides(existingRun.context.providerBindings, runArgs.runId),
            runArgs.providerOverrides,
            runArgs.runId
          );
      const catalog = loadAgentFlowProviderCatalog({
        cwd: options.cwd,
        env: options.env,
        homeDir: options.homeDir,
        ...(runArgs.configPath === undefined ? {} : { configPath: runArgs.configPath }),
        overrides: providerOverrides,
        aliases: requiredProviders.filter((provider) => provider !== "fixture" && provider !== "codex")
      });
      const configuredValidations = registeredWorkflows.map((registeredWorkflow) => ({
        workflow: registeredWorkflow,
        validation: validateAgentFlowWorkflow(
          registeredWorkflow,
          (provider) => provider === "codex" ? "frontier"
            : Object.hasOwn(repositoryAliases, provider) ? repositoryAliases[provider]!.kind : undefined
        )
      }));
      const invalidConfiguredWorkflow = configuredValidations.find(({ validation }) => !validation.valid);
      if (invalidConfiguredWorkflow !== undefined) {
        return {
          exitCode: 2,
          stderr: `Agent Flow run failed: ${workflowPath} (${invalidConfiguredWorkflow.workflow.name})\n${formatAgentFlowWorkflowIssues(invalidConfiguredWorkflow.validation.errors)}`
        };
      }
      if (usesFixtureProvider && fixture === null) {
        return {
          exitCode: 1,
          stderr: "Session-request workflows using provider \"fixture\" require --fixture <file>."
        };
      }
      const missingProviders = [...new Set(requiredProviders)]
        .filter((provider) => provider !== "fixture" && provider !== "codex" && !Object.hasOwn(catalog.bindings, provider))
        .sort();
      if (missingProviders.length > 0) {
        return {
          exitCode: 1,
          stderr: fixture === null
            ? `No configured CLI target resolves unsupported providers: ${missingProviders.join(", ")}. Add aliases to .agent-flow.yml or use provider "fixture" with --fixture.`
            : `CLI fixture mode supports only provider "fixture" unless other providers are configured; unsupported providers: ${missingProviders.join(", ")}.`
        };
      }
      if (fixture !== null) {
        const unsupportedOutputStep = sessionRequestSteps.find(({ workflow, step }) =>
          fixture.arrayOutputSteps.has(String(step.id ?? "").trim())
          && providerForSessionRequestStep(workflow, step) === "fixture"
        );
        if (unsupportedOutputStep !== undefined) {
          return {
            exitCode: 2,
            stderr: `Run fixture step ${String(unsupportedOutputStep.step.id).trim()}.outputs must be an object with materializable output values; array-form outputs are simulation-only.`
          };
        }
      }
      const serializedBindings = serializeAgentFlowProviderBindings(
        [...new Map(registeredWorkflows.flatMap((registeredWorkflow) =>
          providerBindingsForWorkflow(registeredWorkflow, catalog).map((binding) => [binding.alias, binding] as const)
        )).values()]
      );
      const initialContext = existingRun === null
        ? {
            ...(Object.keys(serializedBindings).length === 0 ? {} : { providerBindings: serializedBindings }),
            ...(runArgs.codexOptions === undefined ? {} : { codexOptions: runArgs.codexOptions }),
            workflowRegistry: workflowSnapshot as unknown as import("../runtime/index").AgentFlowRunStateValue
          }
        : existingRun.context.agentFlowInitialContext !== null
            && typeof existingRun.context.agentFlowInitialContext === "object"
            && !Array.isArray(existingRun.context.agentFlowInitialContext)
          ? existingRun.context.agentFlowInitialContext
          : {};
      const result = createAgentFlowLifecycleRun(store, {
        id: runArgs.runId,
        workflow: workflowResult!.workflow,
        inputs,
        context: initialContext,
        allowInterruptedRecovery: true
      });
      const providers = createAgentFlowConfiguredProviderRegistry(catalog, { env: options.env });
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
        notifications,
        workflows,
        fixture === null ? undefined : () => {
          if (usesFixtureProvider) {
            store!.updateRun(result.run.id, {
              context: {
                ...store!.getRun(result.run.id)!.context,
                cliFixturePath: path.resolve(options.cwd ?? process.cwd(), runArgs.fixturePath!)
              }
            });
          }
          if (result.run.status !== "pending") return;
          for (const [index, [artifactPath, value]] of Object.entries(fixture.artifacts)
            .sort(([left], [right]) => left.localeCompare(right)).entries()) {
            store!.writeArtifact({
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
      const resumeArgs = parsedResume as ParsedResumeLifecycleArgs;
      const run = requireRun(store, runId);
      const workflow = run.context.workflow;
      if (workflow === null || typeof workflow !== "object" || Array.isArray(workflow)) {
        throw new AgentFlowRunStateError(
          `Agent Flow run ${runId} does not contain its persisted workflow definition.`,
          "AGENT_FLOW_RESUME_STATE"
        );
      }
      const response = resumeArgs.responseKind === "outcome"
        ? { outcome: resumeArgs.responseValue }
        : resumeArgs.responseKind === "answer"
          ? { answer: parseCliAnswer(resumeArgs.responseValue) }
          : { resetSession: resumeArgs.responseValue };
      const persistedFixturePath = typeof run.context.cliFixturePath === "string"
        ? run.context.cliFixturePath
        : undefined;
      const fixturePath = resumeArgs.fixturePath ?? persistedFixturePath;
      const fixture = fixturePath === undefined ? null : readRunFixture(fixturePath, options.cwd);
      if (fixture !== null && "exitCode" in fixture) return fixture;
      const persistedWorkflow = workflow as unknown as import("../runtime/index").AgentFlowWorkflow;
      const workflows = reachableWorkflowRegistry(
        createAgentFlowWorkflowRegistryFromSnapshot(
          run.context.workflowRegistry ?? { [persistedWorkflow.name]: persistedWorkflow } as unknown as import("../runtime/index").AgentFlowRunStateValue
        ),
        persistedWorkflow.name
      );
      const registeredWorkflows = workflows.names().map((name) => workflows.get(name)!);
      if (fixture !== null) {
        const unsupportedOutputStep = registeredWorkflows.flatMap((registeredWorkflow) =>
          collectSessionRequestSteps(registeredWorkflow.steps).map((step) => ({ workflow: registeredWorkflow, step }))
        ).find(({ workflow: registeredWorkflow, step }) =>
          fixture.arrayOutputSteps.has(String(step.id ?? "").trim())
          && providerForSessionRequestStep(registeredWorkflow, step) === "fixture"
        );
        if (unsupportedOutputStep !== undefined) {
          return {
            exitCode: 2,
            stderr: `Run fixture step ${String(unsupportedOutputStep.step.id).trim()}.outputs must be an object with materializable output values; array-form outputs are simulation-only.`
          };
        }
      }
      const usesFixtureProvider = registeredWorkflows.some((registeredWorkflow) =>
        collectRequiredWorkflowProviders(registeredWorkflow).includes("fixture")
      );
      const pinnedOverrides = persistedProviderOverrides(run.context.providerBindings, runId);
      const catalog = loadAgentFlowProviderCatalog({
        cwd: options.cwd,
        env: options.env,
        homeDir: options.homeDir,
        ...(resumeArgs.configPath === undefined ? {} : { configPath: resumeArgs.configPath }),
        overrides: pinnedOverrides,
        aliases: pinnedOverrides.map((override) => override.slice(0, override.indexOf("=")))
      });
      const providers = createAgentFlowConfiguredProviderRegistry(catalog, { env: options.env });
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
        persistedWorkflow,
        response,
        undefined,
        providers,
        undefined,
        notifications,
        workflows
      );
      if (usesFixtureProvider && resumeArgs.fixturePath !== undefined && execution.status === "paused") {
        const resumedRun = requireRun(store, runId);
        store.updateRun(runId, {
          context: {
            ...resumedRun.context,
            cliFixturePath: path.resolve(options.cwd ?? process.cwd(), resumeArgs.fixturePath)
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

interface ParsedRunLifecycleArgs {
  workflowPath: string;
  runId: string;
  fixturePath?: string;
  configPath?: string;
  inputFilePath?: string;
  inputValues: string[];
  codexOptions?: {
    profile?: string;
    model?: string;
    reasoningEffort?: string;
  };
  providerOverrides: string[];
}

interface ParsedResumeLifecycleArgs {
  runId: string;
  responseKind: "outcome" | "answer" | "reset_session";
  responseValue: string;
  fixturePath?: string;
  configPath?: string;
}

function parseRunLifecycleArgs(args: string[]): ParsedRunLifecycleArgs | AgentFlowCliResult {
  if (!args[0]) return { exitCode: 1 };
  let runId: string | undefined;
  let fixturePath: string | undefined;
  let configPath: string | undefined;
  let inputFilePath: string | undefined;
  let profile: string | undefined;
  let model: string | undefined;
  let reasoningEffort: string | undefined;
  const inputValues: string[] = [];
  const providerOverrides: string[] = [];
  for (let index = 1; index < args.length; index += 2) {
    const flag = args[index];
    const value = args[index + 1];
    if (!value) return { exitCode: 1 };
    if (flag === "--id" && runId === undefined) runId = value;
    else if (flag === "--fixture" && fixturePath === undefined) fixturePath = value;
    else if (flag === "--config" && configPath === undefined) configPath = value;
    else if (flag === "--input-file" && inputFilePath === undefined) inputFilePath = value;
    else if (flag === "--input") inputValues.push(value);
    else if (flag === "--profile" && profile === undefined) profile = value;
    else if (flag === "--model" && model === undefined) model = value;
    else if (flag === "--reasoning-effort" && reasoningEffort === undefined) reasoningEffort = value;
    else if (flag === "--provider") providerOverrides.push(value);
    else return { exitCode: 1 };
  }
  if (runId === undefined) return { exitCode: 1 };
  if (profile !== undefined && !/^[A-Za-z0-9_-]+$/.test(profile)) return { exitCode: 1 };
  if (reasoningEffort !== undefined
      && !["minimal", "low", "medium", "high", "xhigh"].includes(reasoningEffort)) return { exitCode: 1 };
  return {
    workflowPath: args[0],
    runId,
    inputValues,
    providerOverrides,
    ...(fixturePath === undefined ? {} : { fixturePath }),
    ...(configPath === undefined ? {} : { configPath }),
    ...(inputFilePath === undefined ? {} : { inputFilePath }),
    ...(profile === undefined && model === undefined && reasoningEffort === undefined ? {} : {
      codexOptions: {
        ...(profile === undefined ? {} : { profile }),
        ...(model === undefined ? {} : { model }),
        ...(reasoningEffort === undefined ? {} : { reasoningEffort })
      }
    })
  };
}

function parseRunInputs(
  values: string[]
): { ok: true; inputs: Record<string, import("../runtime/index").AgentFlowRunStateValue> }
  | { ok: false; result: AgentFlowCliResult } {
  const inputs: Record<string, import("../runtime/index").AgentFlowRunStateValue> = {};
  for (const value of values) {
    const separator = value.indexOf("=");
    if (separator < 1) {
      return { ok: false, result: { exitCode: 2, stderr: `Invalid --input ${JSON.stringify(value)}; expected key=value.` } };
    }
    const key = value.slice(0, separator).trim();
    if (key.length === 0) return { ok: false, result: { exitCode: 2, stderr: "Agent Flow input names cannot be empty." } };
    if (Object.hasOwn(inputs, key)) {
      return { ok: false, result: { exitCode: 2, stderr: `Agent Flow input ${JSON.stringify(key)} was provided more than once with --input.` } };
    }
    const raw = value.slice(separator + 1);
    try {
      inputs[key] = JSON.parse(raw) as import("../runtime/index").AgentFlowRunStateValue;
    } catch {
      inputs[key] = raw;
    }
  }
  return { ok: true, inputs };
}

function readRunInputFile(
  inputFilePath: string,
  cwd?: string
): { ok: true; inputs: Record<string, import("../runtime/index").AgentFlowRunStateValue> }
  | { ok: false; result: AgentFlowCliResult } {
  const resolvedPath = cwd === undefined ? inputFilePath : path.resolve(cwd, inputFilePath);
  try {
    const parsed = JSON.parse(fs.readFileSync(resolvedPath, "utf8")) as unknown;
    if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
      return { ok: false, result: { exitCode: 2, stderr: `Agent Flow input file ${inputFilePath} must contain a JSON object.` } };
    }
    return { ok: true, inputs: parsed as Record<string, import("../runtime/index").AgentFlowRunStateValue> };
  } catch (error) {
    return { ok: false, result: { exitCode: 2, stderr: `Could not read Agent Flow input file ${inputFilePath}: ${error instanceof Error ? error.message : String(error)}` } };
  }
}

function validateRunInputs(
  workflow: import("../runtime/index").AgentFlowWorkflow,
  inputs: Record<string, import("../runtime/index").AgentFlowRunStateValue>
): string | undefined {
  const declared = new Set(Object.keys(workflow.inputs ?? {}));
  const unknown = Object.keys(inputs).filter((name) => !declared.has(name)).sort();
  if (unknown.length > 0) return `Agent Flow run has unknown inputs: ${unknown.join(", ")}.`;
  const missing = Object.entries(workflow.inputs ?? {}).flatMap(([name, definition]) =>
    definition !== null && typeof definition === "object" && !Array.isArray(definition)
      && definition.required === true && !Object.hasOwn(inputs, name) ? [name] : []
  ).sort();
  return missing.length === 0 ? undefined : `Agent Flow run is missing required inputs: ${missing.join(", ")}.`;
}

function parseResumeLifecycleArgs(args: string[]): ParsedResumeLifecycleArgs | AgentFlowCliResult {
  if (!args[0] || (args[1] !== "--outcome" && args[1] !== "--answer" && args[1] !== "--reset-session") || args[2] === undefined) return { exitCode: 1 };
  if ((args[1] === "--outcome" || args[1] === "--reset-session") && args[2].length === 0) return { exitCode: 1 };
  let fixturePath: string | undefined;
  let configPath: string | undefined;
  for (let index = 3; index < args.length; index += 2) {
    const flag = args[index];
    const value = args[index + 1];
    if (!value) return { exitCode: 1 };
    if (flag === "--fixture" && fixturePath === undefined) fixturePath = value;
    else if (flag === "--config" && configPath === undefined) configPath = value;
    else return { exitCode: 1 };
  }
  return {
    runId: args[0],
    responseKind: args[1] === "--outcome" ? "outcome" : args[1] === "--answer" ? "answer" : "reset_session",
    responseValue: args[2],
    ...(fixturePath === undefined ? {} : { fixturePath }),
    ...(configPath === undefined ? {} : { configPath })
  };
}

function persistedProviderOverrides(
  value: import("../runtime/index").AgentFlowRunStateValue | undefined,
  runId: string
): string[] {
  if (value === undefined) return [];
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new AgentFlowRunStateError(
      `Agent Flow run ${runId} has invalid pinned provider configuration.`,
      "AGENT_FLOW_PROVIDER_CONFIG_STATE"
    );
  }
  return Object.entries(value).sort(([left], [right]) => left.localeCompare(right)).map(([alias, binding]) => {
    if (binding === null || typeof binding !== "object" || Array.isArray(binding)
        || typeof binding.target !== "string") {
      throw new AgentFlowRunStateError(
        `Agent Flow run ${runId} has invalid pinned provider configuration for alias ${JSON.stringify(alias)}.`,
        "AGENT_FLOW_PROVIDER_CONFIG_STATE"
      );
    }
    return `${alias}=${binding.target}`;
  });
}

function reconcilePinnedProviderOverrides(
  pinned: string[],
  requested: string[],
  runId: string
): string[] {
  if (pinned.length === 0) return requested;
  if (requested.length === 0) return pinned;
  const pinnedTargets = new Map(pinned.map((override) => {
    const separator = override.indexOf("=");
    return [override.slice(0, separator), override.slice(separator + 1)] as const;
  }));
  const requestedAliases = new Set<string>();
  for (const override of requested) {
    const separator = override.indexOf("=");
    if (separator <= 0 || separator === override.length - 1) return [...pinned, ...requested];
    const alias = override.slice(0, separator);
    const target = override.slice(separator + 1);
    if (alias !== alias.trim() || target !== target.trim() || requestedAliases.has(alias)) {
      return [...pinned, ...requested];
    }
    requestedAliases.add(alias);
    if (pinnedTargets.get(alias) !== target) {
      throw new AgentFlowRunStateError(
        `Agent Flow run ${runId} cannot replace pinned provider ${JSON.stringify(alias)} with a different target. Start a new run ID to change providers.`,
        "AGENT_FLOW_PROVIDER_CONFIG_DRIFT"
      );
    }
  }
  return pinned;
}

function validLifecycleArgs(command: ActiveLifecycleCommand, args: string[]): boolean {
  if (command === "inject") {
    return args.length === 3 && args.every((entry) => entry.length > 0);
  }
  if (command === "cleanup") return parseCleanupArgs(args) !== null;
  if (command === "archive" || command === "export") return parsePortableArgs(command, args) !== null;
  return args.length === 1 && args[0].length > 0;
}

function lifecycleUsage(topic: string): string | null {
  if (topic === "run") return "Usage: agent-flow run <workflow> --id <run-id> [--fixture <file>] [--input <key=value>] [--input-file <json>] [--profile <name>] [--model <name>] [--reasoning-effort <level>]";
  if (topic === "resume") return "Usage: agent-flow resume <run-id> (--outcome <choice> | --answer <value> | --reset-session <session-name>) [--fixture <file>] [--config <file>]";
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

function simulateWorkflow(args: string[], options: AgentFlowCliOptions): AgentFlowCliResult {
  if (args.length !== 3 || args[1] !== "--fixture") {
    return { exitCode: 1, stderr: "Usage: agent-flow simulate <workflow> --fixture <file>" };
  }

  const [workflowPath, , fixturePath] = args;
  const workflowResult = readWorkflow(workflowPath, "simulate");
  if ("exitCode" in workflowResult) return workflowResult;
  const aliasResult = repositoryProviderAliases(options);
  if (!aliasResult.ok) return aliasResult.result;
  const aliases = aliasResult.aliases;
  const providerKind = (provider: string) => provider === "codex" ? "frontier"
    : Object.hasOwn(aliases, provider) ? aliases[provider]!.kind : undefined;
  const configuredValidation = validateAgentFlowWorkflow(
    workflowResult.workflow,
    providerKind
  );
  if (!configuredValidation.valid) {
    return { exitCode: 2, stderr: formatAgentFlowWorkflowIssues(configuredValidation.errors) };
  }

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

  const result = simulateAgentFlowWorkflow(
    workflowResult.workflow,
    fixture.fixture,
    undefined,
    providerKind
  );
  return {
    exitCode: result.status === "unresolved" ? 2 : 0,
    stdout: renderAgentFlowSimulationSummary(result)
  };
}

function checkWorkflow(
  command: "validate" | "lint" | "explain" | "graph",
  args: string[],
  options: AgentFlowCliOptions
): AgentFlowCliResult {
  const workflowPath = args[0];

  if (!workflowPath || args.length !== 1) {
    return { exitCode: 1, stderr: `Usage: agent-flow ${command} <workflow>` };
  }

  const workflowResult = readWorkflow(workflowPath, command);
  if ("exitCode" in workflowResult) return workflowResult;
  const workflow = workflowResult.workflow;
  const aliasResult = repositoryProviderAliases(options);
  if (!aliasResult.ok) return aliasResult.result;
  const aliases = aliasResult.aliases;
  const providerKind = (provider: string) => provider === "codex" ? "frontier"
    : Object.hasOwn(aliases, provider) ? aliases[provider]!.kind : undefined;
  const configuredValidation = validateAgentFlowWorkflow(
    workflow,
    providerKind
  );
  if (!configuredValidation.valid) {
    return {
      exitCode: 2,
      stderr: `Agent Flow ${command} failed: ${workflowPath}\n${formatAgentFlowWorkflowIssues(configuredValidation.errors)}`
    };
  }

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
    const warnings = lintAgentFlowWorkflow(workflow, providerKind).warnings;

    return warnings.length === 0
      ? { exitCode: 0, stdout: `Agent Flow validation passed: ${workflowPath}` }
      : {
          exitCode: 0,
          stdout: `Agent Flow validation passed with ${warnings.length} warning${warnings.length === 1 ? "" : "s"}: ${workflowPath}\n${formatAgentFlowWorkflowIssues(warnings)}`
        };
  }

  const lint = lintAgentFlowWorkflow(workflow, providerKind);

  if (lint.warnings.length === 0) {
    return { exitCode: 0, stdout: `Agent Flow lint passed with no warnings: ${workflowPath}` };
  }

  return {
    exitCode: 0,
    stdout: `Agent Flow lint found ${lint.warnings.length} warning${lint.warnings.length === 1 ? "" : "s"}: ${workflowPath}\n${formatAgentFlowWorkflowIssues(lint.warnings)}`
  };
}

function repositoryProviderAliases(
  options: AgentFlowCliOptions
): { ok: true; aliases: Readonly<Record<string, import("../runtime/index").AgentFlowProviderAlias>> }
  | { ok: false; result: AgentFlowCliResult } {
  try {
    return { ok: true, aliases: loadAgentFlowRepositoryProviderAliases({ cwd: options.cwd }) };
  } catch (error) {
    return { ok: false, result: { exitCode: 2, stderr: error instanceof Error ? error.message : String(error) } };
  }
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

function collectRequiredWorkflowProviders(
  workflow: import("../runtime/index").AgentFlowWorkflow,
  sessionRequestSteps = collectSessionRequestSteps(workflow.steps)
): string[] {
  const configuredSessionNames = [
    ...sessionRequestSteps.map((step) => {
      const type = typeof step.type === "string" ? step.type.trim() : "";
      return type === "review" || type === "approval" ? step.reviewer
        : type === "consult" || type === "challenge" ? step.to : step.session;
    })
      .filter((session): session is string => typeof session === "string"),
    ...collectDisagreementResolverSessions(workflow, sessionRequestSteps),
    ...collectRecoveryRouteSessions(workflow.steps)
    , ...collectWorkflowSteps(workflow.steps)
      .filter((step) => String(step.type ?? "").trim() === "mcp_call" && String(step.via ?? "direct").trim() === "codex")
      .flatMap((step) => typeof step.session === "string" ? [step.session] : [])
  ];
  return configuredSessionNames
    .map((session) => workflow.sessions?.[session.trim()])
    .flatMap((session) => session !== null && typeof session === "object" && !Array.isArray(session)
      ? [String((session as Record<string, unknown>).provider ?? "").trim()]
      : []);
}

function collectWorkflowSteps(
  steps: import("../runtime/index").AgentFlowWorkflowStep[]
): import("../runtime/index").AgentFlowWorkflowStep[] {
  const collected: import("../runtime/index").AgentFlowWorkflowStep[] = [];
  const visit = (step: import("../runtime/index").AgentFlowWorkflowStep): void => {
    collected.push(step);
    for (const field of ["body", "steps", "branches"] as const) {
      const nested = step[field];
      if (!Array.isArray(nested)) continue;
      for (const candidate of nested) {
        if (candidate !== null && typeof candidate === "object" && !Array.isArray(candidate)) {
          visit(candidate as import("../runtime/index").AgentFlowWorkflowStep);
        }
      }
    }
  };
  steps.forEach(visit);
  return collected;
}

function reachableWorkflowRegistry(
  available: import("../runtime/index").AgentFlowWorkflowRegistry,
  entryName: string
): import("../runtime/index").AgentFlowWorkflowRegistry {
  const reachable = createAgentFlowWorkflowRegistry();
  const visit = (name: string): void => {
    if (reachable.get(name) !== undefined) return;
    const workflow = available.get(name);
    if (workflow === undefined) throw new Error(`Referenced workflow ${name} is not registered.`);
    reachable.register(name, workflow);
    for (const referenced of referencedWorkflowNames(workflow.steps)) visit(referenced);
  };
  visit(entryName);
  return reachable;
}

function referencedWorkflowNames(
  steps: import("../runtime/index").AgentFlowWorkflowStep[]
): string[] {
  const names = new Set<string>();
  for (const step of collectWorkflowSteps(steps)) {
    if (String(step.type ?? "").trim() === "workflow" && typeof step.workflow === "string"
        && step.workflow.trim().length > 0) {
      names.add(step.workflow.trim());
    }
    const onFailure = step.on_failure;
    if (onFailure === null || typeof onFailure !== "object" || Array.isArray(onFailure)) continue;
    const route = onFailure.route_to;
    if (route === null || typeof route !== "object" || Array.isArray(route)) continue;
    if (typeof route.workflow === "string" && route.workflow.trim().length > 0) {
      names.add(route.workflow.trim());
    }
  }
  return [...names].sort();
}

function providerForSessionRequestStep(
  workflow: import("../runtime/index").AgentFlowWorkflow,
  step: import("../runtime/index").AgentFlowWorkflowStep
): string | undefined {
  const type = typeof step.type === "string" ? step.type.trim() : "";
  const sessionName = type === "review" || type === "approval" ? step.reviewer
    : type === "consult" || type === "challenge" ? step.to : step.session;
  if (typeof sessionName !== "string") return undefined;
  const session = workflow.sessions?.[sessionName.trim()];
  if (session === null || typeof session !== "object" || Array.isArray(session)) return undefined;
  const provider = (session as Record<string, unknown>).provider;
  return typeof provider === "string" ? provider.trim() : undefined;
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
    .filter((step) => typeof step.type === "string" && step.type.trim() === "review"
      && reviewCycleIds.has(String(step.id ?? "").trim()))
    .map((step) => String(step.subject ?? "").trim())
    .filter((session) => session.length > 0);
}

function collectRecoveryRouteSessions(
  steps: import("../runtime/index").AgentFlowWorkflowStep[]
): string[] {
  const sessions: string[] = [];
  const visit = (step: import("../runtime/index").AgentFlowWorkflowStep): void => {
    const onFailure = step.on_failure;
    if (onFailure !== null && typeof onFailure === "object" && !Array.isArray(onFailure)) {
      const route = (onFailure as Record<string, unknown>).route_to;
      if (route !== null && typeof route === "object" && !Array.isArray(route)) {
        const session = (route as Record<string, unknown>).session;
        if (typeof session === "string" && session.trim().length > 0) sessions.push(session.trim());
      }
    }
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
  return sessions;
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
