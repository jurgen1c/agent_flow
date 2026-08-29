import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import { spawn, type ChildProcess } from "node:child_process";
import { StringDecoder } from "node:string_decoder";

import {
  AgentFlowSessionRequestError,
  createAgentFlowSessionProviderRegistry,
  type AgentFlowSessionProviderAdapter,
  type AgentFlowSessionProviderRequest,
  type AgentFlowSessionProviderResponse,
  type AgentFlowSessionProviderRegistry
} from "./session_request";
import {
  agentFlowNativeProviderEnvironment,
  hashAgentFlowProviderModel,
  selectedCodexProviderEnvironment,
  type AgentFlowConfiguredTarget,
  type AgentFlowProviderCatalog,
  type AgentFlowResolvedProviderBinding
} from "./provider_config";
import {
  captureAgentFlowWorkspaceSnapshot,
  changedAgentFlowWorkspacePaths
} from "./workspace";
import { matchesPolicyGlob } from "./policy_utils";
import {
  markAgentFlowWorkspaceWriteLockManaged,
  withAgentFlowWorkspaceWriteLock
} from "./workspace_lock";
import type { AgentFlowRunStateValue } from "./run_state";

const MAX_PROVIDER_RESPONSE_BYTES = 12 * 1024 * 1024;
const NATIVE_SESSION_UNAVAILABLE = "AGENT_FLOW_PROVIDER_SESSION_UNAVAILABLE";

export interface CreateAgentFlowConfiguredProviderRegistryOptions {
  env?: Readonly<Record<string, string | undefined>>;
}

export function createAgentFlowConfiguredProviderRegistry(
  catalog: AgentFlowProviderCatalog,
  options: CreateAgentFlowConfiguredProviderRegistryOptions = {}
): AgentFlowSessionProviderRegistry {
  const registry = createAgentFlowSessionProviderRegistry();
  for (const binding of Object.values(catalog.bindings)) {
    registry.registerConfigured({
      name: binding.alias,
      kind: binding.kind,
      target: binding.target,
      driver: binding.config.driver,
      model: binding.config.model,
      ...(binding.config.profile === undefined ? {} : { profile: binding.config.profile }),
      ...(binding.config.reasoning_effort === undefined
        ? {}
        : { reasoningEffort: binding.config.reasoning_effort }),
      fingerprint: binding.fingerprint
    }, createAgentFlowConfiguredProviderAdapter(binding, options));
  }
  if (!registry.names().includes("codex")) {
    registry.register("codex", createAgentFlowCodexCliProvider(options));
  }
  return registry;
}

export function createAgentFlowCodexCliProvider(
  options: CreateAgentFlowConfiguredProviderRegistryOptions = {}
): AgentFlowSessionProviderAdapter {
  const binding: AgentFlowResolvedProviderBinding = {
    alias: "codex",
    target: "codex",
    kind: "frontier",
    fingerprint: "native-codex-config",
    config: { kind: "frontier", driver: "codex-cli", model: "", enabled: true }
  };
  const adapter: AgentFlowSessionProviderAdapter = (request) =>
    invokeCodexCli(binding, request, options.env ?? process.env);
  adapter.preflight = (request) => {
    if (request.repoRoot === undefined || !path.isAbsolute(request.repoRoot)) {
      throw providerError("Codex CLI requires an absolute repository root.");
    }
    assertUtf8Inputs(request);
  };
  adapter.waitForAbort = true;
  markAgentFlowWorkspaceWriteLockManaged(adapter);
  return adapter;
}

export function createAgentFlowConfiguredProviderAdapter(
  binding: AgentFlowResolvedProviderBinding,
  options: CreateAgentFlowConfiguredProviderRegistryOptions = {}
): AgentFlowSessionProviderAdapter {
  const env = options.env ?? process.env;
  let adapter: AgentFlowSessionProviderAdapter;
  if (binding.config.driver === "openai-responses") {
    adapter = (request) => invokeOpenAiResponses(binding, request, env);
  } else if (binding.config.driver === "anthropic-messages") {
    adapter = (request) => invokeAnthropicMessages(binding, request, env);
  } else if (binding.config.driver === "openai-compatible") {
    adapter = (request) => invokeOpenAiCompatible(binding, request, env);
  } else if (binding.config.driver === "codex-cli") {
    adapter = (request) => invokeCodexCli(binding, request, env);
  } else if (binding.config.driver === "claude-code") {
    adapter = (request) => invokeClaudeCode(binding, request, env);
  } else {
    throw providerError("Unsupported configured provider driver.");
  }
  adapter.preflight = (request) => {
    if (binding.config.driver === "codex-cli") {
      if (request.repoRoot === undefined || !path.isAbsolute(request.repoRoot)) {
        throw providerError(`Configured codex-cli target ${binding.alias} requires an absolute repository root.`);
      }
    } else if (binding.config.driver === "claude-code") {
      assertNativeAuthority(request, binding);
    } else {
      assertApiAuthority(request, binding);
      if (binding.config.api_key_env !== undefined) requiredCredential(binding.config, env);
    }
    assertUtf8Inputs(request);
  };
  if (binding.config.driver === "claude-code") {
    adapter.waitForAbort = true;
    markAgentFlowWorkspaceWriteLockManaged(adapter);
  } else if (binding.config.driver === "codex-cli") {
    adapter.waitForAbort = true;
    markAgentFlowWorkspaceWriteLockManaged(adapter);
  }
  return adapter;
}

async function invokeCodexCli(
  binding: AgentFlowResolvedProviderBinding,
  request: AgentFlowSessionProviderRequest,
  env: Readonly<Record<string, string | undefined>>
): Promise<AgentFlowSessionProviderResponse> {
  {
    const temporaryDirectory = fs.mkdtempSync(path.join(os.tmpdir(), "agent-flow-codex-"));
    const schemaPath = path.join(temporaryDirectory, "output-schema.json");
    const outputPath = path.join(temporaryDirectory, "output.json");
    fs.writeFileSync(schemaPath, `${JSON.stringify(outputSchema(request))}\n`, { mode: 0o600 });
    let observedThreadId: string | undefined;
    const observedMcpCalls: AgentFlowRunStateValue[] = [];
    const observedMcpResults: Array<string | undefined> = [];
    const reportThread = (candidate: unknown): void => {
      if (typeof candidate !== "string" || candidate.trim().length === 0) {
        throw providerError("Codex CLI emitted an invalid thread ID.");
      }
      const threadId = candidate.trim();
      if (request.externalSessionId !== undefined && threadId !== request.externalSessionId) {
        throw providerError("Codex CLI resumed a thread that does not match the persisted Agent Flow session.");
      }
      if (observedThreadId !== undefined && observedThreadId !== threadId) {
        throw providerError("Codex CLI emitted more than one thread ID for one invocation.");
      }
      observedThreadId = threadId;
      if (request.resume) request.reportExternalSessionId?.(threadId);
    };
    try {
      const profile = request.codexOptions?.profile ?? binding.config.profile;
      const model = request.codexOptions?.model ?? (binding.config.model || undefined);
      const reasoningEffort = request.codexOptions?.reasoningEffort ?? binding.config.reasoning_effort;
      const commonArguments = [
        "--json",
        "--output-schema", schemaPath,
        "--output-last-message", outputPath,
        ...(model === undefined ? [] : ["--model", model])
      ];
      const codexConfigurationArguments = [
        ...(profile === undefined ? [] : ["--profile", profile]),
        ...(reasoningEffort === undefined
          ? []
          : ["--config", `model_reasoning_effort=${JSON.stringify(reasoningEffort)}`])
      ];
      const arguments_ = request.externalSessionId === undefined
        ? [
            "exec",
            ...codexConfigurationArguments,
            ...commonArguments,
            ...(request.resume ? [] : ["--ephemeral"]),
            "-"
          ]
        : [
            "exec",
            ...codexConfigurationArguments,
            "resume",
            ...commonArguments,
            request.externalSessionId,
            "-"
          ];
      const result = await runNativeProviderProcess(
        "codex",
        arguments_,
        buildProviderPrompt(request),
        request.repoRoot!,
        env,
        request.signal,
        undefined,
        (line) => {
          let event: unknown;
          try { event = JSON.parse(line); } catch {
            throw providerError("Codex CLI emitted malformed JSONL output.");
          }
          if (isRecord(event) && event.type === "thread.started") reportThread(event.thread_id);
          if (request.captureMcpCallEvidence === true
              && isRecord(event) && event.type === "item.completed" && isRecord(event.item)
              && event.item.type === "mcp_tool_call") {
            const arguments_ = codexMcpArguments(event.item.arguments);
            const result = codexMcpResultContent(event.item.result);
            observedMcpResults.push(result);
            observedMcpCalls.push({
              ...(typeof event.item.server === "string" ? { server: event.item.server } : {}),
              ...(typeof event.item.tool === "string" ? { tool: event.item.tool } : {}),
              ...(arguments_ === undefined ? {} : { arguments: arguments_ }),
              status: typeof event.item.status === "string"
                ? event.item.status
                : event.item.error === undefined || event.item.error === null ? "completed" : "failed",
              ...(result === undefined ? {} : { resultHash: sha256(result) })
            });
          }
        }
      );
      if (observedThreadId === undefined) {
        for (const line of result.stdout.toString("utf8").split(/\r?\n/)) {
          if (line.length === 0) continue;
          let event: unknown;
          try { event = JSON.parse(line); } catch { continue; }
          if (isRecord(event) && event.type === "thread.started") reportThread(event.thread_id);
        }
      }
      assertNativeSuccess("Codex CLI", result, request.externalSessionId !== undefined);
      if (request.resume && observedThreadId === undefined) {
        throw providerError("Codex CLI completed without reporting a resumable thread ID.");
      }
      const structured = readBoundedNativeOutput(outputPath, "Codex CLI");
      const response = responseFromStructuredText(
        structured,
        request,
        binding,
        request.resume ? observedThreadId : undefined,
        {
          cli: "codex",
          ...(profile === undefined ? {} : { profile }),
          ...(reasoningEffort === undefined ? {} : { reasoningEffort }),
          ...(model === undefined ? {} : { modelHash: hashAgentFlowProviderModel(model) }),
          ...(observedMcpCalls.length === 0 ? {} : { mcpCalls: observedMcpCalls })
        }
      );
      if (request.captureMcpCallEvidence === true
          && request.outputs.length === 1
          && observedMcpCalls.length === 1
          && observedMcpResults[0] !== undefined) {
        response.outputs[request.outputs[0]!] = observedMcpResults[0]!;
      }
      return response;
    } finally {
      fs.rmSync(temporaryDirectory, { recursive: true, force: true });
    }
  }
}

async function invokeClaudeCode(
  binding: AgentFlowResolvedProviderBinding,
  request: AgentFlowSessionProviderRequest,
  env: Readonly<Record<string, string | undefined>>
): Promise<AgentFlowSessionProviderResponse> {
  assertNativeAuthority(request, binding);
  return auditNativeWorkspace(request, binding, async () => {
    const temporaryDirectory = fs.mkdtempSync(path.join(os.tmpdir(), "agent-flow-claude-"));
    try {
      const sessionId = request.externalSessionId ?? (request.resume ? randomUUID() : undefined);
      const displayName = `agent-flow-${safeCliName(request.runId)}-${safeCliName(request.sessionId)}`.slice(0, 120);
      const sandbox = nativeProcessSandbox(request, env, temporaryDirectory, "claude");
      const arguments_ = [
        "-p",
        "--output-format", "json",
        "--json-schema", JSON.stringify(outputSchema(request)),
        "--setting-sources", "",
        "--settings", JSON.stringify(claudeHardeningSettings(sandbox)),
        "--mcp-config", JSON.stringify({ mcpServers: {} }),
        "--strict-mcp-config",
        "--permission-mode", request.canModifyFiles === true ? "acceptEdits" : "plan",
        "--model", binding.config.model,
        ...(request.externalSessionId !== undefined
          ? ["--resume", request.externalSessionId]
          : sessionId === undefined
            ? ["--no-session-persistence"]
            : ["--session-id", sessionId, "--name", displayName])
      ];
      const result = await runNativeProviderProcess(
        "claude",
        arguments_,
        buildProviderPrompt(request),
        request.repoRoot!,
        env,
        request.signal,
        sandbox
      );
      assertNativeSuccess("Claude Code", result, request.externalSessionId !== undefined);
      const envelope = parseJsonObject(result.stdout.toString("utf8"), "Claude Code response");
      const returnedSessionId = optionalString(envelope.session_id);
      if (sessionId !== undefined && returnedSessionId !== sessionId) {
        throw providerError("Claude Code returned a session ID that does not match the persisted Agent Flow session.");
      }
      if (sessionId !== undefined && request.resume) request.reportExternalSessionId?.(sessionId);
      if (!isRecord(envelope.structured_output)) {
        throw providerError("Claude Code response did not contain validated structured_output.");
      }
      return responseFromStructuredText(
        JSON.stringify(envelope.structured_output),
        request,
        binding,
        request.resume ? sessionId : undefined,
        { cli: "claude" }
      );
    } finally {
      fs.rmSync(temporaryDirectory, { recursive: true, force: true });
    }
  });
}

interface NativeProviderProcessResult {
  exitCode: number | null;
  signal: NodeJS.Signals | null;
  stdout: Buffer;
  stderr: Buffer;
}

interface NativeProcessSandbox {
  repoRoot: string;
  canModifyFiles: boolean;
  temporaryDirectory: string;
  providerStatePaths: string[];
  providerStateMaskPaths: string[];
  providerStateEnvironment?: { name: "CODEX_HOME" | "CLAUDE_CONFIG_DIR"; value: string };
  providerCredentialEnvironment: NodeJS.ProcessEnv;
  includeOpenAiCredentials: boolean;
  certificatePaths: string[];
  certificateEnvironment: Record<string, string>;
  gitMetadataPaths: string[];
  executablePath: string;
  executableMountPaths: string[];
}

function claudeHardeningSettings(sandbox: NativeProcessSandbox): Record<string, unknown> {
  const permissionPaths = sandbox.providerStatePaths
    .map((statePath) => `//${statePath.replace(/^\/+/, "")}`);
  return {
    permissions: {
      deny: permissionPaths.flatMap((permissionPath) => [
        `Read(${permissionPath})`, `Read(${permissionPath}/**)`,
        `Edit(${permissionPath})`, `Edit(${permissionPath}/**)`
      ])
    },
    sandbox: {
      enabled: true,
      failIfUnavailable: true,
      allowUnsandboxedCommands: false,
      excludedCommands: [],
      filesystem: {
        denyRead: sandbox.providerStatePaths,
        denyWrite: sandbox.providerStatePaths
      }
    }
  };
}

function runNativeProviderProcess(
  command: "codex" | "claude",
  arguments_: string[],
  input: string,
  cwd: string,
  env: Readonly<Record<string, string | undefined>>,
  signal: AbortSignal,
  sandbox?: NativeProcessSandbox,
  onStdoutLine?: (line: string) => void
): Promise<NativeProviderProcessResult> {
  return new Promise((resolve, reject) => {
    const stdout: Buffer[] = [];
    const stderr: Buffer[] = [];
    let stdoutBytes = 0;
    let stderrBytes = 0;
    let lineBuffer = "";
    const stdoutLineDecoder = onStdoutLine === undefined ? undefined : new StringDecoder("utf8");
    let settled = false;
    let terminationTimer: ReturnType<typeof setTimeout> | undefined;
    let failure: Error | undefined;
    let child: ChildProcess;
    try {
      const invocation = sandbox === undefined
        ? { command: resolveNativeExecutable(command, env.PATH), arguments: arguments_ }
        : sandboxedNativeInvocation(command, arguments_, sandbox);
      child = spawn(invocation.command, invocation.arguments, {
        cwd,
        shell: false,
        detached: process.platform !== "win32",
        env: sandbox === undefined ? definedProviderEnvironment(env) : {
          ...agentFlowNativeProviderEnvironment(command, env, sandbox.includeOpenAiCredentials),
          ...(sandbox.providerStateEnvironment === undefined
            ? {}
            : { [sandbox.providerStateEnvironment.name]: sandbox.providerStateEnvironment.value }),
          ...sandbox.providerCredentialEnvironment,
          ...sandbox.certificateEnvironment,
          TMPDIR: sandbox.temporaryDirectory
        },
        stdio: ["pipe", "pipe", "pipe"]
      });
    } catch (error) {
      reject(providerError(`Could not start ${command}: ${error instanceof Error ? error.message : String(error)}.`));
      return;
    }
    const terminate = (error: Error): void => {
      if (failure !== undefined) return;
      failure = error;
      terminateNativeChild(child.pid, "SIGTERM");
      terminationTimer = setTimeout(() => terminateNativeChild(child.pid, "SIGKILL"), 250);
    };
    const capture = (target: Buffer[], chunk: Buffer | string, stream: "stdout" | "stderr"): void => {
      const content = Buffer.from(chunk);
      const current = stream === "stdout" ? stdoutBytes : stderrBytes;
      const next = current + content.byteLength;
      if (stream === "stdout") stdoutBytes = next; else stderrBytes = next;
      if (next > MAX_PROVIDER_RESPONSE_BYTES) {
        terminate(providerError(`${command} ${stream} exceeded the ${MAX_PROVIDER_RESPONSE_BYTES}-byte limit.`));
        return;
      }
      target.push(content);
      if (stream === "stdout" && onStdoutLine !== undefined && stdoutLineDecoder !== undefined) {
        lineBuffer += stdoutLineDecoder.write(content);
        const lines = lineBuffer.split(/\r?\n/);
        lineBuffer = lines.pop() ?? "";
        try {
          for (const line of lines) if (line.length > 0) onStdoutLine(line);
        } catch (error) {
          terminate(error instanceof Error ? error : providerError(String(error)));
        }
      }
    };
    child.stdout?.on("data", (chunk: Buffer | string) => capture(stdout, chunk, "stdout"));
    child.stderr?.on("data", (chunk: Buffer | string) => capture(stderr, chunk, "stderr"));
    const abort = (): void => terminate(
      signal.reason instanceof Error ? signal.reason : providerError(`${command} invocation was aborted.`)
    );
    signal.addEventListener("abort", abort, { once: true });
    const finish = (exitCode: number | null, childSignal: NodeJS.Signals | null): void => {
      if (settled) return;
      settled = true;
      signal.removeEventListener("abort", abort);
      if (terminationTimer !== undefined) clearTimeout(terminationTimer);
      if (failure !== undefined) {
        reject(failure);
        return;
      }
      if (onStdoutLine !== undefined && stdoutLineDecoder !== undefined) {
        lineBuffer += stdoutLineDecoder.end();
      }
      if (onStdoutLine !== undefined && lineBuffer.length > 0) {
        try { onStdoutLine(lineBuffer); } catch (error) {
          reject(error);
          return;
        }
      }
      resolve({ exitCode, signal: childSignal, stdout: Buffer.concat(stdout), stderr: Buffer.concat(stderr) });
    };
    child.on("error", (error) => terminate(providerError(`Could not start ${command}: ${error.message}.`)));
    child.on("close", finish);
    child.stdin?.on("error", (error) => terminate(providerError(`Could not write ${command} prompt: ${error.message}.`)));
    child.stdin?.end(input);
    if (signal.aborted) abort();
  });
}

function definedProviderEnvironment(
  env: Readonly<Record<string, string | undefined>>
): NodeJS.ProcessEnv {
  return {
    HOME: env.HOME ?? os.homedir(),
    ...Object.fromEntries(Object.entries(env).filter((entry): entry is [string, string] => entry[1] !== undefined))
  };
}

function nativeProcessSandbox(
  request: AgentFlowSessionProviderRequest,
  env: Readonly<Record<string, string | undefined>>,
  temporaryDirectory: string,
  command: "codex" | "claude",
  resolvedCodexHome?: string,
  codexProviderEnvironmentNames: readonly string[] = [],
  codexRequiresOpenAiAuth = true
): NativeProcessSandbox {
  const home = env.HOME ?? os.homedir();
  const configuredState = command === "codex"
    ? resolvedCodexHome ?? env.CODEX_HOME ?? path.join(home, ".codex")
    : env.CLAUDE_CONFIG_DIR ?? path.join(home, ".claude");
  const repoRoot = fs.realpathSync(request.repoRoot!);
  const executablePath = resolveNativeExecutable(command, env.PATH);
  const statePath = nativeStateDirectory(configuredState, repoRoot, command);
  const providerStateMaskPaths = command === "codex"
    ? [nativeCodexSkillsDirectory(statePath)]
    : [];
  const defaultClaudeStateFile = command === "claude" && env.CLAUDE_CONFIG_DIR === undefined
    ? nativeStateFile(path.join(home, ".claude.json"), repoRoot)
    : undefined;
  const certificates = nativeCertificateEnvironment(env, repoRoot);
  return {
    repoRoot,
    canModifyFiles: request.canModifyFiles === true,
    temporaryDirectory: fs.realpathSync(temporaryDirectory),
    providerStatePaths: [statePath, ...(defaultClaudeStateFile === undefined ? [] : [defaultClaudeStateFile])],
    providerStateMaskPaths,
    providerStateEnvironment: command === "codex" || env.CLAUDE_CONFIG_DIR !== undefined
      ? {
          name: command === "codex" ? "CODEX_HOME" as const : "CLAUDE_CONFIG_DIR" as const,
          value: statePath
        }
      : undefined,
    providerCredentialEnvironment: command === "codex"
      ? selectedCodexProviderEnvironment(env, codexProviderEnvironmentNames)
      : {},
    includeOpenAiCredentials: command !== "codex" || codexRequiresOpenAiAuth,
    certificatePaths: certificates.paths,
    certificateEnvironment: certificates.environment,
    gitMetadataPaths: nativeGitMetadataPaths(repoRoot),
    executablePath,
    executableMountPaths: nativeExecutableMountPaths(command, executablePath, env.PATH, home)
  };
}

function nativeCodexSkillsDirectory(statePath: string): string {
  const skillsPath = path.join(statePath, "skills");
  try {
    if (!fs.existsSync(skillsPath)) fs.mkdirSync(skillsPath, { mode: 0o700 });
    const stat = fs.lstatSync(skillsPath);
    if (!stat.isDirectory() || stat.isSymbolicLink()) {
      throw providerError("Codex skills state must be a regular non-symlink directory.");
    }
    return fs.realpathSync(skillsPath);
  } catch (error) {
    if (error instanceof AgentFlowSessionRequestError) throw error;
    throw providerError(
      `Could not prepare Codex skills state: ${error instanceof Error ? error.message : String(error)}`
    );
  }
}

function nativeStateFile(candidate: string, repoRoot: string): string {
  if (!path.isAbsolute(candidate) || /[\u0000-\u001F\u007F-\u009F]/u.test(candidate)) {
    throw providerError("Claude Code default state file must have a canonical absolute path.");
  }
  try {
    if (!fs.existsSync(candidate)) fs.writeFileSync(candidate, "{}\n", { mode: 0o600, flag: "wx" });
    const stat = fs.lstatSync(candidate);
    if (!stat.isFile() || stat.isSymbolicLink()) {
      throw providerError("Claude Code default state file must be a regular non-symlink file.");
    }
    const resolved = fs.realpathSync(candidate);
    if (pathsOverlap(resolved, repoRoot) || resolved === path.parse(resolved).root) {
      throw providerError("Claude Code default state file must be separate from the repository.");
    }
    return resolved;
  } catch (error) {
    if (error instanceof AgentFlowSessionRequestError) throw error;
    throw providerError(
      `Could not prepare Claude Code default state file ${candidate}: ${error instanceof Error ? error.message : String(error)}`
    );
  }
}

function nativeStateDirectory(configuredState: string, repoRoot: string, command: "codex" | "claude"): string {
  const environmentName = command === "codex" ? "CODEX_HOME" : "CLAUDE_CONFIG_DIR";
  if (!configuredState || configuredState !== configuredState.trim()
      || /[\u0000-\u001F\u007F-\u009F]/u.test(configuredState)) {
    throw providerError(`${environmentName} must contain a canonical filesystem path.`);
  }
  const candidate = path.isAbsolute(configuredState)
    ? path.normalize(configuredState)
    : path.resolve(repoRoot, configuredState);
  try {
    let existingAncestor = candidate;
    while (!fs.existsSync(existingAncestor)) {
      const parent = path.dirname(existingAncestor);
      if (parent === existingAncestor) break;
      existingAncestor = parent;
    }
    const resolvedAncestor = fs.realpathSync(existingAncestor);
    const prospective = path.resolve(resolvedAncestor, path.relative(existingAncestor, candidate));
    if (pathsOverlap(prospective, repoRoot) || prospective === path.parse(prospective).root) {
      throw providerError(
        `Native CLI state directory ${prospective} must be separate from the repository and cannot be a filesystem root.`
      );
    }
    if (!fs.existsSync(candidate)) fs.mkdirSync(candidate, { recursive: true, mode: 0o700 });
    const resolved = fs.realpathSync(candidate);
    let stateStat = fs.lstatSync(resolved);
    if (!stateStat.isDirectory()) {
      throw providerError(`${environmentName} must resolve to a directory.`);
    }
    if (pathsOverlap(resolved, repoRoot) || resolved === path.parse(resolved).root) {
      throw providerError(
        `Native CLI state directory ${resolved} must be separate from the repository and cannot be a filesystem root.`
      );
    }
    if ((stateStat.mode & 0o7777) !== 0o700) {
      fs.chmodSync(resolved, 0o700);
      stateStat = fs.lstatSync(resolved);
    }
    if (!stateStat.isDirectory() || (stateStat.mode & 0o7777) !== 0o700) {
      throw providerError(`${environmentName} must resolve to an owner-only directory.`);
    }
    return resolved;
  } catch (error) {
    if (error instanceof AgentFlowSessionRequestError) throw error;
    throw providerError(
      `Could not prepare ${environmentName} at ${candidate}: ${error instanceof Error ? error.message : String(error)}`
    );
  }
}

function nativeCertificateEnvironment(
  env: Readonly<Record<string, string | undefined>>,
  repoRoot: string
): { paths: string[]; environment: Record<string, string> } {
  const paths: string[] = [];
  const environment: Record<string, string> = {};
  for (const name of ["NODE_EXTRA_CA_CERTS", "SSL_CERT_FILE"] as const) {
    const value = env[name];
    if (value === undefined) continue;
    const resolved = nativeCertificatePath(value, repoRoot, false, name);
    paths.push(resolved);
    environment[name] = resolved;
  }
  if (env.SSL_CERT_DIR !== undefined) {
    const directories = env.SSL_CERT_DIR.split(path.delimiter).filter(Boolean)
      .map((value) => nativeCertificatePath(value, repoRoot, true, "SSL_CERT_DIR"));
    if (directories.length === 0) throw providerError("SSL_CERT_DIR must name at least one certificate directory.");
    paths.push(...directories);
    environment.SSL_CERT_DIR = directories.join(path.delimiter);
  }
  return {
    paths: paths.filter((candidate, index, values) => values.indexOf(candidate) === index),
    environment
  };
}

function nativeCertificatePath(value: string, repoRoot: string, directory: boolean, name: string): string {
  if (!value || value !== value.trim() || /[\u0000-\u001F\u007F-\u009F]/u.test(value)) {
    throw providerError(`${name} must contain canonical filesystem paths.`);
  }
  try {
    const resolved = fs.realpathSync(path.isAbsolute(value) ? value : path.resolve(repoRoot, value));
    const stat = fs.lstatSync(resolved);
    if (resolved === path.parse(resolved).root || (directory ? !stat.isDirectory() : !stat.isFile())) {
      throw providerError(`${name} must resolve to ${directory ? "a non-root directory" : "a regular file"}.`);
    }
    return resolved;
  } catch (error) {
    if (error instanceof AgentFlowSessionRequestError) throw error;
    throw providerError(`Could not resolve ${name} certificate path.`);
  }
}

function nativeGitMetadataPaths(repoRoot: string): string[] {
  const markerPath = path.join(repoRoot, ".git");
  let marker: fs.Stats;
  try { marker = fs.lstatSync(markerPath); } catch { return []; }
  if (marker.isDirectory()) return [];

  let gitDirectory: string;
  if (marker.isSymbolicLink()) {
    gitDirectory = nativeGitMetadataDirectory(fs.realpathSync(markerPath));
  } else if (marker.isFile() && marker.size <= 8 * 1024) {
    const match = /^gitdir:\s*(.+)\s*$/i.exec(fs.readFileSync(markerPath, "utf8"));
    if (match === null) throw providerError("Repository .git file does not contain a valid Git directory reference.");
    const referenced = match[1]!;
    gitDirectory = nativeGitMetadataDirectory(path.isAbsolute(referenced)
      ? referenced
      : path.resolve(path.dirname(markerPath), referenced));
  } else {
    throw providerError("Repository .git entry is not a supported Git directory reference.");
  }

  const paths = [gitDirectory];
  const commonMarker = path.join(gitDirectory, "commondir");
  if (fs.existsSync(commonMarker)) {
    const referenced = fs.readFileSync(commonMarker, "utf8").trim();
    if (!referenced) throw providerError("Repository Git common-directory reference is empty.");
    paths.push(nativeGitMetadataDirectory(path.isAbsolute(referenced)
      ? referenced
      : path.resolve(gitDirectory, referenced)));
  }
  return paths.filter((candidate, index, values) => values.indexOf(candidate) === index);
}

function nativeGitMetadataDirectory(candidate: string): string {
  const resolved = fs.realpathSync(candidate);
  if (resolved === path.parse(resolved).root || !fs.lstatSync(resolved).isDirectory()) {
    throw providerError("Repository Git metadata must resolve to a non-root directory.");
  }
  return resolved;
}

function resolveNativeExecutable(command: string, pathValue: string | undefined): string {
  for (const directory of (pathValue ?? "/usr/local/bin:/usr/bin:/bin").split(path.delimiter)) {
    if (!directory) continue;
    const candidate = path.resolve(directory, command);
    try {
      fs.accessSync(candidate, fs.constants.X_OK);
      return fs.realpathSync(candidate);
    } catch {
      // Continue through the explicitly supplied PATH.
    }
  }
  throw providerError(`Could not resolve the ${command} executable from PATH.`);
}

export function nativeExecutableMountPaths(
  command: string,
  executablePath: string,
  pathValue: string | undefined,
  home: string
): string[] {
  const paths: string[] = [];
  if (executablePath !== "/usr" && !executablePath.startsWith("/usr/")) {
    paths.push(command === "codex" && path.basename(executablePath) === "codex.js"
      ? codexLauncherMountPath(executablePath)
      : executablePath);
  }
  let firstLine = "";
  try { firstLine = fs.readFileSync(executablePath, "utf8").split(/\r?\n/, 1)[0] ?? ""; } catch {
    return paths;
  }
  const interpreter = /^#!.*\/env(?:\s+-S)?\s+([^\s]+)/.exec(firstLine)?.[1];
  if (interpreter === undefined) return paths;
  const interpreterPath = resolveNativeExecutable(interpreter, pathValue);
  if (interpreterPath === "/usr" || interpreterPath.startsWith("/usr/")) return paths;
  const homeToolRoot = /^(.*\/(?:\.asdf|\.bun|\.nvm|\.rbenv|\.pyenv))(?:\/|$)/.exec(interpreterPath)?.[1];
  paths.push(homeToolRoot ?? interpreterPath);
  if (homeToolRoot?.endsWith("/.asdf")) {
    const asdfPath = optionalNativeExecutable("asdf", pathValue);
    if (asdfPath !== undefined && asdfPath !== "/usr" && !asdfPath.startsWith("/usr/")) paths.push(asdfPath);
    const toolVersionsPath = path.join(home, ".tool-versions");
    if (fs.existsSync(toolVersionsPath)) paths.push(toolVersionsPath);
  }
  return paths.filter((candidate, index, values) => values.indexOf(candidate) === index);
}

function optionalNativeExecutable(command: string, pathValue: string | undefined): string | undefined {
  try { return resolveNativeExecutable(command, pathValue); } catch (error) {
    if (error instanceof AgentFlowSessionRequestError && error.code === "AGENT_FLOW_CONFIGURED_PROVIDER") {
      return undefined;
    }
    throw error;
  }
}

function codexLauncherMountPath(executablePath: string): string {
  const packageRoot = path.dirname(path.dirname(executablePath));
  try {
    const packageJson = JSON.parse(fs.readFileSync(path.join(packageRoot, "package.json"), "utf8")) as unknown;
    if (isRecord(packageJson) && packageJson.name === "@openai/codex") {
      // The launcher resolves the native executable from a sibling optional
      // package such as @openai/codex-linux-x64.
      return path.dirname(packageRoot);
    }
  } catch {
    // A custom launcher only needs its own package tree.
  }
  return packageRoot;
}

function pathsOverlap(left: string, right: string): boolean {
  return left === right
    || left.startsWith(`${right}${path.sep}`)
    || right.startsWith(`${left}${path.sep}`);
}

function sandboxedNativeInvocation(
  command: string,
  arguments_: string[],
  sandbox: NativeProcessSandbox
): { command: string; arguments: string[] } {
  if (process.platform === "linux" && fs.existsSync("/usr/bin/bwrap")) {
    const repoRoot = path.resolve(sandbox.repoRoot);
    const gitPath = path.join(repoRoot, ".git");
    const runtimePath = path.join(repoRoot, ".agent-flow");
    const projectCodexPath = command === "codex" ? path.join(repoRoot, ".codex") : undefined;
    const writablePaths = [sandbox.temporaryDirectory, ...sandbox.providerStatePaths]
      .filter((candidate, index, values) => values.indexOf(candidate) === index);
    const mountedPaths = [
      repoRoot,
      sandbox.temporaryDirectory,
      ...sandbox.providerStatePaths,
      ...sandbox.providerStateMaskPaths,
      ...sandbox.certificatePaths,
      ...sandbox.gitMetadataPaths,
      ...sandbox.executableMountPaths
    ];
    const directoryArguments = sandboxParentDirectories(mountedPaths)
      .flatMap((candidate) => ["--dir", candidate]);
    return {
      command: "/usr/bin/bwrap",
      arguments: [
        "--die-with-parent",
        "--new-session",
        "--unshare-pid",
        "--ro-bind", "/usr", "/usr",
        "--symlink", "usr/bin", "/bin",
        "--symlink", "usr/lib", "/lib",
        ...(fs.existsSync("/usr/lib64") ? ["--symlink", "usr/lib64", "/lib64"] : []),
        ...directoryArguments,
        ...nativeSystemFileArguments(),
        "--proc", "/proc",
        "--dev", "/dev",
        sandbox.canModifyFiles ? "--bind" : "--ro-bind", repoRoot, repoRoot,
        ...(fs.existsSync(gitPath) ? ["--ro-bind", gitPath, gitPath] : []),
        ...sandbox.gitMetadataPaths.flatMap((candidate) => ["--ro-bind", candidate, candidate]),
        ...sandbox.certificatePaths.flatMap((candidate) => ["--ro-bind", candidate, candidate]),
        ...(fs.existsSync(runtimePath) ? ["--tmpfs", runtimePath, "--chmod", "000", runtimePath] : []),
        ...(projectCodexPath !== undefined && fs.existsSync(projectCodexPath)
          ? ["--tmpfs", projectCodexPath, "--chmod", "555", projectCodexPath]
          : []),
        ...sandbox.executableMountPaths.flatMap((candidate) => ["--ro-bind", candidate, candidate]),
        ...writablePaths.flatMap((candidate) => ["--bind", candidate, candidate]),
        ...sandbox.providerStateMaskPaths.flatMap((candidate) => ["--tmpfs", candidate]),
        "--chdir", repoRoot,
        "--",
        sandbox.executablePath,
        ...arguments_
      ]
    };
  }
  throw providerError(
    "Native CLI providers require a supported host filesystem sandbox; install bubblewrap on Linux."
  );
}

function sandboxParentDirectories(paths: readonly string[]): string[] {
  const directories = new Set<string>(["/etc", "/tmp"]);
  for (const candidate of paths) {
    let parent = path.dirname(candidate);
    while (parent !== path.parse(parent).root) {
      if (parent !== "/usr" && !parent.startsWith("/usr/")) directories.add(parent);
      parent = path.dirname(parent);
    }
  }
  return [...directories].sort((left, right) =>
    left.split(path.sep).length - right.split(path.sep).length || left.localeCompare(right)
  );
}

function nativeSystemFileArguments(): string[] {
  const candidates = [
    "/etc/ca-certificates",
    "/etc/gai.conf",
    "/etc/gitconfig",
    "/etc/group",
    "/etc/hosts",
    "/etc/hostname",
    "/etc/ld.so.cache",
    "/etc/ld.so.conf",
    "/etc/ld.so.conf.d",
    "/etc/localtime",
    "/etc/nsswitch.conf",
    "/etc/os-release",
    "/etc/passwd",
    "/etc/pki",
    "/etc/resolv.conf",
    "/etc/ssl"
  ];
  return candidates
    .filter((candidate) => fs.existsSync(candidate))
    .flatMap((candidate) => ["--ro-bind", candidate, candidate]);
}

function terminateNativeChild(pid: number | undefined, signal: NodeJS.Signals): void {
  if (pid === undefined) return;
  try { process.kill(process.platform === "win32" ? pid : -pid, signal); } catch (error) {
    if (!["ESRCH", "EPERM"].includes((error as NodeJS.ErrnoException).code ?? "")) throw error;
  }
}

async function auditNativeWorkspace(
  request: AgentFlowSessionProviderRequest,
  binding: AgentFlowResolvedProviderBinding,
  invoke: () => Promise<AgentFlowSessionProviderResponse>
): Promise<AgentFlowSessionProviderResponse> {
  try {
    return await withAgentFlowWorkspaceWriteLock(request.repoRoot!, request.signal, async () => {
      const before = captureAgentFlowWorkspaceSnapshot(request.repoRoot!);
      let response: AgentFlowSessionProviderResponse | undefined;
      let invocationError: unknown;
      try { response = await invoke(); } catch (error) { invocationError = error; }
      const changedPaths = changedAgentFlowWorkspacePaths(before, captureAgentFlowWorkspaceSnapshot(request.repoRoot!));
      const deniedPaths = changedPaths.filter((candidate) => !nativeWriteAllowed(request, candidate));
      if (deniedPaths.length > 0) {
        const displayed = deniedPaths.slice(0, 20);
        const suffix = deniedPaths.length > displayed.length ? ` (and ${deniedPaths.length - displayed.length} more)` : "";
        throw providerError(
          `${binding.config.driver} target ${binding.alias} changed files outside its authorized scope: ${displayed.join(", ")}${suffix}.`
        );
      }
      if (invocationError !== undefined) throw invocationError;
      return response!;
    }, { required: true });
  } catch (error) {
    if (error instanceof AgentFlowSessionRequestError) throw error;
    if (request.signal.aborted && request.signal.reason instanceof Error) throw request.signal.reason;
    throw providerError(
      `Could not secure the native provider workspace: ${error instanceof Error ? error.message : String(error)}`
    );
  }
}

function nativeWriteAllowed(request: AgentFlowSessionProviderRequest, candidate: string): boolean {
  if (request.canModifyFiles !== true) return false;
  const layers = request.fileScope?.layers ?? [];
  if (!layers.some((layer) => layer.include.length > 0)) return false;
  return layers.every((layer) =>
    (layer.include.length === 0 || layer.include.some((pattern) => matchesPolicyGlob(candidate, pattern)))
      && !layer.exclude.some((pattern) => matchesPolicyGlob(candidate, pattern))
  );
}

function assertNativeAuthority(
  request: AgentFlowSessionProviderRequest,
  binding: AgentFlowResolvedProviderBinding
): void {
  if (request.repoRoot === undefined || !path.isAbsolute(request.repoRoot)) {
    throw providerError(`Configured ${binding.config.driver} target ${binding.alias} requires an absolute repository root.`);
  }
  if (request.canModifyFiles === true
      && !(request.fileScope?.layers ?? []).some((layer) => layer.include.length > 0)) {
    throw providerError(
      `Configured ${binding.config.driver} target ${binding.alias} requires a non-empty file scope before it can modify repository files.`
    );
  }
}

function assertNativeSuccess(label: string, result: NativeProviderProcessResult, resuming: boolean): void {
  if (result.exitCode === 0 && result.signal === null) return;
  const detail = boundedNativeError(result.stderr);
  const unavailable = resuming && /(?:session|thread|conversation|rollout).{0,80}(?:not found|missing|unknown|does not exist|unavailable)|no (?:conversation|rollout) found/i.test(detail);
  throw providerError(
    `${label} exited ${result.signal === null ? `with status ${String(result.exitCode)}` : `on signal ${result.signal}`}${detail ? `: ${detail}` : "."}`,
    unavailable ? NATIVE_SESSION_UNAVAILABLE : undefined
  );
}

function boundedNativeError(content: Buffer): string {
  const value = content.toString("utf8").trim().replace(/\s+/g, " ");
  return value.slice(0, 2_000);
}

function readBoundedNativeOutput(outputPath: string, label: string): string {
  let stat: fs.Stats;
  try { stat = fs.lstatSync(outputPath); } catch {
    throw providerError(`${label} did not write its structured final output.`);
  }
  if (!stat.isFile() || stat.isSymbolicLink() || stat.size > MAX_PROVIDER_RESPONSE_BYTES) {
    throw providerError(`${label} structured final output is invalid or oversized.`);
  }
  return fs.readFileSync(outputPath, "utf8");
}

function safeCliName(value: string): string {
  return value.replace(/[^A-Za-z0-9._-]+/g, "-").replace(/^-+|-+$/g, "") || "session";
}

async function invokeOpenAiResponses(
  binding: AgentFlowResolvedProviderBinding,
  request: AgentFlowSessionProviderRequest,
  env: Readonly<Record<string, string | undefined>>
): Promise<AgentFlowSessionProviderResponse> {
  assertApiAuthority(request, binding);
  const apiKey = requiredCredential(binding.config, env);
  const body = await postJson(
    "https://api.openai.com/v1/responses",
    {
      model: binding.config.model,
      input: buildProviderPrompt(request),
      store: false,
      text: { format: { type: "json_schema", name: "agent_flow_outputs", strict: true, schema: outputSchema(request) } }
    },
    { Authorization: `Bearer ${apiKey}` },
    request.signal
  );
  assertProviderCompletion(body.status, ["completed"], "OpenAI Responses");
  const responseId = optionalString(body.id);
  const content = openAiResponseText(body);
  return responseFromStructuredText(content, request, binding, undefined, responseId === undefined ? undefined : { responseId });
}

async function invokeAnthropicMessages(
  binding: AgentFlowResolvedProviderBinding,
  request: AgentFlowSessionProviderRequest,
  env: Readonly<Record<string, string | undefined>>
): Promise<AgentFlowSessionProviderResponse> {
  assertApiAuthority(request, binding);
  const apiKey = requiredCredential(binding.config, env);
  const body = await postJson(
    "https://api.anthropic.com/v1/messages",
    {
      model: binding.config.model,
      max_tokens: binding.config.max_output_tokens ?? 4096,
      messages: [{ role: "user", content: buildProviderPrompt(request) }],
      output_config: { format: { type: "json_schema", schema: outputSchema(request) } }
    },
    { "x-api-key": apiKey, "anthropic-version": "2023-06-01" },
    request.signal
  );
  assertProviderCompletion(body.stop_reason, ["end_turn", "stop_sequence"], "Anthropic Messages");
  const contentBlocks = Array.isArray(body.content) ? body.content : [];
  const text = contentBlocks
    .filter(isRecord)
    .filter((entry) => entry.type === "text" && typeof entry.text === "string")
    .map((entry) => entry.text as string)
    .join("");
  if (!text) throw providerError("Anthropic Messages response did not contain text output.");
  const responseId = optionalString(body.id);
  return responseFromStructuredText(text, request, binding, undefined, responseId === undefined ? undefined : { responseId });
}

async function invokeOpenAiCompatible(
  binding: AgentFlowResolvedProviderBinding,
  request: AgentFlowSessionProviderRequest,
  env: Readonly<Record<string, string | undefined>>
): Promise<AgentFlowSessionProviderResponse> {
  assertApiAuthority(request, binding);
  const headers: Record<string, string> = {};
  if (binding.config.api_key_env !== undefined) {
    headers.Authorization = `Bearer ${requiredCredential(binding.config, env)}`;
  }
  const endpoint = `${binding.config.base_url!.replace(/\/$/, "")}/chat/completions`;
  const body = await postJson(
    endpoint,
    {
      model: binding.config.model,
      messages: [{ role: "user", content: buildProviderPrompt(request) }],
      response_format: {
        type: "json_schema",
        json_schema: { name: "agent_flow_outputs", strict: true, schema: outputSchema(request) }
      }
    },
    headers,
    request.signal
  );
  const choices = Array.isArray(body.choices) ? body.choices : [];
  const first = choices[0];
  if (isRecord(first)) assertProviderCompletion(first.finish_reason, ["stop"], "OpenAI-compatible");
  const message = isRecord(first) && isRecord(first.message) ? first.message : undefined;
  const content = message === undefined ? undefined : message.content;
  if (typeof content !== "string") throw providerError("OpenAI-compatible response did not contain message content.");
  return responseFromStructuredText(content, request, binding);
}

function buildProviderPrompt(request: AgentFlowSessionProviderRequest): string {
  const decoder = new TextDecoder("utf-8", { fatal: true });
  const inputs = request.inputs.map((input) => {
    let content: string;
    try { content = decoder.decode(input.content); } catch {
      throw providerError(`Configured providers accept UTF-8 text inputs only; ${input.path} is not valid UTF-8.`);
    }
    return [`<agent-flow-input path=${JSON.stringify(input.path)}>`, content, "</agent-flow-input>"].join("\n");
  });
  return [
    request.prompt.content,
    "",
    ...inputs,
    "",
    "Return one JSON object that matches the supplied schema.",
    "The outputs object must contain exactly the declared paths, with complete UTF-8 file content as each value.",
    "Do not wrap the JSON in Markdown fences.",
    `Declared output paths: ${JSON.stringify(request.outputs)}`,
    ...(request.kind === "recovery"
      ? ["Also return recovery_status as either remediated or unresolved."]
      : [])
  ].join("\n");
}

function outputSchema(request: AgentFlowSessionProviderRequest): Record<string, unknown> {
  const properties: Record<string, unknown> = {
    outputs: {
      type: "object",
      properties: Object.fromEntries(request.outputs.map((output) => [output, { type: "string" }])),
      required: [...request.outputs],
      additionalProperties: false
    }
  };
  if (request.kind === "recovery") properties.recovery_status = { enum: ["remediated", "unresolved"] };
  return {
    type: "object",
    properties,
    required: request.kind === "recovery" ? ["outputs", "recovery_status"] : ["outputs"],
    additionalProperties: false
  };
}

function responseFromStructuredText(
  source: string,
  request: AgentFlowSessionProviderRequest,
  binding: AgentFlowResolvedProviderBinding,
  externalSessionId?: string,
  extraMetadata?: Record<string, AgentFlowRunStateValue>
): AgentFlowSessionProviderResponse {
  const parsed = parseJsonObject(source, `${binding.config.driver} structured output`);
  if (!isRecord(parsed.outputs)) throw providerError(`${binding.config.driver} structured output must contain an outputs object.`);
  const parsedOutputs = parsed.outputs;
  const actual = Object.keys(parsedOutputs).sort();
  const expected = [...request.outputs].sort();
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw providerError(`${binding.config.driver} returned output paths ${JSON.stringify(actual)}; expected ${JSON.stringify(expected)}.`);
  }
  const outputs = Object.fromEntries(request.outputs.map((output) => {
    const value = parsedOutputs[output];
    if (typeof value !== "string") throw providerError(`${binding.config.driver} output ${output} must be a UTF-8 string.`);
    assertUnicodeScalarString(value, `${binding.config.driver} output ${output}`);
    return [output, value];
  })) as Record<string, string>;
  return {
    outputs,
    ...(externalSessionId === undefined ? {} : { externalSessionId }),
    metadata: {
      target: binding.target,
      driver: binding.config.driver,
      ...(binding.config.model.length === 0 ? {} : { modelHash: hashAgentFlowProviderModel(binding.config.model) }),
      ...(binding.config.profile === undefined ? {} : { profile: binding.config.profile }),
      ...(binding.config.reasoning_effort === undefined
        ? {}
        : { reasoningEffort: binding.config.reasoning_effort }),
      fingerprint: binding.fingerprint,
      ...(request.kind === "recovery" && (parsed.recovery_status === "remediated" || parsed.recovery_status === "unresolved")
        ? { recovery_status: parsed.recovery_status }
        : {}),
      ...extraMetadata
    }
  };
}

async function postJson(
  endpoint: string,
  body: Record<string, unknown>,
  headers: Record<string, string>,
  signal: AbortSignal
): Promise<Record<string, unknown>> {
  let response: Response;
  try {
    response = await fetch(endpoint, {
      method: "POST",
      redirect: "manual",
      headers: { "content-type": "application/json", ...headers },
      body: JSON.stringify(body),
      signal
    });
  } catch (error) {
    throw providerError(`Provider request failed: ${error instanceof Error ? error.message : String(error)}.`);
  }
  if (response.status >= 300 && response.status < 400) throw providerError("Provider endpoint redirects are not allowed.");
  if (!response.ok) throw providerError(`Provider returned HTTP ${response.status}.`);
  const source = await readBoundedResponse(response);
  return parseJsonObject(source, "Provider response");
}

async function readBoundedResponse(response: Response): Promise<string> {
  const declared = Number(response.headers.get("content-length"));
  if (Number.isFinite(declared) && declared > MAX_PROVIDER_RESPONSE_BYTES) throw providerError("Provider response exceeds the size limit.");
  if (response.body === null) return "";
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  while (true) {
    const result = await reader.read();
    if (result.done) break;
    total += result.value.byteLength;
    if (total > MAX_PROVIDER_RESPONSE_BYTES) {
      await reader.cancel();
      throw providerError("Provider response exceeds the size limit.");
    }
    chunks.push(result.value);
  }
  const combined = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) { combined.set(chunk, offset); offset += chunk.byteLength; }
  try { return new TextDecoder("utf-8", { fatal: true }).decode(combined); } catch {
    throw providerError("Provider response is not valid UTF-8.");
  }
}

function assertApiAuthority(request: AgentFlowSessionProviderRequest, binding: AgentFlowResolvedProviderBinding): void {
  if (request.canModifyFiles === true) {
    throw providerError(`Configured ${binding.config.driver} target ${binding.alias} cannot receive file-modification authority.`);
  }
  if (request.resume) {
    throw providerError(`Configured ${binding.config.driver} target ${binding.alias} does not support conversational resume.`);
  }
}

function assertUtf8Inputs(request: AgentFlowSessionProviderRequest): void {
  const decoder = new TextDecoder("utf-8", { fatal: true });
  for (const input of request.inputs) {
    try { decoder.decode(input.content); } catch {
      throw providerError(`Configured providers accept UTF-8 text inputs only; ${input.path} is not valid UTF-8.`);
    }
  }
}

function requiredCredential(
  config: AgentFlowConfiguredTarget,
  env: Readonly<Record<string, string | undefined>>
): string {
  const name = config.api_key_env;
  const candidate = name !== undefined && Object.hasOwn(env, name) ? env[name] : undefined;
  const value = typeof candidate === "string" ? candidate.trim() : undefined;
  if (name === undefined || !value) throw providerError(`Credential environment variable ${name ?? "(missing api_key_env)"} is not set.`);
  return value;
}

function assertProviderCompletion(value: unknown, allowed: readonly string[], label: string): void {
  if (typeof value !== "string" || !allowed.includes(value)) {
    throw providerError(`${label} response stopped before completing the declared outputs.`);
  }
}

function assertUnicodeScalarString(value: string, label: string): void {
  for (let index = 0; index < value.length; index += 1) {
    const unit = value.charCodeAt(index);
    if (unit >= 0xD800 && unit <= 0xDBFF) {
      const next = value.charCodeAt(index + 1);
      if (next >= 0xDC00 && next <= 0xDFFF) {
        index += 1;
        continue;
      }
      throw providerError(`${label} contains an invalid Unicode scalar sequence.`);
    }
    if (unit >= 0xDC00 && unit <= 0xDFFF) {
      throw providerError(`${label} contains an invalid Unicode scalar sequence.`);
    }
  }
}

function openAiResponseText(body: Record<string, unknown>): string {
  if (typeof body.output_text === "string") return body.output_text;
  const output = Array.isArray(body.output) ? body.output : [];
  const text = output.filter(isRecord).flatMap((item) => Array.isArray(item.content) ? item.content : [])
    .filter(isRecord)
    .filter((item) => (item.type === "output_text" || item.type === "text") && typeof item.text === "string")
    .map((item) => item.text as string)
    .join("");
  if (!text) throw providerError("OpenAI Responses response did not contain output text.");
  return text;
}

function parseJsonObject(source: string, label: string): Record<string, unknown> {
  let value: unknown;
  try { value = JSON.parse(source); } catch { throw providerError(`${label} is not valid JSON.`); }
  if (!isRecord(value)) throw providerError(`${label} must be a JSON object.`);
  return value;
}

function codexMcpArguments(value: unknown): AgentFlowRunStateValue | undefined {
  if (typeof value === "string") {
    try {
      return JSON.parse(value) as AgentFlowRunStateValue;
    } catch {
      return undefined;
    }
  }
  if (value === null || typeof value === "boolean" || typeof value === "number"
      || Array.isArray(value) || isRecord(value)) {
    return value as AgentFlowRunStateValue;
  }
  return undefined;
}

function codexMcpResultContent(value: unknown): string | undefined {
  if (!isRecord(value)) return undefined;
  if (Object.hasOwn(value, "structured_content")) {
    const structured = codexMcpArguments(value.structured_content);
    if (structured !== undefined) return `${stableJson(structured)}\n`;
  }
  if (!Array.isArray(value.content) || value.content.length === 0) return undefined;
  const blocks = value.content.map((entry) =>
    isRecord(entry) && entry.type === "text" && typeof entry.text === "string"
      ? entry.text
      : undefined
  );
  return blocks.every((entry): entry is string => entry !== undefined)
    ? blocks.join("\n")
    : undefined;
}

function stableJson(value: AgentFlowRunStateValue): string {
  return JSON.stringify(value, (_key, entry) => {
    if (entry !== null && typeof entry === "object" && !Array.isArray(entry)) {
      return Object.fromEntries(Object.keys(entry).sort().map((key) => [key, entry[key]]));
    }
    return entry;
  });
}

function sha256(value: string): string {
  return `sha256:${createHash("sha256").update(value, "utf8").digest("hex")}`;
}

function optionalString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function providerError(message: string, code = "AGENT_FLOW_CONFIGURED_PROVIDER"): AgentFlowSessionRequestError {
  return new AgentFlowSessionRequestError(message, code);
}
