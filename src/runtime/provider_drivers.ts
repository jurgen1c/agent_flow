import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { spawn, type ChildProcess } from "node:child_process";

import {
  AgentFlowSessionRequestError,
  createAgentFlowSessionProviderRegistry,
  type AgentFlowSessionProviderAdapter,
  type AgentFlowSessionProviderRequest,
  type AgentFlowSessionProviderResponse,
  type AgentFlowSessionProviderRegistry
} from "./session_request";
import {
  hashAgentFlowProviderModel,
  type AgentFlowConfiguredTarget,
  type AgentFlowProviderCatalog,
  type AgentFlowResolvedProviderBinding
} from "./provider_config";
import {
  captureAgentFlowWorkspaceSnapshot,
  changedAgentFlowWorkspacePaths
} from "./workspace";
import { matchesPolicyGlob } from "./policy_utils";

const MAX_PROVIDER_RESPONSE_BYTES = 12 * 1024 * 1024;
const NATIVE_SESSION_UNAVAILABLE = "AGENT_FLOW_PROVIDER_SESSION_UNAVAILABLE";
const NATIVE_COMMON_ENVIRONMENT = new Set([
  "HOME", "LANG", "LOGNAME", "NODE_EXTRA_CA_CERTS", "NO_PROXY", "PATH", "SHELL",
  "SSL_CERT_DIR", "SSL_CERT_FILE", "TERM", "TMPDIR", "USER",
  "HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "no_proxy"
]);

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
      ...(binding.config.model === undefined ? {} : { model: binding.config.model }),
      fingerprint: binding.fingerprint
    }, createAgentFlowConfiguredProviderAdapter(binding, options));
  }
  return registry;
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
  } else {
    adapter = (request) => invokeClaudeCode(binding, request, env);
  }
  adapter.preflight = (request) => {
    if (binding.config.driver === "codex-cli" || binding.config.driver === "claude-code") {
      assertNativeAuthority(request, binding);
    } else {
      assertApiAuthority(request, binding);
      if (binding.config.api_key_env !== undefined) requiredCredential(binding.config, env);
    }
    assertUtf8Inputs(request);
  };
  if (binding.config.driver === "codex-cli" || binding.config.driver === "claude-code") {
    adapter.waitForAbort = true;
  }
  return adapter;
}

async function invokeCodexCli(
  binding: AgentFlowResolvedProviderBinding,
  request: AgentFlowSessionProviderRequest,
  env: Readonly<Record<string, string | undefined>>
): Promise<AgentFlowSessionProviderResponse> {
  assertNativeAuthority(request, binding);
  return auditNativeWorkspace(request, binding, async () => {
    const temporaryDirectory = fs.mkdtempSync(path.join(os.tmpdir(), "agent-flow-codex-"));
    const schemaPath = path.join(temporaryDirectory, "output-schema.json");
    const outputPath = path.join(temporaryDirectory, "output.json");
    fs.writeFileSync(schemaPath, `${JSON.stringify(outputSchema(request))}\n`, { mode: 0o600 });
    let observedThreadId: string | undefined;
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
      const commonArguments = [
        "--json",
        "--output-schema", schemaPath,
        "--output-last-message", outputPath,
        ...(binding.config.model === undefined ? [] : ["--model", binding.config.model])
      ];
      const arguments_ = request.externalSessionId === undefined
        ? [
            "exec",
            ...commonArguments,
            "--sandbox", request.canModifyFiles === true ? "workspace-write" : "read-only",
            ...(binding.config.profile === undefined ? [] : ["--profile", binding.config.profile]),
            ...(request.resume ? [] : ["--ephemeral"]),
            "-"
          ]
        : [
            "exec",
            "--sandbox", request.canModifyFiles === true ? "workspace-write" : "read-only",
            ...(binding.config.profile === undefined ? [] : ["--profile", binding.config.profile]),
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
        nativeProcessSandbox(request, env, temporaryDirectory, "codex"),
        (line) => {
          let event: unknown;
          try { event = JSON.parse(line); } catch {
            throw providerError("Codex CLI emitted malformed JSONL output.");
          }
          if (isRecord(event) && event.type === "thread.started") reportThread(event.thread_id);
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
      return responseFromStructuredText(
        structured,
        request,
        binding,
        request.resume ? observedThreadId : undefined,
        { cli: "codex" }
      );
    } finally {
      fs.rmSync(temporaryDirectory, { recursive: true, force: true });
    }
  });
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
      if (sessionId !== undefined && request.resume) request.reportExternalSessionId?.(sessionId);
      const displayName = `agent-flow-${safeCliName(request.runId)}-${safeCliName(request.sessionId)}`.slice(0, 120);
      const arguments_ = [
        "-p",
        "--output-format", "json",
        "--json-schema", JSON.stringify(outputSchema(request)),
        "--permission-mode", request.canModifyFiles === true ? "acceptEdits" : "plan",
        ...(binding.config.model === undefined ? [] : ["--model", binding.config.model]),
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
        nativeProcessSandbox(request, env, temporaryDirectory, "claude")
      );
      assertNativeSuccess("Claude Code", result, request.externalSessionId !== undefined);
      const envelope = parseJsonObject(result.stdout.toString("utf8"), "Claude Code response");
      const returnedSessionId = optionalString(envelope.session_id);
      if (sessionId !== undefined && returnedSessionId !== sessionId) {
        throw providerError("Claude Code returned a session ID that does not match the persisted Agent Flow session.");
      }
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
  executablePath: string;
  executableMountPaths: string[];
}

function runNativeProviderProcess(
  command: string,
  arguments_: string[],
  input: string,
  cwd: string,
  env: Readonly<Record<string, string | undefined>>,
  signal: AbortSignal,
  sandbox: NativeProcessSandbox,
  onStdoutLine?: (line: string) => void
): Promise<NativeProviderProcessResult> {
  return new Promise((resolve, reject) => {
    const stdout: Buffer[] = [];
    const stderr: Buffer[] = [];
    let stdoutBytes = 0;
    let stderrBytes = 0;
    let lineBuffer = "";
    let settled = false;
    let terminationTimer: ReturnType<typeof setTimeout> | undefined;
    let failure: Error | undefined;
    let child: ChildProcess;
    try {
      const invocation = sandboxedNativeInvocation(command, arguments_, sandbox);
      child = spawn(invocation.command, invocation.arguments, {
        cwd,
        shell: false,
        detached: process.platform !== "win32",
        env: {
          ...nativeProviderEnvironment(command, env),
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
      if (stream === "stdout" && onStdoutLine !== undefined) {
        lineBuffer += content.toString("utf8");
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

function nativeProcessSandbox(
  request: AgentFlowSessionProviderRequest,
  env: Readonly<Record<string, string | undefined>>,
  temporaryDirectory: string,
  command: "codex" | "claude"
): NativeProcessSandbox {
  const home = env.HOME ?? os.homedir();
  const configuredState = command === "codex"
    ? env.CODEX_HOME ?? path.join(home, ".codex")
    : env.CLAUDE_CONFIG_DIR ?? path.join(home, ".claude");
  const repoRoot = fs.realpathSync(request.repoRoot!);
  const executablePath = resolveNativeExecutable(command, env.PATH);
  const statePath = configuredState !== undefined && fs.existsSync(configuredState)
    ? fs.realpathSync(configuredState)
    : undefined;
  if (statePath !== undefined
      && (pathsOverlap(statePath, repoRoot) || statePath === path.parse(statePath).root)) {
    throw providerError(
      `Native CLI state directory ${statePath} must be separate from the repository and cannot be a filesystem root.`
    );
  }
  return {
    repoRoot,
    canModifyFiles: request.canModifyFiles === true,
    temporaryDirectory: fs.realpathSync(temporaryDirectory),
    providerStatePaths: statePath === undefined ? [] : [statePath],
    executablePath,
    executableMountPaths: nativeExecutableMountPaths(command, executablePath, env.PATH)
  };
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

function nativeExecutableMountPaths(command: string, executablePath: string, pathValue: string | undefined): string[] {
  const paths: string[] = [];
  if (executablePath !== "/usr" && !executablePath.startsWith("/usr/")) {
    paths.push(command === "codex" && path.basename(executablePath) === "codex.js"
      ? path.dirname(path.dirname(executablePath))
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
    const asdfPath = resolveNativeExecutable("asdf", pathValue);
    if (asdfPath !== "/usr" && !asdfPath.startsWith("/usr/")) paths.push(asdfPath);
    const toolVersionsPath = path.join(os.homedir(), ".tool-versions");
    if (fs.existsSync(toolVersionsPath)) paths.push(toolVersionsPath);
  }
  return paths.filter((candidate, index, values) => values.indexOf(candidate) === index);
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
    const writablePaths = [sandbox.temporaryDirectory, ...sandbox.providerStatePaths]
      .filter((candidate, index, values) => values.indexOf(candidate) === index);
    const mountedPaths = [
      repoRoot,
      sandbox.temporaryDirectory,
      ...sandbox.providerStatePaths,
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
        ...(fs.existsSync(runtimePath) ? ["--tmpfs", runtimePath, "--chmod", "000", runtimePath] : []),
        ...sandbox.executableMountPaths.flatMap((candidate) => ["--ro-bind", candidate, candidate]),
        ...writablePaths.flatMap((candidate) => ["--bind", candidate, candidate]),
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
  return withNativeWorkspaceLock(request.repoRoot!, request.signal, async () => {
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
  });
}

async function withNativeWorkspaceLock<T>(
  repoRoot: string,
  signal: AbortSignal,
  callback: () => Promise<T>
): Promise<T> {
  const runtimeDirectory = path.join(fs.realpathSync(repoRoot), ".agent-flow");
  fs.mkdirSync(runtimeDirectory, { recursive: true, mode: 0o700 });
  const runtimeStat = fs.lstatSync(runtimeDirectory);
  if (!runtimeStat.isDirectory() || runtimeStat.isSymbolicLink()) {
    throw providerError("Native provider locking requires a non-symlink .agent-flow directory.");
  }
  const lockPath = path.join(runtimeDirectory, "native-provider.lock");
  fs.closeSync(fs.openSync(
    lockPath,
    fs.constants.O_APPEND | fs.constants.O_CREAT | fs.constants.O_NOFOLLOW | fs.constants.O_WRONLY,
    0o600
  ));
  const release = await acquireNativeWorkspaceLock(lockPath, signal);
  try {
    return await callback();
  } finally {
    await release();
  }
}

function acquireNativeWorkspaceLock(lockPath: string, signal: AbortSignal): Promise<() => Promise<void>> {
  return new Promise((resolve, reject) => {
    const child = spawn("/usr/bin/flock", [
      "--exclusive", lockPath, "/bin/sh", "-c", "printf 'locked\\n'; cat >/dev/null"
    ], {
      shell: false,
      detached: true,
      env: { PATH: "/usr/bin:/bin" },
      stdio: ["pipe", "pipe", "pipe"]
    });
    let settled = false;
    let stdout = "";
    let stderr = "";
    const abort = (): void => {
      terminateNativeChild(child.pid, "SIGTERM");
      if (!settled) {
        settled = true;
        reject(signal.reason instanceof Error ? signal.reason : providerError("Native provider lock acquisition was aborted."));
      }
    };
    signal.addEventListener("abort", abort, { once: true });
    child.stderr?.on("data", (chunk: Buffer | string) => { stderr = `${stderr}${String(chunk)}`.slice(-2_000); });
    child.stdout?.on("data", (chunk: Buffer | string) => {
      stdout += String(chunk);
      if (settled || !stdout.includes("locked\n")) return;
      settled = true;
      signal.removeEventListener("abort", abort);
      resolve(async () => {
        if (child.exitCode !== null) return;
        child.stdin?.end();
        await new Promise<void>((done) => child.once("close", () => done()));
      });
    });
    child.once("error", (error) => {
      if (settled) return;
      settled = true;
      signal.removeEventListener("abort", abort);
      reject(providerError(`Could not acquire native provider workspace lock: ${error.message}.`));
    });
    child.once("close", (status) => {
      if (settled) return;
      settled = true;
      signal.removeEventListener("abort", abort);
      reject(providerError(
        `Could not acquire native provider workspace lock${status === null ? "" : ` (status ${status})`}${stderr.trim() ? `: ${stderr.trim()}` : "."}`
      ));
    });
    if (signal.aborted) abort();
  });
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

function nativeProviderEnvironment(
  command: string,
  source: Readonly<Record<string, string | undefined>>
): NodeJS.ProcessEnv {
  const result: NodeJS.ProcessEnv = {};
  const permitted = (name: string): boolean => {
    if (NATIVE_COMMON_ENVIRONMENT.has(name) || name.startsWith("LC_")) return true;
    if (command === "codex") {
      return name === "CODEX_HOME" || name === "OPENAI_API_KEY" || name.startsWith("OPENAI_");
    }
    return name === "CLAUDE_CONFIG_DIR"
      || name === "CLOUD_ML_REGION"
      || name.startsWith("ANTHROPIC_")
      || name.startsWith("CLAUDE_CODE_")
      || name.startsWith("AWS_")
      || name.startsWith("GOOGLE_")
      || name.startsWith("VERTEX_");
  };
  for (const [name, value] of Object.entries(source)) {
    if (value !== undefined && permitted(name)) result[name] = value;
  }
  if (result.HOME === undefined) result.HOME = os.homedir();
  if (result.PATH === undefined) result.PATH = "/usr/local/bin:/usr/bin:/bin";
  return result;
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
  extraMetadata?: Record<string, string>
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
      ...(binding.config.model === undefined ? {} : { modelHash: hashAgentFlowProviderModel(binding.config.model) }),
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

function optionalString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function providerError(message: string, code = "AGENT_FLOW_CONFIGURED_PROVIDER"): AgentFlowSessionRequestError {
  return new AgentFlowSessionRequestError(message, code);
}
