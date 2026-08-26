import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { findGitRepositoryRoot } from "@jurgen1c/agent-core/repository";
import { parseYamlDocument, type JsonValue } from "@jurgen1c/agent-core/yaml";
import { parse as parseToml, type TomlTable } from "smol-toml";
import { redactAgentFlowSensitiveText } from "./failure_payload";

const NATIVE_PROVIDER_DOCTOR_TIMEOUT_MS = 5_000;
const MAX_CODEX_PROFILE_BYTES = 1024 * 1024;

export type AgentFlowConfiguredProviderKind = "local" | "frontier";
export type AgentFlowProviderDriver =
  | "openai-responses"
  | "anthropic-messages"
  | "openai-compatible"
  | "codex-cli"
  | "claude-code";
export type AgentFlowCodexReasoningEffort = "minimal" | "low" | "medium" | "high" | "xhigh";

export interface AgentFlowConfiguredTarget {
  kind: AgentFlowConfiguredProviderKind;
  driver: AgentFlowProviderDriver;
  model: string;
  enabled: boolean;
  base_url?: string;
  api_key_env?: string;
  max_output_tokens?: number;
  profile?: string;
  reasoning_effort?: AgentFlowCodexReasoningEffort;
}

export interface AgentFlowResolvedCodexProfile {
  home: string;
  path: string;
  fingerprint: string;
  baseConfigPath: string;
  baseConfigFingerprint?: string;
  mcpServerIds: readonly string[];
  providerEnvironmentNames: readonly string[];
  requiresOpenAiAuth: boolean;
}

export interface AgentFlowProviderAlias {
  kind: AgentFlowConfiguredProviderKind;
  target: string;
}

export interface AgentFlowResolvedProviderBinding extends AgentFlowProviderAlias {
  alias: string;
  target: string;
  config: AgentFlowConfiguredTarget;
  fingerprint: string;
  codexProfile?: AgentFlowResolvedCodexProfile;
}

export interface AgentFlowProviderCatalog {
  globalConfigPath: string;
  repoConfigPath: string;
  targets: Readonly<Record<string, AgentFlowConfiguredTarget>>;
  providers: Readonly<Record<string, AgentFlowProviderAlias>>;
  bindings: Readonly<Record<string, AgentFlowResolvedProviderBinding>>;
}

export interface LoadAgentFlowProviderCatalogOptions {
  cwd?: string;
  env?: Readonly<Record<string, string | undefined>>;
  homeDir?: string;
  configPath?: string;
  overrides?: readonly string[];
  aliases?: readonly string[];
}

export class AgentFlowProviderConfigError extends Error {
  readonly code: string;

  constructor(message: string, code = "AGENT_FLOW_PROVIDER_CONFIG") {
    super(message);
    this.name = "AgentFlowProviderConfigError";
    this.code = code;
  }
}

const NATIVE_COMMON_ENVIRONMENT = new Set([
  "HOME", "LANG", "LOGNAME", "NODE_EXTRA_CA_CERTS", "NO_PROXY", "PATH", "SHELL",
  "SSL_CERT_DIR", "SSL_CERT_FILE", "TERM", "TMPDIR", "USER",
  "HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "no_proxy"
]);

const TARGET_FIELDS = new Set([
  "kind", "driver", "model", "enabled", "base_url", "api_key_env", "max_output_tokens",
  "profile", "reasoning_effort"
]);
const ROOT_FIELDS = new Set(["version", "workflows", "prompts", "templates", "runs", "targets", "providers"]);
const ALIAS_FIELDS = new Set(["kind", "target"]);
const RESERVED_ALIASES = new Set(["fixture", "local", "frontier"]);
const RESERVED_OBJECT_NAMES = new Set(["__proto__", "prototype", "constructor"]);
const DRIVERS = new Set<AgentFlowProviderDriver>([
  "openai-responses", "anthropic-messages", "openai-compatible", "codex-cli", "claude-code"
]);
const CODEX_REASONING_EFFORTS = ["minimal", "low", "medium", "high", "xhigh"] as const;

export function loadAgentFlowProviderCatalog(
  options: LoadAgentFlowProviderCatalogOptions = {}
): AgentFlowProviderCatalog {
  const cwd = path.resolve(options.cwd ?? process.cwd());
  const repoRoot = findGitRepositoryRoot(cwd) ?? cwd;
  const env = options.env ?? process.env;
  const homeDir = options.homeDir ?? os.homedir();
  const globalConfigRequired = options.configPath !== undefined || env.AGENT_FLOW_CONFIG !== undefined;
  const configuredPath = options.configPath ?? env.AGENT_FLOW_CONFIG;
  const globalConfigPath = configuredPath === undefined
    ? path.join(xdgConfigHome(env.XDG_CONFIG_HOME, homeDir), "agent-flow", "config.yml")
    : path.resolve(cwd, configuredPath);
  const repoConfigPath = path.join(repoRoot, ".agent-flow.yml");
  const repository = readConfig(repoConfigPath, "repository", false);
  const providers = parseProviders(repository, repoConfigPath);
  if (repository !== undefined && Object.hasOwn(repository, "targets")) {
    throw configError(repoConfigPath, "targets", "Concrete targets belong in the global Agent Flow config file.");
  }
  const overrides = parseOverrides(options.overrides ?? []);
  for (const alias of Object.keys(overrides)) {
    if (!Object.hasOwn(providers, alias)) {
      throw new AgentFlowProviderConfigError(
        `Provider override references undeclared repository alias ${JSON.stringify(alias)}.`,
        "AGENT_FLOW_PROVIDER_OVERRIDE_UNKNOWN"
      );
    }
  }
  const selectedAliases = options.aliases === undefined
    ? Object.keys(providers)
    : [...new Set([...options.aliases, ...Object.keys(overrides)])];
  const needsTargets = options.aliases === undefined || selectedAliases.length > 0;
  const global = needsTargets ? readConfig(globalConfigPath, "global", globalConfigRequired) : undefined;
  const targets = parseTargets(global, globalConfigPath);
  if (global !== undefined && Object.hasOwn(global, "providers")) {
    throw configError(globalConfigPath, "providers", "Provider aliases belong in the repository .agent-flow.yml file.");
  }
  const bindings = emptyMap<AgentFlowResolvedProviderBinding>();
  for (const alias of selectedAliases.sort()) {
    if (!Object.hasOwn(providers, alias)) continue;
    const definition = providers[alias]!;
    const targetName = Object.hasOwn(overrides, alias) ? overrides[alias]! : definition.target;
    const config = Object.hasOwn(targets, targetName) ? targets[targetName] : undefined;
    if (config === undefined) {
      throw configError(repoConfigPath, `providers.${alias}.target`, `Target ${JSON.stringify(targetName)} is not defined in ${globalConfigPath}.`);
    }
    if (!config.enabled) {
      throw configError(globalConfigPath, `targets.${targetName}.enabled`, `Target ${JSON.stringify(targetName)} is disabled.`);
    }
    if (config.kind !== definition.kind) {
      throw configError(
        repoConfigPath,
        `providers.${alias}.kind`,
        `Alias kind ${definition.kind} cannot resolve to ${config.kind} target ${JSON.stringify(targetName)}.`
      );
    }
    const codexProfile = resolveCodexProfile(config, env, homeDir, repoRoot, globalConfigPath, targetName);
    bindings[alias] = Object.freeze({
      alias,
      target: targetName,
      kind: definition.kind,
      config,
      fingerprint: fingerprintTarget(
        config,
        codexProfile?.fingerprint,
        codexProfile?.baseConfigFingerprint
      ),
      ...(codexProfile === undefined ? {} : { codexProfile })
    });
  }
  return Object.freeze({
    globalConfigPath,
    repoConfigPath,
    targets: Object.freeze(targets),
    providers: Object.freeze(providers),
    bindings: Object.freeze(bindings)
  });
}

export function loadAgentFlowRepositoryProviderAliases(
  options: Pick<LoadAgentFlowProviderCatalogOptions, "cwd"> = {}
): Readonly<Record<string, AgentFlowProviderAlias>> {
  const cwd = path.resolve(options.cwd ?? process.cwd());
  const repoRoot = findGitRepositoryRoot(cwd) ?? cwd;
  const repoConfigPath = path.join(repoRoot, ".agent-flow.yml");
  const repository = readConfig(repoConfigPath, "repository", false);
  if (repository !== undefined && Object.hasOwn(repository, "targets")) {
    throw configError(repoConfigPath, "targets", "Concrete targets belong in the global Agent Flow config file.");
  }
  return Object.freeze(parseProviders(repository, repoConfigPath));
}

export function parseAgentFlowProviderConfig(
  source: string,
  sourcePath = "Agent Flow config"
): { targets: Record<string, AgentFlowConfiguredTarget>; providers: Record<string, AgentFlowProviderAlias> } {
  const value = parseConfigSource(source, sourcePath);
  return {
    targets: parseTargets(value, sourcePath),
    providers: parseProviders(value, sourcePath)
  };
}

export function providerBindingsForWorkflow(
  workflow: { sessions?: Record<string, unknown> },
  catalog: AgentFlowProviderCatalog
): AgentFlowResolvedProviderBinding[] {
  const aliases = new Set<string>();
  for (const session of Object.values(workflow.sessions ?? {})) {
    if (!isMapping(session) || typeof session.provider !== "string") continue;
    const provider = session.provider.trim();
    if (Object.hasOwn(catalog.bindings, provider)) aliases.add(provider);
  }
  return [...aliases].sort().map((alias) => catalog.bindings[alias]!);
}

export function serializeAgentFlowProviderBindings(
  bindings: readonly AgentFlowResolvedProviderBinding[]
): Record<string, JsonValue> {
  return Object.fromEntries(bindings.map((binding) => [binding.alias, {
    target: binding.target,
    kind: binding.kind,
    driver: binding.config.driver,
    modelHash: hashAgentFlowProviderModel(binding.config.model),
    ...(binding.config.profile === undefined ? {} : { profile: binding.config.profile }),
    ...(binding.config.reasoning_effort === undefined
      ? {}
      : { reasoningEffort: binding.config.reasoning_effort }),
    fingerprint: binding.fingerprint
  }]));
}

export function hashAgentFlowProviderModel(model: string): string {
  return `sha256:${createHash("sha256").update(model).digest("hex")}`;
}

export function renderAgentFlowProviderCatalog(catalog: AgentFlowProviderCatalog): string {
  const aliases = Object.values(catalog.bindings).sort((left, right) => left.alias.localeCompare(right.alias));
  if (aliases.length === 0) return "No configured Agent Flow provider aliases.";
  return [
    "ALIAS\tTARGET\tKIND\tDRIVER\tMODEL\tCREDENTIAL",
    ...aliases.map((binding) => [
      binding.alias,
      binding.target,
      binding.kind,
      binding.config.driver,
      redactAgentFlowSensitiveText(binding.config.model),
      binding.config.api_key_env ?? "none"
    ].join("\t"))
  ].join("\n");
}

export function doctorAgentFlowProviderCatalog(
  catalog: AgentFlowProviderCatalog,
  env: Readonly<Record<string, string | undefined>> = process.env
): { ok: boolean; lines: string[] } {
  let ok = true;
  const lines = Object.values(catalog.bindings)
    .sort((left, right) => left.alias.localeCompare(right.alias))
    .map((binding) => {
      const keyName = binding.config.api_key_env;
      if (keyName !== undefined && !environmentValue(env, keyName)) {
        ok = false;
        return `${binding.alias}: missing credential environment variable ${keyName}`;
      }
      if (binding.config.driver === "codex-cli" || binding.config.driver === "claude-code") {
        if (process.platform !== "linux" || !fs.existsSync("/usr/bin/bwrap") || !fs.existsSync("/usr/bin/flock")) {
          ok = false;
          return `${binding.alias}: native CLI filesystem sandbox is unavailable (bubblewrap and flock are required on Linux)`;
        }
        const sandboxProbe = spawnSync("/usr/bin/bwrap", [
          "--die-with-parent",
          "--new-session",
          "--unshare-pid",
          "--ro-bind", "/usr", "/usr",
          "--symlink", "usr/bin", "/bin",
          "--symlink", "usr/lib", "/lib",
          ...(fs.existsSync("/usr/lib64") ? ["--symlink", "usr/lib64", "/lib64"] : []),
          "--proc", "/proc",
          "--dev", "/dev",
          "--",
          "/usr/bin/true"
        ], {
          encoding: "utf8",
          env: { PATH: "/usr/bin:/bin" },
          timeout: NATIVE_PROVIDER_DOCTOR_TIMEOUT_MS,
          killSignal: "SIGKILL"
        });
        if (sandboxProbe.error !== undefined || sandboxProbe.status !== 0) {
          ok = false;
          return `${binding.alias}: native CLI filesystem sandbox cannot create the required bubblewrap namespace`;
        }
        const command = binding.config.driver === "codex-cli" ? "codex" : "claude";
        const probeEnvironment = agentFlowNativeProviderEnvironment(
          command,
          env,
          binding.codexProfile?.requiresOpenAiAuth !== false
        );
        if (binding.config.driver === "codex-cli" && binding.codexProfile !== undefined) {
          probeEnvironment.CODEX_HOME = binding.codexProfile.home;
          const missingCredential = binding.codexProfile.providerEnvironmentNames
            .find((name) => !environmentValue(env, name));
          if (missingCredential !== undefined) {
            ok = false;
            return `${binding.alias}: missing credential environment variable ${missingCredential}`;
          }
          Object.assign(
            probeEnvironment,
            selectedCodexProviderEnvironment(env, binding.codexProfile.providerEnvironmentNames)
          );
        }
        const version = runAgentFlowNativeProviderDoctorProbe(command, ["--version"], probeEnvironment);
        if (version.error !== undefined || version.status !== 0) {
          ok = false;
          return `${binding.alias}: ${command} executable is unavailable`;
        }
        if (binding.config.driver === "codex-cli" && binding.config.profile !== undefined) {
          const profile = runAgentFlowNativeProviderDoctorProbe(
            command,
            [
              "exec",
              "--profile", binding.config.profile,
              "--strict-config",
              "--model", binding.config.model,
              "--ephemeral",
              "--skip-git-repo-check",
              "--json",
              "-"
            ],
            probeEnvironment
          );
          const noPromptAfterValidConfig = String(profile.stderr).includes("No prompt provided via stdin");
          if (profile.error !== undefined || (profile.status !== 0 && !noPromptAfterValidConfig)) {
            ok = false;
            return `${binding.alias}: codex profile ${binding.config.profile} is incompatible with the installed CLI`;
          }
        }
        const requiresLogin = binding.config.driver === "claude-code"
          || binding.codexProfile?.requiresOpenAiAuth !== false;
        if (requiresLogin) {
          const authArguments = binding.config.driver === "codex-cli"
            ? ["login", "status"]
            : ["auth", "status"];
          const auth = runAgentFlowNativeProviderDoctorProbe(command, authArguments, probeEnvironment);
          if (auth.error !== undefined || auth.status !== 0) {
            ok = false;
            return `${binding.alias}: ${command} is not authenticated`;
          }
        }
        const normalizedVersion = String(version.stdout || version.stderr).trim().split(/\r?\n/, 1)[0] ?? "unknown version";
        return `${binding.alias}: ready (${binding.config.driver}, ${redactAgentFlowSensitiveText(normalizedVersion)})`;
      }
      return `${binding.alias}: ready (${binding.config.driver}, ${redactAgentFlowSensitiveText(binding.config.model)})`;
    });
  return { ok, lines };
}

export function runAgentFlowNativeProviderDoctorProbe(
  command: string,
  arguments_: readonly string[],
  env: NodeJS.ProcessEnv,
  timeout = NATIVE_PROVIDER_DOCTOR_TIMEOUT_MS
) {
  return spawnSync(command, [...arguments_], {
    encoding: "utf8",
    env,
    timeout,
    killSignal: "SIGKILL"
  });
}

export function agentFlowNativeProviderEnvironment(
  command: "codex" | "claude",
  source: Readonly<Record<string, string | undefined>>,
  includeOpenAiCredentials = true
): NodeJS.ProcessEnv {
  const result: NodeJS.ProcessEnv = {};
  const permitted = (name: string): boolean => {
    if (NATIVE_COMMON_ENVIRONMENT.has(name) || name.startsWith("LC_")) return true;
    if (command === "codex") {
      return name === "CODEX_HOME"
        || (includeOpenAiCredentials && (name === "OPENAI_API_KEY" || name.startsWith("OPENAI_")));
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

export function selectedCodexProviderEnvironment(
  source: Readonly<Record<string, string | undefined>>,
  names: readonly string[]
): NodeJS.ProcessEnv {
  const result: NodeJS.ProcessEnv = {};
  for (const name of names) {
    const value = source[name];
    if (value !== undefined) result[name] = value;
  }
  return result;
}

function readConfig(
  configPath: string,
  label: string,
  required: boolean
): Record<string, JsonValue> | undefined {
  let source: string;
  try {
    source = fs.readFileSync(configPath, "utf8");
  } catch (error) {
    if (!required && isMissing(error)) return undefined;
    throw new AgentFlowProviderConfigError(`Could not read ${label} config ${configPath}: ${errorMessage(error)}.`);
  }
  return parseConfigSource(source, configPath);
}

function parseConfigSource(source: string, sourcePath: string): Record<string, JsonValue> {
  const parsed = parseYamlDocument(source);
  if (!parsed.ok) {
    throw new AgentFlowProviderConfigError(
      `Could not parse Agent Flow config ${sourcePath}: ${parsed.issues.map((issue) => issue.message).join("; ")}.`,
      "AGENT_FLOW_PROVIDER_CONFIG_PARSE"
    );
  }
  if (!isMapping(parsed.value)) throw configError(sourcePath, "$", "Config root must be a mapping.");
  if (parsed.value.version !== 1) throw configError(sourcePath, "version", "Config version must be 1.");
  rejectUnknownFields(parsed.value, ROOT_FIELDS, sourcePath, "$");
  for (const field of ["workflows", "prompts", "templates", "runs"] as const) {
    if (parsed.value[field] !== undefined && typeof parsed.value[field] !== "string") {
      throw configError(sourcePath, field, `Config ${field} must be a string.`);
    }
  }
  return parsed.value;
}

function parseTargets(
  root: Record<string, JsonValue> | undefined,
  sourcePath: string
): Record<string, AgentFlowConfiguredTarget> {
  if (root?.targets === undefined) return {};
  if (!isMapping(root.targets)) throw configError(sourcePath, "targets", "Targets must be a mapping.");
  const targets = emptyMap<AgentFlowConfiguredTarget>();
  for (const [name, value] of Object.entries(root.targets)) {
    validateName(name, sourcePath, "targets.<name>");
    if (!isMapping(value)) throw configError(sourcePath, `targets.${name}`, "Target must be a mapping.");
    rejectUnknownFields(value, TARGET_FIELDS, sourcePath, `targets.${name}`);
    const kind = requiredEnum(value.kind, ["local", "frontier"] as const, sourcePath, `targets.${name}.kind`);
    const driver = requiredEnum(value.driver, [...DRIVERS] as AgentFlowProviderDriver[], sourcePath, `targets.${name}.driver`);
    const model = requiredString(value.model, sourcePath, `targets.${name}.model`);
    if (/[\u0000-\u001F\u007F-\u009F]/.test(model)) {
      throw configError(sourcePath, `targets.${name}.model`, "Model identifiers must not contain control characters.");
    }
    if (value.enabled !== true && value.enabled !== false) {
      throw configError(sourcePath, `targets.${name}.enabled`, "Target enabled must be true or false.");
    }
    const target: AgentFlowConfiguredTarget = {
      kind,
      driver,
      model,
      enabled: value.enabled,
      ...optionalStringField(value, "base_url", sourcePath, `targets.${name}.base_url`),
      ...optionalStringField(value, "api_key_env", sourcePath, `targets.${name}.api_key_env`),
      ...optionalPositiveIntegerField(value, "max_output_tokens", sourcePath, `targets.${name}.max_output_tokens`),
      ...optionalStringField(value, "profile", sourcePath, `targets.${name}.profile`),
      ...(value.reasoning_effort === undefined
        ? {}
        : {
            reasoning_effort: requiredEnum(
              value.reasoning_effort,
              CODEX_REASONING_EFFORTS,
              sourcePath,
              `targets.${name}.reasoning_effort`
            )
          })
    };
    validateTargetShape(name, target, sourcePath);
    targets[name] = Object.freeze(target);
  }
  return targets;
}

function parseProviders(
  root: Record<string, JsonValue> | undefined,
  sourcePath: string
): Record<string, AgentFlowProviderAlias> {
  if (root?.providers === undefined) return {};
  if (!isMapping(root.providers)) throw configError(sourcePath, "providers", "Providers must be a mapping.");
  const providers = emptyMap<AgentFlowProviderAlias>();
  for (const [name, value] of Object.entries(root.providers)) {
    validateName(name, sourcePath, "providers.<name>");
    if (RESERVED_ALIASES.has(name) || name.startsWith("codex:")) {
      throw configError(sourcePath, `providers.${name}`, `Alias ${JSON.stringify(name)} is reserved by the programmatic registry.`);
    }
    if (!isMapping(value)) throw configError(sourcePath, `providers.${name}`, "Provider alias must be a mapping.");
    rejectUnknownFields(value, ALIAS_FIELDS, sourcePath, `providers.${name}`);
    const provider = Object.freeze({
      kind: requiredEnum(value.kind, ["local", "frontier"] as const, sourcePath, `providers.${name}.kind`),
      target: requiredString(value.target, sourcePath, `providers.${name}.target`)
    });
    validateName(provider.target, sourcePath, `providers.${name}.target`);
    providers[name] = provider;
  }
  return providers;
}

function validateTargetShape(name: string, target: AgentFlowConfiguredTarget, sourcePath: string): void {
  const field = `targets.${name}`;
  const nativeCli = target.driver === "codex-cli" || target.driver === "claude-code";
  if (nativeCli && target.kind !== "frontier") {
    throw configError(sourcePath, `${field}.kind`, `${target.driver} targets must be frontier.`);
  }
  if (target.api_key_env !== undefined && !/^[A-Za-z_][A-Za-z0-9_]*$/.test(target.api_key_env)) {
    throw configError(sourcePath, `${field}.api_key_env`, "api_key_env must be an environment variable name.");
  }
  if (target.driver === "openai-responses" || target.driver === "anthropic-messages") {
    if (target.kind !== "frontier") throw configError(sourcePath, `${field}.kind`, `${target.driver} targets must be frontier.`);
    if (target.api_key_env === undefined) throw configError(sourcePath, `${field}.api_key_env`, `${target.driver} requires api_key_env.`);
  }
  if (target.driver === "openai-compatible") {
    if (target.base_url === undefined) throw configError(sourcePath, `${field}.base_url`, "openai-compatible requires base_url.");
    validateBaseUrl(target.base_url, target.kind, sourcePath, `${field}.base_url`);
  } else if (target.base_url !== undefined) {
    throw configError(sourcePath, `${field}.base_url`, "base_url is supported only by openai-compatible targets.");
  }
  if (target.max_output_tokens !== undefined && target.driver !== "anthropic-messages") {
    throw configError(sourcePath, `${field}.max_output_tokens`, "max_output_tokens is supported only by anthropic-messages targets.");
  }
  if (target.profile !== undefined) {
    if (target.driver !== "codex-cli") {
      throw configError(sourcePath, `${field}.profile`, "profile is supported only by codex-cli targets.");
    }
    if (!/^[A-Za-z0-9_-]+$/.test(target.profile)
        || redactAgentFlowSensitiveText(target.profile) !== target.profile) {
      throw configError(
        sourcePath,
        `${field}.profile`,
        "Codex profile names must be non-secret and may contain only letters, numbers, hyphens, and underscores."
      );
    }
  }
  if (target.reasoning_effort !== undefined && target.driver !== "codex-cli") {
    throw configError(
      sourcePath,
      `${field}.reasoning_effort`,
      "reasoning_effort is supported only by codex-cli targets."
    );
  }
  if (target.api_key_env !== undefined
      && target.driver !== "openai-responses"
      && target.driver !== "anthropic-messages"
      && target.driver !== "openai-compatible") {
    throw configError(sourcePath, `${field}.api_key_env`, "api_key_env is supported only by HTTP provider targets.");
  }
}

function validateBaseUrl(value: string, kind: AgentFlowConfiguredProviderKind, sourcePath: string, field: string): void {
  let url: URL;
  try { url = new URL(value); } catch { throw configError(sourcePath, field, "base_url must be an absolute URL."); }
  if (url.username || url.password || value.includes("?") || value.includes("#")) {
    throw configError(sourcePath, field, "base_url must not contain credentials, a query, or a fragment.");
  }
  if (kind === "frontier" && url.protocol !== "https:") {
    throw configError(sourcePath, field, "Frontier targets require HTTPS.");
  }
  if (kind === "local") {
    const loopback = url.hostname === "localhost" || url.hostname === "127.0.0.1" || url.hostname === "[::1]";
    if (!loopback || !["http:", "https:"].includes(url.protocol)) {
      throw configError(sourcePath, field, "Local targets must use an HTTP(S) loopback URL.");
    }
  }
}

function parseOverrides(values: readonly string[]): Record<string, string> {
  const result = emptyMap<string>();
  for (const value of values) {
    if (redactAgentFlowSensitiveText(value) !== value) {
      throw new AgentFlowProviderConfigError("Provider overrides must use non-secret canonical alias=target names.");
    }
    const separator = value.indexOf("=");
    if (separator <= 0 || separator === value.length - 1) {
      throw new AgentFlowProviderConfigError(`Provider override ${JSON.stringify(value)} must use alias=target.`);
    }
    const alias = value.slice(0, separator).trim();
    const target = value.slice(separator + 1).trim();
    if (!alias || !target || alias !== value.slice(0, separator) || target !== value.slice(separator + 1)) {
      throw new AgentFlowProviderConfigError(`Provider override ${JSON.stringify(value)} must use canonical alias=target names.`);
    }
    if (Object.hasOwn(result, alias)) throw new AgentFlowProviderConfigError(`Provider alias ${JSON.stringify(alias)} was overridden more than once.`);
    result[alias] = target;
  }
  return result;
}

function fingerprintTarget(
  target: AgentFlowConfiguredTarget,
  profileFingerprint?: string,
  baseConfigFingerprint?: string
): string {
  const stable = JSON.stringify({
    kind: target.kind,
    driver: target.driver,
    model: target.model,
    api_key_env: target.api_key_env ?? null,
    endpoint: target.base_url === undefined ? null : createHash("sha256").update(target.base_url).digest("hex"),
    max_output_tokens: target.max_output_tokens ?? null,
    ...(target.profile === undefined
      ? {}
      : {
          profile: target.profile,
          profile_fingerprint: profileFingerprint ?? null,
          base_config_fingerprint: baseConfigFingerprint ?? null
        }),
    ...(target.reasoning_effort === undefined
      ? {}
      : { reasoning_effort: target.reasoning_effort })
  });
  return `sha256:${createHash("sha256").update(stable).digest("hex")}`;
}

function resolveCodexProfile(
  target: AgentFlowConfiguredTarget,
  env: Readonly<Record<string, string | undefined>>,
  homeDir: string,
  repoRoot: string,
  sourcePath: string,
  targetName: string
): AgentFlowResolvedCodexProfile | undefined {
  if (target.driver !== "codex-cli" || target.profile === undefined) return undefined;
  const configuredHome = env.CODEX_HOME === undefined
    ? path.join(homeDir, ".codex")
    : env.CODEX_HOME;
  if (!configuredHome || configuredHome !== configuredHome.trim()
      || /[\u0000-\u001F\u007F-\u009F]/u.test(configuredHome)) {
    throw configError(sourcePath, `targets.${targetName}.profile`, "CODEX_HOME must contain a canonical filesystem path.");
  }
  const codexHome = path.isAbsolute(configuredHome)
    ? path.normalize(configuredHome)
    : path.resolve(repoRoot, configuredHome);
  let resolvedCodexHome: string;
  try {
    resolvedCodexHome = fs.realpathSync(codexHome);
  } catch (error) {
    throw configError(
      sourcePath,
      `targets.${targetName}.profile`,
      `Could not resolve CODEX_HOME at ${codexHome}: ${errorMessage(error)}.`
    );
  }
  const profilePath = path.join(resolvedCodexHome, `${target.profile}.config.toml`);
  const baseConfigPath = path.join(resolvedCodexHome, "config.toml");
  try {
    const profileIdentity = codexConfigIdentity(profilePath);
    const baseConfigIdentity = fs.existsSync(baseConfigPath)
      ? codexConfigIdentity(baseConfigPath)
      : undefined;
    const runtimeConfig = resolveCodexProfileRuntimeConfig(
      baseConfigIdentity?.config,
      profileIdentity.config
    );
    return Object.freeze({
      home: resolvedCodexHome,
      path: fs.realpathSync(profilePath),
      fingerprint: profileIdentity.fingerprint,
      baseConfigPath,
      ...(baseConfigIdentity === undefined
        ? {}
        : { baseConfigFingerprint: baseConfigIdentity.fingerprint }),
      mcpServerIds: Object.freeze(runtimeConfig.mcpServerIds),
      providerEnvironmentNames: Object.freeze(runtimeConfig.providerEnvironmentNames),
      requiresOpenAiAuth: runtimeConfig.requiresOpenAiAuth
    });
  } catch (error) {
    throw configError(
      sourcePath,
      `targets.${targetName}.profile`,
      `Could not read Codex profile ${JSON.stringify(target.profile)} at ${profilePath}: ${errorMessage(error)}.`
    );
  }
}

export function fingerprintAgentFlowCodexProfile(profilePath: string): string {
  return `sha256:${createHash("sha256").update(readBoundedCodexConfig(profilePath)).digest("hex")}`;
}

function codexConfigIdentity(configPath: string): { fingerprint: string; config: TomlTable } {
  const snapshot = readBoundedCodexConfig(configPath);
  let config: TomlTable;
  try {
    config = parseToml(snapshot.toString("utf8"));
  } catch (error) {
    throw new Error(`config is not valid TOML: ${redactAgentFlowSensitiveText(errorMessage(error))}`);
  }
  assertCodexConfigHasNoUnpinnedFileInputs(config);
  return {
    fingerprint: `sha256:${createHash("sha256").update(snapshot).digest("hex")}`,
    config
  };
}

function assertCodexConfigHasNoUnpinnedFileInputs(config: TomlTable): void {
  const unsupportedRootKeys = [
    "experimental_compact_prompt_file",
    "experimental_instructions_file",
    "model_catalog_json",
    "model_instructions_file",
    "project_doc_fallback_filenames",
    "project_root_markers",
    "sqlite_home"
  ].filter((key) => Object.hasOwn(config, key));
  if (unsupportedRootKeys.length > 0) {
    throw new Error(
      `path-referenced Codex settings are not supported by Agent Flow profiles: ${unsupportedRootKeys.join(", ")}`
    );
  }
  if (Object.hasOwn(config, "profile") || Object.hasOwn(config, "profiles")) {
    throw new Error("legacy Codex profile selectors and [profiles.*] tables are not supported by Agent Flow profiles");
  }
  if (Object.hasOwn(config, "skills")) {
    throw new Error("path-referenced Codex skills are not supported by Agent Flow profiles");
  }
  if (Object.hasOwn(config, "shell_environment_policy")) {
    throw new Error("shell_environment_policy is managed by Agent Flow and cannot be configured by Agent Flow profiles");
  }
  if (isTomlTable(config.permissions) && Object.hasOwn(config.permissions, "agent_flow_native")) {
    throw new Error("the reserved permissions.agent_flow_native profile cannot be configured by Agent Flow profiles");
  }
  if (isTomlTable(config.agents)) {
    const referencedAgents = Object.entries(config.agents)
      .filter(([, value]) => isTomlTable(value) && Object.hasOwn(value, "config_file"))
      .map(([name]) => name);
    if (referencedAgents.length > 0) {
      throw new Error(
        `path-referenced Codex agent configs are not supported by Agent Flow profiles: ${referencedAgents.join(", ")}`
      );
    }
  }
}

function resolveCodexProfileRuntimeConfig(
  baseConfig: TomlTable | undefined,
  profileConfig: TomlTable
): {
  mcpServerIds: string[];
  providerEnvironmentNames: string[];
  requiresOpenAiAuth: boolean;
} {
  const mcpServerIds = [...new Set([
    ...tomlTableKeys(baseConfig?.mcp_servers),
    ...tomlTableKeys(profileConfig.mcp_servers)
  ])].sort();
  if (mcpServerIds.some((serverId) => !/^[A-Za-z0-9_-]+$/.test(serverId))) {
    throw new Error("Codex MCP server IDs must contain only letters, numbers, hyphens, and underscores");
  }
  const providerName = effectiveTomlString(baseConfig, profileConfig, "model_provider") ?? "openai";
  if (providerName === "amazon-bedrock") {
    throw new Error("the amazon-bedrock Codex provider is not supported by Agent Flow profiles");
  }
  const baseProvider = tomlNamedTable(baseConfig?.model_providers, providerName);
  const profileProvider = tomlNamedTable(profileConfig.model_providers, providerName);
  const openAiBaseUrl = effectiveTomlString(baseConfig, profileConfig, "openai_base_url");
  if (openAiBaseUrl !== undefined) validateCodexProfileEndpoint(openAiBaseUrl, "openai_base_url");
  const providerBaseUrl = effectiveTomlString(baseProvider, profileProvider, "base_url");
  if (providerBaseUrl !== undefined) validateCodexProfileEndpoint(providerBaseUrl, "model provider base_url");
  if (Object.hasOwn(profileProvider ?? {}, "auth") || Object.hasOwn(baseProvider ?? {}, "auth")) {
    throw new Error("command-backed Codex provider authentication is not supported by Agent Flow profiles");
  }
  const environmentNames = new Set<string>();
  const envKey = effectiveTomlString(baseProvider, profileProvider, "env_key");
  if (envKey !== undefined) environmentNames.add(requiredEnvironmentName(envKey));
  const environmentHeaders = {
    ...tomlStringMap(baseProvider?.env_http_headers, "model provider env_http_headers"),
    ...tomlStringMap(profileProvider?.env_http_headers, "model provider env_http_headers")
  };
  for (const name of Object.values(environmentHeaders)) {
    environmentNames.add(requiredEnvironmentName(name));
  }
  return {
    mcpServerIds,
    providerEnvironmentNames: [...environmentNames].sort(),
    requiresOpenAiAuth: providerName === "openai"
      || effectiveTomlBoolean(baseProvider, profileProvider, "requires_openai_auth") === true
  };
}

function tomlTableKeys(value: unknown): string[] {
  return isTomlTable(value) ? Object.keys(value) : [];
}

function tomlNamedTable(value: unknown, name: string): TomlTable | undefined {
  if (!isTomlTable(value)) return undefined;
  const selected = value[name];
  return isTomlTable(selected) ? selected : undefined;
}

function effectiveTomlString(
  base: TomlTable | undefined,
  profile: TomlTable | undefined,
  key: string
): string | undefined {
  const value = profile?.[key] ?? base?.[key];
  if (value === undefined) return undefined;
  if (typeof value !== "string") throw new Error(`Codex ${key} must be a string`);
  return value;
}

function validateCodexProfileEndpoint(value: string, label: string): void {
  let url: URL;
  try { url = new URL(value); } catch { throw new Error(`Codex ${label} must be an absolute URL`); }
  if (url.protocol !== "https:") throw new Error(`Codex ${label} must use HTTPS`);
  if (url.username || url.password || value.includes("?") || value.includes("#")) {
    throw new Error(`Codex ${label} must not contain credentials, a query, or a fragment`);
  }
}

function effectiveTomlBoolean(
  base: TomlTable | undefined,
  profile: TomlTable | undefined,
  key: string
): boolean | undefined {
  const value = profile?.[key] ?? base?.[key];
  return typeof value === "boolean" ? value : undefined;
}

function tomlStringMap(value: unknown, label: string): Record<string, string> {
  if (value === undefined) return {};
  if (!isTomlTable(value) || Object.values(value).some((entry) => typeof entry !== "string")) {
    throw new Error(`${label} must map header names to environment variable names`);
  }
  return value as Record<string, string>;
}

function requiredEnvironmentName(value: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(value)) {
    throw new Error("Codex provider credential references must use canonical environment variable names");
  }
  return value;
}

function readBoundedCodexConfig(configPath: string): Buffer {
  let descriptor: number | undefined;
  try {
    descriptor = fs.openSync(configPath, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
    const stat = fs.fstatSync(descriptor);
    if (!stat.isFile()) throw new Error("profile must be a regular non-symlink file");
    if (stat.size > MAX_CODEX_PROFILE_BYTES) {
      throw new Error(`profile exceeds ${MAX_CODEX_PROFILE_BYTES} bytes`);
    }
    const snapshot = fs.readFileSync(descriptor);
    if (snapshot.byteLength > MAX_CODEX_PROFILE_BYTES) {
      throw new Error(`profile exceeds ${MAX_CODEX_PROFILE_BYTES} bytes`);
    }
    return snapshot;
  } finally {
    if (descriptor !== undefined) fs.closeSync(descriptor);
  }
}

function isTomlTable(value: unknown): value is TomlTable {
  return value !== null && typeof value === "object" && !Array.isArray(value) && !(value instanceof Date);
}

function emptyMap<T>(): Record<string, T> {
  return Object.create(null) as Record<string, T>;
}

function environmentValue(
  env: Readonly<Record<string, string | undefined>>,
  name: string
): string | undefined {
  if (!Object.hasOwn(env, name)) return undefined;
  const value = env[name];
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function xdgConfigHome(value: string | undefined, homeDir: string): string {
  return value !== undefined && value.trim().length > 0 && path.isAbsolute(value)
    ? value
    : path.join(homeDir, ".config");
}

function rejectUnknownFields(value: Record<string, JsonValue>, allowed: Set<string>, sourcePath: string, field: string): void {
  const unknown = Object.keys(value).filter((name) => !allowed.has(name));
  if (unknown.length > 0) throw configError(sourcePath, field, `Unknown field${unknown.length === 1 ? "" : "s"}: ${unknown.join(", ")}.`);
}

function validateName(value: string, sourcePath: string, field: string): void {
  if (!value || value !== value.trim() || /[\s=\u0000-\u001F\u007F-\u009F]/.test(value)
      || redactAgentFlowSensitiveText(value) !== value) {
    throw configError(sourcePath, field, "Names must be non-secret, non-empty, and contain no whitespace, equals signs, or control characters.");
  }
  if (RESERVED_OBJECT_NAMES.has(value)) {
    throw configError(sourcePath, field, `Name ${JSON.stringify(value)} is reserved.`);
  }
}

function requiredString(value: JsonValue | undefined, sourcePath: string, field: string): string {
  if (typeof value !== "string" || !value.trim() || value !== value.trim()) throw configError(sourcePath, field, "Value must be a canonical non-empty string.");
  return value;
}

function requiredEnum<T extends string>(value: JsonValue | undefined, allowed: readonly T[], sourcePath: string, field: string): T {
  if (typeof value !== "string" || !allowed.includes(value as T)) throw configError(sourcePath, field, `Expected one of: ${allowed.join(", ")}.`);
  return value as T;
}

function optionalStringField<K extends string>(value: Record<string, JsonValue>, key: K, sourcePath: string, field: string): Partial<Record<K, string>> {
  return value[key] === undefined ? {} : { [key]: requiredString(value[key], sourcePath, field) } as Partial<Record<K, string>>;
}

function optionalPositiveIntegerField<K extends string>(value: Record<string, JsonValue>, key: K, sourcePath: string, field: string): Partial<Record<K, number>> {
  if (value[key] === undefined) return {};
  if (!Number.isSafeInteger(value[key]) || Number(value[key]) <= 0) throw configError(sourcePath, field, "Value must be a positive integer.");
  return { [key]: Number(value[key]) } as Partial<Record<K, number>>;
}

function isMapping(value: unknown): value is Record<string, JsonValue> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function configError(sourcePath: string, field: string, message: string): AgentFlowProviderConfigError {
  return new AgentFlowProviderConfigError(`${sourcePath} (${field}): ${message}`, "AGENT_FLOW_PROVIDER_CONFIG_INVALID");
}

function isMissing(error: unknown): boolean {
  return error !== null && typeof error === "object" && "code" in error && error.code === "ENOENT";
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
