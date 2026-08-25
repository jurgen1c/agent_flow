import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { findGitRepositoryRoot } from "@jurgen1c/agent-core/repository";
import { parseYamlDocument, type JsonValue } from "@jurgen1c/agent-core/yaml";
import { redactAgentFlowSensitiveText } from "./failure_payload";

export type AgentFlowConfiguredProviderKind = "local" | "frontier";
export type AgentFlowProviderDriver =
  | "openai-responses"
  | "anthropic-messages"
  | "openai-compatible";

export interface AgentFlowConfiguredTarget {
  kind: AgentFlowConfiguredProviderKind;
  driver: AgentFlowProviderDriver;
  model: string;
  enabled: boolean;
  base_url?: string;
  api_key_env?: string;
  max_output_tokens?: number;
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

const TARGET_FIELDS = new Set([
  "kind", "driver", "model", "enabled", "base_url", "api_key_env", "max_output_tokens"
]);
const ROOT_FIELDS = new Set(["version", "workflows", "prompts", "templates", "runs", "targets", "providers"]);
const ALIAS_FIELDS = new Set(["kind", "target"]);
const RESERVED_ALIASES = new Set(["fixture", "local", "frontier"]);
const RESERVED_OBJECT_NAMES = new Set(["__proto__", "prototype", "constructor"]);
const DRIVERS = new Set<AgentFlowProviderDriver>([
  "openai-responses", "anthropic-messages", "openai-compatible"
]);

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
    bindings[alias] = Object.freeze({
      alias,
      target: targetName,
      kind: definition.kind,
      config,
      fingerprint: fingerprintTarget(config)
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
      return `${binding.alias}: ready (${binding.config.driver}, ${redactAgentFlowSensitiveText(binding.config.model)})`;
    });
  return { ok, lines };
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
      ...optionalPositiveIntegerField(value, "max_output_tokens", sourcePath, `targets.${name}.max_output_tokens`)
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

function fingerprintTarget(target: AgentFlowConfiguredTarget): string {
  const stable = JSON.stringify({
    kind: target.kind,
    driver: target.driver,
    model: target.model,
    api_key_env: target.api_key_env ?? null,
    endpoint: target.base_url === undefined ? null : createHash("sha256").update(target.base_url).digest("hex"),
    max_output_tokens: target.max_output_tokens ?? null
  });
  return `sha256:${createHash("sha256").update(stable).digest("hex")}`;
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
