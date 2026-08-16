import { parseYamlDocumentOrThrow } from "@jurgen1c/agent-core/yaml";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  AGENT_FLOW_FAILURE_REDACTION_MARKER,
  agentFlowInputKeyLooksSensitive,
  redactAgentFlowSensitiveInputText
} from "./failure_payload";
import type { AgentFlowRunStateValue } from "./run_state";
import type { AgentFlowWorkflow, AgentFlowYamlMapping } from "./workflow";

export type AgentFlowSensitiveInputMode = "allow" | "deny" | "redact";

export class AgentFlowSensitiveInputError extends Error {
  readonly code = "AGENT_FLOW_SENSITIVE_INPUT";

  constructor(message: string) {
    super(message);
    this.name = "AgentFlowSensitiveInputError";
  }
}

export function agentFlowSensitiveInputMode(workflow: AgentFlowWorkflow): AgentFlowSensitiveInputMode {
  const configured = mapping(workflow.policies)?.sensitive_inputs;
  if (configured === undefined) return "redact";
  if (configured === "allow" || configured === "deny" || configured === "redact") return configured;
  throw new AgentFlowSensitiveInputError("policies.sensitive_inputs must be allow, deny, or redact.");
}

export function agentFlowPathLooksSensitive(value: string): boolean {
  return agentFlowPathAnalysisLooksSensitive(value, true);
}

function agentFlowPathAnalysisLooksSensitive(value: string, includeCredentialMaterial: boolean): boolean {
  const analysis = agentFlowPathCandidates(value);
  const candidates = analysis.candidates.flatMap((candidate) => {
    const normalized = path.posix.normalize(candidate.replaceAll("\\", "/"));
    return normalized === candidate ? [candidate] : [candidate, normalized];
  });
  return analysis.unresolvedEncoding || candidates.some((candidate) =>
    agentFlowPathCandidateLooksSensitive(candidate)
      || (includeCredentialMaterial && redactAgentFlowSensitiveInputText(candidate) !== candidate)
  );
}

function agentFlowPathCandidateLooksSensitive(candidate: string): boolean {
  const candidates = [candidate];
  let withoutBackupSuffix = candidate;
  for (let index = 0; index < 4; index += 1) {
    const stripped = withoutBackupSuffix.replace(/(?:~|\.(?:bak|backup|old|orig|save|sw[op]|tmp))$/i, "");
    if (stripped === withoutBackupSuffix) break;
    candidates.push(stripped);
    withoutBackupSuffix = stripped;
  }
  return candidates.some((value) =>
    /(?:^|[/\\])(?:\.env(?:rc|[._-][^/\\]+)?|[^/\\]+\.env(?:[._-][^/\\]+)?)(?:[/\\]|$)/i.test(value)
      || /(?:^|[/\\])config[/\\](?:master|credentials[/\\][^/\\]+)\.key(?:[/\\]|$)/i.test(value)
      || /(?:^|[/\\])etc[/\\](?:g?shadow|passwd|sudoers(?:\.d[/\\][^/\\]+)?)(?:[/\\]|$)/i.test(value)
      || /(?:^|[/\\])proc[/\\](?:self|thread-self|[0-9]+)(?:[/\\]task[/\\][0-9]+)?[/\\](?:auxv|cmdline|environ|maps|mem|mountinfo|smaps|stack|syscall|fd(?:info)?(?:[/\\][^/\\]+)?)(?:[/\\]|$)/i.test(value)
      || /(?:^|[/\\])dev[/\\](?:fd(?:[/\\][^/\\]+)?|stdin|stdout|stderr)(?:[/\\]|$)/i.test(value)
      || /(?:^|[/\\])\.ssh(?:[/\\]|$)/i.test(value)
      || /(?:^|[/\\])\.gnupg(?:[/\\]|$)/i.test(value)
      || /(?:^|[/\\])(?:\.docker[/\\]config\.json|\.kube[/\\]config|\.aws[/\\]credentials|\.azure[/\\]accessTokens\.json|\.config[/\\]containers[/\\]auth\.json)(?:[/\\]|$)/i.test(value)
      || /(^|[/\\._-])(\.env|\.htpasswd|\.netrc|\.npmrc|\.pgpass|\.pypirc|\.my\.cnf|credentials|id_(?:rsa|dsa|ecdsa|ed25519)|private[_-]?key|secrets?)([/\\._-]|$)/i.test(value)
  );
}

export function preflightAgentFlowTextInputPath(
  workflow: AgentFlowWorkflow,
  label: string,
  path?: string,
  contentType?: string
): void {
  const mode = agentFlowSensitiveInputMode(workflow);
  if (mode === "allow") return;

  if (path !== undefined && agentFlowPathLooksSensitive(path)) {
    if (mode === "deny") {
      throw new AgentFlowSensitiveInputError(`${label} uses a secret-like path and is denied by policies.sensitive_inputs.`);
    }
    throw new AgentFlowSensitiveInputError(
      `${label} uses a secret-like path whose complete redaction cannot be verified; set policies.sensitive_inputs to allow only after review.`
    );
  }

  if (structuredInputFormat(path, contentType) === undefined && declaresUnsupportedStructuredInput(path, contentType)) {
    throw new AgentFlowSensitiveInputError(
      `${label} declares a structured format that has no supported safe sanitizer; set policies.sensitive_inputs to allow only after review.`
    );
  }
}

export function secureAgentFlowTextInput(
  workflow: AgentFlowWorkflow,
  label: string,
  value: string,
  path?: string,
  contentType?: string
): { value: string; redacted: boolean } {
  const mode = agentFlowSensitiveInputMode(workflow);
  if (mode === "allow") return { value, redacted: false };

  preflightAgentFlowTextInputPath(workflow, label, path, contentType);
  const format = structuredInputFormat(path, contentType) ?? sniffedJsonInputFormat(value);
  if (format !== undefined) {
    const parseableValue = value.startsWith("\uFEFF") ? value.slice(1) : value;
    let hasLossyJsonNumber = false;
    let hasDuplicateJsonKey = false;
    let parsed: unknown;
    try {
      hasLossyJsonNumber = format === "json" && jsonHasLossyNumberToken(parseableValue);
      hasDuplicateJsonKey = format === "json" && jsonHasDuplicateObjectKey(parseableValue);
      parsed = format === "json" ? JSON.parse(parseableValue) : parseYamlDocumentOrThrow(parseableValue);
    } catch (error) {
      if (error instanceof AgentFlowSensitiveInputError) throw error;
      throw new AgentFlowSensitiveInputError(
        `${label} is declared as structured content but could not be parsed and sanitized safely.`
      );
    }
    const secured = secureJsonValue(parsed);
    if (!secured.redacted) {
      if (structuredSourceContainsSensitiveContent(value)) {
        throw new AgentFlowSensitiveInputError(
          `${label} contains secret-like structured source text that cannot be sanitized safely.`
        );
      }
      return { value, redacted: false };
    }
    if (hasLossyJsonNumber) {
      throw new AgentFlowSensitiveInputError(
        "Structured JSON input contains a number outside the safe lossless redaction range."
      );
    }
    if (hasDuplicateJsonKey) {
      throw new AgentFlowSensitiveInputError(
        "Structured JSON input contains duplicate object keys and cannot be redacted without changing its meaning."
      );
    }
    if (mode === "deny") {
      throw new AgentFlowSensitiveInputError(`${label} contains secret-like content and is denied by policies.sensitive_inputs.`);
    }
    if (format === "yaml") {
      throw new AgentFlowSensitiveInputError(
        `${label} contains secret-like YAML data that cannot be reserialized safely; set policies.sensitive_inputs to allow only after review.`
      );
    }
    return { value: `${JSON.stringify(secured.value)}\n`, redacted: true };
  }

  if (hasSecretBearingUnsupportedStructuredContent(value)) {
    throw new AgentFlowSensitiveInputError(
      `${label} contains secret-bearing structured content that has no supported safe sanitizer; set policies.sensitive_inputs to allow only after review.`
    );
  }

  if (hasIndexedSecretAssignment(value)) {
    throw new AgentFlowSensitiveInputError(
      `${label} contains an indexed secret-like assignment that cannot be redacted safely; set policies.sensitive_inputs to allow only after review.`
    );
  }

  if (hasMultilineSecretAssignment(value)) {
    throw new AgentFlowSensitiveInputError(
      `${label} contains a multiline secret-like assignment whose complete redaction cannot be verified; set policies.sensitive_inputs to allow only after review.`
    );
  }
  if (hasUnbalancedPrivateKeyMaterial(value)) {
    throw new AgentFlowSensitiveInputError(
      `${label} contains unterminated or mismatched private-key material whose complete redaction cannot be verified; set policies.sensitive_inputs to allow only after review.`
    );
  }
  if (embeddedSensitivePathToken(value)) {
    throw new AgentFlowSensitiveInputError(
      `${label} references a secret-like path that cannot be sent to an adapter without explicit policies.sensitive_inputs: allow.`
    );
  }

  const secured = redactAgentFlowSensitiveInputText(value);
  if (hasAmbiguousCredentialHeaderAfterRedaction(secured)) {
    throw new AgentFlowSensitiveInputError(
      `${label} contains an ambiguous credential header whose complete redaction cannot be verified; set policies.sensitive_inputs to allow only after review.`
    );
  }
  const containsSensitiveContent = secured !== value;
  if (mode === "deny" && containsSensitiveContent) {
    throw new AgentFlowSensitiveInputError(`${label} contains secret-like content and is denied by policies.sensitive_inputs.`);
  }
  return { value: secured, redacted: containsSensitiveContent };
}

export function assertAgentFlowAdapterStringSafe(
  workflow: AgentFlowWorkflow,
  label: string,
  value: string,
  options: { path?: boolean } = {}
): void {
  if (options.path === true) {
    preflightAgentFlowTextInputPath(workflow, label, value);
  }
  const secured = secureAgentFlowTextInput(workflow, label, value);
  if (secured.redacted) {
    throw new AgentFlowSensitiveInputError(
      `${label} contains secret-like content and cannot be safely rewritten before adapter invocation; set policies.sensitive_inputs to allow only after review.`
    );
  }
}

function agentFlowPathCandidates(value: string): { candidates: string[]; unresolvedEncoding: boolean } {
  const candidates = [value];
  const trimmed = value.trim();
  if (/^[A-Za-z]:[^/\\]/.test(trimmed)) candidates.push(trimmed.slice(2));
  if (!/^(?:file:|[A-Za-z][A-Za-z0-9+.-]*:\/\/)/i.test(trimmed)) {
    return { candidates, unresolvedEncoding: false };
  }

  try {
    const url = new URL(trimmed);
    const fragment = url.hash.startsWith("#") ? url.hash.slice(1) : url.hash;
    const fragmentParameters = new URLSearchParams(fragment);
    const encodedCandidates = [
      url.pathname,
      url.search,
      url.hash,
      fragment,
      ...url.searchParams.keys(),
      ...url.searchParams.values(),
      ...fragmentParameters.keys(),
      ...fragmentParameters.values()
    ];
    candidates.push(...encodedCandidates.filter((candidate) => candidate.length > 0));
    if (url.protocol === "file:") {
      try {
        candidates.push(fileURLToPath(url));
      } catch {
        // Decode below so non-local and unusually encoded file URLs still fail closed.
      }
    }
    for (const encoded of encodedCandidates) {
      let decoded = encoded;
      for (let attempt = 0; attempt < 8; attempt += 1) {
        let next: string;
        try {
          next = decodeURIComponent(decoded);
        } catch {
          return { candidates, unresolvedEncoding: true };
        }
        if (next === decoded) break;
        candidates.push(next);
        decoded = next;
        if (attempt === 7) {
          try {
            if (decodeURIComponent(decoded) !== decoded) return { candidates, unresolvedEncoding: true };
          } catch {
            return { candidates, unresolvedEncoding: true };
          }
        }
      }
    }
    return { candidates, unresolvedEncoding: false };
  } catch {
    return { candidates, unresolvedEncoding: true };
  }
}

function structuredSourceContainsSensitiveContent(value: string): boolean {
  return hasMultilineSecretAssignment(value)
    || hasUnbalancedPrivateKeyMaterial(value)
    || redactAgentFlowSensitiveInputText(value) !== value;
}

function hasSecretBearingUnsupportedStructuredContent(value: string): boolean {
  for (const match of value.matchAll(/<\/?([A-Za-z_][A-Za-z0-9_.:-]*)\b[^>]*>/g)) {
    if (structuredNameLooksSensitive(match[1]!)) return true;
    if (/\btype\s*=\s*(?:(["'])password\1|password(?=[\s/>]))/i.test(match[0])) return true;
    for (const attribute of match[0].matchAll(/\b(?:key|name)\s*=\s*(?:(["'])([^"']+)\1|([^\s"'=<>`]+))/gi)) {
      if (structuredNameLooksSensitive(attribute[2] ?? attribute[3] ?? "")) return true;
    }
  }

  const tomlAssignment = /^[ \t]*((?:"[^"\r\n]+"|'[^'\r\n]+'|[A-Za-z0-9_-]+)(?:[ \t]*\.[ \t]*(?:"[^"\r\n]+"|'[^'\r\n]+'|[A-Za-z0-9_-]+))*)[ \t]*=/gm;
  for (const match of value.matchAll(tomlAssignment)) {
    if (!match[1]!.includes(".") && !/^["']/.test(match[1]!)) continue;
    for (const segment of match[1]!.matchAll(/"([^"\r\n]+)"|'([^'\r\n]+)'|([A-Za-z0-9_-]+)/g)) {
      if (segment[1]?.includes("\\")) return true;
      if (structuredNameLooksSensitive(segment[1] ?? segment[2] ?? segment[3] ?? "")) return true;
    }
  }
  for (const match of value.matchAll(/^[ \t]*\[([^\]\r\n]+)]/gm)) {
    if (match[1]!.split(".").some(structuredNameLooksSensitive)) return true;
  }

  if (/^--[^\r\n]+/m.test(value) && /\bContent-Disposition\s*:\s*form-data\b/i.test(value)) {
    for (const match of value.matchAll(/\bname\s*=\s*(?:(["'])([^"']+)\1|([^\s"'=;]+))/gi)) {
      if (structuredNameLooksSensitive(match[2] ?? match[3] ?? "")) return true;
    }
  }
  return false;
}

function structuredNameLooksSensitive(value: string): boolean {
  const localName = (value.split(":").at(-1) ?? value).trim().replace(/^(?:"([^"]*)"|'([^']*)')$/, "$1$2");
  return agentFlowInputKeyLooksSensitive(localName);
}

function hasIndexedSecretAssignment(value: string): boolean {
  const assignment = /(?:^|[\s;&|])(?:(?:export|readonly|declare|typeset|local|env)[ \t]+)*([A-Za-z_][A-Za-z0-9_.-]*)((?:\[[^\]\r\n]+])+)(?=[ \t]*(?:::=|::=|[+?!:-]=|[:=]))/gm;
  for (const match of value.matchAll(assignment)) {
    if (secretObjectKey(match[1]!)) return true;
    for (const indexedKey of match[2]!.matchAll(/\[([^\]\r\n]+)]/g)) {
      if (structuredNameLooksSensitive(indexedKey[1]!)) return true;
    }
  }
  return false;
}

function hasMultilineSecretAssignment(value: string): boolean {
  const lines = value.split(/\r\n|[\r\n]/);
  const assignment = /(?:^|(?<=[\s;&|]))(?:(?:export|readonly|declare|typeset|local|env)[ \t]+)*(?:\$env:)?(?:([A-Za-z_][A-Za-z0-9_.-]*)|(["'])([A-Za-z_][A-Za-z0-9_.-]*)\2)[ \t]*(?:::=|::=|[+?!:-]=|[:=])[ \t]*/gi;
  for (let index = 0; index < lines.length - 1; index += 1) {
    const line = lines[index]!;
    for (const match of line.matchAll(assignment)) {
      if (!secretObjectKey(match[1] ?? match[3] ?? "")) continue;
      const assignedValue = line.slice(match.index + match[0].length);
      const nextLine = lines[index + 1]!;
      if (assignmentMayContinue(assignedValue, nextLine)) return true;
    }
  }
  return false;
}

function assignmentMayContinue(assignedValue: string, nextLine: string): boolean {
  const trimmed = assignedValue.trim();
  if (/^[|>](?:[1-9]|[+-]){0,2}(?:[ \t]+#.*)?$/.test(trimmed)) return true;
  if (trimmed.length === 0 || /^[ \t]+/.test(nextLine) || /\\$/.test(assignedValue)) return true;
  if (/^<<[-~]?[ \t]*(?:["'][^"'\r\n]+["']|[A-Za-z_][A-Za-z0-9_]*)$/.test(trimmed)) return true;
  if (/^@["']/.test(trimmed) && !/["']@[ \t]*$/.test(trimmed)) return true;
  if ((trimmed.startsWith("$(") || trimmed.startsWith("`")) && hasShellHeredocOperator(trimmed)) return true;
  for (const [open, close] of [["$(", ")"], ["${", "}"]] as const) {
    if (trimmed.startsWith(open) && !shellDelimitedValueCloses(trimmed, open.at(-1)!, close)) return true;
  }
  for (const [open, close] of [["[", "]"], ["{", "}"], ["(", ")"]] as const) {
    if (trimmed.startsWith(open) && !trimmed.endsWith(close)) return true;
  }
  for (const quote of ["\"\"\"", "'''"] as const) {
    const occurrences = trimmed.split(quote).length - 1;
    if (occurrences % 2 === 1) return true;
  }
  return hasUnclosedShellQuote(trimmed);
}

function hasShellHeredocOperator(value: string): boolean {
  let quote: "\"" | "'" | undefined;
  let escaped = false;
  for (let index = 0; index < value.length - 1; index += 1) {
    const character = value[index]!;
    if (escaped) {
      escaped = false;
      continue;
    }
    if (character === "\\" && quote !== "'") {
      escaped = true;
      continue;
    }
    if (quote !== undefined) {
      if (character === quote) quote = undefined;
      continue;
    }
    if (character === "\"" || character === "'") {
      quote = character;
      continue;
    }
    if (character !== "<" || value[index + 1] !== "<") continue;
    const suffix = value.slice(index + 2);
    if (/^-?[ \t]*(?:["'][^"'\r\n]+["']|[A-Za-z_][A-Za-z0-9_]*)/.test(suffix)) return true;
  }
  return false;
}

function shellDelimitedValueCloses(value: string, open: string, close: string): boolean {
  let depth = 0;
  let quote: "\"" | "'" | "`" | undefined;
  let escaped = false;
  for (const character of value) {
    if (escaped) {
      escaped = false;
      continue;
    }
    if (character === "\\" && quote !== "'") {
      escaped = true;
      continue;
    }
    if (quote !== undefined) {
      if (character === quote) quote = undefined;
      continue;
    }
    if (character === "\"" || character === "'" || character === "`") {
      quote = character;
      continue;
    }
    if (character === open) depth += 1;
    if (character === close && --depth === 0) return true;
  }
  return false;
}

function hasUnclosedShellQuote(value: string): boolean {
  let quote: "\"" | "'" | "`" | undefined;
  let escaped = false;
  for (const character of value) {
    if (escaped) {
      escaped = false;
      continue;
    }
    if (character === "\\" && quote !== "'") {
      escaped = true;
      continue;
    }
    if (quote !== undefined) {
      if (character === quote) quote = undefined;
      continue;
    }
    if (character === "\"" || character === "'" || character === "`") quote = character;
  }
  return quote !== undefined;
}

function hasUnbalancedPrivateKeyMaterial(value: string): boolean {
  const labels: string[] = [];
  for (const match of value.matchAll(/-----(BEGIN|END) ([A-Z0-9 ]*PRIVATE KEY(?: BLOCK)?)-----/g)) {
    const [, kind, label] = match;
    if (kind === "BEGIN") {
      labels.push(label!);
    } else if (labels.pop() !== label) {
      return true;
    }
  }
  return labels.length > 0;
}

function hasAmbiguousCredentialHeaderAfterRedaction(value: string): boolean {
  const header = /(?:^|\\[nrt]|[^A-Za-z0-9_-])(?:Proxy-)?Authorization[ \t]*:[ \t]*|(?:^|\\[nrt]|[^A-Za-z0-9_-])(?:Set-)?Cookie[ \t]*:[ \t]*/gim;
  for (const match of value.matchAll(header)) {
    const remainderStart = match.index + match[0].length;
    const lineBreak = value.slice(remainderStart).search(/[\r\n]/);
    const remainder = value.slice(
      remainderStart,
      lineBreak === -1 ? value.length : remainderStart + lineBreak
    ).trim();
    if (!/^(?:[A-Za-z][A-Za-z0-9_-]*[ \t]+)?\[REDACTED\]$/.test(remainder)) return true;
  }
  return false;
}

export function secureAgentFlowByteInput(
  workflow: AgentFlowWorkflow,
  label: string,
  value: Uint8Array,
  path?: string,
  contentType?: string
): { value: Uint8Array; redacted: boolean } {
  const mode = agentFlowSensitiveInputMode(workflow);
  if (mode === "allow") return { value: Uint8Array.from(value), redacted: false };

  let decoded: string;
  try {
    decoded = new TextDecoder("utf-8", { fatal: true }).decode(value);
  } catch {
    throw new AgentFlowSensitiveInputError(
      `${label} is not UTF-8 text and cannot be inspected or redacted safely; set policies.sensitive_inputs to allow only after review.`
    );
  }

  const secured = secureAgentFlowTextInput(workflow, label, decoded, path, contentType);
  return {
    value: secured.redacted ? Buffer.from(secured.value, "utf8") : Uint8Array.from(value),
    redacted: secured.redacted
  };
}

export function secureAgentFlowReferencedByteInput(
  workflow: AgentFlowWorkflow,
  label: string,
  value: Uint8Array,
  path: string | undefined,
  contentType: string | undefined,
  sensitiveProvenance: boolean
): { value: Uint8Array; redacted: boolean } {
  if (!sensitiveProvenance) {
    return secureAgentFlowByteInput(workflow, label, value, path, contentType);
  }

  const mode = agentFlowSensitiveInputMode(workflow);
  if (mode === "allow") return { value: Uint8Array.from(value), redacted: false };
  preflightAgentFlowTextInputPath(workflow, label, path, contentType);
  if (mode === "deny") {
    throw new AgentFlowSensitiveInputError(
      `${label} originates from a secret-like input and is denied by policies.sensitive_inputs.`
    );
  }

  const format = structuredInputFormat(path, contentType);
  const replacement = format === "json" || format === "yaml"
    ? `${JSON.stringify(AGENT_FLOW_FAILURE_REDACTION_MARKER)}\n`
    : AGENT_FLOW_FAILURE_REDACTION_MARKER;
  return { value: Buffer.from(replacement, "utf8"), redacted: true };
}

export function secureAgentFlowJsonInput(
  workflow: AgentFlowWorkflow,
  label: string,
  value: Record<string, AgentFlowRunStateValue>
): { value: Record<string, AgentFlowRunStateValue>; redacted: boolean } {
  const mode = agentFlowSensitiveInputMode(workflow);
  if (mode === "allow") return { value: structuredClone(value), redacted: false };
  const secured = secureJsonValue(value);
  if (mode === "deny" && secured.redacted) {
    throw new AgentFlowSensitiveInputError(`${label} contains secret-like content and is denied by policies.sensitive_inputs.`);
  }
  return { value: secured.value as Record<string, AgentFlowRunStateValue>, redacted: secured.redacted };
}

export function secureAgentFlowSensitiveJsonInputValue(
  workflow: AgentFlowWorkflow,
  label: string,
  value: AgentFlowRunStateValue
): { value: AgentFlowRunStateValue; redacted: boolean } {
  const secured = secureAgentFlowJsonInput(workflow, label, { credential: value });
  return { value: secured.value.credential!, redacted: secured.redacted };
}

function secureJsonValue(
  value: unknown,
  key?: string,
  ancestors: Set<object> = new Set(),
  depth = 0
): { value: AgentFlowRunStateValue; redacted: boolean } {
  if (depth > 50) throw new AgentFlowSensitiveInputError("Structured input nesting exceeds the safe redaction limit.");
  if (key !== undefined && secretObjectKey(key)) {
    return { value: "[REDACTED]", redacted: true };
  }
  if (typeof value === "string" && hasMultilineSecretAssignment(value)) {
    throw new AgentFlowSensitiveInputError(
      "Structured input contains a multiline secret-like assignment that cannot be sent to an adapter without explicit policies.sensitive_inputs: allow."
    );
  }
  if (typeof value === "string" && hasUnbalancedPrivateKeyMaterial(value)) {
    throw new AgentFlowSensitiveInputError(
      "Structured input contains unterminated or mismatched private-key material that cannot be sent to an adapter without explicit policies.sensitive_inputs: allow."
    );
  }
  if (typeof value === "string" && hasIndexedSecretAssignment(value)) {
    throw new AgentFlowSensitiveInputError(
      "Structured input contains an indexed secret-like assignment that cannot be sent to an adapter without explicit policies.sensitive_inputs: allow."
    );
  }
  if (typeof value === "string" && hasSecretBearingUnsupportedStructuredContent(value)) {
    throw new AgentFlowSensitiveInputError(
      "Structured input contains secret-bearing structured content that has no supported safe sanitizer; set policies.sensitive_inputs to allow only after review."
    );
  }
  if (typeof value === "string"
      && hasAmbiguousCredentialHeaderAfterRedaction(redactAgentFlowSensitiveInputText(value))) {
    throw new AgentFlowSensitiveInputError(
      "Structured input contains an ambiguous credential header whose complete redaction cannot be verified; set policies.sensitive_inputs to allow only after review."
    );
  }
  if (typeof value === "string" && structuredValueReferencesSensitivePath(value, key)) {
    throw new AgentFlowSensitiveInputError(
      "Structured input references a secret-like path that cannot be sent to an adapter without explicit policies.sensitive_inputs: allow."
    );
  }
  if (typeof value === "string") {
    const nestedJson = secureNestedJsonString(value, ancestors, depth);
    if (nestedJson !== undefined) return nestedJson;
    const secured = redactAgentFlowSensitiveInputText(value);
    if (hasAmbiguousCredentialHeaderAfterRedaction(secured)) {
      throw new AgentFlowSensitiveInputError(
        "Structured input contains an ambiguous credential header whose complete redaction cannot be verified; set policies.sensitive_inputs to allow only after review."
      );
    }
    return { value: secured, redacted: secured !== value };
  }
  if (Array.isArray(value)) {
    if (ancestors.has(value)) throw new AgentFlowSensitiveInputError("Structured input contains a cycle.");
    ancestors.add(value);
    try {
      let redacted = false;
      const entries = value.map((entry) => {
        const secured = secureJsonValue(entry, key, ancestors, depth + 1);
        redacted ||= secured.redacted;
        return secured.value;
      });
      return { value: entries, redacted };
    } finally {
      ancestors.delete(value);
    }
  }
  if (value !== null && typeof value === "object") {
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) {
      throw new AgentFlowSensitiveInputError("Structured input must contain only plain objects.");
    }
    if (ancestors.has(value)) throw new AgentFlowSensitiveInputError("Structured input contains a cycle.");
    ancestors.add(value);
    try {
      let redacted = false;
      const entries = Object.entries(value).map(([entryKey, entryValue]) => {
        if (redactAgentFlowSensitiveInputText(entryKey) !== entryKey) {
          throw new AgentFlowSensitiveInputError(
            "Structured input contains secret material in an object key that cannot be sent to an adapter without explicit policies.sensitive_inputs: allow."
          );
        }
        if (structuredObjectKeyLooksLikeSensitivePath(entryKey)) {
          throw new AgentFlowSensitiveInputError(
            "Structured input contains a secret-like path as an object key that cannot be sent to an adapter without explicit policies.sensitive_inputs: allow."
          );
        }
        const secured = secureJsonValue(entryValue, entryKey, ancestors, depth + 1);
        redacted ||= secured.redacted;
        return [entryKey, secured.value] as const;
      });
      return { value: Object.fromEntries(entries), redacted };
    } finally {
      ancestors.delete(value);
    }
  }
  if (value === null || typeof value === "boolean") return { value, redacted: false };
  if (typeof value === "number" && Number.isFinite(value)) return { value, redacted: false };
  throw new AgentFlowSensitiveInputError("Structured input contains a value that cannot be sanitized safely.");
}

function secureNestedJsonString(
  value: string,
  ancestors: Set<object>,
  depth: number
): { value: string; redacted: boolean } | undefined {
  if (sniffedJsonInputFormat(value) === undefined) return undefined;
  let parsed: unknown;
  let hasLossyJsonNumber = false;
  let hasDuplicateJsonKey = false;
  const parseableValue = value.startsWith("\uFEFF") ? value.slice(1) : value;
  try {
    hasLossyJsonNumber = jsonHasLossyNumberToken(parseableValue);
    hasDuplicateJsonKey = jsonHasDuplicateObjectKey(parseableValue);
    parsed = JSON.parse(parseableValue);
  } catch {
    throw new AgentFlowSensitiveInputError(
      "Structured input contains a JSON-shaped string that could not be parsed and sanitized safely."
    );
  }
  const secured = secureJsonValue(parsed, undefined, ancestors, depth + 1);
  if (!secured.redacted) {
    if (structuredSourceContainsSensitiveContent(value)) {
      throw new AgentFlowSensitiveInputError(
        "Structured input contains secret-like JSON source text that cannot be sanitized safely."
      );
    }
    return { value, redacted: false };
  }
  if (hasLossyJsonNumber) {
    throw new AgentFlowSensitiveInputError(
      "Structured JSON string contains a number outside the safe lossless redaction range."
    );
  }
  if (hasDuplicateJsonKey) {
    throw new AgentFlowSensitiveInputError(
      "Structured JSON string contains duplicate object keys and cannot be redacted without changing its meaning."
    );
  }
  return { value: JSON.stringify(secured.value), redacted: true };
}

function structuredObjectKeyLooksLikeSensitivePath(value: string): boolean {
  if (!agentFlowPathLooksSensitive(value)) return false;
  return !secretObjectKey(value) || /[./\\:]|^file:/i.test(value);
}

function structuredValueReferencesSensitivePath(value: string, key: string | undefined): boolean {
  if (agentFlowPathAnalysisLooksSensitive(value, false)) return true;
  if (embeddedSensitivePathToken(value)) return true;
  if (!structuredPathKey(key) && !looksLikePath(value)) return false;
  return agentFlowPathLooksSensitive(value);
}

function embeddedSensitivePathToken(value: string): boolean {
  return value
    .split(/[\s"'`,;|&<>()\[\]{}]+/)
    .map((candidate) => candidate.replace(/^[=:]+|[=:]+$/g, ""))
    .filter((candidate) => candidate.length > 0)
    .flatMap(embeddedPathCandidateVariants)
    .flatMap((candidate) => {
      const withoutTerminalPunctuation = candidate.replace(/[.!?]+$/g, "");
      return withoutTerminalPunctuation.length > 0 && withoutTerminalPunctuation !== candidate
        ? [candidate, withoutTerminalPunctuation]
        : [candidate];
    })
    .filter((candidate) => looksLikePath(candidate)
      || /^(?:\.env(?:rc|[._-].+)?|\.netrc|\.npmrc|\.pgpass|\.pypirc|\.my\.cnf|id_(?:rsa|dsa|ecdsa|ed25519)|private[_-]?key)$/i.test(candidate))
    .some((candidate) => agentFlowPathAnalysisLooksSensitive(candidate, /%[0-9A-Fa-f]{2}/.test(candidate)));
}

function embeddedPathCandidateVariants(candidate: string): string[] {
  const variants = [candidate];
  const assignment = /^([^:=]+)[:=](.+)$/.exec(candidate);
  if (assignment !== null && structuredPathKey(assignment[1])) variants.push(assignment[2]!);
  for (const encoded of [...variants]) {
    let decoded = encoded;
    for (let attempt = 0; attempt < 8 && /%[0-9A-Fa-f]{2}/.test(decoded); attempt += 1) {
      try {
        const next = decodeURIComponent(decoded);
        if (next === decoded) break;
        variants.push(next);
        decoded = next;
      } catch {
        break;
      }
    }
  }
  return [...new Set(variants)];
}

function structuredPathKey(value: string | undefined): boolean {
  if (value === undefined) return false;
  return /^(?:artifact|file|filename|filepath|input|output|path|source|uri|url)s?$/
    .test(value.replace(/([a-z0-9])([A-Z])/g, "$1_$2").replace(/[^A-Za-z0-9]+/g, "_").toLowerCase());
}

function looksLikePath(value: string): boolean {
  const trimmed = value.trim();
  return /^file:/i.test(trimmed)
    || /[/\\]/.test(trimmed)
    || /(?:^|[/\\])[^/\\\s]+\.[A-Za-z0-9]{1,16}(?:[?#].*)?$/.test(trimmed);
}

function structuredInputFormat(path: string | undefined, contentType: string | undefined): "json" | "yaml" | undefined {
  const mediaType = contentType?.split(";", 1)[0]?.trim().toLowerCase();
  if (mediaType?.includes("json") || path?.toLowerCase().endsWith(".json")) return "json";
  if (mediaType?.includes("yaml") || /\.ya?ml$/i.test(path ?? "")) return "yaml";
  return undefined;
}

function sniffedJsonInputFormat(value: string): "json" | undefined {
  const trimmed = value.trimStart();
  if (/^\{\s*(?:["},/])/.test(trimmed)) return "json";
  if (/^\[\s*(?:["{\[\]},/\-0-9]|true\b|false\b|null\b)/.test(trimmed)) return "json";
  return undefined;
}

function declaresUnsupportedStructuredInput(path: string | undefined, contentType: string | undefined): boolean {
  if (/\.(?:csv|html?|ini|properties|toml|xml)$/i.test(path ?? "")) return true;
  const mediaType = contentType?.split(";", 1)[0]?.trim().toLowerCase();
  if (mediaType === undefined || mediaType.length === 0) return false;
  if (["text/plain", "text/markdown", "text/x-markdown", "application/octet-stream"].includes(mediaType)) {
    return false;
  }
  return mediaType.startsWith("multipart/")
    || mediaType.startsWith("message/")
    || mediaType.startsWith("text/")
    || mediaType.startsWith("application/")
    || mediaType.includes("+xml")
    || mediaType.includes("+toml");
}

function jsonHasLossyNumberToken(value: string): boolean {
  let inString = false;
  let escaped = false;
  for (let index = 0; index < value.length; index += 1) {
    const character = value[index]!;
    if (inString) {
      if (escaped) escaped = false;
      else if (character === "\\") escaped = true;
      else if (character === '"') inString = false;
      continue;
    }
    if (character === '"') {
      inString = true;
      continue;
    }
    if (character !== "-" && (character < "0" || character > "9")) continue;
    const token = value.slice(index).match(/^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?/)?.[0];
    if (token === undefined) continue;
    index += token.length - 1;
    if (jsonNumberIsLossy(token)) return true;
  }
  return false;
}

function jsonHasDuplicateObjectKey(value: string): boolean {
  type JsonContainer =
    | { type: "array" }
    | { type: "object"; expectingKey: boolean; keys: Set<string> };
  const containers: JsonContainer[] = [];
  for (let index = 0; index < value.length; index += 1) {
    const character = value[index]!;
    if (character === "{") {
      containers.push({ type: "object", expectingKey: true, keys: new Set() });
      continue;
    }
    if (character === "[") {
      containers.push({ type: "array" });
      continue;
    }
    if (character === "}" || character === "]") {
      containers.pop();
      continue;
    }
    if (character === ",") {
      const container = containers.at(-1);
      if (container?.type === "object") container.expectingKey = true;
      continue;
    }
    if (character !== '"') continue;

    const start = index;
    let escaped = false;
    for (index += 1; index < value.length; index += 1) {
      const stringCharacter = value[index]!;
      if (escaped) {
        escaped = false;
      } else if (stringCharacter === "\\") {
        escaped = true;
      } else if (stringCharacter === '"') {
        break;
      }
    }
    const container = containers.at(-1);
    if (container?.type !== "object" || !container.expectingKey) continue;
    const key = JSON.parse(value.slice(start, index + 1)) as string;
    if (container.keys.has(key)) return true;
    container.keys.add(key);
    container.expectingKey = false;
  }
  return false;
}

function jsonNumberIsLossy(token: string): boolean {
  const parsed = Number(token);
  if (!Number.isFinite(parsed) || Object.is(parsed, -0)) return true;
  return canonicalJsonNumber(token) !== canonicalJsonNumber(JSON.stringify(parsed));
}

function canonicalJsonNumber(token: string): string {
  const parts = /^(-?)([0-9]+)(?:\.([0-9]+))?(?:[eE]([+-]?[0-9]+))?$/.exec(token)!;
  let digits = `${parts[2]}${parts[3] ?? ""}`.replace(/^0+/, "");
  if (digits.length === 0) return "0";
  const rawExponent = parts[4] ?? "0";
  const exponentMagnitude = rawExponent.replace(/^[+-]/, "").replace(/^0+/, "") || "0";
  let exponent = BigInt(`${rawExponent.startsWith("-") ? "-" : ""}${exponentMagnitude}`)
    - BigInt(parts[3]?.length ?? 0);
  const trailingZeros = digits.match(/0+$/)?.[0].length ?? 0;
  if (trailingZeros > 0) {
    digits = digits.slice(0, -trailingZeros);
    exponent += BigInt(trailingZeros);
  }
  return `${parts[1] === "-" ? "-" : "+"}:${digits}:${exponent}`;
}

function secretObjectKey(value: string): boolean {
  const normalized = value.replace(/([a-z0-9])([A-Z])/g, "$1_$2").toLowerCase().replace(/[^a-z0-9]+/g, "_");
  return normalized !== "key" && agentFlowInputKeyLooksSensitive(normalized);
}

function mapping(value: unknown): AgentFlowYamlMapping | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as AgentFlowYamlMapping
    : undefined;
}
