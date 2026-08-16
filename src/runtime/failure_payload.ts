import path from "node:path";
import { createHash } from "node:crypto";
import { parseYamlDocumentOrThrow } from "@jurgen1c/agent-core/yaml";
import {
  type AgentFlowFailureOutcome,
  type AgentFlowRunStateStore,
  type AgentFlowRunStateValue,
  type WriteAgentFlowArtifactInput
} from "./run_state";

export const AGENT_FLOW_FAILURE_REDACTION_MARKER = "[REDACTED]";
export const MAX_AGENT_FLOW_FAILURE_ATTACHMENT_SCAN_BYTES = 1024 * 1024;
export const MAX_AGENT_FLOW_FAILURE_ATTACHMENT_COUNT = 64;
export const MAX_AGENT_FLOW_FAILURE_TOTAL_ATTACHMENT_BYTES = 8 * 1024 * 1024;
const SECRET_KEY_PATTERN = /^(?:auth|(?:proxy_?)?authorization|(?:set_?)?cookie|pgpassword|mysql_?pwd|(?:[a-z0-9]+_)*(?:api_?(?:keys?|tokens?)|access_?(?:keys?|tokens?)|auth_?tokens?|client_?secrets?|credentials?|encryption_?keys?|key|master_?keys?|pass|passphrases?|private_?keys?|passwords?|passwds?|pwd|secrets?|signing_?keys?|tokens?)(?:_[a-z0-9]+)*)$/;
const INPUT_SECRET_KEY_PATTERN = /^(?:auth|(?:proxy_?)?authorization|(?:set_?)?cookie|pgpassword|mysql_?pwd|key|(?:ssh|deploy|service(?:_account)?)_keys?(?:_[a-z0-9]+)*|(?:[a-z0-9]+_)*(?:api_?(?:keys?|tokens?)|access_?(?:keys?|tokens?)|auth_?tokens?|client_?secrets?|credentials?|encryption_?keys?|master_?keys?|pass|passphrases?|private_?keys?|passwords?|passwds?|pwd|secrets?|signing_?keys?|tokens?)(?:_[a-z0-9]+)*)$/;
const SHELL_WORD_PATTERN = String.raw`(?:(?:\\[^\r\n])|'[^'\r\n]*'|"(?:\\[^\r\n]|[^"\\\r\n])*"|[^\s"'\\;&|()<>])+`;

export function redactAgentFlowSensitiveText(value: string): string {
  return redactSensitiveText(value).value;
}

export function redactAgentFlowSensitiveInputText(value: string): string {
  return redactSensitiveText(value, "text", agentFlowInputKeyLooksSensitive, true).value;
}

export function agentFlowInputKeyLooksSensitive(value: string): boolean {
  return INPUT_SECRET_KEY_PATTERN.test(normalizeSecretKey(value));
}

export interface AgentFlowFailurePayload {
  id: string;
  step_id: string;
  step_type: string;
  status: "failed";
  attempt: number;
  exit_code: number | null;
  command: string | null;
  summary: string;
  logs: {
    stdout: string | null;
    stderr: string | null;
  };
  artifacts: {
    available: string[];
    withheld: string[];
  };
  classification: string;
  remediation_status: string | null;
  path: string;
  redactions: {
    applied: boolean;
    marker: typeof AGENT_FLOW_FAILURE_REDACTION_MARKER;
    fields: string[];
    unscanned_artifacts: string[];
  };
}

export interface PersistAgentFlowFailurePayloadInput {
  id: string;
  runId: string;
  stepId: string;
  sessionId?: string;
  stepType: string;
  attempt: number;
  exitCode?: number | null;
  command?: string | null;
  summary: string;
  classification: string;
  retryable: boolean;
  outcome: AgentFlowFailureOutcome;
  logs?: {
    stdout?: string;
    stderr?: string;
  };
  indexPayload?: Record<string, AgentFlowRunStateValue>;
}

export interface PersistAgentFlowFailurePayloadResult {
  path: string | null;
  persistenceError: string | null;
  redacted: boolean;
  indexPayload: Record<string, AgentFlowRunStateValue>;
}

export function persistAgentFlowFailurePayload(
  store: AgentFlowRunStateStore,
  input: PersistAgentFlowFailurePayloadInput
): PersistAgentFlowFailurePayloadResult {
  const payloadPath = `failures/${safeSegment(input.id)}.json`;
  const redactionFields: string[] = [];
  const unscannedArtifacts: string[] = [];
  const safeArtifactPaths = new Map<string, string>();
  const attachmentWrites: WriteAgentFlowArtifactInput[] = [];
  const withheldArtifacts = new Set<string>();
  const command = redactText(input.command ?? null, "command", redactionFields, "shell");
  const summary = redactText(input.summary, "summary", redactionFields)!;
  const indexPayload = redactRunStateMapping(
    input.indexPayload ?? {},
    "run_state",
    redactionFields
  );
  const replayFingerprint = failureReplayFingerprint({
    stepId: input.stepId,
    sessionId: input.sessionId ?? null,
    stepType: input.stepType,
    attempt: input.attempt,
    exitCode: input.exitCode ?? null,
    command,
    summary,
    classification: input.classification,
    retryable: input.retryable,
    outcome: input.outcome,
    stdout: input.logs?.stdout ?? null,
    stderr: input.logs?.stderr ?? null,
    indexPayload
  });
  const existingFailure = store.listFailures(input.runId).find((failure) => failure.id === input.id);
  if (existingFailure !== undefined) {
    const existingPayload = runStateMapping(existingFailure.payload);
    const sameFailure = existingFailure.stepId === input.stepId
      && existingFailure.sessionId === (input.sessionId ?? null)
      && existingFailure.classification === input.classification
      && existingFailure.message === summary
      && existingFailure.retryable === input.retryable
      && existingFailure.attempt === input.attempt
      && existingFailure.outcome === input.outcome
      && existingPayload?.failureReplayFingerprint === replayFingerprint;
    if (!sameFailure) {
      throw new Error(`Agent Flow failure ${input.id} already exists with different failure data.`);
    }
    return {
      path: existingFailure.payloadPath,
      persistenceError: typeof existingPayload?.payloadPersistenceError === "string"
        ? existingPayload.payloadPersistenceError
        : null,
      redacted: existingPayload?.redacted === true,
      indexPayload
    };
  }

  let attachmentScanError: string | null = null;
  let attachmentScanCount = 0;
  let attachmentScanBytes = 0;
  let artifacts: ReturnType<AgentFlowRunStateStore["listArtifactMetadata"]> = [];
  try {
    artifacts = store.listArtifactMetadata(input.runId);
  } catch (error) {
    attachmentScanError = redactSensitiveText(
      error instanceof Error ? error.message : String(error)
    ).value;
  }

  for (const artifact of artifacts) {
    if (artifact.producerStepId !== input.stepId
        || artifact.kind === "failure_payload"
        || artifact.kind === "failure_attachment"
        || artifact.metadata.attempt !== input.attempt) {
      continue;
    }
    if (!textContentType(artifact.contentType)) {
      withheldArtifacts.add(artifact.declaredPath);
      unscannedArtifacts.push(artifact.declaredPath);
      continue;
    }
    if (artifact.sizeBytes === null
        || artifact.sizeBytes > MAX_AGENT_FLOW_FAILURE_ATTACHMENT_SCAN_BYTES
        || attachmentScanCount >= MAX_AGENT_FLOW_FAILURE_ATTACHMENT_COUNT
        || attachmentScanBytes + artifact.sizeBytes > MAX_AGENT_FLOW_FAILURE_TOTAL_ATTACHMENT_BYTES) {
      withheldArtifacts.add(artifact.declaredPath);
      unscannedArtifacts.push(artifact.declaredPath);
      continue;
    }
    attachmentScanCount += 1;
    attachmentScanBytes += artifact.sizeBytes;
    let sanitized: ReturnType<typeof redactSensitiveText>;
    try {
      const snapshot = store.readArtifact(input.runId, artifact.declaredPath, {
        maxBytes: MAX_AGENT_FLOW_FAILURE_ATTACHMENT_SCAN_BYTES
      });
      if (snapshot.artifact.id !== artifact.id
          || snapshot.artifact.producerStepId !== input.stepId
          || snapshot.artifact.metadata.attempt !== input.attempt
          || snapshot.artifact.generation !== artifact.generation
          || snapshot.artifact.checksum !== artifact.checksum) {
        throw new Error("Artifact changed while preparing its failure snapshot.");
      }
      const content = decodeText(snapshot.content);
      if (content === null) {
        withheldArtifacts.add(artifact.declaredPath);
        unscannedArtifacts.push(artifact.declaredPath);
        continue;
      }
      if (structuredContentHasSecretKey(content, artifact.contentType)) {
        withheldArtifacts.add(artifact.declaredPath);
        unscannedArtifacts.push(artifact.declaredPath);
        continue;
      }
      sanitized = redactSensitiveText(content);
      const redacted = sanitized.markers.length > 0;
      if (redacted && structuredTextContentType(artifact.contentType)) {
        withheldArtifacts.add(artifact.declaredPath);
        unscannedArtifacts.push(artifact.declaredPath);
        continue;
      }
      if (redacted) redactionFields.push(`artifacts.${artifact.declaredPath}`);
    } catch {
      withheldArtifacts.add(artifact.declaredPath);
      unscannedArtifacts.push(artifact.declaredPath);
      continue;
    }
    const attachmentPath = failureAttachmentPath(input.id, artifact.declaredPath);
    attachmentWrites.push({
        id: `failure:${digest(input.id)}:attachment:${digest(artifact.declaredPath)}`,
        runId: input.runId,
        stepId: input.stepId,
        path: attachmentPath,
        kind: "failure_attachment",
        contentType: artifact.contentType,
        content: sanitized.value,
        metadata: {
          failureId: input.id,
          sourcePath: artifact.declaredPath,
          redacted: sanitized.markers.length > 0
        }
      });
    safeArtifactPaths.set(artifact.declaredPath, attachmentPath);
  }

  const payload: AgentFlowFailurePayload = {
    id: input.id,
    step_id: input.stepId,
    step_type: input.stepType,
    status: "failed",
    attempt: input.attempt,
    exit_code: input.exitCode ?? null,
    command,
    summary,
    logs: {
      stdout: safeLogPath(input.logs?.stdout, safeArtifactPaths, withheldArtifacts),
      stderr: safeLogPath(input.logs?.stderr, safeArtifactPaths, withheldArtifacts)
    },
    artifacts: {
      available: [...new Set(safeArtifactPaths.values())].sort(),
      withheld: [...withheldArtifacts].sort()
    },
    classification: input.classification,
    remediation_status: null,
    path: payloadPath,
    redactions: {
      applied: redactionFields.length > 0,
      marker: AGENT_FLOW_FAILURE_REDACTION_MARKER,
      fields: [...new Set(redactionFields)].sort(),
      unscanned_artifacts: [...new Set(unscannedArtifacts)].sort()
    }
  };

  let persistedPath: string | null = payloadPath;
  let persistenceError: string | null = attachmentScanError;
  const payloadWrite = (): WriteAgentFlowArtifactInput => ({
    id: `failure:${digest(input.id)}:payload`,
    runId: input.runId,
    stepId: input.stepId,
    path: payloadPath,
    kind: "failure_payload",
    contentType: "application/json",
    content: `${JSON.stringify(payload, null, 2)}\n`,
    metadata: {
      failureId: input.id,
      attempt: input.attempt,
      classification: input.classification,
      redacted: payload.redactions.applied
    }
  });
  try {
    if (attachmentWrites.length === 0) {
      store.writeArtifact(payloadWrite());
    } else {
      store.writeArtifactsAtomically([...attachmentWrites, payloadWrite()]);
    }
  } catch (error) {
    const writeError = redactSensitiveText(error instanceof Error ? error.message : String(error)).value;
    persistenceError = persistenceError === null ? writeError : `${persistenceError}; ${writeError}`;
    if (attachmentWrites.length === 0) {
      persistedPath = null;
    } else {
      for (const sourcePath of safeArtifactPaths.keys()) {
        withheldArtifacts.add(sourcePath);
        unscannedArtifacts.push(sourcePath);
      }
      safeArtifactPaths.clear();
      payload.logs.stdout = null;
      payload.logs.stderr = null;
      payload.artifacts.available = [];
      payload.artifacts.withheld = [...withheldArtifacts].sort();
      payload.redactions.unscanned_artifacts = [...new Set(unscannedArtifacts)].sort();
      try {
        store.writeArtifact(payloadWrite());
      } catch (payloadError) {
        persistedPath = null;
        const payloadWriteError = redactSensitiveText(
          payloadError instanceof Error ? payloadError.message : String(payloadError)
        ).value;
        if (payloadWriteError !== writeError) {
          persistenceError = `${persistenceError}; ${payloadWriteError}`;
        }
      }
    }
  }

  store.recordFailure({
    id: input.id,
    runId: input.runId,
    stepId: input.stepId,
    ...(input.sessionId === undefined ? {} : { sessionId: input.sessionId }),
    classification: input.classification,
    message: summary,
    retryable: input.retryable,
    payload: {
      ...indexPayload,
      attempt: input.attempt,
      outcome: input.outcome,
      failurePayloadPath: persistedPath,
      redacted: payload.redactions.applied,
      failureReplayFingerprint: replayFingerprint,
      ...(persistenceError === null ? {} : { payloadPersistenceError: persistenceError })
    }
  });

  return {
    path: persistedPath,
    persistenceError,
    redacted: payload.redactions.applied,
    indexPayload
  };
}

function safeLogPath(
  declaredPath: string | undefined,
  safePaths: Map<string, string>,
  withheld: Set<string>
): string | null {
  if (declaredPath === undefined || withheld.has(declaredPath)) return null;
  return safePaths.get(declaredPath) ?? null;
}

function failureAttachmentPath(failureId: string, declaredPath: string): string {
  const basename = safeFilename(path.posix.basename(declaredPath));
  return `failures/${safeSegment(failureId)}/attachments/${digest(declaredPath)}/${basename}`;
}

function safeFilename(value: string): string {
  const sanitized = value.trim().replace(/[^A-Za-z0-9._-]+/g, "-").replace(/^-+|-+$/g, "");
  return sanitized.slice(0, 160) || "attachment.txt";
}

function safeSegment(value: string): string {
  const slug = value.trim().replace(/[^A-Za-z0-9._-]+/g, "-").replace(/^-+|-+$/g, "") || "failure";
  return `${slug.slice(0, 160)}-${digest(value)}`;
}

function digest(value: string): string {
  return createHash("sha256").update(value).digest("hex").slice(0, 12);
}

function failureReplayFingerprint(
  value: Record<string, AgentFlowRunStateValue>
): string {
  return createHash("sha256")
    .update(JSON.stringify(canonicalRunStateValue(value)))
    .digest("hex");
}

function canonicalRunStateValue(value: AgentFlowRunStateValue): AgentFlowRunStateValue {
  if (Array.isArray(value)) return value.map(canonicalRunStateValue);
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.keys(value).sort().map((key) => [key, canonicalRunStateValue(value[key]!)])
    );
  }
  return value;
}

function textContentType(contentType: string): boolean {
  const mediaType = contentType.split(";", 1)[0]!.trim().toLowerCase();
  return mediaType === "text/plain"
    || mediaType === "text/markdown"
    || mediaType === "text/x-markdown"
    || mediaType === "application/json"
    || mediaType.endsWith("+json")
    || ["application/yaml", "application/x-yaml", "text/yaml", "text/x-yaml"].includes(mediaType);
}

function structuredTextContentType(contentType: string): boolean {
  const normalized = contentType.toLowerCase();
  return normalized.includes("json") || normalized.includes("yaml");
}

function decodeText(content: Buffer): string | null {
  try {
    const decoded = new TextDecoder("utf-8", { fatal: true }).decode(content);
    return /[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/.test(decoded) ? null : decoded;
  } catch {
    return null;
  }
}

function structuredContentHasSecretKey(content: string, contentType: string): boolean {
  const normalized = contentType.toLowerCase();
  if (unsafeSecretStructure(content)) return true;
  const tomlAssignment = /^[ \t]*((?:"[^"\r\n]+"|'[^'\r\n]+'|[A-Za-z0-9_-]+)(?:[ \t]*\.[ \t]*(?:"[^"\r\n]+"|'[^'\r\n]+'|[A-Za-z0-9_-]+))*)[ \t]*=/gm;
  for (const match of content.matchAll(tomlAssignment)) {
    for (const segment of match[1]!.matchAll(/"([^"\r\n]+)"|'([^'\r\n]+)'|([A-Za-z0-9_-]+)/g)) {
      if (segment[1]?.includes("\\")) return true;
    }
  }
  if (/<\s*\/?[A-Za-z][^>]*>/.test(content)) {
    return true;
  }
  const trimmed = content.trimStart();
  const jsonLike = normalized.includes("json") || trimmed.startsWith("{") || trimmed.startsWith("[");
  if (!jsonLike && !normalized.includes("yaml")) return false;
  try {
    const value = jsonLike
      ? JSON.parse(content) as unknown
      : parseYamlData(content);
    return valueHasSecretKey(value, new Set<object>());
  } catch {
    return normalized.includes("json") || normalized.includes("yaml");
  }
}

function unsafeSecretStructure(content: string): boolean {
  for (const match of content.matchAll(
    /^\s*["']?([A-Za-z0-9_-]+)["']?\s*:\s*(?:[|>][+-]?\d*|\{|\[)?\s*$/gm
  )) {
    if (secretKey(match[1]!)) return true;
  }
  for (const match of content.matchAll(/<\s*([A-Za-z0-9_-]+)(?:\s[^>]*)?>/g)) {
    if (secretKey(match[1]!)) return true;
  }
  return false;
}

function parseYamlData(content: string): unknown {
  return parseYamlDocumentOrThrow(content);
}

function valueHasSecretKey(value: unknown, ancestors: Set<object>): boolean {
  if (value === null || typeof value !== "object") return false;
  if (ancestors.has(value)) return true;
  ancestors.add(value);
  const found = Array.isArray(value)
    ? value.some((entry) => valueHasSecretKey(entry, ancestors))
    : Object.entries(value).some(([key, entry]) =>
        secretKey(key) || valueHasSecretKey(entry, ancestors)
      );
  ancestors.delete(value);
  return found;
}

function redactText(
  value: string | null,
  field: string,
  redactionFields: string[],
  assignmentMode: "shell" | "text" = "text"
): string | null {
  if (value === null) return null;
  const sanitized = redactSensitiveText(value, assignmentMode);
  if (sanitized.markers.length > 0) redactionFields.push(field);
  return sanitized.value;
}

function redactRunStateMapping(
  value: Record<string, AgentFlowRunStateValue>,
  field: string,
  redactionFields: string[]
): Record<string, AgentFlowRunStateValue> {
  return Object.fromEntries(Object.entries(value).map(([key, entry]) => {
    const entryField = `${field}.${key}`;
    if (secretKey(key)) {
      redactionFields.push(entryField);
      return [key, AGENT_FLOW_FAILURE_REDACTION_MARKER];
    }
    return [key, redactRunStateValue(entry, entryField, redactionFields)];
  }));
}

function redactRunStateValue(
  value: AgentFlowRunStateValue,
  field: string,
  redactionFields: string[]
): AgentFlowRunStateValue {
  if (typeof value === "string") return redactText(value, field, redactionFields)!;
  if (Array.isArray(value)) {
    return value.map((entry, index) => redactRunStateValue(entry, `${field}[${index}]`, redactionFields));
  }
  if (value !== null && typeof value === "object") {
    return redactRunStateMapping(value, field, redactionFields);
  }
  return value;
}

function runStateMapping(
  value: AgentFlowRunStateValue | null
): Record<string, AgentFlowRunStateValue> | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value
    : undefined;
}

function redactSensitiveText(
  value: string,
  assignmentMode: "shell" | "text" = "text",
  keyClassifier: (value: string) => boolean = secretKey,
  preserveColonKey = false
): { value: string; markers: string[] } {
  const markers: string[] = [];
  let sanitized = value;
  const replace = (pattern: RegExp, marker: string, replacement: string | ((...args: string[]) => string)): void => {
    const next = sanitized.replace(pattern, replacement as string);
    if (next !== sanitized) markers.push(marker);
    sanitized = next;
  };
  let shellAssignmentRestorations: Array<{ placeholder: string; assignment: string }> = [];
  if (assignmentMode === "shell") {
    const delimitedAssignments = redactShellDelimitedSecretAssignments(sanitized, keyClassifier);
    if (delimitedAssignments.value !== sanitized) markers.push("secret_assignment");
    sanitized = delimitedAssignments.value;
    shellAssignmentRestorations = delimitedAssignments.restorations;
  }
  const positionalAssignments = redactPositionalSecretAssignments(sanitized, keyClassifier);
  if (positionalAssignments !== sanitized) markers.push("secret_assignment");
  sanitized = positionalAssignments;

  const multilineAssignment = redactMultilineSecretAssignmentRemainder(sanitized, keyClassifier);
  if (multilineAssignment !== sanitized) markers.push("secret_assignment");
  sanitized = multilineAssignment;

  replace(
    /-----BEGIN ([A-Z0-9 ]*PRIVATE KEY(?: BLOCK)?)-----[\s\S]*?-----END \1-----/g,
    "private_key",
    AGENT_FLOW_FAILURE_REDACTION_MARKER
  );
  replace(
    /-----BEGIN [A-Z0-9 ]*PRIVATE KEY(?: BLOCK)?-----[\s\S]*$/g,
    "private_key",
    AGENT_FLOW_FAILURE_REDACTION_MARKER
  );
  if (assignmentMode === "shell") {
    replace(
      new RegExp(`(\\B(?:--proxy-user|--user|-u)(?:\\s+|=))(?!-)(${SHELL_WORD_PATTERN})`, "gi"),
      "basic_auth_argument",
      (_match: string, prefix: string) => `${prefix}${AGENT_FLOW_FAILURE_REDACTION_MARKER}`
    );
    replace(
      new RegExp(`(^|[\\s;&|])(-u)(${SHELL_WORD_PATTERN})`, "gim"),
      "basic_auth_argument",
      (match: string, delimiter: string, flag: string, word: string) =>
        word.includes(":")
          ? `${delimiter}${flag}${AGENT_FLOW_FAILURE_REDACTION_MARKER}`
          : match
    );
    replace(
      new RegExp(`(\\B--?)([A-Za-z][A-Za-z0-9_.-]*)(\\s+|=)(?!-)(${SHELL_WORD_PATTERN})`, "g"),
      "secret_cli_argument",
      (match: string, dashes: string, key: string, separator: string) =>
        keyClassifier(key)
          ? `${dashes}${key}${separator}${AGENT_FLOW_FAILURE_REDACTION_MARKER}`
          : match
    );
    replace(
      new RegExp(`\\b([A-Za-z_][A-Za-z0-9_.-]*)(\\s*=\\s*)(${SHELL_WORD_PATTERN})`, "g"),
      "secret_assignment",
      (match: string, key: string, separator: string) =>
        keyClassifier(key)
          ? `${key}${separator}${AGENT_FLOW_FAILURE_REDACTION_MARKER}`
          : match
    );
    replace(
      new RegExp(`(\\b(?:aws\\s+configure|(?:git|npm|pnpm|yarn)\\s+config)\\s+set\\s+)([A-Za-z0-9_.-]+)(\\s+)(${SHELL_WORD_PATTERN})`, "gi"),
      "positional_secret_argument",
      (match: string, prefix: string, key: string, separator: string) =>
        keyClassifier(key)
          ? `${prefix}${key}${separator}${AGENT_FLOW_FAILURE_REDACTION_MARKER}`
          : match
    );
    replace(
      new RegExp(`(\\B(?:-H|--header)(?:\\s+|=))(${SHELL_WORD_PATTERN})`, "gi"),
      "secret_header_argument",
      (match: string, prefix: string, word: string) => {
        const header = word.match(/([A-Za-z][A-Za-z0-9_.-]*)\s*:/);
        return header !== null && keyClassifier(header[1]!)
          ? `${prefix}${AGENT_FLOW_FAILURE_REDACTION_MARKER}`
          : match;
      }
    );
  }
  replace(
    /\bBasic\s+(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{4}|[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)(?![A-Za-z0-9+/=])/gi,
    "basic_auth",
    `Basic ${AGENT_FLOW_FAILURE_REDACTION_MARKER}`
  );
  replace(
    /\bBearer\s+[A-Za-z0-9._~+/=-]{8,}/gi,
    "bearer_token",
    `Bearer ${AGENT_FLOW_FAILURE_REDACTION_MARKER}`
  );
  replace(
    /(^|\\[nrt]|[^A-Za-z0-9_-])((?:Proxy-)?Authorization\s*:\s*)(["'])((?:\\[\s\S]|(?!\3)[\s\S])*)\3/gim,
    "authorization_header",
    (_match: string, delimiter: string, prefix: string, quote: string) =>
      `${delimiter}${prefix}${quote}${AGENT_FLOW_FAILURE_REDACTION_MARKER}${quote}`
  );
  replace(
    /(^|\\[nrt]|[^A-Za-z0-9_-])((?:Proxy-)?Authorization\s*:\s*)(?![ \t]*["'])([A-Za-z][A-Za-z0-9_-]*\s+)?((?:\\(?![nrt])[\s\S]|[^\\\r\n])*)/gim,
    "authorization_header",
    (_match: string, delimiter: string, prefix: string, scheme = "") =>
      `${delimiter}${prefix}${scheme}${AGENT_FLOW_FAILURE_REDACTION_MARKER}`
  );
  replace(
    /\b([A-Za-z_][A-Za-z0-9_.-]*)(\s*(?:::=|::=|[+?!:-]=)\s*)([^\r\n]*)/g,
    "secret_assignment",
    (match: string, key: string, separator: string) =>
      keyClassifier(key)
        ? `${key}${separator}${AGENT_FLOW_FAILURE_REDACTION_MARKER}`
        : match
  );
  replace(
    /(^|\\[nrt]|[^A-Za-z0-9_-])((?:Set-)?Cookie\s*:\s*)([^\r\n\\]+)/gim,
    "cookie_header",
    (_match: string, delimiter: string, prefix: string) =>
      `${delimiter}${prefix}${AGENT_FLOW_FAILURE_REDACTION_MARKER}`
  );
  replace(
    /(\B(?:--proxy-user|--user|-u))(\s+|=)(\$?)(["'])((?:\\[\s\S]|(?!\4)[\s\S])*)\4/gi,
    "basic_auth_argument",
    (_match: string, flag: string, separator: string, shellPrefix: string, quote: string) =>
      `${flag}${separator}${shellPrefix}${quote}${AGENT_FLOW_FAILURE_REDACTION_MARKER}${quote}`
  );
  replace(
    /(\B(?:--proxy-user|--user|-u))(\s+|=)(\$?)(["'])(?:(?!\4)[^\r\n])*$/gim,
    "basic_auth_argument",
    (_match: string, flag: string, separator: string, shellPrefix: string, quote: string) =>
      `${flag}${separator}${shellPrefix}${quote}${AGENT_FLOW_FAILURE_REDACTION_MARKER}`
  );
  replace(
    /(\B(?:--proxy-user|--user|-u))(\s+|=)(?!-)((?:\\[^\r\n]|[^\s$"',;\\])+)/gi,
    "basic_auth_argument",
    (_match: string, flag: string, separator: string) =>
      `${flag}${separator}${AGENT_FLOW_FAILURE_REDACTION_MARKER}`
  );
  replace(
    /(^|[\s;])(-u)((?:\\[^\r\n]|[^\s$"',;\\])*:(?:\\[^\r\n]|[^\s$"',;\\])+)/gim,
    "basic_auth_argument",
    (_match: string, delimiter: string, flag: string) =>
      `${delimiter}${flag}${AGENT_FLOW_FAILURE_REDACTION_MARKER}`
  );
  replace(
    /(\B--?(?:[A-Za-z0-9]+[_-])*(?:api[_-]?(?:key|token)|access[_-]?token|auth[_-]?token|client[_-]?secret|private[_-]?key|password|passwd|secret|token)(?:[_-][A-Za-z0-9]+)*)(\s+)(\$?)(["'])((?:\\[\s\S]|(?!\4)[\s\S])*)\4/gi,
    "secret_cli_argument",
    (_match: string, flag: string, separator: string, shellPrefix: string, quote: string) =>
      `${flag}${separator}${shellPrefix}${quote}${AGENT_FLOW_FAILURE_REDACTION_MARKER}${quote}`
  );
  replace(
    /(\B--?(?:[A-Za-z0-9]+[_-])*(?:api[_-]?(?:key|token)|access[_-]?token|auth[_-]?token|client[_-]?secret|private[_-]?key|password|passwd|secret|token)(?:[_-][A-Za-z0-9]+)*)(\s+)(\$?)(["'])(?:(?!\4)[^\r\n])*$/gim,
    "secret_cli_argument",
    (_match: string, flag: string, separator: string, shellPrefix: string, quote: string) =>
      `${flag}${separator}${shellPrefix}${quote}${AGENT_FLOW_FAILURE_REDACTION_MARKER}`
  );
  replace(
    /(\B--?(?:[A-Za-z0-9]+[_-])*(?:api[_-]?(?:key|token)|access[_-]?token|auth[_-]?token|client[_-]?secret|private[_-]?key|password|passwd|secret|token)(?:[_-][A-Za-z0-9]+)*)(\s+)(?!-)((?:\\[^\r\n]|[^\s$"',;\\])+)/gi,
    "secret_cli_argument",
    (_match: string, flag: string, separator: string) =>
      `${flag}${separator}${AGENT_FLOW_FAILURE_REDACTION_MARKER}`
  );
  replace(
    /\b(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,}|glpat-[A-Za-z0-9_-]{20,}|npm_[A-Za-z0-9]{20,}|(?:sk|rk)_(?:live|test)_[A-Za-z0-9]{16,}|sk-[A-Za-z0-9_-]{16,}|xox[baprs]-[A-Za-z0-9-]{20,}|AKIA[A-Z0-9]{16}|AIza[A-Za-z0-9_-]{30,}|SK[a-f0-9]{32})\b/gi,
    "credential_token",
    AGENT_FLOW_FAILURE_REDACTION_MARKER
  );
  replace(
    /\bSG\.[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}\b/g,
    "credential_token",
    AGENT_FLOW_FAILURE_REDACTION_MARKER
  );
  replace(
    /\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b/g,
    "jwt_token",
    AGENT_FLOW_FAILURE_REDACTION_MARKER
  );
  replace(
    /((?:\/\/[^\s="'\\]+\/?:)?_?auth[_-]?token\s*(?:=(?!=)|:(?!=))\s*)(["'])((?:\\[\s\S]|(?!\2)[\s\S])*)\2/gi,
    "auth_token_assignment",
    (_match: string, prefix: string, quote: string) =>
      `${prefix}${quote}${AGENT_FLOW_FAILURE_REDACTION_MARKER}${quote}`
  );
  replace(
    /((?:\/\/[^\s="'\\]+\/?:)?_?auth[_-]?token\s*(?:=(?!=)|:(?!=))\s*)((?:\\[^\r\n]|[^\s$"',;\\])+)/gi,
    "auth_token_assignment",
    (_match: string, prefix: string) =>
      `${prefix}${AGENT_FLOW_FAILURE_REDACTION_MARKER}`
  );
  replace(
    /(^|[^A-Za-z0-9_.-])(["'])([A-Za-z_][A-Za-z0-9_.-]*)\2(\s*=(?!=)\s*)(\$?)(["'])((?:\\[\s\S]|(?!\6)[\s\S])*)\6/g,
    "secret_assignment",
    (match: string, delimiter: string, keyQuote: string, key: string, separator: string, shellPrefix: string, valueQuote: string) =>
      assignmentSecretKey(key, separator, keyClassifier, preserveColonKey)
        ? `${delimiter}${keyQuote}${key}${keyQuote}${separator}${shellPrefix}${valueQuote}${AGENT_FLOW_FAILURE_REDACTION_MARKER}${valueQuote}`
        : match
  );
  replace(
    /(^|[^A-Za-z0-9_.-])(["'])([A-Za-z_][A-Za-z0-9_.-]*)\2(\s*=(?!=)\s*)(\$?)(["'])(?:(?!\6)[^\r\n])*$/gm,
    "secret_assignment",
    (match: string, delimiter: string, keyQuote: string, key: string, separator: string, shellPrefix: string, valueQuote: string) =>
      assignmentSecretKey(key, separator, keyClassifier, preserveColonKey)
        ? `${delimiter}${keyQuote}${key}${keyQuote}${separator}${shellPrefix}${valueQuote}${AGENT_FLOW_FAILURE_REDACTION_MARKER}`
        : match
  );
  replace(
    /(^|[^A-Za-z0-9_.-])(["'])([A-Za-z_][A-Za-z0-9_.-]*)\2(\s*=(?!=)\s*)((?:\\[\s\S]|[^\s"',;])+)/gm,
    "secret_assignment",
    (match: string, delimiter: string, keyQuote: string, key: string, separator: string) =>
      assignmentSecretKey(key, separator, keyClassifier, preserveColonKey)
        ? `${delimiter}${keyQuote}${key}${keyQuote}${separator}${AGENT_FLOW_FAILURE_REDACTION_MARKER}`
        : match
  );
  replace(
    /(\$env:)([A-Za-z_][A-Za-z0-9_.-]*)(\s*=\s*)(["'])((?:\\[\s\S]|(?!\4)[\s\S])*)\4/gi,
    "secret_assignment",
    (match: string, prefix: string, key: string, separator: string, quote: string) =>
      keyClassifier(key)
        ? `${prefix}${key}${separator}${quote}${AGENT_FLOW_FAILURE_REDACTION_MARKER}${quote}`
        : match
  );
  replace(
    /(\$env:)([A-Za-z_][A-Za-z0-9_.-]*)(\s*=\s*)(["'])(?:(?!\4)[^\r\n])*$/gim,
    "secret_assignment",
    (match: string, prefix: string, key: string, separator: string, quote: string) =>
      keyClassifier(key)
        ? `${prefix}${key}${separator}${quote}${AGENT_FLOW_FAILURE_REDACTION_MARKER}`
        : match
  );
  replace(
    /(\$env:)([A-Za-z_][A-Za-z0-9_.-]*)(\s*=\s*)((?:\\[^\r\n]|[^\s"',;])+)/gi,
    "secret_assignment",
    (match: string, prefix: string, key: string, separator: string) =>
      keyClassifier(key)
        ? `${prefix}${key}${separator}${AGENT_FLOW_FAILURE_REDACTION_MARKER}`
        : match
  );
  replace(
    /\b([A-Za-z_][A-Za-z0-9_.-]*)(\s*(?:=(?!=)|:(?!=))\s*)(\$?)(["'])((?:\\[\s\S]|(?!\4)[\s\S])*)\4/g,
    "secret_assignment",
    (match: string, key: string, separator: string, shellPrefix: string, quote: string) =>
      assignmentSecretKey(key, separator, keyClassifier, preserveColonKey)
        ? `${key}${separator}${shellPrefix}${quote}${AGENT_FLOW_FAILURE_REDACTION_MARKER}${quote}`
        : match
  );
  replace(
    /\b([A-Za-z_][A-Za-z0-9_.-]*)(\s*(?:=(?!=)|:(?!=))\s*)(\$?)(["'])(?:(?!\4)[^\r\n])*$/gm,
    "secret_assignment",
    (match: string, key: string, separator: string, shellPrefix: string, quote: string) =>
      assignmentSecretKey(key, separator, keyClassifier, preserveColonKey)
        ? `${key}${separator}${shellPrefix}${quote}${AGENT_FLOW_FAILURE_REDACTION_MARKER}`
        : match
  );
  replace(
    /\b([A-Za-z_][A-Za-z0-9_.-]*)(\s*(?:=(?!=)|:(?!=))\s*)((?:\\[\s\S]|[^\s"',;])+)/g,
    "secret_assignment",
    (match: string, key: string, separator: string) =>
      assignmentSecretKey(key, separator, keyClassifier, preserveColonKey)
        ? `${key}${separator}${AGENT_FLOW_FAILURE_REDACTION_MARKER}`
        : match
  );
  replace(
    /(\B--?)([A-Za-z][A-Za-z0-9_.-]*)(\s+|=)(\$?)(["'])((?:\\[\s\S]|(?!\5)[\s\S])*)\5/g,
    "secret_cli_argument",
    (match: string, dashes: string, key: string, separator: string, shellPrefix: string, quote: string) =>
      keyClassifier(key)
        ? `${dashes}${key}${separator}${shellPrefix}${quote}${AGENT_FLOW_FAILURE_REDACTION_MARKER}${quote}`
        : match
  );
  replace(
    /(\B--?)([A-Za-z][A-Za-z0-9_.-]*)(\s+|=)(?!-)((?:\\[^\r\n]|[^\s$"',;\\])+)/g,
    "secret_cli_argument",
    (match: string, dashes: string, key: string, separator: string) =>
      keyClassifier(key)
        ? `${dashes}${key}${separator}${AGENT_FLOW_FAILURE_REDACTION_MARKER}`
        : match
  );
  replace(
    /(["'])((?:[A-Za-z0-9]+[_-])*(?:api[_-]?(?:key|token)|access[_-]?token|auth[_-]?token|client[_-]?secret|private[_-]?key|password|passwd|secret|token)(?:[_-][A-Za-z0-9]+)*)\1(\s*:\s*)(["'])((?:\\[\s\S]|(?!\4)[\s\S])*)\4/gi,
    "secret_json_field",
    (_match: string, keyQuote: string, key: string, separator: string, valueQuote: string) =>
      `${keyQuote}${key}${keyQuote}${separator}${valueQuote}${AGENT_FLOW_FAILURE_REDACTION_MARKER}${valueQuote}`
  );
  replace(
    /(\b(?:aws\s+configure|(?:git|npm|pnpm|yarn)\s+config)\s+set\s+)((?:(?:\/\/[^\s"'\\]*\/:)?_?(?:[A-Za-z0-9]+[_.-])*(?:api[_.-]?(?:key|token)|access[_.-]?(?:key|token)|auth[_.-]?token|client[_.-]?secret|private[_.-]?key|password|passwd|secret|token)(?:[_.-][A-Za-z0-9]+)*))(\s+)(\$?)(["'])((?:\\[\s\S]|(?!\5)[\s\S])*)\5/gi,
    "positional_secret_argument",
    (_match: string, prefix: string, key: string, separator: string, shellPrefix: string, quote: string) =>
      `${prefix}${key}${separator}${shellPrefix}${quote}${AGENT_FLOW_FAILURE_REDACTION_MARKER}${quote}`
  );
  replace(
    /(\b(?:aws\s+configure|(?:git|npm|pnpm|yarn)\s+config)\s+set\s+)((?:(?:\/\/[^\s"'\\]*\/:)?_?(?:[A-Za-z0-9]+[_.-])*(?:api[_.-]?(?:key|token)|access[_.-]?(?:key|token)|auth[_.-]?token|client[_.-]?secret|private[_.-]?key|password|passwd|secret|token)(?:[_.-][A-Za-z0-9]+)*))(\s+)(?!-)((?:\\[^\r\n]|[^\s$"',;\\])+)/gi,
    "positional_secret_argument",
    (_match: string, prefix: string, key: string, separator: string) =>
      `${prefix}${key}${separator}${AGENT_FLOW_FAILURE_REDACTION_MARKER}`
  );
  if (assignmentMode === "shell") {
    replace(
      /\b(PGPASSWORD|MYSQL_PWD)(\s*=\s*)(\$?)(["'])((?:\\[\s\S]|(?!\4)[\s\S])*)\4/gi,
      "credential_environment",
      (_match: string, key: string, separator: string, shellPrefix: string, quote: string) =>
        `${key}${separator}${shellPrefix}${quote}${AGENT_FLOW_FAILURE_REDACTION_MARKER}${quote}`
    );
    replace(
      /\b(PGPASSWORD|MYSQL_PWD)(\s*=\s*)(\$?)(["'])(?:(?!\4)[^\r\n])*$/gim,
      "credential_environment",
      (_match: string, key: string, separator: string, shellPrefix: string, quote: string) =>
        `${key}${separator}${shellPrefix}${quote}${AGENT_FLOW_FAILURE_REDACTION_MARKER}`
    );
    replace(
      /\b(PGPASSWORD|MYSQL_PWD)(\s*=\s*)((?:\\[\s\S]|[^\s"',;])+)/gi,
      "credential_environment",
      (_match: string, key: string, separator: string) =>
        `${key}${separator}${AGENT_FLOW_FAILURE_REDACTION_MARKER}`
    );
    replace(
      /\b((?:[A-Za-z0-9]+[_-])*(?:api[_-]?(?:key|token)|access[_-]?token|auth[_-]?token|client[_-]?secret|private[_-]?key|password|passwd|secret|token)(?:[_-][A-Za-z0-9]+)*)(\s*(?:=(?!=)|:(?!=))\s*)(\$?)(["'])((?:\\[\s\S]|(?!\4)[\s\S])*)\4/gi,
      "secret_assignment",
      (_match: string, key: string, separator: string, shellPrefix: string, quote: string) =>
        `${key}${separator}${shellPrefix}${quote}${AGENT_FLOW_FAILURE_REDACTION_MARKER}${quote}`
    );
    replace(
      /\b((?:[A-Za-z0-9]+[_-])*(?:api[_-]?(?:key|token)|access[_-]?token|auth[_-]?token|client[_-]?secret|private[_-]?key|password|passwd|secret|token)(?:[_-][A-Za-z0-9]+)*)(\s*(?:=(?!=)|:(?!=))\s*)(\$?)(["'])(?:(?!\4)[^\r\n])*$/gim,
      "secret_assignment",
      (_match: string, key: string, separator: string, shellPrefix: string, quote: string) =>
        `${key}${separator}${shellPrefix}${quote}${AGENT_FLOW_FAILURE_REDACTION_MARKER}`
    );
    replace(
      /\b((?:[A-Za-z0-9]+[_-])*(?:api[_-]?(?:key|token)|access[_-]?token|auth[_-]?token|client[_-]?secret|private[_-]?key|password|passwd|secret|token)(?:[_-][A-Za-z0-9]+)*)(\s*(?:=(?!=)|:(?!=))\s*)((?:\\[\s\S]|[^\s"',;])+)/gi,
      "secret_assignment",
      (_match: string, key: string, separator: string) =>
        `${key}${separator}${AGENT_FLOW_FAILURE_REDACTION_MARKER}`
    );
  } else {
    replace(
      /\b((?:api\s+(?:key|token)|access\s+(?:key|token)|auth\s+token|client\s+secret|private\s+key))(\s+(?:is)\s+|\s*(?:=(?!=)|:(?!=))\s*)([^\r\n]*)/gi,
      "secret_statement",
      (_match: string, key: string, separator: string) =>
        `${key}${separator}${AGENT_FLOW_FAILURE_REDACTION_MARKER}`
    );
    replace(
      /(["'])([A-Za-z_][A-Za-z0-9_.-]*)\1(\s*:\s*)(?:(["'])((?:\\[^\r\n]|(?!\4)[^\\\r\n])*)\4|([^,}\r\n]*))/g,
      "secret_assignment",
      (match: string, quote: string, key: string, separator: string, valueQuote?: string) =>
        quotedSecretKey(key, keyClassifier, preserveColonKey)
          ? `${quote}${key}${quote}${separator}${valueQuote ?? ""}${AGENT_FLOW_FAILURE_REDACTION_MARKER}${valueQuote ?? ""}`
          : match
    );
    replace(
      /\b(PGPASSWORD|MYSQL_PWD)(\s*(?:=(?!=)|:(?!=))\s*)([^\r\n]*)/gi,
      "credential_environment",
      (_match: string, key: string, separator: string) =>
        `${key}${separator}${AGENT_FLOW_FAILURE_REDACTION_MARKER}`
    );
    replace(
      /\b([A-Za-z_][A-Za-z0-9_.-]*)(\s+is\s+)([^\r\n]*)/gi,
      "secret_statement",
      (match: string, key: string, separator: string) =>
        keyClassifier(key)
          ? `${key}${separator}${AGENT_FLOW_FAILURE_REDACTION_MARKER}`
          : match
    );
    replace(
      /\b([A-Za-z_][A-Za-z0-9_.-]*)(\s*(?:=(?!=)|:(?!=))\s*)([^\r\n]*)/g,
      "secret_assignment",
      (match: string, key: string, separator: string) =>
        assignmentSecretKey(key, separator, keyClassifier, preserveColonKey)
          ? `${key}${separator}${AGENT_FLOW_FAILURE_REDACTION_MARKER}`
          : match
    );
  }
  const nestedAssignments = redactNestedSecretAssignments(sanitized, keyClassifier, preserveColonKey);
  if (nestedAssignments !== sanitized) markers.push("secret_assignment");
  sanitized = nestedAssignments;
  replace(
    /(\b[a-z][a-z0-9+.-]*:\/\/[^:\s/@]*:)([^@\s/]+)(@)/gi,
    "url_password",
    `$1${AGENT_FLOW_FAILURE_REDACTION_MARKER}$3`
  );
  replace(
    /(\b[a-z][a-z0-9+.-]*:\/\/)([^:\s/@]+)(@)/gi,
    "url_userinfo",
    `$1${AGENT_FLOW_FAILURE_REDACTION_MARKER}$3`
  );
  for (const restoration of shellAssignmentRestorations) {
    sanitized = sanitized.replaceAll(restoration.placeholder, restoration.assignment);
  }

  return { value: sanitized, markers };
}

function redactPositionalSecretAssignments(
  value: string,
  keyClassifier: (value: string) => boolean
): string {
  const redact = (pattern: RegExp, source: string): string => source.replace(
    pattern,
    (match: string, boundary: string, indentation: string, command: string, key: string, separator: string) =>
      keyClassifier(key)
        ? `${boundary}${indentation}${command}${key}${separator}${AGENT_FLOW_FAILURE_REDACTION_MARKER}`
        : match
  );
  const setenv = /(^|[\r\n;&|])([ \t]*)(setenv[ \t]+)([A-Za-z_][A-Za-z0-9_.-]*)([ \t]+)[^\r\n]*/gim;
  const fishSet = /(^|[\r\n;&|])([ \t]*)(set(?:[ \t]+--?[A-Za-z][A-Za-z-]*)*[ \t]+)([A-Za-z_][A-Za-z0-9_.-]*)([ \t]+)[^\r\n]*/gim;
  const printfVariable = /(^|[\r\n;&|])([ \t]*)(printf[ \t]+-v[ \t]+)([A-Za-z_][A-Za-z0-9_.-]*)([ \t]+)[^\r\n]*/gim;
  const shellRedacted = redact(printfVariable, redact(fishSet, redact(setenv, value)));
  const dockerfileRedacted = shellRedacted.replace(
    /(^|[\r\n])([ \t]*)(ENV\b)([ \t]+(?:[^\r\n]*\\\r?\n)*[^\r\n]*)/gim,
    (match: string, boundary: string, indentation: string, command: string, argumentsValue: string) => {
      const key = positionalTokenValues(argumentsValue)[0];
      if (key === undefined || key.includes("=") || !keyClassifier(key)) return match;
      return `${boundary}${indentation}${command} ${key} ${AGENT_FLOW_FAILURE_REDACTION_MARKER}`;
    }
  );
  const setxRedacted = dockerfileRedacted.replace(
    /(^|[\r\n;&|])([ \t]*)(setx\b)((?:[^\r\n]*\^\r?\n)*[^\r\n]*)/gim,
    (match: string, boundary: string, indentation: string, command: string, argumentsValue: string) => {
      const key = positionalTokenValues(argumentsValue).find((candidate) => keyClassifier(candidate));
      if (key === undefined) return match;
      return `${boundary}${indentation}${command} ${key} ${AGENT_FLOW_FAILURE_REDACTION_MARKER}`;
    }
  );
  const powerShellRedacted = setxRedacted.replace(
    /(^|[\r\n;&|])([ \t]*)(Set-Variable\b)([^\r\n]*)/gim,
    (match: string, boundary: string, indentation: string, command: string, argumentsValue: string) => {
      const namedKey = /(?:^|[ \t])-Name(?:[ \t]+|=)(?:"([^"]+)"|'([^']+)'|([A-Za-z_][A-Za-z0-9_.-]*))/i
        .exec(argumentsValue);
      const positionalKey = /^[ \t]+(?:"([^"]+)"|'([^']+)'|([A-Za-z_][A-Za-z0-9_.-]*))(?:[ \t]|$)/
        .exec(argumentsValue);
      const key = namedKey?.slice(1).find((candidate) => candidate !== undefined)
        ?? positionalKey?.slice(1).find((candidate) => candidate !== undefined);
      if (key === undefined || !keyClassifier(key)) return match;
      return `${boundary}${indentation}${command} -Name ${key} -Value ${AGENT_FLOW_FAILURE_REDACTION_MARKER}`;
    }
  );
  return powerShellRedacted.replace(
    /(^|[\r\n;&|])([ \t]*)(\[(?:System\.)?Environment][ \t]*::[ \t]*SetEnvironmentVariable[ \t]*\([ \t]*)(["'])([^"'\r\n]+)\4[^\r\n]*/gim,
    (match: string, boundary: string, indentation: string, command: string, quote: string, key: string) =>
      keyClassifier(key)
        ? `${boundary}${indentation}${command}${quote}${key}${quote}, ${quote}${AGENT_FLOW_FAILURE_REDACTION_MARKER}${quote})`
        : match
  );
}

function positionalTokenValues(value: string): string[] {
  return [...value.matchAll(/"([^"]+)"|'([^']+)'|([^ \t\r\n]+)/g)]
    .map((match) => match[1] ?? match[2] ?? match[3]!);
}

function redactShellDelimitedSecretAssignments(
  value: string,
  keyClassifier: (value: string) => boolean
): { value: string; restorations: Array<{ placeholder: string; assignment: string }> } {
  const assignment = /\b([A-Za-z_][A-Za-z0-9_.-]*)(\s*=\s*)/g;
  const quotedIndexes = shellQuotedIndexes(value);
  const replacements: Array<{ start: number; end: number; assignment: string }> = [];
  for (const match of value.matchAll(assignment)) {
    if (!keyClassifier(match[1]!)) continue;
    if (quotedIndexes[match.index] === 1) continue;
    const valueStart = match.index + match[0].length;
    if (shellStartingQuoteIsUnbalanced(value, valueStart)) continue;
    const valueEnd = shellAssignmentValueEnd(value, valueStart);
    if (valueEnd === valueStart) continue;
    replacements.push({
      start: match.index,
      end: valueEnd,
      assignment: `${match[1]}${match[2]}${AGENT_FLOW_FAILURE_REDACTION_MARKER}`
    });
  }
  if (replacements.length === 0) return { value, restorations: [] };

  let result = "";
  let cursor = 0;
  const restorations: Array<{ placeholder: string; assignment: string }> = [];
  for (const replacement of replacements) {
    if (replacement.start < cursor) continue;
    const placeholder = shellAssignmentRedactionPlaceholder(value, restorations.length);
    result += value.slice(cursor, replacement.start);
    result += placeholder;
    restorations.push({ placeholder, assignment: replacement.assignment });
    cursor = replacement.end;
  }
  return { value: result + value.slice(cursor), restorations };
}

function shellQuotedIndexes(value: string): Uint8Array {
  const quoted = new Uint8Array(value.length);
  let quote: "\"" | "'" | "`" | undefined;
  let escaped = false;
  for (let index = 0; index < value.length; index += 1) {
    const character = value[index]!;
    if (escaped) {
      if (quote !== undefined) quoted[index] = 1;
      escaped = false;
      continue;
    }
    if (character === "\\" && quote !== "'") {
      if (quote !== undefined) quoted[index] = 1;
      escaped = true;
      continue;
    }
    if (quote !== undefined) {
      quoted[index] = 1;
      if (character === quote) quote = undefined;
      continue;
    }
    if (character === "\"" || character === "'" || character === "`") {
      quote = character;
      quoted[index] = 1;
    }
  }
  return quoted;
}

function shellStartingQuoteIsUnbalanced(value: string, start: number): boolean {
  const quote = value[start];
  if (quote !== "\"" && quote !== "'") return false;
  let escaped = false;
  for (let index = start + 1; index < value.length; index += 1) {
    const character = value[index]!;
    if (character === "\r" || character === "\n") return true;
    if (escaped) {
      escaped = false;
      continue;
    }
    if (character === "\\" && quote !== "'") {
      escaped = true;
      continue;
    }
    if (character === quote) return false;
  }
  return true;
}

function shellAssignmentRedactionPlaceholder(value: string, index: number): string {
  let suffix = index;
  let placeholder = `\uE000agent-flow-redacted-shell-assignment-${suffix}\uE001`;
  while (value.includes(placeholder)) {
    suffix += 1;
    placeholder = `\uE000agent-flow-redacted-shell-assignment-${suffix}\uE001`;
  }
  return placeholder;
}

function shellAssignmentValueEnd(value: string, start: number): number {
  const closingDelimiters: string[] = [];
  let quote: "\"" | "'" | "`" | undefined;
  let escaped = false;
  for (let index = start; index < value.length; index += 1) {
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
    if (character === "\"" || character === "'" || character === "`") {
      quote = character;
      continue;
    }
    const expectedClose = closingDelimiters.at(-1);
    if (expectedClose !== undefined && character === expectedClose) {
      closingDelimiters.pop();
      continue;
    }
    if (value.startsWith("$(", index) || value.startsWith("${", index)) {
      closingDelimiters.push(value[index + 1] === "(" ? ")" : "}");
      index += 1;
      continue;
    }
    if (character === "(" || character === "{" || character === "[") {
      closingDelimiters.push(character === "(" ? ")" : character === "{" ? "}" : "]");
      continue;
    }
    if (closingDelimiters.length === 0 && /[\s;&|<>]/.test(character)) {
      return index;
    }
  }
  return value.length;
}

function redactNestedSecretAssignments(
  value: string,
  keyClassifier: (value: string) => boolean,
  preserveColonKey: boolean
): string {
  if (!/\b[A-Za-z_][A-Za-z0-9_.-]*\s*(?:=(?!=)|:(?!=))\s*[A-Za-z_][A-Za-z0-9_.-]*\s*(?:=(?!=)|:(?!=))/.test(value)) {
    return value;
  }
  const assignment = /\b([A-Za-z_][A-Za-z0-9_.-]*)(\s*(?:=(?!=)|:(?!=))\s*)/g;
  return value.split(/(\r\n|[\r\n])/).map((line) => {
    if (/^(?:\r\n|[\r\n])$/.test(line)) return line;
    assignment.lastIndex = 0;
    for (const match of line.matchAll(assignment)) {
      const key = match[1]!;
      const separator = match[2]!;
      if (!assignmentSecretKey(key, separator, keyClassifier, preserveColonKey)) continue;
      return `${line.slice(0, match.index)}${key}${separator}${AGENT_FLOW_FAILURE_REDACTION_MARKER}`;
    }
    return line;
  }).join("");
}

function redactMultilineSecretAssignmentRemainder(
  value: string,
  keyClassifier: (value: string) => boolean
): string {
  const lines: Array<{ start: number; text: string }> = [];
  let start = 0;
  for (const match of value.matchAll(/\r\n|[\r\n]/g)) {
    lines.push({ start, text: value.slice(start, match.index) });
    start = match.index! + match[0].length;
  }
  lines.push({ start, text: value.slice(start) });

  const assignment = /(?:^|(?<=[\s;&|]))(?:(?:export|readonly|declare|typeset|local|env)[ \t]+)*(?:\$env:)?([A-Za-z_][A-Za-z0-9_.-]*)[ \t]*(?:::=|::=|[+?!:-]=|[:=])[ \t]*/gi;
  for (let index = 0; index < lines.length - 1; index += 1) {
    const line = lines[index]!;
    for (const match of line.text.matchAll(assignment)) {
      if (!keyClassifier(match[1]!)) continue;
      const valueOffset = match.index + match[0].length;
      const assignedValue = line.text.slice(valueOffset);
      if (!assignmentContinuesOnNextLine(assignedValue, lines[index + 1]!.text)) continue;
      return `${value.slice(0, line.start + valueOffset)}${AGENT_FLOW_FAILURE_REDACTION_MARKER}`;
    }
  }
  return value;
}

function assignmentContinuesOnNextLine(assignedValue: string, nextLine: string): boolean {
  const trimmed = assignedValue.trim();
  if (trimmed.length === 0 || /^[ \t]+/.test(nextLine) || /\\$/.test(assignedValue)) return true;
  if (/^[|>](?:[1-9]|[+-]){0,2}(?:[ \t]+#.*)?$/.test(trimmed)) return true;
  if (/^<<[-~]?[ \t]*(?:["'][^"'\r\n]+["']|[A-Za-z_][A-Za-z0-9_]*)$/.test(trimmed)) return true;
  if (/^@["']/.test(trimmed) && !/["']@[ \t]*$/.test(trimmed)) return true;
  for (const [open, close] of [["$(", ")"], ["${", "}"], ["[", "]"], ["{", "}"], ["(", ")"]] as const) {
    if (trimmed.startsWith(open) && !trimmed.endsWith(close)) return true;
  }
  return hasUnclosedShellQuote(trimmed);
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

function secretKey(value: string): boolean {
  return SECRET_KEY_PATTERN.test(normalizeSecretKey(value));
}

function assignmentSecretKey(
  value: string,
  separator: string,
  keyClassifier: (value: string) => boolean = secretKey,
  preserveColonKey = false
): boolean {
  const normalized = normalizeSecretKey(value);
  return keyClassifier(value)
    && !(preserveColonKey && nonSecretIdentifierKey(normalized) && !separator.includes("="))
    && (separator.includes("=")
      || !/^(?:(?:proxy_?)?authorization|(?:set_?)?cookie)$/.test(normalized));
}

function quotedSecretKey(
  value: string,
  keyClassifier: (value: string) => boolean,
  preserveColonKey: boolean
): boolean {
  return keyClassifier(value)
    && !(preserveColonKey && nonSecretIdentifierKey(normalizeSecretKey(value)));
}

function nonSecretIdentifierKey(value: string): boolean {
  return value === "key" || value === "ticket_key";
}

function normalizeSecretKey(value: string): string {
  return value
    .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
    .replace(/[^A-Za-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .toLowerCase();
}
