import { createHash } from "node:crypto";
import {
  AgentflowRunStateError,
  type AgentflowArtifactRecord,
  type AgentflowRunStateStore
} from "./run_state";
import type { AgentflowWorkflowStep, AgentflowYamlValue } from "./workflow";

export const MAX_AGENTFLOW_TRANSFORM_INPUT_BYTES = 10 * 1024 * 1024;

export interface AgentflowArtifactTransformContext {
  inputPath: string;
  outputPath: string;
}

export interface AgentflowArtifactTransformOutput {
  content: string | Uint8Array;
  contentType: string;
}

export type AgentflowArtifactTransform = (
  input: Uint8Array,
  context: AgentflowArtifactTransformContext
) => AgentflowArtifactTransformOutput;

export interface AgentflowArtifactTransformExecutionResult {
  transform: string;
  inputPath: string;
  outputPath: string;
  artifact: AgentflowArtifactRecord;
}

export interface ExecuteAgentflowArtifactTransformOptions {
  beforePublish?: () => void;
}

export interface AgentflowBinaryArtifactValue {
  [key: string]: AgentflowYamlValue;
  __agentflow_binary__: "base64";
  data: string;
}

export class AgentflowArtifactTransformError extends Error {
  readonly code: string;

  constructor(message: string, code = "AGENTFLOW_ARTIFACT_TRANSFORM", options?: ErrorOptions) {
    super(message, options);
    this.name = "AgentflowArtifactTransformError";
    this.code = code;
  }
}

export class AgentflowArtifactTransformRegistry {
  private readonly transforms = new Map<string, AgentflowArtifactTransform>();

  register(name: string, transform: AgentflowArtifactTransform): this {
    const normalized = requiredName(name, "Transform name");
    if (this.transforms.has(normalized)) {
      throw new AgentflowArtifactTransformError(
        `Artifact transform ${normalized} is already registered.`,
        "AGENTFLOW_ARTIFACT_TRANSFORM_COLLISION"
      );
    }
    this.transforms.set(normalized, transform);
    return this;
  }

  get(name: string): AgentflowArtifactTransform | undefined {
    return this.transforms.get(requiredName(name, "Transform name"));
  }

  names(): string[] {
    return [...this.transforms.keys()].sort();
  }
}

export function createAgentflowArtifactTransformRegistry(): AgentflowArtifactTransformRegistry {
  return new AgentflowArtifactTransformRegistry()
    .register("jira_ticket_to_markdown", jiraTicketToMarkdown);
}

export function executeAgentflowArtifactTransform(
  store: AgentflowRunStateStore,
  runId: string,
  step: AgentflowWorkflowStep,
  registry: AgentflowArtifactTransformRegistry = createAgentflowArtifactTransformRegistry(),
  options: ExecuteAgentflowArtifactTransformOptions = {}
): AgentflowArtifactTransformExecutionResult {
  const stepId = requiredName(step.id, "Artifact transform step ID");
  const inputPath = requiredName(step.input, `Artifact transform ${stepId} input`);
  const outputPath = requiredName(step.output, `Artifact transform ${stepId} output`);
  const transformName = requiredName(step.transform, `Artifact transform ${stepId} transform`);
  const transform = registry.get(transformName);
  if (transform === undefined) {
    throw new AgentflowArtifactTransformError(
      `Unknown artifact transform ${transformName}; register it explicitly before running step ${stepId}.`,
      "AGENTFLOW_ARTIFACT_TRANSFORM_UNKNOWN"
    );
  }

  let input: ReturnType<AgentflowRunStateStore["readArtifact"]>;
  try {
    input = store.readArtifact(runId, inputPath, { maxBytes: MAX_AGENTFLOW_TRANSFORM_INPUT_BYTES });
  } catch (error) {
    if (error instanceof AgentflowRunStateError) {
      const guidance = error.code === "AGENTFLOW_ARTIFACT_TOO_LARGE"
        ? " Reduce the artifact size before running the transform."
        : " Please publish it before running the transform.";
      throw new AgentflowArtifactTransformError(
        `Could not read declared input ${inputPath} for transform step ${stepId}: ${error.message}${guidance}`,
        "AGENTFLOW_ARTIFACT_TRANSFORM_INPUT",
        { cause: error }
      );
    }
    throw error;
  }
  let output: AgentflowArtifactTransformOutput;
  try {
    output = transform(input.content, { inputPath, outputPath });
  } catch (error) {
    if (error instanceof AgentflowArtifactTransformError) throw error;
    throw new AgentflowArtifactTransformError(
      `Artifact transform ${transformName} failed for ${inputPath}: ${error instanceof Error ? error.message : String(error)}`,
      "AGENTFLOW_ARTIFACT_TRANSFORM_FAILED",
      { cause: error }
    );
  }
  validateTransformOutput(transformName, output);

  options.beforePublish?.();

  const existingOutput = store.getArtifact(runId, outputPath);
  const artifact = store.writeArtifact({
    id: existingOutput?.id ?? `artifact-transform:${createHash("sha256").update(outputPath).digest("hex")}`,
    runId,
    stepId,
    path: outputPath,
    kind: "artifact_transform",
    contentType: output.contentType,
    content: output.content,
    overwrite: step.overwrite === true,
    requiredRunStatus: "running",
    requiredArtifacts: [{ path: inputPath, checksum: input.artifact.checksum! }],
    metadata: { transform: transformName, input: inputPath, inputChecksum: input.artifact.checksum }
  });
  return { transform: transformName, inputPath, outputPath, artifact };
}

export function transformAgentflowFixtureArtifact(
  transformName: string,
  input: AgentflowYamlValue,
  context: AgentflowArtifactTransformContext,
  registry: AgentflowArtifactTransformRegistry = createAgentflowArtifactTransformRegistry()
): AgentflowYamlValue {
  const transform = registry.get(transformName);
  if (transform === undefined) {
    throw new AgentflowArtifactTransformError(
      `Unknown artifact transform ${transformName}; register it explicitly before simulation.`,
      "AGENTFLOW_ARTIFACT_TRANSFORM_UNKNOWN"
    );
  }
  const source = isBinaryArtifactValue(input)
    ? decodeBinaryArtifactValue(input, context.inputPath)
    : Buffer.from(typeof input === "string" ? input : stableJson(input), "utf8");
  if (source.byteLength > MAX_AGENTFLOW_TRANSFORM_INPUT_BYTES) {
    throw new AgentflowArtifactTransformError(
      `Fixture artifact ${context.inputPath} exceeds the ${MAX_AGENTFLOW_TRANSFORM_INPUT_BYTES}-byte transform input limit.`,
      "AGENTFLOW_ARTIFACT_TRANSFORM_INPUT"
    );
  }
  const result = transform(source, context);
  validateTransformOutput(transformName, result);
  return typeof result.content === "string"
    ? result.content
    : { __agentflow_binary__: "base64", data: Buffer.from(result.content).toString("base64") };
}

function decodeBinaryArtifactValue(input: AgentflowBinaryArtifactValue, inputPath: string): Buffer {
  const canonicalBase64 = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;
  if (!canonicalBase64.test(input.data)) {
    throw new AgentflowArtifactTransformError(
      `Fixture artifact ${inputPath} contains invalid base64 binary data.`,
      "AGENTFLOW_ARTIFACT_TRANSFORM_INPUT"
    );
  }
  const decoded = Buffer.from(input.data, "base64");
  if (decoded.toString("base64") !== input.data) {
    throw new AgentflowArtifactTransformError(
      `Fixture artifact ${inputPath} contains non-canonical base64 binary data.`,
      "AGENTFLOW_ARTIFACT_TRANSFORM_INPUT"
    );
  }
  return decoded;
}

function validateTransformOutput(transformName: string, output: AgentflowArtifactTransformOutput): void {
  if (typeof output?.contentType !== "string" || output.contentType.trim().length === 0) {
    throw new AgentflowArtifactTransformError(
      `Artifact transform ${transformName} returned an invalid content type.`,
      "AGENTFLOW_ARTIFACT_TRANSFORM_OUTPUT"
    );
  }
}

function jiraTicketToMarkdown(input: Uint8Array): AgentflowArtifactTransformOutput {
  let parsed: unknown;
  try {
    parsed = JSON.parse(Buffer.from(input).toString("utf8"));
  } catch (error) {
    throw new AgentflowArtifactTransformError(
      `Jira ticket input must be valid JSON: ${error instanceof Error ? error.message : String(error)}`,
      "AGENTFLOW_ARTIFACT_TRANSFORM_INPUT"
    );
  }
  if (!isRecord(parsed)) {
    throw new AgentflowArtifactTransformError(
      "Jira ticket input must be a JSON object.",
      "AGENTFLOW_ARTIFACT_TRANSFORM_INPUT"
    );
  }

  const issue = isRecord(parsed.issue) ? parsed.issue : parsed;
  const fields = isRecord(issue.fields) ? issue.fields : issue;
  const key = displayValue(issue.key) ?? displayValue(fields.key) ?? "Jira ticket";
  const summary = displayValue(fields.summary) ?? "Untitled";
  const metadata = [
    ["Status", namedValue(fields.status)],
    ["Type", namedValue(fields.issuetype)],
    ["Priority", namedValue(fields.priority)],
    ["Assignee", namedValue(fields.assignee)]
  ].filter((entry): entry is [string, string] => entry[1] !== undefined);
  const lines = [`# ${key}: ${summary}`];
  if (metadata.length > 0) {
    lines.push("", ...metadata.map(([label, value]) => `- ${label}: ${value}`));
  }
  const description = fields.description;
  if (description !== undefined && description !== null && description !== "") {
    lines.push("", "## Description", "", typeof description === "string" ? description.trim() : `\`\`\`json\n${stableJson(description, 2)}\n\`\`\``);
  }
  return { content: `${lines.join("\n").trimEnd()}\n`, contentType: "text/markdown; charset=utf-8" };
}

function namedValue(value: unknown): string | undefined {
  if (isRecord(value)) return displayValue(value.name) ?? displayValue(value.displayName);
  return displayValue(value);
}

function displayValue(value: unknown): string | undefined {
  if (typeof value === "string" && value.trim().length > 0) return value.trim().replaceAll("\n", " ");
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return undefined;
}

function requiredName(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new AgentflowArtifactTransformError(`${label} must be a non-empty string.`, "AGENTFLOW_ARTIFACT_TRANSFORM_INVALID");
  }
  return value.trim();
}

function stableJson(value: unknown, space?: number): string {
  return JSON.stringify(sortJson(value), null, space);
}

function sortJson(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sortJson);
  if (!isRecord(value)) return value;
  return Object.fromEntries(Object.keys(value).sort().map((key) => [key, sortJson(value[key])]));
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function isBinaryArtifactValue(value: AgentflowYamlValue): value is AgentflowBinaryArtifactValue {
  return isRecord(value)
    && value.__agentflow_binary__ === "base64"
    && typeof value.data === "string";
}
