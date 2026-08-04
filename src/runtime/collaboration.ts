import { createHash } from "node:crypto";

interface CollaborationProviderOutput {
  content: string | Uint8Array;
  contentType?: string;
}

interface CollaborationPrompt {
  path: string;
  content: string;
  checksum: string;
}

export const MAX_AGENT_FLOW_COLLABORATION_QUESTION_BYTES = 4 * 1024;

export interface AgentFlowConsultRecommendation {
  recommendation: string;
  priority?: "low" | "medium" | "high";
}

export interface AgentFlowConsultResult {
  status: "advice" | "blocked";
  blocking: boolean;
  summary: string;
  recommendations: AgentFlowConsultRecommendation[];
}

export interface AgentFlowChallengeResult {
  status: "answered" | "unresolved";
  rationale: string;
  evidence?: string[];
}

export class AgentFlowCollaborationError extends Error {
  readonly code = "AGENT_FLOW_COLLABORATION_INVALID";

  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "AgentFlowCollaborationError";
  }
}

export function createAgentFlowConsultPrompt(
  stepId: string,
  from: string,
  to: string,
  question: string,
  artifacts: string[],
  output: string,
  blocking: boolean
): CollaborationPrompt {
  return collaborationPrompt(stepId, "consult", [
    `Session ${JSON.stringify(from)} is consulting session ${JSON.stringify(to)}.`,
    `Answer this single bounded question: ${JSON.stringify(question)}`,
    "",
    "Consider these input artifacts:",
    ...artifacts.map((artifact) => `- ${artifact}`),
    "",
    "Write the declared output as JSON with this contract:",
    '- status: "advice", or "blocked" only when blocking is allowed',
    `- blocking: ${blocking ? "true only when the advice must block continuation; otherwise false" : "false"}`,
    "- summary: a non-empty bounded summary",
    "- recommendations: an array of objects with a non-empty recommendation and optional low, medium, or high priority",
    "",
    `Declared output: ${output}`,
    ""
  ]);
}

export function createAgentFlowChallengePrompt(
  stepId: string,
  from: string,
  to: string,
  question: string,
  artifacts: string[],
  output: string
): CollaborationPrompt {
  return collaborationPrompt(stepId, "challenge", [
    `Session ${JSON.stringify(from)} is challenging session ${JSON.stringify(to)} for recorded rationale.`,
    `Answer this single bounded question: ${JSON.stringify(question)}`,
    "",
    "Consider these input artifacts:",
    ...artifacts.map((artifact) => `- ${artifact}`),
    "",
    "Write the declared output as JSON with this contract:",
    '- status: "answered" or "unresolved"',
    "- rationale: a non-empty bounded rationale",
    "- evidence: an optional array of non-empty artifact or evidence references",
    "",
    `Declared output: ${output}`,
    ""
  ]);
}

export function parseAgentFlowConsultResult(
  value: string | Uint8Array | CollaborationProviderOutput,
  outputPath = "consult output",
  blockingAllowed?: boolean
): AgentFlowConsultResult {
  const parsed = parseJsonObject(value, outputPath);
  if (parsed.status !== "advice" && parsed.status !== "blocked") {
    throw new AgentFlowCollaborationError(`${outputPath} status must be one of: advice, blocked.`);
  }
  if (typeof parsed.blocking !== "boolean") {
    throw new AgentFlowCollaborationError(`${outputPath} blocking must be a boolean.`);
  }
  if (blockingAllowed === false && (parsed.blocking || parsed.status === "blocked")) {
    throw new AgentFlowCollaborationError(`${outputPath} cannot block because the consult step is advisory.`);
  }
  if ((parsed.status === "blocked") !== parsed.blocking) {
    throw new AgentFlowCollaborationError(`${outputPath} status and blocking must agree.`);
  }
  requireNonEmptyString(parsed.summary, `${outputPath} summary`);
  if (!Array.isArray(parsed.recommendations)) {
    throw new AgentFlowCollaborationError(`${outputPath} recommendations must be an array.`);
  }
  parsed.recommendations.forEach((recommendation, index) => {
    if (!isRecord(recommendation)) {
      throw new AgentFlowCollaborationError(`${outputPath} recommendation ${index + 1} must be an object.`);
    }
    requireNonEmptyString(recommendation.recommendation, `${outputPath} recommendation ${index + 1}`);
    if (recommendation.priority !== undefined && !["low", "medium", "high"].includes(String(recommendation.priority))) {
      throw new AgentFlowCollaborationError(`${outputPath} recommendation ${index + 1} priority must be low, medium, or high.`);
    }
  });
  return parsed as unknown as AgentFlowConsultResult;
}

export function parseAgentFlowChallengeResult(
  value: string | Uint8Array | CollaborationProviderOutput,
  outputPath = "challenge output"
): AgentFlowChallengeResult {
  const parsed = parseJsonObject(value, outputPath);
  if (parsed.status !== "answered" && parsed.status !== "unresolved") {
    throw new AgentFlowCollaborationError(`${outputPath} status must be one of: answered, unresolved.`);
  }
  requireNonEmptyString(parsed.rationale, `${outputPath} rationale`);
  if (parsed.evidence !== undefined &&
      (!Array.isArray(parsed.evidence) || !parsed.evidence.every((entry) => typeof entry === "string" && entry.trim().length > 0))) {
    throw new AgentFlowCollaborationError(`${outputPath} evidence must be a list of non-empty strings when present.`);
  }
  return parsed as unknown as AgentFlowChallengeResult;
}

function collaborationPrompt(stepId: string, kind: "consult" | "challenge", lines: string[]): CollaborationPrompt {
  const content = lines.join("\n");
  return {
    path: `agent-flow/${kind}s/${safeSegment(stepId)}.md`,
    content,
    checksum: `sha256:${createHash("sha256").update(content).digest("hex")}`
  };
}

function parseJsonObject(
  value: string | Uint8Array | CollaborationProviderOutput,
  outputPath: string
): Record<string, unknown> {
  const content = typeof value === "object" && !(value instanceof Uint8Array) && "content" in value
    ? value.content
    : value;
  let parsed: unknown;
  try {
    parsed = JSON.parse(typeof content === "string" ? content : Buffer.from(content).toString("utf8"));
  } catch (error) {
    throw new AgentFlowCollaborationError(`${outputPath} must contain valid JSON.`, { cause: error });
  }
  if (!isRecord(parsed)) throw new AgentFlowCollaborationError(`${outputPath} must contain a JSON object.`);
  return parsed;
}

function requireNonEmptyString(value: unknown, label: string): void {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new AgentFlowCollaborationError(`${label} must be a non-empty string.`);
  }
}

function safeSegment(value: string): string {
  return value.replace(/[^A-Za-z0-9._-]/g, "_");
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
