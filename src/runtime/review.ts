import { createHash } from "node:crypto";

interface ReviewProviderOutput {
  content: string | Uint8Array;
  contentType?: string;
}

interface ReviewPrompt {
  path: string;
  content: string;
  checksum: string;
}

export const AGENT_FLOW_REVIEW_STATUSES = [
  "approved",
  "changes_requested",
  "unresolved"
] as const;

export type AgentFlowReviewStatus = typeof AGENT_FLOW_REVIEW_STATUSES[number];

export interface AgentFlowReviewFinding {
  summary: string;
}

export interface AgentFlowReviewResult {
  status: AgentFlowReviewStatus;
  findings: AgentFlowReviewFinding[];
  summary?: string;
}

export class AgentFlowReviewError extends Error {
  readonly code = "AGENT_FLOW_REVIEW_INVALID";

  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "AgentFlowReviewError";
  }
}

export function createAgentFlowReviewPrompt(
  stepId: string,
  reviewer: string,
  subject: string,
  artifacts: string[],
  outputs: string[]
): ReviewPrompt {
  const path = `agent-flow/reviews/${safeSegment(stepId)}.md`;
  const content = [
    `Perform a formal review of session ${JSON.stringify(subject)} as reviewer ${JSON.stringify(reviewer)}.`,
    "",
    "Review these input artifacts:",
    ...artifacts.map((artifact) => `- ${artifact}`),
    "",
    "Write each declared output as JSON with this contract:",
    '- status: one of "approved", "changes_requested", or "unresolved"',
    "- findings: an array of objects, each with a non-empty summary",
    "- summary: an optional overall non-empty summary",
    "",
    "Declared outputs:",
    ...outputs.map((output) => `- ${output}`),
    ""
  ].join("\n");
  return {
    path,
    content,
    checksum: `sha256:${createHash("sha256").update(content).digest("hex")}`
  };
}

export function parseAgentFlowReviewResult(
  value: string | Uint8Array | ReviewProviderOutput,
  outputPath = "review output"
): AgentFlowReviewResult {
  const content = typeof value === "object" && !(value instanceof Uint8Array) && "content" in value
    ? value.content
    : value;
  let parsed: unknown;
  try {
    parsed = JSON.parse(typeof content === "string" ? content : Buffer.from(content).toString("utf8"));
  } catch (error) {
    throw new AgentFlowReviewError(`${outputPath} must contain valid JSON.`, { cause: error });
  }
  if (!isRecord(parsed)) {
    throw new AgentFlowReviewError(`${outputPath} must contain a JSON object.`);
  }
  if (typeof parsed.status !== "string" || !AGENT_FLOW_REVIEW_STATUSES.includes(parsed.status as AgentFlowReviewStatus)) {
    throw new AgentFlowReviewError(
      `${outputPath} status must be one of: ${AGENT_FLOW_REVIEW_STATUSES.join(", ")}.`
    );
  }
  if (!Array.isArray(parsed.findings)) {
    throw new AgentFlowReviewError(`${outputPath} findings must be an array.`);
  }
  parsed.findings.forEach((finding, index) => {
    if (!isRecord(finding) || typeof finding.summary !== "string" || finding.summary.trim().length === 0) {
      throw new AgentFlowReviewError(`${outputPath} finding ${index + 1} must be an object with a non-empty summary.`);
    }
  });
  if (parsed.summary !== undefined &&
      (typeof parsed.summary !== "string" || parsed.summary.trim().length === 0)) {
    throw new AgentFlowReviewError(`${outputPath} summary must be a non-empty string when present.`);
  }
  return parsed as unknown as AgentFlowReviewResult;
}

function safeSegment(value: string): string {
  return value.replace(/[^A-Za-z0-9._-]/g, "_");
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
