import { createHash } from "node:crypto";

export const AGENT_FLOW_APPROVAL_STATUSES = ["approved", "rejected"] as const;
export type AgentFlowApprovalResultStatus = (typeof AGENT_FLOW_APPROVAL_STATUSES)[number];

export interface AgentFlowApprovalResult {
  status: AgentFlowApprovalResultStatus;
  decision: string;
}

export class AgentFlowApprovalError extends Error {
  readonly code: string;

  constructor(message: string, code = "AGENT_FLOW_APPROVAL_INVALID", options?: ErrorOptions) {
    super(message, options);
    this.name = "AgentFlowApprovalError";
    this.code = code;
  }
}

export function defaultAgentFlowApprovalOutputPath(stepId: string): string {
  const segment = stepId.replace(/[^A-Za-z0-9._-]+/g, "-").replace(/^-+|-+$/g, "") || "approval";
  return `approvals/${segment}.json`;
}

export function createAgentFlowApprovalPrompt(
  stepId: string,
  reviewer: string,
  artifacts: string[],
  output: string,
  message?: string
): { path: string; content: string; checksum: string } {
  const content = [
    `You are the approval authority ${reviewer} for Agent Flow step ${stepId}.`,
    ...(message === undefined ? [] : [`Approval criteria: ${message}`]),
    "Inspect every supplied artifact and return one strict JSON object at the declared output path.",
    `Output path: ${output}`,
    'Required shape: {"status":"approved|rejected","decision":"non-empty rationale summary"}',
    "Do not include Markdown fences or additional keys.",
    `Artifacts: ${artifacts.join(", ")}`,
    ""
  ].join("\n");
  return {
    path: `generated://approval/${encodeURIComponent(stepId)}.md`,
    content,
    checksum: `sha256:${createHash("sha256").update(content).digest("hex")}`
  };
}

export function parseAgentFlowApprovalResult(
  source: string | Uint8Array | { content: string | Uint8Array },
  outputPath: string
): AgentFlowApprovalResult {
  let value: unknown;
  try {
    const content = typeof source === "object" && !(source instanceof Uint8Array) ? source.content : source;
    value = JSON.parse(typeof content === "string" ? content : Buffer.from(content).toString("utf8"));
  } catch (error) {
    throw new AgentFlowApprovalError(
      `Approval output ${outputPath} must be valid JSON: ${error instanceof Error ? error.message : String(error)}`,
      "AGENT_FLOW_APPROVAL_JSON",
      { cause: error }
    );
  }
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new AgentFlowApprovalError(`Approval output ${outputPath} must be a JSON object.`);
  }
  const record = value as Record<string, unknown>;
  const keys = Object.keys(record).sort();
  if (keys.length !== 2 || keys[0] !== "decision" || keys[1] !== "status") {
    throw new AgentFlowApprovalError(`Approval output ${outputPath} must contain only status and decision.`);
  }
  if (!AGENT_FLOW_APPROVAL_STATUSES.includes(record.status as AgentFlowApprovalResultStatus)) {
    throw new AgentFlowApprovalError(
      `Approval output ${outputPath} status must be one of: ${AGENT_FLOW_APPROVAL_STATUSES.join(", ")}.`
    );
  }
  if (typeof record.decision !== "string" || record.decision.trim().length === 0) {
    throw new AgentFlowApprovalError(`Approval output ${outputPath} decision must be non-empty text.`);
  }
  return { status: record.status as AgentFlowApprovalResultStatus, decision: record.decision.trim() };
}
