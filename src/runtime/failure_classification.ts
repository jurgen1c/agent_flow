import path from "node:path";

export const AGENT_FLOW_FAILURE_CLASSIFICATION_KINDS = [
  "flake",
  "implementation_error",
  "formatting_error",
  "environment_error",
  "missing_requirement",
  "unsafe_change",
  "unknown"
] as const;

export const AGENT_FLOW_FAILURE_CLASSIFICATION_CONFIDENCES = ["low", "medium", "high"] as const;

export type AgentFlowFailureClassificationKind = typeof AGENT_FLOW_FAILURE_CLASSIFICATION_KINDS[number];
export type AgentFlowFailureClassificationConfidence = typeof AGENT_FLOW_FAILURE_CLASSIFICATION_CONFIDENCES[number];

export type AgentFlowFailureClassification = {
  kind: AgentFlowFailureClassificationKind;
  confidence: AgentFlowFailureClassificationConfidence;
  summary: string;
  recommended_owner: string;
  safe_to_retry: boolean;
  requires_user: boolean;
};

export class AgentFlowFailureClassificationError extends Error {
  constructor(
    message: string,
    readonly code: "AGENT_FLOW_FAILURE_CLASSIFICATION_INVALID" | "AGENT_FLOW_FAILURE_CLASSIFICATION_UNKNOWN"
  ) {
    super(message);
    this.name = "AgentFlowFailureClassificationError";
  }
}

export function parseAgentFlowFailureClassification(value: unknown): AgentFlowFailureClassification {
  if (!isRecord(value)) return invalid("must be a JSON object");
  const expected = [
    "kind",
    "confidence",
    "summary",
    "recommended_owner",
    "safe_to_retry",
    "requires_user"
  ];
  const unexpected = Object.keys(value).filter((field) => !expected.includes(field)).sort();
  if (unexpected.length > 0) return invalid(`contains unsupported field${unexpected.length === 1 ? "" : "s"}: ${unexpected.join(", ")}`);

  if (!includes(AGENT_FLOW_FAILURE_CLASSIFICATION_KINDS, value.kind)) {
    return invalid(`kind must be one of: ${AGENT_FLOW_FAILURE_CLASSIFICATION_KINDS.join(", ")}`);
  }
  if (!includes(AGENT_FLOW_FAILURE_CLASSIFICATION_CONFIDENCES, value.confidence)) {
    return invalid(`confidence must be one of: ${AGENT_FLOW_FAILURE_CLASSIFICATION_CONFIDENCES.join(", ")}`);
  }
  if (typeof value.summary !== "string" || value.summary.trim().length === 0) {
    return invalid("summary must be non-empty text");
  }
  if (typeof value.recommended_owner !== "string" || value.recommended_owner.trim().length === 0) {
    return invalid("recommended_owner must be non-empty text");
  }
  if (typeof value.safe_to_retry !== "boolean") return invalid("safe_to_retry must be a boolean");
  if (typeof value.requires_user !== "boolean") return invalid("requires_user must be a boolean");

  return {
    kind: value.kind,
    confidence: value.confidence,
    summary: value.summary,
    recommended_owner: value.recommended_owner,
    safe_to_retry: value.safe_to_retry,
    requires_user: value.requires_user
  };
}

export function assertAgentFlowFailureClassificationRoutable(value: unknown): AgentFlowFailureClassification {
  const classification = parseAgentFlowFailureClassification(value);
  if (classification.kind === "unknown") {
    throw new AgentFlowFailureClassificationError(
      "Agent Flow failure classification kind \"unknown\" cannot be routed automatically; pause for review.",
      "AGENT_FLOW_FAILURE_CLASSIFICATION_UNKNOWN"
    );
  }
  return classification;
}

export function isAgentFlowFailureClassificationPath(declaredPath: string): boolean {
  return path.posix.basename(declaredPath).toLowerCase() === "failure-classification.json";
}

function invalid(reason: string): never {
  throw new AgentFlowFailureClassificationError(
    `Agent Flow failure classification ${reason}.`,
    "AGENT_FLOW_FAILURE_CLASSIFICATION_INVALID"
  );
}

function includes<const Values extends readonly string[]>(values: Values, value: unknown): value is Values[number] {
  return typeof value === "string" && (values as readonly string[]).includes(value);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
