import { createHash } from "node:crypto";
import type { AgentFlowWorkflowStep } from "./workflow";

export const AGENT_FLOW_DISAGREEMENT_STRATEGIES = [
  "ask_user",
  "arbiter",
  "arbiter_then_user",
  "owner_decides",
  "fail"
] as const;

export const MAX_AGENT_FLOW_DISAGREEMENT_ROUNDS = 100;

export type AgentFlowDisagreementStrategy = typeof AGENT_FLOW_DISAGREEMENT_STRATEGIES[number];
export type AgentFlowDisagreementDecision = "approved" | "changes_requested";

export interface AgentFlowDisagreementPolicy {
  strategy: AgentFlowDisagreementStrategy;
  arbiter?: string;
  maxRounds?: number;
}

export interface AgentFlowDisagreementResult {
  status: "resolved" | "unresolved";
  decision?: AgentFlowDisagreementDecision;
  rationale: string;
}

interface DisagreementProviderOutput {
  content: string | Uint8Array;
  contentType?: string;
}

export interface AgentFlowDisagreementPrompt {
  path: string;
  content: string;
  checksum: string;
}

export class AgentFlowDisagreementError extends Error {
  readonly code = "AGENT_FLOW_DISAGREEMENT_INVALID";

  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "AgentFlowDisagreementError";
  }
}

export function parseAgentFlowDisagreementPolicy(value: unknown): AgentFlowDisagreementPolicy {
  const configured = typeof value === "string" ? { strategy: value } : value;
  if (!isRecord(configured)) {
    throw new AgentFlowDisagreementError("Collaboration on_disagreement must be a strategy name or mapping.");
  }
  const strategy = configured.strategy;
  if (typeof strategy !== "string" || !AGENT_FLOW_DISAGREEMENT_STRATEGIES.includes(strategy as AgentFlowDisagreementStrategy)) {
    throw new AgentFlowDisagreementError(
      `Collaboration disagreement strategy must be one of: ${AGENT_FLOW_DISAGREEMENT_STRATEGIES.join(", ")}.`
    );
  }
  const unknown = Object.keys(configured).find((field) => !["strategy", "arbiter", "max_rounds"].includes(field));
  if (unknown !== undefined) {
    throw new AgentFlowDisagreementError(`Collaboration disagreement field ${unknown} is not supported.`);
  }
  const needsArbiter = strategy === "arbiter" || strategy === "arbiter_then_user";
  const arbiter = typeof configured.arbiter === "string" && configured.arbiter.trim().length > 0
    ? configured.arbiter.trim()
    : undefined;
  if (needsArbiter && arbiter === undefined) {
    throw new AgentFlowDisagreementError(`Collaboration disagreement strategy ${strategy} requires a non-empty arbiter session.`);
  }
  if (!needsArbiter && configured.arbiter !== undefined) {
    throw new AgentFlowDisagreementError(`Collaboration disagreement strategy ${strategy} does not use an arbiter session.`);
  }
  const maxRounds = configured.max_rounds;
  if (needsArbiter && !(Number.isSafeInteger(maxRounds)
      && Number(maxRounds) > 0
      && Number(maxRounds) <= MAX_AGENT_FLOW_DISAGREEMENT_ROUNDS)) {
    throw new AgentFlowDisagreementError(
      `Collaboration disagreement strategy ${strategy} requires max_rounds from 1 through ${MAX_AGENT_FLOW_DISAGREEMENT_ROUNDS}.`
    );
  }
  if (!needsArbiter && maxRounds !== undefined) {
    throw new AgentFlowDisagreementError(`Collaboration disagreement strategy ${strategy} does not use max_rounds.`);
  }
  return {
    strategy: strategy as AgentFlowDisagreementStrategy,
    ...(arbiter === undefined ? {} : { arbiter }),
    ...(typeof maxRounds === "number" ? { maxRounds } : {})
  };
}

export function createAgentFlowDisagreementPrompt(
  reviewStepId: string,
  resolver: string,
  reviewer: string,
  subject: string,
  artifacts: string[],
  output: string,
  round: number
): AgentFlowDisagreementPrompt {
  const content = [
    `Resolve disagreement for review step ${JSON.stringify(reviewStepId)} as ${JSON.stringify(resolver)}.`,
    `The reviewer is ${JSON.stringify(reviewer)} and the reviewed subject is ${JSON.stringify(subject)}.`,
    `This is bounded resolution round ${round}.`,
    "",
    "Consider these disputed artifacts:",
    ...artifacts.map((artifact) => `- ${artifact}`),
    "",
    "Write the declared output as JSON with this contract:",
    '- status: "resolved" or "unresolved"',
    '- decision: "approved" or "changes_requested" when status is "resolved"; omit it when unresolved',
    "- rationale: a non-empty explanation of the resolution or why it remains unresolved",
    "",
    `Declared output: ${output}`,
    ""
  ].join("\n");
  return {
    path: `agent-flow/disagreements/${safeSegment(reviewStepId)}-round-${round}.md`,
    content,
    checksum: `sha256:${createHash("sha256").update(content).digest("hex")}`
  };
}

export function defaultAgentFlowDisagreementOutputPath(
  reviewStepId: string,
  round: number,
  episode = 1
): string {
  const digest = createHash("sha256").update(reviewStepId).digest("hex").slice(0, 12);
  const directory = `disagreements/${safeSegment(reviewStepId).slice(0, 160)}-${digest}`;
  return episode === 1
    ? `${directory}/round-${round}.json`
    : `${directory}/episode-${episode}/round-${round}.json`;
}

export function parseAgentFlowDisagreementResult(
  value: string | Uint8Array | DisagreementProviderOutput,
  outputPath = "disagreement output"
): AgentFlowDisagreementResult {
  if (value === null) {
    throw new AgentFlowDisagreementError(`${outputPath} must contain valid JSON.`);
  }
  const content = typeof value === "object" && !(value instanceof Uint8Array) && "content" in value
    ? value.content
    : value;
  let parsed: unknown;
  try {
    parsed = JSON.parse(typeof content === "string" ? content : Buffer.from(content).toString("utf8"));
  } catch (error) {
    throw new AgentFlowDisagreementError(`${outputPath} must contain valid JSON.`, { cause: error });
  }
  if (!isRecord(parsed)) throw new AgentFlowDisagreementError(`${outputPath} must contain a JSON object.`);
  if (parsed.status !== "resolved" && parsed.status !== "unresolved") {
    throw new AgentFlowDisagreementError(`${outputPath} status must be resolved or unresolved.`);
  }
  if (typeof parsed.rationale !== "string" || parsed.rationale.trim().length === 0) {
    throw new AgentFlowDisagreementError(`${outputPath} rationale must be a non-empty string.`);
  }
  if (parsed.status === "resolved" && parsed.decision !== "approved" && parsed.decision !== "changes_requested") {
    throw new AgentFlowDisagreementError(`${outputPath} resolved decisions must be approved or changes_requested.`);
  }
  if (parsed.status === "unresolved" && parsed.decision !== undefined) {
    throw new AgentFlowDisagreementError(`${outputPath} unresolved results must not declare a decision.`);
  }
  return parsed as unknown as AgentFlowDisagreementResult;
}

export function collectAgentFlowReviewCycleStepIds(steps: AgentFlowWorkflowStep[]): Set<string> {
  const { graph, reviewIds } = collectReviewCycleGraph(steps);
  const cycleIds = new Set<string>();
  for (const reviewId of reviewIds) {
    const returnsToReview = [...(graph.get(reviewId) ?? [])]
      .some((target) => reachableFrom(target, graph).has(reviewId));
    if (returnsToReview) cycleIds.add(reviewId);
  }
  return cycleIds;
}

export function collectAgentFlowReviewCyclePathStepIds(steps: AgentFlowWorkflowStep[]): Set<string> {
  return new Set(collectAgentFlowReviewCyclePathReviewIds(steps).keys());
}

export function collectAgentFlowReviewCyclePathReviewIds(
  steps: AgentFlowWorkflowStep[]
): Map<string, Set<string>> {
  const { graph } = collectReviewCycleGraph(steps);
  const cycleReviewIds = collectAgentFlowReviewCycleStepIds(steps);
  const reviewIdsByPathStep = new Map<string, Set<string>>();
  for (const reviewId of cycleReviewIds) {
    for (const candidate of reachableFrom(reviewId, graph)) {
      if (!reachableFrom(candidate, graph).has(reviewId)) continue;
      const reviewIds = reviewIdsByPathStep.get(candidate) ?? new Set<string>();
      reviewIds.add(reviewId);
      reviewIdsByPathStep.set(candidate, reviewIds);
    }
  }
  return reviewIdsByPathStep;
}

function collectReviewCycleGraph(steps: AgentFlowWorkflowStep[]): {
  graph: Map<string, Set<string>>;
  reviewIds: string[];
} {
  const locations = collectStepLocations(steps);
  const graph = new Map<string, Set<string>>([...locations.keys()].map((id) => [id, new Set()]));
  for (const [id, location] of locations) {
    const step = location.steps[location.index]!;
    const targets = [step.then, step.goto, step.else, step.on_approve, step.on_reject, step.on_cancel];
    if (Array.isArray(step.branches)) {
      for (const branch of step.branches) {
        if (isRecord(branch)) targets.push(branch.then);
      }
    }
    for (const target of targets) {
      const normalized = nonEmptyString(target);
      if (normalized !== undefined && locations.has(normalized)) graph.get(id)!.add(normalized);
    }
    const type = nonEmptyString(step.type);
    const hasExplicitSuccess = type === "condition"
      ? hasStaticSuccessTarget(step.else, locations)
      : type === "approval"
        ? hasStaticSuccessTarget(step.on_approve, locations)
        : hasStaticSuccessTarget(step.then, locations) || hasStaticSuccessTarget(step.goto, locations);
    const next = type === "result" || hasExplicitSuccess
      ? undefined
      : nonEmptyString(location.steps[location.index + 1]?.id);
    if (next !== undefined && locations.has(next)) graph.get(id)!.add(next);
  }

  const reviewIds = [...locations]
    .filter(([, location]) => nonEmptyString(location.steps[location.index]?.type) === "review")
    .map(([id]) => id);
  return { graph, reviewIds };
}

function hasStaticSuccessTarget(
  value: unknown,
  locations: ReadonlyMap<string, { steps: AgentFlowWorkflowStep[]; index: number }>
): boolean {
  const target = nonEmptyString(value);
  return target !== undefined
    && (locations.has(target) || !["continue", "ignore"].includes(target))
    && !target.includes("{{")
    && !target.includes("}}");
}

function collectStepLocations(
  steps: AgentFlowWorkflowStep[],
  locations = new Map<string, { steps: AgentFlowWorkflowStep[]; index: number }>()
): Map<string, { steps: AgentFlowWorkflowStep[]; index: number }> {
  steps.forEach((step, index) => {
    const id = nonEmptyString(step.id);
    if (id !== undefined && nonEmptyString(step.type) !== undefined && !locations.has(id)) {
      locations.set(id, { steps, index });
    }
    for (const field of ["body", "steps"] as const) {
      const nested = step[field];
      if (Array.isArray(nested)) collectStepLocations(nested.filter(isRecord) as AgentFlowWorkflowStep[], locations);
    }
    if (nonEmptyString(step.type) === "parallel" && Array.isArray(step.branches)) {
      collectStepLocations(step.branches.filter(isRecord) as AgentFlowWorkflowStep[], locations);
    }
  });
  return locations;
}

function reachableFrom(start: string, graph: Map<string, Set<string>>): Set<string> {
  const reached = new Set<string>();
  const pending = [start];
  while (pending.length > 0) {
    const current = pending.pop()!;
    if (reached.has(current)) continue;
    reached.add(current);
    for (const target of graph.get(current) ?? []) pending.push(target);
  }
  return reached;
}

function safeSegment(value: string): string {
  const normalized = value.replace(/[^A-Za-z0-9._-]/g, "_");
  return normalized.length === 0 ? "review" : normalized;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function nonEmptyString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : undefined;
}
