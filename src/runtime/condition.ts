import type { AgentFlowRunStateStore, AgentFlowRunStateValue } from "./run_state";
import type { AgentFlowWorkflowStep, AgentFlowYamlMapping, AgentFlowYamlValue } from "./workflow";
import {
  AgentFlowFailureClassificationError,
  assertAgentFlowFailureClassificationRoutable,
  isAgentFlowFailureClassificationPath
} from "./failure_classification";

const MAX_CONDITION_ARTIFACT_BYTES = 10 * 1024 * 1024;
const EXPRESSION = /^(?:(inputs|artifacts)\.)?([A-Za-z_][A-Za-z0-9_-]*(?:\.[A-Za-z_][A-Za-z0-9_-]*)*)\s*(==|!=|>=|<=|>|<)\s*(.+)$/;
const REFERENCE = /^(?:(inputs|artifacts)\.)?([A-Za-z_][A-Za-z0-9_-]*(?:\.[A-Za-z_][A-Za-z0-9_-]*)*)$/;
const BARE_INPUT = /^[A-Za-z_][A-Za-z0-9_-]*$/;

export interface AgentFlowConditionSelection {
  target?: string;
  expression?: string;
  matched: boolean;
}

export type AgentFlowConditionReferenceResolver = (
  scope: "inputs" | "artifacts",
  segments: string[]
) => AgentFlowYamlValue | undefined;

export interface AgentFlowConditionReference {
  scope: "inputs" | "artifacts";
  segments: string[];
}

export class AgentFlowConditionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "AgentFlowConditionError";
  }
}

export function selectAgentFlowConditionTarget(
  store: AgentFlowRunStateStore,
  runId: string,
  step: AgentFlowWorkflowStep
): AgentFlowConditionSelection {
  assertRequiredInputsPresent(store, runId);
  return selectAgentFlowConditionTargetWithResolver(step, (scope, segments) =>
    scope === "inputs" ? resolveInput(store, runId, segments) : resolveArtifact(store, runId, segments)
  );
}

function assertRequiredInputsPresent(store: AgentFlowRunStateStore, runId: string): void {
  const run = store.getRun(runId);
  if (run === null) throw new AgentFlowConditionError(`Agent Flow run ${runId} was not found.`);
  const workflow = isRecord(run.context.workflow) ? run.context.workflow : undefined;
  const inputDefinitions = isRecord(workflow?.inputs) ? workflow.inputs : undefined;
  for (const [inputName, value] of Object.entries(inputDefinitions ?? {})) {
    const definition = isRecord(value) ? value : undefined;
    if (definition?.required === true && !Object.hasOwn(run.inputs, inputName)) {
      throw new AgentFlowConditionError(`Required condition input ${inputName} was not provided for run ${runId}.`);
    }
  }
}

export function selectAgentFlowConditionTargetFromValues(
  step: AgentFlowWorkflowStep,
  inputs: AgentFlowYamlMapping,
  artifacts: ReadonlyMap<string, AgentFlowYamlValue>
): AgentFlowConditionSelection {
  return selectAgentFlowConditionTargetWithResolver(step, (scope, segments) =>
    scope === "inputs" ? propertyAt(inputs, segments) : resolveArtifactValue(artifacts, segments)
  );
}

export function selectAgentFlowConditionTargetWithResolver(
  step: AgentFlowWorkflowStep,
  resolve: AgentFlowConditionReferenceResolver
): AgentFlowConditionSelection {
  if (step.branches !== undefined && !Array.isArray(step.branches)) {
    throw new AgentFlowConditionError("Condition branches must be a list of mappings.");
  }
  const branches = step.branches ?? [];
  if (branches.some((branch) => !isRecord(branch))) {
    throw new AgentFlowConditionError("Condition branches must be a list of mappings.");
  }
  if (branches.length > 0 && (step.if !== undefined || step.then !== undefined)) {
    throw new AgentFlowConditionError(
      "Condition steps must use either branches with an optional else target or top-level if/then fields, not both."
    );
  }

  if (branches.length > 0) {
    const normalizedBranches = branches.map((branch) => {
      if (!isRecord(branch)) {
        throw new AgentFlowConditionError("Condition branches must be a list of mappings.");
      }
      return {
        expression: requiredString(branch.if, "Condition branch if"),
        target: requiredString(branch.then, "Condition branch then")
      };
    });
    const elseTarget = step.else === undefined ? undefined : requiredString(step.else, "Condition else");
    for (const { expression } of normalizedBranches) {
      const reference = agentFlowConditionReference(expression);
      if (reference?.scope !== "artifacts" || !isFailureClassificationReference(reference.segments)) continue;
      try {
        resolve(reference.scope, reference.segments);
      } catch (error) {
        if (error instanceof AgentFlowFailureClassificationError) throw error;
        if (!isFailureClassificationFieldReference(reference.segments)) continue;
        throw new AgentFlowFailureClassificationError(
          `Agent Flow failure classification could not be resolved: ${error instanceof Error ? error.message : String(error)}`,
          "AGENT_FLOW_FAILURE_CLASSIFICATION_INVALID"
        );
      }
    }
    for (const { expression, target } of normalizedBranches) {
      if (evaluateAgentFlowConditionWithResolver(expression, resolve)) {
        return { target, expression, matched: true };
      }
    }
    return { target: elseTarget, matched: false };
  }

  const expression = requiredString(step.if, "Condition if");
  const thenTarget = requiredString(step.then, "Condition then");
  const elseTarget = step.else === undefined ? undefined : requiredString(step.else, "Condition else");
  const matched = evaluateAgentFlowConditionWithResolver(expression, resolve);
  return {
    target: matched ? thenTarget : elseTarget,
    expression,
    matched
  };
}

export function evaluateAgentFlowCondition(
  store: AgentFlowRunStateStore,
  runId: string,
  source: string
): boolean {
  return evaluateAgentFlowConditionWithResolver(source, (scope, segments) =>
    scope === "inputs" ? resolveInput(store, runId, segments) : resolveArtifact(store, runId, segments)
  );
}

export function resolveAgentFlowConditionReference(
  store: AgentFlowRunStateStore,
  runId: string,
  scope: "inputs" | "artifacts",
  segments: string[]
): AgentFlowYamlValue | undefined {
  return scope === "inputs" ? resolveInput(store, runId, segments) : resolveArtifact(store, runId, segments);
}

export function resolveAgentFlowConditionReferenceFromValues(
  inputs: AgentFlowYamlMapping,
  artifacts: ReadonlyMap<string, AgentFlowYamlValue>,
  scope: "inputs" | "artifacts",
  segments: string[]
): AgentFlowYamlValue | undefined {
  return scope === "inputs" ? propertyAt(inputs, segments) : resolveArtifactValue(artifacts, segments);
}

export function evaluateAgentFlowConditionWithResolver(
  source: string,
  resolve: AgentFlowConditionReferenceResolver
): boolean {
  const expression = source.trim();
  const comparison = EXPRESSION.exec(expression);
  if (comparison !== null) {
    const [, scope, path, operator, literalSource] = comparison;
    const resolvedScope = scope === "inputs" || scope === "artifacts" ? scope : defaultScope(path!);
    const left = resolve(resolvedScope, path!.split("."));
    const right = parseLiteral(literalSource!.trim(), expression);
    return compare(left, operator!, right, expression);
  }

  if (BARE_INPUT.test(expression)) {
    return truthy(resolve("inputs", [expression]));
  }

  const reference = REFERENCE.exec(expression);
  if (reference !== null) {
    return truthy(resolve(reference[1] as "inputs" | "artifacts" ?? defaultScope(reference[2]!), reference[2]!.split(".")));
  }

  throw new AgentFlowConditionError(
    `Condition expression ${JSON.stringify(expression)} is too complex; use one input or artifact reference with an optional scalar comparison.`
  );
}

function defaultScope(path: string): "inputs" | "artifacts" {
  return path.includes(".") ? "artifacts" : "inputs";
}

export function agentFlowConditionExpressionIsSimple(source: string): boolean {
  const expression = source.trim();
  if (BARE_INPUT.test(expression) || REFERENCE.test(expression)) return true;
  const comparison = EXPRESSION.exec(expression);
  if (comparison === null) return false;
  try {
    const literal = parseLiteral(comparison[4]!.trim(), expression);
    return ![">", ">=", "<", "<="].includes(comparison[3]!)
      || typeof literal === "string"
      || typeof literal === "number";
  } catch {
    return false;
  }
}

export function agentFlowConditionReference(source: string): AgentFlowConditionReference | undefined {
  const expression = source.trim();
  const match = EXPRESSION.exec(expression) ?? REFERENCE.exec(expression);
  if (match === null) return BARE_INPUT.test(expression)
    ? { scope: "inputs", segments: [expression] }
    : undefined;
  const path = match[2]!;
  return {
    scope: match[1] === "inputs" || match[1] === "artifacts" ? match[1] : defaultScope(path),
    segments: path.split(".")
  };
}

function resolveInput(
  store: AgentFlowRunStateStore,
  runId: string,
  segments: string[]
): AgentFlowYamlValue | undefined {
  const run = store.getRun(runId);
  if (run === null) throw new AgentFlowConditionError(`Agent Flow run ${runId} was not found.`);
  const inputName = segments[0]!;
  const workflow = isRecord(run.context.workflow) ? run.context.workflow : undefined;
  const inputDefinitions = isRecord(workflow?.inputs) ? workflow.inputs : undefined;
  const definition = isRecord(inputDefinitions?.[inputName]) ? inputDefinitions[inputName] : undefined;
  if (!Object.hasOwn(run.inputs, inputName) && definition?.required === true) {
    throw new AgentFlowConditionError(`Required condition input ${inputName} was not provided for run ${runId}.`);
  }
  return propertyAt(run.inputs, segments);
}

function resolveArtifact(
  store: AgentFlowRunStateStore,
  runId: string,
  segments: string[]
): AgentFlowYamlValue | undefined {
  const candidates = store.listArtifactMetadata(runId)
    .filter((artifact) => artifact.writtenAt !== null)
    .map((artifact) => ({ artifact, alias: agentFlowConditionArtifactAlias(artifact.declaredPath) }))
    .filter(({ alias }) => artifactAliasMatches(segments, alias))
    .sort((left, right) => right.alias.length - left.alias.length);
  const classificationCandidates = candidates
    .filter(({ artifact }) => isAgentFlowFailureClassificationPath(artifact.declaredPath));
  const classificationNamespace = candidates.some(({ alias }) =>
    alias.some((segment) => segment.toLowerCase() === "failure_classification")
  );
  const classificationRequested = segments.some((segment) => segment.toLowerCase() === "failure_classification");
  if ((classificationNamespace || (classificationRequested && candidates.length === 0)) &&
      classificationCandidates.length === 0) {
    throw new AgentFlowFailureClassificationError(
      `Condition artifact reference artifacts.${segments.join(".")} requires a published failure-classification.json artifact.`,
      "AGENT_FLOW_FAILURE_CLASSIFICATION_INVALID"
    );
  }
  const candidate = classificationCandidates[0] ?? candidates[0];
  if (candidate === undefined) {
    throw new AgentFlowConditionError(`Condition artifact reference artifacts.${segments.join(".")} does not match a published JSON artifact.`);
  }
  const ambiguous = candidates.filter(({ alias }) => artifactAliasesEqual(alias, candidate.alias));
  if (ambiguous.length > 1) {
    if (classificationCandidates.length > 0) {
      throw new AgentFlowFailureClassificationError(
        `Agent Flow failure classification reference artifacts.${segments.join(".")} matches multiple published artifacts: ${ambiguous.map(({ artifact }) => artifact.declaredPath).join(", ")}.`,
        "AGENT_FLOW_FAILURE_CLASSIFICATION_INVALID"
      );
    }
    throw new AgentFlowConditionError(
      `Condition artifact reference artifacts.${segments.join(".")} matches multiple published artifacts: ${ambiguous.map(({ artifact }) => artifact.declaredPath).join(", ")}.`
    );
  }

  let content: Buffer;
  try {
    ({ content } = store.readArtifact(runId, candidate.artifact.declaredPath, {
      maxBytes: MAX_CONDITION_ARTIFACT_BYTES
    }));
  } catch (error) {
    if (isAgentFlowFailureClassificationPath(candidate.artifact.declaredPath)) {
      throw new AgentFlowFailureClassificationError(
        `Agent Flow failure classification could not be read: ${error instanceof Error ? error.message : String(error)}`,
        "AGENT_FLOW_FAILURE_CLASSIFICATION_INVALID"
      );
    }
    throw error;
  }
  let value: AgentFlowRunStateValue;
  try {
    value = JSON.parse(content.toString("utf8")) as AgentFlowRunStateValue;
  } catch (error) {
    if (isAgentFlowFailureClassificationPath(candidate.artifact.declaredPath)) {
      throw new AgentFlowFailureClassificationError(
        `Agent Flow failure classification must contain valid JSON: ${error instanceof Error ? error.message : String(error)}`,
        "AGENT_FLOW_FAILURE_CLASSIFICATION_INVALID"
      );
    }
    throw new AgentFlowConditionError(
      `Condition artifact ${candidate.artifact.declaredPath} must contain valid JSON: ${error instanceof Error ? error.message : String(error)}`
    );
  }
  if (isAgentFlowFailureClassificationPath(candidate.artifact.declaredPath)) {
    value = assertAgentFlowFailureClassificationRoutable(value);
  }
  return propertyAt(value, segments.slice(candidate.alias.length));
}

function resolveArtifactValue(
  artifacts: ReadonlyMap<string, AgentFlowYamlValue>,
  segments: string[]
): AgentFlowYamlValue | undefined {
  const candidates = [...artifacts]
    .map(([declaredPath, value]) => ({ declaredPath, value, alias: agentFlowConditionArtifactAlias(declaredPath) }))
    .filter(({ alias }) => artifactAliasMatches(segments, alias))
    .sort((left, right) => right.alias.length - left.alias.length);
  const classificationCandidates = candidates
    .filter(({ declaredPath }) => isAgentFlowFailureClassificationPath(declaredPath));
  const classificationNamespace = candidates.some(({ alias }) =>
    alias.some((segment) => segment.toLowerCase() === "failure_classification")
  );
  const classificationRequested = segments.some((segment) => segment.toLowerCase() === "failure_classification");
  if ((classificationNamespace || (classificationRequested && candidates.length === 0)) &&
      classificationCandidates.length === 0) {
    throw new AgentFlowFailureClassificationError(
      `Condition artifact reference artifacts.${segments.join(".")} requires a published failure-classification.json artifact.`,
      "AGENT_FLOW_FAILURE_CLASSIFICATION_INVALID"
    );
  }
  const candidate = classificationCandidates[0] ?? candidates[0];
  if (candidate === undefined) {
    throw new AgentFlowConditionError(`Condition artifact reference artifacts.${segments.join(".")} does not match a published JSON artifact.`);
  }
  const ambiguous = candidates.filter(({ alias }) => artifactAliasesEqual(alias, candidate.alias));
  if (ambiguous.length > 1) {
    if (classificationCandidates.length > 0) {
      throw new AgentFlowFailureClassificationError(
        `Agent Flow failure classification reference artifacts.${segments.join(".")} matches multiple published artifacts: ${ambiguous.map(({ declaredPath }) => declaredPath).join(", ")}.`,
        "AGENT_FLOW_FAILURE_CLASSIFICATION_INVALID"
      );
    }
    throw new AgentFlowConditionError(
      `Condition artifact reference artifacts.${segments.join(".")} matches multiple published artifacts: ${ambiguous.map(({ declaredPath }) => declaredPath).join(", ")}.`
    );
  }
  let value = candidate.value;
  const serialized = typeof value === "string" ? value : JSON.stringify(value);
  if (Buffer.byteLength(serialized, "utf8") > MAX_CONDITION_ARTIFACT_BYTES) {
    if (isAgentFlowFailureClassificationPath(candidate.declaredPath)) {
      throw new AgentFlowFailureClassificationError(
        `Agent Flow failure classification exceeds the ${MAX_CONDITION_ARTIFACT_BYTES}-byte read limit.`,
        "AGENT_FLOW_FAILURE_CLASSIFICATION_INVALID"
      );
    }
    throw new AgentFlowConditionError(
      `Condition artifact ${candidate.declaredPath} exceeds the ${MAX_CONDITION_ARTIFACT_BYTES}-byte read limit.`
    );
  }
  if (typeof value === "string") {
    try {
      value = JSON.parse(value) as AgentFlowYamlValue;
    } catch (error) {
      if (isAgentFlowFailureClassificationPath(candidate.declaredPath)) {
        throw new AgentFlowFailureClassificationError(
          `Agent Flow failure classification must contain valid JSON: ${error instanceof Error ? error.message : String(error)}`,
          "AGENT_FLOW_FAILURE_CLASSIFICATION_INVALID"
        );
      }
      throw new AgentFlowConditionError(
        `Condition artifact ${candidate.declaredPath} must contain valid JSON: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }
  if (isAgentFlowFailureClassificationPath(candidate.declaredPath)) {
    value = assertAgentFlowFailureClassificationRoutable(value);
  }
  return propertyAt(value, segments.slice(candidate.alias.length));
}

export function agentFlowConditionArtifactAlias(declaredPath: string): string[] {
  return declaredPath
    .replace(/\.json$/i, "")
    .split("/")
    .flatMap((segment) => segment.split("."))
    .map((segment) => segment.replace(/-/g, "_"));
}

function artifactAliasMatches(segments: string[], alias: string[]): boolean {
  return alias.every((segment, index) => segment.toLowerCase() === "failure_classification"
    ? segments[index]?.toLowerCase() === "failure_classification"
    : segments[index] === segment);
}

function artifactAliasesEqual(left: string[], right: string[]): boolean {
  return left.length === right.length && left.every((segment, index) => {
    const other = right[index]!;
    return segment.toLowerCase() === "failure_classification" || other.toLowerCase() === "failure_classification"
      ? segment.toLowerCase() === other.toLowerCase()
      : segment === other;
  });
}

function isFailureClassificationFieldReference(segments: string[]): boolean {
  const classificationIndex = segments.findIndex((segment) => segment.toLowerCase() === "failure_classification");
  return classificationIndex >= 0 && [
    "kind",
    "confidence",
    "summary",
    "recommended_owner",
    "safe_to_retry",
    "requires_user"
  ].includes(segments[classificationIndex + 1] ?? "");
}

function isFailureClassificationReference(segments: string[]): boolean {
  const classificationIndex = segments.findIndex((segment) => segment.toLowerCase() === "failure_classification");
  return classificationIndex === segments.length - 1 || isFailureClassificationFieldReference(segments);
}

function propertyAt(value: AgentFlowYamlValue, segments: string[]): AgentFlowYamlValue | undefined {
  let current: AgentFlowYamlValue | undefined = value;
  for (const segment of segments) {
    if (!isRecord(current) || !Object.hasOwn(current, segment)) return undefined;
    current = current[segment];
  }
  return current;
}

function parseLiteral(source: string, expression: string): AgentFlowYamlValue {
  let value: unknown;
  try {
    value = JSON.parse(source);
  } catch {
    throw new AgentFlowConditionError(`Condition expression ${JSON.stringify(expression)} must compare against a JSON scalar.`);
  }
  if (value !== null && !["boolean", "number", "string"].includes(typeof value)) {
    throw new AgentFlowConditionError(`Condition expression ${JSON.stringify(expression)} must compare against a JSON scalar.`);
  }
  return value as AgentFlowYamlValue;
}

function compare(
  left: AgentFlowYamlValue | undefined,
  operator: string,
  right: AgentFlowYamlValue,
  expression: string
): boolean {
  if (left === undefined) {
    throw new AgentFlowConditionError(`Condition reference in ${JSON.stringify(expression)} did not resolve to a value.`);
  }
  if (operator === "==") return left === right;
  if (operator === "!=") return left !== right;
  if ((typeof left !== "number" && typeof left !== "string") || typeof left !== typeof right) {
    throw new AgentFlowConditionError(`Ordered condition ${JSON.stringify(expression)} requires values of the same string or number type.`);
  }
  if (typeof left === "number" && typeof right === "number") return orderedComparison(left, operator, right);
  if (typeof left === "string" && typeof right === "string") return orderedComparison(left, operator, right);
  throw new AgentFlowConditionError(`Ordered condition ${JSON.stringify(expression)} requires values of the same string or number type.`);
}

function orderedComparison<T extends number | string>(left: T, operator: string, right: T): boolean {
  if (operator === ">") return left > right;
  if (operator === ">=") return left >= right;
  if (operator === "<") return left < right;
  return left <= right;
}

function truthy(value: AgentFlowYamlValue | undefined): boolean {
  return value === true || (typeof value === "number" && value !== 0) || (typeof value === "string" && value.length > 0);
}

function requiredString(value: unknown, label: string): string {
  const normalized = optionalString(value);
  if (normalized === undefined) throw new AgentFlowConditionError(`${label} must be a non-empty string.`);
  return normalized;
}

function optionalString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : undefined;
}

function isRecord(value: unknown): value is AgentFlowYamlMapping {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
