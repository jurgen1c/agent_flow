import type { AgentflowRunStateStore, AgentflowRunStateValue } from "./run_state";
import type { AgentflowWorkflowStep, AgentflowYamlMapping, AgentflowYamlValue } from "./workflow";

const MAX_CONDITION_ARTIFACT_BYTES = 10 * 1024 * 1024;
const EXPRESSION = /^(?:(inputs|artifacts)\.)?([A-Za-z_][A-Za-z0-9_-]*(?:\.[A-Za-z_][A-Za-z0-9_-]*)*)\s*(==|!=|>=|<=|>|<)\s*(.+)$/;
const REFERENCE = /^(?:(inputs|artifacts)\.)?([A-Za-z_][A-Za-z0-9_-]*(?:\.[A-Za-z_][A-Za-z0-9_-]*)*)$/;
const BARE_INPUT = /^[A-Za-z_][A-Za-z0-9_-]*$/;

export interface AgentflowConditionSelection {
  target?: string;
  expression?: string;
  matched: boolean;
}

export type AgentflowConditionReferenceResolver = (
  scope: "inputs" | "artifacts",
  segments: string[]
) => AgentflowYamlValue | undefined;

export interface AgentflowConditionReference {
  scope: "inputs" | "artifacts";
  segments: string[];
}

export class AgentflowConditionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "AgentflowConditionError";
  }
}

export function selectAgentflowConditionTarget(
  store: AgentflowRunStateStore,
  runId: string,
  step: AgentflowWorkflowStep
): AgentflowConditionSelection {
  assertRequiredInputsPresent(store, runId);
  return selectAgentflowConditionTargetWithResolver(step, (scope, segments) =>
    scope === "inputs" ? resolveInput(store, runId, segments) : resolveArtifact(store, runId, segments)
  );
}

function assertRequiredInputsPresent(store: AgentflowRunStateStore, runId: string): void {
  const run = store.getRun(runId);
  if (run === null) throw new AgentflowConditionError(`Agentflow run ${runId} was not found.`);
  const workflow = isRecord(run.context.workflow) ? run.context.workflow : undefined;
  const inputDefinitions = isRecord(workflow?.inputs) ? workflow.inputs : undefined;
  for (const [inputName, value] of Object.entries(inputDefinitions ?? {})) {
    const definition = isRecord(value) ? value : undefined;
    if (definition?.required === true && !Object.hasOwn(run.inputs, inputName)) {
      throw new AgentflowConditionError(`Required condition input ${inputName} was not provided for run ${runId}.`);
    }
  }
}

export function selectAgentflowConditionTargetFromValues(
  step: AgentflowWorkflowStep,
  inputs: AgentflowYamlMapping,
  artifacts: ReadonlyMap<string, AgentflowYamlValue>
): AgentflowConditionSelection {
  return selectAgentflowConditionTargetWithResolver(step, (scope, segments) =>
    scope === "inputs" ? propertyAt(inputs, segments) : resolveArtifactValue(artifacts, segments)
  );
}

export function selectAgentflowConditionTargetWithResolver(
  step: AgentflowWorkflowStep,
  resolve: AgentflowConditionReferenceResolver
): AgentflowConditionSelection {
  if (step.branches !== undefined && !Array.isArray(step.branches)) {
    throw new AgentflowConditionError("Condition branches must be a list of mappings.");
  }
  const branches = step.branches ?? [];
  if (branches.some((branch) => !isRecord(branch))) {
    throw new AgentflowConditionError("Condition branches must be a list of mappings.");
  }
  if (branches.length > 0 && (step.if !== undefined || step.then !== undefined)) {
    throw new AgentflowConditionError(
      "Condition steps must use either branches with an optional else target or top-level if/then fields, not both."
    );
  }

  if (branches.length > 0) {
    const normalizedBranches = branches.map((branch) => {
      if (!isRecord(branch)) {
        throw new AgentflowConditionError("Condition branches must be a list of mappings.");
      }
      return {
        expression: requiredString(branch.if, "Condition branch if"),
        target: requiredString(branch.then, "Condition branch then")
      };
    });
    const elseTarget = step.else === undefined ? undefined : requiredString(step.else, "Condition else");
    for (const { expression, target } of normalizedBranches) {
      if (evaluateAgentflowConditionWithResolver(expression, resolve)) {
        return { target, expression, matched: true };
      }
    }
    return { target: elseTarget, matched: false };
  }

  const expression = requiredString(step.if, "Condition if");
  const thenTarget = requiredString(step.then, "Condition then");
  const elseTarget = step.else === undefined ? undefined : requiredString(step.else, "Condition else");
  const matched = evaluateAgentflowConditionWithResolver(expression, resolve);
  return {
    target: matched ? thenTarget : elseTarget,
    expression,
    matched
  };
}

export function evaluateAgentflowCondition(
  store: AgentflowRunStateStore,
  runId: string,
  source: string
): boolean {
  return evaluateAgentflowConditionWithResolver(source, (scope, segments) =>
    scope === "inputs" ? resolveInput(store, runId, segments) : resolveArtifact(store, runId, segments)
  );
}

export function evaluateAgentflowConditionWithResolver(
  source: string,
  resolve: AgentflowConditionReferenceResolver
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

  throw new AgentflowConditionError(
    `Condition expression ${JSON.stringify(expression)} is too complex; use one input or artifact reference with an optional scalar comparison.`
  );
}

function defaultScope(path: string): "inputs" | "artifacts" {
  return path.includes(".") ? "artifacts" : "inputs";
}

export function agentflowConditionExpressionIsSimple(source: string): boolean {
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

export function agentflowConditionReference(source: string): AgentflowConditionReference | undefined {
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
  store: AgentflowRunStateStore,
  runId: string,
  segments: string[]
): AgentflowYamlValue | undefined {
  const run = store.getRun(runId);
  if (run === null) throw new AgentflowConditionError(`Agentflow run ${runId} was not found.`);
  const inputName = segments[0]!;
  const workflow = isRecord(run.context.workflow) ? run.context.workflow : undefined;
  const inputDefinitions = isRecord(workflow?.inputs) ? workflow.inputs : undefined;
  const definition = isRecord(inputDefinitions?.[inputName]) ? inputDefinitions[inputName] : undefined;
  if (!Object.hasOwn(run.inputs, inputName) && definition?.required === true) {
    throw new AgentflowConditionError(`Required condition input ${inputName} was not provided for run ${runId}.`);
  }
  return propertyAt(run.inputs, segments);
}

function resolveArtifact(
  store: AgentflowRunStateStore,
  runId: string,
  segments: string[]
): AgentflowYamlValue | undefined {
  const candidates = store.listArtifactMetadata(runId)
    .filter((artifact) => artifact.writtenAt !== null)
    .map((artifact) => ({ artifact, alias: agentflowConditionArtifactAlias(artifact.declaredPath) }))
    .filter(({ alias }) => segments.slice(0, alias.length).join(".") === alias.join("."))
    .sort((left, right) => right.alias.length - left.alias.length);
  const candidate = candidates[0];
  if (candidate === undefined) {
    throw new AgentflowConditionError(`Condition artifact reference artifacts.${segments.join(".")} does not match a published JSON artifact.`);
  }
  const ambiguous = candidates.filter(({ alias }) => alias.join(".") === candidate.alias.join("."));
  if (ambiguous.length > 1) {
    throw new AgentflowConditionError(
      `Condition artifact reference artifacts.${segments.join(".")} matches multiple published artifacts: ${ambiguous.map(({ artifact }) => artifact.declaredPath).join(", ")}.`
    );
  }

  const { content } = store.readArtifact(runId, candidate.artifact.declaredPath, {
    maxBytes: MAX_CONDITION_ARTIFACT_BYTES
  });
  let value: AgentflowRunStateValue;
  try {
    value = JSON.parse(content.toString("utf8")) as AgentflowRunStateValue;
  } catch (error) {
    throw new AgentflowConditionError(
      `Condition artifact ${candidate.artifact.declaredPath} must contain valid JSON: ${error instanceof Error ? error.message : String(error)}`
    );
  }
  return propertyAt(value, segments.slice(candidate.alias.length));
}

function resolveArtifactValue(
  artifacts: ReadonlyMap<string, AgentflowYamlValue>,
  segments: string[]
): AgentflowYamlValue | undefined {
  const candidates = [...artifacts]
    .map(([declaredPath, value]) => ({ declaredPath, value, alias: agentflowConditionArtifactAlias(declaredPath) }))
    .filter(({ alias }) => segments.slice(0, alias.length).join(".") === alias.join("."))
    .sort((left, right) => right.alias.length - left.alias.length);
  const candidate = candidates[0];
  if (candidate === undefined) {
    throw new AgentflowConditionError(`Condition artifact reference artifacts.${segments.join(".")} does not match a published JSON artifact.`);
  }
  const ambiguous = candidates.filter(({ alias }) => alias.join(".") === candidate.alias.join("."));
  if (ambiguous.length > 1) {
    throw new AgentflowConditionError(
      `Condition artifact reference artifacts.${segments.join(".")} matches multiple published artifacts: ${ambiguous.map(({ declaredPath }) => declaredPath).join(", ")}.`
    );
  }
  let value = candidate.value;
  const serialized = typeof value === "string" ? value : JSON.stringify(value);
  if (Buffer.byteLength(serialized, "utf8") > MAX_CONDITION_ARTIFACT_BYTES) {
    throw new AgentflowConditionError(
      `Condition artifact ${candidate.declaredPath} exceeds the ${MAX_CONDITION_ARTIFACT_BYTES}-byte read limit.`
    );
  }
  if (typeof value === "string") {
    try {
      value = JSON.parse(value) as AgentflowYamlValue;
    } catch (error) {
      throw new AgentflowConditionError(
        `Condition artifact ${candidate.declaredPath} must contain valid JSON: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }
  return propertyAt(value, segments.slice(candidate.alias.length));
}

export function agentflowConditionArtifactAlias(declaredPath: string): string[] {
  return declaredPath
    .replace(/\.json$/i, "")
    .split("/")
    .flatMap((segment) => segment.split("."))
    .map((segment) => segment.replace(/-/g, "_"));
}

function propertyAt(value: AgentflowYamlValue, segments: string[]): AgentflowYamlValue | undefined {
  let current: AgentflowYamlValue | undefined = value;
  for (const segment of segments) {
    if (!isRecord(current) || !Object.hasOwn(current, segment)) return undefined;
    current = current[segment];
  }
  return current;
}

function parseLiteral(source: string, expression: string): AgentflowYamlValue {
  let value: unknown;
  try {
    value = JSON.parse(source);
  } catch {
    throw new AgentflowConditionError(`Condition expression ${JSON.stringify(expression)} must compare against a JSON scalar.`);
  }
  if (value !== null && !["boolean", "number", "string"].includes(typeof value)) {
    throw new AgentflowConditionError(`Condition expression ${JSON.stringify(expression)} must compare against a JSON scalar.`);
  }
  return value as AgentflowYamlValue;
}

function compare(
  left: AgentflowYamlValue | undefined,
  operator: string,
  right: AgentflowYamlValue,
  expression: string
): boolean {
  if (left === undefined) {
    throw new AgentflowConditionError(`Condition reference in ${JSON.stringify(expression)} did not resolve to a value.`);
  }
  if (operator === "==") return left === right;
  if (operator === "!=") return left !== right;
  if ((typeof left !== "number" && typeof left !== "string") || typeof left !== typeof right) {
    throw new AgentflowConditionError(`Ordered condition ${JSON.stringify(expression)} requires values of the same string or number type.`);
  }
  if (typeof left === "number" && typeof right === "number") return orderedComparison(left, operator, right);
  if (typeof left === "string" && typeof right === "string") return orderedComparison(left, operator, right);
  throw new AgentflowConditionError(`Ordered condition ${JSON.stringify(expression)} requires values of the same string or number type.`);
}

function orderedComparison<T extends number | string>(left: T, operator: string, right: T): boolean {
  if (operator === ">") return left > right;
  if (operator === ">=") return left >= right;
  if (operator === "<") return left < right;
  return left <= right;
}

function truthy(value: AgentflowYamlValue | undefined): boolean {
  return value === true || (typeof value === "number" && value !== 0) || (typeof value === "string" && value.length > 0);
}

function requiredString(value: unknown, label: string): string {
  const normalized = optionalString(value);
  if (normalized === undefined) throw new AgentflowConditionError(`${label} must be a non-empty string.`);
  return normalized;
}

function optionalString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : undefined;
}

function isRecord(value: unknown): value is AgentflowYamlMapping {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
