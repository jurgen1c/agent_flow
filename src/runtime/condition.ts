import { normalizeAgentFlowArtifactPath, type AgentFlowRunStateStore, type AgentFlowRunStateValue } from "./run_state";
import type { AgentFlowWorkflowStep, AgentFlowYamlMapping, AgentFlowYamlValue } from "./workflow";
import {
  AgentFlowFailureClassificationError,
  assertAgentFlowFailureClassificationRoutable,
  isAgentFlowFailureClassificationPath
} from "./failure_classification";

const MAX_CONDITION_ARTIFACT_BYTES = 10 * 1024 * 1024;
const MAX_CONDITION_EXPRESSION_BYTES = 16 * 1024;
const MAX_CONDITION_EXPRESSION_DEPTH = 64;
const MAX_CONDITION_EXPRESSION_NODES = 512;
const UNSAFE_PROPERTY_SEGMENTS = new Set(["__proto__", "constructor", "prototype"]);

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

type AgentFlowConditionExpression =
  | { kind: "reference"; reference: AgentFlowConditionReference }
  | { kind: "comparison"; reference: AgentFlowConditionReference; operator: string; value: AgentFlowYamlValue }
  | { kind: "not"; operand: AgentFlowConditionExpression }
  | { kind: "logical"; operator: "&&" | "||"; left: AgentFlowConditionExpression; right: AgentFlowConditionExpression };

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
  const artifactCache = new Map<string, AgentFlowYamlValue>();
  const classificationArtifactPaths = agentFlowConditionArtifactPathsForRun(store, runId);
  return selectAgentFlowConditionTargetWithResolver(step, (scope, segments) =>
    scope === "inputs" ? resolveInput(store, runId, segments) : resolveArtifact(store, runId, segments, artifactCache),
  classificationArtifactPaths);
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
  artifacts: ReadonlyMap<string, AgentFlowYamlValue>,
  artifactPaths: Iterable<string> = artifacts.keys()
): AgentFlowConditionSelection {
  const artifactCache = new Map<string, AgentFlowYamlValue>();
  return selectAgentFlowConditionTargetWithResolver(step, (scope, segments) =>
    scope === "inputs" ? propertyAt(inputs, segments) : resolveArtifactValue(artifacts, segments, artifactCache),
  artifactPaths);
}

export function selectAgentFlowConditionTargetWithResolver(
  step: AgentFlowWorkflowStep,
  resolve: AgentFlowConditionReferenceResolver,
  classificationArtifactPaths: Iterable<string> = []
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
        expression: requiredConditionExpression(branch.if, "Condition branch if"),
        target: requiredString(branch.then, "Condition branch then")
      };
    });
    const elseTarget = step.else === undefined ? undefined : requiredString(step.else, "Condition else");
    preflightAgentFlowFailureClassificationReferences(
      normalizedBranches.map((branch) => branch.expression),
      resolve,
      classificationArtifactPaths
    );
    for (const { expression, target } of normalizedBranches) {
      if (evaluateAgentFlowConditionWithResolver(expression, resolve)) {
        return { target, expression, matched: true };
      }
    }
    return { target: elseTarget, matched: false };
  }

  const expression = requiredConditionExpression(step.if, "Condition if");
  const thenTarget = requiredString(step.then, "Condition then");
  const elseTarget = step.else === undefined ? undefined : requiredString(step.else, "Condition else");
  preflightAgentFlowFailureClassificationReferences([expression], resolve, classificationArtifactPaths);
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
  const artifactCache = new Map<string, AgentFlowYamlValue>();
  const resolve: AgentFlowConditionReferenceResolver = (scope, segments) =>
    scope === "inputs" ? resolveInput(store, runId, segments) : resolveArtifact(store, runId, segments, artifactCache);
  preflightAgentFlowFailureClassificationReferences(
    [source],
    resolve,
    agentFlowConditionArtifactPathsForRun(store, runId)
  );
  return evaluateAgentFlowConditionWithResolver(source, resolve);
}

function agentFlowConditionArtifactPathsForRun(
  store: AgentFlowRunStateStore,
  runId: string
): Set<string> {
  const persistedWorkflow = store.getRun(runId)?.context.workflow;
  const declaredArtifactPaths = isRecord(persistedWorkflow) && Array.isArray(persistedWorkflow.steps)
    ? agentFlowConditionDeclaredArtifactPaths(
      persistedWorkflow.steps.filter(isRecord) as AgentFlowWorkflowStep[]
    )
    : [];
  return new Set([
    ...store.listArtifactMetadata(runId).map((artifact) => artifact.declaredPath),
    ...declaredArtifactPaths
  ]);
}

export function resolveAgentFlowConditionReference(
  store: AgentFlowRunStateStore,
  runId: string,
  scope: "inputs" | "artifacts",
  segments: string[],
  artifactCache?: Map<string, AgentFlowYamlValue>
): AgentFlowYamlValue | undefined {
  return scope === "inputs" ? resolveInput(store, runId, segments) : resolveArtifact(store, runId, segments, artifactCache);
}

export function resolveAgentFlowConditionReferenceFromValues(
  inputs: AgentFlowYamlMapping,
  artifacts: ReadonlyMap<string, AgentFlowYamlValue>,
  scope: "inputs" | "artifacts",
  segments: string[],
  artifactCache?: Map<string, AgentFlowYamlValue>
): AgentFlowYamlValue | undefined {
  return scope === "inputs" ? propertyAt(inputs, segments) : resolveArtifactValue(artifacts, segments, artifactCache);
}

export function evaluateAgentFlowConditionWithResolver(
  source: string,
  resolve: AgentFlowConditionReferenceResolver,
  options: { missingReferences?: "error" | "false" } = {}
): boolean {
  const expression = parseAgentFlowConditionExpression(source);
  const missingReferencesAreFalse = options.missingReferences === "false";
  return evaluateParsedCondition(expression, resolve, source.trim(), missingReferencesAreFalse) ?? false;
}

function defaultScope(path: string): "inputs" | "artifacts" {
  return path.includes(".") ? "artifacts" : "inputs";
}

export function agentFlowConditionExpressionIsSimple(source: string): boolean {
  try {
    parseAgentFlowConditionExpression(source);
    return true;
  } catch {
    return false;
  }
}

export function agentFlowConditionReference(source: string): AgentFlowConditionReference | undefined {
  try {
    const references = agentFlowConditionReferences(source);
    return references.length === 1 ? references[0] : undefined;
  } catch {
    return undefined;
  }
}

export function agentFlowConditionReferences(source: string): AgentFlowConditionReference[] {
  const references: AgentFlowConditionReference[] = [];
  collectConditionReferences(parseAgentFlowConditionExpression(source), references);
  const seen = new Set<string>();
  return references.filter((reference) => {
    const key = `${reference.scope}:${reference.segments.join(".")}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

export function agentFlowConditionExpressionError(source: string): string | undefined {
  try {
    parseAgentFlowConditionExpression(source);
    return undefined;
  } catch (error) {
    return error instanceof Error ? error.message : String(error);
  }
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
  segments: string[],
  artifactCache?: Map<string, AgentFlowYamlValue>
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
  const classificationRequested = segments[0]?.toLowerCase() === "failure_classification";
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

  let value = artifactCache?.get(candidate.artifact.declaredPath);
  if (value === undefined) {
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
    artifactCache?.set(candidate.artifact.declaredPath, value);
  }
  return propertyAt(value, segments.slice(candidate.alias.length));
}

function resolveArtifactValue(
  artifacts: ReadonlyMap<string, AgentFlowYamlValue>,
  segments: string[],
  artifactCache?: Map<string, AgentFlowYamlValue>
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
  const classificationRequested = segments[0]?.toLowerCase() === "failure_classification";
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
  let value = artifactCache?.get(candidate.declaredPath);
  if (value === undefined) {
    value = candidate.value;
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
    artifactCache?.set(candidate.declaredPath, value);
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

export function agentFlowConditionDeclaredArtifactPaths(steps: AgentFlowWorkflowStep[]): string[] {
  const artifacts = new Set<string>();
  const visit = (step: AgentFlowWorkflowStep): void => {
    const candidates = [
      ...(Array.isArray(step.outputs) ? step.outputs : []),
      step.output,
      ...(optionalString(step.type) === "input_request" ? [step.save_as] : [])
    ];
    for (const candidate of candidates) {
      if (typeof candidate !== "string") continue;
      try {
        artifacts.add(normalizeAgentFlowArtifactPath(candidate));
      } catch {
        // Workflow validation reports malformed or dynamic artifact paths.
      }
    }
    const nestedFields = ["body", "steps", ...(optionalString(step.type) === "parallel" ? ["branches"] : [])];
    for (const field of nestedFields) {
      const nested = step[field];
      if (!Array.isArray(nested)) continue;
      nested.filter(isRecord).forEach((entry) => visit(entry as AgentFlowWorkflowStep));
    }
  };
  steps.forEach(visit);
  return [...artifacts];
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

export function preflightAgentFlowFailureClassificationReferences(
  expressions: string[],
  resolve: AgentFlowConditionReferenceResolver,
  artifactPaths: Iterable<string> = []
): void {
  const classificationAliases = [...artifactPaths]
    .filter(isAgentFlowFailureClassificationPath)
    .map(agentFlowConditionArtifactAlias);
  for (const expression of expressions) {
    for (const reference of agentFlowConditionReferences(expression)) {
      const knownClassification = reference.segments[0]?.toLowerCase() === "failure_classification" ||
        classificationAliases.some((alias) => artifactAliasMatches(reference.segments, alias));
      if (reference.scope !== "artifacts" || !knownClassification) continue;
      try {
        resolve(reference.scope, reference.segments);
      } catch (error) {
        if (error instanceof AgentFlowFailureClassificationError) throw error;
        throw new AgentFlowFailureClassificationError(
          `Agent Flow failure classification could not be resolved: ${error instanceof Error ? error.message : String(error)}`,
          "AGENT_FLOW_FAILURE_CLASSIFICATION_INVALID"
        );
      }
    }
  }
}

function propertyAt(value: AgentFlowYamlValue, segments: string[]): AgentFlowYamlValue | undefined {
  let current: AgentFlowYamlValue | undefined = value;
  for (const segment of segments) {
    if (!isRecord(current) || !Object.hasOwn(current, segment)) return undefined;
    current = current[segment];
  }
  return current;
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

function evaluateParsedCondition(
  expression: AgentFlowConditionExpression,
  resolve: AgentFlowConditionReferenceResolver,
  source: string,
  missingReferencesAreFalse: boolean
): boolean | undefined {
  const resolveReference = (reference: AgentFlowConditionReference) => {
    try {
      return resolve(reference.scope, reference.segments);
    } catch (error) {
      if (missingReferencesAreFalse && error instanceof AgentFlowConditionError &&
          (error.message.includes("does not match a published JSON artifact") ||
           error.message.includes("did not resolve to a value"))) {
        return undefined;
      }
      throw error;
    }
  };
  if (expression.kind === "reference") {
    const value = resolveReference(expression.reference);
    return value === undefined && missingReferencesAreFalse ? undefined : truthy(value);
  }
  if (expression.kind === "comparison") {
    const left = resolveReference(expression.reference);
    if (left === undefined && missingReferencesAreFalse) return undefined;
    return compare(
      left,
      expression.operator,
      expression.value,
      source
    );
  }
  if (expression.kind === "not") {
    const operand = evaluateParsedCondition(expression.operand, resolve, source, missingReferencesAreFalse);
    return operand === undefined ? undefined : !operand;
  }
  if (expression.operator === "&&") {
    const left = evaluateParsedCondition(expression.left, resolve, source, missingReferencesAreFalse);
    if (left !== true) return left;
    return evaluateParsedCondition(expression.right, resolve, source, missingReferencesAreFalse);
  }
  const left = evaluateParsedCondition(expression.left, resolve, source, missingReferencesAreFalse);
  if (left === true) return true;
  const right = evaluateParsedCondition(expression.right, resolve, source, missingReferencesAreFalse);
  if (right === true) return true;
  return left === undefined || right === undefined ? undefined : false;
}

function collectConditionReferences(
  expression: AgentFlowConditionExpression,
  references: AgentFlowConditionReference[]
): void {
  if (expression.kind === "reference" || expression.kind === "comparison") {
    references.push(expression.reference);
    return;
  }
  if (expression.kind === "not") {
    collectConditionReferences(expression.operand, references);
    return;
  }
  collectConditionReferences(expression.left, references);
  collectConditionReferences(expression.right, references);
}

function parseAgentFlowConditionExpression(source: string): AgentFlowConditionExpression {
  return new AgentFlowConditionParser(source).parse();
}

class AgentFlowConditionParser {
  private index = 0;
  private depth = 0;
  private nodes = 0;
  private readonly sourceBytes: number;
  private readonly source: string;

  constructor(source: string) {
    this.sourceBytes = Buffer.byteLength(source, "utf8");
    this.source = source.trim();
  }

  parse(): AgentFlowConditionExpression {
    if (this.sourceBytes > MAX_CONDITION_EXPRESSION_BYTES) {
      throw this.error(`exceeds the ${MAX_CONDITION_EXPRESSION_BYTES}-byte limit`);
    }
    if (this.source.length === 0) throw this.error("must not be empty");
    const expression = this.parseOr();
    this.skipWhitespace();
    if (!this.atEnd()) throw this.error(`contains unsupported syntax ${JSON.stringify(this.preview())}`);
    return expression;
  }

  private parseOr(): AgentFlowConditionExpression {
    let left = this.parseAnd();
    while (this.consume("||")) {
      left = this.node({ kind: "logical", operator: "||", left, right: this.parseAnd() });
    }
    return left;
  }

  private parseAnd(): AgentFlowConditionExpression {
    let left = this.parseUnary();
    while (this.consume("&&")) {
      left = this.node({ kind: "logical", operator: "&&", left, right: this.parseUnary() });
    }
    return left;
  }

  private parseUnary(): AgentFlowConditionExpression {
    if (this.consume("!", "!=")) {
      return this.node({ kind: "not", operand: this.withDepth(() => this.parseUnary()) });
    }
    return this.parsePrimary();
  }

  private parsePrimary(): AgentFlowConditionExpression {
    if (this.consume("(")) {
      const expression = this.withDepth(() => this.parseOr());
      if (!this.consume(")")) throw this.error("is missing a closing parenthesis");
      return expression;
    }

    const reference = this.parseReference();
    const operator = this.parseComparisonOperator();
    if (operator === undefined) return this.node({ kind: "reference", reference });
    const value = this.parseLiteral();
    if ([">", ">=", "<", "<="].includes(operator) &&
        typeof value !== "string" && typeof value !== "number") {
      throw this.error("ordered comparisons require a string or number literal");
    }
    return this.node({ kind: "comparison", reference, operator, value });
  }

  private parseReference(): AgentFlowConditionReference {
    this.skipWhitespace();
    const pathStart = this.index;
    const segments = [this.parseIdentifier()];
    while (this.source[this.index] === ".") {
      this.index += 1;
      segments.push(this.parseIdentifier());
    }
    for (const segment of segments) {
      if (UNSAFE_PROPERTY_SEGMENTS.has(segment)) {
        throw this.error(`cannot access unsafe property segment ${JSON.stringify(segment)}`, pathStart);
      }
    }
    const explicitScope = segments.length > 1 && (segments[0] === "inputs" || segments[0] === "artifacts")
      ? segments.shift() as "inputs" | "artifacts"
      : undefined;
    return {
      scope: explicitScope ?? defaultScope(segments.join(".")),
      segments
    };
  }

  private parseIdentifier(): string {
    const start = this.index;
    const first = this.source[this.index];
    if (first === undefined || !/[A-Za-z_]/.test(first)) {
      throw this.error(`expected an input or artifact reference, found ${JSON.stringify(this.preview())}`);
    }
    this.index += 1;
    while (/[A-Za-z0-9_-]/.test(this.source[this.index] ?? "")) this.index += 1;
    return this.source.slice(start, this.index);
  }

  private parseComparisonOperator(): string | undefined {
    this.skipWhitespace();
    for (const operator of ["==", "!=", ">=", "<=", ">", "<"]) {
      if (this.source.startsWith(operator, this.index)) {
        this.index += operator.length;
        return operator;
      }
    }
    return undefined;
  }

  private parseLiteral(): AgentFlowYamlValue {
    this.skipWhitespace();
    const start = this.index;
    if (this.source[this.index] === '"') {
      this.index += 1;
      let escaped = false;
      while (!this.atEnd()) {
        const character = this.source[this.index++]!;
        if (escaped) {
          escaped = false;
        } else if (character === "\\") {
          escaped = true;
        } else if (character === '"') {
          return this.parseJsonScalar(this.source.slice(start, this.index));
        }
      }
      throw this.error("contains an unterminated string literal", start);
    }
    const remaining = this.source.slice(this.index);
    const match = /^(?:true|false|null|-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?)/.exec(remaining);
    if (match === null) throw this.error("comparisons require a JSON string, number, boolean, or null literal");
    this.index += match[0].length;
    return this.parseJsonScalar(match[0]);
  }

  private parseJsonScalar(source: string): AgentFlowYamlValue {
    try {
      const value = JSON.parse(source) as unknown;
      if (value === null || ["boolean", "number", "string"].includes(typeof value)) {
        return value as AgentFlowYamlValue;
      }
    } catch {
      // The actionable parser error below includes the expression position.
    }
    throw this.error("comparisons require a valid JSON scalar literal");
  }

  private consume(token: string, excludedPrefix?: string): boolean {
    this.skipWhitespace();
    if (excludedPrefix !== undefined && this.source.startsWith(excludedPrefix, this.index)) return false;
    if (!this.source.startsWith(token, this.index)) return false;
    this.index += token.length;
    return true;
  }

  private withDepth<T>(callback: () => T): T {
    this.depth += 1;
    if (this.depth > MAX_CONDITION_EXPRESSION_DEPTH) {
      throw this.error(`exceeds the maximum nesting depth of ${MAX_CONDITION_EXPRESSION_DEPTH}`);
    }
    try {
      return callback();
    } finally {
      this.depth -= 1;
    }
  }

  private node<T extends AgentFlowConditionExpression>(expression: T): T {
    this.nodes += 1;
    if (this.nodes > MAX_CONDITION_EXPRESSION_NODES) {
      throw this.error(`exceeds the maximum complexity of ${MAX_CONDITION_EXPRESSION_NODES} expression nodes`);
    }
    return expression;
  }

  private skipWhitespace(): void {
    while (/\s/.test(this.source[this.index] ?? "")) this.index += 1;
  }

  private preview(): string {
    return this.source.slice(this.index, this.index + 24) || "end of expression";
  }

  private atEnd(): boolean {
    return this.index >= this.source.length;
  }

  private error(reason: string, index = this.index): AgentFlowConditionError {
    return new AgentFlowConditionError(
      `Condition expression ${JSON.stringify(this.source)} ${reason} at position ${index + 1}.`
    );
  }
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

function requiredConditionExpression(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new AgentFlowConditionError(`${label} must be a non-empty string.`);
  }
  return value;
}

function optionalString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : undefined;
}

function isRecord(value: unknown): value is AgentFlowYamlMapping {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
