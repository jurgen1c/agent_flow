import { parseDocument, type YAMLError } from "yaml";

export type AgentflowWorkflowStyle = "pipeline" | "recovery_pipeline" | "collaborative";
export type AgentflowMaturity = "draft" | "experimental" | "stable" | "trusted";

export type AgentflowYamlValue =
  | null
  | boolean
  | number
  | string
  | AgentflowYamlValue[]
  | AgentflowYamlMapping;

export interface AgentflowYamlMapping {
  [key: string]: AgentflowYamlValue | undefined;
}

export interface AgentflowWorkflowStep {
  id?: string;
  type?: string;
  [key: string]: AgentflowYamlValue | undefined;
}

export interface AgentflowWorkflow {
  name: string;
  version: number;
  style: AgentflowWorkflowStyle;
  maturity: AgentflowMaturity;
  inputs?: Record<string, AgentflowYamlValue>;
  sessions?: Record<string, AgentflowYamlValue>;
  artifacts?: Record<string, AgentflowYamlValue>;
  steps: AgentflowWorkflowStep[];
  policies?: Record<string, AgentflowYamlValue>;
  notify?: AgentflowYamlValue[];
  retention?: Record<string, AgentflowYamlValue>;
  [key: string]: AgentflowYamlValue | undefined;
}

export interface AgentflowWorkflowParseSuccess {
  ok: true;
  workflow: AgentflowWorkflow;
}

export interface AgentflowWorkflowParseFailure {
  ok: false;
  errors: AgentflowWorkflowParseIssue[];
}

export interface AgentflowWorkflowParseIssue {
  code: string;
  message: string;
  path?: string;
  line?: number;
  column?: number;
}

export type AgentflowWorkflowParseResult = AgentflowWorkflowParseSuccess | AgentflowWorkflowParseFailure;

const WORKFLOW_STYLES = ["pipeline", "recovery_pipeline", "collaborative"] as const;
const WORKFLOW_MATURITIES = ["draft", "experimental", "stable", "trusted"] as const;

export function parseAgentflowWorkflow(source: string): AgentflowWorkflowParseResult {
  const yamlResult = parseWorkflowYaml(source);

  if (!yamlResult.ok) {
    return yamlResult;
  }

  const errors = validateWorkflowRoot(yamlResult.value);

  if (errors.length > 0) {
    return { ok: false, errors };
  }

  return {
    ok: true,
    workflow: yamlResult.value as AgentflowWorkflow
  };
}

export function parseAgentflowWorkflowOrThrow(source: string): AgentflowWorkflow {
  const result = parseAgentflowWorkflow(source);

  if (result.ok) {
    return result.workflow;
  }

  throw new AgentflowWorkflowParseError(formatWorkflowParseIssues(result.errors), result.errors);
}

export function formatWorkflowParseIssues(issues: AgentflowWorkflowParseIssue[]): string {
  return issues
    .map((issue) => {
      const location =
        issue.line === undefined
          ? ""
          : ` at line ${issue.line}${issue.column === undefined ? "" : `, column ${issue.column}`}`;
      const path = issue.path === undefined ? "" : ` (${issue.path})`;
      return `${issue.code}${location}${path}: ${issue.message}`;
    })
    .join("\n");
}

export class AgentflowWorkflowParseError extends Error {
  readonly issues: AgentflowWorkflowParseIssue[];

  constructor(message: string, issues: AgentflowWorkflowParseIssue[]) {
    super(message);
    this.name = "AgentflowWorkflowParseError";
    this.issues = issues;
  }
}

type WorkflowYamlResult =
  | { ok: true; value: AgentflowYamlValue }
  | { ok: false; errors: AgentflowWorkflowParseIssue[] };

function parseWorkflowYaml(source: string): WorkflowYamlResult {
  const document = parseDocument(source, {
    logLevel: "error",
    prettyErrors: true,
    schema: "core",
    strict: true,
    stringKeys: true,
    uniqueKeys: true
  });

  const diagnostics = [...document.errors, ...document.warnings];

  if (diagnostics.length > 0) {
    return {
      ok: false,
      errors: diagnostics.map(issueFromYamlError)
    };
  }

  try {
    return {
      ok: true,
      value: normalizeYamlValue(document.toJS({ maxAliasCount: 100 }), "$", new Set())
    };
  } catch (error) {
    return {
      ok: false,
      errors: [issueFromValueError(error)]
    };
  }
}

function normalizeYamlValue(value: unknown, path: string, ancestors: Set<object>): AgentflowYamlValue {
  if (value === null || typeof value === "string" || typeof value === "boolean") {
    return value;
  }

  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new WorkflowYamlValueError(path, "Numbers must be finite.");
    }

    if (Number.isInteger(value) && !Number.isSafeInteger(value)) {
      throw new WorkflowYamlValueError(path, "Integers must be within the JavaScript safe integer range.");
    }

    return value;
  }

  if (typeof value !== "object") {
    throw new WorkflowYamlValueError(path, `Unsupported YAML value type: ${typeof value}.`);
  }

  if (ancestors.has(value)) {
    throw new WorkflowYamlValueError(path, "Circular YAML aliases are not supported.");
  }

  ancestors.add(value);

  try {
    if (Array.isArray(value)) {
      return value.map((entry, index) => normalizeYamlValue(entry, `${path}[${index}]`, ancestors));
    }

    const normalized: AgentflowYamlMapping = {};

    for (const [key, entry] of Object.entries(value)) {
      Object.defineProperty(normalized, key, {
        configurable: true,
        enumerable: true,
        value: normalizeYamlValue(entry, pathForKey(path, key), ancestors),
        writable: true
      });
    }

    return normalized;
  } finally {
    ancestors.delete(value);
  }
}

class WorkflowYamlValueError extends Error {
  readonly path: string;

  constructor(path: string, message: string) {
    super(message);
    this.path = path;
  }
}

function issueFromYamlError(error: YAMLError): AgentflowWorkflowParseIssue {
  const position = error.linePos?.[0];

  return {
    code: "workflow.yaml",
    message: error.message.split("\n", 1)[0].replace(/ at line \d+, column \d+:?$/, ""),
    ...(position === undefined ? {} : { line: position.line, column: position.col })
  };
}

function issueFromValueError(error: unknown): AgentflowWorkflowParseIssue {
  if (error instanceof WorkflowYamlValueError) {
    return {
      code: "workflow.yaml.value",
      message: error.message,
      path: error.path
    };
  }

  return {
    code: "workflow.yaml",
    message: error instanceof Error ? error.message : String(error)
  };
}

function validateWorkflowRoot(value: AgentflowYamlValue): AgentflowWorkflowParseIssue[] {
  const errors: AgentflowWorkflowParseIssue[] = [];

  if (!isRecord(value)) {
    return [
      {
        code: "workflow.root",
        message: "Agentflow workflow YAML must parse to a mapping at the document root."
      }
    ];
  }

  validateString(value, "name", errors);
  validateVersion(value, errors);
  validateEnum(value, "style", WORKFLOW_STYLES, errors);
  validateEnum(value, "maturity", WORKFLOW_MATURITIES, errors);
  validateOptionalRecord(value, "inputs", errors);
  validateOptionalRecord(value, "sessions", errors);
  validateOptionalRecord(value, "artifacts", errors);
  validateOptionalRecord(value, "policies", errors);
  validateOptionalArray(value, "notify", errors);
  validateOptionalRecord(value, "retention", errors);

  if (!Array.isArray(value.steps)) {
    errors.push({
      code: "workflow.steps",
      path: "steps",
      message: "Agentflow workflow field steps must be a list."
    });
  } else {
    value.steps.forEach((step, index) => {
      if (!isRecord(step)) {
        errors.push({
          code: "workflow.steps.item",
          path: `steps[${index}]`,
          message: "Agentflow workflow step entries must be mappings."
        });
      }
    });
  }

  return errors;
}

function validateString(
  value: Record<string, AgentflowYamlValue>,
  field: string,
  errors: AgentflowWorkflowParseIssue[]
): void {
  if (typeof value[field] !== "string" || String(value[field]).trim().length === 0) {
    errors.push({
      code: `workflow.${field}`,
      path: field,
      message: `Agentflow workflow field ${field} must be a non-empty string.`
    });
  }
}

function validateVersion(value: Record<string, AgentflowYamlValue>, errors: AgentflowWorkflowParseIssue[]): void {
  if (!Number.isInteger(value.version)) {
    errors.push({
      code: "workflow.version",
      path: "version",
      message: "Agentflow workflow field version must be an integer."
    });
    return;
  }

  if (Number(value.version) < 1) {
    errors.push({
      code: "workflow.version.minimum",
      path: "version",
      message: "Agentflow workflow field version must be greater than or equal to 1."
    });
  }
}

function validateEnum<T extends readonly string[]>(
  value: Record<string, AgentflowYamlValue>,
  field: string,
  allowed: T,
  errors: AgentflowWorkflowParseIssue[]
): void {
  if (typeof value[field] !== "string" || !allowed.includes(value[field])) {
    errors.push({
      code: `workflow.${field}`,
      path: field,
      message: `Agentflow workflow field ${field} must be one of: ${allowed.join(", ")}.`
    });
  }
}

function validateOptionalRecord(
  value: Record<string, AgentflowYamlValue>,
  field: string,
  errors: AgentflowWorkflowParseIssue[]
): void {
  if (value[field] !== undefined && !isRecord(value[field])) {
    errors.push({
      code: `workflow.${field}`,
      path: field,
      message: `Agentflow workflow field ${field} must be a mapping when present.`
    });
  }
}

function validateOptionalArray(
  value: Record<string, AgentflowYamlValue>,
  field: string,
  errors: AgentflowWorkflowParseIssue[]
): void {
  if (value[field] !== undefined && !Array.isArray(value[field])) {
    errors.push({
      code: `workflow.${field}`,
      path: field,
      message: `Agentflow workflow field ${field} must be a list when present.`
    });
  }
}

function pathForKey(parent: string, key: string): string {
  return /^[A-Za-z_][A-Za-z0-9_]*$/.test(key) ? `${parent}.${key}` : `${parent}[${JSON.stringify(key)}]`;
}

function isRecord(value: unknown): value is Record<string, AgentflowYamlValue> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
