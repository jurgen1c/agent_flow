import { parseYamlDocument } from "@jurgen1c/agent-core/yaml";

export type AgentFlowWorkflowStyle = "pipeline" | "recovery_pipeline" | "collaborative";
export type AgentFlowMaturity = "draft" | "experimental" | "stable" | "trusted";

export type AgentFlowYamlValue =
  | null
  | boolean
  | number
  | string
  | AgentFlowYamlValue[]
  | AgentFlowYamlMapping;

export interface AgentFlowYamlMapping {
  [key: string]: AgentFlowYamlValue | undefined;
}

export interface AgentFlowWorkflowStep {
  id?: string;
  type?: string;
  [key: string]: AgentFlowYamlValue | undefined;
}

export interface AgentFlowWorkflow {
  name: string;
  version: number;
  style: AgentFlowWorkflowStyle;
  maturity: AgentFlowMaturity;
  inputs?: Record<string, AgentFlowYamlValue>;
  sessions?: Record<string, AgentFlowYamlValue>;
  artifacts?: Record<string, AgentFlowYamlValue>;
  steps: AgentFlowWorkflowStep[];
  policies?: Record<string, AgentFlowYamlValue>;
  notify?: AgentFlowYamlValue[];
  retention?: Record<string, AgentFlowYamlValue>;
  [key: string]: AgentFlowYamlValue | undefined;
}

export interface AgentFlowWorkflowParseSuccess {
  ok: true;
  workflow: AgentFlowWorkflow;
}

export interface AgentFlowWorkflowParseFailure {
  ok: false;
  errors: AgentFlowWorkflowParseIssue[];
}

export interface AgentFlowWorkflowParseIssue {
  code: string;
  message: string;
  path?: string;
  line?: number;
  column?: number;
}

export type AgentFlowWorkflowParseResult = AgentFlowWorkflowParseSuccess | AgentFlowWorkflowParseFailure;

const WORKFLOW_STYLES = ["pipeline", "recovery_pipeline", "collaborative"] as const;
const WORKFLOW_MATURITIES = ["draft", "experimental", "stable", "trusted"] as const;

export function parseAgentFlowWorkflow(source: string): AgentFlowWorkflowParseResult {
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
    workflow: yamlResult.value as AgentFlowWorkflow
  };
}

export function parseAgentFlowWorkflowOrThrow(source: string): AgentFlowWorkflow {
  const result = parseAgentFlowWorkflow(source);

  if (result.ok) {
    return result.workflow;
  }

  throw new AgentFlowWorkflowParseError(formatWorkflowParseIssues(result.errors), result.errors);
}

export function formatWorkflowParseIssues(issues: AgentFlowWorkflowParseIssue[]): string {
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

export class AgentFlowWorkflowParseError extends Error {
  readonly issues: AgentFlowWorkflowParseIssue[];

  constructor(message: string, issues: AgentFlowWorkflowParseIssue[]) {
    super(message);
    this.name = "AgentFlowWorkflowParseError";
    this.issues = issues;
  }
}

type WorkflowYamlResult =
  | { ok: true; value: AgentFlowYamlValue }
  | { ok: false; errors: AgentFlowWorkflowParseIssue[] };

function parseWorkflowYaml(source: string): WorkflowYamlResult {
  const result = parseYamlDocument(source);

  if (!result.ok) {
    return {
      ok: false,
      errors: result.issues.map((issue) => ({
        code: issue.code === "yaml.syntax" ? "workflow.yaml" : "workflow.yaml.value",
        message: issue.message,
        ...(issue.path === undefined ? {} : { path: issue.path }),
        ...(issue.line === undefined ? {} : { line: issue.line }),
        ...(issue.column === undefined ? {} : { column: issue.column })
      }))
    };
  }

  return {
    ok: true,
    value: result.value as AgentFlowYamlValue
  };
}

function validateWorkflowRoot(value: AgentFlowYamlValue): AgentFlowWorkflowParseIssue[] {
  const errors: AgentFlowWorkflowParseIssue[] = [];

  if (!isRecord(value)) {
    return [
      {
        code: "workflow.root",
        message: "Agent Flow workflow YAML must parse to a mapping at the document root."
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
      message: "Agent Flow workflow field steps must be a list."
    });
  } else {
    value.steps.forEach((step, index) => {
      if (!isRecord(step)) {
        errors.push({
          code: "workflow.steps.item",
          path: `steps[${index}]`,
          message: "Agent Flow workflow step entries must be mappings."
        });
      }
    });
  }

  return errors;
}

function validateString(
  value: Record<string, AgentFlowYamlValue>,
  field: string,
  errors: AgentFlowWorkflowParseIssue[]
): void {
  if (typeof value[field] !== "string" || String(value[field]).trim().length === 0) {
    errors.push({
      code: `workflow.${field}`,
      path: field,
      message: `Agent Flow workflow field ${field} must be a non-empty string.`
    });
  }
}

function validateVersion(value: Record<string, AgentFlowYamlValue>, errors: AgentFlowWorkflowParseIssue[]): void {
  if (!Number.isInteger(value.version)) {
    errors.push({
      code: "workflow.version",
      path: "version",
      message: "Agent Flow workflow field version must be an integer."
    });
    return;
  }

  if (Number(value.version) < 1) {
    errors.push({
      code: "workflow.version.minimum",
      path: "version",
      message: "Agent Flow workflow field version must be greater than or equal to 1."
    });
  }
}

function validateEnum<T extends readonly string[]>(
  value: Record<string, AgentFlowYamlValue>,
  field: string,
  allowed: T,
  errors: AgentFlowWorkflowParseIssue[]
): void {
  if (typeof value[field] !== "string" || !allowed.includes(value[field])) {
    errors.push({
      code: `workflow.${field}`,
      path: field,
      message: `Agent Flow workflow field ${field} must be one of: ${allowed.join(", ")}.`
    });
  }
}

function validateOptionalRecord(
  value: Record<string, AgentFlowYamlValue>,
  field: string,
  errors: AgentFlowWorkflowParseIssue[]
): void {
  if (value[field] !== undefined && !isRecord(value[field])) {
    errors.push({
      code: `workflow.${field}`,
      path: field,
      message: `Agent Flow workflow field ${field} must be a mapping when present.`
    });
  }
}

function validateOptionalArray(
  value: Record<string, AgentFlowYamlValue>,
  field: string,
  errors: AgentFlowWorkflowParseIssue[]
): void {
  if (value[field] !== undefined && !Array.isArray(value[field])) {
    errors.push({
      code: `workflow.${field}`,
      path: field,
      message: `Agent Flow workflow field ${field} must be a list when present.`
    });
  }
}

function isRecord(value: unknown): value is Record<string, AgentFlowYamlValue> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
