import { lintAgentflowWorkflow } from "./validation";
import type {
  AgentflowWorkflow,
  AgentflowWorkflowStep,
  AgentflowYamlMapping,
  AgentflowYamlValue
} from "./workflow";
import { assertAgentflowSuccessTargetsAreUnambiguous } from "./success_routing";

export interface AgentflowWorkflowGraphNode {
  id: string;
  type: string;
  path: string;
  label?: string;
}

export interface AgentflowWorkflowGraphEdge {
  from: string;
  to: string;
  kind: string;
  label?: string;
}

export interface AgentflowWorkflowGraph {
  workflow: {
    name: string;
    version: number;
    style: AgentflowWorkflow["style"];
    maturity: AgentflowWorkflow["maturity"];
  };
  nodes: AgentflowWorkflowGraphNode[];
  edges: AgentflowWorkflowGraphEdge[];
}

export class AgentflowWorkflowGraphError extends Error {
  readonly code = "workflow.graph.node_id_collision";
  readonly nodeId: string;

  constructor(nodeId: string, existing: AgentflowWorkflowGraphNode, duplicate: AgentflowWorkflowGraphNode) {
    super(
      `Graph node id "${nodeId}" collides between ${existing.type} at ${existing.path} and ${duplicate.type} at ${duplicate.path}.`
    );
    this.name = "AgentflowWorkflowGraphError";
    this.nodeId = nodeId;
  }
}

interface LocatedStep {
  step: AgentflowWorkflowStep;
  path: string;
  nextId?: string;
}

export function explainAgentflowWorkflow(workflow: AgentflowWorkflow): string {
  const lines = [
    `Workflow: ${workflow.name} (version ${workflow.version})`,
    `Style: ${workflow.style}`,
    `Maturity: ${workflow.maturity}`
  ];
  const description = nonEmptyString(workflow.description);

  if (description !== undefined) {
    lines.push(`Description: ${description}`);
  }

  lines.push("", "Inputs:");
  appendNamedValues(lines, workflow.inputs);
  lines.push("", "Sessions:");
  appendSessions(lines, workflow.sessions);

  const collaboration = record(workflow.collaboration);
  if (collaboration !== undefined) {
    const details = [
      collaboration.enabled === true ? "enabled" : "disabled",
      ...namedScalarDetails(collaboration, ["max_review_cycles", "on_disagreement"])
    ];
    lines.push(`Collaboration: ${details.join("; ")}`);
  }

  lines.push("", "Steps:");
  if (workflow.steps.length === 0) {
    lines.push("  (none)");
  } else {
    workflow.steps.forEach((step) => appendExplainedStep(lines, step, 1));
  }

  const reads = uniqueSorted(workflow.steps.flatMap((step) => collectArtifacts(step, "read")));
  const writes = uniqueSorted(workflow.steps.flatMap((step) => collectArtifacts(step, "write")));
  lines.push("", "Artifacts:");
  lines.push(`  Declared: ${namedKeys(workflow.artifacts)}`);
  lines.push(`  Read: ${reads.length === 0 ? "(none)" : reads.join(", ")}`);
  lines.push(`  Written: ${writes.length === 0 ? "(none)" : writes.join(", ")}`);

  lines.push("", "Policies:");
  const policyEntries: Array<[string, AgentflowYamlValue | undefined]> = [
    ["policies", workflow.policies],
    ["limits", workflow.limits],
    ["retention", workflow.retention],
    ["notify", workflow.notify]
  ];
  const presentPolicies = policyEntries.filter(([, value]) => value !== undefined);
  if (presentPolicies.length === 0) {
    lines.push("  (none)");
  } else {
    presentPolicies.forEach(([name, value]) => lines.push(`  ${name}: ${stableInline(value)}`));
  }

  const warnings = lintAgentflowWorkflow(workflow).warnings;
  lines.push("", "Warnings:");
  if (warnings.length === 0) {
    lines.push("  (none)");
  } else {
    warnings.forEach((warning) => lines.push(`  - ${warning.code} (${warning.path}): ${warning.message}`));
  }

  return lines.join("\n");
}

export function buildAgentflowWorkflowGraph(workflow: AgentflowWorkflow): AgentflowWorkflowGraph {
  assertAgentflowSuccessTargetsAreUnambiguous(workflow.steps);
  const nodes: AgentflowWorkflowGraphNode[] = [];
  const edges: AgentflowWorkflowGraphEdge[] = [];
  const locatedSteps: LocatedStep[] = [];

  collectStepList(workflow.steps, "steps", nodes, edges, locatedSteps);

  const stepIds = new Set(locatedSteps.map(({ step }) => nonEmptyString(step.id)).filter(isString));
  for (const { step, nextId } of locatedSteps) {
    const source = nonEmptyString(step.id);
    if (source === undefined) {
      continue;
    }

    const targets = explicitTargets(step);
    for (const target of targets) {
      if (stepIds.has(target.to)) {
        edges.push({ from: source, to: target.to, kind: target.kind, ...(target.label === undefined ? {} : { label: target.label }) });
      } else if (["continue", "ignore"].includes(target.to)) {
        if (nextId !== undefined) edges.push({ from: source, to: nextId, kind: "next" });
      } else if (isTerminalTarget(target.to)) {
        const terminalId = `terminal:${target.to}`;
        nodes.push({ id: terminalId, type: "terminal", path: terminalId, label: target.to });
        edges.push({ from: source, to: terminalId, kind: target.kind, ...(target.label === undefined ? {} : { label: target.label }) });
      }
    }

    if (step.type === "manual_gate") {
      for (const option of stringValues(step.options)) {
        if (option === "reject" && !targets.some((target) => target.kind === "on_reject")) {
          const terminalId = "terminal:cancel";
          nodes.push({ id: terminalId, type: "terminal", path: terminalId, label: "cancel" });
          edges.push({ from: source, to: terminalId, kind: "option", label: option });
          continue;
        }
        if (!isTerminalTarget(option) || targets.some((target) => target.to === option)) {
          continue;
        }
        const terminalId = `terminal:${option}`;
        nodes.push({ id: terminalId, type: "terminal", path: terminalId, label: option });
        edges.push({ from: source, to: terminalId, kind: "option", label: option });
      }
    }
  }

  return {
    workflow: {
      name: workflow.name,
      version: workflow.version,
      style: workflow.style,
      maturity: workflow.maturity
    },
    nodes: stableUniqueNodes(nodes),
    edges: stableUniqueEdges(edges)
  };
}

export function renderAgentflowWorkflowGraph(workflow: AgentflowWorkflow): string {
  const graph = buildAgentflowWorkflowGraph(workflow);
  const lines = [
    `Workflow graph: ${graph.workflow.name} (version ${graph.workflow.version})`,
    `Style: ${graph.workflow.style}`,
    `Maturity: ${graph.workflow.maturity}`,
    "",
    "Nodes:"
  ];

  if (graph.nodes.length === 0) {
    lines.push("  (none)");
  } else {
    graph.nodes.forEach((node) => {
      const label = node.label === undefined ? "" : ` — ${node.label}`;
      lines.push(`  ${node.id} [${node.type}]${label} (${node.path})`);
    });
  }

  lines.push("", "Edges:");
  if (graph.edges.length === 0) {
    lines.push("  (none)");
  } else {
    graph.edges.forEach((edge) => {
      const description = edge.label === undefined ? edge.kind : edge.label;
      lines.push(`  ${edge.from} -> ${edge.to} [${description}]`);
    });
  }

  return lines.join("\n");
}

function collectStepList(
  steps: AgentflowWorkflowStep[],
  path: string,
  nodes: AgentflowWorkflowGraphNode[],
  edges: AgentflowWorkflowGraphEdge[],
  locatedSteps: LocatedStep[],
  parent?: { id: string; label: string }
): void {
  const ids = steps.map((step) => nonEmptyString(step.id));
  const first = ids.find(isString);
  if (parent !== undefined && first !== undefined) {
    edges.push({ from: parent.id, to: first, kind: "contains", label: parent.label });
  }

  steps.forEach((step, index) => {
    const stepPath = `${path}[${index}]`;
    const id = nonEmptyString(step.id);
    const type = nonEmptyString(step.type) ?? "unknown";
    if (id === undefined) {
      return;
    }

    nodes.push({ id, type, path: stepPath, ...(stepLabel(step) === undefined ? {} : { label: stepLabel(step) }) });
    const nextId = ids.slice(index + 1).find(isString);
    locatedSteps.push({ step, path: stepPath, ...(nextId === undefined ? {} : { nextId }) });
    if (nextId !== undefined && !hasPrimaryControlTarget(step) && type !== "result") {
      edges.push({ from: id, to: nextId, kind: "next" });
    }

    for (const field of ["body", "steps"] as const) {
      const nested = step[field];
      if (Array.isArray(nested)) {
        collectStepList(
          nested.filter(isRecord) as AgentflowWorkflowStep[],
          `${stepPath}.${field}`,
          nodes,
          edges,
          locatedSteps,
          { id, label: type === "loop" ? `loop ${field}` : field }
        );
      }
    }

    if (type === "parallel" && Array.isArray(step.branches)) {
      step.branches.forEach((value, branchIndex) => {
        if (!isRecord(value)) {
          return;
        }
        const branchName = nonEmptyString(value.id) ?? String(branchIndex + 1);
        const branchId = `${id}.branch.${branchName}`;
        const branchPath = `${stepPath}.branches[${branchIndex}]`;
        const branchLabel = namedScalarDetails(value, ["session", "strategy"]).join("; ");
        nodes.push({ id: branchId, type: "parallel_branch", path: branchPath, ...(branchLabel.length === 0 ? {} : { label: branchLabel }) });
        edges.push({ from: id, to: branchId, kind: "branch", label: branchName });

        for (const field of ["body", "steps"] as const) {
          const nested = value[field];
          if (Array.isArray(nested)) {
            collectStepList(
              nested.filter(isRecord) as AgentflowWorkflowStep[],
              `${branchPath}.${field}`,
              nodes,
              edges,
              locatedSteps,
              { id: branchId, label: field }
            );
          }
        }
      });
    }
  });
}

function appendExplainedStep(lines: string[], step: AgentflowWorkflowStep, depth: number): void {
  const indent = "  ".repeat(depth);
  const id = nonEmptyString(step.id) ?? "(missing id)";
  const type = nonEmptyString(step.type) ?? "unknown";
  const label = stepLabel(step);
  lines.push(`${indent}- ${id} [${type}]${label === undefined ? "" : ` — ${label}`}`);

  const reads = uniqueSorted(collectDirectArtifacts(step, "read"));
  const writes = uniqueSorted(collectDirectArtifacts(step, "write"));
  if (reads.length > 0) {
    lines.push(`${indent}  reads: ${reads.join(", ")}`);
  }
  if (writes.length > 0) {
    lines.push(`${indent}  writes: ${writes.join(", ")}`);
  }

  for (const field of ["body", "steps"] as const) {
    const nested = step[field];
    if (Array.isArray(nested)) {
      lines.push(`${indent}  ${field}:`);
      nested.filter(isRecord).forEach((entry) => appendExplainedStep(lines, entry as AgentflowWorkflowStep, depth + 2));
    }
  }

  if (type === "parallel" && Array.isArray(step.branches)) {
    step.branches.forEach((value, index) => {
      if (!isRecord(value)) {
        return;
      }
      const branchName = nonEmptyString(value.id) ?? String(index + 1);
      const details = namedScalarDetails(value, ["session", "strategy"]);
      lines.push(`${indent}  - branch ${branchName}${details.length === 0 ? "" : ` — ${details.join("; ")}`}`);
      for (const field of ["body", "steps"] as const) {
        const nested = value[field];
        if (Array.isArray(nested)) {
          nested.filter(isRecord).forEach((entry) => appendExplainedStep(lines, entry as AgentflowWorkflowStep, depth + 2));
        }
      }
    });
  }
}

function stepLabel(step: AgentflowWorkflowStep): string | undefined {
  const fieldsByType: Record<string, string[]> = {
    approval: ["reviewer"],
    challenge: ["from", "to"],
    consult: ["from", "to"],
    decision_record: ["owner", "topic"],
    handoff: ["from", "to"],
    manual_gate: ["message"],
    mcp_call: ["server", "tool"],
    review: ["reviewer", "subject"],
    session_request: ["session", "prompt"],
    workflow: ["workflow"]
  };
  const details = namedScalarDetails(step, fieldsByType[String(step.type)] ?? []);
  if (step.type === "manual_gate" && stringValues(step.options).length > 0) {
    details.push(`options=${stringValues(step.options).join(",")}`);
  }
  return details.length === 0 ? undefined : details.join("; ");
}

function explicitTargets(step: AgentflowWorkflowStep): Array<{ to: string; kind: string; label?: string }> {
  const targets: Array<{ to: string; kind: string; label?: string }> = [];
  for (const field of ["then", "else", "goto", "on_approve", "on_cancel", "on_reject", "return_to"] as const) {
    const value = nonEmptyString(step[field]);
    if (value !== undefined && !isDynamic(value)) {
      targets.push({ to: value, kind: field });
    }
  }

  if (Array.isArray(step.branches) && step.type !== "parallel") {
    step.branches.forEach((value) => {
      if (!isRecord(value)) {
        return;
      }
      const target = nonEmptyString(value.then);
      if (target !== undefined && !isDynamic(target)) {
        const condition = nonEmptyString(value.if);
        targets.push({ to: target, kind: "then", ...(condition === undefined ? {} : { label: `if ${condition}` }) });
      }
    });
  }

  const onFailure = record(step.on_failure);
  if (onFailure !== undefined) {
    collectFailureTargets(onFailure, "on_failure", targets);
  }
  return targets;
}

function collectFailureTargets(
  failure: AgentflowYamlMapping,
  prefix: string,
  targets: Array<{ to: string; kind: string; label?: string }>
): void {
  for (const field of ["then", "return_to"] as const) {
    const value = nonEmptyString(failure[field]);
    if (value !== undefined && !isDynamic(value)) {
      targets.push({ to: value, kind: `${prefix}.${field}` });
    }
  }
  for (const field of ["on_remediated", "on_unresolved"] as const) {
    const nested = record(failure[field]);
    if (nested !== undefined) {
      collectFailureTargets(nested, `${prefix}.${field}`, targets);
    }
  }
}

function hasPrimaryControlTarget(step: AgentflowWorkflowStep): boolean {
  if (step.type === "condition") {
    const hasElse = nonEmptyString(step.else) !== undefined;
    const hasThen = nonEmptyString(step.then) !== undefined;
    const hasBranches = Array.isArray(step.branches) && step.branches.length > 0 && step.branches.every((branch) =>
      isRecord(branch) && nonEmptyString(branch.then) !== undefined
    );
    return hasElse && (hasThen || hasBranches);
  }

  return ["goto", "on_approve", "return_to", "then"].some((field) => nonEmptyString(step[field]) !== undefined);
}

function collectArtifacts(step: AgentflowWorkflowStep, direction: "read" | "write"): string[] {
  const direct = collectDirectArtifacts(step, direction);
  const nested: string[] = [];
  for (const field of ["body", "steps", ...(step.type === "parallel" ? ["branches"] : [])]) {
    const values = step[field];
    if (Array.isArray(values)) {
      values.filter(isRecord).forEach((entry) => nested.push(...collectArtifacts(entry as AgentflowWorkflowStep, direction)));
    }
  }
  return [...direct, ...nested];
}

function collectDirectArtifacts(step: AgentflowWorkflowStep, direction: "read" | "write"): string[] {
  if (direction === "read") {
    const input = nonEmptyString(step.input);
    const inputs = stringValues(step.inputs);
    const artifacts = ["approval", "decision_record", "review"].includes(String(step.type)) ? stringValues(step.artifacts) : [];
    return [...inputs, ...(input === undefined ? [] : [input]), ...artifacts];
  }
  const output = nonEmptyString(step.output);
  const saveAs = step.type === "input_request" ? nonEmptyString(step.save_as) : undefined;
  return [...stringValues(step.outputs), ...(output === undefined ? [] : [output]), ...(saveAs === undefined ? [] : [saveAs])];
}

function appendNamedValues(lines: string[], values: Record<string, AgentflowYamlValue> | undefined): void {
  if (values === undefined || Object.keys(values).length === 0) {
    lines.push("  (none)");
    return;
  }
  Object.keys(values).sort().forEach((name) => lines.push(`  ${name}: ${stableInline(values[name])}`));
}

function appendSessions(lines: string[], sessions: Record<string, AgentflowYamlValue> | undefined): void {
  if (sessions === undefined || Object.keys(sessions).length === 0) {
    lines.push("  (none)");
    return;
  }
  Object.keys(sessions).sort().forEach((name) => {
    const value = sessions[name];
    const details = isRecord(value) ? namedScalarDetails(value, ["provider", "role", "resume"]) : [];
    lines.push(`  ${name}${details.length === 0 ? "" : `: ${details.join("; ")}`}`);
  });
}

function stableUniqueNodes(nodes: AgentflowWorkflowGraphNode[]): AgentflowWorkflowGraphNode[] {
  const byId = new Map<string, AgentflowWorkflowGraphNode>();
  nodes.forEach((node) => {
    const existing = byId.get(node.id);
    if (existing === undefined) {
      byId.set(node.id, node);
    } else if (!sameGraphNode(existing, node)) {
      throw new AgentflowWorkflowGraphError(node.id, existing, node);
    }
  });
  return [...byId.values()].sort((left, right) => compareGraphPaths(left.path, right.path) || left.id.localeCompare(right.id));
}

function sameGraphNode(left: AgentflowWorkflowGraphNode, right: AgentflowWorkflowGraphNode): boolean {
  return left.type === right.type && left.path === right.path && left.label === right.label;
}

function compareGraphPaths(left: string, right: string): number {
  const leftParts = left.split(/(\d+)/);
  const rightParts = right.split(/(\d+)/);
  const length = Math.max(leftParts.length, rightParts.length);

  for (let index = 0; index < length; index += 1) {
    const leftPart = leftParts[index];
    const rightPart = rightParts[index];
    if (leftPart === rightPart) {
      continue;
    }
    if (leftPart === undefined) {
      return -1;
    }
    if (rightPart === undefined) {
      return 1;
    }
    if (/^\d+$/.test(leftPart) && /^\d+$/.test(rightPart)) {
      const difference = Number(leftPart) - Number(rightPart);
      if (difference !== 0) {
        return difference;
      }
    }
    return leftPart.localeCompare(rightPart);
  }

  return 0;
}

function stableUniqueEdges(edges: AgentflowWorkflowGraphEdge[]): AgentflowWorkflowGraphEdge[] {
  const unique = new Map<string, AgentflowWorkflowGraphEdge>();
  edges.forEach((edge) => unique.set(`${edge.from}\0${edge.to}\0${edge.kind}\0${edge.label ?? ""}`, edge));
  return [...unique.values()].sort((left, right) =>
    left.from.localeCompare(right.from) || left.to.localeCompare(right.to) || left.kind.localeCompare(right.kind) || (left.label ?? "").localeCompare(right.label ?? "")
  );
}

function namedScalarDetails(value: AgentflowYamlMapping, fields: string[]): string[] {
  return fields.flatMap((field) => {
    const entry = value[field];
    return typeof entry === "string" || typeof entry === "number" || typeof entry === "boolean" ? [`${field}=${String(entry)}`] : [];
  });
}

function stableInline(value: AgentflowYamlValue | undefined): string {
  return JSON.stringify(sortValue(value));
}

function sortValue(value: AgentflowYamlValue | undefined): AgentflowYamlValue | undefined {
  if (Array.isArray(value)) {
    return value.map(sortValue) as AgentflowYamlValue[];
  }
  if (isRecord(value)) {
    const sorted: AgentflowYamlMapping = {};
    Object.keys(value).sort().forEach((key) => { sorted[key] = sortValue(value[key]); });
    return sorted;
  }
  return value;
}

function namedKeys(value: Record<string, AgentflowYamlValue> | undefined): string {
  const keys = Object.keys(value ?? {}).sort();
  return keys.length === 0 ? "(none)" : keys.join(", ");
}

function stringValues(value: AgentflowYamlValue | undefined): string[] {
  return Array.isArray(value) ? value.filter(isString) : [];
}

function record(value: AgentflowYamlValue | undefined): AgentflowYamlMapping | undefined {
  return isRecord(value) ? value : undefined;
}

function isRecord(value: unknown): value is AgentflowYamlMapping {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function nonEmptyString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : undefined;
}

function isString(value: unknown): value is string {
  return typeof value === "string";
}

function uniqueSorted(values: string[]): string[] {
  return [...new Set(values)].sort();
}

function isTerminalTarget(value: string): boolean {
  return [
    "cancel",
    "cancelled",
    "complete",
    "completed",
    "continue",
    "fail",
    "failed",
    "ignore",
    "pause",
    "paused"
  ].includes(value);
}

function isDynamic(value: string): boolean {
  return value.includes("{{") || value.includes("}}");
}
