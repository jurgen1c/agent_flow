import {
  AgentFlowRunStateError,
  MAX_AGENT_FLOW_RECOVERY_CONTEXT_BYTES,
  normalizeAgentFlowArtifactPath,
  type AgentFlowRunStateStore,
  type AgentFlowSessionRecord
} from "./run_state";
import { defaultAgentFlowApprovalOutputPath } from "./approval";
import { defaultAgentFlowDecisionRecordPath } from "./decision_record";
import { parseAgentFlowWorkflowOrThrow, type AgentFlowWorkflow, type AgentFlowWorkflowStep } from "./workflow";

export type AgentFlowRecoveryStatus = "remediated" | "unresolved";
export { MAX_AGENT_FLOW_RECOVERY_CONTEXT_BYTES } from "./run_state";

export class AgentFlowWorkflowRegistry {
  private readonly workflows = new Map<string, AgentFlowWorkflow>();

  register(name: string, workflow: AgentFlowWorkflow): this {
    const normalized = requiredName(name, "Workflow name");
    if (this.workflows.has(normalized)) {
      throw new Error(`Workflow ${normalized} is already registered.`);
    }
    this.workflows.set(normalized, workflow);
    return this;
  }

  get(name: string): AgentFlowWorkflow | undefined {
    return this.workflows.get(requiredName(name, "Workflow name"));
  }

  names(): string[] {
    return [...this.workflows.keys()].sort();
  }
}

export function createAgentFlowWorkflowRegistry(): AgentFlowWorkflowRegistry {
  return new AgentFlowWorkflowRegistry();
}

export function loadAgentFlowWorkflowRegistry(
  entryWorkflowPath: string,
  options: { cwd?: string } = {}
): AgentFlowWorkflowRegistry {
  const cwd = path.resolve(options.cwd ?? process.cwd());
  const entry = path.resolve(cwd, entryWorkflowPath);
  const repoRoot = findGitRepositoryRoot(cwd) ?? cwd;
  const configPath = path.join(repoRoot, ".agent-flow.yml");
  let configuredPath: string | undefined;
  if (fs.existsSync(configPath)) {
    const parsed = parseYamlDocument(fs.readFileSync(configPath, "utf8"));
    if (!parsed.ok) throw new Error(`Could not parse ${configPath}: ${parsed.issues[0]?.message ?? "invalid YAML"}`);
    const root = parsed.value;
    if (root !== null && typeof root === "object" && !Array.isArray(root)
        && typeof root.workflows === "string" && root.workflows.trim().length > 0) {
      configuredPath = root.workflows.trim();
    }
  }
  const source = configuredPath === undefined
    ? path.dirname(entry)
    : containedWorkflowRegistrySource(repoRoot, configuredPath);
  const files = (configuredPath === undefined ? siblingWorkflowFiles(source) : workflowFiles(source))
    .filter((candidate) => path.basename(candidate) !== ".agent-flow.yml");
  if (!files.includes(entry)) files.push(entry);
  const registry = createAgentFlowWorkflowRegistry();
  const loaded = [...new Set(files)].sort().flatMap((file) => {
    const sourceText = fs.readFileSync(file, "utf8");
    if (configuredPath !== undefined || file === entry) {
      return [{ file, workflow: parseAgentFlowWorkflowOrThrow(sourceText) }];
    }
    try {
      return [{ file, workflow: parseAgentFlowWorkflowOrThrow(sourceText) }];
    } catch {
      return [];
    }
  });
  const entryWorkflow = loaded.find((candidate) => candidate.file === entry)?.workflow;
  if (entryWorkflow === undefined) throw new Error(`Entry workflow ${entry} was not loaded.`);
  if (configuredPath !== undefined) {
    for (const { workflow } of loaded) registry.register(workflow.name, workflow);
  } else {
    const candidates = new Map<string, AgentFlowWorkflow[]>();
    for (const { workflow } of loaded) {
      candidates.set(workflow.name, [...(candidates.get(workflow.name) ?? []), workflow]);
    }
    const registerReachable = (workflow: AgentFlowWorkflow): void => {
      if (registry.get(workflow.name) !== undefined) return;
      registry.register(workflow.name, workflow);
      for (const childName of referencedWorkflowNames(workflow.steps)) {
        const matches = candidates.get(childName) ?? [];
        if (matches.length === 0) throw new Error(`Workflow ${workflow.name} references missing workflow ${childName}.`);
        if (matches.length > 1) throw new Error(`Workflow ${childName} is declared more than once in ${source}.`);
        registerReachable(matches[0]!);
      }
    };
    registerReachable(entryWorkflow);
  }
  assertAgentFlowWorkflowRegistryContracts(registry, [entryWorkflow.name]);
  return registry;
}

function containedWorkflowRegistrySource(repoRoot: string, configuredPath: string): string {
  if (path.isAbsolute(configuredPath)) {
    throw new Error(`Workflow registry path ${configuredPath} must be repository-relative and stay inside ${repoRoot}.`);
  }
  try {
    return resolveContainedPath(repoRoot, configuredPath).absolutePath;
  } catch (error) {
    throw new Error(
      `Workflow registry path ${configuredPath} must be repository-relative and stay inside ${repoRoot}.`,
      { cause: error }
    );
  }
}

export function serializeAgentFlowWorkflowRegistry(
  registry: AgentFlowWorkflowRegistry
): Record<string, AgentFlowWorkflow> {
  return Object.fromEntries(registry.names().map((name) => [name, structuredClone(registry.get(name)!)]));
}

export function createAgentFlowWorkflowRegistryFromSnapshot(
  snapshot: unknown
): AgentFlowWorkflowRegistry {
  if (snapshot === null || typeof snapshot !== "object" || Array.isArray(snapshot)) {
    throw new AgentFlowRunStateError("Persisted workflow registry snapshot is invalid.", "AGENT_FLOW_WORKFLOW_REGISTRY_STATE");
  }
  const registry = createAgentFlowWorkflowRegistry();
  for (const [name, workflow] of Object.entries(snapshot)) {
    if (workflow === null || typeof workflow !== "object" || Array.isArray(workflow)) {
      throw new AgentFlowRunStateError(`Persisted workflow ${name} is invalid.`, "AGENT_FLOW_WORKFLOW_REGISTRY_STATE");
    }
    registry.register(name, workflow as AgentFlowWorkflow);
  }
  assertAgentFlowWorkflowRegistryContracts(registry);
  return registry;
}

export function assertAgentFlowWorkflowRegistryContracts(
  registry: AgentFlowWorkflowRegistry,
  roots: string[] = registry.names()
): void {
  // Recovery workflows are bounded by the runtime lineage guard and may route
  // back to an ancestor. Only direct nested-workflow edges must be acyclic.
  assertAcyclicWorkflowRegistry(registry, roots, referencedNestedWorkflowNames);
  assertWorkflowStepInputContracts(registry, roots);
}

function workflowFiles(source: string): string[] {
  const stat = fs.statSync(source);
  if (stat.isFile()) return [source];
  if (!stat.isDirectory()) throw new Error(`Workflow registry path ${source} is not a file or directory.`);
  return fs.readdirSync(source, { withFileTypes: true }).flatMap((entry) => {
    const candidate = path.join(source, entry.name);
    if (entry.isDirectory()) return workflowFiles(candidate);
    return entry.isFile() && /\.ya?ml$/i.test(entry.name) ? [candidate] : [];
  });
}

function siblingWorkflowFiles(source: string): string[] {
  const stat = fs.statSync(source);
  if (stat.isFile()) return [source];
  if (!stat.isDirectory()) throw new Error(`Workflow registry path ${source} is not a file or directory.`);
  return fs.readdirSync(source, { withFileTypes: true }).flatMap((entry) =>
    entry.isFile() && /\.ya?ml$/i.test(entry.name) ? [path.join(source, entry.name)] : []
  );
}

function assertAcyclicWorkflowRegistry(
  registry: AgentFlowWorkflowRegistry,
  roots: string[],
  references: (steps: AgentFlowWorkflowStep[]) => string[]
): void {
  const visiting = new Set<string>();
  const visited = new Set<string>();
  const visit = (name: string, lineage: string[]): void => {
    if (visiting.has(name)) {
      throw new Error(`Recursive workflow reference detected: ${[...lineage, name].join(" -> ")}.`);
    }
    if (visited.has(name)) return;
    const workflow = registry.get(name);
    if (workflow === undefined) throw new Error(`Referenced workflow ${name} is not registered.`);
    visiting.add(name);
    for (const child of references(workflow.steps)) {
      if (registry.get(child) === undefined) throw new Error(`Workflow ${name} references missing workflow ${child}.`);
      visit(child, [...lineage, name]);
    }
    visiting.delete(name);
    visited.add(name);
  };
  roots.forEach((name) => visit(name, []));
}

function referencedNestedWorkflowNames(steps: AgentFlowWorkflowStep[]): string[] {
  return [...new Set(workflowSteps(steps).flatMap((step) =>
    typeof step.type === "string" && step.type.trim() === "workflow"
      && typeof step.workflow === "string" && step.workflow.trim().length > 0
      ? [step.workflow.trim()]
      : []
  ))].sort();
}

function assertWorkflowStepInputContracts(
  registry: AgentFlowWorkflowRegistry,
  roots: string[] = registry.names()
): void {
  const reachable = new Set<string>();
  const visit = (name: string): void => {
    if (reachable.has(name)) return;
    reachable.add(name);
    const workflow = registry.get(name);
    if (workflow === undefined) return;
    referencedWorkflowNames(workflow.steps).forEach(visit);
  };
  roots.forEach(visit);
  for (const parentName of [...reachable].sort()) {
    const parent = registry.get(parentName)!;
    for (const step of workflowSteps(parent.steps)) {
      if (typeof step.type !== "string" || step.type.trim() !== "workflow") continue;
      if (typeof step.workflow !== "string" || step.workflow.trim().length === 0) continue;
      const childName = step.workflow.trim();
      const child = registry.get(childName);
      if (child === undefined) continue;
      const inputs = step.inputs;
      if (inputs === null || typeof inputs !== "object" || Array.isArray(inputs)) continue;
      const expressionIssue = validateAgentFlowWorkflowStepInputExpressions(
        inputs,
        new Set(Object.keys(parent.inputs ?? {}))
      );
      if (expressionIssue !== undefined) {
        throw new Error(
          `Workflow ${parentName} step ${String(step.id ?? "(unnamed)")} ${expressionIssue}`
        );
      }
      const supplied = Object.keys(inputs);
      const unknown = supplied.filter((name) => !Object.hasOwn(child.inputs ?? {}, name)).sort();
      if (unknown.length > 0) {
        throw new Error(
          `Workflow ${parentName} step ${String(step.id ?? "(unnamed)")} supplies unknown inputs to ${childName}: ${unknown.join(", ")}.`
        );
      }
      const missing = Object.entries(child.inputs ?? {}).flatMap(([name, definition]) =>
        definition !== null && typeof definition === "object" && !Array.isArray(definition)
          && definition.required === true && !Object.hasOwn(inputs, name) ? [name] : []
      ).sort();
      if (missing.length > 0) {
        throw new Error(
          `Workflow ${parentName} step ${String(step.id ?? "(unnamed)")} omits required inputs for ${childName}: ${missing.join(", ")}.`
        );
      }
      const declaredOutputs = workflowDeclaredOutputPaths(child);
      const undeclaredOutputs = Array.isArray(step.outputs)
        ? step.outputs.flatMap((output) => {
            if (typeof output !== "string") return [];
            try {
              const normalized = normalizeAgentFlowArtifactPath(output);
              return declaredOutputs.has(normalized) ? [] : [normalized];
            } catch {
              return [];
            }
          })
        : [];
      if (undeclaredOutputs.length > 0) {
        throw new Error(
          `Workflow ${parentName} step ${String(step.id ?? "(unnamed)")} requests output${undeclaredOutputs.length === 1 ? "" : "s"} `
            + `${undeclaredOutputs.map((output) => JSON.stringify(output)).join(", ")} that child workflow ${childName} does not declare.`
        );
      }
    }
  }
}

function workflowDeclaredOutputPaths(workflow: AgentFlowWorkflow): Set<string> {
  const outputs = new Set<string>();
  for (const step of workflowSteps(workflow.steps)) {
    const stepId = typeof step.id === "string" ? step.id.trim() : "";
    const type = typeof step.type === "string" ? step.type.trim() : "";
    const candidates: unknown[] = [
      step.output,
      step.save_as,
      ...(Array.isArray(step.outputs) ? step.outputs : []),
      ...(stepId.length > 0 && type === "approval" && step.output === undefined
        ? [defaultAgentFlowApprovalOutputPath(stepId)]
        : []),
      ...(stepId.length > 0 && type === "decision_record" && step.output === undefined
        ? [defaultAgentFlowDecisionRecordPath(stepId)]
        : [])
    ];
    for (const candidate of candidates) {
      if (typeof candidate !== "string") continue;
      try { outputs.add(normalizeAgentFlowArtifactPath(candidate)); } catch { /* Validation reports malformed paths. */ }
    }
  }
  return outputs;
}

export function validateAgentFlowWorkflowStepInputExpressions(
  value: unknown,
  declaredInputs: ReadonlySet<string>
): string | undefined {
  if (typeof value === "string") {
    if (!value.includes("{{") && !value.includes("}}")) return undefined;
    const expression = /^\{\{\s*(?:step\.id|inputs\.([A-Za-z_][A-Za-z0-9_-]*)|artifacts\.[A-Za-z0-9_.-]+)\s*}}$/.exec(value);
    if (expression === null) {
      return `has unsupported input expression ${JSON.stringify(value)}; expressions must occupy the whole value and use step.id, inputs.<name>, or artifacts.<path>.`;
    }
    const inputName = expression[1];
    return inputName === undefined || declaredInputs.has(inputName)
      ? undefined
      : `references undeclared workflow input ${inputName}.`;
  }
  if (Array.isArray(value)) {
    for (const entry of value) {
      const issue = validateAgentFlowWorkflowStepInputExpressions(entry, declaredInputs);
      if (issue !== undefined) return issue;
    }
    return undefined;
  }
  if (value !== null && typeof value === "object") {
    for (const entry of Object.values(value)) {
      const issue = validateAgentFlowWorkflowStepInputExpressions(entry, declaredInputs);
      if (issue !== undefined) return issue;
    }
  }
  return undefined;
}

function workflowSteps(steps: AgentFlowWorkflowStep[]): AgentFlowWorkflowStep[] {
  const collected: AgentFlowWorkflowStep[] = [];
  const visit = (step: AgentFlowWorkflowStep): void => {
    collected.push(step);
    for (const field of ["body", "steps", "branches"] as const) {
      const nested = step[field];
      if (Array.isArray(nested)) nested.forEach((candidate) => {
        if (candidate !== null && typeof candidate === "object" && !Array.isArray(candidate)) {
          visit(candidate as AgentFlowWorkflowStep);
        }
      });
    }
  };
  steps.forEach(visit);
  return collected;
}

function referencedWorkflowNames(steps: AgentFlowWorkflowStep[]): string[] {
  const names = new Set<string>();
  const visit = (step: AgentFlowWorkflowStep): void => {
    if (typeof step.type === "string" && step.type.trim() === "workflow"
        && typeof step.workflow === "string" && step.workflow.trim().length > 0) {
      names.add(step.workflow.trim());
    }
    const onFailure = step.on_failure;
    if (onFailure !== null && typeof onFailure === "object" && !Array.isArray(onFailure)) {
      const route = onFailure.route_to;
      if (route !== null && typeof route === "object" && !Array.isArray(route)
          && typeof route.workflow === "string" && route.workflow.trim().length > 0) {
        names.add(route.workflow.trim());
      }
    }
    for (const field of ["body", "steps", "branches"] as const) {
      const nested = step[field];
      if (Array.isArray(nested)) nested.forEach((candidate) => {
        if (candidate !== null && typeof candidate === "object" && !Array.isArray(candidate)) visit(candidate as AgentFlowWorkflowStep);
      });
    }
  };
  steps.forEach(visit);
  return [...names].sort();
}

export function injectAgentFlowRecoveryContext(
  store: AgentFlowRunStateStore,
  runId: string,
  sessionId: string,
  context: string
): AgentFlowSessionRecord {
  if (typeof context !== "string" || context.trim().length === 0) {
    throw new AgentFlowRunStateError(
      "Injected recovery context must be non-empty text.",
      "AGENT_FLOW_RECOVERY_CONTEXT_INVALID"
    );
  }
  if (Buffer.byteLength(context, "utf8") > MAX_AGENT_FLOW_RECOVERY_CONTEXT_BYTES) {
    throw new AgentFlowRunStateError(
      `Injected recovery context exceeds the ${MAX_AGENT_FLOW_RECOVERY_CONTEXT_BYTES}-byte limit.`,
      "AGENT_FLOW_RECOVERY_CONTEXT_INVALID"
    );
  }
  return store.injectRecoverySessionContext(runId, sessionId, context);
}

function requiredName(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`${label} must be a non-empty string.`);
  }
  return value.trim();
}
import fs from "node:fs";
import path from "node:path";
import { findGitRepositoryRoot, resolveContainedPath } from "@jurgen1c/agent-core/repository";
import { parseYamlDocument } from "@jurgen1c/agent-core/yaml";
