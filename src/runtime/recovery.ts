import {
  AgentFlowRunStateError,
  MAX_AGENT_FLOW_RECOVERY_CONTEXT_BYTES,
  type AgentFlowRunStateStore,
  type AgentFlowSessionRecord
} from "./run_state";
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
  const source = configuredPath === undefined ? path.dirname(entry) : path.resolve(repoRoot, configuredPath);
  const files = workflowFiles(source).filter((candidate) => path.basename(candidate) !== ".agent-flow.yml");
  if (!files.includes(entry)) files.push(entry);
  const registry = createAgentFlowWorkflowRegistry();
  const loaded = [...new Set(files)].sort().map((file) => ({
    file,
    workflow: parseAgentFlowWorkflowOrThrow(fs.readFileSync(file, "utf8"))
  }));
  if (configuredPath !== undefined) {
    for (const { workflow } of loaded) registry.register(workflow.name, workflow);
  } else {
    const entryWorkflow = loaded.find((candidate) => candidate.file === entry)?.workflow;
    if (entryWorkflow === undefined) throw new Error(`Entry workflow ${entry} was not loaded.`);
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
  assertAcyclicWorkflowRegistry(registry);
  return registry;
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
  assertAcyclicWorkflowRegistry(registry);
  return registry;
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

function assertAcyclicWorkflowRegistry(registry: AgentFlowWorkflowRegistry): void {
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
    for (const child of referencedWorkflowNames(workflow.steps)) {
      if (registry.get(child) === undefined) throw new Error(`Workflow ${name} references missing workflow ${child}.`);
      visit(child, [...lineage, name]);
    }
    visiting.delete(name);
    visited.add(name);
  };
  registry.names().forEach((name) => visit(name, []));
}

function referencedWorkflowNames(steps: AgentFlowWorkflowStep[]): string[] {
  const names = new Set<string>();
  const visit = (step: AgentFlowWorkflowStep): void => {
    if (step.type === "workflow" && typeof step.workflow === "string" && step.workflow.trim().length > 0) {
      names.add(step.workflow.trim());
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
import { findGitRepositoryRoot } from "@jurgen1c/agent-core/repository";
import { parseYamlDocument } from "@jurgen1c/agent-core/yaml";
