import type {
  AgentflowWorkflow,
  AgentflowWorkflowStep,
  AgentflowYamlMapping,
  AgentflowYamlValue
} from "./workflow";

export type AgentflowSimulationStatus = "completed" | "failed" | "paused" | "cancelled" | "unresolved";
export type AgentflowSimulationStepOutcome = "succeeded" | "failed";
export type AgentflowSimulationVisitedOutcome = AgentflowSimulationStepOutcome | "selected";

export interface AgentflowSimulationStepFixture {
  outcome?: AgentflowSimulationStepOutcome | AgentflowSimulationStepOutcome[];
  outputs?: string[] | Record<string, AgentflowYamlValue>;
  condition?: string | boolean | Array<string | boolean>;
  choice?: string | string[];
  iterations?: number;
  input?: AgentflowYamlValue;
  recovery?: "remediated" | "unresolved";
}

export interface AgentflowSimulationFixture {
  inputs?: Record<string, AgentflowYamlValue>;
  artifacts?: Record<string, AgentflowYamlValue>;
  steps?: Record<string, AgentflowSimulationStepFixture>;
}

export interface AgentflowSimulationVisitedStep {
  id: string;
  type: string;
  outcome: AgentflowSimulationVisitedOutcome;
}

export interface AgentflowSimulationMissingArtifact {
  stepId: string;
  artifact: string;
  kind: "input" | "output";
}

export interface AgentflowSimulationUnresolvedBranch {
  stepId: string;
  reason: string;
}

export interface AgentflowSimulationTerminalState {
  stepId: string;
  status: string;
}

export interface AgentflowSimulationResult {
  workflow: {
    name: string;
    version: number;
    style: AgentflowWorkflow["style"];
  };
  status: AgentflowSimulationStatus;
  visitedSteps: AgentflowSimulationVisitedStep[];
  missingInputs: string[];
  availableArtifacts: string[];
  missingArtifacts: AgentflowSimulationMissingArtifact[];
  unresolvedBranches: AgentflowSimulationUnresolvedBranch[];
  terminalStates: AgentflowSimulationTerminalState[];
}

export type AgentflowSimulationFixtureParseResult =
  | { ok: true; fixture: AgentflowSimulationFixture }
  | { ok: false; error: string };

interface SimulationState {
  fixture: AgentflowSimulationFixture;
  artifacts: Set<string>;
  visitedSteps: AgentflowSimulationVisitedStep[];
  missingArtifacts: AgentflowSimulationMissingArtifact[];
  unresolvedBranches: AgentflowSimulationUnresolvedBranch[];
  terminalStates: AgentflowSimulationTerminalState[];
  missingInputs: string[];
  visits: Map<string, number>;
  retryAttempts: Map<string, number>;
  recoveryCycles: Map<string, number>;
  maxRecoveryCycles?: number;
  stepLocations: Map<string, SimulationStepLocation>;
  transitionCount: number;
  status?: AgentflowSimulationStatus;
}

interface SimulationStepLocation {
  steps: AgentflowWorkflowStep[];
  index: number;
  insideLoop: boolean;
}

type SequenceControl =
  | { kind: "done" }
  | { kind: "target"; target: string; budgetChecked?: boolean }
  | { kind: "break_loop" }
  | { kind: "terminal"; status: AgentflowSimulationStatus };

const TERMINAL_TARGETS = new Set([
  "cancel", "cancelled", "complete", "completed", "fail", "failed", "pause", "paused", "unresolved"
]);
const MAX_SIMULATION_TRANSITIONS = 10_000;

export function parseAgentflowSimulationFixture(source: string): AgentflowSimulationFixtureParseResult {
  let value: unknown;

  try {
    value = JSON.parse(source);
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : String(error) };
  }

  if (!isRecord(value)) {
    return { ok: false, error: "Simulation fixture must be a JSON object." };
  }

  const topLevelFields = new Set(["inputs", "artifacts", "steps"]);
  const unknownTopLevel = Object.keys(value).find((field) => !topLevelFields.has(field));
  if (unknownTopLevel !== undefined) {
    return { ok: false, error: `Unknown simulation fixture field ${unknownTopLevel}.` };
  }

  for (const field of ["inputs", "artifacts", "steps"] as const) {
    if (value[field] !== undefined && !isRecord(value[field])) {
      return { ok: false, error: `Simulation fixture field ${field} must be an object.` };
    }
    if (isRecord(value[field]) && Object.keys(value[field]).some((key) => key.trim().length === 0)) {
      return { ok: false, error: `Simulation fixture field ${field} keys must be non-empty strings.` };
    }
  }

  if (isRecord(value.steps)) {
    for (const [stepId, stepFixture] of Object.entries(value.steps)) {
      if (!isRecord(stepFixture)) {
        return { ok: false, error: `Simulation fixture step ${stepId} must be an object.` };
      }
      const stepFields = new Set(["outcome", "outputs", "condition", "choice", "iterations", "input", "recovery"]);
      const unknownStepField = Object.keys(stepFixture).find((field) => !stepFields.has(field));
      if (unknownStepField !== undefined) {
        return { ok: false, error: `Unknown simulation fixture field steps.${stepId}.${unknownStepField}.` };
      }
      if (!validOutcome(stepFixture.outcome)) {
        return { ok: false, error: `Simulation fixture step ${stepId}.outcome must be succeeded, failed, or a non-empty list of those values.` };
      }
      if (!validOutputs(stepFixture.outputs)) {
        return { ok: false, error: `Simulation fixture step ${stepId}.outputs must be a list of non-empty artifact names or an object.` };
      }
      if (!validCondition(stepFixture.condition)) {
        return { ok: false, error: `Simulation fixture step ${stepId}.condition must be a target, boolean, or a non-empty list of targets and booleans.` };
      }
      if (!validChoice(stepFixture.choice)) {
        return { ok: false, error: `Simulation fixture step ${stepId}.choice must be a non-empty string or list of non-empty strings.` };
      }
      if (stepFixture.iterations !== undefined && (!Number.isSafeInteger(stepFixture.iterations) || Number(stepFixture.iterations) < 0)) {
        return { ok: false, error: `Simulation fixture step ${stepId}.iterations must be a non-negative integer.` };
      }
      if (stepFixture.recovery !== undefined && !["remediated", "unresolved"].includes(String(stepFixture.recovery))) {
        return { ok: false, error: `Simulation fixture step ${stepId}.recovery must be remediated or unresolved.` };
      }
    }
  }

  return { ok: true, fixture: value as AgentflowSimulationFixture };
}

export function simulateAgentflowWorkflow(
  workflow: AgentflowWorkflow,
  fixture: AgentflowSimulationFixture
): AgentflowSimulationResult {
  const state: SimulationState = {
    fixture,
    artifacts: new Set(Object.keys(fixture.artifacts ?? {})),
    visitedSteps: [],
    missingArtifacts: [],
    unresolvedBranches: [],
    terminalStates: [],
    missingInputs: requiredWorkflowInputs(workflow).filter((name) => !Object.hasOwn(fixture.inputs ?? {}, name)),
    visits: new Map(),
    retryAttempts: new Map(),
    recoveryCycles: new Map(),
    maxRecoveryCycles: workflowRecoveryLimit(workflow),
    stepLocations: collectSimulationStepLocations(workflow.steps),
    transitionCount: 0
  };

  const workflowStepIdCounts = collectSimulationStepIdCounts(workflow.steps);
  const workflowStepIds = new Set(workflowStepIdCounts.keys());
  const ambiguousStepIds = [...workflowStepIdCounts]
    .filter(([, count]) => count > 1)
    .map(([id]) => id)
    .sort();
  for (const stepId of ambiguousStepIds) {
    addUnresolved(state, stepId, "Workflow step ID is ambiguous in simulation fixtures and targets.");
  }
  for (const stepId of Object.keys(fixture.steps ?? {}).sort()) {
    if (!workflowStepIds.has(stepId)) {
      addUnresolved(state, stepId, "Fixture references an unknown workflow step ID.");
    }
  }

  let control: SequenceControl = ambiguousStepIds.length > 0
    ? { kind: "terminal", status: "unresolved" }
    : runSequence(workflow.steps, state, false);
  while (control.kind === "target") {
    const location = state.stepLocations.get(control.target);
    if (location === undefined) {
      addUnresolved(state, control.target, `Target "${control.target}" does not identify a workflow step.`);
      control = { kind: "terminal", status: "unresolved" };
      break;
    }
    control = runSequence(location.steps, state, location.insideLoop, location.index);
  }
  if (control.kind === "terminal") {
    state.status = control.status;
  }

  const status = state.unresolvedBranches.length > 0 || state.missingArtifacts.length > 0 || state.missingInputs.length > 0
    ? "unresolved"
    : state.status ?? "completed";

  return {
    workflow: { name: workflow.name, version: workflow.version, style: workflow.style },
    status,
    visitedSteps: state.visitedSteps,
    missingInputs: state.missingInputs,
    availableArtifacts: [...state.artifacts].sort(),
    missingArtifacts: state.missingArtifacts,
    unresolvedBranches: state.unresolvedBranches,
    terminalStates: state.terminalStates
  };
}

export function renderAgentflowSimulationSummary(result: AgentflowSimulationResult): string {
  const lines = [
    `Agentflow simulation: ${result.workflow.name} (version ${result.workflow.version})`,
    `Style: ${result.workflow.style}`,
    `Status: ${result.status}`,
    "",
    "Visited steps:"
  ];

  if (result.visitedSteps.length === 0) {
    lines.push("  (none)");
  } else {
    for (const step of result.visitedSteps) {
      lines.push(`  - ${step.id} [${step.type}]: ${step.outcome}`);
    }
  }

  lines.push("", "Available artifacts:");
  lines.push(...(result.availableArtifacts.length > 0
    ? result.availableArtifacts.map((artifact) => `  - ${artifact}`)
    : ["  (none)"]));

  lines.push("", "Missing inputs:");
  lines.push(...(result.missingInputs.length > 0
    ? result.missingInputs.map((input) => `  - ${input}`)
    : ["  (none)"]));

  lines.push("", "Missing artifacts:");
  lines.push(...(result.missingArtifacts.length > 0
    ? result.missingArtifacts.map((entry) => `  - ${entry.stepId}: missing ${entry.kind} artifact ${entry.artifact}`)
    : ["  (none)"]));

  lines.push("", "Unresolved branches:");
  lines.push(...(result.unresolvedBranches.length > 0
    ? result.unresolvedBranches.map((entry) => `  - ${entry.stepId}: ${entry.reason}`)
    : ["  (none)"]));

  lines.push("", "Terminal states:");
  lines.push(...(result.terminalStates.length > 0
    ? result.terminalStates.map((entry) => `  - ${entry.stepId}: ${entry.status}`)
    : ["  (none)"]));

  return lines.join("\n");
}

function runSequence(
  steps: AgentflowWorkflowStep[],
  state: SimulationState,
  insideLoop: boolean,
  startIndex = 0
): SequenceControl {
  const ids = new Map<string, number>();
  steps.forEach((step, index) => {
    const id = nonEmptyString(step.id);
    if (id !== undefined) ids.set(id, index);
  });

  let index = startIndex;
  while (index < steps.length) {
    if (!takeTransition(state, nonEmptyString(steps[index]?.id) ?? "workflow")) {
      return { kind: "terminal", status: "unresolved" };
    }

    let control = runStep(steps[index], state, insideLoop);
    if (control.kind === "done") {
      index += 1;
      continue;
    }
    if (control.kind === "target") {
      control = checkTargetBudget(control, state);
      if (control.kind !== "target") return control;
      const targetIndex = ids.get(control.target);
      if (targetIndex === undefined) return control;
      index = targetIndex;
      continue;
    }
    return control;
  }

  return { kind: "done" };
}

function runStep(step: AgentflowWorkflowStep, state: SimulationState, insideLoop: boolean): SequenceControl {
  const id = nonEmptyString(step.id) ?? "(unnamed)";
  const type = nonEmptyString(step.type) ?? "unknown";
  const visit = state.visits.get(id) ?? 0;
  state.visits.set(id, visit + 1);
  const stepFixture = state.fixture.steps?.[id] ?? {};
  const outcome = pickAt(stepFixture.outcome, visit) ?? "succeeded";

  state.visitedSteps.push({ id, type, outcome: type === "condition" && outcome === "succeeded" ? "selected" : outcome });
  checkInputs(step, id, state);

  if (outcome === "failed") {
    return failureControl(step, stepFixture, id, state);
  }

  state.retryAttempts.delete(id);

  recordOutputs(step, stepFixture, id, state);

  if (type === "condition") return conditionControl(step, stepFixture, id, visit, state);
  if (type === "manual_gate") return gateControl(step, stepFixture, id, visit, state);
  if (type === "input_request") {
    if (stepFixture.input === undefined) {
      state.terminalStates.push({ stepId: id, status: "paused" });
      return { kind: "terminal", status: "paused" };
    }
    const saved = nonEmptyString(step.save_as);
    if (saved !== undefined) state.artifacts.add(saved);
  }
  if (type === "loop") return loopControl(step, stepFixture, id, state);
  if (type === "parallel") return parallelControl(step, state, insideLoop);
  if (type === "result") {
    const resultStatus = nonEmptyString(step.status) ?? "completed";
    state.terminalStates.push({ stepId: id, status: resultStatus });
    if (insideLoop && resultStatus === "continue") return { kind: "break_loop" };
    return { kind: "terminal", status: statusFromTerminal(resultStatus) };
  }

  const target = nonEmptyString(step.then) ?? nonEmptyString(step.goto);
  return target === undefined ? { kind: "done" } : controlForTarget(target, id, state);
}

function conditionControl(
  step: AgentflowWorkflowStep,
  stepFixture: AgentflowSimulationStepFixture,
  id: string,
  visit: number,
  state: SimulationState
): SequenceControl {
  const selection = pickAt(stepFixture.condition, visit);
  let target: string | undefined;

  if (typeof selection === "boolean") {
    target = nonEmptyString(selection ? step.then : step.else);
    if (!selection && target === undefined) return { kind: "done" };
  } else if (typeof selection === "string") {
    target = selection.trim();
  }

  if (target === undefined || target.length === 0) {
    addUnresolved(state, id, "Fixture does not select a condition target.");
    return { kind: "terminal", status: "unresolved" };
  }

  const allowed = conditionTargets(step);
  if (!allowed.has(target)) {
    addUnresolved(state, id, `Fixture condition target "${target}" is not declared by this step.`);
    return { kind: "terminal", status: "unresolved" };
  }

  return controlForTarget(target, id, state);
}

function gateControl(
  step: AgentflowWorkflowStep,
  stepFixture: AgentflowSimulationStepFixture,
  id: string,
  visit: number,
  state: SimulationState
): SequenceControl {
  const choice = pickAt(stepFixture.choice, visit);
  if (choice === undefined) {
    addUnresolved(state, id, "Fixture does not select a manual gate choice.");
    return { kind: "terminal", status: "unresolved" };
  }

  const options = Array.isArray(step.options) ? step.options.flatMap((value) => nonEmptyString(value) ?? []) : [];
  if (!options.includes(choice)) {
    addUnresolved(state, id, `Fixture gate choice "${choice}" is not declared by this step.`);
    return { kind: "terminal", status: "unresolved" };
  }

  const field = choice === "approve" ? "on_approve" : choice === "cancel" ? "on_cancel" : choice === "reject" ? "on_reject" : undefined;
  const target = field === undefined ? undefined : nonEmptyString(step[field]);
  if (target !== undefined) return controlForTarget(target, id, state);
  if (choice === "reject") return controlForTarget("cancel", id, state);
  if (["pause", "cancel", "cancelled", "fail", "failed"].includes(choice)) return controlForTarget(choice, id, state);
  return { kind: "done" };
}

function loopControl(
  step: AgentflowWorkflowStep,
  stepFixture: AgentflowSimulationStepFixture,
  id: string,
  state: SimulationState
): SequenceControl {
  const iterations = stepFixture.iterations;
  if (iterations === undefined) {
    addUnresolved(state, id, "Fixture does not declare a loop iteration count.");
    return { kind: "terminal", status: "unresolved" };
  }

  const maxIterations = typeof step.max_iterations === "number" ? step.max_iterations : iterations;
  if (iterations > maxIterations) {
    addUnresolved(state, id, `Fixture requests ${iterations} loop iterations, exceeding max_iterations ${maxIterations}.`);
    return { kind: "terminal", status: "unresolved" };
  }

  const body = Array.isArray(step.body) ? step.body.filter(isRecord) as AgentflowWorkflowStep[] : [];
  for (let iteration = 0; iteration < iterations; iteration += 1) {
    const control = runSequence(body, state, true);
    if (control.kind === "break_loop") continue;
    if (control.kind !== "done") return control;
  }
  return { kind: "done" };
}

function parallelControl(step: AgentflowWorkflowStep, state: SimulationState, insideLoop: boolean): SequenceControl {
  const initialArtifacts = new Set(state.artifacts);
  const mergedArtifacts = new Set(initialArtifacts);
  let finalControl: SequenceControl = { kind: "done" };

  for (const entries of [step.branches, step.body, step.steps]) {
    if (!Array.isArray(entries)) continue;
    for (const entry of entries) {
      if (!isRecord(entry)) continue;
      state.artifacts = new Set(initialArtifacts);
      const branchId = nonEmptyString(entry.id) ?? "(unnamed)";
      if (!takeTransition(state, branchId)) {
        finalControl = { kind: "terminal", status: "unresolved" };
        break;
      }
      const nestedLists = [entry.body, entry.steps].filter(Array.isArray) as AgentflowYamlValue[][];
      const branchControl = runStep(
        { ...entry, body: undefined, steps: undefined, type: nonEmptyString(entry.type) ?? "parallel_branch" },
        state,
        insideLoop
      );
      let resolvedBranchControl = branchControl;
      while (resolvedBranchControl.kind === "target" && resolvedBranchControl.target === branchId) {
        resolvedBranchControl = checkTargetBudget(resolvedBranchControl, state);
        if (resolvedBranchControl.kind !== "target") break;
        if (!takeTransition(state, branchId)) {
          resolvedBranchControl = { kind: "terminal", status: "unresolved" };
          break;
        }
        resolvedBranchControl = runStep(
          { ...entry, body: undefined, steps: undefined, type: nonEmptyString(entry.type) ?? "parallel_branch" },
          state,
          insideLoop
        );
      }
      let control = resolvedBranchControl;
      for (const nested of control.kind === "done" ? nestedLists : []) {
        control = runSequence(nested.filter(isRecord) as AgentflowWorkflowStep[], state, insideLoop);
        if (control.kind !== "done") break;
      }
      for (const artifact of state.artifacts) mergedArtifacts.add(artifact);
      if (control.kind !== "done" && finalControl.kind === "done") {
        finalControl = control;
      }
    }
  }

  state.artifacts = mergedArtifacts;
  return finalControl;
}

function failureControl(
  step: AgentflowWorkflowStep,
  stepFixture: AgentflowSimulationStepFixture,
  id: string,
  state: SimulationState
): SequenceControl {
  const onFailure = isRecord(step.on_failure) ? step.on_failure : undefined;
  const retries = typeof onFailure?.retry === "number" && Number.isSafeInteger(onFailure.retry) && onFailure.retry > 0
    ? onFailure.retry
    : 0;
  const retryAttempt = state.retryAttempts.get(id) ?? 0;
  if (retryAttempt < retries) {
    state.retryAttempts.set(id, retryAttempt + 1);
    return { kind: "target", target: id };
  }
  state.retryAttempts.delete(id);

  const target = nonEmptyString(onFailure?.then) ?? nonEmptyString(onFailure?.goto);
  if (target !== undefined) return controlForTarget(target, id, state);

  if (onFailure?.route_to !== undefined) {
    if (stepFixture.recovery === undefined) {
      addUnresolved(state, id, "Fixture does not select a routed recovery outcome.");
      return { kind: "terminal", status: "unresolved" };
    }

    const handlerName = stepFixture.recovery === "remediated" ? "on_remediated" : "on_unresolved";
    const handler = isRecord(onFailure?.[handlerName]) ? onFailure[handlerName] : undefined;
    const recoveryTarget = nonEmptyString(handler?.then) ?? nonEmptyString(handler?.return_to);
    if (recoveryTarget !== undefined) return controlForTarget(recoveryTarget, id, state);

    addUnresolved(state, id, `Routed recovery outcome ${stepFixture.recovery} has no declared target.`);
    return { kind: "terminal", status: "unresolved" };
  }

  const unresolved = isRecord(onFailure?.on_unresolved) ? onFailure.on_unresolved : undefined;
  const unresolvedTarget = nonEmptyString(unresolved?.then);
  if (unresolvedTarget !== undefined) return controlForTarget(unresolvedTarget, id, state);

  if (onFailure?.retry !== undefined) {
    state.terminalStates.push({ stepId: id, status: "failed" });
    return { kind: "terminal", status: "failed" };
  }

  state.terminalStates.push({ stepId: id, status: "failed" });
  return { kind: "terminal", status: "failed" };
}

function checkInputs(step: AgentflowWorkflowStep, stepId: string, state: SimulationState): void {
  const values: string[] = [];
  if (Array.isArray(step.inputs)) values.push(...step.inputs.flatMap((value) => artifactName(value, state)));
  if (isRecord(step.inputs)) values.push(...nestedArtifactNames(step.inputs, state));
  if (Array.isArray(step.artifacts)) values.push(...step.artifacts.flatMap((value) => artifactName(value, state)));
  if (step.type === "artifact_transform") values.push(...artifactName(step.input, state));

  for (const artifact of values) {
    if (!state.artifacts.has(artifact)) addMissingArtifact(state, { stepId, artifact, kind: "input" });
  }
}

function recordOutputs(
  step: AgentflowWorkflowStep,
  fixture: AgentflowSimulationStepFixture,
  stepId: string,
  state: SimulationState
): void {
  const declared = new Set<string>();
  if (Array.isArray(step.outputs)) {
    for (const value of step.outputs) {
      const name = nonEmptyString(value);
      if (name !== undefined) declared.add(name);
    }
  }
  const singleOutput = nonEmptyString(step.output);
  if (singleOutput !== undefined) declared.add(singleOutput);

  const provided = new Set(Array.isArray(fixture.outputs) ? fixture.outputs : Object.keys(fixture.outputs ?? {}));
  for (const artifact of declared) {
    if (provided.has(artifact)) state.artifacts.add(artifact);
    else addMissingArtifact(state, { stepId, artifact, kind: "output" });
  }
  for (const artifact of provided) {
    if (!declared.has(artifact)) {
      addUnresolved(state, stepId, `Fixture provides undeclared output artifact ${artifact}.`);
    }
  }
}

function conditionTargets(step: AgentflowWorkflowStep): Set<string> {
  const targets = new Set<string>();
  for (const value of [step.then, step.else]) {
    const target = nonEmptyString(value);
    if (target !== undefined) targets.add(target);
  }
  if (Array.isArray(step.branches)) {
    for (const branch of step.branches) {
      if (!isRecord(branch)) continue;
      const target = nonEmptyString(branch.then);
      if (target !== undefined) targets.add(target);
    }
  }
  return targets;
}

function controlForTarget(target: string, stepId: string, state: SimulationState): SequenceControl {
  if (state.stepLocations.has(target)) return { kind: "target", target };
  if (target === "continue" || target === "ignore") return { kind: "done" };
  if (!TERMINAL_TARGETS.has(target)) return { kind: "target", target };
  const status = statusFromTerminal(target);
  state.terminalStates.push({ stepId, status });
  return { kind: "terminal", status };
}

function statusFromTerminal(status: string): AgentflowSimulationStatus {
  if (["fail", "failed"].includes(status)) return "failed";
  if (["pause", "paused", "unresolved"].includes(status)) return status === "unresolved" ? "unresolved" : "paused";
  if (["cancel", "cancelled"].includes(status)) return "cancelled";
  return "completed";
}

function artifactName(value: AgentflowYamlValue | undefined, state: SimulationState): string[] {
  const name = nonEmptyString(value);
  if (name === undefined) return [];
  const inputReference = /^\{\{\s*inputs\.([A-Za-z0-9_-]+)\s*}}$/.exec(name);
  if (inputReference !== null) {
    const resolved = state.fixture.inputs?.[inputReference[1]];
    return typeof resolved === "string" ? artifactName(resolved, state) : [];
  }
  if (name.includes("{{")) return [];
  return [name];
}

function nestedArtifactNames(value: AgentflowYamlValue | undefined, state: SimulationState): string[] {
  if (value === undefined) return [];
  if (Array.isArray(value)) return value.flatMap((entry) => nestedArtifactNames(entry, state));
  if (isRecord(value)) return Object.values(value).flatMap((entry) => nestedArtifactNames(entry, state));
  return artifactName(value, state);
}

function checkTargetBudget(control: Extract<SequenceControl, { kind: "target" }>, state: SimulationState): SequenceControl {
  if (control.budgetChecked || state.maxRecoveryCycles === undefined || (state.visits.get(control.target) ?? 0) === 0) {
    return { ...control, budgetChecked: true };
  }

  const cycles = (state.recoveryCycles.get(control.target) ?? 0) + 1;
  state.recoveryCycles.set(control.target, cycles);
  if (cycles <= state.maxRecoveryCycles) return { ...control, budgetChecked: true };

  addUnresolved(state, control.target, `Simulation exceeded limits.max_recovery_cycles ${state.maxRecoveryCycles}.`);
  return { kind: "terminal", status: "unresolved" };
}

function workflowRecoveryLimit(workflow: AgentflowWorkflow): number | undefined {
  const limits = isRecord(workflow.limits) ? workflow.limits : undefined;
  return typeof limits?.max_recovery_cycles === "number" && Number.isSafeInteger(limits.max_recovery_cycles) && limits.max_recovery_cycles > 0
    ? limits.max_recovery_cycles
    : undefined;
}

function addMissingArtifact(state: SimulationState, entry: AgentflowSimulationMissingArtifact): void {
  if (!state.missingArtifacts.some((candidate) => candidate.stepId === entry.stepId && candidate.artifact === entry.artifact && candidate.kind === entry.kind)) {
    state.missingArtifacts.push(entry);
  }
}

function addUnresolved(state: SimulationState, stepId: string, reason: string): void {
  state.unresolvedBranches.push({ stepId, reason });
}

function takeTransition(state: SimulationState, stepId: string): boolean {
  state.transitionCount += 1;
  if (state.transitionCount <= MAX_SIMULATION_TRANSITIONS) return true;
  addUnresolved(state, stepId, "Simulation exceeded its deterministic transition limit.");
  return false;
}

function pickAt<T>(value: T | T[] | undefined, index: number): T | undefined {
  if (!Array.isArray(value)) return value;
  return value[Math.min(index, value.length - 1)];
}

function nonEmptyString(value: AgentflowYamlValue | undefined): string | undefined {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : undefined;
}

function isRecord(value: unknown): value is AgentflowYamlMapping {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function requiredWorkflowInputs(workflow: AgentflowWorkflow): string[] {
  return Object.entries(workflow.inputs ?? {})
    .filter(([, definition]) => isRecord(definition) && definition.required === true)
    .map(([name]) => name)
    .sort();
}

function validOutcome(value: AgentflowYamlValue | undefined): boolean {
  if (value === undefined) return true;
  const valid = (entry: unknown) => entry === "succeeded" || entry === "failed";
  return Array.isArray(value) ? value.length > 0 && value.every(valid) : valid(value);
}

function validOutputs(value: AgentflowYamlValue | undefined): boolean {
  if (value === undefined) return true;
  return (isRecord(value) && Object.keys(value).every((key) => key.trim().length > 0))
    || (Array.isArray(value) && value.every((entry) => nonEmptyString(entry) !== undefined));
}

function validCondition(value: AgentflowYamlValue | undefined): boolean {
  if (value === undefined) return true;
  const valid = (entry: unknown) => typeof entry === "boolean" || (typeof entry === "string" && entry.trim().length > 0);
  return Array.isArray(value) ? value.length > 0 && value.every(valid) : valid(value);
}

function validChoice(value: AgentflowYamlValue | undefined): boolean {
  if (value === undefined) return true;
  return Array.isArray(value)
    ? value.length > 0 && value.every((entry) => nonEmptyString(entry) !== undefined)
    : nonEmptyString(value) !== undefined;
}

function collectSimulationStepIdCounts(
  steps: AgentflowWorkflowStep[],
  counts = new Map<string, number>()
): Map<string, number> {
  for (const step of steps) {
    const id = nonEmptyString(step.id);
    if (id !== undefined) counts.set(id, (counts.get(id) ?? 0) + 1);

    for (const field of ["body", "steps"] as const) {
      const nested = step[field];
      if (Array.isArray(nested)) {
        collectSimulationStepIdCounts(nested.filter(isRecord) as AgentflowWorkflowStep[], counts);
      }
    }

    if (step.type === "parallel" && Array.isArray(step.branches)) {
      collectSimulationStepIdCounts(step.branches.filter(isRecord) as AgentflowWorkflowStep[], counts);
    }
  }
  return counts;
}

function collectSimulationStepLocations(
  steps: AgentflowWorkflowStep[],
  insideLoop = false,
  locations = new Map<string, SimulationStepLocation>()
): Map<string, SimulationStepLocation> {
  steps.forEach((step, index) => {
    const id = nonEmptyString(step.id);
    if (id !== undefined) locations.set(id, { steps, index, insideLoop });

    const nestedInsideLoop = insideLoop || step.type === "loop";
    for (const field of ["body", "steps"] as const) {
      const nested = step[field];
      if (Array.isArray(nested)) {
        collectSimulationStepLocations(
          nested.filter(isRecord) as AgentflowWorkflowStep[],
          nestedInsideLoop,
          locations
        );
      }
    }

    if (step.type === "parallel" && Array.isArray(step.branches)) {
      const branches = step.branches.filter(isRecord) as AgentflowWorkflowStep[];
      collectSimulationStepLocations(branches, insideLoop, locations);
    }
  });

  return locations;
}
