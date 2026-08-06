import { isDeepStrictEqual } from "node:util";
import type {
  AgentFlowWorkflow,
  AgentFlowWorkflowStep,
  AgentFlowYamlMapping,
  AgentFlowYamlValue
} from "./workflow";
import {
  AgentFlowArtifactTransformError,
  type AgentFlowArtifactTransformRegistry,
  createAgentFlowArtifactTransformRegistry,
  transformAgentFlowFixtureArtifact
} from "./artifact_transform";
import { normalizeAgentFlowArtifactPath, type AgentFlowRunStateValue } from "./run_state";
import { resolveAgentFlowMcpArguments } from "./mcp_call";
import {
  AgentFlowConditionError,
  agentFlowConditionArtifactAlias,
  evaluateAgentFlowConditionWithResolver,
  resolveAgentFlowConditionReferenceFromValues,
  selectAgentFlowConditionTargetFromValues
} from "./condition";
import { AgentFlowFailureClassificationError } from "./failure_classification";
import { parseAgentFlowReviewResult } from "./review";
import { defaultAgentFlowApprovalOutputPath, parseAgentFlowApprovalResult } from "./approval";
import { defaultAgentFlowDecisionRecordPath, resolveAgentFlowDecisionRecordContract } from "./decision_record";
import { parseAgentFlowChallengeResult, parseAgentFlowConsultResult } from "./collaboration";
import {
  agentFlowAmbiguousSuccessTargetMessage,
  collectAgentFlowAmbiguousSuccessTargets
} from "./success_routing";

export type AgentFlowSimulationStatus = "completed" | "failed" | "paused" | "cancelled" | "timed_out" | "unresolved";
export type AgentFlowSimulationStepOutcome = "succeeded" | "failed";
export type AgentFlowSimulationVisitedOutcome = AgentFlowSimulationStepOutcome | "selected";

export interface AgentFlowSimulationStepFixture {
  outcome?: AgentFlowSimulationStepOutcome | AgentFlowSimulationStepOutcome[];
  outputs?: string[] | Record<string, AgentFlowYamlValue>;
  choice?: string | string[];
  iterations?: number;
  loop_termination?: "condition_met" | "max_iterations" | "max_duration";
  input?: AgentFlowYamlValue;
  recovery?: "remediated" | "unresolved";
}

export interface AgentFlowSimulationFixture {
  inputs?: Record<string, AgentFlowYamlValue>;
  artifacts?: Record<string, AgentFlowYamlValue>;
  steps?: Record<string, AgentFlowSimulationStepFixture>;
}

export interface AgentFlowSimulationVisitedStep {
  id: string;
  type: string;
  outcome: AgentFlowSimulationVisitedOutcome;
}

export interface AgentFlowSimulationMissingArtifact {
  stepId: string;
  artifact: string;
  kind: "input" | "output";
}

export interface AgentFlowSimulationUnresolvedBranch {
  stepId: string;
  reason: string;
}

export interface AgentFlowSimulationTerminalState {
  stepId: string;
  status: string;
}

export interface AgentFlowSimulationResult {
  workflow: {
    name: string;
    version: number;
    style: AgentFlowWorkflow["style"];
  };
  status: AgentFlowSimulationStatus;
  visitedSteps: AgentFlowSimulationVisitedStep[];
  missingInputs: string[];
  availableArtifacts: string[];
  artifactValues: Record<string, AgentFlowYamlValue>;
  missingArtifacts: AgentFlowSimulationMissingArtifact[];
  unresolvedBranches: AgentFlowSimulationUnresolvedBranch[];
  terminalStates: AgentFlowSimulationTerminalState[];
}

export type AgentFlowSimulationFixtureParseResult =
  | { ok: true; fixture: AgentFlowSimulationFixture }
  | { ok: false; error: string };

interface SimulationState {
  workflow: AgentFlowWorkflow;
  workflowStyle: AgentFlowWorkflow["style"];
  fixture: AgentFlowSimulationFixture;
  artifacts: Set<string>;
  artifactValues: Map<string, AgentFlowYamlValue>;
  producedArtifacts: Map<string, number>;
  artifactProducers: Map<string, string>;
  transforms: AgentFlowArtifactTransformRegistry;
  visitedSteps: AgentFlowSimulationVisitedStep[];
  missingArtifacts: AgentFlowSimulationMissingArtifact[];
  handledMissingArtifacts: Set<string>;
  unresolvedBranches: AgentFlowSimulationUnresolvedBranch[];
  terminalStates: AgentFlowSimulationTerminalState[];
  missingInputs: string[];
  visits: Map<string, number>;
  retryAttempts: Map<string, number>;
  immediateRetries: Set<string>;
  failureAttempts: Map<string, number>;
  modelBudgetUsage: Map<string, number>;
  recoveryCycles: Map<string, number>;
  maxRecoveryCycles?: number;
  stepAttemptLimits: Map<string, number>;
  stepLocations: Map<string, SimulationStepLocation>;
  transitionCount: number;
  status?: AgentFlowSimulationStatus;
}

interface SimulationStepLocation {
  steps: AgentFlowWorkflowStep[];
  index: number;
  insideLoop: boolean;
}

type SequenceControl =
  | { kind: "done" }
  | { kind: "target"; target: string; budgetChecked?: boolean }
  | { kind: "break_loop" }
  | { kind: "terminal"; status: AgentFlowSimulationStatus };

const TERMINAL_TARGETS = new Set([
  "cancel", "cancelled", "complete", "completed", "fail", "failed", "pause", "paused", "unresolved"
]);
const MAX_SIMULATION_TRANSITIONS = 10_000;

export function parseAgentFlowSimulationFixture(source: string): AgentFlowSimulationFixtureParseResult {
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
      const stepFields = new Set(["outcome", "outputs", "choice", "iterations", "loop_termination", "input", "recovery"]);
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
      if (!validChoice(stepFixture.choice)) {
        return { ok: false, error: `Simulation fixture step ${stepId}.choice must be a non-empty string or list of non-empty strings.` };
      }
      if (stepFixture.iterations !== undefined && (!Number.isSafeInteger(stepFixture.iterations) || Number(stepFixture.iterations) < 0)) {
        return { ok: false, error: `Simulation fixture step ${stepId}.iterations must be a non-negative integer.` };
      }
      if (stepFixture.loop_termination !== undefined &&
          !["condition_met", "max_iterations", "max_duration"].includes(String(stepFixture.loop_termination))) {
        return { ok: false, error: `Simulation fixture step ${stepId}.loop_termination must be condition_met, max_iterations, or max_duration.` };
      }
      if (stepFixture.recovery !== undefined && !["remediated", "unresolved"].includes(String(stepFixture.recovery))) {
        return { ok: false, error: `Simulation fixture step ${stepId}.recovery must be remediated or unresolved.` };
      }
    }
  }

  return { ok: true, fixture: value as AgentFlowSimulationFixture };
}

export function simulateAgentFlowWorkflow(
  workflow: AgentFlowWorkflow,
  fixture: AgentFlowSimulationFixture,
  transforms: AgentFlowArtifactTransformRegistry = createAgentFlowArtifactTransformRegistry()
): AgentFlowSimulationResult {
  const fixtureArtifacts = canonicalFixtureArtifacts(fixture.artifacts ?? {});
  const state: SimulationState = {
    workflow,
    workflowStyle: workflow.style,
    fixture,
    artifacts: new Set([...fixtureArtifacts.values.keys(), ...fixtureArtifacts.collisions]),
    artifactValues: fixtureArtifacts.values,
    producedArtifacts: new Map(),
    artifactProducers: new Map(),
    transforms,
    visitedSteps: [],
    missingArtifacts: [],
    handledMissingArtifacts: new Set(),
    unresolvedBranches: [],
    terminalStates: [],
    missingInputs: requiredWorkflowInputs(workflow).filter((name) => !Object.hasOwn(fixture.inputs ?? {}, name)),
    visits: new Map(),
    retryAttempts: new Map(),
    immediateRetries: new Set(),
    failureAttempts: new Map(),
    modelBudgetUsage: new Map(),
    recoveryCycles: new Map(),
    maxRecoveryCycles: workflowRecoveryLimit(workflow),
    stepAttemptLimits: workflowStepAttemptLimits(workflow),
    stepLocations: collectSimulationStepLocations(workflow.steps),
    transitionCount: 0
  };
  for (const artifact of fixtureArtifacts.collisions) {
    addUnresolved(state, "(fixture)", `Fixture artifact keys collide at canonical path ${artifact}.`);
  }

  const workflowStepIdCounts = collectSimulationStepIdCounts(workflow.steps);
  const workflowStepIds = new Set(workflowStepIdCounts.keys());
  const ambiguousSuccessTargets = collectAgentFlowAmbiguousSuccessTargets(workflow.steps);
  const ambiguousStepIds = [...workflowStepIdCounts]
    .filter(([, count]) => count > 1)
    .map(([id]) => id)
    .sort();
  for (const stepId of ambiguousStepIds) {
    addUnresolved(state, stepId, "Workflow step ID is ambiguous in simulation fixtures and targets.");
  }
  for (const conflict of ambiguousSuccessTargets) {
    addUnresolved(
      state,
      conflict.stepId ?? "(workflow)",
      agentFlowAmbiguousSuccessTargetMessage(conflict.stepId)
    );
  }
  for (const stepId of Object.keys(fixture.steps ?? {}).sort()) {
    if (!workflowStepIds.has(stepId)) {
      addUnresolved(state, stepId, "Fixture references an unknown workflow step ID.");
    }
  }

  let control: SequenceControl = ambiguousStepIds.length > 0 || ambiguousSuccessTargets.length > 0
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

  const hasUnhandledMissingArtifacts = state.missingArtifacts.some((artifact) =>
    !state.handledMissingArtifacts.has(missingArtifactKey(artifact))
  );
  const status = state.unresolvedBranches.length > 0 || hasUnhandledMissingArtifacts || state.missingInputs.length > 0
    ? "unresolved"
    : state.status ?? "completed";

  return {
    workflow: { name: workflow.name, version: workflow.version, style: workflow.style },
    status,
    visitedSteps: state.visitedSteps,
    missingInputs: state.missingInputs,
    availableArtifacts: [...state.artifacts].sort(),
    artifactValues: Object.fromEntries([...state.artifactValues].sort(([left], [right]) => left.localeCompare(right))),
    missingArtifacts: state.missingArtifacts,
    unresolvedBranches: state.unresolvedBranches,
    terminalStates: state.terminalStates
  };
}

export function renderAgentFlowSimulationSummary(result: AgentFlowSimulationResult): string {
  const lines = [
    `Agent Flow simulation: ${result.workflow.name} (version ${result.workflow.version})`,
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
  steps: AgentFlowWorkflowStep[],
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
      const fallthroughTarget = nonEmptyString(steps[index + 1]?.id);
      if (fallthroughTarget !== undefined) {
        control = checkTargetBudget({ kind: "target", target: fallthroughTarget }, state);
        if (control.kind !== "target") return control;
      }
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

function runStep(step: AgentFlowWorkflowStep, state: SimulationState, insideLoop: boolean): SequenceControl {
  const id = nonEmptyString(step.id) ?? "(unnamed)";
  const type = nonEmptyString(step.type) ?? "unknown";
  const immediateRetry = state.immediateRetries.delete(id);
  if (!immediateRetry) {
    const guardControl = simulationRecoveryGuard(step, id, state);
    if (guardControl !== undefined) return guardControl;
  }
  const visit = state.visits.get(id) ?? 0;
  const attemptLimit = state.stepAttemptLimits.get(id);
  if (attemptLimit !== undefined && visit + 1 > attemptLimit) {
    const status = simulationRecoveryLimitStatus(state);
    state.terminalStates.push({ stepId: id, status });
    return { kind: "terminal", status };
  }
  state.visits.set(id, visit + 1);
  const stepFixture = state.fixture.steps?.[id] ?? {};
  const outcome = pickAt(stepFixture.outcome, visit) ?? "succeeded";

  state.visitedSteps.push({ id, type, outcome: type === "condition" && outcome === "succeeded" ? "selected" : outcome });
  checkInputs(step, id, state);

  if (type === "parallel_branch" && nonEmptyString(step.session) !== undefined) {
    const budgetControl = simulationModelBudgetControl(step, id, state);
    if (budgetControl !== undefined) {
      state.visitedSteps.at(-1)!.outcome = "failed";
      return budgetControl;
    }
  }

  if (outcome === "failed") {
    state.failureAttempts.set(id, Math.max(state.failureAttempts.get(id) ?? 0, visit + 1));
    if (type === "artifact_transform") {
      return failureControl(step, stepFixture, id, state);
    }
    if (["challenge", "consult", "review", "session_request"].includes(type)
        || (type === "approval" && nonEmptyString(step.reviewer) !== "human")) {
      return simulateSessionRequestStep(step, stepFixture, id, state, true);
    }
    if (type === "mcp_call") {
      return simulatedSessionFailure(step, stepFixture, id, state, "Fixture marks the MCP call as failed.");
    }
    return failureControl(step, stepFixture, id, state);
  }

  if (type === "artifact_transform") {
    const transformControl = simulateTransformStep(step, stepFixture, id, state);
    if (transformControl.kind !== "done") return transformControl;
    state.retryAttempts.delete(id);
  } else if (["challenge", "consult", "review", "session_request"].includes(type)
      || (type === "approval" && nonEmptyString(step.reviewer) !== "human")) {
    const sessionControl = simulateSessionRequestStep(step, stepFixture, id, state, false);
    if (sessionControl.kind !== "done") return sessionControl;
    state.retryAttempts.delete(id);
  } else if (type === "mcp_call") {
    const mcpControl = simulateMcpCallStep(step, stepFixture, id, state);
    if (mcpControl.kind !== "done") return mcpControl;
    state.retryAttempts.delete(id);
  } else if (type === "command") {
    const fixtureOutputs = canonicalFixtureOutputValues(stepFixture);
    const collision = declaredOutputArtifacts(step).find((artifact) =>
      state.artifacts.has(artifact)
      && state.artifactProducers.get(artifact) !== id
      && !sameCommandOutputArtifact(state, artifact, fixtureOutputs.get(artifact))
      && step.overwrite !== true
    );
    if (collision !== undefined) {
      const visitRecord = state.visitedSteps.at(-1);
      if (visitRecord?.id === id && visitRecord.outcome === "succeeded") visitRecord.outcome = "failed";
      addUnresolved(state, id, `Artifact ${collision} already exists; declare overwrite: true to replace it during simulation.`);
      return { kind: "terminal", status: "unresolved" };
    }
    state.retryAttempts.delete(id);
    recordOutputs(step, stepFixture, id, state);
  } else if (type !== "condition" && type !== "decision_record"
      && !(type === "approval" && nonEmptyString(step.reviewer) === "human")) {
    state.retryAttempts.delete(id);
    recordOutputs(step, stepFixture, id, state);
  }

  if (type === "condition") return conditionControl(step, id, state);
  if (type === "approval") {
    if (nonEmptyString(step.reviewer) === "human") {
      if (hasMissingDeclaredArtifacts(step, state)) {
        state.visitedSteps.at(-1)!.outcome = "failed";
        return { kind: "terminal", status: "unresolved" };
      }
      const outcome = typeof stepFixture.input === "string" ? stepFixture.input.trim() : undefined;
      if (outcome === undefined) {
        state.terminalStates.push({ stepId: id, status: "paused" });
        return { kind: "terminal", status: "paused" };
      }
      if (outcome !== "approve" && outcome !== "reject" && outcome !== "cancel") {
        addUnresolved(state, id, "Approval fixture input must be one of: approve, reject, cancel.");
        return { kind: "terminal", status: "unresolved" };
      }
      const target = outcome === "approve" ? staticTarget(step.on_approve)
        : outcome === "reject" ? staticTarget(step.on_reject) ?? "cancel"
          : staticTarget(step.on_cancel) ?? "cancel";
      if (outcome === "approve" || outcome === "reject") {
        const output = canonicalArtifactName(nonEmptyString(step.output) ?? defaultAgentFlowApprovalOutputPath(id));
        if (state.artifacts.has(output) && state.artifactProducers.get(output) !== id && step.overwrite !== true) {
          const visitRecord = state.visitedSteps.at(-1);
          if (visitRecord?.id === id && visitRecord.outcome === "succeeded") visitRecord.outcome = "failed";
          addUnresolved(state, id, `Artifact ${output} already exists; declare overwrite: true to replace it during simulation.`);
          return { kind: "terminal", status: "unresolved" };
        }
        state.artifacts.add(output);
        state.artifactValues.set(output, {
          status: outcome === "approve" ? "approved" : "rejected",
          decision: outcome
        });
        markArtifactProduced(state, output, id);
      }
      return target === undefined ? { kind: "done" } : controlForTarget(target, id, state);
    }
    const output = canonicalArtifactName(nonEmptyString(step.output) ?? defaultAgentFlowApprovalOutputPath(id));
    const value = state.artifactValues.get(output);
    const approval = parseAgentFlowApprovalResult(typeof value === "string" ? value : `${JSON.stringify(value)}\n`, output);
    const target = approval.status === "approved" ? staticTarget(step.on_approve)
      : staticTarget(step.on_reject) ?? "cancel";
    if (target !== undefined) return controlForTarget(target, id, state);
  }
  if (type === "manual_gate") return gateControl(step, stepFixture, id, visit, state);
  if (type === "input_request") {
    if (stepFixture.input === undefined) {
      state.terminalStates.push({ stepId: id, status: "paused" });
      return { kind: "terminal", status: "paused" };
    }
    const saved = nonEmptyString(step.save_as);
    if (saved !== undefined) {
      const artifact = canonicalArtifactName(saved);
      state.artifacts.add(artifact);
      state.artifactValues.set(artifact, stepFixture.input);
      markArtifactProduced(state, artifact, id);
    }
  }
  if (type === "decision_record") {
    let contract: ReturnType<typeof resolveAgentFlowDecisionRecordContract>;
    try {
      contract = resolveAgentFlowDecisionRecordContract(state.workflow, step);
    } catch {
      state.visitedSteps.at(-1)!.outcome = "failed";
      state.terminalStates.push({ stepId: id, status: "failed" });
      return { kind: "terminal", status: "failed" };
    }
    if (hasMissingDeclaredArtifacts(step, state)) {
      state.visitedSteps.at(-1)!.outcome = "failed";
      return { kind: "terminal", status: "unresolved" };
    }
    const output = canonicalArtifactName(contract.output);
    if (state.artifacts.has(output) && state.artifactProducers.get(output) !== id && step.overwrite !== true) {
      const visitRecord = state.visitedSteps.at(-1);
      if (visitRecord?.id === id && visitRecord.outcome === "succeeded") visitRecord.outcome = "failed";
      addUnresolved(state, id, `Artifact ${output} already exists; declare overwrite: true to replace it during simulation.`);
      return { kind: "terminal", status: "unresolved" };
    }
    state.artifacts.add(output);
    state.artifactValues.set(output, {
      decision_id: contract.decision_id,
      owner: contract.owner,
      topic: contract.topic,
      rationale_summary: contract.rationale_summary,
      consulted: contract.consulted,
      approved_by: contract.approved_by,
      artifacts: contract.artifacts,
      created_at: "1970-01-01T00:00:00.000Z"
    });
    markArtifactProduced(state, output, id);
  }
  if (type === "loop") return loopControl(step, stepFixture, id, state);
  if (type === "parallel") return parallelControl(step, state, insideLoop);
  if (type === "result") {
    const resultStatus = nonEmptyString(step.status) ?? "completed";
    state.terminalStates.push({ stepId: id, status: resultStatus });
    if (insideLoop && resultStatus === "continue") return { kind: "break_loop" };
    return { kind: "terminal", status: statusFromTerminal(resultStatus) };
  }

  const target = staticTarget(step.then) ?? staticTarget(step.goto);
  return target === undefined ? { kind: "done" } : controlForTarget(target, id, state);
}

function simulationRecoveryGuard(
  step: AgentFlowWorkflowStep,
  stepId: string,
  state: SimulationState
): SequenceControl | undefined {
  if (state.workflowStyle !== "recovery_pipeline") return undefined;
  const expressions = [state.workflow.short_circuit_if, step.short_circuit_if]
    .flatMap((value) => Array.isArray(value) ? value : [])
    .filter((value): value is string => typeof value === "string");
  for (const expression of expressions) {
    let matched: boolean;
    try {
      matched = evaluateAgentFlowConditionWithResolver(expression, (scope, segments) => {
        if (scope === "artifacts" && segments[0] === "budget") {
          return simulationBudgetReference(state, segments.slice(1));
        }
        if (scope === "artifacts" && segments[0] === "failures") {
          return segments.length >= 3 && segments.at(-1) === "attempts"
            ? state.failureAttempts.get(segments.slice(1, -1).join("."))
            : undefined;
        }
        assertSimulationArtifactValueAvailable(state, scope, segments);
        return resolveAgentFlowConditionReferenceFromValues(
          state.fixture.inputs ?? {}, state.artifactValues, scope, segments
        );
      });
    } catch (error) {
      if (error instanceof AgentFlowConditionError &&
          (error.message.includes("does not match a published JSON artifact") ||
           error.message.includes("did not resolve to a value"))) {
        continue;
      }
      state.terminalStates.push({ stepId, status: "paused" });
      return { kind: "terminal", status: "paused" };
    }
    if (matched) {
      state.terminalStates.push({ stepId, status: "paused" });
      return { kind: "terminal", status: "paused" };
    }
  }
  return undefined;
}

function simulationModelBudgetControl(
  step: AgentFlowWorkflowStep,
  stepId: string,
  state: SimulationState
): SequenceControl | undefined {
  const actor = nonEmptyString(step.session)
    ?? (step.type === "review" || step.type === "approval" ? nonEmptyString(step.reviewer) : undefined)
    ?? (step.type === "consult" || step.type === "challenge" ? nonEmptyString(step.to) : undefined);
  return simulationSessionBudgetControl(actor, stepId, state);
}

function simulationSessionBudgetControl(
  sessionId: string | undefined,
  stepId: string,
  state: SimulationState
): SequenceControl | undefined {
  const session = sessionId === undefined || !isRecord(state.workflow.sessions?.[sessionId])
    ? undefined
    : state.workflow.sessions[sessionId];
  const provider = isRecord(session) ? nonEmptyString(session.provider) : undefined;
  const kinds = ["model_calls", ...(provider === "frontier" ? ["frontier_calls"] : [])];
  const limits = isRecord(state.workflow.limits) ? state.workflow.limits : undefined;
  for (const kind of kinds) {
    const limit = limits?.[`max_${kind}`];
    if (limit === undefined && kind !== "frontier_calls") continue;
    if (typeof limit !== "number" || (state.modelBudgetUsage.get(kind) ?? 0) + 1 > limit) {
      const status = simulationRecoveryLimitStatus(state);
      state.terminalStates.push({ stepId, status });
      return { kind: "terminal", status };
    }
  }
  for (const kind of kinds) {
    const limit = limits?.[`max_${kind}`];
    if (typeof limit === "number") state.modelBudgetUsage.set(kind, (state.modelBudgetUsage.get(kind) ?? 0) + 1);
  }
  return undefined;
}

function simulationBudgetReference(state: SimulationState, segments: string[]): AgentFlowYamlValue | undefined {
  if (segments.length !== 1 || !segments[0]!.endsWith("_remaining")) return undefined;
  const kind = segments[0]!.slice(0, -"_remaining".length);
  const limits = isRecord(state.workflow.limits) ? state.workflow.limits : undefined;
  const limit = limits?.[`max_${kind}`];
  return typeof limit === "number" ? Math.max(0, limit - (state.modelBudgetUsage.get(kind) ?? 0)) : undefined;
}

function canonicalFixtureOutputValues(
  fixture: AgentFlowSimulationStepFixture
): Map<string, AgentFlowYamlValue | undefined> {
  if (Array.isArray(fixture.outputs)) {
    return new Map(fixture.outputs.map((artifact) => [canonicalArtifactName(artifact), undefined]));
  }
  return new Map(canonicalFixtureArtifacts(fixture.outputs ?? {}).values);
}

function sameCommandOutputArtifact(
  state: SimulationState,
  artifact: string,
  proposed: AgentFlowYamlValue | undefined
): boolean {
  const producerId = state.artifactProducers.get(artifact);
  const location = producerId === undefined ? undefined : state.stepLocations.get(producerId);
  const producer = location === undefined ? undefined : location.steps[location.index];
  const existing = state.artifactValues.get(artifact);
  return nonEmptyString(producer?.type) === "command"
    && existing !== undefined
    && proposed !== undefined
    && isDeepEqualArtifactValue(existing, proposed);
}

function declaredOutputArtifacts(step: AgentFlowWorkflowStep): string[] {
  const outputs = [...(Array.isArray(step.outputs) ? step.outputs : []), step.output];
  const declared = outputs.flatMap((value) => {
    const name = nonEmptyString(value);
    return name === undefined ? [] : [canonicalArtifactName(name)];
  });
  const id = nonEmptyString(step.id);
  if (declared.length === 0 && id !== undefined && step.type === "approval") declared.push(defaultAgentFlowApprovalOutputPath(id));
  if (declared.length === 0 && id !== undefined && step.type === "decision_record") declared.push(defaultAgentFlowDecisionRecordPath(id));
  return declared;
}

function staticTarget(value: AgentFlowYamlValue | undefined): string | undefined {
  const target = nonEmptyString(value);
  return target === undefined || target.includes("{{") || target.includes("}}") ? undefined : target;
}

function simulateMcpCallStep(
  step: AgentFlowWorkflowStep,
  fixture: AgentFlowSimulationStepFixture,
  stepId: string,
  state: SimulationState
): SequenceControl {
  try {
    resolveAgentFlowMcpArguments(
      step.arguments,
      (state.fixture.inputs ?? {}) as Record<string, AgentFlowRunStateValue>,
      stepId
    );
  } catch (error) {
    return simulatedMcpContractFailure(
      step,
      fixture,
      stepId,
      state,
      error instanceof Error ? error.message : String(error)
    );
  }
  const declaredOutputs = new Set(
    (Array.isArray(step.outputs) ? step.outputs : [])
      .flatMap((output) => nonEmptyString(output) ?? [])
      .map(canonicalArtifactName)
  );
  const providedOutputs = Array.isArray(fixture.outputs)
    ? exactFixtureArtifactNames(fixture.outputs)
    : exactFixtureArtifacts(fixture.outputs ?? {});
  const invalidOutput = providedOutputs.collisions.values().next().value
    ?? [...declaredOutputs].find((output) => !providedOutputs.values.has(output))
    ?? [...providedOutputs.values.keys()].find((output) => !declaredOutputs.has(output));
  if (invalidOutput !== undefined) {
    return simulatedMcpContractFailure(
      step,
      fixture,
      stepId,
      state,
      `MCP fixture outputs must match declared outputs exactly; invalid output ${invalidOutput}.`
    );
  }
  for (const output of declaredOutputs) {
    if (state.artifacts.has(output) && state.artifactProducers.get(output) !== stepId && step.overwrite !== true) {
      return simulatedMcpContractFailure(
        step,
        fixture,
        stepId,
        state,
        `Artifact ${output} already exists; declare overwrite: true to replace it during simulation.`
      );
    }
  }
  recordOutputs(step, fixture, stepId, state);
  return { kind: "done" };
}

function simulatedMcpContractFailure(
  step: AgentFlowWorkflowStep,
  fixture: AgentFlowSimulationStepFixture,
  stepId: string,
  state: SimulationState,
  _message: string
): SequenceControl {
  const visit = state.visitedSteps.at(-1);
  if (visit?.id === stepId && visit.outcome === "succeeded") visit.outcome = "failed";
  recordSimulationFailure(state, stepId);
  if (!isRecord(step.on_failure)) {
    state.terminalStates.push({ stepId, status: "paused" });
    return { kind: "terminal", status: "paused" };
  }
  const control = failureControl(step, fixture, stepId, state, false);
  const hasExplicitTarget = nonEmptyString(step.on_failure.then) !== undefined
    || nonEmptyString(step.on_failure.goto) !== undefined
    || step.on_failure.route_to !== undefined
    || step.on_failure.on_unresolved !== undefined;
  if (control.kind === "terminal" && control.status === "failed" && !hasExplicitTarget) {
    const terminal = state.terminalStates.at(-1);
    if (terminal?.stepId === stepId && terminal.status === "failed") terminal.status = "paused";
    return { kind: "terminal", status: "paused" };
  }
  return control;
}

function simulateSessionRequestStep(
  step: AgentFlowWorkflowStep,
  fixture: AgentFlowSimulationStepFixture,
  stepId: string,
  state: SimulationState,
  providerOutcomeFailed: boolean
): SequenceControl {
  const isReview = step.type === "review";
  const isApproval = step.type === "approval";
  const isExchange = step.type === "consult" || step.type === "challenge";
  if (isApproval) {
    const reviewer = nonEmptyString(step.reviewer);
    const session = reviewer === undefined ? undefined : state.workflow.sessions?.[reviewer];
    const authority = isRecord(session) && isRecord(session.authority) ? session.authority : undefined;
    if (authority?.can_approve !== true) {
      return simulatedSessionFailure(
        step,
        fixture,
        stepId,
        state,
        "Approval simulation reviewers must explicitly declare can_approve authority."
      );
    }
  }
  if (step.type === "consult" && step.blocking === true) {
    const target = nonEmptyString(step.to);
    const session = target === undefined ? undefined : state.workflow.sessions?.[target];
    const authority = isRecord(session) && isRecord(session.authority) ? session.authority : undefined;
    if (authority?.can_block !== true) {
      return simulatedSessionFailure(
        step,
        fixture,
        stepId,
        state,
        "Blocking consult simulation targets must explicitly declare can_block authority."
      );
    }
  }
  const declaredInputs = isApproval || isReview || isExchange ? step.artifacts : step.inputs;
  const resolvedInputs: string[] = [];
  for (const value of Array.isArray(declaredInputs) ? declaredInputs as AgentFlowYamlValue[] : []) {
    const name = nonEmptyString(value);
    const reference = name === undefined ? null : /^\{\{\s*inputs\.([A-Za-z0-9_-]+)\s*}}$/.exec(name);
    if (reference === null) {
      resolvedInputs.push(...artifactName(value, state));
      continue;
    }
    const resolved = state.fixture.inputs?.[reference[1]!];
    const normalized = typeof resolved === "string" ? tryNormalizeArtifactPath(resolved.trim()) : undefined;
    if (normalized === undefined) {
      return simulatedSessionFailure(
        step,
        fixture,
        stepId,
        state,
        `Session input ${name} must resolve to a non-empty normalized artifact path.`
      );
    }
    resolvedInputs.push(normalized);
  }
  const missingInputs = resolvedInputs.filter((artifact) => !state.artifacts.has(artifact));
  if (missingInputs.length > 0) {
    for (const artifact of missingInputs) {
      state.handledMissingArtifacts.add(missingArtifactKey({ stepId, artifact, kind: "input" }));
    }
    return simulatedSessionFailure(
      step,
      fixture,
      stepId,
      state,
      `Fixture does not provide declared session input ${missingInputs[0]}.`
    );
  }

  const rawOutputs = isExchange ? [step.output]
    : isApproval ? [nonEmptyString(step.output) ?? defaultAgentFlowApprovalOutputPath(stepId)]
      : Array.isArray(step.outputs) ? step.outputs : [];
  const declaredOutputs = new Set(
    rawOutputs
      .flatMap((output) => nonEmptyString(output) ?? [])
      .map(canonicalArtifactName)
  );
  if (isExchange && declaredOutputs.size !== 1) {
    return simulatedSessionFailure(
      step,
      fixture,
      stepId,
      state,
      `${step.type === "consult" ? "Consult" : "Challenge"} simulation requires one declared output artifact.`
    );
  }
  for (const output of rawOutputs) {
    const name = nonEmptyString(output);
    if (name === undefined) continue;
    const artifact = canonicalArtifactName(name);
    if (state.artifacts.has(artifact) && state.artifactProducers.get(artifact) !== stepId && step.overwrite !== true) {
      return simulatedSessionFailure(
        step,
        fixture,
        stepId,
        state,
        `Artifact ${artifact} already exists; declare overwrite: true to replace it during simulation.`
      );
    }
  }
  const budgetControl = simulationModelBudgetControl(step, stepId, state);
  if (budgetControl !== undefined) {
    state.visitedSteps.at(-1)!.outcome = "failed";
    return budgetControl;
  }
  if (providerOutcomeFailed) {
    return simulatedSessionFailure(
      step,
      fixture,
      stepId,
      state,
      `Fixture marks the ${isReview ? "review" : isExchange ? String(step.type) : "session request"} as failed.`
    );
  }
  const providedOutputs = Array.isArray(fixture.outputs)
    ? canonicalFixtureArtifactNames(fixture.outputs)
    : canonicalFixtureArtifacts(fixture.outputs ?? {});
  const invalidOutput = providedOutputs.collisions.values().next().value
    ?? [...declaredOutputs].find((output) => !providedOutputs.values.has(output))
    ?? [...providedOutputs.values.keys()].find((output) => !declaredOutputs.has(output));
  if (invalidOutput !== undefined) {
    return simulatedSessionFailure(
      step,
      fixture,
      stepId,
      state,
      `Session fixture outputs must match declared outputs exactly; invalid output ${invalidOutput}.`
    );
  }
  if (isReview) {
    for (const output of declaredOutputs) {
      const value = providedOutputs.values.get(output);
      try {
        parseAgentFlowReviewResult(typeof value === "string" ? value : `${JSON.stringify(value)}\n`, output);
      } catch (error) {
        return simulatedSessionFailure(
          step,
          fixture,
          stepId,
          state,
          error instanceof Error ? error.message : String(error)
        );
      }
    }
  } else if (isApproval) {
    const output = [...declaredOutputs][0]!;
    const value = providedOutputs.values.get(output);
    try {
      parseAgentFlowApprovalResult(typeof value === "string" ? value : `${JSON.stringify(value)}\n`, output);
    } catch (error) {
      return simulatedSessionFailure(step, fixture, stepId, state, error instanceof Error ? error.message : String(error));
    }
  } else if (step.type === "consult") {
    const output = [...declaredOutputs][0]!;
    const value = providedOutputs.values.get(output);
    let consultResult;
    try {
      consultResult = parseAgentFlowConsultResult(
        typeof value === "string" ? value : `${JSON.stringify(value)}\n`,
        output,
        step.blocking === true
      );
    } catch (error) {
      return simulatedSessionFailure(step, fixture, stepId, state, error instanceof Error ? error.message : String(error));
    }
    if (consultResult.status === "blocked") {
      recordOutputs(step, fixture, stepId, state);
      state.terminalStates.push({ stepId, status: "paused" });
      return { kind: "terminal", status: "paused" };
    }
  } else if (step.type === "challenge") {
    const output = [...declaredOutputs][0]!;
    const value = providedOutputs.values.get(output);
    try {
      parseAgentFlowChallengeResult(typeof value === "string" ? value : `${JSON.stringify(value)}\n`, output);
    } catch (error) {
      return simulatedSessionFailure(step, fixture, stepId, state, error instanceof Error ? error.message : String(error));
    }
  }
  recordOutputs(step, fixture, stepId, state);
  return { kind: "done" };
}

function simulatedSessionFailure(
  step: AgentFlowWorkflowStep,
  fixture: AgentFlowSimulationStepFixture,
  stepId: string,
  state: SimulationState,
  message: string
): SequenceControl {
  recordSimulationFailure(state, stepId);
  if (isRecord(step.on_failure)) return simulatedTransformFailure(step, fixture, stepId, state, message);
  const visit = state.visitedSteps.at(-1);
  if (visit?.id === stepId && visit.outcome === "succeeded") visit.outcome = "failed";
  state.terminalStates.push({ stepId, status: "paused" });
  return { kind: "terminal", status: "paused" };
}

function conditionControl(
  step: AgentFlowWorkflowStep,
  id: string,
  state: SimulationState
): SequenceControl {
  const missingRequired = state.missingInputs[0];
  if (missingRequired !== undefined) {
    markConditionVisitFailed(state, id);
    addUnresolved(state, id, `Required condition input ${missingRequired} is missing from the simulation fixture.`);
    return { kind: "terminal", status: "unresolved" };
  }
  let target: string | undefined;
  try {
    target = selectAgentFlowConditionTargetFromValues(
      step,
      state.fixture.inputs ?? {},
      state.artifactValues
    ).target;
  } catch (error) {
    markConditionVisitFailed(state, id);
    if (error instanceof AgentFlowFailureClassificationError) {
      state.terminalStates.push({ stepId: id, status: "paused" });
      return { kind: "terminal", status: "paused" };
    }
    addUnresolved(state, id, error instanceof Error ? error.message : String(error));
    return { kind: "terminal", status: "unresolved" };
  }

  if (target === undefined) return { kind: "done" };

  const allowed = conditionTargets(step);
  if (!allowed.has(target)) {
    addUnresolved(state, id, `Condition target "${target}" is not declared by this step.`);
    return { kind: "terminal", status: "unresolved" };
  }

  return controlForTarget(target, id, state);
}

function markConditionVisitFailed(state: SimulationState, stepId: string): void {
  const visit = state.visitedSteps.at(-1);
  if (visit?.id === stepId && visit.outcome === "selected") visit.outcome = "failed";
}

function gateControl(
  step: AgentFlowWorkflowStep,
  stepFixture: AgentFlowSimulationStepFixture,
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

  const field = choice === "approve"
    ? "on_approve"
    : choice === "cancel" || choice === "cancelled"
      ? "on_cancel"
      : choice === "reject"
        ? "on_reject"
        : undefined;
  const target = field === undefined ? undefined : nonEmptyString(step[field]);
  if (target !== undefined) return controlForTarget(target, id, state);
  if (choice === "reject") return controlForTarget("cancel", id, state);
  if (["pause", "cancel", "cancelled", "fail", "failed", "complete", "completed"].includes(choice)) {
    return controlForTarget(choice, id, state);
  }
  return { kind: "done" };
}

function loopControl(
  step: AgentFlowWorkflowStep,
  stepFixture: AgentFlowSimulationStepFixture,
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

  const termination = stepFixture.loop_termination ?? "condition_met";
  if (termination === "max_iterations" && typeof step.max_iterations !== "number") {
    addUnresolved(state, id, "Fixture selects max_iterations termination for a loop without max_iterations.");
    return { kind: "terminal", status: "unresolved" };
  }
  if (termination === "max_iterations" && iterations !== step.max_iterations) {
    addUnresolved(state, id, `Fixture selects max_iterations termination but requests ${iterations} of ${step.max_iterations} iterations.`);
    return { kind: "terminal", status: "unresolved" };
  }
  if (termination === "max_duration" &&
      typeof step.max_duration_seconds !== "number" && typeof step.max_duration_minutes !== "number") {
    addUnresolved(state, id, "Fixture selects max_duration termination for a loop without a duration bound.");
    return { kind: "terminal", status: "unresolved" };
  }

  const body = Array.isArray(step.body) ? step.body.filter(isRecord) as AgentFlowWorkflowStep[] : [];
  for (let iteration = 0; iteration < iterations; iteration += 1) {
    if (iteration > 0) {
      const guardControl = simulationRecoveryGuard(step, id, state);
      if (guardControl !== undefined) return guardControl;
    }
    const control = runSequence(body, state, true);
    if (control.kind === "break_loop") continue;
    if (control.kind !== "done") return control;
  }
  if (termination !== "condition_met") {
    state.terminalStates.push({ stepId: id, status: termination });
    return { kind: "terminal", status: "timed_out" };
  }
  return { kind: "done" };
}

function parallelControl(step: AgentFlowWorkflowStep, state: SimulationState, insideLoop: boolean): SequenceControl {
  const initialArtifacts = new Set(state.artifacts);
  const mergedArtifacts = new Set(initialArtifacts);
  const initialArtifactValues = new Map(state.artifactValues);
  const mergedArtifactValues = new Map(initialArtifactValues);
  const initialProducedArtifacts = new Map(state.producedArtifacts);
  const mergedProducedArtifacts = new Map(initialProducedArtifacts);
  const initialArtifactProducers = new Map(state.artifactProducers);
  const mergedArtifactProducers = new Map(initialArtifactProducers);
  const parallelArtifactValues = new Map<string, AgentFlowYamlValue | undefined>();
  const conflictedArtifacts = new Set<string>();
  const parallelId = nonEmptyString(step.id) ?? "(unnamed)";
  let finalControl: SequenceControl = { kind: "done" };

  for (const entries of [step.branches, step.body, step.steps]) {
    if (!Array.isArray(entries)) continue;
    for (const entry of entries) {
      if (!isRecord(entry)) continue;
      state.artifacts = new Set(initialArtifacts);
      state.artifactValues = new Map(initialArtifactValues);
      state.producedArtifacts = new Map(initialProducedArtifacts);
      state.artifactProducers = new Map(initialArtifactProducers);
      const branchId = nonEmptyString(entry.id) ?? "(unnamed)";
      if (!takeTransition(state, branchId)) {
        finalControl = { kind: "terminal", status: "unresolved" };
        break;
      }
      const nestedLists = [entry.body, entry.steps].filter(Array.isArray) as AgentFlowYamlValue[][];
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
        control = runSequence(nested.filter(isRecord) as AgentFlowWorkflowStep[], state, insideLoop);
        if (control.kind !== "done") break;
      }
      for (const artifact of state.artifacts) mergedArtifacts.add(artifact);
      for (const [artifact, producedCount] of state.producedArtifacts) {
        const initialCount = initialProducedArtifacts.get(artifact) ?? 0;
        if (producedCount <= initialCount) continue;
        mergedProducedArtifacts.set(artifact, Math.max(mergedProducedArtifacts.get(artifact) ?? 0, producedCount));
        const producer = state.artifactProducers.get(artifact);
        if (producer !== undefined) mergedArtifactProducers.set(artifact, producer);
        if (conflictedArtifacts.has(artifact)) continue;
        const hasValue = state.artifactValues.has(artifact);
        const value = state.artifactValues.get(artifact);
        const previous = parallelArtifactValues.get(artifact);
        const valuesConflict = parallelArtifactValues.has(artifact)
          && (!hasValue || previous === undefined || !isDeepEqualArtifactValue(previous, value!));
        if (valuesConflict) {
          addUnresolved(state, parallelId, `Parallel branches produced conflicting values for artifact ${artifact}; fixture simulation cannot apply the declared conflict policy.`);
          mergedArtifactValues.delete(artifact);
          mergedArtifactProducers.delete(artifact);
          conflictedArtifacts.add(artifact);
          finalControl = { kind: "terminal", status: "unresolved" };
          continue;
        }
        parallelArtifactValues.set(artifact, value);
        if (hasValue) mergedArtifactValues.set(artifact, value!);
        else mergedArtifactValues.delete(artifact);
      }
      if (control.kind !== "done" && finalControl.kind === "done") {
        finalControl = control;
      }
    }
  }

  state.artifacts = mergedArtifacts;
  state.artifactValues = mergedArtifactValues;
  state.producedArtifacts = mergedProducedArtifacts;
  state.artifactProducers = mergedArtifactProducers;
  return finalControl;
}

function failureControl(
  step: AgentFlowWorkflowStep,
  stepFixture: AgentFlowSimulationStepFixture,
  id: string,
  state: SimulationState,
  allowRetry = true
): SequenceControl {
  const onFailure = isRecord(step.on_failure) ? step.on_failure : undefined;
  const retries = allowRetry && typeof onFailure?.retry === "number" && Number.isSafeInteger(onFailure.retry) && onFailure.retry > 0
    ? onFailure.retry
    : 0;
  const retryAttempt = state.retryAttempts.get(id) ?? 0;
  if (retryAttempt < retries) {
    state.retryAttempts.set(id, retryAttempt + 1);
    state.immediateRetries.add(id);
    return { kind: "target", target: id, budgetChecked: true };
  }
  state.retryAttempts.delete(id);

  const target = nonEmptyString(onFailure?.then) ?? nonEmptyString(onFailure?.goto);
  if (target !== undefined) return controlForTarget(target, id, state);

  if (onFailure?.route_to !== undefined) {
    const guardControl = simulationRecoveryGuard(step, id, state);
    if (guardControl !== undefined) return guardControl;
    const route = isRecord(onFailure.route_to) ? onFailure.route_to : undefined;
    const routeSession = nonEmptyString(route?.session);
    if (routeSession !== undefined) {
      const budgetControl = simulationSessionBudgetControl(routeSession, id, state);
      if (budgetControl !== undefined) return budgetControl;
    }
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

  const defaultStatus = state.workflowStyle === "pipeline" && nonEmptyString(step.type) !== "condition"
    ? "paused"
    : "failed";
  state.terminalStates.push({ stepId: id, status: defaultStatus });
  return { kind: "terminal", status: defaultStatus };
}

function checkInputs(step: AgentFlowWorkflowStep, stepId: string, state: SimulationState): void {
  const values: string[] = [];
  if (Array.isArray(step.inputs)) values.push(...step.inputs.flatMap((value) => artifactName(value, state)));
  if (isRecord(step.inputs)) values.push(...nestedArtifactNames(step.inputs, state));
  if (Array.isArray(step.artifacts)) values.push(...step.artifacts.flatMap((value) => artifactName(value, state)));
  if (step.type === "artifact_transform") values.push(...transformArtifactName(step.input, state));

  for (const artifact of values) {
    if (!state.artifacts.has(artifact)) addMissingArtifact(state, { stepId, artifact, kind: "input" });
  }
}

function hasMissingDeclaredArtifacts(step: AgentFlowWorkflowStep, state: SimulationState): boolean {
  return Array.isArray(step.artifacts) && step.artifacts
    .flatMap((value) => artifactName(value, state))
    .some((artifact) => !state.artifacts.has(artifact));
}

function recordOutputs(
  step: AgentFlowWorkflowStep,
  fixture: AgentFlowSimulationStepFixture,
  stepId: string,
  state: SimulationState
): void {
  const declared = new Set(declaredOutputArtifacts(step));

  let provided: Map<string, AgentFlowYamlValue | undefined>;
  if (Array.isArray(fixture.outputs)) {
    provided = new Map(fixture.outputs.map((artifact) => [canonicalArtifactName(artifact), undefined]));
  } else {
    const canonical = canonicalFixtureArtifacts(fixture.outputs ?? {});
    provided = new Map(canonical.values);
    for (const artifact of canonical.collisions) {
      provided.set(artifact, undefined);
      addUnresolved(state, stepId, `Fixture output keys collide at canonical path ${artifact}.`);
    }
  }
  for (const artifact of declared) {
    if (provided.has(artifact)) {
      state.artifacts.add(artifact);
      markArtifactProduced(state, artifact, stepId);
      const value = provided.get(artifact);
      if (value !== undefined) state.artifactValues.set(artifact, value);
      else state.artifactValues.delete(artifact);
    }
    else addMissingArtifact(state, { stepId, artifact, kind: "output" });
  }
  for (const artifact of provided.keys()) {
    if (!declared.has(artifact)) {
      addUnresolved(state, stepId, `Fixture provides undeclared output artifact ${artifact}.`);
    }
  }
}

function simulateTransformStep(
  step: AgentFlowWorkflowStep,
  stepFixture: AgentFlowSimulationStepFixture,
  stepId: string,
  state: SimulationState
): SequenceControl {
  const inputPath = transformArtifactName(step.input, state)[0];
  const outputPath = transformArtifactName(step.output, state)[0];
  const transform = nonEmptyString(step.transform);
  if (inputPath === undefined || outputPath === undefined || transform === undefined) {
    return simulatedTransformFailure(step, stepFixture, stepId, state, "Artifact transform paths and transform name must resolve before simulation.");
  }
  if (!state.artifacts.has(inputPath)) {
    const control = simulatedTransformFailure(step, stepFixture, stepId, state, `Fixture does not provide declared transform input ${inputPath}.`);
    if (!(control.kind === "terminal" && control.status === "unresolved")) {
      state.handledMissingArtifacts.add(missingArtifactKey({ stepId, artifact: inputPath, kind: "input" }));
    }
    return control;
  }

  const input = state.artifactValues.get(inputPath);
  if (input === undefined) {
    return simulatedTransformFailure(step, stepFixture, stepId, state, `Fixture artifact ${inputPath} must include a value to simulate transform ${transform}.`);
  }
  try {
    const output = transformAgentFlowFixtureArtifact(
      transform,
      input,
      { inputPath, outputPath },
      state.transforms
    );
    const existing = state.artifactValues.get(outputPath);
    if (state.artifacts.has(outputPath)
        && state.artifactProducers.get(outputPath) !== stepId
        && !isDeepEqualArtifactValue(existing, output)
        && step.overwrite !== true) {
      return simulatedTransformFailure(step, stepFixture, stepId, state, `Artifact ${outputPath} already exists; declare overwrite: true to replace it during simulation.`);
    }
    state.artifacts.add(outputPath);
    state.artifactValues.set(outputPath, output);
    markArtifactProduced(state, outputPath, stepId);
    return { kind: "done" };
  } catch (error) {
    const message = error instanceof AgentFlowArtifactTransformError
      ? error.message
      : error instanceof Error ? error.message : String(error);
    return simulatedTransformFailure(step, stepFixture, stepId, state, message);
  }
}

function simulatedTransformFailure(
  step: AgentFlowWorkflowStep,
  stepFixture: AgentFlowSimulationStepFixture,
  stepId: string,
  state: SimulationState,
  message: string
): SequenceControl {
  const visit = state.visitedSteps.at(-1);
  if (visit?.id === stepId && visit.outcome === "succeeded") visit.outcome = "failed";
  recordSimulationFailure(state, stepId);
  if (isRecord(step.on_failure)) {
    const control = failureControl(step, stepFixture, stepId, state);
    const hasExplicitTarget = nonEmptyString(step.on_failure.then) !== undefined
      || nonEmptyString(step.on_failure.goto) !== undefined
      || step.on_failure.route_to !== undefined
      || step.on_failure.on_unresolved !== undefined;
    if (control.kind === "terminal" && control.status === "failed" && !hasExplicitTarget) {
      const terminal = state.terminalStates.at(-1);
      if (terminal?.stepId === stepId && terminal.status === "failed") terminal.status = "paused";
      return { kind: "terminal", status: "paused" };
    }
    return control;
  }
  addUnresolved(state, stepId, message);
  return { kind: "terminal", status: "unresolved" };
}

function isDeepEqualArtifactValue(left: AgentFlowYamlValue | undefined, right: AgentFlowYamlValue): boolean {
  return left !== undefined && isDeepStrictEqual(left, right);
}

function conditionTargets(step: AgentFlowWorkflowStep): Set<string> {
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

function statusFromTerminal(status: string): AgentFlowSimulationStatus {
  if (["fail", "failed"].includes(status)) return "failed";
  if (["pause", "paused", "unresolved"].includes(status)) return status === "unresolved" ? "unresolved" : "paused";
  if (["cancel", "cancelled"].includes(status)) return "cancelled";
  return "completed";
}

function artifactName(value: AgentFlowYamlValue | undefined, state: SimulationState): string[] {
  const name = nonEmptyString(value);
  if (name === undefined) return [];
  const inputReference = /^\{\{\s*inputs\.([A-Za-z0-9_-]+)\s*}}$/.exec(name);
  if (inputReference !== null) {
    const resolved = state.fixture.inputs?.[inputReference[1]];
    return typeof resolved === "string" ? artifactName(resolved, state) : [];
  }
  if (name.includes("{{")) return [];
  return [canonicalArtifactName(name)];
}

function transformArtifactName(value: AgentFlowYamlValue | undefined, state: SimulationState): string[] {
  return artifactName(value, state).flatMap((artifact) => tryNormalizeArtifactPath(artifact) ?? []);
}

function canonicalFixtureArtifacts(artifacts: Record<string, AgentFlowYamlValue>): {
  values: Map<string, AgentFlowYamlValue>;
  collisions: Set<string>;
} {
  const values = new Map<string, AgentFlowYamlValue>();
  const collisions = new Set<string>();
  for (const [artifact, value] of Object.entries(artifacts)) {
    const canonical = canonicalArtifactName(artifact);
    if (values.has(canonical) || collisions.has(canonical)) {
      values.delete(canonical);
      collisions.add(canonical);
    } else {
      values.set(canonical, value);
    }
  }
  return { values, collisions };
}

function exactFixtureArtifacts(artifacts: Record<string, AgentFlowYamlValue>): {
  values: Map<string, AgentFlowYamlValue>;
  collisions: Set<string>;
} {
  return { values: new Map(Object.entries(artifacts)), collisions: new Set() };
}

function exactFixtureArtifactNames(artifacts: string[]): {
  values: Map<string, AgentFlowYamlValue>;
  collisions: Set<string>;
} {
  const values = new Map<string, AgentFlowYamlValue>();
  const collisions = new Set<string>();
  for (const artifact of artifacts) {
    if (values.has(artifact)) collisions.add(artifact);
    else values.set(artifact, null);
  }
  return { values, collisions };
}

function canonicalFixtureArtifactNames(artifacts: string[]): {
  values: Map<string, AgentFlowYamlValue>;
  collisions: Set<string>;
} {
  const values = new Map<string, AgentFlowYamlValue>();
  const collisions = new Set<string>();
  for (const artifact of artifacts) {
    const canonical = canonicalArtifactName(artifact);
    if (values.has(canonical) || collisions.has(canonical)) {
      values.delete(canonical);
      collisions.add(canonical);
    } else {
      values.set(canonical, null);
    }
  }
  return { values, collisions };
}

function canonicalArtifactName(artifact: string): string {
  return tryNormalizeArtifactPath(artifact) ?? artifact;
}

function tryNormalizeArtifactPath(artifact: string): string | undefined {
  try {
    return normalizeAgentFlowArtifactPath(artifact);
  } catch {
    return undefined;
  }
}

function nestedArtifactNames(value: AgentFlowYamlValue | undefined, state: SimulationState): string[] {
  if (value === undefined) return [];
  if (Array.isArray(value)) return value.flatMap((entry) => nestedArtifactNames(entry, state));
  if (isRecord(value)) return Object.values(value).flatMap((entry) => nestedArtifactNames(entry, state));
  return artifactName(value, state);
}

function checkTargetBudget(control: Extract<SequenceControl, { kind: "target" }>, state: SimulationState): SequenceControl {
  const attemptLimit = state.stepAttemptLimits.get(control.target);
  if (attemptLimit !== undefined && (state.visits.get(control.target) ?? 0) + 1 > attemptLimit) {
    const status = simulationRecoveryLimitStatus(state);
    state.terminalStates.push({ stepId: control.target, status });
    return { kind: "terminal", status };
  }
  if (control.budgetChecked || state.maxRecoveryCycles === undefined || (state.visits.get(control.target) ?? 0) === 0) {
    return { ...control, budgetChecked: true };
  }

  const cycles = (state.recoveryCycles.get(control.target) ?? 0) + 1;
  state.recoveryCycles.set(control.target, cycles);
  if (cycles <= state.maxRecoveryCycles) return { ...control, budgetChecked: true };

  const status = simulationRecoveryLimitStatus(state);
  state.terminalStates.push({ stepId: control.target, status });
  return { kind: "terminal", status };
}

function simulationRecoveryLimitStatus(state: SimulationState): "failed" | "paused" {
  return state.workflowStyle === "recovery_pipeline"
    && isRecord(state.workflow.policies) && state.workflow.policies.recovery_limits === "fail"
    ? "failed"
    : "paused";
}

function recordSimulationFailure(state: SimulationState, stepId: string): void {
  state.failureAttempts.set(stepId, Math.max(state.failureAttempts.get(stepId) ?? 0, state.visits.get(stepId) ?? 1));
}

function assertSimulationArtifactValueAvailable(
  state: SimulationState,
  scope: "inputs" | "artifacts",
  segments: string[]
): void {
  if (scope !== "artifacts") return;
  const published = [...state.artifacts]
    .map((artifact) => ({ artifact, alias: agentFlowConditionArtifactAlias(artifact) }))
    .filter(({ alias }) => {
      return segments.slice(0, alias.length).join(".") === alias.join(".");
    })
    .sort((left, right) => right.alias.length - left.alias.length);
  const selectedAliasLength = published[0]?.alias.length;
  const selected = published.filter(({ alias }) => alias.length === selectedAliasLength);
  if (selected.some(({ artifact }) => !state.artifactValues.has(artifact))) {
    throw new AgentFlowConditionError(
      `Condition artifact reference artifacts.${segments.join(".")} matches a published artifact without an evaluable fixture value.`
    );
  }
}

function workflowStepAttemptLimits(workflow: AgentFlowWorkflow): Map<string, number> {
  const limits = isRecord(workflow.limits) ? workflow.limits : undefined;
  const configured = isRecord(limits?.max_step_attempts) ? limits.max_step_attempts : undefined;
  return new Map(Object.entries(configured ?? {}).flatMap(([stepId, value]) =>
    typeof value === "number" && Number.isFinite(value) && value > 0 ? [[stepId, value] as const] : []
  ));
}

function workflowRecoveryLimit(workflow: AgentFlowWorkflow): number | undefined {
  const limits = isRecord(workflow.limits) ? workflow.limits : undefined;
  return workflow.style !== "pipeline" && typeof limits?.max_recovery_cycles === "number"
    && Number.isSafeInteger(limits.max_recovery_cycles) && limits.max_recovery_cycles > 0
    ? limits.max_recovery_cycles
    : undefined;
}

function addMissingArtifact(state: SimulationState, entry: AgentFlowSimulationMissingArtifact): void {
  if (!state.missingArtifacts.some((candidate) => candidate.stepId === entry.stepId && candidate.artifact === entry.artifact && candidate.kind === entry.kind)) {
    state.missingArtifacts.push(entry);
  }
}

function markArtifactProduced(state: SimulationState, artifact: string, stepId: string): void {
  state.producedArtifacts.set(artifact, (state.producedArtifacts.get(artifact) ?? 0) + 1);
  state.artifactProducers.set(artifact, stepId);
}

function missingArtifactKey(entry: AgentFlowSimulationMissingArtifact): string {
  return `${entry.stepId}\0${entry.kind}\0${entry.artifact}`;
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

function nonEmptyString(value: AgentFlowYamlValue | undefined): string | undefined {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : undefined;
}

function isRecord(value: unknown): value is AgentFlowYamlMapping {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function requiredWorkflowInputs(workflow: AgentFlowWorkflow): string[] {
  return Object.entries(workflow.inputs ?? {})
    .filter(([, definition]) => isRecord(definition) && definition.required === true)
    .map(([name]) => name)
    .sort();
}

function validOutcome(value: AgentFlowYamlValue | undefined): boolean {
  if (value === undefined) return true;
  const valid = (entry: unknown) => entry === "succeeded" || entry === "failed";
  return Array.isArray(value) ? value.length > 0 && value.every(valid) : valid(value);
}

function validOutputs(value: AgentFlowYamlValue | undefined): boolean {
  if (value === undefined) return true;
  return (isRecord(value) && Object.keys(value).every((key) => key.trim().length > 0))
    || (Array.isArray(value) && value.every((entry) => nonEmptyString(entry) !== undefined));
}

function validChoice(value: AgentFlowYamlValue | undefined): boolean {
  if (value === undefined) return true;
  return Array.isArray(value)
    ? value.length > 0 && value.every((entry) => nonEmptyString(entry) !== undefined)
    : nonEmptyString(value) !== undefined;
}

function collectSimulationStepIdCounts(
  steps: AgentFlowWorkflowStep[],
  counts = new Map<string, number>()
): Map<string, number> {
  for (const step of steps) {
    const id = nonEmptyString(step.id);
    if (id !== undefined) counts.set(id, (counts.get(id) ?? 0) + 1);

    for (const field of ["body", "steps"] as const) {
      const nested = step[field];
      if (Array.isArray(nested)) {
        collectSimulationStepIdCounts(nested.filter(isRecord) as AgentFlowWorkflowStep[], counts);
      }
    }

    if (step.type === "parallel" && Array.isArray(step.branches)) {
      collectSimulationStepIdCounts(step.branches.filter(isRecord) as AgentFlowWorkflowStep[], counts);
    }
  }
  return counts;
}

function collectSimulationStepLocations(
  steps: AgentFlowWorkflowStep[],
  insideLoop = false,
  locations = new Map<string, SimulationStepLocation>()
): Map<string, SimulationStepLocation> {
  steps.forEach((step, index) => {
    const id = nonEmptyString(step.id);
    if (id !== undefined && nonEmptyString(step.type) !== undefined) {
      locations.set(id, { steps, index, insideLoop });
    }

    const nestedInsideLoop = insideLoop || step.type === "loop";
    for (const field of ["body", "steps"] as const) {
      const nested = step[field];
      if (Array.isArray(nested)) {
        collectSimulationStepLocations(
          nested.filter(isRecord) as AgentFlowWorkflowStep[],
          nestedInsideLoop,
          locations
        );
      }
    }

    if (step.type === "parallel" && Array.isArray(step.branches)) {
      const branches = step.branches.filter(isRecord) as AgentFlowWorkflowStep[];
      collectSimulationStepLocations(branches, insideLoop, locations);
    }
  });

  return locations;
}
