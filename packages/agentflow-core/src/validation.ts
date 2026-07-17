import type {
  AgentflowWorkflow,
  AgentflowWorkflowStep,
  AgentflowYamlMapping,
  AgentflowYamlValue
} from "./workflow";
import { validateAgentflowPolicyPrimitives } from "./policy";
import { policyGlobLayersHaveWritablePath } from "./policy_utils";

export const MAX_AGENTFLOW_COMMAND_TIMEOUT_SECONDS = 2_147_483.647;

export interface AgentflowWorkflowIssue {
  code: string;
  message: string;
  path: string;
  stepId?: string;
}

export interface AgentflowWorkflowValidationResult {
  valid: boolean;
  errors: AgentflowWorkflowIssue[];
}

export interface AgentflowWorkflowLintResult {
  warnings: AgentflowWorkflowIssue[];
}

interface StepContext {
  step: AgentflowWorkflowStep;
  path: string;
  id?: string;
  type?: string;
  depth: number;
}

interface WriterScope {
  session: string;
  includeLayers: string[][];
  exclusions: string[];
  path: string;
}

const STEP_TYPES = new Set([
  "approval",
  "artifact_transform",
  "challenge",
  "command",
  "condition",
  "consult",
  "decision_record",
  "handoff",
  "input_request",
  "loop",
  "manual_gate",
  "mcp_call",
  "parallel",
  "result",
  "review",
  "session_request",
  "workflow"
]);

const TERMINAL_TARGETS = new Set(["cancel", "complete", "completed", "continue", "fail", "ignore", "pause"]);
const TARGET_FIELDS = new Set(["else", "goto", "on_approve", "on_cancel", "on_reject", "return_to", "then"]);
const SECRET_PATH = /(^|[/._-])(\.env|credentials|id_rsa|id_ed25519|private[_-]?key|secrets?)([/._-]|$)/i;
const SHELL_EXECUTABLES = new Set(["bash", "dash", "ksh", "sh", "zsh"]);
const SHELL_ANALYSIS_BUDGET = 65_536;
export const MAX_AGENTFLOW_COMMAND_RETRIES = 100;
const STEP_REQUIREMENTS: Readonly<Record<string, ReadonlyArray<readonly [string, "string" | "array"]>>> = {
  approval: [["reviewer", "string"], ["artifacts", "array"]],
  artifact_transform: [["input", "string"], ["output", "string"], ["transform", "string"]],
  challenge: [["from", "string"], ["to", "string"], ["question", "string"]],
  command: [["command", "string"]],
  consult: [["from", "string"], ["to", "string"], ["question", "string"]],
  decision_record: [["owner", "string"], ["topic", "string"], ["artifacts", "array"]],
  handoff: [["from", "string"], ["to", "string"]],
  input_request: [["question", "string"], ["save_as", "string"]],
  loop: [["body", "array"]],
  manual_gate: [["message", "string"], ["options", "array"]],
  mcp_call: [["server", "string"], ["tool", "string"]],
  result: [["status", "string"]],
  review: [["reviewer", "string"], ["subject", "string"], ["artifacts", "array"]],
  session_request: [["session", "string"], ["prompt", "string"]],
  workflow: [["workflow", "string"]]
};

export function validateAgentflowWorkflow(workflow: AgentflowWorkflow): AgentflowWorkflowValidationResult {
  const errors: AgentflowWorkflowIssue[] = [];
  const contexts = collectStepContexts(workflow.steps);
  const ids = new Set(contexts.flatMap((context) => context.id === undefined ? [] : [context.id]));

  validateSessionDefinitions(workflow, errors);
  errors.push(...validateAgentflowPolicyPrimitives(workflow));
  validateStepShapes(contexts, errors);
  validateNestedStepShapes(workflow.steps, errors);
  validateDynamicReferences(workflow, errors);
  validateTargetShapes(contexts, errors);
  validateTargets(contexts, ids, errors);
  validateCommands(workflow, contexts, errors);
  validateSessionReferences(workflow, contexts, errors);
  validateControlStepShapes(workflow, contexts, errors);
  validateLoopBounds(contexts, errors);
  validateControlFlowCycles(workflow, contexts, errors);
  validateInputReferences(workflow, errors);
  validateArtifactOutputs(workflow, contexts, errors);
  validateApprovals(contexts, errors);
  validateParallelWriters(workflow, contexts, errors);
  validateCollaborativeReviewBounds(workflow, contexts, errors);

  return { valid: errors.length === 0, errors };
}

export function lintAgentflowWorkflow(workflow: AgentflowWorkflow): AgentflowWorkflowLintResult {
  const warnings: AgentflowWorkflowIssue[] = [];
  const contexts = collectStepContexts(workflow.steps);

  lintFrontierBudgets(workflow, warnings);
  lintReviewCycles(workflow, warnings);
  lintCommands(contexts, warnings);
  lintComplexity(contexts, warnings);
  lintArtifactOverwrites(contexts, warnings);

  return { warnings };
}

export function formatAgentflowWorkflowIssues(issues: AgentflowWorkflowIssue[]): string {
  return issues
    .map((issue) => `${issue.code} (${issue.path}): ${issue.message}`)
    .join("\n");
}

function collectStepContexts(steps: AgentflowWorkflowStep[], basePath = "steps", depth = 0): StepContext[] {
  const contexts: StepContext[] = [];

  steps.forEach((step, index) => {
    collectStepContext(step, `${basePath}[${index}]`, depth, contexts);
  });

  return contexts;
}

function collectStepContext(
  step: AgentflowWorkflowStep,
  path: string,
  depth: number,
  contexts: StepContext[]
): void {
  contexts.push({
    step,
    path,
    id: nonEmptyString(step.id),
    type: nonEmptyString(step.type),
    depth
  });

  for (const { entries, path: nestedPath } of nestedStepLists(step, path)) {
    entries.forEach((entry, index) => {
        if (isRecord(entry)) {
          collectStepContext(entry as AgentflowWorkflowStep, `${nestedPath}[${index}]`, depth + 1, contexts);
        }
    });
  }
}

function validateSessionDefinitions(workflow: AgentflowWorkflow, errors: AgentflowWorkflowIssue[]): void {
  const collaboration = isRecord(workflow.collaboration) ? workflow.collaboration : undefined;

  if (workflow.style === "collaborative" && collaboration?.enabled !== true) {
    errors.push({
      code: "workflow.collaboration.enabled.required",
      message: "Collaborative workflows must explicitly declare collaboration.enabled: true.",
      path: "collaboration.enabled"
    });
  }

  for (const [name, value] of Object.entries(workflow.sessions ?? {})) {
    if (!isRecord(value)) {
      errors.push({
        code: "workflow.session.definition.invalid",
        message: `Session "${name}" must be a mapping with executable session configuration.`,
        path: `sessions.${name}`
      });
    } else {
      if (nonEmptyString(value.provider) === undefined) {
        errors.push({
          code: "workflow.session.provider.required",
          message: `Session "${name}" must declare a non-empty provider.`,
          path: `sessions.${name}.provider`
        });
      }

      if (workflow.style === "collaborative" && nonEmptyString(value.role) === undefined) {
        errors.push({
          code: "workflow.session.role.required",
          message: `Collaborative session "${name}" must declare a non-empty role.`,
          path: `sessions.${name}.role`
        });
      }

      if (value.authority !== undefined && !isRecord(value.authority)) {
        errors.push({
          code: "workflow.session.authority.invalid",
          message: "Session authority must be a mapping of capability names to booleans.",
          path: `sessions.${name}.authority`
        });
      } else if (isRecord(value.authority)) {
        for (const [capability, enabled] of Object.entries(value.authority)) {
          if (typeof enabled !== "boolean") {
            errors.push({
              code: "workflow.session.authority.invalid",
              message: `Session authority capability "${capability}" must be a boolean.`,
              path: `sessions.${name}.authority.${capability}`
            });
          }
        }
      }

      if (value.file_scope !== undefined && !isRecord(value.file_scope)) {
        errors.push({
          code: "workflow.parallel.file_scope.invalid",
          message: "Session file_scope must be a mapping.",
          path: `sessions.${name}.file_scope`
        });
      } else if (isRecord(value.file_scope) && value.file_scope.include !== undefined) {
        const include = value.file_scope.include;

        if (!Array.isArray(include) || !include.every((entry) => nonEmptyString(entry) !== undefined)) {
          errors.push({
            code: "workflow.parallel.file_scope.invalid",
            message: "Session file_scope.include must be a list of non-empty strings.",
            path: `sessions.${name}.file_scope.include`
          });
        } else {
          validateRepoRelativeScopePatterns(
            include as string[],
            `sessions.${name}.file_scope.include`,
            errors
          );
        }
      }
    }
  }
}

function validateStepShapes(contexts: StepContext[], errors: AgentflowWorkflowIssue[]): void {
  const seenIds = new Set<string>();

  for (const context of contexts) {
    if (context.id === undefined) {
      addStepIssue(errors, context, "workflow.step.id.required", "id", "Workflow steps must declare a non-empty id.");
    } else if (seenIds.has(context.id)) {
      addStepIssue(errors, context, "workflow.step.id.duplicate", "id", `Step id "${context.id}" is declared more than once.`);
    } else {
      seenIds.add(context.id);
    }

    if (context.type === undefined) {
      addStepIssue(errors, context, "workflow.step.type.required", "type", "Workflow steps must declare a non-empty type.");
      continue;
    }

    if (!STEP_TYPES.has(context.type)) {
      addStepIssue(
        errors,
        context,
        "workflow.step.type.unknown",
        "type",
        `Unknown workflow step type "${context.type}".`
      );
      continue;
    }

    validateRequiredStepFields(context, errors);
  }
}

function validateRequiredStepFields(context: StepContext, errors: AgentflowWorkflowIssue[]): void {
  for (const [field, kind] of STEP_REQUIREMENTS[context.type ?? ""] ?? []) {
    const value = context.step[field];
    const valid = kind === "string" ? nonEmptyString(value) !== undefined : Array.isArray(value) && value.length > 0;

    if (!valid) {
      addStepIssue(
        errors,
        context,
        "workflow.step.field.required",
        field,
        `Step type "${context.type}" requires ${field} to be a non-empty ${kind === "string" ? "string" : "list"}.`
      );
    }
  }

  validateArtifactFieldShapes(context, errors);

  if (context.type === "command" && context.step.timeout_seconds !== undefined &&
      (typeof context.step.timeout_seconds !== "number" || !Number.isFinite(context.step.timeout_seconds) || context.step.timeout_seconds <= 0)) {
    addStepIssue(
      errors,
      context,
      "workflow.command.timeout.invalid",
      "timeout_seconds",
      "Command timeout_seconds must be a positive finite number."
    );
  }
  if (context.type === "command" && typeof context.step.timeout_seconds === "number" &&
      Number.isFinite(context.step.timeout_seconds) && context.step.timeout_seconds > MAX_AGENTFLOW_COMMAND_TIMEOUT_SECONDS) {
    addStepIssue(
      errors,
      context,
      "workflow.command.timeout.invalid",
      "timeout_seconds",
      `Command timeout_seconds cannot exceed ${MAX_AGENTFLOW_COMMAND_TIMEOUT_SECONDS}.`
    );
  }

  if (context.type === "command" && isRecord(context.step.on_failure)) {
    const retry = context.step.on_failure.retry;
    if (retry !== undefined && (!Number.isSafeInteger(retry) || Number(retry) < 0 || Number(retry) > MAX_AGENTFLOW_COMMAND_RETRIES)) {
      addStepIssue(
        errors,
        context,
        "workflow.command.retry.invalid",
        "on_failure.retry",
        `Command on_failure.retry must be an integer from 0 through ${MAX_AGENTFLOW_COMMAND_RETRIES}.`
      );
    }
    if (context.step.on_failure.then === "continue" && context.step.on_failure.allowed !== true) {
      addStepIssue(
        errors,
        context,
        "workflow.command.continue.not_allowed",
        "on_failure.allowed",
        "Command failures may continue only when on_failure.allowed is true."
      );
    }
  }

  if (context.type === "condition") {
    const hasBranches = Array.isArray(context.step.branches) && context.step.branches.length > 0;
    const hasInlineCondition = nonEmptyString(context.step.if) !== undefined && nonEmptyString(context.step.then) !== undefined;

    if (!hasBranches && !hasInlineCondition) {
      addStepIssue(
        errors,
        context,
        "workflow.step.field.required",
        "branches",
        "Condition step requires non-empty branches or both if and then fields."
      );
    }
  }

  if (context.type === "parallel") {
    const hasBranches = [context.step.branches, context.step.body, context.step.steps]
      .some((value) => Array.isArray(value) && value.length > 0);

    if (!hasBranches) {
      addStepIssue(
        errors,
        context,
        "workflow.step.field.required",
        "branches",
        "Parallel step requires a non-empty branches, body, or steps list."
      );
    }
  }
}

function validateArtifactFieldShapes(context: StepContext, errors: AgentflowWorkflowIssue[]): void {
  for (const field of ["artifacts", "inputs", "options", "outputs"]) {
    const value = context.step[field];
    const mappingAllowed = field === "inputs" && isRecord(value);

    if (value !== undefined && !mappingAllowed && (!Array.isArray(value) || !value.every((entry) => nonEmptyString(entry) !== undefined))) {
      addStepIssue(
        errors,
        context,
        "workflow.step.field.list",
        field,
        `Step field ${field} must be a list of non-empty strings.`
      );
    }
  }
}

function validateNestedStepShapes(steps: AgentflowWorkflowStep[], errors: AgentflowWorkflowIssue[]): void {
  steps.forEach((step, stepIndex) => {
    validateNestedStepShapesAt(step, `steps[${stepIndex}]`, errors);
  });
}

function validateNestedStepShapesAt(
  step: AgentflowWorkflowStep,
  stepPath: string,
  errors: AgentflowWorkflowIssue[]
): void {
  for (const { entries, path: nestedPath } of nestedStepLists(step, stepPath)) {
    entries.forEach((entry, index) => {
      const path = `${nestedPath}[${index}]`;

      if (!isRecord(entry)) {
        errors.push({
          code: "workflow.step.nested.item",
          message: "Nested workflow step entries must be mappings.",
          path,
          ...(nonEmptyString(step.id) === undefined ? {} : { stepId: String(step.id) })
        });
      } else {
        validateNestedStepShapesAt(entry as AgentflowWorkflowStep, path, errors);
      }
    });
  }
}

function nestedStepLists(
  step: AgentflowWorkflowStep,
  stepPath: string
): Array<{ entries: AgentflowYamlValue[]; path: string }> {
  const lists: Array<{ entries: AgentflowYamlValue[]; path: string }> = [];

  for (const field of ["body", "steps"]) {
    if (Array.isArray(step[field])) {
      lists.push({ entries: step[field] as AgentflowYamlValue[], path: `${stepPath}.${field}` });
    }
  }

  if (step.type === "parallel" && Array.isArray(step.branches)) {
    step.branches.forEach((branch, branchIndex) => {
      if (!isRecord(branch)) {
        return;
      }

      for (const field of ["body", "steps"]) {
        if (Array.isArray(branch[field])) {
          lists.push({
            entries: branch[field] as AgentflowYamlValue[],
            path: `${stepPath}.branches[${branchIndex}].${field}`
          });
        }
      }
    });
  }

  return lists;
}

function validateTargets(contexts: StepContext[], ids: Set<string>, errors: AgentflowWorkflowIssue[]): void {
  for (const context of contexts) {
    for (const target of stepTargetReferences(context.step, context.path)) {
      if (!ids.has(target.value) && !TERMINAL_TARGETS.has(target.value)) {
        errors.push({
          code: "workflow.step.target.unresolved",
          message: `Step target "${target.value}" does not match a declared step id or terminal outcome.`,
          path: target.path,
          ...(context.id === undefined ? {} : { stepId: context.id })
        });
      }
    }
  }
}

function validateTargetShapes(contexts: StepContext[], errors: AgentflowWorkflowIssue[]): void {
  for (const context of contexts) {
    validateTargetMapping(context.step, context.path, context, errors);

    if (context.type === "condition" && Array.isArray(context.step.branches)) {
      context.step.branches.forEach((branch, index) => {
        if (isRecord(branch)) {
          validateTargetMapping(branch, `${context.path}.branches[${index}]`, context, errors);
        }
      });
    }

    if (context.step.on_failure !== undefined && !isRecord(context.step.on_failure)) {
      addStepIssue(
        errors,
        context,
        "workflow.step.on_failure.shape",
        "on_failure",
        "Failure handlers must be mappings."
      );
    } else if (isRecord(context.step.on_failure)) {
      validateTargetMapping(context.step.on_failure, `${context.path}.on_failure`, context, errors);
      for (const field of ["on_remediated", "on_unresolved"]) {
        const nested = context.step.on_failure[field];
        if (nested !== undefined && !isRecord(nested)) {
          addStepIssue(
            errors,
            context,
            "workflow.step.on_failure.shape",
            `on_failure.${field}`,
            `Failure handler ${field} must be a mapping.`
          );
        } else if (isRecord(nested)) {
          validateTargetMapping(nested, `${context.path}.on_failure.${field}`, context, errors);
        }
      }
    }
  }
}

function validateTargetMapping(
  value: AgentflowYamlMapping,
  path: string,
  context: StepContext,
  errors: AgentflowWorkflowIssue[]
): void {
  for (const field of TARGET_FIELDS) {
    if (value[field] !== undefined && nonEmptyString(value[field]) === undefined) {
      errors.push({
        code: "workflow.step.target.shape",
        message: `Control-flow target ${field} must be a non-empty string.`,
        path: `${path}.${field}`,
        ...(context.id === undefined ? {} : { stepId: context.id })
      });
    }
  }
}

function validateDynamicReferences(workflow: AgentflowWorkflow, errors: AgentflowWorkflowIssue[]): void {
  visitValue(workflow, "", (value, path) => {
    if (typeof value === "string" && (value.includes("{{") || value.includes("}}")) && !isDynamicReference(value)) {
      errors.push({
        code: "workflow.reference.dynamic.malformed",
        message: "Dynamic references must use a complete {{ expression }} delimiter pair.",
        path
      });
    }
  });
}

function validateCommands(
  workflow: AgentflowWorkflow,
  contexts: StepContext[],
  errors: AgentflowWorkflowIssue[]
): void {
  for (const context of contexts) {
    if (context.type !== "command" || typeof context.step.command !== "string") {
      continue;
    }

    const unsafe = agentflowCommandUnsafeReason(context.step.command);

    const unsafePolicy = isRecord(workflow.policies) ? workflow.policies.unsafe_operations : undefined;

    if (unsafe && unsafePolicy !== "require_approval" && unsafePolicy !== "allow") {
      addStepIssue(
        errors,
        context,
        "workflow.command.unsafe",
        "command",
        `Command is unsafe because it performs ${unsafe}.`
      );
    }
  }
}

export function agentflowCommandUnsafeReason(command: string): string | undefined {
  return destructiveRmReason(command) ?? unsafeShellReason(command);
}

function validateSessionReferences(
  workflow: AgentflowWorkflow,
  contexts: StepContext[],
  errors: AgentflowWorkflowIssue[]
): void {
  const sessions = new Set(Object.keys(workflow.sessions ?? {}));

  for (const context of contexts) {
    const fields = sessionReferenceFields(context.type);

    for (const field of fields) {
      const value = nonEmptyString(context.step[field]);

      if (value !== undefined && !isDynamicReference(value) && !sessions.has(value)) {
        addStepIssue(
          errors,
          context,
          "workflow.session.undeclared",
          field,
          `Session "${value}" is referenced but not declared in workflow sessions.`
        );
      }
    }

    visitValue(context.step.on_failure, `${context.path}.on_failure`, (value, path, key) => {
      if (key === "session" && typeof value === "string" && !isDynamicReference(value) && !sessions.has(value)) {
        errors.push({
          code: "workflow.session.undeclared",
          message: `Session "${value}" is referenced but not declared in workflow sessions.`,
          path,
          ...(context.id === undefined ? {} : { stepId: context.id })
        });
      }
    });
  }
}

function validateLoopBounds(contexts: StepContext[], errors: AgentflowWorkflowIssue[]): void {
  for (const context of contexts) {
    if (context.type !== "loop") {
      continue;
    }

    const bounded = ["max_iterations", "max_duration_seconds", "max_duration_minutes"]
      .some((field) => typeof context.step[field] === "number" && Number(context.step[field]) > 0);

    if (!bounded) {
      addStepIssue(
        errors,
        context,
        "workflow.loop.unbounded",
        "max_iterations",
        "Loop must declare a positive max_iterations, max_duration_seconds, or max_duration_minutes bound."
      );
    }
  }
}

function validateControlFlowCycles(
  workflow: AgentflowWorkflow,
  contexts: StepContext[],
  errors: AgentflowWorkflowIssue[]
): void {
  const graph = buildControlFlowGraph(contexts);
  const cyclicContexts = contexts.filter((context) => context.id !== undefined && nodeParticipatesInCycle(context.id, graph));
  const components = cyclicComponents(cyclicContexts, graph).filter((component) =>
    !(workflow.style === "collaborative" && component.some((context) => context.type === "review"))
  );

  const limits = isRecord(workflow.limits) ? workflow.limits : undefined;
  const globallyBounded = typeof limits?.max_recovery_cycles === "number" && limits.max_recovery_cycles > 0;

  for (const component of globallyBounded ? [] : components) {
    const locallyBounded = contexts.some((boundContext) => {
      const boundFields = boundContext.type === "loop"
        ? ["max_iterations", "max_duration_seconds", "max_duration_minutes"]
        : boundContext.type === "review"
          ? ["max_cycles"]
          : [];
      const hasBound = boundFields
        .some((field) => typeof boundContext.step[field] === "number" && Number(boundContext.step[field]) > 0);
      if (!hasBound) {
        return false;
      }

      return boundContext.type === "loop"
        ? component.every((context) =>
            context.path === boundContext.path || context.path.startsWith(`${boundContext.path}.`)
          )
        : component.some((context) => context.path === boundContext.path);
    });

    if (locallyBounded) {
      continue;
    }

    errors.push({
      code: "workflow.control_flow.cycle.unbounded",
      message: `Control-flow cycle involving ${component.map((context) => `"${context.id}"`).join(", ")} needs a positive limits.max_recovery_cycles or step-level bound.`,
      path: "limits.max_recovery_cycles"
    });
  }
}

function validateControlStepShapes(
  workflow: AgentflowWorkflow,
  contexts: StepContext[],
  errors: AgentflowWorkflowIssue[]
): void {
  const sessions = new Set(Object.keys(workflow.sessions ?? {}));

  for (const context of contexts) {
    if (context.type === "condition" && Array.isArray(context.step.branches)) {
      context.step.branches.forEach((branch, index) => {
        const path = `${context.path}.branches[${index}]`;

        if (!isRecord(branch)) {
          errors.push(controlShapeIssue(context, path, "Condition branch entries must be mappings."));
          return;
        }

        if (nonEmptyString(branch.if) === undefined || nonEmptyString(branch.then) === undefined) {
          errors.push(controlShapeIssue(context, path, "Condition branches must declare non-empty if and then fields."));
        }
      });
    }

    if (context.type === "parallel" && Array.isArray(context.step.branches)) {
      const branchIds = new Set<string>();

      context.step.branches.forEach((branch, index) => {
        const path = `${context.path}.branches[${index}]`;

        if (!isRecord(branch)) {
          errors.push(controlShapeIssue(context, path, "Parallel branch entries must be mappings."));
          return;
        }

        const branchId = nonEmptyString(branch.id);
        const session = nonEmptyString(branch.session);

        if (branchId === undefined || session === undefined) {
          errors.push(controlShapeIssue(context, path, "Parallel branches must declare non-empty id and session fields."));
        } else {
          if (branchIds.has(branchId)) {
            errors.push({
              code: "workflow.parallel.branch.id.duplicate",
              message: `Parallel branch id "${branchId}" is declared more than once.`,
              path: `${path}.id`,
              ...(context.id === undefined ? {} : { stepId: context.id })
            });
          } else {
            branchIds.add(branchId);
          }
        }

        if (session !== undefined && isDynamicReference(session)) {
          errors.push({
            code: "workflow.parallel.session.dynamic",
            message: "Parallel branches must use a declared static session so writer authority can be validated.",
            path: `${path}.session`,
            ...(context.id === undefined ? {} : { stepId: context.id })
          });
        } else if (session !== undefined && !sessions.has(session)) {
          errors.push({
            code: "workflow.session.undeclared",
            message: `Session "${session}" is referenced but not declared in workflow sessions.`,
            path: `${path}.session`,
            ...(context.id === undefined ? {} : { stepId: context.id })
          });
        }

        validateArtifactFieldShapes({
          step: branch as AgentflowWorkflowStep,
          path,
          depth: context.depth + 1,
          id: branchId,
          type: undefined
        }, errors);
      });
    }
  }
}

function controlShapeIssue(context: StepContext, path: string, message: string): AgentflowWorkflowIssue {
  return {
    code: "workflow.step.control.shape",
    message,
    path,
    ...(context.id === undefined ? {} : { stepId: context.id })
  };
}

function validateInputReferences(workflow: AgentflowWorkflow, errors: AgentflowWorkflowIssue[]): void {
  const inputs = new Set(Object.keys(workflow.inputs ?? {}));
  const seen = new Set<string>();

  visitValue(workflow, "", (value, path) => {
    if (typeof value !== "string") {
      return;
    }

    const expressionValues = isWorkflowExpressionPath(path)
      ? [value]
      : Array.from(value.matchAll(/\{\{([\s\S]*?)\}\}/g), (match) => match[1]);

    for (const match of expressionValues.flatMap((expression) =>
      Array.from(expression.matchAll(/\binputs\.([A-Za-z_][A-Za-z0-9_-]*)\b/g)))) {
      const name = match[1];
      const key = `${path}:${name}`;

      if (!inputs.has(name) && !seen.has(key)) {
        seen.add(key);
        errors.push({
          code: "workflow.input.undeclared",
          message: `Input "${name}" is referenced but not declared in workflow inputs.`,
          path
        });
      }
    }
  });
}

function isWorkflowExpressionPath(path: string): boolean {
  return /(?:^|\.)(?:if|until|condition|when|while)$/.test(path) ||
    /(?:^|\.)short_circuit_if\[\d+\]$/.test(path);
}

function validateArtifactOutputs(
  workflow: AgentflowWorkflow,
  contexts: StepContext[],
  errors: AgentflowWorkflowIssue[]
): void {
  if (workflow.style !== "pipeline") {
    return;
  }

  const outputs = new Map<string, StepContext>();

  for (const context of contexts) {
    for (const { output, outputContext } of stepOutputContexts(context)) {
      const outputKey = normalizeArtifactPath(output);
      const previous = outputs.get(outputKey);

      if (previous && outputContext.step.overwrite !== true) {
        addStepIssue(
          errors,
          outputContext,
          "workflow.artifact.output.collision",
          outputField(outputContext.step),
          `Artifact "${output}" is already produced by step "${previous.id ?? previous.path}"; declare overwrite: true or use a distinct output.`
        );
      } else {
        outputs.set(outputKey, outputContext);
      }
    }
  }
}

function validateApprovals(contexts: StepContext[], errors: AgentflowWorkflowIssue[]): void {
  for (const context of contexts) {
    if (context.type !== "manual_gate") {
      continue;
    }

    const options = stringList(context.step.options);
    const hasEscape = options.some((option) => ["cancel", "pause", "reject"].includes(option));

    if (!hasEscape) {
      addStepIssue(
        errors,
        context,
        "workflow.approval.deadlock",
        "options",
        "Manual gate needs a pause, cancel, or reject outcome so the workflow cannot wait forever."
      );
    }
  }
}

function validateParallelWriters(
  workflow: AgentflowWorkflow,
  contexts: StepContext[],
  errors: AgentflowWorkflowIssue[]
): void {
  for (const context of contexts) {
    if (context.type !== "parallel") {
      continue;
    }

    const children = parallelChildren(context);

    if (children.length === 0) {
      continue;
    }

    const outputs = new Set<string>();
    const scopedBranches: Array<{ id: string; writers: WriterScope[]; field: string }> = [];
    const writerFields: string[] = [];
    let writerBranchCount = 0;

    for (const { child, path, field } of children) {
      for (const output of new Set(nestedStepOutputs(child as AgentflowWorkflowStep))) {
        const outputKey = normalizeArtifactPath(output);

        if (outputs.has(outputKey) && !parallelOverlapAllowed(context.step)) {
          addStepIssue(
            errors,
            context,
            "workflow.parallel.output.overlap",
            field,
            `Parallel branches both write artifact "${output}" without allow_overlap: true and a conflict policy.`
          );
        }

        outputs.add(outputKey);
      }

      validateParallelFileScopeEntries(child as AgentflowWorkflowStep, path, context, errors);
      const writerScopes = collectWriterScopes(child as AgentflowWorkflowStep, workflow, path);

      if (writerScopes.length > 0) {
        writerBranchCount += 1;
        writerFields.push(field);
      }

      for (const writer of writerScopes) {
        if (writer.includeLayers.length === 0) {
          errors.push({
            code: "workflow.parallel.file_scope.required",
            message: `Parallel writer session "${writer.session}" requires a non-empty effective file_scope.include list from policies.file_scope, the session, or the parallel operation.`,
            path: `${writer.path}.file_scope.include`,
            ...(context.id === undefined ? {} : { stepId: context.id })
          });
        }
      }

      if (writerScopes.some((writer) => writer.includeLayers.length > 0)) {
        scopedBranches.push({ id: nonEmptyString(child.id) ?? "unnamed", writers: writerScopes, field });
      }
    }

    if (workflow.style === "pipeline" && writerBranchCount > 1) {
      addStepIssue(
        errors,
        context,
        "workflow.pipeline.parallel_writers",
        writerFields.at(-1) ?? "branches",
        "Pipeline workflows cannot run multiple file-writing sessions in parallel."
      );
    }

    for (let leftIndex = 0; leftIndex < scopedBranches.length; leftIndex += 1) {
      for (let rightIndex = leftIndex + 1; rightIndex < scopedBranches.length; rightIndex += 1) {
        const left = scopedBranches[leftIndex];
        const right = scopedBranches[rightIndex];
        const overlap = firstWriterScopeOverlap(left.writers, right.writers);

        if (overlap && !parallelOverlapAllowed(context.step)) {
          addStepIssue(
            errors,
            context,
            "workflow.parallel.file_scope.overlap",
            right.field,
            `Parallel branches "${left.id}" and "${right.id}" have overlapping file scopes (${overlap[0]} and ${overlap[1]}).`
          );
        }
      }
    }
  }
}

function validateParallelFileScopeEntries(
  step: AgentflowWorkflowStep,
  path: string,
  parallel: StepContext,
  errors: AgentflowWorkflowIssue[]
): void {
  if (step.file_scope !== undefined && !isRecord(step.file_scope)) {
    errors.push({
      code: "workflow.parallel.file_scope.invalid",
      message: "Parallel file_scope must be a mapping.",
      path: `${path}.file_scope`,
      ...(parallel.id === undefined ? {} : { stepId: parallel.id })
    });
  } else if (isRecord(step.file_scope) && step.file_scope.include !== undefined) {
    const include = step.file_scope.include;

    if (!Array.isArray(include) || !include.every((entry) => nonEmptyString(entry) !== undefined)) {
      errors.push({
        code: "workflow.parallel.file_scope.invalid",
        message: "Parallel file_scope.include must be a list of non-empty strings.",
        path: `${path}.file_scope.include`,
        ...(parallel.id === undefined ? {} : { stepId: parallel.id })
      });
    } else {
      validateRepoRelativeScopePatterns(
        include as string[],
        `${path}.file_scope.include`,
        errors,
        parallel.id
      );
    }
  }

  for (const field of nestedExecutionFields(step)) {
    const nested = step[field];

    if (Array.isArray(nested)) {
      nested.forEach((entry, index) => {
        if (isRecord(entry)) {
          validateParallelFileScopeEntries(
            entry as AgentflowWorkflowStep,
            `${path}.${field}[${index}]`,
            parallel,
            errors
          );
        }
      });
    }
  }

  const failureRoute = isRecord(step.on_failure) && isRecord(step.on_failure.route_to)
    ? step.on_failure.route_to
    : undefined;
  if (failureRoute !== undefined) {
    validateParallelFileScopeEntries(
      failureRoute as AgentflowWorkflowStep,
      `${path}.on_failure.route_to`,
      parallel,
      errors
    );
  }
}

function collectWriterScopes(
  step: AgentflowWorkflowStep,
  workflow: AgentflowWorkflow,
  path: string,
  inheritedScopes: AgentflowYamlMapping[] = []
): WriterScope[] {
  const ownScope = isRecord(step.file_scope) ? step.file_scope : undefined;
  const operationScopes = ownScope === undefined ? inheritedScopes : [...inheritedScopes, ownScope];
  const session = nonEmptyString(step.session);
  const sessionDefinition = session === undefined ? undefined : workflow.sessions?.[session];
  const sessionScope = isRecord(sessionDefinition) && isRecord(sessionDefinition.file_scope)
    ? sessionDefinition.file_scope
    : undefined;
  const globalScope = isRecord(workflow.policies) && isRecord(workflow.policies.file_scope)
    ? workflow.policies.file_scope
    : undefined;
  const scopes = [globalScope, sessionScope, ...operationScopes].filter(
    (scope): scope is AgentflowYamlMapping => scope !== undefined
  );
  const includeLayers = scopes.map((scope) => stringList(scope.include)).filter((layer) => layer.length > 0);
  const exclusions = scopes.flatMap((scope) => stringList(scope.exclude));
  const writers = session !== undefined && sessionCanModifyFiles(workflow.sessions?.[session])
    ? [{ session, includeLayers, exclusions, path }]
    : [];

  for (const field of nestedExecutionFields(step)) {
    const nested = step[field];
    if (Array.isArray(nested)) {
      nested.forEach((entry, index) => {
        if (isRecord(entry)) {
          writers.push(...collectWriterScopes(
            entry as AgentflowWorkflowStep,
            workflow,
            `${path}.${field}[${index}]`,
            operationScopes
          ));
        }
      });
    }
  }

  const failureRoute = isRecord(step.on_failure) && isRecord(step.on_failure.route_to)
    ? step.on_failure.route_to
    : undefined;
  if (failureRoute !== undefined) {
    writers.push(...collectWriterScopes(
      failureRoute as AgentflowWorkflowStep,
      workflow,
      `${path}.on_failure.route_to`,
      operationScopes
    ));
  }

  return writers;
}

function parallelChildren(context: StepContext): Array<{ child: AgentflowYamlMapping; path: string; field: string }> {
  const children: Array<{ child: AgentflowYamlMapping; path: string; field: string }> = [];

  for (const field of ["branches", "body", "steps"]) {
    const entries = context.step[field];

    if (!Array.isArray(entries)) {
      continue;
    }

    entries.forEach((entry, index) => {
      if (isRecord(entry)) {
        children.push({ child: entry, path: `${context.path}.${field}[${index}]`, field });
      }
    });
  }

  return children;
}

function validateCollaborativeReviewBounds(
  workflow: AgentflowWorkflow,
  contexts: StepContext[],
  errors: AgentflowWorkflowIssue[]
): void {
  if (workflow.style !== "collaborative" || !contexts.some((context) => context.type === "review")) {
    return;
  }

  const graph = buildControlFlowGraph(contexts);
  const hasBackEdge = contexts.some((context) =>
    context.type === "review" && context.id !== undefined && nodeParticipatesInCycle(context.id, graph)
  );

  if (!hasBackEdge) {
    return;
  }

  const collaboration = isRecord(workflow.collaboration) ? workflow.collaboration : undefined;

  if (!(typeof collaboration?.max_review_cycles === "number" && collaboration.max_review_cycles > 0)) {
    errors.push({
      code: "workflow.collaboration.review_cycles.unbounded",
      message: "Collaborative review cycles must declare a positive collaboration.max_review_cycles bound.",
      path: "collaboration.max_review_cycles"
    });
  }
}

function lintFrontierBudgets(workflow: AgentflowWorkflow, warnings: AgentflowWorkflowIssue[]): void {
  const frontierSessions = Object.entries(workflow.sessions ?? {})
    .filter(([, session]) => isRecord(session) && session.provider === "frontier")
    .map(([name]) => name);
  const limits = isRecord(workflow.limits) ? workflow.limits : undefined;

  if (frontierSessions.length > 0 && !(typeof limits?.max_frontier_calls === "number" && limits.max_frontier_calls > 0)) {
    warnings.push({
      code: "workflow.lint.frontier.unbounded",
      message: `Frontier sessions (${frontierSessions.join(", ")}) are configured without a positive limits.max_frontier_calls budget.`,
      path: "limits.max_frontier_calls"
    });
  }
}

function lintReviewCycles(workflow: AgentflowWorkflow, warnings: AgentflowWorkflowIssue[]): void {
  const collaboration = isRecord(workflow.collaboration) ? workflow.collaboration : undefined;

  if (typeof collaboration?.max_review_cycles === "number" && collaboration.max_review_cycles > 5) {
    warnings.push({
      code: "workflow.lint.review_cycles.high",
      message: "Collaborative review cycles above 5 are difficult to reason about and can consume excessive model budget.",
      path: "collaboration.max_review_cycles"
    });
  }
}

function lintCommands(contexts: StepContext[], warnings: AgentflowWorkflowIssue[]): void {
  for (const context of contexts) {
    const riskyReason = context.type === "command" && typeof context.step.command === "string"
      ? riskyShellReason(context.step.command)
      : undefined;

    if (riskyReason) {
      addStepIssue(
        warnings,
        context,
        "workflow.lint.command.risky",
        "command",
        `Command needs explicit review because it ${riskyReason}.`
      );
    }

    const modelFacing = ["challenge", "consult", "handoff", "review", "session_request"].includes(context.type ?? "");

    if (context.type !== "command" && !modelFacing) {
      continue;
    }

    const secretCandidates = [
      ...nestedStrings(context.step.inputs).map((value) => ({ value, field: "inputs" })),
      ...(modelFacing
        ? nestedStrings(context.step.artifacts).map((value) => ({ value, field: "artifacts" }))
        : []),
      ...(context.type === "session_request" && nonEmptyString(context.step.prompt) !== undefined
        ? [{ value: String(context.step.prompt), field: "prompt" }]
        : []),
      ...(["challenge", "consult"].includes(context.type ?? "") && nonEmptyString(context.step.question) !== undefined
        ? [{ value: String(context.step.question), field: "question" }]
        : [])
    ];

    for (const { value: input, field } of secretCandidates) {
      if (SECRET_PATH.test(input)) {
        addStepIssue(
          warnings,
          context,
          "workflow.lint.secret.input",
          field,
          `Input "${input}" looks secret-bearing and should not be passed to a command or model without redaction.`
        );
      }
    }
  }
}

function lintComplexity(contexts: StepContext[], warnings: AgentflowWorkflowIssue[]): void {
  if (contexts.length > 12) {
    warnings.push({
      code: "workflow.lint.steps.complex",
      message: `Workflow contains ${contexts.length} steps; consider extracting a nested workflow or simplifying branches.`,
      path: "steps"
    });
  }

  const maxDepth = contexts.reduce((depth, context) => Math.max(depth, context.depth), 0);

  if (maxDepth > 2) {
    warnings.push({
      code: "workflow.lint.nesting.complex",
      message: `Workflow nesting depth is ${maxDepth}; deeply nested control flow is difficult to inspect and recover.`,
      path: "steps"
    });
  }
}

function lintArtifactOverwrites(contexts: StepContext[], warnings: AgentflowWorkflowIssue[]): void {
  const producers = new Map<string, StepContext[]>();
  const seenOutputs = new Map<string, StepContext>();
  const dominators = controlFlowDominators(contexts);

  for (const context of contexts) {
    for (const { output, outputContext } of stepOutputContexts(context)) {
      const outputKey = normalizeArtifactPath(output);
      producers.set(outputKey, [...(producers.get(outputKey) ?? []), outputContext]);
    }
  }

  for (const context of contexts) {
    lintDirectParallelBranchInputs(context, contexts, producers, dominators, warnings);

    for (const input of stepInputs(context.step)) {
      const inputKey = normalizeArtifactPath(input);
      const available = (producers.get(inputKey) ?? []).some((producer) =>
        producer.path !== context.path && artifactProducerAvailable(producer, context, contexts, dominators)
      );

      if (!isDynamicReference(input) && !available) {
        addStepIssue(
          warnings,
          context,
          "workflow.lint.artifact.read_before_write",
          inputField(context.step),
          `Artifact "${input}" is read before any step produces it.`
        );
      }
    }

    for (const { output, outputContext } of stepOutputContexts(context)) {
      const outputKey = normalizeArtifactPath(output);
      const previous = seenOutputs.get(outputKey);

      if (previous && outputContext.step.overwrite !== true) {
        addStepIssue(
          warnings,
          outputContext,
          "workflow.lint.artifact.overwrite",
          outputField(outputContext.step),
          `Artifact "${output}" is produced more than once; declare overwrite intent and approval invalidation where appropriate.`
        );
      }

      seenOutputs.set(outputKey, outputContext);
    }
  }
}

function lintDirectParallelBranchInputs(
  context: StepContext,
  contexts: StepContext[],
  producers: Map<string, StepContext[]>,
  dominators: Map<string, Set<string>>,
  warnings: AgentflowWorkflowIssue[]
): void {
  if (context.type !== "parallel" || !Array.isArray(context.step.branches)) {
    return;
  }

  context.step.branches.forEach((branch, index) => {
    if (!isRecord(branch)) {
      return;
    }

    const branchContext: StepContext = {
      step: branch as AgentflowWorkflowStep,
      path: `${context.path}.branches[${index}]`,
      depth: context.depth + 1,
      id: nonEmptyString(branch.id),
      type: undefined
    };

    for (const input of stepInputs(branchContext.step)) {
      const available = (producers.get(normalizeArtifactPath(input)) ?? []).some((producer) =>
        producer.path !== branchContext.path && artifactProducerAvailable(producer, branchContext, contexts, dominators)
      );

      if (!isDynamicReference(input) && !available) {
        addStepIssue(
          warnings,
          branchContext,
          "workflow.lint.artifact.read_before_write",
          inputField(branchContext.step),
          `Artifact "${input}" is read before any step produces it.`
        );
      }
    }
  });
}

function stepOutputContexts(context: StepContext): Array<{ output: string; outputContext: StepContext }> {
  const outputs = stepOutputs(context.step).map((output) => ({ output, outputContext: context }));

  if (context.type !== "parallel" || !Array.isArray(context.step.branches)) {
    return outputs;
  }

  context.step.branches.forEach((branch, index) => {
    if (!isRecord(branch)) {
      return;
    }

    const outputContext: StepContext = {
      step: branch as AgentflowWorkflowStep,
      path: `${context.path}.branches[${index}]`,
      depth: context.depth + 1,
      id: nonEmptyString(branch.id),
      type: undefined
    };

    outputs.push(...stepOutputs(outputContext.step).map((output) => ({ output, outputContext })));
  });

  return outputs;
}

function artifactProducerAvailable(
  producer: StepContext,
  consumer: StepContext,
  contexts: StepContext[],
  dominators: Map<string, Set<string>>
): boolean {
  const producerMemberships = parallelBranchMembership(producer.path, contexts);
  const consumerMemberships = parallelBranchMembership(consumer.path, contexts);
  const consumerMembership = new Map(consumerMemberships
    .map(({ parallelPath, branchIndex }) => [parallelPath, branchIndex]));

  const sameExecutionBranch = producerMemberships.every(({ parallelPath, branchIndex }) => {
    const consumerBranch = consumerMembership.get(parallelPath);
    return consumerBranch === undefined || consumerBranch === branchIndex;
  });

  if (!sameExecutionBranch) {
    return false;
  }

  const producerId = artifactControlFlowAnchor(producer, producerMemberships, consumerMemberships, dominators);
  const consumerId = artifactControlFlowAnchor(consumer, consumerMemberships, producerMemberships, dominators);

  if (producerId !== undefined && consumerId !== undefined) {
    return dominators.get(consumerId)?.has(producerId) === true;
  }

  return true;
}

interface ParallelBranchMembership {
  parallelPath: string;
  parallelId?: string;
  branchIndex: number;
}

function parallelBranchMembership(path: string, contexts: StepContext[]): ParallelBranchMembership[] {
  const memberships: ParallelBranchMembership[] = [];

  for (const parallel of contexts.filter((context) => context.type === "parallel")) {
    for (const field of ["branches", "body", "steps"]) {
      const prefix = `${parallel.path}.${field}[`;
      if (!path.startsWith(prefix)) {
        continue;
      }

      const match = /^(\d+)]/.exec(path.slice(prefix.length));
      if (match !== null) {
        memberships.push({ parallelPath: parallel.path, parallelId: parallel.id, branchIndex: Number(match[1]) });
      }
    }
  }

  return memberships;
}

function artifactControlFlowAnchor(
  context: StepContext,
  memberships: ParallelBranchMembership[],
  otherMemberships: ParallelBranchMembership[],
  dominators: Map<string, Set<string>>
): string | undefined {
  if (context.id !== undefined && dominators.has(context.id)) {
    const leavesParallel = [...memberships].reverse().find((membership) =>
      !otherMemberships.some((other) => other.parallelPath === membership.parallelPath)
    );
    return leavesParallel?.parallelId ?? context.id;
  }

  return memberships.at(-1)?.parallelId ?? context.id;
}

function sessionReferenceFields(type: string | undefined): string[] {
  switch (type) {
    case "session_request":
      return ["session"];
    case "consult":
    case "challenge":
    case "handoff":
      return ["from", "to"];
    case "review":
      return ["reviewer", "subject"];
    case "approval":
      return ["reviewer"];
    case "decision_record":
      return ["owner"];
    default:
      return [];
  }
}

function stepInputs(step: AgentflowWorkflowStep): string[] {
  const inputs = nestedStrings(step.inputs);
  const input = nonEmptyString(step.input);
  const artifacts = ["approval", "decision_record", "review"].includes(String(step.type))
    ? stringList(step.artifacts)
    : [];
  return [...inputs, ...(input === undefined ? [] : [input]), ...artifacts];
}

function stepOutputs(step: AgentflowWorkflowStep): string[] {
  const outputs = stringList(step.outputs);
  const output = nonEmptyString(step.output);
  const saveAs = step.type === "input_request" ? nonEmptyString(step.save_as) : undefined;
  return [...outputs, ...(output === undefined ? [] : [output]), ...(saveAs === undefined ? [] : [saveAs])];
}

function nestedStepOutputs(step: AgentflowWorkflowStep): string[] {
  const nested = nestedExecutionFields(step).map((field) => step[field]).flatMap((entries) => Array.isArray(entries)
    ? entries.filter(isRecord).flatMap((entry) => nestedStepOutputs(entry as AgentflowWorkflowStep))
    : []);
  return [...stepOutputs(step), ...nested];
}

function nestedExecutionFields(step: AgentflowWorkflowStep): string[] {
  return step.type === "parallel" ? ["branches", "body", "steps"] : ["body", "steps"];
}

function inputField(step: AgentflowWorkflowStep): string {
  if (nonEmptyString(step.input) !== undefined) {
    return "input";
  }
  return ["approval", "decision_record", "review"].includes(String(step.type)) && stringList(step.artifacts).length > 0
    ? "artifacts"
    : "inputs";
}

function outputField(step: AgentflowWorkflowStep): string {
  if (nonEmptyString(step.output) !== undefined) {
    return "output";
  }
  return step.type === "input_request" && nonEmptyString(step.save_as) !== undefined ? "save_as" : "outputs";
}

function directTargets(step: AgentflowWorkflowStep): string[] {
  return stepTargetReferences(step, "").map((target) => target.value);
}

function buildControlFlowGraph(contexts: StepContext[]): Map<string, string[]> {
  const idCounts = new Map<string, number>();
  contexts.forEach((context) => {
    if (context.id !== undefined) {
      idCounts.set(context.id, (idCounts.get(context.id) ?? 0) + 1);
    }
  });
  const ids = new Set([...idCounts].filter(([, count]) => count === 1).map(([id]) => id));
  const siblings = new Map<string, Array<{ index: number; id: string }>>();

  for (const context of contexts) {
    const match = /^(.*)\[(\d+)]$/.exec(context.path);

    if (match && context.id !== undefined && ids.has(context.id)) {
      const entries = siblings.get(match[1]) ?? [];
      entries.push({ index: Number(match[2]), id: context.id });
      siblings.set(match[1], entries);
    }
  }

  const fallthrough = new Map<string, string>();
  for (const entries of siblings.values()) {
    entries.sort((left, right) => left.index - right.index);
    entries.slice(0, -1).forEach((entry, index) => fallthrough.set(entry.id, entries[index + 1].id));
  }

  return new Map(contexts.flatMap((context) => {
    if (context.id === undefined || !ids.has(context.id)) {
      return [];
    }

    const nestedEntries = nestedStepLists(context.step, context.path).flatMap(({ entries }) => {
      const first = entries.find(isRecord);
      const id = first === undefined ? undefined : nonEmptyString(first.id);
      return id !== undefined && ids.has(id) ? [id] : [];
    });
    const explicit = [...directTargets(context.step).filter((target) => ids.has(target)), ...nestedEntries];
    const next = !hasPrimaryControlTarget(context.step) && context.type !== "result" ? fallthrough.get(context.id) : undefined;
    return [[context.id, [...new Set(next === undefined ? explicit : [...explicit, next])]] as const];
  }));
}

function controlFlowDominators(contexts: StepContext[]): Map<string, Set<string>> {
  const graph = buildControlFlowGraph(contexts);
  const entry = contexts.find((context) => context.depth === 0 && context.id !== undefined && graph.has(context.id))?.id;

  if (entry === undefined) {
    return new Map();
  }

  const reachable = new Set<string>();
  const pending = [entry];

  while (pending.length > 0) {
    const current = pending.pop();
    if (current === undefined || reachable.has(current)) {
      continue;
    }
    reachable.add(current);
    pending.push(...(graph.get(current) ?? []));
  }

  const predecessors = new Map<string, Set<string>>([...reachable].map((node) => [node, new Set<string>()]));
  for (const [source, targets] of graph) {
    if (!reachable.has(source)) {
      continue;
    }
    for (const target of targets) {
      if (reachable.has(target)) {
        predecessors.get(target)?.add(source);
      }
    }
  }

  const dominators = new Map<string, Set<string>>([...reachable].map((node) => [
    node,
    node === entry ? new Set([entry]) : new Set(reachable)
  ]));
  let changed = true;

  while (changed) {
    changed = false;

    for (const node of reachable) {
      if (node === entry) {
        continue;
      }

      const nodePredecessors = [...(predecessors.get(node) ?? [])];
      const intersection = nodePredecessors.length === 0
        ? new Set<string>()
        : new Set([...reachable].filter((candidate) =>
          nodePredecessors.every((predecessor) => dominators.get(predecessor)?.has(candidate) === true)
        ));
      intersection.add(node);

      const previous = dominators.get(node) ?? new Set<string>();
      if (previous.size !== intersection.size || [...previous].some((candidate) => !intersection.has(candidate))) {
        dominators.set(node, intersection);
        changed = true;
      }
    }
  }

  return dominators;
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

  if (["goto", "on_approve", "return_to", "then"].some((field) => nonEmptyString(step[field]) !== undefined)) {
    return true;
  }

  return false;
}

function nodeParticipatesInCycle(node: string, graph: Map<string, string[]>): boolean {
  const pending = [...(graph.get(node) ?? [])];
  return nodeReachesFromPending(node, pending, graph);
}

function cyclicComponents(contexts: StepContext[], graph: Map<string, string[]>): StepContext[][] {
  const remaining = new Set(contexts.flatMap((context) => context.id === undefined ? [] : [context.id]));
  const components: StepContext[][] = [];

  for (const context of contexts) {
    if (context.id === undefined || !remaining.has(context.id)) {
      continue;
    }

    const component = contexts.filter((candidate) => candidate.id !== undefined && remaining.has(candidate.id) &&
      nodeReaches(context.id as string, candidate.id as string, graph) &&
      nodeReaches(candidate.id as string, context.id as string, graph));
    component.forEach((candidate) => remaining.delete(candidate.id as string));
    components.push(component);
  }

  return components;
}

function nodeReaches(source: string, target: string, graph: Map<string, string[]>): boolean {
  return source === target || nodeReachesFromPending(target, [...(graph.get(source) ?? [])], graph);
}

function nodeReachesFromPending(target: string, pending: string[], graph: Map<string, string[]>): boolean {
  const visited = new Set<string>();

  while (pending.length > 0) {
    const current = pending.pop();

    if (current === target) {
      return true;
    }

    if (current === undefined || visited.has(current)) {
      continue;
    }

    visited.add(current);
    pending.push(...(graph.get(current) ?? []));
  }

  return false;
}

function firstWriterScopeOverlap(left: WriterScope[], right: WriterScope[]): [string, string] | undefined {
  for (const leftWriter of left) {
    for (const rightWriter of right) {
      if (policyGlobLayersHaveWritablePath(
        [...leftWriter.includeLayers, ...rightWriter.includeLayers],
        [...leftWriter.exclusions, ...rightWriter.exclusions]
      )) {
        return [leftWriter.includeLayers.at(-1)?.[0] ?? "**", rightWriter.includeLayers.at(-1)?.[0] ?? "**"];
      }
    }
  }
  return undefined;
}

function validateRepoRelativeScopePatterns(
  patterns: string[],
  path: string,
  errors: AgentflowWorkflowIssue[],
  stepId?: string
): void {
  patterns.forEach((pattern, index) => {
    if (scopePatternEscapesRepo(pattern)) {
      errors.push({
        code: "workflow.parallel.file_scope.invalid",
        message: `File scope pattern "${pattern}" must be repo-relative and stay within the repository.`,
        path: `${path}[${index}]`,
        ...(stepId === undefined ? {} : { stepId })
      });
    }
  });
}

function scopePatternEscapesRepo(pattern: string): boolean {
  const normalized = pattern.trim().replaceAll("\\", "/");

  if (normalized.startsWith("/") || /^[A-Za-z]:/.test(normalized)) {
    return true;
  }

  const segments: string[] = [];

  for (const segment of normalized.split("/")) {
    if (segment.length === 0 || segment === ".") {
      continue;
    }

    if (segment !== "..") {
      segments.push(segment);
      continue;
    }

    const parent = segments.at(-1);

    if (parent === undefined || hasGlob(parent)) {
      return true;
    }

    segments.pop();
  }

  return false;
}

function hasGlob(pattern: string): boolean {
  return /[?*{\[]/.test(pattern);
}

function normalizeScopePattern(pattern: string): string {
  const segments: string[] = [];

  for (const segment of pattern.replaceAll("\\", "/").split("/")) {
    if (segment.length === 0 || segment === ".") {
      continue;
    }

    if (segment === ".." && segments.length > 0 && segments.at(-1) !== "**") {
      segments.pop();
    } else {
      segments.push(segment);
    }
  }

  return segments.join("/");
}


function parallelOverlapAllowed(step: AgentflowWorkflowStep): boolean {
  const conflictPolicy = step.conflict_policy;
  const configuredPolicy = nonEmptyString(conflictPolicy) !== undefined ||
    (isRecord(conflictPolicy) && Object.keys(conflictPolicy).length > 0);
  return step.allow_overlap === true && configuredPolicy;
}

function sessionCanModifyFiles(value: AgentflowYamlValue | undefined): boolean {
  return isRecord(value) && isRecord(value.authority) && value.authority.can_modify_files === true;
}

function destructiveRmReason(command: string, budget = SHELL_ANALYSIS_BUDGET): string | undefined {
  if (budget <= 0) {
    return "a command nested beyond the safety analysis budget";
  }
  command = stripHeredocBodies(removeShellLineContinuations(command));

  for (const payload of shellSubstitutionPayloads(command)) {
    const reason = destructiveRmReason(payload, remainingShellBudget(budget, payload));
    if (reason !== undefined) {
      return reason;
    }
  }

  for (const segment of splitUnquoted(command, new Set(["\n", "\r", ";", "&", "|", "(", ")", "`"]))) {
    const parsed = parseShellSegment(segment);

    const payload = shellCommandPayload(parsed);
    const payloadReason = payload === undefined
      ? undefined
      : destructiveRmReason(payload, remainingShellBudget(budget, payload));

    if (payloadReason !== undefined) {
      return payloadReason;
    }

    if (parsed?.executable !== "rm") {
      continue;
    }

    const operands = parsed.args.filter((token) => token !== "--" && !token.startsWith("-"));
    const recursive = parsed.args.some((token) => token === "--recursive" || /^-[^-]*r/i.test(token));
    const force = parsed.args.some((token) => token === "--force" || /^-[^-]*f/.test(token));

    if (recursive && force && (parsed.wrappers.includes("xargs") || operands.some(isProtectedDeletionPath))) {
      return "recursive deletion of a root or home path";
    }
  }

  return undefined;
}

function unsafeShellReason(command: string, budget = SHELL_ANALYSIS_BUDGET): string | undefined {
  if (budget <= 0) {
    return "a command nested beyond the safety analysis budget";
  }
  command = stripHeredocBodies(removeShellLineContinuations(command));

  for (const payload of shellSubstitutionPayloads(command)) {
    const reason = unsafeShellReason(payload, remainingShellBudget(budget, payload));
    if (reason !== undefined) {
      return reason;
    }
  }

  const segments = splitUnquoted(command, new Set(["\n", "\r", ";", "&", "|", "(", ")", "`"]))
    .map(parseShellSegment).filter((segment) => segment !== undefined);

  for (const segment of segments) {
    const payload = shellCommandPayload(segment);
    const payloadReason = payload === undefined
      ? undefined
      : unsafeShellReason(payload, remainingShellBudget(budget, payload));

    if (payloadReason !== undefined) {
      return payloadReason;
    }

    if (segment.executable === "git" && gitSubcommand(segment.args) === "reset" && segment.args.includes("--hard")) {
      return "destructive Git reset";
    }

    if ([...segment.redirections, ...outputRedirectionTargets(segment.args)]
      .some((target) => target.startsWith("/dev/")) ||
      segment.executable.startsWith("mkfs") ||
      (segment.executable === "dd" && (
        segment.args.some((argument) => /^of=\/dev\//.test(argument)) ||
        outputRedirectionTargets(segment.args).some((target) => target.startsWith("/dev/"))
      ))) {
      return "direct filesystem or device mutation";
    }

    if (segment.executable === "chmod" &&
      segment.args.some((argument) => argument === "--recursive" || /^-[^-]*R/.test(argument)) &&
      segment.args.some((argument) => /^0*777$/.test(argument))) {
      return "recursive world-writable permissions";
    }
  }

  for (const pipeline of shellPipelineGroups(command)) {
    let hasDownloadedInput = false;

    for (const commandPart of pipeline.map(parseShellSegment)) {
      if (commandPart && ["curl", "wget"].includes(commandPart.executable)) {
        hasDownloadedInput = true;
      } else if (commandPart && hasDownloadedInput && SHELL_EXECUTABLES.has(commandPart.executable)) {
        return "piping downloaded content into a shell";
      }
    }
  }

  return undefined;
}

function riskyShellReason(command: string, budget = SHELL_ANALYSIS_BUDGET): string | undefined {
  if (budget <= 0) {
    return "is nested beyond the safety analysis budget";
  }
  command = stripHeredocBodies(removeShellLineContinuations(command));

  for (const payload of shellSubstitutionPayloads(command)) {
    const reason = riskyShellReason(payload, remainingShellBudget(budget, payload));
    if (reason !== undefined) {
      return reason;
    }
  }

  for (const segment of splitUnquoted(command, new Set(["\n", "\r", ";", "&", "|", "(", ")", "`"]))) {
    const parsed = parseShellSegment(segment);
    const payload = shellCommandPayload(parsed);
    const payloadReason = payload === undefined
      ? undefined
      : riskyShellReason(payload, remainingShellBudget(budget, payload));

    if (payloadReason !== undefined) {
      return payloadReason;
    }

    if (parsed?.wrappers.includes("sudo")) {
      return "requests elevated privileges";
    }

    if (parsed?.executable === "rm" && parsed.args.some(isRecursiveRmOption)) {
      return "recursively deletes files";
    }

    if (parsed?.executable === "git" && gitSubcommand(parsed.args) === "push" &&
      parsed.args.some((argument) => argument === "-f" || argument === "--force" || argument.startsWith("--force-with-lease"))) {
      return "force-pushes Git history";
    }
  }

  return undefined;
}

function remainingShellBudget(budget: number, payload: string): number {
  return budget - Math.max(1, payload.length);
}

function isRecursiveRmOption(argument: string): boolean {
  return argument === "--recursive" || (/^-[^-]+$/.test(argument) && /r/i.test(argument.slice(1)));
}

function gitSubcommand(args: string[]): string | undefined {
  const optionsWithValues = new Set(["-C", "-c", "--git-dir", "--work-tree", "--namespace", "--super-prefix", "--config-env"]);

  for (let index = 0; index < args.length; index += 1) {
    const argument = args[index];

    if (optionsWithValues.has(argument)) {
      index += 1;
    } else if (argument.startsWith("-")) {
      continue;
    } else {
      return argument;
    }
  }

  return undefined;
}

function shellSubstitutionPayloads(value: string): string[] {
  const payloads: string[] = [];
  let quote: "'" | '"' | undefined;
  let escaped = false;

  for (let index = 0; index < value.length; index += 1) {
    const character = value[index];

    if (escaped) {
      escaped = false;
      continue;
    }

    if (character === "\\" && quote !== "'") {
      escaped = true;
      continue;
    }

    if (character === "'" && quote !== '"') {
      quote = quote === "'" ? undefined : "'";
      continue;
    }

    if (character === '"' && quote !== "'") {
      quote = quote === '"' ? undefined : '"';
      continue;
    }

    if (quote === "'") {
      continue;
    }

    if (character === "$" && value[index + 1] === "(") {
      const payload = parenthesizedShellPayload(value, index + 2);
      if (payload !== undefined) {
        payloads.push(payload.value);
        index = payload.endIndex;
      }
      continue;
    }

    if (character === "`") {
      const payload = backtickShellPayload(value, index + 1);
      if (payload !== undefined) {
        payloads.push(payload.value);
        index = payload.endIndex;
      }
    }
  }

  return payloads;
}

function parenthesizedShellPayload(
  value: string,
  start: number
): { value: string; endIndex: number } | undefined {
  let depth = 1;
  let quote: "'" | '"' | undefined;
  let escaped = false;

  for (let index = start; index < value.length; index += 1) {
    const character = value[index];

    if (escaped) {
      escaped = false;
    } else if (character === "\\" && quote !== "'") {
      escaped = true;
    } else if (character === "'" && quote !== '"') {
      quote = quote === "'" ? undefined : "'";
    } else if (character === '"' && quote !== "'") {
      quote = quote === '"' ? undefined : '"';
    } else if (quote === undefined && character === "(") {
      depth += 1;
    } else if (quote === undefined && character === ")") {
      depth -= 1;
      if (depth === 0) {
        return { value: value.slice(start, index), endIndex: index };
      }
    }
  }

  return undefined;
}

function backtickShellPayload(value: string, start: number): { value: string; endIndex: number } | undefined {
  let escaped = false;

  for (let index = start; index < value.length; index += 1) {
    const character = value[index];

    if (escaped) {
      escaped = false;
    } else if (character === "\\") {
      escaped = true;
    } else if (character === "`") {
      return { value: value.slice(start, index), endIndex: index };
    }
  }

  return undefined;
}

function stripHeredocBodies(value: string): string {
  const output: string[] = [];
  const pendingDelimiters: Array<{ delimiter: string; quoted: boolean; stripTabs: boolean }> = [];

  for (const line of value.split(/\r?\n/)) {
    const current = pendingDelimiters[0];

    if (current !== undefined) {
      const candidate = current.stripTabs ? line.replace(/^\t+/, "") : line;
      if (candidate.trimEnd() === current.delimiter) {
        pendingDelimiters.shift();
      } else if (!current.quoted) {
        output.push(...shellSubstitutionPayloads(line).map((payload) => `$(${payload})`));
      }
      continue;
    }

    output.push(line);
    for (const match of shellHeredocOpeners(line)) {
      pendingDelimiters.push({
        delimiter: match.delimiter,
        quoted: match.quoted,
        stripTabs: match.stripTabs
      });
    }
  }

  return output.join("\n");
}

function shellHeredocOpeners(line: string): Array<{ delimiter: string; quoted: boolean; stripTabs: boolean }> {
  const openers: Array<{ delimiter: string; quoted: boolean; stripTabs: boolean }> = [];
  let quote: "'" | '"' | undefined;
  let escaped = false;

  for (let index = 0; index < line.length; index += 1) {
    const character = line[index];

    if (escaped) {
      escaped = false;
    } else if (character === "\\") {
      escaped = true;
    } else if (character === "'" || character === '"') {
      quote = quote === undefined ? character : quote === character ? undefined : quote;
    } else if (quote === undefined && line.startsWith("<<", index) && line[index + 2] !== "<") {
      const match = /^<<(\-?)\s*(?:(['"])([^'"\r\n]+)\2|([A-Za-z_][A-Za-z0-9_]*))/.exec(line.slice(index));

      if (match !== null) {
        openers.push({
          delimiter: match[3] ?? match[4],
          quoted: match[2] !== undefined,
          stripTabs: match[1] === "-"
        });
        index += match[0].length - 1;
      }
    }
  }

  return openers;
}

function shellPipelineGroups(value: string): string[][] {
  const groups: string[][] = [];

  for (const commandGroup of splitUnquoted(value, new Set(["\n", "\r", ";", "&", "(", ")", "`"]))) {
    let pipeline: string[] = [];

    for (const part of splitUnquoted(commandGroup, new Set(["|"]))) {
      if (part.trim().length === 0) {
        if (pipeline.length > 0) {
          groups.push(pipeline);
          pipeline = [];
        }
      } else {
        pipeline.push(part);
      }
    }

    if (pipeline.length > 0) {
      groups.push(pipeline);
    }
  }

  return groups;
}

function splitUnquoted(value: string, separators: Set<string>): string[] {
  const segments: string[] = [];
  let current = "";
  let quote: "'" | '"' | undefined;
  let escaped = false;

  for (let index = 0; index < value.length; index += 1) {
    const character = value[index];

    if (escaped) {
      current += character;
      escaped = false;
    } else if (character === "\\" && quote !== "'") {
      current += character;
      escaped = true;
    } else if (character === "'" || character === '"') {
      current += character;
      quote = quote === undefined ? character : quote === character ? undefined : quote;
    } else if (quote === undefined && separators.has(character) &&
      !(character === "&" && (value[index - 1] === ">" || value[index + 1] === ">"))) {
      segments.push(current);
      current = "";
    } else {
      current += character;
    }
  }

  segments.push(current);
  return segments;
}

function removeShellLineContinuations(value: string): string {
  let output = "";
  let quote: "'" | '"' | undefined;

  for (let index = 0; index < value.length; index += 1) {
    const character = value[index];

    if (character === "\\" && quote !== "'" && (value[index + 1] === "\n" ||
      (value[index + 1] === "\r" && value[index + 2] === "\n"))) {
      index += value[index + 1] === "\r" ? 2 : 1;
    } else {
      output += character;
      if (character === "'" || character === '"') {
        quote = quote === undefined ? character : quote === character ? undefined : quote;
      }
    }
  }

  return output;
}

interface ParsedShellCommand {
  executable: string;
  args: string[];
  wrappers: string[];
  redirections: string[];
}

function parseShellSegment(segment: string): ParsedShellCommand | undefined {
  const tokens = shellTokens(segment);
  const wrappers: string[] = [];
  const prefixEnd = skipShellPrefixes(tokens, 0);
  const redirections = outputRedirectionTargets(tokens.slice(0, prefixEnd));
  let index = skipShellControlPrefixes(tokens, prefixEnd);

  let executable = shellBasename(tokens[index]);

  while (["command", "env", "eval", "exec", "ionice", "nice", "nohup", "setsid", "stdbuf", "sudo", "time", "timeout", "xargs"]
    .includes(executable ?? "")) {
    const wrapper = executable;
    if (wrapper === undefined) {
      break;
    }
    wrappers.push(wrapper);
    index += 1;

    if (wrapper === "sudo") {
      const valued = new Set(["-u", "-g", "-h", "-p", "-C", "-T", "-R", "-D", "--user", "--group", "--host", "--prompt", "--chdir", "--role", "--type", "--other-user", "--close-from"]);
      while (tokens[index]?.startsWith("-")) {
        const option = tokens[index];
        index += 1;
        if (valued.has(option)) {
          index += 1;
        }
      }
    } else if (wrapper === "env") {
      const valued = new Set(["-u", "-C", "-a", "-S", "--unset", "--chdir", "--argv0", "--split-string"]);
      while (tokens[index]?.startsWith("-") || isShellAssignment(tokens[index])) {
        const option = tokens[index];

        if (option === "-S" || option === "--split-string") {
          const payload = tokens[index + 1];
          tokens.splice(index, 2, ...shellTokens(payload ?? ""));
          break;
        }

        const inlineSplit = option.startsWith("--split-string=")
          ? option.slice("--split-string=".length)
          : option.startsWith("-S") && option.length > 2
            ? option.slice(2)
            : undefined;

        if (inlineSplit !== undefined) {
          tokens.splice(index, 1, ...shellTokens(inlineSplit));
          break;
        }

        index += 1;
        const optionName = option.split("=", 1)[0];
        if (valued.has(optionName) && !option.includes("=")) {
          index += 1;
        }
      }
    } else if (wrapper === "eval") {
      const payload = tokens.slice(index).join(" ");
      tokens.splice(index, tokens.length - index, ...shellTokens(payload));
    } else if (wrapper === "exec") {
      while (tokens[index]?.startsWith("-")) {
        const option = tokens[index];
        index += 1;
        if (option === "-a") {
          index += 1;
        }
      }
    } else if (["ionice", "nice", "setsid", "stdbuf", "time", "timeout", "xargs"].includes(wrapper)) {
      const valuedOptions: Record<string, Set<string>> = {
        ionice: new Set(["-c", "-n", "-p", "-P", "-u", "--class", "--classdata", "--pid", "--pgid", "--uid"]),
        nice: new Set(["-n", "--adjustment"]),
        setsid: new Set(),
        stdbuf: new Set(["-e", "-i", "-o", "--error", "--input", "--output"]),
        time: new Set(["-f", "-o", "--format", "--output"]),
        timeout: new Set(["-k", "-s", "--kill-after", "--signal"]),
        xargs: new Set(["-a", "-d", "-E", "-I", "-L", "-n", "-P", "-s", "--arg-file", "--delimiter", "--eof", "--replace", "--max-lines", "--max-args", "--max-procs", "--max-chars"])
      };
      index = skipShellOptions(tokens, index, valuedOptions[wrapper]);
      if (wrapper === "timeout" && tokens[index] !== undefined) {
        index += 1;
        if (tokens[index] === "--") {
          index += 1;
        }
      }
    } else {
      while (tokens[index]?.startsWith("-")) {
        index += 1;
      }
    }

    const nestedPrefixEnd = skipShellPrefixes(tokens, index);
    redirections.push(...outputRedirectionTargets(tokens.slice(index, nestedPrefixEnd)));
    index = skipShellControlPrefixes(tokens, nestedPrefixEnd);
    executable = shellBasename(tokens[index]);
  }

  return executable === undefined ? undefined : { executable, args: tokens.slice(index + 1), wrappers, redirections };
}

function skipShellControlPrefixes(tokens: string[], start: number): number {
  let index = start;
  const prefixes = new Set(["!", "{", "do", "elif", "else", "if", "then", "until", "while"]);

  while (prefixes.has(tokens[index]?.toLowerCase() ?? "")) {
    index += 1;
  }

  return index;
}

function skipShellOptions(tokens: string[], start: number, valued: Set<string>): number {
  let index = start;

  while (tokens[index]?.startsWith("-")) {
    const option = tokens[index];
    index += 1;

    if (option === "--") {
      break;
    }

    const optionName = option.split("=", 1)[0];
    if (valued.has(optionName) && !option.includes("=")) {
      index += 1;
    }
  }

  return index;
}

function skipShellPrefixes(tokens: string[], start: number): number {
  let index = start;

  while (index < tokens.length) {
    if (isShellAssignment(tokens[index])) {
      index += 1;
      continue;
    }

    const redirectionWidth = shellRedirectionWidth(tokens[index]);
    if (redirectionWidth > 0) {
      index += redirectionWidth;
      continue;
    }

    break;
  }

  return index;
}

function shellRedirectionWidth(token: string | undefined): number {
  const match = /^(?:\d*|&)(?:<<-?|<<<|>>?|<>|<&|>&)(.*)$/.exec(token ?? "");
  return match === null ? 0 : match[1].length === 0 ? 2 : 1;
}

function outputRedirectionTargets(args: string[]): string[] {
  const targets: string[] = [];

  for (let index = 0; index < args.length; index += 1) {
    const match = /^(?:\d*|&)(?:>&|>>?|<>)(.*)$/.exec(args[index]);
    if (match === null) {
      continue;
    }

    const target = match[1].length > 0 ? match[1] : args[index + 1];
    if (target !== undefined) {
      targets.push(target);
    }
  }

  return targets;
}

function shellCommandPayload(command: ParsedShellCommand | undefined): string | undefined {
  if (command === undefined) {
    return undefined;
  }

  if (command.executable === "find") {
    const execIndex = command.args.findIndex((argument) => argument === "-exec" || argument === "-execdir");
    return execIndex === -1 ? undefined : nonEmptyString(command.args.slice(execIndex + 1).join(" "));
  }

  if (!SHELL_EXECUTABLES.has(command.executable)) {
    return undefined;
  }

  const commandOption = command.args.findIndex((argument) => /^-[^-]*c/.test(argument));
  const payloadIndex = commandOption !== -1 && command.args[commandOption + 1] === "--"
    ? commandOption + 2
    : commandOption + 1;
  return commandOption === -1 ? undefined : nonEmptyString(command.args[payloadIndex]);
}

function shellTokens(value: string): string[] {
  const tokens: string[] = [];
  const source = value.trim();
  let token = "";
  let quote: "'" | '"' | undefined;
  let ansiCQuote = false;
  let escaped = false;

  for (let index = 0; index < source.length; index += 1) {
    const character = source[index];

    if (escaped) {
      token += character;
      escaped = false;
    } else if (character === "\\" && ansiCQuote) {
      const decoded = decodeAnsiCEscape(source, index + 1);
      token += decoded.value;
      index += decoded.consumed;
    } else if (character === "\\" && (quote !== "'" || ansiCQuote)) {
      escaped = true;
    } else if (character === "'" || character === '"') {
      if (quote === undefined) {
        ansiCQuote = character === "'" && token.endsWith("$");
        if (ansiCQuote) {
          token = token.slice(0, -1);
        }
        quote = character;
      } else if (quote === character) {
        quote = undefined;
        ansiCQuote = false;
      } else {
        token += character;
      }
    } else if (/\s/.test(character) && quote === undefined) {
      if (token.length > 0) {
        tokens.push(token);
        token = "";
      }
    } else {
      token += character;
    }
  }

  if (escaped) {
    token += "\\";
  }

  if (token.length > 0) {
    tokens.push(token);
  }

  return tokens;
}

function decodeAnsiCEscape(source: string, start: number): { value: string; consumed: number } {
  const character = source[start];

  if (character === undefined) {
    return { value: "\\", consumed: 0 };
  }

  const escapes: Record<string, string> = {
    a: "\u0007",
    b: "\b",
    e: "\u001b",
    E: "\u001b",
    f: "\f",
    n: "\n",
    r: "\r",
    t: "\t",
    v: "\v",
    "\\": "\\",
    "'": "'",
    '"': '"'
  };

  if (escapes[character] !== undefined) {
    return { value: escapes[character], consumed: 1 };
  }

  const numeric = character === "x"
    ? { pattern: /^[0-9a-fA-F]{1,2}/, offset: 1, radix: 16 }
    : character === "u"
      ? { pattern: /^[0-9a-fA-F]{1,4}/, offset: 1, radix: 16 }
      : character === "U"
        ? { pattern: /^[0-9a-fA-F]{1,8}/, offset: 1, radix: 16 }
        : /[0-7]/.test(character)
          ? { pattern: /^[0-7]{1,3}/, offset: 0, radix: 8 }
          : undefined;

  if (numeric !== undefined) {
    const digits = numeric.pattern.exec(source.slice(start + numeric.offset))?.[0];
    const codePoint = digits === undefined ? Number.NaN : Number.parseInt(digits, numeric.radix);

    if (digits !== undefined && Number.isFinite(codePoint) && codePoint <= 0x10ffff) {
      return {
        value: String.fromCodePoint(codePoint),
        consumed: numeric.offset + digits.length
      };
    }
  }

  return { value: character, consumed: 1 };
}

function isShellAssignment(token: string | undefined): boolean {
  return token !== undefined && /^[A-Za-z_][A-Za-z0-9_]*=/.test(token);
}

function shellBasename(token: string | undefined): string | undefined {
  return token?.split("/").pop()?.toLowerCase();
}

function normalizeArtifactPath(path: string): string {
  return isDynamicReference(path) ? path : normalizeScopePattern(path);
}

function isProtectedDeletionPath(token: string): boolean {
  const relativeTarget = token.replace(/^\.\//, "").replace(/\/$/, "");
  if ([".", "*", ".*", ".[!.]*", ".[^.]*", "{*,.*}"].includes(relativeTarget)) {
    return true;
  }

  token = normalizeDeletionPath(token);
  return token === "/" || /^\/[^/]*[?*{\[]/.test(token) || token === "~" || token.startsWith("~/") ||
    token === "$HOME" || token.startsWith("$HOME/") || token === "${HOME}" || token.startsWith("${HOME}/");
}

function normalizeDeletionPath(value: string): string {
  const guardedHome = /^\$\{HOME(?::[-=?+][^}]*)?\}/.exec(value)?.[0];
  const sourcePrefix = guardedHome ?? (value.startsWith("$HOME") ? "$HOME" :
    value.startsWith("~") ? "~" : value.startsWith("/") ? "/" : undefined);

  if (sourcePrefix === undefined) {
    return value;
  }

  const prefix = guardedHome === undefined ? sourcePrefix : "${HOME}";
  const remainder = sourcePrefix === "/" ? value.slice(1) : value.slice(sourcePrefix.length).replace(/^\//, "");
  const segments: string[] = [];

  for (const segment of remainder.split("/")) {
    if (segment.length === 0 || segment === ".") {
      continue;
    }

    if (segment === "..") {
      segments.pop();
    } else {
      segments.push(segment);
    }
  }

  if (prefix === "/") {
    return `/${segments.join("/")}`;
  }

  return segments.length === 0 ? prefix : `${prefix}/${segments.join("/")}`;
}

interface StepTargetReference {
  value: string;
  path: string;
}

function stepTargetReferences(step: AgentflowWorkflowStep, path: string): StepTargetReference[] {
  const targets: StepTargetReference[] = [];

  collectDirectTargets(step, path, targets);

  if (step.type === "condition" && Array.isArray(step.branches)) {
    step.branches.forEach((branch, index) => {
      if (isRecord(branch)) {
        collectDirectTargets(branch, `${path}.branches[${index}]`, targets);
      }
    });
  }

  if (isRecord(step.on_failure)) {
    collectFailureTargets(step.on_failure, `${path}.on_failure`, targets);
  }

  return targets;
}

function collectDirectTargets(
  value: AgentflowYamlMapping,
  path: string,
  targets: StepTargetReference[]
): void {
  for (const field of TARGET_FIELDS) {
    const target = nonEmptyString(value[field]);

    if (target !== undefined && !isDynamicReference(target)) {
      targets.push({ value: target, path: path.length === 0 ? field : `${path}.${field}` });
    }
  }
}

function collectFailureTargets(
  value: AgentflowYamlMapping,
  path: string,
  targets: StepTargetReference[]
): void {
  collectDirectTargets(value, path, targets);

  for (const field of ["on_remediated", "on_unresolved"]) {
    const nested = value[field];

    if (isRecord(nested)) {
      collectDirectTargets(nested, `${path}.${field}`, targets);
    }
  }
}


function addStepIssue(
  issues: AgentflowWorkflowIssue[],
  context: StepContext,
  code: string,
  field: string,
  message: string
): void {
  issues.push({
    code,
    message,
    path: `${context.path}.${field}`,
    ...(context.id === undefined ? {} : { stepId: context.id })
  });
}

function visitValue(
  value: AgentflowYamlValue | undefined,
  path: string,
  visitor: (value: AgentflowYamlValue, path: string, key: string) => void
): void {
  if (value === undefined) {
    return;
  }

  if (Array.isArray(value)) {
    value.forEach((entry, index) => {
      const childPath = `${path}[${index}]`;
      visitor(entry, childPath, String(index));
      visitValue(entry, childPath, visitor);
    });
    return;
  }

  if (isRecord(value)) {
    for (const [key, entry] of Object.entries(value)) {
      if (entry !== undefined) {
        const childPath = path.length === 0 ? key : `${path}.${key}`;
        visitor(entry, childPath, key);
        visitValue(entry, childPath, visitor);
      }
    }
  }
}

function stringList(value: AgentflowYamlValue | undefined): string[] {
  return Array.isArray(value)
    ? value.flatMap((entry) => {
        const normalized = nonEmptyString(entry);
        return normalized === undefined ? [] : [normalized];
      })
    : [];
}

function nestedStrings(value: AgentflowYamlValue | undefined): string[] {
  if (typeof value === "string") {
    const normalized = nonEmptyString(value);
    return normalized === undefined ? [] : [normalized];
  }
  if (Array.isArray(value)) {
    return value.flatMap(nestedStrings);
  }
  if (isRecord(value)) {
    return Object.values(value).flatMap(nestedStrings);
  }
  return [];
}

function nonEmptyString(value: AgentflowYamlValue | undefined): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }

  const normalized = value.trim();
  return normalized.length > 0 ? normalized : undefined;
}

function isDynamicReference(value: string): boolean {
  let openingEnd: number | undefined;
  let sawReference = false;

  for (const match of value.matchAll(/\{\{|\}\}/g)) {
    if (match[0] === "{{") {
      if (openingEnd !== undefined) {
        return false;
      }
      openingEnd = (match.index ?? 0) + 2;
    } else {
      if (openingEnd === undefined || value.slice(openingEnd, match.index).trim().length === 0) {
        return false;
      }
      openingEnd = undefined;
      sawReference = true;
    }
  }

  return sawReference && openingEnd === undefined;
}

function isRecord(value: unknown): value is AgentflowYamlMapping {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
