import type { AgentflowApprovalStatus, AgentflowRunStatus } from "./run_state";
import type { AgentflowWorkflow, AgentflowYamlValue } from "./workflow";
import { validateAgentflowPolicyPrimitives } from "./policy_validation";
import {
  isSupportedPolicyGlob,
  isPolicyRootDirectory,
  mapping,
  matchesPolicyGlob,
  nonEmptyString,
  nonNegativeFinite,
  normalizeRepoPattern,
  policyGlobCanMatchDescendant,
  policyGlobsCoverSubtree,
  positiveFinite,
  quotePolicyValue,
  resolveScopedRepoPath,
  stringList
} from "./policy_utils";

export { validateAgentflowPolicyPrimitives } from "./policy_validation";
export type { AgentflowPolicyIssue } from "./policy_validation";

export type AgentflowPolicyStatus = "allow" | "pause" | "fail";

export interface AgentflowPolicyDecision {
  status: AgentflowPolicyStatus;
  code: string;
  message: string;
}

export type AgentflowPolicyRequest =
  | {
      kind: "budget";
      budget: string;
      step?: string;
      used: number;
      amount: number;
    }
  | {
      kind: "model_usage";
      session: string;
      usage: Record<string, number>;
      requested?: Record<string, number>;
    }
  | {
      kind: "approval";
      operation: string;
      approvalStatus?: AgentflowApprovalStatus;
    }
  | {
      kind: "file_write";
      rootPath: string;
      session?: string;
      path: string;
      fileScope?: {
        include?: string[];
        exclude?: string[];
      };
    }
  | {
      kind: "cleanup";
      rootPath: string;
      recursive: boolean;
      runStatus: AgentflowRunStatus;
      paths: string[];
      ageDays?: number;
      approvalStatus?: AgentflowApprovalStatus;
    }
  | {
      kind: "unsafe_operation";
      operation: string;
      approvalStatus?: AgentflowApprovalStatus;
    };

const POLICY_MODES = new Set(["allow", "deny", "require_approval"]);

export function evaluateAgentflowPolicy(
  workflow: AgentflowWorkflow,
  request: AgentflowPolicyRequest
): AgentflowPolicyDecision {
  const configurationIssue = validateAgentflowPolicyPrimitives(workflow)[0];
  if (configurationIssue !== undefined) {
    return fail(
      "policy.configuration.invalid",
      `Workflow policy is invalid: ${configurationIssue.code} (${configurationIssue.path}): ${configurationIssue.message}`
    );
  }

  switch (request.kind) {
    case "budget":
      return checkBudget(workflow, request.budget, request.used, request.amount, request.step);
    case "model_usage":
      return checkModelUsage(workflow, request);
    case "approval":
      return checkApproval(workflow, request.operation, request.approvalStatus);
    case "file_write":
      return checkFileWrite(workflow, request);
    case "cleanup":
      return checkCleanup(workflow, request);
    case "unsafe_operation":
      return checkUnsafeOperation(workflow, request.operation, request.approvalStatus);
  }
}

function checkBudget(
  workflow: AgentflowWorkflow,
  budget: string,
  used: number,
  amount: number,
  step?: string
): AgentflowPolicyDecision {
  if (!nonEmptyString(budget) || !nonNegativeFinite(used) || !positiveFinite(amount) ||
      (step !== undefined && !nonEmptyString(step))) {
    return fail("policy.input.invalid", "Budget checks require a name, non-negative usage, and a positive requested amount.");
  }

  const configuredLimit = mapping(workflow.limits)?.[`max_${budget}`];
  const stepName = step?.trim();
  const limit = budget === "step_attempts" && mapping(configuredLimit) !== undefined
    ? mapping(configuredLimit)?.[stepName ?? ""]
    : configuredLimit;
  if (!positiveFinite(limit)) {
    const scope = stepName === undefined ? "" : ` for step "${stepName}"`;
    return fail("policy.budget.unbounded", `Budget "${budget}"${scope} does not have a positive limits.max_${budget} bound.`);
  }

  if (used + amount > limit) {
    return pause(
      "policy.budget.exhausted",
      `Budget "${budget}" would exceed its limit of ${limit} (${used} used, ${amount} requested).`
    );
  }

  return allow();
}

function checkModelUsage(
  workflow: AgentflowWorkflow,
  request: Extract<AgentflowPolicyRequest, { kind: "model_usage" }>
): AgentflowPolicyDecision {
  if (!policyNumberRecord(request.usage, nonNegativeFinite) ||
      (request.requested !== undefined && !policyNumberRecord(request.requested, positiveFinite))) {
    return fail("policy.input.invalid", "Model usage checks require non-negative usage and positive requested budget amounts.");
  }

  const sessionName = typeof request.session === "string" ? request.session.trim() : "";
  const session = mapping(workflow.sessions?.[sessionName]);
  const provider = typeof session?.provider === "string" && session.provider.trim().length > 0
    ? session.provider.trim()
    : undefined;
  if (session === undefined || provider === undefined) {
    return fail("policy.model.session.invalid", `Model session "${sessionName}" is not declared with a provider.`);
  }

  const allowedProviders = stringList(mapping(mapping(workflow.policies)?.model_usage)?.allowed_providers)
    .map((entry) => entry.trim());
  if (allowedProviders.length > 0 && !allowedProviders.includes(provider)) {
    return fail("policy.model.provider.denied", `Provider "${provider}" is not allowed for session "${sessionName}".`);
  }

  const budgets = ["model_calls", ...(provider === "frontier" ? ["frontier_calls"] : [])];
  for (const budget of budgets) {
    const limit = mapping(workflow.limits)?.[`max_${budget}`];
    if (limit === undefined && budget !== "frontier_calls") continue;
    const used = request.usage[budget] ?? 0;
    const amount = request.requested?.[budget] ?? 1;
    const decision = checkBudget(workflow, budget, used, amount);
    if (decision.status !== "allow") return decision;
  }

  for (const [budget, amount] of Object.entries(request.requested ?? {})) {
    if (budgets.includes(budget)) continue;
    const limit = mapping(workflow.limits)?.[`max_${budget}`];
    if (limit === undefined) continue;
    const decision = checkBudget(workflow, budget, request.usage[budget] ?? 0, amount);
    if (decision.status !== "allow") return decision;
  }

  return allow();
}

function checkApproval(
  workflow: AgentflowWorkflow,
  operation: string,
  approvalStatus?: AgentflowApprovalStatus
): AgentflowPolicyDecision {
  if (!nonEmptyString(operation)) {
    return fail("policy.input.invalid", "Approval checks require a non-empty operation name.");
  }

  const approvals = mapping(mapping(workflow.policies)?.approvals);
  if (!stringList(approvals?.required_for).map((entry) => entry.trim()).includes(operation.trim())) return allow();
  return approvalDecision(operation, approvalStatus);
}

function checkFileWrite(
  workflow: AgentflowWorkflow,
  request: Extract<AgentflowPolicyRequest, { kind: "file_write" }>
): AgentflowPolicyDecision {
  if (typeof request.path !== "string") {
    return fail("policy.input.invalid", "File-write checks require a string path.");
  }
  if (!isPolicyRootDirectory(request.rootPath)) {
    return fail("policy.input.invalid", "File-write checks require rootPath to identify an existing directory.");
  }
  const sessionName = request.session === undefined
    ? undefined
    : typeof request.session === "string"
      ? request.session.trim()
      : "";
  const session = sessionName === undefined ? undefined : mapping(workflow.sessions?.[sessionName]);
  if (sessionName !== undefined && session === undefined) {
    return fail("policy.file_scope.session.invalid", `File-writing session "${sessionName}" is not declared.`);
  }
  if (sessionName !== undefined && mapping(session?.authority)?.can_modify_files !== true) {
    return fail("policy.file_scope.authority.denied", `Session "${sessionName}" is not authorized to modify files.`);
  }
  if (request.fileScope !== undefined && !validOperationScope(request.fileScope)) {
    return fail("policy.input.invalid", "File-write scope must contain supported repo-relative include and exclude globs.");
  }

  const normalizedPath = resolveScopedRepoPath(request.rootPath, request.path);
  if (normalizedPath === undefined) {
    return fail(
      "policy.file_scope.denied",
      `File path ${quotePolicyValue(request.path)} must be repo-relative and stay inside the repository.`
    );
  }

  const globalScope = mapping(mapping(workflow.policies)?.file_scope);
  const sessionScope = mapping(session?.file_scope);
  const scopes = [
    ...(globalScope === undefined ? [] : [{
      include: stringList(globalScope.include),
      exclude: stringList(globalScope.exclude)
    }]),
    ...(sessionScope === undefined ? [] : [{
      include: stringList(sessionScope.include),
      exclude: stringList(sessionScope.exclude)
    }]),
    ...(request.fileScope === undefined ? [] : [{
      include: request.fileScope.include ?? [],
      exclude: request.fileScope.exclude ?? []
    }])
  ];
  const denied = !scopes.some((scope) => scope.include.length > 0) || scopes.some((scope) =>
    (scope.include.length > 0 && !scope.include.some((pattern) => matchesPolicyGlob(normalizedPath, pattern))) ||
    (scope.exclude ?? []).some((pattern) => matchesPolicyGlob(normalizedPath, pattern))
  );
  if (denied) {
    const actor = sessionName === undefined ? "Workflow operation" : `Session "${sessionName}"`;
    return fail("policy.file_scope.denied", `${actor} cannot write "${normalizedPath}" under its file scope.`);
  }

  return allow();
}

function checkCleanup(
  workflow: AgentflowWorkflow,
  request: Extract<AgentflowPolicyRequest, { kind: "cleanup" }>
): AgentflowPolicyDecision {
  if (!Array.isArray(request.paths) || request.paths.length === 0 || request.paths.some((entry) => !nonEmptyString(entry))) {
    return fail("policy.input.invalid", "Cleanup checks require at least one non-empty path.");
  }
  if (typeof request.recursive !== "boolean") {
    return fail("policy.input.invalid", "Cleanup checks must declare whether deletion is recursive.");
  }
  if (request.ageDays !== undefined && !nonNegativeFinite(request.ageDays)) {
    return fail("policy.input.invalid", "Cleanup ageDays must be a non-negative finite number when present.");
  }
  if (!isPolicyRootDirectory(request.rootPath)) {
    return fail("policy.input.invalid", "Cleanup checks require rootPath to identify an existing directory.");
  }

  const cleanupMode = policyMode(workflow, "cleanup", "allow");
  if (cleanupMode === "deny") {
    return fail("policy.cleanup.denied", "Cleanup is denied by workflow policy.");
  }
  if (cleanupMode === "require_approval") {
    const decision = approvalDecision("cleanup", request.approvalStatus);
    if (decision.status !== "allow") return decision;
  }

  const ruleName = retentionRuleName(request.runStatus);
  const rule = ruleName === undefined ? undefined : mapping(workflow.retention?.[ruleName]);
  if (rule === undefined) {
    return fail("policy.cleanup.retained", `No cleanup retention rule is declared for run status "${request.runStatus}".`);
  }

  if (rule.ask_user === true) {
    const decision = approvalDecision("cleanup", request.approvalStatus);
    if (decision.status !== "allow") return decision;
  }

  const keepAllForDays = numeric(rule.keep_all_for_days);
  const ageDays = request.ageDays ?? 0;
  if (keepAllForDays !== undefined && ageDays < keepAllForDays) {
    return fail(
      "policy.cleanup.retained",
      `Cleanup cannot delete artifacts retained for ${keepAllForDays} days by ${ruleName}.`
    );
  }
  const minimumAge = numeric(rule.after_days);
  if (minimumAge !== undefined && ageDays < minimumAge) {
    return fail(
      "policy.cleanup.retention_period",
      `Cleanup must wait ${minimumAge} days for ${ruleName}; received ${ageDays} days.`
    );
  }

  const keep = stringList(rule.keep);
  const deletions = stringList(rule.delete);
  for (const candidate of request.paths) {
    const normalized = resolveScopedRepoPath(request.rootPath, candidate);
    if (normalized === undefined) {
      return fail(
        "policy.cleanup.path.invalid",
        `Cleanup path ${quotePolicyValue(candidate)} must be repo-relative and stay inside the run directory.`
      );
    }
    if (keep.some((pattern) => matchesPolicyGlob(normalized, pattern) ||
        (request.recursive && policyGlobCanMatchDescendant(normalized, pattern)))) {
      return fail("policy.cleanup.retained", `Cleanup cannot delete retained path "${normalized}".`);
    }
    if (deletions.length > 0 && !(request.recursive
      ? policyGlobsCoverSubtree(normalized, deletions)
      : deletions.some((pattern) => matchesPolicyGlob(normalized, pattern)))) {
      return fail("policy.cleanup.not_declared", `Cleanup path "${normalized}" is not covered by ${ruleName}.delete.`);
    }
  }

  return allow();
}

function checkUnsafeOperation(
  workflow: AgentflowWorkflow,
  operation: string,
  approvalStatus?: AgentflowApprovalStatus
): AgentflowPolicyDecision {
  if (!nonEmptyString(operation)) {
    return fail("policy.input.invalid", "Unsafe-operation checks require a non-empty operation description.");
  }

  const mode = policyMode(workflow, "unsafe_operations", "deny");
  if (mode === "deny") {
    return fail("policy.unsafe.denied", `Unsafe operation "${operation}" is denied by workflow policy.`);
  }
  if (mode === "require_approval") {
    return approvalDecision(`unsafe operation: ${operation}`, approvalStatus);
  }
  return allow();
}

function approvalDecision(operation: string, status?: AgentflowApprovalStatus): AgentflowPolicyDecision {
  if (status === "approved") return allow();
  if (status === "rejected" || status === "cancelled") {
    return fail("policy.approval.rejected", `Approval for ${operation} was ${status}.`);
  }
  return pause("policy.approval.required", `Approval is required before ${operation}.`);
}

function policyMode(workflow: AgentflowWorkflow, field: string, fallback: string): string {
  const value = mapping(workflow.policies)?.[field];
  return typeof value === "string" && POLICY_MODES.has(value) ? value : fallback;
}

function retentionRuleName(status: AgentflowRunStatus): string | undefined {
  if (status === "completed") return "on_success";
  if (status === "failed") return "on_failure";
  if (status === "cancelled") return "on_cancelled";
  return undefined;
}

function numeric(value: AgentflowYamlValue | undefined): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function validOperationScope(value: { include?: string[]; exclude?: string[] }): boolean {
  return (value.include === undefined || validPatternList(value.include)) &&
    (value.exclude === undefined || validPatternList(value.exclude));
}

function validPatternList(value: unknown): value is string[] {
  return Array.isArray(value) && value.length > 0 &&
    value.every((entry) =>
      nonEmptyString(entry) && normalizeRepoPattern(entry) !== undefined && isSupportedPolicyGlob(entry)
    );
}

function policyNumberRecord(value: unknown, valid: (entry: unknown) => boolean): value is Record<string, number> {
  return typeof value === "object" && value !== null && !Array.isArray(value) &&
    Object.values(value).every(valid);
}

function allow(): AgentflowPolicyDecision {
  return { status: "allow", code: "policy.allow", message: "Policy check passed." };
}

function pause(code: string, message: string): AgentflowPolicyDecision {
  return { status: "pause", code, message };
}

function fail(code: string, message: string): AgentflowPolicyDecision {
  return { status: "fail", code, message };
}
