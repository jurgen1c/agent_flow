import type { AgentflowWorkflow, AgentflowYamlMapping, AgentflowYamlValue } from "./workflow";
import {
  isSupportedPolicyGlob,
  mapping,
  nonEmptyStringList,
  nonNegativeFinite,
  normalizeRepoPattern,
  policyGlobLayersHaveWritablePath,
  positiveFinite,
  quotePolicyValue,
  stringList
} from "./policy_utils";

export interface AgentflowPolicyIssue {
  code: string;
  message: string;
  path: string;
}

const POLICY_MODES = new Set(["allow", "deny", "require_approval"]);

export function validateAgentflowPolicyPrimitives(workflow: AgentflowWorkflow): AgentflowPolicyIssue[] {
  const errors: AgentflowPolicyIssue[] = [];
  const limits = mapping(workflow.limits);
  const policies = mapping(workflow.policies);
  const globalScope = mapping(policies?.file_scope);
  const stepIds = declaredStepIds(workflow.steps);
  const frontierSessions = Object.entries(workflow.sessions ?? {})
    .filter(([, session]) => {
      const provider = mapping(session)?.provider;
      return typeof provider === "string" && provider.trim() === "frontier";
    })
    .map(([name]) => name);

  for (const [name, sessionValue] of Object.entries(workflow.sessions ?? {})) {
    const provider = mapping(sessionValue)?.provider;
    if (typeof provider === "string" && provider.includes("{{")) {
      errors.push(issue(
        "workflow.policy.model_usage.provider.dynamic",
        `sessions.${name}.provider`,
        `Session "${name}" must declare a static provider so model budgets can be enforced before execution.`
      ));
    }
  }

  if (workflow.limits !== undefined && limits === undefined) {
    errors.push(issue("workflow.policy.budget.invalid", "limits", "Workflow limits must be a mapping."));
  }

  for (const [name, value] of Object.entries(limits ?? {})) {
    if (name === "max_step_attempts") {
      const attempts = mapping(value);
      if (attempts === undefined || Object.keys(attempts).length === 0 ||
          Object.entries(attempts).some(([step, entry]) =>
            step.length === 0 || step !== step.trim() || !stepIds.has(step) || !positiveFinite(entry)
          )) {
        errors.push(issue(
          "workflow.policy.budget.invalid",
          "limits.max_step_attempts",
          "Budget limit limits.max_step_attempts must map declared canonical step names to positive finite numbers."
        ));
      }
      continue;
    }
    if (name.startsWith("max_") && !positiveFinite(value)) {
      errors.push(issue(
        "workflow.policy.budget.invalid",
        `limits.${name}`,
        `Budget limit limits.${name} must be a positive finite number.`
      ));
    }
  }

  if (frontierSessions.length > 0 && !positiveFinite(limits?.max_frontier_calls)) {
    errors.push(issue(
      "workflow.policy.budget.frontier.required",
      "limits.max_frontier_calls",
      `Frontier sessions (${frontierSessions.join(", ")}) require a positive limits.max_frontier_calls budget.`
    ));
  }

  validatePolicyMode(policies, "cleanup", errors);
  validatePolicyMode(policies, "unsafe_operations", errors);
  validateModelPolicy(workflow, policies, errors);
  validateApprovalPolicy(policies, errors);
  if (policies?.file_scope !== undefined && globalScope === undefined) {
    errors.push(issue(
      "workflow.policy.file_scope.invalid",
      "policies.file_scope",
      "Workflow file scope policy must be a mapping."
    ));
  }
  validateFileScope(globalScope, "policies.file_scope", errors);

  for (const [name, sessionValue] of Object.entries(workflow.sessions ?? {})) {
    const session = mapping(sessionValue);
    if (session === undefined) continue;
    const sessionScope = mapping(session.file_scope);
    if (session.file_scope !== undefined && sessionScope === undefined) {
      errors.push(issue(
        "workflow.policy.file_scope.invalid",
        `sessions.${name}.file_scope`,
        "Session file scope must be a mapping."
      ));
    }
    validateFileScope(sessionScope, `sessions.${name}.file_scope`, errors);

    const globalIncludes = stringList(globalScope?.include);
    const sessionIncludes = stringList(sessionScope?.include);
    const exclusions = [
      ...stringList(globalScope?.exclude),
      ...stringList(sessionScope?.exclude)
    ];
    if (mapping(session.authority)?.can_modify_files === true &&
        (globalIncludes.length > 0 || sessionIncludes.length > 0) &&
        !policyGlobLayersHaveWritablePath([globalIncludes, sessionIncludes], exclusions)) {
      errors.push(issue(
        "workflow.policy.file_scope.disjoint",
        `sessions.${name}.file_scope.include`,
        `File-writing session "${name}" has no writable path shared with policies.file_scope.include.`
      ));
    }

    if (mapping(session.authority)?.can_modify_files === true && sessionUsedOutsideParallel(workflow.steps, name)) {
      const effectiveIncludes = [
        ...globalIncludes,
        ...sessionIncludes
      ];
      if (effectiveIncludes.length === 0) {
        errors.push(issue(
          "workflow.policy.file_scope.required",
          `sessions.${name}.file_scope.include`,
          `File-writing session "${name}" must declare a non-empty file_scope.include list.`
        ));
      }
    }
  }

  validateStepFileScopes(workflow, workflow.steps, "steps", errors);
  errors.push(...validateAgentflowRetentionPolicy(workflow.retention));
  return errors;
}

function validateStepFileScopes(
  workflow: AgentflowWorkflow,
  value: AgentflowYamlValue,
  basePath: string,
  errors: AgentflowPolicyIssue[],
  inheritedScopes: AgentflowYamlMapping[] = []
): void {
  if (Array.isArray(value)) {
    value.forEach((entry, index) =>
      validateStepFileScopes(workflow, entry, `${basePath}[${index}]`, errors, inheritedScopes)
    );
    return;
  }
  const record = mapping(value);
  if (record === undefined) return;

  const operationScope = mapping(record.file_scope);
  if (record.file_scope !== undefined) {
    if (operationScope === undefined) {
      errors.push(issue(
        "workflow.policy.file_scope.invalid",
        `${basePath}.file_scope`,
        "Operation file scope must be a mapping."
      ));
    } else {
      validateFileScope(operationScope, `${basePath}.file_scope`, errors);
    }
  }

  const operationScopes = operationScope === undefined ? inheritedScopes : [...inheritedScopes, operationScope];
  const sessionName = typeof record.session === "string" ? record.session.trim() : undefined;
  const session = sessionName === undefined ? undefined : mapping(workflow.sessions?.[sessionName]);
  if (operationScopes.length > 0 && mapping(session?.authority)?.can_modify_files === true) {
    const globalScope = mapping(mapping(workflow.policies)?.file_scope);
    const sessionScope = mapping(session?.file_scope);
    const scopes = [globalScope, sessionScope, ...operationScopes].filter(
      (scope): scope is AgentflowYamlMapping => scope !== undefined
    );
    const includeLayers = scopes.map((scope) => stringList(scope.include));
    const exclusions = scopes.flatMap((scope) => stringList(scope.exclude));
    if (includeLayers.some((layer) => layer.length > 0) &&
        !policyGlobLayersHaveWritablePath(includeLayers, exclusions)) {
      errors.push(issue(
        "workflow.policy.file_scope.disjoint",
        `${basePath}.file_scope.include`,
        `File-writing operation "${typeof record.id === "string" ? record.id.trim() : basePath}" has no writable path shared by its policy layers.`
      ));
    }
  }

  const type = typeof record.type === "string" ? record.type.trim() : undefined;
  for (const field of type === "parallel" ? ["branches", "body", "steps"] : ["body", "steps"]) {
    const entry = record[field];
    if (entry !== undefined) {
      validateStepFileScopes(workflow, entry, `${basePath}.${field}`, errors, operationScopes);
    }
  }

  const failureRoute = mapping(mapping(record.on_failure)?.route_to);
  if (failureRoute !== undefined) {
    validateStepFileScopes(
      workflow,
      failureRoute,
      `${basePath}.on_failure.route_to`,
      errors,
      operationScopes
    );
  }
}

function validatePolicyMode(
  policies: AgentflowYamlMapping | undefined,
  field: string,
  errors: AgentflowPolicyIssue[]
): void {
  const value = policies?.[field];
  if (value !== undefined && (typeof value !== "string" || !POLICY_MODES.has(value))) {
    errors.push(issue(
      `workflow.policy.${field}.invalid`,
      `policies.${field}`,
      `Policy ${field} must be one of: allow, deny, require_approval.`
    ));
  }
}

function validateModelPolicy(
  workflow: AgentflowWorkflow,
  policies: AgentflowYamlMapping | undefined,
  errors: AgentflowPolicyIssue[]
): void {
  const value = policies?.model_usage;
  if (value === undefined) return;
  const modelUsage = mapping(value);
  if (modelUsage === undefined) {
    errors.push(issue("workflow.policy.model_usage.invalid", "policies.model_usage", "Model usage policy must be a mapping."));
    return;
  }
  if (modelUsage.allowed_providers !== undefined && !nonEmptyStringList(modelUsage.allowed_providers)) {
    errors.push(issue(
      "workflow.policy.model_usage.invalid",
      "policies.model_usage.allowed_providers",
      "Allowed model providers must be a non-empty list of non-empty strings."
    ));
    return;
  }

  const allowedProviders = stringList(modelUsage.allowed_providers).map((provider) => provider.trim());
  if (allowedProviders.length === 0) return;
  for (const [name, sessionValue] of Object.entries(workflow.sessions ?? {})) {
    const provider = mapping(sessionValue)?.provider;
    if (typeof provider === "string" && provider.trim().length > 0 && !provider.includes("{{") &&
        !allowedProviders.includes(provider.trim())) {
      errors.push(issue(
        "workflow.policy.model_usage.provider.denied",
        `sessions.${name}.provider`,
        `Session "${name}" uses provider "${provider.trim()}", which is not in policies.model_usage.allowed_providers.`
      ));
    }
  }
}

function validateApprovalPolicy(policies: AgentflowYamlMapping | undefined, errors: AgentflowPolicyIssue[]): void {
  const value = policies?.approvals;
  if (value === undefined) return;
  const approvals = mapping(value);
  if (approvals === undefined) {
    errors.push(issue("workflow.policy.approvals.invalid", "policies.approvals", "Approval policy must be a mapping."));
    return;
  }
  if (approvals.required_for !== undefined && !nonEmptyStringList(approvals.required_for)) {
    errors.push(issue(
      "workflow.policy.approvals.invalid",
      "policies.approvals.required_for",
      "Approval required_for must be a non-empty list of non-empty operation names."
    ));
  }
}

function validateFileScope(
  scope: AgentflowYamlMapping | undefined,
  basePath: string,
  errors: AgentflowPolicyIssue[]
): void {
  if (scope === undefined) return;
  for (const field of ["include", "exclude"] as const) {
    const value = scope[field];
    if (value === undefined) continue;
    if (!nonEmptyStringList(value)) {
      errors.push(issue(
        "workflow.policy.file_scope.invalid",
        `${basePath}.${field}`,
        `File scope ${field} must be a non-empty list of repo-relative patterns.`
      ));
      continue;
    }
    stringList(value).forEach((pattern, index) => {
      if (normalizeRepoPattern(pattern) === undefined || !isSupportedPolicyGlob(pattern)) {
        errors.push(issue(
          "workflow.policy.file_scope.invalid",
          `${basePath}.${field}[${index}]`,
          `File scope pattern ${quotePolicyValue(pattern)} must be a supported repo-relative glob and stay inside the repository.`
        ));
      }
    });
  }
}

export function validateAgentflowRetentionPolicy(value: AgentflowYamlValue | undefined): AgentflowPolicyIssue[] {
  const errors: AgentflowPolicyIssue[] = [];
  if (value === undefined) return errors;
  const retention = mapping(value);
  if (retention === undefined) {
    errors.push(issue("workflow.policy.retention.invalid", "retention", "Retention policy must be a mapping."));
    return errors;
  }
  for (const name of ["on_success", "on_failure", "on_cancelled"]) {
    const ruleValue = retention[name];
    if (ruleValue === undefined) continue;
    const rule = mapping(ruleValue);
    if (rule === undefined) {
      errors.push(issue("workflow.policy.retention.invalid", `retention.${name}`, "Retention outcome rules must be mappings."));
      continue;
    }
    for (const field of ["keep", "delete"] as const) {
      if (rule[field] !== undefined && !nonEmptyStringList(rule[field])) {
        errors.push(issue(
          "workflow.policy.retention.invalid",
          `retention.${name}.${field}`,
          `Retention ${field} must be a non-empty list of run-directory-relative patterns.`
        ));
      } else {
        stringList(rule[field]).forEach((pattern, index) => {
          if (normalizeRepoPattern(pattern) === undefined || !isSupportedPolicyGlob(pattern)) {
            errors.push(issue(
              "workflow.policy.retention.invalid",
              `retention.${name}.${field}[${index}]`,
              `Retention path pattern ${quotePolicyValue(pattern)} must be a supported relative glob and stay inside the run directory.`
            ));
          }
        });
      }
    }
    for (const field of ["after_days", "keep_all_for_days"] as const) {
      if (rule[field] !== undefined && !nonNegativeFinite(rule[field])) {
        errors.push(issue(
          "workflow.policy.retention.invalid",
          `retention.${name}.${field}`,
          `Retention ${field} must be a non-negative finite number.`
        ));
      }
    }
    if (rule.ask_user !== undefined && typeof rule.ask_user !== "boolean") {
      errors.push(issue(
        "workflow.policy.retention.invalid",
        `retention.${name}.ask_user`,
        "Retention ask_user must be a boolean."
      ));
    }
  }
  return errors;
}

function sessionUsedOutsideParallel(value: AgentflowYamlValue, sessionName: string, insideParallel = false): boolean {
  if (!Array.isArray(value)) return false;

  return value.some((entry) => {
    const record = mapping(entry);
    if (record === undefined) return false;
    if (!insideParallel && typeof record.session === "string") {
      const referencedSession = record.session.trim();
      if (referencedSession === sessionName || referencedSession.includes("{{")) return true;
    }

    const nestedInsideParallel = insideParallel || record.type === "parallel";
    for (const field of ["body", "steps"]) {
      if (sessionUsedOutsideParallel(record[field] ?? null, sessionName, nestedInsideParallel)) return true;
    }
    if (record.type === "parallel" && sessionUsedOutsideParallel(record.branches ?? null, sessionName, true)) {
      return true;
    }
    const failureRoute = mapping(mapping(record.on_failure)?.route_to);
    if (!insideParallel && failureRoute !== undefined && typeof failureRoute.session === "string") {
      const referencedSession = failureRoute.session.trim();
      if (referencedSession === sessionName || referencedSession.includes("{{")) return true;
    }
    return false;
  });
}

function declaredStepIds(value: AgentflowYamlValue): Set<string> {
  const ids = new Set<string>();
  const visit = (entry: AgentflowYamlValue): void => {
    if (Array.isArray(entry)) {
      entry.forEach(visit);
      return;
    }
    const record = mapping(entry);
    if (record === undefined) return;
    if (typeof record.id === "string" && record.id.trim().length > 0) ids.add(record.id.trim());
    const type = typeof record.type === "string" ? record.type.trim() : undefined;
    for (const field of type === "parallel" ? ["branches", "body", "steps"] : ["body", "steps"]) {
      const nested = record[field];
      if (nested !== undefined) visit(nested);
    }
  };
  visit(value);
  return ids;
}

function issue(code: string, issuePath: string, message: string): AgentflowPolicyIssue {
  return { code, message, path: issuePath };
}
