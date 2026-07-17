import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  AgentflowRunStateError,
  createAgentflowLifecycleRun,
  evaluateAgentflowPolicy,
  openAgentflowRunState,
  parseAgentflowWorkflowOrThrow,
  validateAgentflowWorkflow
} from "../../packages/agentflow-core/src";
import {
  normalizeRepoPath,
  normalizeRepoPattern,
  policyGlobsCoverSubtree,
  policyGlobsIntersect
} from "../../packages/agentflow-core/src/policy_utils";

const POLICY_WORKFLOW = `
name: policy-runtime
version: 1
style: collaborative
maturity: experimental
collaboration:
  enabled: true
  max_review_cycles: 2
  on_disagreement:
    strategy: ask_user
sessions:
  writer:
    provider: frontier
    role: implementer
    authority:
      can_modify_files: true
    file_scope:
      include: [src/**]
      exclude: [src/secrets/**]
limits:
  max_frontier_calls: 2
  max_model_calls: 4
policies:
  model_usage:
    allowed_providers: [local, frontier]
  approvals:
    required_for: [publish]
  cleanup: require_approval
  unsafe_operations: require_approval
retention:
  on_success:
    keep: [state.json, final/**]
    delete: [temp/**, logs/**]
    after_days: 7
  on_failure:
    keep_all_for_days: 30
  on_cancelled:
    ask_user: true
steps:
  - id: implement
    type: session_request
    session: writer
    prompt: prompts/implement.md
    outputs: [final/summary.md]
`;

describe("Agentflow policy primitives", () => {
  test("rejects control characters in repo paths and policy patterns", () => {
    for (const control of ["\0", "\t", "\n", "\r", "\u007f", "\u0085"]) {
      expect(normalizeRepoPath(`src/token${control}.txt`)).toBeUndefined();
      expect(normalizeRepoPattern(`src/token${control}.txt`)).toBeUndefined();
    }
  });

  test("fails closed for malformed globs passed directly to exported helpers", () => {
    expect(policyGlobsIntersect("src/[", "src/**")).toBe(false);
    expect(policyGlobsIntersect("src/[z-a]", "src/**")).toBe(false);
    expect(policyGlobsCoverSubtree("src", ["src/[]/**"])).toBe(false);
  });

  test("ignores session-like fields in ordinary step data", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`
name: policy-metadata
version: 1
style: pipeline
maturity: draft
sessions:
  writer:
    provider: local
    authority: { can_modify_files: true }
steps:
  - id: build
    type: command
    command: echo ok
    metadata: { session: writer }
`);

    expect(validateAgentflowWorkflow(workflow)).toEqual({ valid: true, errors: [] });
  });

  test("validates file policy for executable failure routes", () => {
    const unscoped = parseAgentflowWorkflowOrThrow(`
name: recovery-policy
version: 1
style: recovery_pipeline
maturity: draft
sessions:
  fixer:
    provider: local
    authority: { can_modify_files: true }
steps:
  - id: check
    type: command
    command: bin/check
    on_failure:
      route_to:
        session: fixer
        prompt: Fix the failure
`);

    expect(validateAgentflowWorkflow(unscoped).errors).toContainEqual({
      code: "workflow.policy.file_scope.required",
      message: 'File-writing session "fixer" must declare a non-empty file_scope.include list.',
      path: "sessions.fixer.file_scope.include"
    });

    const malformedRouteScope = parseAgentflowWorkflowOrThrow(`
name: recovery-policy
version: 1
style: recovery_pipeline
maturity: draft
sessions:
  fixer:
    provider: local
    authority: { can_modify_files: true }
    file_scope: { include: [src/**] }
steps:
  - id: check
    type: command
    command: bin/check
    on_failure:
      route_to:
        session: fixer
        prompt: Fix the failure
        file_scope: { include: [../outside/**] }
`);

    expect(validateAgentflowWorkflow(malformedRouteScope).errors).toContainEqual({
      code: "workflow.policy.file_scope.invalid",
      message: 'File scope pattern "../outside/**" must be a supported repo-relative glob and stay inside the repository.',
      path: "steps[0].on_failure.route_to.file_scope.include[0]"
    });
  });

  test("pauses before exhausted budget usage", () => {
    const workflow = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW);

    expect(evaluateAgentflowPolicy(workflow, {
      kind: "budget",
      budget: "frontier_calls",
      used: 1,
      amount: 1
    })).toMatchObject({ status: "allow", code: "policy.allow" });
    expect(evaluateAgentflowPolicy(workflow, {
      kind: "budget",
      budget: "frontier_calls",
      used: 2,
      amount: 1
    })).toMatchObject({ status: "pause", code: "policy.budget.exhausted" });

    const perStep = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW.replace(
      "  max_model_calls: 4",
      "  max_model_calls: 4\n  max_step_attempts: { implement: 3 }"
    ));
    expect(evaluateAgentflowPolicy(perStep, {
      kind: "budget",
      budget: "step_attempts",
      step: "implement",
      used: 2,
      amount: 1
    })).toMatchObject({ status: "allow" });
    expect(evaluateAgentflowPolicy(perStep, {
      kind: "budget",
      budget: "step_attempts",
      step: "implement",
      used: 3,
      amount: 1
    })).toMatchObject({ status: "pause", code: "policy.budget.exhausted" });
    expect(evaluateAgentflowPolicy(perStep, {
      kind: "budget",
      budget: "step_attempts",
      step: "review",
      used: 0,
      amount: 1
    })).toMatchObject({ status: "fail", code: "policy.budget.unbounded" });
  });

  test("checks model providers and their declared budgets", () => {
    const workflow = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW);

    expect(evaluateAgentflowPolicy(workflow, {
      kind: "model_usage",
      session: "writer",
      usage: { frontier_calls: 1, model_calls: 3 }
    })).toMatchObject({ status: "allow" });
    expect(evaluateAgentflowPolicy(workflow, {
      kind: "model_usage",
      session: "writer",
      usage: { frontier_calls: 2, model_calls: 3 }
    })).toMatchObject({ status: "pause", code: "policy.budget.exhausted" });

    const denied = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW.replace(
      "allowed_providers: [local, frontier]",
      "allowed_providers: [local]"
    ));
    expect(evaluateAgentflowPolicy(denied, {
      kind: "model_usage",
      session: "writer",
      usage: { frontier_calls: 0, model_calls: 0 }
    })).toMatchObject({ status: "fail", code: "policy.configuration.invalid" });
  });

  test("pauses for required approvals and fails rejected approvals", () => {
    const workflow = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW);

    expect(evaluateAgentflowPolicy(workflow, {
      kind: "approval",
      operation: "publish",
      approvalStatus: "requested"
    })).toMatchObject({ status: "pause", code: "policy.approval.required" });
    expect(evaluateAgentflowPolicy(workflow, {
      kind: "approval",
      operation: "publish",
      approvalStatus: "approved"
    })).toMatchObject({ status: "allow" });
    expect(evaluateAgentflowPolicy(workflow, {
      kind: "approval",
      operation: "publish",
      approvalStatus: "rejected"
    })).toMatchObject({ status: "fail", code: "policy.approval.rejected" });
  });

  test("fails writes outside session scope or inside excluded scope", () => {
    const workflow = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW);
    const rootPath = temporaryRepo();
    fs.mkdirSync(path.join(rootPath, "src"));
    const fileRootPath = path.join(rootPath, "not-a-directory");
    fs.writeFileSync(fileRootPath, "not a directory\n", "utf8");

    expect(evaluateAgentflowPolicy(workflow, {
      kind: "file_write",
      rootPath,
      session: "writer",
      path: undefined as unknown as string
    })).toEqual({
      status: "fail",
      code: "policy.input.invalid",
      message: "File-write checks require a string path."
    });
    for (const invalidRootPath of [
      undefined as unknown as string,
      "",
      path.join(rootPath, "missing"),
      fileRootPath
    ]) {
      expect(evaluateAgentflowPolicy(workflow, {
        kind: "file_write",
        rootPath: invalidRootPath,
        session: "writer",
        path: "src/index.ts"
      })).toEqual({
        status: "fail",
        code: "policy.input.invalid",
        message: "File-write checks require rootPath to identify an existing directory."
      });
    }

    expect(evaluateAgentflowPolicy(workflow, {
      kind: "file_write",
      rootPath,
      session: "writer",
      path: "src/index.ts"
    })).toMatchObject({ status: "allow" });
    expect(evaluateAgentflowPolicy(workflow, {
      kind: "file_write",
      rootPath,
      session: "writer",
      path: "tests/index.test.ts"
    })).toMatchObject({ status: "fail", code: "policy.file_scope.denied" });

    const newlinePath = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW.replace(
      "include: [src/**]",
      "include: [src/*/*]"
    ));
    expect(evaluateAgentflowPolicy(newlinePath, {
      kind: "file_write",
      rootPath,
      session: "writer",
      path: "src/secrets/token\n.txt"
    })).toEqual({
      status: "fail",
      code: "policy.file_scope.denied",
      message: 'File path "src/secrets/token\\n.txt" must be repo-relative and stay inside the repository.'
    });
    expect(evaluateAgentflowPolicy(newlinePath, {
      kind: "file_write",
      rootPath,
      session: "writer",
      path: "src/secrets/token\u0085.txt"
    })).toEqual({
      status: "fail",
      code: "policy.file_scope.denied",
      message: 'File path "src/secrets/token\\u0085.txt" must be repo-relative and stay inside the repository.'
    });

    const negatedClass = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW.replace(
      "include: [src/**]",
      'include: ["src[!x]secret/**"]'
    ));
    expect(evaluateAgentflowPolicy(negatedClass, {
      kind: "file_write",
      rootPath,
      session: "writer",
      path: "src/secret/file.ts"
    })).toMatchObject({ status: "fail", code: "policy.file_scope.denied" });
    expect(evaluateAgentflowPolicy(workflow, {
      kind: "file_write",
      rootPath,
      session: "writer",
      path: "src/secrets/token.ts"
    })).toMatchObject({ status: "fail", code: "policy.file_scope.denied" });
    expect(evaluateAgentflowPolicy(workflow, {
      kind: "file_write",
      rootPath,
      path: "README.md",
      fileScope: { include: ["**/*.md"] }
    })).toMatchObject({ status: "allow" });
    expect(evaluateAgentflowPolicy(workflow, {
      kind: "file_write",
      rootPath,
      session: "writer",
      path: "tests/index.test.ts",
      fileScope: { include: ["**"] }
    })).toMatchObject({ status: "fail", code: "policy.file_scope.denied" });
    expect(evaluateAgentflowPolicy(workflow, {
      kind: "file_write",
      rootPath,
      session: "writer",
      path: "src/index.ts",
      fileScope: { exclude: ["src/secrets/**"] }
    })).toMatchObject({ status: "allow" });
    expect(evaluateAgentflowPolicy(workflow, {
      kind: "file_write",
      rootPath,
      session: "writer",
      path: "src/secrets/token.ts",
      fileScope: { exclude: ["src/secrets/**"] }
    })).toMatchObject({ status: "fail", code: "policy.file_scope.denied" });

    const globalScope = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW.replace(
      "policies:\n",
      "policies:\n  file_scope:\n    include: [src/public/**]\n"
    ));
    expect(evaluateAgentflowPolicy(globalScope, {
      kind: "file_write",
      rootPath,
      session: "writer",
      path: "src/index.ts"
    })).toMatchObject({ status: "fail", code: "policy.file_scope.denied" });
    expect(evaluateAgentflowPolicy(globalScope, {
      kind: "file_write",
      rootPath,
      session: "writer",
      path: "src/public/index.ts"
    })).toMatchObject({ status: "allow" });

    const globalExclusion = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW
      .replace("      exclude: [src/secrets/**]\n", "")
      .replace("policies:\n", "policies:\n  file_scope:\n    exclude: [src/secrets/**]\n"));
    expect(evaluateAgentflowPolicy(globalExclusion, {
      kind: "file_write",
      rootPath,
      session: "writer",
      path: "src/index.ts"
    })).toMatchObject({ status: "allow" });
    expect(evaluateAgentflowPolicy(globalExclusion, {
      kind: "file_write",
      rootPath,
      session: "writer",
      path: "src/secrets/token.ts"
    })).toMatchObject({ status: "fail", code: "policy.file_scope.denied" });

    const braces = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW.replace(
      "exclude: [src/secrets/**]",
      "exclude: [\"src/{secrets,credentials}/**\"]"
    ));
    expect(evaluateAgentflowPolicy(braces, {
      kind: "file_write",
      rootPath,
      session: "writer",
      path: "src/credentials/token.ts"
    })).toMatchObject({ status: "fail", code: "policy.file_scope.denied" });

    const braceInclude = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW.replace(
      "include: [src/**]",
      "include: [\"src/**/*.{ts,js}\"]"
    ));
    expect(evaluateAgentflowPolicy(braceInclude, {
      kind: "file_write",
      rootPath,
      session: "writer",
      path: "src/models/user.ts"
    })).toMatchObject({ status: "allow" });
    expect(evaluateAgentflowPolicy(workflow, {
      kind: "file_write",
      rootPath,
      path: "src/x.ts",
      fileScope: { include: ["src/**"], exclude: ["src/[z-a].ts"] }
    })).toMatchObject({ status: "fail", code: "policy.input.invalid" });
    for (const candidatePath of [" src/index.ts", "src\\index.ts"]) {
      expect(evaluateAgentflowPolicy(workflow, {
        kind: "file_write",
        rootPath,
        session: "writer",
        path: candidatePath
      })).toMatchObject({ status: "fail", code: "policy.file_scope.denied" });
    }

    fs.symlinkSync(os.tmpdir(), path.join(rootPath, "src", "outside"));
    expect(evaluateAgentflowPolicy(workflow, {
      kind: "file_write",
      rootPath,
      session: "writer",
      path: "src/outside/policy-bypass.ts"
    })).toMatchObject({ status: "fail", code: "policy.file_scope.denied" });

    fs.symlinkSync(path.join(os.tmpdir(), "missing-agentflow-policy-target"), path.join(rootPath, "src", "broken"));
    expect(evaluateAgentflowPolicy(workflow, {
      kind: "file_write",
      rootPath,
      session: "writer",
      path: "src/broken/policy-bypass.ts"
    })).toMatchObject({ status: "fail", code: "policy.file_scope.denied" });

    fs.mkdirSync(path.join(rootPath, "src", "secrets"));
    if (fs.existsSync(path.join(rootPath, "src", "Secrets"))) {
      expect(evaluateAgentflowPolicy(workflow, {
        kind: "file_write",
        rootPath,
        session: "writer",
        path: "src/Secrets/token.ts"
      })).toMatchObject({ status: "fail", code: "policy.file_scope.denied" });
    }
  });

  test("enforces cleanup approval and retention restrictions", () => {
    const workflow = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW);
    const rootPath = temporaryRepo();
    const fileRootPath = path.join(rootPath, "not-a-directory");
    fs.writeFileSync(fileRootPath, "not a directory\n", "utf8");

    for (const invalidRootPath of [
      undefined as unknown as string,
      "",
      path.join(rootPath, "missing"),
      fileRootPath
    ]) {
      expect(evaluateAgentflowPolicy(workflow, {
        kind: "cleanup",
        rootPath: invalidRootPath,
        recursive: false,
        runStatus: "completed",
        paths: ["temp/cache.json"]
      })).toEqual({
        status: "fail",
        code: "policy.input.invalid",
        message: "Cleanup checks require rootPath to identify an existing directory."
      });
    }

    expect(evaluateAgentflowPolicy(workflow, {
      kind: "cleanup",
      rootPath,
      recursive: false,
      runStatus: "completed",
      paths: ["temp/cache.json"]
    })).toMatchObject({ status: "pause", code: "policy.approval.required" });
    expect(evaluateAgentflowPolicy(workflow, {
      kind: "cleanup",
      rootPath,
      recursive: false,
      runStatus: "completed",
      paths: ["temp/cache.json"],
      ageDays: 7,
      approvalStatus: "approved"
    })).toMatchObject({ status: "allow" });

    const recursiveWildcard = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW.replace(
      "delete: [temp/**, logs/**]",
      "delete: [temp/**/*, logs/**]"
    ));
    expect(evaluateAgentflowPolicy(recursiveWildcard, {
      kind: "cleanup",
      rootPath,
      recursive: true,
      runStatus: "completed",
      paths: ["temp"],
      ageDays: 7,
      approvalStatus: "approved"
    })).toMatchObject({ status: "allow" });

    const recursiveUnion = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW.replace(
      "delete: [temp/**, logs/**]",
      "delete: [temp/*, temp/*/**, logs/**]"
    ));
    expect(evaluateAgentflowPolicy(recursiveUnion, {
      kind: "cleanup",
      rootPath,
      recursive: true,
      runStatus: "completed",
      paths: ["temp"],
      ageDays: 7,
      approvalStatus: "approved"
    })).toMatchObject({ status: "allow" });

    const classRule = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW.replace(
      "delete: [temp/**, logs/**]",
      'delete: ["temp/[a]/**", logs/**]'
    ));
    expect(evaluateAgentflowPolicy(classRule, {
      kind: "cleanup",
      rootPath,
      recursive: true,
      runStatus: "completed",
      paths: ["temp/[a]"],
      ageDays: 7,
      approvalStatus: "approved"
    })).toMatchObject({ status: "fail", code: "policy.cleanup.not_declared" });
    expect(evaluateAgentflowPolicy(workflow, {
      kind: "cleanup",
      rootPath,
      recursive: true,
      runStatus: "completed",
      paths: ["temp/["],
      ageDays: 7,
      approvalStatus: "approved"
    })).toMatchObject({ status: "allow" });
    expect(evaluateAgentflowPolicy(workflow, {
      kind: "cleanup",
      rootPath,
      recursive: true,
      runStatus: "completed",
      paths: ["temp/cache"],
      ageDays: 7,
      approvalStatus: "approved"
    })).toMatchObject({ status: "allow" });
    expect(evaluateAgentflowPolicy(workflow, {
      kind: "cleanup",
      rootPath,
      recursive: false,
      runStatus: "completed",
      paths: ["state.json"],
      ageDays: 7,
      approvalStatus: "approved"
    })).toMatchObject({ status: "fail", code: "policy.cleanup.retained" });

    const embeddedGlobstar = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW.replace(
      "keep: [state.json, final/**]",
      'keep: [state.json, final/**, "temp/**.log"]'
    ));
    expect(evaluateAgentflowPolicy(embeddedGlobstar, {
      kind: "cleanup",
      rootPath,
      recursive: true,
      runStatus: "completed",
      paths: ["temp/work"],
      ageDays: 7,
      approvalStatus: "approved"
    })).toMatchObject({ status: "fail", code: "policy.cleanup.retained" });
    expect(evaluateAgentflowPolicy(workflow, {
      kind: "cleanup",
      rootPath,
      recursive: true,
      runStatus: "completed",
      paths: ["final"],
      ageDays: 7,
      approvalStatus: "approved"
    })).toMatchObject({ status: "fail", code: "policy.cleanup.retained" });
    expect(evaluateAgentflowPolicy(workflow, {
      kind: "cleanup",
      rootPath,
      recursive: false,
      runStatus: "failed",
      paths: ["logs/failure.log"],
      approvalStatus: "approved"
    })).toMatchObject({ status: "fail", code: "policy.cleanup.retained" });
    expect(evaluateAgentflowPolicy(workflow, {
      kind: "cleanup",
      rootPath,
      recursive: false,
      runStatus: "failed",
      paths: ["logs/failure.log"],
      ageDays: 30,
      approvalStatus: "approved"
    })).toMatchObject({ status: "allow" });

    const immediate = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW.replace("after_days: 7", "after_days: 0"));
    expect(evaluateAgentflowPolicy(immediate, {
      kind: "cleanup",
      rootPath,
      recursive: false,
      runStatus: "completed",
      paths: ["temp/cache.json"],
      approvalStatus: "approved"
    })).toMatchObject({ status: "allow" });

    const wildcardKeep = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW.replace(
      "keep: [state.json, final/**]",
      "keep: [\"**/*.log\"]"
    ));
    expect(evaluateAgentflowPolicy(wildcardKeep, {
      kind: "cleanup",
      rootPath,
      recursive: false,
      runStatus: "completed",
      paths: ["temp/cache.json"],
      ageDays: 7,
      approvalStatus: "approved"
    })).toMatchObject({ status: "allow" });
    expect(evaluateAgentflowPolicy(wildcardKeep, {
      kind: "cleanup",
      rootPath,
      recursive: true,
      runStatus: "completed",
      paths: ["temp"],
      ageDays: 7,
      approvalStatus: "approved"
    })).toMatchObject({ status: "fail", code: "policy.cleanup.retained" });
  });

  test("pauses unsafe operations for approval and denies them by default", () => {
    const workflow = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW);

    expect(evaluateAgentflowPolicy(workflow, {
      kind: "unsafe_operation",
      operation: "force push"
    })).toMatchObject({ status: "pause", code: "policy.approval.required" });
    expect(evaluateAgentflowPolicy(workflow, {
      kind: "unsafe_operation",
      operation: "force push",
      approvalStatus: "approved"
    })).toMatchObject({ status: "allow" });

    const denied = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW.replace(
      "unsafe_operations: require_approval",
      "unsafe_operations: deny"
    ));
    expect(evaluateAgentflowPolicy(denied, {
      kind: "unsafe_operation",
      operation: "force push",
      approvalStatus: "approved"
    })).toMatchObject({ status: "fail", code: "policy.unsafe.denied" });
  });

  test("validation rejects unbounded model use and unscoped file writers", () => {
    const unbounded = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW.replace("  max_frontier_calls: 2\n", ""));
    const unscoped = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW.replace(
      "    file_scope:\n      include: [src/**]\n      exclude: [src/secrets/**]\n",
      ""
    ));

    expect(validateAgentflowWorkflow(unbounded).errors).toContainEqual({
      code: "workflow.policy.budget.frontier.required",
      message: 'Frontier sessions (writer) require a positive limits.max_frontier_calls budget.',
      path: "limits.max_frontier_calls"
    });
    expect(validateAgentflowWorkflow(unscoped).errors).toContainEqual({
      code: "workflow.policy.file_scope.required",
      message: 'File-writing session "writer" must declare a non-empty file_scope.include list.',
      path: "sessions.writer.file_scope.include"
    });

    const paddedProvider = parseAgentflowWorkflowOrThrow(unboundedSource().replace(
      "provider: frontier",
      "provider: \" frontier \""
    ));
    expect(validateAgentflowWorkflow(paddedProvider).errors.map((issue) => issue.code)).toContain(
      "workflow.policy.budget.frontier.required"
    );

    const dynamicProvider = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW
      .replace("provider: frontier", 'provider: "{{ inputs.provider }}"')
      .replace("  max_frontier_calls: 2\n", ""));
    expect(validateAgentflowWorkflow(dynamicProvider).errors).toContainEqual({
      code: "workflow.policy.model_usage.provider.dynamic",
      message: 'Session "writer" must declare a static provider so model budgets can be enforced before execution.',
      path: "sessions.writer.provider"
    });

    const paddedWriter = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW
      .replace("    session: writer", "    session: \" writer \"")
      .replace("    file_scope:\n      include: [src/**]\n      exclude: [src/secrets/**]\n", ""));
    expect(validateAgentflowWorkflow(paddedWriter).errors.map((issue) => issue.code)).toContain(
      "workflow.policy.file_scope.required"
    );

    const perStepAttempts = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW.replace(
      "  max_model_calls: 4",
      "  max_model_calls: 4\n  max_step_attempts: { implement: 3 }"
    ));
    expect(validateAgentflowWorkflow(perStepAttempts).errors).not.toContainEqual(expect.objectContaining({
      code: "workflow.policy.budget.invalid"
    }));

    const layeredScope = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW
      .replace("      include: [src/**]\n", "")
      .replace("policies:\n", "policies:\n  file_scope:\n    include: [src/**]\n"));
    expect(validateAgentflowWorkflow(layeredScope).errors).not.toContainEqual(expect.objectContaining({
      code: "workflow.policy.file_scope.required"
    }));

    const dynamicUnscopedWriter = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW
      .replace("    file_scope:\n      include: [src/**]\n      exclude: [src/secrets/**]\n", "")
      .replace("    session: writer", "    session: \"{{ inputs.writer }}\""));
    expect(validateAgentflowWorkflow(dynamicUnscopedWriter).errors).toContainEqual({
      code: "workflow.policy.file_scope.required",
      message: 'File-writing session "writer" must declare a non-empty file_scope.include list.',
      path: "sessions.writer.file_scope.include"
    });

    const disjointScope = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW.replace(
      "policies:\n",
      "policies:\n  file_scope:\n    include: [docs/**]\n"
    ));
    expect(validateAgentflowWorkflow(disjointScope).errors).toContainEqual({
      code: "workflow.policy.file_scope.disjoint",
      message: 'File-writing session "writer" has no writable path shared with policies.file_scope.include.',
      path: "sessions.writer.file_scope.include"
    });

    const complexDisjointScope = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW
      .replace("      include: [src/**]", "      include: [docs/**]")
      .replace("policies:\n", 'policies:\n  file_scope:\n    include: ["src/{a,b}/**"]\n'));
    expect(validateAgentflowWorkflow(complexDisjointScope).errors).toContainEqual({
      code: "workflow.policy.file_scope.disjoint",
      message: 'File-writing session "writer" has no writable path shared with policies.file_scope.include.',
      path: "sessions.writer.file_scope.include"
    });

    const complexIntersectingScope = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW
      .replace("      include: [src/**]", "      include: [src/a/**]")
      .replace("policies:\n", 'policies:\n  file_scope:\n    include: ["src/{a,b}/**"]\n'));
    expect(validateAgentflowWorkflow(complexIntersectingScope).errors).not.toContainEqual(expect.objectContaining({
      code: "workflow.policy.file_scope.disjoint"
    }));

    const fullyExcludedScope = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW
      .replace("      include: [src/**]", "      include: [src/public/**]")
      .replace("policies:\n", "policies:\n  file_scope:\n    include: [src/public/**]\n    exclude: [src/public/**]\n"));
    expect(validateAgentflowWorkflow(fullyExcludedScope).errors).toContainEqual({
      code: "workflow.policy.file_scope.disjoint",
      message: 'File-writing session "writer" has no writable path shared with policies.file_scope.include.',
      path: "sessions.writer.file_scope.include"
    });

    const deniedProvider = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW.replace(
      "allowed_providers: [local, frontier]",
      "allowed_providers: [local]"
    ));
    expect(validateAgentflowWorkflow(deniedProvider).errors).toContainEqual({
      code: "workflow.policy.model_usage.provider.denied",
      message: 'Session "writer" uses provider "frontier", which is not in policies.model_usage.allowed_providers.',
      path: "sessions.writer.provider"
    });
  });

  test("validation rejects malformed policy and retention shapes", () => {
    const workflow = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW
      .replace("  model_usage:\n    allowed_providers: [local, frontier]\n", "  file_scope: all\n")
      .replace("    delete: [temp/**, logs/**]", "    delete: [../outside/**]"));

    expect(validateAgentflowWorkflow(workflow).errors).toEqual(expect.arrayContaining([
      {
        code: "workflow.policy.file_scope.invalid",
        message: "Workflow file scope policy must be a mapping.",
        path: "policies.file_scope"
      },
      {
        code: "workflow.policy.retention.invalid",
        message: 'Retention path pattern "../outside/**" must be a supported relative glob and stay inside the run directory.',
        path: "retention.on_success.delete[0]"
      }
    ]));
    expect(evaluateAgentflowPolicy(workflow, {
      kind: "unsafe_operation",
      operation: "force push"
    })).toMatchObject({ status: "fail", code: "policy.configuration.invalid" });

    const emptyRetentionList = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW.replace(
      "delete: [temp/**, logs/**]",
      "delete: []"
    ));
    expect(validateAgentflowWorkflow(emptyRetentionList).errors).toContainEqual({
      code: "workflow.policy.retention.invalid",
      message: "Retention delete must be a non-empty list of run-directory-relative patterns.",
      path: "retention.on_success.delete"
    });

    const controlCharacterPattern = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW.replace(
      "include: [src/**]",
      'include: ["src/line\\nbreak/**"]'
    ));
    expect(validateAgentflowWorkflow(controlCharacterPattern).errors).toContainEqual({
      code: "workflow.policy.file_scope.invalid",
      message: 'File scope pattern "src/line\\nbreak/**" must be a supported repo-relative glob and stay inside the repository.',
      path: "sessions.writer.file_scope.include[0]"
    });

    const invalidRange = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW.replace(
      "include: [src/**]",
      "include: [\"src/[z-a].ts\"]"
    ));
    expect(validateAgentflowWorkflow(invalidRange).errors).toContainEqual({
      code: "workflow.policy.file_scope.invalid",
      message: 'File scope pattern "src/[z-a].ts" must be a supported repo-relative glob and stay inside the repository.',
      path: "sessions.writer.file_scope.include[0]"
    });
    expect(evaluateAgentflowPolicy(invalidRange, {
      kind: "file_write",
      rootPath: temporaryRepo(),
      session: "writer",
      path: "src/x.ts"
    })).toMatchObject({ status: "fail", code: "policy.configuration.invalid" });

    const paddedStepBudget = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW.replace(
      "  max_model_calls: 4",
      "  max_model_calls: 4\n  max_step_attempts: { \" implement \" : 3 }"
    ));
    expect(validateAgentflowWorkflow(paddedStepBudget).errors).toContainEqual({
      code: "workflow.policy.budget.invalid",
      message: "Budget limit limits.max_step_attempts must map declared canonical step names to positive finite numbers.",
      path: "limits.max_step_attempts"
    });

    const unknownStepBudget = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW.replace(
      "  max_model_calls: 4",
      "  max_model_calls: 4\n  max_step_attempts: { implemnt: 3 }"
    ));
    expect(validateAgentflowWorkflow(unknownStepBudget).errors).toContainEqual({
      code: "workflow.policy.budget.invalid",
      message: "Budget limit limits.max_step_attempts must map declared canonical step names to positive finite numbers.",
      path: "limits.max_step_attempts"
    });

    const malformedSessionScope = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW
      .replace("    file_scope:\n      include: [src/**]\n      exclude: [src/secrets/**]", "    file_scope: malformed")
      .replace("policies:\n", "policies:\n  file_scope:\n    include: [src/**]\n"));
    expect(evaluateAgentflowPolicy(malformedSessionScope, {
      kind: "file_write",
      rootPath: temporaryRepo(),
      session: "writer",
      path: "src/x.ts"
    })).toMatchObject({ status: "fail", code: "policy.configuration.invalid" });

    const invalidBranchScope = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW.replace(
      "steps:\n  - id: implement",
      "steps:\n  - id: parallel\n    type: parallel\n    branches:\n      - id: branch\n        session: writer\n        file_scope: { include: [\"src/[z-a].ts\"] }\n  - id: implement"
    ));
    expect(validateAgentflowWorkflow(invalidBranchScope).errors).toContainEqual({
      code: "workflow.policy.file_scope.invalid",
      message: 'File scope pattern "src/[z-a].ts" must be a supported repo-relative glob and stay inside the repository.',
      path: "steps[0].branches[0].file_scope.include[0]"
    });

    const escapingBrace = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW.replace(
      "include: [src/**]",
      "include: [\"{src,../outside}/**\"]"
    ));
    expect(validateAgentflowWorkflow(escapingBrace).errors.map((issue) => issue.code)).toContain(
      "workflow.policy.file_scope.invalid"
    );
  });

  test("run creation performs policy preflight before persisting state", async () => {
    const repoRoot = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW.replace("  max_frontier_calls: 2\n", ""));
    const store = await openAgentflowRunState({ cwd: repoRoot });

    expect(() => createAgentflowLifecycleRun(store, { id: "unsafe-run", workflow }))
      .toThrow(AgentflowRunStateError);
    expect(() => createAgentflowLifecycleRun(store, { id: "unsafe-run", workflow }))
      .toThrow("cannot start because workflow validation failed");
    expect(store.getRun("unsafe-run")).toBeNull();

    const malformed = parseAgentflowWorkflowOrThrow(POLICY_WORKFLOW.replace(
      "- id: implement\n    type: session_request",
      "- type: session_request"
    ));
    try {
      createAgentflowLifecycleRun(store, { id: "malformed-run", workflow: malformed });
      throw new Error("Expected malformed workflow preflight to fail.");
    } catch (error) {
      expect(error).toBeInstanceOf(AgentflowRunStateError);
      expect((error as AgentflowRunStateError).code).toBe("AGENTFLOW_WORKFLOW_INVALID");
      expect((error as Error).message).toContain("workflow.step.id.required");
    }
    expect(store.getRun("malformed-run")).toBeNull();
    store.close();
  });
});

function temporaryRepo(): string {
  const repoRoot = fs.mkdtempSync(path.join(os.tmpdir(), "agentflow-policy-"));
  fs.mkdirSync(path.join(repoRoot, ".git"));
  return repoRoot;
}

function unboundedSource(): string {
  return POLICY_WORKFLOW.replace("  max_frontier_calls: 2\n", "");
}
