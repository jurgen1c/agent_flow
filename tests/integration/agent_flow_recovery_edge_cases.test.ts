import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFileSync } from "node:child_process";

import {
  createAgentFlowLifecycleRun,
  createAgentFlowSessionProviderRegistry,
  createAgentFlowWorkflowRegistry,
  executeAgentFlowCommandPipeline,
  injectAgentFlowRecoveryContext,
  openAgentFlowRunState,
  parseAgentFlowWorkflowOrThrow,
  transitionAgentFlowLifecycleRun,
  type AgentFlowSessionProviderRequest,
  type AgentFlowSessionProviderResponse
} from "../../src/index";

describe("Agent Flow recovery edge-case protections", () => {
  test("pauses when remediation changes a file outside its authorized scope", async () => {
    const root = temporaryRepo();
    writePrompt(root);
    const workflow = recoverySessionWorkflow(`
sessions:
  fixer:
    provider: fixture
    authority: { can_modify_files: true }
    file_scope: { include: [src/**] }
steps:
  - id: check
    type: command
    command: "false"
    on_failure:
      route_to:
        session: fixer
        prompt: prompts/fix.md
        file_scope: { include: [src/**] }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "unrelated-files", workflow });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      fs.writeFileSync(path.join(root, "README.md"), "unrelated remediation\n");
      return remediated();
    });

    const result = await executeAgentFlowCommandPipeline(
      store, "unrelated-files", workflow, undefined, providers
    );

    expect(result).toMatchObject({
      status: "paused",
      failedStep: "check",
      failureOutcome: "pause",
      message: expect.stringContaining("README.md")
    });
    expect(store.listFailures("unrelated-files")).toContainEqual(expect.objectContaining({
      classification: "recovery_unrelated_files"
    }));
    expect(store.listEvents("unrelated-files")).toContainEqual(expect.objectContaining({
      type: "recovery.workspace_scope_violated",
      payload: expect.objectContaining({ deniedPaths: ["README.md"] })
    }));
    store.close();
  });

  test("allows remediation changes explicitly authorized by the layered file scope", async () => {
    const root = temporaryRepo();
    writePrompt(root);
    fs.mkdirSync(path.join(root, "src"));
    const workflow = recoverySessionWorkflow(`
sessions:
  fixer:
    provider: fixture
    authority: { can_modify_files: true }
    file_scope: { include: [src/**] }
steps:
  - id: check
    type: command
    command: "false"
    on_failure:
      route_to:
        session: fixer
        prompt: prompts/fix.md
        file_scope: { include: [src/fix.ts] }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "scoped-files", workflow });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      fs.writeFileSync(path.join(root, "src/fix.ts"), "export const fixed = true;\n");
      return remediated();
    });

    const result = await executeAgentFlowCommandPipeline(
      store, "scoped-files", workflow, undefined, providers
    );

    expect(result).toEqual({ status: "completed", completedSteps: [] });
    expect(store.listEvents("scoped-files").map((event) => event.type))
      .not.toContain("recovery.workspace_scope_violated");
    store.close();
  });

  test("intersects the failed step scope with a routed remediation scope", async () => {
    const root = temporaryRepo();
    writePrompt(root);
    fs.mkdirSync(path.join(root, "src"));
    const workflow = recoverySessionWorkflow(`
sessions:
  fixer:
    provider: fixture
    authority: { can_modify_files: true }
    file_scope: { include: [src/**] }
steps:
  - id: check
    type: command
    command: "false"
    file_scope: { include: [src/narrow/**] }
    on_failure:
      route_to:
        session: fixer
        prompt: prompts/fix.md
        file_scope: { include: [src/**] }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "inherited-scope", workflow });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      fs.writeFileSync(path.join(root, "src/outside.ts"), "outside inherited scope\n");
      return remediated();
    });

    const result = await executeAgentFlowCommandPipeline(
      store, "inherited-scope", workflow, undefined, providers
    );

    expect(result).toMatchObject({
      status: "paused",
      message: expect.stringContaining("src/outside.ts")
    });
    expect(store.listFailures("inherited-scope")).toContainEqual(expect.objectContaining({
      classification: "recovery_unrelated_files"
    }));
    store.close();
  });

  test("combines exclusion-only recovery scope layers with their inherited include", async () => {
    const root = temporaryRepo();
    writePrompt(root);
    fs.mkdirSync(path.join(root, "src"));
    const workflow = recoverySessionWorkflow(`
sessions:
  fixer:
    provider: fixture
    authority: { can_modify_files: true }
    file_scope: { include: [src/**] }
steps:
  - id: check
    type: command
    command: "false"
    file_scope: { include: [src/**] }
    on_failure:
      route_to:
        session: fixer
        prompt: prompts/fix.md
        file_scope: { exclude: [src/private/**] }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "exclusion-layer", workflow });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      fs.writeFileSync(path.join(root, "src/fix.ts"), "authorized\n");
      return remediated();
    });

    const result = await executeAgentFlowCommandPipeline(
      store, "exclusion-layer", workflow, undefined, providers
    );

    expect(result).toEqual({ status: "completed", completedSteps: [] });
    store.close();
  });

  test("detects ignored remediation changes inside a tracked Git submodule", async () => {
    const { root, submodule } = temporaryRepoWithSubmodule();
    writePrompt(root);
    const workflow = recoverySessionWorkflow(`
sessions:
  fixer:
    provider: fixture
    authority: { can_modify_files: true }
    file_scope: { include: [src/**] }
steps:
  - id: check
    type: command
    command: "false"
    on_failure:
      route_to: { session: fixer, prompt: prompts/fix.md, file_scope: { include: [src/**] } }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "submodule-change", workflow });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      fs.writeFileSync(path.join(submodule, "ignored.txt"), "changed by remediation\n");
      return remediated();
    });

    const result = await executeAgentFlowCommandPipeline(
      store, "submodule-change", workflow, undefined, providers
    );

    expect(result).toMatchObject({ status: "paused", message: expect.stringContaining("vendor/plugin") });
    store.close();
  });

  test("allows scoped remediation changes inside a tracked Git submodule", async () => {
    const { root, submodule } = temporaryRepoWithSubmodule();
    writePrompt(root);
    const workflow = recoverySessionWorkflow(`
sessions:
  fixer:
    provider: fixture
    authority: { can_modify_files: true }
    file_scope: { include: [vendor/plugin/**] }
steps:
  - id: check
    type: command
    command: "false"
    on_failure:
      route_to:
        session: fixer
        prompt: prompts/fix.md
        file_scope: { include: [vendor/plugin/**] }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "scoped-submodule-change", workflow });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      fs.writeFileSync(path.join(submodule, "tracked.txt"), "authorized remediation\n");
      return remediated();
    });

    const result = await executeAgentFlowCommandPipeline(
      store, "scoped-submodule-change", workflow, undefined, providers
    );

    expect(result).toEqual({ status: "completed", completedSteps: [] });
    store.close();
  });

  test("authorizes a submodule commit change against its exact gitlink path", async () => {
    const { root, submodule } = temporaryRepoWithSubmodule();
    writePrompt(root);
    const workflow = recoverySessionWorkflow(`
sessions:
  fixer:
    provider: fixture
    authority: { can_modify_files: true }
    file_scope: { include: [vendor/plugin] }
steps:
  - id: check
    type: command
    command: "false"
    on_failure:
      route_to:
        session: fixer
        prompt: prompts/fix.md
        file_scope: { include: [vendor/plugin] }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "exact-gitlink-change", workflow });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      fs.writeFileSync(path.join(submodule, "tracked.txt"), "new submodule commit\n");
      git(submodule, "add", "tracked.txt");
      git(submodule, "-c", "user.name=Agent Flow", "-c", "user.email=agent-flow@example.test", "commit", "-m", "remediation");
      return remediated();
    });

    const result = await executeAgentFlowCommandPipeline(
      store, "exact-gitlink-change", workflow, undefined, providers
    );

    expect(result).toEqual({ status: "completed", completedSteps: [] });
    store.close();
  });

  test("does not hide a dirty submodule descendant behind an authorized gitlink change", async () => {
    const { root, submodule } = temporaryRepoWithSubmodule();
    writePrompt(root);
    const workflow = recoverySessionWorkflow(`
sessions:
  fixer:
    provider: fixture
    authority: { can_modify_files: true }
    file_scope: { include: [vendor/plugin] }
steps:
  - id: check
    type: command
    command: "false"
    on_failure:
      route_to:
        session: fixer
        prompt: prompts/fix.md
        file_scope: { include: [vendor/plugin] }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "gitlink-with-dirty-descendant", workflow });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      fs.writeFileSync(path.join(submodule, "tracked.txt"), "new submodule commit\n");
      git(submodule, "add", "tracked.txt");
      git(submodule, "-c", "user.name=Agent Flow", "-c", "user.email=agent-flow@example.test", "commit", "-m", "remediation");
      fs.writeFileSync(path.join(submodule, "ignored.txt"), "unauthorized dirty file\n");
      return remediated();
    });

    const result = await executeAgentFlowCommandPipeline(
      store, "gitlink-with-dirty-descendant", workflow, undefined, providers
    );

    expect(result).toMatchObject({
      status: "paused",
      message: expect.stringContaining("vendor/plugin/ignored.txt")
    });
    store.close();
  });

  test("detects remediation changes to ignored workspace files", async () => {
    const root = temporaryGitRepo();
    writePrompt(root);
    fs.appendFileSync(path.join(root, ".gitignore"), ".env\ndist/\n");
    git(root, "add", ".gitignore", "prompts/fix.md");
    git(root, "-c", "user.name=Agent Flow", "-c", "user.email=agent-flow@example.test", "commit", "-m", "fixture");
    const workflow = recoverySessionWorkflow(`
sessions:
  fixer:
    provider: fixture
    authority: { can_modify_files: true }
    file_scope: { include: [src/**] }
steps:
  - id: check
    type: command
    command: "false"
    on_failure:
      route_to: { session: fixer, prompt: prompts/fix.md, file_scope: { include: [src/**] } }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "ignored-change", workflow });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      fs.writeFileSync(path.join(root, ".env"), "TOKEN=unsafe\n");
      fs.mkdirSync(path.join(root, "dist"));
      fs.writeFileSync(path.join(root, "dist/generated.js"), "unauthorized output\n");
      return remediated();
    });

    const result = await executeAgentFlowCommandPipeline(
      store, "ignored-change", workflow, undefined, providers
    );

    expect(result).toMatchObject({ status: "paused", message: expect.stringContaining("dist/generated.js") });
    expect(store.listEvents("ignored-change")).toContainEqual(expect.objectContaining({
      type: "recovery.workspace_scope_violated",
      payload: expect.objectContaining({ deniedPaths: [".env", "dist/generated.js"] })
    }));
    store.close();
  });

  test("detects staging a pre-existing out-of-scope workspace change", async () => {
    const root = temporaryGitRepo();
    writePrompt(root);
    fs.writeFileSync(path.join(root, "README.md"), "baseline\n");
    git(root, "add", ".gitignore", "README.md", "prompts/fix.md");
    git(root, "-c", "user.name=Agent Flow", "-c", "user.email=agent-flow@example.test", "commit", "-m", "fixture");
    fs.writeFileSync(path.join(root, "README.md"), "dirty before remediation\n");
    const workflow = recoverySessionWorkflow(`
sessions:
  fixer:
    provider: fixture
    authority: { can_modify_files: true }
    file_scope: { include: [src/**] }
steps:
  - id: check
    type: command
    command: "false"
    on_failure:
      route_to: { session: fixer, prompt: prompts/fix.md, file_scope: { include: [src/**] } }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "staged-preexisting-change", workflow });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      git(root, "add", "README.md");
      return remediated();
    });

    const result = await executeAgentFlowCommandPipeline(
      store, "staged-preexisting-change", workflow, undefined, providers
    );

    expect(result).toMatchObject({ status: "paused", message: expect.stringContaining("README.md") });
    store.close();
  });

  test("stops a repeated identical failure at the configured recovery-cycle limit", async () => {
    const root = temporaryRepo();
    writePrompt(root);
    const workflow = recoverySessionWorkflow(`
limits: { max_recovery_cycles: 1 }
sessions:
  fixer: { provider: fixture }
steps:
  - id: check
    type: command
    command: "false"
    on_failure:
      route_to: { session: fixer, prompt: prompts/fix.md }
      on_remediated: { return_to: check }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "repeated-failure", workflow });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", remediated);

    const result = await executeAgentFlowCommandPipeline(
      store, "repeated-failure", workflow, undefined, providers
    );

    expect(result).toMatchObject({ status: "paused", failedStep: "check" });
    expect(store.listEvents("repeated-failure")).toContainEqual(expect.objectContaining({
      type: "recovery.limit_reached"
    }));
    store.close();
  });

  test("pauses before an unknown triage classification can select an else route", async () => {
    const root = temporaryRepo();
    const workflow = recoverySessionWorkflow(`
steps:
  - id: route
    type: condition
    if: artifacts.failure_classification.kind == "flake"
    then: complete
    else: complete
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "unknown-triage", workflow });
    store.writeArtifact({
      id: "classification",
      runId: "unknown-triage",
      stepId: "classify",
      path: "failure-classification.json",
      kind: "session_output",
      contentType: "application/json; charset=utf-8",
      content: JSON.stringify({
        kind: "unknown",
        confidence: "low",
        summary: "Could not classify",
        recommended_owner: "user",
        safe_to_retry: false,
        requires_user: true
      })
    });

    const result = await executeAgentFlowCommandPipeline(store, "unknown-triage", workflow);

    expect(result).toMatchObject({ status: "paused", failedStep: "route" });
    expect(store.listFailures("unknown-triage")[0]?.classification)
      .toBe("failure_classification_unknown");
    store.close();
  });

  test("redacts secret-bearing failure logs before recovery session input", async () => {
    const root = temporaryRepo();
    writePrompt(root);
    const workflow = recoverySessionWorkflow(`
sessions:
  fixer: { provider: fixture }
steps:
  - id: check
    type: command
    command: >-
      sh -c "printf 'MY_API_TOKEN=session-input-secret\\n' >&2; exit 1"
    on_failure:
      route_to:
        session: fixer
        prompt: prompts/fix.md
        inputs: { failure_payload: "{{ failure.path }}" }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "redacted-input", workflow });
    let sessionInput = "";
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => {
      sessionInput = request.inputs.map((input) => Buffer.from(input.content).toString("utf8")).join("\n");
      return { outputs: {}, metadata: { recovery_status: "unresolved" } };
    });

    await executeAgentFlowCommandPipeline(store, "redacted-input", workflow, undefined, providers);

    expect(sessionInput).toContain("[REDACTED]");
    expect(sessionInput).not.toContain("session-input-secret");
    store.close();
  });

  test("routes every failed or paused nested recovery outcome through on_unresolved", async () => {
    const root = temporaryRepo();
    const child = recoverySessionWorkflow(`
steps:
  - { id: nested_check, type: command, command: "false" }
`);
    const parent = recoverySessionWorkflow(`
steps:
  - id: check
    type: command
    command: "false"
    on_failure:
      route_to: { workflow: nested }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "nested-unresolved", workflow: parent });

    const result = await executeAgentFlowCommandPipeline(
      store,
      "nested-unresolved",
      parent,
      undefined,
      undefined,
      undefined,
      undefined,
      createAgentFlowWorkflowRegistry().register("nested", child)
    );

    expect(result).toMatchObject({ status: "paused" });
    expect(store.listEvents("nested-unresolved")).toContainEqual(expect.objectContaining({
      type: "recovery.completed",
      payload: expect.objectContaining({ status: "unresolved", route: "workflow" })
    }));
    store.close();
  });

  test("marks injected recovery context dirty and reruns remediation with that context", async () => {
    const root = temporaryRepo();
    writePrompt(root);
    const workflow = recoverySessionWorkflow(`
limits: { max_frontier_calls: 3 }
sessions:
  fixer: { provider: frontier, resume: true }
steps:
  - id: check
    type: command
    command: "false"
    on_failure:
      route_to: { session: fixer, prompt: prompts/fix.md }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "injected-context", workflow });
    let resolveFirst!: (response: AgentFlowSessionProviderResponse) => void;
    const firstResponse = new Promise<AgentFlowSessionProviderResponse>((resolve) => {
      resolveFirst = resolve;
    });
    const requests: AgentFlowSessionProviderRequest[] = [];
    const providers = createAgentFlowSessionProviderRegistry().register("frontier", async (request) => {
      requests.push(request);
      return requests.length === 1 ? firstResponse : remediated();
    });
    const execution = executeAgentFlowCommandPipeline(
      store, "injected-context", workflow, undefined, providers
    );
    await waitFor(() => store.getSession("injected-context", "fixer")?.status === "running");

    const injectedText = "  The failure only occurs when FEATURE_FLAG is enabled.  \n";
    const dirty = injectAgentFlowRecoveryContext(
      store,
      "injected-context",
      "fixer",
      injectedText
    );
    expect(dirty.state).toMatchObject({ dirty: true, contextRevision: 1 });
    expect(dirty.state.contextInjections).toContainEqual(expect.objectContaining({ context: injectedText }));
    resolveFirst({ ...remediated(), externalSessionId: "resumable-recovery" });
    const result = await execution;

    expect(result.status).toBe("completed");
    expect(requests).toHaveLength(2);
    expect(requests[1]!.externalSessionId).toBe("resumable-recovery");
    expect(requests[1]!.inputs).toContainEqual(expect.objectContaining({
      path: "recovery-context/injected.md"
    }));
    expect(Buffer.from(requests[1]!.inputs.at(-1)!.content).toString("utf8"))
      .toContain("FEATURE_FLAG is enabled");
    expect(store.getSession("injected-context", "fixer")?.state).toMatchObject({
      dirty: false,
      contextRevision: 1,
      appliedContextRevision: 1
    });
    expect(store.listEvents("injected-context").map((event) => event.type))
      .toEqual(expect.arrayContaining(["recovery.context.injected", "recovery.context.rerun"]));
    store.close();
  });

  test("reruns when context races with atomic recovery-session settlement", async () => {
    const root = temporaryRepo();
    writePrompt(root);
    const workflow = recoverySessionWorkflow(`
limits: { max_frontier_calls: 3 }
sessions:
  fixer: { provider: frontier, resume: true }
steps:
  - id: check
    type: command
    command: "false"
    on_failure:
      route_to: { session: fixer, prompt: prompts/fix.md }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "settlement-race", workflow });
    const requests: AgentFlowSessionProviderRequest[] = [];
    const providers = createAgentFlowSessionProviderRegistry().register("frontier", (request) => {
      requests.push(request);
      return remediated();
    });
    const settle = store.settleRecoverySessionForRunAtContextRevision.bind(store);
    let injected = false;
    store.settleRecoverySessionForRunAtContextRevision = (input, revision) => {
      if (!injected) {
        injected = true;
        injectAgentFlowRecoveryContext(
          store,
          "settlement-race",
          "fixer",
          "Context accepted immediately before settlement."
        );
      }
      return settle(input, revision);
    };

    const result = await executeAgentFlowCommandPipeline(
      store, "settlement-race", workflow, undefined, providers
    );

    expect(result.status).toBe("completed");
    expect(requests).toHaveLength(2);
    expect(Buffer.from(requests[1]!.inputs.at(-1)!.content).toString("utf8"))
      .toContain("immediately before settlement");
    expect(store.getSession("settlement-race", "fixer")?.state).toMatchObject({
      dirty: false,
      contextRevision: 1,
      appliedContextRevision: 1
    });
    store.close();
  });

  test("reruns injected context before rejecting an invalid stale provider response", async () => {
    const root = temporaryRepo();
    writePrompt(root);
    const workflow = recoverySessionWorkflow(`
limits: { max_frontier_calls: 3 }
sessions:
  fixer: { provider: frontier, resume: true }
steps:
  - id: check
    type: command
    command: "false"
    on_failure:
      route_to: { session: fixer, prompt: prompts/fix.md }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "invalid-stale-response", workflow });
    let resolveFirst!: (response: AgentFlowSessionProviderResponse) => void;
    const firstResponse = new Promise<AgentFlowSessionProviderResponse>((resolve) => {
      resolveFirst = resolve;
    });
    const requests: AgentFlowSessionProviderRequest[] = [];
    const providers = createAgentFlowSessionProviderRegistry().register("frontier", async (request) => {
      requests.push(request);
      return requests.length === 1 ? firstResponse : remediated();
    });
    const execution = executeAgentFlowCommandPipeline(
      store, "invalid-stale-response", workflow, undefined, providers
    );
    await waitFor(() => store.getSession("invalid-stale-response", "fixer")?.status === "running");
    injectAgentFlowRecoveryContext(store, "invalid-stale-response", "fixer", "Retry with accepted context.");
    resolveFirst(null as unknown as AgentFlowSessionProviderResponse);

    const result = await execution;

    expect(result.status).toBe("completed");
    expect(requests).toHaveLength(2);
    expect(Buffer.from(requests[1]!.inputs.at(-1)!.content).toString("utf8"))
      .toContain("Retry with accepted context");
    store.close();
  });

  test("reruns injected context after a stale provider rejection", async () => {
    const root = temporaryRepo();
    writePrompt(root);
    const workflow = recoverySessionWorkflow(`
limits: { max_frontier_calls: 3 }
sessions:
  fixer: { provider: frontier, resume: true }
steps:
  - id: check
    type: command
    command: "false"
    on_failure:
      route_to: { session: fixer, prompt: prompts/fix.md }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "rejected-stale-response", workflow });
    let rejectFirst!: (error: Error) => void;
    const firstResponse = new Promise<AgentFlowSessionProviderResponse>((_resolve, reject) => {
      rejectFirst = reject;
    });
    const requests: AgentFlowSessionProviderRequest[] = [];
    const providers = createAgentFlowSessionProviderRegistry().register("frontier", async (request) => {
      requests.push(request);
      return requests.length === 1 ? firstResponse : remediated();
    });
    const execution = executeAgentFlowCommandPipeline(
      store, "rejected-stale-response", workflow, undefined, providers
    );
    await waitFor(() => store.getSession("rejected-stale-response", "fixer")?.status === "running");
    injectAgentFlowRecoveryContext(store, "rejected-stale-response", "fixer", "Retry after rejection.");
    rejectFirst(new Error("stale provider failure"));

    const result = await execution;

    expect(result.status).toBe("completed");
    expect(requests).toHaveLength(2);
    expect(Buffer.from(requests[1]!.inputs.at(-1)!.content).toString("utf8"))
      .toContain("Retry after rejection");
    store.close();
  });

  test("does not carry a stale provider session ID into a non-resumable rerun", async () => {
    const root = temporaryRepo();
    writePrompt(root);
    const workflow = recoverySessionWorkflow(`
limits: { max_frontier_calls: 3 }
sessions:
  fixer: { provider: frontier }
steps:
  - id: check
    type: command
    command: "false"
    on_failure:
      route_to: { session: fixer, prompt: prompts/fix.md }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "non-resumable-rerun", workflow });
    let resolveFirst!: (response: AgentFlowSessionProviderResponse) => void;
    const firstResponse = new Promise<AgentFlowSessionProviderResponse>((resolve) => {
      resolveFirst = resolve;
    });
    const requests: AgentFlowSessionProviderRequest[] = [];
    const providers = createAgentFlowSessionProviderRegistry().register("frontier", async (request) => {
      requests.push(request);
      return requests.length === 1 ? firstResponse : remediated();
    });
    const execution = executeAgentFlowCommandPipeline(
      store, "non-resumable-rerun", workflow, undefined, providers
    );
    await waitFor(() => store.getSession("non-resumable-rerun", "fixer")?.status === "running");
    injectAgentFlowRecoveryContext(store, "non-resumable-rerun", "fixer", "Retry without resuming.");
    resolveFirst({ ...remediated(), externalSessionId: "must-not-resume" });

    expect((await execution).status).toBe("completed");
    expect(requests).toHaveLength(2);
    expect(requests[1]!.externalSessionId).toBeUndefined();
    store.close();
  });

  test("rejects authored recovery inputs that collide with injected context", async () => {
    const root = temporaryRepo();
    writePrompt(root);
    const workflow = recoverySessionWorkflow(`
sessions:
  fixer: { provider: fixture }
steps:
  - id: check
    type: command
    command: "false"
    on_failure:
      route_to:
        session: fixer
        prompt: prompts/fix.md
        inputs: { context: recovery-context/injected.md }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "context-path-collision", workflow });
    store.writeArtifact({
      id: "colliding-context",
      runId: "context-path-collision",
      stepId: "fixture",
      path: "recovery-context/injected.md",
      kind: "fixture",
      contentType: "text/markdown; charset=utf-8",
      content: "authored collision\n"
    });
    let calls = 0;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      calls += 1;
      return remediated();
    });

    const result = await executeAgentFlowCommandPipeline(
      store, "context-path-collision", workflow, undefined, providers
    );

    expect(result).toMatchObject({ status: "paused" });
    expect(store.listEvents("context-path-collision")).toContainEqual(expect.objectContaining({
      type: "recovery.completed",
      payload: expect.objectContaining({ message: expect.stringContaining("reserved") })
    }));
    expect(calls).toBe(0);
    store.close();
  });

  test("rejects context injection into ordinary sessions with recovery-like step IDs", async () => {
    const root = temporaryRepo();
    const workflow = recoverySessionWorkflow(`
sessions:
  worker: { provider: fixture }
steps:
  - id: "ordinary:recovery"
    type: session_request
    session: worker
    prompt: prompts/fix.md
    inputs: [request.md]
    outputs: [response.md]
`);
    const store = await openAgentFlowRunState({ cwd: root });
    writePrompt(root);
    createAgentFlowLifecycleRun(store, { id: "ordinary-session", workflow });
    transitionAgentFlowLifecycleRun(store, "ordinary-session", "resume");
    store.upsertSession({
      id: "worker",
      runId: "ordinary-session",
      stepId: "ordinary:recovery",
      provider: "fixture",
      status: "running",
      state: { requestArtifact: "request.json" }
    });

    expect(() => injectAgentFlowRecoveryContext(store, "ordinary-session", "worker", "not recovery"))
      .toThrow("not the active recovery remediation session");
    store.close();
  });

  test("bounds cumulative injected recovery context atomically", async () => {
    const root = temporaryRepo();
    writePrompt(root);
    const workflow = recoverySessionWorkflow(`
limits: { max_frontier_calls: 3 }
sessions:
  fixer: { provider: frontier, resume: true }
steps:
  - id: check
    type: command
    command: "false"
    on_failure:
      route_to: { session: fixer, prompt: prompts/fix.md }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "bounded-context", workflow });
    let resolveFirst!: (response: AgentFlowSessionProviderResponse) => void;
    const firstResponse = new Promise<AgentFlowSessionProviderResponse>((resolve) => {
      resolveFirst = resolve;
    });
    let calls = 0;
    const providers = createAgentFlowSessionProviderRegistry().register("frontier", () => {
      calls += 1;
      return calls === 1 ? firstResponse : remediated();
    });
    const execution = executeAgentFlowCommandPipeline(
      store, "bounded-context", workflow, undefined, providers
    );
    await waitFor(() => store.getSession("bounded-context", "fixer")?.status === "running");
    injectAgentFlowRecoveryContext(store, "bounded-context", "fixer", "a".repeat(40 * 1024));

    expect(() => injectAgentFlowRecoveryContext(
      store, "bounded-context", "fixer", "b".repeat(25 * 1024)
    )).toThrow("aggregate limit");
    expect(store.getSession("bounded-context", "fixer")?.state.contextRevision).toBe(1);
    resolveFirst(remediated());
    expect((await execution).status).toBe("completed");
    store.close();
  });

  test("audits workspace changes before pausing a context rerun at its model budget", async () => {
    const root = temporaryRepo();
    writePrompt(root);
    const workflow = recoverySessionWorkflow(`
limits: { max_frontier_calls: 1 }
sessions:
  fixer:
    provider: frontier
    resume: true
    authority: { can_modify_files: true }
    file_scope: { include: [src/**] }
steps:
  - id: check
    type: command
    command: "false"
    on_failure:
      route_to: { session: fixer, prompt: prompts/fix.md, file_scope: { include: [src/**] } }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "budgeted-rerun-audit", workflow });
    let resolveFirst!: (response: AgentFlowSessionProviderResponse) => void;
    const firstResponse = new Promise<AgentFlowSessionProviderResponse>((resolve) => {
      resolveFirst = resolve;
    });
    const providers = createAgentFlowSessionProviderRegistry().register("frontier", () => {
      fs.writeFileSync(path.join(root, "README.md"), "unauthorized\n");
      return firstResponse;
    });
    const execution = executeAgentFlowCommandPipeline(
      store, "budgeted-rerun-audit", workflow, undefined, providers
    );
    await waitFor(() => store.getSession("budgeted-rerun-audit", "fixer")?.status === "running");
    injectAgentFlowRecoveryContext(store, "budgeted-rerun-audit", "fixer", "Retry after this context.");
    resolveFirst(remediated());

    const result = await execution;

    expect(result).toMatchObject({ status: "paused", message: expect.stringContaining("README.md") });
    expect(store.listFailures("budgeted-rerun-audit")).toContainEqual(expect.objectContaining({
      classification: "recovery_unrelated_files"
    }));
    expect(store.listEvents("budgeted-rerun-audit").map((event) => event.type))
      .not.toContain("recovery.context.rerun");
    store.close();
  });
});

function recoverySessionWorkflow(body: string) {
  return parseAgentFlowWorkflowOrThrow(`name: recovery-edge-case
version: 1
style: recovery_pipeline
maturity: experimental
${body}`);
}

function temporaryRepo(): string {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "agent-flow-recovery-edge-"));
  fs.mkdirSync(path.join(root, ".git"));
  return root;
}

function temporaryGitRepo(): string {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "agent-flow-recovery-git-"));
  git(root, "init");
  fs.writeFileSync(path.join(root, ".gitignore"), ".agent-flow/\n");
  return root;
}

function temporaryRepoWithSubmodule(): { root: string; submodule: string } {
  const source = fs.mkdtempSync(path.join(os.tmpdir(), "agent-flow-submodule-source-"));
  git(source, "init");
  fs.writeFileSync(path.join(source, ".gitignore"), "ignored.txt\n");
  fs.writeFileSync(path.join(source, "tracked.txt"), "original\n");
  git(source, "add", ".gitignore", "tracked.txt");
  git(source, "-c", "user.name=Agent Flow", "-c", "user.email=agent-flow@example.test", "commit", "-m", "fixture");

  const root = fs.mkdtempSync(path.join(os.tmpdir(), "agent-flow-recovery-repo-"));
  git(root, "init");
  fs.mkdirSync(path.join(root, "vendor"));
  git(root, "-c", "protocol.file.allow=always", "submodule", "add", source, "vendor/plugin");
  git(root, "-c", "user.name=Agent Flow", "-c", "user.email=agent-flow@example.test", "commit", "-m", "fixture");
  return { root, submodule: path.join(root, "vendor/plugin") };
}

function git(cwd: string, ...args: string[]): void {
  execFileSync("git", args, { cwd, stdio: "ignore" });
}

function writePrompt(root: string): void {
  fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
  fs.writeFileSync(path.join(root, "prompts/fix.md"), "Fix the failure safely.\n");
}

function remediated(): AgentFlowSessionProviderResponse {
  return { outputs: {}, metadata: { recovery_status: "remediated" } };
}

async function waitFor(predicate: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 200; attempt += 1) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  throw new Error("Timed out waiting for recovery session state.");
}
