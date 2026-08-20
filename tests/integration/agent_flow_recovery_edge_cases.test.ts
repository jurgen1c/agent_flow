import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";

import {
  createAgentFlowLifecycleRun,
  createAgentFlowMcpCallRegistry,
  createAgentFlowSessionProviderRegistry,
  createAgentFlowWorkflowRegistry,
  executeAgentFlowCommandPipeline,
  injectAgentFlowRecoveryContext,
  MAX_AGENT_FLOW_SESSION_PROMPT_BYTES,
  MAX_AGENT_FLOW_SESSION_TOTAL_INPUT_BYTES,
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

  test("applies sensitive-input redaction to recovery session prompts", async () => {
    const root = temporaryRepo();
    writePrompt(root);
    fs.writeFileSync(
      path.join(root, "prompts", "fix.md"),
      "Investigate with Authorization: Bearer recovery-prompt-secret.\n"
    );
    const workflow = recoverySessionWorkflow(`
sessions:
  fixer: { provider: fixture }
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
    createAgentFlowLifecycleRun(store, { id: "redacted-recovery-prompt", workflow });
    let prompt = "";
    let providerChecksum = "";
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => {
      prompt = request.prompt.content;
      providerChecksum = request.prompt.checksum;
      return { outputs: {}, metadata: { recovery_status: "unresolved" } };
    });

    await executeAgentFlowCommandPipeline(store, "redacted-recovery-prompt", workflow, undefined, providers);

    expect(prompt).toContain("Authorization: Bearer [REDACTED]");
    expect(prompt).not.toContain("recovery-prompt-secret");
    const requestPath = store.getSession("redacted-recovery-prompt", "fixer")!.state.requestArtifact as string;
    const evidence = JSON.parse(store.readArtifact(
      "redacted-recovery-prompt", requestPath
    ).content.toString("utf8"));
    expect(evidence.prompt).toEqual({
      path: "prompts/fix.md",
      checksum: `sha256:${createHash("sha256").update(
        "Investigate with Authorization: Bearer recovery-prompt-secret.\n"
      ).digest("hex")}`,
      providerChecksum,
      redacted: true
    });
    store.close();
  });

  test("redacts recovery input manifests before durable persistence", async () => {
    const root = temporaryRepo();
    writePrompt(root);
    const workflow = recoverySessionWorkflow(`
policies: { sensitive_inputs: redact }
inputs: { key: { required: true } }
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
        inputs:
          API_TOKEN: recovery-manifest-secret
          opaque: "{{ inputs.key }}"
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, {
      id: "redacted-recovery-manifest",
      workflow,
      inputs: { key: "opaque-key-secret" }
    });
    let providerManifest = "";
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => {
      const manifest = request.inputs.find((input) => input.path.endsWith("/inputs.json"));
      providerManifest = manifest === undefined ? "" : Buffer.from(manifest.content).toString("utf8");
      return { outputs: {}, metadata: { recovery_status: "unresolved" } };
    });

    await executeAgentFlowCommandPipeline(
      store,
      "redacted-recovery-manifest",
      workflow,
      undefined,
      providers
    );

    const manifestArtifact = store.listArtifacts("redacted-recovery-manifest")
      .find((artifact) => artifact.kind === "recovery_input")!;
    const persistedManifest = store.readArtifact(
      "redacted-recovery-manifest",
      manifestArtifact.declaredPath
    ).content.toString("utf8");
    expect(providerManifest).toContain("[REDACTED]");
    expect(persistedManifest).toContain("[REDACTED]");
    expect(`${providerManifest}\n${persistedManifest}`).not.toContain("recovery-manifest-secret");
    expect(`${providerManifest}\n${persistedManifest}`).not.toContain("opaque-key-secret");
    store.close();
  });

  test("enforces recovery manifest source limits before sensitive-data redaction", async () => {
    const root = temporaryRepo();
    writePrompt(root);
    const workflow = recoverySessionWorkflow(`
policies: { sensitive_inputs: redact }
inputs: { credential: { required: true } }
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
        inputs: { API_TOKEN: "{{ inputs.credential }}" }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, {
      id: "oversized-recovery-manifest-source",
      workflow,
      inputs: { credential: "x".repeat(MAX_AGENT_FLOW_SESSION_TOTAL_INPUT_BYTES) }
    });
    let invoked = false;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      invoked = true;
      return { outputs: {}, metadata: { recovery_status: "unresolved" } };
    });

    const result = await executeAgentFlowCommandPipeline(
      store,
      "oversized-recovery-manifest-source",
      workflow,
      undefined,
      providers
    );

    expect(result).toMatchObject({ status: "paused" });
    expect(store.listEvents("oversized-recovery-manifest-source")).toContainEqual(expect.objectContaining({
      type: "recovery.completed",
      payload: expect.objectContaining({
        message: expect.stringContaining("aggregate limit before sensitive-data handling")
      })
    }));
    expect(store.listArtifacts("oversized-recovery-manifest-source")
      .some((artifact) => artifact.kind === "recovery_input")).toBe(false);
    expect(invoked).toBe(false);
    store.close();
  });

  test("counts the pre-redaction recovery manifest toward the source aggregate limit", async () => {
    const root = temporaryRepo();
    writePrompt(root);
    const workflow = recoverySessionWorkflow(`
policies: { sensitive_inputs: redact }
inputs:
  credential: { required: true }
  evidence: { required: true }
sessions: { fixer: { provider: fixture } }
steps:
  - id: check
    type: command
    command: "false"
    on_failure:
      route_to:
        session: fixer
        prompt: prompts/fix.md
        inputs:
          API_TOKEN: "{{ inputs.credential }}"
          evidence: "{{ inputs.evidence }}"
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, {
      id: "aggregate-recovery-manifest-source",
      workflow,
      inputs: {
        credential: "x".repeat(6 * 1024 * 1024),
        evidence: "evidence.txt"
      }
    });
    store.writeArtifact({
      id: "aggregate-recovery-evidence",
      runId: "aggregate-recovery-manifest-source",
      stepId: "fixture",
      path: "evidence.txt",
      kind: "fixture",
      contentType: "text/plain",
      content: "y".repeat(6 * 1024 * 1024)
    });
    let invoked = false;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      invoked = true;
      return { outputs: {}, metadata: { recovery_status: "unresolved" } };
    });

    const result = await executeAgentFlowCommandPipeline(
      store,
      "aggregate-recovery-manifest-source",
      workflow,
      undefined,
      providers
    );

    expect(result).toMatchObject({ status: "paused" });
    expect(store.listEvents("aggregate-recovery-manifest-source")).toContainEqual(expect.objectContaining({
      type: "recovery.completed",
      payload: expect.objectContaining({ message: expect.stringContaining("aggregate limit") })
    }));
    expect(invoked).toBe(false);
    store.close();
  });

  test("preserves sensitive provenance for opaque recovery artifacts referenced through run inputs", async () => {
    const root = temporaryRepo();
    writePrompt(root);
    const workflow = recoverySessionWorkflow(`
inputs: { credential: { required: true } }
sessions: { fixer: { provider: fixture } }
steps:
  - id: check
    type: command
    command: "false"
    on_failure:
      route_to:
        session: fixer
        prompt: prompts/fix.md
        inputs: { evidence: "{{ inputs.credential }}" }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, {
      id: "referenced-recovery-credential",
      workflow,
      inputs: { credential: { certificate: "payload.txt" } }
    });
    store.writeArtifact({
      id: "credential-payload",
      runId: "referenced-recovery-credential",
      stepId: "fixture",
      path: "payload.txt",
      kind: "fixture",
      contentType: "text/plain",
      content: "hunter2abc"
    });
    let providerInput = "";
    let providerChecksum = "";
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => {
      const payload = request.inputs.find((input) => input.path === "payload.txt");
      providerInput = payload === undefined ? "" : Buffer.from(payload.content).toString("utf8");
      providerChecksum = payload?.checksum ?? "";
      return { outputs: {}, metadata: { recovery_status: "unresolved" } };
    });

    expect((await executeAgentFlowCommandPipeline(
      store,
      "referenced-recovery-credential",
      workflow,
      undefined,
      providers
    )).status).toBe("paused");
    expect(providerInput).toBe("[REDACTED]");
    expect(providerInput).not.toContain("hunter2abc");
    const sourceArtifact = store.getArtifact("referenced-recovery-credential", "payload.txt")!;
    const requestPath = store.getSession(
      "referenced-recovery-credential", "fixer"
    )!.state.requestArtifact as string;
    const inputEvidence = (JSON.parse(store.readArtifact(
      "referenced-recovery-credential", requestPath
    ).content.toString("utf8")).inputs as Array<Record<string, unknown>>)
      .find((input) => input.path === "payload.txt");
    expect(inputEvidence).toEqual({
      path: "payload.txt",
      checksum: sourceArtifact.checksum,
      contentType: "text/plain",
      providerChecksum,
      redacted: true
    });
    store.close();
  });

  test("preflights sensitive recovery prompt paths before filesystem reads", async () => {
    const root = temporaryRepo();
    const workflow = recoverySessionWorkflow(`
sessions:
  fixer: { provider: fixture }
steps:
  - id: check
    type: command
    command: "false"
    on_failure:
      route_to: { session: fixer, prompt: .env }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "preflight-recovery-prompt-path", workflow });
    let invoked = false;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      invoked = true;
      return remediated();
    });

    const result = await executeAgentFlowCommandPipeline(
      store,
      "preflight-recovery-prompt-path",
      workflow,
      undefined,
      providers
    );

    expect(result).toMatchObject({ status: "paused" });
    expect(store.listEvents("preflight-recovery-prompt-path")).toContainEqual(expect.objectContaining({
      type: "recovery.completed",
      payload: expect.objectContaining({
        message: expect.stringContaining("secret-like path")
      })
    }));
    expect(JSON.stringify(store.listEvents("preflight-recovery-prompt-path"))).not.toContain("ENOENT");
    expect(invoked).toBe(false);
    store.close();
  });

  test("preflights sensitive recovery artifact paths before artifact reads", async () => {
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
        inputs: { evidence: .env }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "preflight-recovery-artifact-path", workflow });
    store.writeArtifact({
      id: "environment",
      runId: "preflight-recovery-artifact-path",
      stepId: "fixture",
      path: ".env",
      kind: "fixture",
      contentType: "text/plain",
      content: "API_TOKEN=recovery-artifact-secret\n"
    });
    const readArtifact = store.readArtifact.bind(store);
    let sensitiveArtifactRead = false;
    store.readArtifact = ((runId, artifactPath, options) => {
      if (artifactPath === ".env") sensitiveArtifactRead = true;
      return readArtifact(runId, artifactPath, options);
    });
    let invoked = false;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      invoked = true;
      return remediated();
    });

    const result = await executeAgentFlowCommandPipeline(
      store,
      "preflight-recovery-artifact-path",
      workflow,
      undefined,
      providers
    );

    expect(result).toMatchObject({ status: "paused" });
    expect(sensitiveArtifactRead).toBe(false);
    expect(invoked).toBe(false);
    store.close();
  });

  test("rejects recovery prompts that exceed the limit after redaction", async () => {
    const root = temporaryRepo();
    writePrompt(root);
    const source = "token=x\n".repeat(100_000);
    expect(Buffer.byteLength(source)).toBeLessThan(MAX_AGENT_FLOW_SESSION_PROMPT_BYTES);
    fs.writeFileSync(path.join(root, "prompts", "fix.md"), source);
    const workflow = recoverySessionWorkflow(`
sessions:
  fixer: { provider: fixture }
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
    createAgentFlowLifecycleRun(store, { id: "oversized-redacted-recovery-prompt", workflow });
    let invoked = false;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      invoked = true;
      return { outputs: {}, metadata: { recovery_status: "unresolved" } };
    });

    const result = await executeAgentFlowCommandPipeline(
      store,
      "oversized-redacted-recovery-prompt",
      workflow,
      undefined,
      providers
    );

    expect(result).toMatchObject({ status: "paused" });
    expect(store.listEvents("oversized-redacted-recovery-prompt")).toContainEqual(expect.objectContaining({
      type: "recovery.completed",
      payload: expect.objectContaining({
        message: expect.stringContaining("session prompt limit after sensitive-data handling")
      })
    }));
    expect(invoked).toBe(false);
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

  test("preserves sensitive provenance when a nested recovery renames a run input", async () => {
    const root = temporaryRepo();
    writePrompt(root);
    const child = recoverySessionWorkflow(`
inputs: { evidence: { required: true } }
sessions: { inspector: { provider: fixture } }
steps:
  - id: inspect
    type: session_request
    session: inspector
    prompt: prompts/fix.md
    inputs: ["{{ inputs.evidence }}"]
    outputs: [inspection.json]
`);
    const parent = recoverySessionWorkflow(`
inputs: { credential: { required: true } }
steps:
  - id: check
    type: command
    command: "false"
    on_failure:
      route_to:
        workflow: nested
        inputs: { evidence: "{{ inputs.credential }}" }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, {
      id: "nested-renamed-input",
      workflow: parent,
      inputs: { credential: "payload.txt" }
    });
    store.writeArtifact({
      id: "nested-credential-payload",
      runId: "nested-renamed-input",
      stepId: "fixture",
      path: "payload.txt",
      kind: "fixture",
      contentType: "text/plain",
      content: "opaque-nested-credential"
    });
    let providerInput = "";
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => {
      providerInput = Buffer.from(request.inputs[0]!.content).toString("utf8");
      return { outputs: { "inspection.json": "{}\n" } };
    });

    const result = await executeAgentFlowCommandPipeline(
      store,
      "nested-renamed-input",
      parent,
      undefined,
      providers,
      undefined,
      undefined,
      createAgentFlowWorkflowRegistry().register("nested", child)
    );

    expect(result.status).toBe("paused");
    expect(providerInput).toBe("[REDACTED]");
    expect(providerInput).not.toContain("opaque-nested-credential");
    const recoveryEvent = store.listEvents("nested-renamed-input")
      .find((event) => event.type === "recovery.completed");
    const nestedRunId = recoveryEvent?.payload.recoveryRunId;
    expect(typeof nestedRunId).toBe("string");
    const nestedRun = store.getRun(nestedRunId as string);
    expect(nestedRun?.inputs).toEqual({ evidence: "payload.txt" });
    expect(store.readArtifact(nestedRun!.id, "payload.txt").content.toString("utf8"))
      .toBe("[REDACTED]");
    store.close();
  });

  test("applies the parent sensitive-input policy to every nested recovery scalar", async () => {
    const root = temporaryRepo();
    const child = recoverySessionWorkflow(`
policies: { sensitive_inputs: allow }
inputs: { context: { required: true } }
steps:
  - id: inspect
    type: mcp_call
    server: fixture
    tool: inspect
    arguments: { body: "{{ inputs.context }}" }
    outputs: [inspection.json]
  - { id: done, type: result, status: remediated }
`);
    const parent = recoverySessionWorkflow(`
inputs: { context: { required: true } }
steps:
  - id: check
    type: command
    command: "false"
    on_failure:
      route_to:
        workflow: nested
        inputs: { context: "{{ inputs.context }}" }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
  - { id: complete, type: result, status: completed }
  - { id: pause, type: result, status: paused }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, {
      id: "nested-scalar-policy",
      workflow: parent,
      inputs: { context: "API_TOKEN=opaquevalue" }
    });
    let adapterArguments: Record<string, unknown> | undefined;
    const mcpCalls = createAgentFlowMcpCallRegistry().register("fixture", (request) => {
      adapterArguments = request.arguments;
      return { outputs: { "inspection.json": { safe: true } } };
    });

    const result = await executeAgentFlowCommandPipeline(
      store,
      "nested-scalar-policy",
      parent,
      undefined,
      undefined,
      mcpCalls,
      undefined,
      createAgentFlowWorkflowRegistry().register("nested", child)
    );

    expect(result.status).toBe("completed");
    expect(adapterArguments).toEqual({ body: "API_TOKEN=[REDACTED]" });
    expect(JSON.stringify(adapterArguments)).not.toContain("opaquevalue");
    const nestedRunId = store.listEvents("nested-scalar-policy")
      .find((event) => event.type === "recovery.completed")?.payload.recoveryRunId;
    expect(store.getRun(nestedRunId as string)?.inputs)
      .toEqual({ context: "API_TOKEN=[REDACTED]" });
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
    }, { enabled: true });
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
    const requestPath = store.getSession("injected-context", "fixer")!.state.requestArtifact as string;
    const requestEvidence = JSON.parse(store.readArtifact(
      "injected-context", requestPath
    ).content.toString("utf8"));
    expect(requestEvidence.inputs).toContainEqual(expect.objectContaining({
      path: "recovery-context/injected.md",
      checksum: `sha256:${createHash("sha256").update(
        `## Injected context 1\n\n${injectedText}\n`
      ).digest("hex")}`
    }));
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
    }, { enabled: true });
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

  test("does not persist stale recovery evidence when the required rerun fails", async () => {
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
    createAgentFlowLifecycleRun(store, { id: "failed-settlement-rerun", workflow });
    let calls = 0;
    const providers = createAgentFlowSessionProviderRegistry().register("frontier", () => {
      calls += 1;
      if (calls === 1) {
        return { ...remediated(), metadata: { recovery_status: "remediated", phase: "stale" } };
      }
      throw new Error("required rerun failed");
    }, { enabled: true });
    const settle = store.settleRecoverySessionForRunAtContextRevision.bind(store);
    let injected = false;
    store.settleRecoverySessionForRunAtContextRevision = (input, revision) => {
      if (!injected) {
        injected = true;
        injectAgentFlowRecoveryContext(
          store,
          "failed-settlement-rerun",
          "fixer",
          "Context accepted immediately before settlement."
        );
      }
      return settle(input, revision);
    };

    const result = await executeAgentFlowCommandPipeline(
      store, "failed-settlement-rerun", workflow, undefined, providers
    );

    expect(result.status).toBe("paused");
    expect(calls).toBe(2);
    expect(store.listArtifacts("failed-settlement-rerun").filter((artifact) => artifact.kind === "session_request"))
      .toEqual([]);
    expect(store.getSession("failed-settlement-rerun", "fixer")?.state.requestArtifact).toBeUndefined();
    store.close();
  });

  test("snapshots recovery evidence before invoking a mutating provider", async () => {
    const root = temporaryRepo();
    writePrompt(root);
    const workflow = recoverySessionWorkflow(`
inputs: { evidence: { required: true } }
sessions: { fixer: { provider: fixture } }
steps:
  - id: check
    type: command
    command: "false"
    on_failure:
      route_to:
        session: fixer
        prompt: prompts/fix.md
        inputs: { evidence: "{{ inputs.evidence }}" }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, {
      id: "mutating-recovery-provider",
      workflow,
      inputs: { evidence: "evidence.txt" }
    });
    store.writeArtifact({
      id: "recovery-evidence",
      runId: "mutating-recovery-provider",
      stepId: "fixture",
      path: "evidence.txt",
      kind: "fixture",
      contentType: "text/plain",
      content: "Original evidence."
    });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => {
      request.prompt.path = "mutated-secret.env";
      request.prompt.checksum = "sha256:mutated";
      request.inputs[0]!.path = "mutated-input.env";
      request.inputs[0]!.contentType = "application/mutated";
      request.inputs.splice(0, request.inputs.length);
      return remediated();
    });

    const result = await executeAgentFlowCommandPipeline(
      store, "mutating-recovery-provider", workflow, undefined, providers
    );

    expect(result.status).toBe("completed");
    const requestPath = store.getSession("mutating-recovery-provider", "fixer")!
      .state.requestArtifact as string;
    const evidence = JSON.parse(store.readArtifact(
      "mutating-recovery-provider", requestPath
    ).content.toString("utf8"));
    expect(evidence.prompt).toMatchObject({ path: "prompts/fix.md" });
    expect(evidence.inputs).toContainEqual(expect.objectContaining({
      path: "evidence.txt",
      contentType: "text/plain",
      checksum: `sha256:${createHash("sha256").update("Original evidence.").digest("hex")}`
    }));
    expect(JSON.stringify(evidence)).not.toContain("mutated");
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
    }, { enabled: true });
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
    }, { enabled: true });
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
    }, { enabled: true });
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
    }, { enabled: true });
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
    }, { enabled: true });
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
