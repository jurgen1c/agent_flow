import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import {
  type AgentFlowYamlMapping,
  createAgentFlowArtifactTransformRegistry,
  createAgentFlowLifecycleRun,
  createAgentFlowNotificationRegistry,
  createAgentFlowSessionProviderRegistry,
  createAgentFlowWorkflowRegistry,
  executeAgentFlowCommandPipeline,
  openAgentFlowRunState,
  parseAgentFlowWorkflowOrThrow,
  transitionAgentFlowLifecycleRun,
  validateAgentFlowWorkflow
} from "../../src/runtime";

describe("Agent Flow recovery routes", () => {
  test("validates recovery targets, outcome handlers, and result statuses", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: invalid-recovery
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_recovery_cycles: 1 }
steps:
  - id: missing
    type: command
    command: exit 1
    on_failure:
      route_to: {}
      on_remediated: { return_to: missing }
      on_unresolved: { then: pause }
  - id: ambiguous
    type: command
    command: exit 1
    on_failure:
      then: fail
      route_to: { session: fixer, workflow: repair, prompt: prompts/fix.md }
      on_remediated: { return_to: ambiguous }
      on_unresolved: { then: pause }
  - id: incomplete
    type: command
    command: exit 1
    on_failure:
      route_to: { workflow: repair }
      on_remediated: { return_to: incomplete }
  - id: unsupported-input
    type: command
    command: exit 1
    on_failure:
      route_to:
        session: fixer
        prompt: ../fix.md
        inputs: { diagnostic: "{{ artifacts.failure }}" }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
  - id: wrong-return
    type: command
    command: exit 1
    on_failure:
      route_to: { workflow: repair }
      on_remediated: { return_to: incomplete }
      on_unresolved: { then: pause }
  - id: dynamic-handler
    type: command
    command: exit 1
    on_failure:
      route_to: { workflow: repair }
      on_remediated: { then: complete }
      on_unresolved: { then: "{{ inputs.fallback }}" }
  - id: unsupported-step
    type: input_request
    question: Continue?
    save_as: answer.txt
    on_failure:
      route_to: { session: fixer, prompt: fix.md }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
  - { id: result, type: result, status: maybe }
inputs:
  fallback: { required: false }
sessions:
  fixer: { provider: fixture }
`);

    expect(validateAgentFlowWorkflow(workflow).errors.map((issue) => issue.code)).toEqual(expect.arrayContaining([
      "workflow.recovery.route.target",
      "workflow.recovery.route.ambiguous",
      "workflow.recovery.outcome.required",
      "workflow.recovery.session.prompt.invalid",
      "workflow.recovery.inputs.expression.unsupported",
      "workflow.recovery.remediated.return_to",
      "workflow.recovery.outcome.dynamic",
      "workflow.recovery.step.unsupported",
      "workflow.result.status.unsupported"
    ]));
  });

  test("normalizes padded recovery route references", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: padded-recovery
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_recovery_cycles: 1 }
sessions:
  fixer: { provider: fixture }
steps:
  - id: check
    type: command
    command: exit 1
    on_failure:
      route_to: { session: " fixer ", prompt: fix.md }
      on_remediated: { return_to: " check " }
      on_unresolved: { then: " pause " }
`);

    expect(validateAgentFlowWorkflow(workflow)).toEqual({ valid: true, errors: [] });
  });

  test("rejects recovery outcome handlers without a recovery route", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: handler-only-recovery
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - id: transform
    type: artifact_transform
    input: source.json
    output: result.json
    transform: fixture
    on_failure:
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
  - id: command
    type: command
    command: exit 1
    on_failure:
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);

    expect(validateAgentFlowWorkflow(workflow).errors
      .filter((issue) => issue.code === "workflow.recovery.route.required")).toHaveLength(2);
  });

  test("treats nested session fields in recovery inputs as ordinary payload data", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: nested-session-payload
version: 1
style: recovery_pipeline
maturity: experimental
sessions:
  fixer: { provider: fixture }
steps:
  - id: check
    type: command
    command: exit 1
    on_failure:
      route_to:
        session: fixer
        prompt: fix.md
        inputs:
          context: { session: external-session-id }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);

    expect(validateAgentFlowWorkflow(workflow)).toEqual({ valid: true, errors: [] });
  });

  test("allows declared optional recovery inputs to remain absent until recovery", async () => {
    const root = temporaryRepo();
    fs.writeFileSync(path.join(root, "fix.md"), "fix\n");
    const workflow = parseAgentFlowWorkflowOrThrow(`name: optional-recovery-input
version: 1
style: recovery_pipeline
maturity: experimental
inputs:
  context: { required: false }
sessions:
  fixer: { provider: fixture }
steps:
  - id: check
    type: command
    command: touch completed
    on_failure:
      route_to:
        session: fixer
        prompt: fix.md
        inputs: { context: "{{ inputs.context }}" }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "optional-recovery-input", workflow });
    let calls = 0;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      calls += 1;
      return { outputs: {}, metadata: { recovery_status: "unresolved" } };
    });

    const result = await executeAgentFlowCommandPipeline(
      store, "optional-recovery-input", workflow, undefined, providers
    );

    expect(result.status).toBe("completed");
    expect(calls).toBe(0);
    store.close();
  });

  test("validates recovery routes declared directly on parallel branches", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: parallel-recovery
version: 1
style: pipeline
maturity: experimental
sessions:
  worker: { provider: fixture }
  fixer: { provider: fixture }
steps:
  - id: source
    type: command
    command: exit 1
    on_failure:
      route_to: { session: fixer, prompt: fix.md }
      on_remediated: { then: descriptor }
      on_unresolved: { then: pause }
  - id: fanout
    type: parallel
    branches:
      - id: direct
        type: command
        session: worker
        command: exit 1
        on_failure:
          route_to: { session: missing, prompt: fix.md }
      - id: bad-target
        type: command
        session: worker
        command: exit 1
        on_failure:
          route_to: { session: fixer, prompt: fix.md }
          on_remediated: { then: missing-target }
          on_unresolved: { then: pause }
      - { id: descriptor, session: worker }
      - { id: incomplete, type: command, session: worker }
`);

    expect(validateAgentFlowWorkflow(workflow).errors).toEqual(expect.arrayContaining([
      expect.objectContaining({
        code: "workflow.recovery.style.required",
        path: "steps[1].branches[0].on_failure.route_to"
      }),
      expect.objectContaining({
        code: "workflow.recovery.outcome.required",
        path: "steps[1].branches[0].on_failure.on_remediated"
      }),
      expect.objectContaining({
        code: "workflow.recovery.outcome.required",
        path: "steps[1].branches[0].on_failure.on_unresolved"
      }),
      expect.objectContaining({
        code: "workflow.step.target.unresolved",
        path: "steps[1].branches[1].on_failure.on_remediated.then"
      }),
      expect.objectContaining({
        code: "workflow.session.undeclared",
        path: "steps[1].branches[0].on_failure.route_to.session"
      }),
      expect.objectContaining({
        code: "workflow.step.target.unresolved",
        path: "steps[0].on_failure.on_remediated.then"
      }),
      expect.objectContaining({
        code: "workflow.step.field.required",
        path: "steps[1].branches[3].command"
      })
    ]));
  });

  test("executes a nested recovery workflow, persists its decision, and retries the failed step", async () => {
    const root = temporaryRepo();
    const parent = parseAgentFlowWorkflowOrThrow(`name: parent
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_recovery_cycles: 1, max_step_attempts: { check: 2 } }
steps:
  - id: check
    type: command
    command: test -f fixed.txt
    on_failure:
      route_to:
        workflow: repair
        inputs:
          failure_payload: "{{ failure.path }}"
          failed_step: "{{ step.id }}"
      on_remediated: { return_to: check }
      on_unresolved: { then: pause }
`);
    const repair = parseAgentFlowWorkflowOrThrow(`name: repair
version: 1
style: recovery_pipeline
maturity: experimental
inputs:
  failure_payload: { required: true }
  failed_step: { required: true }
steps:
  - { id: fix, type: command, command: touch fixed.txt }
  - { id: done, type: result, status: remediated, return_to: "{{ inputs.failed_step }}" }
`);
    const workflows = createAgentFlowWorkflowRegistry().register("repair", repair);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "parent-run", workflow: parent });

    const result = await executeAgentFlowCommandPipeline(
      store, "parent-run", parent, undefined, undefined, undefined, undefined, workflows
    );

    expect(result).toMatchObject({ status: "completed", completedSteps: ["check"] });
    const failure = store.listFailures("parent-run")[0]!;
    const recoveryRunId = (failure.payload as { recovery: { recoveryRunId: string } }).recovery.recoveryRunId;
    const recoveryRun = store.getRun(recoveryRunId);
    expect(recoveryRun).toMatchObject({
      status: "completed",
      parentRunId: "parent-run",
      recoveryOfRunId: "parent-run",
      output: { resultStatus: "remediated", returnTo: "check" }
    });
    expect(store.getArtifact(recoveryRun!.id, store.listFailures("parent-run")[0]!.payloadPath!)).not.toBeNull();
    expect(store.listEvents("parent-run").map((event) => event.type)).toEqual(expect.arrayContaining([
      "recovery.routed",
      "recovery.completed"
    ]));
    expect(failure.resolvedAt).not.toBeNull();
    expect(failure.payload).toMatchObject({
      recovery: {
        status: "remediated",
        route: "workflow",
        target: "repair",
        recoveryRunId: recoveryRun!.id
      }
    });
    const decision = store.listArtifacts("parent-run").find((artifact) => artifact.kind === "recovery_decision")!;
    expect(JSON.parse(store.readArtifact("parent-run", decision.declaredPath).content.toString())).toMatchObject({
      status: "remediated",
      route: "workflow",
      target: "repair"
    });
    store.close();
  });

  test("preserves parent output ownership when recovery produces the retried command output", async () => {
    const root = temporaryRepo();
    const parent = parseAgentFlowWorkflowOrThrow(`name: shared-output-parent
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_recovery_cycles: 1, max_step_attempts: { check: 2 } }
steps:
  - id: check
    type: command
    command: test -f fixed.txt
    outputs: [fixed.txt]
    on_failure:
      route_to: { workflow: shared-output-repair }
      on_remediated: { return_to: check }
      on_unresolved: { then: pause }
`);
    const repair = parseAgentFlowWorkflowOrThrow(`name: shared-output-repair
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - { id: fix, type: command, command: echo fixed > fixed.txt, outputs: [fixed.txt] }
  - { id: done, type: result, status: remediated }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "shared-output-parent", workflow: parent });

    const result = await executeAgentFlowCommandPipeline(
      store,
      "shared-output-parent",
      parent,
      undefined,
      undefined,
      undefined,
      undefined,
      createAgentFlowWorkflowRegistry().register("shared-output-repair", repair)
    );

    expect(result).toMatchObject({ status: "completed", completedSteps: ["check"] });
    expect(store.getArtifact("shared-output-parent", "fixed.txt")).toMatchObject({
      id: `command-output:${createHash("sha256").update("fixed.txt").digest("hex")}`,
      producerStepId: "check",
      kind: "command_output"
    });
    store.close();
  });

  test("preserves an earlier parent producer when recovery repairs its artifact", async () => {
    const root = temporaryRepo();
    const parent = parseAgentFlowWorkflowOrThrow(`name: earlier-producer-parent
version: 1
style: recovery_pipeline
maturity: experimental
limits:
  max_recovery_cycles: 1
  max_step_attempts: { seed: 2, check: 2 }
steps:
  - { id: seed, type: command, command: echo seeded > source.txt, outputs: [source.txt] }
  - id: check
    type: command
    command: test -f recovered.txt
    on_failure:
      route_to: { workflow: repair-earlier-output }
      on_remediated: { then: seed }
      on_unresolved: { then: pause }
`);
    const repair = parseAgentFlowWorkflowOrThrow(`name: repair-earlier-output
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - id: repair
    type: command
    command: echo repaired > source.txt; touch recovered.txt
    outputs: [source.txt, recovered.txt]
    overwrite: true
  - { id: done, type: result, status: remediated }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "earlier-producer-parent", workflow: parent });

    const result = await executeAgentFlowCommandPipeline(
      store,
      "earlier-producer-parent",
      parent,
      undefined,
      undefined,
      undefined,
      undefined,
      createAgentFlowWorkflowRegistry().register("repair-earlier-output", repair)
    );

    expect(result).toMatchObject({ status: "completed", completedSteps: ["seed", "seed", "check"] });
    expect(store.getArtifact("earlier-producer-parent", "source.txt")).toMatchObject({
      producerStepId: "seed",
      kind: "command_output"
    });
    store.close();
  });

  test("promotes nested recovery outputs before retrying an artifact-backed parent step", async () => {
    const root = temporaryRepo();
    const parent = parseAgentFlowWorkflowOrThrow(`name: artifact-parent
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_recovery_cycles: 1, max_step_attempts: { transform: 2 } }
steps:
  - id: transform
    type: artifact_transform
    input: source.txt
    output: result.txt
    transform: require-fixed
    on_failure:
      route_to: { workflow: repair-artifact }
      on_remediated: { return_to: transform }
      on_unresolved: { then: pause }
`);
    const repair = parseAgentFlowWorkflowOrThrow(`name: repair-artifact
version: 1
style: pipeline
maturity: experimental
steps:
  - id: create-source
    type: command
    command: echo fixed > source.txt
    outputs: [source.txt]
    overwrite: true
  - { id: done, type: result, status: remediated }
retention:
  on_success:
    delete: [source.txt]
`);
    const transforms = createAgentFlowArtifactTransformRegistry().register("require-fixed", (input) => {
      if (Buffer.from(input).toString() !== "fixed\n") throw new Error("source is not repaired");
      return { content: input, contentType: "text/plain" };
    });
    const workflows = createAgentFlowWorkflowRegistry().register("repair-artifact", repair);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "artifact-parent", workflow: parent });
    store.writeArtifact({
      id: "source-input",
      runId: "artifact-parent",
      path: "source.txt",
      kind: "input",
      contentType: "text/plain",
      content: "broken\n"
    });

    const result = await executeAgentFlowCommandPipeline(
      store, "artifact-parent", parent, transforms, undefined, undefined, undefined, workflows
    );

    expect(result).toMatchObject({ status: "completed", completedSteps: ["transform"] });
    expect(store.readArtifact("artifact-parent", "source.txt").content.toString()).toBe("fixed\n");
    expect(store.readArtifact("artifact-parent", "result.txt").content.toString()).toBe("fixed\n");
    expect(store.getArtifact("artifact-parent", "source.txt")?.metadata).toMatchObject({
      recoveryRunId: expect.stringContaining("artifact-parent:recovery:transform-")
    });
    const recoveryRunId = store.getArtifact("artifact-parent", "source.txt")!.metadata.recoveryRunId as string;
    expect(store.getArtifact(recoveryRunId, "source.txt")?.status).toBe("missing");
    expect(store.listEvents("artifact-parent").map((event) => event.type))
      .toContain("recovery.outputs.promoted");
    store.close();
  });

  test("does not notify completion when nested output promotion fails", async () => {
    const root = temporaryRepo();
    const parent = parseAgentFlowWorkflowOrThrow(`name: promotion-failure-parent
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - id: check
    type: command
    command: exit 1
    on_failure:
      route_to: { workflow: promotion-failure-child }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const child = parseAgentFlowWorkflowOrThrow(`name: promotion-failure-child
version: 1
style: pipeline
maturity: experimental
steps:
  - id: produce
    type: command
    command: printf a > a.txt; printf b > b.txt
    outputs: [a.txt, b.txt]
  - { id: done, type: result, status: remediated }
notify:
  - { on: workflow.completed, channels: [terminal] }
  - { on: workflow.failed, channels: [terminal] }
`);
    const delivered: string[] = [];
    const notifications = createAgentFlowNotificationRegistry({
      terminal: (notification) => { delivered.push(notification.event); }
    });
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "promotion-failure-parent", workflow: parent });
    const writeArtifact = store.writeArtifact.bind(store);
    let promoted = 0;
    store.writeArtifact = (input) => {
      if (input.runId === "promotion-failure-parent" && input.metadata?.recoveryRunId !== undefined) {
        promoted += 1;
        if (promoted === 2) throw new Error("injected second promotion failure");
      }
      return writeArtifact(input);
    };

    const result = await executeAgentFlowCommandPipeline(
      store,
      "promotion-failure-parent",
      parent,
      undefined,
      undefined,
      undefined,
      notifications,
      createAgentFlowWorkflowRegistry().register("promotion-failure-child", child)
    );

    expect(result.status).toBe("paused");
    expect(delivered).toEqual(["workflow.failed"]);
    expect(store.getArtifact("promotion-failure-parent", "a.txt")).toBeNull();
    expect(store.getArtifact("promotion-failure-parent", "b.txt")).toBeNull();
    const failure = store.listFailures("promotion-failure-parent")[0]!;
    const recoveryRunId = (failure.payload as { recovery: { recoveryRunId: string } }).recovery.recoveryRunId;
    expect(store.getRun(recoveryRunId)?.status).toBe("failed");
    store.close();
  });

  test("does not promote copied recovery inputs that no child step produced", async () => {
    const root = temporaryRepo();
    const parent = parseAgentFlowWorkflowOrThrow(`name: skipped-artifact-parent
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - id: check
    type: command
    command: exit 1
    on_failure:
      route_to:
        workflow: skipped-repair
        inputs: { source: source.txt }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const child = parseAgentFlowWorkflowOrThrow(`name: skipped-repair
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - { id: skip, type: command, command: "true", then: done }
  - id: repair
    type: command
    command: echo fixed > source.txt
    outputs: [source.txt]
    overwrite: true
  - { id: done, type: result, status: remediated }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "skipped-artifact-parent", workflow: parent });
    store.writeArtifact({
      id: "source-input",
      runId: "skipped-artifact-parent",
      path: "source.txt",
      kind: "input",
      contentType: "text/plain",
      content: "broken\n"
    });

    const result = await executeAgentFlowCommandPipeline(
      store,
      "skipped-artifact-parent",
      parent,
      undefined,
      undefined,
      undefined,
      undefined,
      createAgentFlowWorkflowRegistry().register("skipped-repair", child)
    );

    expect(result.status).toBe("completed");
    expect(store.readArtifact("skipped-artifact-parent", "source.txt").content.toString()).toBe("broken\n");
    expect(store.listEvents("skipped-artifact-parent").map((event) => event.type))
      .not.toContain("recovery.outputs.promoted");
    store.close();
  });

  test("routes missing required nested recovery inputs as unresolved before creating the child", async () => {
    const root = temporaryRepo();
    const parent = parseAgentFlowWorkflowOrThrow(`name: missing-child-input-parent
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - id: check
    type: command
    command: exit 1
    on_failure:
      route_to: { workflow: required-input-child }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const child = parseAgentFlowWorkflowOrThrow(`name: required-input-child
version: 1
style: recovery_pipeline
maturity: experimental
inputs:
  failure_context: { required: true }
steps:
  - id: write-repair
    type: command
    command: echo repaired > repaired.txt
    outputs: [repaired.txt]
  - { id: done, type: result, status: remediated }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "missing-child-input-parent", workflow: parent });

    const result = await executeAgentFlowCommandPipeline(
      store,
      "missing-child-input-parent",
      parent,
      undefined,
      undefined,
      undefined,
      undefined,
      createAgentFlowWorkflowRegistry().register("required-input-child", child)
    );

    expect(result.status).toBe("paused");
    const failure = store.listFailures("missing-child-input-parent")[0]!;
    expect(failure).toMatchObject({
      resolvedAt: null,
      payload: { recovery: { status: "unresolved", target: "required-input-child" } }
    });
    const decision = store.listArtifacts("missing-child-input-parent")
      .find((artifact) => artifact.kind === "recovery_decision")!;
    expect(JSON.parse(store.readArtifact("missing-child-input-parent", decision.declaredPath).content.toString()))
      .toMatchObject({
        status: "unresolved",
        message: expect.stringContaining("missing required route inputs: failure_context")
      });
    store.close();
  });

  test("rolls back nested recovery child creation when copying an input fails", async () => {
    const root = temporaryRepo();
    const parent = parseAgentFlowWorkflowOrThrow(`name: copy-failure-parent
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - id: check
    type: command
    command: exit 1
    on_failure:
      route_to:
        workflow: copy-failure-child
        inputs: { source: source.txt }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const child = parseAgentFlowWorkflowOrThrow(`name: copy-failure-child
version: 1
style: recovery_pipeline
maturity: experimental
inputs: { source: { required: true } }
steps:
  - { id: done, type: result, status: remediated }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "copy-failure-parent", workflow: parent });
    store.writeArtifact({
      id: "source-input",
      runId: "copy-failure-parent",
      path: "source.txt",
      kind: "input",
      contentType: "text/plain",
      content: "source\n"
    });
    const writeArtifact = store.writeArtifact.bind(store);
    store.writeArtifact = (input) => {
      if (input.kind === "recovery_input") throw new Error("injected recovery input copy failure");
      return writeArtifact(input);
    };

    const result = await executeAgentFlowCommandPipeline(
      store,
      "copy-failure-parent",
      parent,
      undefined,
      undefined,
      undefined,
      undefined,
      createAgentFlowWorkflowRegistry().register("copy-failure-child", child)
    );

    expect(result.status).toBe("paused");
    expect(store.getRun("copy-failure-parent:recovery:check-20f65c28:attempt-1")).toBeNull();
    expect(store.listFailures("copy-failure-parent")[0]?.payload).toMatchObject({
      recovery: { status: "unresolved", target: "copy-failure-child" }
    });
    const decision = store.listArtifacts("copy-failure-parent")
      .find((artifact) => artifact.kind === "recovery_decision")!;
    expect(JSON.parse(store.readArtifact("copy-failure-parent", decision.declaredPath).content.toString()))
      .toMatchObject({ status: "unresolved", message: "injected recovery input copy failure" });
    store.close();
  });

  test("fails and links a nested child when its runtime preflight fails", async () => {
    const root = temporaryRepo();
    const parent = parseAgentFlowWorkflowOrThrow(`name: child-preflight-parent
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - id: check
    type: command
    command: exit 1
    on_failure:
      route_to: { workflow: child-preflight-failure }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const child = parseAgentFlowWorkflowOrThrow(`name: child-preflight-failure
version: 1
style: recovery_pipeline
maturity: experimental
sessions: { fixer: { provider: absent } }
steps:
  - id: repair
    type: command
    command: exit 1
    on_failure:
      route_to: { session: fixer, prompt: fix.md }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "child-preflight-parent", workflow: parent });

    const result = await executeAgentFlowCommandPipeline(
      store,
      "child-preflight-parent",
      parent,
      undefined,
      undefined,
      undefined,
      undefined,
      createAgentFlowWorkflowRegistry().register("child-preflight-failure", child)
    );

    const failure = store.listFailures("child-preflight-parent")[0]!;
    const recoveryRunId = (failure.payload as { recovery: { recoveryRunId: string } }).recovery.recoveryRunId;
    expect(result.status).toBe("paused");
    expect(store.getRun(recoveryRunId)).toMatchObject({
      status: "failed",
      error: { code: "recovery.startup.failed", message: expect.stringContaining("registered provider adapter") }
    });
    expect(failure.payload).toMatchObject({
      recovery: { status: "unresolved", recoveryRunId }
    });
    store.close();
  });

  test("routes unresolved nested results through the parent on_unresolved handler", async () => {
    const root = temporaryRepo();
    const parent = parseAgentFlowWorkflowOrThrow(`name: unresolved-parent
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - id: check
    type: command
    command: exit 1
    on_failure:
      route_to: { workflow: diagnose }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const diagnose = parseAgentFlowWorkflowOrThrow(`name: diagnose
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - { id: done, type: result, status: unresolved }
`);
    const workflows = createAgentFlowWorkflowRegistry().register("diagnose", diagnose);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "unresolved-parent", workflow: parent });

    const result = await executeAgentFlowCommandPipeline(
      store, "unresolved-parent", parent, undefined, undefined, undefined, undefined, workflows
    );

    expect(result).toMatchObject({ status: "paused" });
    expect(store.listFailures("unresolved-parent")[0]).toMatchObject({
      resolvedAt: null,
      payload: { recovery: { status: "unresolved", route: "workflow", target: "diagnose" } }
    });
    store.close();
  });

  test("routes failures to a declared session and handles provider recovery status", async () => {
    const root = temporaryRepo();
    fs.mkdirSync(path.join(root, "prompts"), { recursive: true });
    fs.writeFileSync(path.join(root, "prompts/fix.md"), "Fix the supplied failure.\n");
    const workflow = parseAgentFlowWorkflowOrThrow(`name: session-recovery
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_recovery_cycles: 1, max_step_attempts: { check: 2 } }
inputs:
  ticket_key: { required: true }
sessions:
  fixer: { provider: fixture, resume: true }
steps:
  - id: check
    type: command
    command: printf diagnostic && test -f fixed.txt
    on_failure:
      route_to:
        session: fixer
        prompt: prompts/fix.md
        inputs:
          failure_payload: "{{ failure.path }}"
          failed_step: "{{ step.id }}"
          ticket_key: "{{ inputs.ticket_key }}"
          instruction: repair
      on_remediated: { return_to: check }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, {
      id: "session-recovery",
      workflow,
      inputs: { ticket_key: "AM-32" }
    });
    store.upsertSession({
      id: "fixer",
      runId: "session-recovery",
      provider: "fixture",
      status: "waiting",
      externalSessionId: "existing-session"
    });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => {
      expect(request.stepId).toBe("check:recovery");
      expect(request.externalSessionId).toBe("existing-session");
      expect(request.inputs.map((input) => input.path)).toEqual(expect.arrayContaining([
        expect.stringMatching(/^failures\/.+\.json$/),
        expect.stringMatching(/^failures\/.+\/attachments\//)
      ]));
      expect(request.inputs.some((input) => Buffer.from(input.content).toString() === "diagnostic")).toBe(true);
      const recoveryInputs = request.inputs.find((input) => input.path.endsWith("/inputs.json"));
      expect(recoveryInputs).toBeDefined();
      expect(JSON.parse(Buffer.from(recoveryInputs!.content).toString())).toMatchObject({
        failure_payload: expect.stringMatching(/^failures\/.+\.json$/),
        failed_step: "check",
        ticket_key: "AM-32",
        instruction: "repair"
      });
      fs.writeFileSync(path.join(root, "fixed.txt"), "fixed\n");
      return {
        outputs: {},
        externalSessionId: "continued-session",
        metadata: { recovery_status: "remediated", message: "fixed" }
      };
    });

    const result = await executeAgentFlowCommandPipeline(
      store, "session-recovery", workflow, undefined, providers
    );
    expect(result).toMatchObject({ status: "completed", completedSteps: ["check"] });
    expect(store.getSession("session-recovery", "fixer")).toMatchObject({
      status: "waiting",
      externalSessionId: "continued-session",
      state: { recoveryStatus: "remediated", failureId: expect.any(String) }
    });
    expect(store.listFailures("session-recovery")[0]?.resolvedAt).not.toBeNull();
    store.close();
  });

  test("reuses resumable recovery sessions across bounded recovery cycles", async () => {
    const root = temporaryRepo();
    fs.writeFileSync(path.join(root, "fix.md"), "fix\n");
    const workflow = parseAgentFlowWorkflowOrThrow(`name: repeated-session-recovery
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_recovery_cycles: 2, max_step_attempts: { check: 3 } }
sessions:
  fixer: { provider: fixture, resume: true }
steps:
  - id: check
    type: command
    command: test -f fixed.txt
    on_failure:
      route_to: { session: fixer, prompt: fix.md }
      on_remediated: { return_to: check }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "repeated-session-recovery", workflow });
    let calls = 0;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => {
      calls += 1;
      if (calls === 1) expect(request.externalSessionId).toBeUndefined();
      if (calls === 2) {
        expect(request.externalSessionId).toBe("recovery-session-1");
        fs.writeFileSync(path.join(root, "fixed.txt"), "fixed\n");
      }
      return {
        outputs: {},
        externalSessionId: `recovery-session-${calls}`,
        metadata: { recovery_status: "remediated" }
      };
    });

    const result = await executeAgentFlowCommandPipeline(
      store, "repeated-session-recovery", workflow, undefined, providers
    );

    expect(result).toMatchObject({ status: "completed", completedSteps: ["check"] });
    expect(calls).toBe(2);
    expect(store.getSession("repeated-session-recovery", "fixer")).toMatchObject({
      status: "waiting",
      externalSessionId: "recovery-session-2"
    });
    expect(store.listFailures("repeated-session-recovery").every((failure) => failure.resolvedAt !== null)).toBe(true);
    store.close();
  });

  test("checks recovery-cycle limits before invoking another recovery route", async () => {
    const root = temporaryRepo();
    fs.writeFileSync(path.join(root, "fix.md"), "fix\n");
    const workflow = parseAgentFlowWorkflowOrThrow(`name: bounded-recovery-invocation
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_recovery_cycles: 1, max_step_attempts: { check: 3 } }
sessions:
  fixer: { provider: fixture, resume: true }
steps:
  - id: check
    type: command
    command: exit 1
    on_failure:
      route_to: { session: fixer, prompt: fix.md }
      on_remediated: { return_to: check }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "bounded-recovery-invocation", workflow });
    let calls = 0;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      calls += 1;
      return { outputs: {}, metadata: { recovery_status: "remediated" } };
    });

    const result = await executeAgentFlowCommandPipeline(
      store, "bounded-recovery-invocation", workflow, undefined, providers
    );

    expect(result).toMatchObject({
      status: "paused",
      message: expect.stringContaining("cannot start recovery")
    });
    expect(calls).toBe(1);
    expect(store.listArtifacts("bounded-recovery-invocation")
      .filter((artifact) => artifact.kind === "recovery_decision")).toHaveLength(1);
    store.close();
  });

  test("checks target limits only after selecting the recovery outcome handler", async () => {
    const root = temporaryRepo();
    fs.writeFileSync(path.join(root, "fix.md"), "fix\n");
    const workflow = parseAgentFlowWorkflowOrThrow(`name: selected-handler-budget
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_recovery_cycles: 2, max_step_attempts: { used: 1, check: 1 } }
sessions:
  fixer: { provider: fixture }
steps:
  - { id: used, type: command, command: exit 0 }
  - id: check
    type: command
    command: exit 1
    on_failure:
      route_to: { session: fixer, prompt: fix.md }
      on_remediated: { then: complete }
      on_unresolved: { then: used }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "selected-handler-budget", workflow });
    let calls = 0;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      calls += 1;
      return { outputs: {}, metadata: { recovery_status: "remediated" } };
    });

    const result = await executeAgentFlowCommandPipeline(
      store, "selected-handler-budget", workflow, undefined, providers
    );

    expect(result.status).toBe("completed");
    expect(calls).toBe(1);
    store.close();
  });

  test("rejects indirect recursive recovery workflows before creating the recursive child", async () => {
    const root = temporaryRepo();
    const parent = parseAgentFlowWorkflowOrThrow(`name: repair-a
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - id: check-a
    type: command
    command: exit 1
    on_failure:
      route_to: { workflow: repair-b }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const child = parseAgentFlowWorkflowOrThrow(`name: repair-b
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - id: check-b
    type: command
    command: exit 1
    on_failure:
      route_to: { workflow: repair-a }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register("repair-a", parent)
      .register("repair-b", child);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "recursive-recovery", workflow: parent });

    const result = await executeAgentFlowCommandPipeline(
      store, "recursive-recovery", parent, undefined, undefined, undefined, undefined, workflows
    );

    const childRunId = "recursive-recovery:recovery:check-a-e19d0851:attempt-1";
    expect(result.status).toBe("paused");
    expect(store.getRun(childRunId)?.status).toBe("paused");
    expect(store.getRun(`${childRunId}:recovery:check-b-3de71b94:attempt-1`)).toBeNull();
    const childDecision = store.listArtifacts(childRunId)
      .find((artifact) => artifact.kind === "recovery_decision")!;
    expect(JSON.parse(store.readArtifact(childRunId, childDecision.declaredPath).content.toString()))
      .toMatchObject({ status: "unresolved", message: expect.stringContaining("recovery lineage") });
    store.close();
  });

  test("rejects unrelated terminal runs that collide with a recovery child ID", async () => {
    const root = temporaryRepo();
    const parent = parseAgentFlowWorkflowOrThrow(`name: collision-parent
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - id: check
    type: command
    command: exit 1
    on_failure:
      route_to: { workflow: repair }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const repair = parseAgentFlowWorkflowOrThrow(`name: repair
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - { id: done, type: result, status: remediated }
`);
    const unrelated = parseAgentFlowWorkflowOrThrow(`name: unrelated
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - { id: done, type: result, status: remediated }
`);
    const childRunId = "colliding-parent:recovery:check-20f65c28:attempt-1";
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: childRunId, workflow: unrelated });
    await executeAgentFlowCommandPipeline(store, childRunId, unrelated);
    createAgentFlowLifecycleRun(store, { id: "colliding-parent", workflow: parent });

    const result = await executeAgentFlowCommandPipeline(
      store,
      "colliding-parent",
      parent,
      undefined,
      undefined,
      undefined,
      undefined,
      createAgentFlowWorkflowRegistry().register("repair", repair)
    );

    expect(result.status).toBe("paused");
    expect(store.listFailures("colliding-parent")[0]?.payload).toMatchObject({
      recovery: { status: "unresolved", target: "repair" }
    });
    const decision = store.listArtifacts("colliding-parent")
      .find((artifact) => artifact.kind === "recovery_decision")!;
    expect(JSON.parse(store.readArtifact("colliding-parent", decision.declaredPath).content.toString()))
      .toMatchObject({ status: "unresolved", message: expect.stringContaining("different workflow") });
    store.close();
  });

  test("rejects recovery prompts that escape through a symlinked directory", async () => {
    const root = temporaryRepo();
    const outside = fs.mkdtempSync(path.join(os.tmpdir(), "agent-flow-recovery-outside-"));
    fs.writeFileSync(path.join(outside, "fix.md"), "outside secret\n");
    fs.symlinkSync(outside, path.join(root, "prompts"), "dir");
    const workflow = sessionRecoveryWorkflow("symlink-recovery", "prompts/fix.md");
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "symlink-recovery", workflow });
    let calls = 0;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      calls += 1;
      return { outputs: {}, metadata: { recovery_status: "unresolved" } };
    });

    const result = await executeAgentFlowCommandPipeline(
      store, "symlink-recovery", workflow, undefined, providers
    );

    expect(result.status).toBe("paused");
    expect(calls).toBe(0);
    const decision = store.listArtifacts("symlink-recovery")
      .find((artifact) => artifact.kind === "recovery_decision")!;
    expect(JSON.parse(store.readArtifact("symlink-recovery", decision.declaredPath).content.toString()))
      .toMatchObject({ status: "unresolved", message: expect.stringContaining("inside the repository") });
    store.close();
  });

  test("does not invoke recovery sessions after the model-call budget is exhausted", async () => {
    const root = temporaryRepo();
    fs.writeFileSync(path.join(root, "fix.md"), "fix\n");
    const workflow = sessionRecoveryWorkflow("budgeted-recovery", "fix.md", "limits: { max_model_calls: 1 }");
    (workflow.steps[0]!.on_failure as AgentFlowYamlMapping).on_unresolved = { then: "complete" };
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "budgeted-recovery", workflow });
    store.reserveBudgets([{
      id: "model:model_calls",
      runId: "budgeted-recovery",
      scope: "workflow",
      kind: "model_calls",
      limit: 1,
      amount: 1,
      unit: "calls"
    }]);
    let calls = 0;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      calls += 1;
      return { outputs: {}, metadata: { recovery_status: "unresolved" } };
    });

    const result = await executeAgentFlowCommandPipeline(
      store, "budgeted-recovery", workflow, undefined, providers
    );

    expect(result.status).toBe("paused");
    expect(result.message).toContain("model_calls");
    expect(calls).toBe(0);
    expect(store.getBudget("budgeted-recovery", "model:model_calls")?.used).toBe(1);
    expect(store.listArtifacts("budgeted-recovery")
      .filter((artifact) => artifact.kind === "recovery_decision")).toHaveLength(0);
    expect(store.getRun("budgeted-recovery")?.status).toBe("paused");
    store.close();
  });

  test("fails the run when a recovery decision cannot be persisted", async () => {
    const root = temporaryRepo();
    fs.writeFileSync(path.join(root, "fix.md"), "fix\n");
    const decisionPath = "recoveries/command-check-20f65c28-attempt-1-777065d3/decision.json";
    const workflow = parseAgentFlowWorkflowOrThrow(`name: colliding-recovery-decision
version: 1
style: recovery_pipeline
maturity: experimental
sessions:
  fixer: { provider: fixture }
steps:
  - id: occupy
    type: command
    command: mkdir -p $(dirname ${decisionPath}) && echo occupied > ${decisionPath}
    outputs: [${decisionPath}]
  - id: check
    type: command
    command: exit 1
    on_failure:
      route_to: { session: fixer, prompt: fix.md }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "colliding-recovery-decision", workflow });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => ({
      outputs: {}, metadata: { recovery_status: "unresolved" }
    }));

    const result = await executeAgentFlowCommandPipeline(
      store, "colliding-recovery-decision", workflow, undefined, providers
    );

    expect(result).toMatchObject({
      status: "failed",
      failedStep: "check",
      message: expect.stringContaining("Could not persist the recovery decision")
    });
    expect(store.getRun("colliding-recovery-decision")?.status).toBe("failed");
    store.close();
  });

  test("aborts recovery sessions and skips recovery decisions when the parent is cancelled", async () => {
    const root = temporaryRepo();
    fs.writeFileSync(path.join(root, "fix.md"), "fix\n");
    const workflow = sessionRecoveryWorkflow("cancel-session-recovery", "fix.md");
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "cancel-session-recovery", workflow });
    let started!: () => void;
    const providerStarted = new Promise<void>((resolve) => { started = resolve; });
    let aborted = false;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => {
      request.signal.addEventListener("abort", () => { aborted = true; });
      started();
      return new Promise(() => {});
    });
    const execution = executeAgentFlowCommandPipeline(
      store, "cancel-session-recovery", workflow, undefined, providers
    );
    await providerStarted;

    transitionAgentFlowLifecycleRun(store, "cancel-session-recovery", "cancel");
    const result = await execution;

    expect(result.status).toBe("cancelled");
    expect(aborted).toBe(true);
    expect(store.listEvents("cancel-session-recovery").map((event) => event.type)).not.toContain("recovery.completed");
    expect(store.listFailures("cancel-session-recovery")[0]?.resolvedAt).toBeNull();
    store.close();
  });

  test("keeps recovery sessions cancelled when cancellation happens as the provider returns", async () => {
    const root = temporaryRepo();
    fs.writeFileSync(path.join(root, "fix.md"), "fix\n");
    const workflow = sessionRecoveryWorkflow("cancel-on-response", "fix.md");
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "cancel-on-response", workflow });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      transitionAgentFlowLifecycleRun(store, "cancel-on-response", "cancel");
      return {
        outputs: {},
        externalSessionId: "cancelled-session",
        metadata: { recovery_status: "remediated" }
      };
    });

    const result = await executeAgentFlowCommandPipeline(
      store, "cancel-on-response", workflow, undefined, providers
    );

    expect(result.status).toBe("cancelled");
    expect(store.getSession("cancel-on-response", "fixer")).toMatchObject({
      status: "cancelled",
      externalSessionId: "cancelled-session",
      state: { interrupted: "cancelled" }
    });
    expect(store.listEvents("cancel-on-response").map((event) => event.type))
      .not.toContain("recovery.completed");
    store.close();
  });

  test("settles recovery sessions as cancelled when cancellation races with the waiting write", async () => {
    const root = temporaryRepo();
    fs.writeFileSync(path.join(root, "fix.md"), "fix\n");
    const workflow = sessionRecoveryWorkflow("cancel-while-settling", "fix.md");
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "cancel-while-settling", workflow });
    const settleSessionForRun = store.settleSessionForRun.bind(store);
    let cancelled = false;
    store.settleSessionForRun = (input) => {
      if (!cancelled) {
        cancelled = true;
        transitionAgentFlowLifecycleRun(store, "cancel-while-settling", "cancel");
      }
      return settleSessionForRun(input);
    };
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => ({
      outputs: {},
      externalSessionId: "cancelled-session",
      metadata: { recovery_status: "remediated" }
    }));

    const result = await executeAgentFlowCommandPipeline(
      store, "cancel-while-settling", workflow, undefined, providers
    );

    expect(result.status).toBe("cancelled");
    expect(store.getSession("cancel-while-settling", "fixer")).toMatchObject({
      status: "cancelled",
      externalSessionId: "cancelled-session",
      state: { interrupted: "cancelled" }
    });
    expect(store.listEvents("cancel-while-settling").map((event) => event.type))
      .not.toContain("recovery.completed");
    store.close();
  });

  test("cancels active nested recovery work with its parent", async () => {
    const root = temporaryRepo();
    const parent = parseAgentFlowWorkflowOrThrow(`name: cancel-nested-parent
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - id: check
    type: command
    command: exit 1
    on_failure:
      route_to: { workflow: slow-repair }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const child = parseAgentFlowWorkflowOrThrow(`name: slow-repair
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - { id: fix, type: command, command: sleep 1 && touch child-finished }
  - { id: done, type: result, status: remediated }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "cancel-nested-parent", workflow: parent });
    const workflows = createAgentFlowWorkflowRegistry().register("slow-repair", child);
    const execution = executeAgentFlowCommandPipeline(
      store, "cancel-nested-parent", parent, undefined, undefined, undefined, undefined, workflows
    );
    const childRunId = "cancel-nested-parent:recovery:check-20f65c28:attempt-1";
    for (let count = 0; count < 100 && store.getRun(childRunId)?.status !== "running"; count += 1) {
      await Bun.sleep(10);
    }

    transitionAgentFlowLifecycleRun(store, "cancel-nested-parent", "cancel");
    const result = await execution;

    expect(result.status).toBe("cancelled");
    expect(store.getRun(childRunId)?.status).toBe("cancelled");
    expect(fs.existsSync(path.join(root, "child-finished"))).toBe(false);
    expect(store.listEvents("cancel-nested-parent").map((event) => event.type)).not.toContain("recovery.completed");
    store.close();
  });

  test("rejects idempotent run reuse when recovery lineage differs", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: lineage
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - { id: done, type: result, status: remediated }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "parent-a", workflow });
    createAgentFlowLifecycleRun(store, { id: "parent-b", workflow });
    createAgentFlowLifecycleRun(store, {
      id: "shared-child", workflow, parentRunId: "parent-a", recoveryOfRunId: "parent-a"
    });

    expect(() => createAgentFlowLifecycleRun(store, {
      id: "shared-child", workflow, parentRunId: "parent-b", recoveryOfRunId: "parent-b"
    })).toThrow(/already exists/);
    store.close();
  });

  test("fails runtime preflight for directly persisted recovery routes with missing handlers", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: malformed-persisted-recovery
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - id: check
    type: command
    command: touch should-not-run
    on_failure:
      route_to: { workflow: repair }
`);
    const repair = parseAgentFlowWorkflowOrThrow(`name: repair
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - { id: done, type: result, status: unresolved }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    store.createRun({
      id: "malformed-persisted-recovery",
      workflow: {
        name: workflow.name,
        version: workflow.version,
        style: workflow.style,
        maturity: workflow.maturity
      },
      context: { workflow: workflow as never }
    });

    await expect(executeAgentFlowCommandPipeline(
      store,
      "malformed-persisted-recovery",
      workflow,
      undefined,
      undefined,
      undefined,
      undefined,
      createAgentFlowWorkflowRegistry().register("repair", repair)
    )).rejects.toMatchObject({ code: "AGENT_FLOW_WORKFLOW_INVALID" });
    expect(store.getRun("malformed-persisted-recovery")?.status).toBe("pending");
    expect(fs.existsSync(path.join(root, "should-not-run"))).toBe(false);
    store.close();
  });

  test("fails runtime preflight before invoking recovery with an invalid return target", async () => {
    const root = temporaryRepo();
    fs.writeFileSync(path.join(root, "fix.md"), "fix\n");
    const workflow = parseAgentFlowWorkflowOrThrow(`name: invalid-runtime-return
version: 1
style: recovery_pipeline
maturity: experimental
sessions:
  fixer: { provider: fixture }
steps:
  - id: check
    type: command
    command: exit 1
    on_failure:
      route_to: { session: fixer, prompt: fix.md }
      on_remediated: { return_to: other }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    store.createRun({
      id: "invalid-runtime-return",
      workflow: {
        name: workflow.name,
        version: workflow.version,
        style: workflow.style,
        maturity: workflow.maturity
      },
      context: { workflow }
    });
    let calls = 0;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      calls += 1;
      return { outputs: {}, metadata: { recovery_status: "remediated" } };
    });

    await expect(executeAgentFlowCommandPipeline(
      store, "invalid-runtime-return", workflow, undefined, providers
    )).rejects.toThrow("on_remediated.return_to must name the failed step");
    expect(calls).toBe(0);
    expect(store.getRun("invalid-runtime-return")?.status).toBe("pending");
    store.close();
  });

  test("fails runtime preflight for malformed persisted recovery configuration", async () => {
    const root = temporaryRepo();
    fs.writeFileSync(path.join(root, "fix.md"), "fix\n");
    for (const [suffix, inputs, message] of [
      ["shape", ["not", "a", "mapping"], "route_to.inputs must be a mapping"],
      ["expression", { value: "{{ artifacts.failure }}" }, "route_to.inputs expressions must use"],
      ["undeclared-input", { value: "{{ inputs.missing }}" }, "references undeclared workflow input missing"],
      ["prompt", {}, "session prompt must be a normalized repo-relative file path"],
      ["target", {}, "on_remediated.then target missing-target is unresolved"]
    ] as const) {
      const workflow = parseAgentFlowWorkflowOrThrow(`name: invalid-runtime-inputs-${suffix}
version: 1
style: recovery_pipeline
maturity: experimental
sessions:
  fixer: { provider: fixture }
steps:
  - id: check
    type: command
    command: touch should-not-run-${suffix}
    on_failure:
      route_to:
        session: fixer
        prompt: fix.md
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
      const onFailure = workflow.steps[0]!.on_failure as AgentFlowYamlMapping;
      onFailure.route_to = {
        session: "fixer",
        prompt: suffix === "prompt" ? "../outside.md" : "fix.md",
        inputs
      };
      if (suffix === "target") onFailure.on_remediated = { then: "missing-target" };
      const runId = `invalid-runtime-inputs-${suffix}`;
      const store = await openAgentFlowRunState({ cwd: root });
      store.createRun({
        id: runId,
        workflow: {
          name: workflow.name,
          version: workflow.version,
          style: workflow.style,
          maturity: workflow.maturity
        },
        context: { workflow }
      });
      await expect(executeAgentFlowCommandPipeline(
        store,
        runId,
        workflow,
        undefined,
        createAgentFlowSessionProviderRegistry().register("fixture", () => ({
          outputs: {}, metadata: { recovery_status: "unresolved" }
        }))
      )).rejects.toThrow(message);
      expect(store.getRun(runId)?.status).toBe("pending");
      expect(fs.existsSync(path.join(root, `should-not-run-${suffix}`))).toBe(false);
      store.close();
    }
  });

  test("rejects dynamic recovery targets hidden beside static persisted targets", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: dynamic-persisted-route
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - id: check
    type: command
    command: touch should-not-run
    on_failure:
      route_to: { workflow: repair }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const repair = parseAgentFlowWorkflowOrThrow(`name: repair
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - { id: done, type: result, status: unresolved }
`);
    const route = (workflow.steps[0]!.on_failure as AgentFlowYamlMapping).route_to as AgentFlowYamlMapping;
    route.session = "{{ inputs.which }}";
    const store = await openAgentFlowRunState({ cwd: root });
    store.createRun({
      id: "dynamic-persisted-route",
      workflow: {
        name: workflow.name,
        version: workflow.version,
        style: workflow.style,
        maturity: workflow.maturity
      },
      context: { workflow }
    });

    await expect(executeAgentFlowCommandPipeline(
      store,
      "dynamic-persisted-route",
      workflow,
      undefined,
      undefined,
      undefined,
      undefined,
      createAgentFlowWorkflowRegistry().register("repair", repair)
    )).rejects.toThrow("session and workflow targets must be static");
    expect(store.getRun("dynamic-persisted-route")?.status).toBe("pending");
    expect(fs.existsSync(path.join(root, "should-not-run"))).toBe(false);
    store.close();
  });

  test("routes a remediated result through on_unresolved when child finalization fails", async () => {
    const root = temporaryRepo();
    const parent = parseAgentFlowWorkflowOrThrow(`name: finalization-parent
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - id: check
    type: command
    command: exit 1
    on_failure:
      route_to: { workflow: repair }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const repair = parseAgentFlowWorkflowOrThrow(`name: repair
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: done, type: result, status: remediated }
notify:
  - { on: workflow.completed, channels: [system], required: true }
`);
    const notifications = createAgentFlowNotificationRegistry({
      system: () => { throw new Error("notification failed"); }
    });
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "finalization-parent", workflow: parent });

    const result = await executeAgentFlowCommandPipeline(
      store,
      "finalization-parent",
      parent,
      undefined,
      undefined,
      undefined,
      notifications,
      createAgentFlowWorkflowRegistry().register("repair", repair)
    );

    expect(result.status).toBe("paused");
    expect(store.listFailures("finalization-parent")[0]).toMatchObject({
      resolvedAt: null,
      payload: { recovery: { status: "unresolved", target: "repair" } }
    });
    expect(store.getArtifact("finalization-parent", "repaired.txt")).toBeNull();
    store.close();
  });

  test("preserves a returned resumable session ID when recovery metadata is invalid", async () => {
    const root = temporaryRepo();
    fs.writeFileSync(path.join(root, "fix.md"), "fix\n");
    const workflow = sessionRecoveryWorkflow("invalid-recovery-metadata", "fix.md",
      "limits: { max_recovery_cycles: 2, max_step_attempts: { check: 2 } }");
    workflow.sessions!.fixer = { provider: "fixture", resume: true };
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "invalid-recovery-metadata", workflow });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => ({
      outputs: {},
      externalSessionId: "created-before-invalid-metadata",
      metadata: {}
    }));

    const result = await executeAgentFlowCommandPipeline(
      store, "invalid-recovery-metadata", workflow, undefined, providers
    );

    expect(result.status).toBe("paused");
    expect(store.getSession("invalid-recovery-metadata", "fixer")).toMatchObject({
      status: "paused",
      externalSessionId: "created-before-invalid-metadata"
    });
    store.close();
  });

  test("rejects invalid persisted result statuses before starting the run", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: invalid-result-status
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - { id: done, type: result, status: unsupported }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    store.createRun({
      id: "invalid-result-status",
      workflow: {
        name: workflow.name,
        version: workflow.version,
        style: workflow.style,
        maturity: workflow.maturity
      },
      context: { workflow: workflow as never }
    });

    await expect(executeAgentFlowCommandPipeline(store, "invalid-result-status", workflow))
      .rejects.toMatchObject({ code: "AGENT_FLOW_RESULT_STATUS" });
    expect(store.getRun("invalid-result-status")?.status).toBe("pending");
    store.close();
  });

  test("rejects persisted command recovery handlers without a route before starting the run", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: orphan-runtime-handler
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - id: check
    type: command
    command: touch should-not-run
    on_failure:
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    store.createRun({
      id: "orphan-runtime-handler",
      workflow: {
        name: workflow.name,
        version: workflow.version,
        style: workflow.style,
        maturity: workflow.maturity
      },
      context: { workflow: workflow as never }
    });

    await expect(executeAgentFlowCommandPipeline(store, "orphan-runtime-handler", workflow))
      .rejects.toThrow("on_remediated and on_unresolved require route_to");
    expect(store.getRun("orphan-runtime-handler")?.status).toBe("pending");
    expect(fs.existsSync(path.join(root, "should-not-run"))).toBe(false);
    store.close();
  });

  test("fails before execution when a nested recovery workflow is not registered", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: missing-registry
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - id: check
    type: command
    command: touch should-not-run
    on_failure:
      route_to: { workflow: missing }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "missing-registry", workflow });

    await expect(executeAgentFlowCommandPipeline(store, "missing-registry", workflow))
      .rejects.toMatchObject({ code: "AGENT_FLOW_RECOVERY_WORKFLOW_UNKNOWN" });
    expect(store.getRun("missing-registry")?.status).toBe("pending");
    expect(fs.existsSync(path.join(root, "should-not-run"))).toBe(false);
    store.close();
  });
});

function temporaryRepo(): string {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "agent-flow-recovery-"));
  fs.mkdirSync(path.join(root, ".git"));
  return root;
}

function sessionRecoveryWorkflow(name: string, prompt: string, extra = "") {
  return parseAgentFlowWorkflowOrThrow(`name: ${name}
version: 1
style: recovery_pipeline
maturity: experimental
${extra}
sessions:
  fixer: { provider: fixture }
steps:
  - id: check
    type: command
    command: exit 1
    on_failure:
      route_to: { session: fixer, prompt: ${prompt} }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
}
