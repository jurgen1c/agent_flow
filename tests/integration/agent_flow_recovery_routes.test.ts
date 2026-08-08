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
  - id: malformed-remediated
    type: command
    command: exit 1
    on_failure:
      on_remediated: complete
  - id: malformed-unresolved
    type: command
    command: exit 1
    on_failure:
      on_unresolved: pause
`);

    expect(validateAgentFlowWorkflow(workflow).errors
      .filter((issue) => issue.code === "workflow.recovery.route.required")
      .map((issue) => issue.path)).toEqual([
      "steps[0].on_failure.on_remediated",
      "steps[1].on_failure.on_remediated",
      "steps[2].on_failure.on_remediated",
      "steps[3].on_failure.on_unresolved"
    ]);
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
    strategy: fail_fast
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
        file_scope: { include: [fixed.txt] }
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
    expect(recoveryRun!.inputs.failure_payload).toEqual(expect.stringMatching(/^recovery-inputs\//));
    expect(store.getArtifact(recoveryRun!.id, recoveryRun!.inputs.failure_payload as string)).not.toBeNull();
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
      route_to: { workflow: shared-output-repair, file_scope: { include: [fixed.txt] } }
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
      route_to: { workflow: repair-earlier-output, file_scope: { include: [source.txt, recovered.txt] } }
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
      route_to: { workflow: repair-artifact, file_scope: { include: [source.txt] } }
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

  test("promotes implicit approval and decision record outputs from nested recovery", async () => {
    const root = temporaryRepo();
    const parent = parseAgentFlowWorkflowOrThrow(`name: implicit-output-parent
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_recovery_cycles: 1 }
steps:
  - id: check
    type: command
    command: exit 1
    on_failure:
      route_to: { workflow: implicit-output-child, file_scope: { include: [evidence.md] } }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const child = parseAgentFlowWorkflowOrThrow(`name: implicit-output-child
version: 1
style: pipeline
maturity: experimental
sessions:
  reviewer:
    provider: fixture
    authority: { can_approve: true }
steps:
  - { id: evidence, type: command, command: echo evidence > evidence.md, outputs: [evidence.md] }
  - { id: approve, type: approval, reviewer: reviewer, artifacts: [evidence.md] }
  - { id: record, type: decision_record, owner: reviewer, topic: Recovery approved, artifacts: [approvals/approve.json] }
  - { id: done, type: result, status: remediated }
`);
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => ({
      outputs: {
        "approvals/approve.json": JSON.stringify({ status: "approved", decision: "Recovery evidence is valid." })
      }
    }));
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "implicit-output-parent", workflow: parent });

    const result = await executeAgentFlowCommandPipeline(
      store,
      "implicit-output-parent",
      parent,
      undefined,
      providers,
      undefined,
      undefined,
      createAgentFlowWorkflowRegistry().register("implicit-output-child", child)
    );

    expect(result).toMatchObject({ status: "completed" });
    expect(store.getArtifact("implicit-output-parent", "approvals/approve.json")?.metadata)
      .toMatchObject({ recoveryRunId: expect.any(String) });
    expect(store.getArtifact("implicit-output-parent", "decision-records/record.json")?.metadata)
      .toMatchObject({ recoveryRunId: expect.any(String) });
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
      route_to: { workflow: promotion-failure-child, file_scope: { include: [a.txt, b.txt] } }
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

  test("namespaces copied failure artifacts away from nested command log paths", async () => {
    const root = temporaryRepo();
    const collidingLogPath = `logs/check-${createHash("sha256").update("check").digest("hex").slice(0, 8)}/attempt-1/stdout.log`;
    const parent = parseAgentFlowWorkflowOrThrow(`name: colliding-log-parent
version: 1
style: recovery_pipeline
maturity: experimental
inputs: { log_path: { required: true } }
steps:
  - id: check
    type: command
    command: printf diagnostic; exit 1
    on_failure:
      route_to:
        workflow: colliding-log-child
        inputs:
          diagnostic: "{{ inputs.log_path }}"
          failure_payload: "{{ failure.path }}"
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const child = parseAgentFlowWorkflowOrThrow(`name: colliding-log-child
version: 1
style: recovery_pipeline
maturity: experimental
inputs:
  diagnostic: { required: true }
  failure_payload: { required: true }
steps:
  - { id: check, type: command, command: printf repaired }
  - { id: done, type: result, status: remediated }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, {
      id: "colliding-log-parent",
      workflow: parent,
      inputs: { log_path: collidingLogPath }
    });

    const result = await executeAgentFlowCommandPipeline(
      store,
      "colliding-log-parent",
      parent,
      undefined,
      undefined,
      undefined,
      undefined,
      createAgentFlowWorkflowRegistry().register("colliding-log-child", child)
    );

    expect(result.status).toBe("completed");
    const failure = store.listFailures("colliding-log-parent")[0]!;
    const recoveryRunId = (failure.payload as { recovery: { recoveryRunId: string } }).recovery.recoveryRunId;
    const recoveryRun = store.getRun(recoveryRunId)!;
    const failureInputPath = recoveryRun.inputs.failure_payload as string;
    expect(failureInputPath).toMatch(/^recovery-inputs\//);
    const copiedFailure = JSON.parse(store.readArtifact(recoveryRunId, failureInputPath).content.toString()) as {
      logs: { stdout: string };
    };
    expect(copiedFailure.logs.stdout).toMatch(/^recovery-inputs\//);
    const copiedLog = store.getArtifact(recoveryRunId, copiedFailure.logs.stdout)!;
    expect(copiedLog.kind).toBe("recovery_input");
    expect(recoveryRun.inputs.diagnostic).toMatch(/^recovery-inputs\//);
    expect(store.getArtifact(recoveryRunId, recoveryRun.inputs.diagnostic as string)?.metadata.sourcePath)
      .toBe(collidingLogPath);
    expect(store.getArtifact(recoveryRunId, collidingLogPath)?.kind).toBe("command_log");
    store.close();
  });

  test("promotes outputs authored by a nested step named recovery-input", async () => {
    const root = temporaryRepo();
    const parent = parseAgentFlowWorkflowOrThrow(`name: recovery-input-step-parent
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - id: check
    type: command
    command: exit 1
    on_failure:
      route_to: { workflow: recovery-input-step-child, file_scope: { include: [repaired.txt] } }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const child = parseAgentFlowWorkflowOrThrow(`name: recovery-input-step-child
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - id: recovery-input
    type: command
    command: printf repaired > repaired.txt
    outputs: [repaired.txt]
  - { id: done, type: result, status: remediated }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "recovery-input-step-parent", workflow: parent });

    const result = await executeAgentFlowCommandPipeline(
      store,
      "recovery-input-step-parent",
      parent,
      undefined,
      undefined,
      undefined,
      undefined,
      createAgentFlowWorkflowRegistry().register("recovery-input-step-child", child)
    );

    expect(result.status).toBe("completed");
    expect(store.readArtifact("recovery-input-step-parent", "repaired.txt").content.toString()).toBe("repaired");
    store.close();
  });

  test("preserves nonconflicting copied paths for static child artifact consumers", async () => {
    const root = temporaryRepo();
    const parent = parseAgentFlowWorkflowOrThrow(`name: static-artifact-parent
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - id: check
    type: command
    command: exit 1
    on_failure:
      route_to:
        workflow: static-artifact-child
        inputs: { ticket: ticket.json }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const child = parseAgentFlowWorkflowOrThrow(`name: static-artifact-child
version: 1
style: recovery_pipeline
maturity: experimental
inputs: { ticket: { required: true } }
steps:
  - id: render
    type: artifact_transform
    input: ticket.json
    output: ticket.md
    transform: jira_ticket_to_markdown
  - { id: done, type: result, status: remediated }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "static-artifact-parent", workflow: parent });
    store.writeArtifact({
      id: "ticket-input",
      runId: "static-artifact-parent",
      path: "ticket.json",
      kind: "input",
      contentType: "application/json",
      content: JSON.stringify({ key: "AM-32", fields: { summary: "Recovery", description: "Route failure" } })
    });

    const result = await executeAgentFlowCommandPipeline(
      store,
      "static-artifact-parent",
      parent,
      undefined,
      undefined,
      undefined,
      undefined,
      createAgentFlowWorkflowRegistry().register("static-artifact-child", child)
    );

    expect(result.status).toBe("completed");
    const failure = store.listFailures("static-artifact-parent")[0]!;
    const recoveryRunId = (failure.payload as { recovery: { recoveryRunId: string } }).recovery.recoveryRunId;
    expect(store.getRun(recoveryRunId)?.inputs.ticket).toBe("ticket.json");
    expect(store.getArtifact(recoveryRunId, "ticket.md")).not.toBeNull();
    expect(store.getArtifact("static-artifact-parent", "ticket.md")).not.toBeNull();
    store.close();
  });

  test("remaps copied JSON inputs away from nested session metadata paths", async () => {
    const root = temporaryRepo();
    fs.writeFileSync(path.join(root, "fix.md"), "Fix the failure.\n");
    const requestPath = `session-requests/fix-${createHash("sha256").update("fix").digest("hex").slice(0, 12)}.json`;
    const inputAlias = `alias/../${requestPath}`;
    const parent = parseAgentFlowWorkflowOrThrow(`name: session-metadata-collision-parent
version: 1
style: recovery_pipeline
maturity: experimental
inputs: { source: { required: true } }
steps:
  - id: check
    type: command
    command: exit 1
    on_failure:
      route_to:
        workflow: session-metadata-collision-child
        inputs: { source: "{{ inputs.source }}" }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);
    const child = parseAgentFlowWorkflowOrThrow(`name: session-metadata-collision-child
version: 1
style: recovery_pipeline
maturity: experimental
inputs: { source: { required: true } }
sessions: { fixer: { provider: fixture } }
steps:
  - id: fix
    type: session_request
    session: fixer
    prompt: fix.md
    inputs: ["{{ inputs.source }}"]
    outputs: [repaired.txt]
  - { id: done, type: result, status: remediated }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, {
      id: "session-metadata-collision-parent",
      workflow: parent,
      inputs: { source: inputAlias }
    });
    store.writeArtifact({
      id: "colliding-session-input",
      runId: "session-metadata-collision-parent",
      path: requestPath,
      kind: "input",
      contentType: "application/problem+json; charset=utf-8",
      content: `{"count":9007199254740993,"path":${JSON.stringify(inputAlias)}}`
    });
    let calls = 0;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => {
      calls += 1;
      expect(request.inputs[0]?.path).toMatch(/^recovery-inputs\//);
      expect(Buffer.from(request.inputs[0]!.content).toString()).toBe(
        `{"count":9007199254740993,"path":${JSON.stringify(request.inputs[0]!.path)}}`
      );
      return { outputs: { "repaired.txt": "repaired" } };
    });

    const result = await executeAgentFlowCommandPipeline(
      store,
      "session-metadata-collision-parent",
      parent,
      undefined,
      providers,
      undefined,
      undefined,
      createAgentFlowWorkflowRegistry().register("session-metadata-collision-child", child)
    );

    expect(result.status).toBe("completed");
    expect(calls).toBe(1);
    expect(store.readArtifact("session-metadata-collision-parent", "repaired.txt").content.toString())
      .toBe("repaired");
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
  fixer:
    provider: fixture
    resume: true
    authority: { can_modify_files: true }
    file_scope: { include: [fixed.txt] }
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
  fixer:
    provider: fixture
    resume: true
    authority: { can_modify_files: true }
    file_scope: { include: [fixed.txt] }
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
        expect(store.listFailures("repeated-session-recovery")).toEqual([
          expect.objectContaining({
            attempt: 1,
            resolvedAt: null,
            payload: expect.objectContaining({
              recovery: expect.objectContaining({ status: "remediated" })
            })
          }),
          expect.objectContaining({ attempt: 2, resolvedAt: null })
        ]);
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
    expect(store.listEvents("repeated-session-recovery")
      .filter((event) => event.type === "step.started" && event.stepId === "check")
      .map((event) => (event.payload as { attempt: number }).attempt)).toEqual([1, 2, 3]);
    expect(store.listEvents("repeated-session-recovery")
      .filter((event) => event.type === "recovery.completed")).toHaveLength(2);
    expect(store.listEvents("repeated-session-recovery")
      .filter((event) => event.type === "recovery.returned")
      .map((event) => event.payload)).toEqual([
      expect.objectContaining({ failedAttempt: 1, successfulAttempt: 3 }),
      expect.objectContaining({ failedAttempt: 2, successfulAttempt: 3 })
    ]);
    store.close();
  });

  test("does not retry a successful returned provider call when recovery bookkeeping fails", async () => {
    const root = temporaryRepo();
    fs.writeFileSync(path.join(root, "work.md"), "Do the work.\n");
    fs.writeFileSync(path.join(root, "fix.md"), "Fix the failure.\n");
    const workflow = parseAgentFlowWorkflowOrThrow(`name: recovery-bookkeeping-failure
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_recovery_cycles: 1, max_step_attempts: { work: 2 } }
sessions:
  worker: { provider: fixture }
  fixer: { provider: fixture }
steps:
  - id: work
    type: session_request
    session: worker
    prompt: work.md
    inputs: [request.txt]
    outputs: [result.txt]
    on_failure:
      route_to: { session: fixer, prompt: fix.md }
      on_remediated: { return_to: work }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "recovery-bookkeeping-failure", workflow });
    store.writeArtifact({
      id: "request",
      runId: "recovery-bookkeeping-failure",
      path: "request.txt",
      kind: "fixture",
      contentType: "text/plain",
      content: "request"
    });
    let workCalls = 0;
    let recoveryCalls = 0;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => {
      if (request.stepId === "work:recovery") {
        recoveryCalls += 1;
        return { outputs: {}, metadata: { recovery_status: "remediated" } };
      }
      workCalls += 1;
      if (workCalls === 1) throw new Error("provider failed");
      return { outputs: { "result.txt": "done" } };
    });
    const appendRunEvent = store.appendRunEvent.bind(store);
    store.appendRunEvent = (runId, event) => {
      if (event.type === "recovery.returned") throw new Error("bookkeeping unavailable");
      return appendRunEvent(runId, event);
    };

    const result = await executeAgentFlowCommandPipeline(
      store, "recovery-bookkeeping-failure", workflow, undefined, providers
    );

    expect(result).toMatchObject({
      status: "failed",
      completedSteps: ["work"],
      message: expect.stringContaining("Could not persist return-to recovery completion")
    });
    expect(workCalls).toBe(2);
    expect(recoveryCalls).toBe(1);
    expect(store.listEvents("recovery-bookkeeping-failure")).toContainEqual(expect.objectContaining({
      type: "step.completed",
      stepId: "work",
      payload: expect.objectContaining({ attempt: 2 })
    }));
    expect(store.listFailures("recovery-bookkeeping-failure")[0]?.resolvedAt).toBeNull();
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
    expect(store.listFailures("bounded-recovery-invocation")).toEqual([
      expect.objectContaining({
        attempt: 1,
        resolvedAt: null,
        payload: expect.objectContaining({
          recovery: expect.objectContaining({ status: "remediated" })
        })
      }),
      expect.objectContaining({ attempt: 2, resolvedAt: null }),
      expect.objectContaining({ classification: "routing_limit", resolvedAt: null })
    ]);
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
    expect(store.listFailures("selected-handler-budget")[0]).toMatchObject({
      resolvedAt: expect.any(String),
      payload: { recovery: { status: "remediated" } }
    });
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
    const settleRecoverySession = store.settleRecoverySessionForRunAtContextRevision.bind(store);
    let cancelled = false;
    store.settleRecoverySessionForRunAtContextRevision = (input, revision) => {
      if (!cancelled) {
        cancelled = true;
        transitionAgentFlowLifecycleRun(store, "cancel-while-settling", "cancel");
      }
      return settleRecoverySession(input, revision);
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
