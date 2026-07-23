import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  AgentflowArtifactTransformRegistry,
  type AgentflowRunStateValue,
  createAgentflowLifecycleRun,
  executeAgentflowCommandPipeline,
  openAgentflowRunState,
  parseAgentflowWorkflowOrThrow,
  validateAgentflowWorkflow
} from "../../packages/agentflow-core/src";

describe("Agentflow pipeline ordering and conditions", () => {
  test("runs successful steps in listed order by default", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`name: listed-order
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: first, type: command, command: "printf 'first\\n' >> order.txt" }
  - { id: second, type: command, command: "printf 'second\\n' >> order.txt" }
  - { id: third, type: command, command: "printf 'third\\n' >> order.txt" }
`);
    const store = await openAgentflowRunState({ cwd: root });
    createAgentflowLifecycleRun(store, { id: "listed-order", workflow });

    const result = await executeAgentflowCommandPipeline(store, "listed-order", workflow);

    expect(result).toEqual({ status: "completed", completedSteps: ["first", "second", "third"] });
    expect(fs.readFileSync(path.join(root, "order.txt"), "utf8")).toBe("first\nsecond\nthird\n");
    store.close();
  });

  test("normalizes padded executable step types during runtime dispatch", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`name: normalized-runtime-dispatch
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: write, type: " command ", command: "printf source > source.txt", outputs: [source.txt] }
  - { id: render, type: " artifact_transform ", input: source.txt, output: rendered.txt, transform: uppercase }
`);
    expect(validateAgentflowWorkflow(workflow)).toEqual({ valid: true, errors: [] });
    const store = await openAgentflowRunState({ cwd: root });
    createAgentflowLifecycleRun(store, { id: "normalized-runtime-dispatch", workflow });
    const transforms = new AgentflowArtifactTransformRegistry().register("uppercase", (input) => ({
      content: Buffer.from(input).toString("utf8").toUpperCase(),
      contentType: "text/plain"
    }));

    const result = await executeAgentflowCommandPipeline(
      store,
      "normalized-runtime-dispatch",
      workflow,
      transforms
    );

    expect(result).toEqual({ status: "completed", completedSteps: ["write", "render"] });
    expect(store.readArtifact("normalized-runtime-dispatch", "rendered.txt").content.toString()).toBe("SOURCE");
    store.close();
  });

  test("routes successful steps through an explicit then target", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`name: explicit-success-route
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: choose, type: command, command: "printf 'choose\\n' >> order.txt", then: finish }
  - { id: skipped, type: command, command: "printf 'skipped\\n' >> order.txt" }
  - { id: finish, type: command, command: "printf 'finish\\n' >> order.txt", then: complete }
`);
    const store = await openAgentflowRunState({ cwd: root });
    createAgentflowLifecycleRun(store, { id: "explicit-route", workflow });

    const result = await executeAgentflowCommandPipeline(store, "explicit-route", workflow);

    expect(result).toEqual({ status: "completed", completedSteps: ["choose", "finish"] });
    expect(fs.readFileSync(path.join(root, "order.txt"), "utf8")).toBe("choose\nfinish\n");
    store.close();
  });

  test("routes successful steps through a validated goto target", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`name: goto-route
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: first, type: command, command: "printf first > result.txt", goto: third }
  - { id: second, type: command, command: "printf second >> result.txt" }
  - { id: third, type: command, command: "printf third >> result.txt" }
`);
    const store = await openAgentflowRunState({ cwd: root });
    createAgentflowLifecycleRun(store, { id: "goto-route", workflow });

    const result = await executeAgentflowCommandPipeline(store, "goto-route", workflow);

    expect(result.completedSteps).toEqual(["first", "third"]);
    expect(fs.readFileSync(path.join(root, "result.txt"), "utf8")).toBe("firstthird");
    store.close();
  });

  test("routes to nested step ids before interpreting terminal aliases", async () => {
    const repo = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`name: nested-route
version: 1
style: pipeline
maturity: experimental
inputs: { ready: {} }
steps:
  - { id: route, type: condition, if: ready, then: complete, else: fail }
  - id: container
    type: loop
    max_iterations: 1
    body:
      - { id: complete, type: command, command: echo nested, then: completed }
`);
    const store = await openAgentflowRunState({ cwd: repo });
    createAgentflowLifecycleRun(store, { id: "nested-route", workflow, inputs: { ready: true } });

    const result = await executeAgentflowCommandPipeline(store, "nested-route", workflow);

    expect(result).toEqual({ status: "completed", completedSteps: ["route", "complete"] });
    store.close();
  });

  test("does not treat parallel branch descriptors as executable terminal aliases", async () => {
    const repo = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`name: descriptor-alias
version: 1
style: pipeline
maturity: experimental
sessions: { worker: { provider: local } }
steps:
  - { id: start, type: command, command: echo start, then: complete }
  - id: parallel_work
    type: parallel
    branches:
      - { id: complete, session: worker }
`);
    const store = await openAgentflowRunState({ cwd: repo });
    createAgentflowLifecycleRun(store, { id: "descriptor-alias", workflow });

    const result = await executeAgentflowCommandPipeline(store, "descriptor-alias", workflow);

    expect(result).toEqual({ status: "completed", completedSteps: ["start"] });
    store.close();
  });

  test("gives declared step ids precedence over terminal aliases", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`name: declared-terminal-alias
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: start, type: command, command: "printf start > result.txt", then: complete }
  - { id: complete, type: command, command: "printf routed >> result.txt" }
`);
    const store = await openAgentflowRunState({ cwd: root });
    createAgentflowLifecycleRun(store, { id: "declared-terminal", workflow });

    const result = await executeAgentflowCommandPipeline(store, "declared-terminal", workflow);

    expect(result.completedSteps).toEqual(["start", "complete"]);
    expect(fs.readFileSync(path.join(root, "result.txt"), "utf8")).toBe("startrouted");
    store.close();
  });

  test("keeps dynamic success targets as listed-order fallthrough", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`name: dynamic-success-route
version: 1
style: pipeline
maturity: experimental
inputs: { next: {} }
steps:
  - { id: first, type: command, command: "printf first > result.txt", then: "target-{{ inputs.next }}" }
  - { id: second, type: command, command: "printf second >> result.txt" }
`);
    const store = await openAgentflowRunState({ cwd: root });
    createAgentflowLifecycleRun(store, { id: "dynamic-route", workflow, inputs: { next: "second" } });

    const result = await executeAgentflowCommandPipeline(store, "dynamic-route", workflow);

    expect(result.completedSteps).toEqual(["first", "second"]);
    expect(fs.readFileSync(path.join(root, "result.txt"), "utf8")).toBe("firstsecond");
    store.close();
  });

  test("routes simple input and artifact conditions to the first matching branch", async () => {
    for (const [runId, inputs, expected] of [
      ["input-true", { ready: true }, "approved"],
      ["input-false", { ready: false }, "rejected"]
    ] as const) {
      const root = temporaryRepo();
      const workflow = parseAgentflowWorkflowOrThrow(`name: input-condition
version: 1
style: pipeline
maturity: experimental
inputs: { ready: { required: true } }
steps:
  - { id: route, type: " condition ", if: inputs.ready == true, then: approved, else: rejected }
  - { id: approved, type: command, command: "printf approved > result.txt", then: complete }
  - { id: rejected, type: command, command: "printf rejected > result.txt", then: complete }
`);
      const store = await openAgentflowRunState({ cwd: root });
      createAgentflowLifecycleRun(store, { id: runId, workflow, inputs: { ...inputs } });

      const result = await executeAgentflowCommandPipeline(store, runId, workflow);

      expect(result.completedSteps).toEqual(["route", expected]);
      expect(fs.readFileSync(path.join(root, "result.txt"), "utf8")).toBe(expected);
      store.close();
    }

    const missingRoot = temporaryRepo();
    const requiredWorkflow = parseAgentflowWorkflowOrThrow(`name: required-input-condition
version: 1
style: pipeline
maturity: experimental
inputs: { ready: { required: true } }
steps:
  - { id: route, type: condition, if: ready, then: approved, else: rejected }
  - { id: approved, type: command, command: "printf approved > result.txt", then: complete }
  - { id: rejected, type: command, command: "printf rejected > result.txt", then: complete }
`);
    const missingStore = await openAgentflowRunState({ cwd: missingRoot });
    createAgentflowLifecycleRun(missingStore, { id: "required-input", workflow: requiredWorkflow });
    const missingResult = await executeAgentflowCommandPipeline(missingStore, "required-input", requiredWorkflow);
    expect(missingResult).toMatchObject({
      status: "failed",
      failedStep: "route",
      message: "Required condition input ready was not provided for run required-input."
    });
    expect(fs.existsSync(path.join(missingRoot, "result.txt"))).toBe(false);
    missingStore.close();

    const shortCircuitRoot = temporaryRepo();
    const shortCircuitWorkflow = parseAgentflowWorkflowOrThrow(`name: required-input-short-circuit
version: 1
style: pipeline
maturity: experimental
inputs:
  ready: { required: true }
  later: { required: true }
steps:
  - id: route
    type: condition
    branches:
      - { if: ready, then: complete }
      - { if: later, then: complete }
    else: fail
`);
    const shortCircuitStore = await openAgentflowRunState({ cwd: shortCircuitRoot });
    createAgentflowLifecycleRun(shortCircuitStore, {
      id: "required-input-short-circuit",
      workflow: shortCircuitWorkflow,
      inputs: { ready: true }
    });

    const shortCircuitResult = await executeAgentflowCommandPipeline(
      shortCircuitStore,
      "required-input-short-circuit",
      shortCircuitWorkflow
    );

    expect(shortCircuitResult).toMatchObject({
      status: "failed",
      failedStep: "route",
      message: "Required condition input later was not provided for run required-input-short-circuit."
    });
    shortCircuitStore.close();

    const root = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`name: artifact-condition
version: 1
style: pipeline
maturity: experimental
steps:
  - id: route
    type: condition
    branches:
      - { if: artifacts.ci.failure_classification.kind == "flake", then: retry }
      - { if: artifacts.ci.failure_classification.requires_user == true, then: pause }
    else: fail
  - { id: retry, type: command, command: "printf retried > result.txt", then: complete }
`);
    const store = await openAgentflowRunState({ cwd: root });
    createAgentflowLifecycleRun(store, { id: "artifact-route", workflow });
    store.writeArtifact({
      id: "classification",
      runId: "artifact-route",
      stepId: "fixture",
      path: "ci/failure-classification.json",
      kind: "fixture",
      contentType: "application/json; charset=utf-8",
      content: JSON.stringify({ kind: "flake", requires_user: false })
    });
    store.listArtifacts = () => {
      throw new Error("condition lookup must not inspect every artifact");
    };

    const result = await executeAgentflowCommandPipeline(store, "artifact-route", workflow);

    expect(result.completedSteps).toEqual(["route", "retry"]);
    expect(fs.readFileSync(path.join(root, "result.txt"), "utf8")).toBe("retried");
    store.close();
  });

  test("preserves cancellation when artifact condition evaluation fails", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`name: cancelled-condition
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: route, type: condition, if: artifacts.state.ready == true, then: complete, else: fail }
`);
    const store = await openAgentflowRunState({ cwd: root });
    createAgentflowLifecycleRun(store, { id: "cancelled-condition", workflow });
    store.writeArtifact({
      id: "state",
      runId: "cancelled-condition",
      stepId: "fixture",
      path: "state.json",
      kind: "fixture",
      contentType: "application/json",
      content: JSON.stringify({ ready: true })
    });
    const readArtifact = store.readArtifact.bind(store);
    store.readArtifact = (...args) => {
      readArtifact(...args);
      store.transitionRunWithEvent("cancelled-condition", {
        status: "cancelled",
        allowedFrom: ["running"],
        event: { type: "run.cancelled", payload: { source: "test" } }
      });
      throw new Error("Artifact became stale during condition evaluation.");
    };

    const result = await executeAgentflowCommandPipeline(store, "cancelled-condition", workflow);

    expect(result).toMatchObject({ status: "cancelled", completedSteps: [] });
    expect(store.getRun("cancelled-condition")?.status).toBe("cancelled");
    expect(store.listEvents("cancelled-condition").map((event) => event.type)).not.toContain("step.failed");
    store.close();
  });

  test("ignores unpublished artifact reservations during condition lookup", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`name: published-condition-artifact
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: route, type: condition, if: artifacts.ci.ready == true, then: complete, else: fail }
`);
    const store = await openAgentflowRunState({ cwd: root });
    createAgentflowLifecycleRun(store, { id: "published-condition-artifact", workflow });
    store.writeArtifact({
      id: "published",
      runId: "published-condition-artifact",
      stepId: "fixture",
      path: "ci.json",
      kind: "fixture",
      contentType: "application/json",
      content: JSON.stringify({ ready: true })
    });
    store.upsertArtifact({
      id: "reserved",
      runId: "published-condition-artifact",
      stepId: "future",
      path: "ci/ready.json",
      kind: "fixture",
      contentType: "application/json"
    });

    const result = await executeAgentflowCommandPipeline(store, "published-condition-artifact", workflow);

    expect(result).toMatchObject({ status: "completed", completedSteps: ["route"] });
    store.close();
  });

  test("resolves dotted artifact filenames and rejects ambiguous aliases", async () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: artifact-aliases
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: route, type: condition, if: artifacts.ci.report.v1.status == "ready", then: complete, else: fail }
`);

    const dottedRoot = temporaryRepo();
    const dottedStore = await openAgentflowRunState({ cwd: dottedRoot });
    createAgentflowLifecycleRun(dottedStore, { id: "dotted-artifact", workflow });
    dottedStore.writeArtifact({
      id: "dotted",
      runId: "dotted-artifact",
      stepId: "fixture",
      path: "ci/report.v1.json",
      kind: "fixture",
      contentType: "application/json",
      content: JSON.stringify({ status: "ready" })
    });

    expect(await executeAgentflowCommandPipeline(dottedStore, "dotted-artifact", workflow))
      .toMatchObject({ status: "completed", completedSteps: ["route"] });
    dottedStore.close();

    const ambiguousRoot = temporaryRepo();
    const ambiguousStore = await openAgentflowRunState({ cwd: ambiguousRoot });
    const ambiguousWorkflow = parseAgentflowWorkflowOrThrow(`name: ambiguous-artifact-alias
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: route, type: condition, if: artifacts.foo_bar.status == "ready", then: complete, else: fail }
`);
    createAgentflowLifecycleRun(ambiguousStore, { id: "ambiguous-artifact", workflow: ambiguousWorkflow });
    for (const [id, artifactPath] of [["hyphen", "foo-bar.json"], ["underscore", "foo_bar.json"]]) {
      ambiguousStore.writeArtifact({
        id,
        runId: "ambiguous-artifact",
        stepId: "fixture",
        path: artifactPath,
        kind: "fixture",
        contentType: "application/json",
        content: JSON.stringify({ status: "ready" })
      });
    }

    const ambiguous = await executeAgentflowCommandPipeline(ambiguousStore, "ambiguous-artifact", ambiguousWorkflow);
    expect(ambiguous).toMatchObject({ status: "failed", failedStep: "route" });
    expect(ambiguous.message).toContain("matches multiple published artifacts");
    ambiguousStore.close();
  });

  test("keeps missing and cyclic route diagnostics deterministic", () => {
    const missing = parseAgentflowWorkflowOrThrow(`name: missing-route
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: start, type: command, command: echo start, then: nowhere }
`);
    const cyclic = parseAgentflowWorkflowOrThrow(`name: cyclic-route
version: 1
style: pipeline
maturity: experimental
limits: { max_recovery_cycles: 2 }
steps:
  - { id: first, type: command, command: 'node -e "console.log(process.pid)"', then: second }
  - { id: second, type: command, command: echo second, then: first }
`);
    const dynamicFallthroughCycle = parseAgentflowWorkflowOrThrow(`name: dynamic-cycle
version: 1
style: pipeline
maturity: experimental
inputs: { next: {} }
steps:
  - { id: first, type: command, command: echo first, then: "{{ inputs.next }}" }
  - { id: second, type: command, command: echo second, then: first }
`);

    expect(validateAgentflowWorkflow(missing).errors).toContainEqual(expect.objectContaining({
      code: "workflow.step.target.unresolved",
      path: "steps[0].then"
    }));
    expect(validateAgentflowWorkflow(cyclic).errors).toContainEqual(expect.objectContaining({
      code: "workflow.control_flow.cycle.unbounded",
      message: 'Pipeline control flow cannot contain a cycle involving "first", "second".',
      path: "steps"
    }));
    expect(validateAgentflowWorkflow(dynamicFallthroughCycle).errors).toContainEqual(expect.objectContaining({
      code: "workflow.control_flow.cycle.unbounded",
      message: 'Pipeline control flow cannot contain a cycle involving "first", "second".',
      path: "steps"
    }));
  });

  test("pauses repeated routes in directly persisted workflows without a bound", async () => {
    const repo = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`name: unvalidated-cycle
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: first, type: command, command: echo first, then: second }
  - { id: second, type: command, command: echo second, then: first }
`);
    const store = await openAgentflowRunState({ cwd: repo });
    store.createRunWithEvent({
      id: "unvalidated-cycle",
      workflow: {
        name: workflow.name,
        version: workflow.version,
        style: workflow.style,
        maturity: workflow.maturity
      },
      context: { workflow: workflow as unknown as AgentflowRunStateValue }
    }, { type: "run.created", payload: { status: "pending" } });

    const result = await executeAgentflowCommandPipeline(store, "unvalidated-cycle", workflow);

    expect(result).toMatchObject({
      status: "paused",
      completedSteps: ["first", "second"],
      failedStep: "second",
      message: "Step second repeated route target first without a positive executable limits.max_recovery_cycles bound."
    });
    store.close();
  });

  test("rejects ambiguous step IDs before executing a directly persisted workflow", async () => {
    const repo = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`name: ambiguous-runtime-route
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: start, type: command, command: echo start, then: duplicate }
  - { id: duplicate, type: command, command: echo first }
  - id: duplicate
    type: loop
    max_iterations: 1
    body:
      - { id: nested, type: command, command: echo nested }
`);
    const store = await openAgentflowRunState({ cwd: repo });
    store.createRunWithEvent({
      id: "ambiguous-runtime-route",
      workflow: {
        name: workflow.name,
        version: workflow.version,
        style: workflow.style,
        maturity: workflow.maturity
      },
      context: { workflow: workflow as unknown as AgentflowRunStateValue }
    }, { type: "run.created", payload: { status: "pending" } });

    await expect(executeAgentflowCommandPipeline(store, "ambiguous-runtime-route", workflow))
      .rejects.toMatchObject({
        code: "AGENTFLOW_STEP_AMBIGUOUS",
        message: 'Agentflow workflow has multiple steps with ID "duplicate"; runtime routing is ambiguous.'
      });
    expect(store.getRun("ambiguous-runtime-route")?.status).toBe("pending");
    expect(store.listEvents("ambiguous-runtime-route").map((event) => event.type)).toEqual(["run.created"]);
    store.close();
  });

  test("rejects ambiguous success targets before executing a directly persisted workflow", async () => {
    const repo = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`name: ambiguous-runtime-success-target
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: start, type: command, command: echo start, then: second, goto: third }
  - { id: second, type: command, command: echo second }
  - { id: third, type: command, command: echo third }
`);
    const store = await openAgentflowRunState({ cwd: repo });
    store.createRunWithEvent({
      id: "ambiguous-runtime-success-target",
      workflow: {
        name: workflow.name,
        version: workflow.version,
        style: workflow.style,
        maturity: workflow.maturity
      },
      context: { workflow: workflow as unknown as AgentflowRunStateValue }
    }, { type: "run.created", payload: { status: "pending" } });

    await expect(executeAgentflowCommandPipeline(store, "ambiguous-runtime-success-target", workflow))
      .rejects.toMatchObject({
        code: "workflow.step.success_target.ambiguous",
        message: 'Step "start" cannot declare both then and goto success targets.',
        stepId: "start"
      });
    expect(store.getRun("ambiguous-runtime-success-target")?.status).toBe("pending");
    expect(store.listEvents("ambiguous-runtime-success-target").map((event) => event.type)).toEqual(["run.created"]);
    store.close();
  });

  test("rejects condition expressions that would require code evaluation", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: complex-condition
version: 1
style: pipeline
maturity: experimental
inputs: { ready: {}, approved: {} }
steps:
  - { id: route, type: condition, if: inputs.ready && inputs.approved, then: complete, else: fail }
`);

    expect(validateAgentflowWorkflow(workflow).errors).toContainEqual({
      code: "workflow.condition.expression.unsupported",
      message: "Condition expressions support one input or artifact reference with an optional scalar comparison.",
      path: "steps[0].if",
      stepId: "route"
    });

    for (const expression of ["inputs.ready > true", "inputs.ready < null"]) {
      const invalidOrderedComparison = parseAgentflowWorkflowOrThrow(`name: invalid-ordered-condition
version: 1
style: pipeline
maturity: experimental
inputs: { ready: {} }
steps:
  - { id: route, type: condition, if: ${expression}, then: complete, else: fail }
`);
      expect(validateAgentflowWorkflow(invalidOrderedComparison).errors).toContainEqual({
        code: "workflow.condition.expression.unsupported",
        message: "Condition expressions support one input or artifact reference with an optional scalar comparison.",
        path: "steps[0].if",
        stepId: "route"
      });
    }

    const dynamicTarget = parseAgentflowWorkflowOrThrow(`name: dynamic-condition-target
version: 1
style: pipeline
maturity: experimental
inputs: { ready: {}, next: {} }
steps:
  - { id: route, type: condition, if: ready, then: "{{ inputs.next }}", else: fail }
`);
    expect(validateAgentflowWorkflow(dynamicTarget).errors).toContainEqual({
      code: "workflow.condition.target.dynamic",
      message: "Condition routes must use a static step ID or terminal target.",
      path: "steps[0].then",
      stepId: "route"
    });

    const bareTypo = parseAgentflowWorkflowOrThrow(`name: bare-condition-typo
version: 1
style: pipeline
maturity: experimental
inputs: { ready: {} }
steps:
  - { id: route, type: condition, if: redy, then: complete, else: fail }
`);
    expect(validateAgentflowWorkflow(bareTypo).errors).toContainEqual({
      code: "workflow.input.undeclared",
      message: 'Input "redy" is referenced but not declared in workflow inputs.',
      path: "steps[0].if",
      stepId: "route"
    });

    const failurePolicy = parseAgentflowWorkflowOrThrow(`name: condition-failure-policy
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: route, type: condition, if: artifacts.missing.ready, then: complete, else: fail, on_failure: { then: pause } }
`);
    expect(validateAgentflowWorkflow(failurePolicy).errors).toContainEqual({
      code: "workflow.condition.on_failure.unsupported",
      message: "Condition steps do not support on_failure policies in this runtime phase.",
      path: "steps[0].on_failure",
      stepId: "route"
    });

    const gotoTarget = parseAgentflowWorkflowOrThrow(`name: condition-goto
version: 1
style: pipeline
maturity: experimental
inputs: { ready: {} }
steps:
  - { id: route, type: condition, if: ready, then: complete, else: fail, goto: skipped }
  - { id: skipped, type: command, command: echo skipped }
`);
    expect(validateAgentflowWorkflow(gotoTarget).errors).toContainEqual({
      code: "workflow.condition.goto.unsupported",
      message: "Condition steps route through branches or if/then/else and do not support goto.",
      path: "steps[0].goto",
      stepId: "route"
    });

    const literalText = parseAgentflowWorkflowOrThrow(`name: literal-input-text
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: route, type: condition, if: artifacts.message.value == "inputs.not_a_reference", then: complete, else: fail }
`);
    expect(validateAgentflowWorkflow(literalText)).toEqual({ valid: true, errors: [] });

    const declaredOutput = parseAgentflowWorkflowOrThrow(`name: condition-output
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: route, type: condition, if: artifacts.state.ready == true, then: complete, else: fail, outputs: [state.json] }
`);
    expect(validateAgentflowWorkflow(declaredOutput).errors).toContainEqual({
      code: "workflow.condition.outputs.unsupported",
      message: "Condition steps evaluate existing inputs and artifacts and cannot declare outputs.",
      path: "steps[0].outputs",
      stepId: "route"
    });
  });

  test("fails closed for missing compared values and malformed persisted branches", async () => {
    const cases = [
      {
        id: "missing-compared-value",
        source: `name: missing-compared-value
version: 1
style: pipeline
maturity: experimental
inputs: { status: {} }
steps:
  - { id: route, type: condition, if: status != "changes_requested", then: complete, else: fail }
`,
        message: "did not resolve to a value"
      },
      {
        id: "malformed-condition-branches",
        source: `name: malformed-condition-branches
version: 1
style: pipeline
maturity: experimental
inputs: { ready: {} }
steps:
  - id: route
    type: condition
    branches:
      - true
      - { if: ready, then: complete }
`,
        inputs: { ready: true },
        message: "Condition branches must be a list of mappings"
      },
      {
        id: "malformed-condition-else",
        source: `name: malformed-condition-else
version: 1
style: pipeline
maturity: experimental
inputs: { ready: {} }
steps:
  - { id: route, type: condition, branches: [{ if: ready, then: complete }], else: 42 }
`,
        inputs: { ready: false },
        message: "Condition else must be a non-empty string"
      },
      {
        id: "mixed-condition-form",
        source: `name: mixed-condition-form
version: 1
style: pipeline
maturity: experimental
inputs: { ready: {} }
steps:
  - id: route
    type: condition
    if: ready
    then: fail
    branches:
      - { if: ready, then: complete }
    else: fail
`,
        inputs: { ready: true },
        message: "Condition steps must use either branches with an optional else target or top-level if/then fields, not both"
      },
      {
        id: "missing-inline-condition-then",
        source: `name: missing-inline-condition-then
version: 1
style: pipeline
maturity: experimental
inputs: { ready: {} }
steps:
  - { id: route, type: condition, if: ready, else: fail }
  - { id: next, type: command, command: echo next }
`,
        inputs: { ready: true },
        message: "Condition then must be a non-empty string"
      }
    ] as const;

    for (const entry of cases) {
      const root = temporaryRepo();
      const workflow = parseAgentflowWorkflowOrThrow(entry.source);
      const store = await openAgentflowRunState({ cwd: root });
      store.createRunWithEvent({
        id: entry.id,
        workflow: {
          name: workflow.name,
          version: workflow.version,
          style: workflow.style,
          maturity: workflow.maturity
        },
        context: { workflow: workflow as unknown as AgentflowRunStateValue },
        ...(entry.inputs === undefined ? {} : { inputs: entry.inputs })
      }, { type: "run.created", payload: { status: "pending" } });

      const result = await executeAgentflowCommandPipeline(store, entry.id, workflow);

      expect(result).toMatchObject({ status: "failed", failedStep: "route" });
      expect(result.message).toContain(entry.message);
      store.close();
    }
  });

  test("enforces recovery cycle bounds before repeating successful routes", async () => {
    const repo = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`name: bounded-recovery
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_recovery_cycles: 1 }
steps:
  - { id: first, type: command, command: echo first, then: second }
  - { id: second, type: command, command: echo second, then: first }
`);
    const store = await openAgentflowRunState({ cwd: repo });
    createAgentflowLifecycleRun(store, { id: "bounded-recovery", workflow });

    const result = await executeAgentflowCommandPipeline(store, "bounded-recovery", workflow);

    expect(result).toMatchObject({
      status: "paused",
      completedSteps: ["first", "second", "first", "second"],
      failedStep: "second",
      message: "Step second exceeded limits.max_recovery_cycles 1 while routing to first."
    });
    expect(store.listEvents("bounded-recovery")
      .filter((event) => event.type === "step.started" && event.stepId === "first")
      .map((event) => event.payload.attempt)).toEqual([1, 2]);
    expect(store.listArtifacts("bounded-recovery").map((artifact) => artifact.declaredPath)).toEqual(expect.arrayContaining([
      expect.stringContaining("attempt-1/stdout.log"),
      expect.stringContaining("attempt-2/stdout.log")
    ]));
    store.close();
  });

  test("counts implicit fallthrough edges toward recovery cycle bounds", async () => {
    const repo = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`name: fallthrough-recovery
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_recovery_cycles: 1 }
steps:
  - { id: start, type: command, command: "echo start >> order.txt", then: third }
  - { id: second, type: command, command: "echo second >> order.txt" }
  - { id: third, type: command, command: "echo third >> order.txt", then: second }
`);
    const store = await openAgentflowRunState({ cwd: repo });
    createAgentflowLifecycleRun(store, { id: "fallthrough-recovery", workflow });

    const result = await executeAgentflowCommandPipeline(store, "fallthrough-recovery", workflow);

    expect(result).toMatchObject({
      status: "paused",
      completedSteps: ["start", "third", "second", "third", "second"],
      failedStep: "second",
      message: "Step second exceeded limits.max_recovery_cycles 1 while routing to third."
    });
    expect(fs.readFileSync(path.join(repo, "order.txt"), "utf8").trim().split("\n")).toEqual([
      "start", "third", "second", "third", "second"
    ]);
    store.close();
  });

  test("counts explicit continue fallthroughs toward recovery cycle bounds", async () => {
    const repo = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`name: continue-recovery
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_recovery_cycles: 1 }
steps:
  - { id: start, type: command, command: echo start, then: third }
  - { id: second, type: command, command: echo second, then: continue }
  - { id: third, type: command, command: echo third, then: second }
`);
    const store = await openAgentflowRunState({ cwd: repo });
    createAgentflowLifecycleRun(store, { id: "continue-recovery", workflow });

    const result = await executeAgentflowCommandPipeline(store, "continue-recovery", workflow);

    expect(result).toMatchObject({
      status: "paused",
      completedSteps: ["start", "third", "second", "third", "second"],
      failedStep: "second",
      message: "Step second exceeded limits.max_recovery_cycles 1 while routing to third."
    });
    store.close();
  });

  test("enforces per-step attempt limits before following recovery routes", async () => {
    const repo = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`name: step-attempt-limit
version: 1
style: recovery_pipeline
maturity: experimental
inputs: { again: {} }
limits:
  max_recovery_cycles: 3
  max_step_attempts: { work: 1 }
steps:
  - { id: work, type: command, command: echo work }
  - { id: route, type: condition, if: again, then: work, else: complete }
`);
    const store = await openAgentflowRunState({ cwd: repo });
    createAgentflowLifecycleRun(store, { id: "step-attempt-limit", workflow, inputs: { again: true } });

    const result = await executeAgentflowCommandPipeline(store, "step-attempt-limit", workflow);

    expect(result).toMatchObject({
      status: "paused",
      completedSteps: ["work", "route"],
      failedStep: "route",
      message: "Step route cannot route to work because limits.max_step_attempts allows 1 attempt(s)."
    });
    store.close();
  });

  test("enforces per-step attempt limits across immediate retries", async () => {
    const repo = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`name: retry-attempt-limit
version: 1
style: recovery_pipeline
maturity: experimental
limits:
  max_step_attempts: { work: 1 }
steps:
  - id: work
    type: command
    command: "if [ -f marker ]; then exit 0; else touch marker; exit 1; fi"
    on_failure: { retry: 1, then: pause }
`);
    const store = await openAgentflowRunState({ cwd: repo });
    createAgentflowLifecycleRun(store, { id: "retry-attempt-limit", workflow });

    const result = await executeAgentflowCommandPipeline(store, "retry-attempt-limit", workflow);

    expect(result).toMatchObject({
      status: "paused",
      completedSteps: [],
      failedStep: "work",
      message: "Step work cannot start because limits.max_step_attempts allows 1 attempt(s)."
    });
    expect(store.listEvents("retry-attempt-limit").filter((event) => event.type === "step.started").length).toBe(1);
    store.close();
  });

  test("enforces attempt limits before transform and session preflight rejection", async () => {
    for (const [kind, step] of [
      ["transform", "{ id: work, type: artifact_transform, transform: copy }"],
      ["session", "{ id: work, type: session_request, session: '', prompt: '', inputs: [], outputs: [] }"]
    ] as const) {
      const repo = temporaryRepo();
      const workflow = parseAgentflowWorkflowOrThrow(`name: bounded-${kind}-preflight
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_step_attempts: { work: 0.5 } }
steps:
  - ${step}
`);
      const store = await openAgentflowRunState({ cwd: repo });
      store.createRunWithEvent({
        id: `bounded-${kind}-preflight`,
        workflow: {
          name: workflow.name,
          version: workflow.version,
          style: workflow.style,
          maturity: workflow.maturity
        },
        context: { workflow: workflow as never }
      }, { type: "run.created", payload: { status: "pending" } });

      const result = await executeAgentflowCommandPipeline(store, `bounded-${kind}-preflight`, workflow);

      expect(result).toMatchObject({
        status: "paused",
        failedStep: "work",
        message: "Step work cannot start because limits.max_step_attempts allows 0.5 attempt(s)."
      });
      expect(store.listEvents(`bounded-${kind}-preflight`).map((event) => event.type)).toEqual([
        "run.created", "run.started", "run.paused"
      ]);
      store.close();
    }
  });

  test("allows rerouted transforms to replace outputs they already own", async () => {
    const repo = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`name: transform-revisit
version: 1
style: recovery_pipeline
maturity: experimental
limits:
  max_recovery_cycles: 1
  max_step_attempts: { update: 1 }
steps:
  - { id: seed, type: command, command: "printf one > source.txt", outputs: [source.txt] }
  - { id: render, type: artifact_transform, input: source.txt, output: rendered.txt, transform: copy }
  - { id: update, type: command, command: "printf two > source.txt", outputs: [source.txt], overwrite: true, then: render }
`);
    const store = await openAgentflowRunState({ cwd: repo });
    createAgentflowLifecycleRun(store, { id: "transform-revisit", workflow });
    const transforms = new AgentflowArtifactTransformRegistry().register("copy", (input) => ({
      content: input,
      contentType: "text/plain"
    }));

    const result = await executeAgentflowCommandPipeline(store, "transform-revisit", workflow, transforms);

    expect(result).toMatchObject({ status: "paused", completedSteps: ["seed", "render", "update", "render"] });
    expect(store.readArtifact("transform-revisit", "rendered.txt").content.toString()).toBe("two");
    store.close();
  });

  test("does not grant implicit overwrite authority across routed command revisits", async () => {
    const repo = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`name: revisit-output-owner
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_recovery_cycles: 1 }
steps:
  - { id: first, type: command, command: "printf first > shared.txt", outputs: [shared.txt] }
  - { id: second, type: command, command: "printf second > shared.txt", outputs: [shared.txt], overwrite: true, then: first }
`);
    const store = await openAgentflowRunState({ cwd: repo });
    createAgentflowLifecycleRun(store, { id: "revisit-output-owner", workflow });

    const result = await executeAgentflowCommandPipeline(store, "revisit-output-owner", workflow);

    expect(result).toMatchObject({
      status: "paused",
      completedSteps: ["first", "second"],
      failedStep: "first",
      message: expect.stringContaining("was already published")
    });
    expect(store.getArtifact("revisit-output-owner", "shared.txt")?.producerStepId).toBe("second");
    store.close();
  });

  test("records rerouted command preflight failures as fresh attempts", async () => {
    const repo = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`name: revisit-preflight
version: 1
style: recovery_pipeline
maturity: experimental
inputs: { again: {} }
limits: { max_recovery_cycles: 1 }
steps:
  - { id: write, type: command, command: "mkdir -p output && printf ok > output/result.txt", outputs: [output/result.txt] }
  - { id: redirect, type: command, command: "mv output original-output && ln -s /tmp output" }
  - { id: route, type: condition, if: again, then: write, else: complete }
`);
    const store = await openAgentflowRunState({ cwd: repo });
    createAgentflowLifecycleRun(store, { id: "revisit-preflight", workflow, inputs: { again: true } });

    const result = await executeAgentflowCommandPipeline(store, "revisit-preflight", workflow);

    expect(result).toMatchObject({ status: "failed", completedSteps: ["write", "redirect", "route"], failedStep: "write" });
    expect(store.listEvents("revisit-preflight").filter((event) =>
      event.type === "step.rejected" && event.stepId === "write"
    ).map((event) => event.payload.attempt)).toEqual([2]);
    store.close();
  });

  test("preserves cancellation before terminal success routing", async () => {
    const repo = temporaryRepo();
    const workflow = parseAgentflowWorkflowOrThrow(`name: cancel-before-terminal-route
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: work, type: command, command: echo work, then: complete }
`);
    const store = await openAgentflowRunState({ cwd: repo });
    createAgentflowLifecycleRun(store, { id: "cancel-before-terminal-route", workflow });
    const appendRunEvent = store.appendRunEvent.bind(store);
    let cancelled = false;
    store.appendRunEvent = (runId, event) => {
      appendRunEvent(runId, event);
      if (!cancelled && event.type === "step.completed") {
        cancelled = true;
        store.transitionRunWithEvent(runId, {
          status: "cancelled",
          allowedFrom: ["running"],
          event: { type: "run.cancelled", payload: { source: "test" } }
        });
      }
    };

    const result = await executeAgentflowCommandPipeline(store, "cancel-before-terminal-route", workflow);

    expect(result).toMatchObject({ status: "cancelled", completedSteps: ["work"] });
    expect(store.getRun("cancel-before-terminal-route")?.status).toBe("cancelled");
    store.close();
  });

  test("keeps successful terminal routers out of failure metadata", async () => {
    for (const [target, status] of [["fail", "failed"], ["pause", "paused"], ["cancel", "cancelled"]] as const) {
      const repo = temporaryRepo();
      const workflow = parseAgentflowWorkflowOrThrow(`name: terminal-${target}
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: route, type: command, command: echo route, then: ${target} }
`);
      const store = await openAgentflowRunState({ cwd: repo });
      createAgentflowLifecycleRun(store, { id: `terminal-${target}`, workflow });

      const result = await executeAgentflowCommandPipeline(store, `terminal-${target}`, workflow);

      expect(result).toMatchObject({ status, completedSteps: ["route"] });
      expect(result.failedStep).toBeUndefined();
      expect(store.getRun(`terminal-${target}`)).toMatchObject({ status, currentStepId: null, error: null });
      store.close();
    }
  });
});

function temporaryRepo(): string {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "agentflow-ordering-"));
  fs.mkdirSync(path.join(root, ".git"));
  return root;
}
