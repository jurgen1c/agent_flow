import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import { createHash } from "node:crypto";
import os from "node:os";
import path from "node:path";
import {
  createAgentFlowLifecycleRun,
  createAgentFlowSessionProviderRegistry,
  executeAgentFlowCommandPipeline,
  openAgentFlowRunState,
  parseAgentFlowWorkflowOrThrow,
  simulateAgentFlowWorkflow,
  validateAgentFlowWorkflow
} from "../../src/runtime";

describe("Agent Flow recovery limits", () => {
  test("fails at the configured duration before starting more automation", async () => {
    const root = temporaryRepo();
    let now = "2026-08-01T12:00:00.000Z";
    const workflow = parseAgentFlowWorkflowOrThrow(`name: duration-limit
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_duration_minutes: 1 }
policies: { recovery_limits: fail }
steps:
  - { id: first, type: command, command: "printf first > first.txt", outputs: [first.txt] }
  - { id: second, type: command, command: "touch should-not-run" }
`);
    const store = await openAgentFlowRunState({ cwd: root, now: () => now });
    createAgentFlowLifecycleRun(store, { id: "duration-limit", workflow });
    const appendRunEvent = store.appendRunEvent.bind(store);
    store.appendRunEvent = (runId, event) => {
      appendRunEvent(runId, event);
      if (event.type === "step.completed" && event.stepId === "first") {
        now = "2026-08-01T12:01:01.000Z";
      }
    };

    const result = await executeAgentFlowCommandPipeline(store, "duration-limit", workflow);

    expect(result).toMatchObject({
      status: "failed",
      completedSteps: ["first"],
      failedStep: "second",
      message: "Recovery duration exceeded limits.max_duration_minutes 1."
    });
    expect(fs.existsSync(path.join(root, "should-not-run"))).toBe(false);
    expect(store.listEvents("duration-limit")).toContainEqual(expect.objectContaining({
      type: "recovery.limit_reached",
      stepId: "second",
      payload: expect.objectContaining({ limit: "max_duration_minutes", outcome: "fail" })
    }));
    expect(store.listFailures("duration-limit")).toContainEqual(expect.objectContaining({
      stepId: "second",
      classification: "recovery_duration_limit",
      outcome: "fail"
    }));
    store.close();
  });

  test("pauses on a high-risk short circuit before further automation", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: risk-short-circuit
version: 1
style: recovery_pipeline
maturity: experimental
short_circuit_if:
  - "risk.high == true"
steps:
  - id: assess
    type: command
    command: |
      printf '{"high":true}' > risk.json
    outputs: [risk.json]
  - { id: automate, type: command, command: "touch should-not-run" }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "risk-short-circuit", workflow });

    const result = await executeAgentFlowCommandPipeline(store, "risk-short-circuit", workflow);

    expect(result).toMatchObject({
      status: "paused",
      completedSteps: ["assess"],
      failedStep: "automate",
      message: 'Recovery short circuit matched "risk.high == true".'
    });
    expect(fs.existsSync(path.join(root, "should-not-run"))).toBe(false);
    expect(store.listEvents("risk-short-circuit")).toContainEqual(expect.objectContaining({
      type: "recovery.short_circuited",
      stepId: "automate",
      payload: expect.objectContaining({ expression: "risk.high == true", outcome: "pause" })
    }));
    expect(store.listFailures("risk-short-circuit")).toContainEqual(expect.objectContaining({
      classification: "recovery_short_circuit",
      outcome: "pause"
    }));
    store.close();
  });

  test("does not invoke frontier remediation after its model budget is exhausted", async () => {
    const root = temporaryRepo();
    fs.writeFileSync(path.join(root, "work.md"), "work\n");
    fs.writeFileSync(path.join(root, "fix.md"), "fix\n");
    const workflow = parseAgentFlowWorkflowOrThrow(`name: frontier-recovery-budget
version: 1
style: recovery_pipeline
maturity: experimental
limits:
  max_frontier_calls: 1
  max_recovery_cycles: 1
policies: { recovery_limits: fail }
sessions:
  fixer: { provider: frontier }
steps:
  - { id: seed, type: command, command: "printf work > work.md", outputs: [work.md] }
  - { id: consume, type: session_request, session: fixer, prompt: work.md, inputs: [work.md], outputs: [result.md] }
  - id: check
    type: command
    command: exit 1
    on_failure:
      route_to: { session: fixer, prompt: fix.md }
      on_remediated: { return_to: check }
      on_unresolved: { then: pause }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "frontier-recovery-budget", workflow });
    let calls = 0;
    const providers = createAgentFlowSessionProviderRegistry().register("frontier", () => {
      calls += 1;
      return { outputs: { "result.md": "done\n" }, metadata: { recovery_status: "remediated" } };
    });

    const result = await executeAgentFlowCommandPipeline(
      store, "frontier-recovery-budget", workflow, undefined, providers
    );

    expect(result).toMatchObject({ status: "failed", failedStep: "check" });
    expect(result.message).toContain('Budget "frontier_calls" would exceed its limit of 1');
    expect(calls).toBe(1);
    expect(store.getBudget("frontier-recovery-budget", "model:frontier_calls")).toMatchObject({ used: 1, limit: 1 });
    expect(store.listEvents("frontier-recovery-budget")).toContainEqual(expect.objectContaining({
      type: "recovery.limit_reached",
      stepId: "check",
      payload: expect.objectContaining({ limit: "max_frontier_calls", outcome: "fail" })
    }));
    expect(store.listFailures("frontier-recovery-budget")).toContainEqual(expect.objectContaining({
      classification: "recovery_model_limit",
      payload: expect.objectContaining({ limit: "max_frontier_calls", outcome: "fail" })
    }));
    expect(store.getSession("frontier-recovery-budget", "fixer")).toMatchObject({
      stepId: "check:recovery",
      status: "failed"
    });
    store.close();
  });

  test("marks ordinary sessions failed when fail-policy model budgets reject them", async () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: ordinary-model-budget-fail
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_model_calls: 0.5 }
policies: { recovery_limits: fail }
sessions: { worker: { provider: local } }
steps:
  - { id: seed, type: command, command: "printf input > input.md", outputs: [input.md] }
  - { id: request, type: session_request, session: worker, prompt: request.md, inputs: [input.md], outputs: [result.json] }
`);
    const root = temporaryRepo();
    fs.writeFileSync(path.join(root, "request.md"), "request\n");
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "ordinary-model-budget-fail", workflow });
    let calls = 0;
    const providers = createAgentFlowSessionProviderRegistry().register("local", () => {
      calls += 1;
      return { outputs: { "result.json": "{}" } };
    });

    expect(await executeAgentFlowCommandPipeline(
      store, "ordinary-model-budget-fail", workflow, undefined, providers
    )).toMatchObject({ status: "failed", failedStep: "request" });
    expect(calls).toBe(0);
    expect(store.getSession("ordinary-model-budget-fail", "worker")).toMatchObject({ status: "failed" });
    store.close();
  });

  test("pauses safely when a published risk artifact cannot be evaluated", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: malformed-risk
version: 1
style: recovery_pipeline
maturity: experimental
short_circuit_if: ["risk.high == true"]
steps:
  - { id: assess, type: command, command: "printf invalid > risk.json", outputs: [risk.json] }
  - { id: automate, type: command, command: "touch should-not-run" }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "malformed-risk", workflow });

    const result = await executeAgentFlowCommandPipeline(store, "malformed-risk", workflow);

    expect(result).toMatchObject({ status: "paused", failedStep: "automate" });
    expect(result.message).toContain("could not be evaluated");
    expect(fs.existsSync(path.join(root, "should-not-run"))).toBe(false);
    expect(store.listEvents("malformed-risk")).toContainEqual(expect.objectContaining({
      type: "recovery.short_circuit_failed",
      stepId: "automate"
    }));
    expect(store.listFailures("malformed-risk")).toContainEqual(expect.objectContaining({
      classification: "recovery_short_circuit_evaluation",
      outcome: "pause"
    }));
    store.close();
  });

  test("pauses safely when a published short-circuit artifact backing disappears", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: missing-risk-backing
version: 1
style: recovery_pipeline
maturity: experimental
short_circuit_if: ["risk.high == true"]
steps:
  - { id: assess, type: command, command: 'printf "{\\"high\\":true}" > risk.json', outputs: [risk.json] }
  - { id: automate, type: command, command: "touch should-not-run" }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "missing-risk-backing", workflow });
    const appendRunEvent = store.appendRunEvent.bind(store);
    store.appendRunEvent = (runId, event) => {
      appendRunEvent(runId, event);
      if (event.type === "step.completed" && event.stepId === "assess") {
        fs.unlinkSync(artifactStoragePath(root, runId, "risk.json"));
      }
    };

    const result = await executeAgentFlowCommandPipeline(store, "missing-risk-backing", workflow);

    expect(result).toMatchObject({ status: "paused", failedStep: "automate" });
    expect(result.message).toContain("could not be evaluated");
    expect(store.listFailures("missing-risk-backing")).toContainEqual(expect.objectContaining({
      classification: "recovery_short_circuit_evaluation",
      outcome: "pause"
    }));
    store.close();
  });

  test("rechecks failure short circuits before invoking routed remediation", async () => {
    const root = temporaryRepo();
    fs.writeFileSync(path.join(root, "fix.md"), "fix\n");
    const workflow = parseAgentFlowWorkflowOrThrow(`name: pre-remediation-guard
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_frontier_calls: 1, max_recovery_cycles: 1 }
short_circuit_if: ["failures.check.attempts >= 1"]
sessions: { fixer: { provider: frontier } }
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
    createAgentFlowLifecycleRun(store, { id: "pre-remediation-guard", workflow });
    let calls = 0;
    const providers = createAgentFlowSessionProviderRegistry().register("frontier", () => {
      calls += 1;
      return { outputs: {}, metadata: { recovery_status: "remediated" } };
    });

    const result = await executeAgentFlowCommandPipeline(
      store, "pre-remediation-guard", workflow, undefined, providers
    );

    expect(result).toMatchObject({
      status: "paused",
      failedStep: "check",
      message: 'Recovery short circuit matched "failures.check.attempts >= 1".'
    });
    expect(calls).toBe(0);
    store.close();
  });

  test("applies fail policy and records attempt-limit decisions", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: attempt-limit-policy
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_step_attempts: { check: 1 } }
policies: { recovery_limits: fail }
steps:
  - { id: check, type: command, command: exit 1, on_failure: { retry: 1, then: pause } }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "attempt-limit-policy", workflow });

    const result = await executeAgentFlowCommandPipeline(store, "attempt-limit-policy", workflow);

    expect(result).toMatchObject({ status: "failed", failedStep: "check" });
    expect(store.listEvents("attempt-limit-policy")).toContainEqual(expect.objectContaining({
      type: "recovery.limit_reached",
      stepId: "check",
      payload: expect.objectContaining({ limit: "max_step_attempts", outcome: "fail" })
    }));
    expect(store.listFailures("attempt-limit-policy")).toContainEqual(expect.objectContaining({
      classification: "step_attempt_limit",
      outcome: "fail"
    }));
    store.close();
  });

  test("simulates high-risk and exhausted-budget short circuits before the next step", () => {
    const riskWorkflow = parseAgentFlowWorkflowOrThrow(`name: simulated-risk
version: 1
style: recovery_pipeline
maturity: experimental
short_circuit_if: ["risk.high == true"]
steps:
  - { id: assess, type: command, command: assess, outputs: [risk.json] }
  - { id: automate, type: command, command: automate }
`);

    expect(simulateAgentFlowWorkflow(riskWorkflow, {
      steps: { assess: { outputs: { "risk.json": { high: true } } } }
    })).toMatchObject({
      status: "paused",
      visitedSteps: [{ id: "assess", type: "command", outcome: "succeeded" }],
      terminalStates: [{ stepId: "automate", status: "paused" }]
    });

    const budgetWorkflow = parseAgentFlowWorkflowOrThrow(`name: simulated-budget
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_frontier_calls: 1 }
short_circuit_if: ["budget.frontier_calls_remaining == 0"]
sessions: { fixer: { provider: frontier } }
steps:
  - { id: fix, type: session_request, session: fixer, prompt: fix.md, inputs: [input.md], outputs: [result.md] }
  - { id: automate, type: command, command: automate }
`);
    expect(simulateAgentFlowWorkflow(budgetWorkflow, {
      artifacts: { "input.md": "input" },
      steps: { fix: { outputs: { "result.md": "done" } } }
    })).toMatchObject({ status: "paused", terminalStates: [{ stepId: "automate", status: "paused" }] });

    const routedBudgetWorkflow = parseAgentFlowWorkflowOrThrow(`name: simulated-routed-budget
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_frontier_calls: 1, max_recovery_cycles: 1 }
sessions: { fixer: { provider: frontier } }
steps:
  - { id: use-budget, type: session_request, session: fixer, prompt: fix.md, outputs: [used.md] }
  - id: check
    type: command
    command: check
    on_failure:
      route_to: { session: fixer, prompt: fix.md }
      on_remediated: { return_to: check }
      on_unresolved: { then: pause }
`);
    expect(simulateAgentFlowWorkflow(routedBudgetWorkflow, {
      steps: {
        "use-budget": { outputs: { "used.md": "used" } },
        check: { outcome: "failed", recovery: "remediated" }
      }
    })).toMatchObject({
      status: "paused",
      terminalStates: [{ stepId: "check", status: "paused" }]
    });
  });

  test("resolves failure-attempt short circuits for dotted step IDs", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: dotted-failure-reference
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_recovery_cycles: 1 }
short_circuit_if: ["failures.ci.check.attempts >= 1"]
steps:
  - { id: ci.check, type: command, command: exit 1, on_failure: { then: continue, allowed: true } }
  - { id: automate, type: command, command: automate }
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "dotted-failure-reference", workflow });

    const result = await executeAgentFlowCommandPipeline(store, "dotted-failure-reference", workflow);

    expect(result).toMatchObject({
      status: "paused",
      completedSteps: [],
      failedStep: "automate",
      message: 'Recovery short circuit matched "failures.ci.check.attempts >= 1".'
    });
    store.close();
  });

  test("leaves absent failure counters nonmatching in runtime and simulation", async () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: absent-failure-reference
version: 1
style: recovery_pipeline
maturity: experimental
short_circuit_if: ["failures.never.attempts == 0"]
steps:
  - { id: check, type: command, command: "true" }
`);
    expect(simulateAgentFlowWorkflow(workflow, {})).toMatchObject({ status: "completed" });

    const root = temporaryRepo();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "absent-failure-reference", workflow });
    expect(await executeAgentFlowCommandPipeline(store, "absent-failure-reference", workflow))
      .toMatchObject({ status: "completed", completedSteps: ["check"] });
    store.close();
  });

  test("does not charge simulated model budgets when request preflight fails", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: simulated-preflight-budget
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_model_calls: 1 }
sessions: { worker: { provider: local } }
steps:
  - id: invalid
    type: session_request
    session: worker
    prompt: invalid.md
    inputs: [missing.md]
    outputs: [invalid.json]
    on_failure: { then: valid, allowed: true }
  - { id: valid, type: session_request, session: worker, prompt: valid.md, outputs: [valid.json] }
`);

    const result = simulateAgentFlowWorkflow(workflow, {
      steps: {
        invalid: { outputs: { "invalid.json": {} } },
        valid: { outputs: { "valid.json": {} } }
      }
    });

    expect(result.status).toBe("completed");
    expect(result.visitedSteps.map(({ id }) => id)).toEqual(["invalid", "valid"]);
  });

  test("records simulated request preflight failures for later short circuits", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: simulated-preflight-failure-count
version: 1
style: recovery_pipeline
maturity: experimental
short_circuit_if: ["failures.invalid.attempts >= 1"]
sessions: { worker: { provider: local } }
steps:
  - id: invalid
    type: session_request
    session: worker
    prompt: invalid.md
    inputs: [missing.md]
    outputs: [invalid.json]
    on_failure: { then: next, allowed: true }
  - { id: next, type: command, command: next }
`);

    expect(simulateAgentFlowWorkflow(workflow, {})).toMatchObject({
      status: "paused",
      terminalStates: [{ stepId: "next", status: "paused" }]
    });
  });

  test("charges failed simulated provider calls before routed remediation", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: simulated-failed-provider-budget
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_model_calls: 1 }
sessions: { worker: { provider: local } }
steps:
  - id: request
    type: session_request
    session: worker
    prompt: request.md
    outputs: [result.json]
    on_failure:
      route_to: { session: worker, prompt: recover.md }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);

    expect(simulateAgentFlowWorkflow(workflow, {
      steps: { request: { outcome: "failed", recovery: "remediated" } }
    })).toMatchObject({
      status: "paused",
      terminalStates: [{ stepId: "request", status: "paused" }]
    });
  });

  test("charges model budgets for direct parallel session branches", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: simulated-parallel-session-budget
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_frontier_calls: 1 }
short_circuit_if: ["budget.frontier_calls_remaining == 0"]
sessions: { worker: { provider: frontier } }
steps:
  - id: parallel
    type: parallel
    strategy: fail_fast
    branches:
      - { id: worker-branch, session: worker, outputs: [result.json] }
  - { id: automate, type: command, command: automate }
`);

    expect(simulateAgentFlowWorkflow(workflow, {
      steps: { "worker-branch": { outputs: { "result.json": {} } } }
    })).toMatchObject({
      status: "paused",
      terminalStates: [{ stepId: "automate", status: "paused" }]
    });
  });

  test("rechecks loop-level short circuits between simulated iterations", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: simulated-loop-short-circuit
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - id: repeat
    type: loop
    max_iterations: 2
    short_circuit_if: ["risk.high == true"]
    body:
      - { id: assess, type: command, command: assess, outputs: [risk.json], overwrite: true }
`);

    expect(simulateAgentFlowWorkflow(workflow, {
      steps: {
        repeat: { iterations: 2 },
        assess: { outputs: { "risk.json": { high: true } } }
      }
    })).toMatchObject({
      status: "paused",
      visitedSteps: [
        { id: "repeat", type: "loop", outcome: "succeeded" },
        { id: "assess", type: "command", outcome: "succeeded" }
      ],
      terminalStates: [{ stepId: "repeat", status: "paused" }]
    });
  });

  test("applies fail policy to simulated recovery-cycle and step-attempt limits", () => {
    const cycleWorkflow = parseAgentFlowWorkflowOrThrow(`name: simulated-cycle-fail
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_recovery_cycles: 1 }
policies: { recovery_limits: fail }
steps:
  - { id: cycle, type: condition, if: again, then: cycle, else: complete }
`);
    expect(simulateAgentFlowWorkflow(cycleWorkflow, { inputs: { again: true } })).toMatchObject({
      status: "failed",
      terminalStates: [{ stepId: "cycle", status: "failed" }]
    });

    const attemptWorkflow = parseAgentFlowWorkflowOrThrow(`name: simulated-attempt-fail
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_recovery_cycles: 2, max_step_attempts: { work: 1 } }
policies: { recovery_limits: fail }
steps:
  - { id: work, type: command, command: work, then: route }
  - { id: route, type: condition, if: again, then: work, else: complete }
`);
    expect(simulateAgentFlowWorkflow(attemptWorkflow, { inputs: { again: true } })).toMatchObject({
      status: "failed",
      terminalStates: [{ stepId: "work", status: "failed" }]
    });
  });

  test("ignores recovery fail policy outside recovery workflows", async () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: pipeline-attempt-policy
version: 1
style: pipeline
maturity: experimental
limits: { max_step_attempts: { work: 0.5 } }
policies: { recovery_limits: fail }
steps:
  - { id: work, type: command, command: "true" }
`);

    expect(simulateAgentFlowWorkflow(workflow, {})).toMatchObject({
      status: "paused",
      terminalStates: [{ stepId: "work", status: "paused" }]
    });

    const store = await openAgentFlowRunState({ cwd: temporaryRepo() });
    createAgentFlowLifecycleRun(store, { id: "pipeline-attempt-policy", workflow });
    expect(await executeAgentFlowCommandPipeline(store, "pipeline-attempt-policy", workflow)).toMatchObject({
      status: "paused",
      failedStep: "work"
    });
    store.close();
  });

  test("evaluates failure short circuits only after immediate retries are exhausted", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: simulated-immediate-retry-short-circuit
version: 1
style: recovery_pipeline
maturity: experimental
short_circuit_if: ["failures.work.attempts >= 1"]
steps:
  - { id: work, type: command, command: work, on_failure: { retry: 1 } }
`);

    const result = simulateAgentFlowWorkflow(workflow, {
      steps: { work: { outcome: ["failed", "succeeded"] } }
    });

    expect(result.status).toBe("completed");
    expect(result.visitedSteps.map(({ outcome }) => outcome)).toEqual(["failed", "succeeded"]);
  });

  test("rechecks simulated failure short circuits before routed remediation", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: simulated-routed-short-circuit
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_frontier_calls: 1 }
short_circuit_if: ["failures.check.attempts >= 1"]
sessions: { fixer: { provider: frontier } }
steps:
  - id: check
    type: command
    command: check
    on_failure:
      route_to: { session: fixer, prompt: fix.md }
      on_remediated: { then: complete }
      on_unresolved: { then: pause }
`);

    const result = simulateAgentFlowWorkflow(workflow, {
      steps: { check: { outcome: "failed", recovery: "remediated" } }
    });

    expect(result).toMatchObject({
      status: "paused",
      visitedSteps: [{ id: "check", type: "command", outcome: "failed" }],
      terminalStates: [{ stepId: "check", status: "paused" }]
    });
  });

  test("pauses simulation when a published short-circuit artifact has no fixture value", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: simulated-missing-artifact-value
version: 1
style: recovery_pipeline
maturity: experimental
short_circuit_if: ["risk.high == true"]
steps:
  - { id: assess, type: command, command: assess, outputs: [risk.json] }
  - { id: automate, type: command, command: automate }
`);

    const result = simulateAgentFlowWorkflow(workflow, {
      steps: { assess: { outputs: ["risk.json"] } }
    });

    expect(result).toMatchObject({
      status: "paused",
      visitedSteps: [{ id: "assess", type: "command", outcome: "succeeded" }],
      terminalStates: [{ stepId: "automate", status: "paused" }]
    });
  });

  test("uses the longest matching published artifact alias in simulation", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: simulated-longest-artifact-alias
version: 1
style: recovery_pipeline
maturity: experimental
short_circuit_if: ["risk.high == true"]
steps:
  - { id: detailed, type: command, command: detailed, outputs: [risk/high.json] }
  - { id: general, type: command, command: general, outputs: [risk.json] }
  - { id: automate, type: command, command: automate }
`);

    const result = simulateAgentFlowWorkflow(workflow, {
      steps: {
        detailed: { outputs: { "risk/high.json": false } },
        general: { outputs: ["risk.json"] }
      }
    });

    expect(result.status).toBe("completed");
    expect(result.visitedSteps.map(({ id }) => id)).toEqual(["detailed", "general", "automate"]);
  });

  test("validates duration, recovery-limit policy, and short-circuit declarations", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: invalid-recovery-limits
version: 1
style: recovery_pipeline
maturity: experimental
limits:
  max_duration_seconds: 30
  max_duration_minutes: 1
policies: { recovery_limits: allow }
short_circuit_if: risk.high
steps:
  - { id: check, type: command, command: echo check }
`);

    expect(validateAgentFlowWorkflow(workflow).errors).toEqual(expect.arrayContaining([
      expect.objectContaining({ code: "workflow.recovery.duration.ambiguous", path: "limits" }),
      expect.objectContaining({ code: "workflow.policy.recovery_limits.invalid", path: "policies.recovery_limits" }),
      expect.objectContaining({ code: "workflow.recovery.short_circuit.invalid", path: "short_circuit_if" })
    ]));
  });

  test("publishes duration-unit mutual exclusion in the workflow schema", () => {
    const schema = JSON.parse(fs.readFileSync(path.join(import.meta.dir, "../../schemas/workflow.schema.json"), "utf8")) as {
      properties: { limits: { allOf?: unknown[] } };
    };
    expect(schema.properties.limits.allOf).toContainEqual({
      not: { required: ["max_duration_seconds", "max_duration_minutes"] }
    });
  });

  test("reserves virtual budget and failure artifact namespaces", async () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: reserved-recovery-namespace
version: 1
style: recovery_pipeline
maturity: experimental
short_circuit_if: ["budget.high == true"]
steps:
  - id: publish
    type: " input_request "
    question: publish
    save_as: ./budget.json
`);

    expect(validateAgentFlowWorkflow(workflow).errors).toEqual(expect.arrayContaining([
      expect.objectContaining({
        code: "workflow.recovery.short_circuit.namespace.reserved",
        path: "steps[0].save_as"
      })
    ]));

    const store = await openAgentFlowRunState({ cwd: temporaryRepo() });
    store.createRun({
      id: "reserved-recovery-namespace",
      workflow: {
        name: workflow.name,
        version: workflow.version,
        style: workflow.style,
        maturity: workflow.maturity
      },
      context: { workflow }
    });
    await expect(executeAgentFlowCommandPipeline(store, "reserved-recovery-namespace", workflow))
      .rejects.toMatchObject({
        code: "AGENT_FLOW_WORKFLOW_INVALID",
        message: expect.stringContaining("steps[0].save_as")
      });
    store.close();
  });

  test("validates short circuits on direct parallel branch descriptors", async () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: direct-branch-short-circuit
version: 1
style: pipeline
maturity: experimental
steps:
  - id: parallel
    type: parallel
    strategy: fail_fast
    branches:
      - id: worker
        session: worker
        short_circuit_if: ["risk.high == true"]
`);

    expect(validateAgentFlowWorkflow(workflow).errors).toContainEqual(expect.objectContaining({
      code: "workflow.recovery.short_circuit.style",
      path: "steps[0].branches[0].short_circuit_if"
    }));

    const store = await openAgentFlowRunState({ cwd: temporaryRepo() });
    store.createRun({
      id: "direct-branch-short-circuit",
      workflow: {
        name: workflow.name,
        version: workflow.version,
        style: workflow.style,
        maturity: workflow.maturity
      },
      context: { workflow }
    });
    await expect(executeAgentFlowCommandPipeline(store, "direct-branch-short-circuit", workflow))
      .rejects.toMatchObject({
        code: "AGENT_FLOW_WORKFLOW_INVALID",
        message: expect.stringContaining("steps[0].branches[0].short_circuit_if")
      });
    store.close();
  });

  test("rejects undeclared input references in recovery short circuits", async () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: undeclared-short-circuit-input
version: 1
style: recovery_pipeline
maturity: experimental
short_circuit_if: ["high_risk == true"]
steps:
  - { id: automate, type: command, command: automate }
`);

    expect(validateAgentFlowWorkflow(workflow).errors).toContainEqual({
      code: "workflow.input.undeclared",
      message: 'Input "high_risk" is referenced but not declared in workflow inputs.',
      path: "short_circuit_if[0]"
    });

    const store = await openAgentFlowRunState({ cwd: temporaryRepo() });
    store.createRun({
      id: "undeclared-short-circuit-input",
      workflow: {
        name: workflow.name,
        version: workflow.version,
        style: workflow.style,
        maturity: workflow.maturity
      },
      context: { workflow }
    });
    await expect(executeAgentFlowCommandPipeline(store, "undeclared-short-circuit-input", workflow))
      .rejects.toMatchObject({ code: "AGENT_FLOW_WORKFLOW_INVALID" });
    store.close();
  });

  test("rejects malformed directly persisted limit declarations before starting", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: direct-invalid-limit
version: 1
style: recovery_pipeline
maturity: experimental
short_circuit_if: ["risk.high == true"]
steps:
  - { id: check, type: command, command: "touch should-not-run" }
`);
    const invalid = { ...workflow, short_circuit_if: "risk.high == true" };
    const store = await openAgentFlowRunState({ cwd: root });
    store.createRun({
      id: "direct-invalid-limit",
      workflow: {
        name: invalid.name,
        version: invalid.version,
        style: invalid.style,
        maturity: invalid.maturity
      },
      context: { workflow: invalid as never }
    });

    await expect(executeAgentFlowCommandPipeline(store, "direct-invalid-limit", invalid))
      .rejects.toMatchObject({ code: "AGENT_FLOW_WORKFLOW_INVALID" });
    expect(store.getRun("direct-invalid-limit")?.status).toBe("pending");
    expect(fs.existsSync(path.join(root, "should-not-run"))).toBe(false);
    store.close();
  });

  test("rejects recovery short circuits on non-recovery workflow styles", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: pipeline-short-circuit
version: 1
style: pipeline
maturity: experimental
short_circuit_if: ["risk.high == true"]
steps:
  - { id: check, type: command, command: echo check }
`);

    expect(validateAgentFlowWorkflow(workflow).errors).toContainEqual({
      code: "workflow.recovery.short_circuit.style",
      message: "Recovery short_circuit_if is only supported by recovery_pipeline workflows.",
      path: "short_circuit_if"
    });
  });
});

function temporaryRepo(): string {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "agent-flow-recovery-limits-"));
  fs.mkdirSync(path.join(root, ".git"));
  return root;
}

function artifactStoragePath(root: string, runId: string, declaredPath: string): string {
  const runDirectory = `r-${createHash("sha256").update(runId).digest("hex")}`;
  const artifact = `a-${createHash("sha256").update(declaredPath).digest("hex")}`;
  return path.join(root, ".agent-flow", "runs", runDirectory, "artifacts", artifact);
}
