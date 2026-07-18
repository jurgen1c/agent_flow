import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import path from "node:path";
import {
  AgentflowArtifactTransformRegistry,
  parseAgentflowWorkflowOrThrow,
  renderAgentflowSimulationSummary,
  parseAgentflowSimulationFixture,
  simulateAgentflowWorkflow
} from "../../packages/agentflow-core/src";

const repoRoot = path.resolve(".");
const examples = path.join(repoRoot, "agentflow-examples/agentflow-examples/workflows");

function loadExample(name: string) {
  return parseAgentflowWorkflowOrThrow(fs.readFileSync(path.join(examples, name), "utf8"));
}

describe("Agentflow workflow simulation", () => {
  test("simulates a pipeline from fixture-provided step outcomes and outputs without mutation", () => {
    const workflow = loadExample("simple-ci.yml");
    const before = JSON.stringify(workflow);
    const first = simulateAgentflowWorkflow(workflow, {
      steps: {
        install: { outcome: "succeeded" },
        lint: { outputs: ["ci/rubocop.log"] },
        test: { outputs: ["ci/test.log"] }
      }
    });
    const second = simulateAgentflowWorkflow(workflow, {
      steps: {
        install: { outcome: "succeeded" },
        lint: { outputs: ["ci/rubocop.log"] },
        test: { outputs: ["ci/test.log"] }
      }
    });

    expect(first).toEqual(second);
    expect(first.status).toBe("completed");
    expect(first.visitedSteps.map((step) => step.id)).toEqual(["install", "lint", "test"]);
    expect(first.availableArtifacts).toEqual(["ci/rubocop.log", "ci/test.log"]);
    expect(first.missingArtifacts).toEqual([]);
    expect(JSON.stringify(workflow)).toBe(before);
  });

  test("simulates recovery routing from a fixture-selected condition", () => {
    const result = simulateAgentflowWorkflow(loadExample("ci-triage.yml"), {
      artifacts: {
        "failures/failure.json": { kind: "implementation_error" }
      },
      inputs: {
        failure_payload: "failures/failure.json",
        failed_step: "local_ci"
      },
      steps: {
        classify: { outputs: ["ci/failure-classification.json"] },
        route: { condition: "return_remediated" }
      }
    });

    expect(result.status).toBe("completed");
    expect(result.visitedSteps.map((step) => step.id)).toEqual(["classify", "route", "return_remediated"]);
    expect(result.terminalStates).toEqual([{ stepId: "return_remediated", status: "remediated" }]);
  });

  test("simulates collaboration routing and artifact contracts", () => {
    const result = simulateAgentflowWorkflow(loadExample("implement-review-collab.yml"), {
      artifacts: {
        "git.diff": "fixture diff",
        "spec.md": "fixture spec"
      },
      steps: {
        implement: { outputs: ["implementation-summary.md"] },
        review: { outputs: ["reviews/code-review.json"] },
        route_review: { condition: "record_approval" },
        ask_user: { input: "continue" }
      }
    });

    expect(result.status).toBe("completed");
    expect(result.visitedSteps.map((step) => step.id)).toEqual([
      "implement",
      "review",
      "route_review",
      "record_approval",
      "ask_user"
    ]);
    expect(result.missingArtifacts).toEqual([]);
  });

  test("reports unresolved conditions and missing artifacts deterministically", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: unresolved
version: 1
style: pipeline
maturity: draft
steps:
  - id: inspect
    type: session_request
    session: worker
    prompt: prompts/inspect.md
    inputs: [missing.json]
    outputs: [result.json]
  - id: route
    type: condition
    if: artifacts.result.ready == true
    then: finish
    else: pause
  - id: finish
    type: result
    status: completed
sessions:
  worker: { provider: local }
`);
    const result = simulateAgentflowWorkflow(workflow, { steps: { inspect: {} } });
    const summary = renderAgentflowSimulationSummary(result);

    expect(result.status).toBe("paused");
    expect(result.missingArtifacts).toEqual([
      { stepId: "inspect", artifact: "missing.json", kind: "input" }
    ]);
    expect(result.unresolvedBranches).toEqual([]);
    expect(result.visitedSteps).toEqual([{ id: "inspect", type: "session_request", outcome: "failed" }]);
    expect(summary).toContain("Status: paused");
    expect(summary).toContain("inspect: missing input artifact missing.json");
  });

  test("requires declared workflow inputs and permits fixture-selected condition fallthrough", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: inputs
version: 1
style: pipeline
maturity: draft
inputs:
  required_value: { required: true }
  optional_value: { required: false }
steps:
  - id: route
    type: condition
    if: inputs.required_value == true
    then: finish
  - id: finish
    type: result
    status: completed
`);

    const missing = simulateAgentflowWorkflow(workflow, { steps: { route: { condition: false } } });
    expect(missing.status).toBe("unresolved");
    expect(missing.missingInputs).toEqual(["required_value"]);

    const present = simulateAgentflowWorkflow(workflow, {
      inputs: { required_value: false },
      steps: { route: { condition: false } }
    });
    expect(present.status).toBe("completed");
    expect(present.visitedSteps.map((step) => step.id)).toEqual(["route", "finish"]);
  });

  test("replays bounded retries from sequential fixture outcomes", () => {
    const workflow = loadExample("simple-ci.yml");
    const result = simulateAgentflowWorkflow(workflow, {
      steps: {
        install: { outcome: ["failed", "succeeded"] },
        lint: { outputs: ["ci/rubocop.log"] },
        test: { outputs: ["ci/test.log"] }
      }
    });

    expect(result.status).toBe("completed");
    expect(result.visitedSteps.slice(0, 2)).toEqual([
      { id: "install", type: "command", outcome: "failed" },
      { id: "install", type: "command", outcome: "succeeded" }
    ]);
  });

  test("simulates standard parallel branch descriptors", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: parallel
version: 1
style: collaborative
maturity: draft
collaboration: { enabled: true }
sessions:
  backend: { provider: local, role: backend, authority: { can_modify_files: false } }
  docs: { provider: local, role: docs, authority: { can_modify_files: false } }
steps:
  - id: split
    type: parallel
    branches:
      - { id: backend, session: backend, outputs: [backend.json] }
      - { id: docs, session: docs, inputs: [brief.md], outputs: [docs.md] }
`);
    const result = simulateAgentflowWorkflow(workflow, {
      artifacts: { "brief.md": "fixture brief" },
      steps: {
        backend: { outputs: ["backend.json"] },
        docs: { outputs: ["docs.md"] }
      }
    });

    expect(result.status).toBe("completed");
    expect(result.visitedSteps.map((step) => step.id)).toEqual(["split", "backend", "docs"]);
    expect(result.availableArtifacts).toEqual(["backend.json", "brief.md", "docs.md"]);
  });

  test("rejects malformed simulation fixture fields", () => {
    for (const source of [
      { steps: { run: { outcome: "bogus" } } },
      { steps: { run: { outputs: "artifact.txt" } } },
      { steps: { run: { condition: [] } } },
      { steps: { run: { choice: ["approve", 2] } } },
      { steps: { run: { iterations: -1 } } },
      { steps: { run: { recovery: "unknown" } } },
      { inputs: { "": "value" } },
      { artifacts: { " ": "value" } },
      { steps: { "": { outcome: "succeeded" } } },
      { steps: { run: { outputs: { "": "value" } } } }
    ]) {
      expect(parseAgentflowSimulationFixture(JSON.stringify(source)).ok).toBe(false);
    }
  });

  test("continues after valid continue and ignore failure targets", () => {
    for (const target of ["continue", "ignore"]) {
      const workflow = parseAgentflowWorkflowOrThrow(`name: failure-${target}
version: 1
style: pipeline
maturity: draft
steps:
  - id: optional
    type: command
    command: echo optional
    on_failure: { then: ${target} }
  - id: finish
    type: result
    status: completed
`);
      const result = simulateAgentflowWorkflow(workflow, {
        steps: { optional: { outcome: "failed" } }
      });

      expect(result.status).toBe("completed");
      expect(result.visitedSteps.map((step) => step.id)).toEqual(["optional", "finish"]);
    }
  });

  test("reports unknown fixture step IDs and undeclared outputs", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: fixture-contract
version: 1
style: pipeline
maturity: draft
steps:
  - { id: produce, type: command, command: echo result, outputs: [declared.json] }
  - { id: consume, type: command, command: cat declared.json, inputs: [declared.json] }
`);
    const result = simulateAgentflowWorkflow(workflow, {
      steps: {
        misspelled: { outcome: "failed" },
        produce: { outputs: ["undeclared.json"] }
      }
    });

    expect(result.status).toBe("unresolved");
    expect(result.unresolvedBranches).toEqual([
      { stepId: "misspelled", reason: "Fixture references an unknown workflow step ID." },
      { stepId: "produce", reason: "Fixture provides undeclared output artifact undeclared.json." }
    ]);
    expect(result.availableArtifacts).not.toContain("undeclared.json");
    expect(result.missingArtifacts).toContainEqual({ stepId: "produce", artifact: "declared.json", kind: "output" });
  });

  test("uses fixture loop counts without executing loop commands", () => {
    const result = simulateAgentflowWorkflow(loadExample("pr-feedback-loop.yml"), {
      inputs: { pr_url: "https://github.test/example/pull/1" },
      artifacts: { "implementation-summary.md": "fixture summary" },
      steps: {
        wait_for_review: { iterations: 1 },
        collect_pr_state: { outputs: ["github/pr-state.json"] },
        classify_comments: { outputs: ["github/actionable-comments.json"] },
        route_comments: { condition: "continue_loop" }
      }
    });

    expect(result.status).toBe("completed");
    expect(result.visitedSteps.map((step) => step.id)).toEqual([
      "wait_for_review",
      "collect_pr_state",
      "classify_comments",
      "route_comments",
      "continue_loop",
      "return_complete"
    ]);
    expect(result.terminalStates).toEqual([
      { stepId: "continue_loop", status: "continue" },
      { stepId: "return_complete", status: "completed" }
    ]);
  });

  test("stops enclosing loops at the deterministic transition limit", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: bounded-simulation
version: 1
style: recovery_pipeline
maturity: draft
steps:
  - id: repeat
    type: loop
    max_iterations: 10001
    body:
      - { id: inspect, type: command, command: echo inspect }
`);
    const result = simulateAgentflowWorkflow(workflow, {
      steps: { repeat: { iterations: 10001 } }
    });

    expect(result.status).toBe("unresolved");
    expect(result.unresolvedBranches).toEqual([
      { stepId: "inspect", reason: "Simulation exceeded its deterministic transition limit." }
    ]);
    expect(result.visitedSteps).toHaveLength(10000);
  });

  test("enters globally targeted steps nested inside control-flow containers", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: nested-target
version: 1
style: pipeline
maturity: draft
steps:
  - id: route
    type: condition
    if: artifacts.ready == true
    then: nested_finish
    else: cancel
  - id: container
    type: loop
    max_iterations: 1
    body:
      - { id: nested_finish, type: result, status: completed }
`);
    const result = simulateAgentflowWorkflow(workflow, {
      steps: { route: { condition: "nested_finish" } }
    });

    expect(result.status).toBe("completed");
    expect(result.visitedSteps.map((step) => step.id)).toEqual(["route", "nested_finish"]);
  });

  test("records failed condition outcomes before taking their failure path", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: failed-condition
version: 1
style: pipeline
maturity: draft
steps:
  - id: route
    type: condition
    if: artifacts.ready == true
    then: finish
    else: cancel
  - { id: finish, type: result, status: completed }
`);
    const result = simulateAgentflowWorkflow(workflow, {
      steps: { route: { outcome: "failed" } }
    });

    expect(result.status).toBe("failed");
    expect(result.visitedSteps).toEqual([
      { id: "route", type: "condition", outcome: "failed" }
    ]);
  });

  test("requires fixture-selected routed recovery outcomes", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: recovery-route
version: 1
style: recovery_pipeline
maturity: draft
limits: { max_recovery_cycles: 2 }
steps:
  - id: work
    type: command
    command: echo work
    on_failure:
      route_to: { workflow: repair }
      on_remediated: { return_to: work }
      on_unresolved: { then: pause }
`);

    const missing = simulateAgentflowWorkflow(workflow, {
      steps: { work: { outcome: "failed" } }
    });
    expect(missing.status).toBe("unresolved");
    expect(missing.unresolvedBranches).toEqual([
      { stepId: "work", reason: "Fixture does not select a routed recovery outcome." }
    ]);

    const unresolved = simulateAgentflowWorkflow(workflow, {
      steps: { work: { outcome: "failed", recovery: "unresolved" } }
    });
    expect(unresolved.status).toBe("paused");
    expect(unresolved.terminalStates).toEqual([{ stepId: "work", status: "paused" }]);
  });

  test("preserves paused result status", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: paused-result
version: 1
style: pipeline
maturity: draft
steps:
  - { id: wait, type: result, status: paused }
`);
    const result = simulateAgentflowWorkflow(workflow, {});

    expect(result.status).toBe("paused");
    expect(result.terminalStates).toEqual([{ stepId: "wait", status: "paused" }]);
  });

  test("isolates parallel branch artifact reads and merges their outputs", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: isolated-parallel
version: 1
style: pipeline
maturity: draft
steps:
  - id: split
    type: parallel
    branches:
      - { id: produce, type: command, command: echo data, outputs: [shared.json] }
      - { id: consume, type: command, command: cat shared.json, inputs: [shared.json], outputs: [used.json] }
`);
    const result = simulateAgentflowWorkflow(workflow, {
      steps: {
        produce: { outputs: ["shared.json"] },
        consume: { outputs: ["used.json"] }
      }
    });

    expect(result.status).toBe("unresolved");
    expect(result.availableArtifacts).toEqual(["shared.json", "used.json"]);
    expect(result.missingArtifacts).toContainEqual({ stepId: "consume", artifact: "shared.json", kind: "input" });
  });

  test("reports conflicting parallel artifact values instead of choosing a branch", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: conflicting-parallel-artifacts
version: 1
style: pipeline
maturity: draft
steps:
  - id: split
    type: parallel
    allow_overlap: true
    conflict_policy: { strategy: manual }
    branches:
      - { id: first, type: command, command: echo first, outputs: [shared.json] }
      - { id: second, type: command, command: echo second, outputs: [shared.json] }
      - { id: third, type: command, command: echo first-again, outputs: [shared.json] }
`);

    const result = simulateAgentflowWorkflow(workflow, {
      steps: {
        first: { outputs: { "shared.json": "first" } },
        second: { outputs: { "shared.json": "second" } },
        third: { outputs: { "shared.json": "first" } }
      }
    });

    expect(result.status).toBe("unresolved");
    expect(result.artifactValues["shared.json"]).toBeUndefined();
    expect(result.unresolvedBranches).toContainEqual({
      stepId: "split",
      reason: "Parallel branches produced conflicting values for artifact shared.json; fixture simulation cannot apply the declared conflict policy."
    });
  });

  test("propagates availability-only artifact replacement through parallel merges", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: parallel-availability-replacement
version: 1
style: pipeline
maturity: draft
steps:
  - id: split
    type: parallel
    branches:
      - id: replace
        type: command
        command: echo replacement
        outputs: [ticket.json]
        overwrite: true
  - id: render
    type: artifact_transform
    input: ticket.json
    output: ticket.md
    transform: jira_ticket_to_markdown
`);

    const result = simulateAgentflowWorkflow(workflow, {
      artifacts: { "ticket.json": { key: "STALE", fields: { summary: "Old" } } },
      steps: { replace: { outputs: ["ticket.json"] } }
    });

    expect(result.status).toBe("unresolved");
    expect(result.artifactValues["ticket.json"]).toBeUndefined();
    expect(result.artifactValues["ticket.md"]).toBeUndefined();
  });

  test("merges parallel overwrites of artifacts produced before the parallel step", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: parallel-prior-artifact-overwrite
version: 1
style: pipeline
maturity: draft
steps:
  - id: seed
    type: command
    command: echo old
    outputs: [shared.txt]
  - id: split
    type: parallel
    branches:
      - id: replace
        type: artifact_transform
        input: source.txt
        output: shared.txt
        transform: uppercase
        overwrite: true
`);
    const registry = new AgentflowArtifactTransformRegistry().register("uppercase", (input) => ({
      content: Buffer.from(input).toString("utf8").toUpperCase(),
      contentType: "text/plain"
    }));

    const result = simulateAgentflowWorkflow(workflow, {
      artifacts: { "source.txt": "new" },
      steps: { seed: { outputs: { "shared.txt": "old" } } }
    }, registry);

    expect(result.status).toBe("completed");
    expect(result.artifactValues["shared.txt"]).toBe("NEW");
  });

  test("reports overlapping availability-only parallel outputs as unresolved", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: parallel-availability-conflict
version: 1
style: pipeline
maturity: draft
steps:
  - id: split
    type: parallel
    allow_overlap: true
    conflict_policy: { strategy: manual }
    branches:
      - { id: first, type: command, command: echo first, outputs: [shared.txt] }
      - { id: second, type: command, command: echo second, outputs: [shared.txt] }
`);

    const result = simulateAgentflowWorkflow(workflow, {
      steps: {
        first: { outputs: ["shared.txt"] },
        second: { outputs: ["shared.txt"] }
      }
    });

    expect(result.status).toBe("unresolved");
    expect(result.artifactValues["shared.txt"]).toBeUndefined();
    expect(result.unresolvedBranches).toContainEqual({
      stepId: "split",
      reason: "Parallel branches produced conflicting values for artifact shared.txt; fixture simulation cannot apply the declared conflict policy."
    });
  });

  test("traverses every declared parallel child list", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: all-parallel-lists
version: 1
style: pipeline
maturity: draft
steps:
  - id: split
    type: parallel
    body:
      - { id: from_body, type: command, command: echo body, outputs: [body.json] }
    steps:
      - { id: from_steps, type: command, command: echo steps, outputs: [steps.json] }
`);
    const result = simulateAgentflowWorkflow(workflow, {
      steps: {
        from_body: { outputs: ["body.json"] },
        from_steps: { outputs: ["steps.json"] }
      }
    });

    expect(result.status).toBe("completed");
    expect(result.visitedSteps.map((step) => step.id)).toEqual(["split", "from_body", "from_steps"]);
    expect(result.availableArtifacts).toEqual(["body.json", "steps.json"]);
  });

  test("checks artifact references nested in mapped inputs", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: mapped-input
version: 1
style: pipeline
maturity: draft
steps:
  - id: nested
    type: workflow
    workflow: child
    inputs: { payload: missing.json }
`);
    const result = simulateAgentflowWorkflow(workflow, {});

    expect(result.status).toBe("unresolved");
    expect(result.missingArtifacts).toEqual([
      { stepId: "nested", artifact: "missing.json", kind: "input" }
    ]);
  });

  test("marks exhausted retry-only failures as failed", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: exhausted-retry
version: 1
style: recovery_pipeline
maturity: draft
steps:
  - id: run
    type: command
    command: echo run
    on_failure: { retry: 1 }
`);
    const result = simulateAgentflowWorkflow(workflow, {
      steps: { run: { outcome: ["failed", "failed"] } }
    });

    expect(result.status).toBe("failed");
    expect(result.terminalStates).toEqual([{ stepId: "run", status: "failed" }]);
  });

  test("preserves enclosing loop context through parallel branches", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: looped-parallel
version: 1
style: pipeline
maturity: draft
sessions:
  worker: { provider: local }
steps:
  - id: repeat
    type: loop
    max_iterations: 2
    body:
      - id: split
        type: parallel
        branches:
          - id: branch
            session: worker
            steps:
              - { id: next_iteration, type: result, status: continue }
      - { id: skipped_each_iteration, type: command, command: echo skipped }
  - { id: finish, type: result, status: completed }
`);
    const result = simulateAgentflowWorkflow(workflow, {
      steps: { repeat: { iterations: 2 } }
    });

    expect(result.status).toBe("completed");
    expect(result.visitedSteps.map((step) => step.id)).toEqual([
      "repeat", "split", "branch", "next_iteration", "split", "branch", "next_iteration", "finish"
    ]);
  });

  test("resets retry accounting for repeated successful invocations", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: repeated-retry
version: 1
style: recovery_pipeline
maturity: draft
steps:
  - id: repeat
    type: loop
    max_iterations: 2
    body:
      - id: flaky
        type: command
        command: echo flaky
        on_failure: { retry: 1 }
  - { id: finish, type: result, status: completed }
`);
    const result = simulateAgentflowWorkflow(workflow, {
      steps: {
        repeat: { iterations: 2 },
        flaky: { outcome: ["succeeded", "failed", "succeeded"] }
      }
    });

    expect(result.status).toBe("completed");
    expect(result.visitedSteps.map((step) => `${step.id}:${step.outcome}`)).toEqual([
      "repeat:succeeded", "flaky:succeeded", "flaky:failed", "flaky:succeeded", "finish:succeeded"
    ]);
  });

  test("processes branch-level contracts before nested parallel steps", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: nested-branch-contract
version: 1
style: collaborative
maturity: draft
collaboration: { enabled: true }
sessions:
  worker: { provider: local, role: worker, authority: { can_modify_files: false } }
steps:
  - id: split
    type: parallel
    branches:
      - id: branch
        session: worker
        inputs: [missing.json]
        outputs: [branch.json]
        steps:
          - { id: nested, type: command, command: echo nested }
`);
    const result = simulateAgentflowWorkflow(workflow, { steps: { branch: {} } });

    expect(result.status).toBe("unresolved");
    expect(result.visitedSteps.map((step) => step.id)).toEqual(["split", "branch", "nested"]);
    expect(result.missingArtifacts).toEqual([
      { stepId: "branch", artifact: "missing.json", kind: "input" },
      { stepId: "branch", artifact: "branch.json", kind: "output" }
    ]);
  });

  test("retries parallel branches in place and resumes the parent sequence", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: parallel-retry
version: 1
style: collaborative
maturity: draft
collaboration: { enabled: true }
sessions:
  worker: { provider: local, role: worker, authority: { can_modify_files: false } }
steps:
  - id: split
    type: parallel
    branches:
      - id: branch
        session: worker
        on_failure: { retry: 1 }
        outputs: [branch.json]
  - { id: finish, type: result, status: completed }
`);
    const result = simulateAgentflowWorkflow(workflow, {
      steps: { branch: { outcome: ["failed", "succeeded"], outputs: ["branch.json"] } }
    });

    expect(result.status).toBe("completed");
    expect(result.visitedSteps.map((step) => step.id)).toEqual(["split", "branch", "branch", "finish"]);
  });

  test("enforces the workflow recovery-cycle limit", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: bounded-cycle
version: 1
style: recovery_pipeline
maturity: draft
limits: { max_recovery_cycles: 2 }
steps:
  - { id: retry, type: condition, if: retry, then: retry, else: complete }
`);
    const result = simulateAgentflowWorkflow(workflow, {
      steps: { retry: { condition: "retry" } }
    });

    expect(result.status).toBe("unresolved");
    expect(result.visitedSteps).toHaveLength(3);
    expect(result.unresolvedBranches).toEqual([
      { stepId: "retry", reason: "Simulation exceeded limits.max_recovery_cycles 2." }
    ]);
  });

  test("resolves fixture inputs used as artifact references", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: dynamic-artifact
version: 1
style: pipeline
maturity: draft
inputs:
  payload: { required: true }
steps:
  - { id: inspect, type: command, command: echo inspect, inputs: ["{{ inputs.payload }}"] }
`);
    const result = simulateAgentflowWorkflow(workflow, {
      inputs: { payload: "missing.json" }
    });

    expect(result.status).toBe("unresolved");
    expect(result.missingArtifacts).toEqual([
      { stepId: "inspect", artifact: "missing.json", kind: "input" }
    ]);
  });

  test("gives declared step IDs precedence over terminal aliases", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: alias-step
version: 1
style: pipeline
maturity: draft
steps:
  - { id: route, type: condition, if: ready, then: complete, else: fail }
  - { id: complete, type: command, command: echo complete, outputs: [done.json] }
`);
    const result = simulateAgentflowWorkflow(workflow, {
      steps: {
        route: { condition: "complete" },
        complete: { outputs: ["done.json"] }
      }
    });

    expect(result.status).toBe("completed");
    expect(result.visitedSteps.map((step) => step.id)).toEqual(["route", "complete"]);
    expect(result.availableArtifacts).toEqual(["done.json"]);
  });

  test("counts parallel branch retries against the transition limit", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: bounded-parallel-retry
version: 1
style: collaborative
maturity: draft
collaboration: { enabled: true }
sessions:
  worker: { provider: local, role: worker, authority: { can_modify_files: false } }
steps:
  - id: split
    type: parallel
    branches:
      - id: branch
        session: worker
        on_failure: { retry: 10001 }
`);
    const result = simulateAgentflowWorkflow(workflow, {
      steps: { branch: { outcome: "failed" } }
    });

    expect(result.status).toBe("unresolved");
    expect(result.unresolvedBranches).toEqual([
      { stepId: "branch", reason: "Simulation exceeded its deterministic transition limit." }
    ]);
    expect(result.visitedSteps).toHaveLength(10000);
  });

  test("terminates rejected gates that have no explicit rejection target", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: rejected-gate
version: 1
style: pipeline
maturity: draft
steps:
  - id: gate
    type: manual_gate
    message: Deploy?
    options: [approve, reject]
  - { id: deploy, type: command, command: echo deploy, outputs: [deployed.json] }
`);
    const result = simulateAgentflowWorkflow(workflow, {
      steps: {
        gate: { choice: "reject" },
        deploy: { outputs: ["deployed.json"] }
      }
    });

    expect(result.status).toBe("cancelled");
    expect(result.visitedSteps.map((step) => step.id)).toEqual(["gate"]);
    expect(result.terminalStates).toEqual([{ stepId: "gate", status: "cancelled" }]);
  });

  test("traverses both nested lists in parallel branch descriptors", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: both-branch-lists
version: 1
style: collaborative
maturity: draft
collaboration: { enabled: true }
sessions:
  worker: { provider: local, role: worker, authority: { can_modify_files: false } }
steps:
  - id: split
    type: parallel
    branches:
      - id: branch
        session: worker
        body:
          - { id: from_body, type: command, command: echo body, outputs: [body.json] }
        steps:
          - { id: from_steps, type: command, command: echo steps, outputs: [steps.json] }
`);
    const result = simulateAgentflowWorkflow(workflow, {
      steps: {
        from_body: { outputs: ["body.json"] },
        from_steps: { outputs: ["steps.json"] }
      }
    });

    expect(result.status).toBe("completed");
    expect(result.visitedSteps.map((step) => step.id)).toEqual(["split", "branch", "from_body", "from_steps"]);
    expect(result.availableArtifacts).toEqual(["body.json", "steps.json"]);
  });

  test("rejects ambiguous parallel branch IDs before traversal", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: ambiguous-branches
version: 1
style: collaborative
maturity: draft
collaboration: { enabled: true }
sessions:
  worker: { provider: local, role: worker, authority: { can_modify_files: false } }
steps:
  - id: first
    type: parallel
    branches:
      - { id: shared, session: worker }
  - id: second
    type: parallel
    branches:
      - { id: shared, session: worker }
`);
    const result = simulateAgentflowWorkflow(workflow, {
      steps: { shared: { outcome: "succeeded" } }
    });

    expect(result.status).toBe("unresolved");
    expect(result.visitedSteps).toEqual([]);
    expect(result.unresolvedBranches).toEqual([
      { stepId: "shared", reason: "Workflow step ID is ambiguous in simulation fixtures and targets." }
    ]);
  });

  test("honors goto targets in failure handlers", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: failure-goto
version: 1
style: recovery_pipeline
maturity: draft
steps:
  - id: work
    type: command
    command: echo work
    on_failure: { goto: recover }
  - { id: skipped, type: command, command: echo skipped }
  - { id: recover, type: result, status: completed }
`);
    const result = simulateAgentflowWorkflow(workflow, {
      steps: { work: { outcome: "failed" } }
    });

    expect(result.status).toBe("completed");
    expect(result.visitedSteps.map((step) => step.id)).toEqual(["work", "recover"]);
  });

  test("collects every parallel branch diagnostic before returning a terminal result", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: parallel-diagnostics
version: 1
style: collaborative
maturity: draft
collaboration: { enabled: true }
sessions:
  worker: { provider: local, role: worker, authority: { can_modify_files: false } }
steps:
  - id: split
    type: parallel
    branches:
      - { id: failing, session: worker }
      - { id: unchecked, session: worker, inputs: [missing.json], outputs: [result.json] }
`);
    const result = simulateAgentflowWorkflow(workflow, {
      steps: { failing: { outcome: "failed" } }
    });

    expect(result.status).toBe("unresolved");
    expect(result.visitedSteps.map((step) => step.id)).toEqual(["split", "failing", "unchecked"]);
    expect(result.missingArtifacts).toEqual([
      { stepId: "unchecked", artifact: "missing.json", kind: "input" },
      { stepId: "unchecked", artifact: "result.json", kind: "output" }
    ]);
    expect(result.terminalStates).toEqual([{ stepId: "failing", status: "failed" }]);
  });

  test("recognizes paused and unresolved terminal target aliases", () => {
    for (const [target, status] of [["paused", "paused"], ["unresolved", "unresolved"]] as const) {
      const workflow = parseAgentflowWorkflowOrThrow(`name: terminal-${target}
version: 1
style: recovery_pipeline
maturity: draft
steps:
  - id: fail
    type: command
    command: echo fail
    on_failure: { then: ${target} }
`);
      const result = simulateAgentflowWorkflow(workflow, {
        steps: { fail: { outcome: "failed" } }
      });

      expect(result.status).toBe(status);
      expect(result.terminalStates).toEqual([{ stepId: "fail", status }]);
    }
  });
});
