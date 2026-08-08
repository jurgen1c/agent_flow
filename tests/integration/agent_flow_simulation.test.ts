import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import path from "node:path";
import {
  AgentFlowArtifactTransformRegistry,
  parseAgentFlowWorkflowOrThrow,
  renderAgentFlowSimulationSummary,
  parseAgentFlowSimulationFixture,
  simulateAgentFlowWorkflow,
  validateAgentFlowWorkflow
} from "../../src/runtime";

const repoRoot = path.resolve(".");
const examples = path.join(repoRoot, "examples/workflows");

function loadExample(name: string) {
  return parseAgentFlowWorkflowOrThrow(fs.readFileSync(path.join(examples, name), "utf8"));
}

describe("Agent Flow workflow simulation", () => {
  test("simulates a pipeline from fixture-provided step outcomes and outputs without mutation", () => {
    const workflow = loadExample("simple-ci.yml");
    const before = JSON.stringify(workflow);
    const first = simulateAgentFlowWorkflow(workflow, {
      steps: {
        install: { outcome: "succeeded" },
        lint: { outputs: ["ci/rubocop.log"] },
        test: { outputs: ["ci/test.log"] }
      }
    });
    const second = simulateAgentFlowWorkflow(workflow, {
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

  test("simulates recovery routing from fixture artifact values", () => {
    const result = simulateAgentFlowWorkflow(loadExample("ci-triage.yml"), {
      artifacts: {
        "failures/failure.json": { kind: "implementation_error" }
      },
      inputs: {
        failure_payload: "failures/failure.json",
        failed_step: "local_ci"
      },
      steps: {
        classify: {
          outputs: {
            "ci/failure-classification.json": {
              kind: "flake",
              confidence: "high",
              summary: "Transient test failure.",
              recommended_owner: "workflow_owner",
              safe_to_retry: true,
              requires_user: false
            }
          }
        }
      }
    });

    expect(result.status).toBe("completed");
    expect(result.visitedSteps.map((step) => step.id)).toEqual(["classify", "route", "return_remediated"]);
    expect(result.terminalStates).toEqual([{ stepId: "return_remediated", status: "remediated" }]);
  });

  test("simulates collaboration routing and artifact contracts", () => {
    const result = simulateAgentFlowWorkflow(loadExample("implement-review-collab.yml"), {
      artifacts: {
        "git.diff": "fixture diff",
        "spec.md": "fixture spec"
      },
      steps: {
        implement: { outputs: ["implementation-summary.md"] },
        review: { outputs: { "reviews/code-review.json": { status: "approved", findings: [] } } },
        approve_implementation: {
          outputs: {
            "approvals/approve_implementation.json": { status: "approved", decision: "Reviewed evidence is current." }
          }
        },
        ask_user: { input: "continue" }
      }
    });

    expect(result.status).toBe("completed");
    expect(result.visitedSteps.map((step) => step.id)).toEqual([
      "implement",
      "review",
      "route_review",
      "approve_implementation",
      "record_approval",
      "ask_user"
    ]);
    expect(result.missingArtifacts).toEqual([]);
  });

  test("fails simulation before merge-capable continuation when a watched artifact changes", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: simulated-approval-invalidation
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true }
sessions:
  reviewer: { provider: fixture, role: reviewer, authority: { can_approve: true } }
  merger: { provider: fixture, role: merger, authority: { can_merge: true } }
steps:
  - { id: approve, type: approval, reviewer: reviewer, artifacts: [spec.md] }
  - { id: revise, type: command, command: revise, outputs: [summary.md], overwrite: true }
  - { id: merge, type: session_request, session: merger, prompt: merge.md, inputs: [approvals/approve.json], outputs: [merged.md] }
  - { id: done, type: result, status: completed }
approvals:
  approve: { invalidated_by: [summary.md] }
`);

    const result = simulateAgentFlowWorkflow(workflow, {
      artifacts: { "spec.md": "specification", "summary.md": "original" },
      steps: {
        approve: {
          outputs: {
            "approvals/approve.json": { status: "approved", decision: "Approved original summary." }
          }
        },
        revise: { outputs: { "summary.md": "revised" } },
        merge: { outputs: { "merged.md": "merged" } }
      }
    });

    expect(result.status).toBe("failed");
    expect(result.visitedSteps.map((step) => step.id)).toEqual(["approve", "revise"]);
    expect(result.availableArtifacts).not.toContain("merged.md");
    expect(result.terminalStates).toContainEqual({ stepId: "merge", status: "failed" });
  });

  test("makes stale approval outputs unavailable to ordinary simulation consumers", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: simulated-stale-approval-output
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true }
sessions:
  reviewer: { provider: fixture, role: reviewer, authority: { can_approve: true } }
  worker: { provider: fixture, role: worker }
steps:
  - { id: approve, type: approval, reviewer: reviewer, artifacts: [spec.md] }
  - { id: revise, type: command, command: revise, outputs: [summary.md] }
  - { id: consume, type: session_request, session: worker, prompt: consume.md, inputs: [approvals/approve.json], outputs: [result.md] }
  - { id: done, type: result, status: completed }
approvals:
  approve: { invalidated_by: [summary.md] }
`);

    const result = simulateAgentFlowWorkflow(workflow, {
      artifacts: { "spec.md": "specification" },
      steps: {
        approve: {
          outputs: {
            "approvals/approve.json": { status: "approved", decision: "Approved." }
          }
        },
        revise: { outputs: { "summary.md": "revised" } },
        consume: { outputs: { "result.md": "consumed" } }
      }
    });

    expect(result.status).toBe("paused");
    expect(result.availableArtifacts).not.toContain("approvals/approve.json");
    expect(result.availableArtifacts).not.toContain("result.md");
    expect(result.visitedSteps.at(-1)).toEqual({ id: "consume", type: "session_request", outcome: "failed" });
  });

  test("invalidates a simulated approval when its outcome is overwritten", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: simulated-approval-outcome-overwrite
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true }
sessions:
  reviewer: { provider: fixture, authority: { can_approve: true } }
steps:
  - { id: approve, type: approval, reviewer: reviewer, artifacts: [spec.md] }
  - { id: replace, type: command, command: replace, outputs: [approvals/approve.json], overwrite: true }
  - { id: done, type: result, status: completed }
`);

    const result = simulateAgentFlowWorkflow(workflow, {
      artifacts: { "spec.md": "specification" },
      steps: {
        approve: {
          outputs: { "approvals/approve.json": { status: "approved", decision: "Approved." } }
        },
        replace: {
          outputs: { "approvals/approve.json": { status: "replaced", decision: "No longer approved." } }
        }
      }
    });

    expect(result.status).toBe("failed");
    expect(result.availableArtifacts).not.toContain("approvals/approve.json");
    expect(result.terminalStates).toContainEqual({ stepId: "done", status: "failed" });
  });

  test("treats repeated value-less watched output production as an unknown change", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: simulated-value-less-invalidation
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true }
sessions:
  reviewer: { provider: fixture, role: reviewer, authority: { can_approve: true } }
steps:
  - { id: seed, type: command, command: seed, outputs: [watched.md] }
  - { id: approve, type: approval, reviewer: reviewer, artifacts: [watched.md] }
  - { id: revise, type: command, command: revise, outputs: [watched.md], overwrite: true }
  - { id: done, type: result, status: completed }
approvals:
  approve: { invalidated_by: [watched.md] }
`);

    const result = simulateAgentFlowWorkflow(workflow, {
      steps: {
        seed: { outputs: ["watched.md"] },
        approve: {
          outputs: { "approvals/approve.json": { status: "approved", decision: "Approved." } }
        },
        revise: { outputs: ["watched.md"] }
      }
    });

    expect(result.status).toBe("failed");
    expect(result.availableArtifacts).not.toContain("approvals/approve.json");
    expect(result.terminalStates).toContainEqual({ stepId: "done", status: "failed" });
  });

  test("does not treat incidental actor fields on commands as merge sessions", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: simulated-incidental-merge-actor
version: 1
style: pipeline
maturity: experimental
sessions:
  reviewer: { provider: fixture, authority: { can_approve: true } }
  merger: { provider: fixture, authority: { can_merge: true } }
steps:
  - { id: approve, type: approval, reviewer: reviewer, artifacts: [spec.md] }
  - { id: revise, type: command, command: revise, outputs: [watched.md] }
  - { id: prepare, type: command, command: prepare, owner: merger, outputs: [prepared.md] }
  - { id: done, type: result, status: completed }
approvals:
  approve: { invalidated_by: [watched.md] }
`);

    const result = simulateAgentFlowWorkflow(workflow, {
      artifacts: { "spec.md": "Specification" },
      steps: {
        approve: {
          outputs: { "approvals/approve.json": { status: "approved", decision: "Approved." } }
        },
        revise: { outputs: { "watched.md": "Changed" } },
        prepare: { outputs: { "prepared.md": "Prepared" } }
      }
    });

    expect(result.visitedSteps).toContainEqual({ id: "prepare", type: "command", outcome: "succeeded" });
    expect(result.availableArtifacts).toContain("prepared.md");
    expect(result.status).toBe("failed");
  });

  test("preserves stale approval output unavailability across parallel joins", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: simulated-parallel-approval-invalidation
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true }
sessions:
  reviewer: { provider: fixture, role: reviewer, authority: { can_approve: true } }
  worker: { provider: fixture, role: worker }
steps:
  - { id: approve, type: approval, reviewer: reviewer, artifacts: [spec.md] }
  - id: split
    type: parallel
    branches:
      - { id: revise, type: command, command: revise, outputs: [summary.md] }
  - { id: consume, type: session_request, session: worker, prompt: consume.md, inputs: [approvals/approve.json], outputs: [result.md] }
approvals:
  approve: { invalidated_by: [summary.md] }
`);

    const result = simulateAgentFlowWorkflow(workflow, {
      artifacts: { "spec.md": "specification" },
      steps: {
        approve: {
          outputs: { "approvals/approve.json": { status: "approved", decision: "Approved." } }
        },
        revise: { outputs: { "summary.md": "revised" } },
        consume: { outputs: { "result.md": "consumed" } }
      }
    });

    expect(result.status).toBe("paused");
    expect(result.availableArtifacts).not.toContain("approvals/approve.json");
    expect(result.availableArtifacts).not.toContain("result.md");
  });

  test("lets a parallel approval rerun clear inherited staleness", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: simulated-parallel-approval-rerun
version: 1
style: recovery_pipeline
maturity: experimental
sessions:
  reviewer: { provider: fixture, authority: { can_approve: true } }
steps:
  - { id: start, type: command, command: start, then: approve }
  - { id: revise, type: command, command: revise, outputs: [summary.md] }
  - id: split
    type: parallel
    branches:
      - { id: approve, session: reviewer, type: approval, reviewer: reviewer, artifacts: [spec.md] }
      - id: route
        session: reviewer
        type: command
        command: route
        then: done
        on_failure: { then: revise, allowed: true }
  - { id: done, type: result, status: completed }
approvals:
  approve: { invalidated_by: [summary.md] }
limits: { max_recovery_cycles: 3 }
`);
    expect(validateAgentFlowWorkflow(workflow)).toEqual({ valid: true, errors: [] });

    const result = simulateAgentFlowWorkflow(workflow, {
      artifacts: { "spec.md": "specification" },
      steps: {
        start: {},
        revise: { outputs: { "summary.md": "revised" } },
        approve: {
          outputs: { "approvals/approve.json": { status: "approved", decision: "Approved." } }
        },
        route: { outcome: ["failed", "succeeded"] }
      }
    });

    expect(result.status).toBe("completed");
    expect(result.availableArtifacts).toContain("approvals/approve.json");
    expect(result.terminalStates).toContainEqual({ stepId: "done", status: "completed" });
  });

  test("keeps an approval stale when a sibling branch changes watched evidence during its rerun", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: simulated-parallel-approval-rerun-race
version: 1
style: recovery_pipeline
maturity: experimental
sessions:
  reviewer: { provider: fixture, authority: { can_approve: true } }
  worker: { provider: fixture }
steps:
  - { id: start, type: command, command: start, then: approve }
  - { id: revise, type: command, command: revise, outputs: [summary.md] }
  - id: split
    type: parallel
    branches:
      - { id: approve, session: reviewer, type: approval, reviewer: reviewer, artifacts: [spec.md] }
      - id: route
        session: worker
        type: command
        command: route
        then: done
        on_failure: { then: revise, allowed: true }
      - { id: revise_again, session: worker, type: command, command: revise, outputs: [summary.md], overwrite: true }
  - { id: done, type: result, status: completed }
approvals:
  approve: { invalidated_by: [summary.md] }
limits: { max_recovery_cycles: 5 }
`);
    expect(validateAgentFlowWorkflow(workflow)).toEqual({ valid: true, errors: [] });

    const result = simulateAgentFlowWorkflow(workflow, {
      artifacts: { "spec.md": "specification" },
      steps: {
        start: {},
        revise: { outputs: { "summary.md": "first revision" } },
        approve: {
          outputs: { "approvals/approve.json": { status: "approved", decision: "Approved." } }
        },
        route: { outcome: ["failed", "succeeded"] },
        revise_again: { outputs: { "summary.md": "concurrent revision" } }
      }
    });

    expect(result.status).toBe("failed");
    expect(result.availableArtifacts).not.toContain("approvals/approve.json");
    expect(result.terminalStates).toContainEqual({ stepId: "done", status: "failed" });
  });

  test("reports unresolved conditions and missing artifacts deterministically", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: unresolved
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
    const result = simulateAgentFlowWorkflow(workflow, { steps: { inspect: {} } });
    const summary = renderAgentFlowSimulationSummary(result);

    expect(result.status).toBe("paused");
    expect(result.missingArtifacts).toEqual([
      { stepId: "inspect", artifact: "missing.json", kind: "input" }
    ]);
    expect(result.unresolvedBranches).toEqual([]);
    expect(result.visitedSteps).toEqual([{ id: "inspect", type: "session_request", outcome: "failed" }]);
    expect(summary).toContain("Status: paused");
    expect(summary).toContain("inspect: missing input artifact missing.json");
  });

  test("requires declared workflow inputs and evaluates condition fallthrough from fixture values", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: inputs
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

    const missing = simulateAgentFlowWorkflow(workflow, {});
    expect(missing.status).toBe("unresolved");
    expect(missing.missingInputs).toEqual(["required_value"]);
    expect(missing.visitedSteps.map((step) => step.id)).toEqual(["route"]);
    expect(missing.terminalStates).toEqual([]);

    const present = simulateAgentFlowWorkflow(workflow, {
      inputs: { required_value: false }
    });
    expect(present.status).toBe("completed");
    expect(present.visitedSteps.map((step) => step.id)).toEqual(["route", "finish"]);
  });

  test("parses fixture artifact strings as the JSON bytes used by runtime conditions", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: condition-json-bytes
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: route, type: condition, if: artifacts.state.ready == true, then: complete, else: fail }
`);

    const matched = simulateAgentFlowWorkflow(workflow, {
      artifacts: { "state.json": '{"ready":true}' }
    });
    expect(matched.status).toBe("completed");

    const invalid = simulateAgentFlowWorkflow(workflow, {
      artifacts: { "state.json": "not-json" }
    });
    expect(invalid.status).toBe("unresolved");
    expect(invalid.unresolvedBranches[0]?.reason).toContain("must contain valid JSON");
  });

  test("matches runtime fallthrough and output behavior for dynamic routes and conditions", () => {
    const dynamic = parseAgentFlowWorkflowOrThrow(`name: dynamic-success-fallthrough
version: 1
style: pipeline
maturity: experimental
inputs: { next: {} }
steps:
  - { id: first, type: command, command: echo first, then: "target-{{ inputs.next }}" }
  - { id: second, type: command, command: echo second }
`);
    const fallenThrough = simulateAgentFlowWorkflow(dynamic, { inputs: { next: "second" } });
    expect(fallenThrough.status).toBe("completed");
    expect(fallenThrough.visitedSteps.map((step) => step.id)).toEqual(["first", "second"]);

    const conditionOutput = parseAgentFlowWorkflowOrThrow(`name: condition-fixture-output
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: route, type: condition, if: artifacts.state.ready == true, then: complete, else: fail, outputs: [state.json] }
`);
    const notFabricated = simulateAgentFlowWorkflow(conditionOutput, {
      steps: { route: { outputs: { "state.json": { ready: true } } } }
    });
    expect(notFabricated.status).toBe("unresolved");
    expect(notFabricated.availableArtifacts).toEqual([]);
  });

  test("replays bounded retries from sequential fixture outcomes", () => {
    const workflow = loadExample("simple-ci.yml");
    const result = simulateAgentFlowWorkflow(workflow, {
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

  test("does not charge immediate retries to the recovery-cycle budget", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: immediate-retry-budget
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_recovery_cycles: 1 }
steps:
  - id: work
    type: command
    command: echo work
    on_failure: { retry: 2 }
`);

    const result = simulateAgentFlowWorkflow(workflow, {
      steps: { work: { outcome: ["failed", "failed", "succeeded"] } }
    });

    expect(result.status).toBe("completed");
    expect(result.visitedSteps.map((step) => step.outcome)).toEqual(["failed", "failed", "succeeded"]);
    expect(result.unresolvedBranches).toEqual([]);
  });

  test("simulates standard parallel branch descriptors", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: parallel
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
    const result = simulateAgentFlowWorkflow(workflow, {
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
      { steps: { run: { condition: false } } },
      { steps: { run: { choice: ["approve", 2] } } },
      { steps: { run: { iterations: -1 } } },
      { steps: { run: { recovery: "unknown" } } },
      { inputs: { "": "value" } },
      { artifacts: { " ": "value" } },
      { steps: { "": { outcome: "succeeded" } } },
      { steps: { run: { outputs: { "": "value" } } } }
    ]) {
      expect(parseAgentFlowSimulationFixture(JSON.stringify(source)).ok).toBe(false);
    }
  });

  test("continues after valid continue and ignore failure targets", () => {
    for (const target of ["continue", "ignore"]) {
      const workflow = parseAgentFlowWorkflowOrThrow(`name: failure-${target}
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
      const result = simulateAgentFlowWorkflow(workflow, {
        steps: { optional: { outcome: "failed" } }
      });

      expect(result.status).toBe("completed");
      expect(result.visitedSteps.map((step) => step.id)).toEqual(["optional", "finish"]);
    }
  });

  test("reports unknown fixture step IDs and undeclared outputs", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: fixture-contract
version: 1
style: pipeline
maturity: draft
steps:
  - { id: produce, type: command, command: echo result, outputs: [declared.json] }
  - { id: consume, type: command, command: cat declared.json, inputs: [declared.json] }
`);
    const result = simulateAgentFlowWorkflow(workflow, {
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
    const result = simulateAgentFlowWorkflow(loadExample("pr-feedback-loop.yml"), {
      inputs: { pr_url: "https://github.test/example/pull/1" },
      artifacts: { "implementation-summary.md": "fixture summary" },
      steps: {
        wait_for_review: { iterations: 1 },
        collect_pr_state: { outputs: ["github/pr-state.json"] },
        classify_comments: { outputs: { "github/actionable-comments.json": { count: 0 } } }
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
    const workflow = parseAgentFlowWorkflowOrThrow(`name: bounded-simulation
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
    const result = simulateAgentFlowWorkflow(workflow, {
      steps: { repeat: { iterations: 10001 } }
    });

    expect(result.status).toBe("unresolved");
    expect(result.unresolvedBranches).toEqual([
      { stepId: "inspect", reason: "Simulation exceeded its deterministic transition limit." }
    ]);
    expect(result.visitedSteps).toHaveLength(10000);
  });

  test("enters globally targeted steps nested inside control-flow containers", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: nested-target
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
    const result = simulateAgentFlowWorkflow(workflow, { artifacts: { "ready.json": true } });

    expect(result.status).toBe("completed");
    expect(result.visitedSteps.map((step) => step.id)).toEqual(["route", "nested_finish"]);
  });

  test("records failed condition outcomes before taking their failure path", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: failed-condition
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
    const result = simulateAgentFlowWorkflow(workflow, {
      steps: { route: { outcome: "failed" } }
    });

    expect(result.status).toBe("failed");
    expect(result.visitedSteps).toEqual([
      { id: "route", type: "condition", outcome: "failed" }
    ]);
  });

  test("records condition evaluation errors as failed visits", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: unresolved-condition
version: 1
style: pipeline
maturity: draft
steps:
  - { id: route, type: condition, if: artifacts.missing == true, then: complete, else: cancel }
`);

    const result = simulateAgentFlowWorkflow(workflow, {});

    expect(result.status).toBe("unresolved");
    expect(result.visitedSteps).toEqual([
      { id: "route", type: "condition", outcome: "failed" }
    ]);
  });

  test("fails closed for missing compared values and malformed branch entries", () => {
    const missingValue = parseAgentFlowWorkflowOrThrow(`name: simulated-missing-comparison
version: 1
style: pipeline
maturity: draft
inputs: { status: {} }
steps:
  - { id: route, type: condition, if: status != "changes_requested", then: complete, else: fail }
`);
    const missingResult = simulateAgentFlowWorkflow(missingValue, {});
    expect(missingResult.status).toBe("unresolved");
    expect(missingResult.unresolvedBranches[0]?.reason).toContain("did not resolve to a value");

    const malformedBranches = parseAgentFlowWorkflowOrThrow(`name: simulated-malformed-branches
version: 1
style: pipeline
maturity: draft
inputs: { ready: {} }
steps:
  - id: route
    type: condition
    branches:
      - true
      - { if: ready, then: complete }
`);
    const malformedResult = simulateAgentFlowWorkflow(malformedBranches, { inputs: { ready: true } });
    expect(malformedResult.status).toBe("unresolved");
    expect(malformedResult.unresolvedBranches[0]?.reason).toContain("Condition branches must be a list of mappings");

    const malformedElse = parseAgentFlowWorkflowOrThrow(`name: simulated-malformed-else
version: 1
style: pipeline
maturity: draft
inputs: { ready: {} }
steps:
  - { id: route, type: condition, branches: [{ if: ready, then: complete }], else: 42 }
`);
    const malformedElseResult = simulateAgentFlowWorkflow(malformedElse, { inputs: { ready: false } });
    expect(malformedElseResult.status).toBe("unresolved");
    expect(malformedElseResult.unresolvedBranches[0]?.reason).toContain("Condition else must be a non-empty string");
  });

  test("stops conditions before routing when any required input is absent", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: unrelated-required-input
version: 1
style: pipeline
maturity: draft
inputs:
  ready: { required: true }
  unrelated: { required: true }
steps:
  - { id: route, type: condition, if: ready, then: complete, else: cancel }
`);

    const result = simulateAgentFlowWorkflow(workflow, { inputs: { ready: true } });

    expect(result.status).toBe("unresolved");
    expect(result.visitedSteps).toEqual([{ id: "route", type: "condition", outcome: "failed" }]);
    expect(result.terminalStates).toEqual([]);
  });

  test("enforces the runtime artifact read limit during condition simulation", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: oversized-condition-artifact
version: 1
style: pipeline
maturity: draft
steps:
  - { id: route, type: condition, if: artifacts.payload.ready, then: complete, else: cancel }
`);

    const result = simulateAgentFlowWorkflow(workflow, {
      artifacts: { "payload.json": "x".repeat(10 * 1024 * 1024 + 1) }
    });

    expect(result.status).toBe("unresolved");
    expect(result.visitedSteps).toEqual([{ id: "route", type: "condition", outcome: "failed" }]);
    expect(result.unresolvedBranches[0]?.reason).toContain("exceeds the 10485760-byte read limit");
  });

  test("requires fixture-selected routed recovery outcomes", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: recovery-route
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

    const missing = simulateAgentFlowWorkflow(workflow, {
      steps: { work: { outcome: "failed" } }
    });
    expect(missing.status).toBe("unresolved");
    expect(missing.unresolvedBranches).toEqual([
      { stepId: "work", reason: "Fixture does not select a routed recovery outcome." }
    ]);

    const unresolved = simulateAgentFlowWorkflow(workflow, {
      steps: { work: { outcome: "failed", recovery: "unresolved" } }
    });
    expect(unresolved.status).toBe("paused");
    expect(unresolved.terminalStates).toEqual([{ stepId: "work", status: "paused" }]);
  });

  test("preserves paused result status", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: paused-result
version: 1
style: pipeline
maturity: draft
steps:
  - { id: wait, type: result, status: paused }
`);
    const result = simulateAgentFlowWorkflow(workflow, {});

    expect(result.status).toBe("paused");
    expect(result.terminalStates).toEqual([{ stepId: "wait", status: "paused" }]);
  });

  test("isolates parallel branch artifact reads and merges their outputs", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: isolated-parallel
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
    const result = simulateAgentFlowWorkflow(workflow, {
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
    const workflow = parseAgentFlowWorkflowOrThrow(`name: conflicting-parallel-artifacts
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

    const result = simulateAgentFlowWorkflow(workflow, {
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
    const workflow = parseAgentFlowWorkflowOrThrow(`name: parallel-availability-replacement
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

    const result = simulateAgentFlowWorkflow(workflow, {
      artifacts: { "ticket.json": { key: "STALE", fields: { summary: "Old" } } },
      steps: { replace: { outputs: ["ticket.json"] } }
    });

    expect(result.status).toBe("unresolved");
    expect(result.artifactValues["ticket.json"]).toBeUndefined();
    expect(result.artifactValues["ticket.md"]).toBeUndefined();
  });

  test("merges parallel overwrites of artifacts produced before the parallel step", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: parallel-prior-artifact-overwrite
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
    const registry = new AgentFlowArtifactTransformRegistry().register("uppercase", (input) => ({
      content: Buffer.from(input).toString("utf8").toUpperCase(),
      contentType: "text/plain"
    }));

    const result = simulateAgentFlowWorkflow(workflow, {
      artifacts: { "source.txt": "new" },
      steps: { seed: { outputs: { "shared.txt": "old" } } }
    }, registry);

    expect(result.status).toBe("completed");
    expect(result.artifactValues["shared.txt"]).toBe("NEW");
  });

  test("allows rerouted transforms to replace outputs they already own", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: simulated-transform-revisit
version: 1
style: recovery_pipeline
maturity: experimental
limits:
  max_recovery_cycles: 1
  max_step_attempts: { update: 1 }
steps:
  - { id: seed, type: command, command: echo one, outputs: [source.txt] }
  - { id: render, type: artifact_transform, input: source.txt, output: rendered.txt, transform: uppercase }
  - { id: update, type: command, command: echo two, outputs: [source.txt], overwrite: true, then: render }
`);
    const registry = new AgentFlowArtifactTransformRegistry().register("uppercase", (input) => ({
      content: Buffer.from(input).toString("utf8").toUpperCase(),
      contentType: "text/plain"
    }));

    const result = simulateAgentFlowWorkflow(workflow, {
      steps: {
        seed: { outputs: { "source.txt": "one" } },
        update: { outputs: { "source.txt": "two" } }
      }
    }, registry);

    expect(result.visitedSteps.map((step) => step.id)).toEqual(["seed", "render", "update", "render"]);
    expect(result.artifactValues["rendered.txt"]).toBe("TWO");
  });

  test("stops rerouted commands from replacing foreign-owned outputs", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: simulated-output-owner
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_recovery_cycles: 1 }
steps:
  - { id: first, type: command, command: echo first, outputs: [shared.txt] }
  - { id: second, type: command, command: echo second, outputs: [shared.txt], overwrite: true, then: first }
`);

    const result = simulateAgentFlowWorkflow(workflow, {
      steps: {
        first: { outputs: { "shared.txt": "first" } },
        second: { outputs: { "shared.txt": "second" } }
      }
    });

    expect(result.status).toBe("unresolved");
    expect(result.visitedSteps.map((step) => `${step.id}:${step.outcome}`)).toEqual([
      "first:succeeded", "second:succeeded", "first:failed"
    ]);
    expect(result.artifactValues["shared.txt"]).toBe("second");
  });

  test("allows commands to idempotently publish identical fixture output", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: simulated-idempotent-output
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - { id: first, type: command, command: echo same, outputs: [shared.txt] }
  - { id: second, type: command, command: echo same, outputs: [shared.txt] }
`);

    const result = simulateAgentFlowWorkflow(workflow, {
      steps: {
        first: { outputs: { "shared.txt": "same" } },
        second: { outputs: { "shared.txt": "same" } }
      }
    });

    expect(result.status).toBe("completed");
    expect(result.visitedSteps.map((step) => step.id)).toEqual(["first", "second"]);
    expect(result.artifactValues["shared.txt"]).toBe("same");
  });

  test("does not claim identical fixture-owned output as a command artifact", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: simulated-fixture-output-owner
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: write, type: command, command: echo same, outputs: [shared.txt] }
`);

    const result = simulateAgentFlowWorkflow(workflow, {
      artifacts: { "shared.txt": "same" },
      steps: { write: { outputs: { "shared.txt": "same" } } }
    });

    expect(result.status).toBe("unresolved");
    expect(result.visitedSteps).toEqual([{ id: "write", type: "command", outcome: "failed" }]);
    expect(result.artifactValues["shared.txt"]).toBe("same");
  });

  test("reports overlapping availability-only parallel outputs as unresolved", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: parallel-availability-conflict
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

    const result = simulateAgentFlowWorkflow(workflow, {
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
    const workflow = parseAgentFlowWorkflowOrThrow(`name: all-parallel-lists
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
    const result = simulateAgentFlowWorkflow(workflow, {
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
    const workflow = parseAgentFlowWorkflowOrThrow(`name: mapped-input
version: 1
style: pipeline
maturity: draft
steps:
  - id: nested
    type: workflow
    workflow: child
    inputs: { payload: missing.json }
`);
    const result = simulateAgentFlowWorkflow(workflow, {});

    expect(result.status).toBe("unresolved");
    expect(result.missingArtifacts).toEqual([
      { stepId: "nested", artifact: "missing.json", kind: "input" }
    ]);
  });

  test("marks exhausted retry-only failures as failed", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: exhausted-retry
version: 1
style: recovery_pipeline
maturity: draft
steps:
  - id: run
    type: command
    command: echo run
    on_failure: { retry: 1 }
`);
    const result = simulateAgentFlowWorkflow(workflow, {
      steps: { run: { outcome: ["failed", "failed"] } }
    });

    expect(result.status).toBe("failed");
    expect(result.terminalStates).toEqual([{ stepId: "run", status: "failed" }]);
  });

  test("pauses pipeline failures by default unless the policy explicitly fails", () => {
    for (const [name, onFailure, expected] of [
      ["unexpected", "", "paused"],
      ["retry-exhausted", "on_failure: { retry: 1 }", "paused"],
      ["explicit-fail", "on_failure: { then: fail }", "failed"]
    ] as const) {
      const workflow = parseAgentFlowWorkflowOrThrow(`name: ${name}
version: 1
style: pipeline
maturity: draft
steps:
  - id: run
    type: command
    command: echo run
    ${onFailure}
`);
      const result = simulateAgentFlowWorkflow(workflow, {
        steps: { run: { outcome: ["failed", "failed"] } }
      });

      expect(result.status).toBe(expected);
      expect(result.terminalStates).toEqual([{ stepId: "run", status: expected }]);
    }
  });

  test("preserves enclosing loop context through parallel branches", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: looped-parallel
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
    const result = simulateAgentFlowWorkflow(workflow, {
      steps: { repeat: { iterations: 2 } }
    });

    expect(result.status).toBe("completed");
    expect(result.visitedSteps.map((step) => step.id)).toEqual([
      "repeat", "split", "branch", "next_iteration", "split", "branch", "next_iteration", "finish"
    ]);
  });

  test("resets retry accounting for repeated successful invocations", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: repeated-retry
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
    const result = simulateAgentFlowWorkflow(workflow, {
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
    const workflow = parseAgentFlowWorkflowOrThrow(`name: nested-branch-contract
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
    const result = simulateAgentFlowWorkflow(workflow, { steps: { branch: {} } });

    expect(result.status).toBe("unresolved");
    expect(result.visitedSteps.map((step) => step.id)).toEqual(["split", "branch", "nested"]);
    expect(result.missingArtifacts).toEqual([
      { stepId: "branch", artifact: "missing.json", kind: "input" },
      { stepId: "branch", artifact: "branch.json", kind: "output" }
    ]);
  });

  test("retries parallel branches in place and resumes the parent sequence", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: parallel-retry
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
    const result = simulateAgentFlowWorkflow(workflow, {
      steps: { branch: { outcome: ["failed", "succeeded"], outputs: ["branch.json"] } }
    });

    expect(result.status).toBe("completed");
    expect(result.visitedSteps.map((step) => step.id)).toEqual(["split", "branch", "branch", "finish"]);
  });

  test("enforces the workflow recovery-cycle limit", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: bounded-cycle
version: 1
style: recovery_pipeline
maturity: draft
limits: { max_recovery_cycles: 2 }
steps:
  - { id: retry, type: condition, if: retry, then: retry, else: complete }
`);
    const result = simulateAgentFlowWorkflow(workflow, { inputs: { retry: true } });

    expect(result.status).toBe("paused");
    expect(result.visitedSteps).toHaveLength(3);
    expect(result.terminalStates).toEqual([{ stepId: "retry", status: "paused" }]);
  });

  test("counts implicit fallthrough edges in simulated recovery cycles", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: simulated-fallthrough-cycle
version: 1
style: recovery_pipeline
maturity: draft
limits: { max_recovery_cycles: 1 }
steps:
  - { id: start, type: command, command: echo start, then: third }
  - { id: second, type: command, command: echo second }
  - { id: third, type: command, command: echo third, then: second }
`);

    const result = simulateAgentFlowWorkflow(workflow, {});

    expect(result.status).toBe("paused");
    expect(result.visitedSteps.map((step) => step.id)).toEqual(["start", "third", "second", "third", "second"]);
    expect(result.terminalStates).toEqual([{ stepId: "third", status: "paused" }]);
  });

  test("applies per-step attempt limits to condition routes", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: simulation-step-attempts
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

    const result = simulateAgentFlowWorkflow(workflow, { inputs: { again: true } });

    expect(result.status).toBe("paused");
    expect(result.visitedSteps.map((step) => step.id)).toEqual(["work", "route"]);
    expect(result.terminalStates).toEqual([{ stepId: "work", status: "paused" }]);
  });

  test("applies fractional step-attempt limits before the first visit", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: simulation-initial-attempt-limit
version: 1
style: recovery_pipeline
maturity: experimental
limits:
  max_step_attempts: { work: 0.5 }
steps:
  - { id: work, type: command, command: echo work }
`);

    const result = simulateAgentFlowWorkflow(workflow, {});

    expect(result.status).toBe("paused");
    expect(result.visitedSteps).toEqual([]);
    expect(result.terminalStates).toEqual([{ stepId: "work", status: "paused" }]);
  });

  test("resolves fixture inputs used as artifact references", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: dynamic-artifact
version: 1
style: pipeline
maturity: draft
inputs:
  payload: { required: true }
steps:
  - { id: inspect, type: command, command: echo inspect, inputs: ["{{ inputs.payload }}"] }
`);
    const result = simulateAgentFlowWorkflow(workflow, {
      inputs: { payload: "missing.json" }
    });

    expect(result.status).toBe("unresolved");
    expect(result.missingArtifacts).toEqual([
      { stepId: "inspect", artifact: "missing.json", kind: "input" }
    ]);
  });

  test("gives declared step IDs precedence over terminal aliases", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: alias-step
version: 1
style: pipeline
maturity: draft
steps:
  - { id: route, type: condition, if: ready, then: complete, else: fail }
  - { id: complete, type: command, command: echo complete, outputs: [done.json] }
`);
    const result = simulateAgentFlowWorkflow(workflow, {
      inputs: { ready: true },
      steps: {
        complete: { outputs: ["done.json"] }
      }
    });

    expect(result.status).toBe("completed");
    expect(result.visitedSteps.map((step) => step.id)).toEqual(["route", "complete"]);
    expect(result.availableArtifacts).toEqual(["done.json"]);
  });

  test("does not index typeless parallel descriptors as route targets", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: simulated-descriptor-alias
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

    const result = simulateAgentFlowWorkflow(workflow, {});

    expect(result.status).toBe("completed");
    expect(result.visitedSteps.map((step) => step.id)).toEqual(["start"]);
  });

  test("counts parallel branch retries against the transition limit", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: bounded-parallel-retry
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
    const result = simulateAgentFlowWorkflow(workflow, {
      steps: { branch: { outcome: "failed" } }
    });

    expect(result.status).toBe("unresolved");
    expect(result.unresolvedBranches).toEqual([
      { stepId: "branch", reason: "Simulation exceeded its deterministic transition limit." }
    ]);
    expect(result.visitedSteps).toHaveLength(10000);
  });

  test("terminates rejected gates that have no explicit rejection target", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: rejected-gate
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
    const result = simulateAgentFlowWorkflow(workflow, {
      steps: {
        gate: { choice: "reject" },
        deploy: { outputs: ["deployed.json"] }
      }
    });

    expect(result.status).toBe("cancelled");
    expect(result.visitedSteps.map((step) => step.id)).toEqual(["gate"]);
    expect(result.terminalStates).toEqual([{ stepId: "gate", status: "cancelled" }]);
  });

  test("treats declared completion gate outcomes as terminal", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: completed-gate
version: 1
style: pipeline
maturity: draft
steps:
  - { id: gate, type: manual_gate, message: Finish?, options: [complete, cancel] }
  - { id: never, type: command, command: echo never }
`);
    const result = simulateAgentFlowWorkflow(workflow, {
      steps: { gate: { choice: "complete" } }
    });

    expect(result.status).toBe("completed");
    expect(result.visitedSteps.map((step) => step.id)).toEqual(["gate"]);
    expect(result.terminalStates).toEqual([{ stepId: "gate", status: "completed" }]);
  });

  test("routes the cancelled gate alias through on_cancel", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: cancelled-gate-handler
version: 1
style: pipeline
maturity: draft
steps:
  - { id: gate, type: manual_gate, message: Continue?, options: [approve, cancelled], on_cancel: cleanup }
  - { id: cleanup, type: command, command: echo cleanup }
`);
    const result = simulateAgentFlowWorkflow(workflow, {
      steps: { gate: { choice: "cancelled" } }
    });

    expect(result.status).toBe("completed");
    expect(result.visitedSteps.map((step) => step.id)).toEqual(["gate", "cleanup"]);
  });

  test("traverses both nested lists in parallel branch descriptors", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: both-branch-lists
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
    const result = simulateAgentFlowWorkflow(workflow, {
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
    const workflow = parseAgentFlowWorkflowOrThrow(`name: ambiguous-branches
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
    const result = simulateAgentFlowWorkflow(workflow, {
      steps: { shared: { outcome: "succeeded" } }
    });

    expect(result.status).toBe("unresolved");
    expect(result.visitedSteps).toEqual([]);
    expect(result.unresolvedBranches).toEqual([
      { stepId: "shared", reason: "Workflow step ID is ambiguous in simulation fixtures and targets." }
    ]);
  });

  test("rejects ambiguous success targets before traversal", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: ambiguous-simulation-success-target
version: 1
style: pipeline
maturity: draft
steps:
  - { id: start, type: command, command: echo start, then: second, goto: third }
  - { id: second, type: command, command: echo second }
  - { id: third, type: command, command: echo third }
`);

    const result = simulateAgentFlowWorkflow(workflow, {});

    expect(result.status).toBe("unresolved");
    expect(result.visitedSteps).toEqual([]);
    expect(result.unresolvedBranches).toEqual([
      {
        stepId: "start",
        reason: 'Step "start" cannot declare both then and goto success targets.'
      }
    ]);
  });

  test("honors goto targets in failure handlers", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: failure-goto
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
    const result = simulateAgentFlowWorkflow(workflow, {
      steps: { work: { outcome: "failed" } }
    });

    expect(result.status).toBe("completed");
    expect(result.visitedSteps.map((step) => step.id)).toEqual(["work", "recover"]);
  });

  test("collects every parallel branch diagnostic before returning a terminal result", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: parallel-diagnostics
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
    const result = simulateAgentFlowWorkflow(workflow, {
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
      const workflow = parseAgentFlowWorkflowOrThrow(`name: terminal-${target}
version: 1
style: recovery_pipeline
maturity: draft
steps:
  - id: fail
    type: command
    command: echo fail
    on_failure: { then: ${target} }
`);
      const result = simulateAgentFlowWorkflow(workflow, {
        steps: { fail: { outcome: "failed" } }
      });

      expect(result.status).toBe(status);
      expect(result.terminalStates).toEqual([{ stepId: "fail", status }]);
    }
  });
});
