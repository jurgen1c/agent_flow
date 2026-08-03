import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import path from "node:path";
import {
  parseAgentFlowSimulationFixture,
  parseAgentFlowWorkflowOrThrow,
  simulateAgentFlowWorkflow,
  validateAgentFlowWorkflow
} from "../../src/runtime";
import { dispatch } from "../../src/cli/router";

const repositoryRoot = path.resolve(".");
const workflowPath = path.join(repositoryRoot, "examples/workflows/pr-feedback-loop.yml");
const fixtureRoot = path.join(repositoryRoot, "examples/fixtures/pr-feedback-loop");

function loadWorkflow() {
  return parseAgentFlowWorkflowOrThrow(fs.readFileSync(workflowPath, "utf8"));
}

function loadFixture(name: string) {
  const parsed = parseAgentFlowSimulationFixture(fs.readFileSync(path.join(fixtureRoot, `${name}.json`), "utf8"));
  if (!parsed.ok) throw new Error(parsed.error);
  return parsed.fixture;
}

describe("PR feedback recovery loop example", () => {
  test("validates and simulates completed feedback remediation without network access", () => {
    const workflow = loadWorkflow();
    expect(validateAgentFlowWorkflow(workflow)).toEqual({ valid: true, errors: [] });

    const completed = simulateAgentFlowWorkflow(workflow, loadFixture("completed"));
    expect(completed.status).toBe("completed");
    expect(completed.visitedSteps.map((step) => step.id)).toEqual([
      "wait_for_review", "collect_pr_state", "classify_comments", "route_comments", "continue_loop", "return_complete"
    ]);

    const remediated = simulateAgentFlowWorkflow(workflow, loadFixture("remediated"));
    expect(remediated.status).toBe("completed");
    expect(remediated.visitedSteps.map((step) => step.id)).toEqual([
      "wait_for_review", "collect_pr_state", "classify_comments", "route_comments", "resolve_comments",
      "rerun_ci", "push_fixes", "continue_loop", "return_complete"
    ]);
    expect(remediated.artifactValues["implementation-summary.md"])
      .toBe("Fixture FM resolved the actionable review comment.\n");
  });

  test("routes failed CI through the declared triage recovery and pauses when unresolved", () => {
    const workflow = loadWorkflow();
    const loop = workflow.steps[0];
    const rerun = Array.isArray(loop?.body)
      ? loop.body.find((step) => typeof step === "object" && step !== null && step.id === "rerun_ci")
      : undefined;
    expect(rerun).toMatchObject({
      on_failure: {
        route_to: { workflow: "ci-triage" },
        on_remediated: { return_to: "rerun_ci" },
        on_unresolved: { then: "pause" }
      }
    });

    const recovered = simulateAgentFlowWorkflow(workflow, loadFixture("ci-recovered"));
    expect(recovered.status).toBe("completed");
    expect(recovered.visitedSteps.filter((step) => step.id === "rerun_ci").map((step) => step.outcome))
      .toEqual(["failed", "succeeded"]);
    expect(recovered.visitedSteps.map((step) => step.id)).toContain("push_fixes");

    const paused = simulateAgentFlowWorkflow(workflow, loadFixture("ci-paused"));
    expect(paused.status).toBe("paused");
    expect(paused.visitedSteps.map((step) => step.id)).not.toContain("push_fixes");
  });

  test("short circuits on high risk and frontier budget exhaustion", () => {
    const workflow = loadWorkflow();
    const highRisk = simulateAgentFlowWorkflow(workflow, loadFixture("high-risk"));
    expect(highRisk).toMatchObject({
      status: "paused",
      visitedSteps: [],
      terminalStates: [{ stepId: "wait_for_review", status: "paused" }]
    });

    const exhausted = simulateAgentFlowWorkflow(workflow, loadFixture("budget-exhausted"));
    expect(exhausted.status).toBe("paused");
    expect(exhausted.visitedSteps.filter((step) => step.id === "resolve_comments")).toHaveLength(6);
    expect(exhausted.terminalStates.at(-1)).toEqual({ stepId: "wait_for_review", status: "paused" });
  });

  test("reports max-iteration and max-duration exhaustion as deterministic timeouts", () => {
    const timeout = simulateAgentFlowWorkflow(loadWorkflow(), loadFixture("timeout"));
    expect(timeout.status).toBe("timed_out");
    expect(timeout.visitedSteps.filter((step) => step.id === "collect_pr_state")).toHaveLength(24);
    expect(timeout.terminalStates.at(-1)).toEqual({ stepId: "wait_for_review", status: "max_iterations" });
    expect(timeout.visitedSteps.map((step) => step.id)).not.toContain("return_complete");

    const durationTimeout = simulateAgentFlowWorkflow(loadWorkflow(), loadFixture("duration-timeout"));
    expect(durationTimeout.status).toBe("timed_out");
    expect(durationTimeout.terminalStates.at(-1)).toEqual({ stepId: "wait_for_review", status: "max_duration" });

    const cli = dispatch([
      "simulate",
      "examples/workflows/pr-feedback-loop.yml",
      "--fixture",
      "examples/fixtures/pr-feedback-loop/timeout.json"
    ]);
    expect(cli.exitCode).toBe(0);
    expect(cli.stdout).toContain("Status: timed_out");
  });

  test("rejects loop termination fixtures that do not match a declared bound", () => {
    const workflow = loadWorkflow();
    const loop = workflow.steps[0];
    if (loop === undefined) throw new Error("Expected PR feedback loop step.");

    const withoutDuration = {
      ...workflow,
      steps: [{ ...loop, max_duration_seconds: undefined }, ...workflow.steps.slice(1)]
    };
    const result = simulateAgentFlowWorkflow(withoutDuration, loadFixture("duration-timeout"));
    expect(result.status).toBe("unresolved");
    expect(result.unresolvedBranches).toEqual([{
      stepId: "wait_for_review",
      reason: "Fixture selects max_duration termination for a loop without a duration bound."
    }]);
  });
});
