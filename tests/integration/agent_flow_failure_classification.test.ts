import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  AGENT_FLOW_FAILURE_CLASSIFICATION_KINDS,
  AgentFlowFailureClassificationError,
  createAgentFlowLifecycleRun,
  executeAgentFlowCommandPipeline,
  openAgentFlowRunState,
  parseAgentFlowFailureClassification,
  parseAgentFlowWorkflowOrThrow,
  simulateAgentFlowWorkflow
} from "../../src/runtime";

describe("Agent Flow failure classifications", () => {
  test("publishes a stable schema and parses every known classification kind", () => {
    const schema = JSON.parse(
      fs.readFileSync(path.join(import.meta.dir, "../../schemas/failure-classification.schema.json"), "utf8")
    ) as {
      required: string[];
      additionalProperties: boolean;
      properties: { kind: { enum: string[] }; confidence: { enum: string[] } };
    };

    expect(schema.required).toEqual([
      "kind",
      "confidence",
      "summary",
      "recommended_owner",
      "safe_to_retry",
      "requires_user"
    ]);
    expect(schema.additionalProperties).toBe(false);
    expect(schema.properties.kind.enum).toEqual(AGENT_FLOW_FAILURE_CLASSIFICATION_KINDS);
    expect(schema.properties.confidence.enum).toEqual(["low", "medium", "high"]);

    for (const kind of AGENT_FLOW_FAILURE_CLASSIFICATION_KINDS) {
      expect(parseAgentFlowFailureClassification(classification(kind))).toEqual(classification(kind));
    }
  });

  test("rejects missing, mistyped, unsupported, and extra classification fields", () => {
    const invalid = [
      { ...classification("flake"), kind: "transient" },
      { ...classification("flake"), confidence: "certain" },
      { ...classification("flake"), summary: "" },
      { ...classification("flake"), recommended_owner: "" },
      { ...classification("flake"), safe_to_retry: "yes" },
      { ...classification("flake"), requires_user: 1 },
      { ...classification("flake"), extra: true },
      { kind: "flake" }
    ];

    for (const value of invalid) {
      expect(() => parseAgentFlowFailureClassification(value)).toThrow(AgentFlowFailureClassificationError);
    }
  });

  test("routes every actionable kind and pauses classifications whose default route is unsafe", async () => {
    const expectations = new Map([
      ["flake", "completed"],
      ["implementation_error", "completed"],
      ["formatting_error", "completed"],
      ["environment_error", "paused"],
      ["missing_requirement", "paused"],
      ["unsafe_change", "paused"],
      ["unknown", "paused"]
    ] as const);

    for (const kind of AGENT_FLOW_FAILURE_CLASSIFICATION_KINDS) {
      const root = temporaryRepo();
      const workflow = parseAgentFlowWorkflowOrThrow(`name: classify-${kind}
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - id: route
    type: condition
    branches:
      - { if: artifacts.ci.failure_classification.kind == "flake", then: complete }
      - { if: artifacts.ci.failure_classification.kind == "implementation_error", then: complete }
      - { if: artifacts.ci.failure_classification.kind == "formatting_error", then: complete }
      - { if: artifacts.ci.failure_classification.kind == "environment_error", then: pause }
      - { if: artifacts.ci.failure_classification.kind == "missing_requirement", then: pause }
      - { if: artifacts.ci.failure_classification.kind == "unsafe_change", then: pause }
      - { if: artifacts.ci.failure_classification.kind == "unknown", then: complete }
    else: complete
`);
      const store = await openAgentFlowRunState({ cwd: root });
      createAgentFlowLifecycleRun(store, { id: `classify-${kind}`, workflow });
      store.writeArtifact({
        id: `classification-${kind}`,
        runId: `classify-${kind}`,
        stepId: "classifier",
        path: "ci/failure-classification.json",
        kind: "session_output",
        contentType: "application/json; charset=utf-8",
        content: JSON.stringify(classification(kind))
      });

      const result = await executeAgentFlowCommandPipeline(store, `classify-${kind}`, workflow);

      expect(result.status).toBe(expectations.get(kind));
      if (kind === "unknown") {
        expect(result.message).toContain(
          "Agent Flow failure classification kind \"unknown\" cannot be routed automatically"
        );
      }
      store.close();
    }
  });

  test("validates classification branches before an earlier branch can route", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: preflight-classification
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - id: route
    type: condition
    branches:
      - { if: artifacts.control.route == "complete", then: complete }
      - { if: artifacts.failure_classification, then: complete }
    else: complete
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "preflight-classification", workflow });
    store.writeArtifact({
      id: "control",
      runId: "preflight-classification",
      stepId: "classifier",
      path: "control.json",
      kind: "session_output",
      contentType: "application/json",
      content: JSON.stringify({ route: "complete" })
    });
    store.writeArtifact({
      id: "classification",
      runId: "preflight-classification",
      stepId: "classifier",
      path: "failure-classification.json",
      kind: "session_output",
      contentType: "application/json",
      content: JSON.stringify(classification("unknown"))
    });
    store.writeArtifact({
      id: "classification-shadow",
      runId: "preflight-classification",
      stepId: "classifier",
      path: "failure-classification/kind.json",
      kind: "session_output",
      contentType: "application/json",
      content: JSON.stringify("flake")
    });

    const result = await executeAgentFlowCommandPipeline(store, "preflight-classification", workflow);

    expect(result).toMatchObject({ status: "paused", failedStep: "route", failureOutcome: "pause" });
    expect(store.listFailures("preflight-classification")[0]?.classification)
      .toBe("failure_classification_unknown");
    store.close();
  });

  test("pauses when a classification branch has no canonical artifact", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: missing-classification
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - id: route
    type: condition
    branches:
      - { if: artifacts.control.route == "complete", then: complete }
      - { if: artifacts.failure_classification.kind == "flake", then: complete }
    else: complete
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "missing-classification", workflow });
    for (const [id, declaredPath, content] of [
      ["control", "control.json", { route: "complete" }],
      ["classification-shadow", "failure-classification/kind.json", "flake"]
    ] as const) {
      store.writeArtifact({
        id,
        runId: "missing-classification",
        stepId: "classifier",
        path: declaredPath,
        kind: "session_output",
        contentType: "application/json",
        content: JSON.stringify(content)
      });
    }

    const result = await executeAgentFlowCommandPipeline(store, "missing-classification", workflow);

    expect(result).toMatchObject({ status: "paused", failedStep: "route", failureOutcome: "pause" });
    expect(store.listFailures("missing-classification")[0]?.classification)
      .toBe("failure_classification_invalid");
    store.close();
  });

  test("does not reserve ordinary JSON properties named failure_classification", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: ordinary-property
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - id: route
    type: condition
    branches:
      - { if: artifacts.control.route == "complete", then: complete }
      - { if: artifacts.report.failure_classification.kind == "flake", then: pause }
    else: pause
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "ordinary-property", workflow });
    store.writeArtifact({
      id: "control",
      runId: "ordinary-property",
      stepId: "reporter",
      path: "control.json",
      kind: "session_output",
      contentType: "application/json",
      content: JSON.stringify({ route: "complete" })
    });
    store.writeArtifact({
      id: "report",
      runId: "ordinary-property",
      stepId: "reporter",
      path: "report.json",
      kind: "session_output",
      contentType: "application/json",
      content: JSON.stringify({ failure_classification: { kind: "flake" } })
    });

    const result = await executeAgentFlowCommandPipeline(store, "ordinary-property", workflow);

    expect(result.status).toBe("completed");
    store.close();
  });

  test("rejects case-variant canonical classification aliases as ambiguous", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: ambiguous-classification
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - id: route
    type: condition
    if: artifacts.failure_classification.kind == "flake"
    then: complete
    else: complete
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "ambiguous-classification", workflow });
    for (const [id, declaredPath] of [
      ["lower", "failure-classification.json"],
      ["upper", "Failure-Classification.json"]
    ] as const) {
      store.writeArtifact({
        id,
        runId: "ambiguous-classification",
        stepId: "classifier",
        path: declaredPath,
        kind: "session_output",
        contentType: "application/json",
        content: JSON.stringify(classification("flake"))
      });
    }

    const result = await executeAgentFlowCommandPipeline(store, "ambiguous-classification", workflow);

    expect(result).toMatchObject({ status: "paused", failureOutcome: "pause" });
    expect(store.listFailures("ambiguous-classification")[0]?.classification)
      .toBe("failure_classification_invalid");
    store.close();
  });

  test("preflights mixed-case canonical filenames before an earlier branch routes", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: mixed-case-classification
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - id: route
    type: condition
    branches:
      - { if: artifacts.control.route == "complete", then: complete }
      - { if: artifacts.failure_classification.kind == "flake", then: complete }
    else: complete
`);

    const result = simulateAgentFlowWorkflow(workflow, {
      artifacts: {
        "control.json": { route: "complete" },
        "Failure-Classification.json": classification("flake")
      }
    });

    expect(result.status).toBe("completed");
    expect(result.visitedSteps).toEqual([{ id: "route", type: "condition", outcome: "selected" }]);
  });

  test("pauses oversized classifications with the stable invalid failure code", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: oversized-classification
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - id: route
    type: condition
    if: artifacts.failure_classification.kind == "flake"
    then: complete
    else: complete
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "oversized-classification", workflow });
    store.writeArtifact({
      id: "oversized-classification",
      runId: "oversized-classification",
      stepId: "classifier",
      path: "failure-classification.json",
      kind: "session_output",
      contentType: "application/json",
      content: JSON.stringify({ ...classification("flake"), summary: "x".repeat(10 * 1024 * 1024) })
    });

    const result = await executeAgentFlowCommandPipeline(store, "oversized-classification", workflow);

    expect(result).toMatchObject({ status: "paused", failureOutcome: "pause" });
    expect(store.listFailures("oversized-classification")[0]?.classification)
      .toBe("failure_classification_invalid");
    store.close();
  });

  test("pauses instead of routing a malformed classification through an else branch", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: malformed-classification
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - id: route
    type: condition
    if: artifacts.ci.failure_classification.kind == "flake"
    then: complete
    else: complete
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "malformed-classification", workflow });
    store.writeArtifact({
      id: "malformed-classification",
      runId: "malformed-classification",
      stepId: "classifier",
      path: "ci/failure-classification.json",
      kind: "session_output",
      contentType: "application/json",
      content: JSON.stringify({ kind: "flake", safe_to_retry: true })
    });

    const result = await executeAgentFlowCommandPipeline(store, "malformed-classification", workflow);

    expect(result).toMatchObject({
      status: "paused",
      failedStep: "route",
      failureOutcome: "pause",
      message: expect.stringContaining("failure classification")
    });
    expect(store.listFailures("malformed-classification")[0]?.classification)
      .toBe("failure_classification_invalid");
    store.close();
  });

  test("pauses when a classification artifact is not valid JSON", async () => {
    const root = temporaryRepo();
    const workflow = parseAgentFlowWorkflowOrThrow(`name: invalid-json-classification
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - id: route
    type: condition
    if: artifacts.failure_classification.kind == "flake"
    then: complete
    else: complete
`);
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "invalid-json-classification", workflow });
    store.writeArtifact({
      id: "invalid-json-classification",
      runId: "invalid-json-classification",
      stepId: "classifier",
      path: "failure-classification.json",
      kind: "session_output",
      contentType: "application/json",
      content: "{not-json"
    });

    const result = await executeAgentFlowCommandPipeline(store, "invalid-json-classification", workflow);

    expect(result).toMatchObject({ status: "paused", failureOutcome: "pause" });
    expect(store.listFailures("invalid-json-classification")[0]?.classification)
      .toBe("failure_classification_invalid");
    store.close();
  });

  test("preserves classification failure codes from recovery short-circuit conditions", async () => {
    for (const [kind, expected] of [
      ["unknown", "failure_classification_unknown"],
      ["invalid", "failure_classification_invalid"]
    ] as const) {
      const root = temporaryRepo();
      const workflow = parseAgentFlowWorkflowOrThrow(`name: short-circuit-${kind}
version: 1
style: recovery_pipeline
maturity: experimental
short_circuit_if:
  - artifacts.failure_classification.kind == "flake"
steps:
  - id: work
    type: command
    command: "true"
`);
      const store = await openAgentFlowRunState({ cwd: root });
      createAgentFlowLifecycleRun(store, { id: `short-circuit-${kind}`, workflow });
      store.writeArtifact({
        id: `short-circuit-${kind}`,
        runId: `short-circuit-${kind}`,
        stepId: "classifier",
        path: "failure-classification.json",
        kind: "session_output",
        contentType: "application/json",
        content: JSON.stringify(kind === "unknown" ? classification("unknown") : { kind: "flake" })
      });

      const result = await executeAgentFlowCommandPipeline(store, `short-circuit-${kind}`, workflow);

      expect(result).toMatchObject({ status: "paused", failureOutcome: "pause" });
      expect(store.listFailures(`short-circuit-${kind}`)[0]?.classification).toBe(expected);
      store.close();
    }
  });

  test("pauses simulation before selecting a route for invalid and unknown classifications", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: simulate-classification
version: 1
style: recovery_pipeline
maturity: experimental
steps:
  - id: route
    type: condition
    if: artifacts.failure_classification.kind == "flake"
    then: complete
    else: complete
`);

    for (const value of [classification("unknown"), { kind: "flake" }]) {
      const result = simulateAgentFlowWorkflow(workflow, {
        artifacts: { "failure-classification.json": value }
      });

      expect(result.status).toBe("paused");
      expect(result.visitedSteps).toEqual([{ id: "route", type: "condition", outcome: "failed" }]);
      expect(result.terminalStates).toEqual([{ stepId: "route", status: "paused" }]);
    }
  });
});

function classification(kind: string) {
  return {
    kind,
    confidence: "high",
    summary: `Classified as ${kind}.`,
    recommended_owner: kind === "implementation_error" ? "fm" : "workflow_owner",
    safe_to_retry: kind === "flake",
    requires_user: kind === "missing_requirement"
  };
}

function temporaryRepo(): string {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "agent-flow-classification-"));
  fs.mkdirSync(path.join(root, ".git"));
  return root;
}
