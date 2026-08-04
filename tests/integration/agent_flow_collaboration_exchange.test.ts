import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  AgentFlowCollaborationError,
  createAgentFlowLifecycleRun,
  createAgentFlowSessionProviderRegistry,
  executeAgentFlowCommandPipeline,
  openAgentFlowRunState,
  parseAgentFlowChallengeResult,
  parseAgentFlowConsultResult,
  parseAgentFlowWorkflowOrThrow,
  simulateAgentFlowWorkflow,
  validateAgentFlowWorkflow,
  type AgentFlowRunStateValue,
  type AgentFlowSessionProviderRequest
} from "../../src/runtime";

describe("Agent Flow consult and challenge steps", () => {
  test("executes bounded exchanges and persists their structured results", async () => {
    const root = temporaryRepo();
    const workflow = exchangeWorkflow();
    expect(validateAgentFlowWorkflow(workflow)).toEqual({ valid: true, errors: [] });
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "collaboration-exchange", workflow });
    store.writeArtifact({
      id: "implementation",
      runId: "collaboration-exchange",
      path: "implementation.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Implemented with a dedicated exporter."
    });
    const requests: AgentFlowSessionProviderRequest[] = [];
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => {
      requests.push(request);
      if (request.stepId === "consult") {
        return {
          outputs: {
            "consultations/design.json": JSON.stringify({
              status: "advice",
              blocking: false,
              summary: "The failure state needs a retry action.",
              recommendations: [{ priority: "medium", recommendation: "Add retry guidance." }]
            })
          }
        };
      }
      return {
        outputs: {
          "challenges/exporter.json": JSON.stringify({
            status: "answered",
            rationale: "A separate exporter owns the new wire format.",
            evidence: ["implementation.md"]
          })
        }
      };
    });

    const result = await executeAgentFlowCommandPipeline(
      store,
      "collaboration-exchange",
      workflow,
      undefined,
      providers
    );

    expect(result).toMatchObject({ status: "completed", completedSteps: ["consult", "challenge"] });
    expect(requests).toHaveLength(2);
    expect(requests[0]).toMatchObject({
      sessionId: "designer",
      inputs: [expect.objectContaining({ path: "implementation.md" })],
      outputs: ["consultations/design.json"]
    });
    expect(requests[0]!.prompt.content).toContain("single bounded question");
    expect(requests[1]).toMatchObject({
      sessionId: "implementer",
      inputs: [expect.objectContaining({ path: "implementation.md" })],
      outputs: ["challenges/exporter.json"]
    });
    expect(store.getArtifact("collaboration-exchange", "consultations/design.json")?.kind).toBe("consult_output");
    expect(store.getArtifact("collaboration-exchange", "challenges/exporter.json")?.kind).toBe("challenge_output");
    expect(store.listArtifacts("collaboration-exchange").map((artifact) => artifact.kind)).toContain("consult_request");
    expect(store.listArtifacts("collaboration-exchange").map((artifact) => artifact.kind)).toContain("challenge_request");
    store.close();
  });

  test("fails closed when advisory consultation output attempts to block", async () => {
    const root = temporaryRepo();
    const workflow = exchangeWorkflow();
    workflow.steps = [workflow.steps[0]!];
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "invalid-consult", workflow });
    store.writeArtifact({
      id: "implementation",
      runId: "invalid-consult",
      path: "implementation.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Implementation"
    });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => ({
      outputs: {
        "consultations/design.json": JSON.stringify({
          status: "blocked",
          blocking: true,
          summary: "Stop.",
          recommendations: []
        })
      }
    }));

    const result = await executeAgentFlowCommandPipeline(store, "invalid-consult", workflow, undefined, providers);

    expect(result).toMatchObject({ status: "paused", failedStep: "consult" });
    expect(result.message).toContain("cannot block because the consult step is advisory");
    expect(store.getArtifact("invalid-consult", "consultations/design.json")).toBeNull();
    store.close();
  });

  test("stops runtime and simulation when an authorized blocking consult blocks", async () => {
    const root = temporaryRepo();
    const workflow = exchangeWorkflow();
    workflow.steps[0]!.blocking = true;
    workflow.sessions!.designer!.authority = { can_block: true };
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "blocked-consult", workflow });
    store.writeArtifact({
      id: "implementation",
      runId: "blocked-consult",
      path: "implementation.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Implementation"
    });
    const requests: string[] = [];
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => {
      requests.push(request.stepId);
      return {
        outputs: {
          "consultations/design.json": JSON.stringify({
            status: "blocked",
            blocking: true,
            summary: "The failure path is unsafe.",
            recommendations: [{ recommendation: "Add an explicit recovery route.", priority: "high" }]
          })
        }
      };
    });

    const result = await executeAgentFlowCommandPipeline(store, "blocked-consult", workflow, undefined, providers);

    expect(result).toMatchObject({
      status: "paused",
      completedSteps: ["consult"],
      resultStatus: "blocked"
    });
    expect(requests).toEqual(["consult"]);
    expect(store.getArtifact("blocked-consult", "consultations/design.json")?.kind).toBe("consult_output");
    store.close();

    const simulated = simulateAgentFlowWorkflow(workflow, {
      artifacts: { "implementation.md": "Implementation" },
      steps: {
        consult: {
          outputs: {
            "consultations/design.json": {
              status: "blocked",
              blocking: true,
              summary: "The failure path is unsafe.",
              recommendations: []
            }
          }
        }
      }
    });
    expect(simulated.status).toBe("paused");
    expect(simulated.visitedSteps.map((visit) => visit.id)).toEqual(["consult"]);
    expect(simulated.availableArtifacts).toContain("consultations/design.json");
  });

  test("simulates structured outputs and rejects malformed rationale", () => {
    const workflow = exchangeWorkflow();
    const valid = simulateAgentFlowWorkflow(workflow, {
      artifacts: { "implementation.md": "Implementation" },
      steps: {
        consult: {
          outputs: {
            "consultations/design.json": {
              status: "advice",
              blocking: false,
              summary: "Looks sound.",
              recommendations: []
            }
          }
        },
        challenge: {
          outputs: {
            "challenges/exporter.json": { status: "answered", rationale: "It owns the format." }
          }
        }
      }
    });
    expect(valid.status).toBe("completed");

    const malformed = simulateAgentFlowWorkflow(workflow, {
      artifacts: { "implementation.md": "Implementation" },
      steps: {
        consult: {
          outputs: {
            "consultations/design.json": {
              status: "advice",
              blocking: false,
              summary: "Looks sound.",
              recommendations: []
            }
          }
        },
        challenge: { outputs: { "challenges/exporter.json": { status: "answered" } } }
      }
    });
    expect(malformed.status).toBe("paused");
    expect(malformed.availableArtifacts).not.toContain("challenges/exporter.json");
  });

  test("fails closed when public collaboration parsers receive null", () => {
    expect(() => parseAgentFlowConsultResult(null as never)).toThrow(AgentFlowCollaborationError);
    expect(() => parseAgentFlowChallengeResult(null as never)).toThrow(AgentFlowCollaborationError);
  });

  test("rejects unauthorized blocking consultation during simulation", () => {
    const workflow = exchangeWorkflow();
    workflow.steps = [workflow.steps[0]!];
    workflow.steps[0]!.blocking = true;

    const result = simulateAgentFlowWorkflow(workflow, {
      artifacts: { "implementation.md": "Implementation" },
      steps: {
        consult: {
          outputs: {
            "consultations/design.json": {
              status: "blocked",
              blocking: true,
              summary: "Stop.",
              recommendations: []
            }
          }
        }
      }
    });

    expect(result.status).toBe("paused");
    expect(result.visitedSteps).toContainEqual(expect.objectContaining({ id: "consult", outcome: "failed" }));
    expect(result.availableArtifacts).not.toContain("consultations/design.json");
  });

  test("fails closed when a simulated exchange has no declared output", () => {
    const workflow = exchangeWorkflow();
    workflow.steps = [workflow.steps[0]!];
    delete workflow.steps[0]!.output;

    const result = simulateAgentFlowWorkflow(workflow, {
      artifacts: { "implementation.md": "Implementation" },
      steps: { consult: {} }
    });

    expect(result.status).toBe("paused");
    expect(result.visitedSteps).toContainEqual(expect.objectContaining({ id: "consult", outcome: "failed" }));
  });

  test("rejects vague, unbounded, and unauthorized consult contracts", () => {
    const workflow = exchangeWorkflow();
    workflow.steps[0]!.question = "Thoughts?";
    workflow.steps[0]!.blocking = true;
    expect(validateAgentFlowWorkflow(workflow).errors).toEqual(expect.arrayContaining([
      expect.objectContaining({ code: "workflow.collaboration.authority.can_block.required" }),
      expect.objectContaining({ code: "workflow.consult.question.vague" })
    ]));

    workflow.steps[0]!.question = "Does this\rhandle errors?";
    expect(validateAgentFlowWorkflow(workflow).errors).toContainEqual(expect.objectContaining({
      code: "workflow.consult.question.vague"
    }));
  });

  test("rejects non-static or non-normalized exchange artifact paths", () => {
    const workflow = exchangeWorkflow();
    workflow.steps[0]!.output = " {{ inputs.output }}.json ";
    workflow.steps[0]!.artifacts = [" implementation.md "];

    expect(validateAgentFlowWorkflow(workflow).errors).toEqual(expect.arrayContaining([
      expect.objectContaining({ code: "workflow.consult.output.invalid", path: "steps[0].output" }),
      expect.objectContaining({ code: "workflow.consult.artifact.invalid", path: "steps[0].artifacts[0]" })
    ]));

    workflow.steps[0]!.output = "./consultations/design.json";
    expect(validateAgentFlowWorkflow(workflow).errors).toContainEqual(expect.objectContaining({
      code: "workflow.consult.output.invalid",
      path: "steps[0].output"
    }));
  });

  test("fails runtime preflight for malformed persisted exchange contracts", async () => {
    const cases = [
      {
        id: "undeclared-source",
        mutate: (workflow: ReturnType<typeof exchangeWorkflow>) => { workflow.steps[0]!.from = "missing"; },
        message: "undeclared source session missing"
      },
      {
        id: "unauthorized-advisor",
        mutate: (workflow: ReturnType<typeof exchangeWorkflow>) => {
          workflow.sessions!.designer!.authority = { can_advise: false };
        },
        message: "effective can_advise authority"
      },
      {
        id: "unbounded-question",
        mutate: (workflow: ReturnType<typeof exchangeWorkflow>) => { workflow.steps[0]!.question = "Thoughts?"; },
        message: "one static, specific question"
      },
      {
        id: "carriage-return-question",
        mutate: (workflow: ReturnType<typeof exchangeWorkflow>) => {
          workflow.steps[0]!.question = "Does this\rhandle errors?";
        },
        message: "one static, specific question"
      },
      {
        id: "non-normalized-output",
        mutate: (workflow: ReturnType<typeof exchangeWorkflow>) => {
          workflow.steps[0]!.output = "./consultations/design.json";
        },
        message: "normalized static .json artifact path"
      },
      {
        id: "dynamic-output",
        mutate: (workflow: ReturnType<typeof exchangeWorkflow>) => {
          workflow.steps[0]!.output = "{{ inputs.output }}.json";
        },
        message: "normalized static .json artifact path"
      },
      {
        id: "padded-artifact",
        mutate: (workflow: ReturnType<typeof exchangeWorkflow>) => {
          workflow.steps[0]!.artifacts = [" implementation.md "];
        },
        message: "normalized static artifact paths"
      }
    ];

    for (const scenario of cases) {
      const root = temporaryRepo();
      const workflow = exchangeWorkflow();
      workflow.steps = [workflow.steps[0]!];
      scenario.mutate(workflow);
      const store = await openAgentFlowRunState({ cwd: root });
      store.createRunWithEvent({
        id: scenario.id,
        workflow: {
          name: workflow.name,
          version: workflow.version,
          style: workflow.style,
          maturity: workflow.maturity
        },
        context: { workflow: workflow as unknown as AgentFlowRunStateValue }
      }, { type: "run.created", payload: { status: "pending" } });
      let called = false;
      const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
        called = true;
        return { outputs: {} };
      });

      const result = await executeAgentFlowCommandPipeline(store, scenario.id, workflow, undefined, providers);

      expect(result).toMatchObject({ status: "failed", failedStep: "consult" });
      expect(result.message).toContain(scenario.message);
      expect(called).toBe(false);
      store.close();
    }
  });
});

function exchangeWorkflow() {
  return parseAgentFlowWorkflowOrThrow(`name: collaboration-exchange
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true }
sessions:
  implementer: { provider: fixture, role: implementer }
  designer: { provider: fixture, role: designer }
  reviewer: { provider: fixture, role: reviewer }
steps:
  - id: consult
    type: consult
    from: implementer
    to: designer
    question: Does the implementation handle empty and failure states?
    artifacts: [implementation.md]
    output: consultations/design.json
    blocking: false
  - id: challenge
    type: challenge
    from: reviewer
    to: implementer
    question: Why does this implementation need a separate exporter service?
    artifacts: [implementation.md]
    output: challenges/exporter.json
`);
}

function temporaryRepo(): string {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "agent-flow-collaboration-"));
  fs.mkdirSync(path.join(root, ".git"));
  return root;
}
