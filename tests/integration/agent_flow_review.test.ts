import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  createAgentFlowLifecycleRun,
  createAgentFlowSessionProviderRegistry,
  executeAgentFlowCommandPipeline,
  MAX_AGENT_FLOW_SESSION_PROMPT_BYTES,
  openAgentFlowRunState,
  parseAgentFlowWorkflowOrThrow,
  simulateAgentFlowWorkflow,
  validateAgentFlowWorkflow,
  type AgentFlowRunStateValue,
  type AgentFlowSessionProviderRequest
} from "../../src/runtime";

describe("Agent Flow collaborative review steps", () => {
  test("executes a reviewer session, persists structured findings, and routes by status", async () => {
    const root = temporaryRepo();
    const workflow = reviewWorkflow();
    expect(validateAgentFlowWorkflow(workflow)).toEqual({ valid: true, errors: [] });
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "approved-review", workflow });
    store.writeArtifact({
      id: "implementation",
      runId: "approved-review",
      path: "implementation.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Implemented and tested."
    });
    const requests: AgentFlowSessionProviderRequest[] = [];
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => {
      requests.push(request);
      return {
        outputs: {
          "reviews/code-review.json": JSON.stringify({
            status: "approved",
            summary: "No actionable findings.",
            findings: []
          })
        }
      };
    });

    const result = await executeAgentFlowCommandPipeline(
      store,
      "approved-review",
      workflow,
      undefined,
      providers
    );

    expect(result).toMatchObject({
      status: "completed",
      completedSteps: ["review", "route", "approved"]
    });
    expect(requests[0]).toMatchObject({
      stepId: "review",
      sessionId: "reviewer",
      inputs: [expect.objectContaining({ path: "implementation.md" })],
      outputs: ["reviews/code-review.json"]
    });
    expect(requests[0]!.prompt.content).toContain('one of "approved", "changes_requested", or "unresolved"');
    expect(JSON.parse(store.readArtifact("approved-review", "reviews/code-review.json").content.toString()))
      .toEqual({ status: "approved", summary: "No actionable findings.", findings: [] });
    expect(store.getArtifact("approved-review", "reviews/code-review.json")?.kind).toBe("review_output");
    expect(store.listArtifacts("approved-review").some((artifact) => artifact.kind === "review_request")).toBe(true);
    store.close();
  });

  test("fails closed without publishing malformed review findings", async () => {
    const root = temporaryRepo();
    const workflow = reviewWorkflow();
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "invalid-review", workflow });
    store.writeArtifact({
      id: "implementation",
      runId: "invalid-review",
      path: "implementation.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Implementation"
    });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => ({
      outputs: { "reviews/code-review.json": JSON.stringify({ status: "approved" }) }
    }));

    const result = await executeAgentFlowCommandPipeline(
      store,
      "invalid-review",
      workflow,
      undefined,
      providers
    );

    expect(result).toMatchObject({ status: "paused", failedStep: "review" });
    expect(result.message).toContain("findings must be an array");
    expect(store.getArtifact("invalid-review", "reviews/code-review.json")).toBeNull();
    store.close();
  });

  test("simulates all formal review statuses and rejects malformed fixtures", () => {
    for (const [status, expected] of [
      ["approved", "completed"],
      ["changes_requested", "failed"],
      ["unresolved", "paused"]
    ] as const) {
      const result = simulateAgentFlowWorkflow(reviewWorkflow(), {
        artifacts: { "implementation.md": "Implementation" },
        steps: {
          review: {
            outputs: { "reviews/code-review.json": { status, findings: [] } }
          }
        }
      });
      expect(result.status).toBe(expected);
    }

    const malformed = simulateAgentFlowWorkflow(reviewWorkflow(), {
      artifacts: { "implementation.md": "Implementation" },
      steps: {
        review: {
          outputs: { "reviews/code-review.json": { status: "approved" } }
        }
      }
    });
    expect(malformed.status).toBe("paused");
    expect(malformed.availableArtifacts).not.toContain("reviews/code-review.json");

    const serialized = simulateAgentFlowWorkflow(reviewWorkflow(), {
      artifacts: { "implementation.md": "Implementation" },
      steps: {
        review: {
          outputs: { "reviews/code-review.json": '{"status":"approved","findings":[]}' }
        }
      }
    });
    expect(serialized.status).toBe("completed");
  });

  test("rejects unauthorized reviewers and unsupported review failure policies before execution", () => {
    const unauthorized = reviewWorkflow();
    unauthorized.sessions!.reviewer = { provider: "fixture", role: "reviewer" };
    expect(validateAgentFlowWorkflow(unauthorized).errors.map((issue) => issue.code)).toEqual([
      "workflow.collaboration.authority.can_request_changes.required",
      "workflow.collaboration.authority.can_approve.required"
    ]);

    const unsupportedFailure = reviewWorkflow();
    unsupportedFailure.steps[0]!.on_failure = { retry: 1, then: "pause" };
    expect(validateAgentFlowWorkflow(unsupportedFailure).errors).toContainEqual(expect.objectContaining({
      code: "workflow.review.on_failure.unsupported",
      path: "steps[0].on_failure"
    }));
  });

  test("fails runtime preflight for non-mapping review failure policies in persisted workflows", async () => {
    const root = temporaryRepo();
    const parsed = reviewWorkflow();
    const workflow = {
      ...parsed,
      steps: [{ ...parsed.steps[0]!, on_failure: "pause" }, ...parsed.steps.slice(1)]
    } as unknown as typeof parsed;
    const store = await openAgentFlowRunState({ cwd: root });
    store.createRunWithEvent({
      id: "persisted-review-policy",
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

    const result = await executeAgentFlowCommandPipeline(
      store,
      "persisted-review-policy",
      workflow,
      undefined,
      providers
    );

    expect(result).toMatchObject({ status: "failed", failedStep: "review" });
    expect(result.message).toContain("do not support on_failure policies");
    expect(called).toBe(false);
    store.close();
  });

  test("rejects generated review prompts above the session prompt limit", async () => {
    const root = temporaryRepo();
    const workflow = reviewWorkflow();
    const oversizedSubject = "x".repeat(MAX_AGENT_FLOW_SESSION_PROMPT_BYTES + 1);
    workflow.sessions![oversizedSubject] = workflow.sessions!.implementer!;
    workflow.steps[0]!.subject = oversizedSubject;
    const store = await openAgentFlowRunState({ cwd: root });
    createAgentFlowLifecycleRun(store, { id: "oversized-review-prompt", workflow });
    store.writeArtifact({
      id: "implementation",
      runId: "oversized-review-prompt",
      path: "implementation.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Implementation"
    });
    let called = false;
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", () => {
      called = true;
      return { outputs: {} };
    });

    const result = await executeAgentFlowCommandPipeline(
      store,
      "oversized-review-prompt",
      workflow,
      undefined,
      providers
    );

    expect(result).toMatchObject({ status: "paused", failedStep: "review" });
    expect(result.message).toContain("session prompt limit");
    expect(called).toBe(false);
    store.close();
  });
});

function reviewWorkflow() {
  return parseAgentFlowWorkflowOrThrow(`name: formal-review
version: 1
style: collaborative
maturity: experimental
collaboration: { enabled: true }
sessions:
  implementer: { provider: fixture, role: implementer }
  reviewer:
    provider: fixture
    role: reviewer
    authority: { can_request_changes: true, can_approve: true }
steps:
  - id: review
    type: review
    reviewer: reviewer
    subject: implementer
    artifacts: [implementation.md]
    outputs: [reviews/code-review.json]
  - id: route
    type: condition
    branches:
      - { if: 'artifacts.reviews.code_review.status == "approved"', then: approved }
      - { if: 'artifacts.reviews.code_review.status == "changes_requested"', then: changes }
    else: unresolved
  - { id: approved, type: result, status: completed }
  - { id: changes, type: result, status: failed }
  - { id: unresolved, type: result, status: paused }
`);
}

function temporaryRepo(): string {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "agent-flow-review-"));
  fs.mkdirSync(path.join(root, ".git"));
  return root;
}
