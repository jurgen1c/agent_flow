import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  parseAgentFlowSimulationFixture,
  parseAgentFlowWorkflowOrThrow,
  simulateAgentFlowWorkflow,
  validateAgentFlowWorkflow
} from "../../src/runtime";
import { runCli } from "../../src/cli/router";

const repositoryRoot = path.resolve(".");
const exampleNames = ["implement-review-collab", "content-review-collab"] as const;
const fixtureNames = ["approved", "changes-requested", "unresolved"] as const;

function loadWorkflow(example: typeof exampleNames[number]) {
  return parseAgentFlowWorkflowOrThrow(fs.readFileSync(
    path.join(repositoryRoot, "examples/workflows", `${example}.yml`),
    "utf8"
  ));
}

function loadFixture(example: typeof exampleNames[number], fixture: typeof fixtureNames[number]) {
  const parsed = parseAgentFlowSimulationFixture(fs.readFileSync(
    path.join(repositoryRoot, "examples/fixtures", example, `${fixture}.json`),
    "utf8"
  ));
  if (!parsed.ok) throw new Error(parsed.error);
  return parsed.fixture;
}

describe("collaborative workflow examples", () => {
  test("feeds a rejected content approval back to the copywriter", () => {
    const workflow = loadWorkflow("content-review-collab");

    expect(workflow.steps).toContainEqual(expect.objectContaining({
      id: "approve_content",
      on_reject: "revise_rejected_copy"
    }));
    expect(workflow.steps).toContainEqual(expect.objectContaining({
      id: "revise_rejected_copy",
      inputs: expect.arrayContaining(["approvals/approve_content.json"]),
      then: "product_review"
    }));
  });

  test("validates and simulates approved, changes-requested, unresolved, and human-escalation paths", () => {
    for (const example of exampleNames) {
      const workflow = loadWorkflow(example);
      expect(validateAgentFlowWorkflow(workflow), example).toEqual({ valid: true, errors: [] });

      for (const fixture of fixtureNames) {
        const result = simulateAgentFlowWorkflow(workflow, loadFixture(example, fixture));
        expect(result.status, `${example}/${fixture}`).toBe(fixture === "approved" ? "completed" : "paused");
        expect(result.missingArtifacts, `${example}/${fixture}`).toEqual([]);

        if (fixture === "approved") {
          expect(result.availableArtifacts, example).toContain("decision-records/record_approval.json");
        } else {
          expect(result.visitedSteps, `${example}/${fixture}`).toContainEqual(
            expect.objectContaining({ type: fixture === "unresolved" ? "input_request" : "disagreement" })
          );
        }
      }
    }
  });

  test("runs every fixture offline through the CLI fixture provider", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "agent-flow-collaborative-examples-"));
    fs.mkdirSync(path.join(root, ".git"));
    fs.cpSync(path.join(repositoryRoot, "examples"), path.join(root, "examples"), { recursive: true });

    for (const example of exampleNames) {
      for (const fixture of fixtureNames) {
        const result = await captureCli([
          "run",
          `examples/workflows/${example}.yml`,
          "--id",
          `${example}-${fixture}`,
          "--fixture",
          `examples/fixtures/${example}/${fixture}.json`
        ], root);
        expect(result.exitCode, `${example}/${fixture}: ${result.stderr}`).toBe(fixture === "approved" ? 0 : 3);
        expect(result.stdout, `${example}/${fixture}`).toContain(
          `Status: ${fixture === "approved" ? "completed" : "paused"}`
        );
      }
    }

    for (const example of exampleNames) {
      const result = await captureCli([
        "resume",
        `${example}-changes-requested`,
        "--outcome",
        "approve"
      ], root);
      expect(result.exitCode, `${example}/resume: ${result.stderr}`).toBe(0);
      expect(result.stdout, `${example}/resume`).toContain("Status: completed");
      expect((await captureCli(["artifacts", `${example}-changes-requested`], root)).stdout)
        .toContain("decision-records/record_approval.json");
    }

    expect((await captureCli(["artifacts", "implement-review-collab-approved"], root)).stdout)
      .toContain("decision-records/record_approval.json");
    expect((await captureCli(["status", "content-review-collab-unresolved"], root)).stdout)
      .toContain("Answer artifact: user-input/content-decision.md");
  });
});

async function captureCli(args: string[], cwd: string): Promise<{ exitCode: number; stdout: string; stderr: string }> {
  let stdout = "";
  let stderr = "";
  const exitCode = await runCli(args, {
    stdout: { write: (chunk: string) => { stdout += chunk; return true; } },
    stderr: { write: (chunk: string) => { stderr += chunk; return true; } }
  }, { cwd });
  return { exitCode, stdout, stderr };
}
