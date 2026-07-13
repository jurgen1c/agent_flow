import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import path from "node:path";
import {
  formatWorkflowParseIssues,
  parseAgentflowWorkflow,
  parseAgentflowWorkflowOrThrow
} from "../../packages/agentflow-core/src/index";

const repoRoot = path.resolve(".");
const fixtureRoot = path.join(repoRoot, "tests/fixtures/agentflow/workflows");

describe("Agentflow workflow parser", () => {
  test("parses committed workflow examples into stable typed structures", () => {
    const expectations = [
      { file: "workflow-skeleton.yml", name: "example-workflow", style: "pipeline", maturity: "draft", firstStep: undefined },
      { file: "simple-ci.yml", name: "simple-ci", style: "pipeline", maturity: "draft", firstStep: "install" },
      {
        file: "pr-feedback-loop.yml",
        name: "pr-feedback-loop",
        style: "recovery_pipeline",
        maturity: "experimental",
        firstStep: "wait_for_review"
      },
      {
        file: "content-review-collab.yml",
        name: "content-review-collab",
        style: "collaborative",
        maturity: "draft",
        firstStep: "draft_copy"
      }
    ] as const;

    for (const expectation of expectations) {
      const workflow = parseAgentflowWorkflowOrThrow(readFixture(expectation.file));

      expect(workflow.name).toBe(expectation.name);
      expect(workflow.version).toBe(1);
      expect(workflow.style).toBe(expectation.style);
      expect(workflow.maturity).toBe(expectation.maturity);
      expect(workflow.steps[0]?.id).toBe(expectation.firstStep);

      if (expectation.firstStep !== undefined) {
        expect(workflow.steps[0]?.type).toBeString();
      }

      expect(JSON.parse(JSON.stringify(workflow))).toEqual(workflow);
    }
  });

  test("preserves nested workflow data without executing step content", () => {
    const workflow = parseAgentflowWorkflowOrThrow(readFixture("pr-feedback-loop.yml"));
    const loopStep = workflow.steps[0];

    expect(loopStep?.type).toBe("loop");
    expect(loopStep?.until).toBe("pr.checks_passed == true && pr.actionable_comments_count == 0");
    expect(loopStep?.body).toBeArray();

    const body = loopStep?.body as Array<Record<string, unknown>>;
    expect(body[0]).toMatchObject({
      id: "collect_pr_state",
      type: "command",
      command: "gh pr view {{ inputs.pr_url }} --json reviews,comments,statusCheckRollup"
    });
    expect(body[4]?.on_failure).toEqual({
      route_to: {
        workflow: "ci-triage",
        inputs: {
          failure_payload: "{{ failure.path }}",
          failed_step: "rerun_ci"
        }
      },
      on_remediated: {
        return_to: "rerun_ci"
      },
      on_unresolved: {
        then: "pause"
      }
    });
  });

  test("parses inline arrays used by notification and approval examples", () => {
    const workflow = parseAgentflowWorkflowOrThrow(readFixture("simple-ci.yml"));

    expect(workflow.notify).toEqual([
      { on: "workflow.completed", channels: ["terminal"] },
      { on: "workflow.failed", channels: ["terminal", "system"] }
    ]);
  });

  test("keeps URL-like and colon-bearing list items as scalar data", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: scalar-list
version: 1
style: pipeline
maturity: draft
steps:
  - id: fetch
    type: command
    inputs:
      - https://example.com/a
      - token:read
`);

    expect(workflow.steps[0]?.inputs).toEqual(["https://example.com/a", "token:read"]);
  });

  test("preserves blank and comment-looking lines inside block scalar values", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: block-scalar
version: 1
style: pipeline
maturity: draft
steps:
  - id: prompt
    type: session_request
    prompt: |
      # Review notes

      Keep this line.
`);

    expect(workflow.steps[0]?.prompt).toBe("# Review notes\n\nKeep this line.\n");
  });

  test("keeps sequence-item block scalars from consuming sibling fields", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: sequence-block-scalar
version: 1
style: pipeline
maturity: draft
steps:
  - prompt: |
      Write the review.
    id: prompt
    type: session_request
`);

    expect(workflow.steps[0]).toEqual({
      prompt: "Write the review.\n",
      id: "prompt",
      type: "session_request"
    });
  });

  test("supports block scalar chomp indicators", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: block-scalar-chomp
version: 1
style: pipeline
maturity: draft
steps:
  - id: prompt
    type: session_request
    prompt: |-
      Keep this line.
`);

    expect(workflow.steps[0]?.prompt).toBe("Keep this line.");
  });

  test("preserves folded block scalar paragraph semantics", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: folded-block-scalar
version: 1
style: pipeline
maturity: draft
steps:
  - id: prompt
    type: session_request
    prompt: >
      First line
      second line

      Next paragraph
`);

    expect(workflow.steps[0]?.prompt).toBe("First line second line\nNext paragraph\n");
  });

  test("keeps bracket-bearing command strings as scalar data", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`name: command-brackets
version: 1
style: pipeline
maturity: draft
steps:
  - id: echo
    type: command
    command: echo [done]
`);

    expect(workflow.steps[0]?.command).toBe("echo [done]");
  });

  test("supports compact nested lists in sequence item mappings", () => {
    const compactWorkflow = parseAgentflowWorkflowOrThrow(`name: compact-nested-list
version: 1
style: pipeline
maturity: draft
steps:
  - id: install
    type: command
    inputs:
    - bun.lock
    - package.json
    command: bun install
`);
    const indentedWorkflow = parseAgentflowWorkflowOrThrow(`name: indented-nested-list
version: 1
style: pipeline
maturity: draft
steps:
  - id: install
    type: command
    inputs:
      - bun.lock
      - package.json
    command: bun install
`);

    expect(compactWorkflow.steps[0]).toEqual(indentedWorkflow.steps[0]);
    expect(compactWorkflow.steps[0]).toEqual({
      id: "install",
      type: "command",
      inputs: ["bun.lock", "package.json"],
      command: "bun install"
    });
  });

  test("returns actionable errors for invalid YAML", () => {
    const result = parseAgentflowWorkflow(`name: broken
version: 1
style: pipeline
maturity: draft
steps:
  - id: one
    type: command
    inputs: [one,, two]
`);

    expect(result.ok).toBe(false);

    if (!result.ok) {
      expect(result.errors).toHaveLength(1);
      expect(result.errors[0]).toMatchObject({ code: "workflow.yaml", line: 8, column: 18 });
      expect(result.errors[0]?.message).toBeString();
      expect(formatWorkflowParseIssues(result.errors)).toContain("workflow.yaml at line 8, column 18");
    }

    const unterminatedQuoteResult = parseAgentflowWorkflow(`name: "broken
version: 1
style: pipeline
maturity: draft
steps: []
`);

    expect(unterminatedQuoteResult.ok).toBe(false);

    if (!unterminatedQuoteResult.ok) {
      expect(unterminatedQuoteResult.errors).toHaveLength(1);
      expect(unterminatedQuoteResult.errors[0]).toMatchObject({ code: "workflow.yaml" });
      expect(unterminatedQuoteResult.errors[0]?.message).toContain("Missing closing");
    }
  });

  test("rejects duplicate keys and non-JSON numeric values", () => {
    const duplicateKeyResult = parseAgentflowWorkflow(`name: duplicate
name: duplicate-again
version: 1
style: pipeline
maturity: draft
steps: []
`);
    const nonFiniteResult = parseAgentflowWorkflow(`name: non-finite
version: 1
style: pipeline
maturity: draft
steps:
  - id: wait
    type: command
    timeout_seconds: .inf
`);
    const unsafeIntegerResult = parseAgentflowWorkflow(`name: unsafe-integer
version: 9007199254740993
style: pipeline
maturity: draft
steps: []
`);

    expect(duplicateKeyResult.ok).toBe(false);
    expect(nonFiniteResult.ok).toBe(false);
    expect(unsafeIntegerResult.ok).toBe(false);

    if (!duplicateKeyResult.ok) {
      expect(duplicateKeyResult.errors[0]).toMatchObject({ code: "workflow.yaml", line: 2 });
    }

    if (!nonFiniteResult.ok) {
      expect(nonFiniteResult.errors).toEqual([
        {
          code: "workflow.yaml.value",
          path: "$.steps[0].timeout_seconds",
          message: "Numbers must be finite."
        }
      ]);
    }

    if (!unsafeIntegerResult.ok) {
      expect(unsafeIntegerResult.errors).toEqual([
        {
          code: "workflow.yaml.value",
          path: "$.version",
          message: "Integers must be within the JavaScript safe integer range."
        }
      ]);
    }
  });

  test("rejects multiple documents and unresolved YAML tags", () => {
    const multipleDocumentsResult = parseAgentflowWorkflow(`name: first
version: 1
style: pipeline
maturity: draft
steps: []
---
name: second
version: 1
style: pipeline
maturity: draft
steps: []
`);
    const unresolvedTagResult = parseAgentflowWorkflow(`name: !env WORKFLOW_NAME
version: 1
style: pipeline
maturity: draft
steps: []
`);

    expect(multipleDocumentsResult.ok).toBe(false);
    expect(unresolvedTagResult.ok).toBe(false);

    if (!multipleDocumentsResult.ok) {
      expect(multipleDocumentsResult.errors[0]).toMatchObject({ code: "workflow.yaml", line: 6, column: 1 });
      expect(multipleDocumentsResult.errors[0]?.message).toContain("multiple documents");
    }

    if (!unresolvedTagResult.ok) {
      expect(unresolvedTagResult.errors[0]).toMatchObject({ code: "workflow.yaml", line: 1, column: 7 });
      expect(unresolvedTagResult.errors[0]?.message).toContain("Unresolved tag: !env");
    }
  });

  test("returns actionable errors for invalid root shapes and required fields", () => {
    const listResult = parseAgentflowWorkflow(`- name: not-a-workflow`);
    const missingResult = parseAgentflowWorkflow(`name: missing-fields`);
    const invalidVersionResult = parseAgentflowWorkflow(`name: invalid-version
version: 0
style: pipeline
maturity: draft
steps: []
`);

    expect(listResult.ok).toBe(false);
    expect(missingResult.ok).toBe(false);
    expect(invalidVersionResult.ok).toBe(false);

    if (!listResult.ok) {
      expect(listResult.errors[0]).toMatchObject({
        code: "workflow.root",
        message: "Agentflow workflow YAML must parse to a mapping at the document root."
      });
    }

    if (!missingResult.ok) {
      expect(missingResult.errors.map((error) => error.path)).toEqual(["version", "style", "maturity", "steps"]);
    }

    if (!invalidVersionResult.ok) {
      expect(invalidVersionResult.errors).toContainEqual({
        code: "workflow.version.minimum",
        path: "version",
        message: "Agentflow workflow field version must be greater than or equal to 1."
      });
    }
  });
});

function readFixture(fileName: string): string {
  return fs.readFileSync(path.join(fixtureRoot, fileName), "utf8");
}
