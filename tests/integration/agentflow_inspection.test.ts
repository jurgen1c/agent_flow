import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import path from "node:path";
import {
  AgentflowWorkflowGraphError,
  buildAgentflowWorkflowGraph,
  explainAgentflowWorkflow,
  parseAgentflowWorkflowOrThrow,
  renderAgentflowWorkflowGraph
} from "../../packages/agentflow-core/src";

const repoRoot = path.resolve(".");
const examples = path.join(repoRoot, "agentflow-examples/agentflow-examples/workflows");

function loadExample(name: string) {
  return parseAgentflowWorkflowOrThrow(fs.readFileSync(path.join(examples, name), "utf8"));
}

describe("Agentflow workflow inspection", () => {
  test("explains workflow metadata, steps, artifacts, policies, and warnings without mutation", () => {
    const workflow = loadExample("ticket-lifecycle.yml");
    const before = JSON.stringify(workflow);
    const explanation = explainAgentflowWorkflow(workflow);

    expect(explanation).toContain("Workflow: ticket-lifecycle (version 1)");
    expect(explanation).toContain("Style: recovery_pipeline");
    expect(explanation).toContain("- local_ci [command]");
    expect(explanation).toContain("- pr_feedback_loop [workflow] — workflow=pr-feedback-loop");
    expect(explanation).toContain("- approve_merge [manual_gate]");
    expect(explanation).toContain("options=approve,pause,cancel");
    expect(explanation).toContain("Artifacts:");
    expect(explanation).toContain("Policies:");
    expect(explanation).toContain("Warnings:");
    expect(JSON.stringify(workflow)).toBe(before);
  });

  test("renders deterministic graph edges for sequence, conditions, loops, and nested workflows", () => {
    const workflow = loadExample("pr-feedback-loop.yml");
    const first = renderAgentflowWorkflowGraph(workflow);
    const second = renderAgentflowWorkflowGraph(workflow);

    expect(first).toBe(second);
    expect(first).toContain("Workflow graph: pr-feedback-loop (version 1)");
    expect(first).toContain("wait_for_review [loop]");
    expect(first).toContain("collect_pr_state [command]");
    expect(first).toContain("wait_for_review -> collect_pr_state [loop body]");
    expect(first).toContain("route_comments -> resolve_comments [then]");
    expect(first).toContain("route_comments -> continue_loop [else]");
    expect(first).toContain("wait_for_review -> return_complete [next]");
    expect(first).toContain("rerun_ci -> terminal:pause [on_failure.on_unresolved.then]");
  });

  test("includes parallel branch containers and collaboration metadata", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`
name: collaborate
version: 1
style: collaborative
maturity: draft
collaboration:
  enabled: true
  max_review_cycles: 2
sessions:
  writer:
    provider: frontier
    role: writer
  reviewer:
    provider: local
    role: reviewer
steps:
  - id: split
    type: parallel
    branches:
      - id: draft
        session: writer
        steps:
          - id: write
            type: session_request
            session: writer
            prompt: prompts/write.md
      - id: advise
        session: reviewer
  - id: approve
    type: approval
    reviewer: reviewer
    artifacts: [draft.md]
`);

    const graph = buildAgentflowWorkflowGraph(workflow);
    const explanation = explainAgentflowWorkflow(workflow);

    expect(graph.nodes.map((node) => node.id)).toEqual(expect.arrayContaining(["split", "split.branch.draft", "write", "split.branch.advise", "approve"]));
    expect(graph.edges).toEqual(expect.arrayContaining([
      { from: "split", to: "split.branch.draft", kind: "branch", label: "draft" },
      { from: "split.branch.draft", to: "write", kind: "contains", label: "steps" },
      { from: "split", to: "approve", kind: "next" }
    ]));
    expect(explanation).toContain("Collaboration: enabled; max_review_cycles=2");
    expect(explanation).toContain("branch draft — session=writer");
    expect(explanation).toContain("approve [approval] — reviewer=reviewer");
  });

  test("preserves validator fallthrough semantics and terminal gate outcomes", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`
name: fallthrough
version: 1
style: pipeline
maturity: draft
steps:
  - id: choose
    type: condition
    if: inputs.publish == true
    then: publish
  - id: publish
    type: command
    command: echo publish
  - id: gate
    type: manual_gate
    message: Continue?
    options: [approve, pause, cancel]
    on_reject: cancel
  - id: finish
    type: result
    status: completed
`);

    const graph = buildAgentflowWorkflowGraph(workflow);

    expect(graph.edges).toEqual(expect.arrayContaining([
      { from: "choose", to: "publish", kind: "then" },
      { from: "choose", to: "publish", kind: "next" },
      { from: "gate", to: "finish", kind: "next" },
      { from: "gate", to: "terminal:pause", kind: "option", label: "pause" },
      { from: "gate", to: "terminal:cancel", kind: "on_reject" }
    ]));
  });

  test("does not confuse shell-style target text with Agentflow interpolation", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`
name: target-syntax
version: 1
style: pipeline
maturity: draft
steps:
  - id: start
    type: command
    command: echo start
    then: \${finish
  - id: \${finish
    type: result
    status: completed
`);

    expect(buildAgentflowWorkflowGraph(workflow).edges).toContainEqual({
      from: "start",
      to: "${finish",
      kind: "then"
    });
  });

  test("normalizes padded ids and targets like workflow validation", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`
name: padded-targets
version: 1
style: pipeline
maturity: draft
steps:
  - id: " start "
    type: command
    command: echo start
    then: " finish "
  - id: " finish "
    type: result
    status: completed
`);

    const graph = buildAgentflowWorkflowGraph(workflow);
    expect(graph.nodes.map((node) => node.id)).toEqual(["start", "finish"]);
    expect(graph.edges).toContainEqual({ from: "start", to: "finish", kind: "then" });
  });

  test("rejects collisions between authored and generated graph node ids", () => {
    const workflow = parseAgentflowWorkflowOrThrow(`
name: colliding-nodes
version: 1
style: pipeline
maturity: draft
steps:
  - id: run
    type: command
    command: echo run
    on_failure:
      then: pause
  - id: terminal:pause
    type: result
    status: completed
`);

    expect(() => buildAgentflowWorkflowGraph(workflow)).toThrow(AgentflowWorkflowGraphError);
    expect(() => buildAgentflowWorkflowGraph(workflow)).toThrow('Graph node id "terminal:pause" collides');
  });

  test("orders numeric workflow path indices naturally", () => {
    const steps = Array.from({ length: 12 }, (_, index) => `
  - id: step-${index}
    type: command
    command: echo ${index}`).join("");
    const workflow = parseAgentflowWorkflowOrThrow(`
name: many-steps
version: 1
style: pipeline
maturity: draft
steps:${steps}
`);

    expect(buildAgentflowWorkflowGraph(workflow).nodes.map((node) => node.id)).toEqual(
      Array.from({ length: 12 }, (_, index) => `step-${index}`)
    );
  });
});
