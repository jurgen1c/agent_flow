import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import path from "node:path";
import {
  AgentFlowAmbiguousSuccessTargetError,
  AgentFlowWorkflowGraphError,
  buildAgentFlowWorkflowGraph,
  explainAgentFlowWorkflow,
  parseAgentFlowWorkflowOrThrow,
  renderAgentFlowWorkflowGraph
} from "../../src/runtime";

const repoRoot = path.resolve(".");
const examples = path.join(repoRoot, "examples/workflows");

function loadExample(name: string) {
  return parseAgentFlowWorkflowOrThrow(fs.readFileSync(path.join(examples, name), "utf8"));
}

describe("Agent Flow workflow inspection", () => {
  test("explains workflow metadata, steps, artifacts, policies, and warnings without mutation", () => {
    const workflow = loadExample("ticket-lifecycle.yml");
    const before = JSON.stringify(workflow);
    const explanation = explainAgentFlowWorkflow(workflow);

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
    const first = renderAgentFlowWorkflowGraph(workflow);
    const second = renderAgentFlowWorkflowGraph(workflow);

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

  test("renders continue and ignore success routes as listed-order fallthroughs", () => {
    for (const target of ["continue", "ignore"]) {
      const workflow = parseAgentFlowWorkflowOrThrow(`name: ${target}-fallthrough
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: first, type: command, command: echo first, then: ${target} }
  - { id: second, type: command, command: echo second }
`);

      const graph = buildAgentFlowWorkflowGraph(workflow);

      expect(graph.edges).toContainEqual({ from: "first", to: "second", kind: "next" });
      expect(graph.nodes.map((node) => node.id)).not.toContain(`terminal:${target}`);
    }
  });

  test("rejects graphs with ambiguous success targets", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: ambiguous-graph-success-target
version: 1
style: pipeline
maturity: draft
steps:
  - { id: start, type: command, command: echo start, then: second, goto: third }
  - { id: second, type: command, command: echo second }
  - { id: third, type: command, command: echo third }
`);

    expect(() => buildAgentFlowWorkflowGraph(workflow)).toThrow(AgentFlowAmbiguousSuccessTargetError);
    expect(() => buildAgentFlowWorkflowGraph(workflow)).toThrow(
      'Step "start" cannot declare both then and goto success targets.'
    );
  });

  test("gives declared continue and ignore step IDs precedence over fallthrough aliases", () => {
    for (const target of ["continue", "ignore"]) {
      const workflow = parseAgentFlowWorkflowOrThrow(`name: declared-${target}
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: first, type: command, command: echo first, then: ${target} }
  - { id: skipped, type: command, command: echo skipped }
  - { id: ${target}, type: command, command: echo target }
`);
      const graph = buildAgentFlowWorkflowGraph(workflow);
      expect(graph.edges).toContainEqual({ from: "first", to: target, kind: "then" });
      expect(graph.edges).not.toContainEqual({ from: "first", to: "skipped", kind: "next" });
    }
  });

  test("includes parallel branch containers and collaboration metadata", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`
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
    owns: [" implementation "]
    authority:
      can_modify_files: true
    file_scope:
      include: [./src/**]
  reviewer:
    provider: local
    role: reviewer
    authority:
      can_request_changes: true
      can_approve: true
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
            inputs: [brief.md]
            outputs: [draft.md]
      - id: advise
        session: reviewer
  - id: approve
    type: approval
    reviewer: reviewer
    artifacts: [draft.md]
`);

    const graph = buildAgentFlowWorkflowGraph(workflow);
    const explanation = explainAgentFlowWorkflow(workflow);

    expect(graph.nodes.map((node) => node.id)).toEqual(expect.arrayContaining(["split", "split.branch.draft", "write", "split.branch.advise", "approve"]));
    expect(graph.edges).toEqual(expect.arrayContaining([
      { from: "split", to: "split.branch.draft", kind: "branch", label: "draft" },
      { from: "split.branch.draft", to: "write", kind: "contains", label: "steps" },
      { from: "split", to: "approve", kind: "next" }
    ]));
    expect(explanation).toContain("Collaboration: enabled; max_review_cycles=2");
    expect(explanation).toContain("writer: provider=frontier; role=writer; owns=implementation; authority=can_modify_files; file_scope.include=src/**");
    expect(explanation).toContain("reviewer: provider=local; role=reviewer; authority=can_approve,can_request_changes");
    expect(explanation).toContain("branch draft — session=writer");
    expect(explanation).toContain("approve [approval] — reviewer=reviewer");
    expect(graph.sessions).toEqual([
      {
        name: "reviewer",
        provider: "local",
        role: "reviewer",
        owns: [],
        authority: ["can_approve", "can_request_changes"],
        fileScope: { include: [], exclude: [] }
      },
      {
        name: "writer",
        provider: "frontier",
        role: "writer",
        owns: ["implementation"],
        authority: ["can_modify_files"],
        fileScope: { include: ["src/**"], exclude: [] }
      }
    ]);
    expect(renderAgentFlowWorkflowGraph(workflow)).toContain(
      "writer: role=writer; authority=can_modify_files; owns=implementation; file_scope.include=src/**"
    );
  });

  test("models executable direct parallel branches as typed routed steps", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: direct-specialized-branches
version: 1
style: pipeline
maturity: experimental
sessions:
  reviewer: { provider: fixture, authority: { can_approve: true } }
  owner: { provider: fixture }
steps:
  - id: split
    type: parallel
    branches:
      - id: approve
        type: approval
        session: reviewer
        reviewer: reviewer
        artifacts: [release.md]
        on_approve: done
        on_reject: revise
        on_cancel: cancel
      - id: record
        type: decision_record
        session: owner
        owner: owner
        topic: Record the direct branch decision
        artifacts: [release.md]
  - { id: revise, type: result, status: failed }
  - { id: done, type: result, status: completed }
`);

    const graph = buildAgentFlowWorkflowGraph(workflow);
    const explanation = explainAgentFlowWorkflow(workflow);

    expect(graph.nodes).toEqual(expect.arrayContaining([
      expect.objectContaining({ id: "approve", type: "approval", path: "steps[0].branches[0]" }),
      expect.objectContaining({ id: "record", type: "decision_record", path: "steps[0].branches[1]" })
    ]));
    expect(graph.nodes.map((node) => node.id)).not.toEqual(expect.arrayContaining([
      "split.branch.approve",
      "split.branch.record"
    ]));
    expect(graph.edges).toEqual(expect.arrayContaining([
      { from: "split", to: "approve", kind: "branch", label: "approve" },
      { from: "split", to: "record", kind: "branch", label: "record" },
      { from: "approve", to: "done", kind: "on_approve" },
      { from: "approve", to: "revise", kind: "on_reject" },
      { from: "approve", to: "terminal:cancel", kind: "on_cancel" }
    ]));
    expect(explanation).toContain([
      "    - approve [approval] — reviewer=reviewer",
      "      reads: release.md",
      "      writes: approvals/approve.json"
    ].join("\n"));
    expect(explanation).toContain([
      "    - record [decision_record] — owner=owner; topic=Record the direct branch decision",
      "      reads: release.md",
      "      writes: decision-records/record.json"
    ].join("\n"));
    expect(explanation).not.toContain("- branch approve");
    expect(explanation).not.toContain("- branch record");
  });

  test("renders advisory authority for sessions without stronger grants", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: advisory
version: 1
style: collaborative
maturity: draft
collaboration: { enabled: true }
sessions:
  advisor: { provider: local, role: " advisor ", authority: { can_block: false } }
steps: []
`);

    expect(explainAgentFlowWorkflow(workflow)).toContain("advisor: provider=local; role=advisor; authority=advisory");
    expect(buildAgentFlowWorkflowGraph(workflow).sessions[0]?.authority).toEqual(["advisory"]);
    expect(renderAgentFlowWorkflowGraph(workflow)).toContain("advisor: role=advisor; authority=advisory");
  });

  test("preserves explicit denial of advisory authority", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: denied-advice
version: 1
style: collaborative
maturity: draft
collaboration: { enabled: true }
sessions:
  advisor:
    provider: local
    role: advisor
    authority: { can_advise: false }
steps: []
`);

    expect(explainAgentFlowWorkflow(workflow)).toContain("advisor: provider=local; role=advisor; authority=none");
    expect(buildAgentFlowWorkflowGraph(workflow).sessions[0]?.authority).toEqual([]);
    expect(renderAgentFlowWorkflowGraph(workflow)).toContain("advisor: role=advisor; authority=none");
  });

  test("does not grant advisory authority outside collaborative workflows", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: ordinary-pipeline
version: 1
style: pipeline
maturity: draft
sessions:
  worker: { provider: local }
steps: []
`);

    expect(explainAgentFlowWorkflow(workflow)).toContain("worker: provider=local; authority=none");
    expect(buildAgentFlowWorkflowGraph(workflow).sessions[0]?.authority).toEqual([]);
    expect(renderAgentFlowWorkflowGraph(workflow)).toContain("worker: authority=none");
  });

  test("surfaces invalid declared file scopes instead of silently dropping them", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: invalid-scope-inspection
version: 1
style: collaborative
maturity: draft
collaboration: { enabled: true }
sessions:
  writer:
    provider: local
    role: writer
    authority: { can_modify_files: true }
    file_scope: { include: [.] }
steps: []
`);

    expect(explainAgentFlowWorkflow(workflow)).toContain('file_scope.include=invalid:"."');
    expect(buildAgentFlowWorkflowGraph(workflow).sessions[0]?.fileScope.include).toEqual(['invalid:"."']);
    expect(renderAgentFlowWorkflowGraph(workflow)).toContain('file_scope.include=invalid:"."');
  });

  test("marks unsupported enabled authority without advertising it as a capability", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: invalid-authority-inspection
version: 1
style: collaborative
maturity: draft
collaboration: { enabled: true }
sessions:
  reviewer:
    provider: local
    role: reviewer
    authority: { can_veto: true }
steps: []
`);

    expect(explainAgentFlowWorkflow(workflow)).toContain('authority=invalid:{"can_veto":true}');
    expect(buildAgentFlowWorkflowGraph(workflow).sessions[0]?.authority).toEqual([
      'invalid:{"can_veto":true}'
    ]);
    expect(renderAgentFlowWorkflowGraph(workflow)).toContain('authority=invalid:{"can_veto":true}');
  });

  test("surfaces malformed supported authority and non-string file scopes", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: malformed-session-inspection
version: 1
style: collaborative
maturity: draft
collaboration: { enabled: true }
sessions:
  reviewer:
    provider: local
    role: reviewer
    authority: { can_block: "true" }
    file_scope: { include: [42] }
steps: []
`);

    const session = buildAgentFlowWorkflowGraph(workflow).sessions[0];
    expect(session?.authority).toEqual(["advisory", 'invalid:{"can_block":"true"}']);
    expect(session?.fileScope.include).toEqual(["invalid:42"]);
    expect(explainAgentFlowWorkflow(workflow)).toContain('authority=advisory,invalid:{"can_block":"true"}');
    expect(renderAgentFlowWorkflowGraph(workflow)).toContain("file_scope.include=invalid:42");
  });

  test("surfaces malformed session metadata roots", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: malformed-session-roots
version: 1
style: collaborative
maturity: draft
collaboration: { enabled: true }
sessions:
  reviewer:
    provider: 42
    role: " "
    owns: invalid
    authority: invalid
    file_scope: invalid
steps: []
`);

    expect(buildAgentFlowWorkflowGraph(workflow).sessions[0]).toEqual({
      name: "reviewer",
      provider: "invalid:42",
      role: 'invalid:" "',
      owns: ['invalid:"invalid"'],
      authority: ['invalid:"invalid"'],
      fileScope: { include: ['invalid:"invalid"'], exclude: [] }
    });
  });

  test("preserves malformed session declarations in inspection", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`name: malformed-session-declaration
version: 1
style: collaborative
maturity: draft
collaboration: { enabled: true }
sessions: { reviewer: invalid }
steps: []
`);

    expect(buildAgentFlowWorkflowGraph(workflow).sessions).toEqual([{
      name: "reviewer",
      owns: [],
      authority: ['invalid_session:"invalid"'],
      fileScope: { include: [], exclude: [] }
    }]);
    expect(explainAgentFlowWorkflow(workflow)).toContain('reviewer: authority=invalid_session:"invalid"');
    expect(renderAgentFlowWorkflowGraph(workflow)).toContain('reviewer: authority=invalid_session:"invalid"');
  });

  test("preserves validator fallthrough semantics and terminal gate outcomes", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`
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

    const graph = buildAgentFlowWorkflowGraph(workflow);

    expect(graph.edges).toEqual(expect.arrayContaining([
      { from: "choose", to: "publish", kind: "then" },
      { from: "choose", to: "publish", kind: "next" },
      { from: "gate", to: "finish", kind: "next" },
      { from: "gate", to: "terminal:pause", kind: "option", label: "pause" },
      { from: "gate", to: "terminal:cancel", kind: "on_reject" }
    ]));
  });

  test("renders implicit rejection and terminal completion gate outcomes", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: implicit-gate-outcomes
version: 1
style: pipeline
maturity: draft
steps:
  - id: gate
    type: manual_gate
    message: Finish?
    options: [reject, completed, cancel]
`);

    const graph = buildAgentFlowWorkflowGraph(workflow);

    expect(graph.edges).toEqual(expect.arrayContaining([
      { from: "gate", to: "terminal:cancel", kind: "option", label: "reject" },
      { from: "gate", to: "terminal:completed", kind: "option", label: "completed" },
      { from: "gate", to: "terminal:cancel", kind: "option", label: "cancel" }
    ]));
  });

  test("renders implicit approval rejection and human cancellation outcomes", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: implicit-approval-outcomes
version: 1
style: collaborative
maturity: experimental
sessions:
  reviewer: { provider: fixture, authority: { can_approve: true } }
steps:
  - { id: session_approval, type: approval, reviewer: reviewer, artifacts: [spec.md] }
  - { id: human_approval, type: approval, reviewer: human, artifacts: [release.md] }
  - { id: done, type: result, status: completed }
`);

    expect(buildAgentFlowWorkflowGraph(workflow).edges).toEqual(expect.arrayContaining([
      { from: "session_approval", to: "terminal:cancel", kind: "on_reject" },
      { from: "human_approval", to: "terminal:cancel", kind: "on_reject" },
      { from: "human_approval", to: "terminal:cancel", kind: "on_cancel" }
    ]));
  });

  test("does not confuse shell-style target text with Agent Flow interpolation", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`
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

    expect(buildAgentFlowWorkflowGraph(workflow).edges).toContainEqual({
      from: "start",
      to: "${finish",
      kind: "then"
    });
  });

  test("normalizes padded ids and targets like workflow validation", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`
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

    const graph = buildAgentFlowWorkflowGraph(workflow);
    expect(graph.nodes.map((node) => node.id)).toEqual(["start", "finish"]);
    expect(graph.edges).toContainEqual({ from: "start", to: "finish", kind: "then" });
  });

  test("rejects collisions between authored and generated graph node ids", () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`
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

    expect(() => buildAgentFlowWorkflowGraph(workflow)).toThrow(AgentFlowWorkflowGraphError);
    expect(() => buildAgentFlowWorkflowGraph(workflow)).toThrow('Graph node id "terminal:pause" collides');
  });

  test("orders numeric workflow path indices naturally", () => {
    const steps = Array.from({ length: 12 }, (_, index) => `
  - id: step-${index}
    type: command
    command: echo ${index}`).join("");
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: many-steps
version: 1
style: pipeline
maturity: draft
steps:${steps}
`);

    expect(buildAgentFlowWorkflowGraph(workflow).nodes.map((node) => node.id)).toEqual(
      Array.from({ length: 12 }, (_, index) => `step-${index}`)
    );
  });
});
