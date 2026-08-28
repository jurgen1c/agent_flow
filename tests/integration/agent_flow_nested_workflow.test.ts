import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import path from "node:path";
import {
  createAgentFlowLifecycleRun,
  createAgentFlowWorkflowRegistry,
  executeAgentFlowCommandPipeline,
  openAgentFlowRunState,
  parseAgentFlowWorkflowOrThrow,
  resumeAgentFlowCommandPipeline
} from "../../src/runtime";

describe("Agent Flow nested workflow execution", () => {
  test("links a child run, forwards approval resume, and atomically promotes declared outputs", async () => {
    const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-nested-workflow-"));
    fs.mkdirSync(path.join(repo, ".git"));
    const child = parseAgentFlowWorkflowOrThrow(`
name: child-workflow
version: 1
style: pipeline
maturity: experimental
inputs:
  ticket: { required: true }
steps:
  - id: approve
    type: manual_gate
    message: Approve child?
    options: [approve, pause, cancel]
  - id: write
    type: command
    command: printf 'child output\\n' > child.txt
    outputs: [child.txt]
`);
    const parent = parseAgentFlowWorkflowOrThrow(`
name: parent-workflow
version: 1
style: pipeline
maturity: experimental
inputs:
  ticket: { required: true }
steps:
  - id: child
    type: workflow
    workflow: child-workflow
    inputs:
      ticket: "{{ inputs.ticket }}"
    outputs: [child.txt]
`);
    const workflows = createAgentFlowWorkflowRegistry()
      .register(parent.name, parent)
      .register(child.name, child);
    const store = await openAgentFlowRunState({ cwd: repo });
    createAgentFlowLifecycleRun(store, { id: "parent", workflow: parent, inputs: { ticket: "AF-1" } });

    const started = await executeAgentFlowCommandPipeline(
      store, "parent", parent, undefined, undefined, undefined, undefined, workflows
    );
    expect(started).toMatchObject({ status: "paused", failedStep: "child" });
    const waiting = store.getRun("parent")?.context.waiting as { childRunId?: string };
    const childRunId = waiting.childRunId!;
    expect(store.getRun(childRunId)).toMatchObject({ parentRunId: "parent", status: "paused" });

    const resumed = await resumeAgentFlowCommandPipeline(
      store, "parent", parent, { outcome: "approve" }, undefined, undefined, undefined, undefined, workflows
    );
    expect(resumed).toMatchObject({ status: "completed" });
    expect(store.getRun(childRunId)?.status).toBe("completed");
    expect(store.readArtifact("parent", "child.txt").content.toString("utf8")).toBe("child output\n");
    expect(store.getArtifact("parent", "child.txt")?.metadata).toMatchObject({
      childRunId
    });
    store.close();
  });
});
