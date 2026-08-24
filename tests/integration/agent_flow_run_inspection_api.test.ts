import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import fs from "node:fs";
import net from "node:net";
import path from "node:path";
import vm from "node:vm";
import {
  AGENT_FLOW_RUN_INSPECTION_TOKEN_HEADER,
  AGENT_FLOW_RUN_INSPECTION_RUN_ID_HEADER,
  type AgentFlowRunAction,
  buildAgentFlowRunActionSnapshot,
  buildAgentFlowRunInspectionModel,
  buildAgentFlowRunInspectionPage,
  createAgentFlowSessionProviderRegistry,
  createAgentFlowLifecycleRun,
  executeAgentFlowRunAction,
  executeAgentFlowCommandPipeline,
  listAgentFlowRunInspectionSummaries,
  openAgentFlowRunState,
  parseAgentFlowWorkflowOrThrow,
  startAgentFlowRunInspectionApi,
  transitionAgentFlowLifecycleRun
} from "../../src/runtime";

describe("Agent Flow run inspection API", () => {
  test("builds run summaries and a full inspection model from persisted state", async () => {
    const repo = makeRepo();
    const store = await openAgentFlowRunState({ cwd: repo });
    createInspectableRun(store, "run-model");

    const summaries = listAgentFlowRunInspectionSummaries(store);
    const model = buildAgentFlowRunInspectionModel(store, "run-model");

    expect(summaries).toEqual([
      expect.objectContaining({
        id: "run-model",
        workflowName: "inspectable",
        status: "pending",
        currentStepId: null
      })
    ]);
    expect(summaries[0]).not.toHaveProperty("inputs");
    expect(model.state.inputs).toEqual({ ticket: "AF-58" });
    expect(model.steps).toEqual([
      expect.objectContaining({
        stepId: "inspect",
        attempt: 1,
        status: "failed",
        error: { message: "Inspection failed" }
      })
    ]);
    expect(model.events).toEqual([
      expect.objectContaining({ sequence: 1, type: "run.created" })
    ]);
    expect(model.artifacts.map((artifact) => artifact.kind)).toEqual([
      "decision_record",
      "failure_payload"
    ]);
    expect(model.failures).toEqual([
      expect.objectContaining({
        id: "failure:inspect:1",
        classification: "command_failure",
        failurePayload: expect.objectContaining({
          document: expect.objectContaining({ id: "failure:inspect:1", status: "failed" }),
          error: null
        })
      })
    ]);
    expect(model.approvals).toEqual([
      expect.objectContaining({ id: "approval:inspect", status: "requested" })
    ]);
    expect(model.decisions).toEqual([
      expect.objectContaining({
        document: expect.objectContaining({ decision_id: "decision:inspect", owner: "owner" }),
        error: null
      })
    ]);
    expect(model.warnings).toEqual(expect.arrayContaining([
      expect.objectContaining({ code: "run.failure.unresolved" }),
      expect.objectContaining({ code: "run.approval.requested" })
    ]));

    store.close();
  });

  test("loads paged evidence without materializing unrelated run sections", async () => {
    const repo = makeRepo();
    const store = await openAgentFlowRunState({ cwd: repo });
    createInspectableRun(store, "paged-model");
    const listArtifactMetadata = store.listArtifactMetadata.bind(store);
    const listEvents = store.listEvents.bind(store);
    const listSteps = store.listSteps.bind(store);

    store.listArtifactMetadata = (() => {
      throw new Error("failure and decision pages must use targeted artifact queries");
    }) as typeof store.listArtifactMetadata;
    expect(buildAgentFlowRunInspectionPage(store, "paged-model", "failures", 0, 1).items)
      .toHaveLength(1);
    expect(buildAgentFlowRunInspectionPage(store, "paged-model", "decisions", 0, 1).items)
      .toHaveLength(1);

    store.listArtifactMetadata = listArtifactMetadata;
    store.listEvents = (() => { throw new Error("warning pages must not load events"); }) as typeof store.listEvents;
    store.listSteps = (() => { throw new Error("warning pages must not load steps"); }) as typeof store.listSteps;
    expect(buildAgentFlowRunInspectionPage(store, "paged-model", "warnings", 0, 10).items)
      .toEqual(expect.arrayContaining([
        expect.objectContaining({ code: "run.failure.unresolved" }),
        expect.objectContaining({ code: "run.approval.requested" })
      ]));
    let artifactPage: { offset?: number; limit: number } | undefined;
    store.listArtifactMetadata = ((runId, page) => {
      artifactPage = page;
      return listArtifactMetadata(runId, page);
    }) as typeof store.listArtifactMetadata;
    const warningPage = buildAgentFlowRunInspectionPage(store, "paged-model", "warnings", 1, 1);
    expect(artifactPage).toEqual({ offset: 0, limit: 1 });
    expect(warningPage.nextOffset).toBe(2);

    store.listSteps = listSteps;
    const firstStep = store.listSteps("paged-model")[0]!;
    expect(() => store.listSteps("paged-model", {
      limit: 1,
      after: { sortValue: firstStep.createdAt }
    })).toThrow("offset pagination only");
    expect(() => buildAgentFlowRunInspectionPage(store, "paged-model", "events", 0, 201))
      .toThrow("limit between 1 and 200");
    store.listEvents = listEvents;
    store.close();
  });

  test("treats noncanonical paged failure payload pointers as unavailable evidence", async () => {
    const repo = makeRepo();
    const store = await openAgentFlowRunState({ cwd: repo });
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: invalid-payload-pointer
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: inspect, type: command, command: echo inspect }
`);
    createAgentFlowLifecycleRun(store, { id: "invalid-payload-pointer", workflow });
    store.recordFailure({
      id: "failure:invalid-pointer",
      runId: "invalid-payload-pointer",
      stepId: "inspect",
      classification: "command_failure",
      message: "Invalid persisted payload pointer",
      retryable: false,
      payload: { failurePayloadPath: "../escape.json" }
    });

    const failures = buildAgentFlowRunInspectionPage(
      store,
      "invalid-payload-pointer",
      "failures",
      0,
      10
    );
    const warnings = buildAgentFlowRunInspectionPage(
      store,
      "invalid-payload-pointer",
      "warnings",
      0,
      10
    );

    expect(failures.items).toEqual([
      expect.objectContaining({ id: "failure:invalid-pointer", failurePayload: null })
    ]);
    expect(warnings.items).toEqual(expect.arrayContaining([
      expect.objectContaining({ code: "run.failure.payload_unavailable" })
    ]));
    store.close();
  });

  test("captures database rows in one snapshot before inspecting artifact files", async () => {
    const repo = makeRepo();
    const writer = await openAgentFlowRunState({ cwd: repo });
    createInspectableRun(writer, "snapshot-model");
    const reader = await openAgentFlowRunState({ cwd: repo });
    const inspectArtifact = reader.inspectArtifactRecordForInspection.bind(reader);
    let transitioned = false;
    reader.inspectArtifactRecordForInspection = (artifact) => {
      if (!transitioned) {
        transitioned = true;
        writer.transitionRunWithEvent("snapshot-model", {
          status: "running",
          allowedFrom: ["pending"],
          event: { type: "run.started", payload: { status: "running" } }
        });
      }
      return inspectArtifact(artifact);
    };

    const model = buildAgentFlowRunInspectionModel(reader, "snapshot-model");

    expect(model.run.status).toBe("pending");
    expect(model.events.map((event) => event.type)).toEqual(["run.created"]);
    expect(writer.getRun("snapshot-model")?.status).toBe("running");
    expect(writer.listEvents("snapshot-model").map((event) => event.type))
      .toEqual(["run.created", "run.started"]);
    reader.close();
    writer.close();
  });

  test("captures each paged response in one database snapshot", async () => {
    const repo = makeRepo();
    const writer = await openAgentFlowRunState({ cwd: repo });
    createInspectableRun(writer, "snapshot-page");
    const reader = await openAgentFlowRunState({ cwd: repo });
    const inspectArtifact = reader.inspectArtifactRecordForInspection.bind(reader);
    let transitioned = false;
    reader.inspectArtifactRecordForInspection = (artifact) => {
      if (!transitioned) {
        transitioned = true;
        writer.transitionRunWithEvent("snapshot-page", {
          status: "running",
          allowedFrom: ["pending"],
          event: { type: "run.started", payload: { status: "running" } }
        });
      }
      return inspectArtifact(artifact);
    };

    const page = buildAgentFlowRunInspectionPage(reader, "snapshot-page", "failures", 0, 10);

    expect(page.items).toEqual([
      expect.objectContaining({
        id: "failure:inspect:1",
        failurePayload: expect.objectContaining({
          document: expect.objectContaining({ id: "failure:inspect:1" }),
          error: null
        })
      })
    ]);
    expect(writer.getRun("snapshot-page")?.status).toBe("running");
    expect(writer.listEvents("snapshot-page").map((event) => event.type))
      .toEqual(["run.created", "run.started"]);
    reader.close();
    writer.close();
  });

  test("rejects asynchronous read-transaction callbacks without leaving a transaction open", async () => {
    const repo = makeRepo();
    const store = await openAgentFlowRunState({ cwd: repo });
    createInspectableRun(store, "synchronous-snapshot");

    expect(() => store.withRunStateReadTransaction(async () => store.getRun("synchronous-snapshot")))
      .toThrow("require a synchronous callback");
    expect(store.withRunStateReadTransaction(() => store.getRun("synchronous-snapshot")?.id))
      .toBe("synchronous-snapshot");
    store.close();
  });

  test("reports structurally malformed persisted workflows as warnings", async () => {
    const repo = makeRepo();
    const store = await openAgentFlowRunState({ cwd: repo });
    store.createRun({
      id: "malformed-workflow",
      workflow: { name: "malformed", version: 1, style: "pipeline", maturity: "draft" },
      context: { workflow: {} }
    });

    const model = buildAgentFlowRunInspectionModel(store, "malformed-workflow");

    expect(model.run.id).toBe("malformed-workflow");
    expect(model.warnings).toContainEqual(expect.objectContaining({
      code: "run.workflow.invalid",
      source: "workflow"
    }));
    store.close();
  });

  test("reports structurally malformed decision and failure documents as warnings", async () => {
    const repo = makeRepo();
    const store = await openAgentFlowRunState({ cwd: repo });
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: malformed-documents
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: inspect, type: command, command: echo inspect }
`);
    createAgentFlowLifecycleRun(store, { id: "malformed-documents", workflow });
    store.writeArtifact({
      id: "decision",
      runId: "malformed-documents",
      path: "decision.json",
      kind: "decision_record",
      contentType: "application/json",
      content: JSON.stringify({
        decision_id: "decision:inspect",
        owner: "owner",
        topic: "Inspection",
        rationale_summary: "Inspect persisted state.",
        consulted: ["reviewer", "reviewer"],
        approved_by: [],
        artifacts: [],
        created_at: "not-a-date",
        extra: true
      })
    });
    store.writeArtifact({
      id: "failure",
      runId: "malformed-documents",
      path: "failure.json",
      kind: "failure_payload",
      contentType: "application/json",
      content: "null",
      metadata: { failureId: "failure:1" }
    });
    store.recordFailure({
      id: "failure:1",
      runId: "malformed-documents",
      classification: "command_failure",
      message: "Inspection failed",
      payload: { failurePayloadPath: "failure.json" }
    });

    const model = buildAgentFlowRunInspectionModel(store, "malformed-documents");

    expect(model.decisions[0]).toMatchObject({ document: null });
    expect(model.decisions[0]?.error).toContain("decision_record document shape");
    expect(model.failures[0]?.failurePayload).toMatchObject({ document: null });
    expect(model.failures[0]?.failurePayload?.error).toContain("failure_payload document shape");
    expect(model.warnings).toEqual(expect.arrayContaining([
      expect.objectContaining({ code: "run.decision.unavailable", path: "decision.json" }),
      expect.objectContaining({ code: "run.failure.payload_unavailable", path: "failure.json" })
    ]));
    store.close();
  });

  test("rejects failure payloads whose body identity does not match the failure", async () => {
    const repo = makeRepo();
    const store = await openAgentFlowRunState({ cwd: repo });
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: mismatched-failure
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: inspect, type: command, command: echo inspect }
`);
    createAgentFlowLifecycleRun(store, { id: "mismatched-failure", workflow });
    store.writeArtifact({
      id: "failure",
      runId: "mismatched-failure",
      path: "failure.json",
      kind: "failure_payload",
      contentType: "application/json",
      content: JSON.stringify(failurePayloadDocument("failure:other", "other.json")),
      metadata: { failureId: "failure:expected" }
    });
    store.recordFailure({
      id: "failure:expected",
      runId: "mismatched-failure",
      classification: "command_failure",
      message: "Inspection failed",
      payload: { failurePayloadPath: "failure.json" }
    });

    const model = buildAgentFlowRunInspectionModel(store, "mismatched-failure");

    expect(model.failures[0]?.failurePayload).toMatchObject({ document: null });
    expect(model.failures[0]?.failurePayload?.error).toContain("identity does not match");
    expect(model.warnings).toContainEqual(expect.objectContaining({
      code: "run.failure.payload_unavailable",
      path: "failure.json"
    }));
    store.close();
  });

  test("serves token-protected inspection endpoints on loopback", async () => {
    const repo = makeRepo();
    const store = await openAgentFlowRunState({ cwd: repo });
    createInspectableRun(store, "run-api");
    store.close();
    const server = await startAgentFlowRunInspectionApi({
      cwd: repo,
      port: 0,
      token: "inspection-token"
    });
    const headers = { [AGENT_FLOW_RUN_INSPECTION_TOKEN_HEADER]: "inspection-token" };

    try {
      const denied = await fetch(`${server.url}/api/runs`);
      const queryToken = await fetch(`${server.url}/api/runs?token=inspection-token`);
      const wrongToken = await fetch(`${server.url}/api/runs`, {
        headers: { [AGENT_FLOW_RUN_INSPECTION_TOKEN_HEADER]: "wrong-token" }
      });
      expect(denied.status).toBe(403);
      expect(queryToken.status).toBe(403);
      expect(wrongToken.status).toBe(403);

      const list = await fetch(`${server.url}/api/runs`, { headers });
      const listBody = await list.json() as { runs: Array<{ id: string }> };
      expect(list.status).toBe(200);
      expect(list.headers.get("cache-control")).toBe("no-store");
      expect(listBody.runs.map((run) => run.id)).toEqual(["run-api"]);

      const detail = await fetch(`${server.url}/api/runs/run-api`, { headers });
      const detailBody = await detail.json() as {
        run: { id: string };
        steps: unknown[];
        failures: Array<{ failurePayload: { document: { id: string } | null } | null }>;
        approvals: unknown[];
        decisions: unknown[];
      };
      expect(detail.status).toBe(200);
      expect(detailBody).toMatchObject({ run: { id: "run-api" } });
      expect(detailBody.steps).toHaveLength(1);
      expect(detailBody.failures).toHaveLength(1);
      expect(detailBody.failures[0]?.failurePayload?.document).toMatchObject({ id: "failure:inspect:1" });
      expect(detailBody.approvals).toHaveLength(1);
      expect(detailBody.decisions).toHaveLength(1);

      const missing = await fetch(`${server.url}/api/runs/missing`, { headers });
      expect(missing.status).toBe(404);
      expect(await missing.json()).toMatchObject({ code: "AGENT_FLOW_RUN_NOT_FOUND" });

      const invalidRunId = await rawGet(server.url, "/api/runs/%20", headers);
      expect(invalidRunId.status).toBe(400);
      expect(JSON.parse(invalidRunId.body)).toMatchObject({
        code: "AGENT_FLOW_INSPECTION_BAD_REQUEST"
      });

      const mutation = await fetch(`${server.url}/api/runs/run-api`, {
        method: "POST",
        headers
      });
      expect(mutation.status).toBe(405);
      expect(mutation.headers.get("allow")).toBe("GET");

      const gitDirectory = path.join(repo, ".git");
      const hiddenGitDirectory = path.join(repo, ".git-hidden");
      fs.renameSync(gitDirectory, hiddenGitDirectory);
      try {
        const unknown = await fetch(`${server.url}/not-an-inspection-endpoint`, { headers });
        expect(unknown.status).toBe(404);
        expect(await unknown.json()).toMatchObject({ code: "AGENT_FLOW_INSPECTION_NOT_FOUND" });
      } finally {
        fs.renameSync(hiddenGitDirectory, gitDirectory);
      }
    } finally {
      await server.close();
    }
  });

  test("guards token-protected approval actions and completes successful decisions", async () => {
    const repo = makeRepo();
    const store = await openAgentFlowRunState({ cwd: repo });
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: ui-approval
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: approve, type: approval, reviewer: human, artifacts: [release.md] }
  - { id: done, type: result, status: completed }
`);
    createAgentFlowLifecycleRun(store, { id: "ui-approval", workflow });
    store.writeArtifact({
      id: "release",
      runId: "ui-approval",
      path: "release.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Release candidate"
    });
    expect(await executeAgentFlowCommandPipeline(store, "ui-approval", workflow))
      .toMatchObject({ status: "paused" });
    createAgentFlowLifecycleRun(store, { id: "ui-rejection", workflow });
    store.writeArtifact({
      id: "release",
      runId: "ui-rejection",
      path: "release.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Rejected candidate"
    });
    expect(await executeAgentFlowCommandPipeline(store, "ui-rejection", workflow))
      .toMatchObject({ status: "paused" });
    store.close();
    const server = await startAgentFlowRunInspectionApi({
      cwd: repo,
      port: 0,
      token: "inspection-token"
    });
    const headers = {
      [AGENT_FLOW_RUN_INSPECTION_TOKEN_HEADER]: "inspection-token",
      [AGENT_FLOW_RUN_INSPECTION_RUN_ID_HEADER]: encodeURIComponent("ui-approval")
    };

    try {
      const denied = await fetch(`${server.url}/api/run/actions`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ action: "approve", guard: "x".repeat(43) })
      });
      expect(denied.status).toBe(403);

      const snapshotResponse = await fetch(`${server.url}/api/run/actions`, { headers });
      expect(snapshotResponse.status).toBe(200);
      const snapshot = await snapshotResponse.json() as {
        guard: string;
        waiting: { kind: string; stepId: string };
        staleApprovals: unknown[];
        actions: Array<{ action: string; enabled: boolean }>;
      };
      expect(snapshot.waiting).toEqual(expect.objectContaining({ kind: "approval", stepId: "approve" }));
      expect(snapshot.staleApprovals).toEqual([]);
      expect(snapshot.actions).toContainEqual(expect.objectContaining({ action: "approve", enabled: true }));

      const approved = await fetch(`${server.url}/api/run/actions`, {
        method: "POST",
        headers: { ...headers, "content-type": "application/json" },
        body: JSON.stringify({ action: "approve", guard: snapshot.guard })
      });
      expect(approved.status).toBe(200);
      expect(await approved.json()).toMatchObject({
        action: "approve",
        status: "completed",
        completedSteps: ["approve", "done"]
      });

      const rejectionHeaders = {
        ...headers,
        [AGENT_FLOW_RUN_INSPECTION_RUN_ID_HEADER]: encodeURIComponent("ui-rejection")
      };
      const rejectionSnapshot = await (await fetch(`${server.url}/api/run/actions`, {
        headers: rejectionHeaders
      })).json() as { guard: string };
      const rejected = await fetch(`${server.url}/api/run/actions`, {
        method: "POST",
        headers: { ...rejectionHeaders, "content-type": "application/json" },
        body: JSON.stringify({ action: "reject", guard: rejectionSnapshot.guard })
      });
      expect(rejected.status).toBe(200);
      expect(await rejected.json()).toMatchObject({ action: "reject", status: "cancelled" });

      const inspected = await openAgentFlowRunState({ cwd: repo });
      expect(inspected.listApprovals("ui-approval")).toEqual([
        expect.objectContaining({ status: "approved", decidedBy: "local-ui", decision: "approve" })
      ]);
      expect(inspected.listApprovals("ui-rejection")).toEqual([
        expect.objectContaining({ status: "rejected", decidedBy: "local-ui", decision: "reject" })
      ]);
      inspected.close();
    } finally {
      await server.close();
    }
  });

  test("rejects stale action guards and exposes stale approval warnings before mutation", async () => {
    const repo = makeRepo();
    const store = await openAgentFlowRunState({ cwd: repo });
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: stale-ui-approval
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: approve, type: approval, reviewer: human, artifacts: [release.md] }
  - { id: done, type: result, status: completed }
`);
    createAgentFlowLifecycleRun(store, { id: "stale-ui-approval", workflow });
    const release = store.writeArtifact({
      id: "release",
      runId: "stale-ui-approval",
      path: "release.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "First candidate"
    });
    expect(await executeAgentFlowCommandPipeline(store, "stale-ui-approval", workflow))
      .toMatchObject({ status: "paused" });
    store.close();
    const server = await startAgentFlowRunInspectionApi({
      cwd: repo,
      port: 0,
      token: "inspection-token"
    });
    const headers = {
      [AGENT_FLOW_RUN_INSPECTION_TOKEN_HEADER]: "inspection-token",
      [AGENT_FLOW_RUN_INSPECTION_RUN_ID_HEADER]: encodeURIComponent("stale-ui-approval")
    };

    try {
      const initial = await (await fetch(`${server.url}/api/run/actions`, { headers })).json() as { guard: string };
      const writer = await openAgentFlowRunState({ cwd: repo });
      writer.writeArtifact({
        id: release.id,
        runId: "stale-ui-approval",
        path: release.declaredPath,
        kind: release.kind,
        contentType: release.contentType,
        content: "Changed candidate",
        overwrite: true
      });
      writer.close();

      const staleAction = await fetch(`${server.url}/api/run/actions`, {
        method: "POST",
        headers: { ...headers, "content-type": "application/json" },
        body: JSON.stringify({ action: "approve", guard: initial.guard })
      });
      expect(staleAction.status).toBe(409);
      expect(await staleAction.json()).toMatchObject({ code: "AGENT_FLOW_ACTION_STALE" });

      const refreshed = await (await fetch(`${server.url}/api/run/actions`, { headers })).json() as {
        staleApprovals: Array<{ id: string }>;
        warnings: Array<{ code: string }>;
        actions: Array<{ action: string; enabled: boolean; reason: string | null }>;
      };
      expect(refreshed.staleApprovals).toHaveLength(1);
      expect(refreshed.warnings).toContainEqual(expect.objectContaining({ code: "action.approval.stale" }));
      expect(refreshed.actions).toContainEqual(expect.objectContaining({
        action: "approve",
        enabled: false,
        reason: expect.stringContaining("stale")
      }));
      const inspected = await openAgentFlowRunState({ cwd: repo });
      expect(inspected.getRun("stale-ui-approval")?.status).toBe("paused");
      expect(inspected.listApprovals("stale-ui-approval")).toHaveLength(1);
      inspected.close();
    } finally {
      await server.close();
    }
  });

  test("detects same-size approval evidence changes without mutating inspection state", async () => {
    const repo = makeRepo();
    const store = await openAgentFlowRunState({ cwd: repo });
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: byte-guarded-ui-approval
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: approve, type: approval, reviewer: human, artifacts: [release.md] }
`);
    createAgentFlowLifecycleRun(store, { id: "byte-guarded-ui-approval", workflow });
    const release = store.writeArtifact({
      id: "release",
      runId: "byte-guarded-ui-approval",
      path: "release.md",
      kind: "fixture",
      contentType: "text/plain",
      content: "AAAA"
    });
    expect(await executeAgentFlowCommandPipeline(store, "byte-guarded-ui-approval", workflow))
      .toMatchObject({ status: "paused" });
    const initial = buildAgentFlowRunActionSnapshot(store, "byte-guarded-ui-approval");

    fs.writeFileSync(path.join(repo, release.storagePath), "BBBB");
    const changed = buildAgentFlowRunActionSnapshot(store, "byte-guarded-ui-approval");

    expect(changed.guard).not.toBe(initial.guard);
    expect(changed.staleApprovals).toHaveLength(1);
    expect(changed.actions).toContainEqual(expect.objectContaining({ action: "approve", enabled: false }));
    expect(store.listApprovals("byte-guarded-ui-approval")[0]?.status).toBe("requested");
    store.close();
  });

  test("accepts checksum-verified evidence overwritten before approval waiting", async () => {
    const repo = makeRepo();
    const store = await openAgentFlowRunState({ cwd: repo });
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: overwritten-evidence-guard
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: approve, type: approval, reviewer: human, artifacts: [release.md] }
`);
    createAgentFlowLifecycleRun(store, { id: "overwritten-evidence-guard", workflow });
    store.writeArtifact({
      id: "release",
      runId: "overwritten-evidence-guard",
      path: "release.md",
      kind: "fixture",
      contentType: "text/plain",
      content: "First candidate"
    });
    store.writeArtifact({
      id: "release",
      runId: "overwritten-evidence-guard",
      path: "release.md",
      kind: "fixture",
      contentType: "text/plain",
      content: "Retried candidate",
      overwrite: true
    });
    expect(await executeAgentFlowCommandPipeline(store, "overwritten-evidence-guard", workflow))
      .toMatchObject({ status: "paused" });

    const snapshot = buildAgentFlowRunActionSnapshot(store, "overwritten-evidence-guard");

    expect(snapshot.staleApprovals).toEqual([]);
    expect(snapshot.actions).toContainEqual(expect.objectContaining({ action: "approve", enabled: true }));
    expect(snapshot.actions).toContainEqual(expect.objectContaining({ action: "reject", enabled: true }));
    store.close();
  });

  test("clears historical stale warnings after a later approval attempt succeeds", async () => {
    const repo = makeRepo();
    const store = await openAgentFlowRunState({ cwd: repo });
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: rerun-approval-guard
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: approve, type: approval, reviewer: human, artifacts: [release.md] }
`);
    createAgentFlowLifecycleRun(store, { id: "rerun-approval-guard", workflow });
    const evidence = store.writeArtifact({
      id: "release",
      runId: "rerun-approval-guard",
      path: "release.md",
      kind: "fixture",
      contentType: "text/plain",
      content: "Retried candidate"
    });
    const context = { evidence: [{ path: evidence.declaredPath, checksum: evidence.checksum! }] };
    store.upsertApproval({
      id: "approval:approve:attempt-1",
      runId: "rerun-approval-guard",
      stepId: "approve",
      status: "stale",
      decision: "approve",
      context
    });
    store.upsertApproval({
      id: "approval:approve:attempt-2",
      runId: "rerun-approval-guard",
      stepId: "approve",
      status: "approved",
      decision: "approve",
      context
    });

    const snapshot = buildAgentFlowRunActionSnapshot(store, "rerun-approval-guard");

    expect(snapshot.staleApprovals).toEqual([]);
    expect(snapshot.warnings).not.toContainEqual(expect.objectContaining({ code: "action.approval.stale" }));
    store.close();
  });

  test("guards prior approval output bytes while a later interaction is waiting", async () => {
    const repo = makeRepo();
    const store = await openAgentFlowRunState({ cwd: repo });
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: approval-output-guard
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: approve, type: approval, reviewer: human, artifacts: [release.md], output: approval.json }
  - { id: confirm, type: manual_gate, message: Continue?, options: [approve, reject] }
`);
    createAgentFlowLifecycleRun(store, { id: "approval-output-guard", workflow });
    store.writeArtifact({
      id: "release",
      runId: "approval-output-guard",
      path: "release.md",
      kind: "fixture",
      contentType: "text/plain",
      content: "Candidate"
    });
    expect(await executeAgentFlowCommandPipeline(store, "approval-output-guard", workflow))
      .toMatchObject({ status: "paused" });
    const first = buildAgentFlowRunActionSnapshot(store, "approval-output-guard");
    expect(await executeAgentFlowRunAction(store, "approval-output-guard", {
      action: "approve",
      guard: first.guard
    })).toMatchObject({ status: "paused" });
    const initial = buildAgentFlowRunActionSnapshot(store, "approval-output-guard");
    const output = store.getArtifact("approval-output-guard", "approval.json")!;
    const bytes = fs.readFileSync(path.join(repo, output.storagePath));

    fs.writeFileSync(path.join(repo, output.storagePath), Buffer.alloc(bytes.length, 0x20));
    const changed = buildAgentFlowRunActionSnapshot(store, "approval-output-guard");

    expect(changed.guard).not.toBe(initial.guard);
    expect(changed.staleApprovals).toContainEqual(expect.objectContaining({
      id: expect.stringContaining("approval:approve"),
      detected: true
    }));
    expect(changed.actions).toContainEqual(expect.objectContaining({
      action: "approve",
      enabled: false,
      reason: expect.stringContaining("stale")
    }));
    expect(changed.actions).toContainEqual(expect.objectContaining({ action: "cancel", enabled: true }));
    await expect(executeAgentFlowRunAction(store, "approval-output-guard", {
      action: "approve",
      guard: changed.guard
    })).rejects.toMatchObject({ code: "AGENT_FLOW_ACTION_NOT_ALLOWED" });
    expect(store.getRun("approval-output-guard")).toMatchObject({
      status: "paused",
      context: { waiting: expect.objectContaining({ kind: "manual_gate", stepId: "confirm" }) }
    });
    expect(store.listApprovals("approval-output-guard")[0]?.status).toBe("approved");
    store.close();
  });

  test("rejects guards captured before another actor finalizes the active approval", async () => {
    const repo = makeRepo();
    const store = await openAgentFlowRunState({ cwd: repo });
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: concurrent-ui-approval
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: gate, type: manual_gate, message: Ship it?, options: [approve, reject] }
  - { id: done, type: result, status: completed }
`);
    createAgentFlowLifecycleRun(store, { id: "concurrent-ui-approval", workflow });
    expect(await executeAgentFlowCommandPipeline(store, "concurrent-ui-approval", workflow))
      .toMatchObject({ status: "paused" });
    store.close();
    const server = await startAgentFlowRunInspectionApi({
      cwd: repo,
      port: 0,
      token: "inspection-token"
    });
    const headers = {
      [AGENT_FLOW_RUN_INSPECTION_TOKEN_HEADER]: "inspection-token",
      [AGENT_FLOW_RUN_INSPECTION_RUN_ID_HEADER]: encodeURIComponent("concurrent-ui-approval")
    };

    try {
      const initial = await (await fetch(`${server.url}/api/run/actions`, { headers })).json() as { guard: string };
      const writer = await openAgentFlowRunState({ cwd: repo });
      const active = writer.listApprovals("concurrent-ui-approval")[0]!;
      writer.upsertApproval({
        id: active.id,
        runId: active.runId,
        stepId: active.stepId!,
        status: "rejected",
        decision: "reject",
        decidedBy: "another-reviewer"
      });
      writer.close();

      const staleAction = await fetch(`${server.url}/api/run/actions`, {
        method: "POST",
        headers: { ...headers, "content-type": "application/json" },
        body: JSON.stringify({ action: "approve", guard: initial.guard })
      });
      expect(staleAction.status).toBe(409);
      expect(await staleAction.json()).toMatchObject({ code: "AGENT_FLOW_ACTION_STALE" });

      const refreshed = await (await fetch(`${server.url}/api/run/actions`, { headers })).json() as {
        warnings: Array<{ code: string }>;
        actions: Array<{ action: string; enabled: boolean; reason: string | null }>;
      };
      expect(refreshed.warnings).toContainEqual(expect.objectContaining({ code: "action.approval.finalized" }));
      expect(refreshed.actions).toContainEqual(expect.objectContaining({
        action: "approve",
        enabled: false,
        reason: expect.stringContaining("finalized")
      }));
      const inspected = await openAgentFlowRunState({ cwd: repo });
      expect(inspected.getRun("concurrent-ui-approval")?.status).toBe("paused");
      expect(inspected.listApprovals("concurrent-ui-approval")[0]).toMatchObject({
        status: "rejected",
        decision: "reject",
        decidedBy: "another-reviewer"
      });
      inspected.close();
    } finally {
      await server.close();
    }
  });

  test("rechecks approval freshness inside the response transaction", async () => {
    const repo = makeRepo();
    const store = await openAgentFlowRunState({ cwd: repo });
    const competitor = await openAgentFlowRunState({ cwd: repo });
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: transactional-ui-approval
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: gate, type: manual_gate, message: Ship it?, options: [approve, reject] }
  - { id: done, type: result, status: completed }
`);
    createAgentFlowLifecycleRun(store, { id: "transactional-ui-approval", workflow });
    expect(await executeAgentFlowCommandPipeline(store, "transactional-ui-approval", workflow))
      .toMatchObject({ status: "paused" });
    const snapshot = buildAgentFlowRunActionSnapshot(store, "transactional-ui-approval");
    const approval = competitor.listApprovals("transactional-ui-approval")[0]!;
    const withRunStateTransaction = store.withRunStateTransaction.bind(store);
    let finalized = false;
    store.withRunStateTransaction = ((runId, callback) => {
      if (!finalized) {
        finalized = true;
        competitor.upsertApproval({
          id: approval.id,
          runId: approval.runId,
          stepId: approval.stepId!,
          status: "rejected",
          decision: "reject",
          decidedBy: "another-reviewer"
        });
      }
      return withRunStateTransaction(runId, callback);
    }) as typeof store.withRunStateTransaction;

    await expect(executeAgentFlowRunAction(store, "transactional-ui-approval", {
      action: "approve",
      guard: snapshot.guard
    })).rejects.toMatchObject({ code: "AGENT_FLOW_ACTION_STALE" });
    expect(store.getRun("transactional-ui-approval")?.status).toBe("paused");
    expect(competitor.listApprovals("transactional-ui-approval")[0]).toMatchObject({
      status: "rejected",
      decision: "reject",
      decidedBy: "another-reviewer"
    });
    competitor.close();
    store.close();
  });

  test("rechecks action guards before recovering stale execution leases", async () => {
    for (const action of ["approve", "resume"] as AgentFlowRunAction[]) {
      const repo = makeRepo();
      const store = await openAgentFlowRunState({ cwd: repo });
      const competitor = await openAgentFlowRunState({ cwd: repo });
      const workflow = parseAgentFlowWorkflowOrThrow(action === "approve" ? `
name: recovery-guarded-approval
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: gate, type: manual_gate, message: Ship it?, options: [approve, reject] }
  - { id: done, type: result, status: completed }
` : `
name: recovery-guarded-resume
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: done, type: result, status: completed }
`);
      const runId = `recovery-guarded-${action}`;
      createAgentFlowLifecycleRun(store, { id: runId, workflow });
      if (action === "approve") {
        expect(await executeAgentFlowCommandPipeline(store, runId, workflow))
          .toMatchObject({ status: "paused" });
      } else {
        transitionAgentFlowLifecycleRun(store, runId, "pause");
      }
      const snapshot = buildAgentFlowRunActionSnapshot(store, runId);
      const withRunLock = store.withRunLock.bind(store);
      let raced = false;
      let recoveryCalled = false;
      store.withRunLock = ((lockedRunId, operation, callback, options) => {
        if (!raced) {
          raced = true;
          competitor.acquireRunLock(lockedRunId, operation, { ttlMs: 60_000 });
          competitor.transitionRunWithEvent(lockedRunId, {
            status: "running",
            allowedFrom: ["paused"],
            event: { type: "test.concurrent_execution", payload: { action } }
          });
          const current = competitor.getRun(lockedRunId)!;
          const stepId = action === "approve" ? "gate" : "done";
          competitor.updateRun(lockedRunId, {
            currentStepId: stepId,
            context: {
              ...current.context,
              executionRouting: {
                stepAttemptLimits: {},
                visits: { [stepId]: 1 },
                recoveryCycles: {},
                recoveryInvocations: {},
                disagreementEpisodes: {},
                disagreementRounds: {},
                attempts: {}
              },
              executionCheckpoint: { stepId, visit: 1, completedAttempts: 0 }
            }
          });
          competitor.close();
        }
        return withRunLock(lockedRunId, operation, callback, options);
      }) as typeof store.withRunLock;
      const recoverInterruptedRun = store.recoverInterruptedRun.bind(store);
      store.recoverInterruptedRun = ((lock) => {
        recoveryCalled = true;
        return recoverInterruptedRun(lock);
      }) as typeof store.recoverInterruptedRun;

      await expect(executeAgentFlowRunAction(store, runId, {
        action,
        guard: snapshot.guard
      })).rejects.toMatchObject({ code: "AGENT_FLOW_ACTION_STALE" });
      expect(recoveryCalled).toBe(false);
      expect(store.listEvents(runId).map((event) => event.type)).not.toContain("run.execution_recovered");
      store.close();
    }
  });

  test("reports workflow changes raced at lock acquisition as stale actions", async () => {
    for (const action of ["approve", "resume"] as AgentFlowRunAction[]) {
      const repo = makeRepo();
      const store = await openAgentFlowRunState({ cwd: repo });
      const competitor = await openAgentFlowRunState({ cwd: repo });
      const workflow = parseAgentFlowWorkflowOrThrow(action === "approve" ? `
name: workflow-raced-approval
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: gate, type: manual_gate, message: Ship it?, options: [approve, reject] }
  - { id: done, type: result, status: completed }
` : `
name: workflow-raced-resume
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: done, type: result, status: completed }
`);
      const runId = `workflow-raced-${action}`;
      createAgentFlowLifecycleRun(store, { id: runId, workflow });
      if (action === "approve") {
        expect(await executeAgentFlowCommandPipeline(store, runId, workflow))
          .toMatchObject({ status: "paused" });
      } else {
        transitionAgentFlowLifecycleRun(store, runId, "pause");
      }
      const snapshot = buildAgentFlowRunActionSnapshot(store, runId);
      const changedWorkflow = { ...workflow, description: "Changed by another writer" };
      const withRunLock = store.withRunLock.bind(store);
      let raced = false;
      store.withRunLock = ((lockedRunId, operation, callback, options) => {
        if (!raced) {
          raced = true;
          const current = competitor.getRun(lockedRunId)!;
          competitor.updateRun(lockedRunId, {
            context: {
              ...current.context,
              workflow: changedWorkflow as unknown as typeof current.context.workflow
            }
          });
        }
        return withRunLock(lockedRunId, operation, callback, options);
      }) as typeof store.withRunLock;

      await expect(executeAgentFlowRunAction(store, runId, {
        action,
        guard: snapshot.guard
      })).rejects.toMatchObject({ code: "AGENT_FLOW_ACTION_STALE" });
      expect(store.getRun(runId)?.status).toBe("paused");
      competitor.close();
      store.close();
    }
  });

  test("maps guarded disagreement decisions to supported persisted outcomes", async () => {
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: ui-disagreement
version: 1
style: collaborative
maturity: experimental
collaboration:
  enabled: true
  max_review_cycles: 1
  on_disagreement: ask_user
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
    outputs: [reviews/review.json]
    then: route
  - id: route
    type: condition
    branches:
      - { if: 'artifacts.reviews.review.status == "approved"', then: done }
      - { if: 'artifacts.reviews.review.status == "changes_requested"', then: revise }
    else: failed
  - { id: revise, type: command, command: "true", then: review }
  - { id: done, type: result, status: completed }
  - { id: failed, type: result, status: failed }
`);
    for (const action of ["approve", "reject"] as const) {
      const repo = makeRepo();
      const store = await openAgentFlowRunState({ cwd: repo });
      const runId = `ui-disagreement-${action}`;
      createAgentFlowLifecycleRun(store, { id: runId, workflow });
      store.writeArtifact({
        id: "implementation",
        runId,
        path: "implementation.md",
        kind: "fixture",
        contentType: "text/markdown",
        content: "Implementation"
      });
      const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => ({
        outputs: Object.fromEntries(request.outputs.map((output) => [output, JSON.stringify({
          status: "changes_requested",
          findings: [{ summary: "Revise the implementation." }],
          summary: "Changes requested."
        })]))
      }));
      expect(await executeAgentFlowCommandPipeline(store, runId, workflow, undefined, providers))
        .toMatchObject({ status: "paused" });
      const snapshot = buildAgentFlowRunActionSnapshot(store, runId);
      expect(snapshot.waiting?.kind).toBe("disagreement");
      expect(snapshot.actions).toContainEqual(expect.objectContaining({ action, enabled: true }));

      const result = await executeAgentFlowRunAction(store, runId, {
        action,
        guard: snapshot.guard
      }, { sessionProviders: providers });

      expect(result.status).toBe(action === "approve" ? "completed" : "paused");
      expect(store.listEvents(runId)).toContainEqual(expect.objectContaining({
        type: "collaboration.disagreement.resolved",
        payload: expect.objectContaining({
          outcome: action === "approve" ? "approve" : "request_changes",
          decidedBy: "local-ui"
        })
      }));
      store.close();
    }
  });

  test("rejects changed disagreement evidence before mutating the paused run", async () => {
    const repo = makeRepo();
    const store = await openAgentFlowRunState({ cwd: repo });
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: stale-ui-disagreement
version: 1
style: collaborative
maturity: experimental
collaboration:
  enabled: true
  max_review_cycles: 1
  on_disagreement: ask_user
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
    outputs: [reviews/review.json]
    then: route
  - id: route
    type: condition
    branches:
      - { if: 'artifacts.reviews.review.status == "approved"', then: done }
      - { if: 'artifacts.reviews.review.status == "changes_requested"', then: revise }
    else: failed
  - { id: revise, type: command, command: "true", then: review }
  - { id: done, type: result, status: completed }
  - { id: failed, type: result, status: failed }
`);
    const runId = "stale-ui-disagreement";
    createAgentFlowLifecycleRun(store, { id: runId, workflow });
    const artifact = store.writeArtifact({
      id: "implementation",
      runId,
      path: "implementation.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Original implementation"
    });
    const providers = createAgentFlowSessionProviderRegistry().register("fixture", (request) => ({
      outputs: Object.fromEntries(request.outputs.map((output) => [output, JSON.stringify({
        status: "changes_requested",
        findings: [{ summary: "Revise the implementation." }],
        summary: "Changes requested."
      })]))
    }));
    expect(await executeAgentFlowCommandPipeline(store, runId, workflow, undefined, providers))
      .toMatchObject({ status: "paused" });
    const snapshot = buildAgentFlowRunActionSnapshot(store, runId);

    fs.writeFileSync(path.join(repo, artifact.storagePath), "Changed implementation");
    const refreshed = buildAgentFlowRunActionSnapshot(store, runId);
    expect(refreshed.guard).not.toBe(snapshot.guard);
    expect(refreshed.warnings).toContainEqual(expect.objectContaining({
      code: "action.disagreement.evidence_stale"
    }));
    expect(refreshed.actions.every((candidate) => !candidate.enabled)).toBe(true);

    await expect(executeAgentFlowRunAction(store, runId, {
      action: "approve",
      guard: snapshot.guard
    }, { sessionProviders: providers })).rejects.toMatchObject({ code: "AGENT_FLOW_ACTION_STALE" });
    expect(store.getRun(runId)).toMatchObject({
      status: "paused",
      context: { waiting: expect.objectContaining({ kind: "disagreement", stepId: "review" }) }
    });
    expect(store.listEvents(runId).some((event) => event.type === "collaboration.disagreement.evidence_changed"))
      .toBe(false);
    store.close();
  });

  test("guards the complete persisted workflow even when timestamps do not advance", async () => {
    const repo = makeRepo();
    const store = await openAgentFlowRunState({
      cwd: repo,
      now: () => "2026-08-23T12:00:00.000Z"
    });
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: fixed-clock-ui-approval
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: gate, type: manual_gate, message: Ship it?, options: [approve, reject] }
  - { id: done, type: result, status: completed }
`);
    createAgentFlowLifecycleRun(store, { id: "fixed-clock-ui-approval", workflow });
    expect(await executeAgentFlowCommandPipeline(store, "fixed-clock-ui-approval", workflow))
      .toMatchObject({ status: "paused" });
    const initial = buildAgentFlowRunActionSnapshot(store, "fixed-clock-ui-approval");
    const changedWorkflow = parseAgentFlowWorkflowOrThrow(`
name: fixed-clock-ui-approval
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: gate, type: manual_gate, message: Ship it?, options: [approve, reject] }
  - { id: changed, type: result, status: completed }
`);
    const run = store.getRun("fixed-clock-ui-approval")!;
    store.updateRun(run.id, {
      context: { ...run.context, workflow: changedWorkflow as unknown as typeof run.context.workflow }
    });
    const refreshed = buildAgentFlowRunActionSnapshot(store, run.id);

    expect(refreshed.updatedAt).toBe(initial.updatedAt);
    expect(refreshed.guard).not.toBe(initial.guard);
    await expect(executeAgentFlowRunAction(store, run.id, {
      action: "approve",
      guard: initial.guard
    })).rejects.toMatchObject({ code: "AGENT_FLOW_ACTION_STALE" });
    store.close();
  });

  test("disables actions for malformed or incomplete persisted waiting state", async () => {
    const repo = makeRepo();
    const store = await openAgentFlowRunState({ cwd: repo });
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: malformed-ui-waiting
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: details, type: input_request, question: What changed?, save_as: answer.txt }
`);
    createAgentFlowLifecycleRun(store, { id: "malformed-ui-waiting", workflow });
    expect(await executeAgentFlowCommandPipeline(store, "malformed-ui-waiting", workflow))
      .toMatchObject({ status: "paused" });
    const run = store.getRun("malformed-ui-waiting")!;
    const waiting = run.context.waiting as Record<string, unknown>;
    const { attempt: _attempt, ...incompleteWaiting } = waiting;
    store.updateRun(run.id, {
      context: { ...run.context, waiting: incompleteWaiting as typeof run.context.waiting }
    });
    const incomplete = buildAgentFlowRunActionSnapshot(store, run.id);
    expect(incomplete.warnings).toContainEqual(expect.objectContaining({ code: "action.waiting.invalid" }));
    expect(incomplete.actions.every((candidate) => !candidate.enabled)).toBe(true);

    store.updateRun(run.id, { context: { ...run.context, waiting: "malformed" } });
    const malformed = buildAgentFlowRunActionSnapshot(store, run.id);
    expect(malformed.actions).toContainEqual(expect.objectContaining({ action: "resume", enabled: false }));
    expect(malformed.actions.every((candidate) => !candidate.enabled)).toBe(true);
    store.close();
  });

  test("keeps malformed approval evidence inspectable while disabling actions", async () => {
    const repo = makeRepo();
    const store = await openAgentFlowRunState({ cwd: repo });
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: malformed-ui-approval
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: approve, type: approval, reviewer: human, artifacts: [release.md] }
`);
    createAgentFlowLifecycleRun(store, { id: "malformed-ui-approval", workflow });
    store.writeArtifact({
      id: "release",
      runId: "malformed-ui-approval",
      path: "release.md",
      kind: "fixture",
      contentType: "text/markdown",
      content: "Candidate"
    });
    expect(await executeAgentFlowCommandPipeline(store, "malformed-ui-approval", workflow))
      .toMatchObject({ status: "paused" });
    const active = store.listApprovals("malformed-ui-approval")[0]!;
    store.upsertApproval({
      id: active.id,
      runId: active.runId,
      stepId: active.stepId!,
      status: "requested",
      context: { evidence: [{ path: "../outside", checksum: "invalid" }] }
    });
    store.close();
    const server = await startAgentFlowRunInspectionApi({
      cwd: repo,
      port: 0,
      token: "inspection-token"
    });
    const headers = {
      [AGENT_FLOW_RUN_INSPECTION_TOKEN_HEADER]: "inspection-token",
      [AGENT_FLOW_RUN_INSPECTION_RUN_ID_HEADER]: encodeURIComponent("malformed-ui-approval")
    };

    try {
      const response = await fetch(`${server.url}/api/run/actions`, { headers });
      expect(response.status).toBe(200);
      const snapshot = await response.json() as {
        warnings: Array<{ code: string }>;
        actions: Array<{ enabled: boolean }>;
      };
      expect(snapshot.warnings).toContainEqual(expect.objectContaining({
        code: "action.approval.evidence_invalid"
      }));
      expect(snapshot.actions.every((candidate) => !candidate.enabled)).toBe(true);
    } finally {
      await server.close();
    }

    const malformedStore = await openAgentFlowRunState({ cwd: repo });
    const malformedActive = malformedStore.listApprovals("malformed-ui-approval")[0]!;
    malformedStore.upsertApproval({
      id: malformedActive.id,
      runId: malformedActive.runId,
      stepId: malformedActive.stepId!,
      status: "requested",
      context: { evidence: "malformed" }
    });
    const malformed = buildAgentFlowRunActionSnapshot(malformedStore, "malformed-ui-approval");
    expect(malformed.warnings).toContainEqual(expect.objectContaining({
      code: "action.approval.evidence_invalid"
    }));
    expect(malformed.actions.every((candidate) => !candidate.enabled)).toBe(true);
    malformedStore.close();
  });

  test("supports guarded input, pause, plain resume, and cancel action flows", async () => {
    const repo = makeRepo();
    const store = await openAgentFlowRunState({ cwd: repo });
    const inputWorkflow = parseAgentFlowWorkflowOrThrow(`
name: ui-input
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: details, type: input_request, question: What changed?, save_as: answer.json }
  - { id: done, type: result, status: completed }
`);
    createAgentFlowLifecycleRun(store, { id: "ui-input", workflow: inputWorkflow });
    expect(await executeAgentFlowCommandPipeline(store, "ui-input", inputWorkflow))
      .toMatchObject({ status: "paused" });
    createAgentFlowLifecycleRun(store, { id: "ui-nonfinite-input", workflow: inputWorkflow });
    expect(await executeAgentFlowCommandPipeline(store, "ui-nonfinite-input", inputWorkflow))
      .toMatchObject({ status: "paused" });
    createAgentFlowLifecycleRun(store, { id: "ui-direct-nonfinite-input", workflow: inputWorkflow });
    expect(await executeAgentFlowCommandPipeline(store, "ui-direct-nonfinite-input", inputWorkflow))
      .toMatchObject({ status: "paused" });
    const directSnapshot = buildAgentFlowRunActionSnapshot(store, "ui-direct-nonfinite-input");
    await expect(executeAgentFlowRunAction(store, "ui-direct-nonfinite-input", {
      action: "provide_input",
      guard: directSnapshot.guard,
      answer: { nested: [Number.POSITIVE_INFINITY] }
    })).rejects.toMatchObject({ code: "AGENT_FLOW_ACTION_BODY_INVALID", status: 400 });
    expect(store.getRun("ui-direct-nonfinite-input")?.status).toBe("paused");
    expect(store.getArtifact("ui-direct-nonfinite-input", "answer.json")).toBeNull();
    const lifecycleWorkflow = parseAgentFlowWorkflowOrThrow(`
name: ui-lifecycle
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: done, type: result, status: completed }
`);
    createAgentFlowLifecycleRun(store, { id: "ui-lifecycle", workflow: lifecycleWorkflow });
    createAgentFlowLifecycleRun(store, { id: "ui-cancel", workflow: lifecycleWorkflow });
    store.close();
    const server = await startAgentFlowRunInspectionApi({
      cwd: repo,
      port: 0,
      token: "inspection-token"
    });
    const tokenHeader = { [AGENT_FLOW_RUN_INSPECTION_TOKEN_HEADER]: "inspection-token" };
    const action = async (runId: string, actionName: string, extra: Record<string, unknown> = {}) => {
      const headers = {
        ...tokenHeader,
        [AGENT_FLOW_RUN_INSPECTION_RUN_ID_HEADER]: encodeURIComponent(runId)
      };
      const snapshot = await (await fetch(`${server.url}/api/run/actions`, { headers })).json() as { guard: string };
      return fetch(`${server.url}/api/run/actions`, {
        method: "POST",
        headers: { ...headers, "content-type": "application/json" },
        body: JSON.stringify({ action: actionName, guard: snapshot.guard, ...extra })
      });
    };

    try {
      const provided = await action("ui-input", "provide_input", { answer: { ticket: "AF-68" } });
      expect(provided.status).toBe(200);
      expect(await provided.json()).toMatchObject({ status: "completed" });

      const nonFiniteHeaders = {
        ...tokenHeader,
        [AGENT_FLOW_RUN_INSPECTION_RUN_ID_HEADER]: encodeURIComponent("ui-nonfinite-input")
      };
      const nonFiniteSnapshot = await (await fetch(`${server.url}/api/run/actions`, {
        headers: nonFiniteHeaders
      })).json() as { guard: string };
      const nonFinite = await fetch(`${server.url}/api/run/actions`, {
        method: "POST",
        headers: { ...nonFiniteHeaders, "content-type": "application/json" },
        body: `{"action":"provide_input","guard":${JSON.stringify(nonFiniteSnapshot.guard)},"answer":{"nested":[1e400]}}`
      });
      expect(nonFinite.status).toBe(400);
      expect(await nonFinite.json()).toMatchObject({ code: "AGENT_FLOW_ACTION_BODY_INVALID" });

      const deeplyNested = `${"[".repeat(64)}null${"]".repeat(64)}`;
      const excessiveDepth = await fetch(`${server.url}/api/run/actions`, {
        method: "POST",
        headers: { ...nonFiniteHeaders, "content-type": "application/json" },
        body: `{"action":"provide_input","guard":${JSON.stringify(nonFiniteSnapshot.guard)},"answer":${deeplyNested}}`
      });
      expect(excessiveDepth.status).toBe(400);
      expect(await excessiveDepth.json()).toMatchObject({ code: "AGENT_FLOW_ACTION_BODY_INVALID" });

      const paused = await action("ui-lifecycle", "pause");
      expect(paused.status).toBe(200);
      expect(await paused.json()).toMatchObject({ status: "paused" });
      const resumed = await action("ui-lifecycle", "resume");
      expect(resumed.status).toBe(200);
      expect(await resumed.json()).toMatchObject({ status: "completed", completedSteps: ["done"] });
      const cancelled = await action("ui-cancel", "cancel");
      expect(cancelled.status).toBe(200);
      expect(await cancelled.json()).toMatchObject({ status: "cancelled" });

      const inspected = await openAgentFlowRunState({ cwd: repo });
      expect(JSON.parse(inspected.readArtifact("ui-input", "answer.json").content.toString()))
        .toEqual({ ticket: "AF-68" });
      expect(inspected.getRun("ui-nonfinite-input")?.status).toBe("paused");
      expect(inspected.getArtifact("ui-nonfinite-input", "answer.json")).toBeNull();
      expect(inspected.listEvents("ui-lifecycle").map((event) => event.type)).toEqual(expect.arrayContaining([
        "run.pause", "run.resume", "run.started", "run.completed"
      ]));
      expect(inspected.listEvents("ui-cancel").map((event) => event.type)).toContain("run.cancel");
      inspected.close();
    } finally {
      await server.close();
    }
  });

  test("rechecks plain resume freshness inside its transition transaction", async () => {
    const repo = makeRepo();
    const store = await openAgentFlowRunState({ cwd: repo });
    const competitor = await openAgentFlowRunState({ cwd: repo });
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: transactional-ui-resume
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: done, type: result, status: completed }
`);
    createAgentFlowLifecycleRun(store, { id: "transactional-ui-resume", workflow });
    transitionAgentFlowLifecycleRun(store, "transactional-ui-resume", "pause");
    const snapshot = buildAgentFlowRunActionSnapshot(store, "transactional-ui-resume");
    const withRunStateTransaction = store.withRunStateTransaction.bind(store);
    let cancelled = false;
    store.withRunStateTransaction = ((runId, callback) => {
      if (!cancelled) {
        cancelled = true;
        transitionAgentFlowLifecycleRun(competitor, runId, "cancel");
      }
      return withRunStateTransaction(runId, callback);
    }) as typeof store.withRunStateTransaction;

    await expect(executeAgentFlowRunAction(store, "transactional-ui-resume", {
      action: "resume",
      guard: snapshot.guard
    })).rejects.toMatchObject({ code: "AGENT_FLOW_ACTION_STALE" });
    expect(store.getRun("transactional-ui-resume")?.status).toBe("cancelled");
    competitor.close();
    store.close();
  });

  test("serves a secure run inspection UI with fragment-held credentials", async () => {
    const repo = makeRepo();
    const store = await openAgentFlowRunState({ cwd: repo });
    createInspectableRun(store, "run-ui");
    store.appendRunEvent("run-ui", { type: "inspection.page.one", payload: { page: 1 } });
    store.appendRunEvent("run-ui", { type: "inspection.page.two", payload: { page: 2 } });
    store.close();
    const server = await startAgentFlowRunInspectionApi({
      cwd: repo,
      port: 0,
      token: "inspection-token&with=specials"
    });

    try {
      expect(server.uiUrl).toBe(`${server.url}/#token=inspection-token%26with%3Dspecials`);

      const page = await fetch(server.url);
      expect(page.status).toBe(200);
      expect(page.headers.get("content-type")).toBe("text/html; charset=utf-8");
      expect(page.headers.get("cache-control")).toBe("no-store");
      expect(page.headers.get("content-security-policy")).toContain("script-src 'self'");
      expect(page.headers.get("content-security-policy")).toContain("form-action 'none'");
      expect(page.headers.get("referrer-policy")).toBe("no-referrer");
      const html = await page.text();
      expect(html).toContain("Run inspector");
      expect(html).toContain("Select a run");
      expect(html).not.toContain("inspection-token");
      expect(html).not.toContain("<form");
      expect(html).not.toContain('name="token"');

      const stylesheet = await fetch(`${server.url}/inspection.css`);
      expect(stylesheet.status).toBe(200);
      expect(stylesheet.headers.get("content-type")).toBe("text/css; charset=utf-8");
      const css = await stylesheet.text();
      expect(css).toContain(".timeline-entry");
      expect(css).toContain(".action-panel");
      expect(css).toContain(".token-panel #token-form { max-width: 520px; }");
      expect(css).not.toContain(".token-panel form { max-width: 520px; }");
      expect(css).toContain(".code-toolbar { display: flex; justify-content: flex-end;");
      expect(css).not.toContain(".code-block .copy-button { position: absolute;");

      const script = await fetch(`${server.url}/inspection.js`);
      expect(script.status).toBe(200);
      expect(script.headers.get("content-type")).toBe("text/javascript; charset=utf-8");
      const javascript = await script.text();
      expect(javascript).toContain('var TOKEN_HEADER = "x-agent-flow-token"');
      expect(javascript).toContain('var RUN_ID_HEADER = "x-agent-flow-run-id"');
      expect(javascript).toContain("EVENT_PAGE_SIZE = 100");
      expect(javascript).toContain("requestId !== state.detailRequestId");
      expect(javascript).toContain('api("/api/run?section=overview"');
      expect(javascript).toContain('api("/api/run/actions"');
      expect(javascript).toContain("async function performAction(snapshot");
      expect(javascript).toContain("window.confirm(availability.confirmation)");
      const actionParserSource = javascript.slice(
        javascript.indexOf("function assertFiniteActionAnswer"),
        javascript.indexOf("function summaryGrid")
      );
      const parseActionAnswer = vm.runInNewContext(
        `(function () { ${actionParserSource}; return parseActionAnswer; })()`
      ) as (value: string) => unknown;
      expect(parseActionAnswer('{"nested":[42]}')).toEqual({ nested: [42] });
      expect(parseActionAnswer("not JSON")).toBe("not JSON");
      expect(() => parseActionAnswer('{"nested":[1e400]}')).toThrow("Input answer JSON numbers must be finite.");
      expect(javascript).toContain("async function loadSection(id)");
      expect(javascript).toContain("appendSectionPage(id, view");
      expect(javascript).not.toContain("model.events");
      expect(javascript).toContain("navigator.clipboard.writeText");
      expect(javascript).toContain('document.createElement(tag)');
      expect(javascript).toContain('history.replaceState(null, "", location.pathname + location.search)');
      expect(javascript).not.toContain('location.hash = "token="');
      expect(javascript).not.toContain("innerHTML");

      const detailHeaders = {
        [AGENT_FLOW_RUN_INSPECTION_TOKEN_HEADER]: "inspection-token&with=specials",
        [AGENT_FLOW_RUN_INSPECTION_RUN_ID_HEADER]: encodeURIComponent("run-ui")
      };
      const overview = await fetch(`${server.url}/api/run?section=overview`, { headers: detailHeaders });
      expect(overview.status).toBe(200);
      const overviewBody = await overview.json();
      expect(overviewBody).toMatchObject({ run: { id: "run-ui" } });
      expect(overviewBody).not.toHaveProperty("state");
      expect(overviewBody).not.toHaveProperty("events");
      expect(overviewBody).not.toHaveProperty("artifacts");

      const stateSection = await fetch(`${server.url}/api/run?section=state`, { headers: detailHeaders });
      expect(stateSection.status).toBe(200);
      expect(await stateSection.json()).toMatchObject({ state: { inputs: { ticket: "AF-58" } } });

      const firstPage = await fetch(`${server.url}/api/run?section=events&offset=0&limit=1`, {
        headers: detailHeaders
      });
      expect(firstPage.status).toBe(200);
      expect(await firstPage.json()).toMatchObject({
        section: "events",
        items: [{ sequence: 1, type: "run.created" }],
        offset: 0,
        nextOffset: 1
      });

      const secondPage = await fetch(`${server.url}/api/run?section=events&offset=1&limit=1`, {
        headers: detailHeaders
      });
      expect(secondPage.status).toBe(200);
      expect(await secondPage.json()).toMatchObject({
        section: "events",
        items: [{ sequence: 2, type: "inspection.page.one" }],
        offset: 1,
        nextOffset: 2
      });

      const badPage = await fetch(`${server.url}/api/run?section=events&limit=201`, { headers: detailHeaders });
      expect(badPage.status).toBe(400);
      expect(await badPage.json()).toMatchObject({ code: "AGENT_FLOW_INSPECTION_BAD_REQUEST" });

      const response = await fetch(`${server.url}/api/runs/run-ui`, {
        headers: { [AGENT_FLOW_RUN_INSPECTION_TOKEN_HEADER]: "inspection-token&with=specials" }
      });
      expect(response.status).toBe(200);
    } finally {
      await server.close();
    }
  });

  test("preserves encoded dot-segment run IDs in detail routes", async () => {
    const repo = makeRepo();
    const store = await openAgentFlowRunState({ cwd: repo });
    for (const id of [".", ".."]) {
      createAgentFlowLifecycleRun(store, {
        id,
        workflow: parseAgentFlowWorkflowOrThrow(`
name: dot-segment
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: inspect, type: command, command: echo inspect }
`)
      });
    }
    store.close();
    const server = await startAgentFlowRunInspectionApi({
      cwd: repo,
      port: 0,
      token: "inspection-token"
    });

    try {
      for (const [encoded, id] of [["%2e", "."], ["%2e%2e", ".."]] as const) {
        const response = await rawGet(server.url, `/api/runs/${encoded}`, {
          [AGENT_FLOW_RUN_INSPECTION_TOKEN_HEADER]: "inspection-token"
        });
        expect(response.status).toBe(200);
        expect(JSON.parse(response.body)).toMatchObject({ run: { id } });

        const browserRoute = await fetch(`${server.url}/api/run`, {
          headers: {
            [AGENT_FLOW_RUN_INSPECTION_TOKEN_HEADER]: "inspection-token",
            [AGENT_FLOW_RUN_INSPECTION_RUN_ID_HEADER]: encoded
          }
        });
        expect(browserRoute.status).toBe(200);
        expect(await browserRoute.json()).toMatchObject({ run: { id } });
      }

      const missingRunId = await fetch(`${server.url}/api/run`, {
        headers: { [AGENT_FLOW_RUN_INSPECTION_TOKEN_HEADER]: "inspection-token" }
      });
      expect(missingRunId.status).toBe(400);
      expect(await missingRunId.json()).toMatchObject({ code: "AGENT_FLOW_INSPECTION_BAD_REQUEST" });
    } finally {
      await server.close();
    }
  });

  test("does not reconcile artifacts or invalidate approvals while inspecting", async () => {
    const repo = makeRepo();
    let now = "2026-08-22T10:00:00.000Z";
    const store = await openAgentFlowRunState({ cwd: repo, now: () => now });
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: read-only-inspection
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: inspect, type: command, command: echo inspect }
`);
    createAgentFlowLifecycleRun(store, { id: "read-only", workflow });
    const evidence = store.writeArtifact({
      id: "evidence",
      runId: "read-only",
      path: "evidence.txt",
      kind: "output",
      contentType: "text/plain",
      content: "before"
    });
    store.upsertApproval({
      id: "approval",
      runId: "read-only",
      status: "approved",
      decision: "ship",
      context: { evidence: [{ path: evidence.declaredPath, checksum: evidence.checksum! }] }
    });
    fs.writeFileSync(path.join(repo, evidence.storagePath), "after");
    now = "2026-08-22T11:00:00.000Z";

    const model = buildAgentFlowRunInspectionModel(store, "read-only");

    expect(model.artifacts).toEqual([
      expect.objectContaining({
        declaredPath: "evidence.txt",
        status: "stale",
        checkedAt: "2026-08-22T11:00:00.000Z",
        updatedAt: evidence.updatedAt
      })
    ]);
    expect(model.warnings).toContainEqual(expect.objectContaining({ code: "run.artifact.stale" }));
    expect(store.listArtifactMetadata("read-only")).toEqual([
      expect.objectContaining({ declaredPath: "evidence.txt", status: "available", checkedAt: evidence.checkedAt })
    ]);
    expect(store.listApprovals("read-only")).toEqual([
      expect.objectContaining({ id: "approval", status: "approved" })
    ]);
    expect(store.listEvents("read-only").map((event) => event.type)).toEqual(["run.created"]);
    store.close();
  });

  test("does not hash metadata-only artifact content during inspection", async () => {
    const repo = makeRepo();
    const store = await openAgentFlowRunState({ cwd: repo });
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: metadata-only-artifact
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: inspect, type: command, command: echo inspect }
`);
    createAgentFlowLifecycleRun(store, { id: "metadata-only", workflow });
    const artifact = store.writeArtifact({
      id: "output",
      runId: "metadata-only",
      path: "output.txt",
      kind: "output",
      contentType: "text/plain",
      content: "before"
    });
    fs.writeFileSync(path.join(repo, artifact.storagePath), "after!");

    const model = buildAgentFlowRunInspectionModel(store, "metadata-only");

    expect(model.artifacts).toEqual([
      expect.objectContaining({ declaredPath: "output.txt", status: "available" })
    ]);
    expect(model.warnings).not.toContainEqual(expect.objectContaining({ code: "run.artifact.stale" }));
    store.close();
  });

  test("reads a restored document without persisting its recovered status", async () => {
    const repo = makeRepo();
    const store = await openAgentFlowRunState({ cwd: repo });
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: restored-document
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: inspect, type: command, command: echo inspect }
`);
    createAgentFlowLifecycleRun(store, { id: "restored-document", workflow });
    const content = JSON.stringify({
      decision_id: "decision:inspect",
      owner: "owner",
      topic: "Inspection",
      rationale_summary: "Inspect restored state.",
      consulted: [],
      approved_by: [],
      artifacts: ["evidence.txt"],
      created_at: "2026-08-22T00:00:00.000Z"
    });
    const artifact = store.writeArtifact({
      id: "decision",
      runId: "restored-document",
      path: "decision.json",
      kind: "decision_record",
      contentType: "application/json",
      content
    });
    const artifactPath = path.join(repo, artifact.storagePath);
    fs.writeFileSync(artifactPath, "x".repeat(Buffer.byteLength(content)));
    expect(store.listArtifacts("restored-document")[0]?.status).toBe("stale");
    fs.writeFileSync(artifactPath, content);

    const model = buildAgentFlowRunInspectionModel(store, "restored-document");

    expect(model.artifacts[0]?.status).toBe("available");
    expect(model.decisions[0]).toMatchObject({
      document: expect.objectContaining({ decision_id: "decision:inspect" }),
      error: null
    });
    expect(store.listArtifactMetadata("restored-document")[0]?.status).toBe("stale");
    store.close();
  });

  test("migrates legacy run-state before serving through read-only connections", async () => {
    const repo = makeRepo();
    const store = await openAgentFlowRunState({ cwd: repo });
    createInspectableRun(store, "legacy-database");
    const databasePath = store.databasePath;
    store.close();
    const database = new Database(databasePath);
    database.exec(`
      DROP INDEX run_locks_expiry_lookup;
      DROP TABLE run_locks;
      UPDATE run_state_metadata SET value = '4' WHERE key = 'schema_version';
    `);
    database.close();

    const server = await startAgentFlowRunInspectionApi({
      cwd: repo,
      port: 0,
      token: "inspection-token"
    });

    try {
      const response = await fetch(`${server.url}/api/runs`, {
        headers: { [AGENT_FLOW_RUN_INSPECTION_TOKEN_HEADER]: "inspection-token" }
      });
      expect(response.status).toBe(200);
      expect(await response.json()).toMatchObject({ runs: [{ id: "legacy-database" }] });
      const migrated = new Database(databasePath, { readonly: true });
      expect(migrated.query("SELECT value FROM run_state_metadata WHERE key = 'schema_version'").get())
        .toEqual({ value: "5" });
      migrated.close();
    } finally {
      await server.close();
    }
  });

  test("reports unreadable and missing artifacts without mutating the filesystem", async () => {
    const repo = makeRepo();
    const store = await openAgentFlowRunState({ cwd: repo });
    const workflow = parseAgentFlowWorkflowOrThrow(`
name: unavailable-artifacts
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: inspect, type: command, command: echo inspect }
`);
    createAgentFlowLifecycleRun(store, { id: "unavailable", workflow });
    const unreadable = store.writeArtifact({
      id: "unreadable",
      runId: "unavailable",
      path: "decisions/unreadable.json",
      kind: "decision_record",
      contentType: "application/json",
      content: "{}"
    });
    store.upsertArtifact({
      id: "missing",
      runId: "unavailable",
      path: "decisions/missing.json",
      kind: "decision_record",
      contentType: "application/json"
    });
    const unreadablePath = path.join(repo, unreadable.storagePath);
    fs.chmodSync(unreadablePath, 0);
    const before = filesystemEntries(repo);

    try {
      const model = buildAgentFlowRunInspectionModel(store, "unavailable");
      expect(model.artifacts).toEqual(expect.arrayContaining([
        expect.objectContaining({ declaredPath: "decisions/missing.json", status: "missing" }),
        expect.objectContaining({ declaredPath: "decisions/unreadable.json", status: "available" })
      ]));
      expect(model.warnings).toEqual(expect.arrayContaining([
        expect.objectContaining({ code: "run.artifact.missing", path: "decisions/missing.json" }),
        expect.objectContaining({ code: "run.decision.unavailable", path: "decisions/missing.json" }),
        expect.objectContaining({ code: "run.decision.unavailable", path: "decisions/unreadable.json" })
      ]));
      expect(model.warnings).not.toContainEqual(expect.objectContaining({
        code: "run.artifact.stale",
        path: "decisions/unreadable.json"
      }));
      expect(filesystemEntries(repo)).toEqual(before);
    } finally {
      fs.chmodSync(unreadablePath, 0o600);
      store.close();
    }
  });

  test("uses the configured run-state database without creating the default", async () => {
    const repo = makeRepo();
    const databasePath = ".agent-flow/custom.sqlite";
    const store = await openAgentFlowRunState({ cwd: repo, databasePath });
    createInspectableRun(store, "custom-database");
    store.close();
    const server = await startAgentFlowRunInspectionApi({
      cwd: repo,
      databasePath,
      port: 0,
      token: "inspection-token"
    });

    try {
      const response = await fetch(`${server.url}/api/runs`, {
        headers: { [AGENT_FLOW_RUN_INSPECTION_TOKEN_HEADER]: "inspection-token" }
      });
      expect(response.status).toBe(200);
      expect(await response.json()).toMatchObject({ runs: [{ id: "custom-database" }] });
      expect(fs.existsSync(path.join(repo, ".agent-flow/agent-flow.sqlite"))).toBe(false);
    } finally {
      await server.close();
    }
  });

  test("pins the prepared repository when callers mutate startup options", async () => {
    const firstRepo = makeRepo();
    const firstStore = await openAgentFlowRunState({ cwd: firstRepo });
    createInspectableRun(firstStore, "first-repository");
    firstStore.close();
    const secondRepo = makeRepo();
    const secondStore = await openAgentFlowRunState({ cwd: secondRepo });
    createInspectableRun(secondStore, "second-repository");
    secondStore.close();
    const options = {
      cwd: firstRepo,
      port: 0,
      token: "inspection-token"
    };
    const server = await startAgentFlowRunInspectionApi(options);
    options.cwd = secondRepo;

    try {
      const response = await fetch(`${server.url}/api/runs`, {
        headers: { [AGENT_FLOW_RUN_INSPECTION_TOKEN_HEADER]: "inspection-token" }
      });
      expect(response.status).toBe(200);
      expect(await response.json()).toMatchObject({ runs: [{ id: "first-repository" }] });
    } finally {
      await server.close();
    }
  });

  test("rejects public bind addresses and empty configured tokens", async () => {
    await expect(startAgentFlowRunInspectionApi({ host: "0.0.0.0", port: 0 }))
      .rejects.toThrow("numeric loopback");
    await expect(startAgentFlowRunInspectionApi({ host: "localhost", port: 0 }))
      .rejects.toThrow("numeric loopback");
    await expect(startAgentFlowRunInspectionApi({ host: "localhost.", port: 0 }))
      .rejects.toThrow("numeric loopback");
    await expect(startAgentFlowRunInspectionApi({ token: "", port: 0 }))
      .rejects.toThrow("must be non-empty");
    await expect(startAgentFlowRunInspectionApi({ token: " inspection-token ", port: 0 }))
      .rejects.toThrow("header-safe ASCII");
    await expect(startAgentFlowRunInspectionApi({ token: "inspection-🔑", port: 0 }))
      .rejects.toThrow("header-safe ASCII");
  });
});

type Store = Awaited<ReturnType<typeof openAgentFlowRunState>>;

function createInspectableRun(store: Store, id: string): void {
  const workflow = parseAgentFlowWorkflowOrThrow(`
name: inspectable
version: 1
style: pipeline
maturity: experimental
steps:
  - { id: inspect, type: command, command: echo inspect }
`);
  createAgentFlowLifecycleRun(store, {
    id,
    workflow,
    inputs: { ticket: "AF-58" }
  });
  store.upsertStep({
    runId: id,
    stepId: "inspect",
    status: "failed",
    input: { command: "echo inspect" },
    error: { message: "Inspection failed" }
  });
  store.writeArtifact({
    id: "decision-record:inspect",
    runId: id,
    stepId: "inspect",
    path: "decision-records/inspect.json",
    kind: "decision_record",
    contentType: "application/json",
    content: JSON.stringify({
      decision_id: "decision:inspect",
      owner: "owner",
      topic: "Inspection",
      rationale_summary: "Inspect persisted state.",
      consulted: [],
      approved_by: [],
      artifacts: ["evidence.txt"],
      created_at: "2026-08-22T00:00:00.000Z"
    })
  });
  store.writeArtifact({
    id: "failure:inspect:payload",
    runId: id,
    stepId: "inspect",
    path: "failures/inspect.json",
    kind: "failure_payload",
    contentType: "application/json",
    content: JSON.stringify({
      id: "failure:inspect:1",
      step_id: "inspect",
      step_type: "command",
      status: "failed",
      attempt: 1,
      exit_code: 1,
      command: "echo inspect",
      summary: "Inspection failed",
      logs: { stdout: null, stderr: null },
      artifacts: { available: [], withheld: [] },
      classification: "command_failure",
      remediation_status: null,
      path: "failures/inspect.json",
      redactions: {
        applied: false,
        marker: "[REDACTED]",
        fields: [],
        unscanned_artifacts: []
      }
    }),
    metadata: { failureId: "failure:inspect:1" }
  });
  store.recordFailure({
    id: "failure:inspect:1",
    runId: id,
    stepId: "inspect",
    classification: "command_failure",
    message: "Inspection failed",
    retryable: true,
    payload: {
      attempt: 1,
      outcome: "pause",
      failurePayloadPath: "failures/inspect.json"
    }
  });
  store.upsertApproval({
    id: "approval:inspect",
    runId: id,
    stepId: "inspect",
    status: "requested",
    requestedBy: "runtime"
  });
}

function makeRepo(): string {
  const repo = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-inspection-api-"));
  fs.mkdirSync(path.join(repo, ".git"));
  return repo;
}

function failurePayloadDocument(id: string, documentPath: string): Record<string, unknown> {
  return {
    id,
    step_id: "inspect",
    step_type: "command",
    status: "failed",
    attempt: 1,
    exit_code: 1,
    command: "echo inspect",
    summary: "Inspection failed",
    logs: { stdout: null, stderr: null },
    artifacts: { available: [], withheld: [] },
    classification: "command_failure",
    remediation_status: null,
    path: documentPath,
    redactions: {
      applied: false,
      marker: "[REDACTED]",
      fields: [],
      unscanned_artifacts: []
    }
  };
}

function filesystemEntries(root: string): string[] {
  return fs.readdirSync(root, { recursive: true })
    .map(String)
    .sort();
}

function rawGet(
  baseUrl: string,
  requestPath: string,
  headers: Record<string, string>
): Promise<{ status: number; body: string }> {
  const url = new URL(baseUrl);
  return new Promise((resolve, reject) => {
    const socket = net.createConnection({ host: url.hostname, port: Number(url.port) });
    const chunks: Buffer[] = [];
    socket.once("connect", () => {
      const serializedHeaders = Object.entries(headers)
        .map(([name, value]) => `${name}: ${value}`)
        .join("\r\n");
      socket.write(
        `GET ${requestPath} HTTP/1.1\r\nHost: ${url.host}\r\n${serializedHeaders}\r\nConnection: close\r\n\r\n`
      );
    });
    socket.on("data", (chunk: Buffer) => chunks.push(chunk));
    socket.once("error", reject);
    socket.once("end", () => {
      const response = Buffer.concat(chunks).toString("utf8");
      const boundary = response.indexOf("\r\n\r\n");
      const head = response.slice(0, boundary);
      const body = response.slice(boundary + 4);
      const status = Number(/^HTTP\/1\.1 (\d{3})/.exec(head)?.[1] ?? 0);
      resolve({
        status,
        body: /transfer-encoding:\s*chunked/i.test(head) ? decodeChunkedBody(body) : body
      });
    });
  });
}

function decodeChunkedBody(body: string): string {
  let decoded = "";
  let offset = 0;
  while (offset < body.length) {
    const lineEnd = body.indexOf("\r\n", offset);
    if (lineEnd === -1) break;
    const size = Number.parseInt(body.slice(offset, lineEnd), 16);
    if (!Number.isFinite(size) || size === 0) break;
    const chunkStart = lineEnd + 2;
    decoded += body.slice(chunkStart, chunkStart + size);
    offset = chunkStart + size + 2;
  }
  return decoded;
}
