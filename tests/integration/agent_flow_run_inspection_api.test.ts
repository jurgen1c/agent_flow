import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import fs from "node:fs";
import net from "node:net";
import path from "node:path";
import {
  AGENT_FLOW_RUN_INSPECTION_TOKEN_HEADER,
  AGENT_FLOW_RUN_INSPECTION_RUN_ID_HEADER,
  buildAgentFlowRunInspectionModel,
  buildAgentFlowRunInspectionPage,
  createAgentFlowLifecycleRun,
  listAgentFlowRunInspectionSummaries,
  openAgentFlowRunState,
  parseAgentFlowWorkflowOrThrow,
  startAgentFlowRunInspectionApi
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

  test("serves only token-protected read endpoints on loopback", async () => {
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
