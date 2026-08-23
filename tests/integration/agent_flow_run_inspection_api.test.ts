import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import fs from "node:fs";
import net from "node:net";
import path from "node:path";
import {
  AGENT_FLOW_RUN_INSPECTION_TOKEN_HEADER,
  buildAgentFlowRunInspectionModel,
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
      }
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
