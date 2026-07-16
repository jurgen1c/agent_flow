import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { compileMemory } from "../../packages/core/src";
import {
  createAgentflowAgentMemoryAdapter,
  type AgentflowMemoryContextSnapshot
} from "../../packages/agentflow-agent-memory-adapter/src";
import { AgentflowRunStateError, openAgentflowRunState } from "../../packages/agentflow-core/src";

const repoRoot = path.resolve(".");
const mockApp = path.join(repoRoot, "examples/mock-app");
const FIXED_TIME = "2026-07-16T04:00:00.000Z";

describe("Agentflow Agent Memory adapter", () => {
  test("captures run-start context as an inspectable Agentflow artifact", async () => {
    const cwd = await compiledMockApp();
    const memoryDatabasePath = path.join(cwd, ".agent-memory/memory.sqlite");
    const databaseBefore = fs.readFileSync(memoryDatabasePath);
    const claimPath = path.join(cwd, "docs/agent-memory/claims/auth/student_oauth_uid_is_tenant_scoped.md");
    const claimBefore = fs.readFileSync(claimPath, "utf8");
    const store = await openAgentflowRunState({ cwd, now: () => FIXED_TIME });
    store.createRun({
      id: "run-memory",
      workflow: { name: "memory-aware", version: 1, style: "pipeline", maturity: "stable" }
    });

    const adapter = createAgentflowAgentMemoryAdapter({ cwd, runState: store, now: () => FIXED_TIME });
    const captured = await adapter.captureContext({
      runId: "run-memory",
      boundary: { kind: "run_start" },
      request: { task: "modify student oauth", changedFiles: ["src/auth.js"] }
    });

    expect(captured.artifact).toMatchObject({
      id: "agent-memory-context.run-start",
      runId: "run-memory",
      producerStepId: null,
      declaredPath: "memory/context/run-start.json",
      kind: "agent-memory-context",
      contentType: "application/json",
      status: "available"
    });
    const snapshot = readSnapshot(cwd, captured.artifact.storagePath);
    expect(snapshot).toEqual(captured.snapshot);
    expect(snapshot).toMatchObject({
      schemaVersion: 1,
      capturedAt: FIXED_TIME,
      runId: "run-memory",
      boundary: { kind: "run_start" },
      memoryDatabasePath,
      request: { task: "modify student oauth", changedFiles: ["src/auth.js"] }
    });
    expect(snapshot.compileMetadata).toMatchObject({
      schema_version: "1",
      database_path: memoryDatabasePath
    });
    expect(snapshot.selectedClaimIds).toContain("auth.student_oauth.uid_is_tenant_scoped");
    expect(snapshot.recipeIds).toContain("recipe.auth.modify_student_oauth");
    expect(snapshot.verificationCommands).toContain("bun test");
    expect(snapshot.context.matchedClaims.map((claim) => claim.id)).toContain("auth.student_oauth.uid_is_tenant_scoped");
    expect(store.listArtifacts("run-memory")).toHaveLength(1);
    expect(fs.readFileSync(memoryDatabasePath)).toEqual(databaseBefore);
    expect(fs.readFileSync(claimPath, "utf8")).toBe(claimBefore);
    store.close();
  });

  test("captures distinct step-boundary snapshots and protects published artifacts", async () => {
    const cwd = await compiledMockApp();
    const store = await openAgentflowRunState({ cwd, now: () => FIXED_TIME });
    store.createRun({
      id: "run-steps",
      workflow: { name: "memory-aware", version: 1, style: "pipeline", maturity: "stable" }
    });
    const adapter = createAgentflowAgentMemoryAdapter({ cwd, runState: store, now: () => FIXED_TIME });
    const first = await adapter.captureContext({
      runId: "run-steps",
      boundary: { kind: "step_boundary", stepId: "review/input" },
      request: { task: "modify student oauth" }
    });

    expect(first.artifact.producerStepId).toBe("review/input");
    expect(first.artifact.declaredPath).toMatch(/^memory\/context\/steps\/review-input-[a-f0-9]{12}\.json$/);
    await expect(adapter.captureContext({
      runId: "run-steps",
      boundary: { kind: "step_boundary", stepId: "review/input" },
      request: { task: "different context" }
    })).rejects.toMatchObject<Partial<AgentflowRunStateError>>({ code: "AGENTFLOW_ARTIFACT_OVERWRITE" });

    const replaced = await adapter.captureContext({
      runId: "run-steps",
      boundary: { kind: "step_boundary", stepId: "review/input" },
      request: { task: "different context" },
      overwrite: true
    });
    expect(replaced.artifact.status).toBe("overwritten");
    expect(replaced.snapshot.boundary).toEqual({ kind: "step_boundary", stepId: "review/input" });
    const collidingSlug = await adapter.captureContext({
      runId: "run-steps",
      boundary: { kind: "step_boundary", stepId: "review input" },
      request: { task: "modify student oauth" }
    });
    expect(collidingSlug.artifact.declaredPath).not.toBe(replaced.artifact.declaredPath);
    expect(store.listArtifacts("run-steps")).toHaveLength(2);
    await expect(adapter.captureContext({
      runId: "run-steps",
      boundary: { kind: "step_boundary", stepId: "   " }
    })).rejects.toThrow("Step ID must not be blank.");
    store.close();
  });

  test("validates capture identifiers before reading memory context", async () => {
    const cwd = temporaryMockApp();
    const store = await openAgentflowRunState({ cwd, now: () => FIXED_TIME });
    store.createRun({
      id: "run-invalid",
      workflow: { name: "memory-aware", version: 1, style: "pipeline", maturity: "stable" }
    });
    const adapter = createAgentflowAgentMemoryAdapter({ cwd, runState: store, now: () => FIXED_TIME });

    await expect(adapter.captureContext({
      runId: "run-invalid",
      boundary: { kind: "step_boundary", stepId: "   " }
    })).rejects.toThrow("Step ID must not be blank.");
    await expect(adapter.captureContext({
      runId: "   ",
      boundary: { kind: "run_start" }
    })).rejects.toThrow("Run ID must not be blank.");
    await expect(adapter.captureContext({
      runId: null as unknown as string,
      boundary: { kind: "run_start" }
    })).rejects.toThrow("Run ID must be a string.");
    await expect(adapter.captureContext({
      runId: "run-invalid",
      boundary: { kind: "step_boundary", stepId: 42 as unknown as string }
    })).rejects.toThrow("Step ID must be a string.");
    expect(fs.existsSync(path.join(cwd, ".agent-memory/memory.sqlite"))).toBe(false);
    store.close();
  });
});

async function compiledMockApp(): Promise<string> {
  const cwd = temporaryMockApp();
  await compileMemory({ cwd });
  return cwd;
}

function temporaryMockApp(): string {
  const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "agentflow-memory-adapter-"));
  fs.cpSync(mockApp, cwd, { recursive: true });
  fs.mkdirSync(path.join(cwd, ".git"));
  return cwd;
}

function readSnapshot(cwd: string, storagePath: string): AgentflowMemoryContextSnapshot {
  return JSON.parse(fs.readFileSync(path.join(cwd, storagePath), "utf8")) as AgentflowMemoryContextSnapshot;
}
