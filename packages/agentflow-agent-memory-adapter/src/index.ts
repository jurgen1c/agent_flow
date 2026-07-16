import { createHash } from "node:crypto";
import {
  buildContext,
  openSqliteDatabase,
  type AgentContext,
  type ContextBudget
} from "@jurgen1c/agent-memory-core";
import type {
  AgentflowArtifactRecord,
  AgentflowCorePackageBoundary,
  AgentflowRunStateStore
} from "@jurgen1c/agentflow-core";

export interface AgentflowAgentMemoryAdapterPackageBoundary {
  packageName: "@jurgen1c/agentflow-agent-memory-adapter";
  role: "agent-memory-adapter";
  status: "active";
  corePackage: AgentflowCorePackageBoundary["packageName"];
  agentMemoryPackage: "@jurgen1c/agent-memory-core";
}

export interface AgentflowMemoryContextRequest {
  task?: string;
  changedFiles?: readonly string[];
  gitDiff?: boolean;
  budget?: ContextBudget;
  depth?: number;
  includeInferred?: boolean;
  recipeIds?: readonly string[];
  planId?: string;
  stageId?: string;
  profileAlias?: string;
  profileTraitIds?: readonly string[];
}

export type AgentflowMemoryContextBoundary =
  | { kind: "run_start" }
  | { kind: "step_boundary"; stepId: string };

export interface AgentflowMemoryContextSnapshot {
  schemaVersion: 1;
  capturedAt: string;
  runId: string;
  boundary: AgentflowMemoryContextBoundary;
  request: AgentflowMemoryContextRequest;
  memoryDatabasePath: string;
  compileMetadata: Record<string, string>;
  selectedClaimIds: string[];
  recipeIds: string[];
  profileTraitIds: string[];
  warnings: string[];
  verificationCommands: string[];
  memoryUpdatePrompts: string[];
  context: AgentContext;
}

export interface CaptureAgentflowMemoryContextInput {
  runId: string;
  boundary: AgentflowMemoryContextBoundary;
  request?: AgentflowMemoryContextRequest;
  overwrite?: boolean;
}

export interface CapturedAgentflowMemoryContext {
  snapshot: AgentflowMemoryContextSnapshot;
  artifact: AgentflowArtifactRecord;
}

export interface AgentflowAgentMemoryAdapter {
  buildContext(request?: AgentflowMemoryContextRequest): Promise<AgentContext>;
  captureContext(input: CaptureAgentflowMemoryContextInput): Promise<CapturedAgentflowMemoryContext>;
}

export interface CreateAgentflowAgentMemoryAdapterOptions {
  cwd?: string;
  runState: AgentflowRunStateStore;
  now?: () => string;
}

export function createAgentflowAgentMemoryAdapter(
  options: CreateAgentflowAgentMemoryAdapterOptions
): AgentflowAgentMemoryAdapter {
  const now = options.now ?? (() => new Date().toISOString());

  return {
    buildContext: (request = {}) => buildAdapterContext(options.cwd, request),
    async captureContext(input): Promise<CapturedAgentflowMemoryContext> {
      const runId = requiredText(input.runId, "Run ID");
      const boundary = normalizedBoundary(input.boundary);
      const request = normalizedRequest(input.request ?? {});
      const context = await buildAdapterContext(options.cwd, request);
      const compileMetadata = await readCompileMetadata(context.databasePath);
      const snapshot: AgentflowMemoryContextSnapshot = {
        schemaVersion: 1,
        capturedAt: now(),
        runId,
        boundary,
        request,
        memoryDatabasePath: context.databasePath,
        compileMetadata,
        selectedClaimIds: selectedClaimIds(context),
        recipeIds: uniqueSorted(context.matchedRecipes.map((recipe) => recipe.id)),
        profileTraitIds: uniqueSorted(context.profileTraits.map((trait) => trait.id)),
        warnings: [...context.warnings],
        verificationCommands: [...context.verificationSteps],
        memoryUpdatePrompts: memoryUpdatePrompts(context),
        context
      };
      const location = artifactLocation(boundary);
      const artifact = options.runState.writeArtifact({
        id: location.id,
        runId: snapshot.runId,
        ...(boundary.kind === "step_boundary" ? { stepId: boundary.stepId } : {}),
        path: location.path,
        kind: "agent-memory-context",
        contentType: "application/json",
        content: `${JSON.stringify(snapshot, null, 2)}\n`,
        overwrite: input.overwrite,
        metadata: {
          schemaVersion: snapshot.schemaVersion,
          boundary: boundary.kind,
          ...(boundary.kind === "step_boundary" ? { stepId: boundary.stepId } : {}),
          memoryDatabasePath: snapshot.memoryDatabasePath,
          selectedClaimIds: snapshot.selectedClaimIds,
          recipeIds: snapshot.recipeIds,
          profileTraitIds: snapshot.profileTraitIds
        }
      });

      return { snapshot, artifact };
    }
  };
}

export async function readCompileMetadata(databasePath: string): Promise<Record<string, string>> {
  const database = await openSqliteDatabase(databasePath, { readonly: true });

  try {
    return Object.fromEntries(
      database
        .all<{ key: string; value: string }>("SELECT key, value FROM compile_metadata ORDER BY key")
        .map((row) => [row.key, row.value])
    );
  } finally {
    database.close();
  }
}

export const agentflowAgentMemoryAdapterPackageBoundary: AgentflowAgentMemoryAdapterPackageBoundary = {
  packageName: "@jurgen1c/agentflow-agent-memory-adapter",
  role: "agent-memory-adapter",
  status: "active",
  corePackage: "@jurgen1c/agentflow-core",
  agentMemoryPackage: "@jurgen1c/agent-memory-core"
};

function buildAdapterContext(cwd: string | undefined, request: AgentflowMemoryContextRequest): Promise<AgentContext> {
  return buildContext({
    cwd,
    ...request,
    changedFiles: request.changedFiles ? [...request.changedFiles] : undefined,
    recipeIds: request.recipeIds ? [...request.recipeIds] : undefined,
    profileTraitIds: request.profileTraitIds ? [...request.profileTraitIds] : undefined
  });
}

function normalizedRequest(request: AgentflowMemoryContextRequest): AgentflowMemoryContextRequest {
  return {
    ...(request.task === undefined ? {} : { task: request.task }),
    ...(request.changedFiles === undefined ? {} : { changedFiles: [...request.changedFiles] }),
    ...(request.gitDiff === undefined ? {} : { gitDiff: request.gitDiff }),
    ...(request.budget === undefined ? {} : { budget: request.budget }),
    ...(request.depth === undefined ? {} : { depth: request.depth }),
    ...(request.includeInferred === undefined ? {} : { includeInferred: request.includeInferred }),
    ...(request.recipeIds === undefined ? {} : { recipeIds: [...request.recipeIds] }),
    ...(request.planId === undefined ? {} : { planId: request.planId }),
    ...(request.stageId === undefined ? {} : { stageId: request.stageId }),
    ...(request.profileAlias === undefined ? {} : { profileAlias: request.profileAlias }),
    ...(request.profileTraitIds === undefined ? {} : { profileTraitIds: [...request.profileTraitIds] })
  };
}

function normalizedBoundary(boundary: AgentflowMemoryContextBoundary): AgentflowMemoryContextBoundary {
  if (boundary.kind === "run_start") return { kind: "run_start" };
  return { kind: "step_boundary", stepId: requiredText(boundary.stepId, "Step ID") };
}

function artifactLocation(boundary: AgentflowMemoryContextBoundary): { id: string; path: string } {
  if (boundary.kind === "run_start") {
    return { id: "agent-memory-context.run-start", path: "memory/context/run-start.json" };
  }

  const slug = boundary.stepId
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 48) || "step";
  const digest = createHash("sha256").update(boundary.stepId).digest("hex").slice(0, 12);
  const key = `${slug}-${digest}`;
  return {
    id: `agent-memory-context.step.${key}`,
    path: `memory/context/steps/${key}.json`
  };
}

function selectedClaimIds(context: AgentContext): string[] {
  return uniqueSorted([
    ...context.criticalRules.map((claim) => claim.id),
    ...context.matchedClaims.map((claim) => claim.id),
    ...context.relatedClaims.map((related) => related.claim.id)
  ]);
}

function memoryUpdatePrompts(context: AgentContext): string[] {
  return uniqueSorted([
    ...context.recipes.flatMap((recipe) => recipe.memoryUpdates),
    ...(context.planStage?.memoryUpdates ?? [])
  ]);
}

function uniqueSorted(values: readonly string[]): string[] {
  return Array.from(new Set(values)).sort((left, right) => left.localeCompare(right));
}

function requiredText(value: unknown, label: string): string {
  if (typeof value !== "string") throw new Error(`${label} must be a string.`);
  const normalized = value.trim();
  if (!normalized) throw new Error(`${label} must not be blank.`);
  return normalized;
}
