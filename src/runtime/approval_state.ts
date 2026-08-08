import type {
  AgentFlowApprovalStatus,
  AgentFlowRunStateStore
} from "./run_state";

export function staleApprovalStepIdsAcrossLineage(
  store: AgentFlowRunStateStore,
  runId: string
): string[] {
  const stale = new Set<string>();
  const visited = new Set<string>();
  let current = store.getRun(runId);
  while (current !== null && !visited.has(current.id)) {
    visited.add(current.id);
    latestStaleApprovalStepIds(store, current.id).forEach((stepId) => stale.add(stepId));
    current = current.parentRunId === null ? null : store.getRun(current.parentRunId);
  }
  return [...stale].sort();
}

export function persistedStaleApprovalStepIdsAcrossLineage(
  store: AgentFlowRunStateStore,
  runId: string
): string[] {
  const stale = new Set<string>();
  const visited = new Set<string>();
  let current = store.getRun(runId);
  while (current !== null && !visited.has(current.id)) {
    visited.add(current.id);
    persistedLatestStaleApprovalStepIds(store, current.id).forEach((stepId) => stale.add(stepId));
    current = current.parentRunId === null ? null : store.getRun(current.parentRunId);
  }
  return [...stale].sort();
}

export function latestStaleApprovalStepIds(
  store: AgentFlowRunStateStore,
  runId: string
): string[] {
  refreshApprovedArtifactStatus(store, runId);
  return persistedLatestStaleApprovalStepIds(store, runId);
}

function persistedLatestStaleApprovalStepIds(
  store: AgentFlowRunStateStore,
  runId: string
): string[] {
  const latestByStep = new Map<string, {
    status: AgentFlowApprovalStatus;
    attempt: number;
    updatedAt: string;
    id: string;
  }>();
  for (const approval of store.listApprovals(runId)) {
    if (approval.stepId === null) continue;
    if (approval.status === "requested" || approval.status === "cancelled") continue;
    const attempt = Number(/:attempt-(\d+)(?::|$)/.exec(approval.id)?.[1] ?? 0);
    const previous = latestByStep.get(approval.stepId);
    if (previous === undefined || attempt > previous.attempt
        || attempt === previous.attempt && (approval.updatedAt > previous.updatedAt
          || approval.updatedAt === previous.updatedAt && approval.id > previous.id)) {
      latestByStep.set(approval.stepId, {
        status: approval.status,
        attempt,
        updatedAt: approval.updatedAt,
        id: approval.id
      });
    }
  }
  return [...latestByStep]
    .filter(([, approval]) => approval.status === "stale")
    .map(([stepId]) => stepId)
    .sort();
}

export function staleApprovalMessage(stepIds: string[], continuation: string): string {
  return `Stale approval${stepIds.length === 1 ? "" : "s"} ${stepIds.join(", ")} must be rerun before ${continuation}.`;
}

function refreshApprovedArtifactStatus(store: AgentFlowRunStateStore, runId: string): void {
  const paths = new Set<string>();
  store.validateApprovalInvalidationConfiguration(runId);
  for (const approval of store.listApprovals(runId)) {
    if (approval.status !== "approved") continue;
    const evidence = Array.isArray(approval.context.evidence) ? approval.context.evidence : [];
    for (const entry of evidence) {
      const path = recordValue(entry)?.path;
      if (typeof path === "string") paths.add(path);
    }
    if (typeof approval.context.output === "string") paths.add(approval.context.output);
    if (approval.stepId !== null) {
      for (const path of store.approvalInvalidationPaths(runId, approval.stepId)) paths.add(path);
    }
  }
  for (const path of paths) store.getArtifact(runId, path);

  function recordValue(value: unknown): Record<string, unknown> | undefined {
    return value !== null && typeof value === "object" && !Array.isArray(value)
      ? value as Record<string, unknown>
      : undefined;
  }
}
