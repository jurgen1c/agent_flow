import type { AgentflowRunStateStore } from "./run_state";

const activePipelineFinalizations = new Set<string>();

export function withAgentflowPipelineFinalization<T>(
  store: AgentflowRunStateStore,
  runId: string,
  reentrantResult: () => T,
  finalize: () => T
): T {
  const key = `${store.databasePath}\0${runId}`;
  if (activePipelineFinalizations.has(key)) return reentrantResult();
  activePipelineFinalizations.add(key);
  try {
    return store.withRunFinalizationTransaction(runId, finalize);
  } finally {
    activePipelineFinalizations.delete(key);
  }
}
