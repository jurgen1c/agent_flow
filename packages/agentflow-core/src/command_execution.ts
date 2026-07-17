import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import { isDeepStrictEqual } from "node:util";
import {
  AgentflowRunStateError,
  type AgentflowRunStateValue,
  type AgentflowRunStateStore,
  type AgentflowRunStatus
} from "./run_state";
import type { AgentflowWorkflow, AgentflowWorkflowStep, AgentflowYamlMapping } from "./workflow";
import { evaluateAgentflowPolicy } from "./policy";
import {
  agentflowCommandUnsafeReason,
  MAX_AGENTFLOW_COMMAND_RETRIES,
  MAX_AGENTFLOW_COMMAND_TIMEOUT_SECONDS
} from "./validation";

const MAX_CAPTURE_BYTES = 10 * 1024 * 1024;

export interface AgentflowCommandPipelineResult {
  status: Extract<AgentflowRunStatus, "completed" | "failed" | "paused" | "cancelled">;
  completedSteps: string[];
  failedStep?: string;
  exitCode?: number | null;
  timedOut?: boolean;
  message?: string;
}

interface CommandAttemptResult {
  exitCode: number | null;
  signal: NodeJS.Signals | null;
  timedOut: boolean;
  stdout: Buffer;
  stderr: Buffer;
  message?: string;
}

interface CommandPreflightFailure {
  status: "failed" | "paused";
  message: string;
}

export async function executeAgentflowCommandPipeline(
  store: AgentflowRunStateStore,
  runId: string,
  workflow: AgentflowWorkflow
): Promise<AgentflowCommandPipelineResult> {
  const existing = store.getRun(runId);
  if (existing === null) throw new Error(`Agentflow run ${runId} was not found.`);
  if (!isDeepStrictEqual(existing.context.workflow, workflow)) {
    throw new AgentflowRunStateError(
      `Agentflow run ${runId} cannot execute a workflow that differs from its persisted definition.`,
      "AGENTFLOW_RUN_COLLISION"
    );
  }
  if (existing.status !== "pending") {
    throw new Error(`Agentflow run ${runId} cannot execute while its status is ${existing.status}.`);
  }

  store.transitionRunWithEvent(runId, {
    status: "running",
    allowedFrom: ["pending"],
    event: { type: "run.started", payload: { status: "running" } }
  });

  const completedSteps: string[] = [];
  for (const step of workflow.steps) {
    const stoppedBeforeStep = stoppedPipelineResult(store, runId, completedSteps);
    if (stoppedBeforeStep !== undefined) return stoppedBeforeStep;
    const stepId = requiredStepId(step);
    if (step.type !== "command") {
      return finishFailure(store, runId, completedSteps, stepId, {
        exitCode: null,
        timedOut: false,
        message: `Step ${stepId} has unsupported type ${String(step.type)}; only command steps can execute in this runtime phase.`
      }, "paused");
    }

    const preflightError = validateCommandStep(store.repoRoot, workflow, step);
    if (preflightError !== undefined) {
      persistPreflightFailure(store, runId, stepId, preflightError.message);
      return finishFailure(store, runId, completedSteps, stepId, {
        exitCode: null,
        timedOut: false,
        message: preflightError.message
      }, preflightError.status);
    }

    const retries = failureRetries(step);
    let lastResult: CommandAttemptResult | undefined;
    for (let attempt = 1; attempt <= retries + 1; attempt += 1) {
      store.updateRun(runId, { currentStepId: stepId, error: null });
      store.upsertStep({ runId, stepId, attempt, status: "running", input: { command: step.command as string } });
      store.appendRunEvent(runId, { type: "step.started", stepId, payload: { attempt, command: step.command as string } });

      lastResult = await runCommand(
        store.repoRoot,
        step.command as string,
        timeoutMilliseconds(step),
        () => activeStopStatus(store, runId)
      );
      const stoppedAfterCommand = stoppedPipelineResult(store, runId, completedSteps);
      if (stoppedAfterCommand !== undefined) {
        let logPersistenceError: string | undefined;
        try {
          persistCommandLog(store, runId, stepId, attempt, "stdout", lastResult.stdout);
          persistCommandLog(store, runId, stepId, attempt, "stderr", lastResult.stderr);
        } catch (error) {
          logPersistenceError = `Could not persist interrupted command logs: ${(error as Error).message}`;
        }
        store.upsertStep({
          runId,
          stepId,
          attempt,
          status: stoppedAfterCommand.status,
          output: { ...commandOutput(lastResult, attempt), logPersistenceError: logPersistenceError ?? null }
        });
        store.appendRunEvent(runId, {
          type: "step.interrupted",
          stepId,
          payload: { attempt, status: stoppedAfterCommand.status, logPersistenceError: logPersistenceError ?? null }
        });
        return stoppedAfterCommand;
      }
      try {
        persistCommandLog(store, runId, stepId, attempt, "stdout", lastResult.stdout);
        persistCommandLog(store, runId, stepId, attempt, "stderr", lastResult.stderr);
      } catch (error) {
        lastResult.message = `Could not persist command logs: ${(error as Error).message}`;
      }

      if (lastResult.exitCode === 0 && !lastResult.timedOut && lastResult.message === undefined) {
        const artifactError = persistDeclaredOutputs(store, runId, stepId, step, attempt);
        if (artifactError === undefined) {
          const output = commandOutput(lastResult, attempt);
          store.upsertStep({ runId, stepId, attempt, status: "completed", output });
          store.appendRunEvent(runId, { type: "step.completed", stepId, payload: output });
          completedSteps.push(stepId);
          lastResult = undefined;
          break;
        }
        lastResult.message = artifactError;
      }

      const error = commandError(lastResult, attempt);
      store.upsertStep({ runId, stepId, attempt, status: "failed", error });
      store.recordFailure({
        id: `command:${safeId(stepId)}:attempt-${attempt}`,
        runId,
        stepId,
        classification: lastResult.timedOut ? "command_timeout" : "command_failure",
        message: error.message as string,
        retryable: attempt <= retries,
        payload: error
      });
      store.appendRunEvent(runId, {
        type: lastResult.timedOut ? "step.timed_out" : "step.failed",
        stepId,
        payload: error
      });
    }

    if (lastResult !== undefined) {
      if (failureThen(step) === "continue") continue;
      return finishFailure(store, runId, completedSteps, stepId, {
        exitCode: lastResult.exitCode,
        timedOut: lastResult.timedOut,
        message: lastResult.message ?? failureMessage(lastResult)
      }, failureStatus(step));
    }
  }

  store.updateRun(runId, { currentStepId: null, output: { completedSteps } });
  store.transitionRunWithEvent(runId, {
    status: "completed",
    allowedFrom: ["running"],
    event: { type: "run.completed", payload: { completedSteps } }
  });
  return { status: "completed", completedSteps };
}

function stoppedPipelineResult(
  store: AgentflowRunStateStore,
  runId: string,
  completedSteps: string[]
): AgentflowCommandPipelineResult | undefined {
  const status = store.getRun(runId)?.status;
  if (status === "running") return undefined;
  if (status === "paused" || status === "cancelled") {
    return {
      status,
      completedSteps,
      message: `Agentflow run ${runId} was ${status}; no additional commands were started.`
    };
  }
  throw new AgentflowRunStateError(
    `Agentflow run ${runId} cannot continue while its status is ${String(status)}.`,
    "AGENTFLOW_RUN_TRANSITION"
  );
}

function activeStopStatus(store: AgentflowRunStateStore, runId: string): "paused" | "cancelled" | undefined {
  const status = store.getRun(runId)?.status;
  return status === "paused" || status === "cancelled" ? status : undefined;
}

function finishFailure(
  store: AgentflowRunStateStore,
  runId: string,
  completedSteps: string[],
  stepId: string,
  failure: { exitCode: number | null; timedOut: boolean; message: string },
  status: "failed" | "paused"
): AgentflowCommandPipelineResult {
  store.updateRun(runId, { currentStepId: stepId, error: failure });
  store.transitionRunWithEvent(runId, {
    status,
    allowedFrom: ["running"],
    event: { type: `run.${status}`, payload: { stepId, ...failure } }
  });
  return { status, completedSteps, failedStep: stepId, ...failure };
}

function runCommand(
  repoRoot: string,
  command: string,
  timeoutMs: number | undefined,
  stopStatus: () => "paused" | "cancelled" | undefined
): Promise<CommandAttemptResult> {
  return new Promise((resolve) => {
    const stdout: Buffer[] = [];
    const stderr: Buffer[] = [];
    let capturedBytes = 0;
    let timedOut = false;
    let settled = false;
    let killTimer: ReturnType<typeof setTimeout> | undefined;
    let terminationMessage: string | undefined;
    const child = spawn(command, {
      cwd: repoRoot,
      shell: true,
      detached: process.platform !== "win32",
      env: { ...process.env, AGENTFLOW_REPO_ROOT: repoRoot },
      stdio: ["ignore", "pipe", "pipe"]
    });

    const requestTermination = (message: string, timeout: boolean): void => {
      if (terminationMessage !== undefined) return;
      terminationMessage = message;
      timedOut = timeout;
      terminateChild(child.pid, "SIGTERM");
      killTimer = setTimeout(() => terminateChild(child.pid, "SIGKILL"), 250);
    };
    const capture = (chunks: Buffer[], chunk: Buffer | string): void => {
      const content = Buffer.from(chunk);
      const remaining = MAX_CAPTURE_BYTES - capturedBytes;
      if (remaining > 0) chunks.push(content.subarray(0, remaining));
      capturedBytes += Math.min(content.byteLength, Math.max(remaining, 0));
      if (content.byteLength > remaining) {
        requestTermination(`Command output exceeded the ${MAX_CAPTURE_BYTES}-byte capture limit.`, false);
      }
    };
    child.stdout?.on("data", (chunk: Buffer | string) => capture(stdout, chunk));
    child.stderr?.on("data", (chunk: Buffer | string) => capture(stderr, chunk));
    const timer = timeoutMs === undefined ? undefined : setTimeout(() => {
      requestTermination("Command exceeded timeout_seconds and was terminated.", true);
    }, timeoutMs);
    const lifecycleTimer = setInterval(() => {
      const status = stopStatus();
      if (status !== undefined) requestTermination(`Agentflow run was ${status}; command was terminated.`, false);
    }, 25);

    const finish = (result: Omit<CommandAttemptResult, "stdout" | "stderr" | "timedOut">): void => {
      if (settled) return;
      settled = true;
      if (timer !== undefined) clearTimeout(timer);
      if (killTimer !== undefined) clearTimeout(killTimer);
      if (lifecycleTimer !== undefined) clearInterval(lifecycleTimer);
      resolve({
        ...result,
        ...(result.message === undefined && terminationMessage !== undefined ? { message: terminationMessage } : {}),
        timedOut,
        stdout: Buffer.concat(stdout),
        stderr: Buffer.concat(stderr)
      });
    };
    child.on("error", (error) => finish({ exitCode: null, signal: null, message: `Could not start command: ${error.message}` }));
    child.on("close", (exitCode, signal) => finish({ exitCode, signal }));
  });
}

function terminateChild(pid: number | undefined, signal: NodeJS.Signals): void {
  if (pid === undefined) return;
  try {
    process.kill(process.platform === "win32" ? pid : -pid, signal);
  } catch (error) {
    if (!(["ESRCH", "EPERM"] as Array<string | undefined>).includes((error as NodeJS.ErrnoException).code)) throw error;
  }
}

function persistCommandLog(
  store: AgentflowRunStateStore,
  runId: string,
  stepId: string,
  attempt: number,
  stream: "stdout" | "stderr",
  content: Buffer
): void {
  store.writeArtifact({
    id: `command:${safeId(stepId)}:attempt-${attempt}:${stream}`,
    runId,
    stepId,
    path: `logs/${safeId(stepId)}/attempt-${attempt}/${stream}.log`,
    kind: "command_log",
    contentType: "text/plain; charset=utf-8",
    content,
    metadata: { attempt, stream }
  });
}

function persistDeclaredOutputs(
  store: AgentflowRunStateStore,
  runId: string,
  stepId: string,
  step: AgentflowWorkflowStep,
  attempt: number
): string | undefined {
  for (const declaredPath of stringList(step.outputs)) {
    const source = resolveOutputPath(store.repoRoot, declaredPath);
    if (source === undefined) return `Declared output ${JSON.stringify(declaredPath)} must be repo-relative and stay inside the repository.`;
    let stat: fs.Stats;
    try {
      stat = fs.lstatSync(source);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return `Command completed without creating declared output ${declaredPath}.`;
      return `Could not inspect declared output ${declaredPath}: ${(error as Error).message}`;
    }
    if (stat.isSymbolicLink() || !stat.isFile()) return `Declared output ${declaredPath} must be a regular file, not a symlink or directory.`;
    const realSource = fs.realpathSync(source);
    if (!inside(store.repoRoot, realSource)) return `Declared output ${declaredPath} resolves outside the repository.`;
    try {
      store.writeArtifact({
        id: `command-output:${createHash("sha256").update(declaredPath).digest("hex")}`,
        runId,
        stepId,
        path: declaredPath,
        kind: "command_output",
        contentType: "application/octet-stream",
        content: fs.readFileSync(realSource),
        overwrite: attempt > 1 || step.overwrite === true,
        metadata: { attempt, source: declaredPath }
      });
    } catch (error) {
      return `Could not publish declared output ${declaredPath}: ${(error as Error).message}`;
    }
  }
  return undefined;
}

function validateCommandStep(
  repoRoot: string,
  workflow: AgentflowWorkflow,
  step: AgentflowWorkflowStep
): CommandPreflightFailure | undefined {
  if (typeof step.command !== "string" || step.command.trim().length === 0) {
    return { status: "failed", message: "Command steps require a non-empty command." };
  }
  if (step.timeout_seconds !== undefined &&
      (typeof step.timeout_seconds !== "number" || !Number.isFinite(step.timeout_seconds) || step.timeout_seconds <= 0)) {
    return { status: "failed", message: "Command timeout_seconds must be a positive finite number." };
  }
  if (typeof step.timeout_seconds === "number" && step.timeout_seconds > MAX_AGENTFLOW_COMMAND_TIMEOUT_SECONDS) {
    return {
      status: "failed",
      message: `Command timeout_seconds cannot exceed ${MAX_AGENTFLOW_COMMAND_TIMEOUT_SECONDS}.`
    };
  }

  const onFailure = mapping(step.on_failure);
  const retry = onFailure?.retry;
  if (retry !== undefined &&
      (!Number.isSafeInteger(retry) || Number(retry) < 0 || Number(retry) > MAX_AGENTFLOW_COMMAND_RETRIES)) {
    return {
      status: "failed",
      message: `Command on_failure.retry must be an integer from 0 through ${MAX_AGENTFLOW_COMMAND_RETRIES}.`
    };
  }
  if (onFailure?.then === "continue" && onFailure.allowed !== true) {
    return {
      status: "failed",
      message: "Command failures may continue only when on_failure.allowed is true."
    };
  }

  const approval = evaluateAgentflowPolicy(workflow, { kind: "approval", operation: "command" });
  if (approval.status !== "allow") return policyFailure(approval.status, approval.message);

  if (agentflowCommandUnsafeReason(step.command) !== undefined) {
    const unsafe = evaluateAgentflowPolicy(workflow, { kind: "unsafe_operation", operation: "command" });
    if (unsafe.status !== "allow") return policyFailure(unsafe.status, unsafe.message);
  }

  if (mapping(workflow.policies)?.file_scope !== undefined) {
    return {
      status: "failed",
      message: "Command steps cannot execute with policies.file_scope because this runtime cannot confine arbitrary shell writes."
    };
  }
  for (const output of stringList(step.outputs)) {
    if (resolveOutputPath(repoRoot, output) === undefined) {
      return {
        status: "failed",
        message: `Declared output ${JSON.stringify(output)} must be repo-relative and stay inside the repository.`
      };
    }
  }
  return undefined;
}

function policyFailure(status: "pause" | "fail", message: string): CommandPreflightFailure {
  return { status: status === "pause" ? "paused" : "failed", message };
}

function persistPreflightFailure(
  store: AgentflowRunStateStore,
  runId: string,
  stepId: string,
  message: string
): void {
  const error = { exitCode: null, timedOut: false, message };
  store.upsertStep({ runId, stepId, attempt: 1, status: "failed", error });
  store.recordFailure({
    id: `command:${safeId(stepId)}:preflight`,
    runId,
    stepId,
    classification: "command_policy",
    message,
    retryable: false,
    payload: error
  });
  store.appendRunEvent(runId, { type: "step.rejected", stepId, payload: error });
}

function resolveOutputPath(repoRoot: string, declaredPath: string): string | undefined {
  if (declaredPath.trim() !== declaredPath || declaredPath.length === 0 || declaredPath.includes("\\")) return undefined;
  const normalized = path.posix.normalize(declaredPath);
  if (path.posix.isAbsolute(normalized) || normalized === ".." || normalized.startsWith("../")) return undefined;
  const candidate = path.resolve(repoRoot, ...normalized.split("/"));
  if (!inside(repoRoot, candidate)) return undefined;
  let existingAncestor = candidate;
  while (!fs.existsSync(existingAncestor)) {
    const parent = path.dirname(existingAncestor);
    if (parent === existingAncestor) return undefined;
    existingAncestor = parent;
  }
  try {
    return inside(repoRoot, fs.realpathSync(existingAncestor)) ? candidate : undefined;
  } catch {
    return undefined;
  }
}

function inside(root: string, candidate: string): boolean {
  const relative = path.relative(root, candidate);
  return relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative);
}

function timeoutMilliseconds(step: AgentflowWorkflowStep): number | undefined {
  return typeof step.timeout_seconds === "number" ? Math.ceil(step.timeout_seconds * 1_000) : undefined;
}

function failureRetries(step: AgentflowWorkflowStep): number {
  const retry = mapping(step.on_failure)?.retry;
  return typeof retry === "number" && Number.isSafeInteger(retry) && retry > 0 ? retry : 0;
}

function failureThen(step: AgentflowWorkflowStep): string | undefined {
  const then = mapping(step.on_failure)?.then;
  return typeof then === "string" ? then : undefined;
}

function failureStatus(step: AgentflowWorkflowStep): "failed" | "paused" {
  return failureThen(step) === "fail" ? "failed" : "paused";
}

function commandOutput(result: CommandAttemptResult, attempt: number): Record<string, AgentflowRunStateValue> {
  return { attempt, exitCode: result.exitCode, signal: result.signal, timedOut: result.timedOut };
}

function commandError(result: CommandAttemptResult, attempt: number): Record<string, AgentflowRunStateValue> & { message: string } {
  return { ...commandOutput(result, attempt), message: result.message ?? failureMessage(result) };
}

function failureMessage(result: CommandAttemptResult): string {
  if (result.timedOut) return "Command exceeded timeout_seconds and was terminated.";
  if (result.message !== undefined) return result.message;
  if (result.signal !== null) return `Command terminated by signal ${result.signal}.`;
  return `Command exited with status ${String(result.exitCode)}.`;
}

function requiredStepId(step: AgentflowWorkflowStep): string {
  if (typeof step.id !== "string" || step.id.trim().length === 0) throw new Error("Executable workflow steps require an ID.");
  return step.id.trim();
}

function safeId(value: string): string {
  const slug = value.trim().replace(/[^A-Za-z0-9._-]+/g, "-").replace(/^-+|-+$/g, "") || "step";
  return `${slug}-${createHash("sha256").update(value).digest("hex").slice(0, 8)}`;
}

function stringList(value: unknown): string[] {
  return Array.isArray(value) ? value.filter((entry): entry is string => typeof entry === "string") : [];
}

function mapping(value: unknown): AgentflowYamlMapping | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value as AgentflowYamlMapping : undefined;
}
