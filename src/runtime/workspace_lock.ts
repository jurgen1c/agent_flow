import fs from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";

const workspaceWriteLockManaged = new WeakSet<object>();

export function markAgentFlowWorkspaceWriteLockManaged<T extends object>(adapter: T): T {
  workspaceWriteLockManaged.add(adapter);
  return adapter;
}

export function isAgentFlowWorkspaceWriteLockManaged(adapter: object): boolean {
  return workspaceWriteLockManaged.has(adapter);
}

export async function withAgentFlowWorkspaceWriteLock<T>(
  repoRoot: string,
  signal: AbortSignal,
  callback: () => Promise<T>,
  options: { required?: boolean } = {}
): Promise<T> {
  if (process.platform !== "linux" || !fs.existsSync("/usr/bin/flock")) {
    if (options.required === true) {
      throw new Error("Workspace mutation locking requires /usr/bin/flock on Linux.");
    }
    return callback();
  }
  const runtimeDirectory = path.join(fs.realpathSync(repoRoot), ".agent-flow");
  fs.mkdirSync(runtimeDirectory, { recursive: true, mode: 0o700 });
  let runtimeStat = fs.lstatSync(runtimeDirectory);
  if (!runtimeStat.isDirectory() || runtimeStat.isSymbolicLink()) {
    throw new Error("Workspace mutation locking requires a non-symlink .agent-flow directory.");
  }
  if ((runtimeStat.mode & 0o022) !== 0) {
    fs.chmodSync(runtimeDirectory, 0o700);
    runtimeStat = fs.lstatSync(runtimeDirectory);
  }
  if (!runtimeStat.isDirectory() || runtimeStat.isSymbolicLink() || (runtimeStat.mode & 0o022) !== 0) {
    throw new Error("Workspace mutation locking requires .agent-flow to not be group- or world-writable.");
  }
  const lockPath = path.join(runtimeDirectory, "workspace-write.lock");
  fs.closeSync(fs.openSync(
    lockPath,
    fs.constants.O_APPEND | fs.constants.O_CREAT | fs.constants.O_NOFOLLOW | fs.constants.O_WRONLY,
    0o600
  ));
  const release = await acquireWorkspaceWriteLock(lockPath, signal);
  try {
    return await callback();
  } finally {
    await release();
  }
}

function acquireWorkspaceWriteLock(lockPath: string, signal: AbortSignal): Promise<() => Promise<void>> {
  return new Promise((resolve, reject) => {
    const child = spawn("/usr/bin/flock", [
      "--exclusive", lockPath, "/bin/sh", "-c", "printf 'locked\\n'; cat >/dev/null"
    ], {
      shell: false,
      detached: true,
      env: { PATH: "/usr/bin:/bin" },
      stdio: ["pipe", "pipe", "pipe"]
    });
    let settled = false;
    let stdout = "";
    let stderr = "";
    const abort = (): void => {
      terminateLockChild(child.pid);
      if (!settled) {
        settled = true;
        reject(signal.reason instanceof Error ? signal.reason : new Error("Workspace write lock acquisition was aborted."));
      }
    };
    signal.addEventListener("abort", abort, { once: true });
    child.stderr?.on("data", (chunk: Buffer | string) => { stderr = `${stderr}${String(chunk)}`.slice(-2_000); });
    child.stdout?.on("data", (chunk: Buffer | string) => {
      stdout += String(chunk);
      if (settled || !stdout.includes("locked\n")) return;
      settled = true;
      signal.removeEventListener("abort", abort);
      resolve(async () => {
        if (child.exitCode !== null) return;
        child.stdin?.end();
        await new Promise<void>((done) => child.once("close", () => done()));
      });
    });
    child.once("error", (error) => {
      if (settled) return;
      settled = true;
      signal.removeEventListener("abort", abort);
      reject(new Error(`Could not acquire workspace write lock: ${error.message}.`));
    });
    child.once("close", (status) => {
      if (settled) return;
      settled = true;
      signal.removeEventListener("abort", abort);
      reject(new Error(
        `Could not acquire workspace write lock${status === null ? "" : ` (status ${status})`}${stderr.trim() ? `: ${stderr.trim()}` : "."}`
      ));
    });
    if (signal.aborted) abort();
  });
}

function terminateLockChild(pid: number | undefined): void {
  if (pid === undefined) return;
  try { process.kill(process.platform === "win32" ? pid : -pid, "SIGTERM"); } catch (error) {
    if (!["ESRCH", "EPERM"].includes((error as NodeJS.ErrnoException).code ?? "")) throw error;
  }
}
