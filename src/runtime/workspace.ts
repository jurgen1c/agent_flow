import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";

import { AgentFlowRunStateError } from "./run_state";

export type AgentFlowWorkspaceSnapshot = Map<string, string>;

const MAX_WORKSPACE_PATHS = 200_000;
const MAX_IGNORED_CONTENT_BYTES = 32 * 1024 * 1024;
const MAX_IGNORED_FILE_BYTES = 256 * 1024;
const GIT_LIST_MAX_BUFFER = 16 * 1024 * 1024;

export function captureAgentFlowWorkspaceSnapshot(repoRoot: string): AgentFlowWorkspaceSnapshot {
  const snapshot: AgentFlowWorkspaceSnapshot = new Map();
  const tracked = listGitWorkspacePaths(repoRoot, ["--cached", "--others", "--exclude-standard"]);
  if (tracked === undefined) {
    if (isUsableGitRepository(repoRoot)) {
      throw snapshotLimitError("Could not enumerate Git-visible paths for recovery remediation.");
    }
    const budget = { remainingContentBytes: MAX_IGNORED_CONTENT_BYTES };
    for (const entry of listFallbackWorkspacePaths(repoRoot)) {
      snapshotWorkspaceEntry(snapshot, repoRoot, entry, budget);
    }
    return snapshot;
  }
  const ignored = listGitWorkspacePaths(repoRoot, ["--others", "--ignored", "--exclude-standard"]);
  if (ignored === undefined) {
    throw snapshotLimitError("Could not enumerate ignored repository paths for recovery remediation.");
  }
  const trackedEntries = normalizedWorkspaceEntries(tracked);
  const ignoredEntries = normalizedWorkspaceEntries(ignored);
  if (trackedEntries.length + ignoredEntries.length > MAX_WORKSPACE_PATHS) {
    throw snapshotLimitError(`Recovery workspace contains more than ${MAX_WORKSPACE_PATHS} auditable paths.`);
  }
  const identities = gitPathIdentities(repoRoot);
  for (const entry of trackedEntries) {
    snapshotWorkspaceEntry(snapshot, repoRoot, entry);
    addGitPathIdentity(snapshot, entry, identities);
  }
  const budget = { remainingContentBytes: MAX_IGNORED_CONTENT_BYTES };
  for (const entry of ignoredEntries) snapshotWorkspaceEntry(snapshot, repoRoot, entry, budget);
  return snapshot;
}

function isUsableGitRepository(repoRoot: string): boolean {
  const result = spawnSync("git", ["rev-parse", "--show-toplevel"], { cwd: repoRoot, encoding: "utf8" });
  return result.error === undefined && result.status === 0
    && path.resolve(result.stdout.trim()) === path.resolve(repoRoot);
}

function snapshotWorkspaceEntry(
  snapshot: AgentFlowWorkspaceSnapshot,
  repoRoot: string,
  entry: string,
  ignoredBudget?: { remainingContentBytes: number }
): void {
  if (entry.length === 0) return;
  const candidate = path.join(repoRoot, ...entry.split("/"));
  const nestedRepositoryHead = directoryGitHead(candidate);
  if (nestedRepositoryHead !== undefined) {
    snapshot.set(entry, `git-head:${nestedRepositoryHead}`);
    snapshotNestedRepositoryEntries(snapshot, candidate, entry);
  } else {
    snapshot.set(entry, ignoredBudget === undefined
      ? workspaceEntryFingerprint(candidate)
      : boundedWorkspaceEntryFingerprint(candidate, ignoredBudget));
  }
}

function listGitWorkspacePaths(repoRoot: string, arguments_: string[]): string[] | undefined {
  const listed = spawnSync("git", ["ls-files", ...arguments_, "-z"], {
    cwd: repoRoot,
    encoding: "buffer",
    maxBuffer: GIT_LIST_MAX_BUFFER
  });
  return listed.error === undefined && listed.status === 0 && Buffer.isBuffer(listed.stdout)
    ? listed.stdout.toString("utf8").split("\0").filter(Boolean)
    : undefined;
}

function gitPathIdentities(repoRoot: string): { index: Map<string, string>; head: Map<string, string> } {
  const index = listGitObjectIdentities(repoRoot, ["ls-files", "--stage", "-z"]);
  const head = listGitObjectIdentities(repoRoot, ["ls-tree", "-r", "-z", "HEAD"], true);
  if (index === undefined || head === undefined) {
    throw snapshotLimitError("Could not snapshot repository index and HEAD identities for recovery remediation.");
  }
  return { index, head };
}

function listGitObjectIdentities(
  repoRoot: string,
  arguments_: string[],
  allowMissingHead = false
): Map<string, string> | undefined {
  const listed = spawnSync("git", arguments_, {
    cwd: repoRoot,
    encoding: "buffer",
    maxBuffer: GIT_LIST_MAX_BUFFER
  });
  if (listed.error !== undefined || !Buffer.isBuffer(listed.stdout)) return undefined;
  if (listed.status !== 0) return allowMissingHead ? new Map() : undefined;
  const identities = new Map<string, string>();
  for (const record of listed.stdout.toString("utf8").split("\0").filter(Boolean)) {
    const separator = record.indexOf("\t");
    if (separator < 0) continue;
    const identity = record.slice(0, separator);
    const entry = record.slice(separator + 1);
    identities.set(entry, `${identities.get(entry) ?? ""}|${identity}`);
  }
  return identities;
}

function addGitPathIdentity(
  snapshot: AgentFlowWorkspaceSnapshot,
  entry: string,
  identities: { index: Map<string, string>; head: Map<string, string> }
): void {
  const fingerprint = snapshot.get(entry) ?? "missing";
  const identityFingerprint = createHash("sha256").update(
    `${fingerprint}:index:${identities.index.get(entry) ?? "none"}:head:${identities.head.get(entry) ?? "none"}`
  ).digest("hex");
  snapshot.set(entry, fingerprint.startsWith("git-head:")
    ? `git-head:${identityFingerprint}`
    : identityFingerprint);
}

function normalizedWorkspaceEntries(entries: string[]): string[] {
  return [...new Set(entries)].filter((entry) => !isAgentFlowRuntimeMetadata(entry)).sort();
}

function isAgentFlowRuntimeMetadata(entry: string): boolean {
  return entry === ".agent-flow" || entry.startsWith(".agent-flow/");
}

const FALLBACK_EXCLUDED_DIRECTORIES = new Set([
  ".agent-flow",
  ".git"
]);

function listFallbackWorkspacePaths(repoRoot: string): string[] {
  if (!fs.existsSync(path.join(repoRoot, ".git"))) {
    throw new AgentFlowRunStateError(
      "Could not snapshot recovery remediation because the repository root is unavailable.",
      "AGENT_FLOW_RECOVERY_WORKSPACE_SNAPSHOT"
    );
  }
  const entries: string[] = [];
  const visit = (relativeDirectory: string): void => {
    const directory = relativeDirectory.length === 0
      ? repoRoot
      : path.join(repoRoot, ...relativeDirectory.split("/"));
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      if (FALLBACK_EXCLUDED_DIRECTORIES.has(entry.name)) continue;
      const relativePath = relativeDirectory.length === 0
        ? entry.name
        : `${relativeDirectory}/${entry.name}`;
      if (entry.isDirectory()) visit(relativePath);
      else entries.push(relativePath);
      if (entries.length > MAX_WORKSPACE_PATHS) {
        throw snapshotLimitError(`Recovery workspace contains more than ${MAX_WORKSPACE_PATHS} auditable paths.`);
      }
    }
  };
  visit("");
  return entries.sort();
}

export function changedAgentFlowWorkspacePaths(
  before: AgentFlowWorkspaceSnapshot,
  after: AgentFlowWorkspaceSnapshot
): string[] {
  const changed = [...new Set([...before.keys(), ...after.keys()])]
    .filter((entry) => before.get(entry) !== after.get(entry));
  const changedGitlinks = changed.filter((entry) =>
    before.get(entry)?.startsWith("git-head:") === true
      || after.get(entry)?.startsWith("git-head:") === true
  );
  return changed
    .filter((entry) => !changedGitlinks.some((gitlink) =>
      entry !== gitlink && entry.startsWith(`${gitlink}/`) && !after.has(entry)
    ))
    .sort();
}

function workspaceEntryFingerprint(candidate: string): string {
  let stat: fs.Stats;
  try {
    stat = fs.lstatSync(candidate);
  } catch (error) {
    if (errorCode(error) === "ENOENT") return "missing";
    throw error;
  }
  const hash = createHash("sha256").update(`${stat.mode}:${stat.size}:`);
  if (stat.isSymbolicLink()) return hash.update(fs.readlinkSync(candidate)).digest("hex");
  if (stat.isFile()) return hashFileContents(hash, candidate).digest("hex");
  if (stat.isDirectory()) {
    const gitDirectory = gitDirectoryFingerprint(candidate);
    if (gitDirectory !== undefined) return hash.update(gitDirectory).digest("hex");
  }
  return hash.update(stat.isDirectory() ? "directory" : "other").digest("hex");
}

function boundedWorkspaceEntryFingerprint(
  candidate: string,
  budget: { remainingContentBytes: number }
): string {
  let stat: fs.Stats;
  try {
    stat = fs.lstatSync(candidate);
  } catch (error) {
    if (errorCode(error) === "ENOENT") return "missing";
    throw error;
  }
  const hash = createHash("sha256").update(
    `${stat.mode}:${stat.size}:${stat.mtimeMs}:${stat.ctimeMs}:`
  );
  if (stat.isSymbolicLink()) return hash.update(fs.readlinkSync(candidate)).digest("hex");
  if (stat.isFile() && stat.size <= MAX_IGNORED_FILE_BYTES && stat.size <= budget.remainingContentBytes) {
    budget.remainingContentBytes -= stat.size;
    return hash.update(fs.readFileSync(candidate)).digest("hex");
  }
  if (stat.isDirectory()) {
    const gitDirectory = gitDirectoryFingerprint(candidate);
    if (gitDirectory !== undefined) return hash.update(gitDirectory).digest("hex");
  }
  return hash.update(stat.isDirectory() ? "directory" : "metadata").digest("hex");
}

function directoryGitHead(candidate: string): string | undefined {
  try {
    if (!fs.lstatSync(candidate).isDirectory()) return undefined;
  } catch (error) {
    if (errorCode(error) === "ENOENT") return undefined;
    throw error;
  }
  const root = spawnSync("git", ["rev-parse", "--show-toplevel"], { cwd: candidate, encoding: "utf8" });
  if (root.error !== undefined || root.status !== 0 || path.resolve(root.stdout.trim()) !== path.resolve(candidate)) {
    return undefined;
  }
  const head = spawnSync("git", ["rev-parse", "HEAD"], { cwd: candidate, encoding: "utf8" });
  return head.status === 0 ? head.stdout.trim() : "no-head";
}

function snapshotNestedRepositoryEntries(
  snapshot: AgentFlowWorkspaceSnapshot,
  candidate: string,
  prefix: string
): void {
  const modified = listGitWorkspacePaths(candidate, ["--modified", "--deleted"]);
  const staged = listGitCachedDiffPaths(candidate);
  const untracked = listGitWorkspacePaths(candidate, ["--others", "--exclude-standard"]);
  const ignored = listGitWorkspacePaths(candidate, ["--others", "--ignored", "--exclude-standard"]);
  if (modified === undefined || staged === undefined || untracked === undefined || ignored === undefined) {
    throw snapshotLimitError(`Could not enumerate nested recovery workspace ${prefix}.`);
  }
  const trackedEntries = normalizedWorkspaceEntries([...modified, ...staged, ...untracked]);
  const ignoredEntries = normalizedWorkspaceEntries(ignored);
  if (trackedEntries.length + ignoredEntries.length > MAX_WORKSPACE_PATHS) {
    throw snapshotLimitError(`Nested recovery workspace contains more than ${MAX_WORKSPACE_PATHS} auditable paths.`);
  }
  const identities = gitPathIdentities(candidate);
  const prefixedIdentities = {
    index: prefixedIdentityMap(identities.index, prefix),
    head: prefixedIdentityMap(identities.head, prefix)
  };
  for (const entry of trackedEntries) {
    snapshot.set(`${prefix}/${entry}`, workspaceEntryFingerprint(path.join(candidate, ...entry.split("/"))));
    addGitPathIdentity(snapshot, `${prefix}/${entry}`, prefixedIdentities);
  }
  const budget = { remainingContentBytes: MAX_IGNORED_CONTENT_BYTES };
  for (const entry of ignoredEntries) {
    snapshot.set(
      `${prefix}/${entry}`,
      boundedWorkspaceEntryFingerprint(path.join(candidate, ...entry.split("/")), budget)
    );
  }
}

function prefixedIdentityMap(identities: Map<string, string>, prefix: string): Map<string, string> {
  return new Map([...identities].map(([entry, identity]) => [`${prefix}/${entry}`, identity]));
}

function gitDirectoryFingerprint(candidate: string): string | undefined {
  const root = spawnSync("git", ["rev-parse", "--show-toplevel"], {
    cwd: candidate,
    encoding: "utf8"
  });
  if (root.error !== undefined || root.status !== 0 || path.resolve(root.stdout.trim()) !== path.resolve(candidate)) {
    return undefined;
  }
  const head = spawnSync("git", ["rev-parse", "HEAD"], { cwd: candidate, encoding: "utf8" });
  const modified = listGitWorkspacePaths(candidate, ["--modified", "--deleted"]);
  const staged = listGitCachedDiffPaths(candidate);
  const untracked = listGitWorkspacePaths(candidate, ["--others", "--exclude-standard"]);
  const ignored = listGitWorkspacePaths(candidate, ["--others", "--ignored", "--exclude-standard"]);
  if (modified === undefined || staged === undefined || untracked === undefined || ignored === undefined) return undefined;
  const trackedEntries = normalizedWorkspaceEntries([...modified, ...staged, ...untracked]);
  const ignoredEntries = normalizedWorkspaceEntries(ignored);
  if (trackedEntries.length + ignoredEntries.length > MAX_WORKSPACE_PATHS) {
    throw snapshotLimitError(`Nested recovery workspace contains more than ${MAX_WORKSPACE_PATHS} auditable paths.`);
  }
  const hash = createHash("sha256").update(head.status === 0 ? head.stdout.trim() : "no-head");
  const identities = gitPathIdentities(candidate);
  for (const entry of trackedEntries) {
    hash.update(
      `\0${entry}\0${workspaceEntryFingerprint(path.join(candidate, ...entry.split("/")))}`
      + `\0index:${identities.index.get(entry) ?? "none"}\0head:${identities.head.get(entry) ?? "none"}`
    );
  }
  const budget = { remainingContentBytes: MAX_IGNORED_CONTENT_BYTES };
  for (const entry of ignoredEntries) {
    hash.update(`\0ignored:${entry}\0${boundedWorkspaceEntryFingerprint(
      path.join(candidate, ...entry.split("/")), budget
    )}`);
  }
  return hash.digest("hex");
}

function listGitCachedDiffPaths(repoRoot: string): string[] | undefined {
  const listed = spawnSync(
    "git",
    ["diff", "--cached", "--name-only", "--diff-filter=ACDMRTUXB", "-z"],
    { cwd: repoRoot, encoding: "buffer", maxBuffer: GIT_LIST_MAX_BUFFER }
  );
  return listed.error === undefined && listed.status === 0 && Buffer.isBuffer(listed.stdout)
    ? listed.stdout.toString("utf8").split("\0").filter(Boolean)
    : undefined;
}

function hashFileContents(hash: ReturnType<typeof createHash>, candidate: string): ReturnType<typeof createHash> {
  const descriptor = fs.openSync(candidate, "r");
  const chunk = Buffer.allocUnsafe(64 * 1024);
  try {
    let bytesRead: number;
    do {
      bytesRead = fs.readSync(descriptor, chunk, 0, chunk.length, null);
      if (bytesRead > 0) hash.update(chunk.subarray(0, bytesRead));
    } while (bytesRead > 0);
  } finally {
    fs.closeSync(descriptor);
  }
  return hash;
}

function snapshotLimitError(message: string): AgentFlowRunStateError {
  return new AgentFlowRunStateError(message, "AGENT_FLOW_RECOVERY_WORKSPACE_SNAPSHOT");
}

function errorCode(error: unknown): string | undefined {
  return error !== null && typeof error === "object" && "code" in error
    ? String(error.code)
    : undefined;
}
