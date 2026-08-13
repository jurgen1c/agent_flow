import fs from "node:fs";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import {
  PathContainmentError,
  resolveContainedPath
} from "@jurgen1c/agent-core/repository";
import {
  AgentFlowRunStateError,
  type AgentFlowArtifactRecord,
  type AgentFlowRecordPage,
  type AgentFlowRunRecord,
  type AgentFlowRunStateStore
} from "./run_state";

const ZIP_UTF8_FLAG = 0x0800;
const ZIP_VERSION = 20;
const ZIP_MINIMUM_DATE = 0x0021;
const MAX_ZIP_ENTRIES = 0xffff;
const MAX_ZIP_VALUE = 0xffffffff;
const MAX_PORTABLE_FILENAME_SLUG_BYTES = 80;
const PORTABLE_RECORD_PAGE_SIZE = 128;
const PORTABLE_JSON_STRING_CHUNK_BYTES = 64 * 1024;

export const MAX_AGENT_FLOW_PORTABLE_ARCHIVE_CONTENT_BYTES = 64 * 1024 * 1024;

export interface AgentFlowPortableArchiveResult {
  outputPath: string;
  entryCount: number;
  sizeBytes: number;
}

interface PortableEntry {
  name: string;
  content: Buffer;
}

interface PortableArtifactSnapshot {
  artifact: AgentFlowArtifactRecord;
  entry?: PortableEntry;
}

interface PortableDirectoryHandle {
  descriptor: number;
  identity: fs.Stats;
  component: string | null;
  created: boolean;
}

interface PortablePublication {
  handles: PortableDirectoryHandle[];
  stableParentPath: string;
  stableTarget: string;
  targetPath: string;
}

export function defaultAgentFlowArchivePath(runId: string): string {
  return path.join(".agent-flow", "archives", `${portableFilename(runId)}.zip`);
}

export function defaultAgentFlowExportPath(runId: string): string {
  return `${portableFilename(runId)}.zip`;
}

export function writeAgentFlowPortableArchive(
  store: AgentFlowRunStateStore,
  runId: string,
  outputPath: string
): AgentFlowPortableArchiveResult {
  if (store.hasActiveFinalizationTransaction()) {
    throw new AgentFlowRunStateError(
      "Portable Agent Flow archives cannot be published from an active finalization transaction.",
      "AGENT_FLOW_ARCHIVE_TRANSACTION"
    );
  }
  const run = store.getRun(runId);
  if (run === null) {
    throw new AgentFlowRunStateError(`Agent Flow run ${runId} was not found.`, "AGENT_FLOW_RUN_NOT_FOUND");
  }
  const canonicalRunId = run.id;
  const target = resolvePortableOutput(store.repoRoot, outputPath);
  const publication = openPortablePublication(store.repoRoot, target);
  const temporaryPath = path.join(
    publication.stableParentPath,
    `.agent-flow-archive-${randomUUID()}.tmp`
  );
  let stagedDescriptor: number | undefined;
  let stagedIdentity: fs.Stats | undefined;
  try {
    assertPortableTargetAbsent(publication);
    const entries = store.withRunFinalizationTransaction(
      canonicalRunId,
      () => collectPortableEntries(store, canonicalRunId)
    );
    const archive = createStoredZip(entries);
    stagedDescriptor = fs.openSync(
      temporaryPath,
      fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_NOFOLLOW,
      0o600
    );
    stagedIdentity = fs.fstatSync(stagedDescriptor);
    fs.writeFileSync(stagedDescriptor, archive);
    fs.fsyncSync(stagedDescriptor);
    assertPortablePublicationAttached(store.repoRoot, publication);
    fs.linkSync(temporaryPath, publication.stableTarget);
    const publishedIdentity = fs.lstatSync(publication.stableTarget);
    if (!sameFileIdentity(publishedIdentity, stagedIdentity)) {
      throw portableArchiveChanged(publication.targetPath);
    }
    assertPortablePublicationAttached(store.repoRoot, publication);
    // Node does not expose unlink-by-identity. Retain the randomized staging
    // link instead of risking deletion of a pathname replaced after inspection.
    syncPortablePublication(publication);
    return {
      outputPath: publication.targetPath,
      entryCount: entries.length,
      sizeBytes: archive.byteLength
    };
  } catch (error) {
    // Failed publications are retained for the same reason: a separate
    // identity check followed by unlink would permit a replacement race.
    if ((error as NodeJS.ErrnoException).code === "EEXIST") {
      throw portableArchiveExists(publication.targetPath, error);
    }
    if (error instanceof AgentFlowRunStateError) throw error;
    throw new AgentFlowRunStateError(
      `Could not write portable Agent Flow archive ${publication.targetPath}: ${error instanceof Error ? error.message : String(error)}`,
      "AGENT_FLOW_ARCHIVE_WRITE",
      { cause: error }
    );
  } finally {
    if (stagedDescriptor !== undefined) {
      try {
        fs.closeSync(stagedDescriptor);
      } catch {
        // Preserve the publication result or original error if descriptor cleanup fails.
      }
    }
    closePortablePublication(publication);
  }
}

function collectPortableEntries(
  store: AgentFlowRunStateStore,
  runId: string
): PortableEntry[] {
  const initialStructuredBytes = store.runSnapshotStructuredBytes(runId);
  if (initialStructuredBytes > MAX_AGENT_FLOW_PORTABLE_ARCHIVE_CONTENT_BYTES) {
    throw portableArchiveTooLarge();
  }
  reconcilePortableArtifacts(store, runId);
  const structuredBytes = store.runSnapshotStructuredBytes(runId);
  const artifactBytes = store.runSnapshotArtifactBytes(runId);
  if (structuredBytes > MAX_AGENT_FLOW_PORTABLE_ARCHIVE_CONTENT_BYTES - artifactBytes) {
    throw portableArchiveTooLarge();
  }
  const run = store.getRun(runId);
  if (run === null) {
    throw new AgentFlowRunStateError(`Agent Flow run ${runId} was not found.`, "AGENT_FLOW_RUN_NOT_FOUND");
  }
  const artifactSnapshots = collectPortableArtifacts(store, runId);
  const entries: PortableEntry[] = [];
  let contentBytes = 0;
  const appendEntry = (entry: PortableEntry): void => {
    if (entry.content.byteLength > MAX_AGENT_FLOW_PORTABLE_ARCHIVE_CONTENT_BYTES - contentBytes) {
      throw new AgentFlowRunStateError(
        `Portable Agent Flow archive content exceeds the ${MAX_AGENT_FLOW_PORTABLE_ARCHIVE_CONTENT_BYTES}-byte limit.`,
        "AGENT_FLOW_ARCHIVE_TOO_LARGE"
      );
    }
    entries.push(entry);
    contentBytes += entry.content.byteLength;
  };
  appendEntry({
    name: "manifest.json",
    content: portableManifestBuffer(
      store,
      runId,
      artifactSnapshots.map(({ artifact }) => artifact),
      MAX_AGENT_FLOW_PORTABLE_ARCHIVE_CONTENT_BYTES - contentBytes
    )
  });
  appendEntry({
    name: "state.json",
    content: boundedJsonBuffer(
      portableRunState(run),
      MAX_AGENT_FLOW_PORTABLE_ARCHIVE_CONTENT_BYTES - contentBytes,
      true
    )
  });
  appendEntry({
    name: "events.jsonl",
    content: portableEventsBuffer(
      store,
      runId,
      MAX_AGENT_FLOW_PORTABLE_ARCHIVE_CONTENT_BYTES - contentBytes
    )
  });

  for (const snapshot of artifactSnapshots) {
    if (snapshot.entry !== undefined) appendEntry(snapshot.entry);
  }
  return entries;
}

function collectPortableArtifacts(
  store: AgentFlowRunStateStore,
  runId: string
): PortableArtifactSnapshot[] {
  const snapshots: PortableArtifactSnapshot[] = [];
  let contentBytes = 0;
  forEachPortablePage(
    (page) => store.listArtifacts(runId, page),
    { sortValue: "", id: "" },
    (artifact) => ({ sortValue: artifact.declaredPath, id: artifact.id }),
    (artifact) => {
      if (artifact.status !== "available" && artifact.status !== "overwritten") {
        snapshots.push({ artifact });
        return;
      }
      const remainingBytes = MAX_AGENT_FLOW_PORTABLE_ARCHIVE_CONTENT_BYTES - contentBytes;
      if (artifact.sizeBytes === null || artifact.sizeBytes > remainingBytes) throw portableArchiveTooLarge();
      store.recoverArtifactBacking(runId, artifact.declaredPath);
      const current = store.readArtifact(runId, artifact.declaredPath, { maxBytes: remainingBytes });
      contentBytes += current.content.byteLength;
      snapshots.push({
        artifact: current.artifact,
        entry: {
          name: `artifacts/${artifact.declaredPath}`,
          content: current.content
        }
      });
    }
  );
  return snapshots;
}

function reconcilePortableArtifacts(store: AgentFlowRunStateStore, runId: string): void {
  forEachPortablePage(
    (page) => store.listArtifacts(runId, page),
    { sortValue: "", id: "" },
    (artifact) => ({ sortValue: artifact.declaredPath, id: artifact.id }),
    () => undefined
  );
}

function portableRunState(run: AgentFlowRunRecord): AgentFlowRunRecord {
  const { cliFixturePath: _cliFixturePath, ...context } = run.context;
  return { ...run, context };
}

function portableManifestBuffer(
  store: AgentFlowRunStateStore,
  runId: string,
  artifacts: AgentFlowArtifactRecord[],
  maxBytes: number
): Buffer {
  const writer = new BoundedBufferWriter(maxBytes);
  writer.append("{");
  appendJsonProperty(writer, "format", "agent-flow-portable-run", true);
  appendJsonProperty(writer, "version", 1);
  appendJsonProperty(writer, "exportedAt", store.currentTimestamp());
  appendJsonProperty(writer, "runId", runId);
  writer.append(',"artifacts":');
  appendJsonArray(writer, artifacts, portableArtifactMetadata);
  writer.append(',"failures":');
  appendPagedJsonArray(
    writer,
    (page) => store.listFailures(runId, page),
    { sortValue: "", id: "" },
    (failure) => ({ sortValue: failure.createdAt, id: failure.id })
  );
  writer.append(',"approvals":');
  appendPagedJsonArray(
    writer,
    (page) => store.listApprovals(runId, page),
    { sortValue: "", id: "" },
    (approval) => ({ sortValue: approval.createdAt, id: approval.id })
  );
  writer.append(',"sessions":');
  appendPagedJsonArray(
    writer,
    (page) => store.listSessions(runId, page),
    { sortValue: "" },
    (session) => ({ sortValue: session.id })
  );
  writer.append("}\n");
  return writer.toBuffer();
}

function portableEventsBuffer(store: AgentFlowRunStateStore, runId: string, maxBytes: number): Buffer {
  const writer = new BoundedBufferWriter(maxBytes);
  forEachPortablePage(
    (page) => store.listEvents(runId, page),
    { sortValue: 0, id: "" },
    (event) => ({ sortValue: event.sequence, id: event.id }),
    (event) => {
      appendJsonValue(writer, event, false);
      writer.append("\n");
    }
  );
  return writer.toBuffer();
}

function appendJsonProperty(
  writer: BoundedBufferWriter,
  name: string,
  value: unknown,
  first = false
): void {
  if (!first) writer.append(",");
  writer.append(JSON.stringify(name));
  writer.append(":");
  appendJsonValue(writer, value, false);
}

function appendPagedJsonArray<T>(
  writer: BoundedBufferWriter,
  load: (page: AgentFlowRecordPage) => T[],
  initialCursor: NonNullable<AgentFlowRecordPage["after"]>,
  cursor: (record: T) => NonNullable<AgentFlowRecordPage["after"]>,
  transform: (record: T) => unknown = (record) => record
): void {
  let first = true;
  writer.append("[");
  forEachPortablePage(load, initialCursor, cursor, (record) => {
    if (!first) writer.append(",");
    appendJsonValue(writer, transform(record), false);
    first = false;
  });
  writer.append("]");
}

function appendJsonArray<T>(
  writer: BoundedBufferWriter,
  records: T[],
  transform: (record: T) => unknown = (record) => record
): void {
  writer.append("[");
  records.forEach((record, index) => {
    if (index > 0) writer.append(",");
    appendJsonValue(writer, transform(record), false);
  });
  writer.append("]");
}

function forEachPortablePage<T>(
  load: (page: AgentFlowRecordPage) => T[],
  initialCursor: NonNullable<AgentFlowRecordPage["after"]>,
  cursor: (record: T) => NonNullable<AgentFlowRecordPage["after"]>,
  visit: (record: T) => void
): void {
  let after: NonNullable<AgentFlowRecordPage["after"]> = initialCursor;
  while (true) {
    const records = load({ limit: PORTABLE_RECORD_PAGE_SIZE, after });
    records.forEach(visit);
    if (records.length < PORTABLE_RECORD_PAGE_SIZE) return;
    after = cursor(records.at(-1)!);
  }
}

function portableArtifactMetadata(artifact: AgentFlowArtifactRecord): Omit<AgentFlowArtifactRecord, "storagePath"> & { archivePath?: string } {
  const { storagePath: _storagePath, ...metadata } = artifact;
  return {
    ...metadata,
    ...(["available", "overwritten"].includes(artifact.status)
      ? { archivePath: `artifacts/${artifact.declaredPath}` }
      : {})
  };
}

function resolvePortableOutput(repoRoot: string, outputPath: string): string {
  try {
    const rootPath = path.resolve(repoRoot);
    const target = resolveContainedPath(rootPath, outputPath, {
      rejectFinalSymlink: true,
      rejectSymlinkComponents: true
    }).absolutePath;
    if (target === rootPath) {
      throw new AgentFlowRunStateError(
        "Portable Agent Flow archive output must name a file beneath the repository root.",
        "AGENT_FLOW_ARCHIVE_PATH"
      );
    }
    return target;
  } catch (error) {
    if (error instanceof AgentFlowRunStateError) throw error;
    if (!(error instanceof PathContainmentError)) throw error;
    throw new AgentFlowRunStateError(
      `Portable Agent Flow archive path must stay inside the repository and cannot be a symbolic link: ${error.candidatePath}`,
      "AGENT_FLOW_ARCHIVE_PATH",
      { cause: error }
    );
  }
}

function openPortablePublication(repoRoot: string, targetPath: string): PortablePublication {
  const rootPath = fs.realpathSync(repoRoot);
  const relativeTarget = path.relative(path.resolve(repoRoot), targetPath);
  const components = relativeTarget.split(path.sep);
  const filename = components.pop();
  if (filename === undefined || filename.length === 0 || relativeTarget === "" || relativeTarget === ".") {
    throw new AgentFlowRunStateError(
      "Portable Agent Flow archive output must name a file beneath the repository root.",
      "AGENT_FLOW_ARCHIVE_PATH"
    );
  }
  if (process.platform !== "linux") {
    throw new AgentFlowRunStateError(
      `Secure portable Agent Flow archive publication is not supported on ${process.platform}.`,
      "AGENT_FLOW_ARCHIVE_WRITE"
    );
  }

  const handles: PortableDirectoryHandle[] = [];
  try {
    const rootDescriptor = openPortableDirectory(rootPath);
    handles.push({
      descriptor: rootDescriptor,
      identity: fs.fstatSync(rootDescriptor),
      component: null,
      created: false
    });
    for (const component of components) {
      const parent = handles.at(-1)!;
      const candidate = path.join(portableDescriptorPath(parent.descriptor), component);
      let created = false;
      try {
        fs.mkdirSync(candidate);
        created = true;
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
      }
      const descriptor = openPortableDirectory(candidate);
      handles.push({ descriptor, identity: fs.fstatSync(descriptor), component, created });
    }
    const stableParentPath = portableDescriptorPath(handles.at(-1)!.descriptor);
    return {
      handles,
      stableParentPath,
      stableTarget: path.join(stableParentPath, filename),
      targetPath: path.join(rootPath, relativeTarget)
    };
  } catch (error) {
    closePortableHandles(handles);
    if (error instanceof AgentFlowRunStateError) throw error;
    throw new AgentFlowRunStateError(
      `Could not prepare portable Agent Flow archive output ${targetPath}: ${error instanceof Error ? error.message : String(error)}`,
      "AGENT_FLOW_ARCHIVE_PATH",
      { cause: error }
    );
  }
}

function assertPortablePublicationAttached(repoRoot: string, publication: PortablePublication): void {
  const root = publication.handles[0]!;
  let currentRoot: fs.Stats;
  try {
    currentRoot = fs.lstatSync(path.resolve(repoRoot));
  } catch (error) {
    throw portableDirectoryChanged(repoRoot, error);
  }
  if (!sameDirectoryIdentity(currentRoot, root.identity)) throw portableDirectoryChanged(repoRoot);

  for (let index = 1; index < publication.handles.length; index += 1) {
    const parent = publication.handles[index - 1]!;
    const child = publication.handles[index]!;
    let descriptor: number;
    try {
      descriptor = openPortableDirectory(path.join(portableDescriptorPath(parent.descriptor), child.component!));
    } catch (error) {
      throw portableDirectoryChanged(publication.targetPath, error);
    }
    try {
      if (!sameDirectoryIdentity(fs.fstatSync(descriptor), child.identity)) {
        throw portableDirectoryChanged(publication.targetPath);
      }
    } finally {
      fs.closeSync(descriptor);
    }
  }
}

function assertPortableTargetAbsent(publication: PortablePublication): void {
  try {
    fs.lstatSync(publication.stableTarget);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return;
    throw error;
  }
  throw portableArchiveExists(publication.targetPath);
}

function syncPortablePublication(publication: PortablePublication): void {
  fs.fsyncSync(publication.handles.at(-1)!.descriptor);
  for (let index = publication.handles.length - 1; index >= 1; index -= 1) {
    if (publication.handles[index]!.created) {
      fs.fsyncSync(publication.handles[index - 1]!.descriptor);
    }
  }
}

function openPortableDirectory(directoryPath: string): number {
  return fs.openSync(
    directoryPath,
    fs.constants.O_RDONLY | fs.constants.O_DIRECTORY | fs.constants.O_NOFOLLOW
  );
}

function portableDescriptorPath(descriptor: number): string {
  return `/proc/self/fd/${descriptor}`;
}

function sameDirectoryIdentity(current: fs.Stats, expected: fs.Stats): boolean {
  return current.isDirectory() && !current.isSymbolicLink()
    && current.dev === expected.dev && current.ino === expected.ino;
}

function sameFileIdentity(current: fs.Stats, expected: fs.Stats): boolean {
  return current.isFile() && !current.isSymbolicLink()
    && current.dev === expected.dev && current.ino === expected.ino;
}

function portableArchiveChanged(targetPath: string): AgentFlowRunStateError {
  return new AgentFlowRunStateError(
    `Portable Agent Flow staged archive changed during publication: ${targetPath}`,
    "AGENT_FLOW_ARCHIVE_WRITE"
  );
}

function portableArchiveExists(targetPath: string, cause?: unknown): AgentFlowRunStateError {
  const message = `Portable Agent Flow archive already exists: ${targetPath}`;
  return cause === undefined
    ? new AgentFlowRunStateError(message, "AGENT_FLOW_ARCHIVE_EXISTS")
    : new AgentFlowRunStateError(message, "AGENT_FLOW_ARCHIVE_EXISTS", { cause });
}

function portableDirectoryChanged(targetPath: string, cause?: unknown): AgentFlowRunStateError {
  const message = `Portable Agent Flow archive directory changed during publication: ${targetPath}`;
  return cause === undefined
    ? new AgentFlowRunStateError(message, "AGENT_FLOW_ARCHIVE_PATH")
    : new AgentFlowRunStateError(message, "AGENT_FLOW_ARCHIVE_PATH", { cause });
}

function closePortablePublication(publication: PortablePublication): void {
  closePortableHandles(publication.handles);
}

function closePortableHandles(handles: PortableDirectoryHandle[]): void {
  for (const handle of handles.reverse()) {
    try {
      fs.closeSync(handle.descriptor);
    } catch {
      // Preserve the publication result or original error if descriptor cleanup fails.
    }
  }
}

function portableFilename(runId: string): string {
  const canonicalRunId = runId.trim();
  const slug = canonicalRunId
    .replace(/[^A-Za-z0-9._-]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, MAX_PORTABLE_FILENAME_SLUG_BYTES)
    .replace(/[-.]+$/g, "");
  const digest = createHash("sha256").update(canonicalRunId).digest("hex").slice(0, 16);
  return `${slug.length > 0 ? slug : "agent-flow-run"}-${digest}`;
}

class BoundedBufferWriter {
  private readonly parts: Buffer[] = [];
  private pendingParts: string[] = [];
  private pendingBytes = 0;
  length = 0;

  constructor(private readonly maxBytes: number) {}

  append(value: string): void {
    const bytes = Buffer.byteLength(value, "utf8");
    if (bytes > this.maxBytes - this.length) throw portableArchiveTooLarge();
    if (bytes >= PORTABLE_JSON_STRING_CHUNK_BYTES) {
      this.flushPending();
      this.parts.push(Buffer.from(value, "utf8"));
    } else {
      if (bytes > PORTABLE_JSON_STRING_CHUNK_BYTES - this.pendingBytes) this.flushPending();
      this.pendingParts.push(value);
      this.pendingBytes += bytes;
    }
    this.length += bytes;
  }

  toBuffer(): Buffer {
    this.flushPending();
    return Buffer.concat(this.parts, this.length);
  }

  private flushPending(): void {
    if (this.pendingParts.length === 0) return;
    this.parts.push(Buffer.from(this.pendingParts.join(""), "utf8"));
    this.pendingParts = [];
    this.pendingBytes = 0;
  }
}

function boundedJsonBuffer(value: unknown, maxBytes: number, spaceAfterColon: boolean): Buffer {
  const writer = new BoundedBufferWriter(maxBytes);
  appendJsonValue(writer, value, spaceAfterColon);
  writer.append("\n");
  return writer.toBuffer();
}

function appendJsonValue(writer: BoundedBufferWriter, value: unknown, spaceAfterColon: boolean): void {
  if (value === null || value === undefined || typeof value === "boolean") {
    writer.append(value === undefined ? "null" : String(value));
    return;
  }
  if (typeof value === "string") {
    appendJsonString(writer, value);
    return;
  }
  if (typeof value === "number") {
    writer.append(Number.isFinite(value) ? String(value) : "null");
    return;
  }
  if (Array.isArray(value)) {
    writer.append("[");
    value.forEach((entry, index) => {
      if (index > 0) writer.append(",");
      appendJsonValue(writer, entry, spaceAfterColon);
    });
    writer.append("]");
    return;
  }
  if (typeof value !== "object") {
    throw new AgentFlowRunStateError("Portable Agent Flow archive contains unsupported JSON data.", "AGENT_FLOW_ARCHIVE_WRITE");
  }
  writer.append("{");
  let fieldIndex = 0;
  for (const [key, entry] of Object.entries(value)) {
    if (entry === undefined || typeof entry === "function" || typeof entry === "symbol") continue;
    if (fieldIndex > 0) writer.append(",");
    appendJsonString(writer, key);
    writer.append(spaceAfterColon ? ": " : ":");
    appendJsonValue(writer, entry, spaceAfterColon);
    fieldIndex += 1;
  }
  writer.append("}");
}

function appendJsonString(writer: BoundedBufferWriter, value: string): void {
  writer.append("\"");
  const escapable = /["\\\u0000-\u001f\ud800-\udfff]/gu;
  let start = 0;
  let chunkBytes = 0;
  let chunk: string[] = [];
  const appendChunkPart = (part: string): void => {
    if (part.length === 0) return;
    chunk.push(part);
    chunkBytes += Buffer.byteLength(part, "utf8");
    if (chunkBytes >= PORTABLE_JSON_STRING_CHUNK_BYTES) {
      writer.append(chunk.join(""));
      chunk = [];
      chunkBytes = 0;
    }
  };
  for (const match of value.matchAll(escapable)) {
    const index = match.index;
    if (index > start) appendChunkPart(value.slice(start, index));
    appendChunkPart(JSON.stringify(match[0]).slice(1, -1));
    start = index + match[0].length;
  }
  if (start < value.length) appendChunkPart(value.slice(start));
  if (chunk.length > 0) writer.append(chunk.join(""));
  writer.append("\"");
}

function portableArchiveTooLarge(): AgentFlowRunStateError {
  return new AgentFlowRunStateError(
    `Portable Agent Flow archive content exceeds the ${MAX_AGENT_FLOW_PORTABLE_ARCHIVE_CONTENT_BYTES}-byte limit.`,
    "AGENT_FLOW_ARCHIVE_TOO_LARGE"
  );
}

function createStoredZip(entries: PortableEntry[]): Buffer {
  if (entries.length > MAX_ZIP_ENTRIES) {
    throw new AgentFlowRunStateError("Portable Agent Flow archive has too many entries.", "AGENT_FLOW_ARCHIVE_TOO_LARGE");
  }
  const localParts: Buffer[] = [];
  const centralParts: Buffer[] = [];
  let offset = 0;

  for (const entry of entries.sort((left, right) => left.name.localeCompare(right.name))) {
    const name = Buffer.from(entry.name, "utf8");
    if (name.byteLength > 0xffff || entry.content.byteLength > MAX_ZIP_VALUE || offset > MAX_ZIP_VALUE) {
      throw new AgentFlowRunStateError("Portable Agent Flow archive exceeds ZIP32 limits.", "AGENT_FLOW_ARCHIVE_TOO_LARGE");
    }
    const checksum = crc32(entry.content);
    const local = Buffer.alloc(30);
    local.writeUInt32LE(0x04034b50, 0);
    local.writeUInt16LE(ZIP_VERSION, 4);
    local.writeUInt16LE(ZIP_UTF8_FLAG, 6);
    local.writeUInt16LE(0, 8);
    local.writeUInt16LE(0, 10);
    local.writeUInt16LE(ZIP_MINIMUM_DATE, 12);
    local.writeUInt32LE(checksum, 14);
    local.writeUInt32LE(entry.content.byteLength, 18);
    local.writeUInt32LE(entry.content.byteLength, 22);
    local.writeUInt16LE(name.byteLength, 26);
    local.writeUInt16LE(0, 28);
    localParts.push(local, name, entry.content);

    const central = Buffer.alloc(46);
    central.writeUInt32LE(0x02014b50, 0);
    central.writeUInt16LE(ZIP_VERSION, 4);
    central.writeUInt16LE(ZIP_VERSION, 6);
    central.writeUInt16LE(ZIP_UTF8_FLAG, 8);
    central.writeUInt16LE(0, 10);
    central.writeUInt16LE(0, 12);
    central.writeUInt16LE(ZIP_MINIMUM_DATE, 14);
    central.writeUInt32LE(checksum, 16);
    central.writeUInt32LE(entry.content.byteLength, 20);
    central.writeUInt32LE(entry.content.byteLength, 24);
    central.writeUInt16LE(name.byteLength, 28);
    central.writeUInt16LE(0, 30);
    central.writeUInt16LE(0, 32);
    central.writeUInt16LE(0, 34);
    central.writeUInt16LE(0, 36);
    central.writeUInt32LE(0, 38);
    central.writeUInt32LE(offset, 42);
    centralParts.push(central, name);
    offset += local.byteLength + name.byteLength + entry.content.byteLength;
  }

  const centralDirectory = Buffer.concat(centralParts);
  const end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50, 0);
  end.writeUInt16LE(0, 4);
  end.writeUInt16LE(0, 6);
  end.writeUInt16LE(entries.length, 8);
  end.writeUInt16LE(entries.length, 10);
  end.writeUInt32LE(centralDirectory.byteLength, 12);
  end.writeUInt32LE(offset, 16);
  end.writeUInt16LE(0, 20);
  return Buffer.concat([...localParts, centralDirectory, end]);
}

function crc32(content: Buffer): number {
  let crc = 0xffffffff;
  for (const byte of content) {
    crc ^= byte;
    for (let bit = 0; bit < 8; bit += 1) {
      crc = (crc >>> 1) ^ (crc & 1 ? 0xedb88320 : 0);
    }
  }
  return (crc ^ 0xffffffff) >>> 0;
}
