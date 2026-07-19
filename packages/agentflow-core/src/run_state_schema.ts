type SqliteValue = string | number | bigint | null | Uint8Array;

interface SchemaDatabase {
  exec(sql: string): void;
  run(sql: string, params?: SqliteValue[]): void;
  get<T>(sql: string, params?: SqliteValue[]): T | null;
}

const REQUIRED_SCHEMA_OBJECTS = [
  "run_state_metadata",
  "runs",
  "runs_resume_lookup",
  "runs_parent_lookup",
  "runs_recovery_lookup",
  "run_steps",
  "run_steps_status_lookup",
  "artifacts",
  "events",
  "sessions",
  "failures",
  "approvals",
  "budgets"
] as const;

export class AgentflowRunStateSchemaVersionError extends Error {}

export function initializeAgentflowRunStateSchema(database: SchemaDatabase, schemaVersion: number): void {
  let existingVersion = existingSchemaVersion(database);
  if (existingVersion === null) {
    createSchema(database, schemaVersion, true);
  } else if (existingVersion === "1" && schemaVersion >= 2) {
    migrateVersionOneToTwo(database);
    existingVersion = "2";
  }
  if (existingVersion === "2" && schemaVersion === 3) {
    if (schemaNeedsRepair(database)) createSchema(database, 2);
    migrateVersionTwoToThree(database);
    existingVersion = "3";
  }
  verifySchemaVersion(database, schemaVersion);
  if (existingVersion !== null && schemaNeedsRepair(database)) createSchema(database, schemaVersion);
}

function existingSchemaVersion(database: SchemaDatabase): string | null {
  const metadataTable = database.get<{ name: string }>(
    "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'run_state_metadata'"
  );
  if (metadataTable === null) return null;
  return database.get<{ value: string }>("SELECT value FROM run_state_metadata WHERE key = 'schema_version'")?.value ?? "missing";
}

function verifyEmptyDatabase(database: SchemaDatabase, schemaVersion: number): void {
  const existingObject = database.get<{ name: string }>(
    "SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY name LIMIT 1"
  );
  if (existingObject !== null) throw schemaVersionError("missing", schemaVersion);
}

function schemaNeedsRepair(database: SchemaDatabase): boolean {
  const placeholders = REQUIRED_SCHEMA_OBJECTS.map(() => "?").join(", ");
  const row = database.get<{ count: number }>(
    `SELECT COUNT(*) AS count FROM sqlite_master WHERE name IN (${placeholders})`,
    [...REQUIRED_SCHEMA_OBJECTS]
  );
  return row?.count !== REQUIRED_SCHEMA_OBJECTS.length;
}

function createSchema(database: SchemaDatabase, schemaVersion: number, requireEmpty = false): void {
  database.exec("BEGIN IMMEDIATE");
  try {
    if (requireEmpty) {
      const lockedVersion = existingSchemaVersion(database);
      if (lockedVersion === null) verifyEmptyDatabase(database, schemaVersion);
      else if (lockedVersion !== String(schemaVersion)) throw schemaVersionError(lockedVersion, schemaVersion);
    }
    database.exec(`
CREATE TABLE IF NOT EXISTS run_state_metadata (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS runs (
  id TEXT PRIMARY KEY,
  workflow_name TEXT NOT NULL,
  workflow_version INTEGER NOT NULL CHECK (workflow_version > 0),
  workflow_style TEXT NOT NULL CHECK (workflow_style IN ('pipeline', 'recovery_pipeline', 'collaborative')),
  workflow_maturity TEXT NOT NULL CHECK (workflow_maturity IN ('draft', 'experimental', 'stable', 'trusted')),
  status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'waiting', 'paused', 'completed', 'failed', 'cancelled')),
  parent_run_id TEXT REFERENCES runs(id) ON DELETE SET NULL,
  recovery_of_run_id TEXT REFERENCES runs(id) ON DELETE SET NULL,
  current_step_id TEXT,
  inputs_json TEXT NOT NULL CHECK (json_valid(inputs_json)),
  context_json TEXT NOT NULL CHECK (json_valid(context_json)),
  output_json TEXT CHECK (output_json IS NULL OR json_valid(output_json)),
  error_json TEXT CHECK (error_json IS NULL OR json_valid(error_json)),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  started_at TEXT,
  finished_at TEXT
);

CREATE INDEX IF NOT EXISTS runs_resume_lookup ON runs(workflow_name, workflow_version, status, updated_at DESC, id ASC);
CREATE INDEX IF NOT EXISTS runs_parent_lookup ON runs(parent_run_id, id);
CREATE INDEX IF NOT EXISTS runs_recovery_lookup ON runs(recovery_of_run_id, id);

CREATE TABLE IF NOT EXISTS run_steps (
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  step_id TEXT NOT NULL,
  attempt INTEGER NOT NULL CHECK (attempt > 0),
  parent_step_id TEXT,
  session_id TEXT,
  status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'waiting', 'paused', 'completed', 'failed', 'cancelled', 'skipped')),
  input_json TEXT CHECK (input_json IS NULL OR json_valid(input_json)),
  output_json TEXT CHECK (output_json IS NULL OR json_valid(output_json)),
  error_json TEXT CHECK (error_json IS NULL OR json_valid(error_json)),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  started_at TEXT,
  finished_at TEXT,
  PRIMARY KEY (run_id, step_id, attempt)
);

CREATE INDEX IF NOT EXISTS run_steps_status_lookup ON run_steps(run_id, status, step_id, attempt);

CREATE TABLE IF NOT EXISTS artifacts (
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  id TEXT NOT NULL,
  step_id TEXT,
  path TEXT NOT NULL,
  kind TEXT NOT NULL,
  content_type TEXT NOT NULL,
  checksum TEXT,
  size_bytes INTEGER CHECK (size_bytes IS NULL OR size_bytes >= 0),
  status TEXT NOT NULL CHECK (status IN ('available', 'missing', 'stale', 'overwritten')),
  previous_checksum TEXT,
  metadata_json TEXT NOT NULL CHECK (json_valid(metadata_json)),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  written_at TEXT,
  checked_at TEXT,
  generation INTEGER NOT NULL DEFAULT 1 CHECK (generation > 0),
  PRIMARY KEY (run_id, id),
  UNIQUE (run_id, path)
);

CREATE TABLE IF NOT EXISTS events (
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  id TEXT NOT NULL,
  sequence INTEGER NOT NULL CHECK (sequence > 0),
  step_id TEXT,
  session_id TEXT,
  type TEXT NOT NULL,
  payload_json TEXT CHECK (payload_json IS NULL OR json_valid(payload_json)),
  created_at TEXT NOT NULL,
  PRIMARY KEY (run_id, id),
  UNIQUE (run_id, sequence)
);

CREATE TABLE IF NOT EXISTS sessions (
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  id TEXT NOT NULL,
  step_id TEXT,
  provider TEXT NOT NULL,
  external_session_id TEXT,
  status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'waiting', 'paused', 'completed', 'failed', 'cancelled')),
  state_json TEXT NOT NULL CHECK (json_valid(state_json)),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  started_at TEXT,
  finished_at TEXT,
  PRIMARY KEY (run_id, id)
);

CREATE TABLE IF NOT EXISTS failures (
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  id TEXT NOT NULL,
  step_id TEXT,
  session_id TEXT,
  classification TEXT NOT NULL,
  message TEXT NOT NULL,
  retryable INTEGER NOT NULL CHECK (retryable IN (0, 1)),
  payload_json TEXT CHECK (payload_json IS NULL OR json_valid(payload_json)),
  created_at TEXT NOT NULL,
  resolved_at TEXT,
  PRIMARY KEY (run_id, id)
);

CREATE TABLE IF NOT EXISTS approvals (
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  id TEXT NOT NULL,
  step_id TEXT,
  status TEXT NOT NULL CHECK (status IN ('requested', 'approved', 'rejected', 'cancelled')),
  requested_by TEXT,
  decided_by TEXT,
  decision TEXT,
  context_json TEXT NOT NULL CHECK (json_valid(context_json)),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  decided_at TEXT,
  PRIMARY KEY (run_id, id)
);

CREATE TABLE IF NOT EXISTS budgets (
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  id TEXT NOT NULL,
  step_id TEXT,
  session_id TEXT,
  scope TEXT NOT NULL,
  kind TEXT NOT NULL,
  limit_value REAL NOT NULL CHECK (limit_value >= 0),
  used REAL NOT NULL CHECK (used >= 0),
  unit TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (run_id, id)
);
    `);
    database.run("INSERT OR IGNORE INTO run_state_metadata (key, value) VALUES ('schema_version', ?)", [String(schemaVersion)]);
    database.exec("COMMIT");
  } catch (error) {
    rollback(database);
    throw error;
  }
}

function migrateVersionOneToTwo(database: SchemaDatabase): void {
  database.exec("BEGIN IMMEDIATE");
  try {
    const lockedVersion = existingSchemaVersion(database);
    if (lockedVersion === "2" || lockedVersion === "3") {
      database.exec("COMMIT");
      return;
    }
    if (lockedVersion !== "1") throw schemaVersionError(lockedVersion ?? "missing", 2);
    database.exec(`
ALTER TABLE artifacts ADD COLUMN status TEXT NOT NULL DEFAULT 'missing'
  CHECK (status IN ('available', 'missing', 'stale', 'overwritten'));
ALTER TABLE artifacts ADD COLUMN previous_checksum TEXT;
ALTER TABLE artifacts ADD COLUMN written_at TEXT;
ALTER TABLE artifacts ADD COLUMN checked_at TEXT;
    `);
    database.run("UPDATE run_state_metadata SET value = '2' WHERE key = 'schema_version'");
    database.exec("COMMIT");
  } catch (error) {
    rollback(database);
    throw error;
  }
}

function migrateVersionTwoToThree(database: SchemaDatabase): void {
  database.exec("BEGIN IMMEDIATE");
  try {
    const lockedVersion = existingSchemaVersion(database);
    if (lockedVersion === "3") {
      database.exec("COMMIT");
      return;
    }
    if (lockedVersion !== "2") throw schemaVersionError(lockedVersion ?? "missing", 3);
    const generation = database.get<{ name: string }>(
      "SELECT name FROM pragma_table_info('artifacts') WHERE name = 'generation'"
    );
    if (generation === null) {
      database.exec("ALTER TABLE artifacts ADD COLUMN generation INTEGER NOT NULL DEFAULT 1 CHECK (generation > 0)");
    }
    database.run("UPDATE run_state_metadata SET value = '3' WHERE key = 'schema_version'");
    database.exec("COMMIT");
  } catch (error) {
    rollback(database);
    throw error;
  }
}

function verifySchemaVersion(database: SchemaDatabase, schemaVersion: number): void {
  const row = database.get<{ value: string }>("SELECT value FROM run_state_metadata WHERE key = 'schema_version'");
  if (row?.value !== String(schemaVersion)) throw schemaVersionError(row?.value ?? "missing", schemaVersion);
}

function schemaVersionError(found: string, expected: number): AgentflowRunStateSchemaVersionError {
  return new AgentflowRunStateSchemaVersionError(
    `Unsupported Agentflow run-state schema version ${found}; expected ${expected}.`
  );
}

function rollback(database: SchemaDatabase): void {
  try {
    database.exec("ROLLBACK");
  } catch {
    // Preserve the original schema initialization error.
  }
}
