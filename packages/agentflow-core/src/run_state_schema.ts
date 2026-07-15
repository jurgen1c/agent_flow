type SqliteValue = string | number | bigint | null | Uint8Array;

interface SchemaDatabase {
  exec(sql: string): void;
  run(sql: string, params?: SqliteValue[]): void;
  get<T>(sql: string, params?: SqliteValue[]): T | null;
}

export class AgentflowRunStateSchemaVersionError extends Error {}

export function initializeAgentflowRunStateSchema(database: SchemaDatabase, schemaVersion: number): void {
  verifyExistingSchema(database, schemaVersion);
  createSchema(database, schemaVersion);
  verifySchemaVersion(database, schemaVersion);
}

function verifyExistingSchema(database: SchemaDatabase, schemaVersion: number): void {
  const metadataTable = database.get<{ name: string }>(
    "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'run_state_metadata'"
  );
  if (metadataTable !== null) {
    verifySchemaVersion(database, schemaVersion);
    return;
  }

  const existingObject = database.get<{ name: string }>(
    "SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY name LIMIT 1"
  );
  if (existingObject !== null) throw schemaVersionError("missing", schemaVersion);
}

function createSchema(database: SchemaDatabase, schemaVersion: number): void {
  database.exec("BEGIN IMMEDIATE");
  try {
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
  metadata_json TEXT NOT NULL CHECK (json_valid(metadata_json)),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
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
