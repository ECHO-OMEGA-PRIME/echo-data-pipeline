export const SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS pipelines (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  description TEXT DEFAULT '',
  source_type TEXT NOT NULL,
  source_config TEXT NOT NULL DEFAULT '{}',
  destination_type TEXT NOT NULL,
  destination_config TEXT NOT NULL DEFAULT '{}',
  transform_steps TEXT NOT NULL DEFAULT '[]',
  schedule TEXT DEFAULT '',
  status TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active','paused','error')),
  last_run TEXT,
  next_run TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
  id TEXT PRIMARY KEY,
  pipeline_id TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','running','completed','failed','retrying')),
  started_at TEXT,
  completed_at TEXT,
  records_processed INTEGER DEFAULT 0,
  records_failed INTEGER DEFAULT 0,
  error_message TEXT,
  retry_count INTEGER DEFAULT 0,
  output TEXT,
  FOREIGN KEY (pipeline_id) REFERENCES pipelines(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS pipeline_steps (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  step_name TEXT NOT NULL,
  step_type TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  input_count INTEGER DEFAULT 0,
  output_count INTEGER DEFAULT 0,
  started_at TEXT,
  completed_at TEXT,
  error TEXT,
  FOREIGN KEY (run_id) REFERENCES pipeline_runs(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS pipeline_queue (
  id TEXT PRIMARY KEY,
  pipeline_id TEXT NOT NULL,
  priority INTEGER DEFAULT 0,
  scheduled_for TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (pipeline_id) REFERENCES pipelines(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_runs_pipeline ON pipeline_runs(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_runs_status ON pipeline_runs(status);
CREATE INDEX IF NOT EXISTS idx_steps_run ON pipeline_steps(run_id);
CREATE INDEX IF NOT EXISTS idx_queue_scheduled ON pipeline_queue(scheduled_for);
CREATE INDEX IF NOT EXISTS idx_pipelines_status ON pipelines(status);
`;

export async function ensureSchema(db: D1Database): Promise<void> {
  const statements = SCHEMA_SQL
    .split(';')
    .map(s => s.trim())
    .filter(s => s.length > 0);

  for (const sql of statements) {
    await db.prepare(sql).run();
  }
}
