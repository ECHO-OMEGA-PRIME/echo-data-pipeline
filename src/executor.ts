import type { Env, Pipeline, PipelineRun, TransformStep } from './types';
import { executeTransformPipeline } from './transforms';
import { generateId, nowISO, getRetryDelay, MAX_RETRIES } from './utils';
import { logger } from './logger';

type DataRecord = Record<string, unknown>;

// ─── SQL injection prevention ───────────────────────────────────────────────
// Only these tables may be targeted by d1 destination writes.
// Add new tables here as pipelines require them.
const TABLE_ALLOWLIST = new Set([
  'pipelines',
  'pipeline_runs',
  'pipeline_steps',
  'pipeline_queue',
  'records',
  'documents',
  'metadata',
  'knowledge_entries',
  'embeddings',
  'analytics',
  'events',
  'logs',
  'metrics',
  'scraped_data',
  'ingested_records',
  'pipeline_output',
]);

// Column names must be plain identifiers: alphanumeric + underscore only.
const SAFE_IDENTIFIER_RE = /^[a-zA-Z_][a-zA-Z0-9_]{0,63}$/;

function validateTableName(table: string): boolean {
  return TABLE_ALLOWLIST.has(table);
}

function validateColumnName(col: string): boolean {
  return SAFE_IDENTIFIER_RE.test(col);
}

/**
 * Fetch source data based on pipeline source configuration
 */
async function fetchSourceData(
  pipeline: Pipeline,
  env: Env
): Promise<{ records: DataRecord[]; error: string | null }> {
  const sourceConfig = JSON.parse(pipeline.source_config) as Record<string, unknown>;

  switch (pipeline.source_type) {
    case 'http': {
      const url = sourceConfig.url as string;
      if (!url) return { records: [], error: 'Source URL is required for http source' };
      const method = (sourceConfig.method as string) || 'GET';
      const headers = (sourceConfig.headers || {}) as Record<string, string>;
      const body = sourceConfig.body ? JSON.stringify(sourceConfig.body) : undefined;

      const resp = await fetch(url, { method, headers, body });
      if (!resp.ok) return { records: [], error: `HTTP source returned ${resp.status}: ${resp.statusText}` };

      const data = await resp.json() as unknown;
      const dataPath = sourceConfig.data_path as string | undefined;
      let records: unknown;
      if (dataPath) {
        records = dataPath.split('.').reduce((obj: unknown, key) => {
          if (obj && typeof obj === 'object') return (obj as Record<string, unknown>)[key];
          return undefined;
        }, data);
      } else if (Array.isArray(data)) {
        records = data;
      } else if (data && typeof data === 'object' && 'data' in data) {
        records = (data as Record<string, unknown>).data;
      } else {
        records = [data];
      }

      return { records: Array.isArray(records) ? records as DataRecord[] : [records as DataRecord], error: null };
    }

    case 'd1': {
      const query = sourceConfig.query as string;
      if (!query) return { records: [], error: 'SQL query is required for d1 source' };

      // Only allow SELECT queries to prevent destructive operations via d1 source
      const trimmed = query.trim().toUpperCase();
      if (!trimmed.startsWith('SELECT')) {
        logger.error('D1 source blocked: only SELECT queries allowed', { query_prefix: trimmed.slice(0, 30) });
        return { records: [], error: 'D1 source only allows SELECT queries' };
      }

      // Block dangerous SQL patterns even within SELECT (subquery attacks)
      const dangerous = /\b(DROP|DELETE|INSERT|UPDATE|ALTER|CREATE|ATTACH|DETACH|PRAGMA)\b/i;
      if (dangerous.test(query)) {
        logger.error('D1 source blocked: dangerous SQL keyword detected', { query_prefix: query.slice(0, 60) });
        return { records: [], error: 'D1 source query contains disallowed SQL keywords' };
      }

      const result = await env.DB.prepare(query).all();
      return { records: (result.results || []) as DataRecord[], error: null };
    }

    case 'kv': {
      const prefix = (sourceConfig.prefix as string) || '';
      const listResult = await env.CACHE.list({ prefix, limit: 1000 });
      const records: DataRecord[] = [];
      for (const key of listResult.keys) {
        const value = await env.CACHE.get(key.name, 'json');
        if (value) {
          records.push({ key: key.name, value, metadata: key.metadata });
        }
      }
      return { records, error: null };
    }

    case 'shared_brain': {
      const endpoint = (sourceConfig.endpoint as string) || '/stats';
      try {
        const resp = await env.SHARED_BRAIN.fetch(new Request(`https://internal${endpoint}`, {
          headers: { 'X-Echo-API-Key': env.ECHO_API_KEY },
        }));
        const data = await resp.json() as unknown;
        const arr = Array.isArray(data) ? data : (data && typeof data === 'object' && 'data' in data ? (data as Record<string, unknown>).data : [data]);
        return { records: (Array.isArray(arr) ? arr : [arr]) as DataRecord[], error: null };
      } catch (err) {
        return { records: [], error: `Shared Brain fetch failed: ${err instanceof Error ? err.message : String(err)}` };
      }
    }

    case 'knowledge_forge': {
      const endpoint = (sourceConfig.endpoint as string) || '/health';
      try {
        const resp = await env.KNOWLEDGE_FORGE.fetch(new Request(`https://internal${endpoint}`, {
          headers: { 'X-Echo-API-Key': env.ECHO_API_KEY },
        }));
        const data = await resp.json() as unknown;
        const arr = Array.isArray(data) ? data : [data];
        return { records: arr as DataRecord[], error: null };
      } catch (err) {
        return { records: [], error: `Knowledge Forge fetch failed: ${err instanceof Error ? err.message : String(err)}` };
      }
    }

    case 'static': {
      const records = (sourceConfig.data || []) as DataRecord[];
      return { records, error: null };
    }

    default:
      return { records: [], error: `Unknown source type: ${pipeline.source_type}` };
  }
}

/**
 * Write output data to destination
 */
async function writeDestination(
  pipeline: Pipeline,
  records: DataRecord[],
  env: Env
): Promise<{ written: number; error: string | null }> {
  const destConfig = JSON.parse(pipeline.destination_config) as Record<string, unknown>;

  switch (pipeline.destination_type) {
    case 'kv': {
      const prefix = (destConfig.prefix as string) || `pipeline:${pipeline.id}:output:`;
      let written = 0;
      for (let i = 0; i < records.length; i++) {
        const key = `${prefix}${i}`;
        const ttl = (destConfig.ttl as number) || 86400;
        await env.CACHE.put(key, JSON.stringify(records[i]), { expirationTtl: ttl });
        written++;
      }
      // Store manifest
      await env.CACHE.put(`${prefix}_manifest`, JSON.stringify({
        count: records.length,
        written_at: nowISO(),
        pipeline_id: pipeline.id,
      }), { expirationTtl: (destConfig.ttl as number) || 86400 });
      return { written, error: null };
    }

    case 'd1': {
      const table = destConfig.table as string;
      if (!table) return { written: 0, error: 'Table name required for d1 destination' };

      // Validate table name against allowlist to prevent SQL injection
      if (!validateTableName(table)) {
        logger.error('D1 destination blocked: invalid table name', { table, allowed: [...TABLE_ALLOWLIST] });
        return { written: 0, error: `Invalid table name '${table}'. Must be one of: ${[...TABLE_ALLOWLIST].join(', ')}` };
      }

      const mode = (destConfig.mode as string) || 'insert';
      let written = 0;

      if (mode === 'replace') {
        await env.DB.prepare(`DELETE FROM ${table} WHERE 1=1`).run();
      }

      for (const record of records) {
        const keys = Object.keys(record);

        // Validate all column names to prevent SQL injection via crafted field names
        const invalidCols = keys.filter(k => !validateColumnName(k));
        if (invalidCols.length > 0) {
          logger.warn('D1 insert skipped: invalid column names', { table, invalid_columns: invalidCols });
          continue;
        }

        const placeholders = keys.map(() => '?').join(',');
        const values = keys.map(k => {
          const v = record[k];
          return typeof v === 'object' ? JSON.stringify(v) : v;
        });
        try {
          await env.DB.prepare(`INSERT OR REPLACE INTO ${table} (${keys.join(',')}) VALUES (${placeholders})`).bind(...values).run();
          written++;
        } catch (err) {
          logger.warn('D1 insert failed for record', { table, error: err instanceof Error ? err.message : String(err) });
        }
      }
      return { written, error: null };
    }

    case 'http': {
      const url = destConfig.url as string;
      if (!url) return { written: 0, error: 'URL required for http destination' };
      const headers = {
        'Content-Type': 'application/json',
        ...(destConfig.headers || {}) as Record<string, string>,
      };

      const batchSize = (destConfig.batch_size as number) || records.length;
      let written = 0;

      for (let i = 0; i < records.length; i += batchSize) {
        const batch = records.slice(i, i + batchSize);
        const resp = await fetch(url, {
          method: 'POST',
          headers,
          body: JSON.stringify({ data: batch, pipeline_id: pipeline.id, timestamp: nowISO() }),
        });
        if (resp.ok) {
          written += batch.length;
        } else {
          return { written, error: `HTTP destination returned ${resp.status}` };
        }
      }
      return { written, error: null };
    }

    case 'shared_brain': {
      const endpoint = (destConfig.endpoint as string) || '/ingest';
      try {
        const resp = await env.SHARED_BRAIN.fetch(new Request(`https://internal${endpoint}`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-Echo-API-Key': env.ECHO_API_KEY,
          },
          body: JSON.stringify({ records, pipeline_id: pipeline.id, timestamp: nowISO() }),
        }));
        if (!resp.ok) {
          return { written: 0, error: `Shared Brain write failed: ${resp.status}` };
        }
        return { written: records.length, error: null };
      } catch (err) {
        return { written: 0, error: `Shared Brain write error: ${err instanceof Error ? err.message : String(err)}` };
      }
    }

    case 'log': {
      logger.info('Pipeline output', { pipeline_id: pipeline.id, record_count: records.length, sample: records.slice(0, 3) });
      return { written: records.length, error: null };
    }

    default:
      return { written: 0, error: `Unknown destination type: ${pipeline.destination_type}` };
  }
}

/**
 * Execute a full pipeline run
 */
export async function executePipeline(
  pipeline: Pipeline,
  env: Env,
  existingRunId?: string
): Promise<{ runId: string; success: boolean; error?: string }> {
  const runId = existingRunId || generateId();
  const correlationId = `run:${runId}`;

  logger.info('Pipeline execution started', { pipeline_id: pipeline.id, pipeline_name: pipeline.name, run_id: runId }, correlationId);

  // Create or update run record
  if (!existingRunId) {
    await env.DB.prepare(
      `INSERT INTO pipeline_runs (id, pipeline_id, status, started_at) VALUES (?, ?, 'running', ?)`
    ).bind(runId, pipeline.id, nowISO()).run();
  } else {
    await env.DB.prepare(
      `UPDATE pipeline_runs SET status = 'running', started_at = ? WHERE id = ?`
    ).bind(nowISO(), runId).run();
  }

  try {
    // Step 1: Fetch source data
    const stepFetchId = generateId();
    await env.DB.prepare(
      `INSERT INTO pipeline_steps (id, run_id, step_name, step_type, status, started_at) VALUES (?, ?, 'fetch_source', 'source', 'running', ?)`
    ).bind(stepFetchId, runId, nowISO()).run();

    const sourceResult = await fetchSourceData(pipeline, env);
    if (sourceResult.error) {
      await env.DB.prepare(
        `UPDATE pipeline_steps SET status = 'failed', error = ?, completed_at = ? WHERE id = ?`
      ).bind(sourceResult.error, nowISO(), stepFetchId).run();
      throw new Error(`Source fetch failed: ${sourceResult.error}`);
    }

    await env.DB.prepare(
      `UPDATE pipeline_steps SET status = 'completed', output_count = ?, completed_at = ? WHERE id = ?`
    ).bind(sourceResult.records.length, nowISO(), stepFetchId).run();

    logger.info('Source data fetched', { pipeline_id: pipeline.id, record_count: sourceResult.records.length }, correlationId);

    // Step 2: Execute transforms
    const transformSteps = JSON.parse(pipeline.transform_steps) as TransformStep[];
    let currentRecords = sourceResult.records;

    for (const step of transformSteps) {
      const stepId = generateId();
      await env.DB.prepare(
        `INSERT INTO pipeline_steps (id, run_id, step_name, step_type, status, input_count, started_at) VALUES (?, ?, ?, ?, 'running', ?, ?)`
      ).bind(stepId, runId, step.name, step.type, currentRecords.length, nowISO()).run();

      const { records: transformed, stepResults } = executeTransformPipeline(currentRecords, [step]);
      const stepResult = stepResults[0];

      if (stepResult?.error) {
        await env.DB.prepare(
          `UPDATE pipeline_steps SET status = 'failed', error = ?, output_count = ?, completed_at = ? WHERE id = ?`
        ).bind(stepResult.error, 0, nowISO(), stepId).run();
        throw new Error(`Transform step '${step.name}' failed: ${stepResult.error}`);
      }

      await env.DB.prepare(
        `UPDATE pipeline_steps SET status = 'completed', output_count = ?, completed_at = ? WHERE id = ?`
      ).bind(transformed.length, nowISO(), stepId).run();

      currentRecords = transformed;
    }

    // Step 3: Write to destination
    const stepWriteId = generateId();
    await env.DB.prepare(
      `INSERT INTO pipeline_steps (id, run_id, step_name, step_type, status, input_count, started_at) VALUES (?, ?, 'write_destination', 'destination', 'running', ?, ?)`
    ).bind(stepWriteId, runId, currentRecords.length, nowISO()).run();

    const writeResult = await writeDestination(pipeline, currentRecords, env);
    if (writeResult.error) {
      await env.DB.prepare(
        `UPDATE pipeline_steps SET status = 'failed', error = ?, output_count = ?, completed_at = ? WHERE id = ?`
      ).bind(writeResult.error, writeResult.written, nowISO(), stepWriteId).run();
      throw new Error(`Destination write failed: ${writeResult.error}`);
    }

    await env.DB.prepare(
      `UPDATE pipeline_steps SET status = 'completed', output_count = ?, completed_at = ? WHERE id = ?`
    ).bind(writeResult.written, nowISO(), stepWriteId).run();

    // Mark run as completed
    await env.DB.prepare(
      `UPDATE pipeline_runs SET status = 'completed', completed_at = ?, records_processed = ?, output = ? WHERE id = ?`
    ).bind(nowISO(), writeResult.written, JSON.stringify({ records_output: currentRecords.length, records_written: writeResult.written }), runId).run();

    // Update pipeline last_run
    await env.DB.prepare(
      `UPDATE pipelines SET last_run = ?, updated_at = ? WHERE id = ?`
    ).bind(nowISO(), nowISO(), pipeline.id).run();

    logger.info('Pipeline execution completed', {
      pipeline_id: pipeline.id,
      run_id: runId,
      records_processed: writeResult.written,
    }, correlationId);

    return { runId, success: true };

  } catch (err) {
    const errorMsg = err instanceof Error ? err.message : String(err);
    logger.error('Pipeline execution failed', { pipeline_id: pipeline.id, run_id: runId, error: errorMsg }, correlationId);

    // Check retry eligibility
    const runResult = await env.DB.prepare(
      `SELECT retry_count FROM pipeline_runs WHERE id = ?`
    ).bind(runId).first<{ retry_count: number }>();

    const retryCount = runResult?.retry_count ?? 0;

    if (retryCount < MAX_RETRIES) {
      const delay = getRetryDelay(retryCount);
      const nextRetry = new Date(Date.now() + delay).toISOString();

      await env.DB.prepare(
        `UPDATE pipeline_runs SET status = 'retrying', error_message = ?, retry_count = ?, records_failed = records_failed + 1 WHERE id = ?`
      ).bind(errorMsg, retryCount + 1, runId).run();

      // Queue retry
      await env.DB.prepare(
        `INSERT INTO pipeline_queue (id, pipeline_id, priority, scheduled_for) VALUES (?, ?, 10, ?)`
      ).bind(generateId(), pipeline.id, nextRetry).run();

      logger.info('Pipeline queued for retry', { pipeline_id: pipeline.id, run_id: runId, retry: retryCount + 1, next_retry: nextRetry }, correlationId);
    } else {
      await env.DB.prepare(
        `UPDATE pipeline_runs SET status = 'failed', error_message = ?, completed_at = ?, records_failed = records_failed + 1 WHERE id = ?`
      ).bind(errorMsg, nowISO(), runId).run();

      // Mark pipeline as error state
      await env.DB.prepare(
        `UPDATE pipelines SET status = 'error', updated_at = ? WHERE id = ?`
      ).bind(nowISO(), pipeline.id).run();

      logger.error('Pipeline permanently failed after max retries', { pipeline_id: pipeline.id, run_id: runId, retries: retryCount }, correlationId);
    }

    return { runId, success: false, error: errorMsg };
  }
}
