import { Hono } from 'hono';
import { cors } from 'hono/cors';
import type { Env, Pipeline, PipelineRun, PipelineStep, QueueItem, CreatePipelineBody, UpdatePipelineBody } from './types';
import { ensureSchema } from './schema';
import { logger } from './logger';
import { generateId, response, errorResponse, nowISO, parsePagination } from './utils';
import { executePipeline } from './executor';
import { processQueue, aggregateHourlyStats, dailyCleanup } from './crons';

const app = new Hono<{ Bindings: Env }>();

const VERSION = '1.0.0';
const SERVICE = 'echo-data-pipeline';
const START_TIME = Date.now();

// ─── CORS ───────────────────────────────────────────────────────────────────
app.use('*', cors({
  origin: '*',
  allowMethods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowHeaders: ['Content-Type', 'X-Echo-API-Key', 'Authorization'],
}));
// Security headers middleware
app.use('*', async (c, next) => {
  await next();
  c.res.headers.set('X-Content-Type-Options', 'nosniff');
  c.res.headers.set('X-Frame-Options', 'DENY');
  c.res.headers.set('X-XSS-Protection', '1; mode=block');
  c.res.headers.set('Referrer-Policy', 'strict-origin-when-cross-origin');
  c.res.headers.set('Strict-Transport-Security', 'max-age=31536000; includeSubDomains');
});


// ─── Auth middleware (skip /health) ─────────────────────────────────────────
app.use('*', async (c, next) => {
  if (c.req.path === '/' || c.req.path === '/health' || c.req.method === 'OPTIONS') {
    return next();
  }
  const apiKey = c.req.header('X-Echo-API-Key');
  if (!c.env.ECHO_API_KEY || apiKey !== c.env.ECHO_API_KEY) {
    return c.json({ success: false, error: 'Unauthorized: invalid or missing X-Echo-API-Key', timestamp: nowISO() }, 401);
  }
  return next();
});

// ─── Schema init middleware ─────────────────────────────────────────────────
let schemaInitialized = false;
app.use('*', async (c, next) => {
  if (!schemaInitialized) {
    try {
      await ensureSchema(c.env.DB);
      schemaInitialized = true;
    } catch (err) {
      logger.error('Schema initialization failed', { error: err instanceof Error ? err.message : String(err) });
    }
  }
  return next();
});

// ─── Global error handler ───────────────────────────────────────────────────
app.onError((err, c) => {
  const msg = err instanceof Error ? err.message : String(err);
  logger.error('Unhandled request error', { path: c.req.path, method: c.req.method, error: msg });
  return c.json({ success: false, error: msg, timestamp: nowISO() }, 500);
});

// ═══════════════════════════════════════════════════════════════════════════
// 1. GET /health
// ═══════════════════════════════════════════════════════════════════════════
app.get("/", (c) => c.json({ service: 'echo-data-pipeline', status: 'operational' }));

app.get('/health', async (c) => {
  const uptime = Math.floor((Date.now() - START_TIME) / 1000);
  let dbOk = false;
  try {
    await c.env.DB.prepare('SELECT 1').first();
    dbOk = true;
  } catch { /* db unavailable */ }

  return c.json({
    success: true,
    data: {
      service: SERVICE,
      version: VERSION,
      status: 'operational',
      uptime_seconds: uptime,
      timestamp: nowISO(),
      dependencies: {
        d1: dbOk ? 'connected' : 'unavailable',
        kv: 'connected',
        shared_brain: 'bound',
        alert_router: 'bound',
        knowledge_forge: 'bound',
      },
    },
    timestamp: nowISO(),
  });
});

// ═══════════════════════════════════════════════════════════════════════════
// 2. GET /stats
// ═══════════════════════════════════════════════════════════════════════════
app.get('/stats', async (c) => {
  // Try KV cache first
  const cached = await c.env.CACHE.get('stats:current', 'json');
  if (cached) {
    return response(cached);
  }

  // Build fresh stats
  const totalPipelines = await c.env.DB.prepare(`SELECT COUNT(*) as cnt FROM pipelines`).first<{ cnt: number }>();
  const activePipelines = await c.env.DB.prepare(`SELECT COUNT(*) as cnt FROM pipelines WHERE status = 'active'`).first<{ cnt: number }>();
  const pausedPipelines = await c.env.DB.prepare(`SELECT COUNT(*) as cnt FROM pipelines WHERE status = 'paused'`).first<{ cnt: number }>();
  const errorPipelines = await c.env.DB.prepare(`SELECT COUNT(*) as cnt FROM pipelines WHERE status = 'error'`).first<{ cnt: number }>();

  const totalRuns = await c.env.DB.prepare(`SELECT COUNT(*) as cnt FROM pipeline_runs`).first<{ cnt: number }>();
  const completedRuns = await c.env.DB.prepare(`SELECT COUNT(*) as cnt FROM pipeline_runs WHERE status = 'completed'`).first<{ cnt: number }>();
  const failedRuns = await c.env.DB.prepare(`SELECT COUNT(*) as cnt FROM pipeline_runs WHERE status = 'failed'`).first<{ cnt: number }>();
  const runningRuns = await c.env.DB.prepare(`SELECT COUNT(*) as cnt FROM pipeline_runs WHERE status IN ('running', 'pending')`).first<{ cnt: number }>();

  const avgDuration = await c.env.DB.prepare(
    `SELECT AVG((julianday(completed_at) - julianday(started_at)) * 86400) as avg_seconds
     FROM pipeline_runs WHERE status = 'completed' AND completed_at IS NOT NULL`
  ).first<{ avg_seconds: number | null }>();

  const totalProcessed = await c.env.DB.prepare(
    `SELECT SUM(records_processed) as total FROM pipeline_runs WHERE status = 'completed'`
  ).first<{ total: number | null }>();

  const queueDepth = await c.env.DB.prepare(`SELECT COUNT(*) as cnt FROM pipeline_queue`).first<{ cnt: number }>();

  const total = totalRuns?.cnt ?? 0;
  const completed = completedRuns?.cnt ?? 0;
  const failed = failedRuns?.cnt ?? 0;

  const stats = {
    timestamp: nowISO(),
    pipelines: {
      total: totalPipelines?.cnt ?? 0,
      active: activePipelines?.cnt ?? 0,
      paused: pausedPipelines?.cnt ?? 0,
      error: errorPipelines?.cnt ?? 0,
    },
    runs: {
      total,
      completed,
      failed,
      running: runningRuns?.cnt ?? 0,
      success_rate: total > 0 ? ((completed / total) * 100).toFixed(1) + '%' : 'N/A',
      failure_rate: total > 0 ? ((failed / total) * 100).toFixed(1) + '%' : 'N/A',
    },
    avg_run_duration_seconds: Math.round((avgDuration?.avg_seconds ?? 0) * 100) / 100,
    total_records_processed: totalProcessed?.total ?? 0,
    queue_depth: queueDepth?.cnt ?? 0,
  };

  // Cache for 300s
  await c.env.CACHE.put('stats:current', JSON.stringify(stats), { expirationTtl: 300 });

  return response(stats);
});

// ═══════════════════════════════════════════════════════════════════════════
// 3. GET /pipelines — list with filters
// ═══════════════════════════════════════════════════════════════════════════
app.get('/pipelines', async (c) => {
  const url = new URL(c.req.url);
  const { limit, offset } = parsePagination(url);
  const status = url.searchParams.get('status');
  const sourceType = url.searchParams.get('source_type');

  let query = 'SELECT * FROM pipelines WHERE 1=1';
  const params: string[] = [];

  if (status) {
    query += ' AND status = ?';
    params.push(status);
  }
  if (sourceType) {
    query += ' AND source_type = ?';
    params.push(sourceType);
  }

  query += ' ORDER BY updated_at DESC LIMIT ? OFFSET ?';
  params.push(String(limit), String(offset));

  const result = await c.env.DB.prepare(query).bind(...params).all<Pipeline>();

  // Get total count
  let countQuery = 'SELECT COUNT(*) as cnt FROM pipelines WHERE 1=1';
  const countParams: string[] = [];
  if (status) {
    countQuery += ' AND status = ?';
    countParams.push(status);
  }
  if (sourceType) {
    countQuery += ' AND source_type = ?';
    countParams.push(sourceType);
  }

  const countResult = await c.env.DB.prepare(countQuery).bind(...countParams).first<{ cnt: number }>();

  return response({
    pipelines: result.results || [],
    total: countResult?.cnt ?? 0,
    limit,
    offset,
  });
});

// ═══════════════════════════════════════════════════════════════════════════
// 4. GET /pipelines/:id — full detail with recent runs
// ═══════════════════════════════════════════════════════════════════════════
app.get('/pipelines/:id', async (c) => {
  const id = c.req.param('id');
  const pipeline = await c.env.DB.prepare('SELECT * FROM pipelines WHERE id = ?').bind(id).first<Pipeline>();

  if (!pipeline) {
    return errorResponse('Pipeline not found', 404);
  }

  const recentRuns = await c.env.DB.prepare(
    `SELECT * FROM pipeline_runs WHERE pipeline_id = ? ORDER BY started_at DESC LIMIT 10`
  ).bind(id).all<PipelineRun>();

  return response({
    ...pipeline,
    source_config: JSON.parse(pipeline.source_config),
    destination_config: JSON.parse(pipeline.destination_config),
    transform_steps: JSON.parse(pipeline.transform_steps),
    recent_runs: recentRuns.results || [],
  });
});

// ═══════════════════════════════════════════════════════════════════════════
// 5. POST /pipelines — create pipeline
// ═══════════════════════════════════════════════════════════════════════════
app.post('/pipelines', async (c) => {
  const body = await c.req.json<CreatePipelineBody>();

  if (!body.name || !body.source_type || !body.destination_type) {
    return errorResponse('name, source_type, and destination_type are required');
  }

  const id = generateId();
  const now = nowISO();

  await c.env.DB.prepare(
    `INSERT INTO pipelines (id, name, description, source_type, source_config, destination_type, destination_config, transform_steps, schedule, status, next_run, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?)`
  ).bind(
    id,
    body.name,
    body.description || '',
    body.source_type,
    JSON.stringify(body.source_config || {}),
    body.destination_type,
    JSON.stringify(body.destination_config || {}),
    JSON.stringify(body.transform_steps || []),
    body.schedule || '',
    body.schedule ? new Date(Date.now() + 600000).toISOString() : null,
    now,
    now,
  ).run();

  const pipeline = await c.env.DB.prepare('SELECT * FROM pipelines WHERE id = ?').bind(id).first<Pipeline>();

  logger.info('Pipeline created', { pipeline_id: id, name: body.name, source: body.source_type, destination: body.destination_type });

  return response(pipeline, 201);
});

// ═══════════════════════════════════════════════════════════════════════════
// 6. PUT /pipelines/:id — update pipeline
// ═══════════════════════════════════════════════════════════════════════════
app.put('/pipelines/:id', async (c) => {
  const id = c.req.param('id');
  const existing = await c.env.DB.prepare('SELECT * FROM pipelines WHERE id = ?').bind(id).first<Pipeline>();

  if (!existing) {
    return errorResponse('Pipeline not found', 404);
  }

  const body = await c.req.json<UpdatePipelineBody>();
  const now = nowISO();

  const updates: string[] = [];
  const values: unknown[] = [];

  if (body.name !== undefined) { updates.push('name = ?'); values.push(body.name); }
  if (body.description !== undefined) { updates.push('description = ?'); values.push(body.description); }
  if (body.source_type !== undefined) { updates.push('source_type = ?'); values.push(body.source_type); }
  if (body.source_config !== undefined) { updates.push('source_config = ?'); values.push(JSON.stringify(body.source_config)); }
  if (body.destination_type !== undefined) { updates.push('destination_type = ?'); values.push(body.destination_type); }
  if (body.destination_config !== undefined) { updates.push('destination_config = ?'); values.push(JSON.stringify(body.destination_config)); }
  if (body.transform_steps !== undefined) { updates.push('transform_steps = ?'); values.push(JSON.stringify(body.transform_steps)); }
  if (body.schedule !== undefined) { updates.push('schedule = ?'); values.push(body.schedule); }
  if (body.status !== undefined) { updates.push('status = ?'); values.push(body.status); }

  if (updates.length === 0) {
    return errorResponse('No fields to update');
  }

  updates.push('updated_at = ?');
  values.push(now);
  values.push(id);

  await c.env.DB.prepare(
    `UPDATE pipelines SET ${updates.join(', ')} WHERE id = ?`
  ).bind(...values).run();

  const updated = await c.env.DB.prepare('SELECT * FROM pipelines WHERE id = ?').bind(id).first<Pipeline>();

  logger.info('Pipeline updated', { pipeline_id: id });
  return response(updated);
});

// ═══════════════════════════════════════════════════════════════════════════
// 7. DELETE /pipelines/:id
// ═══════════════════════════════════════════════════════════════════════════
app.delete('/pipelines/:id', async (c) => {
  const id = c.req.param('id');
  const existing = await c.env.DB.prepare('SELECT * FROM pipelines WHERE id = ?').bind(id).first<Pipeline>();

  if (!existing) {
    return errorResponse('Pipeline not found', 404);
  }

  // Delete associated steps, runs, queue entries
  await c.env.DB.prepare(
    `DELETE FROM pipeline_steps WHERE run_id IN (SELECT id FROM pipeline_runs WHERE pipeline_id = ?)`
  ).bind(id).run();
  await c.env.DB.prepare(`DELETE FROM pipeline_runs WHERE pipeline_id = ?`).bind(id).run();
  await c.env.DB.prepare(`DELETE FROM pipeline_queue WHERE pipeline_id = ?`).bind(id).run();
  await c.env.DB.prepare(`DELETE FROM pipelines WHERE id = ?`).bind(id).run();

  logger.info('Pipeline deleted', { pipeline_id: id, name: existing.name });
  return response({ deleted: true, id, name: existing.name });
});

// ═══════════════════════════════════════════════════════════════════════════
// 8. POST /pipelines/:id/run — manually trigger
// ═══════════════════════════════════════════════════════════════════════════
app.post('/pipelines/:id/run', async (c) => {
  const id = c.req.param('id');
  const pipeline = await c.env.DB.prepare('SELECT * FROM pipelines WHERE id = ?').bind(id).first<Pipeline>();

  if (!pipeline) {
    return errorResponse('Pipeline not found', 404);
  }

  if (pipeline.status === 'paused') {
    return errorResponse('Pipeline is paused. Resume it first.');
  }

  // Check for already running
  const running = await c.env.DB.prepare(
    `SELECT COUNT(*) as cnt FROM pipeline_runs WHERE pipeline_id = ? AND status IN ('running', 'pending')`
  ).bind(id).first<{ cnt: number }>();

  if ((running?.cnt ?? 0) > 0) {
    return errorResponse('Pipeline already has an active run');
  }

  const result = await executePipeline(pipeline, c.env);

  return response({
    run_id: result.runId,
    pipeline_id: id,
    success: result.success,
    error: result.error || null,
  }, result.success ? 200 : 500);
});

// ═══════════════════════════════════════════════════════════════════════════
// 9. POST /pipelines/:id/pause
// ═══════════════════════════════════════════════════════════════════════════
app.post('/pipelines/:id/pause', async (c) => {
  const id = c.req.param('id');
  const existing = await c.env.DB.prepare('SELECT * FROM pipelines WHERE id = ?').bind(id).first<Pipeline>();

  if (!existing) {
    return errorResponse('Pipeline not found', 404);
  }

  if (existing.status === 'paused') {
    return errorResponse('Pipeline is already paused');
  }

  await c.env.DB.prepare(
    `UPDATE pipelines SET status = 'paused', updated_at = ? WHERE id = ?`
  ).bind(nowISO(), id).run();

  // Remove queued items
  await c.env.DB.prepare(`DELETE FROM pipeline_queue WHERE pipeline_id = ?`).bind(id).run();

  logger.info('Pipeline paused', { pipeline_id: id });
  return response({ id, status: 'paused' });
});

// ═══════════════════════════════════════════════════════════════════════════
// 10. POST /pipelines/:id/resume
// ═══════════════════════════════════════════════════════════════════════════
app.post('/pipelines/:id/resume', async (c) => {
  const id = c.req.param('id');
  const existing = await c.env.DB.prepare('SELECT * FROM pipelines WHERE id = ?').bind(id).first<Pipeline>();

  if (!existing) {
    return errorResponse('Pipeline not found', 404);
  }

  if (existing.status === 'active') {
    return errorResponse('Pipeline is already active');
  }

  await c.env.DB.prepare(
    `UPDATE pipelines SET status = 'active', updated_at = ? WHERE id = ?`
  ).bind(nowISO(), id).run();

  logger.info('Pipeline resumed', { pipeline_id: id });
  return response({ id, status: 'active' });
});

// ═══════════════════════════════════════════════════════════════════════════
// 11. GET /pipelines/:id/runs — list runs with pagination
// ═══════════════════════════════════════════════════════════════════════════
app.get('/pipelines/:id/runs', async (c) => {
  const id = c.req.param('id');
  const url = new URL(c.req.url);
  const { limit, offset } = parsePagination(url);

  const pipeline = await c.env.DB.prepare('SELECT id FROM pipelines WHERE id = ?').bind(id).first();
  if (!pipeline) {
    return errorResponse('Pipeline not found', 404);
  }

  const runs = await c.env.DB.prepare(
    `SELECT * FROM pipeline_runs WHERE pipeline_id = ? ORDER BY started_at DESC LIMIT ? OFFSET ?`
  ).bind(id, limit, offset).all<PipelineRun>();

  const total = await c.env.DB.prepare(
    `SELECT COUNT(*) as cnt FROM pipeline_runs WHERE pipeline_id = ?`
  ).bind(id).first<{ cnt: number }>();

  return response({
    runs: runs.results || [],
    total: total?.cnt ?? 0,
    limit,
    offset,
  });
});

// ═══════════════════════════════════════════════════════════════════════════
// 12. GET /runs/:id — detailed run info with step-level breakdown
// ═══════════════════════════════════════════════════════════════════════════
app.get('/runs/:id', async (c) => {
  const id = c.req.param('id');
  const run = await c.env.DB.prepare('SELECT * FROM pipeline_runs WHERE id = ?').bind(id).first<PipelineRun>();

  if (!run) {
    return errorResponse('Run not found', 404);
  }

  const steps = await c.env.DB.prepare(
    `SELECT * FROM pipeline_steps WHERE run_id = ? ORDER BY started_at ASC`
  ).bind(id).all<PipelineStep>();

  const pipeline = await c.env.DB.prepare(
    'SELECT name, source_type, destination_type FROM pipelines WHERE id = ?'
  ).bind(run.pipeline_id).first<{ name: string; source_type: string; destination_type: string }>();

  return response({
    ...run,
    pipeline_name: pipeline?.name ?? 'unknown',
    source_type: pipeline?.source_type ?? 'unknown',
    destination_type: pipeline?.destination_type ?? 'unknown',
    steps: steps.results || [],
    output: run.output ? JSON.parse(run.output) : null,
  });
});

// ═══════════════════════════════════════════════════════════════════════════
// 13. POST /runs/:id/retry — retry a failed run
// ═══════════════════════════════════════════════════════════════════════════
app.post('/runs/:id/retry', async (c) => {
  const id = c.req.param('id');
  const run = await c.env.DB.prepare('SELECT * FROM pipeline_runs WHERE id = ?').bind(id).first<PipelineRun>();

  if (!run) {
    return errorResponse('Run not found', 404);
  }

  if (run.status !== 'failed') {
    return errorResponse('Only failed runs can be retried');
  }

  const pipeline = await c.env.DB.prepare('SELECT * FROM pipelines WHERE id = ?').bind(run.pipeline_id).first<Pipeline>();
  if (!pipeline) {
    return errorResponse('Pipeline not found for this run', 404);
  }

  // Reset run status
  await c.env.DB.prepare(
    `UPDATE pipeline_runs SET status = 'pending', error_message = NULL, records_processed = 0, records_failed = 0, retry_count = retry_count + 1 WHERE id = ?`
  ).bind(id).run();

  // Clear old steps
  await c.env.DB.prepare(`DELETE FROM pipeline_steps WHERE run_id = ?`).bind(id).run();

  // Reset pipeline status if in error
  if (pipeline.status === 'error') {
    await c.env.DB.prepare(
      `UPDATE pipelines SET status = 'active', updated_at = ? WHERE id = ?`
    ).bind(nowISO(), pipeline.id).run();
  }

  const result = await executePipeline(pipeline, c.env, id);

  return response({
    run_id: id,
    pipeline_id: run.pipeline_id,
    success: result.success,
    error: result.error || null,
  });
});

// ═══════════════════════════════════════════════════════════════════════════
// 14. GET /queue — view pending queue
// ═══════════════════════════════════════════════════════════════════════════
app.get('/queue', async (c) => {
  const url = new URL(c.req.url);
  const { limit, offset } = parsePagination(url);

  const queue = await c.env.DB.prepare(
    `SELECT q.*, p.name as pipeline_name, p.status as pipeline_status
     FROM pipeline_queue q
     LEFT JOIN pipelines p ON q.pipeline_id = p.id
     ORDER BY q.priority DESC, q.scheduled_for ASC
     LIMIT ? OFFSET ?`
  ).bind(limit, offset).all<QueueItem & { pipeline_name: string; pipeline_status: string }>();

  const total = await c.env.DB.prepare(`SELECT COUNT(*) as cnt FROM pipeline_queue`).first<{ cnt: number }>();

  return response({
    queue: queue.results || [],
    total: total?.cnt ?? 0,
    limit,
    offset,
  });
});

// ═══════════════════════════════════════════════════════════════════════════
// 15. POST /queue/flush — process all queued pipelines now
// ═══════════════════════════════════════════════════════════════════════════
app.post('/queue/flush', async (c) => {
  logger.info('Manual queue flush triggered');
  await processQueue(c.env);
  return response({ flushed: true, timestamp: nowISO() });
});

// ═══════════════════════════════════════════════════════════════════════════
// 16. GET /pipelines/:id/metrics — performance metrics
// ═══════════════════════════════════════════════════════════════════════════
app.get('/pipelines/:id/metrics', async (c) => {
  const id = c.req.param('id');
  const pipeline = await c.env.DB.prepare('SELECT * FROM pipelines WHERE id = ?').bind(id).first<Pipeline>();

  if (!pipeline) {
    return errorResponse('Pipeline not found', 404);
  }

  // Check KV cache
  const cacheKey = `metrics:pipeline:${id}`;
  const cached = await c.env.CACHE.get(cacheKey, 'json');
  if (cached) {
    return response(cached);
  }

  const totalRuns = await c.env.DB.prepare(
    `SELECT COUNT(*) as cnt FROM pipeline_runs WHERE pipeline_id = ?`
  ).bind(id).first<{ cnt: number }>();

  const completedRuns = await c.env.DB.prepare(
    `SELECT COUNT(*) as cnt FROM pipeline_runs WHERE pipeline_id = ? AND status = 'completed'`
  ).bind(id).first<{ cnt: number }>();

  const failedRuns = await c.env.DB.prepare(
    `SELECT COUNT(*) as cnt FROM pipeline_runs WHERE pipeline_id = ? AND status = 'failed'`
  ).bind(id).first<{ cnt: number }>();

  const avgDuration = await c.env.DB.prepare(
    `SELECT AVG((julianday(completed_at) - julianday(started_at)) * 86400) as avg_seconds,
            MIN((julianday(completed_at) - julianday(started_at)) * 86400) as min_seconds,
            MAX((julianday(completed_at) - julianday(started_at)) * 86400) as max_seconds
     FROM pipeline_runs WHERE pipeline_id = ? AND status = 'completed' AND completed_at IS NOT NULL`
  ).bind(id).first<{ avg_seconds: number | null; min_seconds: number | null; max_seconds: number | null }>();

  const throughput = await c.env.DB.prepare(
    `SELECT SUM(records_processed) as total_records,
            AVG(records_processed) as avg_records_per_run
     FROM pipeline_runs WHERE pipeline_id = ? AND status = 'completed'`
  ).bind(id).first<{ total_records: number | null; avg_records_per_run: number | null }>();

  const retryStats = await c.env.DB.prepare(
    `SELECT SUM(retry_count) as total_retries,
            AVG(retry_count) as avg_retries_per_run
     FROM pipeline_runs WHERE pipeline_id = ?`
  ).bind(id).first<{ total_retries: number | null; avg_retries_per_run: number | null }>();

  // Last 10 runs for trend
  const recentRuns = await c.env.DB.prepare(
    `SELECT status, records_processed,
            (julianday(completed_at) - julianday(started_at)) * 86400 as duration_seconds,
            started_at
     FROM pipeline_runs WHERE pipeline_id = ? AND completed_at IS NOT NULL
     ORDER BY started_at DESC LIMIT 10`
  ).bind(id).all<{ status: string; records_processed: number; duration_seconds: number; started_at: string }>();

  const total = totalRuns?.cnt ?? 0;
  const completed = completedRuns?.cnt ?? 0;

  const metrics = {
    pipeline_id: id,
    pipeline_name: pipeline.name,
    total_runs: total,
    completed_runs: completed,
    failed_runs: failedRuns?.cnt ?? 0,
    success_rate: total > 0 ? ((completed / total) * 100).toFixed(1) + '%' : 'N/A',
    duration: {
      avg_seconds: Math.round((avgDuration?.avg_seconds ?? 0) * 100) / 100,
      min_seconds: Math.round((avgDuration?.min_seconds ?? 0) * 100) / 100,
      max_seconds: Math.round((avgDuration?.max_seconds ?? 0) * 100) / 100,
    },
    throughput: {
      total_records_processed: throughput?.total_records ?? 0,
      avg_records_per_run: Math.round((throughput?.avg_records_per_run ?? 0) * 10) / 10,
    },
    retries: {
      total: retryStats?.total_retries ?? 0,
      avg_per_run: Math.round((retryStats?.avg_retries_per_run ?? 0) * 100) / 100,
    },
    recent_trend: (recentRuns.results || []).map(r => ({
      status: r.status,
      records: r.records_processed,
      duration_seconds: Math.round(r.duration_seconds * 100) / 100,
      started_at: r.started_at,
    })),
    computed_at: nowISO(),
  };

  // Cache for 300s
  await c.env.CACHE.put(cacheKey, JSON.stringify(metrics), { expirationTtl: 300 });

  return response(metrics);
});

// ═══════════════════════════════════════════════════════════════════════════
// Cron handler
// ═══════════════════════════════════════════════════════════════════════════
export default {
  fetch: app.fetch,

  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
    const cron = event.cron;
    logger.info('Cron triggered', { cron, scheduled_time: new Date(event.scheduledTime).toISOString() });

    try {
      switch (cron) {
        case '*/10 * * * *':
          ctx.waitUntil(processQueue(env));
          break;
        case '0 * * * *':
          ctx.waitUntil(aggregateHourlyStats(env));
          break;
        case '0 0 * * *':
          ctx.waitUntil(dailyCleanup(env));
          break;
        default:
          logger.warn('Unknown cron expression', { cron });
      }
    } catch (err) {
      logger.error('Cron handler failed', { cron, error: err instanceof Error ? err.message : String(err) });
    }
  },
};
