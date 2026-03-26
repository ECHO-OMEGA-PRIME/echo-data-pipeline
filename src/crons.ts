import type { Env, Pipeline, QueueItem } from './types';
import { executePipeline } from './executor';
import { nowISO, generateId } from './utils';
import { logger } from './logger';

/**
 * Process pipeline queue — runs every 10 minutes
 * Picks up queued items that are due and executes them
 */
export async function processQueue(env: Env): Promise<void> {
  const now = nowISO();
  logger.info('Cron: processing pipeline queue', { trigger_time: now });

  // Fetch due queue items, ordered by priority (higher first) then scheduled time
  const queueItems = await env.DB.prepare(
    `SELECT q.*, p.status as pipeline_status FROM pipeline_queue q
     JOIN pipelines p ON q.pipeline_id = p.id
     WHERE q.scheduled_for <= ? AND p.status != 'paused'
     ORDER BY q.priority DESC, q.scheduled_for ASC
     LIMIT 20`
  ).bind(now).all<QueueItem & { pipeline_status: string }>();

  if (!queueItems.results || queueItems.results.length === 0) {
    logger.info('Cron: no queued pipelines to process');
    return;
  }

  logger.info('Cron: found queued pipelines', { count: queueItems.results.length });

  for (const item of queueItems.results) {
    // Remove from queue
    await env.DB.prepare(`DELETE FROM pipeline_queue WHERE id = ?`).bind(item.id).run();

    // Fetch full pipeline
    const pipeline = await env.DB.prepare(
      `SELECT * FROM pipelines WHERE id = ?`
    ).bind(item.pipeline_id).first<Pipeline>();

    if (!pipeline || pipeline.status === 'paused') {
      logger.info('Cron: skipping paused/deleted pipeline', { pipeline_id: item.pipeline_id });
      continue;
    }

    try {
      await executePipeline(pipeline, env);
    } catch (err) {
      logger.error('Cron: pipeline execution error', {
        pipeline_id: item.pipeline_id,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  // Also check for scheduled pipelines that are due
  const activePipelines = await env.DB.prepare(
    `SELECT * FROM pipelines WHERE status = 'active' AND schedule != '' AND (next_run IS NULL OR next_run <= ?)`
  ).bind(now).all<Pipeline>();

  if (activePipelines.results && activePipelines.results.length > 0) {
    for (const pipeline of activePipelines.results) {
      // Check if already queued or running
      const existing = await env.DB.prepare(
        `SELECT COUNT(*) as cnt FROM pipeline_queue WHERE pipeline_id = ?`
      ).bind(pipeline.id).first<{ cnt: number }>();

      const running = await env.DB.prepare(
        `SELECT COUNT(*) as cnt FROM pipeline_runs WHERE pipeline_id = ? AND status IN ('running', 'pending')`
      ).bind(pipeline.id).first<{ cnt: number }>();

      if ((existing?.cnt ?? 0) > 0 || (running?.cnt ?? 0) > 0) {
        continue;
      }

      // Queue it
      await env.DB.prepare(
        `INSERT INTO pipeline_queue (id, pipeline_id, priority, scheduled_for) VALUES (?, ?, 5, ?)`
      ).bind(generateId(), pipeline.id, now).run();

      // Calculate next run based on schedule
      const nextRun = calculateNextRun(pipeline.schedule);
      if (nextRun) {
        await env.DB.prepare(
          `UPDATE pipelines SET next_run = ?, updated_at = ? WHERE id = ?`
        ).bind(nextRun, now, pipeline.id).run();
      }

      logger.info('Cron: queued scheduled pipeline', { pipeline_id: pipeline.id, next_run: nextRun });
    }
  }

  logger.info('Cron: queue processing complete');
}

/**
 * Aggregate hourly stats into KV cache
 */
export async function aggregateHourlyStats(env: Env): Promise<void> {
  const now = nowISO();
  logger.info('Cron: aggregating hourly stats', { trigger_time: now });

  const hourAgo = new Date(Date.now() - 3600000).toISOString();

  // Get counts
  const totalPipelines = await env.DB.prepare(`SELECT COUNT(*) as cnt FROM pipelines`).first<{ cnt: number }>();
  const activePipelines = await env.DB.prepare(`SELECT COUNT(*) as cnt FROM pipelines WHERE status = 'active'`).first<{ cnt: number }>();
  const pausedPipelines = await env.DB.prepare(`SELECT COUNT(*) as cnt FROM pipelines WHERE status = 'paused'`).first<{ cnt: number }>();
  const errorPipelines = await env.DB.prepare(`SELECT COUNT(*) as cnt FROM pipelines WHERE status = 'error'`).first<{ cnt: number }>();

  // Run stats from last hour
  const hourlyRuns = await env.DB.prepare(
    `SELECT
      COUNT(*) as total_runs,
      SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
      SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
      SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) as running,
      SUM(records_processed) as total_records,
      SUM(records_failed) as total_failures
     FROM pipeline_runs WHERE started_at >= ?`
  ).bind(hourAgo).first<{
    total_runs: number;
    completed: number;
    failed: number;
    running: number;
    total_records: number;
    total_failures: number;
  }>();

  // Average run duration for completed runs in last hour
  const avgDuration = await env.DB.prepare(
    `SELECT AVG(
      (julianday(completed_at) - julianday(started_at)) * 86400
    ) as avg_seconds
     FROM pipeline_runs
     WHERE status = 'completed' AND started_at >= ? AND completed_at IS NOT NULL`
  ).bind(hourAgo).first<{ avg_seconds: number | null }>();

  // Queue depth
  const queueDepth = await env.DB.prepare(`SELECT COUNT(*) as cnt FROM pipeline_queue`).first<{ cnt: number }>();

  const stats = {
    timestamp: now,
    pipelines: {
      total: totalPipelines?.cnt ?? 0,
      active: activePipelines?.cnt ?? 0,
      paused: pausedPipelines?.cnt ?? 0,
      error: errorPipelines?.cnt ?? 0,
    },
    hourly_runs: {
      total: hourlyRuns?.total_runs ?? 0,
      completed: hourlyRuns?.completed ?? 0,
      failed: hourlyRuns?.failed ?? 0,
      running: hourlyRuns?.running ?? 0,
      records_processed: hourlyRuns?.total_records ?? 0,
      records_failed: hourlyRuns?.total_failures ?? 0,
    },
    avg_run_duration_seconds: avgDuration?.avg_seconds ?? 0,
    queue_depth: queueDepth?.cnt ?? 0,
  };

  // Cache stats with 300s TTL
  await env.CACHE.put('stats:current', JSON.stringify(stats), { expirationTtl: 300 });
  // Also keep hourly snapshot
  const hourKey = `stats:hourly:${new Date().toISOString().slice(0, 13)}`;
  await env.CACHE.put(hourKey, JSON.stringify(stats), { expirationTtl: 86400 * 7 });

  logger.info('Cron: hourly stats aggregated', { stats });
}

/**
 * Daily cleanup: remove completed runs older than 30 days, archive stats
 */
export async function dailyCleanup(env: Env): Promise<void> {
  const now = nowISO();
  logger.info('Cron: daily cleanup started', { trigger_time: now });

  const thirtyDaysAgo = new Date(Date.now() - 30 * 86400000).toISOString();

  // Count records to clean
  const oldRuns = await env.DB.prepare(
    `SELECT COUNT(*) as cnt FROM pipeline_runs WHERE status IN ('completed', 'failed') AND completed_at < ?`
  ).bind(thirtyDaysAgo).first<{ cnt: number }>();

  const runCount = oldRuns?.cnt ?? 0;

  if (runCount > 0) {
    // Delete old steps first (cascade doesn't always work with batch)
    await env.DB.prepare(
      `DELETE FROM pipeline_steps WHERE run_id IN (
        SELECT id FROM pipeline_runs WHERE status IN ('completed', 'failed') AND completed_at < ?
      )`
    ).bind(thirtyDaysAgo).run();

    // Delete old runs
    await env.DB.prepare(
      `DELETE FROM pipeline_runs WHERE status IN ('completed', 'failed') AND completed_at < ?`
    ).bind(thirtyDaysAgo).run();

    logger.info('Cron: cleaned old pipeline runs', { deleted: runCount, before: thirtyDaysAgo });
  }

  // Clean old queue entries
  await env.DB.prepare(
    `DELETE FROM pipeline_queue WHERE scheduled_for < ?`
  ).bind(thirtyDaysAgo).run();

  // Archive daily summary to KV
  const todayStats = await env.CACHE.get('stats:current', 'json');
  if (todayStats) {
    const archiveKey = `stats:daily:${new Date().toISOString().slice(0, 10)}`;
    await env.CACHE.put(archiveKey, JSON.stringify(todayStats), { expirationTtl: 86400 * 90 });
  }

  logger.info('Cron: daily cleanup complete', { runs_cleaned: runCount });
}

/**
 * Simple next-run calculator for common schedule patterns.
 * Supports: "every:Xm", "every:Xh", "every:Xd", cron-like not fully parsed (returns +1h default)
 */
function calculateNextRun(schedule: string): string | null {
  if (!schedule) return null;

  const now = Date.now();

  if (schedule.startsWith('every:')) {
    const spec = schedule.slice(6);
    const unit = spec.slice(-1);
    const value = parseInt(spec.slice(0, -1), 10);
    if (isNaN(value)) return new Date(now + 3600000).toISOString();

    switch (unit) {
      case 'm': return new Date(now + value * 60000).toISOString();
      case 'h': return new Date(now + value * 3600000).toISOString();
      case 'd': return new Date(now + value * 86400000).toISOString();
      default: return new Date(now + 3600000).toISOString();
    }
  }

  // Default: 1 hour from now for cron-like expressions
  return new Date(now + 3600000).toISOString();
}
