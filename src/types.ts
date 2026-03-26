export interface Env {
  DB: D1Database;
  CACHE: KVNamespace;
  SHARED_BRAIN: Fetcher;
  ALERT_ROUTER: Fetcher;
  KNOWLEDGE_FORGE: Fetcher;
}

export interface Pipeline {
  id: string;
  name: string;
  description: string;
  source_type: string;
  source_config: string;
  destination_type: string;
  destination_config: string;
  transform_steps: string;
  schedule: string;
  status: 'active' | 'paused' | 'error';
  last_run: string | null;
  next_run: string | null;
  created_at: string;
  updated_at: string;
}

export interface PipelineRun {
  id: string;
  pipeline_id: string;
  status: 'pending' | 'running' | 'completed' | 'failed' | 'retrying';
  started_at: string | null;
  completed_at: string | null;
  records_processed: number;
  records_failed: number;
  error_message: string | null;
  retry_count: number;
  output: string | null;
}

export interface PipelineStep {
  id: string;
  run_id: string;
  step_name: string;
  step_type: string;
  status: string;
  input_count: number;
  output_count: number;
  started_at: string | null;
  completed_at: string | null;
  error: string | null;
}

export interface QueueItem {
  id: string;
  pipeline_id: string;
  priority: number;
  scheduled_for: string;
  created_at: string;
}

export interface ApiResponse<T = unknown> {
  success: boolean;
  data?: T;
  error?: string;
  timestamp: string;
}

export interface TransformStep {
  name: string;
  type: 'map' | 'filter' | 'aggregate' | 'enrich';
  config: Record<string, unknown>;
}

export interface CreatePipelineBody {
  name: string;
  source_type: string;
  source_config: Record<string, unknown>;
  destination_type: string;
  destination_config: Record<string, unknown>;
  transform_steps: TransformStep[];
  schedule?: string;
  description?: string;
}

export interface UpdatePipelineBody {
  name?: string;
  source_type?: string;
  source_config?: Record<string, unknown>;
  destination_type?: string;
  destination_config?: Record<string, unknown>;
  transform_steps?: TransformStep[];
  schedule?: string;
  description?: string;
  status?: 'active' | 'paused';
}
