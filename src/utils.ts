import type { ApiResponse } from './types';

export function generateId(): string {
  const ts = Date.now().toString(36);
  const rand = Math.random().toString(36).substring(2, 10);
  return `${ts}-${rand}`;
}

export function response<T>(data: T, status = 200): Response {
  const body: ApiResponse<T> = {
    success: status >= 200 && status < 300,
    data,
    timestamp: new Date().toISOString(),
  };
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  });
}

export function errorResponse(error: string, status = 400): Response {
  const body: ApiResponse = {
    success: false,
    error,
    timestamp: new Date().toISOString(),
  };
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  });
}

export function nowISO(): string {
  return new Date().toISOString();
}

/**
 * Exponential backoff delay calculation: 1s, 2s, 4s, 8s (max 3 retries)
 */
export function getRetryDelay(retryCount: number): number {
  const baseMs = 1000;
  return baseMs * Math.pow(2, retryCount);
}

export const MAX_RETRIES = 3;

/**
 * Parse pagination params from URL search params
 */
export function parsePagination(url: URL): { limit: number; offset: number } {
  const limit = Math.min(Math.max(parseInt(url.searchParams.get('limit') || '50', 10), 1), 200);
  const offset = Math.max(parseInt(url.searchParams.get('offset') || '0', 10), 0);
  return { limit, offset };
}
