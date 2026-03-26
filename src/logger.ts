type LogLevel = 'debug' | 'info' | 'warn' | 'error' | 'fatal';

interface LogEntry {
  timestamp: string;
  level: LogLevel;
  service: string;
  message: string;
  data?: Record<string, unknown>;
  correlation_id?: string;
}

const LOG_LEVELS: Record<LogLevel, number> = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3,
  fatal: 4,
};

const MIN_LEVEL: LogLevel = 'info';

function emit(entry: LogEntry): void {
  if (LOG_LEVELS[entry.level] < LOG_LEVELS[MIN_LEVEL]) return;
  const output = JSON.stringify(entry);
  if (entry.level === 'error' || entry.level === 'fatal') {
    console.error(output);
  } else if (entry.level === 'warn') {
    console.warn(output);
  } else {
    console.log(output);
  }
}

export const logger = {
  debug(message: string, data?: Record<string, unknown>, correlationId?: string): void {
    emit({ timestamp: new Date().toISOString(), level: 'debug', service: 'echo-data-pipeline', message, data, correlation_id: correlationId });
  },
  info(message: string, data?: Record<string, unknown>, correlationId?: string): void {
    emit({ timestamp: new Date().toISOString(), level: 'info', service: 'echo-data-pipeline', message, data, correlation_id: correlationId });
  },
  warn(message: string, data?: Record<string, unknown>, correlationId?: string): void {
    emit({ timestamp: new Date().toISOString(), level: 'warn', service: 'echo-data-pipeline', message, data, correlation_id: correlationId });
  },
  error(message: string, data?: Record<string, unknown>, correlationId?: string): void {
    emit({ timestamp: new Date().toISOString(), level: 'error', service: 'echo-data-pipeline', message, data, correlation_id: correlationId });
  },
  fatal(message: string, data?: Record<string, unknown>, correlationId?: string): void {
    emit({ timestamp: new Date().toISOString(), level: 'fatal', service: 'echo-data-pipeline', message, data, correlation_id: correlationId });
  },
};
