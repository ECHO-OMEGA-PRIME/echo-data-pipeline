import type { TransformStep } from './types';
import { logger } from './logger';

type DataRecord = Record<string, unknown>;

/**
 * Map transform: applies field mappings/renaming to each record.
 * config.mappings: { newField: "oldField" } or { newField: { field: "oldField", default: value } }
 */
function applyMap(records: DataRecord[], config: Record<string, unknown>): DataRecord[] {
  const mappings = (config.mappings || {}) as Record<string, unknown>;
  return records.map(record => {
    const result: DataRecord = { ...record };
    for (const [newKey, mapping] of Object.entries(mappings)) {
      if (typeof mapping === 'string') {
        result[newKey] = record[mapping] ?? null;
      } else if (mapping && typeof mapping === 'object' && 'field' in mapping) {
        const m = mapping as { field: string; default?: unknown };
        result[newKey] = record[m.field] ?? m.default ?? null;
      }
    }
    if (config.remove_fields && Array.isArray(config.remove_fields)) {
      for (const field of config.remove_fields) {
        delete result[field as string];
      }
    }
    return result;
  });
}

/**
 * Filter transform: keeps records matching conditions.
 * config.conditions: [{ field, operator, value }]
 * operators: eq, ne, gt, lt, gte, lte, contains, exists, not_exists
 */
function applyFilter(records: DataRecord[], config: Record<string, unknown>): DataRecord[] {
  const conditions = (config.conditions || []) as Array<{ field: string; operator: string; value?: unknown }>;
  const mode = (config.mode as string) || 'all'; // 'all' or 'any'

  return records.filter(record => {
    const results = conditions.map(cond => {
      const fieldVal = record[cond.field];
      switch (cond.operator) {
        case 'eq': return fieldVal === cond.value;
        case 'ne': return fieldVal !== cond.value;
        case 'gt': return typeof fieldVal === 'number' && typeof cond.value === 'number' && fieldVal > cond.value;
        case 'lt': return typeof fieldVal === 'number' && typeof cond.value === 'number' && fieldVal < cond.value;
        case 'gte': return typeof fieldVal === 'number' && typeof cond.value === 'number' && fieldVal >= cond.value;
        case 'lte': return typeof fieldVal === 'number' && typeof cond.value === 'number' && fieldVal <= cond.value;
        case 'contains': return typeof fieldVal === 'string' && typeof cond.value === 'string' && fieldVal.includes(cond.value);
        case 'exists': return fieldVal !== undefined && fieldVal !== null;
        case 'not_exists': return fieldVal === undefined || fieldVal === null;
        default: return true;
      }
    });

    return mode === 'any' ? results.some(Boolean) : results.every(Boolean);
  });
}

/**
 * Aggregate transform: groups and aggregates records.
 * config.group_by: field name
 * config.aggregations: [{ field, operation, as }]
 * operations: count, sum, avg, min, max
 */
function applyAggregate(records: DataRecord[], config: Record<string, unknown>): DataRecord[] {
  const groupBy = config.group_by as string | undefined;
  const aggregations = (config.aggregations || []) as Array<{ field: string; operation: string; as: string }>;

  if (!groupBy) {
    // Single group aggregation over all records
    const result: DataRecord = { _group: 'all', _count: records.length };
    for (const agg of aggregations) {
      result[agg.as] = computeAggregation(records, agg.field, agg.operation);
    }
    return [result];
  }

  const groups = new Map<string, DataRecord[]>();
  for (const record of records) {
    const key = String(record[groupBy] ?? '_null');
    const group = groups.get(key);
    if (group) {
      group.push(record);
    } else {
      groups.set(key, [record]);
    }
  }

  const results: DataRecord[] = [];
  for (const [key, groupRecords] of groups) {
    const row: DataRecord = { [groupBy]: key, _count: groupRecords.length };
    for (const agg of aggregations) {
      row[agg.as] = computeAggregation(groupRecords, agg.field, agg.operation);
    }
    results.push(row);
  }
  return results;
}

function computeAggregation(records: DataRecord[], field: string, operation: string): unknown {
  const values = records
    .map(r => r[field])
    .filter((v): v is number => typeof v === 'number');

  switch (operation) {
    case 'count': return records.length;
    case 'sum': return values.reduce((a, b) => a + b, 0);
    case 'avg': return values.length > 0 ? values.reduce((a, b) => a + b, 0) / values.length : 0;
    case 'min': return values.length > 0 ? Math.min(...values) : null;
    case 'max': return values.length > 0 ? Math.max(...values) : null;
    default: return null;
  }
}

/**
 * Enrich transform: adds static fields or computed fields.
 * config.fields: { fieldName: staticValue }
 * config.computed: [{ as, expression }] (expression: "concat:field1,field2" | "uppercase:field" | "lowercase:field" | "timestamp")
 */
function applyEnrich(records: DataRecord[], config: Record<string, unknown>): DataRecord[] {
  const staticFields = (config.fields || {}) as Record<string, unknown>;
  const computed = (config.computed || []) as Array<{ as: string; expression: string }>;

  return records.map(record => {
    const result: DataRecord = { ...record, ...staticFields };
    for (const comp of computed) {
      const [op, ...args] = comp.expression.split(':');
      const fieldArgs = args.join(':').split(',');
      switch (op) {
        case 'concat':
          result[comp.as] = fieldArgs.map(f => String(record[f.trim()] ?? '')).join('');
          break;
        case 'uppercase':
          result[comp.as] = typeof record[fieldArgs[0] ?? ''] === 'string' ? (record[fieldArgs[0] ?? ''] as string).toUpperCase() : null;
          break;
        case 'lowercase':
          result[comp.as] = typeof record[fieldArgs[0] ?? ''] === 'string' ? (record[fieldArgs[0] ?? ''] as string).toLowerCase() : null;
          break;
        case 'timestamp':
          result[comp.as] = new Date().toISOString();
          break;
        default:
          result[comp.as] = null;
      }
    }
    return result;
  });
}

/**
 * Execute a single transform step on data records
 */
export function executeTransformStep(
  records: DataRecord[],
  step: TransformStep
): { records: DataRecord[]; error: string | null } {
  try {
    let result: DataRecord[];
    switch (step.type) {
      case 'map':
        result = applyMap(records, step.config);
        break;
      case 'filter':
        result = applyFilter(records, step.config);
        break;
      case 'aggregate':
        result = applyAggregate(records, step.config);
        break;
      case 'enrich':
        result = applyEnrich(records, step.config);
        break;
      default:
        return { records, error: `Unknown transform type: ${step.type}` };
    }
    return { records: result, error: null };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.error('Transform step failed', { step_name: step.name, step_type: step.type, error: msg });
    return { records: [], error: msg };
  }
}

/**
 * Execute full transform pipeline on records
 */
export function executeTransformPipeline(
  records: DataRecord[],
  steps: TransformStep[]
): { records: DataRecord[]; stepResults: Array<{ name: string; type: string; inputCount: number; outputCount: number; error: string | null }> } {
  let current = records;
  const stepResults: Array<{ name: string; type: string; inputCount: number; outputCount: number; error: string | null }> = [];

  for (const step of steps) {
    const inputCount = current.length;
    const result = executeTransformStep(current, step);
    stepResults.push({
      name: step.name,
      type: step.type,
      inputCount,
      outputCount: result.records.length,
      error: result.error,
    });
    if (result.error) {
      break;
    }
    current = result.records;
  }

  return { records: current, stepResults };
}
