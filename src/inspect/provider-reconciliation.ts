import type { DuckDBConnection } from "@duckdb/node-api";
import { COVERAGE_FLOOR } from "../domain/coverage-floor";
import type { ProviderKey } from "../domain/provider-key";
import { isSwapRowWithinIngestScope } from "../ingest/swap-scope";
import { queryFirstPrepared, queryPrepared } from "../storage/duckdb-utils";

interface CountRow {
  count: number | bigint;
}

interface ScopeRow {
  source_chain_canonical: string | null;
  destination_chain_canonical: string | null;
  source_asset_symbol: string | null;
  destination_asset_symbol: string | null;
  source_asset_id: string | null;
  destination_asset_id: string | null;
}

interface DailyCountRow {
  day: unknown;
  count: number | bigint;
}

interface RecentRouteRow {
  route_key: string;
  count: number | bigint;
}

export interface ProviderReconciliationIssue {
  severity: "warn" | "error";
  code:
    | "core_rows_missing_raw"
    | "orphan_raw_rows"
    | "stale_raw_hash_latest_rows"
    | "duplicate_provider_record_rows"
    | "rows_older_than_floor"
    | "out_of_scope_rows"
    | "null_event_at_rows"
    | "gap_days";
  count: number;
  message: string;
}

export interface ProviderDailyCount {
  day: string;
  count: number;
}

export interface ProviderRouteCount {
  routeKey: string;
  count: number;
}

export interface ProviderReconciliationReport {
  severity: "ok" | "warn" | "error";
  coreRowsMissingRaw: number;
  orphanRawRows: number;
  staleRawHashLatestRows: number;
  duplicateProviderRecordRows: number;
  rowsOlderThanFloor: number;
  outOfScopeRows: number;
  nullEventAtRows: number;
  gapDays: string[];
  maxConsecutiveGapDays: number;
  recentDailyCounts: ProviderDailyCount[];
  topRecentRoutes: ProviderRouteCount[];
  issues: ProviderReconciliationIssue[];
}

export interface RunProviderReconciliationInput {
  providerKey: ProviderKey;
  connection: DuckDBConnection;
  windowStart: Date;
  windowEnd: Date;
}

function toNumber(value: number | bigint | null | undefined): number {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : 0;
  }
  if (typeof value === "bigint") {
    return Number(value);
  }
  return 0;
}

function toDayKey(value: unknown): string | null {
  if (typeof value === "string") {
    return value.slice(0, 10);
  }
  if (value instanceof Date) {
    return value.toISOString().slice(0, 10);
  }
  if (value && typeof value === "object" && "toString" in value && typeof value.toString === "function") {
    const rendered = value.toString();
    return typeof rendered === "string" && rendered.length >= 10 ? rendered.slice(0, 10) : null;
  }
  return null;
}

function startOfUtcDay(input: Date): Date {
  return new Date(Date.UTC(input.getUTCFullYear(), input.getUTCMonth(), input.getUTCDate()));
}

function addUtcDays(input: Date, days: number): Date {
  return new Date(input.getTime() + days * 86_400_000);
}

async function getCount(connection: DuckDBConnection, sql: string, params: unknown[] = []): Promise<number> {
  const row = await queryFirstPrepared<CountRow>(connection, sql, params);
  return toNumber(row?.count ?? 0);
}

async function countOutOfScopeRows(connection: DuckDBConnection): Promise<number> {
  const rows = await queryPrepared<ScopeRow>(
    connection,
    `SELECT
       source_chain_canonical,
       destination_chain_canonical,
       source_asset_symbol,
       destination_asset_symbol,
       source_asset_id,
       destination_asset_id
     FROM swaps_core`,
  );

  let outOfScopeRows = 0;
  for (const row of rows) {
    if (!isSwapRowWithinIngestScope(row)) {
      outOfScopeRows += 1;
    }
  }
  return outOfScopeRows;
}

async function loadDailyCounts(
  connection: DuckDBConnection,
  windowStart: Date,
  windowEnd: Date,
): Promise<Map<string, number>> {
  const rows = await queryPrepared<DailyCountRow>(
    connection,
    `SELECT
       CAST(date_trunc('day', event_at) AS DATE) AS day,
       COUNT(*) AS count
     FROM swaps_core
     WHERE event_at >= ? AND event_at <= ?
     GROUP BY 1
     ORDER BY 1 ASC`,
    [windowStart, windowEnd],
  );

  const counts = new Map<string, number>();
  for (const row of rows) {
    const dayKey = toDayKey(row.day);
    if (!dayKey) {
      continue;
    }
    counts.set(dayKey, toNumber(row.count));
  }
  return counts;
}

function computeGapDays(windowStart: Date, windowEnd: Date, dailyCounts: Map<string, number>): {
  gapDays: string[];
  maxConsecutiveGapDays: number;
} {
  const gapDays: string[] = [];
  let maxConsecutiveGapDays = 0;
  let consecutiveGapDays = 0;

  for (let cursor = startOfUtcDay(windowStart); cursor <= windowEnd; cursor = addUtcDays(cursor, 1)) {
    const day = cursor.toISOString().slice(0, 10);
    const count = dailyCounts.get(day) ?? 0;
    if (count > 0) {
      if (consecutiveGapDays > maxConsecutiveGapDays) {
        maxConsecutiveGapDays = consecutiveGapDays;
      }
      consecutiveGapDays = 0;
      continue;
    }

    gapDays.push(day);
    consecutiveGapDays += 1;
  }

  if (consecutiveGapDays > maxConsecutiveGapDays) {
    maxConsecutiveGapDays = consecutiveGapDays;
  }

  return {
    gapDays,
    maxConsecutiveGapDays,
  };
}

function collectRecentDailyCounts(dailyCounts: Map<string, number>, limit: number): ProviderDailyCount[] {
  return [...dailyCounts.entries()]
    .sort(([left], [right]) => left.localeCompare(right))
    .slice(-Math.max(1, limit))
    .map(([day, count]) => ({ day, count }));
}

async function loadTopRecentRoutes(
  connection: DuckDBConnection,
  windowStart: Date,
  windowEnd: Date,
): Promise<ProviderRouteCount[]> {
  const rows = await queryPrepared<RecentRouteRow>(
    connection,
    `SELECT
       concat(
         coalesce(source_chain_canonical, 'unknown'),
         ':',
         coalesce(upper(source_asset_symbol), 'unknown'),
         '->',
         coalesce(destination_chain_canonical, 'unknown'),
         ':',
         coalesce(upper(destination_asset_symbol), 'unknown')
       ) AS route_key,
       COUNT(*) AS count
     FROM swaps_core
     WHERE coalesce(event_at, created_at, updated_at) >= ?
       AND coalesce(event_at, created_at, updated_at) <= ?
     GROUP BY 1
     ORDER BY count DESC, route_key ASC
     LIMIT 8`,
    [windowStart, windowEnd],
  );

  return rows.map((row) => ({
    routeKey: row.route_key,
    count: toNumber(row.count),
  }));
}

function buildIssues(input: {
  coreRowsMissingRaw: number;
  orphanRawRows: number;
  staleRawHashLatestRows: number;
  duplicateProviderRecordRows: number;
  rowsOlderThanFloor: number;
  outOfScopeRows: number;
  nullEventAtRows: number;
  gapDays: string[];
  maxConsecutiveGapDays: number;
}): ProviderReconciliationIssue[] {
  const issues: ProviderReconciliationIssue[] = [];

  if (input.coreRowsMissingRaw > 0) {
    issues.push({
      severity: "error",
      code: "core_rows_missing_raw",
      count: input.coreRowsMissingRaw,
      message: `${input.coreRowsMissingRaw} swaps_core rows have no matching swaps_raw row.`,
    });
  }

  if (input.orphanRawRows > 0) {
    issues.push({
      severity: "error",
      code: "orphan_raw_rows",
      count: input.orphanRawRows,
      message: `${input.orphanRawRows} swaps_raw rows have no matching swaps_core row.`,
    });
  }

  if (input.staleRawHashLatestRows > 0) {
    issues.push({
      severity: "error",
      code: "stale_raw_hash_latest_rows",
      count: input.staleRawHashLatestRows,
      message: `${input.staleRawHashLatestRows} swaps_core rows reference raw_hash_latest values missing from swaps_raw.`,
    });
  }

  if (input.duplicateProviderRecordRows > 0) {
    issues.push({
      severity: "warn",
      code: "duplicate_provider_record_rows",
      count: input.duplicateProviderRecordRows,
      message: `${input.duplicateProviderRecordRows} extra swaps_core rows share a provider_record_id + record_granularity key.`,
    });
  }

  if (input.rowsOlderThanFloor > 0) {
    issues.push({
      severity: "warn",
      code: "rows_older_than_floor",
      count: input.rowsOlderThanFloor,
      message: `${input.rowsOlderThanFloor} rows are older than the locked coverage floor ${COVERAGE_FLOOR.toISOString()}.`,
    });
  }

  if (input.outOfScopeRows > 0) {
    issues.push({
      severity: "warn",
      code: "out_of_scope_rows",
      count: input.outOfScopeRows,
      message: `${input.outOfScopeRows} rows fall outside the configured ingest scope.`,
    });
  }

  if (input.nullEventAtRows > 0) {
    issues.push({
      severity: "warn",
      code: "null_event_at_rows",
      count: input.nullEventAtRows,
      message: `${input.nullEventAtRows} rows are missing event_at timestamps.`,
    });
  }

  if (input.gapDays.length > 0) {
    issues.push({
      severity: input.maxConsecutiveGapDays >= 3 ? "error" : "warn",
      code: "gap_days",
      count: input.gapDays.length,
      message: `${input.gapDays.length} empty UTC day buckets detected inside the audit window (max consecutive gap ${input.maxConsecutiveGapDays} days).`,
    });
  }

  return issues;
}

function deriveSeverity(issues: ProviderReconciliationIssue[]): "ok" | "warn" | "error" {
  if (issues.some((issue) => issue.severity === "error")) {
    return "error";
  }
  if (issues.some((issue) => issue.severity === "warn")) {
    return "warn";
  }
  return "ok";
}

export async function runProviderReconciliation(
  input: RunProviderReconciliationInput,
): Promise<ProviderReconciliationReport> {
  const { connection, windowStart, windowEnd } = input;

  const [
    coreRowsMissingRaw,
    orphanRawRows,
    staleRawHashLatestRows,
    duplicateProviderRecordRows,
    rowsOlderThanFloor,
    nullEventAtRows,
  ] = await Promise.all([
    getCount(
      connection,
      `SELECT COUNT(*) AS count
       FROM swaps_core sc
       LEFT JOIN swaps_raw sr
         ON sr.normalized_id = sc.normalized_id
       WHERE sr.normalized_id IS NULL`,
    ),
    getCount(
      connection,
      `SELECT COUNT(*) AS count
       FROM swaps_raw sr
       LEFT JOIN swaps_core sc
         ON sc.normalized_id = sr.normalized_id
       WHERE sc.normalized_id IS NULL`,
    ),
    getCount(
      connection,
      `SELECT COUNT(*) AS count
       FROM swaps_core sc
       LEFT JOIN swaps_raw sr
         ON sr.normalized_id = sc.normalized_id
        AND sr.raw_hash = sc.raw_hash_latest
       WHERE sr.normalized_id IS NULL`,
    ),
    getCount(
      connection,
      `SELECT coalesce(SUM(duplicate_count), 0) AS count
       FROM (
         SELECT COUNT(*) - 1 AS duplicate_count
         FROM swaps_core
         WHERE provider_record_id IS NOT NULL
         GROUP BY provider_record_id, record_granularity
         HAVING COUNT(*) > 1
       ) duplicates`,
    ),
    getCount(
      connection,
      `SELECT COUNT(*) AS count
       FROM swaps_core
       WHERE coalesce(event_at, created_at, updated_at) < ?`,
      [COVERAGE_FLOOR],
    ),
    getCount(
      connection,
      `SELECT COUNT(*) AS count
       FROM swaps_core
       WHERE event_at IS NULL`,
    ),
  ]);

  const [outOfScopeRows, dailyCounts, topRecentRoutes] = await Promise.all([
    countOutOfScopeRows(connection),
    loadDailyCounts(connection, windowStart, windowEnd),
    loadTopRecentRoutes(connection, windowStart, windowEnd),
  ]);

  const { gapDays, maxConsecutiveGapDays } = computeGapDays(windowStart, windowEnd, dailyCounts);
  const recentDailyCounts = collectRecentDailyCounts(dailyCounts, 10);
  const issues = buildIssues({
    coreRowsMissingRaw,
    orphanRawRows,
    staleRawHashLatestRows,
    duplicateProviderRecordRows,
    rowsOlderThanFloor,
    outOfScopeRows,
    nullEventAtRows,
    gapDays,
    maxConsecutiveGapDays,
  });

  return {
    severity: deriveSeverity(issues),
    coreRowsMissingRaw,
    orphanRawRows,
    staleRawHashLatestRows,
    duplicateProviderRecordRows,
    rowsOlderThanFloor,
    outOfScopeRows,
    nullEventAtRows,
    gapDays,
    maxConsecutiveGapDays,
    recentDailyCounts,
    topRecentRoutes,
    issues,
  };
}
