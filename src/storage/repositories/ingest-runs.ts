import { randomUUID } from "node:crypto";
import type { DuckDBConnection } from "@duckdb/node-api";
import type { ProviderKey } from "../../domain/provider-key";
import { parseDateOrNull, toIsoOrNull } from "../../utils/time";
import { queryFirstPrepared, queryPrepared, runPrepared } from "../duckdb-utils";

export interface IngestRunCounts {
  recordsFetched: number;
  recordsNormalized: number;
  recordsUpserted: number;
}

export interface StartIngestRunInput {
  providerKey: ProviderKey;
  fromTs: Date | null;
  toTs: Date | null;
}

interface IngestRunRow {
  run_id: string;
  provider_key: string;
  started_at: string;
  ended_at: string | null;
  status: string;
  from_ts: string | null;
  to_ts: string | null;
  records_fetched: number | bigint;
  records_normalized: number | bigint;
  records_upserted: number | bigint;
  error_message: string | null;
}

export interface IngestRun {
  runId: string;
  providerKey: string;
  startedAt: Date | null;
  endedAt: Date | null;
  status: string;
  fromTs: Date | null;
  toTs: Date | null;
  recordsFetched: number;
  recordsNormalized: number;
  recordsUpserted: number;
  errorMessage: string | null;
}

function toNumber(value: number | bigint): number {
  return Number(value);
}

function mapIngestRunRow(row: IngestRunRow): IngestRun {
  return {
    runId: row.run_id,
    providerKey: row.provider_key,
    startedAt: parseDateOrNull(row.started_at),
    endedAt: parseDateOrNull(row.ended_at),
    status: row.status,
    fromTs: parseDateOrNull(row.from_ts),
    toTs: parseDateOrNull(row.to_ts),
    recordsFetched: toNumber(row.records_fetched),
    recordsNormalized: toNumber(row.records_normalized),
    recordsUpserted: toNumber(row.records_upserted),
    errorMessage: row.error_message,
  };
}

export async function startIngestRun(
  connection: DuckDBConnection,
  input: StartIngestRunInput,
): Promise<string> {
  const runId = randomUUID();
  const startedAt = new Date();

  await runPrepared(
    connection,
    `INSERT INTO ingest_runs (
      run_id,
      provider_key,
      started_at,
      status,
      from_ts,
      to_ts,
      records_fetched,
      records_normalized,
      records_upserted
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      runId,
      input.providerKey,
      startedAt.toISOString(),
      "running",
      toIsoOrNull(input.fromTs),
      toIsoOrNull(input.toTs),
      0,
      0,
      0,
    ],
  );

  return runId;
}

export async function finishIngestRunSuccess(
  connection: DuckDBConnection,
  runId: string,
  counts: IngestRunCounts,
): Promise<void> {
  await runPrepared(
    connection,
    `UPDATE ingest_runs
     SET ended_at = ?,
         status = ?,
         records_fetched = ?,
         records_normalized = ?,
         records_upserted = ?,
         error_message = NULL
     WHERE run_id = ?`,
    [
      new Date().toISOString(),
      "success",
      counts.recordsFetched,
      counts.recordsNormalized,
      counts.recordsUpserted,
      runId,
    ],
  );
}

export async function finishIngestRunFailure(
  connection: DuckDBConnection,
  runId: string,
  counts: IngestRunCounts,
  errorMessage: string,
): Promise<void> {
  await runPrepared(
    connection,
    `UPDATE ingest_runs
     SET ended_at = ?,
         status = ?,
         records_fetched = ?,
         records_normalized = ?,
         records_upserted = ?,
         error_message = ?
     WHERE run_id = ?`,
    [
      new Date().toISOString(),
      "failed",
      counts.recordsFetched,
      counts.recordsNormalized,
      counts.recordsUpserted,
      errorMessage,
      runId,
    ],
  );
}

export async function listIngestRuns(
  connection: DuckDBConnection,
  limit: number,
): Promise<IngestRun[]> {
  const rows = await queryPrepared<IngestRunRow>(
    connection,
    `SELECT
      run_id,
      provider_key,
      CAST(started_at AS VARCHAR) AS started_at,
      CAST(ended_at AS VARCHAR) AS ended_at,
      status,
      CAST(from_ts AS VARCHAR) AS from_ts,
      CAST(to_ts AS VARCHAR) AS to_ts,
      records_fetched,
      records_normalized,
      records_upserted,
      error_message
     FROM ingest_runs
     ORDER BY started_at DESC
     LIMIT ?`,
    [limit],
  );

  return rows.map(mapIngestRunRow);
}

export async function failRunningIngestRuns(
  connection: DuckDBConnection,
  errorMessage = "interrupted before completion",
): Promise<number> {
  const before = await queryFirstPrepared<{ running_count: number | bigint }>(
    connection,
    `SELECT COUNT(*) AS running_count
     FROM ingest_runs
     WHERE status = ?
       AND ended_at IS NULL`,
    ["running"],
  );
  const runningCount =
    typeof before?.running_count === "bigint"
      ? Number(before.running_count)
      : (before?.running_count ?? 0);
  if (runningCount <= 0) {
    return 0;
  }

  await runPrepared(
    connection,
    `UPDATE ingest_runs
     SET ended_at = ?,
         status = ?,
         error_message = COALESCE(error_message, ?)
     WHERE status = ?
       AND ended_at IS NULL`,
    [new Date().toISOString(), "failed", errorMessage, "running"],
  );
  return runningCount;
}
