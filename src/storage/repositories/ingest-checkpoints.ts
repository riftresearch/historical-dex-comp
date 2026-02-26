import type { DuckDBConnection } from "@duckdb/node-api";
import { parseDateOrNull } from "../../utils/time";
import { queryFirstPrepared, queryPrepared, runPrepared } from "../duckdb-utils";

export interface IngestCheckpoint {
  streamKey: string;
  syncScope: string;
  newestCursor: string | null;
  newestEventAt: Date | null;
  newestProviderRecordId: string | null;
  oldestCursor: string | null;
  oldestEventAt: Date | null;
  oldestProviderRecordId: string | null;
  updatedAt: Date;
  runId: string | null;
}

interface IngestCheckpointRow {
  stream_key: string;
  sync_scope: string;
  newest_cursor: string | null;
  newest_event_at: string | null;
  newest_provider_record_id: string | null;
  oldest_cursor: string | null;
  oldest_event_at: string | null;
  oldest_provider_record_id: string | null;
  updated_at: string;
  run_id: string | null;
}

interface SwapBoundaryRow {
  provider_record_id: string;
  event_at: string;
}

function mapCheckpointRow(row: IngestCheckpointRow): IngestCheckpoint {
  return {
    streamKey: row.stream_key,
    syncScope: row.sync_scope,
    newestCursor: row.newest_cursor,
    newestEventAt: parseDateOrNull(row.newest_event_at),
    newestProviderRecordId: row.newest_provider_record_id,
    oldestCursor: row.oldest_cursor,
    oldestEventAt: parseDateOrNull(row.oldest_event_at),
    oldestProviderRecordId: row.oldest_provider_record_id,
    updatedAt: parseDateOrNull(row.updated_at) ?? new Date(),
    runId: row.run_id,
  };
}

export async function getIngestCheckpoint(
  connection: DuckDBConnection,
  streamKey: string,
): Promise<IngestCheckpoint | null> {
  const row = await queryFirstPrepared<IngestCheckpointRow>(
    connection,
    `SELECT
      stream_key,
      sync_scope,
      newest_cursor,
      CAST(newest_event_at AS VARCHAR) AS newest_event_at,
      newest_provider_record_id,
      oldest_cursor,
      CAST(oldest_event_at AS VARCHAR) AS oldest_event_at,
      oldest_provider_record_id,
      CAST(updated_at AS VARCHAR) AS updated_at,
      run_id
     FROM ingest_checkpoints
     WHERE stream_key = ?`,
    [streamKey],
  );

  return row ? mapCheckpointRow(row) : null;
}

export async function upsertIngestCheckpoint(
  connection: DuckDBConnection,
  checkpoint: IngestCheckpoint,
): Promise<void> {
  await runPrepared(
    connection,
    `INSERT INTO ingest_checkpoints (
      stream_key,
      sync_scope,
      newest_cursor,
      newest_event_at,
      newest_provider_record_id,
      oldest_cursor,
      oldest_event_at,
      oldest_provider_record_id,
      updated_at,
      run_id
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT (stream_key) DO UPDATE SET
      sync_scope = EXCLUDED.sync_scope,
      newest_cursor = EXCLUDED.newest_cursor,
      newest_event_at = EXCLUDED.newest_event_at,
      newest_provider_record_id = EXCLUDED.newest_provider_record_id,
      oldest_cursor = EXCLUDED.oldest_cursor,
      oldest_event_at = EXCLUDED.oldest_event_at,
      oldest_provider_record_id = EXCLUDED.oldest_provider_record_id,
      updated_at = EXCLUDED.updated_at,
      run_id = EXCLUDED.run_id`,
    [
      checkpoint.streamKey,
      checkpoint.syncScope,
      checkpoint.newestCursor,
      checkpoint.newestEventAt?.toISOString() ?? null,
      checkpoint.newestProviderRecordId,
      checkpoint.oldestCursor,
      checkpoint.oldestEventAt?.toISOString() ?? null,
      checkpoint.oldestProviderRecordId,
      checkpoint.updatedAt.toISOString(),
      checkpoint.runId,
    ],
  );
}

export async function listIngestCheckpoints(
  connection: DuckDBConnection,
): Promise<IngestCheckpoint[]> {
  const rows = await queryPrepared<IngestCheckpointRow>(
    connection,
    `SELECT
      stream_key,
      sync_scope,
      newest_cursor,
      CAST(newest_event_at AS VARCHAR) AS newest_event_at,
      newest_provider_record_id,
      oldest_cursor,
      CAST(oldest_event_at AS VARCHAR) AS oldest_event_at,
      oldest_provider_record_id,
      CAST(updated_at AS VARCHAR) AS updated_at,
      run_id
     FROM ingest_checkpoints
     ORDER BY stream_key ASC`,
  );

  return rows.map(mapCheckpointRow);
}

interface ReconcileCheckpointFromSwapsInput {
  streamKey: string;
  checkpoint: IngestCheckpoint | null;
  runId: string | null;
}

async function getOldestSwapBoundary(
  connection: DuckDBConnection,
): Promise<{ providerRecordId: string; eventAt: Date } | null> {
  const row = await queryFirstPrepared<SwapBoundaryRow>(
    connection,
    `SELECT
      provider_record_id,
      CAST(event_at AS VARCHAR) AS event_at
     FROM swaps_core
     WHERE event_at IS NOT NULL
     ORDER BY event_at ASC, provider_record_id ASC
     LIMIT 1`,
  );
  if (!row) {
    return null;
  }

  const eventAt = parseDateOrNull(row.event_at);
  if (!eventAt) {
    return null;
  }

  return {
    providerRecordId: row.provider_record_id,
    eventAt,
  };
}

async function getNewestSwapBoundary(
  connection: DuckDBConnection,
): Promise<{ providerRecordId: string; eventAt: Date } | null> {
  const row = await queryFirstPrepared<SwapBoundaryRow>(
    connection,
    `SELECT
      provider_record_id,
      CAST(event_at AS VARCHAR) AS event_at
     FROM swaps_core
     WHERE event_at IS NOT NULL
     ORDER BY event_at DESC, provider_record_id DESC
     LIMIT 1`,
  );
  if (!row) {
    return null;
  }

  const eventAt = parseDateOrNull(row.event_at);
  if (!eventAt) {
    return null;
  }

  return {
    providerRecordId: row.provider_record_id,
    eventAt,
  };
}

export async function reconcileCheckpointFromSwaps(
  connection: DuckDBConnection,
  input: ReconcileCheckpointFromSwapsInput,
): Promise<IngestCheckpoint | null> {
  const oldest = await getOldestSwapBoundary(connection);
  const newest = await getNewestSwapBoundary(connection);

  if (!input.checkpoint && !oldest && !newest) {
    return null;
  }

  const checkpoint: IngestCheckpoint = {
    streamKey: input.streamKey,
    syncScope: input.checkpoint?.syncScope ?? "swaps",
    newestCursor: input.checkpoint?.newestCursor ?? null,
    newestEventAt: input.checkpoint?.newestEventAt ?? null,
    newestProviderRecordId: input.checkpoint?.newestProviderRecordId ?? null,
    oldestCursor: input.checkpoint?.oldestCursor ?? null,
    oldestEventAt: input.checkpoint?.oldestEventAt ?? null,
    oldestProviderRecordId: input.checkpoint?.oldestProviderRecordId ?? null,
    updatedAt: new Date(),
    runId: input.runId ?? input.checkpoint?.runId ?? null,
  };

  if (newest) {
    checkpoint.newestEventAt = newest.eventAt;
    checkpoint.newestProviderRecordId = newest.providerRecordId;
  }
  if (oldest) {
    checkpoint.oldestEventAt = oldest.eventAt;
    checkpoint.oldestProviderRecordId = oldest.providerRecordId;
  }

  return checkpoint;
}
