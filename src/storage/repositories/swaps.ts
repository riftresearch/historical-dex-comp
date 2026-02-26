import type { DuckDBConnection } from "@duckdb/node-api";
import { isAtOrAfterCoverageFloor } from "../../domain/coverage-floor";
import type { NormalizedSwapRowInput } from "../../domain/normalized-swap";
import { normalizeSqlParams, queryFirstPrepared, runPrepared, type SqlParam } from "../duckdb-utils";

export interface RawSwapRowInput {
  normalized_id: string;
  raw_hash: string;
  raw_json: string;
  observed_at: Date;
  source_endpoint: string;
  source_cursor?: string | null;
  run_id: string;
}

const CORE_COLUMNS = [
  "normalized_id",
  "provider_key",
  "provider_record_id",
  "provider_parent_id",
  "record_granularity",
  "status_canonical",
  "status_raw",
  "failure_reason_raw",
  "created_at",
  "updated_at",
  "event_at",
  "source_chain_canonical",
  "destination_chain_canonical",
  "source_chain_raw",
  "destination_chain_raw",
  "source_chain_id_raw",
  "destination_chain_id_raw",
  "source_asset_id",
  "destination_asset_id",
  "source_asset_symbol",
  "destination_asset_symbol",
  "source_asset_decimals",
  "destination_asset_decimals",
  "amount_in_atomic",
  "amount_out_atomic",
  "amount_in_normalized",
  "amount_out_normalized",
  "amount_in_usd",
  "amount_out_usd",
  "fee_atomic",
  "fee_normalized",
  "fee_usd",
  "slippage_bps",
  "solver_id",
  "route_hint",
  "source_tx_hash",
  "destination_tx_hash",
  "refund_tx_hash",
  "extra_tx_hashes",
  "is_final",
  "raw_hash_latest",
  "source_endpoint",
  "ingested_at",
  "run_id",
] as const;

const CORE_UPSERT_SQL = `
INSERT INTO swaps_core (${CORE_COLUMNS.join(", ")})
VALUES (${CORE_COLUMNS.map(() => "?").join(", ")})
ON CONFLICT (normalized_id) DO UPDATE SET
${CORE_COLUMNS.filter((column) => column !== "normalized_id")
  .map((column) => `${column} = EXCLUDED.${column}`)
  .join(",\n")}
`;

const RAW_INSERT_SQL = `
INSERT INTO swaps_raw (
  normalized_id,
  raw_hash,
  raw_json,
  observed_at,
  source_endpoint,
  source_cursor,
  run_id
)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (normalized_id, raw_hash) DO NOTHING
`;

function serializeJsonOrNull(value: unknown): string | null {
  if (value === null || value === undefined) {
    return null;
  }

  return JSON.stringify(value);
}

function coreRowToParams(row: NormalizedSwapRowInput): SqlParam[] {
  return normalizeSqlParams([
    row.normalized_id,
    row.provider_key,
    row.provider_record_id,
    row.provider_parent_id ?? null,
    row.record_granularity,
    row.status_canonical,
    row.status_raw ?? null,
    row.failure_reason_raw ?? null,
    row.created_at?.toISOString() ?? null,
    row.updated_at?.toISOString() ?? null,
    row.event_at?.toISOString() ?? null,
    row.source_chain_canonical ?? null,
    row.destination_chain_canonical ?? null,
    row.source_chain_raw ?? null,
    row.destination_chain_raw ?? null,
    row.source_chain_id_raw ?? null,
    row.destination_chain_id_raw ?? null,
    row.source_asset_id ?? null,
    row.destination_asset_id ?? null,
    row.source_asset_symbol ?? null,
    row.destination_asset_symbol ?? null,
    row.source_asset_decimals ?? null,
    row.destination_asset_decimals ?? null,
    row.amount_in_atomic ?? null,
    row.amount_out_atomic ?? null,
    row.amount_in_normalized ?? null,
    row.amount_out_normalized ?? null,
    row.amount_in_usd ?? null,
    row.amount_out_usd ?? null,
    row.fee_atomic ?? null,
    row.fee_normalized ?? null,
    row.fee_usd ?? null,
    row.slippage_bps ?? null,
    row.solver_id ?? null,
    row.route_hint ?? null,
    row.source_tx_hash ?? null,
    row.destination_tx_hash ?? null,
    row.refund_tx_hash ?? null,
    serializeJsonOrNull(row.extra_tx_hashes),
    row.is_final ?? null,
    row.raw_hash_latest,
    row.source_endpoint,
    row.ingested_at.toISOString(),
    row.run_id,
  ]);
}

function rawRowToParams(row: RawSwapRowInput): SqlParam[] {
  return normalizeSqlParams([
    row.normalized_id,
    row.raw_hash,
    row.raw_json,
    row.observed_at.toISOString(),
    row.source_endpoint,
    row.source_cursor ?? null,
    row.run_id,
  ]);
}

export interface PersistSwapsResult {
  coreUpserts: number;
  rawInsertAttempts: number;
}

interface CountRow {
  count: number | bigint;
}

function toCount(value: number | bigint | null | undefined): number {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : 0;
  }
  if (typeof value === "bigint") {
    return Number(value);
  }
  return 0;
}

export interface PruneCoverageFloorResult {
  coreDeleted: number;
  rawDeletedByCore: number;
  rawOrphansDeleted: number;
}

export async function pruneSwapsBeforeCoverageFloor(
  connection: DuckDBConnection,
  floorIso: string,
): Promise<PruneCoverageFloorResult> {
  const coreDeleteCountRow = await queryFirstPrepared<CountRow>(
    connection,
    `SELECT COUNT(*) AS count
     FROM swaps_core
     WHERE COALESCE(event_at, created_at, updated_at) IS NOT NULL
       AND COALESCE(event_at, created_at, updated_at) < ?`,
    [floorIso],
  );
  const rawDeleteCountRow = await queryFirstPrepared<CountRow>(
    connection,
    `SELECT COUNT(*) AS count
     FROM swaps_raw
     WHERE normalized_id IN (
       SELECT normalized_id
       FROM swaps_core
       WHERE COALESCE(event_at, created_at, updated_at) IS NOT NULL
         AND COALESCE(event_at, created_at, updated_at) < ?
     )`,
    [floorIso],
  );
  const orphanRawCountRow = await queryFirstPrepared<CountRow>(
    connection,
    `SELECT COUNT(*) AS count
     FROM swaps_raw
     WHERE normalized_id NOT IN (
       SELECT normalized_id
       FROM swaps_core
     )`,
  );

  await connection.run("BEGIN TRANSACTION");
  try {
    await runPrepared(
      connection,
      `DELETE FROM swaps_raw
       WHERE normalized_id IN (
         SELECT normalized_id
         FROM swaps_core
         WHERE COALESCE(event_at, created_at, updated_at) IS NOT NULL
           AND COALESCE(event_at, created_at, updated_at) < ?
       )`,
      [floorIso],
    );
    await runPrepared(
      connection,
      `DELETE FROM swaps_core
       WHERE COALESCE(event_at, created_at, updated_at) IS NOT NULL
         AND COALESCE(event_at, created_at, updated_at) < ?`,
      [floorIso],
    );
    await runPrepared(
      connection,
      `DELETE FROM swaps_raw
       WHERE normalized_id NOT IN (
         SELECT normalized_id
         FROM swaps_core
       )`,
    );
    await connection.run("COMMIT");
  } catch (error) {
    await connection.run("ROLLBACK");
    throw error;
  }

  return {
    coreDeleted: toCount(coreDeleteCountRow?.count),
    rawDeletedByCore: toCount(rawDeleteCountRow?.count),
    rawOrphansDeleted: toCount(orphanRawCountRow?.count),
  };
}

export async function persistSwaps(
  connection: DuckDBConnection,
  coreRows: readonly NormalizedSwapRowInput[],
  rawRows: readonly RawSwapRowInput[],
): Promise<PersistSwapsResult> {
  const successfulCoreRows = coreRows.filter(
    (row) => row.status_canonical === "success" && isAtOrAfterCoverageFloor(row),
  );
  const successfulIds = new Set(successfulCoreRows.map((row) => row.normalized_id));
  const successfulRawRows = rawRows.filter((row) => successfulIds.has(row.normalized_id));

  if (successfulCoreRows.length === 0 && successfulRawRows.length === 0) {
    return { coreUpserts: 0, rawInsertAttempts: 0 };
  }

  const coreStatement = await connection.prepare(CORE_UPSERT_SQL);
  const rawStatement = await connection.prepare(RAW_INSERT_SQL);
  await connection.run("BEGIN TRANSACTION");
  try {
    for (const coreRow of successfulCoreRows) {
      coreStatement.bind(coreRowToParams(coreRow));
      await coreStatement.run();
    }
    for (const rawRow of successfulRawRows) {
      rawStatement.bind(rawRowToParams(rawRow));
      await rawStatement.run();
    }
    await connection.run("COMMIT");
  } catch (error) {
    await connection.run("ROLLBACK");
    coreStatement.destroySync();
    rawStatement.destroySync();
    throw error;
  }
  coreStatement.destroySync();
  rawStatement.destroySync();

  return {
    coreUpserts: successfulCoreRows.length,
    rawInsertAttempts: successfulRawRows.length,
  };
}
