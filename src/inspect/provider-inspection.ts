import type { ProviderKey } from "../domain/provider-key";
import { PROVIDER_KEYS } from "../domain/provider-key";
import { queryPrepared } from "../storage/duckdb-utils";
import {
  listIngestCheckpoints,
  type IngestCheckpoint,
} from "../storage/repositories/ingest-checkpoints";
import { listIngestRuns, type IngestRun } from "../storage/repositories/ingest-runs";
import {
  getProviderDbPath,
  openProviderDatabase,
  providerDatabaseExists,
} from "../storage/provider-db";

interface TableNameRow {
  table_name: string;
}

interface CountsRow {
  swaps_core_count: number | bigint;
  swaps_raw_count: number | bigint;
  checkpoint_count: number | bigint;
}

const REQUIRED_TABLES = [
  "schema_migrations",
  "ingest_runs",
  "ingest_checkpoints",
  "swaps_core",
  "swaps_raw",
] as const;

export interface ProviderStatusReport {
  providerKey: ProviderKey;
  dbPath: string;
  databaseExists: boolean;
  initialized: boolean;
  missingTables: string[];
  swapsCoreCount: number;
  swapsRawCount: number;
  checkpointCount: number;
  checkpoints: IngestCheckpoint[];
  latestRun: IngestRun | null;
  error: string | null;
}

export interface ProviderRunsReport {
  providerKey: ProviderKey;
  dbPath: string;
  databaseExists: boolean;
  initialized: boolean;
  missingTables: string[];
  runs: IngestRun[];
  error: string | null;
}

function toProviders(scope: ProviderKey | "all"): ProviderKey[] {
  return scope === "all" ? [...PROVIDER_KEYS] : [scope];
}

function toNumber(value: number | bigint): number {
  return Number(value);
}

async function getExistingTableNames(providerKey: ProviderKey): Promise<Set<string>> {
  const db = await openProviderDatabase(providerKey);
  try {
    const rows = await queryPrepared<TableNameRow>(
      db.connection,
      `SELECT table_name
       FROM information_schema.tables
       WHERE table_schema = 'main'`,
    );
    return new Set(rows.map((row) => row.table_name));
  } finally {
    db.close();
  }
}

function missingTablesFromSet(tableNames: Set<string>): string[] {
  return REQUIRED_TABLES.filter((tableName) => !tableNames.has(tableName));
}

export async function getProviderStatusReports(
  scope: ProviderKey | "all",
): Promise<ProviderStatusReport[]> {
  const providers = toProviders(scope);
  const reports: ProviderStatusReport[] = [];

  for (const providerKey of providers) {
    const dbPath = getProviderDbPath(providerKey);
    const databaseExists = await providerDatabaseExists(providerKey);
    if (!databaseExists) {
      reports.push({
        providerKey,
        dbPath,
        databaseExists: false,
        initialized: false,
        missingTables: [...REQUIRED_TABLES],
        swapsCoreCount: 0,
        swapsRawCount: 0,
        checkpointCount: 0,
        checkpoints: [],
        latestRun: null,
        error: null,
      });
      continue;
    }

    const tableNames = await getExistingTableNames(providerKey);
    const missingTables = missingTablesFromSet(tableNames);
    const initialized = missingTables.length === 0;
    if (!initialized) {
      reports.push({
        providerKey,
        dbPath,
        databaseExists: true,
        initialized: false,
        missingTables,
        swapsCoreCount: 0,
        swapsRawCount: 0,
        checkpointCount: 0,
        checkpoints: [],
        latestRun: null,
        error: null,
      });
      continue;
    }

    const db = await openProviderDatabase(providerKey);
    try {
      const countsRows = await queryPrepared<CountsRow>(
        db.connection,
        `SELECT
          (SELECT COUNT(*) FROM swaps_core) AS swaps_core_count,
          (SELECT COUNT(*) FROM swaps_raw) AS swaps_raw_count,
          (SELECT COUNT(*) FROM ingest_checkpoints) AS checkpoint_count`,
      );
      const counts = countsRows[0];
      const checkpoints = await listIngestCheckpoints(db.connection);
      const runs = await listIngestRuns(db.connection, 1);

      reports.push({
        providerKey,
        dbPath,
        databaseExists: true,
        initialized: true,
        missingTables: [],
        swapsCoreCount: counts ? toNumber(counts.swaps_core_count) : 0,
        swapsRawCount: counts ? toNumber(counts.swaps_raw_count) : 0,
        checkpointCount: counts ? toNumber(counts.checkpoint_count) : 0,
        checkpoints,
        latestRun: runs[0] ?? null,
        error: null,
      });
    } catch (error) {
      reports.push({
        providerKey,
        dbPath,
        databaseExists: true,
        initialized: true,
        missingTables: [],
        swapsCoreCount: 0,
        swapsRawCount: 0,
        checkpointCount: 0,
        checkpoints: [],
        latestRun: null,
        error: error instanceof Error ? error.message : String(error),
      });
    } finally {
      db.close();
    }
  }

  return reports;
}

export async function getProviderRunsReports(
  scope: ProviderKey | "all",
  limit: number,
): Promise<ProviderRunsReport[]> {
  const providers = toProviders(scope);
  const reports: ProviderRunsReport[] = [];

  for (const providerKey of providers) {
    const dbPath = getProviderDbPath(providerKey);
    const databaseExists = await providerDatabaseExists(providerKey);
    if (!databaseExists) {
      reports.push({
        providerKey,
        dbPath,
        databaseExists: false,
        initialized: false,
        missingTables: [...REQUIRED_TABLES],
        runs: [],
        error: null,
      });
      continue;
    }

    const tableNames = await getExistingTableNames(providerKey);
    const missingTables = missingTablesFromSet(tableNames);
    const initialized = missingTables.length === 0;
    if (!initialized) {
      reports.push({
        providerKey,
        dbPath,
        databaseExists: true,
        initialized: false,
        missingTables,
        runs: [],
        error: null,
      });
      continue;
    }

    const db = await openProviderDatabase(providerKey);
    try {
      const runs = await listIngestRuns(db.connection, limit);
      reports.push({
        providerKey,
        dbPath,
        databaseExists: true,
        initialized: true,
        missingTables: [],
        runs,
        error: null,
      });
    } catch (error) {
      reports.push({
        providerKey,
        dbPath,
        databaseExists: true,
        initialized: true,
        missingTables: [],
        runs: [],
        error: error instanceof Error ? error.message : String(error),
      });
    } finally {
      db.close();
    }
  }

  return reports;
}
