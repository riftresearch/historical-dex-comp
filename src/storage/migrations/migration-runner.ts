import { join } from "node:path";
import type { DuckDBConnection } from "@duckdb/node-api";
import { sha256Hex } from "../../utils/hash";
import { queryFirstPrepared, runPrepared } from "../duckdb-utils";

interface MigrationDefinition {
  version: number;
  name: string;
  fileName: string;
}

interface AppliedMigrationRow {
  version: number;
  checksum: string;
}

const MIGRATIONS: MigrationDefinition[] = [
  {
    version: 1,
    name: "schema_v1",
    fileName: "001_schema_v1.sql",
  },
];

const BOOTSTRAP_MIGRATIONS_TABLE_SQL = `
CREATE TABLE IF NOT EXISTS schema_migrations (
  version INTEGER PRIMARY KEY,
  name VARCHAR NOT NULL,
  checksum VARCHAR NOT NULL,
  applied_at TIMESTAMPTZ NOT NULL
)
`;

function getMigrationFilePath(fileName: string): string {
  return join(import.meta.dir, fileName);
}

async function ensureMigrationsTable(connection: DuckDBConnection): Promise<void> {
  await connection.run(BOOTSTRAP_MIGRATIONS_TABLE_SQL);
}

async function getAppliedMigration(
  connection: DuckDBConnection,
  version: number,
): Promise<AppliedMigrationRow | null> {
  return queryFirstPrepared<AppliedMigrationRow>(
    connection,
    `SELECT version, checksum
     FROM schema_migrations
     WHERE version = ?`,
    [version],
  );
}

export async function applyMigrations(connection: DuckDBConnection): Promise<void> {
  await ensureMigrationsTable(connection);

  for (const migration of MIGRATIONS) {
    const migrationSql = await Bun.file(getMigrationFilePath(migration.fileName)).text();
    const checksum = sha256Hex(migrationSql);
    const applied = await getAppliedMigration(connection, migration.version);

    if (applied) {
      if (applied.checksum !== checksum) {
        throw new Error(
          `Migration checksum mismatch for version ${migration.version} (${migration.name}).`,
        );
      }
      continue;
    }

    await connection.run("BEGIN TRANSACTION");
    try {
      await connection.run(migrationSql);
      await runPrepared(
        connection,
        `INSERT INTO schema_migrations (version, name, checksum, applied_at)
         VALUES (?, ?, ?, ?)`,
        [migration.version, migration.name, checksum, new Date().toISOString()],
      );
      await connection.run("COMMIT");
    } catch (error) {
      await connection.run("ROLLBACK");
      throw error;
    }
  }
}
