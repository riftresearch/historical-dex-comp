import type { DuckDBConnection } from "@duckdb/node-api";

export type SqlParam = string | number | boolean | null;

function normalizeSqlParam(value: unknown): SqlParam {
  if (value === undefined || value === null) {
    return null;
  }

  if (value instanceof Date) {
    return value.toISOString();
  }

  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return value;
  }

  return JSON.stringify(value);
}

export function normalizeSqlParams(values: readonly unknown[]): SqlParam[] {
  return values.map((value) => normalizeSqlParam(value));
}

export async function runPrepared(
  connection: DuckDBConnection,
  sql: string,
  params: readonly unknown[] = [],
): Promise<void> {
  const statement = await connection.prepare(sql);
  try {
    statement.bind(normalizeSqlParams(params));
    await statement.run();
  } finally {
    statement.destroySync();
  }
}

export async function queryPrepared<T extends object>(
  connection: DuckDBConnection,
  sql: string,
  params: readonly unknown[] = [],
): Promise<T[]> {
  const statement = await connection.prepare(sql);
  try {
    statement.bind(normalizeSqlParams(params));
    const reader = await statement.runAndReadAll();
    return reader.getRowObjects() as T[];
  } finally {
    statement.destroySync();
  }
}

export async function queryFirstPrepared<T extends object>(
  connection: DuckDBConnection,
  sql: string,
  params: readonly unknown[] = [],
): Promise<T | null> {
  const rows = await queryPrepared<T>(connection, sql, params);
  return rows[0] ?? null;
}
