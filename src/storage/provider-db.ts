import { access, mkdir } from "node:fs/promises";
import { join } from "node:path";
import { DuckDBInstance, type DuckDBConnection } from "@duckdb/node-api";
import type { ProviderKey } from "../domain/provider-key";
import { getProviderDataDir } from "./data-paths";

export interface ProviderDatabaseHandle {
  filePath: string;
  connection: DuckDBConnection;
  close: () => void;
}

export function getProviderDbPath(providerKey: ProviderKey): string {
  return join(getProviderDataDir(), `${providerKey}.duckdb`);
}

export async function providerDatabaseExists(providerKey: ProviderKey): Promise<boolean> {
  const filePath = getProviderDbPath(providerKey);
  try {
    await access(filePath);
    return true;
  } catch {
    return false;
  }
}

export async function openProviderDatabase(
  providerKey: ProviderKey,
): Promise<ProviderDatabaseHandle> {
  await mkdir(getProviderDataDir(), { recursive: true });

  const filePath = getProviderDbPath(providerKey);
  const instance = await DuckDBInstance.create(filePath);
  const connection = await instance.connect();

  return {
    filePath,
    connection,
    close: () => {
      connection.closeSync();
      instance.closeSync();
    },
  };
}
