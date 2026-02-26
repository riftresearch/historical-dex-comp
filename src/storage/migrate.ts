import type { ProviderKey } from "../domain/provider-key";
import { PROVIDER_KEYS } from "../domain/provider-key";
import { applyMigrations } from "./migrations/migration-runner";
import { openProviderDatabase } from "./provider-db";

export interface MigrateProvidersResult {
  migratedProviders: ProviderKey[];
}

export async function migrateProviderDatabase(providerKey: ProviderKey): Promise<void> {
  const db = await openProviderDatabase(providerKey);
  try {
    await applyMigrations(db.connection);
  } finally {
    db.close();
  }
}

export async function migrateProviders(
  providerKey: ProviderKey | "all",
): Promise<MigrateProvidersResult> {
  const providers = providerKey === "all" ? [...PROVIDER_KEYS] : [providerKey];

  for (const provider of providers) {
    await migrateProviderDatabase(provider);
  }

  return {
    migratedProviders: providers,
  };
}
