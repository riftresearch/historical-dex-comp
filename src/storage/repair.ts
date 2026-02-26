import type { ProviderKey } from "../domain/provider-key";
import { PROVIDER_KEYS } from "../domain/provider-key";
import { getProviderAdapter } from "../providers";
import {
  getIngestCheckpoint,
  reconcileCheckpointFromSwaps,
  type IngestCheckpoint,
  upsertIngestCheckpoint,
} from "./repositories/ingest-checkpoints";
import { failRunningIngestRuns } from "./repositories/ingest-runs";
import { applyMigrations } from "./migrations/migration-runner";
import { getProviderDbPath, openProviderDatabase } from "./provider-db";

export interface ProviderRepairResult {
  providerKey: ProviderKey;
  dbPath: string;
  recoveredRuns: number;
  checkpointBefore: IngestCheckpoint | null;
  checkpointAfter: IngestCheckpoint | null;
  checkpointUpdated: boolean;
}

export interface RepairProvidersResult {
  providers: ProviderRepairResult[];
}

function toProviders(scope: ProviderKey | "all"): ProviderKey[] {
  return scope === "all" ? [...PROVIDER_KEYS] : [scope];
}

function checkpointMaterialKey(checkpoint: IngestCheckpoint | null): string {
  if (!checkpoint) {
    return "null";
  }
  return JSON.stringify({
    streamKey: checkpoint.streamKey,
    syncScope: checkpoint.syncScope,
    newestCursor: checkpoint.newestCursor,
    newestEventAt: checkpoint.newestEventAt?.toISOString() ?? null,
    newestProviderRecordId: checkpoint.newestProviderRecordId,
    oldestCursor: checkpoint.oldestCursor,
    oldestEventAt: checkpoint.oldestEventAt?.toISOString() ?? null,
    oldestProviderRecordId: checkpoint.oldestProviderRecordId,
    runId: checkpoint.runId,
  });
}

export async function repairProviders(scope: ProviderKey | "all"): Promise<RepairProvidersResult> {
  const providers = toProviders(scope);
  const results: ProviderRepairResult[] = [];

  for (const providerKey of providers) {
    const adapter = getProviderAdapter(providerKey);
    const dbPath = getProviderDbPath(providerKey);
    const db = await openProviderDatabase(providerKey);
    let recoveredRuns = 0;
    let checkpointBefore: IngestCheckpoint | null = null;
    let checkpointAfter: IngestCheckpoint | null = null;
    let checkpointUpdated = false;
    try {
      await applyMigrations(db.connection);
      recoveredRuns = await failRunningIngestRuns(db.connection);
      checkpointBefore = await getIngestCheckpoint(db.connection, adapter.streamKey);
      checkpointAfter = await reconcileCheckpointFromSwaps(db.connection, {
        streamKey: adapter.streamKey,
        checkpoint: checkpointBefore,
        runId: checkpointBefore?.runId ?? null,
      });

      checkpointUpdated =
        checkpointMaterialKey(checkpointBefore) !== checkpointMaterialKey(checkpointAfter);
      if (checkpointAfter && checkpointUpdated) {
        await upsertIngestCheckpoint(db.connection, checkpointAfter);
      }
    } finally {
      db.close();
    }

    results.push({
      providerKey,
      dbPath,
      recoveredRuns,
      checkpointBefore,
      checkpointAfter,
      checkpointUpdated,
    });
  }

  return {
    providers: results,
  };
}
