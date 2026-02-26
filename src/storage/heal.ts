import { COVERAGE_FLOOR, COVERAGE_FLOOR_ISO } from "../domain/coverage-floor";
import type { ProviderKey } from "../domain/provider-key";
import { PROVIDER_KEYS } from "../domain/provider-key";
import { runProviderIngest } from "../ingest/run-provider-ingest";
import { getProviderAdapter } from "../providers";
import { parseDateOrNull } from "../utils/time";
import { applyMigrations } from "./migrations/migration-runner";
import { getProviderDbPath, openProviderDatabase } from "./provider-db";
import {
  getIngestCheckpoint,
  reconcileCheckpointFromSwaps,
  type IngestCheckpoint,
  upsertIngestCheckpoint,
} from "./repositories/ingest-checkpoints";
import { failRunningIngestRuns } from "./repositories/ingest-runs";
import { pruneSwapsBeforeCoverageFloor } from "./repositories/swaps";

type HealStopReason = "target_reached" | "stalled" | "max_cycles";

export interface ProviderHealResult {
  providerKey: ProviderKey;
  dbPath: string;
  targetOldestIso: string;
  recoveredRuns: number;
  prunedCoreRows: number;
  prunedRawRows: number;
  prunedRawOrphans: number;
  checkpointBefore: IngestCheckpoint | null;
  checkpointAfterPrune: IngestCheckpoint | null;
  checkpointUpdated: boolean;
  syncNewerRunId: string;
  syncNewerRecordsFetched: number;
  syncNewerRecordsNormalized: number;
  syncNewerRecordsUpserted: number;
  backfillCycles: number;
  backfillRecordsFetched: number;
  backfillRecordsNormalized: number;
  backfillRecordsUpserted: number;
  oldestEventAt: string | null;
  newestEventAt: string | null;
  reachedTargetOldest: boolean;
  stopReason: HealStopReason;
}

export interface HealProvidersResult {
  providers: ProviderHealResult[];
}

const HEAL_MAX_BACKFILL_CYCLES = 200;
const HEAL_STALL_LIMIT = 3;

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

function hasReachedTargetOldest(oldestEventAt: Date | null): boolean {
  if (!oldestEventAt) {
    return false;
  }
  return oldestEventAt.getTime() <= COVERAGE_FLOOR.getTime();
}

export async function healProviders(scope: ProviderKey | "all"): Promise<HealProvidersResult> {
  const providers = toProviders(scope);
  const results: ProviderHealResult[] = [];

  for (const providerKey of providers) {
    const adapter = getProviderAdapter(providerKey);
    const dbPath = getProviderDbPath(providerKey);
    const db = await openProviderDatabase(providerKey);

    let recoveredRuns = 0;
    let prunedCoreRows = 0;
    let prunedRawRows = 0;
    let prunedRawOrphans = 0;
    let checkpointBefore: IngestCheckpoint | null = null;
    let checkpointAfterPrune: IngestCheckpoint | null = null;
    let checkpointUpdated = false;

    try {
      await applyMigrations(db.connection);
      recoveredRuns = await failRunningIngestRuns(db.connection);

      checkpointBefore = await getIngestCheckpoint(db.connection, adapter.streamKey);
      const pruneResult = await pruneSwapsBeforeCoverageFloor(db.connection, COVERAGE_FLOOR_ISO);
      prunedCoreRows = pruneResult.coreDeleted;
      prunedRawRows = pruneResult.rawDeletedByCore;
      prunedRawOrphans = pruneResult.rawOrphansDeleted;

      checkpointAfterPrune = await reconcileCheckpointFromSwaps(db.connection, {
        streamKey: adapter.streamKey,
        checkpoint: checkpointBefore,
        runId: checkpointBefore?.runId ?? null,
      });

      checkpointUpdated =
        checkpointMaterialKey(checkpointBefore) !== checkpointMaterialKey(checkpointAfterPrune);
      if (checkpointAfterPrune && checkpointUpdated) {
        await upsertIngestCheckpoint(db.connection, checkpointAfterPrune);
      }
    } finally {
      db.close();
    }

    const syncNewer = await runProviderIngest({
      providerKey,
      mode: "sync_newer",
    });

    let oldestEventAt = parseDateOrNull(syncNewer.oldestEventAt);
    let newestEventAt = parseDateOrNull(syncNewer.newestEventAt);

    let backfillCycles = 0;
    let backfillRecordsFetched = 0;
    let backfillRecordsNormalized = 0;
    let backfillRecordsUpserted = 0;
    let stopReason: HealStopReason = "target_reached";
    let stallCycles = 0;
    let previousOldestMs = oldestEventAt?.getTime() ?? null;

    if (!hasReachedTargetOldest(oldestEventAt)) {
      stopReason = "max_cycles";
      while (backfillCycles < HEAL_MAX_BACKFILL_CYCLES) {
        const backfill = await runProviderIngest({
          providerKey,
          mode: "backfill_older",
        });
        backfillCycles += 1;
        backfillRecordsFetched += backfill.recordsFetched;
        backfillRecordsNormalized += backfill.recordsNormalized;
        backfillRecordsUpserted += backfill.recordsUpserted;

        const cycleOldest = parseDateOrNull(backfill.oldestEventAt);
        const cycleNewest = parseDateOrNull(backfill.newestEventAt);

        if (cycleOldest && (!oldestEventAt || cycleOldest < oldestEventAt)) {
          oldestEventAt = cycleOldest;
        }
        if (cycleNewest && (!newestEventAt || cycleNewest > newestEventAt)) {
          newestEventAt = cycleNewest;
        }

        const progressedOlder =
          cycleOldest !== null && (previousOldestMs === null || cycleOldest.getTime() < previousOldestMs);
        if (progressedOlder) {
          previousOldestMs = cycleOldest.getTime();
          stallCycles = 0;
        } else if (backfill.recordsUpserted <= 0) {
          stallCycles += 1;
        } else {
          stallCycles = 0;
        }

        if (hasReachedTargetOldest(oldestEventAt)) {
          stopReason = "target_reached";
          break;
        }

        if (stallCycles >= HEAL_STALL_LIMIT) {
          stopReason = "stalled";
          break;
        }
      }
    }

    results.push({
      providerKey,
      dbPath,
      targetOldestIso: COVERAGE_FLOOR_ISO,
      recoveredRuns,
      prunedCoreRows,
      prunedRawRows,
      prunedRawOrphans,
      checkpointBefore,
      checkpointAfterPrune,
      checkpointUpdated,
      syncNewerRunId: syncNewer.runId,
      syncNewerRecordsFetched: syncNewer.recordsFetched,
      syncNewerRecordsNormalized: syncNewer.recordsNormalized,
      syncNewerRecordsUpserted: syncNewer.recordsUpserted,
      backfillCycles,
      backfillRecordsFetched,
      backfillRecordsNormalized,
      backfillRecordsUpserted,
      oldestEventAt: oldestEventAt?.toISOString() ?? null,
      newestEventAt: newestEventAt?.toISOString() ?? null,
      reachedTargetOldest: hasReachedTargetOldest(oldestEventAt),
      stopReason,
    });
  }

  return {
    providers: results,
  };
}

