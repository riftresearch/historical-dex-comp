import type { ProviderKey } from "../domain/provider-key";
import type { IngestMode } from "./types";
import { getProviderIngestTuning } from "./provider-tuning";
import { getProviderAdapter } from "../providers";
import {
  getIngestCheckpoint,
  reconcileCheckpointFromSwaps,
  upsertIngestCheckpoint,
} from "../storage/repositories/ingest-checkpoints";
import {
  failRunningIngestRuns,
  finishIngestRunFailure,
  finishIngestRunSuccess,
  startIngestRun,
  type IngestRunCounts,
} from "../storage/repositories/ingest-runs";
import { openProviderDatabase } from "../storage/provider-db";
import { applyMigrations } from "../storage/migrations/migration-runner";

export interface RunProviderIngestInput {
  providerKey: ProviderKey;
  mode: IngestMode;
}

export interface RunProviderIngestOutput {
  providerKey: ProviderKey;
  runId: string;
  mode: IngestMode;
  recordsFetched: number;
  recordsNormalized: number;
  recordsUpserted: number;
  newestEventAt: string | null;
  oldestEventAt: string | null;
}

export async function runProviderIngest(
  input: RunProviderIngestInput,
): Promise<RunProviderIngestOutput> {
  const adapter = getProviderAdapter(input.providerKey);
  const tuning = getProviderIngestTuning(input.providerKey);
  const db = await openProviderDatabase(input.providerKey);
  const counts: IngestRunCounts = {
    recordsFetched: 0,
    recordsNormalized: 0,
    recordsUpserted: 0,
  };

  await applyMigrations(db.connection);
  const recoveredRuns = await failRunningIngestRuns(db.connection);
  if (recoveredRuns > 0) {
    console.log(`[ingest][${input.providerKey}] recovered stale running runs=${recoveredRuns}`);
  }

  const checkpoint = await getIngestCheckpoint(db.connection, adapter.streamKey);
  const runId = await startIngestRun(db.connection, {
    providerKey: input.providerKey,
    fromTs: null,
    toTs: null,
  });

  try {
    const output = await adapter.ingest({
      connection: db.connection,
      runId,
      mode: input.mode,
      checkpoint,
      now: new Date(),
      pageSize: tuning.pageSize,
      maxPages: tuning.maxPages,
    });

    counts.recordsFetched = output.recordsFetched;
    counts.recordsNormalized = output.recordsNormalized;
    counts.recordsUpserted = output.recordsUpserted;

    const nextCheckpoint = output.checkpoint
      ? await reconcileCheckpointFromSwaps(db.connection, {
          streamKey: adapter.streamKey,
          checkpoint: output.checkpoint,
          runId,
        })
      : null;

    if (nextCheckpoint) {
      await upsertIngestCheckpoint(db.connection, nextCheckpoint);
    }

    await finishIngestRunSuccess(db.connection, runId, counts);

    return {
      providerKey: input.providerKey,
      runId,
      mode: input.mode,
      recordsFetched: output.recordsFetched,
      recordsNormalized: output.recordsNormalized,
      recordsUpserted: output.recordsUpserted,
      newestEventAt: nextCheckpoint?.newestEventAt?.toISOString() ?? null,
      oldestEventAt: nextCheckpoint?.oldestEventAt?.toISOString() ?? null,
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    await finishIngestRunFailure(db.connection, runId, counts, message);
    throw error;
  } finally {
    db.close();
  }
}
