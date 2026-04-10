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
import type { ProviderProgressValue } from "../providers/types";

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

const DEFAULT_PROGRESS_HEARTBEAT_SECONDS = 30;

function parseProgressHeartbeatMs(): number {
  const rawValue = Bun.env.INGEST_PROGRESS_HEARTBEAT_SECONDS ?? process.env.INGEST_PROGRESS_HEARTBEAT_SECONDS;
  if (!rawValue) {
    return DEFAULT_PROGRESS_HEARTBEAT_SECONDS * 1_000;
  }

  const parsed = Number(rawValue);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return DEFAULT_PROGRESS_HEARTBEAT_SECONDS * 1_000;
  }
  return Math.max(5, parsed) * 1_000;
}

function formatProgressValue(value: ProviderProgressValue): string | null {
  if (value === null || value === undefined) {
    return null;
  }
  const rawValue = String(value);
  return rawValue.includes(" ") ? JSON.stringify(rawValue) : rawValue;
}

function formatProgressFields(fields: Record<string, ProviderProgressValue> | undefined): string {
  if (!fields) {
    return "";
  }

  return Object.entries(fields)
    .map(([key, value]) => {
      const formattedValue = formatProgressValue(value);
      return formattedValue === null ? null : `${key}=${formattedValue}`;
    })
    .filter((value): value is string => Boolean(value))
    .join(" | ");
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
  const startedAtMs = Date.now();
  const heartbeatMs = parseProgressHeartbeatMs();
  let lastProgressMessage = "started";
  let lastProgressAtMs = startedAtMs;
  const logProgress = (
    message: string,
    fields: Record<string, ProviderProgressValue> | undefined = undefined,
  ): void => {
    lastProgressMessage = message;
    lastProgressAtMs = Date.now();
    const fieldText = formatProgressFields({
      runId,
      mode: input.mode,
      elapsedSeconds: Math.round((lastProgressAtMs - startedAtMs) / 1000),
      ...fields,
    });
    console.log(`[ingest][${input.providerKey}] ${message}${fieldText ? ` | ${fieldText}` : ""}`);
  };
  const heartbeatId = setInterval(() => {
    const nowMs = Date.now();
    const fieldText = formatProgressFields({
      runId,
      mode: input.mode,
      elapsedSeconds: Math.round((nowMs - startedAtMs) / 1000),
      lastProgress: lastProgressMessage,
      lastProgressAgeSeconds: Math.round((nowMs - lastProgressAtMs) / 1000),
    });
    console.log(`[ingest][${input.providerKey}] heartbeat | ${fieldText}`);
  }, heartbeatMs);

  try {
    logProgress("start", {
      streamKey: adapter.streamKey,
      pageSize: tuning.pageSize,
      maxPages: tuning.maxPages ?? "unlimited",
    });
    const output = await adapter.ingest({
      connection: db.connection,
      runId,
      mode: input.mode,
      checkpoint,
      now: new Date(),
      pageSize: tuning.pageSize,
      maxPages: tuning.maxPages,
      progress: (event) => logProgress(event.message, event.fields),
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
    logProgress("success", {
      recordsFetched: output.recordsFetched,
      recordsNormalized: output.recordsNormalized,
      recordsUpserted: output.recordsUpserted,
      newestEventAt: nextCheckpoint?.newestEventAt?.toISOString() ?? null,
      oldestEventAt: nextCheckpoint?.oldestEventAt?.toISOString() ?? null,
    });

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
    logProgress("failure", { error: message });
    throw error;
  } finally {
    clearInterval(heartbeatId);
    db.close();
  }
}
