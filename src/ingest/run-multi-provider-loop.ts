import type { ProviderKey } from "../domain/provider-key";
import type { IngestMode } from "./types";
import { getProviderIngestTuning } from "./provider-tuning";
import { getProviderAdapter } from "../providers";
import { runProviderIngest } from "./run-provider-ingest";
import { parseDateOrNull } from "../utils/time";
import { openProviderDatabase } from "../storage/provider-db";
import { getIngestCheckpoint } from "../storage/repositories/ingest-checkpoints";
import { failRunningIngestRuns } from "../storage/repositories/ingest-runs";
import { applyMigrations } from "../storage/migrations/migration-runner";

export interface RunMultiProviderIngestLoopInput {
  providers: ProviderKey[];
  mode: IngestMode;
  stopAtOldest: Date | null;
}

export interface RunMultiProviderIngestLoopOutput {
  providers: ProviderKey[];
  mode: IngestMode;
  stopAtOldest: string | null;
  stoppedAt: string;
}

interface ProviderWorkerState {
  oldestEventAt: Date | null;
}

interface LoopController {
  isStopRequested: () => boolean;
}

function emitLog(payload: Record<string, unknown>): void {
  console.log(JSON.stringify(payload));
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function shouldStopByOldestCutoff(
  mode: IngestMode,
  stopAtOldest: Date | null,
  oldestEventAt: Date | null,
): boolean {
  if (mode !== "backfill_older") {
    return false;
  }
  if (!stopAtOldest || !oldestEventAt) {
    return false;
  }
  return oldestEventAt.getTime() <= stopAtOldest.getTime();
}

async function loadProviderWorkerState(providerKey: ProviderKey): Promise<ProviderWorkerState> {
  const adapter = getProviderAdapter(providerKey);
  const db = await openProviderDatabase(providerKey);
  try {
    await applyMigrations(db.connection);
    const recoveredRuns = await failRunningIngestRuns(db.connection);
    if (recoveredRuns > 0) {
      console.log(`[ingest-loop][${providerKey}] recovered stale running runs=${recoveredRuns}`);
    }

    const checkpoint = await getIngestCheckpoint(db.connection, adapter.streamKey);
    return {
      oldestEventAt: checkpoint?.oldestEventAt ?? null,
    };
  } finally {
    db.close();
  }
}

async function runProviderWorker(
  input: RunMultiProviderIngestLoopInput,
  providerKey: ProviderKey,
  controller: LoopController,
): Promise<void> {
  const state = await loadProviderWorkerState(providerKey);
  const tuning = getProviderIngestTuning(providerKey);
  const sleepMs = tuning.sleepSeconds * 1_000;
  let cycleNumber = 0;

  emitLog({
    type: "ingest_worker_started",
    provider: providerKey,
    mode: input.mode,
    pageSize: tuning.pageSize,
    maxPages: tuning.maxPages,
    sleepSeconds: tuning.sleepSeconds,
    stopAtOldest: input.stopAtOldest?.toISOString() ?? null,
    checkpointOldestEventAt: state.oldestEventAt?.toISOString() ?? null,
  });

  if (shouldStopByOldestCutoff(input.mode, input.stopAtOldest, state.oldestEventAt)) {
    emitLog({
      type: "ingest_worker_cutoff_reached",
      provider: providerKey,
      phase: "before_start",
      oldestEventAt: state.oldestEventAt?.toISOString() ?? null,
      cutoff: input.stopAtOldest?.toISOString() ?? null,
    });
    return;
  }

  while (!controller.isStopRequested()) {
    cycleNumber += 1;
    const cycleStartedAt = new Date();
    emitLog({
      type: "ingest_cycle_start",
      provider: providerKey,
      mode: input.mode,
      cycleNumber,
      pageSize: tuning.pageSize,
      maxPages: tuning.maxPages,
      checkpointOldestEventAt: state.oldestEventAt?.toISOString() ?? null,
      startedAt: cycleStartedAt.toISOString(),
    });

    let heartbeatTimer: ReturnType<typeof setInterval> | null = null;
    try {
      heartbeatTimer = setInterval(() => {
        const elapsedMs = Date.now() - cycleStartedAt.getTime();
        emitLog({
          type: "ingest_cycle_heartbeat",
          provider: providerKey,
          mode: input.mode,
          cycleNumber,
          elapsedSeconds: Math.floor(elapsedMs / 1_000),
        });
      }, 30_000);

      const result = await runProviderIngest({
        providerKey,
        mode: input.mode,
      });
      const oldestFromRun = parseDateOrNull(result.oldestEventAt);
      if (oldestFromRun && (!state.oldestEventAt || oldestFromRun < state.oldestEventAt)) {
        state.oldestEventAt = oldestFromRun;
      }

      emitLog({
        type: "ingest_cycle",
        provider: providerKey,
        mode: input.mode,
        cycleNumber,
        runId: result.runId,
        durationMs: Date.now() - cycleStartedAt.getTime(),
        recordsFetched: result.recordsFetched,
        recordsNormalized: result.recordsNormalized,
        recordsUpserted: result.recordsUpserted,
        newestEventAt: result.newestEventAt,
        oldestEventAt: result.oldestEventAt,
      });

      if (shouldStopByOldestCutoff(input.mode, input.stopAtOldest, state.oldestEventAt)) {
        emitLog({
          type: "ingest_worker_cutoff_reached",
          provider: providerKey,
          phase: "after_cycle",
          oldestEventAt: state.oldestEventAt?.toISOString() ?? null,
          cutoff: input.stopAtOldest?.toISOString() ?? null,
        });
        return;
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      emitLog({
        type: "ingest_cycle_error",
        provider: providerKey,
        mode: input.mode,
        cycleNumber,
        durationMs: Date.now() - cycleStartedAt.getTime(),
        message,
      });
    } finally {
      if (heartbeatTimer) {
        clearInterval(heartbeatTimer);
      }
    }

    if (controller.isStopRequested()) {
      break;
    }

    emitLog({
      type: "ingest_worker_sleep",
      provider: providerKey,
      mode: input.mode,
      cycleNumber,
      sleepSeconds: tuning.sleepSeconds,
    });
    await delay(sleepMs);
  }

  emitLog({
    type: "ingest_worker_stopping",
    provider: providerKey,
    mode: input.mode,
  });
}

export async function runMultiProviderIngestLoop(
  input: RunMultiProviderIngestLoopInput,
): Promise<RunMultiProviderIngestLoopOutput> {
  if (input.providers.length === 0) {
    throw new Error("ingest-loop requires at least one provider.");
  }

  emitLog({
    type: "ingest_loop_started",
    providers: input.providers,
    mode: input.mode,
    stopAtOldest: input.stopAtOldest?.toISOString() ?? null,
  });

  let stopRequested = false;
  const requestStop = (signal: string): void => {
    if (stopRequested) {
      return;
    }
    stopRequested = true;
    console.log(`[ingest-loop] received ${signal}, stopping after in-flight cycles complete`);
  };

  const onSigInt = (): void => requestStop("SIGINT");
  const onSigTerm = (): void => requestStop("SIGTERM");
  process.on("SIGINT", onSigInt);
  process.on("SIGTERM", onSigTerm);

  try {
    const controller: LoopController = {
      isStopRequested: () => stopRequested,
    };
    const workers = input.providers.map((providerKey) =>
      runProviderWorker(input, providerKey, controller),
    );
    await Promise.all(workers);

    return {
      providers: [...input.providers],
      mode: input.mode,
      stopAtOldest: input.stopAtOldest?.toISOString() ?? null,
      stoppedAt: new Date().toISOString(),
    };
  } finally {
    process.off("SIGINT", onSigInt);
    process.off("SIGTERM", onSigTerm);
  }
}
