import { isProviderKey, PROVIDER_KEYS } from "../domain/provider-key";
import { startDashboardServer } from "../frontend/serve-dashboard";
import { migrateProviders } from "../storage/migrate";
import { runProviderIngest } from "../ingest/run-provider-ingest";
import { runMultiProviderIngestLoop } from "../ingest/run-multi-provider-loop";
import { INGEST_MODES, isIngestMode, type IngestMode } from "../ingest/types";
import { getProviderRunsReports, getProviderStatusReports } from "../inspect/provider-inspection";
import { maintenanceService } from "../services/maintenance";
import {
  parseDashboardOptions,
  parseIngestLoopOptions,
  parseIntegrityOptions,
  parseProviderScope,
  parseRunsOptions,
} from "./parsers";

export async function handleMigrate(args: string[]): Promise<void> {
  const scope = parseProviderScope(args[0]);

  const result = await migrateProviders(scope);
  console.log(`Migrated: ${result.migratedProviders.join(", ")}`);
}

export async function handleIngest(args: string[]): Promise<void> {
  const providerArg = args[0];
  if (!providerArg || !isProviderKey(providerArg)) {
    throw new Error(`Missing or invalid provider. Use one of: ${PROVIDER_KEYS.join(", ")}`);
  }

  let mode: IngestMode = "bootstrap";
  let optionStartIndex = 1;
  const maybeMode = args[1];
  if (maybeMode && !maybeMode.startsWith("--")) {
    if (!isIngestMode(maybeMode)) {
      throw new Error(`Invalid mode '${maybeMode}'. Use one of: ${INGEST_MODES.join(", ")}`);
    }
    mode = maybeMode;
    optionStartIndex = 2;
  }

  const remainingArgs = args.slice(optionStartIndex);
  if (remainingArgs.length > 0) {
    throw new Error(`Unknown argument '${remainingArgs[0]}'.`);
  }

  const result = await runProviderIngest({
    providerKey: providerArg,
    mode,
  });

  console.log(
    JSON.stringify(
      {
        provider: result.providerKey,
        mode: result.mode,
        runId: result.runId,
        recordsFetched: result.recordsFetched,
        recordsNormalized: result.recordsNormalized,
        recordsUpserted: result.recordsUpserted,
        newestEventAt: result.newestEventAt,
        oldestEventAt: result.oldestEventAt,
      },
      null,
      2,
    ),
  );
}

export async function handleIngestLoop(args: string[]): Promise<void> {
  let mode: IngestMode = "backfill_older";
  let optionStartIndex = 0;
  const maybeMode = args[0];
  if (maybeMode && !maybeMode.startsWith("--")) {
    if (!isIngestMode(maybeMode)) {
      throw new Error(`Invalid mode '${maybeMode}'. Use one of: ${INGEST_MODES.join(", ")}`);
    }
    mode = maybeMode;
    optionStartIndex = 1;
  }

  const options = parseIngestLoopOptions(args.slice(optionStartIndex));
  const result = await runMultiProviderIngestLoop({
    providers: options.providers,
    mode,
    stopAtOldest: options.stopAtOldest,
  });

  console.log(
    JSON.stringify(
      {
        type: "ingest_loop_stopped",
        providers: result.providers,
        mode: result.mode,
        stopAtOldest: result.stopAtOldest,
        stoppedAt: result.stoppedAt,
      },
      null,
      2,
    ),
  );
}

export async function handleStatus(args: string[]): Promise<void> {
  const scope = parseProviderScope(args[0]);
  if (args[1]) {
    throw new Error(`Unknown argument '${args[1]}'.`);
  }

  const reports = await getProviderStatusReports(scope);
  console.log(
    JSON.stringify(
      {
        scope,
        providers: reports,
      },
      null,
      2,
    ),
  );
}

export async function handleRuns(args: string[]): Promise<void> {
  const scope = parseProviderScope(args[0]);
  const options = parseRunsOptions(args.slice(1));

  const reports = await getProviderRunsReports(scope, options.limit);
  console.log(
    JSON.stringify(
      {
        scope,
        limit: options.limit,
        providers: reports,
      },
      null,
      2,
    ),
  );
}

export async function handleRepair(args: string[]): Promise<void> {
  const scope = parseProviderScope(args[0]);
  if (args[1]) {
    throw new Error(`Unknown argument '${args[1]}'.`);
  }

  const result = await maintenanceService.repair(scope);
  console.log(
    JSON.stringify(
      {
        scope,
        providers: result.providers,
      },
      null,
      2,
    ),
  );
}

export async function handleHeal(args: string[]): Promise<void> {
  const scope = parseProviderScope(args[0]);
  if (args[1]) {
    throw new Error(`Unknown argument '${args[1]}'.`);
  }

  const result = await maintenanceService.heal(scope);
  console.log(
    JSON.stringify(
      {
        scope,
        providers: result.providers,
      },
      null,
      2,
    ),
  );
}

export async function handleIntegrity(args: string[]): Promise<void> {
  const parsed = parseIntegrityOptions(args);
  const report = await maintenanceService.integrity({
    scope: parsed.scope,
    options: parsed.options,
  });

  console.log(JSON.stringify(report, null, 2));
}

export async function handleDashboard(args: string[]): Promise<void> {
  const options = parseDashboardOptions(args);
  const server = await startDashboardServer(options);
  console.log(
    JSON.stringify(
      {
        type: "dashboard_started",
        host: options.host,
        port: options.port,
        days: options.days,
      },
      null,
      2,
    ),
  );
  await server.finished;
}
