import { cp, mkdir, readdir } from "node:fs/promises";
import { join, resolve } from "node:path";
import type { ProviderKey } from "../domain/provider-key";
import { parsePositiveInt, parseProvidersCsv } from "../cli/parsers";
import {
  getAnalyticsDataDir,
  getAnalyticsSeedDataDir,
  getProviderDataDir,
  getProviderSeedDataDir,
} from "../storage/data-paths";
import { writePrecomputedShortfallSnapshots } from "./serve-dashboard";

type StablecoinPegMode = "off" | "usd_1";

interface DashboardAutoRefreshOptions {
  refreshMinutes: number;
  windowDays: number;
  alignMinutes: number;
  providers: ProviderKey[];
  stablecoinPegMode: StablecoinPegMode;
}

interface RefreshWindow {
  startAtIso: string;
  endAtIso: string;
}

function readEnv(name: string): string | null {
  const value = Bun.env[name] ?? process.env[name];
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function floorDateToMinuteInterval(date: Date, intervalMinutes: number): Date {
  const intervalMs = intervalMinutes * 60 * 1_000;
  const flooredMs = Math.floor(date.getTime() / intervalMs) * intervalMs;
  return new Date(flooredMs);
}

function buildRefreshWindow(windowDays: number, alignMinutes: number): RefreshWindow {
  const alignedEndAt = floorDateToMinuteInterval(new Date(), alignMinutes);
  const startAt = new Date(alignedEndAt.getTime() - windowDays * 24 * 60 * 60 * 1_000);
  return {
    startAtIso: startAt.toISOString(),
    endAtIso: alignedEndAt.toISOString(),
  };
}

function parseStablecoinPegMode(rawValue: string | null): StablecoinPegMode {
  if (!rawValue) {
    return "usd_1";
  }
  const normalized = rawValue.trim().toLowerCase();
  if (normalized === "off" || normalized === "usd_1") {
    return normalized;
  }
  throw new Error(
    `Invalid DASHBOARD_AUTO_REFRESH_STABLECOIN_PEG_MODE='${rawValue}'. Expected off|usd_1.`,
  );
}

function resolveDashboardAutoRefreshOptions(): DashboardAutoRefreshOptions | null {
  if (readEnv("DASHBOARD_AUTO_REFRESH_DISABLED") === "1") {
    return null;
  }

  const rawRefreshMinutes = readEnv("DASHBOARD_AUTO_REFRESH_MINUTES");
  if (!rawRefreshMinutes) {
    return null;
  }

  const refreshMinutes = parsePositiveInt("DASHBOARD_AUTO_REFRESH_MINUTES", rawRefreshMinutes);
  const rawWindowDays = readEnv("DASHBOARD_AUTO_REFRESH_WINDOW_DAYS");
  const windowDays = rawWindowDays
    ? parsePositiveInt("DASHBOARD_AUTO_REFRESH_WINDOW_DAYS", rawWindowDays)
    : 30;
  const rawAlignMinutes = readEnv("DASHBOARD_AUTO_REFRESH_ALIGN_MINUTES");
  const alignMinutes = rawAlignMinutes
    ? parsePositiveInt("DASHBOARD_AUTO_REFRESH_ALIGN_MINUTES", rawAlignMinutes)
    : refreshMinutes;
  const rawProviders = readEnv("DASHBOARD_AUTO_REFRESH_PROVIDERS") ?? "implemented";
  const providers = parseProvidersCsv(rawProviders);
  const stablecoinPegMode = parseStablecoinPegMode(
    readEnv("DASHBOARD_AUTO_REFRESH_STABLECOIN_PEG_MODE"),
  );

  return {
    refreshMinutes,
    windowDays,
    alignMinutes,
    providers,
    stablecoinPegMode,
  };
}

async function runCommand(label: string, cmd: string[]): Promise<void> {
  console.log(`[dashboard-refresh] start ${label}`);
  const proc = Bun.spawn({
    cmd,
    cwd: process.cwd(),
    stdout: "inherit",
    stderr: "inherit",
  });
  const exitCode = await proc.exited;
  if (exitCode !== 0) {
    throw new Error(`${label} failed with exit code ${exitCode}`);
  }
  console.log(`[dashboard-refresh] done ${label}`);
}

async function runSyncNewerForProviders(providers: ProviderKey[]): Promise<void> {
  const bunBinary = Bun.which("bun") ?? "bun";
  for (const provider of providers) {
    await runCommand(`sync_newer:${provider}`, [
      bunBinary,
      "run",
      "index.ts",
      "ingest",
      provider,
      "sync_newer",
    ]);
  }
}

async function runRollingPrecompute(
  options: DashboardAutoRefreshOptions,
  window: RefreshWindow,
): Promise<void> {
  const bunBinary = Bun.which("bun") ?? "bun";
  await runCommand("regen-frontend-precomputed", [
    bunBinary,
    "run",
    "src/analytics/regen-frontend-precomputed.ts",
    "--providers",
    options.providers.join(","),
    "--start-at",
    window.startAtIso,
    "--end-at",
    window.endAtIso,
    "--stablecoin-peg-mode",
    options.stablecoinPegMode,
  ]);
}

async function listDirectoryEntries(dirPath: string): Promise<string[]> {
  try {
    return await readdir(dirPath);
  } catch {
    return [];
  }
}

async function seedMissingTopLevelEntries(
  label: string,
  targetDir: string,
  seedDir: string,
): Promise<void> {
  const normalizedTargetDir = resolve(targetDir);
  const normalizedSeedDir = resolve(seedDir);

  await mkdir(normalizedTargetDir, { recursive: true });

  if (normalizedTargetDir === normalizedSeedDir) {
    console.log(`[dashboard-refresh] seed_skip ${label} dir=${normalizedTargetDir} reason=same_path`);
    return;
  }

  const targetEntries = await listDirectoryEntries(normalizedTargetDir);
  const seedEntries = await listDirectoryEntries(normalizedSeedDir);
  if (seedEntries.length === 0) {
    console.log(
      `[dashboard-refresh] seed_skip ${label} dir=${normalizedTargetDir} seed=${normalizedSeedDir} reason=empty_seed`,
    );
    return;
  }

  const targetEntrySet = new Set(targetEntries);
  const missingEntries = seedEntries.filter((entry) => !targetEntrySet.has(entry));
  if (missingEntries.length === 0) {
    console.log(
      `[dashboard-refresh] reuse ${label} dir=${normalizedTargetDir} entries=${targetEntries.length}`,
    );
    return;
  }

  const startedAt = Date.now();
  for (const entry of missingEntries) {
    await cp(join(normalizedSeedDir, entry), join(normalizedTargetDir, entry), {
      recursive: true,
      force: false,
      errorOnExist: false,
    });
  }

  console.log(
    [
      "[dashboard-refresh] seeded_missing",
      `label=${label}`,
      `dir=${normalizedTargetDir}`,
      `seed=${normalizedSeedDir}`,
      `missingEntries=${missingEntries.length}`,
      `totalSeedEntries=${seedEntries.length}`,
      `durationSeconds=${((Date.now() - startedAt) / 1000).toFixed(1)}`,
    ].join(" | "),
  );
}

export async function initializeDashboardRuntimeDataFromEnv(): Promise<void> {
  await seedMissingTopLevelEntries("providers", getProviderDataDir(), getProviderSeedDataDir());
  await seedMissingTopLevelEntries("analytics", getAnalyticsDataDir(), getAnalyticsSeedDataDir());
}

export async function warmPrecomputedShortfallSnapshots(): Promise<void> {
  const result = await writePrecomputedShortfallSnapshots();
  console.log(
    `[dashboard-refresh] warmed shortfall snapshots viewCount=${result.viewCount}`,
  );
}

async function runRefreshCycle(options: DashboardAutoRefreshOptions): Promise<void> {
  const window = buildRefreshWindow(options.windowDays, options.alignMinutes);
  const startedAt = Date.now();

  console.log(
    [
      "[dashboard-refresh] cycle_start",
      `providers=${options.providers.join(",")}`,
      `window=${window.startAtIso}..${window.endAtIso}`,
      `refreshMinutes=${options.refreshMinutes}`,
      `alignMinutes=${options.alignMinutes}`,
    ].join(" | "),
  );

  await runSyncNewerForProviders(options.providers);
  await runRollingPrecompute(options, window);
  const snapshotResult = await writePrecomputedShortfallSnapshots();

  console.log(
    [
      "[dashboard-refresh] cycle_done",
      `durationSeconds=${((Date.now() - startedAt) / 1000).toFixed(1)}`,
      `viewCount=${snapshotResult.viewCount}`,
      `window=${window.startAtIso}..${window.endAtIso}`,
    ].join(" | "),
  );
}

async function runDashboardAutoRefreshLoop(options: DashboardAutoRefreshOptions): Promise<void> {
  console.log(
    [
      "[dashboard-refresh] enabled",
      `providers=${options.providers.join(",")}`,
      `refreshMinutes=${options.refreshMinutes}`,
      `windowDays=${options.windowDays}`,
      `alignMinutes=${options.alignMinutes}`,
      `stablecoinPegMode=${options.stablecoinPegMode}`,
    ].join(" | "),
  );

  while (true) {
    try {
      await runRefreshCycle(options);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.error(`[dashboard-refresh] cycle_error ${message}`);
    }
    await delay(options.refreshMinutes * 60 * 1_000);
  }
}

export function startDashboardAutoRefreshFromEnv(): boolean {
  const options = resolveDashboardAutoRefreshOptions();
  if (!options) {
    return false;
  }

  void runDashboardAutoRefreshLoop(options).catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[dashboard-refresh] fatal ${message}`);
    process.exit(1);
  });

  return true;
}
