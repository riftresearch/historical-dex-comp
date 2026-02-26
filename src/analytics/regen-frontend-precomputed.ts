import { isProviderKey, PROVIDER_KEYS, type ProviderKey } from "../domain/provider-key";

type StablecoinPegMode = "off" | "usd_1";

interface RegenConfig {
  providers: ProviderKey[];
  sourceTokens: string[];
  sourceChainCanonical: string;
  destinationChainCanonical: string;
  destinationTokenSymbol: string;
  includeReversePaths: boolean;
  startAtIso: string;
  endAtIso: string;
  stablecoinPegMode: StablecoinPegMode;
}

interface RegenTask {
  provider: ProviderKey;
  sourceChainCanonical: string;
  sourceToken: string;
  destinationChainCanonical: string;
  destinationTokenSymbol: string;
}

interface RegenResult {
  provider: ProviderKey;
  sourceChainCanonical: string;
  sourceToken: string;
  destinationChainCanonical: string;
  destinationTokenSymbol: string;
  durationMs: number;
}

const DEFAULT_WINDOW_DAYS = 30;
const DEFAULT_START_AT_ISO = "2026-01-21T00:00:00.000Z";
const DEFAULT_END_AT_ISO = "2026-02-21T23:59:59.999Z";
const DEFAULT_SOURCE_TOKENS = ["USDC", "USDT", "ETH"];
const DEFAULT_SOURCE_CHAIN = "eip155:1";
const DEFAULT_DESTINATION_CHAIN = "bitcoin:mainnet";
const DEFAULT_DESTINATION_TOKEN = "BTC";
const DEFAULT_INCLUDE_REVERSE_PATHS = true;
const DEFAULT_STABLECOIN_MODE: StablecoinPegMode = "usd_1";
const CBBTC_EXTRA_PROVIDERS: readonly ProviderKey[] = ["kyberswap", "lifi", "relay"];
const CBBTC_EXTRA_SOURCE_CHAIN = "eip155:1";
const CBBTC_EXTRA_DESTINATION_CHAIN = "eip155:1";
const CBBTC_EXTRA_DESTINATION_TOKEN = "CBBTC";
const CBBTC_EXTRA_SOURCE_TOKENS = ["USDC", "USDT", "ETH", "WETH"] as const;

function parsePositiveInt(name: string, rawValue: string): number {
  const parsed = Number(rawValue);
  if (!Number.isFinite(parsed) || !Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`Expected a positive integer for ${name}, received '${rawValue}'.`);
  }
  return parsed;
}

function parseIsoOrThrow(name: string, rawValue: string): string {
  const parsed = new Date(rawValue);
  if (Number.isNaN(parsed.getTime())) {
    throw new Error(`Expected ISO8601 datetime for ${name}, received '${rawValue}'.`);
  }
  return parsed.toISOString();
}

function parseProviders(rawValue: string): ProviderKey[] {
  const normalized = rawValue.trim().toLowerCase();
  if (!normalized || normalized === "all") {
    return [...PROVIDER_KEYS];
  }

  const parsed: ProviderKey[] = [];
  const seen = new Set<ProviderKey>();
  for (const part of normalized.split(",")) {
    const candidate = part.trim();
    if (!candidate) {
      continue;
    }
    if (!isProviderKey(candidate)) {
      throw new Error(`Invalid provider '${candidate}' in --providers.`);
    }
    if (seen.has(candidate)) {
      continue;
    }
    seen.add(candidate);
    parsed.push(candidate);
  }

  if (parsed.length === 0) {
    throw new Error("--providers requires at least one valid provider.");
  }
  return parsed;
}

function parseSourceTokens(rawValue: string): string[] {
  const parsed = rawValue
    .split(",")
    .map((value) => value.trim().toUpperCase())
    .filter((value) => value.length > 0);
  if (parsed.length === 0) {
    throw new Error("--source-tokens requires at least one symbol.");
  }
  return [...new Set(parsed)];
}

function printUsage(): void {
  console.log(`Usage:
  bun run src/analytics/regen-frontend-precomputed.ts [options]

Options:
  --providers <all|lifi,relay,thorchain,chainflip,garden,nearintents,kyberswap>
  --source-tokens <CSV>                  (default: USDC,USDT,ETH)
  --source-chain <canonical-chain>       (default: eip155:1)
  --destination-chain <canonical-chain>  (default: bitcoin:mainnet)
  --destination-token <symbol>           (default: BTC)
  --include-reverse-paths                (default: true)
  --forward-only                         (disable reverse-path precompute)
  --window-days <N>                      (default: 30, used when start/end are not both fixed defaults)
  --start-at <ISO8601>                   (default: 2026-01-21T00:00:00.000Z)
  --end-at <ISO8601>                     (default: 2026-02-21T23:59:59.999Z)
  --stablecoin-peg-mode <off|usd_1>      (default: usd_1)

Notes:
  If kyberswap, lifi, and/or relay are included in --providers, this script
  also precomputes eip155:1 paths for USDC/USDT/ETH/WETH -> CBBTC for those providers.

Example:
  bun run src/analytics/regen-frontend-precomputed.ts
`);
}

function parseArgs(argv: string[]): RegenConfig {
  let providers: ProviderKey[] = [...PROVIDER_KEYS];
  let sourceTokens = [...DEFAULT_SOURCE_TOKENS];
  let sourceChainCanonical = DEFAULT_SOURCE_CHAIN;
  let destinationChainCanonical = DEFAULT_DESTINATION_CHAIN;
  let destinationTokenSymbol = DEFAULT_DESTINATION_TOKEN;
  let includeReversePaths = DEFAULT_INCLUDE_REVERSE_PATHS;
  let windowDays = DEFAULT_WINDOW_DAYS;
  let startAtIso: string | null = null;
  let endAtIso: string | null = null;
  let hasWindowDaysOverride = false;
  let stablecoinPegMode: StablecoinPegMode = DEFAULT_STABLECOIN_MODE;

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (!arg) {
      continue;
    }
    if (arg === "--help" || arg === "-h") {
      printUsage();
      process.exit(0);
    }

    if (arg === "--providers") {
      const value = argv[index + 1];
      if (!value) {
        throw new Error("--providers requires a value.");
      }
      providers = parseProviders(value);
      index += 1;
      continue;
    }
    if (arg.startsWith("--providers=")) {
      providers = parseProviders(arg.split("=")[1] ?? "");
      continue;
    }

    if (arg === "--source-tokens") {
      const value = argv[index + 1];
      if (!value) {
        throw new Error("--source-tokens requires a value.");
      }
      sourceTokens = parseSourceTokens(value);
      index += 1;
      continue;
    }
    if (arg.startsWith("--source-tokens=")) {
      sourceTokens = parseSourceTokens(arg.split("=")[1] ?? "");
      continue;
    }

    if (arg === "--source-chain") {
      const value = argv[index + 1];
      if (!value) {
        throw new Error("--source-chain requires a value.");
      }
      sourceChainCanonical = value.trim();
      index += 1;
      continue;
    }
    if (arg.startsWith("--source-chain=")) {
      sourceChainCanonical = (arg.split("=")[1] ?? "").trim();
      if (!sourceChainCanonical) {
        throw new Error("--source-chain requires a value.");
      }
      continue;
    }

    if (arg === "--destination-chain") {
      const value = argv[index + 1];
      if (!value) {
        throw new Error("--destination-chain requires a value.");
      }
      destinationChainCanonical = value.trim();
      index += 1;
      continue;
    }
    if (arg.startsWith("--destination-chain=")) {
      destinationChainCanonical = (arg.split("=")[1] ?? "").trim();
      if (!destinationChainCanonical) {
        throw new Error("--destination-chain requires a value.");
      }
      continue;
    }

    if (arg === "--destination-token") {
      const value = argv[index + 1];
      if (!value) {
        throw new Error("--destination-token requires a value.");
      }
      destinationTokenSymbol = value.trim().toUpperCase();
      index += 1;
      continue;
    }

    if (arg === "--include-reverse-paths") {
      includeReversePaths = true;
      continue;
    }
    if (arg === "--forward-only") {
      includeReversePaths = false;
      continue;
    }
    if (arg.startsWith("--destination-token=")) {
      destinationTokenSymbol = (arg.split("=")[1] ?? "").trim().toUpperCase();
      if (!destinationTokenSymbol) {
        throw new Error("--destination-token requires a value.");
      }
      continue;
    }

    if (arg === "--window-days") {
      const value = argv[index + 1];
      if (!value) {
        throw new Error("--window-days requires a value.");
      }
      windowDays = parsePositiveInt("--window-days", value);
      hasWindowDaysOverride = true;
      index += 1;
      continue;
    }
    if (arg.startsWith("--window-days=")) {
      windowDays = parsePositiveInt("--window-days", arg.split("=")[1] ?? "");
      hasWindowDaysOverride = true;
      continue;
    }

    if (arg === "--start-at") {
      const value = argv[index + 1];
      if (!value) {
        throw new Error("--start-at requires an ISO8601 value.");
      }
      startAtIso = parseIsoOrThrow("--start-at", value);
      index += 1;
      continue;
    }
    if (arg.startsWith("--start-at=")) {
      startAtIso = parseIsoOrThrow("--start-at", arg.split("=")[1] ?? "");
      continue;
    }

    if (arg === "--end-at") {
      const value = argv[index + 1];
      if (!value) {
        throw new Error("--end-at requires an ISO8601 value.");
      }
      endAtIso = parseIsoOrThrow("--end-at", value);
      index += 1;
      continue;
    }
    if (arg.startsWith("--end-at=")) {
      endAtIso = parseIsoOrThrow("--end-at", arg.split("=")[1] ?? "");
      continue;
    }

    if (arg === "--stablecoin-peg-mode") {
      const value = argv[index + 1];
      if (!value) {
        throw new Error("--stablecoin-peg-mode requires a value.");
      }
      const normalized = value.trim().toLowerCase();
      if (normalized !== "off" && normalized !== "usd_1") {
        throw new Error(`Invalid --stablecoin-peg-mode '${value}'. Expected off|usd_1.`);
      }
      stablecoinPegMode = normalized;
      index += 1;
      continue;
    }
    if (arg.startsWith("--stablecoin-peg-mode=")) {
      const normalized = (arg.split("=")[1] ?? "").trim().toLowerCase();
      if (normalized !== "off" && normalized !== "usd_1") {
        throw new Error(`Invalid --stablecoin-peg-mode '${normalized}'. Expected off|usd_1.`);
      }
      stablecoinPegMode = normalized;
      continue;
    }

    throw new Error(`Unknown argument '${arg}'.`);
  }

  const useFixedDefaultRange = !startAtIso && !endAtIso && !hasWindowDaysOverride;
  const endAt = useFixedDefaultRange
    ? new Date(DEFAULT_END_AT_ISO)
    : endAtIso
      ? new Date(endAtIso)
      : new Date();
  const startAt = useFixedDefaultRange
    ? new Date(DEFAULT_START_AT_ISO)
    : startAtIso
      ? new Date(startAtIso)
      : new Date(endAt.getTime() - windowDays * 24 * 60 * 60 * 1000);
  if (startAt.getTime() > endAt.getTime()) {
    throw new Error("--start-at must be <= --end-at.");
  }

  return {
    providers,
    sourceTokens,
    sourceChainCanonical,
    destinationChainCanonical,
    destinationTokenSymbol,
    includeReversePaths,
    startAtIso: startAt.toISOString(),
    endAtIso: endAt.toISOString(),
    stablecoinPegMode,
  };
}

function tailLines(text: string, maxLines: number): string {
  const lines = text
    .split(/\r?\n/)
    .map((line) => line.trimEnd())
    .filter((line) => line.length > 0);
  return lines.slice(-maxLines).join("\n");
}

async function runTask(config: RegenConfig, task: RegenTask): Promise<RegenResult> {
  const startedAt = Date.now();
  const bunBinary = Bun.which("bun") ?? "bun";
  const cmd = [
    bunBinary,
    "run",
    "src/analytics/implementation-shortfall.ts",
    "--provider",
    task.provider,
    "--source-chain",
    task.sourceChainCanonical,
    "--source-token",
    task.sourceToken,
    "--destination-chain",
    task.destinationChainCanonical,
    "--destination-token",
    task.destinationTokenSymbol,
    "--start-at",
    config.startAtIso,
    "--end-at",
    config.endAtIso,
    "--stablecoin-peg-mode",
    config.stablecoinPegMode,
  ];

  const proc = Bun.spawn({
    cmd,
    stdout: "pipe",
    stderr: "pipe",
    cwd: process.cwd(),
  });

  const stdoutPromise = new Response(proc.stdout).text();
  const stderrPromise = new Response(proc.stderr).text();
  const exitCode = await proc.exited;
  const [stdout, stderr] = await Promise.all([stdoutPromise, stderrPromise]);

  if (exitCode !== 0) {
    const details = tailLines(`${stdout}\n${stderr}`, 25);
    throw new Error(
      `Precompute failed for provider=${task.provider} path=${task.sourceToken}/${task.sourceChainCanonical}->${task.destinationTokenSymbol}/${task.destinationChainCanonical}. Exit=${exitCode}.\n${details}`,
    );
  }

  return {
    provider: task.provider,
    sourceChainCanonical: task.sourceChainCanonical,
    sourceToken: task.sourceToken,
    destinationChainCanonical: task.destinationChainCanonical,
    destinationTokenSymbol: task.destinationTokenSymbol,
    durationMs: Date.now() - startedAt,
  };
}

async function run(): Promise<void> {
  const config = parseArgs(Bun.argv.slice(2));
  const tasks: RegenTask[] = [];
  const seenTaskKeys = new Set<string>();
  const pushTask = (task: RegenTask): void => {
    const key = [
      task.provider,
      task.sourceChainCanonical,
      task.sourceToken,
      task.destinationChainCanonical,
      task.destinationTokenSymbol,
    ].join("|");
    if (seenTaskKeys.has(key)) {
      return;
    }
    seenTaskKeys.add(key);
    tasks.push(task);
  };

  for (const provider of config.providers) {
    for (const sourceToken of config.sourceTokens) {
      pushTask({
        provider,
        sourceChainCanonical: config.sourceChainCanonical,
        sourceToken,
        destinationChainCanonical: config.destinationChainCanonical,
        destinationTokenSymbol: config.destinationTokenSymbol,
      });
    }
  }

  if (config.includeReversePaths) {
    for (const provider of config.providers) {
      for (const destinationTokenSymbol of config.sourceTokens) {
        pushTask({
          provider,
          sourceChainCanonical: config.destinationChainCanonical,
          sourceToken: config.destinationTokenSymbol,
          destinationChainCanonical: config.sourceChainCanonical,
          destinationTokenSymbol,
        });
      }
    }
  }

  for (const provider of CBBTC_EXTRA_PROVIDERS) {
    if (!config.providers.includes(provider)) {
      continue;
    }
    for (const sourceToken of CBBTC_EXTRA_SOURCE_TOKENS) {
      pushTask({
        provider,
        sourceChainCanonical: CBBTC_EXTRA_SOURCE_CHAIN,
        sourceToken,
        destinationChainCanonical: CBBTC_EXTRA_DESTINATION_CHAIN,
        destinationTokenSymbol: CBBTC_EXTRA_DESTINATION_TOKEN,
      });
    }
  }

  const startedAt = Date.now();
  console.log(
    [
      `Precompute start`,
      `providers=${config.providers.join(",")}`,
      `tokens=${config.sourceTokens.join(",")}`,
      `path=${config.sourceChainCanonical}:${config.destinationChainCanonical}/${config.destinationTokenSymbol}`,
      `includeReversePaths=${config.includeReversePaths}`,
      `window=${config.startAtIso}..${config.endAtIso}`,
      `stablecoinPegMode=${config.stablecoinPegMode}`,
      `tasks=${tasks.length}`,
    ].join(" | "),
  );

  const results: RegenResult[] = [];
  for (const task of tasks) {
    console.log(
      `Running provider=${task.provider} path=${task.sourceToken}/${task.sourceChainCanonical}->${task.destinationTokenSymbol}/${task.destinationChainCanonical}`,
    );
    const result = await runTask(config, task);
    results.push(result);
    const seconds = (result.durationMs / 1000).toFixed(1);
    console.log(
      `Done provider=${result.provider} path=${result.sourceToken}/${result.sourceChainCanonical}->${result.destinationTokenSymbol}/${result.destinationChainCanonical} durationSeconds=${seconds}`,
    );
  }

  const totalSeconds = ((Date.now() - startedAt) / 1000).toFixed(1);
  console.log(
    `Precompute complete | succeeded=${results.length}/${tasks.length} | totalDurationSeconds=${totalSeconds}`,
  );
}

run().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`Error: ${message}`);
  process.exit(1);
});
