import { mkdir, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import type { DuckDBConnection } from "@duckdb/node-api";
import { Database } from "bun:sqlite";
import { isProviderKey, type ProviderKey } from "../domain/provider-key";
import { queryPrepared } from "../storage/duckdb-utils";
import { openProviderDatabase } from "../storage/provider-db";

type StablecoinPegMode = "off" | "usd_1";

interface ImplementationShortfallConfig {
  provider: ProviderKey;
  sourceChainCanonical: string;
  sourceTokenSymbol: string;
  destinationChainCanonical: string;
  destinationTokenSymbol: string;
  startAtIso: string | null;
  endAtIso: string | null;
  limit: number | null;
  histogramBucketBps: number;
  minNotionalUsd: number | null;
  maxNotionalUsd: number | null;
  minNotionalInclusive: boolean;
  maxNotionalInclusive: boolean;
  stablecoinPegMode: StablecoinPegMode;
}

interface SwapCandidateRow {
  normalized_id: string;
  provider_record_id: string;
  event_time_iso: string;
  event_epoch_seconds: number | bigint | null;
  source_chain_canonical: string | null;
  destination_chain_canonical: string | null;
  source_asset_symbol: string | null;
  destination_asset_symbol: string | null;
  source_asset_decimals: number | bigint | null;
  destination_asset_decimals: number | bigint | null;
  amount_in_atomic: string | null;
  amount_out_atomic: string | null;
  amount_in_normalized: number | bigint | null;
  amount_out_normalized: number | bigint | null;
}

interface CoinbaseProductResponse {
  id?: string;
  quote_currency?: string;
}

type CoinbaseCandle = [number, number, number, number, number, number];

interface PriceCacheLookupRow {
  price_usd: number | null;
  product_id: string | null;
  candle_time: number | null;
  missing: number;
  note: string | null;
}

interface ProductCacheLookupRow {
  product_id: string | null;
  missing: number;
}

interface TradeShortfallRow {
  normalizedId: string;
  providerRecordId: string;
  eventAt: string;
  eventMinuteTs: number;
  sourceChainCanonical: string | null;
  destinationChainCanonical: string | null;
  sourceTokenSymbol: string;
  destinationTokenSymbol: string;
  amountIn: number;
  amountOut: number;
  sourcePriceUsd: number;
  destinationPriceUsd: number;
  inputUsd: number;
  outputUsd: number;
  implementationShortfallBps: number;
}

interface HistogramBucket {
  bucketStartBps: number;
  bucketEndBps: number;
  count: number;
}

interface AnalysisStats {
  matchedTrades: number;
  pricedTrades: number;
  skippedInvalidTimestamp: number;
  skippedMissingAmount: number;
  skippedNonPositiveAmount: number;
  skippedMissingPrice: number;
  skippedOutsideNotionalBand: number;
}

const COINBASE_API_BASE = "https://api.exchange.coinbase.com";
const PRICE_CACHE_DB_PATH = join(
  process.cwd(),
  "data",
  "analytics",
  "coinbase-price-cache.sqlite",
);
const OUTPUT_ROOT_DIR = join(process.cwd(), "data", "analytics", "execution-quality");

const DEFAULT_ANALYSIS_CONFIG: ImplementationShortfallConfig = {
  provider: "relay",
  sourceChainCanonical: "eip155:8453",
  sourceTokenSymbol: "ETH",
  destinationChainCanonical: "eip155:8453",
  destinationTokenSymbol: "USDC",
  startAtIso: "2026-01-26T05:07:00.000Z",
  endAtIso: "2026-02-22T00:15:00.000Z",
  limit: null,
  histogramBucketBps: 10,
  minNotionalUsd: null,
  maxNotionalUsd: null,
  minNotionalInclusive: false,
  maxNotionalInclusive: false,
  stablecoinPegMode: "off",
};

const COINBASE_SYMBOL_ALIASES: Record<string, string[]> = {
  WETH: ["ETH"],
  WBTC: ["BTC"],
  CBBTC: ["BTC"],
  CBTC: ["BTC"],
  XBT: ["BTC"],
  WSOL: ["SOL"],
  WNEAR: ["NEAR"],
  USDC: ["USDT"],
  "USDC.E": ["USDC", "USDT"],
  USDE: ["USDT"],
  DAI: ["USDT"],
  FRAX: ["USDT"],
  PYUSD: ["USDT"],
  USDCE: ["USDC"],
  USDT0: ["USDT"],
  STETH: ["ETH"],
};

const KNOWN_TOKEN_DECIMALS: Record<string, number> = {
  DAI: 18,
  ETH: 18,
  WETH: 18,
  STETH: 18,
  BTC: 8,
  WBTC: 8,
  CBBTC: 8,
  BBTC: 8,
  BTCB: 8,
  TBTC: 8,
  IBTC: 8,
  UBTC: 8,
  CBTC: 8,
  USDC: 6,
  "USDC.E": 6,
  USDCE: 6,
  USDT: 6,
  USDT0: 6,
};

const STABLECOIN_USD_PEG_SYMBOLS = new Set<string>([
  "DAI",
  "FRAX",
  "PYUSD",
  "USDC",
  "USDC.E",
  "USDCE",
  "USDE",
  "USDT",
  "USDT0",
]);

class CoinbaseHttpError extends Error {
  readonly status: number;

  constructor(message: string, status: number) {
    super(message);
    this.name = "CoinbaseHttpError";
    this.status = status;
  }
}

function printUsage(): void {
  console.log(`Usage:
  bun run src/analytics/implementation-shortfall.ts [options]

Options:
  --provider <lifi|relay|thorchain|chainflip|garden|nearintents|kyberswap>
  --source-chain <canonical-chain>
  --source-token <symbol>
  --destination-chain <canonical-chain>
  --destination-token <symbol>
  --start-at <ISO8601>
  --end-at <ISO8601>
  --limit <N>
  --histogram-bucket-bps <N>
  --min-notional-usd <N>
  --max-notional-usd <N>
  --min-notional-inclusive
  --max-notional-inclusive
  --stablecoin-peg-mode <off|usd_1>

Example:
  bun run src/analytics/implementation-shortfall.ts \\
    --provider relay \\
    --source-chain eip155:8453 \\
    --source-token ETH \\
    --destination-chain eip155:8453 \\
    --destination-token USDC \\
    --limit 1000
`);
}

function parsePositiveInt(name: string, rawValue: string): number {
  const parsed = Number(rawValue);
  if (!Number.isFinite(parsed) || !Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`Expected a positive integer for ${name}, received '${rawValue}'.`);
  }
  return parsed;
}

function parseIsoDateOrThrow(name: string, rawValue: string): string {
  const parsed = new Date(rawValue);
  if (Number.isNaN(parsed.getTime())) {
    throw new Error(`Expected ISO8601 for ${name}, received '${rawValue}'.`);
  }
  return parsed.toISOString();
}

function normalizeSymbol(rawValue: string): string {
  return rawValue.trim().toUpperCase();
}

function parseArgs(argv: string[]): ImplementationShortfallConfig {
  const config: ImplementationShortfallConfig = {
    ...DEFAULT_ANALYSIS_CONFIG,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (!arg) {
      continue;
    }
    if (arg === "--help" || arg === "-h") {
      printUsage();
      process.exit(0);
    }
    if (arg === "--provider") {
      const value = argv[index + 1];
      if (!value) {
        throw new Error("--provider requires a value.");
      }
      const normalized = value.trim().toLowerCase();
      if (!isProviderKey(normalized)) {
        throw new Error(`Invalid --provider '${value}'.`);
      }
      config.provider = normalized;
      index += 1;
      continue;
    }
    if (arg === "--source-chain") {
      const value = argv[index + 1];
      if (!value) {
        throw new Error("--source-chain requires a value.");
      }
      config.sourceChainCanonical = value.trim();
      index += 1;
      continue;
    }
    if (arg === "--source-token") {
      const value = argv[index + 1];
      if (!value) {
        throw new Error("--source-token requires a value.");
      }
      config.sourceTokenSymbol = normalizeSymbol(value);
      index += 1;
      continue;
    }
    if (arg === "--destination-chain") {
      const value = argv[index + 1];
      if (!value) {
        throw new Error("--destination-chain requires a value.");
      }
      config.destinationChainCanonical = value.trim();
      index += 1;
      continue;
    }
    if (arg === "--destination-token") {
      const value = argv[index + 1];
      if (!value) {
        throw new Error("--destination-token requires a value.");
      }
      config.destinationTokenSymbol = normalizeSymbol(value);
      index += 1;
      continue;
    }
    if (arg === "--start-at") {
      const value = argv[index + 1];
      if (!value) {
        throw new Error("--start-at requires an ISO8601 value.");
      }
      config.startAtIso = parseIsoDateOrThrow("--start-at", value);
      index += 1;
      continue;
    }
    if (arg === "--end-at") {
      const value = argv[index + 1];
      if (!value) {
        throw new Error("--end-at requires an ISO8601 value.");
      }
      config.endAtIso = parseIsoDateOrThrow("--end-at", value);
      index += 1;
      continue;
    }
    if (arg === "--limit") {
      const value = argv[index + 1];
      if (!value) {
        throw new Error("--limit requires a numeric value.");
      }
      config.limit = parsePositiveInt("--limit", value);
      index += 1;
      continue;
    }
    if (arg === "--histogram-bucket-bps") {
      const value = argv[index + 1];
      if (!value) {
        throw new Error("--histogram-bucket-bps requires a numeric value.");
      }
      config.histogramBucketBps = parsePositiveInt("--histogram-bucket-bps", value);
      index += 1;
      continue;
    }
    if (arg === "--min-notional-usd") {
      const value = argv[index + 1];
      if (!value) {
        throw new Error("--min-notional-usd requires a numeric value.");
      }
      config.minNotionalUsd = parsePositiveInt("--min-notional-usd", value);
      index += 1;
      continue;
    }
    if (arg === "--min-notional-inclusive") {
      config.minNotionalInclusive = true;
      continue;
    }
    if (arg === "--max-notional-usd") {
      const value = argv[index + 1];
      if (!value) {
        throw new Error("--max-notional-usd requires a numeric value.");
      }
      config.maxNotionalUsd = parsePositiveInt("--max-notional-usd", value);
      index += 1;
      continue;
    }
    if (arg === "--max-notional-inclusive") {
      config.maxNotionalInclusive = true;
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
      config.stablecoinPegMode = normalized;
      index += 1;
      continue;
    }
    throw new Error(`Unknown argument '${arg}'.`);
  }

  if (config.startAtIso && config.endAtIso) {
    const startMs = new Date(config.startAtIso).getTime();
    const endMs = new Date(config.endAtIso).getTime();
    if (startMs > endMs) {
      throw new Error("--start-at must be <= --end-at.");
    }
  }
  if (
    config.minNotionalUsd !== null &&
    config.maxNotionalUsd !== null &&
    config.minNotionalUsd > config.maxNotionalUsd
  ) {
    throw new Error("--min-notional-usd must be <= --max-notional-usd.");
  }
  if (
    config.minNotionalUsd !== null &&
    config.maxNotionalUsd !== null &&
    config.minNotionalUsd === config.maxNotionalUsd &&
    (!config.minNotionalInclusive || !config.maxNotionalInclusive)
  ) {
    throw new Error(
      "If --min-notional-usd equals --max-notional-usd, both --min-notional-inclusive and --max-notional-inclusive are required.",
    );
  }

  return config;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isRetryableStatus(status: number): boolean {
  return status === 408 || status === 429 || status >= 500;
}

async function fetchCoinbaseJson<T>(path: string): Promise<T> {
  const url = `${COINBASE_API_BASE}${path}`;
  const attempts = 4;
  let delayMs = 400;

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      const response = await fetch(url, {
        headers: {
          Accept: "application/json",
          "User-Agent": "historical-dex-comp/implementation-shortfall",
        },
      });
      if (!response.ok) {
        const body = await response.text();
        if (attempt < attempts && isRetryableStatus(response.status)) {
          await sleep(delayMs);
          delayMs = Math.min(delayMs * 2, 5_000);
          continue;
        }
        throw new CoinbaseHttpError(
          `Coinbase request failed (${response.status}) for ${path}: ${body.slice(0, 300)}`,
          response.status,
        );
      }
      return (await response.json()) as T;
    } catch (error) {
      if (error instanceof CoinbaseHttpError) {
        throw error;
      }
      if (attempt >= attempts) {
        throw new Error(`Coinbase request failed for ${path}: ${String(error)}`);
      }
      await sleep(delayMs);
      delayMs = Math.min(delayMs * 2, 5_000);
    }
  }

  throw new Error(`Unreachable Coinbase request state for ${path}`);
}

class PriceCacheStore {
  readonly db: Database;

  constructor(filePath: string) {
    this.db = new Database(filePath, { create: true });
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS product_cache (
        token_symbol TEXT PRIMARY KEY,
        product_id TEXT,
        missing INTEGER NOT NULL DEFAULT 0,
        checked_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS price_cache (
        token_symbol TEXT NOT NULL,
        minute_ts INTEGER NOT NULL,
        product_id TEXT,
        candle_time INTEGER,
        price_usd DOUBLE,
        missing INTEGER NOT NULL DEFAULT 0,
        note TEXT,
        fetched_at TEXT NOT NULL,
        PRIMARY KEY (token_symbol, minute_ts)
      );
    `);
  }

  close(): void {
    this.db.close();
  }

  getProduct(tokenSymbol: string): ProductCacheLookupRow | null {
    const row = this.db
      .query(
        `SELECT product_id, missing FROM product_cache WHERE token_symbol = ?`,
      )
      .get(tokenSymbol) as ProductCacheLookupRow | null;
    return row ?? null;
  }

  upsertProduct(tokenSymbol: string, productId: string | null, missing: boolean): void {
    this.db
      .query(
        `
        INSERT INTO product_cache (token_symbol, product_id, missing, checked_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT (token_symbol) DO UPDATE SET
          product_id = excluded.product_id,
          missing = excluded.missing,
          checked_at = excluded.checked_at
      `,
      )
      .run(tokenSymbol, productId, missing ? 1 : 0, new Date().toISOString());
  }

  getPrice(tokenSymbol: string, minuteTs: number): PriceCacheLookupRow | null {
    const row = this.db
      .query(
        `
        SELECT
          price_usd,
          product_id,
          candle_time,
          missing,
          note
        FROM price_cache
        WHERE token_symbol = ? AND minute_ts = ?
      `,
      )
      .get(tokenSymbol, minuteTs) as PriceCacheLookupRow | null;
    return row ?? null;
  }

  upsertPrice(
    tokenSymbol: string,
    minuteTs: number,
    input: {
      productId: string | null;
      candleTime: number | null;
      priceUsd: number | null;
      missing: boolean;
      note: string | null;
    },
  ): void {
    this.db
      .query(
        `
        INSERT INTO price_cache (
          token_symbol,
          minute_ts,
          product_id,
          candle_time,
          price_usd,
          missing,
          note,
          fetched_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (token_symbol, minute_ts) DO UPDATE SET
          product_id = excluded.product_id,
          candle_time = excluded.candle_time,
          price_usd = excluded.price_usd,
          missing = excluded.missing,
          note = excluded.note,
          fetched_at = excluded.fetched_at
      `,
      )
      .run(
        tokenSymbol,
        minuteTs,
        input.productId,
        input.candleTime,
        input.priceUsd,
        input.missing ? 1 : 0,
        input.note,
        new Date().toISOString(),
      );
  }
}

function uniqueStrings(values: readonly string[]): string[] {
  const seen = new Set<string>();
  const output: string[] = [];
  for (const value of values) {
    const normalized = value.trim().toUpperCase();
    if (!normalized || seen.has(normalized)) {
      continue;
    }
    seen.add(normalized);
    output.push(normalized);
  }
  return output;
}

function buildSymbolCandidates(tokenSymbol: string): string[] {
  const normalized = normalizeSymbol(tokenSymbol);
  const candidates: string[] = [normalized];

  const aliases = COINBASE_SYMBOL_ALIASES[normalized] ?? [];
  candidates.push(...aliases);

  if (normalized.includes(".")) {
    candidates.push(normalized.split(".")[0] ?? "");
  }
  if (normalized.includes("_")) {
    candidates.push(normalized.split("_")[0] ?? "");
  }
  if (normalized.includes("-")) {
    candidates.push(normalized.split("-")[0] ?? "");
  }
  if (normalized.startsWith("W") && normalized.length > 3) {
    candidates.push(normalized.slice(1));
  }

  return uniqueStrings(candidates);
}

function isUsdPegStablecoinSymbol(tokenSymbol: string): boolean {
  const normalized = normalizeSymbol(tokenSymbol);
  if (STABLECOIN_USD_PEG_SYMBOLS.has(normalized)) {
    return true;
  }
  return buildSymbolCandidates(normalized).some((candidate) => STABLECOIN_USD_PEG_SYMBOLS.has(candidate));
}

async function resolveCoinbaseProductId(
  tokenSymbol: string,
  cache: PriceCacheStore,
): Promise<string | null> {
  const normalized = normalizeSymbol(tokenSymbol);
  const cached = cache.getProduct(normalized);
  if (cached) {
    if (cached.missing === 0) {
      return cached.product_id;
    }
    const hasAlternateCandidate = buildSymbolCandidates(normalized).some(
      (candidate) => candidate !== normalized,
    );
    if (!hasAlternateCandidate) {
      return null;
    }
  }

  const candidates = buildSymbolCandidates(normalized);
  for (const candidateSymbol of candidates) {
    const productId = `${candidateSymbol}-USD`;
    try {
      const product = await fetchCoinbaseJson<CoinbaseProductResponse>(`/products/${productId}`);
      const quoteCurrency = normalizeSymbol(product.quote_currency ?? "");
      if (quoteCurrency !== "USD") {
        continue;
      }
      cache.upsertProduct(normalized, productId, false);
      return productId;
    } catch (error) {
      if (error instanceof CoinbaseHttpError && error.status === 404) {
        continue;
      }
      throw error;
    }
  }

  cache.upsertProduct(normalized, null, true);
  return null;
}

function selectCandle(candles: CoinbaseCandle[], minuteTs: number): CoinbaseCandle | null {
  if (candles.length === 0) {
    return null;
  }

  const exact = candles.find((candle) => candle[0] === minuteTs);
  if (exact) {
    return exact;
  }

  let best = candles[0] ?? null;
  let bestDistance = best ? Math.abs(best[0] - minuteTs) : Number.POSITIVE_INFINITY;
  for (const candle of candles) {
    const distance = Math.abs(candle[0] - minuteTs);
    if (distance < bestDistance) {
      best = candle;
      bestDistance = distance;
    }
  }

  if (!best || bestDistance > 60) {
    return null;
  }
  return best;
}

function toUnixMinuteFromEpochSeconds(epochSeconds: number): number | null {
  if (!Number.isFinite(epochSeconds)) {
    return null;
  }
  const seconds = Math.floor(epochSeconds);
  return seconds - (seconds % 60);
}

async function getUsdPriceAtMinute(
  tokenSymbol: string,
  minuteTs: number,
  cache: PriceCacheStore,
  runtimeCache: Map<string, number | null>,
  stablecoinPegMode: StablecoinPegMode,
): Promise<number | null> {
  const normalized = normalizeSymbol(tokenSymbol);
  if (stablecoinPegMode === "usd_1" && isUsdPegStablecoinSymbol(normalized)) {
    return 1;
  }
  const runtimeKey = `${normalized}|${minuteTs}`;
  const runtimeValue = runtimeCache.get(runtimeKey);
  if (runtimeValue !== undefined) {
    return runtimeValue;
  }

  const persisted = cache.getPrice(normalized, minuteTs);
  if (persisted) {
    if (persisted.missing === 0) {
      runtimeCache.set(runtimeKey, persisted.price_usd);
      return persisted.price_usd;
    }
    const hasAlternateCandidate = buildSymbolCandidates(normalized).some(
      (candidate) => candidate !== normalized,
    );
    if (!(persisted.note === "product_not_found" && hasAlternateCandidate)) {
      runtimeCache.set(runtimeKey, null);
      return null;
    }
  }

  const productId = await resolveCoinbaseProductId(normalized, cache);
  if (!productId) {
    cache.upsertPrice(normalized, minuteTs, {
      productId: null,
      candleTime: null,
      priceUsd: null,
      missing: true,
      note: "product_not_found",
    });
    runtimeCache.set(runtimeKey, null);
    return null;
  }

  const startIso = new Date(minuteTs * 1_000).toISOString();
  const endIso = new Date((minuteTs + 60) * 1_000).toISOString();
  const candles = await fetchCoinbaseJson<CoinbaseCandle[]>(
    `/products/${encodeURIComponent(productId)}/candles?granularity=60&start=${encodeURIComponent(startIso)}&end=${encodeURIComponent(endIso)}`,
  );
  const candle = selectCandle(candles, minuteTs);
  if (!candle) {
    cache.upsertPrice(normalized, minuteTs, {
      productId,
      candleTime: null,
      priceUsd: null,
      missing: true,
      note: "missing_candle",
    });
    runtimeCache.set(runtimeKey, null);
    return null;
  }

  const closePrice = Number(candle[4]);
  if (!Number.isFinite(closePrice) || closePrice <= 0) {
    cache.upsertPrice(normalized, minuteTs, {
      productId,
      candleTime: candle[0],
      priceUsd: null,
      missing: true,
      note: "invalid_candle_close",
    });
    runtimeCache.set(runtimeKey, null);
    return null;
  }

  cache.upsertPrice(normalized, minuteTs, {
    productId,
    candleTime: candle[0],
    priceUsd: closePrice,
    missing: false,
    note: null,
  });
  runtimeCache.set(runtimeKey, closePrice);
  return closePrice;
}

function toNullableNumber(value: number | bigint | null | undefined): number | null {
  if (value === null || value === undefined) {
    return null;
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return null;
  }
  return numeric;
}

function parseIntegerString(value: string): string | null {
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }
  if (!/^-?[0-9]+$/.test(trimmed)) {
    return null;
  }
  return trimmed;
}

function atomicToNormalized(atomic: string, decimals: number): number | null {
  const parsed = parseIntegerString(atomic);
  if (!parsed) {
    return null;
  }
  if (!Number.isInteger(decimals) || decimals < 0 || decimals > 36) {
    return null;
  }
  const negative = parsed.startsWith("-");
  const digitsRaw = negative ? parsed.slice(1) : parsed;
  const digits = digitsRaw.replace(/^0+/, "") || "0";
  if (digits === "0") {
    return 0;
  }

  let intPart = "0";
  let fracPart = digits;
  if (digits.length > decimals) {
    intPart = digits.slice(0, digits.length - decimals);
    fracPart = digits.slice(digits.length - decimals);
  } else {
    intPart = "0";
    fracPart = digits.padStart(decimals, "0");
  }
  fracPart = fracPart.replace(/0+$/, "");

  const text = `${negative ? "-" : ""}${intPart}${fracPart ? `.${fracPart}` : ""}`;
  const value = Number(text);
  if (!Number.isFinite(value)) {
    return null;
  }
  return value;
}

function inferFallbackDecimals(
  provider: ProviderKey,
  symbol: string,
  explicitDecimals: number | null,
): number | null {
  if (explicitDecimals !== null && Number.isInteger(explicitDecimals) && explicitDecimals >= 0) {
    return explicitDecimals;
  }
  if (provider === "thorchain") {
    return 8;
  }
  return KNOWN_TOKEN_DECIMALS[normalizeSymbol(symbol)] ?? null;
}

function resolveNormalizedAmount(input: {
  normalized: number | bigint | null;
  atomic: string | null;
  explicitDecimals: number | bigint | null;
  provider: ProviderKey;
  symbol: string;
}): number | null {
  const normalized = toNullableNumber(input.normalized);
  const explicitDecimals = toNullableNumber(input.explicitDecimals);
  const decimals = inferFallbackDecimals(
    input.provider,
    input.symbol,
    explicitDecimals === null ? null : Math.floor(explicitDecimals),
  );
  if (input.atomic && decimals !== null) {
    const converted = atomicToNormalized(input.atomic, decimals);
    if (converted !== null) {
      return converted;
    }
  }
  if (normalized !== null) {
    return normalized;
  }
  return null;
}

function sanitizeSegment(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9._-]+/g, "_").replace(/^_+|_+$/g, "");
}

function csvEscape(value: string | number | null): string {
  if (value === null) {
    return "";
  }
  const text = typeof value === "number" ? String(value) : value;
  if (text.includes(",") || text.includes('"') || text.includes("\n")) {
    return `"${text.replaceAll('"', '""')}"`;
  }
  return text;
}

function percentile(sortedValues: number[], p: number): number | null {
  if (sortedValues.length === 0) {
    return null;
  }
  const index = (sortedValues.length - 1) * p;
  const lower = Math.floor(index);
  const upper = Math.ceil(index);
  if (lower === upper) {
    return sortedValues[lower] ?? null;
  }
  const lowerValue = sortedValues[lower] ?? 0;
  const upperValue = sortedValues[upper] ?? 0;
  return lowerValue + (upperValue - lowerValue) * (index - lower);
}

function sum(values: number[]): number {
  return values.reduce((accumulator, value) => accumulator + value, 0);
}

function mean(values: number[]): number | null {
  if (values.length === 0) {
    return null;
  }
  return sum(values) / values.length;
}

function sampleStdDev(values: number[]): number | null {
  if (values.length < 2) {
    return null;
  }
  const avg = mean(values);
  if (avg === null) {
    return null;
  }
  const variance = values.reduce((accumulator, value) => {
    const delta = value - avg;
    return accumulator + delta * delta;
  }, 0) / (values.length - 1);
  return Math.sqrt(variance);
}

function roundTo(value: number, decimals = 6): number {
  return Number(value.toFixed(decimals));
}

function buildHistogram(values: number[], bucketSizeBps: number): HistogramBucket[] {
  const buckets = new Map<number, number>();
  for (const value of values) {
    const bucketStart = Math.floor(value / bucketSizeBps) * bucketSizeBps;
    buckets.set(bucketStart, (buckets.get(bucketStart) ?? 0) + 1);
  }
  return [...buckets.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([bucketStartBps, count]) => ({
      bucketStartBps,
      bucketEndBps: bucketStartBps + bucketSizeBps,
      count,
    }));
}

async function queryCandidateSwaps(
  connection: DuckDBConnection,
  config: ImplementationShortfallConfig,
): Promise<SwapCandidateRow[]> {
  const params: unknown[] = [
    config.sourceChainCanonical,
    config.destinationChainCanonical,
    normalizeSymbol(config.sourceTokenSymbol),
    normalizeSymbol(config.destinationTokenSymbol),
  ];

  let sql = `
    SELECT
      normalized_id,
      provider_record_id,
      CAST(COALESCE(event_at, created_at, updated_at) AS VARCHAR) AS event_time_iso,
      EPOCH(COALESCE(event_at, created_at, updated_at)) AS event_epoch_seconds,
      source_chain_canonical,
      destination_chain_canonical,
      source_asset_symbol,
      destination_asset_symbol,
      source_asset_decimals,
      destination_asset_decimals,
      amount_in_atomic,
      amount_out_atomic,
      amount_in_normalized,
      amount_out_normalized
    FROM swaps_core
    WHERE status_canonical = 'success'
      AND COALESCE(event_at, created_at, updated_at) IS NOT NULL
      AND source_chain_canonical = ?
      AND destination_chain_canonical = ?
      AND UPPER(COALESCE(source_asset_symbol, '')) = ?
      AND UPPER(COALESCE(destination_asset_symbol, '')) = ?
  `;

  if (config.startAtIso) {
    sql += `\n  AND COALESCE(event_at, created_at, updated_at) >= ?`;
    params.push(config.startAtIso);
  }
  if (config.endAtIso) {
    sql += `\n  AND COALESCE(event_at, created_at, updated_at) <= ?`;
    params.push(config.endAtIso);
  }

  sql += `\nORDER BY event_time_iso ASC`;
  if (config.limit !== null) {
    sql += `\nLIMIT ${config.limit}`;
  }

  return queryPrepared<SwapCandidateRow>(connection, sql, params);
}

async function run(): Promise<void> {
  const config = parseArgs(Bun.argv.slice(2));
  const outputPathKey = [
    sanitizeSegment(config.sourceChainCanonical),
    sanitizeSegment(config.sourceTokenSymbol),
    "to",
    sanitizeSegment(config.destinationChainCanonical),
    sanitizeSegment(config.destinationTokenSymbol),
  ].join("__");

  const outputDir = join(OUTPUT_ROOT_DIR, config.provider, outputPathKey);
  await mkdir(outputDir, { recursive: true });
  await mkdir(dirname(PRICE_CACHE_DB_PATH), { recursive: true });

  const providerDb = await openProviderDatabase(config.provider);
  const cache = new PriceCacheStore(PRICE_CACHE_DB_PATH);
  const runtimePriceCache = new Map<string, number | null>();

  console.log(
    JSON.stringify(
      {
        type: "implementation_shortfall_start",
        provider: config.provider,
        sourceChainCanonical: config.sourceChainCanonical,
        sourceTokenSymbol: config.sourceTokenSymbol,
        destinationChainCanonical: config.destinationChainCanonical,
        destinationTokenSymbol: config.destinationTokenSymbol,
        startAtIso: config.startAtIso,
        endAtIso: config.endAtIso,
        limit: config.limit,
        histogramBucketBps: config.histogramBucketBps,
        minNotionalUsd: config.minNotionalUsd,
        maxNotionalUsd: config.maxNotionalUsd,
        minNotionalInclusive: config.minNotionalInclusive,
        maxNotionalInclusive: config.maxNotionalInclusive,
        stablecoinPegMode: config.stablecoinPegMode,
      },
      null,
      2,
    ),
  );

  const stats: AnalysisStats = {
    matchedTrades: 0,
    pricedTrades: 0,
    skippedInvalidTimestamp: 0,
    skippedMissingAmount: 0,
    skippedNonPositiveAmount: 0,
    skippedMissingPrice: 0,
    skippedOutsideNotionalBand: 0,
  };

  try {
    const candidates = await queryCandidateSwaps(providerDb.connection, config);
    stats.matchedTrades = candidates.length;
    console.log(`[info] matched_trades=${candidates.length}`);

    const outputRows: TradeShortfallRow[] = [];
    for (let index = 0; index < candidates.length; index += 1) {
      const row = candidates[index];
      if (!row) {
        continue;
      }
      const sourceSymbol = normalizeSymbol(row.source_asset_symbol ?? config.sourceTokenSymbol);
      const destinationSymbol = normalizeSymbol(
        row.destination_asset_symbol ?? config.destinationTokenSymbol,
      );
      const amountIn = resolveNormalizedAmount({
        normalized: row.amount_in_normalized,
        atomic: row.amount_in_atomic,
        explicitDecimals: row.source_asset_decimals,
        provider: config.provider,
        symbol: sourceSymbol,
      });
      const amountOut = resolveNormalizedAmount({
        normalized: row.amount_out_normalized,
        atomic: row.amount_out_atomic,
        explicitDecimals: row.destination_asset_decimals,
        provider: config.provider,
        symbol: destinationSymbol,
      });
      if (amountIn === null || amountOut === null) {
        stats.skippedMissingAmount += 1;
        continue;
      }
      if (amountIn <= 0 || amountOut <= 0) {
        stats.skippedNonPositiveAmount += 1;
        continue;
      }

      const eventEpochSeconds = toNullableNumber(row.event_epoch_seconds);
      if (eventEpochSeconds === null) {
        stats.skippedInvalidTimestamp += 1;
        continue;
      }
      const minuteTs = toUnixMinuteFromEpochSeconds(eventEpochSeconds);
      if (minuteTs === null) {
        stats.skippedInvalidTimestamp += 1;
        continue;
      }

      const [sourcePriceUsd, destinationPriceUsd] = await Promise.all([
        getUsdPriceAtMinute(
          sourceSymbol,
          minuteTs,
          cache,
          runtimePriceCache,
          config.stablecoinPegMode,
        ),
        getUsdPriceAtMinute(
          destinationSymbol,
          minuteTs,
          cache,
          runtimePriceCache,
          config.stablecoinPegMode,
        ),
      ]);
      if (sourcePriceUsd === null || destinationPriceUsd === null) {
        stats.skippedMissingPrice += 1;
        continue;
      }

      const inputUsd = amountIn * sourcePriceUsd;
      const outputUsd = amountOut * destinationPriceUsd;
      if (!Number.isFinite(inputUsd) || !Number.isFinite(outputUsd) || inputUsd <= 0) {
        stats.skippedNonPositiveAmount += 1;
        continue;
      }
      if (
        config.minNotionalUsd !== null &&
        (config.minNotionalInclusive ? inputUsd < config.minNotionalUsd : inputUsd <= config.minNotionalUsd)
      ) {
        stats.skippedOutsideNotionalBand += 1;
        continue;
      }
      if (
        config.maxNotionalUsd !== null &&
        (config.maxNotionalInclusive ? inputUsd > config.maxNotionalUsd : inputUsd >= config.maxNotionalUsd)
      ) {
        stats.skippedOutsideNotionalBand += 1;
        continue;
      }

      // Positive bps means worse execution for the user. Negative bps means better.
      const implementationShortfallBps = ((inputUsd - outputUsd) / inputUsd) * 10_000;
      outputRows.push({
        normalizedId: row.normalized_id,
        providerRecordId: row.provider_record_id,
        eventAt: new Date(eventEpochSeconds * 1_000).toISOString(),
        eventMinuteTs: minuteTs,
        sourceChainCanonical: row.source_chain_canonical,
        destinationChainCanonical: row.destination_chain_canonical,
        sourceTokenSymbol: sourceSymbol,
        destinationTokenSymbol: destinationSymbol,
        amountIn: roundTo(amountIn, 12),
        amountOut: roundTo(amountOut, 12),
        sourcePriceUsd: roundTo(sourcePriceUsd, 8),
        destinationPriceUsd: roundTo(destinationPriceUsd, 8),
        inputUsd: roundTo(inputUsd, 8),
        outputUsd: roundTo(outputUsd, 8),
        implementationShortfallBps: roundTo(implementationShortfallBps, 6),
      });
      stats.pricedTrades += 1;

      if ((index + 1) % 250 === 0) {
        console.log(
          `[progress] provider=${config.provider} processed=${index + 1}/${candidates.length} priced=${stats.pricedTrades}`,
        );
      }
    }

    const bpsValues = outputRows.map((row) => row.implementationShortfallBps).sort((a, b) => a - b);
    const histogram = buildHistogram(bpsValues, config.histogramBucketBps);
    const bpsMin = bpsValues[0] ?? null;
    const bpsMax = bpsValues.length === 0 ? null : (bpsValues[bpsValues.length - 1] ?? null);
    const bpsMeanRaw = mean(bpsValues);
    const bpsMean = bpsMeanRaw === null ? null : roundTo(bpsMeanRaw, 6);
    const bpsMedianRaw = percentile(bpsValues, 0.5);
    const bpsMedian = bpsMedianRaw === null ? null : roundTo(bpsMedianRaw, 6);
    const bpsStdDevRaw = sampleStdDev(bpsValues);
    const bpsStdDev = bpsStdDevRaw === null ? null : roundTo(bpsStdDevRaw, 6);
    const bpsP10Raw = percentile(bpsValues, 0.1);
    const bpsP25Raw = percentile(bpsValues, 0.25);
    const bpsP50Raw = percentile(bpsValues, 0.5);
    const bpsP75Raw = percentile(bpsValues, 0.75);
    const bpsP90Raw = percentile(bpsValues, 0.9);
    const bpsP95Raw = percentile(bpsValues, 0.95);
    const bpsP99Raw = percentile(bpsValues, 0.99);
    const bpsIqrRaw =
      bpsP25Raw === null || bpsP75Raw === null ? null : bpsP75Raw - bpsP25Raw;
    const bpsMeanAbsRaw = mean(bpsValues.map((value) => Math.abs(value)));

    const betterTrades = outputRows.filter((row) => row.implementationShortfallBps < 0).length;
    const worseTrades = outputRows.filter((row) => row.implementationShortfallBps > 0).length;
    const flatTrades = outputRows.length - betterTrades - worseTrades;

    const pricedMatchRatePct =
      stats.matchedTrades === 0 ? null : roundTo((stats.pricedTrades / stats.matchedTrades) * 100, 4);
    const betterRatePct =
      stats.pricedTrades === 0 ? null : roundTo((betterTrades / stats.pricedTrades) * 100, 4);
    const worseRatePct =
      stats.pricedTrades === 0 ? null : roundTo((worseTrades / stats.pricedTrades) * 100, 4);
    const flatRatePct =
      stats.pricedTrades === 0 ? null : roundTo((flatTrades / stats.pricedTrades) * 100, 4);

    const inputUsdValues = outputRows.map((row) => row.inputUsd).sort((a, b) => a - b);
    const outputUsdValues = outputRows.map((row) => row.outputUsd).sort((a, b) => a - b);
    const usdDeltaValues = outputRows
      .map((row) => row.inputUsd - row.outputUsd)
      .sort((a, b) => a - b);

    const totalInputUsd = sum(inputUsdValues);
    const totalOutputUsd = sum(outputUsdValues);
    const netUsdDelta = totalInputUsd - totalOutputUsd;
    const weightedShortfallBpsRaw =
      totalInputUsd <= 0 ? null : ((totalInputUsd - totalOutputUsd) / totalInputUsd) * 10_000;
    const meanInputUsdRaw = mean(inputUsdValues);
    const medianInputUsdRaw = percentile(inputUsdValues, 0.5);
    const meanOutputUsdRaw = mean(outputUsdValues);
    const medianOutputUsdRaw = percentile(outputUsdValues, 0.5);
    const meanUsdDeltaRaw = mean(usdDeltaValues);
    const medianUsdDeltaRaw = percentile(usdDeltaValues, 0.5);

    const summary = {
      generatedAt: new Date().toISOString(),
      provider: config.provider,
      tokenPath: {
        sourceChainCanonical: config.sourceChainCanonical,
        sourceTokenSymbol: normalizeSymbol(config.sourceTokenSymbol),
        destinationChainCanonical: config.destinationChainCanonical,
        destinationTokenSymbol: normalizeSymbol(config.destinationTokenSymbol),
      },
      window: {
        startAtIso: config.startAtIso,
        endAtIso: config.endAtIso,
      },
      notionalUsdFilter: {
        min: config.minNotionalUsd,
        max: config.maxNotionalUsd,
        minInclusive: config.minNotionalInclusive,
        maxInclusive: config.maxNotionalInclusive,
        minExclusive:
          config.minNotionalUsd !== null && !config.minNotionalInclusive
            ? config.minNotionalUsd
            : null,
        maxExclusive:
          config.maxNotionalUsd !== null && !config.maxNotionalInclusive
            ? config.maxNotionalUsd
            : null,
        minInclusiveValue:
          config.minNotionalUsd !== null && config.minNotionalInclusive
            ? config.minNotionalUsd
            : null,
        maxInclusiveValue:
          config.maxNotionalUsd !== null && config.maxNotionalInclusive
            ? config.maxNotionalUsd
            : null,
      },
      signConvention: {
        positiveBps: "output USD < input USD (worse for user)",
        negativeBps: "output USD > input USD (better for user)",
      },
      totals: {
        ...stats,
        pricedMatchRatePct,
      },
      outcomeSummary: {
        betterTrades,
        worseTrades,
        flatTrades,
        betterRatePct,
        worseRatePct,
        flatRatePct,
      },
      bpsSummary: {
        count: bpsValues.length,
        mean: bpsMean,
        median: bpsMedian,
        stdDev: bpsStdDev,
        meanAbs: bpsMeanAbsRaw === null ? null : roundTo(bpsMeanAbsRaw, 6),
        min: bpsMin === null ? null : roundTo(bpsMin, 6),
        p10: bpsP10Raw === null ? null : roundTo(bpsP10Raw, 6),
        p25: bpsP25Raw === null ? null : roundTo(bpsP25Raw, 6),
        p50: bpsP50Raw === null ? null : roundTo(bpsP50Raw, 6),
        p75: bpsP75Raw === null ? null : roundTo(bpsP75Raw, 6),
        p90: bpsP90Raw === null ? null : roundTo(bpsP90Raw, 6),
        p95: bpsP95Raw === null ? null : roundTo(bpsP95Raw, 6),
        p99: bpsP99Raw === null ? null : roundTo(bpsP99Raw, 6),
        iqr: bpsIqrRaw === null ? null : roundTo(bpsIqrRaw, 6),
        max: bpsMax === null ? null : roundTo(bpsMax, 6),
      },
      usdSummary: {
        totalInputUsd: roundTo(totalInputUsd, 8),
        totalOutputUsd: roundTo(totalOutputUsd, 8),
        netUsdDelta: roundTo(netUsdDelta, 8),
        weightedShortfallBps:
          weightedShortfallBpsRaw === null ? null : roundTo(weightedShortfallBpsRaw, 6),
        meanInputUsd: meanInputUsdRaw === null ? null : roundTo(meanInputUsdRaw, 8),
        medianInputUsd: medianInputUsdRaw === null ? null : roundTo(medianInputUsdRaw, 8),
        meanOutputUsd: meanOutputUsdRaw === null ? null : roundTo(meanOutputUsdRaw, 8),
        medianOutputUsd: medianOutputUsdRaw === null ? null : roundTo(medianOutputUsdRaw, 8),
        meanUsdDelta: meanUsdDeltaRaw === null ? null : roundTo(meanUsdDeltaRaw, 8),
        medianUsdDelta: medianUsdDeltaRaw === null ? null : roundTo(medianUsdDeltaRaw, 8),
      },
      histogramBucketBps: config.histogramBucketBps,
      distribution: histogram,
      output: {
        directory: outputDir,
        tradesCsv: join(outputDir, "trades.csv"),
        distributionCsv: join(outputDir, "distribution.csv"),
        summaryJson: join(outputDir, "summary.json"),
      },
      pricingSource: {
        venue: "coinbase",
        endpoint: "GET /products/{product_id}/candles",
        granularitySeconds: 60,
        priceFieldUsed: "close",
        cacheDbPath: PRICE_CACHE_DB_PATH,
        stablecoinPegMode: config.stablecoinPegMode,
        stablecoinUsdPegSymbols:
          config.stablecoinPegMode === "usd_1" ? [...STABLECOIN_USD_PEG_SYMBOLS].sort() : [],
      },
    };

    const tradesCsvLines = [
      [
        "normalized_id",
        "provider_record_id",
        "event_at",
        "event_minute_ts",
        "source_chain_canonical",
        "destination_chain_canonical",
        "source_token_symbol",
        "destination_token_symbol",
        "amount_in",
        "amount_out",
        "source_price_usd",
        "destination_price_usd",
        "input_usd",
        "output_usd",
        "implementation_shortfall_bps",
      ].join(","),
      ...outputRows.map((row) =>
        [
          csvEscape(row.normalizedId),
          csvEscape(row.providerRecordId),
          csvEscape(row.eventAt),
          csvEscape(row.eventMinuteTs),
          csvEscape(row.sourceChainCanonical),
          csvEscape(row.destinationChainCanonical),
          csvEscape(row.sourceTokenSymbol),
          csvEscape(row.destinationTokenSymbol),
          csvEscape(row.amountIn),
          csvEscape(row.amountOut),
          csvEscape(row.sourcePriceUsd),
          csvEscape(row.destinationPriceUsd),
          csvEscape(row.inputUsd),
          csvEscape(row.outputUsd),
          csvEscape(row.implementationShortfallBps),
        ].join(","),
      ),
    ];

    const distributionCsvLines = [
      "bucket_start_bps,bucket_end_bps,count",
      ...histogram.map((bucket) =>
        [bucket.bucketStartBps, bucket.bucketEndBps, bucket.count].map(csvEscape).join(","),
      ),
    ];

    await Promise.all([
      writeFile(join(outputDir, "trades.csv"), `${tradesCsvLines.join("\n")}\n`, "utf8"),
      writeFile(join(outputDir, "distribution.csv"), `${distributionCsvLines.join("\n")}\n`, "utf8"),
      writeFile(join(outputDir, "summary.json"), `${JSON.stringify(summary, null, 2)}\n`, "utf8"),
    ]);

    console.log(JSON.stringify({ type: "implementation_shortfall_done", summary }, null, 2));
  } finally {
    providerDb.close();
    cache.close();
  }
}

run().catch((error) => {
  if (error instanceof Error) {
    console.error(`Error: ${error.message}`);
    if (error.stack) {
      console.error(error.stack);
    }
  } else {
    console.error(`Error: ${String(error)}`);
  }
  process.exit(1);
});
