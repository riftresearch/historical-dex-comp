import { extname, join, normalize, resolve, sep } from "node:path";
import { PROVIDER_KEYS, type ProviderKey } from "../domain/provider-key";
import { queryPrepared } from "../storage/duckdb-utils";
import { openProviderDatabase, providerDatabaseExists } from "../storage/provider-db";

export interface DashboardServerOptions {
  host: string;
  port: number;
  days: number;
}

export interface DashboardServerHandle {
  server: Bun.Server<unknown>;
  finished: Promise<never>;
}

interface ProviderTotalsRow {
  swaps_total: number | bigint | null;
  success_swaps: number | bigint | null;
  failed_swaps: number | bigint | null;
  volume_usd: number | bigint | null;
  oldest_event_at: string | Date | null;
  newest_event_at: string | Date | null;
}

interface ProviderDailyRow {
  day: string | Date;
  swaps_total: number | bigint;
  volume_usd: number | bigint | null;
}

interface ProviderStatusRow {
  status_canonical: string | null;
  swaps_total: number | bigint;
}

interface ProviderDailyPoint {
  day: string;
  swapsTotal: number;
  volumeUsd: number;
}

interface ProviderStatusPoint {
  status: string;
  swapsTotal: number;
}

interface ProviderAnalyticsSnapshot {
  providerKey: ProviderKey;
  databaseExists: boolean;
  swapsTotal: number;
  successSwaps: number;
  failedSwaps: number;
  volumeUsd: number;
  successRate: number;
  oldestEventAt: string | null;
  newestEventAt: string | null;
  daily: ProviderDailyPoint[];
  statuses: ProviderStatusPoint[];
  error: string | null;
}

interface DashboardSnapshot {
  generatedAt: string;
  days: number;
  since: string;
  totals: {
    swapsTotal: number;
    successSwaps: number;
    failedSwaps: number;
    volumeUsd: number;
    successRate: number;
    activeProviders: number;
  };
  timeline: string[];
  providers: ProviderAnalyticsSnapshot[];
  series: {
    swapsByProvider: Record<ProviderKey, number[]>;
    volumeByProvider: Record<ProviderKey, number[]>;
  };
}

interface ShortfallTokenPath {
  sourceChainCanonical: string;
  sourceTokenSymbol: string;
  destinationChainCanonical: string;
  destinationTokenSymbol: string;
}

interface ShortfallTradePoint {
  inputUsd: number;
  implementationShortfallBps: number;
}

interface ShortfallNotionalBucket {
  bucketIndex: number;
  lowerUsd: number;
  upperUsd: number;
  centerUsd: number;
  label: string;
  sampleCount: number;
}

interface ShortfallBucketQuantiles {
  bucketIndex: number;
  count: number;
  q05: number | null;
  q25: number | null;
  q50: number | null;
  q75: number | null;
  q95: number | null;
}

interface ShortfallProviderSnapshot {
  providerKey: ProviderKey;
  summaryPath: string;
  tradesPath: string;
  found: boolean;
  generatedAt: string | null;
  analyzedRangeStartAt: string | null;
  analyzedRangeEndAt: string | null;
  matchedTrades: number;
  pricedTrades: number;
  pricedMatchRatePct: number | null;
  analyzedVolumeUsd: number;
  buckets: ShortfallBucketQuantiles[];
  error: string | null;
}

interface ShortfallBucketConfig {
  axisScale: "log10";
  stepLog10: number;
  minSampleCount: number;
  quantileMethod: "nearest_rank";
  smoothing: "none";
  interpolation: "none";
}

interface ShortfallNotionalFilterConfig {
  min: number | null;
  max: number | null;
  minInclusive: boolean;
  maxInclusive: boolean;
}

interface ShortfallNotionalFilter extends ShortfallNotionalFilterConfig {
  minExclusive: number | null;
  maxExclusive: number | null;
  minInclusiveValue: number | null;
  maxInclusiveValue: number | null;
}

interface ShortfallViewConfig {
  id: string;
  label: string;
  days: number;
  tokenPath: ShortfallTokenPath;
  notionalUsdFilter: ShortfallNotionalFilterConfig;
  providers?: ProviderKey[];
}

interface ShortfallSnapshot {
  generatedAt: string;
  viewId: string;
  viewLabel: string;
  days: number;
  tokenPath: ShortfallTokenPath;
  notionalUsdFilter: ShortfallNotionalFilter;
  bucketConfig: ShortfallBucketConfig;
  buckets: ShortfallNotionalBucket[];
  providers: ShortfallProviderSnapshot[];
}

const MIME_BY_EXT: Record<string, string> = {
  ".css": "text/css; charset=utf-8",
  ".html": "text/html; charset=utf-8",
  ".ico": "image/x-icon",
  ".js": "application/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
};

const BASE_CTE_SQL = `
WITH base AS (
  SELECT
    COALESCE(event_at, created_at, updated_at) AS event_time,
    status_canonical,
    COALESCE(amount_in_usd, amount_out_usd, 0) AS volume_usd
  FROM swaps_core
  WHERE COALESCE(event_at, created_at, updated_at) IS NOT NULL
    AND COALESCE(event_at, created_at, updated_at) >= ?
)
`;

const TOTALS_SQL = `
${BASE_CTE_SQL}
SELECT
  COUNT(*) AS swaps_total,
  SUM(CASE WHEN status_canonical = 'success' THEN 1 ELSE 0 END) AS success_swaps,
  SUM(CASE WHEN status_canonical = 'failed' THEN 1 ELSE 0 END) AS failed_swaps,
  SUM(volume_usd) AS volume_usd,
  MIN(event_time) AS oldest_event_at,
  MAX(event_time) AS newest_event_at
FROM base
`;

const DAILY_SQL = `
${BASE_CTE_SQL}
SELECT
  DATE_TRUNC('day', event_time) AS day,
  COUNT(*) AS swaps_total,
  SUM(volume_usd) AS volume_usd
FROM base
GROUP BY 1
ORDER BY 1
`;

const STATUS_SQL = `
${BASE_CTE_SQL}
SELECT
  status_canonical,
  COUNT(*) AS swaps_total
FROM base
GROUP BY 1
ORDER BY swaps_total DESC
`;

const AGGREGATION_WINDOW_DAYS = 30;
const SHORTFALL_BUCKET_STEP_LOG10 = 0.25;
const SHORTFALL_BUCKET_COARSE_STEP_LOG10 = 0.5;
const SHORTFALL_BUCKET_DENSE_STEP_LOG10 = 0.25;
const SHORTFALL_BUCKET_DENSE_MIN_USD = 1_000;
const SHORTFALL_BUCKET_DENSE_MAX_USD = 100_000;
const SHORTFALL_MIN_SAMPLE_COUNT = 10;
const USD_BUCKET_COMPACT_FORMATTER = new Intl.NumberFormat("en-US", {
  notation: "compact",
  maximumFractionDigits: 1,
});
const DEFAULT_SHORTFALL_VIEW_ID = "usdc_eth_to_btc_bitcoin_all";
const CBBTC_PATH_PROVIDERS: ProviderKey[] = ["kyberswap", "lifi", "relay"];
const SHORTFALL_VIEW_CONFIGS: Record<string, ShortfallViewConfig> = {
  usdc_eth_to_btc_bitcoin_all: {
    id: "usdc_eth_to_btc_bitcoin_all",
    label: "USDC/ethereum -> BTC/bitcoin",
    days: AGGREGATION_WINDOW_DAYS,
    tokenPath: {
      sourceChainCanonical: "eip155:1",
      sourceTokenSymbol: "USDC",
      destinationChainCanonical: "bitcoin:mainnet",
      destinationTokenSymbol: "BTC",
    },
    notionalUsdFilter: {
      min: null,
      max: null,
      minInclusive: false,
      maxInclusive: false,
    },
  },
  usdt_eth_to_btc_bitcoin_all: {
    id: "usdt_eth_to_btc_bitcoin_all",
    label: "USDT/ethereum -> BTC/bitcoin",
    days: AGGREGATION_WINDOW_DAYS,
    tokenPath: {
      sourceChainCanonical: "eip155:1",
      sourceTokenSymbol: "USDT",
      destinationChainCanonical: "bitcoin:mainnet",
      destinationTokenSymbol: "BTC",
    },
    notionalUsdFilter: {
      min: null,
      max: null,
      minInclusive: false,
      maxInclusive: false,
    },
  },
  eth_eth_to_btc_bitcoin_all: {
    id: "eth_eth_to_btc_bitcoin_all",
    label: "ETH/ethereum -> BTC/bitcoin",
    days: AGGREGATION_WINDOW_DAYS,
    tokenPath: {
      sourceChainCanonical: "eip155:1",
      sourceTokenSymbol: "ETH",
      destinationChainCanonical: "bitcoin:mainnet",
      destinationTokenSymbol: "BTC",
    },
    notionalUsdFilter: {
      min: null,
      max: null,
      minInclusive: false,
      maxInclusive: false,
    },
  },
  btc_bitcoin_to_usdc_eth_all: {
    id: "btc_bitcoin_to_usdc_eth_all",
    label: "BTC/bitcoin -> USDC/ethereum",
    days: AGGREGATION_WINDOW_DAYS,
    tokenPath: {
      sourceChainCanonical: "bitcoin:mainnet",
      sourceTokenSymbol: "BTC",
      destinationChainCanonical: "eip155:1",
      destinationTokenSymbol: "USDC",
    },
    notionalUsdFilter: {
      min: null,
      max: null,
      minInclusive: false,
      maxInclusive: false,
    },
  },
  btc_bitcoin_to_usdt_eth_all: {
    id: "btc_bitcoin_to_usdt_eth_all",
    label: "BTC/bitcoin -> USDT/ethereum",
    days: AGGREGATION_WINDOW_DAYS,
    tokenPath: {
      sourceChainCanonical: "bitcoin:mainnet",
      sourceTokenSymbol: "BTC",
      destinationChainCanonical: "eip155:1",
      destinationTokenSymbol: "USDT",
    },
    notionalUsdFilter: {
      min: null,
      max: null,
      minInclusive: false,
      maxInclusive: false,
    },
  },
  btc_bitcoin_to_eth_eth_all: {
    id: "btc_bitcoin_to_eth_eth_all",
    label: "BTC/bitcoin -> ETH/ethereum",
    days: AGGREGATION_WINDOW_DAYS,
    tokenPath: {
      sourceChainCanonical: "bitcoin:mainnet",
      sourceTokenSymbol: "BTC",
      destinationChainCanonical: "eip155:1",
      destinationTokenSymbol: "ETH",
    },
    notionalUsdFilter: {
      min: null,
      max: null,
      minInclusive: false,
      maxInclusive: false,
    },
  },
  usdc_eth_to_cbbtc_eth_all_kyberswap: {
    id: "usdc_eth_to_cbbtc_eth_all_kyberswap",
    label: "USDC/ethereum -> CBBTC/ethereum (KyberSwap+LI.FI+Relay)",
    days: AGGREGATION_WINDOW_DAYS,
    tokenPath: {
      sourceChainCanonical: "eip155:1",
      sourceTokenSymbol: "USDC",
      destinationChainCanonical: "eip155:1",
      destinationTokenSymbol: "CBBTC",
    },
    notionalUsdFilter: {
      min: null,
      max: null,
      minInclusive: false,
      maxInclusive: false,
    },
    providers: CBBTC_PATH_PROVIDERS,
  },
  eth_eth_to_cbbtc_eth_all_kyberswap: {
    id: "eth_eth_to_cbbtc_eth_all_kyberswap",
    label: "ETH/ethereum -> CBBTC/ethereum (KyberSwap+LI.FI+Relay)",
    days: AGGREGATION_WINDOW_DAYS,
    tokenPath: {
      sourceChainCanonical: "eip155:1",
      sourceTokenSymbol: "ETH",
      destinationChainCanonical: "eip155:1",
      destinationTokenSymbol: "CBBTC",
    },
    notionalUsdFilter: {
      min: null,
      max: null,
      minInclusive: false,
      maxInclusive: false,
    },
    providers: CBBTC_PATH_PROVIDERS,
  },
  usdt_eth_to_cbbtc_eth_all_kyberswap: {
    id: "usdt_eth_to_cbbtc_eth_all_kyberswap",
    label: "USDT/ethereum -> CBBTC/ethereum (KyberSwap+LI.FI+Relay)",
    days: AGGREGATION_WINDOW_DAYS,
    tokenPath: {
      sourceChainCanonical: "eip155:1",
      sourceTokenSymbol: "USDT",
      destinationChainCanonical: "eip155:1",
      destinationTokenSymbol: "CBBTC",
    },
    notionalUsdFilter: {
      min: null,
      max: null,
      minInclusive: false,
      maxInclusive: false,
    },
    providers: CBBTC_PATH_PROVIDERS,
  },
  weth_eth_to_cbbtc_eth_all_kyberswap: {
    id: "weth_eth_to_cbbtc_eth_all_kyberswap",
    label: "WETH/ethereum -> CBBTC/ethereum (KyberSwap+LI.FI+Relay)",
    days: AGGREGATION_WINDOW_DAYS,
    tokenPath: {
      sourceChainCanonical: "eip155:1",
      sourceTokenSymbol: "WETH",
      destinationChainCanonical: "eip155:1",
      destinationTokenSymbol: "CBBTC",
    },
    notionalUsdFilter: {
      min: null,
      max: null,
      minInclusive: false,
      maxInclusive: false,
    },
    providers: CBBTC_PATH_PROVIDERS,
  },
};

function toNumber(value: number | bigint | null | undefined): number {
  if (value === null || value === undefined) {
    return 0;
  }
  return Number(value);
}

function toOptionalNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number(value.trim());
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return null;
}

function toIsoDateTime(value: string | Date | null | undefined): string | null {
  if (!value) {
    return null;
  }
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) {
    return null;
  }
  return date.toISOString();
}

function toIsoDay(value: string | Date): string {
  const date = value instanceof Date ? value : new Date(value);
  return date.toISOString().slice(0, 10);
}

function roundToTwo(value: number): number {
  return Number(value.toFixed(2));
}

function parseDays(rawValue: string | null, fallback: number): number {
  if (!rawValue) {
    return fallback;
  }
  const parsed = Number(rawValue);
  if (!Number.isFinite(parsed) || parsed <= 0 || !Number.isInteger(parsed)) {
    return fallback;
  }
  return Math.min(parsed, 365);
}

function resolveShortfallView(rawValue: string | null): ShortfallViewConfig {
  if (rawValue) {
    const normalized = rawValue.trim();
    if (normalized && SHORTFALL_VIEW_CONFIGS[normalized]) {
      return SHORTFALL_VIEW_CONFIGS[normalized];
    }
  }
  const fallback = SHORTFALL_VIEW_CONFIGS[DEFAULT_SHORTFALL_VIEW_ID];
  if (fallback) {
    return fallback;
  }
  const first = Object.values(SHORTFALL_VIEW_CONFIGS)[0];
  if (!first) {
    throw new Error("No shortfall views configured.");
  }
  return first;
}

function toNotionalSnapshot(filter: ShortfallNotionalFilterConfig): ShortfallNotionalFilter {
  return {
    min: filter.min,
    max: filter.max,
    minInclusive: filter.minInclusive,
    maxInclusive: filter.maxInclusive,
    minExclusive: filter.min !== null && !filter.minInclusive ? filter.min : null,
    maxExclusive: filter.max !== null && !filter.maxInclusive ? filter.max : null,
    minInclusiveValue: filter.min !== null && filter.minInclusive ? filter.min : null,
    maxInclusiveValue: filter.max !== null && filter.maxInclusive ? filter.max : null,
  };
}

function sanitizeSegment(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9._-]+/g, "_").replace(/^_+|_+$/g, "");
}

function buildShortfallPathKey(path: ShortfallTokenPath): string {
  return [
    sanitizeSegment(path.sourceChainCanonical),
    sanitizeSegment(path.sourceTokenSymbol),
    "to",
    sanitizeSegment(path.destinationChainCanonical),
    sanitizeSegment(path.destinationTokenSymbol),
  ].join("__");
}

interface ShortfallProviderSourceData {
  providerKey: ProviderKey;
  summaryPath: string;
  tradesPath: string;
  found: boolean;
  generatedAt: string | null;
  analyzedRangeStartAt: string | null;
  analyzedRangeEndAt: string | null;
  matchedTrades: number;
  pricedTrades: number;
  pricedMatchRatePct: number | null;
  analyzedVolumeUsd: number;
  trades: ShortfallTradePoint[];
  error: string | null;
}

function parseCsvLine(line: string): string[] {
  const values: string[] = [];
  let current = "";
  let inQuotes = false;
  for (let index = 0; index < line.length; index += 1) {
    const char = line[index];
    if (char === "\"") {
      if (inQuotes && line[index + 1] === "\"") {
        current += "\"";
        index += 1;
        continue;
      }
      inQuotes = !inQuotes;
      continue;
    }
    if (char === "," && !inQuotes) {
      values.push(current);
      current = "";
      continue;
    }
    current += char;
  }
  values.push(current);
  return values;
}

function matchesNotionalFilter(
  inputUsd: number,
  notionalFilter: ShortfallNotionalFilterConfig,
): boolean {
  if (!Number.isFinite(inputUsd) || inputUsd <= 0) {
    return false;
  }
  const { min, max, minInclusive, maxInclusive } = notionalFilter;
  if (min !== null) {
    if (minInclusive ? inputUsd < min : inputUsd <= min) {
      return false;
    }
  }
  if (max !== null) {
    if (maxInclusive ? inputUsd > max : inputUsd >= max) {
      return false;
    }
  }
  return true;
}

function parseShortfallTradePoints(
  csvText: string,
  notionalFilter: ShortfallNotionalFilterConfig,
): ShortfallTradePoint[] {
  const lines = csvText
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
  if (lines.length <= 1) {
    return [];
  }

  const headerLine = lines[0];
  if (!headerLine) {
    return [];
  }
  const header = parseCsvLine(headerLine.replace(/^\uFEFF/, ""));
  const inputUsdIndex = header.indexOf("input_usd");
  const shortfallIndex = header.indexOf("implementation_shortfall_bps");
  if (inputUsdIndex < 0 || shortfallIndex < 0) {
    throw new Error("trades.csv is missing required columns: input_usd, implementation_shortfall_bps");
  }

  const trades: ShortfallTradePoint[] = [];
  for (const rawLine of lines.slice(1)) {
    const fields = parseCsvLine(rawLine);
    const inputUsd = toOptionalNumber(fields[inputUsdIndex]);
    const implementationShortfallBps = toOptionalNumber(fields[shortfallIndex]);
    if (inputUsd === null || implementationShortfallBps === null) {
      continue;
    }
    if (!matchesNotionalFilter(inputUsd, notionalFilter)) {
      continue;
    }
    trades.push({
      inputUsd,
      implementationShortfallBps,
    });
  }
  return trades;
}

function formatUsdBucketValue(value: number): string {
  if (value >= 1_000) {
    return USD_BUCKET_COMPACT_FORMATTER.format(value).replace(/[A-Z]$/, (match) =>
      match.toLowerCase(),
    );
  }
  if (value >= 100) {
    return Math.round(value).toString();
  }
  if (value >= 10) {
    return value.toFixed(1);
  }
  return value.toFixed(2);
}

function roundLogEdge(value: number): number {
  return Number(value.toFixed(10));
}

function buildNotionalBuckets(inputUsdValues: number[], stepLog10: number): ShortfallNotionalBucket[] {
  const values = inputUsdValues.filter((value) => Number.isFinite(value) && value > 0);
  if (values.length === 0) {
    return [];
  }

  const denseStepLog10 = Math.min(stepLog10, SHORTFALL_BUCKET_DENSE_STEP_LOG10);
  const coarseStepLog10 = Math.max(stepLog10, SHORTFALL_BUCKET_COARSE_STEP_LOG10);
  const minUsd = Math.min(...values);
  const maxUsd = Math.max(...values);
  const minLog = Math.floor(Math.log10(minUsd) / coarseStepLog10) * coarseStepLog10;
  let maxLog = Math.ceil(Math.log10(maxUsd) / coarseStepLog10) * coarseStepLog10;
  if (maxLog <= minLog) {
    maxLog = minLog + coarseStepLog10;
  }

  const denseMinLog = Math.log10(SHORTFALL_BUCKET_DENSE_MIN_USD);
  const denseMaxLog = Math.log10(SHORTFALL_BUCKET_DENSE_MAX_USD);
  const segmentSpecs = [
    {
      startLog: Number.NEGATIVE_INFINITY,
      endLog: denseMinLog,
      stepLog10: coarseStepLog10,
    },
    {
      startLog: denseMinLog,
      endLog: denseMaxLog,
      stepLog10: denseStepLog10,
    },
    {
      startLog: denseMaxLog,
      endLog: Number.POSITIVE_INFINITY,
      stepLog10: coarseStepLog10,
    },
  ];

  const edgeSet = new Set<number>();
  const addEdge = (value: number): void => {
    if (!Number.isFinite(value)) {
      return;
    }
    edgeSet.add(roundLogEdge(value));
  };

  addEdge(minLog);
  addEdge(maxLog);

  for (const segment of segmentSpecs) {
    const overlapStart = Math.max(minLog, segment.startLog);
    const overlapEnd = Math.min(maxLog, segment.endLog);
    if (overlapEnd <= overlapStart) {
      continue;
    }
    addEdge(overlapStart);
    addEdge(overlapEnd);

    let cursor =
      Math.ceil((overlapStart + 1e-10) / segment.stepLog10) * segment.stepLog10;
    while (cursor < overlapEnd - 1e-10) {
      addEdge(cursor);
      cursor += segment.stepLog10;
    }
  }

  const sortedEdges = [...edgeSet].sort((left, right) => left - right);
  if (sortedEdges.length < 2) {
    return [];
  }

  const buckets: ShortfallNotionalBucket[] = [];
  for (let edgeIndex = 0; edgeIndex < sortedEdges.length - 1; edgeIndex += 1) {
    const lowerLog = sortedEdges[edgeIndex];
    const upperLog = sortedEdges[edgeIndex + 1];
    if (lowerLog === undefined || upperLog === undefined) {
      continue;
    }
    if (
      !Number.isFinite(lowerLog) ||
      !Number.isFinite(upperLog) ||
      upperLog - lowerLog <= 1e-10
    ) {
      continue;
    }
    const lowerUsd = Number((10 ** lowerLog).toPrecision(12));
    const upperUsd = Number((10 ** upperLog).toPrecision(12));
    const centerUsd = Number(Math.sqrt(lowerUsd * upperUsd).toPrecision(12));
    buckets.push({
      bucketIndex: buckets.length,
      lowerUsd,
      upperUsd,
      centerUsd,
      label: `$${formatUsdBucketValue(lowerUsd)}-$${formatUsdBucketValue(upperUsd)}`,
      sampleCount: 0,
    });
  }
  return buckets;
}

function findNotionalBucketIndex(inputUsd: number, buckets: ShortfallNotionalBucket[]): number {
  for (let index = 0; index < buckets.length; index += 1) {
    const bucket = buckets[index];
    if (!bucket) {
      continue;
    }
    const isLast = index === buckets.length - 1;
    const inLowerBound = inputUsd >= bucket.lowerUsd;
    const inUpperBound = isLast ? inputUsd <= bucket.upperUsd : inputUsd < bucket.upperUsd;
    if (inLowerBound && inUpperBound) {
      return index;
    }
  }
  return -1;
}

function nearestRankQuantile(sortedValues: number[], quantile: number): number | null {
  if (sortedValues.length === 0) {
    return null;
  }
  const rank = Math.max(1, Math.ceil(quantile * sortedValues.length));
  const index = Math.min(sortedValues.length - 1, rank - 1);
  return sortedValues[index] ?? null;
}

function buildProviderBucketQuantiles(
  trades: ShortfallTradePoint[],
  buckets: ShortfallNotionalBucket[],
  minSampleCount: number,
): ShortfallBucketQuantiles[] {
  if (buckets.length === 0) {
    return [];
  }
  const shortfallValuesByBucket = Array.from({ length: buckets.length }, () => [] as number[]);
  for (const trade of trades) {
    const bucketIndex = findNotionalBucketIndex(trade.inputUsd, buckets);
    if (bucketIndex < 0) {
      continue;
    }
    const values = shortfallValuesByBucket[bucketIndex];
    if (!values) {
      continue;
    }
    values.push(trade.implementationShortfallBps);
  }

  return buckets.map((bucket, bucketIndex) => {
    const values = shortfallValuesByBucket[bucketIndex] ?? [];
    const count = values.length;
    if (count < minSampleCount) {
      return {
        bucketIndex: bucket.bucketIndex,
        count,
        q05: null,
        q25: null,
        q50: null,
        q75: null,
        q95: null,
      };
    }
    values.sort((left, right) => left - right);
    return {
      bucketIndex: bucket.bucketIndex,
      count,
      q05: nearestRankQuantile(values, 0.05),
      q25: nearestRankQuantile(values, 0.25),
      q50: nearestRankQuantile(values, 0.5),
      q75: nearestRankQuantile(values, 0.75),
      q95: nearestRankQuantile(values, 0.95),
    };
  });
}

function buildBucketSampleCounts(
  providerTrades: ShortfallTradePoint[][],
  buckets: ShortfallNotionalBucket[],
): number[] {
  const counts = Array.from({ length: buckets.length }, () => 0);
  for (const trades of providerTrades) {
    for (const trade of trades) {
      const bucketIndex = findNotionalBucketIndex(trade.inputUsd, buckets);
      if (bucketIndex < 0) {
        continue;
      }
      counts[bucketIndex] = (counts[bucketIndex] ?? 0) + 1;
    }
  }
  return counts;
}

async function getShortfallProviderSourceData(
  providerKey: ProviderKey,
  tokenPath: ShortfallTokenPath,
  notionalFilter: ShortfallNotionalFilterConfig,
): Promise<ShortfallProviderSourceData> {
  const providerPath = join(
    process.cwd(),
    "data",
    "analytics",
    "execution-quality",
    providerKey,
    buildShortfallPathKey(tokenPath),
  );
  const summaryPath = join(
    providerPath,
    "summary.json",
  );
  const tradesPath = join(providerPath, "trades.csv");

  let generatedAt: string | null = null;
  let analyzedRangeStartAt: string | null = null;
  let analyzedRangeEndAt: string | null = null;
  let matchedTrades: number | null = null;
  let pricedTrades: number | null = null;
  let pricedMatchRatePctFromSummary: number | null = null;
  let analyzedVolumeUsdFromSummary: number | null = null;
  let summaryError: string | null = null;

  try {
    const summaryFile = Bun.file(summaryPath);
    if (await summaryFile.exists()) {
      const parsed = (await summaryFile.json()) as Record<string, unknown>;
      generatedAt = typeof parsed.generatedAt === "string" ? parsed.generatedAt : null;
      const totals =
        parsed.totals && typeof parsed.totals === "object"
          ? (parsed.totals as Record<string, unknown>)
          : null;
      const window =
        parsed.window && typeof parsed.window === "object"
          ? (parsed.window as Record<string, unknown>)
          : null;
      const usdSummary =
        parsed.usdSummary && typeof parsed.usdSummary === "object"
          ? (parsed.usdSummary as Record<string, unknown>)
          : null;

      if (window) {
        analyzedRangeStartAt = toIsoDateTime(
          typeof window.startAtIso === "string" ? window.startAtIso : null,
        );
        analyzedRangeEndAt = toIsoDateTime(
          typeof window.endAtIso === "string" ? window.endAtIso : null,
        );
      }
      if (totals) {
        matchedTrades = toOptionalNumber(totals.matchedTrades);
        pricedTrades = toOptionalNumber(totals.pricedTrades);
        pricedMatchRatePctFromSummary = toOptionalNumber(totals.pricedMatchRatePct);
      }
      if (usdSummary) {
        analyzedVolumeUsdFromSummary = toOptionalNumber(usdSummary.totalInputUsd);
      }
    }
  } catch (error) {
    summaryError = error instanceof Error ? `summary parse error: ${error.message}` : String(error);
  }

  const tradesFile = Bun.file(tradesPath);
  const tradesFileExists = await tradesFile.exists();
  if (!tradesFileExists) {
    const normalizedMatched = Math.max(0, Math.floor(matchedTrades ?? 0));
    const normalizedPriced = Math.max(0, Math.floor(pricedTrades ?? 0));
    const pricedMatchRatePct =
      pricedMatchRatePctFromSummary !== null
        ? roundToTwo(pricedMatchRatePctFromSummary)
        : normalizedMatched === 0
          ? null
          : roundToTwo((normalizedPriced / normalizedMatched) * 100);

    return {
      providerKey,
      summaryPath,
      tradesPath,
      found: false,
      generatedAt,
      analyzedRangeStartAt,
      analyzedRangeEndAt,
      matchedTrades: normalizedMatched,
      pricedTrades: normalizedPriced,
      pricedMatchRatePct,
      analyzedVolumeUsd: Math.max(0, analyzedVolumeUsdFromSummary ?? 0),
      trades: [],
      error: summaryError,
    };
  }

  let trades: ShortfallTradePoint[] = [];
  let tradesError: string | null = null;
  try {
    trades = parseShortfallTradePoints(await tradesFile.text(), notionalFilter);
  } catch (error) {
    tradesError = error instanceof Error ? `trades parse error: ${error.message}` : String(error);
  }

  const fallbackCount = trades.length;
  const normalizedMatched = Math.max(0, Math.floor(matchedTrades ?? fallbackCount));
  const normalizedPriced = Math.max(0, Math.floor(pricedTrades ?? fallbackCount));
  const analyzedVolumeUsdFromTrades = trades.reduce((sum, trade) => sum + trade.inputUsd, 0);
  const pricedMatchRatePct =
    pricedMatchRatePctFromSummary !== null
      ? roundToTwo(pricedMatchRatePctFromSummary)
      : normalizedMatched === 0
        ? null
        : roundToTwo((normalizedPriced / normalizedMatched) * 100);
  const analyzedVolumeUsd =
    tradesError === null
      ? analyzedVolumeUsdFromTrades
      : Math.max(0, analyzedVolumeUsdFromSummary ?? 0);

  return {
    providerKey,
    summaryPath,
    tradesPath,
    found: true,
    generatedAt,
    analyzedRangeStartAt,
    analyzedRangeEndAt,
    matchedTrades: normalizedMatched,
    pricedTrades: normalizedPriced,
    pricedMatchRatePct,
    analyzedVolumeUsd,
    trades,
    error: [summaryError, tradesError].filter((value): value is string => Boolean(value)).join("; ") || null,
  };
}

function toProviderSnapshot(
  source: ShortfallProviderSourceData,
  buckets: ShortfallNotionalBucket[],
  minSampleCount: number,
): ShortfallProviderSnapshot {
  return {
    providerKey: source.providerKey,
    summaryPath: source.summaryPath,
    tradesPath: source.tradesPath,
    found: source.found,
    generatedAt: source.generatedAt,
    analyzedRangeStartAt: source.analyzedRangeStartAt,
    analyzedRangeEndAt: source.analyzedRangeEndAt,
    matchedTrades: source.matchedTrades,
    pricedTrades: source.pricedTrades,
    pricedMatchRatePct: source.pricedMatchRatePct,
    analyzedVolumeUsd: source.analyzedVolumeUsd,
    buckets: buildProviderBucketQuantiles(source.trades, buckets, minSampleCount),
    error: source.error,
  };
}

async function buildShortfallSnapshot(view: ShortfallViewConfig): Promise<ShortfallSnapshot> {
  const bucketConfig: ShortfallBucketConfig = {
    axisScale: "log10",
    stepLog10: SHORTFALL_BUCKET_STEP_LOG10,
    minSampleCount: SHORTFALL_MIN_SAMPLE_COUNT,
    quantileMethod: "nearest_rank",
    smoothing: "none",
    interpolation: "none",
  };

  const providersForView = view.providers && view.providers.length > 0 ? view.providers : PROVIDER_KEYS;
  const providerSources = await Promise.all(
    providersForView.map((providerKey) =>
      getShortfallProviderSourceData(providerKey, view.tokenPath, view.notionalUsdFilter),
    ),
  );

  const allInputUsd = providerSources.flatMap((provider) =>
    provider.trades.map((trade) => trade.inputUsd),
  );
  const buckets = buildNotionalBuckets(allInputUsd, bucketConfig.stepLog10);
  const sampleCounts = buildBucketSampleCounts(
    providerSources.map((provider) => provider.trades),
    buckets,
  );
  for (const bucket of buckets) {
    bucket.sampleCount = sampleCounts[bucket.bucketIndex] ?? 0;
  }

  const providers = providerSources.map((source) =>
    toProviderSnapshot(source, buckets, bucketConfig.minSampleCount),
  );

  return {
    generatedAt: new Date().toISOString(),
    viewId: view.id,
    viewLabel: view.label,
    days: view.days,
    tokenPath: view.tokenPath,
    notionalUsdFilter: toNotionalSnapshot(view.notionalUsdFilter),
    bucketConfig,
    buckets,
    providers,
  };
}

async function getProviderSnapshot(
  providerKey: ProviderKey,
  sinceIso: string,
): Promise<ProviderAnalyticsSnapshot> {
  const exists = await providerDatabaseExists(providerKey);
  if (!exists) {
    return {
      providerKey,
      databaseExists: false,
      swapsTotal: 0,
      successSwaps: 0,
      failedSwaps: 0,
      volumeUsd: 0,
      successRate: 0,
      oldestEventAt: null,
      newestEventAt: null,
      daily: [],
      statuses: [],
      error: null,
    };
  }

  let dbHandle: Awaited<ReturnType<typeof openProviderDatabase>> | null = null;
  try {
    dbHandle = await openProviderDatabase(providerKey);
    const [totalsRows, dailyRows, statusRows] = await Promise.all([
      queryPrepared<ProviderTotalsRow>(dbHandle.connection, TOTALS_SQL, [sinceIso]),
      queryPrepared<ProviderDailyRow>(dbHandle.connection, DAILY_SQL, [sinceIso]),
      queryPrepared<ProviderStatusRow>(dbHandle.connection, STATUS_SQL, [sinceIso]),
    ]);

    const totals = totalsRows[0];
    const swapsTotal = toNumber(totals?.swaps_total);
    const successSwaps = toNumber(totals?.success_swaps);
    const failedSwaps = toNumber(totals?.failed_swaps);
    const volumeUsd = toNumber(totals?.volume_usd);
    const successRate = swapsTotal === 0 ? 0 : roundToTwo((successSwaps / swapsTotal) * 100);

    return {
      providerKey,
      databaseExists: true,
      swapsTotal,
      successSwaps,
      failedSwaps,
      volumeUsd,
      successRate,
      oldestEventAt: toIsoDateTime(totals?.oldest_event_at),
      newestEventAt: toIsoDateTime(totals?.newest_event_at),
      daily: dailyRows.map((row) => ({
        day: toIsoDay(row.day),
        swapsTotal: toNumber(row.swaps_total),
        volumeUsd: toNumber(row.volume_usd),
      })),
      statuses: statusRows.map((row) => ({
        status: row.status_canonical ?? "unknown",
        swapsTotal: toNumber(row.swaps_total),
      })),
      error: null,
    };
  } catch (error) {
    return {
      providerKey,
      databaseExists: true,
      swapsTotal: 0,
      successSwaps: 0,
      failedSwaps: 0,
      volumeUsd: 0,
      successRate: 0,
      oldestEventAt: null,
      newestEventAt: null,
      daily: [],
      statuses: [],
      error: error instanceof Error ? error.message : String(error),
    };
  } finally {
    dbHandle?.close();
  }
}

async function buildSnapshot(days: number): Promise<DashboardSnapshot> {
  const generatedAt = new Date();
  const since = new Date(generatedAt.getTime() - days * 24 * 60 * 60 * 1000);
  const sinceIso = since.toISOString();

  const providerSnapshots = await Promise.all(
    PROVIDER_KEYS.map((providerKey) => getProviderSnapshot(providerKey, sinceIso)),
  );

  const timeline = Array.from(
    new Set(providerSnapshots.flatMap((provider) => provider.daily.map((point) => point.day))),
  ).sort();

  const swapsByProvider = {} as Record<ProviderKey, number[]>;
  const volumeByProvider = {} as Record<ProviderKey, number[]>;

  for (const provider of providerSnapshots) {
    const swapsMap = new Map(provider.daily.map((point) => [point.day, point.swapsTotal]));
    const volumeMap = new Map(provider.daily.map((point) => [point.day, point.volumeUsd]));
    swapsByProvider[provider.providerKey] = timeline.map((day) => swapsMap.get(day) ?? 0);
    volumeByProvider[provider.providerKey] = timeline.map((day) => volumeMap.get(day) ?? 0);
  }

  const swapsTotal = providerSnapshots.reduce((sum, provider) => sum + provider.swapsTotal, 0);
  const successSwaps = providerSnapshots.reduce((sum, provider) => sum + provider.successSwaps, 0);
  const failedSwaps = providerSnapshots.reduce((sum, provider) => sum + provider.failedSwaps, 0);
  const volumeUsd = providerSnapshots.reduce((sum, provider) => sum + provider.volumeUsd, 0);
  const successRate = swapsTotal === 0 ? 0 : roundToTwo((successSwaps / swapsTotal) * 100);
  const activeProviders = providerSnapshots.filter((provider) => provider.swapsTotal > 0).length;

  return {
    generatedAt: generatedAt.toISOString(),
    days,
    since: sinceIso,
    totals: {
      swapsTotal,
      successSwaps,
      failedSwaps,
      volumeUsd: roundToTwo(volumeUsd),
      successRate,
      activeProviders,
    },
    timeline,
    providers: providerSnapshots,
    series: {
      swapsByProvider,
      volumeByProvider,
    },
  };
}

function resolvePublicPath(publicDir: string, requestPathname: string): string | null {
  const requested = requestPathname === "/" ? "index.html" : requestPathname.replace(/^\/+/, "");
  const normalized = normalize(requested);
  const absolute = resolve(publicDir, normalized);
  const withSep = publicDir.endsWith(sep) ? publicDir : `${publicDir}${sep}`;
  if (absolute !== publicDir && !absolute.startsWith(withSep)) {
    return null;
  }
  return absolute;
}

function getMimeType(filePath: string): string {
  return MIME_BY_EXT[extname(filePath).toLowerCase()] ?? "application/octet-stream";
}

export async function startDashboardServer(
  options: DashboardServerOptions,
): Promise<DashboardServerHandle> {
  const publicDir = join(process.cwd(), "public");

  const server = Bun.serve({
    hostname: options.host,
    port: options.port,
    fetch: async (request: Request) => {
      const url = new URL(request.url);
      if (url.pathname === "/api/analytics") {
        const snapshot = await buildSnapshot(AGGREGATION_WINDOW_DAYS);
        return Response.json(snapshot, {
          headers: {
            "Cache-Control": "no-store",
          },
        });
      }
      if (url.pathname === "/api/shortfall-distribution") {
        const shortfallView = resolveShortfallView(url.searchParams.get("view"));
        const snapshot = await buildShortfallSnapshot(shortfallView);
        return Response.json(snapshot, {
          headers: {
            "Cache-Control": "no-store",
          },
        });
      }

      const filePath = resolvePublicPath(publicDir, url.pathname);
      if (!filePath) {
        return new Response("Forbidden", { status: 403 });
      }
      const file = Bun.file(filePath);
      if (!(await file.exists())) {
        return new Response("Not found", { status: 404 });
      }

      return new Response(file, {
        headers: {
          "Content-Type": getMimeType(filePath),
          "Cache-Control": "no-cache",
        },
      });
    },
  });

  console.log(`Dashboard: http://${options.host}:${options.port}`);
  const finished = new Promise<never>(() => {});
  return { server, finished };
}
