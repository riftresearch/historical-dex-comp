import type { DuckDBConnection } from "@duckdb/node-api";
import { canonicalizeChain } from "../../domain/chain-canonicalization";
import type { NormalizedSwapRowInput } from "../../domain/normalized-swap";
import { mapLifiStatus } from "../../domain/status";
import type { IngestMode } from "../../ingest/types";
import { isSwapRowWithinIngestScope } from "../../ingest/swap-scope";
import { fetchJsonWithRetry } from "../../lib/http";
import type { IngestCheckpoint } from "../../storage/repositories/ingest-checkpoints";
import { persistSwaps, type RawSwapRowInput } from "../../storage/repositories/swaps";
import { sha256Hex } from "../../utils/hash";
import { atomicToNormalized, inferTokenDecimals } from "../../utils/amount-normalization";
import { parseFloatOrNull } from "../../utils/number";
import { addDays, parseUnixSecondsOrNull, shiftSeconds, toUnixSeconds } from "../../utils/time";
import type { ProviderAdapter, ProviderIngestInput, ProviderIngestOutput } from "../types";
import type { LifiToken, LifiTransferRecord, LifiTransfersResponse } from "./types";

const LIFI_BASE_URL = "https://li.quest";
const LIFI_SOURCE_ENDPOINT = `${LIFI_BASE_URL}/v1/analytics/transfers`;
const LIFI_STREAM_KEY = "swaps:lifi:analytics:transfers:v1:cbbtc_mainnet";
const LIFI_WINDOW_DAYS = 30;
const LIFI_RESPONSE_CAP = 1_000;
const LIFI_PAGE_SIZE_DEFAULT = 1_000;
const LIFI_PAGE_SIZE_MAX = 1_000;
const LIFI_MIN_WINDOW_SPLIT_SECONDS = 60;
const LIFI_MAX_WINDOW_SPLIT_DEPTH = 12;

const CHAIN_ID_MAINNET = "1";
const CHAIN_CANONICAL_MAINNET = "eip155:1";

const ZERO_ADDRESS = "0x0000000000000000000000000000000000000000";
const USDC_ADDRESS = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
const USDT_ADDRESS = "0xdac17f958d2ee523a2206206994597c13d831ec7";
const WETH_ADDRESS = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2";
const CBBTC_ADDRESS = "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf";

interface LifiSourceTokenScope {
  symbol: "USDC" | "USDT" | "WETH" | "ETH";
  address: string;
}

const LIFI_SOURCE_TOKEN_SCOPES: readonly LifiSourceTokenScope[] = [
  { symbol: "USDC", address: USDC_ADDRESS },
  { symbol: "USDT", address: USDT_ADDRESS },
  { symbol: "WETH", address: WETH_ADDRESS },
  { symbol: "ETH", address: ZERO_ADDRESS },
] as const;

const ALLOWED_SOURCE_SYMBOLS = new Set(["USDC", "USDT", "ETH", "WETH"]);

interface LifiWindow {
  fromTs: Date | null;
  toTs: Date | null;
  fromTimestamp: number | null;
  toTimestamp: number | null;
}

interface LifiWindowChunk {
  window: LifiWindow;
  depth: number;
}

interface LifiFetchInput {
  scope: LifiSourceTokenScope;
  window: LifiWindow;
}

interface BoundaryPoint {
  eventAt: Date;
  providerRecordId: string;
}

interface NormalizedLifiSwap {
  core: NormalizedSwapRowInput;
  raw: RawSwapRowInput;
  eventAt: Date | null;
}

function nonEmptyOrNull(value: unknown): string | null {
  if (value === null || value === undefined) {
    return null;
  }
  const trimmed = (typeof value === "string" ? value : String(value)).trim();
  return trimmed.length > 0 ? trimmed : null;
}

function toStringOrNull(value: unknown): string | null {
  if (value === null || value === undefined) {
    return null;
  }
  return nonEmptyOrNull(String(value));
}

function normalizeAddress(value: string | null | undefined): string | null {
  if (!value) {
    return null;
  }
  const normalized = value.trim().toLowerCase();
  if (!/^0x[a-f0-9]{40}$/.test(normalized)) {
    return null;
  }
  return normalized;
}

function normalizeHash(value: string | null | undefined): string | null {
  if (!value) {
    return null;
  }
  const normalized = value.trim().toLowerCase();
  if (!/^0x[a-f0-9]{64}$/.test(normalized)) {
    return null;
  }
  return normalized;
}

function normalizeSymbol(value: string | null | undefined): string | null {
  if (!value) {
    return null;
  }
  const normalized = value.trim().toUpperCase();
  return normalized.length > 0 ? normalized : null;
}

function clampPageSize(requested: number): number {
  if (!Number.isFinite(requested) || requested <= 0) {
    return LIFI_PAGE_SIZE_DEFAULT;
  }
  return Math.min(Math.floor(requested), LIFI_PAGE_SIZE_MAX);
}

function buildLifiWindow(mode: IngestMode, checkpoint: IngestCheckpoint | null, now: Date): LifiWindow {
  if (mode === "sync_newer") {
    const fromTs = checkpoint?.newestEventAt ? shiftSeconds(checkpoint.newestEventAt, 1) : addDays(now, -30);
    return {
      fromTs,
      toTs: now,
      fromTimestamp: toUnixSeconds(fromTs),
      toTimestamp: toUnixSeconds(now),
    };
  }

  if (mode === "backfill_older") {
    if (checkpoint?.oldestEventAt) {
      const toTs = shiftSeconds(checkpoint.oldestEventAt, -1);
      const fromTs = addDays(checkpoint.oldestEventAt, -LIFI_WINDOW_DAYS);
      return {
        fromTs,
        toTs,
        fromTimestamp: toUnixSeconds(fromTs),
        toTimestamp: toUnixSeconds(toTs),
      };
    }

    const bootstrapFrom = addDays(now, -LIFI_WINDOW_DAYS);
    return {
      fromTs: bootstrapFrom,
      toTs: now,
      fromTimestamp: toUnixSeconds(bootstrapFrom),
      toTimestamp: toUnixSeconds(now),
    };
  }

  const bootstrapFrom = addDays(now, -LIFI_WINDOW_DAYS);
  return {
    fromTs: bootstrapFrom,
    toTs: now,
    fromTimestamp: toUnixSeconds(bootstrapFrom),
    toTimestamp: toUnixSeconds(now),
  };
}

function buildLifiWindowFromUnix(fromTimestamp: number, toTimestamp: number): LifiWindow {
  return {
    fromTs: new Date(fromTimestamp * 1_000),
    toTs: new Date(toTimestamp * 1_000),
    fromTimestamp,
    toTimestamp,
  };
}

function splitLifiWindowChunk(
  chunk: LifiWindowChunk,
): { older: LifiWindowChunk; newer: LifiWindowChunk } | null {
  if (chunk.depth >= LIFI_MAX_WINDOW_SPLIT_DEPTH) {
    return null;
  }
  if (chunk.window.fromTimestamp === null || chunk.window.toTimestamp === null) {
    return null;
  }

  const start = chunk.window.fromTimestamp;
  const end = chunk.window.toTimestamp;
  if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start) {
    return null;
  }

  const spanSeconds = end - start + 1;
  if (spanSeconds <= LIFI_MIN_WINDOW_SPLIT_SECONDS) {
    return null;
  }

  const midpoint = Math.floor((start + end) / 2);
  if (midpoint <= start || midpoint >= end) {
    return null;
  }

  return {
    older: {
      window: buildLifiWindowFromUnix(start, midpoint),
      depth: chunk.depth + 1,
    },
    newer: {
      window: buildLifiWindowFromUnix(midpoint + 1, end),
      depth: chunk.depth + 1,
    },
  };
}

function formatLifiWindow(window: LifiWindow): string {
  return `${window.fromTimestamp ?? "na"}-${window.toTimestamp ?? "na"}`;
}

function buildLifiTransfersUrl(input: LifiFetchInput): string {
  const url = new URL(LIFI_SOURCE_ENDPOINT);
  url.searchParams.set("fromChain", CHAIN_ID_MAINNET);
  url.searchParams.set("toChain", CHAIN_ID_MAINNET);
  url.searchParams.set("fromToken", input.scope.address);
  url.searchParams.set("toToken", CBBTC_ADDRESS);
  if (input.window.fromTimestamp !== null) {
    url.searchParams.set("fromTimestamp", String(input.window.fromTimestamp));
  }
  if (input.window.toTimestamp !== null) {
    url.searchParams.set("toTimestamp", String(input.window.toTimestamp));
  }
  return url.toString();
}

async function fetchLifiTransfers(input: LifiFetchInput): Promise<LifiTransfersResponse> {
  const url = buildLifiTransfersUrl(input);
  return fetchJsonWithRetry<LifiTransfersResponse>(
    url,
    {
      method: "GET",
      headers: {
        Accept: "application/json",
      },
    },
    {
      attempts: 5,
      initialDelayMs: 500,
      maxDelayMs: 5_000,
      timeoutMs: 90_000,
    },
  );
}

function shouldIncludeByWindow(eventAt: Date | null, window: LifiWindow): boolean {
  if (!eventAt) {
    return true;
  }
  if (window.fromTs && eventAt < window.fromTs) {
    return false;
  }
  if (window.toTs && eventAt > window.toTs) {
    return false;
  }
  return true;
}

function deriveEventAt(record: LifiTransferRecord): Date | null {
  const fromSending = parseUnixSecondsOrNull(record.sending?.timestamp);
  if (fromSending) {
    return fromSending;
  }
  return parseUnixSecondsOrNull(record.receiving?.timestamp);
}

function toAssetId(token: LifiToken | null | undefined): string | null {
  const chainIdRaw = toStringOrNull(token?.chainId);
  const address = normalizeAddress(token?.address);

  if (address === ZERO_ADDRESS) {
    if (chainIdRaw) {
      return `${chainIdRaw}:native`;
    }
    return `unknown:native`;
  }

  if (address && chainIdRaw) {
    return `${chainIdRaw}:${address}`;
  }
  if (address) {
    return `unknown:${address}`;
  }
  return null;
}

function amountAtomic(value: string | null | undefined): string | null {
  const normalized = nonEmptyOrNull(value);
  if (!normalized) {
    return null;
  }
  if (!/^[0-9]+$/.test(normalized)) {
    return null;
  }
  return normalized;
}

function collectTxHashes(record: LifiTransferRecord): string[] {
  const hashes = [
    normalizeHash(record.sending?.txHash ?? null),
    normalizeHash(record.receiving?.txHash ?? null),
  ].filter((hash): hash is string => Boolean(hash));

  return [...new Set(hashes)];
}

function deriveRouteHint(record: LifiTransferRecord): string | null {
  const tools = new Set<string>();
  const recordTool = nonEmptyOrNull(record.tool);
  if (recordTool) {
    tools.add(recordTool);
  }

  const sendingSteps = record.sending?.includedSteps ?? [];
  for (const step of sendingSteps) {
    const stepTool = nonEmptyOrNull(step?.tool);
    if (stepTool) {
      tools.add(stepTool);
    }
  }

  if (tools.size === 0) {
    return null;
  }
  return [...tools].join("->");
}

function sumFeeUsd(record: LifiTransferRecord): number | null {
  const feeCostTotal = (record.feeCosts ?? []).reduce((sum, feeCost) => {
    const amountUsd = parseFloatOrNull(feeCost?.amountUSD);
    return amountUsd === null ? sum : sum + amountUsd;
  }, 0);

  if (feeCostTotal > 0) {
    return feeCostTotal;
  }

  return parseFloatOrNull(record.sending?.gasAmountUSD ?? null);
}

function isFinalLifiStatus(statusCanonical: NormalizedSwapRowInput["status_canonical"]): boolean {
  return (
    statusCanonical === "success" ||
    statusCanonical === "failed" ||
    statusCanonical === "refunded" ||
    statusCanonical === "expired"
  );
}

function isSupportedLifiCbbtcPath(
  row: Pick<
    NormalizedSwapRowInput,
    | "source_chain_canonical"
    | "destination_chain_canonical"
    | "source_asset_symbol"
    | "destination_asset_symbol"
    | "source_asset_id"
    | "destination_asset_id"
  >,
): boolean {
  if (row.source_chain_canonical !== CHAIN_CANONICAL_MAINNET) {
    return false;
  }
  if (row.destination_chain_canonical !== CHAIN_CANONICAL_MAINNET) {
    return false;
  }

  const sourceSymbol = normalizeSymbol(row.source_asset_symbol ?? null);
  if (!sourceSymbol || !ALLOWED_SOURCE_SYMBOLS.has(sourceSymbol)) {
    return false;
  }

  const destinationSymbol = normalizeSymbol(row.destination_asset_symbol ?? null);
  if (destinationSymbol !== "CBBTC") {
    return false;
  }

  const destinationAssetId = nonEmptyOrNull(row.destination_asset_id)?.toLowerCase() ?? null;
  if (destinationAssetId !== `${CHAIN_ID_MAINNET}:${CBBTC_ADDRESS}`) {
    return false;
  }

  const sourceAssetId = nonEmptyOrNull(row.source_asset_id)?.toLowerCase() ?? "";
  if (sourceSymbol === "ETH") {
    return sourceAssetId === `${CHAIN_ID_MAINNET}:native`;
  }
  if (sourceSymbol === "USDC") {
    return sourceAssetId === `${CHAIN_ID_MAINNET}:${USDC_ADDRESS}`;
  }
  if (sourceSymbol === "USDT") {
    return sourceAssetId === `${CHAIN_ID_MAINNET}:${USDT_ADDRESS}`;
  }
  if (sourceSymbol === "WETH") {
    return sourceAssetId === `${CHAIN_ID_MAINNET}:${WETH_ADDRESS}`;
  }

  return false;
}

function normalizeLifiRecord(
  record: LifiTransferRecord,
  runId: string,
  observedAt: Date,
  sourceCursor: string,
  expectedSourceSymbol: string,
): NormalizedLifiSwap | null {
  const eventAt = deriveEventAt(record);

  const sourceTxHash = normalizeHash(record.sending?.txHash ?? null);
  const destinationTxHash = normalizeHash(record.receiving?.txHash ?? null);
  const providerRecordId =
    nonEmptyOrNull(record.transactionId) ?? sourceTxHash ?? destinationTxHash;
  if (!providerRecordId) {
    return null;
  }

  const rawJson = JSON.stringify(record);
  const rawHash = sha256Hex(rawJson);

  const sourceToken = record.sending?.token ?? null;
  const destinationToken = record.receiving?.token ?? null;
  const sourceChain = canonicalizeChain({
    provider: "lifi",
    rawChainId: record.sending?.chainId ?? sourceToken?.chainId ?? null,
  });
  const destinationChain = canonicalizeChain({
    provider: "lifi",
    rawChainId: record.receiving?.chainId ?? destinationToken?.chainId ?? null,
  });

  const sourceSymbol = normalizeSymbol(sourceToken?.symbol ?? null) ?? expectedSourceSymbol;
  const destinationSymbol = normalizeSymbol(destinationToken?.symbol ?? null);

  const sourceAssetDecimals = inferTokenDecimals({
    provider: "lifi",
    explicitDecimals: sourceToken?.decimals ?? null,
    chainCanonical: sourceChain.canonical,
    symbol: sourceSymbol,
  });
  const destinationAssetDecimals = inferTokenDecimals({
    provider: "lifi",
    explicitDecimals: destinationToken?.decimals ?? null,
    chainCanonical: destinationChain.canonical,
    symbol: destinationSymbol,
  });

  const amountInAtomic = amountAtomic(record.sending?.amount ?? null);
  const amountOutAtomic = amountAtomic(record.receiving?.amount ?? null);
  const amountInFromAtomic =
    amountInAtomic && sourceAssetDecimals !== null
      ? atomicToNormalized(amountInAtomic, sourceAssetDecimals)
      : null;
  const amountOutFromAtomic =
    amountOutAtomic && destinationAssetDecimals !== null
      ? atomicToNormalized(amountOutAtomic, destinationAssetDecimals)
      : null;

  const statusCanonical = mapLifiStatus(record.status, record.substatus);
  const statusRawParts = [nonEmptyOrNull(record.status), nonEmptyOrNull(record.substatus)].filter(
    (part): part is string => Boolean(part),
  );
  const statusRaw = statusRawParts.length > 0 ? statusRawParts.join(":") : null;
  const failureReasonRaw =
    statusCanonical === "success"
      ? null
      : nonEmptyOrNull(record.substatusMessage) ??
        nonEmptyOrNull(record.substatus) ??
        nonEmptyOrNull(record.status);

  const txHashes = collectTxHashes(record);
  const routeHint = deriveRouteHint(record);
  const toolName = nonEmptyOrNull(record.tool);

  const core: NormalizedSwapRowInput = {
    normalized_id: providerRecordId,
    provider_key: "lifi",
    provider_record_id: providerRecordId,
    provider_parent_id: sourceTxHash,
    record_granularity: "transfer",
    status_canonical: statusCanonical,
    status_raw: statusRaw,
    failure_reason_raw: failureReasonRaw,
    created_at: eventAt,
    updated_at: eventAt,
    event_at: eventAt,
    source_chain_canonical: sourceChain.canonical,
    destination_chain_canonical: destinationChain.canonical,
    source_chain_raw: sourceChain.rawChain,
    destination_chain_raw: destinationChain.rawChain,
    source_chain_id_raw: sourceChain.rawChainId,
    destination_chain_id_raw: destinationChain.rawChainId,
    source_asset_id: toAssetId(sourceToken),
    destination_asset_id: toAssetId(destinationToken),
    source_asset_symbol: sourceSymbol,
    destination_asset_symbol: destinationSymbol,
    source_asset_decimals: sourceAssetDecimals,
    destination_asset_decimals: destinationAssetDecimals,
    amount_in_atomic: amountInAtomic,
    amount_out_atomic: amountOutAtomic,
    amount_in_normalized: amountInFromAtomic,
    amount_out_normalized: amountOutFromAtomic,
    amount_in_usd: parseFloatOrNull(record.sending?.amountUSD ?? null),
    amount_out_usd: parseFloatOrNull(record.receiving?.amountUSD ?? null),
    fee_atomic: null,
    fee_normalized: null,
    fee_usd: sumFeeUsd(record),
    slippage_bps: null,
    solver_id: toolName,
    route_hint: routeHint,
    source_tx_hash: sourceTxHash,
    destination_tx_hash: destinationTxHash,
    refund_tx_hash: null,
    extra_tx_hashes: txHashes.length > 0 ? txHashes : null,
    is_final: isFinalLifiStatus(statusCanonical),
    raw_hash_latest: rawHash,
    source_endpoint: LIFI_SOURCE_ENDPOINT,
    ingested_at: observedAt,
    run_id: runId,
  };

  const raw: RawSwapRowInput = {
    normalized_id: providerRecordId,
    raw_hash: rawHash,
    raw_json: rawJson,
    observed_at: observedAt,
    source_endpoint: LIFI_SOURCE_ENDPOINT,
    source_cursor: sourceCursor,
    run_id: runId,
  };

  return {
    core,
    raw,
    eventAt,
  };
}

function updateNewestBoundary(current: BoundaryPoint | null, candidate: BoundaryPoint): BoundaryPoint {
  if (!current) {
    return candidate;
  }
  if (candidate.eventAt.getTime() > current.eventAt.getTime()) {
    return candidate;
  }
  if (
    candidate.eventAt.getTime() === current.eventAt.getTime() &&
    candidate.providerRecordId > current.providerRecordId
  ) {
    return candidate;
  }
  return current;
}

function updateOldestBoundary(current: BoundaryPoint | null, candidate: BoundaryPoint): BoundaryPoint {
  if (!current) {
    return candidate;
  }
  if (candidate.eventAt.getTime() < current.eventAt.getTime()) {
    return candidate;
  }
  if (
    candidate.eventAt.getTime() === current.eventAt.getTime() &&
    candidate.providerRecordId < current.providerRecordId
  ) {
    return candidate;
  }
  return current;
}

function mergeCheckpoint(
  existing: IngestCheckpoint | null,
  runId: string,
  newest: BoundaryPoint | null,
  oldest: BoundaryPoint | null,
): IngestCheckpoint | null {
  if (!existing && !newest && !oldest) {
    return null;
  }

  const checkpoint: IngestCheckpoint = {
    streamKey: LIFI_STREAM_KEY,
    syncScope: "swaps",
    newestCursor: existing?.newestCursor ?? null,
    newestEventAt: existing?.newestEventAt ?? null,
    newestProviderRecordId: existing?.newestProviderRecordId ?? null,
    oldestCursor: existing?.oldestCursor ?? null,
    oldestEventAt: existing?.oldestEventAt ?? null,
    oldestProviderRecordId: existing?.oldestProviderRecordId ?? null,
    updatedAt: new Date(),
    runId,
  };

  if (newest && (!checkpoint.newestEventAt || newest.eventAt > checkpoint.newestEventAt)) {
    checkpoint.newestEventAt = newest.eventAt;
    checkpoint.newestProviderRecordId = newest.providerRecordId;
  }

  if (oldest && (!checkpoint.oldestEventAt || oldest.eventAt < checkpoint.oldestEventAt)) {
    checkpoint.oldestEventAt = oldest.eventAt;
    checkpoint.oldestProviderRecordId = oldest.providerRecordId;
  }

  return checkpoint;
}

async function ingestLifi(
  connection: DuckDBConnection,
  runId: string,
  mode: IngestMode,
  checkpoint: IngestCheckpoint | null,
  now: Date,
  pageSize: number,
  maxPages: number | null,
): Promise<ProviderIngestOutput> {
  const window = buildLifiWindow(mode, checkpoint, now);
  const effectivePageSize = clampPageSize(pageSize);
  if (effectivePageSize !== LIFI_RESPONSE_CAP) {
    // LI.FI analytics endpoint response appears capped at 1000 regardless of explicit limit,
    // so we keep a deterministic expectation for split logic.
  }

  if (
    window.fromTimestamp !== null &&
    window.toTimestamp !== null &&
    window.fromTimestamp > window.toTimestamp
  ) {
    return {
      recordsFetched: 0,
      recordsNormalized: 0,
      recordsUpserted: 0,
      checkpoint: mergeCheckpoint(checkpoint, runId, null, null),
    };
  }

  const maxRequestsPerScope =
    maxPages === null
      ? null
      : Math.max(1, Math.floor(maxPages / LIFI_SOURCE_TOKEN_SCOPES.length));

  let recordsFetched = 0;
  let recordsNormalized = 0;
  let recordsUpserted = 0;
  let newestBoundary: BoundaryPoint | null = null;
  let oldestBoundary: BoundaryPoint | null = null;

  for (const scope of LIFI_SOURCE_TOKEN_SCOPES) {
    const queue: LifiWindowChunk[] = [{ window, depth: 0 }];
    const seenWindowKeys = new Set<string>();
    let requestsForScope = 0;

    while (queue.length > 0) {
      if (maxRequestsPerScope !== null && requestsForScope >= maxRequestsPerScope) {
        break;
      }

      const chunk = queue.shift();
      if (!chunk) {
        continue;
      }

      const windowKey = `${scope.symbol}|${formatLifiWindow(chunk.window)}|d:${chunk.depth}`;
      if (seenWindowKeys.has(windowKey)) {
        continue;
      }
      seenWindowKeys.add(windowKey);

      const page = await fetchLifiTransfers({
        scope,
        window: chunk.window,
      });
      requestsForScope += 1;

      const transfers = page.transfers ?? [];
      recordsFetched += transfers.length;

      if (transfers.length >= effectivePageSize) {
        const split = splitLifiWindowChunk(chunk);
        if (split) {
          queue.unshift(split.newer);
          queue.unshift(split.older);
          continue;
        }
      }

      if (transfers.length === 0) {
        continue;
      }

      const observedAt = new Date();
      const sourceCursor = `scope:${scope.symbol}|window:${formatLifiWindow(chunk.window)}|depth:${chunk.depth}`;
      const coreRows: NormalizedSwapRowInput[] = [];
      const rawRows: RawSwapRowInput[] = [];

      for (const record of transfers) {
        const normalized = normalizeLifiRecord(
          record,
          runId,
          observedAt,
          sourceCursor,
          scope.symbol,
        );
        if (!normalized) {
          continue;
        }
        if (!shouldIncludeByWindow(normalized.eventAt, chunk.window)) {
          continue;
        }
        if (normalized.core.status_canonical !== "success") {
          continue;
        }
        if (!isSupportedLifiCbbtcPath(normalized.core)) {
          continue;
        }
        if (!isSwapRowWithinIngestScope(normalized.core)) {
          continue;
        }

        coreRows.push(normalized.core);
        rawRows.push(normalized.raw);
        recordsNormalized += 1;

        if (normalized.eventAt) {
          const point: BoundaryPoint = {
            eventAt: normalized.eventAt,
            providerRecordId: normalized.core.provider_record_id,
          };
          newestBoundary = updateNewestBoundary(newestBoundary, point);
          oldestBoundary = updateOldestBoundary(oldestBoundary, point);
        }
      }

      if (coreRows.length > 0) {
        const persisted = await persistSwaps(connection, coreRows, rawRows);
        recordsUpserted += persisted.coreUpserts;
      }
    }
  }

  return {
    recordsFetched,
    recordsNormalized,
    recordsUpserted,
    checkpoint: mergeCheckpoint(checkpoint, runId, newestBoundary, oldestBoundary),
  };
}

export const lifiProviderAdapter: ProviderAdapter = {
  key: "lifi",
  streamKey: LIFI_STREAM_KEY,
  sourceEndpoint: LIFI_SOURCE_ENDPOINT,
  async ingest(input: ProviderIngestInput): Promise<ProviderIngestOutput> {
    return ingestLifi(
      input.connection,
      input.runId,
      input.mode,
      input.checkpoint,
      input.now,
      input.pageSize,
      input.maxPages,
    );
  },
};
