import type { DuckDBConnection } from "@duckdb/node-api";
import { canonicalizeChain } from "../../domain/chain-canonicalization";
import type { NormalizedSwapRowInput } from "../../domain/normalized-swap";
import { mapNearIntentsStatus } from "../../domain/status";
import type { IngestMode } from "../../ingest/types";
import {
  getNearIntentsScopedChains,
  isSwapRowWithinIngestScope,
} from "../../ingest/swap-scope";
import { fetchJsonWithRetry } from "../../lib/http";
import type { IngestCheckpoint } from "../../storage/repositories/ingest-checkpoints";
import { persistSwaps, type RawSwapRowInput } from "../../storage/repositories/swaps";
import { sha256Hex } from "../../utils/hash";
import { atomicToNormalized, inferTokenDecimals } from "../../utils/amount-normalization";
import { parseFloatOrNull } from "../../utils/number";
import {
  addDays,
  parseDateOrNull,
  parseUnixSecondsOrNull,
  shiftSeconds,
  toUnixSeconds,
} from "../../utils/time";
import type { ProviderAdapter, ProviderIngestInput, ProviderIngestOutput } from "../types";
import type { NearIntentsTransaction, NearIntentsTransactionsPagesResponse } from "./types";

const NEAR_INTENTS_BASE_URL = "https://explorer.near-intents.org";
const NEAR_INTENTS_SOURCE_ENDPOINT = `${NEAR_INTENTS_BASE_URL}/api/v0/transactions-pages`;
const NEAR_INTENTS_STREAM_KEY = "swaps:transactions-pages:v0";
const NEAR_INTENTS_WINDOW_DAYS = 30;
const NEAR_INTENTS_PAGE_SIZE_DEFAULT = 50;
const NEAR_INTENTS_PAGE_SIZE_MAX = 1_000;
const NEAR_INTENTS_REQUEST_INTERVAL_MS = 5_000;
const NEAR_INTENTS_REQUEST_TIMEOUT_MS = 90_000;
const NEAR_INTENTS_DEFAULT_STATUSES = "SUCCESS";
const NEAR_INTENTS_MIN_WINDOW_SPLIT_SECONDS = 60 * 60;
const NEAR_INTENTS_MAX_WINDOW_SPLIT_DEPTH = 12;

const KNOWN_CHAIN_IDS = new Set([
  "near",
  "eth",
  "base",
  "arb",
  "btc",
  "sol",
  "ton",
  "doge",
  "xrp",
  "zec",
  "gnosis",
  "bera",
  "bsc",
  "pol",
  "tron",
  "sui",
  "op",
  "avax",
  "cardano",
  "stellar",
  "aptos",
  "ltc",
  "monad",
  "xlayer",
  "starknet",
  "bch",
  "adi",
  "plasma",
  "scroll",
  "aleo",
]);

interface NearIntentsWindow {
  fromTs: Date | null;
  toTs: Date | null;
  startTimestampUnix: number | null;
  endTimestampUnix: number | null;
}

interface NearIntentsWindowChunk {
  window: NearIntentsWindow;
  depth: number;
}

interface NearIntentsPageFetchInput {
  authToken: string;
  page: number;
  perPage: number;
  window: NearIntentsWindow;
  fromChainId: string;
  toChainId: string;
}

interface NearIntentsRateLimitState {
  nextRequestAtMs: number;
}

interface NearIntentsQueryScope {
  key: string;
  fromChainId: string;
  toChainId: string;
}

interface NearIntentsScopeProgress {
  oldestFetchedEventAt: Date | null;
  hitPageCap: boolean;
}

interface BoundaryPoint {
  eventAt: Date;
  providerRecordId: string;
}

interface ParsedNearIntentsAsset {
  raw: string;
  chainRaw: string | null;
  tokenRaw: string | null;
  symbol: string | null;
}

interface NormalizedNearIntentsSwap {
  core: NormalizedSwapRowInput;
  raw: RawSwapRowInput;
  eventAt: Date | null;
}

function toPageCursor(page: number): string {
  return `page:${page}`;
}

function parsePageCursor(value: string | null | undefined): number | null {
  const normalized = nonEmptyOrNull(value);
  if (!normalized) {
    return null;
  }
  const match = normalized.match(/^page:(\d+)$/i);
  if (!match) {
    return null;
  }

  const parsed = Number(match[1]);
  if (!Number.isFinite(parsed) || parsed <= 0 || !Number.isInteger(parsed)) {
    return null;
  }
  return parsed;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function nonEmptyOrNull(value: string | null | undefined): string | null {
  if (!value) {
    return null;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function clampPageSize(requested: number): number {
  if (!Number.isFinite(requested) || requested <= 0) {
    return NEAR_INTENTS_PAGE_SIZE_DEFAULT;
  }
  return Math.min(Math.floor(requested), NEAR_INTENTS_PAGE_SIZE_MAX);
}

function isKnownChain(value: string | null): boolean {
  if (!value) {
    return false;
  }
  return KNOWN_CHAIN_IDS.has(value.toLowerCase());
}

function inferSymbol(tokenRaw: string | null): string | null {
  if (!tokenRaw) {
    return null;
  }
  const cleaned = tokenRaw.trim();
  if (/^[A-Za-z0-9]{2,12}$/.test(cleaned)) {
    return cleaned.toUpperCase();
  }
  return null;
}

const NEAR_INTENTS_OMFT_SUFFIX = ".omft.near";

const NEAR_INTENTS_NATIVE_SYMBOL_BY_CHAIN: Partial<Record<string, string>> = {
  base: "ETH",
  btc: "BTC",
  eth: "ETH",
};

const NEAR_INTENTS_OMFT_CONTRACT_SYMBOL_BY_CHAIN: Partial<Record<string, Record<string, string>>> = {
  base: {
    "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913": "USDC",
    "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf": "CBBTC",
  },
  eth: {
    "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": "WBTC",
    "0x6b175474e89094c44da98b954eedeac495271d0f": "DAI",
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": "USDC",
    "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf": "CBBTC",
    "0xdac17f958d2ee523a2206206994597c13d831ec7": "USDT",
  },
};

function stripNearIntentsOmftSuffix(tokenRaw: string): string {
  if (tokenRaw.toLowerCase().endsWith(NEAR_INTENTS_OMFT_SUFFIX)) {
    return tokenRaw.slice(0, -NEAR_INTENTS_OMFT_SUFFIX.length);
  }
  return tokenRaw;
}

function normalizeNearIntentsTokenRaw(tokenRaw: string | null): string | null {
  const normalized = nonEmptyOrNull(tokenRaw);
  if (!normalized) {
    return null;
  }
  return nonEmptyOrNull(stripNearIntentsOmftSuffix(normalized));
}

function inferNearIntentsSymbol(chainRaw: string | null, tokenRaw: string | null): string | null {
  const normalizedChain = chainRaw?.toLowerCase() ?? null;
  const normalizedToken = normalizeNearIntentsTokenRaw(tokenRaw);
  if (!normalizedToken) {
    if (!normalizedChain) {
      return null;
    }
    return NEAR_INTENTS_NATIVE_SYMBOL_BY_CHAIN[normalizedChain] ?? null;
  }

  const tokenLower = normalizedToken.toLowerCase();
  if (normalizedChain && tokenLower === normalizedChain) {
    return NEAR_INTENTS_NATIVE_SYMBOL_BY_CHAIN[normalizedChain] ?? inferSymbol(normalizedToken);
  }

  if (normalizedChain && /^0x[a-f0-9]{40}$/.test(tokenLower)) {
    const mapped = NEAR_INTENTS_OMFT_CONTRACT_SYMBOL_BY_CHAIN[normalizedChain]?.[tokenLower];
    if (mapped) {
      return mapped;
    }
  }

  return inferSymbol(normalizedToken);
}

function nativeNearIntentsTokenRawForChain(chainRaw: string): string {
  const symbol = NEAR_INTENTS_NATIVE_SYMBOL_BY_CHAIN[chainRaw.toLowerCase()];
  if (symbol) {
    return symbol.toLowerCase();
  }
  return chainRaw.toLowerCase();
}

function parseNearIntentsAsset(rawAsset: string | null | undefined): ParsedNearIntentsAsset | null {
  const raw = nonEmptyOrNull(rawAsset);
  if (!raw) {
    return null;
  }

  const firstColonIndex = raw.indexOf(":");
  if (firstColonIndex < 0) {
    return {
      raw,
      chainRaw: null,
      tokenRaw: raw,
      symbol: inferSymbol(raw),
    };
  }

  const namespace = raw.slice(0, firstColonIndex).toLowerCase();
  const payload = raw.slice(firstColonIndex + 1);
  if (!payload) {
    return {
      raw,
      chainRaw: isKnownChain(namespace) ? namespace : null,
      tokenRaw: null,
      symbol: null,
    };
  }

  if (namespace === "nep141") {
    const normalizedPayload = normalizeNearIntentsTokenRaw(payload);
    if (!normalizedPayload) {
      return {
        raw,
        chainRaw: "near",
        tokenRaw: null,
        symbol: null,
      };
    }

    const dashIndex = normalizedPayload.indexOf("-");
    if (dashIndex > 0) {
      const chainCandidate = normalizedPayload.slice(0, dashIndex).toLowerCase();
      const tokenRaw = normalizeNearIntentsTokenRaw(normalizedPayload.slice(dashIndex + 1));
      if (isKnownChain(chainCandidate)) {
        return {
          raw,
          chainRaw: chainCandidate,
          tokenRaw,
          symbol: inferNearIntentsSymbol(chainCandidate, tokenRaw),
        };
      }
    }

    const maybeNativeChain = normalizedPayload.toLowerCase();
    if (isKnownChain(maybeNativeChain)) {
      const tokenRaw = nativeNearIntentsTokenRawForChain(maybeNativeChain);
      return {
        raw,
        chainRaw: maybeNativeChain,
        tokenRaw,
        symbol: inferNearIntentsSymbol(maybeNativeChain, tokenRaw),
      };
    }

    return {
      raw,
      chainRaw: "near",
      tokenRaw: normalizedPayload,
      symbol: inferNearIntentsSymbol("near", normalizedPayload),
    };
  }

  if (isKnownChain(namespace)) {
    const tokenRaw = normalizeNearIntentsTokenRaw(payload);
    return {
      raw,
      chainRaw: namespace,
      tokenRaw,
      symbol: inferNearIntentsSymbol(namespace, tokenRaw),
    };
  }

  const normalizedPayload = normalizeNearIntentsTokenRaw(payload) ?? payload;
  const payloadDashIndex = normalizedPayload.indexOf("-");
  if (payloadDashIndex > 0) {
    const payloadChainCandidate = normalizedPayload.slice(0, payloadDashIndex).toLowerCase();
    if (isKnownChain(payloadChainCandidate)) {
      const tokenRaw = normalizeNearIntentsTokenRaw(normalizedPayload.slice(payloadDashIndex + 1));
      return {
        raw,
        chainRaw: payloadChainCandidate,
        tokenRaw,
        symbol: inferNearIntentsSymbol(payloadChainCandidate, tokenRaw),
      };
    }
  }

  return {
    raw,
    chainRaw: null,
    tokenRaw: normalizeNearIntentsTokenRaw(payload),
    symbol: inferNearIntentsSymbol(null, payload),
  };
}

function buildAssetId(asset: ParsedNearIntentsAsset | null, chainCanonical: string | null): string | null {
  if (!asset) {
    return null;
  }
  if (!chainCanonical) {
    return asset.raw.toLowerCase();
  }
  const tokenPart = (asset.tokenRaw ?? asset.raw).toLowerCase();
  return `${chainCanonical}:${tokenPart}`;
}

function buildNearIntentsWindow(
  mode: IngestMode,
  checkpoint: IngestCheckpoint | null,
  now: Date,
): NearIntentsWindow {
  if (mode === "sync_newer") {
    const fromTs = checkpoint?.newestEventAt ? shiftSeconds(checkpoint.newestEventAt, 1) : addDays(now, -30);
    return {
      fromTs,
      toTs: now,
      startTimestampUnix: toUnixSeconds(fromTs),
      endTimestampUnix: toUnixSeconds(now),
    };
  }

  if (mode === "backfill_older") {
    if (checkpoint?.oldestEventAt) {
      const toTs = shiftSeconds(checkpoint.oldestEventAt, -1);
      const fromTs = addDays(checkpoint.oldestEventAt, -NEAR_INTENTS_WINDOW_DAYS);
      return {
        fromTs,
        toTs,
        startTimestampUnix: toUnixSeconds(fromTs),
        endTimestampUnix: toUnixSeconds(toTs),
      };
    }
    const bootstrapFrom = addDays(now, -NEAR_INTENTS_WINDOW_DAYS);
    return {
      fromTs: bootstrapFrom,
      toTs: now,
      startTimestampUnix: toUnixSeconds(bootstrapFrom),
      endTimestampUnix: toUnixSeconds(now),
    };
  }

  const bootstrapFrom = addDays(now, -NEAR_INTENTS_WINDOW_DAYS);
  return {
    fromTs: bootstrapFrom,
    toTs: now,
    startTimestampUnix: toUnixSeconds(bootstrapFrom),
    endTimestampUnix: toUnixSeconds(now),
  };
}

function buildNearIntentsWindowFromUnix(
  startTimestampUnix: number,
  endTimestampUnix: number,
): NearIntentsWindow {
  return {
    fromTs: new Date(startTimestampUnix * 1_000),
    toTs: new Date(endTimestampUnix * 1_000),
    startTimestampUnix,
    endTimestampUnix,
  };
}

function formatNearIntentsWindow(window: NearIntentsWindow): string {
  return `${window.startTimestampUnix ?? "na"}-${window.endTimestampUnix ?? "na"}`;
}

function splitNearIntentsWindowChunk(
  chunk: NearIntentsWindowChunk,
): { older: NearIntentsWindowChunk; newer: NearIntentsWindowChunk } | null {
  if (chunk.depth >= NEAR_INTENTS_MAX_WINDOW_SPLIT_DEPTH) {
    return null;
  }
  if (
    chunk.window.startTimestampUnix === null ||
    chunk.window.endTimestampUnix === null
  ) {
    return null;
  }

  const start = chunk.window.startTimestampUnix;
  const end = chunk.window.endTimestampUnix;
  if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start) {
    return null;
  }

  const spanSeconds = end - start + 1;
  if (spanSeconds <= NEAR_INTENTS_MIN_WINDOW_SPLIT_SECONDS) {
    return null;
  }

  const midpoint = Math.floor((start + end) / 2);
  if (midpoint <= start || midpoint >= end) {
    return null;
  }

  return {
    older: {
      window: buildNearIntentsWindowFromUnix(start, midpoint),
      depth: chunk.depth + 1,
    },
    newer: {
      window: buildNearIntentsWindowFromUnix(midpoint + 1, end),
      depth: chunk.depth + 1,
    },
  };
}

function buildNearIntentsQueryScopes(): NearIntentsQueryScope[] {
  const chains = getNearIntentsScopedChains();
  if (chains.length === 0) {
    throw new Error("Near Intents scoped ingestion has no mapped chains.");
  }

  const scopes: NearIntentsQueryScope[] = [];
  for (const fromChainId of chains) {
    for (const toChainId of chains) {
      scopes.push({
        key: `${fromChainId}->${toChainId}`,
        fromChainId,
        toChainId,
      });
    }
  }
  return scopes;
}

function buildNearIntentsPageUrl(input: NearIntentsPageFetchInput): string {
  const url = new URL(NEAR_INTENTS_SOURCE_ENDPOINT);
  url.searchParams.set("page", String(input.page));
  url.searchParams.set("perPage", String(input.perPage));
  url.searchParams.set("statuses", NEAR_INTENTS_DEFAULT_STATUSES);
  url.searchParams.set("showTestTxs", "false");
  url.searchParams.set("fromChainId", input.fromChainId);
  url.searchParams.set("toChainId", input.toChainId);
  if (input.window.startTimestampUnix !== null) {
    url.searchParams.set("startTimestampUnix", String(input.window.startTimestampUnix));
  }
  if (input.window.endTimestampUnix !== null) {
    url.searchParams.set("endTimestampUnix", String(input.window.endTimestampUnix));
  }
  return url.toString();
}

function getNearIntentsAuthToken(): string {
  const token = nonEmptyOrNull(Bun.env.NEAR_INTENTS_KEY ?? process.env.NEAR_INTENTS_KEY);
  if (!token) {
    throw new Error("Missing NEAR_INTENTS_KEY environment variable.");
  }
  return token;
}

async function fetchNearIntentsPage(
  input: NearIntentsPageFetchInput,
): Promise<NearIntentsTransactionsPagesResponse> {
  const url = buildNearIntentsPageUrl(input);
  return fetchJsonWithRetry<NearIntentsTransactionsPagesResponse>(
    url,
    {
      method: "GET",
      headers: {
        Accept: "application/json",
        Authorization: `Bearer ${input.authToken}`,
      },
    },
    {
      attempts: 5,
      initialDelayMs: NEAR_INTENTS_REQUEST_INTERVAL_MS,
      maxDelayMs: 30_000,
      timeoutMs: NEAR_INTENTS_REQUEST_TIMEOUT_MS,
    },
  );
}

async function fetchNearIntentsPageRateLimited(
  input: NearIntentsPageFetchInput,
  rateLimitState: NearIntentsRateLimitState,
): Promise<NearIntentsTransactionsPagesResponse> {
  const nowMs = Date.now();
  const waitMs = rateLimitState.nextRequestAtMs - nowMs;
  if (waitMs > 0) {
    await sleep(waitMs);
  }

  const requestStartedAtMs = Date.now();
  try {
    return await fetchNearIntentsPage(input);
  } finally {
    rateLimitState.nextRequestAtMs = requestStartedAtMs + NEAR_INTENTS_REQUEST_INTERVAL_MS;
  }
}

function deriveEventAt(transaction: NearIntentsTransaction): Date | null {
  const byCreatedAt = parseDateOrNull(transaction.createdAt);
  if (byCreatedAt) {
    return byCreatedAt;
  }
  return parseUnixSecondsOrNull(transaction.createdAtTimestamp);
}

function shouldIncludeByWindow(eventAt: Date | null, window: NearIntentsWindow): boolean {
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

function updateNewestBoundary(current: BoundaryPoint | null, candidate: BoundaryPoint): BoundaryPoint {
  if (!current) {
    return candidate;
  }
  if (candidate.eventAt.getTime() > current.eventAt.getTime()) {
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
  return current;
}

function deriveBackfillOldestBoundary(input: {
  scopeProgresses: NearIntentsScopeProgress[];
  window: NearIntentsWindow;
  fallbackOldestEventAt: Date | null;
  observedOldestBoundary: BoundaryPoint | null;
}): BoundaryPoint | null {
  let globalScopeFrontier: Date | null = null;

  for (const scopeProgress of input.scopeProgresses) {
    let scopeFrontier: Date | null = null;
    if (!scopeProgress.hitPageCap && input.window.fromTs) {
      scopeFrontier = input.window.fromTs;
    } else if (scopeProgress.oldestFetchedEventAt) {
      scopeFrontier = scopeProgress.oldestFetchedEventAt;
    } else {
      scopeFrontier = input.fallbackOldestEventAt;
    }

    if (!scopeFrontier) {
      return input.observedOldestBoundary;
    }
    if (!globalScopeFrontier || scopeFrontier > globalScopeFrontier) {
      globalScopeFrontier = scopeFrontier;
    }
  }

  if (!globalScopeFrontier) {
    return input.observedOldestBoundary;
  }

  return {
    eventAt: globalScopeFrontier,
    providerRecordId:
      input.observedOldestBoundary?.providerRecordId ??
      `scope-frontier:${globalScopeFrontier.toISOString()}`,
  };
}

function mergeCheckpoint(
  existing: IngestCheckpoint | null,
  runId: string,
  newest: BoundaryPoint | null,
  oldest: BoundaryPoint | null,
  mode: IngestMode,
  latestPage: number | null,
): IngestCheckpoint | null {
  if (!existing && !newest && !oldest) {
    return null;
  }

  const checkpoint: IngestCheckpoint = {
    streamKey: NEAR_INTENTS_STREAM_KEY,
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

  if (mode === "sync_newer") {
    checkpoint.newestCursor = toPageCursor(1);
  } else if (latestPage !== null) {
    const cursor = toPageCursor(latestPage);
    if (mode === "backfill_older") {
      checkpoint.oldestCursor = cursor;
    } else {
      checkpoint.newestCursor = cursor;
      checkpoint.oldestCursor = cursor;
    }
  }

  return checkpoint;
}

function isFinalNearIntentsStatus(status: string | null | undefined): boolean {
  const canonical = mapNearIntentsStatus(status ?? undefined);
  return canonical === "success" || canonical === "failed" || canonical === "refunded";
}

function normalizeNearIntentsTransaction(
  transaction: NearIntentsTransaction,
  runId: string,
  observedAt: Date,
  sourceCursor: string,
): NormalizedNearIntentsSwap | null {
  const providerRecordId =
    nonEmptyOrNull(transaction.depositAddressAndMemo) ??
    nonEmptyOrNull(
      `${nonEmptyOrNull(transaction.depositAddress) ?? ""}_${nonEmptyOrNull(transaction.depositMemo) ?? ""}`,
    );
  if (!providerRecordId) {
    return null;
  }

  const eventAt = deriveEventAt(transaction);
  const sourceAsset = parseNearIntentsAsset(transaction.originAsset);
  const destinationAsset = parseNearIntentsAsset(transaction.destinationAsset);
  const sourceChain = canonicalizeChain({
    provider: "nearintents",
    rawChain: sourceAsset?.chainRaw,
  });
  const destinationChain = canonicalizeChain({
    provider: "nearintents",
    rawChain: destinationAsset?.chainRaw,
  });

  const rawJson = JSON.stringify(transaction);
  const rawHash = sha256Hex(rawJson);
  const statusCanonical = mapNearIntentsStatus(transaction.status);
  const allTxHashes = [
    ...(transaction.nearTxHashes ?? []),
    ...(transaction.originChainTxHashes ?? []),
    ...(transaction.destinationChainTxHashes ?? []),
  ];
  const uniqueTxHashes = [...new Set(allTxHashes.filter((hash) => typeof hash === "string" && hash.length > 0))];

  const routeHint =
    sourceAsset?.chainRaw && destinationAsset?.chainRaw
      ? `${sourceAsset.chainRaw}->${destinationAsset.chainRaw}`
      : null;
  const failureReasonRaw =
    nonEmptyOrNull(transaction.refundReason) ??
    (statusCanonical === "failed" ? "failed" : null);
  const sourceAssetDecimals = inferTokenDecimals({
    provider: "nearintents",
    chainCanonical: sourceChain.canonical,
    symbol: sourceAsset?.symbol ?? null,
  });
  const destinationAssetDecimals = inferTokenDecimals({
    provider: "nearintents",
    chainCanonical: destinationChain.canonical,
    symbol: destinationAsset?.symbol ?? null,
  });
  const amountInAtomic = nonEmptyOrNull(transaction.amountIn);
  const amountOutAtomic = nonEmptyOrNull(transaction.amountOut);
  const amountInFromAtomic =
    amountInAtomic && sourceAssetDecimals !== null
      ? atomicToNormalized(amountInAtomic, sourceAssetDecimals)
      : null;
  const amountOutFromAtomic =
    amountOutAtomic && destinationAssetDecimals !== null
      ? atomicToNormalized(amountOutAtomic, destinationAssetDecimals)
      : null;

  const core: NormalizedSwapRowInput = {
    normalized_id: providerRecordId,
    provider_key: "nearintents",
    provider_record_id: providerRecordId,
    provider_parent_id: null,
    record_granularity: "transaction",
    status_canonical: statusCanonical,
    status_raw: transaction.status,
    failure_reason_raw: failureReasonRaw,
    created_at: eventAt,
    updated_at: null,
    event_at: eventAt,
    source_chain_canonical: sourceChain.canonical,
    destination_chain_canonical: destinationChain.canonical,
    source_chain_raw: sourceChain.rawChain,
    destination_chain_raw: destinationChain.rawChain,
    source_chain_id_raw: sourceChain.rawChainId,
    destination_chain_id_raw: destinationChain.rawChainId,
    source_asset_id: buildAssetId(sourceAsset, sourceChain.canonical),
    destination_asset_id: buildAssetId(destinationAsset, destinationChain.canonical),
    source_asset_symbol: sourceAsset?.symbol ?? null,
    destination_asset_symbol: destinationAsset?.symbol ?? null,
    source_asset_decimals: sourceAssetDecimals,
    destination_asset_decimals: destinationAssetDecimals,
    amount_in_atomic: amountInAtomic,
    amount_out_atomic: amountOutAtomic,
    amount_in_normalized: amountInFromAtomic ?? parseFloatOrNull(transaction.amountInFormatted),
    amount_out_normalized: amountOutFromAtomic ?? parseFloatOrNull(transaction.amountOutFormatted),
    amount_in_usd: parseFloatOrNull(transaction.amountInUsd),
    amount_out_usd: parseFloatOrNull(transaction.amountOutUsd),
    fee_atomic: null,
    fee_normalized: null,
    fee_usd: null,
    slippage_bps: null,
    solver_id: null,
    route_hint: routeHint,
    source_tx_hash:
      transaction.originChainTxHashes?.[0] ??
      transaction.nearTxHashes?.[0] ??
      null,
    destination_tx_hash: transaction.destinationChainTxHashes?.[0] ?? null,
    refund_tx_hash:
      statusCanonical === "refunded"
        ? (transaction.destinationChainTxHashes?.[0] ?? transaction.nearTxHashes?.[0] ?? null)
        : null,
    extra_tx_hashes: uniqueTxHashes.length > 0 ? uniqueTxHashes : null,
    is_final: isFinalNearIntentsStatus(transaction.status),
    raw_hash_latest: rawHash,
    source_endpoint: NEAR_INTENTS_SOURCE_ENDPOINT,
    ingested_at: observedAt,
    run_id: runId,
  };

  const raw: RawSwapRowInput = {
    normalized_id: providerRecordId,
    raw_hash: rawHash,
    raw_json: rawJson,
    observed_at: observedAt,
    source_endpoint: NEAR_INTENTS_SOURCE_ENDPOINT,
    source_cursor: sourceCursor,
    run_id: runId,
  };

  return { core, raw, eventAt };
}

async function ingestNearIntents(
  connection: DuckDBConnection,
  runId: string,
  mode: IngestMode,
  checkpoint: IngestCheckpoint | null,
  now: Date,
  pageSize: number,
  maxPages: number | null,
): Promise<ProviderIngestOutput> {
  const authToken = getNearIntentsAuthToken();
  const window = buildNearIntentsWindow(mode, checkpoint, now);
  const effectivePageSize = clampPageSize(pageSize);
  const queryScopes = buildNearIntentsQueryScopes();
  const rateLimitState: NearIntentsRateLimitState = { nextRequestAtMs: 0 };
  const maxPagesPerScope =
    maxPages === null
      ? null
      : Math.max(1, Math.floor(maxPages / queryScopes.length));

  let recordsFetched = 0;
  let recordsNormalized = 0;
  let recordsUpserted = 0;
  let newestBoundary: BoundaryPoint | null = null;
  let oldestBoundary: BoundaryPoint | null = null;
  const scopeProgresses: NearIntentsScopeProgress[] = [];
  for (const scope of queryScopes) {
    const windowQueue: NearIntentsWindowChunk[] = [{ window, depth: 0 }];
    let pageCountForScope = 0;
    let scopeOldestFetchedEventAt: Date | null = null;
    let scopeHitPageCap = false;

    while (windowQueue.length > 0) {
      const chunk = windowQueue.shift();
      if (!chunk) {
        break;
      }

      let page = 1;
      const seenPages = new Set<number>();
      while (true) {
        if (maxPagesPerScope !== null && pageCountForScope >= maxPagesPerScope) {
          scopeHitPageCap = true;
          windowQueue.length = 0;
          break;
        }
        if (seenPages.has(page)) {
          throw new Error(
            `Near Intents page loop detected for scope=${scope.key} window=${formatNearIntentsWindow(chunk.window)} page=${page}`,
          );
        }
        seenPages.add(page);

        let response: NearIntentsTransactionsPagesResponse;
        try {
          response = await fetchNearIntentsPageRateLimited(
            {
              authToken,
              page,
              perPage: effectivePageSize,
              window: chunk.window,
              fromChainId: scope.fromChainId,
              toChainId: scope.toChainId,
            },
            rateLimitState,
          );
        } catch (error) {
          const split = splitNearIntentsWindowChunk(chunk);
          if (!split) {
            throw new Error(
              `Near Intents request failed for scope=${scope.key} window=${formatNearIntentsWindow(chunk.window)} depth=${chunk.depth}: ${String(error)}`,
            );
          }

          if (mode === "sync_newer") {
            windowQueue.unshift(split.newer, split.older);
          } else {
            windowQueue.unshift(split.older, split.newer);
          }
          break;
        }

        const currentPage = response.page ?? page;
        const transactions = response.data ?? [];
        recordsFetched += transactions.length;
        pageCountForScope += 1;

        if (transactions.length === 0) {
          break;
        }

        const observedAt = new Date();
        const coreRows: NormalizedSwapRowInput[] = [];
        const rawRows: RawSwapRowInput[] = [];
        let pageOldestEventAt: Date | null = null;
        const sourceCursor = `scope:${scope.key}|window:${formatNearIntentsWindow(chunk.window)}|page:${page}`;

        for (const transaction of transactions) {
          const transactionEventAt = deriveEventAt(transaction);
          if (transactionEventAt && (!pageOldestEventAt || transactionEventAt < pageOldestEventAt)) {
            pageOldestEventAt = transactionEventAt;
          }
          if (
            transactionEventAt &&
            (!scopeOldestFetchedEventAt || transactionEventAt < scopeOldestFetchedEventAt)
          ) {
            scopeOldestFetchedEventAt = transactionEventAt;
          }

          const normalized = normalizeNearIntentsTransaction(transaction, runId, observedAt, sourceCursor);
          if (!normalized) {
            continue;
          }
          if (!shouldIncludeByWindow(normalized.eventAt, window)) {
            continue;
          }
          if (normalized.core.status_canonical !== "success") {
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

        if (chunk.window.fromTs && pageOldestEventAt && pageOldestEventAt < chunk.window.fromTs) {
          break;
        }

        const nextPage = response.nextPage ?? null;
        if (!nextPage) {
          break;
        }
        if (nextPage <= currentPage) {
          throw new Error(
            `Near Intents next page is invalid for scope=${scope.key} window=${formatNearIntentsWindow(chunk.window)} currentPage=${currentPage} nextPage=${nextPage}`,
          );
        }
        page = nextPage;
      }
    }

    scopeProgresses.push({
      oldestFetchedEventAt: scopeOldestFetchedEventAt,
      hitPageCap: scopeHitPageCap,
    });
  }

  const checkpointOldestBoundary =
    mode === "sync_newer"
      ? oldestBoundary
      : deriveBackfillOldestBoundary({
          scopeProgresses,
          window,
          fallbackOldestEventAt: checkpoint?.oldestEventAt ?? null,
          observedOldestBoundary: oldestBoundary,
        });

  return {
    recordsFetched,
    recordsNormalized,
    recordsUpserted,
    checkpoint: mergeCheckpoint(
      checkpoint,
      runId,
      newestBoundary,
      checkpointOldestBoundary,
      mode,
      null,
    ),
  };
}

export const nearintentsProviderAdapter: ProviderAdapter = {
  key: "nearintents",
  streamKey: NEAR_INTENTS_STREAM_KEY,
  sourceEndpoint: NEAR_INTENTS_SOURCE_ENDPOINT,
  async ingest(input: ProviderIngestInput): Promise<ProviderIngestOutput> {
    return ingestNearIntents(
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
