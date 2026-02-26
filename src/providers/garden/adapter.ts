import type { DuckDBConnection } from "@duckdb/node-api";
import { canonicalizeChain } from "../../domain/chain-canonicalization";
import type { NormalizedSwapRowInput } from "../../domain/normalized-swap";
import type { CanonicalStatus } from "../../domain/status";
import { mapGardenStatus } from "../../domain/status";
import type { IngestMode } from "../../ingest/types";
import {
  getGardenScopedChains,
  isSwapRowWithinIngestScope,
} from "../../ingest/swap-scope";
import { fetchJsonWithRetry } from "../../lib/http";
import type { IngestCheckpoint } from "../../storage/repositories/ingest-checkpoints";
import { persistSwaps, type RawSwapRowInput } from "../../storage/repositories/swaps";
import { sha256Hex } from "../../utils/hash";
import { parseFloatOrNull } from "../../utils/number";
import { addDays, parseDateOrNull, shiftSeconds } from "../../utils/time";
import type { ProviderAdapter, ProviderIngestInput, ProviderIngestOutput } from "../types";
import type { GardenAffiliateFee, GardenOrder, GardenOrdersResponse, GardenSwap } from "./types";

const GARDEN_BASE_URL = "https://api.garden.finance/v2";
const GARDEN_SOURCE_ENDPOINT = `${GARDEN_BASE_URL}/orders`;
const GARDEN_STREAM_KEY = "swaps:orders:v2";
const GARDEN_DEFAULT_STATUSES = "completed";
const GARDEN_WINDOW_DAYS = 30;
const GARDEN_PAGE_SIZE_DEFAULT = 50;
const GARDEN_PAGE_SIZE_MAX = 100;

interface GardenWindow {
  fromTs: Date | null;
  toTs: Date | null;
}

interface GardenPageFetchInput {
  appId: string;
  page: number;
  perPage: number;
  fromChain: string;
  toChain: string;
}

interface GardenQueryScope {
  key: string;
  fromChain: string;
  toChain: string;
}

interface BoundaryPoint {
  eventAt: Date;
  providerRecordId: string;
}

interface ParsedGardenAsset {
  raw: string;
  chainRaw: string | null;
  tokenRaw: string | null;
  symbol: string | null;
}

interface NormalizedGardenSwap {
  core: NormalizedSwapRowInput;
  raw: RawSwapRowInput;
  eventAt: Date | null;
}

const GARDEN_DECIMALS_BY_ASSET_ID: Record<string, number> = {
  "arbitrum:ibtc": 8,
  "arbitrum:wbtc": 8,
  "base:cbbtc": 8,
  "bitcoin:btc": 8,
  "bnbchain:btcb": 8,
  "botanix:btc": 8,
  "citrea:cbtc": 8,
  "ethereum:cbbtc": 8,
  "ethereum:usdc": 6,
  "ethereum:usdt": 6,
  "ethereum:wbtc": 8,
  "hyperevm:ubtc": 8,
  "megaeth:btc.b": 8,
  "monad:usdc": 6,
  "solana:cbbtc": 8,
  "solana:usdc": 6,
  "starknet:wbtc": 8,
};

const GARDEN_DECIMALS_BY_SYMBOL: Record<string, number> = {
  BBTC: 8,
  BTC: 8,
  BTCB: 8,
  CBBTC: 8,
  CBTC: 8,
  DAI: 18,
  ETH: 18,
  IBTC: 8,
  SOL: 9,
  TBTC: 8,
  UBTC: 8,
  USDC: 6,
  USDT: 6,
  WBTC: 8,
  WETH: 18,
};

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

function nonEmptyOrNull(value: unknown): string | null {
  if (value === null || value === undefined) {
    return null;
  }
  const trimmed = (typeof value === "string" ? value : String(value)).trim();
  return trimmed.length > 0 ? trimmed : null;
}

function clampPageSize(pageSize: number): number {
  if (!Number.isFinite(pageSize) || pageSize <= 0) {
    return GARDEN_PAGE_SIZE_DEFAULT;
  }
  return Math.min(Math.floor(pageSize), GARDEN_PAGE_SIZE_MAX);
}

function getGardenAppId(): string {
  const appId = nonEmptyOrNull(Bun.env.GARDEN_APP_ID ?? process.env.GARDEN_APP_ID);
  if (!appId) {
    throw new Error("Missing GARDEN_APP_ID environment variable.");
  }
  return appId;
}

function buildGardenWindow(mode: IngestMode, checkpoint: IngestCheckpoint | null, now: Date): GardenWindow {
  if (mode === "sync_newer") {
    const fromTs = checkpoint?.newestEventAt ? shiftSeconds(checkpoint.newestEventAt, 1) : addDays(now, -30);
    return {
      fromTs,
      toTs: now,
    };
  }

  if (mode === "backfill_older") {
    if (checkpoint?.oldestEventAt) {
      const toTs = shiftSeconds(checkpoint.oldestEventAt, -1);
      const fromTs = addDays(checkpoint.oldestEventAt, -GARDEN_WINDOW_DAYS);
      return {
        fromTs,
        toTs,
      };
    }
    const bootstrapFrom = addDays(now, -GARDEN_WINDOW_DAYS);
    return {
      fromTs: bootstrapFrom,
      toTs: now,
    };
  }

  const bootstrapFrom = addDays(now, -GARDEN_WINDOW_DAYS);
  return {
    fromTs: bootstrapFrom,
    toTs: now,
  };
}

function buildGardenQueryScopes(): GardenQueryScope[] {
  const chains = getGardenScopedChains();
  if (chains.length === 0) {
    throw new Error("Garden scoped ingestion has no mapped chains.");
  }

  const scopes: GardenQueryScope[] = [];
  for (const fromChain of chains) {
    for (const toChain of chains) {
      scopes.push({
        key: `${fromChain}->${toChain}`,
        fromChain,
        toChain,
      });
    }
  }
  return scopes;
}

function buildGardenPageUrl(input: GardenPageFetchInput): string {
  const url = new URL(GARDEN_SOURCE_ENDPOINT);
  url.searchParams.set("page", String(input.page));
  url.searchParams.set("per_page", String(input.perPage));
  url.searchParams.set("status", GARDEN_DEFAULT_STATUSES);
  url.searchParams.set("from_chain", input.fromChain);
  url.searchParams.set("to_chain", input.toChain);
  return url.toString();
}

function toPositiveIntegerOrNull(value: unknown): number | null {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return null;
  }
  const normalized = Math.floor(value);
  if (normalized <= 0) {
    return null;
  }
  return normalized;
}

function extractGardenOrders(url: string, response: GardenOrdersResponse): GardenOrder[] {
  const responseStatus = nonEmptyOrNull(response.status)?.toLowerCase();
  if (responseStatus === "error") {
    throw new Error(`Garden API error for ${url}: ${response.error ?? "unknown error"}`);
  }
  return response.result?.data ?? [];
}

function extractGardenCurrentPage(response: GardenOrdersResponse, fallback: number): number {
  const page = toPositiveIntegerOrNull(response.result?.page);
  return page ?? fallback;
}

function extractGardenTotalPages(response: GardenOrdersResponse): number | null {
  return toPositiveIntegerOrNull(response.result?.total_pages);
}

async function fetchGardenPage(input: GardenPageFetchInput): Promise<GardenOrdersResponse> {
  const url = buildGardenPageUrl(input);
  const response = await fetchJsonWithRetry<GardenOrdersResponse>(
    url,
    {
      method: "GET",
      headers: {
        Accept: "application/json",
        "garden-app-id": input.appId,
      },
    },
    {
      attempts: 5,
      initialDelayMs: 500,
      maxDelayMs: 5_000,
      timeoutMs: 20_000,
    },
  );

  const responseStatus = nonEmptyOrNull(response.status)?.toLowerCase();
  if (responseStatus === "error") {
    throw new Error(`Garden API error for ${url}: ${response.error ?? "unknown error"}`);
  }

  return response;
}

function inferSymbol(tokenRaw: string | null): string | null {
  if (!tokenRaw) {
    return null;
  }
  const cleaned = tokenRaw.trim();
  if (!/^[A-Za-z0-9._-]{2,24}$/.test(cleaned)) {
    return null;
  }
  return cleaned.toUpperCase();
}

function parseGardenAsset(rawAsset: string | null | undefined): ParsedGardenAsset | null {
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

  const chainRaw = nonEmptyOrNull(raw.slice(0, firstColonIndex));
  const tokenRaw = nonEmptyOrNull(raw.slice(firstColonIndex + 1));

  return {
    raw,
    chainRaw,
    tokenRaw,
    symbol: inferSymbol(tokenRaw),
  };
}

function canonicalizeSymbolForDecimals(symbol: string | null): string[] {
  const normalized = nonEmptyOrNull(symbol)?.toUpperCase() ?? null;
  if (!normalized) {
    return [];
  }
  const compact = normalized.replace(/[^A-Z0-9]/g, "");
  const candidates = new Set<string>([normalized, compact]);
  if (compact.startsWith("W") && compact.length > 3) {
    candidates.add(compact.slice(1));
  }
  if (compact.endsWith("B") && compact.length > 4 && compact.startsWith("BTC")) {
    candidates.add("BTC");
  }
  return [...candidates].filter((candidate) => candidate.length > 0);
}

function inferGardenAssetDecimals(
  asset: ParsedGardenAsset | null,
  chainCanonical: string | null,
): number | null {
  const explicitAssetKey = nonEmptyOrNull(asset?.raw)?.toLowerCase() ?? null;
  if (explicitAssetKey && Object.prototype.hasOwnProperty.call(GARDEN_DECIMALS_BY_ASSET_ID, explicitAssetKey)) {
    return GARDEN_DECIMALS_BY_ASSET_ID[explicitAssetKey] ?? null;
  }

  const symbolCandidates = canonicalizeSymbolForDecimals(asset?.symbol ?? null);
  for (const candidate of symbolCandidates) {
    const fromSymbol = GARDEN_DECIMALS_BY_SYMBOL[candidate];
    if (fromSymbol !== undefined) {
      return fromSymbol;
    }
  }

  if (chainCanonical === "bitcoin:mainnet") {
    return 8;
  }
  return null;
}

function parseIntegerString(value: string | null): string | null {
  const normalized = nonEmptyOrNull(value);
  if (!normalized) {
    return null;
  }
  if (!/^-?[0-9]+$/.test(normalized)) {
    return null;
  }
  return normalized;
}

function atomicToNormalized(atomic: string, decimals: number): number | null {
  if (!Number.isInteger(decimals) || decimals < 0 || decimals > 36) {
    return null;
  }

  const parsed = parseIntegerString(atomic);
  if (!parsed) {
    return null;
  }

  const isNegative = parsed.startsWith("-");
  const digitsRaw = isNegative ? parsed.slice(1) : parsed;
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
    fracPart = digits.padStart(decimals, "0");
  }
  fracPart = fracPart.replace(/0+$/, "");
  const text = `${isNegative ? "-" : ""}${intPart}${fracPart ? `.${fracPart}` : ""}`;
  const normalized = Number(text);
  if (!Number.isFinite(normalized)) {
    return null;
  }
  return normalized;
}

function buildAssetId(asset: ParsedGardenAsset | null, chainCanonical: string | null): string | null {
  if (!asset) {
    return null;
  }
  if (!chainCanonical) {
    return asset.raw.toLowerCase();
  }
  const tokenPart = (asset.tokenRaw ?? asset.raw).toLowerCase();
  return `${chainCanonical}:${tokenPart}`;
}

function collectSwapTxHashes(swap: GardenSwap | null | undefined): string[] {
  const hashes = [
    nonEmptyOrNull(swap?.initiate_tx_hash),
    nonEmptyOrNull(swap?.redeem_tx_hash),
    nonEmptyOrNull(swap?.refund_tx_hash),
  ];
  return hashes.filter((hash): hash is string => hash !== null);
}

function collectOrderTxHashes(order: GardenOrder): string[] {
  const all = [...collectSwapTxHashes(order.source_swap), ...collectSwapTxHashes(order.destination_swap)];
  return [...new Set(all)];
}

function deriveOrderCreatedAt(order: GardenOrder): Date | null {
  return (
    parseDateOrNull(order.created_at) ??
    parseDateOrNull(order.source_swap?.created_at) ??
    parseDateOrNull(order.destination_swap?.created_at) ??
    parseDateOrNull(order.source_swap?.initiate_timestamp) ??
    null
  );
}

function latestDate(candidates: Array<Date | null>): Date | null {
  let latest: Date | null = null;
  for (const candidate of candidates) {
    if (!candidate) {
      continue;
    }
    if (!latest || candidate > latest) {
      latest = candidate;
    }
  }
  return latest;
}

function deriveOrderUpdatedAt(order: GardenOrder): Date | null {
  const candidates: Array<Date | null> = [
    parseDateOrNull(order.source_swap?.initiate_timestamp),
    parseDateOrNull(order.source_swap?.redeem_timestamp),
    parseDateOrNull(order.source_swap?.refund_timestamp),
    parseDateOrNull(order.destination_swap?.initiate_timestamp),
    parseDateOrNull(order.destination_swap?.redeem_timestamp),
    parseDateOrNull(order.destination_swap?.refund_timestamp),
  ];
  return latestDate(candidates);
}

function deriveOrderRawStatus(order: GardenOrder): string | null {
  const orderStatus = nonEmptyOrNull(order.status);
  if (orderStatus) {
    return orderStatus;
  }
  const sourceStatus = nonEmptyOrNull(order.source_swap?.status);
  if (sourceStatus) {
    return sourceStatus;
  }
  const destinationStatus = nonEmptyOrNull(order.destination_swap?.status);
  return destinationStatus;
}

function inferGardenStatus(order: GardenOrder, rawStatus: string | null): CanonicalStatus {
  const mapped = mapGardenStatus(rawStatus ?? undefined);
  if (mapped !== "unknown") {
    return mapped;
  }

  if (nonEmptyOrNull(order.source_swap?.refund_tx_hash) || parseDateOrNull(order.source_swap?.refund_timestamp)) {
    return "refunded";
  }

  if (
    nonEmptyOrNull(order.destination_swap?.redeem_tx_hash) ||
    parseDateOrNull(order.destination_swap?.redeem_timestamp)
  ) {
    return "success";
  }

  if (
    nonEmptyOrNull(order.source_swap?.initiate_tx_hash) ||
    parseDateOrNull(order.source_swap?.initiate_timestamp)
  ) {
    return "processing";
  }

  return "pending";
}

function isFinalGardenStatus(status: CanonicalStatus): boolean {
  return status === "success" || status === "refunded" || status === "failed" || status === "expired";
}

function sumAffiliateFeeBps(fees: GardenAffiliateFee[] | null | undefined): number | null {
  let total = 0;
  let seenAny = false;
  for (const fee of fees ?? []) {
    const parsed = Number(fee.fee);
    if (!Number.isFinite(parsed)) {
      continue;
    }
    total += parsed;
    seenAny = true;
  }
  return seenAny ? total : null;
}

function computeUsd(amountNormalized: number | null, unitPrice: number | null): number | null {
  if (amountNormalized === null || unitPrice === null) {
    return null;
  }
  const usd = amountNormalized * unitPrice;
  return Number.isFinite(usd) ? usd : null;
}

function routeHintFromOrder(order: GardenOrder): string | null {
  const parts: string[] = [];

  const sourceChainRaw = nonEmptyOrNull(order.source_swap?.chain);
  const destinationChainRaw = nonEmptyOrNull(order.destination_swap?.chain);
  if (sourceChainRaw || destinationChainRaw) {
    parts.push(`${sourceChainRaw ?? "na"}->${destinationChainRaw ?? "na"}`);
  }

  const integrator = nonEmptyOrNull(order.integrator);
  if (integrator) {
    parts.push(`integrator=${integrator}`);
  }

  const version = nonEmptyOrNull(order.version);
  if (version) {
    parts.push(`version=${version}`);
  }

  const affiliateFeeBps = sumAffiliateFeeBps(order.affiliate_fees);
  if (affiliateFeeBps !== null) {
    parts.push(`affiliate_fee_bps=${affiliateFeeBps}`);
  }

  return parts.length > 0 ? parts.join("|") : null;
}

function normalizeGardenOrder(
  order: GardenOrder,
  runId: string,
  observedAt: Date,
  sourceCursor: string,
): NormalizedGardenSwap | null {
  const providerRecordId =
    nonEmptyOrNull(order.order_id) ??
    nonEmptyOrNull(order.source_swap?.swap_id) ??
    nonEmptyOrNull(order.destination_swap?.swap_id);
  if (!providerRecordId) {
    return null;
  }

  const sourceAsset = parseGardenAsset(order.source_swap?.asset);
  const destinationAsset = parseGardenAsset(order.destination_swap?.asset);

  const sourceChain = canonicalizeChain({
    provider: "garden",
    rawChain: nonEmptyOrNull(order.source_swap?.chain) ?? sourceAsset?.chainRaw,
  });
  const destinationChain = canonicalizeChain({
    provider: "garden",
    rawChain: nonEmptyOrNull(order.destination_swap?.chain) ?? destinationAsset?.chainRaw,
  });

  const createdAt = deriveOrderCreatedAt(order);
  const updatedAt = deriveOrderUpdatedAt(order);
  const eventAt = createdAt;

  const rawStatus = deriveOrderRawStatus(order);
  const statusCanonical = inferGardenStatus(order, rawStatus);

  const amountInAtomic = nonEmptyOrNull(order.source_swap?.amount) ?? nonEmptyOrNull(order.source_swap?.filled_amount);
  const amountOutAtomic =
    nonEmptyOrNull(order.destination_swap?.filled_amount) ?? nonEmptyOrNull(order.destination_swap?.amount);
  const sourceAssetDecimals = inferGardenAssetDecimals(sourceAsset, sourceChain.canonical);
  const destinationAssetDecimals = inferGardenAssetDecimals(destinationAsset, destinationChain.canonical);

  const amountInFromAtomic =
    amountInAtomic && sourceAssetDecimals !== null
      ? atomicToNormalized(amountInAtomic, sourceAssetDecimals)
      : null;
  const amountOutFromAtomic =
    amountOutAtomic && destinationAssetDecimals !== null
      ? atomicToNormalized(amountOutAtomic, destinationAssetDecimals)
      : null;

  const amountInNormalized =
    amountInFromAtomic ??
    parseFloatOrNull(order.source_swap?.filled_amount ?? order.source_swap?.amount);
  const amountOutNormalized =
    amountOutFromAtomic ??
    parseFloatOrNull(order.destination_swap?.filled_amount ?? order.destination_swap?.amount);

  const sourceAssetPrice = parseFloatOrNull(order.source_swap?.asset_price);
  const destinationAssetPrice = parseFloatOrNull(order.destination_swap?.asset_price);

  const amountInUsd = computeUsd(amountInNormalized, sourceAssetPrice);
  const amountOutUsd = computeUsd(amountOutNormalized, destinationAssetPrice);

  const affiliateFeeBps = sumAffiliateFeeBps(order.affiliate_fees);
  const routeHint = routeHintFromOrder(order);

  const sourceTxHash = nonEmptyOrNull(order.source_swap?.initiate_tx_hash);
  const destinationTxHash =
    nonEmptyOrNull(order.destination_swap?.redeem_tx_hash) ?? nonEmptyOrNull(order.source_swap?.redeem_tx_hash);
  const refundTxHash =
    nonEmptyOrNull(order.source_swap?.refund_tx_hash) ?? nonEmptyOrNull(order.destination_swap?.refund_tx_hash);

  const txHashes = collectOrderTxHashes(order);

  const failureReasonRaw =
    statusCanonical === "expired"
      ? "expired"
      : statusCanonical === "failed"
        ? "failed"
        : null;

  const rawJson = JSON.stringify(order);
  const rawHash = sha256Hex(rawJson);

  const core: NormalizedSwapRowInput = {
    normalized_id: providerRecordId,
    provider_key: "garden",
    provider_record_id: providerRecordId,
    provider_parent_id: null,
    record_granularity: "order",
    status_canonical: statusCanonical,
    status_raw: rawStatus,
    failure_reason_raw: failureReasonRaw,
    created_at: createdAt,
    updated_at: updatedAt,
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
    amount_in_normalized: amountInNormalized,
    amount_out_normalized: amountOutNormalized,
    amount_in_usd: amountInUsd,
    amount_out_usd: amountOutUsd,
    fee_atomic: affiliateFeeBps === null ? null : String(affiliateFeeBps),
    fee_normalized: affiliateFeeBps === null ? null : affiliateFeeBps / 10_000,
    fee_usd: null,
    slippage_bps: null,
    solver_id: nonEmptyOrNull(order.solver_id),
    route_hint: routeHint,
    source_tx_hash: sourceTxHash,
    destination_tx_hash: destinationTxHash,
    refund_tx_hash: refundTxHash,
    extra_tx_hashes: txHashes.length > 0 ? txHashes : null,
    is_final: isFinalGardenStatus(statusCanonical),
    raw_hash_latest: rawHash,
    source_endpoint: GARDEN_SOURCE_ENDPOINT,
    ingested_at: observedAt,
    run_id: runId,
  };

  const raw: RawSwapRowInput = {
    normalized_id: providerRecordId,
    raw_hash: rawHash,
    raw_json: rawJson,
    observed_at: observedAt,
    source_endpoint: GARDEN_SOURCE_ENDPOINT,
    source_cursor: sourceCursor,
    run_id: runId,
  };

  return { core, raw, eventAt };
}

function shouldIncludeByWindow(eventAt: Date | null, window: GardenWindow): boolean {
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
    streamKey: GARDEN_STREAM_KEY,
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

async function ingestGarden(
  connection: DuckDBConnection,
  runId: string,
  mode: IngestMode,
  checkpoint: IngestCheckpoint | null,
  now: Date,
  pageSize: number,
  maxPages: number | null,
): Promise<ProviderIngestOutput> {
  const appId = getGardenAppId();
  const window = buildGardenWindow(mode, checkpoint, now);
  const effectivePageSize = clampPageSize(pageSize);
  const queryScopes = buildGardenQueryScopes();
  const maxPagesPerScope =
    maxPages === null
      ? null
      : Math.max(1, Math.floor(maxPages / queryScopes.length));

  let recordsFetched = 0;
  let recordsNormalized = 0;
  let recordsUpserted = 0;
  let newestBoundary: BoundaryPoint | null = null;
  let oldestBoundary: BoundaryPoint | null = null;
  for (const scope of queryScopes) {
    let page = 1;
    let pageCount = 0;
    const seenPages = new Set<number>();

    while (true) {
      if (maxPagesPerScope !== null && pageCount >= maxPagesPerScope) {
        break;
      }
      if (seenPages.has(page)) {
        throw new Error(`Garden page loop detected for scope=${scope.key} page=${page}`);
      }
      seenPages.add(page);

      const response = await fetchGardenPage({
        appId,
        page,
        perPage: effectivePageSize,
        fromChain: scope.fromChain,
        toChain: scope.toChain,
      });
      const currentPage = extractGardenCurrentPage(response, page);
      const pageInput: GardenPageFetchInput = {
        appId,
        page,
        perPage: effectivePageSize,
        fromChain: scope.fromChain,
        toChain: scope.toChain,
      };

      const orders = extractGardenOrders(buildGardenPageUrl(pageInput), response);
      recordsFetched += orders.length;

      if (orders.length === 0) {
        break;
      }

      const observedAt = new Date();
      const coreRows: NormalizedSwapRowInput[] = [];
      const rawRows: RawSwapRowInput[] = [];
      let pageOldestEventAt: Date | null = null;
      const sourceCursor = `scope:${scope.key}|page:${page}`;

      for (const order of orders) {
        const normalized = normalizeGardenOrder(order, runId, observedAt, sourceCursor);
        if (!normalized) {
          continue;
        }

        if (!shouldIncludeByWindow(normalized.eventAt, window)) {
          if (normalized.eventAt && (!pageOldestEventAt || normalized.eventAt < pageOldestEventAt)) {
            pageOldestEventAt = normalized.eventAt;
          }
          continue;
        }
        if (normalized.core.status_canonical !== "success") {
          if (normalized.eventAt && (!pageOldestEventAt || normalized.eventAt < pageOldestEventAt)) {
            pageOldestEventAt = normalized.eventAt;
          }
          continue;
        }
        if (!isSwapRowWithinIngestScope(normalized.core)) {
          if (normalized.eventAt && (!pageOldestEventAt || normalized.eventAt < pageOldestEventAt)) {
            pageOldestEventAt = normalized.eventAt;
          }
          continue;
        }

        coreRows.push(normalized.core);
        rawRows.push(normalized.raw);
        recordsNormalized += 1;

        if (normalized.eventAt) {
          if (!pageOldestEventAt || normalized.eventAt < pageOldestEventAt) {
            pageOldestEventAt = normalized.eventAt;
          }

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

      pageCount += 1;

      if (window.fromTs && pageOldestEventAt && pageOldestEventAt < window.fromTs) {
        break;
      }

      const totalPages = extractGardenTotalPages(response);
      if (totalPages !== null && currentPage >= totalPages) {
        break;
      }

      if (totalPages === null && orders.length < effectivePageSize) {
        break;
      }

      const nextPage = currentPage + 1;
      if (nextPage <= currentPage) {
        throw new Error(
          `Garden next page value is invalid for scope=${scope.key} currentPage=${currentPage} nextPage=${nextPage}`,
        );
      }

      page = nextPage;
    }
  }

  return {
    recordsFetched,
    recordsNormalized,
    recordsUpserted,
    checkpoint: mergeCheckpoint(
      checkpoint,
      runId,
      newestBoundary,
      oldestBoundary,
      mode,
      null,
    ),
  };
}

export const gardenProviderAdapter: ProviderAdapter = {
  key: "garden",
  streamKey: GARDEN_STREAM_KEY,
  sourceEndpoint: GARDEN_SOURCE_ENDPOINT,
  async ingest(input: ProviderIngestInput): Promise<ProviderIngestOutput> {
    return ingestGarden(
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
