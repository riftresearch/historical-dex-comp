import type { DuckDBConnection } from "@duckdb/node-api";
import { canonicalizeChain } from "../../domain/chain-canonicalization";
import type { NormalizedSwapRowInput } from "../../domain/normalized-swap";
import { mapRelayStatus } from "../../domain/status";
import type { IngestMode } from "../../ingest/types";
import {
  getRelayScopedChainIds,
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
import type {
  RelayCurrency,
  RelayCurrencyAmount,
  RelayRequestData,
  RelayRequestRecord,
  RelayRequestTx,
  RelayRequestsPageResponse,
} from "./types";

const RELAY_SOURCE_ENDPOINT = "https://api.relay.link/requests/v2";
const RELAY_STREAM_KEY = "swaps:requests:v2";
const RELAY_WINDOW_DAYS = 30;
const RELAY_PAGE_SIZE_DEFAULT = 50;
const RELAY_PAGE_SIZE_MAX = 50;

interface RelayWindow {
  fromTs: Date | null;
  toTs: Date | null;
  startTimestamp: number | null;
  endTimestamp: number | null;
  sortDirection: "asc" | "desc";
}

interface RelayPageFetchInput {
  pageSize: number;
  continuation: string | null;
  window: RelayWindow;
  originChainId: string;
  destinationChainId: string;
}

interface RelayQueryScope {
  key: string;
  originChainId: string;
  destinationChainId: string;
}

interface BoundaryPoint {
  eventAt: Date;
  providerRecordId: string;
}

interface NormalizedRelaySwap {
  core: NormalizedSwapRowInput;
  raw: RawSwapRowInput;
  eventAt: Date | null;
}

function clampPageSize(requested: number): number {
  if (!Number.isFinite(requested) || requested <= 0) {
    return RELAY_PAGE_SIZE_DEFAULT;
  }
  return Math.min(Math.floor(requested), RELAY_PAGE_SIZE_MAX);
}

function buildRelayWindow(mode: IngestMode, checkpoint: IngestCheckpoint | null, now: Date): RelayWindow {
  if (mode === "sync_newer") {
    const fromTs = checkpoint?.newestEventAt ? shiftSeconds(checkpoint.newestEventAt, 1) : addDays(now, -30);
    const toTs = now;
    return {
      fromTs,
      toTs,
      startTimestamp: toUnixSeconds(fromTs),
      endTimestamp: toUnixSeconds(toTs),
      sortDirection: "asc",
    };
  }

  if (mode === "backfill_older") {
    if (checkpoint?.oldestEventAt) {
      const toTs = shiftSeconds(checkpoint.oldestEventAt, -1);
      const fromTs = addDays(checkpoint.oldestEventAt, -RELAY_WINDOW_DAYS);
      return {
        fromTs,
        toTs,
        startTimestamp: toUnixSeconds(fromTs),
        endTimestamp: toUnixSeconds(toTs),
        sortDirection: "desc",
      };
    }
    const fallbackFrom = addDays(now, -30);
    return {
      fromTs: fallbackFrom,
      toTs: now,
      startTimestamp: toUnixSeconds(fallbackFrom),
      endTimestamp: toUnixSeconds(now),
      sortDirection: "desc",
    };
  }

  const bootstrapFrom = addDays(now, -30);
  return {
    fromTs: bootstrapFrom,
    toTs: now,
    startTimestamp: toUnixSeconds(bootstrapFrom),
    endTimestamp: toUnixSeconds(now),
    sortDirection: "desc",
  };
}

function buildRelayQueryScopes(): RelayQueryScope[] {
  const chainIds = getRelayScopedChainIds();
  if (chainIds.length === 0) {
    throw new Error("Relay scoped ingestion has no mapped chain IDs.");
  }

  const scopes: RelayQueryScope[] = [];
  for (const originChainId of chainIds) {
    for (const destinationChainId of chainIds) {
      scopes.push({
        key: `${originChainId}->${destinationChainId}`,
        originChainId,
        destinationChainId,
      });
    }
  }
  return scopes;
}

function buildRelayPageUrl(input: RelayPageFetchInput): string {
  const url = new URL(RELAY_SOURCE_ENDPOINT);
  url.searchParams.set("limit", String(input.pageSize));
  url.searchParams.set("sortBy", "createdAt");
  url.searchParams.set("sortDirection", input.window.sortDirection);
  url.searchParams.set("originChainId", input.originChainId);
  url.searchParams.set("destinationChainId", input.destinationChainId);
  if (input.window.startTimestamp !== null) {
    url.searchParams.set("startTimestamp", String(input.window.startTimestamp));
  }
  if (input.window.endTimestamp !== null) {
    url.searchParams.set("endTimestamp", String(input.window.endTimestamp));
  }
  if (input.continuation) {
    url.searchParams.set("continuation", input.continuation);
  }
  return url.toString();
}

async function fetchRelayPage(input: RelayPageFetchInput): Promise<RelayRequestsPageResponse> {
  const url = buildRelayPageUrl(input);
  return fetchJsonWithRetry<RelayRequestsPageResponse>(
    url,
    {
      method: "GET",
      headers: {
        Accept: "application/json",
      },
    },
    {
      attempts: 5,
      initialDelayMs: 400,
      maxDelayMs: 4_000,
    },
  );
}

function firstTxWithHash(txs: RelayRequestTx[] | null | undefined): RelayRequestTx | null {
  if (!txs) {
    return null;
  }
  return txs.find((tx) => typeof tx.hash === "string" && tx.hash.length > 0) ?? null;
}

function firstCurrency(input: RelayCurrencyAmount | null | undefined): RelayCurrency | null {
  return input?.currency ?? null;
}

function fallbackCurrencyIn(data: RelayRequestData | null | undefined): RelayCurrencyAmount | null {
  return data?.metadata?.currencyIn ?? data?.metadata?.route?.origin?.inputCurrency ?? null;
}

function fallbackCurrencyOut(data: RelayRequestData | null | undefined): RelayCurrencyAmount | null {
  return data?.metadata?.currencyOut ?? data?.metadata?.route?.destination?.outputCurrency ?? null;
}

function currencyAssetId(currency: RelayCurrency | null): string | null {
  if (!currency) {
    return null;
  }
  const chain = currency.chainId !== null && currency.chainId !== undefined ? String(currency.chainId) : "unknown";
  const address = currency.address ?? "native";
  return `${chain}:${address.toLowerCase()}`;
}

function deriveSourceChainId(
  data: RelayRequestData | null | undefined,
  inTx: RelayRequestTx | null,
): string | number | null {
  const fromCurrency = firstCurrency(fallbackCurrencyIn(data))?.chainId;
  if (fromCurrency !== undefined && fromCurrency !== null) {
    return fromCurrency;
  }
  return inTx?.chainId ?? null;
}

function deriveDestinationChainId(
  data: RelayRequestData | null | undefined,
  outTx: RelayRequestTx | null,
): string | number | null {
  const fromCurrency = firstCurrency(fallbackCurrencyOut(data))?.chainId;
  if (fromCurrency !== undefined && fromCurrency !== null) {
    return fromCurrency;
  }
  return outTx?.chainId ?? null;
}

function earliestTxTimestamp(txs: RelayRequestTx[] | null | undefined): Date | null {
  let earliest: Date | null = null;
  for (const tx of txs ?? []) {
    const candidate = parseUnixSecondsOrNull(tx.timestamp);
    if (!candidate) {
      continue;
    }
    if (!earliest || candidate < earliest) {
      earliest = candidate;
    }
  }
  return earliest;
}

function deriveEventAt(record: RelayRequestRecord): Date | null {
  const byCreated = parseDateOrNull(record.createdAt);
  if (byCreated) {
    return byCreated;
  }

  const byInTx = earliestTxTimestamp(record.data?.inTxs);
  if (byInTx) {
    return byInTx;
  }

  return earliestTxTimestamp(record.data?.outTxs);
}

function routeHintFromRecord(record: RelayRequestRecord): string | null {
  const originRouter = record.data?.metadata?.route?.origin?.router ?? null;
  const destinationRouter = record.data?.metadata?.route?.destination?.router ?? null;
  if (!originRouter && !destinationRouter) {
    return null;
  }
  return `${originRouter ?? "na"}->${destinationRouter ?? "na"}`;
}

function collectTxHashes(data: RelayRequestData | null | undefined): string[] {
  const hashes: string[] = [];
  for (const tx of data?.inTxs ?? []) {
    if (tx.hash) {
      hashes.push(tx.hash);
    }
  }
  for (const tx of data?.outTxs ?? []) {
    if (tx.hash) {
      hashes.push(tx.hash);
    }
  }
  return [...new Set(hashes)];
}

function isFinalRelayStatus(status: string | null | undefined): boolean {
  const normalized = mapRelayStatus(status ?? undefined);
  return normalized === "success" || normalized === "failed" || normalized === "refunded" || normalized === "expired";
}

function normalizeRelayRecord(
  record: RelayRequestRecord,
  runId: string,
  observedAt: Date,
  sourceCursor: string | null,
): NormalizedRelaySwap | null {
  if (!record.id) {
    return null;
  }

  const inTx = firstTxWithHash(record.data?.inTxs);
  const outTx = firstTxWithHash(record.data?.outTxs);
  const currencyIn = fallbackCurrencyIn(record.data);
  const currencyOut = fallbackCurrencyOut(record.data);
  const sourceChain = canonicalizeChain({
    provider: "relay",
    rawChainId: deriveSourceChainId(record.data, inTx),
  });
  const destinationChain = canonicalizeChain({
    provider: "relay",
    rawChainId: deriveDestinationChainId(record.data, outTx),
  });

  const eventAt = deriveEventAt(record);
  const rawJson = JSON.stringify(record);
  const rawHash = sha256Hex(rawJson);
  const failureReason =
    record.data?.failReason ??
    record.data?.refundFailReason ??
    (mapRelayStatus(record.status ?? undefined) === "failed" ? "unknown" : null);
  const txHashes = collectTxHashes(record.data);
  const refundTxHash =
    record.data?.outTxs?.find((tx) => tx.type?.toLowerCase() === "refund")?.hash ?? null;
  const sourceAsset = firstCurrency(currencyIn);
  const destinationAsset = firstCurrency(currencyOut);
  const sourceAssetDecimals = inferTokenDecimals({
    provider: "relay",
    explicitDecimals: sourceAsset?.decimals ?? null,
    chainCanonical: sourceChain.canonical,
    symbol: sourceAsset?.symbol ?? null,
  });
  const destinationAssetDecimals = inferTokenDecimals({
    provider: "relay",
    explicitDecimals: destinationAsset?.decimals ?? null,
    chainCanonical: destinationChain.canonical,
    symbol: destinationAsset?.symbol ?? null,
  });
  const amountInAtomic = currencyIn?.amount ?? null;
  const amountOutAtomic = currencyOut?.amount ?? null;
  const amountInFromAtomic =
    amountInAtomic && sourceAssetDecimals !== null
      ? atomicToNormalized(amountInAtomic, sourceAssetDecimals)
      : null;
  const amountOutFromAtomic =
    amountOutAtomic && destinationAssetDecimals !== null
      ? atomicToNormalized(amountOutAtomic, destinationAssetDecimals)
      : null;

  const core: NormalizedSwapRowInput = {
    normalized_id: record.id,
    provider_key: "relay",
    provider_record_id: record.id,
    provider_parent_id: null,
    record_granularity: "request",
    status_canonical: mapRelayStatus(record.status ?? undefined),
    status_raw: record.status ?? null,
    failure_reason_raw: failureReason,
    created_at: parseDateOrNull(record.createdAt),
    updated_at: parseDateOrNull(record.updatedAt),
    event_at: eventAt,
    source_chain_canonical: sourceChain.canonical,
    destination_chain_canonical: destinationChain.canonical,
    source_chain_raw: sourceChain.rawChain,
    destination_chain_raw: destinationChain.rawChain,
    source_chain_id_raw: sourceChain.rawChainId,
    destination_chain_id_raw: destinationChain.rawChainId,
    source_asset_id: currencyAssetId(sourceAsset),
    destination_asset_id: currencyAssetId(destinationAsset),
    source_asset_symbol: sourceAsset?.symbol ?? null,
    destination_asset_symbol: destinationAsset?.symbol ?? null,
    source_asset_decimals: sourceAssetDecimals,
    destination_asset_decimals: destinationAssetDecimals,
    amount_in_atomic: amountInAtomic,
    amount_out_atomic: amountOutAtomic,
    amount_in_normalized: amountInFromAtomic ?? parseFloatOrNull(currencyIn?.amountFormatted),
    amount_out_normalized: amountOutFromAtomic ?? parseFloatOrNull(currencyOut?.amountFormatted),
    amount_in_usd: parseFloatOrNull(currencyIn?.amountUsd),
    amount_out_usd: parseFloatOrNull(currencyOut?.amountUsd),
    fee_atomic: inTx?.fee ?? null,
    fee_normalized: null,
    fee_usd: parseFloatOrNull(record.data?.feesUsd),
    slippage_bps: parseFloatOrNull(record.data?.slippageTolerance),
    solver_id: record.data?.metadata?.route?.destination?.router ?? null,
    route_hint: routeHintFromRecord(record),
    source_tx_hash: inTx?.hash ?? null,
    destination_tx_hash: outTx?.hash ?? null,
    refund_tx_hash: refundTxHash,
    extra_tx_hashes: txHashes.length > 0 ? txHashes : null,
    is_final: isFinalRelayStatus(record.status),
    raw_hash_latest: rawHash,
    source_endpoint: RELAY_SOURCE_ENDPOINT,
    ingested_at: observedAt,
    run_id: runId,
  };

  const raw: RawSwapRowInput = {
    normalized_id: record.id,
    raw_hash: rawHash,
    raw_json: rawJson,
    observed_at: observedAt,
    source_endpoint: RELAY_SOURCE_ENDPOINT,
    source_cursor: sourceCursor,
    run_id: runId,
  };

  return { core, raw, eventAt };
}

function updateNewestBoundary(
  current: BoundaryPoint | null,
  candidate: BoundaryPoint,
): BoundaryPoint {
  if (!current) {
    return candidate;
  }
  if (candidate.eventAt.getTime() > current.eventAt.getTime()) {
    return candidate;
  }
  return current;
}

function updateOldestBoundary(
  current: BoundaryPoint | null,
  candidate: BoundaryPoint,
): BoundaryPoint {
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
): IngestCheckpoint | null {
  if (!existing && !newest && !oldest) {
    return null;
  }

  const checkpoint: IngestCheckpoint = {
    streamKey: RELAY_STREAM_KEY,
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

function shouldIncludeByWindow(eventAt: Date | null, window: RelayWindow): boolean {
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

async function ingestRelay(
  connection: DuckDBConnection,
  runId: string,
  mode: IngestMode,
  checkpoint: IngestCheckpoint | null,
  now: Date,
  pageSize: number,
  maxPages: number | null,
): Promise<ProviderIngestOutput> {
  const window = buildRelayWindow(mode, checkpoint, now);
  const effectivePageSize = clampPageSize(pageSize);
  const queryScopes = buildRelayQueryScopes();
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
    let continuation: string | null = null;
    let pageCount = 0;
    const seenContinuations = new Set<string>();

    while (true) {
      if (maxPagesPerScope !== null && pageCount >= maxPagesPerScope) {
        break;
      }

      const page = await fetchRelayPage({
        pageSize: effectivePageSize,
        continuation,
        window,
        originChainId: scope.originChainId,
        destinationChainId: scope.destinationChainId,
      });
      const requests = page.requests ?? [];
      recordsFetched += requests.length;

      if (requests.length === 0) {
        break;
      }

      const observedAt = new Date();
      const coreRows: NormalizedSwapRowInput[] = [];
      const rawRows: RawSwapRowInput[] = [];
      const sourceCursor = `scope:${scope.key}|continuation:${continuation ?? "start"}`;

      for (const request of requests) {
        const normalized = normalizeRelayRecord(request, runId, observedAt, sourceCursor);
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

      pageCount += 1;
      continuation = page.continuation ?? null;
      if (!continuation) {
        break;
      }
      if (seenContinuations.has(continuation)) {
        throw new Error(`Relay continuation loop detected for scope=${scope.key} continuation=${continuation}`);
      }
      seenContinuations.add(continuation);
    }
  }

  return {
    recordsFetched,
    recordsNormalized,
    recordsUpserted,
    checkpoint: mergeCheckpoint(checkpoint, runId, newestBoundary, oldestBoundary),
  };
}

export const relayProviderAdapter: ProviderAdapter = {
  key: "relay",
  streamKey: RELAY_STREAM_KEY,
  sourceEndpoint: RELAY_SOURCE_ENDPOINT,
  async ingest(input: ProviderIngestInput): Promise<ProviderIngestOutput> {
    return ingestRelay(
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
