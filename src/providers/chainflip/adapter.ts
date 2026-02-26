import type { DuckDBConnection } from "@duckdb/node-api";
import { canonicalizeChain } from "../../domain/chain-canonicalization";
import type { NormalizedSwapRowInput } from "../../domain/normalized-swap";
import type { CanonicalStatus } from "../../domain/status";
import type { IngestMode } from "../../ingest/types";
import {
  getChainflipScopedAssets,
  getChainflipScopedChains,
  isSwapRowWithinIngestScope,
} from "../../ingest/swap-scope";
import { fetchJsonWithRetry } from "../../lib/http";
import type { IngestCheckpoint } from "../../storage/repositories/ingest-checkpoints";
import { persistSwaps, type RawSwapRowInput } from "../../storage/repositories/swaps";
import { sha256Hex } from "../../utils/hash";
import { atomicToNormalized, inferTokenDecimals } from "../../utils/amount-normalization";
import { parseFloatOrNull } from "../../utils/number";
import { addDays, parseDateOrNull, shiftSeconds } from "../../utils/time";
import type { ProviderAdapter, ProviderIngestInput, ProviderIngestOutput } from "../types";
import type {
  ChainflipConnection,
  ChainflipGraphqlResponse,
  ChainflipPageInfo,
  ChainflipSwapNode,
  ChainflipSwapRequestNode,
  ChainflipSwapRequestsPageData,
} from "./types";

const CHAINFLIP_SOURCE_ENDPOINT = "https://explorer-service-processor.chainflip.io/graphql";
const CHAINFLIP_STREAM_KEY = "swaps:swap_requests:graphql";
const CHAINFLIP_WINDOW_DAYS = 30;
const CHAINFLIP_PAGE_SIZE_DEFAULT = 20;
const CHAINFLIP_PAGE_SIZE_MAX = 20;
const CHAINFLIP_REQUEST_TX_REFS_LIMIT = 10;
const CHAINFLIP_BROADCAST_TX_REFS_LIMIT = 10;
const CHAINFLIP_NESTED_SWAPS_LIMIT = 60;

interface ChainflipWindow {
  fromTs: Date;
  toTs: Date;
}

interface ChainflipPageFetchInput {
  first: number;
  after: string | null;
  window: ChainflipWindow;
  allowedChains: readonly string[];
  allowedAssets: readonly string[];
}

interface BoundaryPoint {
  eventAt: Date;
  providerRecordId: string;
}

interface RequestTxRefs {
  sourceTxHash: string | null;
  destinationTxHash: string | null;
  refundTxHash: string | null;
  extraTxHashes: string[] | null;
}

interface NormalizedChainflipRequest {
  coreRows: NormalizedSwapRowInput[];
  rawRows: RawSwapRowInput[];
  requestEventAt: Date | null;
  requestRecordId: string;
}

const CHAINFLIP_SWAP_REQUESTS_QUERY = `
query ChainflipSwapRequests(
  $first: Int!
  $after: Cursor
  $from: Datetime!
  $to: Datetime!
  $allowedChains: [ChainflipChain!]
  $allowedAssets: [ChainflipAsset!]
  $requestTxRefsLimit: Int!
  $broadcastTxRefsLimit: Int!
  $nestedSwapsLimit: Int!
) {
  allSwapRequests(
    first: $first
    after: $after
    orderBy: ID_DESC
    filter: {
      requestedBlockTimestamp: {
        greaterThanOrEqualTo: $from
        lessThanOrEqualTo: $to
      }
      sourceChain: { in: $allowedChains }
      destinationChain: { in: $allowedChains }
      sourceAsset: { in: $allowedAssets }
      destinationAsset: { in: $allowedAssets }
    }
  ) {
    nodes {
      id
      nativeId
      type
      sourceChain
      destinationChain
      sourceAsset
      destinationAsset
      sourceAddress
      destinationAddress
      depositAmount
      depositValueUsd
      requestedBlockTimestamp
      completedBlockTimestamp
      brokerId
      totalBrokerCommissionBps
      dcaNumberOfChunks
      isLiquidation
      egressId
      refundEgressId
      fallbackEgressId
      transactionRefsBySwapRequestId(first: $requestTxRefsLimit, orderBy: ID_ASC) {
        nodes {
          id
          ref
        }
      }
      egressByEgressId {
        id
        chain
        asset
        amount
        valueUsd
        broadcastByBroadcastId {
          id
          nativeId
          destinationChain
          transactionRefsByBroadcastId(first: $broadcastTxRefsLimit, orderBy: ID_ASC) {
            nodes {
              id
              ref
            }
          }
        }
      }
      egressByRefundEgressId {
        id
        chain
        asset
        amount
        valueUsd
        broadcastByBroadcastId {
          id
          nativeId
          destinationChain
          transactionRefsByBroadcastId(first: $broadcastTxRefsLimit, orderBy: ID_ASC) {
            nodes {
              id
              ref
            }
          }
        }
      }
      swapsBySwapRequestId(first: $nestedSwapsLimit, orderBy: ID_ASC) {
        nodes {
          id
          nativeId
          type
          swapRequestId
          sourceChain
          destinationChain
          sourceAsset
          destinationAsset
          swapInputAmount
          swapOutputAmount
          swapInputValueUsd
          swapOutputValueUsd
          swapScheduledBlockTimestamp
          swapExecutedBlockTimestamp
          swapAbortedBlockTimestamp
          retryCount
          swapAbortedReason
          swapRescheduledReason
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
`;

function nonEmptyOrNull(value: unknown): string | null {
  if (value === null || value === undefined) {
    return null;
  }
  const trimmed = (typeof value === "string" ? value : String(value)).trim();
  return trimmed.length > 0 ? trimmed : null;
}

function clampPageSize(pageSize: number): number {
  if (!Number.isFinite(pageSize) || pageSize <= 0) {
    return CHAINFLIP_PAGE_SIZE_DEFAULT;
  }
  return Math.min(Math.floor(pageSize), CHAINFLIP_PAGE_SIZE_MAX);
}

function buildChainflipWindow(mode: IngestMode, checkpoint: IngestCheckpoint | null, now: Date): ChainflipWindow {
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
      return {
        fromTs: addDays(checkpoint.oldestEventAt, -CHAINFLIP_WINDOW_DAYS),
        toTs,
      };
    }
    return {
      fromTs: addDays(now, -30),
      toTs: now,
    };
  }

  return {
    fromTs: addDays(now, -30),
    toTs: now,
  };
}

function normalizeConnectionNodes<TNode>(connection: ChainflipConnection<TNode> | null | undefined): TNode[] {
  return connection?.nodes ?? [];
}

function extractGraphqlData(
  response: ChainflipGraphqlResponse<ChainflipSwapRequestsPageData>,
): ChainflipSwapRequestsPageData {
  if (response.errors && response.errors.length > 0) {
    const message = response.errors.map((error) => error.message).join(" | ");
    throw new Error(`Chainflip GraphQL returned errors: ${message}`);
  }
  if (!response.data) {
    throw new Error("Chainflip GraphQL response missing data.");
  }
  return response.data;
}

async function fetchChainflipPage(input: ChainflipPageFetchInput): Promise<ChainflipSwapRequestsPageData> {
  const payload = {
    query: CHAINFLIP_SWAP_REQUESTS_QUERY,
    variables: {
      first: input.first,
      after: input.after,
      from: input.window.fromTs.toISOString(),
      to: input.window.toTs.toISOString(),
      allowedChains: input.allowedChains,
      allowedAssets: input.allowedAssets,
      requestTxRefsLimit: CHAINFLIP_REQUEST_TX_REFS_LIMIT,
      broadcastTxRefsLimit: CHAINFLIP_BROADCAST_TX_REFS_LIMIT,
      nestedSwapsLimit: CHAINFLIP_NESTED_SWAPS_LIMIT,
    },
  };

  const response = await fetchJsonWithRetry<ChainflipGraphqlResponse<ChainflipSwapRequestsPageData>>(
    CHAINFLIP_SOURCE_ENDPOINT,
    {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    },
    {
      attempts: 5,
      initialDelayMs: 400,
      maxDelayMs: 4_000,
      timeoutMs: 30_000,
    },
  );

  return extractGraphqlData(response);
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
): IngestCheckpoint | null {
  if (!existing && !newest && !oldest) {
    return null;
  }

  const checkpoint: IngestCheckpoint = {
    streamKey: CHAINFLIP_STREAM_KEY,
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

function makeAssetId(chainCanonical: string | null, symbol: string | null): string | null {
  const normalizedSymbol = nonEmptyOrNull(symbol)?.toLowerCase();
  if (!normalizedSymbol) {
    return null;
  }
  if (!chainCanonical) {
    return normalizedSymbol;
  }
  return `${chainCanonical}:${normalizedSymbol}`;
}

function firstAbortReason(swaps: readonly ChainflipSwapNode[]): string | null {
  for (const swap of swaps) {
    const reason = nonEmptyOrNull(swap.swapAbortedReason);
    if (reason) {
      return reason;
    }
  }
  return null;
}

function firstSuccessfulSwap(swaps: readonly ChainflipSwapNode[]): ChainflipSwapNode | null {
  for (const swap of swaps) {
    if (swap.swapExecutedBlockTimestamp && !swap.swapAbortedReason) {
      return swap;
    }
  }
  return null;
}

function earliestSwapScheduledAt(swaps: readonly ChainflipSwapNode[]): Date | null {
  let earliest: Date | null = null;
  for (const swap of swaps) {
    const scheduledAt = parseDateOrNull(swap.swapScheduledBlockTimestamp);
    if (!scheduledAt) {
      continue;
    }
    if (!earliest || scheduledAt < earliest) {
      earliest = scheduledAt;
    }
  }
  return earliest;
}

function deriveRequestEventAt(request: ChainflipSwapRequestNode, swaps: readonly ChainflipSwapNode[]): Date | null {
  return parseDateOrNull(request.requestedBlockTimestamp) ?? earliestSwapScheduledAt(swaps);
}

function inferChainflipRequestStatus(request: ChainflipSwapRequestNode): CanonicalStatus {
  if (request.refundEgressId !== null && request.refundEgressId !== undefined) {
    return "refunded";
  }
  if (request.completedBlockTimestamp) {
    return "success";
  }
  return "processing";
}

function inferChainflipSwapStatus(swap: ChainflipSwapNode): CanonicalStatus {
  if (swap.swapAbortedReason) {
    return "failed";
  }
  if (swap.swapExecutedBlockTimestamp) {
    return "success";
  }
  if (swap.swapScheduledBlockTimestamp) {
    return "processing";
  }
  return "unknown";
}

function collectRefs(values: readonly (string | null | undefined)[]): string[] {
  const output: string[] = [];
  const seen = new Set<string>();
  for (const value of values) {
    const normalized = nonEmptyOrNull(value);
    if (!normalized || seen.has(normalized)) {
      continue;
    }
    seen.add(normalized);
    output.push(normalized);
  }
  return output;
}

function deriveRequestTxRefs(request: ChainflipSwapRequestNode): RequestTxRefs {
  const requestRefs = collectRefs(
    normalizeConnectionNodes(request.transactionRefsBySwapRequestId).map((row) => row.ref),
  );
  const destinationRefs = collectRefs(
    normalizeConnectionNodes(
      request.egressByEgressId?.broadcastByBroadcastId?.transactionRefsByBroadcastId,
    ).map((row) => row.ref),
  );
  const refundRefs = collectRefs(
    normalizeConnectionNodes(
      request.egressByRefundEgressId?.broadcastByBroadcastId?.transactionRefsByBroadcastId,
    ).map((row) => row.ref),
  );
  const all = collectRefs([...requestRefs, ...destinationRefs, ...refundRefs]);

  return {
    sourceTxHash: requestRefs[0] ?? null,
    destinationTxHash: destinationRefs[0] ?? null,
    refundTxHash: refundRefs[0] ?? null,
    extraTxHashes: all.length > 0 ? all : null,
  };
}

function normalizeSwapRequest(
  request: ChainflipSwapRequestNode,
  runId: string,
  observedAt: Date,
  sourceCursor: string | null,
): NormalizedChainflipRequest | null {
  const requestId = String(request.id);
  const normalizedRequestId = `swap_request:${requestId}`;
  const swaps = normalizeConnectionNodes(request.swapsBySwapRequestId);
  const requestStatus = inferChainflipRequestStatus(request);

  const requestEventAt = deriveRequestEventAt(request, swaps);
  const requestCreatedAt = parseDateOrNull(request.requestedBlockTimestamp) ?? requestEventAt;
  const requestUpdatedAt = parseDateOrNull(request.completedBlockTimestamp);

  const sourceChain = canonicalizeChain({
    provider: "chainflip",
    rawChain: request.sourceChain,
  });
  const destinationChain = canonicalizeChain({
    provider: "chainflip",
    rawChain: request.destinationChain,
  });
  const requestTxRefs = deriveRequestTxRefs(request);
  const successfulSwap = firstSuccessfulSwap(swaps);
  const requestFailureReason = firstAbortReason(swaps);

  const requestAmountOutAtomic =
    request.egressByEgressId?.amount ??
    successfulSwap?.swapOutputAmount ??
    null;
  const requestAmountOutUsd =
    parseFloatOrNull(request.egressByEgressId?.valueUsd) ??
    parseFloatOrNull(successfulSwap?.swapOutputValueUsd);
  const requestSourceAssetSymbol = nonEmptyOrNull(request.sourceAsset);
  const requestDestinationAssetSymbol = nonEmptyOrNull(request.destinationAsset);
  const requestSourceAssetDecimals = inferTokenDecimals({
    provider: "chainflip",
    chainCanonical: sourceChain.canonical,
    symbol: requestSourceAssetSymbol,
  });
  const requestDestinationAssetDecimals = inferTokenDecimals({
    provider: "chainflip",
    chainCanonical: destinationChain.canonical,
    symbol: requestDestinationAssetSymbol,
  });
  const requestAmountInAtomic = nonEmptyOrNull(request.depositAmount);
  const requestAmountOutAtomicNormalized = nonEmptyOrNull(requestAmountOutAtomic);
  const requestAmountInNormalized =
    requestAmountInAtomic && requestSourceAssetDecimals !== null
      ? atomicToNormalized(requestAmountInAtomic, requestSourceAssetDecimals)
      : null;
  const requestAmountOutNormalized =
    requestAmountOutAtomicNormalized && requestDestinationAssetDecimals !== null
      ? atomicToNormalized(requestAmountOutAtomicNormalized, requestDestinationAssetDecimals)
      : null;

  const routeHintParts: string[] = [];
  if (request.sourceChain || request.destinationChain) {
    routeHintParts.push(
      `${request.sourceChain ?? "na"}->${request.destinationChain ?? "na"}`,
    );
  }
  if (request.type) {
    routeHintParts.push(`type=${request.type}`);
  }
  if (request.dcaNumberOfChunks !== null && request.dcaNumberOfChunks !== undefined) {
    routeHintParts.push(`dca_chunks=${request.dcaNumberOfChunks}`);
  }
  if (request.isLiquidation !== null && request.isLiquidation !== undefined) {
    routeHintParts.push(`liquidation=${request.isLiquidation ? "true" : "false"}`);
  }
  if (request.totalBrokerCommissionBps !== null && request.totalBrokerCommissionBps !== undefined) {
    routeHintParts.push(`broker_commission_bps=${request.totalBrokerCommissionBps}`);
  }
  const requestRouteHint = routeHintParts.length > 0 ? routeHintParts.join("|") : null;

  const requestRawJson = JSON.stringify(request);
  const requestRawHash = sha256Hex(requestRawJson);

  const requestCore: NormalizedSwapRowInput = {
    normalized_id: normalizedRequestId,
    provider_key: "chainflip",
    provider_record_id: requestId,
    provider_parent_id: null,
    record_granularity: "swap_request",
    status_canonical: requestStatus,
    status_raw: request.type ?? null,
    failure_reason_raw: requestFailureReason,
    created_at: requestCreatedAt,
    updated_at: requestUpdatedAt,
    event_at: requestEventAt,
    source_chain_canonical: sourceChain.canonical,
    destination_chain_canonical: destinationChain.canonical,
    source_chain_raw: sourceChain.rawChain,
    destination_chain_raw: destinationChain.rawChain,
    source_chain_id_raw: sourceChain.rawChainId,
    destination_chain_id_raw: destinationChain.rawChainId,
    source_asset_id: makeAssetId(sourceChain.canonical, requestSourceAssetSymbol),
    destination_asset_id: makeAssetId(destinationChain.canonical, requestDestinationAssetSymbol),
    source_asset_symbol: requestSourceAssetSymbol,
    destination_asset_symbol: requestDestinationAssetSymbol,
    source_asset_decimals: requestSourceAssetDecimals,
    destination_asset_decimals: requestDestinationAssetDecimals,
    amount_in_atomic: requestAmountInAtomic,
    amount_out_atomic: requestAmountOutAtomicNormalized,
    amount_in_normalized: requestAmountInNormalized,
    amount_out_normalized: requestAmountOutNormalized,
    amount_in_usd: parseFloatOrNull(request.depositValueUsd),
    amount_out_usd: requestAmountOutUsd,
    fee_atomic: null,
    fee_normalized: null,
    fee_usd: null,
    slippage_bps: null,
    solver_id: nonEmptyOrNull(request.brokerId),
    route_hint: requestRouteHint,
    source_tx_hash: requestTxRefs.sourceTxHash,
    destination_tx_hash: requestTxRefs.destinationTxHash,
    refund_tx_hash: requestTxRefs.refundTxHash,
    extra_tx_hashes: requestTxRefs.extraTxHashes,
    is_final: requestStatus === "success" || requestStatus === "refunded",
    raw_hash_latest: requestRawHash,
    source_endpoint: CHAINFLIP_SOURCE_ENDPOINT,
    ingested_at: observedAt,
    run_id: runId,
  };

  const requestRaw: RawSwapRowInput = {
    normalized_id: normalizedRequestId,
    raw_hash: requestRawHash,
    raw_json: requestRawJson,
    observed_at: observedAt,
    source_endpoint: CHAINFLIP_SOURCE_ENDPOINT,
    source_cursor: sourceCursor,
    run_id: runId,
  };

  const coreRows: NormalizedSwapRowInput[] = [];
  const rawRows: RawSwapRowInput[] = [];
  if (requestCore.status_canonical === "success" && isSwapRowWithinIngestScope(requestCore)) {
    coreRows.push(requestCore);
    rawRows.push(requestRaw);
  }

  for (const swap of swaps) {
    const swapId = String(swap.id);
    const normalizedSwapId = `swap:${swapId}`;
    const swapStatus = inferChainflipSwapStatus(swap);
    const swapScheduledAt = parseDateOrNull(swap.swapScheduledBlockTimestamp);
    const swapEventAt = swapScheduledAt ?? requestEventAt;

    const swapSourceChain = canonicalizeChain({
      provider: "chainflip",
      rawChain: swap.sourceChain ?? request.sourceChain,
    });
    const swapDestinationChain = canonicalizeChain({
      provider: "chainflip",
      rawChain: swap.destinationChain ?? request.destinationChain,
    });

    const swapRouteHintParts: string[] = [];
    if (swap.type) {
      swapRouteHintParts.push(`type=${swap.type}`);
    }
    if (swap.retryCount !== null && swap.retryCount !== undefined) {
      swapRouteHintParts.push(`retry_count=${swap.retryCount}`);
    }
    if (swap.swapRescheduledReason) {
      swapRouteHintParts.push(`rescheduled=${swap.swapRescheduledReason}`);
    }
    const swapRouteHint = swapRouteHintParts.length > 0 ? swapRouteHintParts.join("|") : requestRouteHint;
    const swapSourceAssetSymbol = nonEmptyOrNull(swap.sourceAsset ?? request.sourceAsset);
    const swapDestinationAssetSymbol = nonEmptyOrNull(swap.destinationAsset ?? request.destinationAsset);
    const swapSourceAssetDecimals = inferTokenDecimals({
      provider: "chainflip",
      chainCanonical: swapSourceChain.canonical,
      symbol: swapSourceAssetSymbol,
    });
    const swapDestinationAssetDecimals = inferTokenDecimals({
      provider: "chainflip",
      chainCanonical: swapDestinationChain.canonical,
      symbol: swapDestinationAssetSymbol,
    });
    const swapAmountInAtomic = nonEmptyOrNull(swap.swapInputAmount);
    const swapAmountOutAtomic = nonEmptyOrNull(swap.swapOutputAmount);
    const swapAmountInNormalized =
      swapAmountInAtomic && swapSourceAssetDecimals !== null
        ? atomicToNormalized(swapAmountInAtomic, swapSourceAssetDecimals)
        : null;
    const swapAmountOutNormalized =
      swapAmountOutAtomic && swapDestinationAssetDecimals !== null
        ? atomicToNormalized(swapAmountOutAtomic, swapDestinationAssetDecimals)
        : null;

    const swapRawPayload = {
      swapRequestId: request.id,
      swapRequestNativeId: request.nativeId,
      swap,
    };
    const swapRawJson = JSON.stringify(swapRawPayload);
    const swapRawHash = sha256Hex(swapRawJson);

    const swapCore: NormalizedSwapRowInput = {
      normalized_id: normalizedSwapId,
      provider_key: "chainflip",
      provider_record_id: swapId,
      provider_parent_id: normalizedRequestId,
      record_granularity: "swap",
      status_canonical: swapStatus,
      status_raw: swap.type ?? null,
      failure_reason_raw: nonEmptyOrNull(swap.swapAbortedReason),
      created_at: swapScheduledAt ?? requestCreatedAt,
      updated_at:
        parseDateOrNull(swap.swapExecutedBlockTimestamp) ??
        parseDateOrNull(swap.swapAbortedBlockTimestamp),
      event_at: swapEventAt,
      source_chain_canonical: swapSourceChain.canonical,
      destination_chain_canonical: swapDestinationChain.canonical,
      source_chain_raw: swapSourceChain.rawChain,
      destination_chain_raw: swapDestinationChain.rawChain,
      source_chain_id_raw: swapSourceChain.rawChainId,
      destination_chain_id_raw: swapDestinationChain.rawChainId,
      source_asset_id: makeAssetId(
        swapSourceChain.canonical,
        swapSourceAssetSymbol,
      ),
      destination_asset_id: makeAssetId(
        swapDestinationChain.canonical,
        swapDestinationAssetSymbol,
      ),
      source_asset_symbol: swapSourceAssetSymbol,
      destination_asset_symbol: swapDestinationAssetSymbol,
      source_asset_decimals: swapSourceAssetDecimals,
      destination_asset_decimals: swapDestinationAssetDecimals,
      amount_in_atomic: swapAmountInAtomic,
      amount_out_atomic: swapAmountOutAtomic,
      amount_in_normalized: swapAmountInNormalized,
      amount_out_normalized: swapAmountOutNormalized,
      amount_in_usd: parseFloatOrNull(swap.swapInputValueUsd),
      amount_out_usd: parseFloatOrNull(swap.swapOutputValueUsd),
      fee_atomic: null,
      fee_normalized: null,
      fee_usd: null,
      slippage_bps: null,
      solver_id: nonEmptyOrNull(request.brokerId),
      route_hint: swapRouteHint,
      source_tx_hash: requestTxRefs.sourceTxHash,
      destination_tx_hash: requestTxRefs.destinationTxHash,
      refund_tx_hash: requestTxRefs.refundTxHash,
      extra_tx_hashes: requestTxRefs.extraTxHashes,
      is_final: swapStatus === "success" || swapStatus === "failed",
      raw_hash_latest: swapRawHash,
      source_endpoint: CHAINFLIP_SOURCE_ENDPOINT,
      ingested_at: observedAt,
      run_id: runId,
    };

    const swapRaw: RawSwapRowInput = {
      normalized_id: normalizedSwapId,
      raw_hash: swapRawHash,
      raw_json: swapRawJson,
      observed_at: observedAt,
      source_endpoint: CHAINFLIP_SOURCE_ENDPOINT,
      source_cursor: sourceCursor,
      run_id: runId,
    };

    if (swapCore.status_canonical === "success" && isSwapRowWithinIngestScope(swapCore)) {
      coreRows.push(swapCore);
      rawRows.push(swapRaw);
    }
  }

  if (coreRows.length === 0) {
    return null;
  }

  return {
    coreRows,
    rawRows,
    requestEventAt,
    requestRecordId: requestId,
  };
}

function shouldIncludeByWindow(eventAt: Date | null, window: ChainflipWindow): boolean {
  if (!eventAt) {
    return true;
  }
  if (eventAt < window.fromTs) {
    return false;
  }
  if (eventAt > window.toTs) {
    return false;
  }
  return true;
}

function normalizePageInfo(connection: ChainflipConnection<ChainflipSwapRequestNode> | undefined): ChainflipPageInfo {
  return {
    hasNextPage: connection?.pageInfo?.hasNextPage ?? false,
    endCursor: connection?.pageInfo?.endCursor ?? null,
  };
}

async function ingestChainflip(
  connection: DuckDBConnection,
  runId: string,
  mode: IngestMode,
  checkpoint: IngestCheckpoint | null,
  now: Date,
  pageSize: number,
  maxPages: number | null,
): Promise<ProviderIngestOutput> {
  const window = buildChainflipWindow(mode, checkpoint, now);
  const effectivePageSize = clampPageSize(pageSize);
  const allowedChains = getChainflipScopedChains();
  const allowedAssets = getChainflipScopedAssets();
  if (allowedChains.length === 0) {
    throw new Error("Chainflip scoped ingestion has no mapped chains.");
  }
  if (allowedAssets.length === 0) {
    throw new Error("Chainflip scoped ingestion has no mapped assets.");
  }

  let continuation: string | null = null;
  let pageCount = 0;
  let recordsFetched = 0;
  let recordsNormalized = 0;
  let recordsUpserted = 0;
  let newestBoundary: BoundaryPoint | null = null;
  let oldestBoundary: BoundaryPoint | null = null;
  const seenCursors = new Set<string>();

  while (true) {
    if (maxPages !== null && pageCount >= maxPages) {
      break;
    }

    const pageData = await fetchChainflipPage({
      first: effectivePageSize,
      after: continuation,
      window,
      allowedChains,
      allowedAssets,
    });

    const connectionData = pageData.allSwapRequests;
    const requests = normalizeConnectionNodes(connectionData);
    const pageInfo = normalizePageInfo(connectionData);
    recordsFetched += requests.length;

    if (requests.length === 0) {
      break;
    }

    const observedAt = new Date();
    const coreRows: NormalizedSwapRowInput[] = [];
    const rawRows: RawSwapRowInput[] = [];
    const sourceCursor = continuation ? `cursor:${continuation}` : "cursor:start";

    for (const request of requests) {
      const normalized = normalizeSwapRequest(request, runId, observedAt, sourceCursor);
      if (!normalized) {
        continue;
      }
      if (!shouldIncludeByWindow(normalized.requestEventAt, window)) {
        continue;
      }

      coreRows.push(...normalized.coreRows);
      rawRows.push(...normalized.rawRows);
      recordsNormalized += normalized.coreRows.length;

      if (normalized.requestEventAt) {
        const point: BoundaryPoint = {
          eventAt: normalized.requestEventAt,
          providerRecordId: normalized.requestRecordId,
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
    if (!pageInfo.hasNextPage || !pageInfo.endCursor) {
      break;
    }
    if (seenCursors.has(pageInfo.endCursor)) {
      throw new Error(`Chainflip cursor loop detected at cursor=${pageInfo.endCursor}`);
    }
    seenCursors.add(pageInfo.endCursor);
    continuation = pageInfo.endCursor;
  }

  return {
    recordsFetched,
    recordsNormalized,
    recordsUpserted,
    checkpoint: mergeCheckpoint(checkpoint, runId, newestBoundary, oldestBoundary),
  };
}

export const chainflipProviderAdapter: ProviderAdapter = {
  key: "chainflip",
  streamKey: CHAINFLIP_STREAM_KEY,
  sourceEndpoint: CHAINFLIP_SOURCE_ENDPOINT,
  async ingest(input: ProviderIngestInput): Promise<ProviderIngestOutput> {
    return ingestChainflip(
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
