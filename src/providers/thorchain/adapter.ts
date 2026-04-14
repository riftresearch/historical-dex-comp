import type { DuckDBConnection } from "@duckdb/node-api";
import { canonicalizeChain } from "../../domain/chain-canonicalization";
import type { NormalizedSwapRowInput } from "../../domain/normalized-swap";
import { mapThorchainStatus } from "../../domain/status";
import type { IngestMode } from "../../ingest/types";
import { isSwapRowWithinIngestScope } from "../../ingest/swap-scope";
import { fetchJsonWithRetry } from "../../lib/http";
import type { IngestCheckpoint } from "../../storage/repositories/ingest-checkpoints";
import { persistSwaps, type RawSwapRowInput } from "../../storage/repositories/swaps";
import { sha256Hex } from "../../utils/hash";
import { atomicToNormalized, inferTokenDecimals } from "../../utils/amount-normalization";
import { parseFloatOrNull } from "../../utils/number";
import { addDays, shiftSeconds, toUnixSeconds } from "../../utils/time";
import type { ProviderAdapter, ProviderIngestInput, ProviderIngestOutput } from "../types";
import type {
  ThorchainAction,
  ThorchainActionTransfer,
  ThorchainCoin,
  ThorchainActionsPageResponse,
} from "./types";

const THORCHAIN_BASE_URL = "https://midgard.ninerealms.com";
const THORCHAIN_SOURCE_ENDPOINT = `${THORCHAIN_BASE_URL}/v2/actions?type=swap&asset=nosynth`;
const THORCHAIN_STREAM_KEY = "swaps:actions:v2";
const THORCHAIN_WINDOW_DAYS = 30;
const THORCHAIN_PAGE_SIZE_DEFAULT = 50;
const THORCHAIN_PAGE_SIZE_MAX = 50;

interface ThorchainWindow {
  fromTs: Date | null;
  toTs: Date | null;
  timestamp: number | null;
}

interface ThorchainPageFetchInput {
  pageSize: number;
  continuation: string | null;
  window: ThorchainWindow;
}

interface BoundaryPoint {
  eventAt: Date;
  providerRecordId: string;
}

interface NormalizedThorchainSwap {
  core: NormalizedSwapRowInput;
  raw: RawSwapRowInput;
  eventAt: Date | null;
  isSynthAsset: boolean;
  isCrossChain: boolean;
}

interface ParsedThorchainAsset {
  raw: string;
  chainRaw: string | null;
  tokenRaw: string | null;
  symbol: string | null;
  address: string | null;
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
    return THORCHAIN_PAGE_SIZE_DEFAULT;
  }
  return Math.min(Math.floor(requested), THORCHAIN_PAGE_SIZE_MAX);
}

function parseThorchainDateNs(value: string | null | undefined): Date | null {
  if (!value) {
    return null;
  }

  try {
    const ns = BigInt(value);
    if (ns <= 0n) {
      return null;
    }
    const ms = ns / 1_000_000n;
    const parsed = new Date(Number(ms));
    if (Number.isNaN(parsed.getTime())) {
      return null;
    }
    return parsed;
  } catch {
    return null;
  }
}

function collectTransfersCoins(transfers: ThorchainActionTransfer[] | null | undefined): ThorchainCoin[] {
  const output: ThorchainCoin[] = [];
  for (const transfer of transfers ?? []) {
    for (const coin of transfer.coins ?? []) {
      output.push(coin);
    }
  }
  return output;
}

function collectTxIds(transfers: ThorchainActionTransfer[] | null | undefined): string[] {
  const ids = new Set<string>();
  for (const transfer of transfers ?? []) {
    const txId = nonEmptyOrNull(transfer.txID);
    if (txId) {
      ids.add(txId);
    }
  }
  return [...ids];
}

function firstTxId(transfers: ThorchainActionTransfer[] | null | undefined): string | null {
  for (const transfer of transfers ?? []) {
    const txId = nonEmptyOrNull(transfer.txID);
    if (txId) {
      return txId;
    }
  }
  return null;
}

function parseThorchainAsset(rawAsset: string | null | undefined): ParsedThorchainAsset | null {
  const raw = nonEmptyOrNull(rawAsset);
  if (!raw) {
    return null;
  }

  const firstDelimiterIndex = raw.search(/[.~-]/);
  if (firstDelimiterIndex < 0) {
    return {
      raw,
      chainRaw: raw,
      tokenRaw: null,
      symbol: raw,
      address: null,
    };
  }

  const chainRaw = nonEmptyOrNull(raw.slice(0, firstDelimiterIndex));
  const tokenRaw = nonEmptyOrNull(raw.slice(firstDelimiterIndex + 1));
  if (!tokenRaw) {
    return {
      raw,
      chainRaw,
      tokenRaw: null,
      symbol: null,
      address: null,
    };
  }

  const firstTokenDelimiter = tokenRaw.indexOf("-");
  if (firstTokenDelimiter < 0) {
    return {
      raw,
      chainRaw,
      tokenRaw,
      symbol: tokenRaw,
      address: null,
    };
  }

  const symbol = nonEmptyOrNull(tokenRaw.slice(0, firstTokenDelimiter));
  const address = nonEmptyOrNull(tokenRaw.slice(firstTokenDelimiter + 1));
  return {
    raw,
    chainRaw,
    tokenRaw,
    symbol,
    address,
  };
}

function buildAssetId(asset: ParsedThorchainAsset | null, chainCanonical: string | null): string | null {
  if (!asset || !chainCanonical) {
    return null;
  }
  const idPart = (asset.address ?? asset.symbol ?? asset.raw).toLowerCase();
  return `${chainCanonical}:${idPart}`;
}

function parseMemoDestinationAsset(memo: string | null | undefined): string | null {
  const normalized = nonEmptyOrNull(memo);
  if (!normalized) {
    return null;
  }
  const match = normalized.match(/^=:(.+?):/);
  return match?.[1] ?? null;
}

function isThorchainSynthAsset(rawAsset: string | null | undefined): boolean {
  const normalized = nonEmptyOrNull(rawAsset);
  if (!normalized) {
    return false;
  }
  return normalized.includes("~");
}

function isCrossChainSwap(core: Pick<NormalizedSwapRowInput, "source_chain_canonical" | "destination_chain_canonical">): boolean {
  if (!core.source_chain_canonical || !core.destination_chain_canonical) {
    return false;
  }
  return core.source_chain_canonical !== core.destination_chain_canonical;
}

function isFinalThorchainStatus(status: string | null | undefined): boolean {
  const canonical = mapThorchainStatus(status ?? undefined);
  return canonical === "success" || canonical === "failed";
}

function buildThorchainNormalizedId(action: ThorchainAction): string {
  const inTxIds = collectTxIds(action.in);
  const outTxIds = collectTxIds(action.out);
  const firstInputCoin = collectTransfersCoins(action.in)[0];
  const idBasis = inTxIds.length > 0 ? inTxIds.join(",") : outTxIds.join(",");
  const stableKey = [
    "thorchain",
    action.type ?? "swap",
    action.date ?? "",
    action.height ?? "",
    idBasis,
    action.metadata?.swap?.memo ?? "",
    firstInputCoin?.asset ?? "",
  ].join("|");
  return sha256Hex(stableKey);
}

function buildThorchainWindow(mode: IngestMode, checkpoint: IngestCheckpoint | null, now: Date): ThorchainWindow {
  if (mode === "sync_newer") {
    const fromTs = checkpoint?.newestEventAt ? shiftSeconds(checkpoint.newestEventAt, 1) : addDays(now, -30);
    return {
      fromTs,
      toTs: now,
      timestamp: toUnixSeconds(now),
    };
  }

  if (mode === "backfill_older") {
    if (checkpoint?.oldestEventAt) {
      const toTs = shiftSeconds(checkpoint.oldestEventAt, -1);
      return {
        fromTs: null,
        toTs,
        timestamp: toUnixSeconds(toTs),
      };
    }

    const bootstrapFrom = addDays(now, -THORCHAIN_WINDOW_DAYS);
    return {
      fromTs: bootstrapFrom,
      toTs: now,
      timestamp: toUnixSeconds(now),
    };
  }

  const bootstrapFrom = addDays(now, -THORCHAIN_WINDOW_DAYS);
  return {
    fromTs: bootstrapFrom,
    toTs: now,
    timestamp: toUnixSeconds(now),
  };
}

function buildThorchainPageUrl(input: ThorchainPageFetchInput): string {
  const url = new URL(`${THORCHAIN_BASE_URL}/v2/actions`);
  url.searchParams.set("type", "swap");
  // Midgard advertises nosynth as an asset filter; we still enforce synth exclusion client-side.
  url.searchParams.set("asset", "nosynth");
  url.searchParams.set("limit", String(input.pageSize));
  if (!input.continuation && input.window.timestamp !== null) {
    url.searchParams.set("timestamp", String(input.window.timestamp));
  }
  if (input.continuation) {
    url.searchParams.set("nextPageToken", input.continuation);
  }
  return url.toString();
}

async function fetchThorchainPage(input: ThorchainPageFetchInput): Promise<ThorchainActionsPageResponse> {
  const url = buildThorchainPageUrl(input);
  return fetchJsonWithRetry<ThorchainActionsPageResponse>(
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
    },
  );
}

function shouldIncludeByWindow(eventAt: Date | null, window: ThorchainWindow): boolean {
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
): IngestCheckpoint | null {
  if (!existing && !newest && !oldest) {
    return null;
  }

  const checkpoint: IngestCheckpoint = {
    streamKey: THORCHAIN_STREAM_KEY,
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

function normalizeThorchainAction(
  action: ThorchainAction,
  runId: string,
  observedAt: Date,
  sourceCursor: string | null,
): NormalizedThorchainSwap | null {
  const inputCoins = collectTransfersCoins(action.in);
  const outputCoins = collectTransfersCoins(action.out);
  const firstInputCoin = inputCoins[0] ?? null;
  const firstOutputCoin = outputCoins[0] ?? null;
  const memoDestinationAssetRaw = parseMemoDestinationAsset(action.metadata?.swap?.memo);
  const destinationAssetFromMemo = parseThorchainAsset(memoDestinationAssetRaw);
  const containsSynthAsset =
    isThorchainSynthAsset(firstInputCoin?.asset) ||
    isThorchainSynthAsset(firstOutputCoin?.asset) ||
    isThorchainSynthAsset(memoDestinationAssetRaw);

  const sourceAsset = parseThorchainAsset(firstInputCoin?.asset);
  const destinationAsset = parseThorchainAsset(firstOutputCoin?.asset) ?? destinationAssetFromMemo;
  const sourceChain = canonicalizeChain({
    provider: "thorchain",
    rawChain: sourceAsset?.chainRaw,
  });
  const destinationChain = canonicalizeChain({
    provider: "thorchain",
    rawChain: destinationAsset?.chainRaw,
  });

  const eventAt = parseThorchainDateNs(action.date);
  const normalizedId = buildThorchainNormalizedId(action);
  const rawJson = JSON.stringify(action);
  const rawHash = sha256Hex(rawJson);
  const sourceTxHash = firstTxId(action.in);
  const destinationTxHash = firstTxId(action.out);
  const txHashes = [...new Set([...collectTxIds(action.in), ...collectTxIds(action.out)])];

  const sourceAssetId = buildAssetId(sourceAsset, sourceChain.canonical);
  const destinationAssetId = buildAssetId(destinationAsset, destinationChain.canonical);
  const sourceAssetDecimals = inferTokenDecimals({
    provider: "thorchain",
    explicitDecimals: null,
    chainCanonical: sourceChain.canonical,
    symbol: sourceAsset?.symbol ?? null,
  });
  const destinationAssetDecimals = inferTokenDecimals({
    provider: "thorchain",
    explicitDecimals: null,
    chainCanonical: destinationChain.canonical,
    symbol: destinationAsset?.symbol ?? null,
  });
  const amountInAtomic = nonEmptyOrNull(firstInputCoin?.amount);
  const amountOutAtomic = nonEmptyOrNull(firstOutputCoin?.amount) ?? nonEmptyOrNull(action.metadata?.swap?.swapTarget);
  const amountInNormalized =
    amountInAtomic && sourceAssetDecimals !== null
      ? atomicToNormalized(amountInAtomic, sourceAssetDecimals)
      : null;
  const amountOutNormalized =
    amountOutAtomic && destinationAssetDecimals !== null
      ? atomicToNormalized(amountOutAtomic, destinationAssetDecimals)
      : null;
  const feeAtomic = nonEmptyOrNull(action.metadata?.swap?.liquidityFee);
  const feeNormalized =
    feeAtomic && sourceAssetDecimals !== null
      ? atomicToNormalized(feeAtomic, sourceAssetDecimals)
      : null;
  const statusCanonical = mapThorchainStatus(action.status ?? undefined);
  const failureReasonRaw = statusCanonical === "failed" ? "failed" : null;

  const core: NormalizedSwapRowInput = {
    normalized_id: normalizedId,
    provider_key: "thorchain",
    provider_record_id: normalizedId,
    provider_parent_id: null,
    record_granularity: "action",
    status_canonical: statusCanonical,
    status_raw: action.status ?? null,
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
    source_asset_id: sourceAssetId,
    destination_asset_id: destinationAssetId,
    source_asset_symbol: sourceAsset?.symbol ?? null,
    destination_asset_symbol: destinationAsset?.symbol ?? null,
    source_asset_decimals: sourceAssetDecimals,
    destination_asset_decimals: destinationAssetDecimals,
    amount_in_atomic: amountInAtomic,
    amount_out_atomic: amountOutAtomic,
    amount_in_normalized: amountInNormalized,
    amount_out_normalized: amountOutNormalized,
    amount_in_usd: null,
    amount_out_usd: null,
    fee_atomic: feeAtomic,
    fee_normalized: feeNormalized,
    fee_usd: null,
    slippage_bps: parseFloatOrNull(action.metadata?.swap?.swapSlip),
    solver_id: null,
    route_hint: action.pools?.join(">") ?? null,
    source_tx_hash: sourceTxHash,
    destination_tx_hash: destinationTxHash,
    refund_tx_hash: null,
    extra_tx_hashes: txHashes.length > 0 ? txHashes : null,
    is_final: isFinalThorchainStatus(action.status),
    raw_hash_latest: rawHash,
    source_endpoint: THORCHAIN_SOURCE_ENDPOINT,
    ingested_at: observedAt,
    run_id: runId,
  };

  const raw: RawSwapRowInput = {
    normalized_id: normalizedId,
    raw_hash: rawHash,
    raw_json: rawJson,
    observed_at: observedAt,
    source_endpoint: THORCHAIN_SOURCE_ENDPOINT,
    source_cursor: sourceCursor,
    run_id: runId,
  };

  return {
    core,
    raw,
    eventAt,
    isSynthAsset: containsSynthAsset,
    isCrossChain: isCrossChainSwap(core),
  };
}

async function ingestThorchain(
  connection: DuckDBConnection,
  runId: string,
  mode: IngestMode,
  checkpoint: IngestCheckpoint | null,
  now: Date,
  pageSize: number,
  maxPages: number | null,
): Promise<ProviderIngestOutput> {
  const window = buildThorchainWindow(mode, checkpoint, now);
  const effectivePageSize = clampPageSize(pageSize);

  let continuation: string | null = null;
  let pageCount = 0;
  let recordsFetched = 0;
  let recordsNormalized = 0;
  let recordsUpserted = 0;
  let newestBoundary: BoundaryPoint | null = null;
  let oldestBoundary: BoundaryPoint | null = null;
  const seenContinuations = new Set<string>();

  while (true) {
    if (maxPages !== null && pageCount >= maxPages) {
      break;
    }

    const page = await fetchThorchainPage({
      pageSize: effectivePageSize,
      continuation,
      window,
    });
    const actions = page.actions ?? [];
    recordsFetched += actions.length;

    if (actions.length === 0) {
      break;
    }

    const observedAt = new Date();
    const coreRows: NormalizedSwapRowInput[] = [];
    const rawRows: RawSwapRowInput[] = [];
    let pageOldestEventAt: Date | null = null;

    for (const action of actions) {
      const normalized = normalizeThorchainAction(action, runId, observedAt, continuation);
      if (!normalized) {
        continue;
      }
      if (!shouldIncludeByWindow(normalized.eventAt, window)) {
        if (normalized.eventAt) {
          if (!pageOldestEventAt || normalized.eventAt < pageOldestEventAt) {
            pageOldestEventAt = normalized.eventAt;
          }
        }
        continue;
      }
      if (normalized.core.status_canonical !== "success") {
        if (normalized.eventAt) {
          if (!pageOldestEventAt || normalized.eventAt < pageOldestEventAt) {
            pageOldestEventAt = normalized.eventAt;
          }
        }
        continue;
      }
      if (!isSwapRowWithinIngestScope(normalized.core)) {
        if (normalized.eventAt) {
          if (!pageOldestEventAt || normalized.eventAt < pageOldestEventAt) {
            pageOldestEventAt = normalized.eventAt;
          }
        }
        continue;
      }
      if (normalized.isSynthAsset) {
        if (normalized.eventAt) {
          if (!pageOldestEventAt || normalized.eventAt < pageOldestEventAt) {
            pageOldestEventAt = normalized.eventAt;
          }
        }
        continue;
      }
      if (!normalized.isCrossChain) {
        if (normalized.eventAt) {
          if (!pageOldestEventAt || normalized.eventAt < pageOldestEventAt) {
            pageOldestEventAt = normalized.eventAt;
          }
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
    continuation = page.meta?.nextPageToken ?? null;
    if (window.fromTs && pageOldestEventAt && pageOldestEventAt < window.fromTs) {
      break;
    }
    if (!continuation) {
      break;
    }
    if (seenContinuations.has(continuation)) {
      throw new Error(`Thorchain nextPageToken loop detected at token=${continuation}`);
    }
    seenContinuations.add(continuation);
  }

  return {
    recordsFetched,
    recordsNormalized,
    recordsUpserted,
    checkpoint: mergeCheckpoint(checkpoint, runId, newestBoundary, oldestBoundary),
  };
}

export const thorchainProviderAdapter: ProviderAdapter = {
  key: "thorchain",
  streamKey: THORCHAIN_STREAM_KEY,
  sourceEndpoint: THORCHAIN_SOURCE_ENDPOINT,
  async ingest(input: ProviderIngestInput): Promise<ProviderIngestOutput> {
    return ingestThorchain(
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
