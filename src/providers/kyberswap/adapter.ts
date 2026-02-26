import type { DuckDBConnection } from "@duckdb/node-api";
import type { IngestMode } from "../../ingest/types";
import { isSwapRowWithinIngestScope } from "../../ingest/swap-scope";
import { fetchJsonWithRetry } from "../../lib/http";
import type { NormalizedSwapRowInput } from "../../domain/normalized-swap";
import type { IngestCheckpoint } from "../../storage/repositories/ingest-checkpoints";
import { persistSwaps, type RawSwapRowInput } from "../../storage/repositories/swaps";
import { atomicToNormalized } from "../../utils/amount-normalization";
import { sha256Hex } from "../../utils/hash";
import { addDays, toUnixSeconds } from "../../utils/time";
import type { ProviderAdapter, ProviderIngestInput, ProviderIngestOutput } from "../types";

const KYBERSWAP_DEFAULT_META_AGGREGATOR = "0x6131b5fae19ea4f9d964eac0408e4408b66337b5";
const KYBERSWAP_SWAPPED_TOPIC0 =
  "0xd6d4f5681c246c9f42c203e287975af1601f8df8035a9251f79aab5c8f09e2f8";
const KYBERSWAP_STREAM_KEY = "swaps:kyberswap:meta_aggregation_router_v2:swapped:mainnet";
const KYBERSWAP_SOURCE_ENDPOINT = "eth_getLogs:kyberswap:meta_aggregation_router_v2:mainnet";
const KYBERSWAP_WINDOW_DAYS = 30;
const KYBERSWAP_MAX_BLOCK_RANGE = 10_000;

const CHAIN_ID_MAINNET = "1";
const CHAIN_CANONICAL_MAINNET = "eip155:1";

const ETH_SENTINEL_ADDRESS = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
const USDC_ADDRESS = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
const USDT_ADDRESS = "0xdac17f958d2ee523a2206206994597c13d831ec7";
const WETH_ADDRESS = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2";
const CBBTC_ADDRESS = "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf";

const USDC_DECIMALS = 6;
const USDT_DECIMALS = 6;
const WETH_DECIMALS = 18;
const ETH_DECIMALS = 18;
const CBBTC_DECIMALS = 8;

interface JsonRpcError {
  code: number;
  message: string;
  data?: unknown;
}

interface JsonRpcResponse<T> {
  jsonrpc: string;
  id: number | string | null;
  result?: T;
  error?: JsonRpcError;
}

interface EthLog {
  address: string;
  blockHash: string;
  blockNumber: string;
  data: string;
  logIndex: string;
  removed: boolean;
  topics: string[];
  transactionHash: string;
  transactionIndex: string;
}

interface EthBlock {
  number: string;
  timestamp: string;
}

interface CursorPosition {
  blockNumber: number;
  logIndex: number;
}

interface BoundaryPoint {
  position: CursorPosition;
  eventAt: Date;
  providerRecordId: string;
}

interface SourceTokenInfo {
  symbol: "USDC" | "USDT" | "WETH" | "ETH";
  decimals: number;
  assetId: string;
}

interface SwappedEventDecoded {
  sender: string;
  srcToken: string;
  dstToken: string;
  dstReceiver: string;
  spentAmountAtomic: string;
  returnAmountAtomic: string;
}

interface KyberswapRuntimeConfig {
  rpcUrl: string;
  metaAggregatorAddress: string;
}

interface KyberswapWindow {
  fromBlock: number;
  toBlock: number;
  direction: "asc" | "desc";
  skipCursor: CursorPosition | null;
}

function nonEmptyOrNull(value: string | null | undefined): string | null {
  if (!value) {
    return null;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
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

function hexToNumber(hex: string, fieldName: string): number {
  if (!/^0x[0-9a-fA-F]+$/.test(hex)) {
    throw new Error(`Invalid hex value for ${fieldName}: '${hex}'.`);
  }
  const parsed = Number.parseInt(hex.slice(2), 16);
  if (!Number.isFinite(parsed) || !Number.isInteger(parsed) || parsed < 0) {
    throw new Error(`Invalid numeric value for ${fieldName}: '${hex}'.`);
  }
  return parsed;
}

function hexWordToUintString(hexWord: string): string | null {
  if (!/^[0-9a-fA-F]{64}$/.test(hexWord)) {
    return null;
  }
  try {
    return BigInt(`0x${hexWord}`).toString();
  } catch {
    return null;
  }
}

function numberToHex(value: number): string {
  return `0x${value.toString(16)}`;
}

function clampBlockRange(blockRange: number): number {
  if (!Number.isFinite(blockRange) || !Number.isInteger(blockRange) || blockRange <= 0) {
    return KYBERSWAP_MAX_BLOCK_RANGE;
  }
  return Math.min(blockRange, KYBERSWAP_MAX_BLOCK_RANGE);
}

function compareCursorPosition(left: CursorPosition, right: CursorPosition): number {
  if (left.blockNumber !== right.blockNumber) {
    return left.blockNumber - right.blockNumber;
  }
  return left.logIndex - right.logIndex;
}

function encodeCursor(position: CursorPosition): string {
  return `${position.blockNumber}:${position.logIndex}`;
}

function parseCursor(rawCursor: string | null | undefined): CursorPosition | null {
  if (!rawCursor) {
    return null;
  }
  const [blockRaw, logIndexRaw] = rawCursor.split(":");
  if (!blockRaw || !logIndexRaw) {
    return null;
  }
  const blockNumber = Number(blockRaw);
  const logIndex = Number(logIndexRaw);
  if (
    !Number.isFinite(blockNumber) ||
    !Number.isFinite(logIndex) ||
    !Number.isInteger(blockNumber) ||
    !Number.isInteger(logIndex) ||
    blockNumber < 0 ||
    logIndex < 0
  ) {
    return null;
  }
  return { blockNumber, logIndex };
}

function extractLogPosition(log: EthLog): CursorPosition | null {
  try {
    const blockNumber = hexToNumber(log.blockNumber, "log.blockNumber");
    const logIndex = hexToNumber(log.logIndex, "log.logIndex");
    return { blockNumber, logIndex };
  } catch {
    return null;
  }
}

function shouldSkipByCursor(
  mode: IngestMode,
  position: CursorPosition,
  cursor: CursorPosition | null,
): boolean {
  if (!cursor) {
    return false;
  }
  const compared = compareCursorPosition(position, cursor);
  if (mode === "sync_newer") {
    return compared <= 0;
  }
  if (mode === "backfill_older") {
    return compared >= 0;
  }
  return false;
}

function resolveSourceTokenInfo(srcToken: string): SourceTokenInfo | null {
  if (srcToken === USDC_ADDRESS) {
    return {
      symbol: "USDC",
      decimals: USDC_DECIMALS,
      assetId: `${CHAIN_ID_MAINNET}:${USDC_ADDRESS}`,
    };
  }
  if (srcToken === USDT_ADDRESS) {
    return {
      symbol: "USDT",
      decimals: USDT_DECIMALS,
      assetId: `${CHAIN_ID_MAINNET}:${USDT_ADDRESS}`,
    };
  }
  if (srcToken === WETH_ADDRESS) {
    return {
      symbol: "WETH",
      decimals: WETH_DECIMALS,
      assetId: `${CHAIN_ID_MAINNET}:${WETH_ADDRESS}`,
    };
  }
  if (srcToken === ETH_SENTINEL_ADDRESS) {
    return {
      symbol: "ETH",
      decimals: ETH_DECIMALS,
      assetId: `${CHAIN_ID_MAINNET}:native`,
    };
  }
  return null;
}

function readWord(data: string, wordIndex: number): string | null {
  if (!data.startsWith("0x")) {
    return null;
  }
  const start = 2 + wordIndex * 64;
  const end = start + 64;
  if (data.length < end) {
    return null;
  }
  return data.slice(start, end);
}

function wordToAddress(word: string): string | null {
  if (!/^[0-9a-fA-F]{64}$/.test(word)) {
    return null;
  }
  return normalizeAddress(`0x${word.slice(24)}`);
}

function decodeSwappedEvent(logData: string): SwappedEventDecoded | null {
  const senderWord = readWord(logData, 0);
  const srcTokenWord = readWord(logData, 1);
  const dstTokenWord = readWord(logData, 2);
  const dstReceiverWord = readWord(logData, 3);
  const spentAmountWord = readWord(logData, 4);
  const returnAmountWord = readWord(logData, 5);

  if (
    !senderWord ||
    !srcTokenWord ||
    !dstTokenWord ||
    !dstReceiverWord ||
    !spentAmountWord ||
    !returnAmountWord
  ) {
    return null;
  }

  const sender = wordToAddress(senderWord);
  const srcToken = wordToAddress(srcTokenWord);
  const dstToken = wordToAddress(dstTokenWord);
  const dstReceiver = wordToAddress(dstReceiverWord);
  const spentAmountAtomic = hexWordToUintString(spentAmountWord);
  const returnAmountAtomic = hexWordToUintString(returnAmountWord);

  if (!sender || !srcToken || !dstToken || !dstReceiver || !spentAmountAtomic || !returnAmountAtomic) {
    return null;
  }

  return {
    sender,
    srcToken,
    dstToken,
    dstReceiver,
    spentAmountAtomic,
    returnAmountAtomic,
  };
}

async function rpcCall<T>(
  rpcUrl: string,
  method: string,
  params: unknown[],
): Promise<T> {
  const response = await fetchJsonWithRetry<JsonRpcResponse<T>>(
    rpcUrl,
    {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        jsonrpc: "2.0",
        id: 1,
        method,
        params,
      }),
    },
    {
      attempts: 5,
      initialDelayMs: 400,
      maxDelayMs: 5_000,
      timeoutMs: 90_000,
    },
  );

  if (response.error) {
    const detail = response.error.data ? ` data=${JSON.stringify(response.error.data).slice(0, 300)}` : "";
    throw new Error(`RPC ${method} failed (${response.error.code}): ${response.error.message}${detail}`);
  }
  if (response.result === undefined) {
    throw new Error(`RPC ${method} returned no result.`);
  }
  return response.result;
}

async function getLatestBlockNumber(rpcUrl: string): Promise<number> {
  const latestBlockHex = await rpcCall<string>(rpcUrl, "eth_blockNumber", []);
  return hexToNumber(latestBlockHex, "eth_blockNumber");
}

async function getBlockTimestamp(
  rpcUrl: string,
  blockNumber: number,
  blockTimestampCache: Map<number, Date>,
): Promise<Date> {
  const cached = blockTimestampCache.get(blockNumber);
  if (cached) {
    return cached;
  }

  const block = await rpcCall<EthBlock | null>(rpcUrl, "eth_getBlockByNumber", [
    numberToHex(blockNumber),
    false,
  ]);
  if (!block || !block.timestamp) {
    throw new Error(`eth_getBlockByNumber returned empty block for ${blockNumber}.`);
  }
  const timestampSeconds = hexToNumber(block.timestamp, "block.timestamp");
  const timestamp = new Date(timestampSeconds * 1_000);
  blockTimestampCache.set(blockNumber, timestamp);
  return timestamp;
}

async function findFirstBlockAtOrAfterTimestamp(
  rpcUrl: string,
  targetTimestampSeconds: number,
  latestBlockNumber: number,
  blockTimestampCache: Map<number, Date>,
): Promise<number> {
  if (targetTimestampSeconds <= 0) {
    return 0;
  }

  const latestTimestampSeconds = toUnixSeconds(
    await getBlockTimestamp(rpcUrl, latestBlockNumber, blockTimestampCache),
  );
  if (targetTimestampSeconds >= latestTimestampSeconds) {
    return latestBlockNumber;
  }

  let low = 0;
  let high = latestBlockNumber;
  while (low < high) {
    const midpoint = Math.floor((low + high) / 2);
    const midpointTimestampSeconds = toUnixSeconds(
      await getBlockTimestamp(rpcUrl, midpoint, blockTimestampCache),
    );
    if (midpointTimestampSeconds < targetTimestampSeconds) {
      low = midpoint + 1;
    } else {
      high = midpoint;
    }
  }
  return low;
}

async function fetchSwappedLogs(
  rpcUrl: string,
  metaAggregatorAddress: string,
  fromBlock: number,
  toBlock: number,
): Promise<EthLog[]> {
  return rpcCall<EthLog[]>(rpcUrl, "eth_getLogs", [
    {
      address: metaAggregatorAddress,
      fromBlock: numberToHex(fromBlock),
      toBlock: numberToHex(toBlock),
      topics: [KYBERSWAP_SWAPPED_TOPIC0],
    },
  ]);
}

function updateNewestBoundary(current: BoundaryPoint | null, candidate: BoundaryPoint): BoundaryPoint {
  if (!current) {
    return candidate;
  }
  return compareCursorPosition(candidate.position, current.position) > 0 ? candidate : current;
}

function updateOldestBoundary(current: BoundaryPoint | null, candidate: BoundaryPoint): BoundaryPoint {
  if (!current) {
    return candidate;
  }
  return compareCursorPosition(candidate.position, current.position) < 0 ? candidate : current;
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
    streamKey: KYBERSWAP_STREAM_KEY,
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

  const existingNewestCursor = parseCursor(checkpoint.newestCursor);
  if (newest && (!existingNewestCursor || compareCursorPosition(newest.position, existingNewestCursor) > 0)) {
    checkpoint.newestCursor = encodeCursor(newest.position);
    checkpoint.newestEventAt = newest.eventAt;
    checkpoint.newestProviderRecordId = newest.providerRecordId;
  }

  const existingOldestCursor = parseCursor(checkpoint.oldestCursor);
  if (oldest && (!existingOldestCursor || compareCursorPosition(oldest.position, existingOldestCursor) < 0)) {
    checkpoint.oldestCursor = encodeCursor(oldest.position);
    checkpoint.oldestEventAt = oldest.eventAt;
    checkpoint.oldestProviderRecordId = oldest.providerRecordId;
  }

  return checkpoint;
}

function resolveRuntimeConfig(): KyberswapRuntimeConfig {
  const rpcUrl = nonEmptyOrNull(Bun.env.KYBERSWAP_RPC_URL ?? process.env.KYBERSWAP_RPC_URL);
  if (!rpcUrl) {
    throw new Error("Missing KYBERSWAP_RPC_URL environment variable.");
  }

  const configuredMetaAggregator = nonEmptyOrNull(
    Bun.env.KYBERSWAP_META_AGGREGATOR ?? process.env.KYBERSWAP_META_AGGREGATOR,
  );
  const metaAggregatorAddress = normalizeAddress(
    configuredMetaAggregator ?? KYBERSWAP_DEFAULT_META_AGGREGATOR,
  );
  if (!metaAggregatorAddress) {
    throw new Error("Invalid KYBERSWAP_META_AGGREGATOR environment variable.");
  }

  return {
    rpcUrl,
    metaAggregatorAddress,
  };
}

async function buildKyberswapWindow(
  rpcUrl: string,
  mode: IngestMode,
  checkpoint: IngestCheckpoint | null,
  now: Date,
  blockTimestampCache: Map<number, Date>,
): Promise<KyberswapWindow> {
  const latestBlockNumber = await getLatestBlockNumber(rpcUrl);

  if (mode === "sync_newer") {
    const newestCursor = parseCursor(checkpoint?.newestCursor);
    if (newestCursor) {
      return {
        fromBlock: newestCursor.blockNumber,
        toBlock: latestBlockNumber,
        direction: "asc",
        skipCursor: newestCursor,
      };
    }

    const fallbackFrom = checkpoint?.newestEventAt
      ? new Date(checkpoint.newestEventAt.getTime() + 1_000)
      : addDays(now, -KYBERSWAP_WINDOW_DAYS);
    const fromBlock = await findFirstBlockAtOrAfterTimestamp(
      rpcUrl,
      toUnixSeconds(fallbackFrom),
      latestBlockNumber,
      blockTimestampCache,
    );
    return {
      fromBlock,
      toBlock: latestBlockNumber,
      direction: "asc",
      skipCursor: null,
    };
  }

  if (mode === "backfill_older") {
    const oldestCursor = parseCursor(checkpoint?.oldestCursor);
    if (oldestCursor) {
      const anchorTimestampSeconds = checkpoint?.oldestEventAt
        ? toUnixSeconds(checkpoint.oldestEventAt)
        : toUnixSeconds(await getBlockTimestamp(rpcUrl, oldestCursor.blockNumber, blockTimestampCache));
      const fromTimestampSeconds = Math.max(
        0,
        anchorTimestampSeconds - KYBERSWAP_WINDOW_DAYS * 24 * 60 * 60,
      );
      const fromBlock = await findFirstBlockAtOrAfterTimestamp(
        rpcUrl,
        fromTimestampSeconds,
        latestBlockNumber,
        blockTimestampCache,
      );
      return {
        fromBlock: Math.min(fromBlock, oldestCursor.blockNumber),
        toBlock: oldestCursor.blockNumber,
        direction: "desc",
        skipCursor: oldestCursor,
      };
    }
  }

  const bootstrapFrom = addDays(now, -KYBERSWAP_WINDOW_DAYS);
  const fromBlock = await findFirstBlockAtOrAfterTimestamp(
    rpcUrl,
    toUnixSeconds(bootstrapFrom),
    latestBlockNumber,
    blockTimestampCache,
  );
  return {
    fromBlock,
    toBlock: latestBlockNumber,
    direction: "desc",
    skipCursor: null,
  };
}

async function ingestKyberswap(
  connection: DuckDBConnection,
  runId: string,
  mode: IngestMode,
  checkpoint: IngestCheckpoint | null,
  now: Date,
  pageSize: number,
  maxPages: number | null,
): Promise<ProviderIngestOutput> {
  const config = resolveRuntimeConfig();
  const blockTimestampCache = new Map<number, Date>();
  const window = await buildKyberswapWindow(
    config.rpcUrl,
    mode,
    checkpoint,
    now,
    blockTimestampCache,
  );

  if (window.fromBlock > window.toBlock) {
    return {
      recordsFetched: 0,
      recordsNormalized: 0,
      recordsUpserted: 0,
      checkpoint: mergeCheckpoint(checkpoint, runId, null, null),
    };
  }

  const blockRange = clampBlockRange(pageSize);
  const maxRanges =
    maxPages === null
      ? null
      : Math.max(1, Math.floor(maxPages));

  let recordsFetched = 0;
  let recordsNormalized = 0;
  let recordsUpserted = 0;
  let newestBoundary: BoundaryPoint | null = null;
  let oldestBoundary: BoundaryPoint | null = null;

  let rangeCount = 0;
  let nextFromBlock = window.fromBlock;
  let nextToBlock = window.toBlock;

  while (true) {
    if (maxRanges !== null && rangeCount >= maxRanges) {
      break;
    }

    let rangeFromBlock: number;
    let rangeToBlock: number;
    if (window.direction === "asc") {
      if (nextFromBlock > window.toBlock) {
        break;
      }
      rangeFromBlock = nextFromBlock;
      rangeToBlock = Math.min(window.toBlock, rangeFromBlock + blockRange - 1);
      nextFromBlock = rangeToBlock + 1;
    } else {
      if (nextToBlock < window.fromBlock) {
        break;
      }
      rangeToBlock = nextToBlock;
      rangeFromBlock = Math.max(window.fromBlock, rangeToBlock - blockRange + 1);
      nextToBlock = rangeFromBlock - 1;
    }

    const logs = await fetchSwappedLogs(
      config.rpcUrl,
      config.metaAggregatorAddress,
      rangeFromBlock,
      rangeToBlock,
    );
    rangeCount += 1;
    recordsFetched += logs.length;

    if (logs.length === 0) {
      continue;
    }

    const observedAt = new Date();
    const sourceCursor = `blocks:${rangeFromBlock}-${rangeToBlock}`;
    const coreRows: NormalizedSwapRowInput[] = [];
    const rawRows: RawSwapRowInput[] = [];

    const logsWithPositions = logs
      .map((log) => ({
        log,
        position: extractLogPosition(log),
      }))
      .filter(
        (entry): entry is { log: EthLog; position: CursorPosition } => entry.position !== null,
      )
      .sort((left, right) => compareCursorPosition(left.position, right.position));

    for (const entry of logsWithPositions) {
      if (entry.log.removed) {
        continue;
      }
      if (shouldSkipByCursor(mode, entry.position, window.skipCursor)) {
        continue;
      }

      const decoded = decodeSwappedEvent(entry.log.data);
      if (!decoded || decoded.dstToken !== CBBTC_ADDRESS) {
        continue;
      }

      const sourceToken = resolveSourceTokenInfo(decoded.srcToken);
      if (!sourceToken) {
        continue;
      }

      const txHash = normalizeHash(entry.log.transactionHash);
      if (!txHash) {
        continue;
      }

      const providerRecordId = `${txHash}:${entry.position.logIndex}`;
      const eventAt = await getBlockTimestamp(
        config.rpcUrl,
        entry.position.blockNumber,
        blockTimestampCache,
      );
      const rawJson = JSON.stringify(entry.log);
      const rawHash = sha256Hex(rawJson);

      const core: NormalizedSwapRowInput = {
        normalized_id: providerRecordId,
        provider_key: "kyberswap",
        provider_record_id: providerRecordId,
        provider_parent_id: txHash,
        record_granularity: "swap_event",
        status_canonical: "success",
        status_raw: "swapped",
        failure_reason_raw: null,
        created_at: eventAt,
        updated_at: eventAt,
        event_at: eventAt,
        source_chain_canonical: CHAIN_CANONICAL_MAINNET,
        destination_chain_canonical: CHAIN_CANONICAL_MAINNET,
        source_chain_raw: "ethereum",
        destination_chain_raw: "ethereum",
        source_chain_id_raw: CHAIN_ID_MAINNET,
        destination_chain_id_raw: CHAIN_ID_MAINNET,
        source_asset_id: sourceToken.assetId,
        destination_asset_id: `${CHAIN_ID_MAINNET}:${CBBTC_ADDRESS}`,
        source_asset_symbol: sourceToken.symbol,
        destination_asset_symbol: "CBBTC",
        source_asset_decimals: sourceToken.decimals,
        destination_asset_decimals: CBBTC_DECIMALS,
        amount_in_atomic: decoded.spentAmountAtomic,
        amount_out_atomic: decoded.returnAmountAtomic,
        amount_in_normalized: atomicToNormalized(decoded.spentAmountAtomic, sourceToken.decimals),
        amount_out_normalized: atomicToNormalized(decoded.returnAmountAtomic, CBBTC_DECIMALS),
        amount_in_usd: null,
        amount_out_usd: null,
        fee_atomic: null,
        fee_normalized: null,
        fee_usd: null,
        slippage_bps: null,
        solver_id: decoded.sender,
        route_hint: "MetaAggregationRouterV2",
        source_tx_hash: txHash,
        destination_tx_hash: txHash,
        refund_tx_hash: null,
        extra_tx_hashes: [txHash],
        is_final: true,
        raw_hash_latest: rawHash,
        source_endpoint: KYBERSWAP_SOURCE_ENDPOINT,
        ingested_at: observedAt,
        run_id: runId,
      };
      if (!isSwapRowWithinIngestScope(core)) {
        continue;
      }

      const raw: RawSwapRowInput = {
        normalized_id: providerRecordId,
        raw_hash: rawHash,
        raw_json: rawJson,
        observed_at: observedAt,
        source_endpoint: KYBERSWAP_SOURCE_ENDPOINT,
        source_cursor: sourceCursor,
        run_id: runId,
      };

      recordsNormalized += 1;
      coreRows.push(core);
      rawRows.push(raw);

      const boundaryPoint: BoundaryPoint = {
        position: entry.position,
        eventAt,
        providerRecordId,
      };
      newestBoundary = updateNewestBoundary(newestBoundary, boundaryPoint);
      oldestBoundary = updateOldestBoundary(oldestBoundary, boundaryPoint);
    }

    if (coreRows.length > 0) {
      const persisted = await persistSwaps(connection, coreRows, rawRows);
      recordsUpserted += persisted.coreUpserts;
    }
  }

  return {
    recordsFetched,
    recordsNormalized,
    recordsUpserted,
    checkpoint: mergeCheckpoint(checkpoint, runId, newestBoundary, oldestBoundary),
  };
}

export const kyberswapProviderAdapter: ProviderAdapter = {
  key: "kyberswap",
  streamKey: KYBERSWAP_STREAM_KEY,
  sourceEndpoint: KYBERSWAP_SOURCE_ENDPOINT,
  async ingest(input: ProviderIngestInput): Promise<ProviderIngestOutput> {
    return ingestKyberswap(
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
