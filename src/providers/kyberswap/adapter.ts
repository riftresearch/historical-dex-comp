import type { DuckDBConnection } from "@duckdb/node-api";
import type { IngestMode } from "../../ingest/types";
import { isSwapRowWithinIngestScope } from "../../ingest/swap-scope";
import { fetchJsonWithRetry } from "../../lib/http";
import type { NormalizedSwapRowInput } from "../../domain/normalized-swap";
import type { IngestCheckpoint } from "../../storage/repositories/ingest-checkpoints";
import { queryFirstPrepared } from "../../storage/duckdb-utils";
import { persistSwaps, type RawSwapRowInput } from "../../storage/repositories/swaps";
import { atomicToNormalized } from "../../utils/amount-normalization";
import { sha256Hex } from "../../utils/hash";
import { addDays, parseDateOrNull, toUnixSeconds } from "../../utils/time";
import type { ProviderAdapter, ProviderIngestInput, ProviderIngestOutput } from "../types";

const KYBERSWAP_DEFAULT_META_AGGREGATOR = "0x6131b5fae19ea4f9d964eac0408e4408b66337b5";
const KYBERSWAP_SWAPPED_TOPIC0 =
  "0xd6d4f5681c246c9f42c203e287975af1601f8df8035a9251f79aab5c8f09e2f8";
const KYBERSWAP_STREAM_KEY = "swaps:kyberswap:meta_aggregation_router_v2:swapped:evm";
const KYBERSWAP_SOURCE_ENDPOINT = "eth_getLogs:kyberswap:meta_aggregation_router_v2:evm";
const KYBERSWAP_WINDOW_DAYS = 30;
const KYBERSWAP_MAX_BLOCK_RANGE = 2_000;
const KYBERSWAP_MAX_SPLIT_DEPTH = 12;
const KYBERSWAP_BLOCK_TIMESTAMP_BATCH_SIZE = 100;
const DEFAULT_BASE_RPC_URL = "https://base-rpc.publicnode.com";

const CHAIN_ID_MAINNET = "1";
const CHAIN_CANONICAL_MAINNET = "eip155:1";
const CHAIN_RAW_MAINNET = "ethereum";

const CHAIN_ID_BASE = "8453";
const CHAIN_CANONICAL_BASE = "eip155:8453";
const CHAIN_RAW_BASE = "base";

const ETH_SENTINEL_ADDRESS = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";

const USDC_ADDRESS_MAINNET = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
const USDT_ADDRESS_MAINNET = "0xdac17f958d2ee523a2206206994597c13d831ec7";
const WETH_ADDRESS_MAINNET = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2";

const USDC_ADDRESS_BASE = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913";
const USDT_ADDRESS_BASE = "0xfde4c96c8593536e31f229ea8f37b2ada2699bb2";
const WETH_ADDRESS_BASE = "0x4200000000000000000000000000000000000006";

const CBBTC_ADDRESS = "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf";

const USDC_DECIMALS = 6;
const USDT_DECIMALS = 6;
const WETH_DECIMALS = 18;
const ETH_DECIMALS = 18;
const CBBTC_DECIMALS = 8;

type SourceSymbol = "USDC" | "USDT" | "WETH" | "ETH";

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
  chainCanonical: string;
  position: CursorPosition;
  eventAt: Date;
  providerRecordId: string;
}

interface SourceTokenInfo {
  symbol: SourceSymbol;
  decimals: number;
  assetId: string;
}

interface DestinationTokenInfo {
  symbol: "USDC" | "ETH" | "WETH" | "CBBTC";
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

interface KyberswapChainConfig {
  chainCanonical: string;
  chainId: string;
  chainRaw: string;
  rpcUrl: string;
  metaAggregatorAddress: string;
  sourceEndpoint: string;
  sourceTokenMap: Record<string, SourceTokenInfo>;
  destinationTokenMap: Record<string, DestinationTokenInfo>;
}

interface KyberswapRuntimeConfig {
  chains: KyberswapChainConfig[];
}

interface KyberswapWindow {
  fromBlock: number;
  toBlock: number;
  direction: "asc" | "desc";
  skipCursor: CursorPosition | null;
}

interface StoredCursorBoundaryRow {
  provider_record_id: string;
  event_at: string | null;
  block_number: string | null;
  log_index: string | null;
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

function parseCursorMap(rawCursor: string | null | undefined): Map<string, CursorPosition> {
  const cursorByChain = new Map<string, CursorPosition>();
  if (!rawCursor) {
    return cursorByChain;
  }

  const legacyCursor = parseCursor(rawCursor);
  if (legacyCursor) {
    cursorByChain.set(CHAIN_CANONICAL_MAINNET, legacyCursor);
    return cursorByChain;
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(rawCursor);
  } catch {
    return cursorByChain;
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    return cursorByChain;
  }

  for (const [chainCanonical, rawValue] of Object.entries(parsed as Record<string, unknown>)) {
    if (typeof rawValue !== "string") {
      continue;
    }
    const cursor = parseCursor(rawValue);
    if (!cursor) {
      continue;
    }
    cursorByChain.set(chainCanonical, cursor);
  }

  return cursorByChain;
}

function encodeCursorMap(cursorByChain: Map<string, CursorPosition>): string | null {
  if (cursorByChain.size === 0) {
    return null;
  }
  const output: Record<string, string> = {};
  for (const chainCanonical of [...cursorByChain.keys()].sort()) {
    const cursor = cursorByChain.get(chainCanonical);
    if (!cursor) {
      continue;
    }
    output[chainCanonical] = encodeCursor(cursor);
  }
  return JSON.stringify(output);
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

function buildSourceTokenMap(input: {
  chainId: string;
  usdcAddress: string;
  usdtAddress: string;
  wethAddress: string;
}): Record<string, SourceTokenInfo> {
  return {
    [input.usdcAddress]: {
      symbol: "USDC",
      decimals: USDC_DECIMALS,
      assetId: `${input.chainId}:${input.usdcAddress}`,
    },
    [input.usdtAddress]: {
      symbol: "USDT",
      decimals: USDT_DECIMALS,
      assetId: `${input.chainId}:${input.usdtAddress}`,
    },
    [input.wethAddress]: {
      symbol: "WETH",
      decimals: WETH_DECIMALS,
      assetId: `${input.chainId}:${input.wethAddress}`,
    },
    [ETH_SENTINEL_ADDRESS]: {
      symbol: "ETH",
      decimals: ETH_DECIMALS,
      assetId: `${input.chainId}:native`,
    },
  };
}

function buildDestinationTokenMap(input: {
  chainId: string;
  usdcAddress: string;
  wethAddress: string;
  cbbtcAddress: string;
}): Record<string, DestinationTokenInfo> {
  return {
    [input.cbbtcAddress]: {
      symbol: "CBBTC",
      decimals: CBBTC_DECIMALS,
      assetId: `${input.chainId}:${input.cbbtcAddress}`,
    },
    [input.usdcAddress]: {
      symbol: "USDC",
      decimals: USDC_DECIMALS,
      assetId: `${input.chainId}:${input.usdcAddress}`,
    },
    [input.wethAddress]: {
      symbol: "WETH",
      decimals: WETH_DECIMALS,
      assetId: `${input.chainId}:${input.wethAddress}`,
    },
    [ETH_SENTINEL_ADDRESS]: {
      symbol: "ETH",
      decimals: ETH_DECIMALS,
      assetId: `${input.chainId}:native`,
    },
  };
}

function isTargetRoute(sourceToken: SourceTokenInfo, destinationToken: DestinationTokenInfo): boolean {
  if (
    destinationToken.symbol === "CBBTC" &&
    (sourceToken.symbol === "USDC" ||
      sourceToken.symbol === "USDT" ||
      sourceToken.symbol === "ETH" ||
      sourceToken.symbol === "WETH")
  ) {
    return true;
  }

  if (sourceToken.symbol === "USDC" && (destinationToken.symbol === "ETH" || destinationToken.symbol === "WETH")) {
    return true;
  }

  if ((sourceToken.symbol === "ETH" || sourceToken.symbol === "WETH") && destinationToken.symbol === "USDC") {
    return true;
  }

  return false;
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

async function rpcBatchCall<T>(
  rpcUrl: string,
  requests: Array<{ id: number; method: string; params: unknown[] }>,
): Promise<Map<number, T>> {
  if (requests.length === 0) {
    return new Map();
  }

  const response = await fetchJsonWithRetry<unknown>(
    rpcUrl,
    {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify(
        requests.map((request) => ({
          jsonrpc: "2.0",
          id: request.id,
          method: request.method,
          params: request.params,
        })),
      ),
    },
    {
      attempts: 5,
      initialDelayMs: 400,
      maxDelayMs: 5_000,
      timeoutMs: 90_000,
    },
  );

  if (!Array.isArray(response)) {
    throw new Error("RPC batch returned a non-array response.");
  }

  const responseById = new Map<number, JsonRpcResponse<T>>();
  for (const item of response) {
    if (!item || typeof item !== "object" || Array.isArray(item)) {
      continue;
    }
    const parsed = item as JsonRpcResponse<T>;
    if (typeof parsed.id !== "number") {
      continue;
    }
    responseById.set(parsed.id, parsed);
  }

  const results = new Map<number, T>();
  for (const request of requests) {
    const parsed = responseById.get(request.id);
    if (!parsed) {
      throw new Error(`RPC batch missing response for id=${request.id}.`);
    }
    if (parsed.error) {
      const detail = parsed.error.data
        ? ` data=${JSON.stringify(parsed.error.data).slice(0, 300)}`
        : "";
      throw new Error(
        `RPC ${request.method} failed (${parsed.error.code}): ${parsed.error.message}${detail}`,
      );
    }
    if (parsed.result === undefined) {
      throw new Error(`RPC ${request.method} returned no result for id=${request.id}.`);
    }
    results.set(request.id, parsed.result);
  }

  return results;
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

async function warmBlockTimestamps(
  rpcUrl: string,
  blockNumbers: number[],
  blockTimestampCache: Map<number, Date>,
): Promise<void> {
  const missingBlockNumbers = [...new Set(blockNumbers)]
    .filter((blockNumber) => !blockTimestampCache.has(blockNumber))
    .sort((left, right) => left - right);

  async function warmBatch(batch: number[]): Promise<void> {
    try {
      const blocksById = await rpcBatchCall<EthBlock | null>(
        rpcUrl,
        batch.map((blockNumber) => ({
          id: blockNumber,
          method: "eth_getBlockByNumber",
          params: [numberToHex(blockNumber), false],
        })),
      );

      for (const blockNumber of batch) {
        const block = blocksById.get(blockNumber);
        if (!block || !block.timestamp) {
          throw new Error(`eth_getBlockByNumber returned empty block for ${blockNumber}.`);
        }
        const timestampSeconds = hexToNumber(block.timestamp, "block.timestamp");
        blockTimestampCache.set(blockNumber, new Date(timestampSeconds * 1_000));
      }
    } catch (error) {
      if (batch.length <= 1) {
        if (batch[0] !== undefined) {
          await getBlockTimestamp(rpcUrl, batch[0], blockTimestampCache);
          return;
        }
        throw error;
      }
      const midpoint = Math.floor(batch.length / 2);
      await warmBatch(batch.slice(0, midpoint));
      await warmBatch(batch.slice(midpoint));
    }
  }

  for (
    let startIndex = 0;
    startIndex < missingBlockNumbers.length;
    startIndex += KYBERSWAP_BLOCK_TIMESTAMP_BATCH_SIZE
  ) {
    const batch = missingBlockNumbers.slice(
      startIndex,
      startIndex + KYBERSWAP_BLOCK_TIMESTAMP_BATCH_SIZE,
    );
    await warmBatch(batch);
  }
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

function isSplittableLogRangeError(error: unknown): boolean {
  const message = String(error).toLowerCase();
  return (
    message.includes("range is too large") ||
    message.includes("max is") ||
    message.includes("too many results") ||
    message.includes("response size exceeded") ||
    message.includes("query returned more than") ||
    message.includes("more than") ||
    message.includes("request timed out") ||
    message.includes("timed out") ||
    message.includes("timeout") ||
    message.includes("deadline exceeded")
  );
}

async function fetchSwappedLogsAdaptive(
  rpcUrl: string,
  metaAggregatorAddress: string,
  fromBlock: number,
  toBlock: number,
  depth = 0,
): Promise<EthLog[]> {
  try {
    return await fetchSwappedLogs(rpcUrl, metaAggregatorAddress, fromBlock, toBlock);
  } catch (error) {
    if (
      !isSplittableLogRangeError(error) ||
      fromBlock >= toBlock ||
      depth >= KYBERSWAP_MAX_SPLIT_DEPTH
    ) {
      throw error;
    }

    const midpoint = Math.floor((fromBlock + toBlock) / 2);
    const older = await fetchSwappedLogsAdaptive(
      rpcUrl,
      metaAggregatorAddress,
      fromBlock,
      midpoint,
      depth + 1,
    );
    const newer = await fetchSwappedLogsAdaptive(
      rpcUrl,
      metaAggregatorAddress,
      midpoint + 1,
      toBlock,
      depth + 1,
    );
    return [...older, ...newer];
  }
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
  newestByChain: Map<string, BoundaryPoint>,
  oldestByChain: Map<string, BoundaryPoint>,
): IngestCheckpoint | null {
  if (!existing && newestByChain.size === 0 && oldestByChain.size === 0) {
    return null;
  }

  const newestCursorByChain = parseCursorMap(existing?.newestCursor);
  const oldestCursorByChain = parseCursorMap(existing?.oldestCursor);

  let newestEventAt = existing?.newestEventAt ?? null;
  let newestProviderRecordId = existing?.newestProviderRecordId ?? null;
  for (const [chainCanonical, boundary] of newestByChain.entries()) {
    const existingCursor = newestCursorByChain.get(chainCanonical);
    if (!existingCursor || compareCursorPosition(boundary.position, existingCursor) > 0) {
      newestCursorByChain.set(chainCanonical, boundary.position);
    }
    if (
      !newestEventAt ||
      boundary.eventAt > newestEventAt ||
      (boundary.eventAt.getTime() === newestEventAt.getTime() &&
        boundary.providerRecordId > (newestProviderRecordId ?? ""))
    ) {
      newestEventAt = boundary.eventAt;
      newestProviderRecordId = boundary.providerRecordId;
    }
  }

  let oldestEventAt = existing?.oldestEventAt ?? null;
  let oldestProviderRecordId = existing?.oldestProviderRecordId ?? null;
  for (const [chainCanonical, boundary] of oldestByChain.entries()) {
    const existingCursor = oldestCursorByChain.get(chainCanonical);
    if (!existingCursor || compareCursorPosition(boundary.position, existingCursor) < 0) {
      oldestCursorByChain.set(chainCanonical, boundary.position);
    }
    if (
      !oldestEventAt ||
      boundary.eventAt < oldestEventAt ||
      (boundary.eventAt.getTime() === oldestEventAt.getTime() &&
        boundary.providerRecordId < (oldestProviderRecordId ?? "~"))
    ) {
      oldestEventAt = boundary.eventAt;
      oldestProviderRecordId = boundary.providerRecordId;
    }
  }

  return {
    streamKey: KYBERSWAP_STREAM_KEY,
    syncScope: "swaps",
    newestCursor: encodeCursorMap(newestCursorByChain),
    newestEventAt,
    newestProviderRecordId,
    oldestCursor: encodeCursorMap(oldestCursorByChain),
    oldestEventAt,
    oldestProviderRecordId,
    updatedAt: new Date(),
    runId,
  };
}

async function getStoredBoundaryForChain(
  connection: DuckDBConnection,
  chainCanonical: string,
  direction: "newest" | "oldest",
): Promise<BoundaryPoint | null> {
  const orderDirection = direction === "newest" ? "DESC" : "ASC";
  const row = await queryFirstPrepared<StoredCursorBoundaryRow>(
    connection,
    `SELECT
      sc.provider_record_id,
      CAST(sc.event_at AS VARCHAR) AS event_at,
      json_extract_string(sr.raw_json, '$.blockNumber') AS block_number,
      json_extract_string(sr.raw_json, '$.logIndex') AS log_index
     FROM swaps_core sc
     JOIN swaps_raw sr
       ON sr.normalized_id = sc.normalized_id
     WHERE sc.source_chain_canonical = ?
       AND sc.event_at IS NOT NULL
     ORDER BY sc.event_at ${orderDirection}, sc.provider_record_id ${orderDirection}
     LIMIT 1`,
    [chainCanonical],
  );
  if (!row || !row.block_number || !row.log_index) {
    return null;
  }

  const eventAt = parseDateOrNull(row.event_at);
  if (!eventAt) {
    return null;
  }

  return {
    chainCanonical,
    position: {
      blockNumber: hexToNumber(row.block_number, "storedBoundary.blockNumber"),
      logIndex: hexToNumber(row.log_index, "storedBoundary.logIndex"),
    },
    eventAt,
    providerRecordId: row.provider_record_id,
  };
}

async function hydrateCheckpointFromStoredSwaps(
  connection: DuckDBConnection,
  checkpoint: IngestCheckpoint | null,
  runId: string,
  chains: KyberswapChainConfig[],
): Promise<IngestCheckpoint | null> {
  const newestCursorByChain = parseCursorMap(checkpoint?.newestCursor);
  const oldestCursorByChain = parseCursorMap(checkpoint?.oldestCursor);

  let newestEventAt = checkpoint?.newestEventAt ?? null;
  let newestProviderRecordId = checkpoint?.newestProviderRecordId ?? null;
  let oldestEventAt = checkpoint?.oldestEventAt ?? null;
  let oldestProviderRecordId = checkpoint?.oldestProviderRecordId ?? null;
  let changed = false;

  for (const chain of chains) {
    if (!newestCursorByChain.has(chain.chainCanonical)) {
      const boundary = await getStoredBoundaryForChain(connection, chain.chainCanonical, "newest");
      if (boundary) {
        newestCursorByChain.set(chain.chainCanonical, boundary.position);
        if (
          !newestEventAt ||
          boundary.eventAt > newestEventAt ||
          (boundary.eventAt.getTime() === newestEventAt.getTime() &&
            boundary.providerRecordId > (newestProviderRecordId ?? ""))
        ) {
          newestEventAt = boundary.eventAt;
          newestProviderRecordId = boundary.providerRecordId;
        }
        changed = true;
      }
    }

    if (!oldestCursorByChain.has(chain.chainCanonical)) {
      const boundary = await getStoredBoundaryForChain(connection, chain.chainCanonical, "oldest");
      if (boundary) {
        oldestCursorByChain.set(chain.chainCanonical, boundary.position);
        if (
          !oldestEventAt ||
          boundary.eventAt < oldestEventAt ||
          (boundary.eventAt.getTime() === oldestEventAt.getTime() &&
            boundary.providerRecordId < (oldestProviderRecordId ?? ""))
        ) {
          oldestEventAt = boundary.eventAt;
          oldestProviderRecordId = boundary.providerRecordId;
        }
        changed = true;
      }
    }
  }

  if (!checkpoint && newestCursorByChain.size === 0 && oldestCursorByChain.size === 0) {
    return null;
  }
  if (!changed && checkpoint) {
    return checkpoint;
  }

  return {
    streamKey: checkpoint?.streamKey ?? KYBERSWAP_STREAM_KEY,
    syncScope: checkpoint?.syncScope ?? "swaps",
    newestCursor: encodeCursorMap(newestCursorByChain),
    newestEventAt,
    newestProviderRecordId,
    oldestCursor: encodeCursorMap(oldestCursorByChain),
    oldestEventAt,
    oldestProviderRecordId,
    updatedAt: new Date(),
    runId: checkpoint?.runId ?? runId,
  };
}

function resolveRuntimeConfig(): KyberswapRuntimeConfig {
  const mainnetRpcUrl = nonEmptyOrNull(Bun.env.KYBERSWAP_RPC_URL ?? process.env.KYBERSWAP_RPC_URL);
  if (!mainnetRpcUrl) {
    throw new Error("Missing KYBERSWAP_RPC_URL environment variable.");
  }

  const baseRpcUrl =
    nonEmptyOrNull(
      Bun.env.KYBERSWAP_BASE_RPC_URL ??
        process.env.KYBERSWAP_BASE_RPC_URL ??
        Bun.env.BASE_RPC_URL ??
        process.env.BASE_RPC_URL,
    ) ?? DEFAULT_BASE_RPC_URL;

  const configuredMainnetMetaAggregator = nonEmptyOrNull(
    Bun.env.KYBERSWAP_META_AGGREGATOR ?? process.env.KYBERSWAP_META_AGGREGATOR,
  );
  const mainnetMetaAggregatorAddress = normalizeAddress(
    configuredMainnetMetaAggregator ?? KYBERSWAP_DEFAULT_META_AGGREGATOR,
  );
  if (!mainnetMetaAggregatorAddress) {
    throw new Error("Invalid KYBERSWAP_META_AGGREGATOR environment variable.");
  }

  const configuredBaseMetaAggregator = nonEmptyOrNull(
    Bun.env.KYBERSWAP_BASE_META_AGGREGATOR ?? process.env.KYBERSWAP_BASE_META_AGGREGATOR,
  );
  const baseMetaAggregatorAddress = normalizeAddress(
    configuredBaseMetaAggregator ?? mainnetMetaAggregatorAddress,
  );
  if (!baseMetaAggregatorAddress) {
    throw new Error("Invalid KYBERSWAP_BASE_META_AGGREGATOR environment variable.");
  }

  return {
    chains: [
      {
        chainCanonical: CHAIN_CANONICAL_MAINNET,
        chainId: CHAIN_ID_MAINNET,
        chainRaw: CHAIN_RAW_MAINNET,
        rpcUrl: mainnetRpcUrl,
        metaAggregatorAddress: mainnetMetaAggregatorAddress,
        sourceEndpoint: `${KYBERSWAP_SOURCE_ENDPOINT}:${CHAIN_CANONICAL_MAINNET}`,
        sourceTokenMap: buildSourceTokenMap({
          chainId: CHAIN_ID_MAINNET,
          usdcAddress: USDC_ADDRESS_MAINNET,
          usdtAddress: USDT_ADDRESS_MAINNET,
          wethAddress: WETH_ADDRESS_MAINNET,
        }),
        destinationTokenMap: buildDestinationTokenMap({
          chainId: CHAIN_ID_MAINNET,
          usdcAddress: USDC_ADDRESS_MAINNET,
          wethAddress: WETH_ADDRESS_MAINNET,
          cbbtcAddress: CBBTC_ADDRESS,
        }),
      },
      {
        chainCanonical: CHAIN_CANONICAL_BASE,
        chainId: CHAIN_ID_BASE,
        chainRaw: CHAIN_RAW_BASE,
        rpcUrl: baseRpcUrl,
        metaAggregatorAddress: baseMetaAggregatorAddress,
        sourceEndpoint: `${KYBERSWAP_SOURCE_ENDPOINT}:${CHAIN_CANONICAL_BASE}`,
        sourceTokenMap: buildSourceTokenMap({
          chainId: CHAIN_ID_BASE,
          usdcAddress: USDC_ADDRESS_BASE,
          usdtAddress: USDT_ADDRESS_BASE,
          wethAddress: WETH_ADDRESS_BASE,
        }),
        destinationTokenMap: buildDestinationTokenMap({
          chainId: CHAIN_ID_BASE,
          usdcAddress: USDC_ADDRESS_BASE,
          wethAddress: WETH_ADDRESS_BASE,
          cbbtcAddress: CBBTC_ADDRESS,
        }),
      },
    ],
  };
}

async function buildKyberswapWindow(
  chain: KyberswapChainConfig,
  mode: IngestMode,
  checkpoint: IngestCheckpoint | null,
  now: Date,
  blockTimestampCache: Map<number, Date>,
  newestCursor: CursorPosition | null,
  oldestCursor: CursorPosition | null,
): Promise<KyberswapWindow> {
  const latestBlockNumber = await getLatestBlockNumber(chain.rpcUrl);

  if (mode === "sync_newer") {
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
      chain.rpcUrl,
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
    if (oldestCursor) {
      const anchorTimestampSeconds = toUnixSeconds(
        await getBlockTimestamp(chain.rpcUrl, oldestCursor.blockNumber, blockTimestampCache),
      );
      const fromTimestampSeconds = Math.max(
        0,
        anchorTimestampSeconds - KYBERSWAP_WINDOW_DAYS * 24 * 60 * 60,
      );
      const fromBlock = await findFirstBlockAtOrAfterTimestamp(
        chain.rpcUrl,
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
    chain.rpcUrl,
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
  const blockRange = clampBlockRange(pageSize);
  const maxRangesPerChain =
    maxPages === null
      ? null
      : Math.max(1, Math.floor(maxPages / Math.max(1, config.chains.length)));

  const effectiveCheckpoint = await hydrateCheckpointFromStoredSwaps(
    connection,
    checkpoint,
    runId,
    config.chains,
  );
  const newestCursorByChain = parseCursorMap(effectiveCheckpoint?.newestCursor);
  const oldestCursorByChain = parseCursorMap(effectiveCheckpoint?.oldestCursor);

  let recordsFetched = 0;
  let recordsNormalized = 0;
  let recordsUpserted = 0;
  const newestByChain = new Map<string, BoundaryPoint>();
  const oldestByChain = new Map<string, BoundaryPoint>();

  for (const chain of config.chains) {
    const blockTimestampCache = new Map<number, Date>();
    const window = await buildKyberswapWindow(
      chain,
      mode,
      effectiveCheckpoint,
      now,
      blockTimestampCache,
      newestCursorByChain.get(chain.chainCanonical) ?? null,
      oldestCursorByChain.get(chain.chainCanonical) ?? null,
    );

    if (window.fromBlock > window.toBlock) {
      continue;
    }

    console.log(
      [
        "[ingest][kyberswap] chain_window",
        `chain=${chain.chainCanonical}`,
        `mode=${mode}`,
        `direction=${window.direction}`,
        `fromBlock=${window.fromBlock}`,
        `toBlock=${window.toBlock}`,
        `skipCursor=${window.skipCursor ? encodeCursor(window.skipCursor) : "none"}`,
      ].join(" | "),
    );

    let rangeCount = 0;
    let nextFromBlock = window.fromBlock;
    let nextToBlock = window.toBlock;

    while (true) {
      if (maxRangesPerChain !== null && rangeCount >= maxRangesPerChain) {
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

      let logs: EthLog[];
      try {
        logs = await fetchSwappedLogsAdaptive(
          chain.rpcUrl,
          chain.metaAggregatorAddress,
          rangeFromBlock,
          rangeToBlock,
        );
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        throw new Error(
          [
            `kyberswap chain=${chain.chainCanonical}`,
            `range=${rangeFromBlock}-${rangeToBlock}`,
            `mode=${mode}`,
            `rangeIndex=${rangeCount + 1}`,
            message,
          ].join(" | "),
        );
      }
      rangeCount += 1;
      recordsFetched += logs.length;

      if (rangeCount === 1 || rangeCount % 50 === 0 || logs.length > 0) {
        console.log(
          [
            "[ingest][kyberswap] range_done",
            `chain=${chain.chainCanonical}`,
            `range=${rangeFromBlock}-${rangeToBlock}`,
            `rangeIndex=${rangeCount}`,
            `logs=${logs.length}`,
          ].join(" | "),
        );
      }

      if (logs.length === 0) {
        continue;
      }

      const observedAt = new Date();
      const sourceCursor = `${chain.chainCanonical}:blocks:${rangeFromBlock}-${rangeToBlock}`;
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

      const matchedEntries: Array<{
        log: EthLog;
        position: CursorPosition;
        decoded: SwappedEventDecoded;
        sourceToken: SourceTokenInfo;
        destinationToken: DestinationTokenInfo;
        txHash: string;
      }> = [];

      for (const entry of logsWithPositions) {
        if (entry.log.removed) {
          continue;
        }
        if (shouldSkipByCursor(mode, entry.position, window.skipCursor)) {
          continue;
        }

        const decoded = decodeSwappedEvent(entry.log.data);
        if (!decoded) {
          continue;
        }

        const sourceToken = chain.sourceTokenMap[decoded.srcToken] ?? null;
        const destinationToken = chain.destinationTokenMap[decoded.dstToken] ?? null;
        if (!sourceToken || !destinationToken || !isTargetRoute(sourceToken, destinationToken)) {
          continue;
        }

        const txHash = normalizeHash(entry.log.transactionHash);
        if (!txHash) {
          continue;
        }

        matchedEntries.push({
          log: entry.log,
          position: entry.position,
          decoded,
          sourceToken,
          destinationToken,
          txHash,
        });
      }

      await warmBlockTimestamps(
        chain.rpcUrl,
        matchedEntries.map((entry) => entry.position.blockNumber),
        blockTimestampCache,
      );

      for (const entry of matchedEntries) {
        const { decoded, destinationToken, sourceToken, txHash } = entry;

        const providerRecordId = `${chain.chainCanonical}:${txHash}:${entry.position.logIndex}`;
        const eventAt =
          blockTimestampCache.get(entry.position.blockNumber) ??
          (await getBlockTimestamp(chain.rpcUrl, entry.position.blockNumber, blockTimestampCache));
        const rawJson = JSON.stringify(entry.log);
        const rawHash = sha256Hex(rawJson);

        const core: NormalizedSwapRowInput = {
          normalized_id: providerRecordId,
          provider_key: "kyberswap",
          provider_record_id: providerRecordId,
          provider_parent_id: `${chain.chainCanonical}:${txHash}`,
          record_granularity: "swap_event",
          status_canonical: "success",
          status_raw: "swapped",
          failure_reason_raw: null,
          created_at: eventAt,
          updated_at: eventAt,
          event_at: eventAt,
          source_chain_canonical: chain.chainCanonical,
          destination_chain_canonical: chain.chainCanonical,
          source_chain_raw: chain.chainRaw,
          destination_chain_raw: chain.chainRaw,
          source_chain_id_raw: chain.chainId,
          destination_chain_id_raw: chain.chainId,
          source_asset_id: sourceToken.assetId,
          destination_asset_id: destinationToken.assetId,
          source_asset_symbol: sourceToken.symbol,
          destination_asset_symbol: destinationToken.symbol,
          source_asset_decimals: sourceToken.decimals,
          destination_asset_decimals: destinationToken.decimals,
          amount_in_atomic: decoded.spentAmountAtomic,
          amount_out_atomic: decoded.returnAmountAtomic,
          amount_in_normalized: atomicToNormalized(decoded.spentAmountAtomic, sourceToken.decimals),
          amount_out_normalized: atomicToNormalized(decoded.returnAmountAtomic, destinationToken.decimals),
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
          source_endpoint: chain.sourceEndpoint,
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
          source_endpoint: chain.sourceEndpoint,
          source_cursor: sourceCursor,
          run_id: runId,
        };

        recordsNormalized += 1;
        coreRows.push(core);
        rawRows.push(raw);

        const boundaryPoint: BoundaryPoint = {
          chainCanonical: chain.chainCanonical,
          position: entry.position,
          eventAt,
          providerRecordId,
        };

        const currentNewest = newestByChain.get(chain.chainCanonical) ?? null;
        newestByChain.set(chain.chainCanonical, updateNewestBoundary(currentNewest, boundaryPoint));

        const currentOldest = oldestByChain.get(chain.chainCanonical) ?? null;
        oldestByChain.set(chain.chainCanonical, updateOldestBoundary(currentOldest, boundaryPoint));
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
    checkpoint: mergeCheckpoint(effectiveCheckpoint, runId, newestByChain, oldestByChain),
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
