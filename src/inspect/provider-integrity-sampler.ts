import type { DuckDBConnection } from "@duckdb/node-api";
import { canonicalizeChain } from "../domain/chain-canonicalization";
import type { ProviderKey } from "../domain/provider-key";
import { PROVIDER_KEYS } from "../domain/provider-key";
import {
  getChainflipScopedAssets,
  getChainflipScopedChains,
  getGardenScopedChains,
  getNearIntentsScopedChains,
  getRelayScopedChainIds,
  isSwapRowWithinIngestScope,
} from "../ingest/swap-scope";
import { fetchJsonWithRetry } from "../lib/http";
import {
  mapGardenStatus,
  mapLifiStatus,
  mapNearIntentsStatus,
  mapRelayStatus,
  mapThorchainStatus,
  type CanonicalStatus,
} from "../domain/status";
import {
  runProviderReconciliation,
  type ProviderReconciliationReport,
} from "./provider-reconciliation";
import type { LifiTransferRecord, LifiTransfersResponse } from "../providers/lifi/types";
import type { NearIntentsTransaction, NearIntentsTransactionsPagesResponse } from "../providers/nearintents/types";
import type { GardenOrder, GardenOrdersResponse, GardenSwap } from "../providers/garden/types";
import type { RelayCurrency, RelayCurrencyAmount, RelayRequestData, RelayRequestRecord, RelayRequestTx, RelayRequestsPageResponse } from "../providers/relay/types";
import type { ThorchainAction, ThorchainActionTransfer, ThorchainCoin, ThorchainActionsPageResponse } from "../providers/thorchain/types";
import { openProviderDatabase } from "../storage/provider-db";
import { queryFirstPrepared, queryPrepared } from "../storage/duckdb-utils";
import { sha256Hex } from "../utils/hash";
import { addDays, parseDateOrNull, parseUnixSecondsOrNull, toUnixSeconds } from "../utils/time";

const RELAY_SOURCE_ENDPOINT = "https://api.relay.link/requests/v2";
const LIFI_SOURCE_ENDPOINT = "https://li.quest/v1/analytics/transfers";
const THORCHAIN_BASE_URL = "https://midgard.ninerealms.com";
const THORCHAIN_SOURCE_ENDPOINT = `${THORCHAIN_BASE_URL}/v2/actions`;
const CHAINFLIP_SOURCE_ENDPOINT = "https://explorer-service-processor.chainflip.io/graphql";
const GARDEN_SOURCE_ENDPOINT = "https://api.garden.finance/v2/orders";
const NEAR_INTENTS_SOURCE_ENDPOINT = "https://explorer.near-intents.org/api/v0/transactions-pages";

const NEAR_INTENTS_REQUEST_INTERVAL_MS = 5_000;

const LIFI_RESPONSE_CAP = 1_000;
const LIFI_MIN_WINDOW_SPLIT_SECONDS = 60;
const LIFI_MAX_WINDOW_SPLIT_DEPTH = 12;

const RELAY_PAGE_SIZE = 50;
const THORCHAIN_PAGE_SIZE = 50;
const CHAINFLIP_PAGE_SIZE = 20;
const GARDEN_PAGE_SIZE = 100;
const NEAR_INTENTS_PAGE_SIZE = 200;

const DEFAULT_SAMPLE_SIZE = 25;
const DEFAULT_MAX_PAGES = 2;
const DEFAULT_SCOPES_PER_PROVIDER = 9;
const DEFAULT_WINDOW_DAYS = 30;
const DEFAULT_SEED = 1337;

const CHAIN_ID_MAINNET = "1";
const CHAIN_CANONICAL_MAINNET = "eip155:1";
const CHAIN_ID_BASE = "8453";
const CHAIN_CANONICAL_BASE = "eip155:8453";
const ZERO_ADDRESS = "0x0000000000000000000000000000000000000000";
const ETH_SENTINEL_ADDRESS = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
const USDC_ADDRESS_MAINNET = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
const USDT_ADDRESS_MAINNET = "0xdac17f958d2ee523a2206206994597c13d831ec7";
const WETH_ADDRESS_MAINNET = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2";
const USDC_ADDRESS_BASE = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913";
const USDT_ADDRESS_BASE = "0xfde4c96c8593536e31f229ea8f37b2ada2699bb2";
const WETH_ADDRESS_BASE = "0x4200000000000000000000000000000000000006";
const CBBTC_ADDRESS = "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf";
const DEFAULT_BASE_RPC_URL = "https://base-rpc.publicnode.com";
const KYBERSWAP_SWAPPED_TOPIC0 =
  "0xd6d4f5681c246c9f42c203e287975af1601f8df8035a9251f79aab5c8f09e2f8";
const COWSWAP_TRADE_TOPIC0 =
  "0xa07a543ab8a018198e99ca0184c93fe9050a79400a0a723441f84de1d972cc17";
const KYBERSWAP_DEFAULT_META_AGGREGATOR = "0x6131b5fae19ea4f9d964eac0408e4408b66337b5";
const COWSWAP_DEFAULT_SETTLEMENT_CONTRACT = "0x9008d19f58aabd9ed0d60971565aa8510560ab41";

interface ProviderBoundsRow {
  swaps: number | bigint;
  oldest_event_at: string | null;
  newest_event_at: string | null;
}

interface PresenceRow {
  provider_record_id: string;
  record_granularity: string;
}

interface CountRow {
  count: number | bigint;
}

interface ProviderCoverageWindow {
  start: Date;
  end: Date;
}

interface PresenceIndex {
  allIds: Set<string>;
  idsByGranularity: Map<string, Set<string>>;
}

interface ScopeProbe {
  scopeKey: string;
  apiRowsScanned: number;
  apiTotal: number | null;
  dbRows: number | null;
}

interface IntegrityCandidate {
  providerRecordId: string;
  recordGranularity: string;
  eventAt: Date | null;
  scopeKey: string;
}

interface IntegrityMiss {
  providerRecordId: string;
  recordGranularity: string;
  eventAt: string | null;
  scopeKey: string;
}

interface CollectorResult {
  candidates: IntegrityCandidate[];
  scopes: ScopeProbe[];
  apiRequests: number;
}

export interface ProviderIntegrityAuditOptions {
  sampleSize: number;
  maxPages: number;
  scopesPerProvider: number | null;
  windowDays: number;
  seed: number;
}

export interface ProviderIntegrityAuditInput {
  scope: ProviderKey | "all";
  options?: Partial<ProviderIntegrityAuditOptions>;
}

export interface ProviderIntegrityAuditReport {
  providerKey: ProviderKey;
  dbSwaps: number;
  dbOldestEventAt: string | null;
  dbNewestEventAt: string | null;
  windowStart: string;
  windowEnd: string;
  reconciliation: ProviderReconciliationReport;
  apiRequests: number;
  candidatesCollected: number;
  sampled: number;
  matches: number;
  misses: number;
  hitRate: number | null;
  wilsonLowerBound95: number | null;
  scopes: ScopeProbe[];
  missingSamples: IntegrityMiss[];
  notes: string[];
  error: string | null;
}

export interface IntegrityAuditRunReport {
  generatedAt: string;
  scope: ProviderKey | "all";
  options: ProviderIntegrityAuditOptions;
  providers: ProviderIntegrityAuditReport[];
}

interface RelayScope {
  key: string;
  originChainId: string;
  destinationChainId: string;
  sourceChainCanonical: string;
  destinationChainCanonical: string;
}

interface LifiScope {
  key: string;
  sourceChainId: string;
  destinationChainId: string;
  sourceChainCanonical: string;
  destinationChainCanonical: string;
  sourceTokenSymbol: "USDC" | "USDT" | "WETH" | "ETH";
  sourceTokenAddress: string;
  destinationTokenSymbol: "USDC" | "ETH" | "CBBTC";
  destinationTokenAddress: string;
}

interface GardenScope {
  key: string;
  fromChain: string;
  toChain: string;
  sourceChainCanonical: string;
  destinationChainCanonical: string;
}

interface NearIntentsScope {
  key: string;
  fromChainId: string;
  toChainId: string;
  sourceChainCanonical: string;
  destinationChainCanonical: string;
}

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
  blockHash?: string;
  blockNumber: string;
  blockTimestamp?: string;
  data: string;
  logIndex: string;
  removed?: boolean;
  topics: string[];
  transactionHash: string;
  transactionIndex?: string;
}

interface OnchainAuditRawRow {
  provider_record_id: string;
  record_granularity: string;
  source_chain_canonical: string | null;
  raw_json: string;
}

interface OnchainBlockSample {
  chainCanonical: string;
  blockNumber: number;
}

interface KyberswapAuditSourceTokenInfo {
  symbol: "USDC" | "USDT" | "WETH" | "ETH";
}

interface KyberswapAuditDestinationTokenInfo {
  symbol: "USDC" | "ETH" | "WETH" | "CBBTC";
}

interface KyberswapAuditChainConfig {
  chainCanonical: string;
  rpcUrl: string;
  metaAggregatorAddress: string;
  sourceTokenMap: Record<string, KyberswapAuditSourceTokenInfo>;
  destinationTokenMap: Record<string, KyberswapAuditDestinationTokenInfo>;
}

interface KyberswapAuditRuntimeConfig {
  chains: KyberswapAuditChainConfig[];
}

interface CowswapAuditTokenInfo {
  symbol: "USDC" | "USDT" | "WETH" | "ETH" | "CBBTC";
}

interface CowswapAuditChainConfig {
  chainCanonical: string;
  rpcUrl: string;
  settlementContractAddress: string;
  tokenMap: Record<string, CowswapAuditTokenInfo>;
}

interface CowswapAuditRuntimeConfig {
  chains: CowswapAuditChainConfig[];
}

interface ChainflipPageInfo {
  hasNextPage?: boolean;
  endCursor?: string | null;
}

interface ChainflipAuditSwapRequestNode {
  id: number;
  nativeId?: string | null;
  sourceChain?: string | null;
  destinationChain?: string | null;
  sourceAsset?: string | null;
  destinationAsset?: string | null;
  requestedBlockTimestamp?: string | null;
  completedBlockTimestamp?: string | null;
  refundEgressId?: number | null;
}

interface ChainflipAuditSwapRequestsResponse {
  data?: {
    allSwapRequests?: {
      nodes?: ChainflipAuditSwapRequestNode[];
      pageInfo?: ChainflipPageInfo;
    };
  };
  errors?: Array<{ message?: string }>;
}

function nonEmptyOrNull(value: unknown): string | null {
  if (value === null || value === undefined) {
    return null;
  }
  const trimmed = (typeof value === "string" ? value : String(value)).trim();
  return trimmed.length > 0 ? trimmed : null;
}

function toNumber(value: number | bigint | null | undefined): number {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : 0;
  }
  if (typeof value === "bigint") {
    return Number(value);
  }
  return 0;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function resolveOptions(input?: Partial<ProviderIntegrityAuditOptions>): ProviderIntegrityAuditOptions {
  return {
    sampleSize: clampPositiveInt(input?.sampleSize, DEFAULT_SAMPLE_SIZE),
    maxPages: clampPositiveInt(input?.maxPages, DEFAULT_MAX_PAGES),
    scopesPerProvider:
      input?.scopesPerProvider === null
        ? null
        : clampPositiveInt(input?.scopesPerProvider, DEFAULT_SCOPES_PER_PROVIDER),
    windowDays: clampPositiveInt(input?.windowDays, DEFAULT_WINDOW_DAYS),
    seed: clampInteger(input?.seed, DEFAULT_SEED),
  };
}

function clampPositiveInt(value: number | null | undefined, fallback: number): number {
  if (value === null || value === undefined) {
    return fallback;
  }
  if (!Number.isFinite(value) || value <= 0) {
    return fallback;
  }
  return Math.floor(value);
}

function clampInteger(value: number | null | undefined, fallback: number): number {
  if (value === null || value === undefined) {
    return fallback;
  }
  if (!Number.isFinite(value)) {
    return fallback;
  }
  return Math.trunc(value);
}

function toProviders(scope: ProviderKey | "all"): ProviderKey[] {
  return scope === "all" ? [...PROVIDER_KEYS] : [scope];
}

function hashSeed(baseSeed: number, providerKey: ProviderKey): number {
  let hash = baseSeed | 0;
  for (const char of providerKey) {
    hash = Math.imul(hash ^ char.charCodeAt(0), 1_664_525) + 1_013_904_223;
  }
  return hash >>> 0;
}

function createMulberry32(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state += 0x6d2b79f5;
    let value = state;
    value = Math.imul(value ^ (value >>> 15), value | 1);
    value ^= value + Math.imul(value ^ (value >>> 7), value | 61);
    return ((value ^ (value >>> 14)) >>> 0) / 4_294_967_296;
  };
}

function pickRandomItems<T>(items: readonly T[], count: number, rng: () => number): T[] {
  if (count >= items.length) {
    return [...items];
  }

  const copy = [...items];
  for (let index = copy.length - 1; index > 0; index -= 1) {
    const swapIndex = Math.floor(rng() * (index + 1));
    const tmp = copy[index];
    copy[index] = copy[swapIndex] as T;
    copy[swapIndex] = tmp as T;
  }
  return copy.slice(0, count);
}

function deriveCoverageWindow(bounds: ProviderBoundsRow, windowDays: number): ProviderCoverageWindow {
  const newest = parseDateOrNull(bounds.newest_event_at) ?? new Date();
  const oldest = parseDateOrNull(bounds.oldest_event_at) ?? addDays(newest, -windowDays);
  const windowStartCandidate = addDays(newest, -windowDays);
  const start = oldest > windowStartCandidate ? oldest : windowStartCandidate;

  return {
    start,
    end: newest,
  };
}

async function getProviderBounds(connection: DuckDBConnection): Promise<ProviderBoundsRow> {
  const row = await queryFirstPrepared<ProviderBoundsRow>(
    connection,
    `SELECT
      COUNT(*) AS swaps,
      CAST(MIN(event_at) AS VARCHAR) AS oldest_event_at,
      CAST(MAX(event_at) AS VARCHAR) AS newest_event_at
     FROM swaps_core`,
  );

  return (
    row ?? {
      swaps: 0,
      oldest_event_at: null,
      newest_event_at: null,
    }
  );
}

async function loadPresenceIndex(connection: DuckDBConnection): Promise<PresenceIndex> {
  const rows = await queryPrepared<PresenceRow>(
    connection,
    `SELECT provider_record_id, record_granularity
     FROM swaps_core
     WHERE provider_record_id IS NOT NULL`,
  );

  const allIds = new Set<string>();
  const idsByGranularity = new Map<string, Set<string>>();

  for (const row of rows) {
    const providerRecordId = nonEmptyOrNull(row.provider_record_id);
    const recordGranularity = nonEmptyOrNull(row.record_granularity);
    if (!providerRecordId || !recordGranularity) {
      continue;
    }

    allIds.add(providerRecordId);
    const bucket = idsByGranularity.get(recordGranularity) ?? new Set<string>();
    bucket.add(providerRecordId);
    idsByGranularity.set(recordGranularity, bucket);
  }

  return {
    allIds,
    idsByGranularity,
  };
}

function hasCandidateInDb(index: PresenceIndex, candidate: IntegrityCandidate): boolean {
  const byGranularity = index.idsByGranularity.get(candidate.recordGranularity);
  if (byGranularity && byGranularity.has(candidate.providerRecordId)) {
    return true;
  }
  return index.allIds.has(candidate.providerRecordId);
}

async function getRouteCount(
  connection: DuckDBConnection,
  sourceChainCanonical: string,
  destinationChainCanonical: string,
): Promise<number> {
  const row = await queryFirstPrepared<CountRow>(
    connection,
    `SELECT COUNT(*) AS count
     FROM swaps_core
     WHERE source_chain_canonical = ? AND destination_chain_canonical = ?`,
    [sourceChainCanonical, destinationChainCanonical],
  );
  return toNumber(row?.count ?? 0);
}

async function getTokenRouteCount(
  connection: DuckDBConnection,
  input: {
    sourceChainCanonical: string;
    destinationChainCanonical: string;
    sourceAssetSymbol: string;
    destinationAssetSymbol: string;
  },
): Promise<number> {
  const row = await queryFirstPrepared<CountRow>(
    connection,
    `SELECT COUNT(*) AS count
     FROM swaps_core
     WHERE source_chain_canonical = ?
       AND destination_chain_canonical = ?
       AND upper(source_asset_symbol) = ?
       AND upper(destination_asset_symbol) = ?`,
    [
      input.sourceChainCanonical,
      input.destinationChainCanonical,
      input.sourceAssetSymbol.toUpperCase(),
      input.destinationAssetSymbol.toUpperCase(),
    ],
  );
  return toNumber(row?.count ?? 0);
}

function shouldIncludeByWindow(eventAt: Date | null, window: ProviderCoverageWindow): boolean {
  if (!eventAt) {
    return false;
  }
  if (eventAt < window.start) {
    return false;
  }
  if (eventAt > window.end) {
    return false;
  }
  return true;
}

function toScopeCount(scopesPerProvider: number | null, totalScopes: number): number {
  if (scopesPerProvider === null) {
    return totalScopes;
  }
  return Math.max(1, Math.min(scopesPerProvider, totalScopes));
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

function numberToHex(value: number): string {
  return `0x${value.toString(16)}`;
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

function topicToAddress(topic: string | null | undefined): string | null {
  if (!topic || !/^0x[0-9a-fA-F]{64}$/.test(topic)) {
    return null;
  }
  return normalizeAddress(`0x${topic.slice(26)}`);
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
      attempts: 4,
      initialDelayMs: 400,
      maxDelayMs: 4_000,
      timeoutMs: 30_000,
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

async function fetchLogsForExactBlock(input: {
  rpcUrl: string;
  address: string;
  topic0: string;
  blockNumber: number;
}): Promise<EthLog[]> {
  return rpcCall<EthLog[]>(input.rpcUrl, "eth_getLogs", [
    {
      address: input.address,
      fromBlock: numberToHex(input.blockNumber),
      toBlock: numberToHex(input.blockNumber),
      topics: [input.topic0],
    },
  ]);
}

function buildKyberswapSourceTokenMap(input: {
  usdcAddress: string;
  usdtAddress: string;
  wethAddress: string;
}): Record<string, KyberswapAuditSourceTokenInfo> {
  return {
    [input.usdcAddress]: { symbol: "USDC" },
    [input.usdtAddress]: { symbol: "USDT" },
    [input.wethAddress]: { symbol: "WETH" },
    [ETH_SENTINEL_ADDRESS]: { symbol: "ETH" },
  };
}

function buildKyberswapDestinationTokenMap(input: {
  usdcAddress: string;
  wethAddress: string;
  cbbtcAddress: string;
}): Record<string, KyberswapAuditDestinationTokenInfo> {
  return {
    [input.cbbtcAddress]: { symbol: "CBBTC" },
    [input.usdcAddress]: { symbol: "USDC" },
    [input.wethAddress]: { symbol: "WETH" },
    [ETH_SENTINEL_ADDRESS]: { symbol: "ETH" },
  };
}

function buildCowswapTokenMap(input: {
  usdcAddress: string;
  usdtAddress: string;
  wethAddress: string;
  cbbtcAddress: string;
}): Record<string, CowswapAuditTokenInfo> {
  return {
    [input.usdcAddress]: { symbol: "USDC" },
    [input.usdtAddress]: { symbol: "USDT" },
    [input.wethAddress]: { symbol: "WETH" },
    [input.cbbtcAddress]: { symbol: "CBBTC" },
    [ZERO_ADDRESS]: { symbol: "ETH" },
    [ETH_SENTINEL_ADDRESS]: { symbol: "ETH" },
  };
}

function isKyberswapTargetRoute(
  sourceToken: KyberswapAuditSourceTokenInfo,
  destinationToken: KyberswapAuditDestinationTokenInfo,
): boolean {
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

function isCowswapTargetRoute(
  sellToken: CowswapAuditTokenInfo,
  buyToken: CowswapAuditTokenInfo,
): boolean {
  if (
    buyToken.symbol === "CBBTC" &&
    (sellToken.symbol === "USDC" ||
      sellToken.symbol === "USDT" ||
      sellToken.symbol === "ETH" ||
      sellToken.symbol === "WETH")
  ) {
    return true;
  }
  if (sellToken.symbol === "USDC" && (buyToken.symbol === "ETH" || buyToken.symbol === "WETH")) {
    return true;
  }
  if ((sellToken.symbol === "ETH" || sellToken.symbol === "WETH") && buyToken.symbol === "USDC") {
    return true;
  }
  return false;
}

function resolveKyberswapAuditRuntimeConfig(): KyberswapAuditRuntimeConfig {
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

  const mainnetMetaAggregator = normalizeAddress(
    nonEmptyOrNull(Bun.env.KYBERSWAP_META_AGGREGATOR ?? process.env.KYBERSWAP_META_AGGREGATOR) ??
      KYBERSWAP_DEFAULT_META_AGGREGATOR,
  );
  if (!mainnetMetaAggregator) {
    throw new Error("Invalid KYBERSWAP_META_AGGREGATOR environment variable.");
  }

  const baseMetaAggregator = normalizeAddress(
    nonEmptyOrNull(Bun.env.KYBERSWAP_BASE_META_AGGREGATOR ?? process.env.KYBERSWAP_BASE_META_AGGREGATOR) ??
      mainnetMetaAggregator,
  );
  if (!baseMetaAggregator) {
    throw new Error("Invalid KYBERSWAP_BASE_META_AGGREGATOR environment variable.");
  }

  return {
    chains: [
      {
        chainCanonical: CHAIN_CANONICAL_MAINNET,
        rpcUrl: mainnetRpcUrl,
        metaAggregatorAddress: mainnetMetaAggregator,
        sourceTokenMap: buildKyberswapSourceTokenMap({
          usdcAddress: USDC_ADDRESS_MAINNET,
          usdtAddress: USDT_ADDRESS_MAINNET,
          wethAddress: WETH_ADDRESS_MAINNET,
        }),
        destinationTokenMap: buildKyberswapDestinationTokenMap({
          usdcAddress: USDC_ADDRESS_MAINNET,
          wethAddress: WETH_ADDRESS_MAINNET,
          cbbtcAddress: CBBTC_ADDRESS,
        }),
      },
      {
        chainCanonical: CHAIN_CANONICAL_BASE,
        rpcUrl: baseRpcUrl,
        metaAggregatorAddress: baseMetaAggregator,
        sourceTokenMap: buildKyberswapSourceTokenMap({
          usdcAddress: USDC_ADDRESS_BASE,
          usdtAddress: USDT_ADDRESS_BASE,
          wethAddress: WETH_ADDRESS_BASE,
        }),
        destinationTokenMap: buildKyberswapDestinationTokenMap({
          usdcAddress: USDC_ADDRESS_BASE,
          wethAddress: WETH_ADDRESS_BASE,
          cbbtcAddress: CBBTC_ADDRESS,
        }),
      },
    ],
  };
}

function resolveCowswapAuditRuntimeConfig(): CowswapAuditRuntimeConfig {
  const mainnetRpcUrl = nonEmptyOrNull(
    Bun.env.COWSWAP_RPC_URL ??
      process.env.COWSWAP_RPC_URL ??
      Bun.env.KYBERSWAP_RPC_URL ??
      process.env.KYBERSWAP_RPC_URL,
  );
  if (!mainnetRpcUrl) {
    throw new Error("Missing COWSWAP_RPC_URL environment variable.");
  }

  const baseRpcUrl =
    nonEmptyOrNull(
      Bun.env.COWSWAP_BASE_RPC_URL ??
        process.env.COWSWAP_BASE_RPC_URL ??
        Bun.env.BASE_RPC_URL ??
        process.env.BASE_RPC_URL,
    ) ?? DEFAULT_BASE_RPC_URL;

  const mainnetSettlementContract = normalizeAddress(
    nonEmptyOrNull(Bun.env.COWSWAP_SETTLEMENT_CONTRACT ?? process.env.COWSWAP_SETTLEMENT_CONTRACT) ??
      COWSWAP_DEFAULT_SETTLEMENT_CONTRACT,
  );
  if (!mainnetSettlementContract) {
    throw new Error("Invalid COWSWAP_SETTLEMENT_CONTRACT environment variable.");
  }

  const baseSettlementContract = normalizeAddress(
    nonEmptyOrNull(Bun.env.COWSWAP_BASE_SETTLEMENT_CONTRACT ?? process.env.COWSWAP_BASE_SETTLEMENT_CONTRACT) ??
      mainnetSettlementContract,
  );
  if (!baseSettlementContract) {
    throw new Error("Invalid COWSWAP_BASE_SETTLEMENT_CONTRACT environment variable.");
  }

  return {
    chains: [
      {
        chainCanonical: CHAIN_CANONICAL_MAINNET,
        rpcUrl: mainnetRpcUrl,
        settlementContractAddress: mainnetSettlementContract,
        tokenMap: buildCowswapTokenMap({
          usdcAddress: USDC_ADDRESS_MAINNET,
          usdtAddress: USDT_ADDRESS_MAINNET,
          wethAddress: WETH_ADDRESS_MAINNET,
          cbbtcAddress: CBBTC_ADDRESS,
        }),
      },
      {
        chainCanonical: CHAIN_CANONICAL_BASE,
        rpcUrl: baseRpcUrl,
        settlementContractAddress: baseSettlementContract,
        tokenMap: buildCowswapTokenMap({
          usdcAddress: USDC_ADDRESS_BASE,
          usdtAddress: USDT_ADDRESS_BASE,
          wethAddress: WETH_ADDRESS_BASE,
          cbbtcAddress: CBBTC_ADDRESS,
        }),
      },
    ],
  };
}

function buildRelayScopes(): RelayScope[] {
  const chainIds = getRelayScopedChainIds();
  const scopes: RelayScope[] = [];
  for (const originChainId of chainIds) {
    for (const destinationChainId of chainIds) {
      const sourceChainCanonical = canonicalizeChain({
        provider: "relay",
        rawChainId: originChainId,
      }).canonical;
      const destinationChainCanonical = canonicalizeChain({
        provider: "relay",
        rawChainId: destinationChainId,
      }).canonical;
      scopes.push({
        key: `${originChainId}->${destinationChainId}`,
        originChainId,
        destinationChainId,
        sourceChainCanonical,
        destinationChainCanonical,
      });
    }
  }
  return scopes;
}

function buildLifiScopes(): LifiScope[] {
  const chainConfigs = [
    {
      chainId: CHAIN_ID_MAINNET,
      chainCanonical: CHAIN_CANONICAL_MAINNET,
      usdcAddress: USDC_ADDRESS_MAINNET,
      usdtAddress: USDT_ADDRESS_MAINNET,
      wethAddress: WETH_ADDRESS_MAINNET,
    },
    {
      chainId: CHAIN_ID_BASE,
      chainCanonical: CHAIN_CANONICAL_BASE,
      usdcAddress: USDC_ADDRESS_BASE,
      usdtAddress: USDT_ADDRESS_BASE,
      wethAddress: WETH_ADDRESS_BASE,
    },
  ];

  const scopes: LifiScope[] = [];
  for (const chain of chainConfigs) {
    const cbbtcRoutes: Array<Pick<LifiScope, "sourceTokenSymbol" | "sourceTokenAddress">> = [
      { sourceTokenSymbol: "USDC", sourceTokenAddress: chain.usdcAddress },
      { sourceTokenSymbol: "USDT", sourceTokenAddress: chain.usdtAddress },
      { sourceTokenSymbol: "WETH", sourceTokenAddress: chain.wethAddress },
      { sourceTokenSymbol: "ETH", sourceTokenAddress: ZERO_ADDRESS },
    ];

    for (const route of cbbtcRoutes) {
      scopes.push({
        key: `${chain.chainId}:${route.sourceTokenSymbol}->CBBTC`,
        sourceChainId: chain.chainId,
        destinationChainId: chain.chainId,
        sourceChainCanonical: chain.chainCanonical,
        destinationChainCanonical: chain.chainCanonical,
        sourceTokenSymbol: route.sourceTokenSymbol,
        sourceTokenAddress: route.sourceTokenAddress,
        destinationTokenSymbol: "CBBTC",
        destinationTokenAddress: CBBTC_ADDRESS,
      });
    }

    scopes.push({
      key: `${chain.chainId}:USDC->ETH`,
      sourceChainId: chain.chainId,
      destinationChainId: chain.chainId,
      sourceChainCanonical: chain.chainCanonical,
      destinationChainCanonical: chain.chainCanonical,
      sourceTokenSymbol: "USDC",
      sourceTokenAddress: chain.usdcAddress,
      destinationTokenSymbol: "ETH",
      destinationTokenAddress: ZERO_ADDRESS,
    });
    scopes.push({
      key: `${chain.chainId}:ETH->USDC`,
      sourceChainId: chain.chainId,
      destinationChainId: chain.chainId,
      sourceChainCanonical: chain.chainCanonical,
      destinationChainCanonical: chain.chainCanonical,
      sourceTokenSymbol: "ETH",
      sourceTokenAddress: ZERO_ADDRESS,
      destinationTokenSymbol: "USDC",
      destinationTokenAddress: chain.usdcAddress,
    });
  }

  return scopes;
}

function decodeKyberswapSwappedEvent(logData: string): {
  srcToken: string;
  dstToken: string;
} | null {
  const srcTokenWord = readWord(logData, 1);
  const dstTokenWord = readWord(logData, 2);
  if (!srcTokenWord || !dstTokenWord) {
    return null;
  }
  const srcToken = wordToAddress(srcTokenWord);
  const dstToken = wordToAddress(dstTokenWord);
  if (!srcToken || !dstToken) {
    return null;
  }
  return { srcToken, dstToken };
}

function decodeDynamicBytes(data: string, offsetBytes: number): string | null {
  if (!data.startsWith("0x") || offsetBytes < 0) {
    return null;
  }
  const payload = data.slice(2);
  const lengthWordStart = offsetBytes * 2;
  const lengthWordEnd = lengthWordStart + 64;
  if (payload.length < lengthWordEnd) {
    return null;
  }
  const lengthWord = payload.slice(lengthWordStart, lengthWordEnd);
  if (!/^[0-9a-fA-F]{64}$/.test(lengthWord)) {
    return null;
  }

  let bytesLength: number;
  try {
    const parsed = BigInt(`0x${lengthWord}`);
    if (parsed > BigInt(Number.MAX_SAFE_INTEGER)) {
      return null;
    }
    bytesLength = Number(parsed);
  } catch {
    return null;
  }

  const dataStart = lengthWordEnd;
  const dataEnd = dataStart + bytesLength * 2;
  if (payload.length < dataEnd) {
    return null;
  }
  const bytesHex = payload.slice(dataStart, dataEnd).toLowerCase();
  if (!/^[0-9a-f]*$/.test(bytesHex)) {
    return null;
  }
  return `0x${bytesHex}`;
}

function decodeCowswapTradeEvent(log: EthLog): {
  sellToken: string;
  buyToken: string;
} | null {
  const sellTokenWord = readWord(log.data, 0);
  const buyTokenWord = readWord(log.data, 1);
  const orderUidOffsetWord = readWord(log.data, 5);
  if (!sellTokenWord || !buyTokenWord || !orderUidOffsetWord) {
    return null;
  }

  const sellToken = wordToAddress(sellTokenWord);
  const buyToken = wordToAddress(buyTokenWord);
  if (!sellToken || !buyToken) {
    return null;
  }

  try {
    const offset = Number(BigInt(`0x${orderUidOffsetWord}`));
    if (Number.isFinite(offset) && offset >= 0) {
      decodeDynamicBytes(log.data, offset);
    }
  } catch {
    // ignore; orderUid is not required for audit candidate generation
  }

  return { sellToken, buyToken };
}

function parseBlockNumberFromRawJson(rawJson: string): number | null {
  try {
    const parsed = JSON.parse(rawJson) as { blockNumber?: string | null };
    const blockNumber = nonEmptyOrNull(parsed.blockNumber);
    return blockNumber ? hexToNumber(blockNumber, "raw.blockNumber") : null;
  } catch {
    return null;
  }
}

async function loadOnchainBlockSamples(input: {
  connection: DuckDBConnection;
  providerKey: ProviderKey;
  window: ProviderCoverageWindow;
  scopesPerProvider: number | null;
  rng: () => number;
}): Promise<OnchainBlockSample[]> {
  const rows = await queryPrepared<OnchainAuditRawRow>(
    input.connection,
    `SELECT
       sc.provider_record_id,
       sc.record_granularity,
       sc.source_chain_canonical,
       sr.raw_json
     FROM swaps_core sc
     JOIN swaps_raw sr
       ON sr.normalized_id = sc.normalized_id
      AND sr.raw_hash = sc.raw_hash_latest
     WHERE sc.provider_key = ?
       AND sc.event_at >= ?
       AND sc.event_at <= ?`,
    [input.providerKey, input.window.start, input.window.end],
  );

  const blocks = new Map<string, OnchainBlockSample>();
  for (const row of rows) {
    const chainCanonical = nonEmptyOrNull(row.source_chain_canonical);
    const blockNumber = parseBlockNumberFromRawJson(row.raw_json);
    if (!chainCanonical || blockNumber === null) {
      continue;
    }
    blocks.set(`${chainCanonical}:${blockNumber}`, {
      chainCanonical,
      blockNumber,
    });
  }

  return pickRandomItems(
    [...blocks.values()],
    toScopeCount(input.scopesPerProvider, blocks.size),
    input.rng,
  );
}

function firstTxWithHash(txs: RelayRequestTx[] | null | undefined): RelayRequestTx | null {
  if (!txs) {
    return null;
  }
  return txs.find((tx) => nonEmptyOrNull(tx.hash) !== null) ?? null;
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

function deriveRelaySourceChainId(
  data: RelayRequestData | null | undefined,
  inTx: RelayRequestTx | null,
): string | number | null {
  const fromCurrency = firstCurrency(fallbackCurrencyIn(data))?.chainId;
  if (fromCurrency !== undefined && fromCurrency !== null) {
    return fromCurrency;
  }
  return inTx?.chainId ?? null;
}

function deriveRelayDestinationChainId(
  data: RelayRequestData | null | undefined,
  outTx: RelayRequestTx | null,
): string | number | null {
  const toCurrency = firstCurrency(fallbackCurrencyOut(data))?.chainId;
  if (toCurrency !== undefined && toCurrency !== null) {
    return toCurrency;
  }
  return outTx?.chainId ?? null;
}

function earliestRelayTxTimestamp(txs: RelayRequestTx[] | null | undefined): Date | null {
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

function deriveRelayEventAt(record: RelayRequestRecord): Date | null {
  const createdAt = parseDateOrNull(record.createdAt);
  if (createdAt) {
    return createdAt;
  }
  return earliestRelayTxTimestamp(record.data?.inTxs) ?? earliestRelayTxTimestamp(record.data?.outTxs);
}

function toUpperSymbol(value: string | null | undefined): string | null {
  const normalized = nonEmptyOrNull(value);
  if (!normalized) {
    return null;
  }
  return normalized.toUpperCase();
}

async function collectRelayCandidates(input: {
  connection: DuckDBConnection;
  window: ProviderCoverageWindow;
  options: ProviderIntegrityAuditOptions;
  rng: () => number;
}): Promise<CollectorResult> {
  const allScopes = buildRelayScopes();
  const selectedScopes = pickRandomItems(
    allScopes,
    toScopeCount(input.options.scopesPerProvider, allScopes.length),
    input.rng,
  );

  const candidates = new Map<string, IntegrityCandidate>();
  const scopeReports: ScopeProbe[] = [];
  let apiRequests = 0;

  for (const scope of selectedScopes) {
    let continuation: string | null = null;
    let pages = 0;
    let scopeRowsScanned = 0;

    while (pages < input.options.maxPages) {
      const url = new URL(RELAY_SOURCE_ENDPOINT);
      url.searchParams.set("limit", String(RELAY_PAGE_SIZE));
      url.searchParams.set("sortBy", "createdAt");
      url.searchParams.set("sortDirection", "desc");
      url.searchParams.set("originChainId", scope.originChainId);
      url.searchParams.set("destinationChainId", scope.destinationChainId);
      url.searchParams.set("startTimestamp", String(toUnixSeconds(input.window.start)));
      url.searchParams.set("endTimestamp", String(toUnixSeconds(input.window.end)));
      if (continuation) {
        url.searchParams.set("continuation", continuation);
      }

      const response = await fetchJsonWithRetry<RelayRequestsPageResponse>(
        url.toString(),
        {
          method: "GET",
          headers: {
            Accept: "application/json",
          },
        },
        {
          attempts: 4,
          initialDelayMs: 400,
          maxDelayMs: 4_000,
          timeoutMs: 30_000,
        },
      );
      apiRequests += 1;
      pages += 1;

      const requests = response.requests ?? [];
      scopeRowsScanned += requests.length;

      for (const request of requests) {
        const providerRecordId = nonEmptyOrNull(request.id);
        if (!providerRecordId) {
          continue;
        }
        if (mapRelayStatus(request.status ?? undefined) !== "success") {
          continue;
        }

        const inTx = firstTxWithHash(request.data?.inTxs);
        const outTx = firstTxWithHash(request.data?.outTxs);
        const currencyIn = fallbackCurrencyIn(request.data);
        const currencyOut = fallbackCurrencyOut(request.data);
        const sourceChainCanonical = canonicalizeChain({
          provider: "relay",
          rawChainId: deriveRelaySourceChainId(request.data, inTx),
        }).canonical;
        const destinationChainCanonical = canonicalizeChain({
          provider: "relay",
          rawChainId: deriveRelayDestinationChainId(request.data, outTx),
        }).canonical;
        const sourceAssetSymbol = toUpperSymbol(firstCurrency(currencyIn)?.symbol);
        const destinationAssetSymbol = toUpperSymbol(firstCurrency(currencyOut)?.symbol);

        if (
          !isSwapRowWithinIngestScope({
            source_chain_canonical: sourceChainCanonical,
            destination_chain_canonical: destinationChainCanonical,
            source_asset_symbol: sourceAssetSymbol,
            destination_asset_symbol: destinationAssetSymbol,
            source_asset_id: null,
            destination_asset_id: null,
          })
        ) {
          continue;
        }

        const eventAt = deriveRelayEventAt(request);
        if (!shouldIncludeByWindow(eventAt, input.window)) {
          continue;
        }

        const key = `request|${providerRecordId}`;
        if (!candidates.has(key)) {
          candidates.set(key, {
            providerRecordId,
            recordGranularity: "request",
            eventAt,
            scopeKey: scope.key,
          });
        }
      }

      continuation = nonEmptyOrNull(response.continuation);
      if (!continuation || requests.length === 0) {
        break;
      }
    }

    const dbRows = await getRouteCount(
      input.connection,
      scope.sourceChainCanonical,
      scope.destinationChainCanonical,
    );
    scopeReports.push({
      scopeKey: scope.key,
      apiRowsScanned: scopeRowsScanned,
      apiTotal: null,
      dbRows,
    });
  }

  return {
    candidates: [...candidates.values()],
    scopes: scopeReports,
    apiRequests,
  };
}

interface LifiAuditWindowChunk {
  startTimestamp: number;
  endTimestamp: number;
  depth: number;
}

function splitLifiAuditWindowChunk(
  chunk: LifiAuditWindowChunk,
): { older: LifiAuditWindowChunk; newer: LifiAuditWindowChunk } | null {
  if (chunk.depth >= LIFI_MAX_WINDOW_SPLIT_DEPTH) {
    return null;
  }
  if (chunk.endTimestamp <= chunk.startTimestamp) {
    return null;
  }
  const spanSeconds = chunk.endTimestamp - chunk.startTimestamp + 1;
  if (spanSeconds <= LIFI_MIN_WINDOW_SPLIT_SECONDS) {
    return null;
  }

  const midpoint = Math.floor((chunk.startTimestamp + chunk.endTimestamp) / 2);
  if (midpoint <= chunk.startTimestamp || midpoint >= chunk.endTimestamp) {
    return null;
  }

  return {
    older: {
      startTimestamp: chunk.startTimestamp,
      endTimestamp: midpoint,
      depth: chunk.depth + 1,
    },
    newer: {
      startTimestamp: midpoint + 1,
      endTimestamp: chunk.endTimestamp,
      depth: chunk.depth + 1,
    },
  };
}

function buildLifiTransfersUrl(
  scope: LifiScope,
  chunk: LifiAuditWindowChunk,
): string {
  const url = new URL(LIFI_SOURCE_ENDPOINT);
  url.searchParams.set("fromChain", scope.sourceChainId);
  url.searchParams.set("toChain", scope.destinationChainId);
  url.searchParams.set("fromToken", scope.sourceTokenAddress);
  url.searchParams.set("toToken", scope.destinationTokenAddress);
  url.searchParams.set("fromTimestamp", String(chunk.startTimestamp));
  url.searchParams.set("toTimestamp", String(chunk.endTimestamp));
  return url.toString();
}

function deriveLifiEventAt(record: LifiTransferRecord): Date | null {
  return (
    parseUnixSecondsOrNull(record.sending?.timestamp) ??
    parseUnixSecondsOrNull(record.receiving?.timestamp)
  );
}

function deriveLifiProviderRecordId(record: LifiTransferRecord): string | null {
  return (
    nonEmptyOrNull(record.transactionId) ??
    normalizeHash(record.sending?.txHash ?? null) ??
    normalizeHash(record.receiving?.txHash ?? null)
  );
}

async function collectLifiCandidates(input: {
  connection: DuckDBConnection;
  window: ProviderCoverageWindow;
  options: ProviderIntegrityAuditOptions;
  rng: () => number;
}): Promise<CollectorResult> {
  const allScopes = buildLifiScopes();
  const selectedScopes = pickRandomItems(
    allScopes,
    toScopeCount(input.options.scopesPerProvider, allScopes.length),
    input.rng,
  );

  const candidates = new Map<string, IntegrityCandidate>();
  const scopeReports: ScopeProbe[] = [];
  let apiRequests = 0;

  for (const scope of selectedScopes) {
    const queue: LifiAuditWindowChunk[] = [
      {
        startTimestamp: toUnixSeconds(input.window.start),
        endTimestamp: toUnixSeconds(input.window.end),
        depth: 0,
      },
    ];
    const seenWindowKeys = new Set<string>();
    let pages = 0;
    let scopeRowsScanned = 0;

    while (queue.length > 0 && pages < input.options.maxPages) {
      const chunk = queue.shift();
      if (!chunk) {
        continue;
      }
      const windowKey = `${chunk.startTimestamp}-${chunk.endTimestamp}-d${chunk.depth}`;
      if (seenWindowKeys.has(windowKey)) {
        continue;
      }
      seenWindowKeys.add(windowKey);

      const response = await fetchJsonWithRetry<LifiTransfersResponse>(
        buildLifiTransfersUrl(scope, chunk),
        {
          method: "GET",
          headers: {
            Accept: "application/json",
          },
        },
        {
          attempts: 4,
          initialDelayMs: 400,
          maxDelayMs: 4_000,
          timeoutMs: 30_000,
        },
      );
      apiRequests += 1;
      pages += 1;

      const transfers = response.transfers ?? [];
      scopeRowsScanned += transfers.length;

      for (const transfer of transfers) {
        const providerRecordId = deriveLifiProviderRecordId(transfer);
        if (!providerRecordId) {
          continue;
        }
        if (mapLifiStatus(transfer.status, transfer.substatus) !== "success") {
          continue;
        }

        const sourceChainCanonical = canonicalizeChain({
          provider: "lifi",
          rawChainId: transfer.sending?.chainId ?? transfer.sending?.token?.chainId ?? null,
        }).canonical;
        const destinationChainCanonical = canonicalizeChain({
          provider: "lifi",
          rawChainId: transfer.receiving?.chainId ?? transfer.receiving?.token?.chainId ?? null,
        }).canonical;
        const sourceAssetSymbol =
          normalizeSymbol(transfer.sending?.token?.symbol ?? null) ?? scope.sourceTokenSymbol;
        const destinationAssetSymbol =
          normalizeSymbol(transfer.receiving?.token?.symbol ?? null) ?? scope.destinationTokenSymbol;

        if (
          !isSwapRowWithinIngestScope({
            source_chain_canonical: sourceChainCanonical,
            destination_chain_canonical: destinationChainCanonical,
            source_asset_symbol: sourceAssetSymbol,
            destination_asset_symbol: destinationAssetSymbol,
            source_asset_id: toStringOrNull(transfer.sending?.token?.address ?? null),
            destination_asset_id: toStringOrNull(transfer.receiving?.token?.address ?? null),
          })
        ) {
          continue;
        }

        const sourceTokenAddress =
          normalizeAddress(transfer.sending?.token?.address ?? null) ??
          (scope.sourceTokenSymbol === "ETH" ? ZERO_ADDRESS : null);
        const destinationTokenAddress =
          normalizeAddress(transfer.receiving?.token?.address ?? null) ??
          (scope.destinationTokenSymbol === "ETH" ? ZERO_ADDRESS : null);

        if (
          sourceChainCanonical !== scope.sourceChainCanonical ||
          destinationChainCanonical !== scope.destinationChainCanonical ||
          sourceAssetSymbol !== scope.sourceTokenSymbol ||
          destinationAssetSymbol !== scope.destinationTokenSymbol ||
          sourceTokenAddress !== scope.sourceTokenAddress ||
          destinationTokenAddress !== scope.destinationTokenAddress
        ) {
          continue;
        }

        const eventAt = deriveLifiEventAt(transfer);
        if (!shouldIncludeByWindow(eventAt, input.window)) {
          continue;
        }

        const key = `transfer|${providerRecordId}`;
        if (!candidates.has(key)) {
          candidates.set(key, {
            providerRecordId,
            recordGranularity: "transfer",
            eventAt,
            scopeKey: scope.key,
          });
        }
      }

      if (transfers.length >= LIFI_RESPONSE_CAP) {
        const split = splitLifiAuditWindowChunk(chunk);
        if (split) {
          queue.unshift(split.newer);
          queue.unshift(split.older);
        }
      }
    }

    const dbRows = await getTokenRouteCount(input.connection, {
      sourceChainCanonical: scope.sourceChainCanonical,
      destinationChainCanonical: scope.destinationChainCanonical,
      sourceAssetSymbol: scope.sourceTokenSymbol,
      destinationAssetSymbol: scope.destinationTokenSymbol,
    });
    scopeReports.push({
      scopeKey: scope.key,
      apiRowsScanned: scopeRowsScanned,
      apiTotal: null,
      dbRows,
    });
  }

  return {
    candidates: [...candidates.values()],
    scopes: scopeReports,
    apiRequests,
  };
}

async function collectKyberswapCandidates(input: {
  connection: DuckDBConnection;
  window: ProviderCoverageWindow;
  options: ProviderIntegrityAuditOptions;
  rng: () => number;
}): Promise<CollectorResult> {
  const runtimeConfig = resolveKyberswapAuditRuntimeConfig();
  const sampledBlocks = await loadOnchainBlockSamples({
    connection: input.connection,
    providerKey: "kyberswap",
    window: input.window,
    scopesPerProvider: input.options.scopesPerProvider,
    rng: input.rng,
  });

  const candidates = new Map<string, IntegrityCandidate>();
  const scopeReports: ScopeProbe[] = [];
  let apiRequests = 0;

  for (const block of sampledBlocks) {
    const chain = runtimeConfig.chains.find((candidate) => candidate.chainCanonical === block.chainCanonical);
    if (!chain) {
      continue;
    }

    const logs = await fetchLogsForExactBlock({
      rpcUrl: chain.rpcUrl,
      address: chain.metaAggregatorAddress,
      topic0: KYBERSWAP_SWAPPED_TOPIC0,
      blockNumber: block.blockNumber,
    });
    apiRequests += 1;

    let scopeRowsScanned = 0;
    for (const log of logs) {
      if (log.removed) {
        continue;
      }
      const decoded = decodeKyberswapSwappedEvent(log.data);
      if (!decoded) {
        continue;
      }

      const sourceToken = chain.sourceTokenMap[decoded.srcToken] ?? null;
      const destinationToken = chain.destinationTokenMap[decoded.dstToken] ?? null;
      if (!sourceToken || !destinationToken || !isKyberswapTargetRoute(sourceToken, destinationToken)) {
        continue;
      }

      const txHash = normalizeHash(log.transactionHash);
      const logIndex = nonEmptyOrNull(log.logIndex);
      if (!txHash || !logIndex) {
        continue;
      }

      scopeRowsScanned += 1;
      const providerRecordId = `${chain.chainCanonical}:${txHash}:${hexToNumber(logIndex, "log.logIndex")}`;
      candidates.set(`swap_event|${providerRecordId}`, {
        providerRecordId,
        recordGranularity: "swap_event",
        eventAt: null,
        scopeKey: `${chain.chainCanonical}:block:${block.blockNumber}`,
      });
    }

    scopeReports.push({
      scopeKey: `${chain.chainCanonical}:block:${block.blockNumber}`,
      apiRowsScanned: scopeRowsScanned,
      apiTotal: scopeRowsScanned,
      dbRows: null,
    });
  }

  return {
    candidates: [...candidates.values()],
    scopes: scopeReports,
    apiRequests,
  };
}

async function collectCowswapCandidates(input: {
  connection: DuckDBConnection;
  window: ProviderCoverageWindow;
  options: ProviderIntegrityAuditOptions;
  rng: () => number;
}): Promise<CollectorResult> {
  const runtimeConfig = resolveCowswapAuditRuntimeConfig();
  const sampledBlocks = await loadOnchainBlockSamples({
    connection: input.connection,
    providerKey: "cowswap",
    window: input.window,
    scopesPerProvider: input.options.scopesPerProvider,
    rng: input.rng,
  });

  const candidates = new Map<string, IntegrityCandidate>();
  const scopeReports: ScopeProbe[] = [];
  let apiRequests = 0;

  for (const block of sampledBlocks) {
    const chain = runtimeConfig.chains.find((candidate) => candidate.chainCanonical === block.chainCanonical);
    if (!chain) {
      continue;
    }

    const logs = await fetchLogsForExactBlock({
      rpcUrl: chain.rpcUrl,
      address: chain.settlementContractAddress,
      topic0: COWSWAP_TRADE_TOPIC0,
      blockNumber: block.blockNumber,
    });
    apiRequests += 1;

    let scopeRowsScanned = 0;
    for (const log of logs) {
      if (log.removed) {
        continue;
      }
      const decoded = decodeCowswapTradeEvent(log);
      if (!decoded) {
        continue;
      }

      const sellToken = chain.tokenMap[decoded.sellToken] ?? null;
      const buyToken = chain.tokenMap[decoded.buyToken] ?? null;
      if (!sellToken || !buyToken || !isCowswapTargetRoute(sellToken, buyToken)) {
        continue;
      }

      const txHash = normalizeHash(log.transactionHash);
      const logIndex = nonEmptyOrNull(log.logIndex);
      if (!txHash || !logIndex) {
        continue;
      }

      scopeRowsScanned += 1;
      const providerRecordId = `${chain.chainCanonical}:${txHash}:${hexToNumber(logIndex, "log.logIndex")}`;
      candidates.set(`trade_event|${providerRecordId}`, {
        providerRecordId,
        recordGranularity: "trade_event",
        eventAt: null,
        scopeKey: `${chain.chainCanonical}:block:${block.blockNumber}`,
      });
    }

    scopeReports.push({
      scopeKey: `${chain.chainCanonical}:block:${block.blockNumber}`,
      apiRowsScanned: scopeRowsScanned,
      apiTotal: scopeRowsScanned,
      dbRows: null,
    });
  }

  return {
    candidates: [...candidates.values()],
    scopes: scopeReports,
    apiRequests,
  };
}

function collectThorchainTransfersCoins(transfers: ThorchainActionTransfer[] | null | undefined): ThorchainCoin[] {
  const output: ThorchainCoin[] = [];
  for (const transfer of transfers ?? []) {
    for (const coin of transfer.coins ?? []) {
      output.push(coin);
    }
  }
  return output;
}

function collectThorchainTxIds(transfers: ThorchainActionTransfer[] | null | undefined): string[] {
  const output = new Set<string>();
  for (const transfer of transfers ?? []) {
    const txId = nonEmptyOrNull(transfer.txID);
    if (txId) {
      output.add(txId);
    }
  }
  return [...output];
}

interface ParsedThorchainAsset {
  chainRaw: string | null;
  symbol: string | null;
}

function parseThorchainAsset(rawAsset: string | null | undefined): ParsedThorchainAsset | null {
  const raw = nonEmptyOrNull(rawAsset);
  if (!raw) {
    return null;
  }

  const firstDelimiterIndex = raw.search(/[.~-]/);
  if (firstDelimiterIndex < 0) {
    return {
      chainRaw: raw,
      symbol: raw.toUpperCase(),
    };
  }

  const chainRaw = nonEmptyOrNull(raw.slice(0, firstDelimiterIndex));
  const tokenRaw = nonEmptyOrNull(raw.slice(firstDelimiterIndex + 1));
  if (!tokenRaw) {
    return {
      chainRaw,
      symbol: null,
    };
  }

  const symbol = nonEmptyOrNull(tokenRaw.split("-")[0]);
  return {
    chainRaw,
    symbol: symbol?.toUpperCase() ?? null,
  };
}

function parseThorchainDateNs(value: string | null | undefined): Date | null {
  const normalized = nonEmptyOrNull(value);
  if (!normalized) {
    return null;
  }
  try {
    const ns = BigInt(normalized);
    if (ns <= 0n) {
      return null;
    }
    const ms = ns / 1_000_000n;
    const parsed = new Date(Number(ms));
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  } catch {
    return null;
  }
}

function parseThorchainMemoDestinationAsset(memo: string | null | undefined): string | null {
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

function buildThorchainNormalizedId(action: ThorchainAction): string {
  const inTxIds = collectThorchainTxIds(action.in);
  const outTxIds = collectThorchainTxIds(action.out);
  const firstInputCoin = collectThorchainTransfersCoins(action.in)[0];
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

async function collectThorchainCandidates(input: {
  connection: DuckDBConnection;
  window: ProviderCoverageWindow;
  options: ProviderIntegrityAuditOptions;
}): Promise<CollectorResult> {
  const candidates = new Map<string, IntegrityCandidate>();
  let apiRequests = 0;

  let continuation: string | null = null;
  let pages = 0;
  let rowsScanned = 0;

  while (pages < input.options.maxPages) {
    const url = new URL(THORCHAIN_SOURCE_ENDPOINT);
    url.searchParams.set("type", "swap");
    url.searchParams.set("asset", "nosynth");
    url.searchParams.set("limit", String(THORCHAIN_PAGE_SIZE));
    if (!continuation) {
      url.searchParams.set("timestamp", String(toUnixSeconds(input.window.end)));
    } else {
      url.searchParams.set("nextPageToken", continuation);
    }

    const response = await fetchJsonWithRetry<ThorchainActionsPageResponse>(
      url.toString(),
      {
        method: "GET",
        headers: {
          Accept: "application/json",
        },
      },
      {
        attempts: 4,
        initialDelayMs: 500,
        maxDelayMs: 5_000,
        timeoutMs: 30_000,
      },
    );

    apiRequests += 1;
    pages += 1;

    const actions = response.actions ?? [];
    rowsScanned += actions.length;

    for (const action of actions) {
      if (mapThorchainStatus(action.status ?? undefined) !== "success") {
        continue;
      }

      const inputCoins = collectThorchainTransfersCoins(action.in);
      const outputCoins = collectThorchainTransfersCoins(action.out);
      const firstInputCoin = inputCoins[0] ?? null;
      const firstOutputCoin = outputCoins[0] ?? null;
      const memoDestinationAssetRaw = parseThorchainMemoDestinationAsset(action.metadata?.swap?.memo);

      if (
        isThorchainSynthAsset(firstInputCoin?.asset) ||
        isThorchainSynthAsset(firstOutputCoin?.asset) ||
        isThorchainSynthAsset(memoDestinationAssetRaw)
      ) {
        continue;
      }

      const sourceAsset = parseThorchainAsset(firstInputCoin?.asset);
      const destinationAsset =
        parseThorchainAsset(firstOutputCoin?.asset) ?? parseThorchainAsset(memoDestinationAssetRaw);

      const sourceChainCanonical = canonicalizeChain({
        provider: "thorchain",
        rawChain: sourceAsset?.chainRaw,
      }).canonical;
      const destinationChainCanonical = canonicalizeChain({
        provider: "thorchain",
        rawChain: destinationAsset?.chainRaw,
      }).canonical;

      if (sourceChainCanonical === destinationChainCanonical) {
        continue;
      }

      if (
        !isSwapRowWithinIngestScope({
          source_chain_canonical: sourceChainCanonical,
          destination_chain_canonical: destinationChainCanonical,
          source_asset_symbol: sourceAsset?.symbol ?? null,
          destination_asset_symbol: destinationAsset?.symbol ?? null,
          source_asset_id: null,
          destination_asset_id: null,
        })
      ) {
        continue;
      }

      const eventAt = parseThorchainDateNs(action.date);
      if (!shouldIncludeByWindow(eventAt, input.window)) {
        continue;
      }

      const providerRecordId = buildThorchainNormalizedId(action);
      const key = `action|${providerRecordId}`;
      if (!candidates.has(key)) {
        candidates.set(key, {
          providerRecordId,
          recordGranularity: "action",
          eventAt,
          scopeKey: "global",
        });
      }
    }

    continuation = nonEmptyOrNull(response.meta?.nextPageToken);
    if (!continuation || actions.length === 0) {
      break;
    }
  }

  return {
    candidates: [...candidates.values()],
    scopes: [
      {
        scopeKey: "global",
        apiRowsScanned: rowsScanned,
        apiTotal: null,
        dbRows: null,
      },
    ],
    apiRequests,
  };
}

const CHAINFLIP_AUDIT_QUERY = `
query ChainflipAudit(
  $first: Int!
  $after: Cursor
  $from: Datetime!
  $to: Datetime!
  $allowedChains: [ChainflipChain!]
  $allowedAssets: [ChainflipAsset!]
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
      sourceChain
      destinationChain
      sourceAsset
      destinationAsset
      requestedBlockTimestamp
      completedBlockTimestamp
      refundEgressId
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
`;

function inferChainflipRequestStatus(request: ChainflipAuditSwapRequestNode): CanonicalStatus {
  if (request.refundEgressId !== null && request.refundEgressId !== undefined) {
    return "refunded";
  }
  if (request.completedBlockTimestamp) {
    return "success";
  }
  return "processing";
}

async function collectChainflipCandidates(input: {
  window: ProviderCoverageWindow;
  options: ProviderIntegrityAuditOptions;
}): Promise<CollectorResult> {
  const candidates = new Map<string, IntegrityCandidate>();

  const allowedChains = getChainflipScopedChains();
  const allowedAssets = getChainflipScopedAssets();

  let apiRequests = 0;
  let rowsScanned = 0;
  let pages = 0;
  let after: string | null = null;

  while (pages < input.options.maxPages) {
    const payload = {
      query: CHAINFLIP_AUDIT_QUERY,
      variables: {
        first: CHAINFLIP_PAGE_SIZE,
        after,
        from: input.window.start.toISOString(),
        to: input.window.end.toISOString(),
        allowedChains,
        allowedAssets,
      },
    };

    const response = await fetchJsonWithRetry<ChainflipAuditSwapRequestsResponse>(
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
        attempts: 4,
        initialDelayMs: 400,
        maxDelayMs: 4_000,
        timeoutMs: 30_000,
      },
    );

    apiRequests += 1;
    pages += 1;

    if (response.errors && response.errors.length > 0) {
      throw new Error(`Chainflip GraphQL error: ${response.errors.map((err) => err.message ?? "unknown").join(" | ")}`);
    }

    const nodes = response.data?.allSwapRequests?.nodes ?? [];
    rowsScanned += nodes.length;

    for (const request of nodes) {
      if (inferChainflipRequestStatus(request) !== "success") {
        continue;
      }

      const sourceChainCanonical = canonicalizeChain({
        provider: "chainflip",
        rawChain: request.sourceChain,
      }).canonical;
      const destinationChainCanonical = canonicalizeChain({
        provider: "chainflip",
        rawChain: request.destinationChain,
      }).canonical;

      const sourceAssetSymbol = toUpperSymbol(request.sourceAsset);
      const destinationAssetSymbol = toUpperSymbol(request.destinationAsset);

      if (
        !isSwapRowWithinIngestScope({
          source_chain_canonical: sourceChainCanonical,
          destination_chain_canonical: destinationChainCanonical,
          source_asset_symbol: sourceAssetSymbol,
          destination_asset_symbol: destinationAssetSymbol,
          source_asset_id: null,
          destination_asset_id: null,
        })
      ) {
        continue;
      }

      const eventAt = parseDateOrNull(request.requestedBlockTimestamp);
      if (!shouldIncludeByWindow(eventAt, input.window)) {
        continue;
      }

      const providerRecordId = String(request.id);
      const key = `swap_request|${providerRecordId}`;
      if (!candidates.has(key)) {
        candidates.set(key, {
          providerRecordId,
          recordGranularity: "swap_request",
          eventAt,
          scopeKey: "global",
        });
      }
    }

    const pageInfo = response.data?.allSwapRequests?.pageInfo;
    const hasNextPage = pageInfo?.hasNextPage ?? false;
    after = nonEmptyOrNull(pageInfo?.endCursor);

    if (!hasNextPage || !after || nodes.length === 0) {
      break;
    }
  }

  return {
    candidates: [...candidates.values()],
    scopes: [
      {
        scopeKey: "global",
        apiRowsScanned: rowsScanned,
        apiTotal: null,
        dbRows: null,
      },
    ],
    apiRequests,
  };
}

function inferGardenSymbol(tokenRaw: string | null): string | null {
  const normalized = nonEmptyOrNull(tokenRaw);
  if (!normalized) {
    return null;
  }
  return normalized.toUpperCase();
}

interface ParsedGardenAsset {
  chainRaw: string | null;
  symbol: string | null;
}

function parseGardenAsset(rawAsset: string | null | undefined): ParsedGardenAsset | null {
  const raw = nonEmptyOrNull(rawAsset);
  if (!raw) {
    return null;
  }

  const firstColonIndex = raw.indexOf(":");
  if (firstColonIndex < 0) {
    return {
      chainRaw: null,
      symbol: inferGardenSymbol(raw),
    };
  }

  const chainRaw = nonEmptyOrNull(raw.slice(0, firstColonIndex));
  const tokenRaw = nonEmptyOrNull(raw.slice(firstColonIndex + 1));

  return {
    chainRaw,
    symbol: inferGardenSymbol(tokenRaw),
  };
}

function deriveGardenCreatedAt(order: GardenOrder): Date | null {
  return (
    parseDateOrNull(order.created_at) ??
    parseDateOrNull(order.source_swap?.created_at) ??
    parseDateOrNull(order.destination_swap?.created_at) ??
    parseDateOrNull(order.source_swap?.initiate_timestamp) ??
    null
  );
}

function deriveGardenRawStatus(order: GardenOrder): string | null {
  return (
    nonEmptyOrNull(order.status) ??
    nonEmptyOrNull(order.source_swap?.status) ??
    nonEmptyOrNull(order.destination_swap?.status)
  );
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

function buildGardenScopes(): GardenScope[] {
  const chains = getGardenScopedChains();
  const scopes: GardenScope[] = [];
  for (const fromChain of chains) {
    for (const toChain of chains) {
      const sourceChainCanonical = canonicalizeChain({
        provider: "garden",
        rawChain: fromChain,
      }).canonical;
      const destinationChainCanonical = canonicalizeChain({
        provider: "garden",
        rawChain: toChain,
      }).canonical;
      scopes.push({
        key: `${fromChain}->${toChain}`,
        fromChain,
        toChain,
        sourceChainCanonical,
        destinationChainCanonical,
      });
    }
  }
  return scopes;
}

function extractGardenOrders(response: GardenOrdersResponse): GardenOrder[] {
  const status = nonEmptyOrNull(response.status)?.toLowerCase();
  if (status === "error") {
    throw new Error(`Garden API error: ${response.error ?? "unknown"}`);
  }
  return response.result?.data ?? [];
}

async function collectGardenCandidates(input: {
  connection: DuckDBConnection;
  window: ProviderCoverageWindow;
  options: ProviderIntegrityAuditOptions;
  rng: () => number;
}): Promise<CollectorResult> {
  const appId = nonEmptyOrNull(Bun.env.GARDEN_APP_ID ?? process.env.GARDEN_APP_ID);
  if (!appId) {
    throw new Error("Missing GARDEN_APP_ID environment variable.");
  }

  const allScopes = buildGardenScopes();
  const selectedScopes = pickRandomItems(
    allScopes,
    toScopeCount(input.options.scopesPerProvider, allScopes.length),
    input.rng,
  );

  const candidates = new Map<string, IntegrityCandidate>();
  const scopeReports: ScopeProbe[] = [];
  let apiRequests = 0;

  for (const scope of selectedScopes) {
    let page = 1;
    let pages = 0;
    let scopeRowsScanned = 0;
    let scopeTotal: number | null = null;

    while (pages < input.options.maxPages) {
      const url = new URL(GARDEN_SOURCE_ENDPOINT);
      url.searchParams.set("page", String(page));
      url.searchParams.set("per_page", String(GARDEN_PAGE_SIZE));
      url.searchParams.set("status", "completed");
      url.searchParams.set("from_chain", scope.fromChain);
      url.searchParams.set("to_chain", scope.toChain);

      const response = await fetchJsonWithRetry<GardenOrdersResponse>(
        url.toString(),
        {
          method: "GET",
          headers: {
            Accept: "application/json",
            "garden-app-id": appId,
          },
        },
        {
          attempts: 4,
          initialDelayMs: 500,
          maxDelayMs: 5_000,
          timeoutMs: 30_000,
        },
      );
      apiRequests += 1;

      const orders = extractGardenOrders(response);
      scopeRowsScanned += orders.length;
      const totalItems = toNumber((response.result?.total_items ?? null) as number | bigint | null);
      scopeTotal = totalItems > 0 ? totalItems : scopeTotal;

      for (const order of orders) {
        const providerRecordId =
          nonEmptyOrNull(order.order_id) ??
          nonEmptyOrNull(order.source_swap?.swap_id) ??
          nonEmptyOrNull(order.destination_swap?.swap_id);
        if (!providerRecordId) {
          continue;
        }

        const rawStatus = deriveGardenRawStatus(order);
        if (inferGardenStatus(order, rawStatus) !== "success") {
          continue;
        }

        const sourceAsset = parseGardenAsset(order.source_swap?.asset);
        const destinationAsset = parseGardenAsset(order.destination_swap?.asset);

        const sourceChainCanonical = canonicalizeChain({
          provider: "garden",
          rawChain: nonEmptyOrNull(order.source_swap?.chain) ?? sourceAsset?.chainRaw,
        }).canonical;
        const destinationChainCanonical = canonicalizeChain({
          provider: "garden",
          rawChain: nonEmptyOrNull(order.destination_swap?.chain) ?? destinationAsset?.chainRaw,
        }).canonical;

        if (
          !isSwapRowWithinIngestScope({
            source_chain_canonical: sourceChainCanonical,
            destination_chain_canonical: destinationChainCanonical,
            source_asset_symbol: sourceAsset?.symbol ?? null,
            destination_asset_symbol: destinationAsset?.symbol ?? null,
            source_asset_id: null,
            destination_asset_id: null,
          })
        ) {
          continue;
        }

        const eventAt = deriveGardenCreatedAt(order);
        if (!shouldIncludeByWindow(eventAt, input.window)) {
          continue;
        }

        const key = `order|${providerRecordId}`;
        if (!candidates.has(key)) {
          candidates.set(key, {
            providerRecordId,
            recordGranularity: "order",
            eventAt,
            scopeKey: scope.key,
          });
        }
      }

      pages += 1;
      page += 1;

      const totalPagesRaw = response.result?.total_pages;
      const totalPages = typeof totalPagesRaw === "number" && Number.isFinite(totalPagesRaw)
        ? Math.max(1, Math.floor(totalPagesRaw))
        : null;
      if (orders.length === 0 || (totalPages !== null && page > totalPages)) {
        break;
      }
    }

    const dbRows = await getRouteCount(
      input.connection,
      scope.sourceChainCanonical,
      scope.destinationChainCanonical,
    );
    scopeReports.push({
      scopeKey: scope.key,
      apiRowsScanned: scopeRowsScanned,
      apiTotal: scopeTotal,
      dbRows,
    });
  }

  return {
    candidates: [...candidates.values()],
    scopes: scopeReports,
    apiRequests,
  };
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

const KNOWN_NEAR_CHAIN_IDS = new Set([
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

interface ParsedNearAsset {
  chainRaw: string | null;
  tokenRaw: string | null;
  symbol: string | null;
}

function stripNearOmftSuffix(tokenRaw: string): string {
  if (tokenRaw.toLowerCase().endsWith(NEAR_INTENTS_OMFT_SUFFIX)) {
    return tokenRaw.slice(0, -NEAR_INTENTS_OMFT_SUFFIX.length);
  }
  return tokenRaw;
}

function normalizeNearTokenRaw(tokenRaw: string | null): string | null {
  const normalized = nonEmptyOrNull(tokenRaw);
  if (!normalized) {
    return null;
  }
  return nonEmptyOrNull(stripNearOmftSuffix(normalized));
}

function isKnownNearChain(value: string | null): boolean {
  if (!value) {
    return false;
  }
  return KNOWN_NEAR_CHAIN_IDS.has(value.toLowerCase());
}

function inferNearSymbol(tokenRaw: string | null): string | null {
  const normalized = nonEmptyOrNull(tokenRaw);
  if (!normalized) {
    return null;
  }
  if (/^[A-Za-z0-9]{2,12}$/.test(normalized)) {
    return normalized.toUpperCase();
  }
  return null;
}

function inferNearIntentsSymbol(chainRaw: string | null, tokenRaw: string | null): string | null {
  const normalizedChain = chainRaw?.toLowerCase() ?? null;
  const normalizedToken = normalizeNearTokenRaw(tokenRaw);

  if (!normalizedToken) {
    if (!normalizedChain) {
      return null;
    }
    return NEAR_INTENTS_NATIVE_SYMBOL_BY_CHAIN[normalizedChain] ?? null;
  }

  const tokenLower = normalizedToken.toLowerCase();
  if (normalizedChain && tokenLower === normalizedChain) {
    return NEAR_INTENTS_NATIVE_SYMBOL_BY_CHAIN[normalizedChain] ?? inferNearSymbol(normalizedToken);
  }

  if (normalizedChain && /^0x[a-f0-9]{40}$/.test(tokenLower)) {
    const mapped = NEAR_INTENTS_OMFT_CONTRACT_SYMBOL_BY_CHAIN[normalizedChain]?.[tokenLower];
    if (mapped) {
      return mapped;
    }
  }

  return inferNearSymbol(normalizedToken);
}

function parseNearAsset(rawAsset: string | null | undefined): ParsedNearAsset | null {
  const raw = nonEmptyOrNull(rawAsset);
  if (!raw) {
    return null;
  }

  const firstColonIndex = raw.indexOf(":");
  if (firstColonIndex < 0) {
    return {
      chainRaw: null,
      tokenRaw: raw,
      symbol: inferNearSymbol(raw),
    };
  }

  const namespace = raw.slice(0, firstColonIndex).toLowerCase();
  const payload = raw.slice(firstColonIndex + 1);

  if (!payload) {
    return {
      chainRaw: isKnownNearChain(namespace) ? namespace : null,
      tokenRaw: null,
      symbol: null,
    };
  }

  if (namespace === "nep141") {
    const normalizedPayload = normalizeNearTokenRaw(payload);
    if (!normalizedPayload) {
      return {
        chainRaw: "near",
        tokenRaw: null,
        symbol: null,
      };
    }

    const dashIndex = normalizedPayload.indexOf("-");
    if (dashIndex > 0) {
      const chainCandidate = normalizedPayload.slice(0, dashIndex).toLowerCase();
      const tokenRaw = normalizeNearTokenRaw(normalizedPayload.slice(dashIndex + 1));
      if (isKnownNearChain(chainCandidate)) {
        return {
          chainRaw: chainCandidate,
          tokenRaw,
          symbol: inferNearIntentsSymbol(chainCandidate, tokenRaw),
        };
      }
    }

    const maybeChain = normalizedPayload.toLowerCase();
    if (isKnownNearChain(maybeChain)) {
      return {
        chainRaw: maybeChain,
        tokenRaw: maybeChain,
        symbol: inferNearIntentsSymbol(maybeChain, maybeChain),
      };
    }

    return {
      chainRaw: "near",
      tokenRaw: normalizedPayload,
      symbol: inferNearIntentsSymbol("near", normalizedPayload),
    };
  }

  if (isKnownNearChain(namespace)) {
    const tokenRaw = normalizeNearTokenRaw(payload);
    return {
      chainRaw: namespace,
      tokenRaw,
      symbol: inferNearIntentsSymbol(namespace, tokenRaw),
    };
  }

  const normalizedPayload = normalizeNearTokenRaw(payload) ?? payload;
  const payloadDashIndex = normalizedPayload.indexOf("-");
  if (payloadDashIndex > 0) {
    const chainCandidate = normalizedPayload.slice(0, payloadDashIndex).toLowerCase();
    if (isKnownNearChain(chainCandidate)) {
      const tokenRaw = normalizeNearTokenRaw(normalizedPayload.slice(payloadDashIndex + 1));
      return {
        chainRaw: chainCandidate,
        tokenRaw,
        symbol: inferNearIntentsSymbol(chainCandidate, tokenRaw),
      };
    }
  }

  return {
    chainRaw: null,
    tokenRaw: normalizeNearTokenRaw(payload),
    symbol: inferNearIntentsSymbol(null, payload),
  };
}

function deriveNearEventAt(transaction: NearIntentsTransaction): Date | null {
  return parseDateOrNull(transaction.createdAt) ?? parseUnixSecondsOrNull(transaction.createdAtTimestamp);
}

function buildNearScopes(): NearIntentsScope[] {
  const chains = getNearIntentsScopedChains();
  const scopes: NearIntentsScope[] = [];
  for (const fromChainId of chains) {
    for (const toChainId of chains) {
      const sourceChainCanonical = canonicalizeChain({
        provider: "nearintents",
        rawChain: fromChainId,
      }).canonical;
      const destinationChainCanonical = canonicalizeChain({
        provider: "nearintents",
        rawChain: toChainId,
      }).canonical;
      scopes.push({
        key: `${fromChainId}->${toChainId}`,
        fromChainId,
        toChainId,
        sourceChainCanonical,
        destinationChainCanonical,
      });
    }
  }
  return scopes;
}

interface NearRateLimitState {
  nextRequestAtMs: number;
}

async function fetchNearPageRateLimited(
  url: string,
  authToken: string,
  state: NearRateLimitState,
): Promise<NearIntentsTransactionsPagesResponse> {
  const nowMs = Date.now();
  const waitMs = state.nextRequestAtMs - nowMs;
  if (waitMs > 0) {
    await sleep(waitMs);
  }

  const startedAtMs = Date.now();
  try {
    return await fetchJsonWithRetry<NearIntentsTransactionsPagesResponse>(
      url,
      {
        method: "GET",
        headers: {
          Accept: "application/json",
          Authorization: `Bearer ${authToken}`,
        },
      },
      {
        attempts: 4,
        initialDelayMs: NEAR_INTENTS_REQUEST_INTERVAL_MS,
        maxDelayMs: 20_000,
        timeoutMs: 90_000,
      },
    );
  } finally {
    state.nextRequestAtMs = startedAtMs + NEAR_INTENTS_REQUEST_INTERVAL_MS;
  }
}

async function collectNearIntentsCandidates(input: {
  connection: DuckDBConnection;
  window: ProviderCoverageWindow;
  options: ProviderIntegrityAuditOptions;
  rng: () => number;
}): Promise<CollectorResult> {
  const authToken = nonEmptyOrNull(Bun.env.NEAR_INTENTS_KEY ?? process.env.NEAR_INTENTS_KEY);
  if (!authToken) {
    throw new Error("Missing NEAR_INTENTS_KEY environment variable.");
  }

  const allScopes = buildNearScopes();
  const selectedScopes = pickRandomItems(
    allScopes,
    toScopeCount(input.options.scopesPerProvider, allScopes.length),
    input.rng,
  );

  const rateLimitState: NearRateLimitState = { nextRequestAtMs: 0 };
  const candidates = new Map<string, IntegrityCandidate>();
  const scopeReports: ScopeProbe[] = [];
  let apiRequests = 0;

  for (const scope of selectedScopes) {
    let page = 1;
    let pages = 0;
    let scopeRowsScanned = 0;
    let scopeTotal: number | null = null;

    while (pages < input.options.maxPages) {
      const url = new URL(NEAR_INTENTS_SOURCE_ENDPOINT);
      url.searchParams.set("page", String(page));
      url.searchParams.set("perPage", String(NEAR_INTENTS_PAGE_SIZE));
      url.searchParams.set("statuses", "SUCCESS");
      url.searchParams.set("showTestTxs", "false");
      url.searchParams.set("fromChainId", scope.fromChainId);
      url.searchParams.set("toChainId", scope.toChainId);
      url.searchParams.set("startTimestampUnix", String(toUnixSeconds(input.window.start)));
      url.searchParams.set("endTimestampUnix", String(toUnixSeconds(input.window.end)));

      const response = await fetchNearPageRateLimited(url.toString(), authToken, rateLimitState);
      apiRequests += 1;

      const transactions = response.data ?? [];
      scopeRowsScanned += transactions.length;
      scopeTotal = typeof response.total === "number" && Number.isFinite(response.total)
        ? response.total
        : scopeTotal;

      for (const transaction of transactions) {
        const providerRecordId =
          nonEmptyOrNull(transaction.depositAddressAndMemo) ??
          nonEmptyOrNull(
            `${nonEmptyOrNull(transaction.depositAddress) ?? ""}_${nonEmptyOrNull(transaction.depositMemo) ?? ""}`,
          );
        if (!providerRecordId) {
          continue;
        }

        if (mapNearIntentsStatus(transaction.status) !== "success") {
          continue;
        }

        const sourceAsset = parseNearAsset(transaction.originAsset);
        const destinationAsset = parseNearAsset(transaction.destinationAsset);
        const sourceChainCanonical = canonicalizeChain({
          provider: "nearintents",
          rawChain: sourceAsset?.chainRaw,
        }).canonical;
        const destinationChainCanonical = canonicalizeChain({
          provider: "nearintents",
          rawChain: destinationAsset?.chainRaw,
        }).canonical;

        if (
          !isSwapRowWithinIngestScope({
            source_chain_canonical: sourceChainCanonical,
            destination_chain_canonical: destinationChainCanonical,
            source_asset_symbol: sourceAsset?.symbol ?? null,
            destination_asset_symbol: destinationAsset?.symbol ?? null,
            source_asset_id: null,
            destination_asset_id: null,
          })
        ) {
          continue;
        }

        const eventAt = deriveNearEventAt(transaction);
        if (!shouldIncludeByWindow(eventAt, input.window)) {
          continue;
        }

        const key = `transaction|${providerRecordId}`;
        if (!candidates.has(key)) {
          candidates.set(key, {
            providerRecordId,
            recordGranularity: "transaction",
            eventAt,
            scopeKey: scope.key,
          });
        }
      }

      pages += 1;
      const nextPage = response.nextPage ?? null;
      if (!nextPage || transactions.length === 0) {
        break;
      }
      page = nextPage;
    }

    const dbRows = await getRouteCount(
      input.connection,
      scope.sourceChainCanonical,
      scope.destinationChainCanonical,
    );
    scopeReports.push({
      scopeKey: scope.key,
      apiRowsScanned: scopeRowsScanned,
      apiTotal: scopeTotal,
      dbRows,
    });
  }

  return {
    candidates: [...candidates.values()],
    scopes: scopeReports,
    apiRequests,
  };
}

function computeWilsonLowerBound(matches: number, sampled: number, z = 1.96): number | null {
  if (sampled <= 0) {
    return null;
  }
  const pHat = matches / sampled;
  const z2 = z * z;
  const denominator = 1 + z2 / sampled;
  const center = pHat + z2 / (2 * sampled);
  const margin = z * Math.sqrt((pHat * (1 - pHat)) / sampled + z2 / (4 * sampled * sampled));
  return Math.max(0, (center - margin) / denominator);
}

class UnsupportedIntegritySamplerError extends Error {
  constructor(providerKey: ProviderKey) {
    super(`Integrity API sampler is not implemented for provider '${providerKey}' yet.`);
    this.name = "UnsupportedIntegritySamplerError";
  }
}

async function collectCandidatesForProvider(input: {
  providerKey: ProviderKey;
  connection: DuckDBConnection;
  window: ProviderCoverageWindow;
  options: ProviderIntegrityAuditOptions;
  rng: () => number;
}): Promise<CollectorResult> {
  if (input.providerKey === "lifi") {
    return collectLifiCandidates({
      connection: input.connection,
      window: input.window,
      options: input.options,
      rng: input.rng,
    });
  }

  if (input.providerKey === "relay") {
    return collectRelayCandidates({
      connection: input.connection,
      window: input.window,
      options: input.options,
      rng: input.rng,
    });
  }

  if (input.providerKey === "thorchain") {
    return collectThorchainCandidates({
      connection: input.connection,
      window: input.window,
      options: input.options,
    });
  }

  if (input.providerKey === "chainflip") {
    return collectChainflipCandidates({
      window: input.window,
      options: input.options,
    });
  }

  if (input.providerKey === "garden") {
    return collectGardenCandidates({
      connection: input.connection,
      window: input.window,
      options: input.options,
      rng: input.rng,
    });
  }

  if (input.providerKey === "nearintents") {
    return collectNearIntentsCandidates({
      connection: input.connection,
      window: input.window,
      options: input.options,
      rng: input.rng,
    });
  }

  if (input.providerKey === "kyberswap") {
    return collectKyberswapCandidates({
      connection: input.connection,
      window: input.window,
      options: input.options,
      rng: input.rng,
    });
  }

  if (input.providerKey === "cowswap") {
    return collectCowswapCandidates({
      connection: input.connection,
      window: input.window,
      options: input.options,
      rng: input.rng,
    });
  }

  throw new UnsupportedIntegritySamplerError(input.providerKey);
}

function sortByEventAtDesc(a: IntegrityCandidate, b: IntegrityCandidate): number {
  const timeA = a.eventAt?.getTime() ?? 0;
  const timeB = b.eventAt?.getTime() ?? 0;
  return timeB - timeA;
}

function emptyReconciliationReport(): ProviderReconciliationReport {
  return {
    severity: "ok",
    coreRowsMissingRaw: 0,
    orphanRawRows: 0,
    staleRawHashLatestRows: 0,
    duplicateProviderRecordRows: 0,
    rowsOlderThanFloor: 0,
    outOfScopeRows: 0,
    nullEventAtRows: 0,
    gapDays: [],
    maxConsecutiveGapDays: 0,
    recentDailyCounts: [],
    topRecentRoutes: [],
    issues: [],
  };
}

export async function runProviderIntegrityAudit(
  input: ProviderIntegrityAuditInput,
): Promise<IntegrityAuditRunReport> {
  const options = resolveOptions(input.options);
  const providers = toProviders(input.scope);
  const reports: ProviderIntegrityAuditReport[] = [];

  for (const providerKey of providers) {
    const db = await openProviderDatabase(providerKey);
    try {
      const bounds = await getProviderBounds(db.connection);
      const swaps = toNumber(bounds.swaps);
      const window = swaps > 0
        ? deriveCoverageWindow(bounds, options.windowDays)
        : { start: new Date(0), end: new Date(0) };
      const reconciliation = swaps > 0
        ? await runProviderReconciliation({
          providerKey,
          connection: db.connection,
          windowStart: window.start,
          windowEnd: window.end,
        })
        : emptyReconciliationReport();

      if (swaps <= 0) {
        reports.push({
          providerKey,
          dbSwaps: 0,
          dbOldestEventAt: null,
          dbNewestEventAt: null,
          windowStart: window.start.toISOString(),
          windowEnd: window.end.toISOString(),
          reconciliation,
          apiRequests: 0,
          candidatesCollected: 0,
          sampled: 0,
          matches: 0,
          misses: 0,
          hitRate: null,
          wilsonLowerBound95: null,
          scopes: [],
          missingSamples: [],
          notes: ["No rows in swaps_core; skipped API sampling."],
          error: null,
        });
        continue;
      }

      const presenceIndex = await loadPresenceIndex(db.connection);
      const rng = createMulberry32(hashSeed(options.seed, providerKey));

      try {
        const collected = await collectCandidatesForProvider({
          providerKey,
          connection: db.connection,
          window,
          options,
          rng,
        });

        const sampledCandidates = pickRandomItems(
          collected.candidates,
          Math.min(options.sampleSize, collected.candidates.length),
          rng,
        ).sort(sortByEventAtDesc);

        let matches = 0;
        const missingSamples: IntegrityMiss[] = [];

        for (const candidate of sampledCandidates) {
          if (hasCandidateInDb(presenceIndex, candidate)) {
            matches += 1;
            continue;
          }
          missingSamples.push({
            providerRecordId: candidate.providerRecordId,
            recordGranularity: candidate.recordGranularity,
            eventAt: candidate.eventAt?.toISOString() ?? null,
            scopeKey: candidate.scopeKey,
          });
        }

        const sampled = sampledCandidates.length;
        const misses = sampled - matches;

        const notes: string[] = [];
        if (collected.candidates.length < options.sampleSize) {
          notes.push(
            `Candidate pool (${collected.candidates.length}) is smaller than requested sample size (${options.sampleSize}).`,
          );
        }

        reports.push({
          providerKey,
          dbSwaps: swaps,
          dbOldestEventAt: parseDateOrNull(bounds.oldest_event_at)?.toISOString() ?? null,
          dbNewestEventAt: parseDateOrNull(bounds.newest_event_at)?.toISOString() ?? null,
          windowStart: window.start.toISOString(),
          windowEnd: window.end.toISOString(),
          reconciliation,
          apiRequests: collected.apiRequests,
          candidatesCollected: collected.candidates.length,
          sampled,
          matches,
          misses,
          hitRate: sampled > 0 ? matches / sampled : null,
          wilsonLowerBound95: computeWilsonLowerBound(matches, sampled),
          scopes: collected.scopes,
          missingSamples,
          notes,
          error: null,
        });
      } catch (error) {
        const notes =
          error instanceof UnsupportedIntegritySamplerError
            ? [error.message]
            : [];
        reports.push({
          providerKey,
          dbSwaps: swaps,
          dbOldestEventAt: parseDateOrNull(bounds.oldest_event_at)?.toISOString() ?? null,
          dbNewestEventAt: parseDateOrNull(bounds.newest_event_at)?.toISOString() ?? null,
          windowStart: window.start.toISOString(),
          windowEnd: window.end.toISOString(),
          reconciliation,
          apiRequests: 0,
          candidatesCollected: 0,
          sampled: 0,
          matches: 0,
          misses: 0,
          hitRate: null,
          wilsonLowerBound95: null,
          scopes: [],
          missingSamples: [],
          notes,
          error:
            error instanceof UnsupportedIntegritySamplerError
              ? null
              : error instanceof Error
                ? error.message
                : String(error),
        });
      }
    } finally {
      db.close();
    }
  }

  return {
    generatedAt: new Date().toISOString(),
    scope: input.scope,
    options,
    providers: reports,
  };
}
