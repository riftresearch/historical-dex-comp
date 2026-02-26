import type { NormalizedSwapRowInput } from "../domain/normalized-swap";

export interface IngestScopeAssetRule {
  symbol: string;
  allowedChains: readonly string[];
}

export const INGEST_SCOPE_ENABLED = true;

export const INGEST_SCOPE_ALLOWED_CHAINS = [
  "bitcoin:mainnet",
  "eip155:1",
  "eip155:8453",
] as const;

// Tunable whitelist for scoped ingestion.
export const INGEST_SCOPE_ALLOWED_ASSETS: readonly IngestScopeAssetRule[] = [
  { symbol: "BTC", allowedChains: ["bitcoin:mainnet"] },
  { symbol: "CBBTC", allowedChains: ["eip155:1", "eip155:8453"] },
  { symbol: "USDC", allowedChains: ["eip155:1", "eip155:8453"] },
  { symbol: "USDT", allowedChains: ["eip155:1"] },
  { symbol: "ETH", allowedChains: ["eip155:1", "eip155:8453"] },
  { symbol: "WETH", allowedChains: ["eip155:1", "eip155:8453"] },
] as const;

const ALLOWED_CHAIN_SET = new Set<string>(INGEST_SCOPE_ALLOWED_CHAINS);
const ALLOWED_SYMBOL_SET = new Set<string>(INGEST_SCOPE_ALLOWED_ASSETS.map((rule) => rule.symbol.toUpperCase()));

const SYMBOL_ALIASES: Record<string, string> = {
  CBBTC: "CBBTC",
  ETHER: "ETH",
  XBT: "BTC",
};

const CANONICAL_CHAIN_TO_RELAY_CHAIN_ID: Record<string, string> = {
  "bitcoin:mainnet": "8253038",
  "eip155:1": "1",
  "eip155:8453": "8453",
};

const CANONICAL_CHAIN_TO_GARDEN_CHAIN: Record<string, string> = {
  "bitcoin:mainnet": "bitcoin",
  "eip155:1": "ethereum",
  "eip155:8453": "base",
};

const CANONICAL_CHAIN_TO_NEAR_INTENTS_CHAIN: Record<string, string> = {
  "bitcoin:mainnet": "btc",
  "eip155:1": "eth",
  "eip155:8453": "base",
};

const CANONICAL_CHAIN_TO_CHAINFLIP_CHAIN_ENUM: Record<string, string> = {
  "bitcoin:mainnet": "Bitcoin",
  "eip155:1": "Ethereum",
  // Chainflip explorer GraphQL currently does not expose Base as a chain enum.
};

const SYMBOL_TO_CHAINFLIP_ASSET_ENUM: Record<string, string> = {
  BTC: "Btc",
  USDC: "Usdc",
  USDT: "Usdt",
  ETH: "Eth",
  WETH: "Eth",
};

const ASSET_RULES_BY_SYMBOL = new Map<string, Set<string>>();
for (const rule of INGEST_SCOPE_ALLOWED_ASSETS) {
  const symbol = rule.symbol.trim().toUpperCase();
  if (!symbol) {
    continue;
  }
  const chains = ASSET_RULES_BY_SYMBOL.get(symbol) ?? new Set<string>();
  for (const chain of rule.allowedChains) {
    if (!chain || !ALLOWED_CHAIN_SET.has(chain)) {
      continue;
    }
    chains.add(chain);
  }
  ASSET_RULES_BY_SYMBOL.set(symbol, chains);
}

function normalizeTokenSymbol(candidate: string): string {
  const upper = candidate.trim().toUpperCase();
  return SYMBOL_ALIASES[upper] ?? upper;
}

function collectTokenSymbolCandidates(raw: string | null | undefined): string[] {
  if (!raw) {
    return [];
  }
  const trimmed = raw.trim();
  if (!trimmed) {
    return [];
  }

  const upper = trimmed.toUpperCase();
  const candidates = new Set<string>([upper]);
  for (const token of upper.split(/[^A-Z0-9]+/g)) {
    if (token) {
      candidates.add(token);
    }
  }

  if (upper.includes(":")) {
    const tail = upper.split(":").at(-1);
    if (tail) {
      candidates.add(tail);
    }
  }

  const normalized = new Set<string>();
  for (const candidate of candidates) {
    if (!candidate || candidate.startsWith("0X")) {
      continue;
    }
    normalized.add(normalizeTokenSymbol(candidate));
  }
  return [...normalized];
}

function isScopedAssetAllowed(input: {
  chainCanonical: string | null | undefined;
  assetSymbol: string | null | undefined;
  assetId: string | null | undefined;
}): boolean {
  const chainCanonical = input.chainCanonical ?? null;
  if (!chainCanonical || !ALLOWED_CHAIN_SET.has(chainCanonical)) {
    return false;
  }

  const candidates = new Set<string>();
  for (const candidate of collectTokenSymbolCandidates(input.assetSymbol)) {
    candidates.add(candidate);
  }
  for (const candidate of collectTokenSymbolCandidates(input.assetId)) {
    candidates.add(candidate);
  }

  if (candidates.size === 0) {
    return false;
  }

  for (const candidate of candidates) {
    const allowedChains = ASSET_RULES_BY_SYMBOL.get(candidate);
    if (!allowedChains) {
      continue;
    }
    if (allowedChains.has(chainCanonical)) {
      return true;
    }
  }
  return false;
}

function toUniqueList(values: readonly string[]): string[] {
  const output: string[] = [];
  const seen = new Set<string>();
  for (const value of values) {
    const trimmed = value.trim();
    if (!trimmed || seen.has(trimmed)) {
      continue;
    }
    seen.add(trimmed);
    output.push(trimmed);
  }
  return output;
}

export function getRelayScopedChainIds(): string[] {
  const values = INGEST_SCOPE_ALLOWED_CHAINS.map((chain) => CANONICAL_CHAIN_TO_RELAY_CHAIN_ID[chain] ?? "");
  return toUniqueList(values);
}

export function getGardenScopedChains(): string[] {
  const values = INGEST_SCOPE_ALLOWED_CHAINS.map((chain) => CANONICAL_CHAIN_TO_GARDEN_CHAIN[chain] ?? "");
  return toUniqueList(values);
}

export function getNearIntentsScopedChains(): string[] {
  const values = INGEST_SCOPE_ALLOWED_CHAINS.map((chain) => CANONICAL_CHAIN_TO_NEAR_INTENTS_CHAIN[chain] ?? "");
  return toUniqueList(values);
}

export function getChainflipScopedChains(): string[] {
  const values = INGEST_SCOPE_ALLOWED_CHAINS.map(
    (chain) => CANONICAL_CHAIN_TO_CHAINFLIP_CHAIN_ENUM[chain] ?? "",
  );
  return toUniqueList(values);
}

export function getChainflipScopedAssets(): string[] {
  const values = [...ALLOWED_SYMBOL_SET]
    .map((symbol) => SYMBOL_TO_CHAINFLIP_ASSET_ENUM[symbol] ?? "")
    .filter((value) => value.length > 0);
  return toUniqueList(values);
}

export function isSwapRowWithinIngestScope(
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
  if (!INGEST_SCOPE_ENABLED) {
    return true;
  }

  return (
    isScopedAssetAllowed({
      chainCanonical: row.source_chain_canonical,
      assetSymbol: row.source_asset_symbol,
      assetId: row.source_asset_id,
    }) &&
    isScopedAssetAllowed({
      chainCanonical: row.destination_chain_canonical,
      assetSymbol: row.destination_asset_symbol,
      assetId: row.destination_asset_id,
    })
  );
}
