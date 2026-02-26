import type { ProviderKey } from "../domain/provider-key";

const TOKEN_DECIMALS_BY_SYMBOL: Record<string, number> = {
  ADA: 6,
  ARB: 18,
  AVAX: 18,
  BBTC: 8,
  BCH: 8,
  BNB: 18,
  BTC: 8,
  BTCB: 8,
  CBBTC: 8,
  CBTC: 8,
  DAI: 18,
  DOGE: 8,
  ETH: 18,
  FLIP: 18,
  IBTC: 8,
  LTC: 8,
  MATIC: 18,
  NEAR: 24,
  OP: 18,
  POL: 18,
  RUNE: 8,
  SOL: 9,
  STETH: 18,
  TBTC: 8,
  TRX: 6,
  UBTC: 8,
  USDC: 6,
  "USDC.E": 6,
  USDCE: 6,
  USDT: 6,
  USDT0: 6,
  WBTC: 8,
  WETH: 18,
  WNEAR: 24,
  WSOL: 9,
  WSTETH: 18,
  XBT: 8,
  XRP: 6,
};

const TOKEN_DECIMALS_BY_CHAIN_SYMBOL: Record<string, number> = {
  "bitcoin:mainnet:btc": 8,
  "dogecoin:mainnet:doge": 8,
  "eip155:1:eth": 18,
  "litecoin:mainnet:ltc": 8,
  "near:mainnet:near": 24,
  "solana:mainnet:sol": 9,
  "thorchain:mainnet:rune": 8,
  "tron:mainnet:trx": 6,
};

interface InferTokenDecimalsInput {
  provider: ProviderKey;
  explicitDecimals?: number | null;
  chainCanonical?: string | null;
  symbol?: string | null;
}

function parseIntegerString(value: string | null | undefined): string | null {
  if (!value) {
    return null;
  }
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }
  if (!/^-?[0-9]+$/.test(trimmed)) {
    return null;
  }
  return trimmed;
}

function normalizeSymbol(value: string | null | undefined): string | null {
  if (!value) {
    return null;
  }
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }
  return trimmed.toUpperCase();
}

function normalizeDecimals(value: number | null | undefined): number | null {
  if (value === null || value === undefined) {
    return null;
  }
  if (!Number.isFinite(value)) {
    return null;
  }
  const parsed = Math.floor(value);
  if (!Number.isInteger(parsed) || parsed < 0 || parsed > 36) {
    return null;
  }
  return parsed;
}

function buildSymbolCandidates(symbol: string | null): string[] {
  if (!symbol) {
    return [];
  }

  const candidates = new Set<string>([symbol]);
  if (symbol.includes(".")) {
    const head = symbol.split(".")[0];
    if (head) {
      candidates.add(head);
    }
  }
  if (symbol.includes("_")) {
    const head = symbol.split("_")[0];
    if (head) {
      candidates.add(head);
    }
  }
  if (symbol.includes("-")) {
    const head = symbol.split("-")[0];
    if (head) {
      candidates.add(head);
    }
  }
  if (symbol.startsWith("W") && symbol.length > 3) {
    candidates.add(symbol.slice(1));
  }
  return [...candidates];
}

export function atomicToNormalized(atomic: string, decimals: number): number | null {
  const parsed = parseIntegerString(atomic);
  const normalizedDecimals = normalizeDecimals(decimals);
  if (!parsed || normalizedDecimals === null) {
    return null;
  }

  const negative = parsed.startsWith("-");
  const digitsRaw = negative ? parsed.slice(1) : parsed;
  const digits = digitsRaw.replace(/^0+/, "") || "0";
  if (digits === "0") {
    return 0;
  }

  let intPart = "0";
  let fracPart = digits;
  if (digits.length > normalizedDecimals) {
    intPart = digits.slice(0, digits.length - normalizedDecimals);
    fracPart = digits.slice(digits.length - normalizedDecimals);
  } else {
    fracPart = digits.padStart(normalizedDecimals, "0");
  }
  fracPart = fracPart.replace(/0+$/, "");

  const text = `${negative ? "-" : ""}${intPart}${fracPart ? `.${fracPart}` : ""}`;
  const value = Number(text);
  return Number.isFinite(value) ? value : null;
}

export function inferTokenDecimals(input: InferTokenDecimalsInput): number | null {
  const explicit = normalizeDecimals(input.explicitDecimals);
  if (explicit !== null) {
    return explicit;
  }

  // Midgard amounts are emitted in 1e8 base units for swap action coins.
  if (input.provider === "thorchain") {
    return 8;
  }

  const symbol = normalizeSymbol(input.symbol);
  const candidates = buildSymbolCandidates(symbol);
  const chainCanonical = input.chainCanonical?.toLowerCase() ?? null;

  if (chainCanonical) {
    for (const candidate of candidates) {
      const byChainSymbol = TOKEN_DECIMALS_BY_CHAIN_SYMBOL[`${chainCanonical}:${candidate.toLowerCase()}`];
      if (byChainSymbol !== undefined) {
        return byChainSymbol;
      }
    }
  }

  for (const candidate of candidates) {
    const bySymbol = TOKEN_DECIMALS_BY_SYMBOL[candidate];
    if (bySymbol !== undefined) {
      return bySymbol;
    }
  }

  return null;
}
