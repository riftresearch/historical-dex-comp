import type { ProviderKey } from "./provider-key";

export interface ChainCanonicalizationInput {
  provider: ProviderKey;
  rawChain?: string | null;
  rawChainId?: number | string | null;
}

export interface ChainCanonicalizationResult {
  canonical: string;
  rawChain: string | null;
  rawChainId: string | null;
}

const CHAIN_ALIAS_BY_PROVIDER: Partial<
  Record<ProviderKey, Record<string, string>>
> = {
  lifi: {
    ethereum: "eip155:1",
    "1": "eip155:1",
  },
  relay: {
    bitcoin: "bitcoin:mainnet",
    solana: "solana:mainnet",
    hyperliquid: "hyperliquid:mainnet",
    lighter: "lighter:mainnet",
    "8253038": "bitcoin:mainnet",
    "792703809": "solana:mainnet",
    "1337": "hyperliquid:mainnet",
    "3586256": "lighter:mainnet",
  },
  chainflip: {
    ethereum: "eip155:1",
    arbitrum: "eip155:42161",
    bitcoin: "bitcoin:mainnet",
    solana: "solana:mainnet",
    assethub: "polkadot:assethub",
  },
  thorchain: {
    avax: "eip155:43114",
    base: "eip155:8453",
    bch: "bitcoincash:mainnet",
    c: "bitcoincash:mainnet",
    bsc: "eip155:56",
    btc: "bitcoin:mainnet",
    doge: "dogecoin:mainnet",
    e: "eip155:1",
    eth: "eip155:1",
    gaia: "cosmos:cosmoshub-4",
    ltc: "litecoin:mainnet",
    thor: "thorchain:mainnet",
    tron: "tron:mainnet",
    x: "xrpl:mainnet",
    xrp: "xrpl:mainnet",
  },
  garden: {
    ethereum: "eip155:1",
    arbitrum: "eip155:42161",
    base: "eip155:8453",
    bitcoin: "bitcoin:mainnet",
    botanix: "botanix:mainnet",
    citrea: "citrea:mainnet",
    hyperevm: "hyperevm:mainnet",
    litecoin: "litecoin:mainnet",
    megaeth: "megaeth:mainnet",
    monad: "monad:mainnet",
    solana: "solana:mainnet",
    starknet: "starknet:mainnet",
    tron: "tron:mainnet",
    bnbchain: "eip155:56",
    alpen_signet: "alpen:signet",
    alpen_testnet: "alpen:testnet",
    bnbchain_testnet: "eip155:97",
    citrea_testnet: "citrea:testnet",
    ethereum_sepolia: "eip155:11155111",
    base_sepolia: "eip155:84532",
    arbitrum_sepolia: "eip155:421614",
    bitcoin_testnet: "bitcoin:testnet",
    litecoin_testnet: "litecoin:testnet",
    monad_testnet: "monad:testnet",
    solana_testnet: "solana:testnet",
    starknet_sepolia: "starknet:testnet",
    tron_shasta: "tron:testnet",
    xrpl_testnet: "xrpl:testnet",
  },
  nearintents: {
    near: "near:mainnet",
    eth: "eip155:1",
    base: "eip155:8453",
    arb: "eip155:42161",
    btc: "bitcoin:mainnet",
    sol: "solana:mainnet",
    bsc: "eip155:56",
    gnosis: "eip155:100",
    op: "eip155:10",
    avax: "eip155:43114",
    ltc: "litecoin:mainnet",
    sui: "sui:mainnet",
    tron: "tron:mainnet",
    starknet: "starknet:mainnet",
  },
  kyberswap: {
    ethereum: "eip155:1",
    "1": "eip155:1",
  },
};

function toStringOrNull(value: number | string | null | undefined): string | null {
  if (value === null || value === undefined) {
    return null;
  }

  return String(value);
}

function canonicalizeFromRawChainId(rawChainId: string | null): string | null {
  if (!rawChainId) {
    return null;
  }

  const parsed = Number(rawChainId);
  if (!Number.isNaN(parsed) && Number.isInteger(parsed) && parsed > 0) {
    return `eip155:${parsed}`;
  }

  return null;
}

export function canonicalizeChain(
  input: ChainCanonicalizationInput,
): ChainCanonicalizationResult {
  const rawChain = input.rawChain ? input.rawChain.trim() : null;
  const rawChainId = toStringOrNull(input.rawChainId);
  const aliasMap = CHAIN_ALIAS_BY_PROVIDER[input.provider] ?? {};

  if (rawChainId) {
    const byRawChainIdAlias = aliasMap[rawChainId.toLowerCase()];
    if (byRawChainIdAlias) {
      return {
        canonical: byRawChainIdAlias,
        rawChain,
        rawChainId,
      };
    }
  }

  if (rawChain) {
    const aliasKey = rawChain.toLowerCase();
    const byRawChainAlias = aliasMap[aliasKey];
    if (byRawChainAlias) {
      return {
        canonical: byRawChainAlias,
        rawChain,
        rawChainId,
      };
    }
  }

  const fromChainId = canonicalizeFromRawChainId(rawChainId);
  if (fromChainId) {
    return {
      canonical: fromChainId,
      rawChain,
      rawChainId,
    };
  }
  return {
    canonical: `unknown:${input.provider}:${rawChain ?? rawChainId ?? "na"}`,
    rawChain,
    rawChainId,
  };
}
