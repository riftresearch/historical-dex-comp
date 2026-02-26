import type { ProviderKey } from "../domain/provider-key";
import { PROVIDER_KEYS } from "../domain/provider-key";
import { chainflipProviderAdapter } from "./chainflip/adapter";
import { gardenProviderAdapter } from "./garden/adapter";
import { kyberswapProviderAdapter } from "./kyberswap/adapter";
import { lifiProviderAdapter } from "./lifi/adapter";
import { nearintentsProviderAdapter } from "./nearintents/adapter";
import { relayProviderAdapter } from "./relay/adapter";
import { thorchainProviderAdapter } from "./thorchain/adapter";
import type { ProviderAdapter } from "./types";

const ADAPTERS: Partial<Record<ProviderKey, ProviderAdapter>> = {
  lifi: lifiProviderAdapter,
  relay: relayProviderAdapter,
  thorchain: thorchainProviderAdapter,
  chainflip: chainflipProviderAdapter,
  garden: gardenProviderAdapter,
  nearintents: nearintentsProviderAdapter,
  kyberswap: kyberswapProviderAdapter,
};

export function getProviderAdapter(providerKey: ProviderKey): ProviderAdapter {
  const adapter = ADAPTERS[providerKey];
  if (!adapter) {
    throw new Error(`Provider '${providerKey}' is not implemented yet.`);
  }
  return adapter;
}

export function listImplementedProviderKeys(): ProviderKey[] {
  return PROVIDER_KEYS.filter((providerKey) => Boolean(ADAPTERS[providerKey]));
}
