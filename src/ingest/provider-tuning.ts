import type { ProviderKey } from "../domain/provider-key";

export interface ProviderIngestTuning {
  pageSize: number;
  maxPages: number | null;
  sleepSeconds: number;
}

const PROVIDER_INGEST_TUNING: Record<ProviderKey, ProviderIngestTuning> = {
  lifi: {
    pageSize: 1000,
    maxPages: 300,
    sleepSeconds: 5,
  },
  relay: {
    pageSize: 50,
    maxPages: 200,
    sleepSeconds: 5,
  },
  thorchain: {
    pageSize: 200,
    maxPages: 200,
    sleepSeconds: 5,
  },
  chainflip: {
    pageSize: 20,
    maxPages: 200,
    sleepSeconds: 5,
  },
  garden: {
    pageSize: 100,
    maxPages: 200,
    sleepSeconds: 5,
  },
  nearintents: {
    pageSize: 1000,
    maxPages: 100,
    sleepSeconds: 5,
  },
  kyberswap: {
    pageSize: 10000,
    maxPages: 30,
    sleepSeconds: 5,
  },
};

export function getProviderIngestTuning(providerKey: ProviderKey): ProviderIngestTuning {
  const tuning = PROVIDER_INGEST_TUNING[providerKey];
  return {
    pageSize: tuning.pageSize,
    maxPages: tuning.maxPages,
    sleepSeconds: tuning.sleepSeconds,
  };
}
