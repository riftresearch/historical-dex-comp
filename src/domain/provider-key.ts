export const PROVIDER_KEYS = [
  "lifi",
  "relay",
  "thorchain",
  "chainflip",
  "garden",
  "nearintents",
  "kyberswap",
] as const;

export type ProviderKey = (typeof PROVIDER_KEYS)[number];

export function isProviderKey(value: string): value is ProviderKey {
  return (PROVIDER_KEYS as readonly string[]).includes(value);
}
