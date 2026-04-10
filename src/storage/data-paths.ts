import { join, resolve } from "node:path";

function readPathEnv(name: string): string | null {
  const rawValue = Bun.env[name] ?? process.env[name];
  if (typeof rawValue !== "string") {
    return null;
  }

  const trimmed = rawValue.trim();
  if (trimmed.length === 0) {
    return null;
  }

  return trimmed.startsWith("/") ? trimmed : resolve(process.cwd(), trimmed);
}

export function getDefaultProviderDataDir(): string {
  return join(process.cwd(), "data", "providers");
}

export function getDefaultAnalyticsDataDir(): string {
  return join(process.cwd(), "data", "analytics");
}

export function getProviderDataDir(): string {
  return readPathEnv("PROVIDER_DATA_DIR") ?? getDefaultProviderDataDir();
}

export function getAnalyticsDataDir(): string {
  return readPathEnv("ANALYTICS_DATA_DIR") ?? getDefaultAnalyticsDataDir();
}

export function getProviderSeedDataDir(): string {
  return readPathEnv("PROVIDER_DATA_SEED_DIR") ?? getDefaultProviderDataDir();
}

export function getAnalyticsSeedDataDir(): string {
  return readPathEnv("ANALYTICS_DATA_SEED_DIR") ?? getDefaultAnalyticsDataDir();
}
