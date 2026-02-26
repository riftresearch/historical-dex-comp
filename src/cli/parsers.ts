import { isProviderKey, PROVIDER_KEYS, type ProviderKey } from "../domain/provider-key";
import { type ProviderIntegrityAuditOptions } from "../inspect/provider-integrity-sampler";
import { listImplementedProviderKeys } from "../providers";

export interface ParsedRunsOptions {
  limit: number;
}

export interface ParsedIngestLoopOptions {
  providers: ProviderKey[];
  stopAtOldest: Date | null;
}

export interface ParsedDashboardOptions {
  host: string;
  port: number;
  days: number;
}

export interface ParsedIntegrityOptions {
  scope: "all" | ProviderKey;
  options: Partial<ProviderIntegrityAuditOptions>;
}

export function parsePositiveInt(name: string, value: string): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0 || !Number.isInteger(parsed)) {
    throw new Error(`Expected a positive integer for ${name}, received '${value}'.`);
  }
  return parsed;
}

export function parseInteger(name: string, value: string): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || !Number.isInteger(parsed)) {
    throw new Error(`Expected an integer for ${name}, received '${value}'.`);
  }
  return parsed;
}

export function parseIsoDate(name: string, value: string): Date {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    throw new Error(`Expected an ISO8601 datetime for ${name}, received '${value}'.`);
  }
  return parsed;
}

export function parseProvidersCsv(value: string): ProviderKey[] {
  const normalized = value.trim().toLowerCase();
  if (!normalized) {
    throw new Error("--providers requires at least one provider.");
  }

  if (normalized === "implemented") {
    return listImplementedProviderKeys();
  }

  if (normalized === "all") {
    const implemented = new Set(listImplementedProviderKeys());
    const missing = PROVIDER_KEYS.filter((providerKey) => !implemented.has(providerKey));
    if (missing.length > 0) {
      throw new Error(`Cannot use 'all' until implemented: ${missing.join(", ")}`);
    }
    return [...PROVIDER_KEYS];
  }

  const parsed: ProviderKey[] = [];
  const seen = new Set<ProviderKey>();
  for (const piece of normalized.split(",")) {
    const provider = piece.trim();
    if (!provider) {
      continue;
    }
    if (!isProviderKey(provider)) {
      throw new Error(`Invalid provider '${provider}' in --providers.`);
    }
    if (!seen.has(provider)) {
      parsed.push(provider);
      seen.add(provider);
    }
  }

  if (parsed.length === 0) {
    throw new Error("--providers requires at least one provider.");
  }

  const implemented = new Set(listImplementedProviderKeys());
  const unsupported = parsed.filter((providerKey) => !implemented.has(providerKey));
  if (unsupported.length > 0) {
    throw new Error(
      `Providers not implemented yet: ${unsupported.join(", ")}. Implemented providers: ${listImplementedProviderKeys().join(", ")}`,
    );
  }

  return parsed;
}

export function parseIngestLoopOptions(args: string[]): ParsedIngestLoopOptions {
  let providers = listImplementedProviderKeys();
  let stopAtOldest: Date | null = null;

  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (!arg) {
      continue;
    }
    if (arg === "--providers") {
      const value = args[index + 1];
      if (!value) {
        throw new Error("--providers requires a comma-separated value.");
      }
      providers = parseProvidersCsv(value);
      index += 1;
      continue;
    }
    if (arg.startsWith("--providers=")) {
      providers = parseProvidersCsv(arg.split("=")[1] ?? "");
      continue;
    }
    if (arg === "--stop-at-oldest") {
      const value = args[index + 1];
      if (value === undefined) {
        throw new Error("--stop-at-oldest requires an ISO8601 datetime value.");
      }
      stopAtOldest = value ? parseIsoDate("--stop-at-oldest", value) : null;
      index += 1;
      continue;
    }
    if (arg.startsWith("--stop-at-oldest=")) {
      const rawValue = arg.split("=")[1] ?? "";
      stopAtOldest = rawValue ? parseIsoDate("--stop-at-oldest", rawValue) : null;
      continue;
    }
    throw new Error(`Unknown argument '${arg}'.`);
  }

  return {
    providers,
    stopAtOldest,
  };
}

export function parseProviderScope(
  value: string | undefined,
  defaultScope: "all" | "required" = "all",
): "all" | ProviderKey {
  if (!value) {
    if (defaultScope === "required") {
      throw new Error(`Missing provider. Use one of: ${PROVIDER_KEYS.join(", ")}`);
    }
    return "all";
  }

  if (value === "all") {
    return "all";
  }
  if (isProviderKey(value)) {
    return value;
  }
  throw new Error(`Invalid provider '${value}'. Use one of: all, ${PROVIDER_KEYS.join(", ")}`);
}

export function parseRunsOptions(args: string[]): ParsedRunsOptions {
  let limit = 20;

  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (!arg) {
      continue;
    }
    if (arg === "--limit") {
      const value = args[index + 1];
      if (!value) {
        throw new Error("--limit requires a numeric value.");
      }
      limit = parsePositiveInt("--limit", value);
      index += 1;
      continue;
    }
    if (arg.startsWith("--limit=")) {
      limit = parsePositiveInt("--limit", arg.split("=")[1] ?? "");
      continue;
    }
    throw new Error(`Unknown argument '${arg}'.`);
  }

  return { limit };
}

export function parseDashboardOptions(args: string[]): ParsedDashboardOptions {
  let host = "0.0.0.0";
  let port = 3000;
  let days = 30;

  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (!arg) {
      continue;
    }
    if (arg === "--host") {
      const value = args[index + 1];
      if (!value) {
        throw new Error("--host requires a value.");
      }
      host = value;
      index += 1;
      continue;
    }
    if (arg.startsWith("--host=")) {
      host = arg.split("=")[1] ?? "";
      if (!host) {
        throw new Error("--host requires a value.");
      }
      continue;
    }
    if (arg === "--port") {
      const value = args[index + 1];
      if (!value) {
        throw new Error("--port requires a numeric value.");
      }
      port = parsePositiveInt("--port", value);
      index += 1;
      continue;
    }
    if (arg.startsWith("--port=")) {
      port = parsePositiveInt("--port", arg.split("=")[1] ?? "");
      continue;
    }
    if (arg === "--days") {
      const value = args[index + 1];
      if (!value) {
        throw new Error("--days requires a numeric value.");
      }
      days = parsePositiveInt("--days", value);
      index += 1;
      continue;
    }
    if (arg.startsWith("--days=")) {
      days = parsePositiveInt("--days", arg.split("=")[1] ?? "");
      continue;
    }
    throw new Error(`Unknown argument '${arg}'.`);
  }

  return {
    host,
    port,
    days,
  };
}

export function parseIntegrityOptions(args: string[]): ParsedIntegrityOptions {
  let scopeArg: string | undefined;
  let optionStartIndex = 0;
  const firstArg = args[0];
  if (firstArg && !firstArg.startsWith("--")) {
    scopeArg = firstArg;
    optionStartIndex = 1;
  }

  const scope = parseProviderScope(scopeArg);
  const options: Partial<ProviderIntegrityAuditOptions> = {};

  for (let index = optionStartIndex; index < args.length; index += 1) {
    const arg = args[index];
    if (!arg) {
      continue;
    }

    if (arg === "--sample-size") {
      const value = args[index + 1];
      if (!value) {
        throw new Error("--sample-size requires a numeric value.");
      }
      options.sampleSize = parsePositiveInt("--sample-size", value);
      index += 1;
      continue;
    }
    if (arg.startsWith("--sample-size=")) {
      options.sampleSize = parsePositiveInt("--sample-size", arg.split("=")[1] ?? "");
      continue;
    }

    if (arg === "--max-pages") {
      const value = args[index + 1];
      if (!value) {
        throw new Error("--max-pages requires a numeric value.");
      }
      options.maxPages = parsePositiveInt("--max-pages", value);
      index += 1;
      continue;
    }
    if (arg.startsWith("--max-pages=")) {
      options.maxPages = parsePositiveInt("--max-pages", arg.split("=")[1] ?? "");
      continue;
    }

    if (arg === "--window-days") {
      const value = args[index + 1];
      if (!value) {
        throw new Error("--window-days requires a numeric value.");
      }
      options.windowDays = parsePositiveInt("--window-days", value);
      index += 1;
      continue;
    }
    if (arg.startsWith("--window-days=")) {
      options.windowDays = parsePositiveInt("--window-days", arg.split("=")[1] ?? "");
      continue;
    }

    if (arg === "--scopes") {
      const value = args[index + 1];
      if (!value) {
        throw new Error("--scopes requires a numeric value or 'all'.");
      }
      options.scopesPerProvider = value.toLowerCase() === "all" ? null : parsePositiveInt("--scopes", value);
      index += 1;
      continue;
    }
    if (arg.startsWith("--scopes=")) {
      const value = arg.split("=")[1] ?? "";
      options.scopesPerProvider = value.toLowerCase() === "all" ? null : parsePositiveInt("--scopes", value);
      continue;
    }

    if (arg === "--seed") {
      const value = args[index + 1];
      if (!value) {
        throw new Error("--seed requires an integer value.");
      }
      options.seed = parseInteger("--seed", value);
      index += 1;
      continue;
    }
    if (arg.startsWith("--seed=")) {
      options.seed = parseInteger("--seed", arg.split("=")[1] ?? "");
      continue;
    }

    throw new Error(`Unknown argument '${arg}'.`);
  }

  return {
    scope,
    options,
  };
}

