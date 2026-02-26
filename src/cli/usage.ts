import { PROVIDER_KEYS } from "../domain/provider-key";
import { INGEST_MODES } from "../ingest/types";

export function printUsage(): void {
  console.log(`Usage:
  bun run index.ts migrate [all|provider]
  bun run index.ts ingest <provider> [mode]
  bun run index.ts ingest-loop [mode] [--providers CSV] [--stop-at-oldest ISO8601]
  bun run index.ts integrity [all|provider] [--sample-size N] [--max-pages N] [--scopes N|all] [--window-days N] [--seed N]
  bun run index.ts dashboard [--host HOST] [--port N] [--days N]
  bun run index.ts status [all|provider]
  bun run index.ts runs [all|provider] [--limit N]
  bun run index.ts repair [all|provider]
  bun run index.ts heal [all|provider]

Providers:
  ${PROVIDER_KEYS.join(", ")}

Modes:
  ${INGEST_MODES.join(", ")}
`);
}

