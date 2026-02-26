# historical-dex-comp AGENTS

## Mission
Build a TypeScript + Bun viewer that compares historical execution quality across DEX/intent providers.

## Stack
- Runtime: Bun
- Language: TypeScript
- Output target: repeatable historical analytics with provider-normalized swap events

## Communication Preferences
- Do not reference or paste raw JSON log files in user-facing responses.
- Summarize outcomes directly (status, counts, errors, next action) instead of dumping JSON payloads.

## Canonical Provider Keys

```ts
export type ProviderKey = 'relay' | 'thorchain' | 'chainflip' | 'garden' | 'nearintents';
```

## Lock-In Rules
- Lock canonical data sources before building ingestion logic.
- Do not change a provider source unless one of these is true:
  - Endpoint is deprecated or removed.
  - Schema quality is insufficient for execution-quality analysis.
  - A more canonical first-party source is published.
- Every source decision must be documented in a provider file under `docs/providers/`.

## Milestone 1 (Current)
Find canonical historical swap data sources for each provider and capture:
- Base URL / endpoint URL
- Auth requirements
- Pagination/filter strategy
- Core response schema fields needed for execution-quality comparison

## Milestone 1 Deliverables
- `docs/providers/relay.md`
- `docs/providers/thorchain.md`
- `docs/providers/chainflip.md`
- `docs/providers/garden.md`
- `docs/providers/nearintents.md`

## Acceptance Criteria For Source Selection
- First-party or first-party-operated endpoint.
- Supports historical retrieval (time filters, cursor/page, or deterministic ID traversal).
- Includes enough fields to compute execution metrics (amounts, status, timestamps, chain/asset context, tx references).

## Storage Architecture (Locked)
- Primary persistent storage is DuckDB.
- There is one persistent DuckDB file per provider under `data/providers/`.
- Storage is provider-isolated by design. Do not create shared cross-provider tables at storage level.
- Each provider DB uses the same schema contract to keep ingestion logic consistent.
- Parquet is not the source of truth. Parquet exports are optional snapshots for downstream workflows.

## Provider DB Files
- `data/providers/relay.duckdb`
- `data/providers/thorchain.duckdb`
- `data/providers/chainflip.duckdb`
- `data/providers/garden.duckdb`
- `data/providers/nearintents.duckdb`

## Tables Required In Every Provider DB
- `schema_migrations`
- `ingest_runs`
- `ingest_checkpoints`
- `swaps_core`
- `swaps_raw`

## Schema Reference
- The exact Schema v1 column contract is defined in `docs/plan.md`.

## Collection Window (Locked)
- Default bootstrap ingestion window is the most recent 30 days.
- Historical floor is locked at `2026-01-01T00:00:00Z` (UTC) using `COALESCE(event_at, created_at, updated_at)`.
- Rows older than the floor must be pruned from provider storage.
- Ingestion must support:
  - Continuing forward in time (new data only),
  - Backfilling further into the past (older data only),
  - Avoiding re-download/re-insert of already ingested records.
- The system must track both newest and oldest ingested boundaries per provider stream.

## Docker Background Ingest
- Use Docker Compose service `ingester` with one Bun binary (`ingest-loop`) that runs multiple providers concurrently.
- Command:
  - `PROVIDERS=relay,thorchain,chainflip,garden,nearintents MODE=backfill_older STOP_AT_OLDEST=2026-01-01T00:00:00Z docker compose up -d ingester`
- Auto-stop cutoff command:
  - `PROVIDERS=relay,thorchain,chainflip,garden,nearintents MODE=backfill_older STOP_AT_OLDEST=2026-01-01T00:00:00Z docker compose up -d --force-recreate ingester`
- Add a new provider to the running loop:
  - `docker compose stop ingester`
  - `PROVIDERS=relay,thorchain,chainflip,garden,nearintents,<new-provider> MODE=backfill_older STOP_AT_OLDEST=2026-01-01T00:00:00Z docker compose up -d --force-recreate ingester`
- Follow logs:
  - `docker compose logs -f ingester`
- Stop:
  - `docker compose stop ingester`
- Ingest tuning:
  - Provider-specific `pageSize`, `maxPages`, and `sleepSeconds` are fixed in `src/ingest/provider-tuning.ts`.
- Near Intents auth:
  - `NEAR_INTENTS_KEY` must be an Explorer JWT (`key_type=explorer`).
- Garden auth:
  - `GARDEN_APP_ID` must be set (sent as `garden-app-id`).
