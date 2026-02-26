# Implementation Plan

## Implementation Status
- [x] Base domain types (`ProviderKey`, canonical status, normalized swap row shape)
- [x] DuckDB migration runner + Schema v1 table creation
- [x] Shared chain canonicalization utility (including Relay non-EVM chain IDs)
- [x] Ingest repositories (`ingest_runs`, `ingest_checkpoints`, `swaps_core`, `swaps_raw`)
- [x] Relay adapter with:
  - 30-day bootstrap
  - `sync_newer` incremental mode
  - `backfill_older` incremental mode
  - checkpoint oldest/newest boundary tracking
- [x] CLI commands:
  - `migrate [all|provider]`
  - `ingest <provider> [mode]`
  - `ingest-loop [mode] [--providers CSV] [--stop-at-oldest ISO8601]`
  - `status [all|provider]`
  - `runs [all|provider] [--limit N]`
- [x] Provider-specific ingest tuning constants (`pageSize`, `maxPages`, `sleepSeconds`) in `src/ingest/provider-tuning.ts`
- [x] Thorchain adapter implementation
- [x] Multi-provider concurrent ingest loop in one Bun process
- [x] Docker Compose cutoff auto-stop (`STOP_AT_OLDEST`) for `backfill_older`
- [x] Chainflip adapter implementation
- [x] Garden adapter implementation
- [x] Near Intents adapter implementation
- [x] KyberSwap adapter implementation (onchain `Swapped` logs via `eth_getLogs`)

## Storage Decision (Locked)
- Use one persistent DuckDB file per provider.
- Keep provider storage isolated.
- Use an identical schema contract in each provider DB.
- Store normalized rows in `swaps_core`.
- Store source payloads in `swaps_raw`.
- Do not use Parquet as source of truth.
- Use Parquet only as optional export/snapshot output.

## Provider DuckDB Files
- `data/providers/relay.duckdb`
- `data/providers/thorchain.duckdb`
- `data/providers/chainflip.duckdb`
- `data/providers/garden.duckdb`
- `data/providers/nearintents.duckdb`
- `data/providers/kyberswap.duckdb`

## Chain Canonicalization (Locked)
- Canonical chain keys use a CAIP-2 style convention.
- EVM chains: `eip155:<chainId>` (examples: `eip155:1`, `eip155:8453`).
- Non-EVM chains use a locked namespace key (examples: `bitcoin:mainnet`, `solana:mainnet`, `near:mainnet`).
- Canonical and raw values are both persisted.
- If a provider value cannot be mapped, store as `unknown:<provider>:<raw>` and flag for registry update.
- Mapping order:
  - Apply provider-specific chain-ID overrides first (for known non-EVM numeric IDs).
  - Else map provider enums/strings via provider alias maps.
  - Else map numeric chain IDs to `eip155:<chainId>`.
  - Else fallback to `unknown:<provider>:<raw>`.

## Collection Window And Sync Modes (Locked)
- Default bootstrap is last 30 days (`now - 30d` to `now`).
- Sync logic must be boundary-aware and idempotent:
  - `sync_newer`: continue from newest known boundary forward.
  - `backfill_older`: continue from oldest known boundary backward.
- Docker background `backfill_older` supports `STOP_AT_OLDEST=<ISO8601>` to auto-stop once
  `ingest_checkpoints.oldest_event_at <= STOP_AT_OLDEST`.
- Already ingested records must not be inserted again.
- The script must persist both newest and oldest known boundaries per provider stream.

## Schema v1 (Present In Every Provider DB)

### `schema_migrations`
- `version INTEGER PRIMARY KEY`
- `name VARCHAR NOT NULL`
- `checksum VARCHAR NOT NULL`
- `applied_at TIMESTAMPTZ NOT NULL`

### `ingest_runs`
- `run_id VARCHAR PRIMARY KEY`
- `provider_key VARCHAR NOT NULL`
- `started_at TIMESTAMPTZ NOT NULL`
- `ended_at TIMESTAMPTZ`
- `status VARCHAR NOT NULL`
- `from_ts TIMESTAMPTZ`
- `to_ts TIMESTAMPTZ`
- `records_fetched BIGINT NOT NULL DEFAULT 0`
- `records_normalized BIGINT NOT NULL DEFAULT 0`
- `records_upserted BIGINT NOT NULL DEFAULT 0`
- `error_message VARCHAR`

### `ingest_checkpoints`
- `stream_key VARCHAR PRIMARY KEY`
- `sync_scope VARCHAR NOT NULL DEFAULT 'swaps'`
- `newest_cursor VARCHAR`
- `newest_event_at TIMESTAMPTZ`
- `newest_provider_record_id VARCHAR`
- `oldest_cursor VARCHAR`
- `oldest_event_at TIMESTAMPTZ`
- `oldest_provider_record_id VARCHAR`
- `updated_at TIMESTAMPTZ NOT NULL`
- `run_id VARCHAR`

### `swaps_core`
- `normalized_id VARCHAR PRIMARY KEY`
- `provider_key VARCHAR NOT NULL`
- `provider_record_id VARCHAR NOT NULL`
- `provider_parent_id VARCHAR`
- `record_granularity VARCHAR NOT NULL`
- `status_canonical VARCHAR NOT NULL`
- `status_raw VARCHAR`
- `failure_reason_raw VARCHAR`
- `created_at TIMESTAMPTZ`
- `updated_at TIMESTAMPTZ`
- `event_at TIMESTAMPTZ`
- `source_chain_canonical VARCHAR`
- `destination_chain_canonical VARCHAR`
- `source_chain_raw VARCHAR`
- `destination_chain_raw VARCHAR`
- `source_chain_id_raw VARCHAR`
- `destination_chain_id_raw VARCHAR`
- `source_asset_id VARCHAR`
- `destination_asset_id VARCHAR`
- `source_asset_symbol VARCHAR`
- `destination_asset_symbol VARCHAR`
- `source_asset_decimals INTEGER`
- `destination_asset_decimals INTEGER`
- `amount_in_atomic VARCHAR`
- `amount_out_atomic VARCHAR`
- `amount_in_normalized DOUBLE`
- `amount_out_normalized DOUBLE`
- `amount_in_usd DOUBLE`
- `amount_out_usd DOUBLE`
- `fee_atomic VARCHAR`
- `fee_normalized DOUBLE`
- `fee_usd DOUBLE`
- `slippage_bps DOUBLE`
- `solver_id VARCHAR`
- `route_hint VARCHAR`
- `source_tx_hash VARCHAR`
- `destination_tx_hash VARCHAR`
- `refund_tx_hash VARCHAR`
- `extra_tx_hashes JSON`
- `is_final BOOLEAN`
- `raw_hash_latest VARCHAR NOT NULL`
- `source_endpoint VARCHAR NOT NULL`
- `ingested_at TIMESTAMPTZ NOT NULL`
- `run_id VARCHAR NOT NULL`

### `swaps_raw`
- `normalized_id VARCHAR NOT NULL`
- `raw_hash VARCHAR NOT NULL`
- `raw_json JSON NOT NULL`
- `observed_at TIMESTAMPTZ NOT NULL`
- `source_endpoint VARCHAR NOT NULL`
- `source_cursor VARCHAR`
- `run_id VARCHAR NOT NULL`
- `PRIMARY KEY (normalized_id, raw_hash)`

## Notes
- `amount_*_atomic` is kept as `VARCHAR` to avoid precision loss across chains.
- `status_raw` is always preserved even when `status_canonical` exists.
- `*_chain_canonical` is used for normalized analytics keys; `*_chain_raw` and `*_chain_id_raw` preserve provenance.
- `swaps_raw.raw_json` must contain the full original provider payload.
- Provider ingestion must be idempotent via stable `normalized_id` and deduped raw hashes.
- Boundary-aware sync uses `ingest_checkpoints.newest_*` and `ingest_checkpoints.oldest_*` to avoid re-fetching known ranges.
