# historical-dex-comp

## Install

```bash
bun install
```

## Commands

```bash
bun run migrate
bun run ingest -- relay bootstrap
bun run ingest -- relay sync_newer
bun run ingest -- relay backfill_older
bun run ingest -- lifi bootstrap
bun run ingest -- lifi sync_newer
bun run ingest -- lifi backfill_older
bun run ingest -- thorchain bootstrap
bun run ingest -- thorchain sync_newer
bun run ingest -- thorchain backfill_older
bun run ingest -- chainflip bootstrap
bun run ingest -- chainflip sync_newer
bun run ingest -- chainflip backfill_older
bun run ingest -- garden bootstrap
bun run ingest -- garden sync_newer
bun run ingest -- garden backfill_older
bun run ingest -- nearintents bootstrap
bun run ingest -- nearintents sync_newer
bun run ingest -- nearintents backfill_older
bun run ingest -- cowswap bootstrap
bun run ingest -- cowswap sync_newer
bun run ingest -- cowswap backfill_older
bun run ingest -- kyberswap bootstrap
bun run ingest -- kyberswap sync_newer
bun run ingest -- kyberswap backfill_older
bun run index.ts ingest-loop backfill_older --providers lifi,relay,thorchain,chainflip,garden,nearintents,cowswap,kyberswap --stop-at-oldest 2026-01-01T00:00:00Z
PROVIDERS=lifi,relay,thorchain,chainflip,garden,nearintents,cowswap,kyberswap MODE=backfill_older STOP_AT_OLDEST=2026-01-01T00:00:00Z docker compose up -d ingester
bun run index.ts status all
bun run index.ts status relay
bun run index.ts runs all --limit 20
bun run index.ts runs relay --limit 10
bun run index.ts repair all
bun run index.ts repair nearintents
bun run index.ts heal all
bun run index.ts heal nearintents
bun run integrity -- all --sample-size 20 --max-pages 3 --scopes 3 --window-days 30 --seed 42
bun run integrity -- nearintents --sample-size 15 --max-pages 4 --scopes 2 --window-days 14
bun run regen-frontend-precomputed
bun run regen-frontend-precomputed -- --window-days 30
bun run regen-frontend-precomputed -- --start-at 2026-01-26T00:00:00Z --end-at 2026-02-25T00:00:00Z
bun run implementation-shortfall -- --provider kyberswap --source-chain eip155:1 --source-token USDC --destination-chain eip155:1 --destination-token CBBTC --start-at 2026-01-26T00:00:00Z --end-at 2026-02-25T00:00:00Z --stablecoin-peg-mode usd_1
bun run implementation-shortfall -- --provider kyberswap --source-chain eip155:1 --source-token ETH --destination-chain eip155:1 --destination-token CBBTC --start-at 2026-01-26T00:00:00Z --end-at 2026-02-25T00:00:00Z --stablecoin-peg-mode usd_1
bun run implementation-shortfall -- --provider kyberswap --source-chain eip155:1 --source-token USDT --destination-chain eip155:1 --destination-token CBBTC --start-at 2026-01-26T00:00:00Z --end-at 2026-02-25T00:00:00Z --stablecoin-peg-mode usd_1
bun run implementation-shortfall -- --provider kyberswap --source-chain eip155:1 --source-token WETH --destination-chain eip155:1 --destination-token CBBTC --start-at 2026-01-26T00:00:00Z --end-at 2026-02-25T00:00:00Z --stablecoin-peg-mode usd_1
bun run dashboard -- --host 0.0.0.0 --port 3000 --days 30
docker compose -f docker-compose.frontend.yml up -d frontend
docker compose -f docker-compose.frontend.yml logs -f frontend
docker compose -f docker-compose.frontend.yml stop frontend
bun run implementation-shortfall -- --provider relay --source-chain eip155:8453 --source-token ETH --destination-chain eip155:8453 --destination-token USDC --limit 1000 --stablecoin-peg-mode usd_1
START_AT=$(date -u -d '7 days ago' +%Y-%m-%dT%H:%M:%SZ); END_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ); for p in lifi relay thorchain chainflip garden nearintents cowswap kyberswap; do bun run implementation-shortfall -- --provider "$p" --source-chain eip155:1 --source-token ETH --destination-chain bitcoin:mainnet --destination-token BTC --start-at "$START_AT" --end-at "$END_AT" --min-notional-usd 1000 --max-notional-usd 10000 --stablecoin-peg-mode usd_1; done
bun run typecheck
```

## Notes

- Storage is one DuckDB file per provider under `data/providers/`.
- Current ingestion implementation is wired for `lifi`, `relay`, `thorchain`, `chainflip`, `garden`, `nearintents`, `cowswap`, and `kyberswap`.
- `ingest-loop` runs provider workers concurrently in one Bun process.
- `repair` is local-only maintenance: it reconciles checkpoint oldest/newest bounds from persisted `swaps_core` and marks stale `running` runs as failed.
- `heal` is API-backed remediation: it prunes rows older than the floor (`2026-01-01T00:00:00Z` by `COALESCE(event_at, created_at, updated_at)`), runs `sync_newer`, then `backfill_older` cycles until the floor is reached or progress stalls.
- `integrity` now runs two layers of audit:
  - DB reconciliation for every provider (`swaps_core`/`swaps_raw` linkage, `raw_hash_latest` validity, scope violations, floor violations, and daily gap detection),
  - source sampling where implemented (`lifi`, `relay`, `thorchain`, `chainflip`, `garden`, `nearintents`) plus block-sampled onchain log verification for `kyberswap` and `cowswap`, with hit-rate + Wilson lower-bound stats.
- Provider-specific pagination and pacing tuning is configured in `src/ingest/provider-tuning.ts`.
- `STOP_AT_OLDEST` auto-stops each `backfill_older` provider worker once `ingest_checkpoints.oldest_event_at` is older than or equal to the cutoff.
- `dashboard` binds to `0.0.0.0` by default and serves the analytics UI (open via `http://localhost:3000` from the same machine).
- `docker-compose.frontend.yml` runs the dashboard as a dedicated daemonized service separate from `ingester`.
- `implementation-shortfall` is a standalone analytics script (separate from ingester/frontend) that computes per-trade execution shortfall in bps from Coinbase 1-minute USD candles and writes provider/path datasets under `data/analytics/execution-quality/`.
- `regen-frontend-precomputed` runs the full frontend precompute matrix in one command (`lifi,relay,thorchain,chainflip,garden,nearintents,cowswap,kyberswap` x `USDC,USDT,ETH` on `eip155:1 -> bitcoin:mainnet`, default last 30 days, `--stablecoin-peg-mode usd_1`).
- If `kyberswap`, `lifi`, `relay`, and/or `cowswap` are included in `regen-frontend-precomputed`, it also precomputes `USDC/USDT/ETH/WETH -> CBBTC` plus `USDC -> ETH` and `ETH -> USDC` paths on both `eip155:1` and `eip155:8453`.
- `implementation-shortfall` supports `--stablecoin-peg-mode usd_1` to price supported stablecoins at a fixed $1.00 instead of Coinbase candles.
- Coinbase price lookups are cached persistently in SQLite at `data/analytics/coinbase-price-cache.sqlite` to minimize repeated API calls.
- The frontend reads `/api/shortfall-distribution` and renders per-provider shortfall histogram charts for fixed 30-day windows across `USDC/USDT/ETH` (`eip155:1`) to `BTC` (`bitcoin:mainnet`) paths, `USDC/USDT/ETH/WETH -> CBBTC` (`eip155:1` and `eip155:8453`) paths, and `USDC -> ETH`, `ETH -> USDC` (`eip155:1` and `eip155:8453`) paths.
- Garden requires an app ID in `GARDEN_APP_ID` (sent as `garden-app-id`).
- Near Intents requires an Explorer JWT in `NEAR_INTENTS_KEY` (`key_type` must be `explorer`).
- CoW Swap ingestion requires `COWSWAP_RPC_URL` and supports optional Base overrides via `COWSWAP_BASE_RPC_URL` and `COWSWAP_BASE_SETTLEMENT_CONTRACT` (default settlement: `0x9008d19f58aabd9ed0d60971565aa8510560ab41`).
- KyberSwap ingestion requires `KYBERSWAP_RPC_URL` and supports optional Base overrides via `KYBERSWAP_BASE_RPC_URL` and `KYBERSWAP_BASE_META_AGGREGATOR` (default meta-aggregator: `0x6131b5fae19ea4f9d964eac0408e4408b66337b5`).
