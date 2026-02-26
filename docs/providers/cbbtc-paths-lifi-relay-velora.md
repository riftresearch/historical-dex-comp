# CBTC Path Historical Pull Plan (`lifi`, `relay`, `velora`)

Last verified: 2026-02-25

## Target Paths (Strict Direction)
- `USDC -> cbBTC`
- `ETH -> cbBTC`
- `USDT -> cbBTC`
- `WETH -> cbBTC`

## Locked Time Window
- Floor: `2026-01-01T00:00:00Z` (`1767225600`)
- Ceiling: ingest runtime `now` (UTC)
- Chain scope for these paths: `eip155:1 -> eip155:1` (Ethereum mainnet to Ethereum mainnet)

## Token Address Set (Ethereum Mainnet)
- `USDC`: `0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48`
- `USDT`: `0xdac17f958d2ee523a2206206994597c13d831ec7`
- `WETH`: `0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2`
- `ETH (sentinel variants)`: `0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee` and provider-specific `0x0000000000000000000000000000000000000000`
- `cbBTC`: `0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf`

## Provider Strategies

### LI.FI
- Endpoint: `GET https://li.quest/v1/analytics/transfers`
- Query directly by chain + token + timestamp:
  - `fromChain=1&toChain=1`
  - `fromToken=<USDC|USDT|WETH|ETH-zero>`
  - `toToken=cbBTC`
  - `fromTimestamp`, `toTimestamp`
- Pagination:
  - Time-window partitioning (response observed capped at `1000` rows).
  - Split windows when row count hits cap.
- Stable dedupe key: `transactionId`.

### Relay
- Endpoint: `GET https://api.relay.link/requests/v2`
- Query by chain + timestamp:
  - `originChainId=1&destinationChainId=1`
  - `startTimestamp`, `endTimestamp`
  - page via `continuation`
- Path filtering:
  - Post-filter response on `currencyIn.currency.address` and `currencyOut.currency.address`.
  - ETH appears as zero address in Relay payloads.
- Stable dedupe key: `requests[].id`.

### Velora
- Public API status:
  - Quote and token metadata endpoints are public.
  - Delta order history endpoint is user-scoped (`userAddress` required), not provider-global.
- Public global historical path:
  - Use onchain logs from legacy Augustus v5 contract:
    - `0xdef171fe48cf0115b1d80b88dc8eab59176fee57`
  - `eth_getLogs` with indexed token topic filtering (`srcToken`, `destToken`).
  - 10k block chunking required on this RPC.
- Coverage caveat:
  - For the locked window, v5 includes non-zero `USDC -> cbBTC` and `USDT -> cbBTC`.
  - `ETH -> cbBTC` and `WETH -> cbBTC` were not observed in the same v5 sweep.
  - Full Velora global coverage for all four paths likely requires a first-party global history feed beyond user-scoped Delta APIs.

## Execution Order
1. Backfill LI.FI (fastest direct filter surface).
2. Backfill Relay (cursor-based API with post-filter).
3. Backfill Velora v5 logs.
4. Reconcile path-level coverage and mark gaps explicitly before frontend comparison rollup.
