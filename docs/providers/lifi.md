# lifi

Last verified: 2026-04-10

## Canonical Source
- Product: LI.FI Analytics API (`transfers`)
- Base URL: `https://li.quest`
- Endpoint: `GET /v1/analytics/transfers`

## Why This Is Canonical
- First-party LI.FI API endpoint serving historical transfer/swap records.
- Includes chain, token, amount, tx hash, and timestamp data needed for execution-quality analysis.
- Supports direct filtering by source/destination chain, source/destination token, and timestamp window.

## Auth
- Public read endpoint.
- Rate limits are enforced (`ratelimit-*` headers observed in responses).

## Historical Query Surface
- Chain filters:
  - `fromChain`
  - `toChain`
- Token filters:
  - `fromToken`
  - `toToken`
- Time filters:
  - `fromTimestamp` (unix seconds)
  - `toTimestamp` (unix seconds)
- Response shape:
  - Top-level `transfers[]` array only (no continuation cursor in payload).

## Pagination / Backfill Strategy
- Treat one API response as a bounded time-slice pull.
- Observed behavior:
  - Response is capped at `1000` records per request.
  - `limit` does not reliably reduce payload size on this endpoint.
- Deterministic pull strategy:
  - Query by `[fromTimestamp, toTimestamp]`.
  - If `len(transfers) == 1000`, split the time range into smaller windows and retry.
  - Deduplicate by stable key (`transactionId`).

## Core Response Schema (Execution Analysis)
- `transfers[].transactionId`
- `transfers[].status`, `substatus`, `substatusMessage`
- `transfers[].sending`:
  - `txHash`, `timestamp`, `chainId`
  - `token.address`, `token.symbol`, `token.decimals`
  - `amount`, `amountUSD`
- `transfers[].receiving`:
  - `txHash`, `timestamp`, `chainId`
  - `token.address`, `token.symbol`, `token.decimals`
  - `amount`, `amountUSD`
- `transfers[].tool`
- `transfers[].lifiExplorerLink`

## Current Scoped Path Extraction
- Locked repo floor:
  - `fromTimestamp=1767225600` (`2026-01-01T00:00:00Z`)
- Chain IDs:
  - Ethereum: `1`
  - Base: `8453`
  - Bitcoin: `20000000000001`
- Token identifiers:
  - Bitcoin BTC: `bitcoin`
  - EVM ETH/native: `0x0000000000000000000000000000000000000000`
  - Ethereum USDC: `0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48`
  - Ethereum USDT: `0xdac17f958d2ee523a2206206994597c13d831ec7`
  - Ethereum WETH: `0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2`
  - Base USDC: `0x833589fcd6edb6e08f4c7c32d4f71b54bda02913`
  - Base USDT: `0xfde4c96c8593536e31f229ea8f37b2ada2699bb2`
  - Base WETH: `0x4200000000000000000000000000000000000006`
  - cbBTC: `0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf`
- Same-chain Ethereum and Base routes:
  - `USDC|USDT|ETH|WETH -> CBBTC`
  - `USDC -> ETH|WETH`
  - `ETH|WETH -> USDC`
- Ethereum <> Bitcoin routes:
  - `USDC|USDT|ETH|WETH -> BTC`
  - `BTC -> USDC|USDT|ETH|WETH`

## Example Pull
```bash
curl 'https://li.quest/v1/analytics/transfers?fromChain=1&toChain=1&fromToken=0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48&toToken=0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf&fromTimestamp=1767225600&toTimestamp=1772063999'
curl 'https://li.quest/v1/analytics/transfers?fromChain=1&toChain=20000000000001&fromToken=0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48&toToken=bitcoin&fromTimestamp=1767225600&toTimestamp=1772063999'
```

## Notes
- For `event_at`, prefer `sending.timestamp`; fallback to `receiving.timestamp`.
- The Bitcoin chain/token identifiers above were verified against `GET /v1/tokens?chains=20000000000001`.
- The analytics endpoint returns non-zero records for Ethereum -> Bitcoin and Bitcoin -> Ethereum pulls using those identifiers.
