# lifi

Last verified: 2026-02-25

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

## Exact Path Extraction For `USDC|ETH|USDT|WETH -> cbBTC` (Ethereum Mainnet)
- Locked repo floor:
  - `fromTimestamp=1767225600` (`2026-01-01T00:00:00Z`)
- Query filters:
  - `fromChain=1`
  - `toChain=1`
  - `toToken=0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf`
  - `fromToken IN {`
    - `0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48` (USDC)
    - `0xdac17f958d2ee523a2206206994597c13d831ec7` (USDT)
    - `0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2` (WETH)
    - `0x0000000000000000000000000000000000000000` (ETH in LI.FI analytics payloads)
  - `}`

## Example Pull
```bash
curl 'https://li.quest/v1/analytics/transfers?fromChain=1&toChain=1&fromToken=0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48&toToken=0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf&fromTimestamp=1767225600&toTimestamp=1772063999'
```

## Notes
- For `event_at`, prefer `sending.timestamp`; fallback to `receiving.timestamp`.
- In the required window (`2026-01-01` to `2026-02-25`), the endpoint returns non-zero records for all four requested source assets.
