# relay

Last verified: 2026-02-19

## Canonical Source
- Product: Relay API (`Get Requests`)
- Docs: https://docs.relay.link/references/api/get-requests
- Base URL: `https://api.relay.link`
- Endpoint: `GET /requests/v2`

## Why This Is Canonical
- First-party Relay API endpoint for indexed cross-chain requests.
- Supports historical backfills via timestamps, block ranges, sorting, and pagination.
- Response includes request lifecycle status, in/out txs, fee/amount metadata, and routing context.

## Auth
- Public endpoint for reads.
- Relay also documents API keys/rate limits for production throughput.

## Historical Query Surface
- Pagination: `limit`, `continuation`
- Time filters: `startTimestamp`, `endTimestamp`
- Block filters: `startBlock`, `endBlock`
- Targeting: `user`, `hash`, `id`, `orderId`, `originChainId`, `destinationChainId`, `chainId`
- Sorting: `sortBy` (`createdAt|updatedAt`), `sortDirection` (`asc|desc`)

## Exact Path Extraction For `USDC|ETH|USDT|WETH -> cbBTC` (Ethereum Mainnet)
- Locked range for this repo:
  - `startTimestamp=1767225600` (`2026-01-01T00:00:00Z`)
  - `endTimestamp` = current run upper bound (for example `now`)
- Request scope:
  - `originChainId=1`
  - `destinationChainId=1`
- Page through all results using `continuation`.
- Filter each record after fetch on:
  - `data.metadata.currencyOut.currency.address == 0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf`
  - `data.metadata.currencyIn.currency.address IN {`
    - `0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48` (USDC)
    - `0xdac17f958d2ee523a2206206994597c13d831ec7` (USDT)
    - `0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2` (WETH)
    - `0x0000000000000000000000000000000000000000` (ETH in Relay payloads)
  - `}`
- If an address is missing for a record, fallback to symbol match (`USDC|USDT|WETH|ETH` -> `CBBTC`) before dropping.

## Core Response Schema (Execution Analysis)
- `requests[].id`
- `requests[].status` (`waiting|pending|success|failure|refund`)
- `requests[].createdAt`, `requests[].updatedAt`
- `requests[].data.inTxs[]`: `hash`, `chainId`, `timestamp`, `status`, `fee`
- `requests[].data.outTxs[]`: `hash`, `chainId`, `timestamp`, `status`, `fee`
- `requests[].data.metadata.currencyIn` / `currencyOut`: currency + amount + USD fields
- `requests[].data.metadata.rate`
- `requests[].data.fees` / `feesUsd`
- `continuation`

## Example Pull
```bash
curl 'https://api.relay.link/requests/v2?limit=100&sortBy=createdAt&sortDirection=desc'
```

## Notes
- `GET /requests/v2` is the strongest Relay source for historical executed request/swaps.
- Transaction indexing endpoints (`/transactions/index`, `/transactions/single`) are ingestion helpers, not historical query APIs.
