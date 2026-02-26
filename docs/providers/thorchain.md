# thorchain

Last verified: 2026-02-19

## Canonical Sources
THORChain has two first-party Midgard surfaces that should both be used:

1. Per-swap events
- Midgard endpoint: `GET /v2/actions?type=swap`
- Base URLs (mainnet mirrors):
  - `https://midgard.ninerealms.com`
  - `https://midgard.thorswap.net`
  - `https://midgard.thorchain.liquify.com`
- Swagger: `https://midgard.ninerealms.com/v2/swagger.json`

2. Aggregated swap history
- Midgard endpoint: `GET /v2/history/swaps`
- Same base URLs as above

## Why This Is Canonical
- THORChain dev docs explicitly position Midgard as the consumer-facing source for swaps/history.
- `/v2/actions` provides action-level swap records with tx linkage and metadata.
- `/v2/history/swaps` provides bucketed metrics (counts, volume, fees, slip) for longitudinal analytics.

## Auth
- Public read endpoints.

## Historical Query Surface
`GET /v2/actions`
- Key params: `type=swap`, `limit`, `offset`, `nextPageToken`, `prevPageToken`, `timestamp`, `fromTimestamp`, `height`, `fromHeight`
- Optional filters: `address`, `txid`, `asset`, `affiliate`, `txType`

`GET /v2/history/swaps`
- Key params: `interval` (`5min|hour|day|week|month|quarter|year`), `count`, `from`, `to`, optional `pool`

## Core Response Schema (Execution Analysis)
`/v2/actions`:
- `actions[].type` (`swap`)
- `actions[].status` (`success|pending|failed`)
- `actions[].date`, `actions[].height`
- `actions[].in[]` / `actions[].out[]`: `txID`, `address`, `coins[]`
- `actions[].metadata.swap`: `liquidityFee`, `swapSlip`, `swapTarget`, `txType`, `affiliateFee`, `networkFees`
- `meta.nextPageToken`, `meta.prevPageToken`

`/v2/history/swaps`:
- `intervals[].startTime`, `endTime`
- `intervals[].totalCount`, `totalVolume`, `totalVolumeUSD`, `totalFees`, `averageSlip`
- Directional components (`toRune*`, `toAsset*`, `toTrade*`, `fromTrade*`, etc.)

## Example Pulls
```bash
curl 'https://midgard.ninerealms.com/v2/actions?type=swap&limit=50'
curl 'https://midgard.ninerealms.com/v2/history/swaps?interval=day&count=30'
```

