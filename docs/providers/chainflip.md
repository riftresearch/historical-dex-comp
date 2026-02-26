# chainflip

Last verified: 2026-02-19

## Canonical Source (Historical)
- Product: Chainflip explorer backend GraphQL
- Endpoint: `https://explorer-service-processor.chainflip.io/graphql`
- Source of endpoint discovery: official `scan.chainflip.io` frontend bundle references this GraphQL backend.

## Why This Is Canonical For History
- Provides global historical swap/request datasets, not only single-swap lookup.
- Rich query model (`allSwapRequests`, `allSwaps`, nested relations, filters, ordering, pagination).
- Includes swap request lifecycle timestamps, in/out amounts, USD values, retry/abort fields, and chain/asset context.

## Auth
- Public read access (no auth required for observed queries).

## Historical Query Surface
Primary entities:
- `allSwapRequests`
- `allSwaps`
- `swapRequestById`, `swapById`, `swapByNativeId`

Recommended pattern:
- Page over `allSwapRequests(orderBy: ID_DESC, first: N, after: cursor)`
- Expand nested `swapsBySwapRequestId` to capture execution chunks/retries.

## Core Response Schema (Execution Analysis)
`SwapRequest` fields (selected):
- `id`, `nativeId`, `type`
- `sourceChain`, `destinationChain`, `sourceAsset`, `destinationAsset`
- `depositAmount`, `depositValueUsd`
- `requestedBlockTimestamp`, `completedBlockTimestamp`
- `totalBrokerCommissionBps`, `dcaNumberOfChunks`, `isLiquidation`
- Nested: `swapsBySwapRequestId`

`Swap` fields (selected):
- `id`, `nativeId`, `type`
- `swapInputAmount`, `swapOutputAmount`
- `swapInputValueUsd`, `swapOutputValueUsd`
- `swapScheduledBlockTimestamp`, `swapExecutedBlockTimestamp`
- `retryCount`, `swapAbortedReason`, `swapRescheduledReason`
- `swapRequestId`

## Example Pull
```bash
curl -X POST 'https://explorer-service-processor.chainflip.io/graphql' \
  -H 'content-type: application/json' \
  --data '{"query":"query { allSwapRequests(first: 2, orderBy: ID_DESC) { nodes { id nativeId sourceChain destinationChain sourceAsset destinationAsset requestedBlockTimestamp completedBlockTimestamp depositAmount depositValueUsd totalBrokerCommissionBps swapsBySwapRequestId(first: 2, orderBy: ID_DESC) { nodes { id type swapInputAmount swapOutputAmount swapInputValueUsd swapOutputValueUsd swapScheduledBlockTimestamp swapExecutedBlockTimestamp retryCount swapAbortedReason } } } } }"}'
```

## Supplemental (Per-Swap Status API)
- Chainflip SDK default backend (mainnet): `https://chainflip-swap.chainflip.io/`
- Endpoint used by SDK: `GET /v2/swaps/{id}`
- Useful for direct swap status lookup when ID is already known, but not ideal as primary historical index source.

