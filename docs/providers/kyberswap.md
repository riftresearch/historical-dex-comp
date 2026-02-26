# kyberswap

Last verified: 2026-02-25

## Canonical Source (Historical)
- Product: KyberSwap MetaAggregationRouterV2 on Ethereum mainnet
- Contract: `0x6131b5fae19ea4f9d964eac0408e4408b66337b5`
- Contract source (verified via Foundry + Blockscout API): [`docs/contracts/kyberswap/MetaAggregationRouterV2/Contract.sol`](/home/alpinevm/Development/rift/historical-dex-comp/docs/contracts/kyberswap/MetaAggregationRouterV2/Contract.sol)
- Transport: JSON-RPC over HTTPS
- RPC URL (env): `KYBERSWAP_RPC_URL`

## Why This Is Canonical For History
- First-party-operated execution router contract that emits onchain swap executions.
- `Swapped` logs are immutable and replayable by block range.
- Contains required execution-quality primitives: source/destination token, input/output amount, tx reference, and event timestamp (via block).

## Auth
- RPC provider credential required in URL or header (for this environment, QuikNode URL in `KYBERSWAP_RPC_URL`).
- No separate Kyber API key is required for `eth_getLogs`.

## Historical Query Surface
- `eth_getLogs` filtered by:
  - `address = 0x6131b5fae19ea4f9d964eac0408e4408b66337b5`
  - `topics[0] = 0xd6d4f5681c246c9f42c203e287975af1601f8df8035a9251f79aab5c8f09e2f8` (`Swapped(address,address,address,address,uint256,uint256)`)
- `eth_getBlockByNumber` for block timestamp hydration.
- Pagination strategy:
  - Traverse by block windows.
  - QuikNode response observed to require max `10,000` block span per `eth_getLogs` call.
  - Use deterministic window stepping for bootstrap, `sync_newer`, and `backfill_older`.

## Core Response Schema (Execution Analysis)
From each log envelope:
- `transactionHash`
- `blockNumber`
- `logIndex`
- `blockHash`

From `Swapped` data payload:
- `sender` (`address`)
- `srcToken` (`address`)
- `dstToken` (`address`)
- `dstReceiver` (`address`)
- `spentAmount` (`uint256`)
- `returnAmount` (`uint256`)

Derived:
- `event_at` from `eth_getBlockByNumber(blockNumber).timestamp`
- Stable provider record id: `transactionHash:logIndex`

## Scoped Pair Contract (Locked For This Provider Path)
Only retain events matching strict direction:
- `USDC -> cbBTC`
- `ETH -> cbBTC` (ETH sentinel `0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE`)
- `USDT -> cbBTC`
- `WETH -> cbBTC`

Mainnet token addresses:
- `USDC`: `0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48`
- `USDT`: `0xdac17f958d2ee523a2206206994597c13d831ec7`
- `WETH`: `0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2`
- `cbBTC`: `0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf`
- `ETH sentinel`: `0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee`

## Verification Commands
```bash
cast source 0x6131b5fae19ea4f9d964eac0408e4408b66337b5 \
  --explorer-api-url https://eth.blockscout.com/api \
  --explorer-url https://eth.blockscout.com \
  -d docs/contracts/kyberswap

cast sig-event "Swapped(address,address,address,address,uint256,uint256)"
```
