# cowswap

Last verified: 2026-03-05

## Canonical Source (Historical)
- Product: CoW Protocol GPv2 settlement trades on Ethereum mainnet and Base.
- Contract: `0x9008d19f58aabd9ed0d60971565aa8510560ab41` (same on both chains).
- Transport: JSON-RPC over HTTPS.
- RPC URLs (env):
  - Ethereum: `COWSWAP_RPC_URL` (falls back to `KYBERSWAP_RPC_URL` if unset).
  - Base: `COWSWAP_BASE_RPC_URL` (falls back to `BASE_RPC_URL`, then `https://base.llamarpc.com`).

## Why This Is Canonical
- First-party execution is finalized on the settlement contract.
- Trade logs are immutable and replayable by block range.
- Logs contain execution primitives required for analysis:
  - sold token, bought token, sold amount, bought amount, protocol fee, owner, tx reference, block time.

## Auth
- RPC provider credential required in URL/header.
- No separate CoW API key is required for `eth_getLogs`.

## Historical Query Surface
- `eth_getLogs` filtered by:
  - `address = 0x9008d19f58aabd9ed0d60971565aa8510560ab41`
  - `topics[0] = 0xa07a543ab8a018198e99ca0184c93fe9050a79400a0a723441f84de1d972cc17`
    (`Trade(address,address,address,uint256,uint256,uint256,bytes)`).
- `eth_getBlockByNumber` for timestamp hydration fallback when log payload omits block timestamp.
- Pagination strategy:
  - deterministic block window traversal,
  - adaptive range splitting on RPC range/response-size errors.

## Why Not CoW `/api/v2/trades` As Primary
- CoW first-party API endpoint `/api/v2/trades` is owner/order scoped:
  - requires exactly one of `owner` or `orderUid`.
- It does not provide direct global token-path historical traversal.
- For provider-wide path analytics, settlement logs are the canonical complete surface.

## Core Response Schema (Execution Analysis)
From each log envelope:
- `transactionHash`
- `blockNumber`
- `logIndex`
- `blockHash`
- `topics[1]` (owner)

From `Trade` event data payload:
- `sellToken` (`address`)
- `buyToken` (`address`)
- `sellAmount` (`uint256`)
- `buyAmount` (`uint256`)
- `feeAmount` (`uint256`)
- `orderUid` (`bytes`)

Derived:
- `event_at` from block timestamp.
- Stable provider record id: `transactionHash:logIndex`.

## Scoped Path Contract (Locked)
Retain strict direction paths on both `eip155:1` and `eip155:8453`:
- `USDC -> cbBTC`
- `ETH -> cbBTC`
- `USDT -> cbBTC`
- `WETH -> cbBTC`
- `USDC -> ETH`
- `ETH -> USDC`

Mainnet token addresses:
- `USDC`: `0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48`
- `USDT`: `0xdac17f958d2ee523a2206206994597c13d831ec7`
- `WETH`: `0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2`
- `cbBTC`: `0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf`

Base token addresses:
- `USDC`: `0x833589fcd6edb6e08f4c7c32d4f71b54bda02913`
- `USDT`: `0xfde4c96c8593536e31f229ea8f37b2ada2699bb2`
- `WETH`: `0x4200000000000000000000000000000000000006`
- `cbBTC`: `0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf`
