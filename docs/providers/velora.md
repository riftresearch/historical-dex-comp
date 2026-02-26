# velora

Last verified: 2026-02-25

## Canonical Source (Public Historical)
- Product: Velora (ParaSwap) onchain swap events (legacy AugustusSwapper v5)
- Contract: `0xdef171fe48cf0115b1d80b88dc8eab59176fee57` (Ethereum mainnet)
- Transport: JSON-RPC (`eth_getLogs`)
- Upstream docs:
  - Velora API docs: https://developers.velora.xyz
  - Velora subgraphs page: https://developers.velora.xyz/subgraphs
  - Open-source subgraph schema/mappings (event definitions): https://github.com/VeloraDEX/paraswap-subgraph

## Why This Is Canonical
- First-party-operated protocol contract with immutable onchain events.
- Event payloads include source/destination token addresses, amounts, tx hash, and block context.
- Replayable by block range for deterministic backfills.

## Important Scope Caveat
- Velora public API endpoints validated:
  - `GET https://api.velora.xyz/prices` (quotes)
  - `GET https://api.velora.xyz/tokens/:network` (token metadata)
  - `GET https://api.velora.xyz/delta/orders` (historical orders) requires `userAddress` and is user-scoped.
- There is no verified public global transaction-history endpoint for all users in current API docs.
- Result: public global history extraction is currently feasible from legacy onchain swap events; Delta/user-scoped API cannot be used alone for global provider-wide history.

## Auth
- Requires Ethereum RPC access (URL/API key managed by RPC provider).

## Historical Query Surface
- `eth_getLogs` with:
  - `address = 0xdef171fe48cf0115b1d80b88dc8eab59176fee57`
  - `topics[0] IN {` `Swapped`, `Swapped2`, `SwappedV3`, `SwappedDirect`, `Bought`, `Bought2`, `BoughtV3` `}`
  - `topics[2] = srcToken` (indexed)
  - `topics[3] = destToken` (indexed)
- `eth_getBlockByNumber` for timestamp hydration if block timestamp cache is not present.

## Pagination / Backfill Strategy
- This environment's RPC enforces `eth_getLogs` range limit of `10,000` blocks.
- Iterate deterministic block windows (`fromBlock..toBlock`) of max `10,000`.
- Deduplicate by `(transactionHash, logIndex)`.

## Core Event Schema (Execution Analysis)
- Indexed topics:
  - `beneficiary`
  - `srcToken`
  - `destToken`
- Non-indexed data:
  - `srcAmount`
  - `receivedAmount`
  - `expectedAmount` (on `*V3` / `*Direct` variants)
- Envelope:
  - `transactionHash`, `blockNumber`, `logIndex`, `blockHash`

## Exact Path Extraction For `USDC|ETH|USDT|WETH -> cbBTC` (Ethereum Mainnet)
- Destination token (`topics[3]`):
  - `0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf` (cbBTC)
- Source token (`topics[2]`) candidates:
  - `0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48` (USDC)
  - `0xdac17f958d2ee523a2206206994597c13d831ec7` (USDT)
  - `0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2` (WETH)
  - `0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee` (ETH sentinel)
- Locked repo window:
  - From `2026-01-01T00:00:00Z` onward (floor), mapped to block ranges.

## Notes
- In the required window (`2026-01-01` to `2026-02-25`), legacy v5 onchain logs include `USDC -> cbBTC` and `USDT -> cbBTC` events.
- `ETH -> cbBTC` and `WETH -> cbBTC` were not observed in the same v5 sweep for that window.
- If full Velora coverage for all four paths is mandatory, request a first-party global historical endpoint (or first-party index export) that includes post-v5 flow coverage.
