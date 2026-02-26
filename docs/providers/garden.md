# garden

Last verified: 2026-02-19

## Canonical Source
- Product: Garden API v2 Orders endpoints
- Docs:
  - `https://docs.garden.finance/api-reference/endpoint/orders/get-orders`
  - `https://docs.garden.finance/api-reference/endpoint/orders/get-order-by-id`
- Base URL template: `https://{environment}.garden.finance/v2`
  - `environment`: `api` (mainnet), `testnet.api` (testnet)
- Endpoints:
  - `GET /orders` (historical list)
  - `GET /orders/{order}` (single order detail)

## Why This Is Canonical
- First-party Garden API with explicit list + detail surfaces for swaps/orders.
- Supports filters + pagination for backfill pipelines.
- Response includes source and destination swap objects with tx hashes, timestamps, confirmations, amount fields, and solver metadata.

## Auth
- Requires `garden-app-id` request header.

## Historical Query Surface
`GET /orders` params (selected):
- `page`, `per_page`
- `status` (comma-separated: `not-initiated,in-progress,completed,expired,refunded`)
- `from_chain`, `to_chain`, `from_owner`, `to_owner`, `solver_id`, `integrator`, `tx_hash`, `address`

## Core Response Schema (Execution Analysis)
Top-level order:
- `order_id`, `created_at`, `nonce`, `solver_id`, `integrator`, `version`
- `affiliate_fees[]`
- `source_swap`, `destination_swap`

Swap object (both source/destination):
- `swap_id`, `chain`, `asset`
- `initiator`, `redeemer`, `delegate`
- `amount`, `filled_amount`, `asset_price`
- `initiate_tx_hash`, `redeem_tx_hash`, `refund_tx_hash`
- `initiate_block_number`, `redeem_block_number`, `refund_block_number`
- `required_confirmations`, `current_confirmations`
- `initiate_timestamp`, `redeem_timestamp`, `refund_timestamp`

## Example Pull
```bash
curl 'https://testnet.api.garden.finance/v2/orders?page=1&per_page=50&status=completed' \
  -H 'garden-app-id: <YOUR_APP_ID>'
```

