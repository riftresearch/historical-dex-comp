# nearintents

Last verified: 2026-02-25

## Canonical Source
- Product: NEAR Intents Explorer API (historical 1Click swap history)
- Docs page: `https://docs.near-intents.org/near-intents/integration/distribution-channels/intents-explorer-api`
- Swagger UI: `https://explorer.near-intents.org/api/docs`
- OpenAPI: `https://explorer.near-intents.org/api/v0/openapi.yaml`
- Endpoints:
  - `GET /api/v0/transactions`
  - `GET /api/v0/transactions-pages`

## Why This Is Canonical
- Officially documented as the historical source that mirrors NEAR Intents Explorer data.
- Designed for distribution channels and analytics use cases.
- Exposes transaction-level statuses, amounts, tx hashes, referral/app-fee metadata, and chain/token filters.

## Auth
- JWT bearer token is mandatory (`Authorization: Bearer <token>`).
- Access via API key form documented by NEAR Intents.
- Rate limit documented: 1 request / 5 seconds per partner.
- Runtime validation note: Explorer API rejects distribution-channel JWTs; token payload
  `key_type` must be `explorer`.

## Historical Query Surface
Shared filtering includes:
- `search`
- `fromChainId`, `fromTokenId`, `toChainId`, `toTokenId`
- `statuses` (`FAILED,INCOMPLETE_DEPOSIT,PENDING_DEPOSIT,PROCESSING,REFUNDED,SUCCESS`)
- `startTimestamp` / `endTimestamp` (ISO) and Unix variants
- `minUsdPrice`, `maxUsdPrice`, `referral`, `affiliate`, `showTestTxs`

Pagination:
- Cursor-like: `transactions` with `lastDepositAddress`, `lastDepositMemo`, `direction`
- Page-based: `transactions-pages` with `page`, `perPage` (`perPage` supports up to `1000`)

## Core Response Schema (Execution Analysis)
Transaction item fields (selected):
- `originAsset`, `destinationAsset`
- `depositAddress`, `depositMemo`
- `recipient`, `status`
- `createdAt`, `createdAtTimestamp`
- `amountIn`, `amountOut`, `amountInUsd`, `amountOutUsd`
- `amountInFormatted`, `amountOutFormatted`
- `nearTxHashes[]`, `originChainTxHashes[]`, `destinationChainTxHashes[]`
- `referral`, `appFees[]`, `senders[]`, `refundTo`, `refundReason`

## Example Pull
```bash
curl 'https://explorer.near-intents.org/api/v0/transactions-pages?page=1&perPage=1000&statuses=SUCCESS,REFUNDED' \
  -H 'Authorization: Bearer <JWT_TOKEN>'
```
