export interface RelayCurrency {
  chainId?: number | string | null;
  address?: string | null;
  symbol?: string | null;
  name?: string | null;
  decimals?: number | null;
}

export interface RelayCurrencyAmount {
  currency?: RelayCurrency | null;
  amount?: string | null;
  amountFormatted?: string | null;
  amountUsd?: string | null;
}

export interface RelayRouteHop {
  inputCurrency?: RelayCurrencyAmount | null;
  outputCurrency?: RelayCurrencyAmount | null;
  router?: string | null;
}

export interface RelayMetadata {
  currencyIn?: RelayCurrencyAmount | null;
  currencyOut?: RelayCurrencyAmount | null;
  rate?: string | null;
  sender?: string | null;
  recipient?: string | null;
  route?: {
    origin?: RelayRouteHop | null;
    destination?: RelayRouteHop | null;
  } | null;
}

export interface RelayRequestTx {
  hash?: string | null;
  chainId?: number | string | null;
  timestamp?: number | null;
  status?: string | null;
  fee?: string | null;
  type?: string | null;
}

export interface RelayRequestData {
  failReason?: string | null;
  refundFailReason?: string | null;
  fees?: Record<string, unknown> | null;
  feesUsd?: string | null;
  inTxs?: RelayRequestTx[] | null;
  outTxs?: RelayRequestTx[] | null;
  metadata?: RelayMetadata | null;
  slippageTolerance?: string | null;
}

export interface RelayRequestRecord {
  id?: string | null;
  status?: string | null;
  createdAt?: string | null;
  updatedAt?: string | null;
  user?: string | null;
  recipient?: string | null;
  data?: RelayRequestData | null;
}

export interface RelayRequestsPageResponse {
  requests?: RelayRequestRecord[];
  continuation?: string | null;
}
