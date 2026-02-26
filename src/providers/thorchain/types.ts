export interface ThorchainCoin {
  amount?: string | null;
  asset?: string | null;
}

export interface ThorchainActionTransfer {
  address?: string | null;
  coins?: ThorchainCoin[] | null;
  txID?: string | null;
  height?: string | null;
}

export interface ThorchainSwapMetadata {
  affiliateAddress?: string | null;
  affiliateFee?: string | null;
  inPriceUSD?: string | null;
  outPriceUSD?: string | null;
  liquidityFee?: string | null;
  memo?: string | null;
  swapSlip?: string | null;
  swapTarget?: string | null;
  txType?: string | null;
}

export interface ThorchainAction {
  date?: string | null;
  height?: string | null;
  in?: ThorchainActionTransfer[] | null;
  out?: ThorchainActionTransfer[] | null;
  metadata?: {
    swap?: ThorchainSwapMetadata | null;
  } | null;
  pools?: string[] | null;
  status?: string | null;
  type?: string | null;
}

export interface ThorchainActionsPageResponse {
  actions?: ThorchainAction[];
  count?: string | null;
  meta?: {
    nextPageToken?: string | null;
    prevPageToken?: string | null;
  } | null;
}
