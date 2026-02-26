export interface LifiToken {
  address?: string | null;
  chainId?: number | string | null;
  symbol?: string | null;
  decimals?: number | null;
  name?: string | null;
}

export interface LifiTransferStep {
  tool?: string | null;
}

export interface LifiTransferLeg {
  txHash?: string | null;
  chainId?: number | string | null;
  token?: LifiToken | null;
  amount?: string | null;
  amountUSD?: string | number | null;
  timestamp?: number | string | null;
  gasAmountUSD?: string | number | null;
  includedSteps?: LifiTransferStep[] | null;
}

export interface LifiFeeCost {
  amountUSD?: string | number | null;
}

export interface LifiTransferRecord {
  transactionId?: string | null;
  status?: string | null;
  substatus?: string | null;
  substatusMessage?: string | null;
  tool?: string | null;
  lifiExplorerLink?: string | null;
  sending?: LifiTransferLeg | null;
  receiving?: LifiTransferLeg | null;
  feeCosts?: LifiFeeCost[] | null;
}

export interface LifiTransfersResponse {
  transfers?: LifiTransferRecord[];
}
