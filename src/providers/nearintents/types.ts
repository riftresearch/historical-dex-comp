export interface NearIntentsAppFee {
  recipient: string;
  fee: number;
}

export interface NearIntentsTransaction {
  originAsset: string;
  destinationAsset: string;
  depositAddress: string;
  depositMemo: string | null;
  depositAddressAndMemo: string;
  recipient: string;
  status: string;
  createdAt: string;
  createdAtTimestamp: number;
  intentHashes: string | null;
  referral: string | null;
  amountInFormatted: string;
  amountOutFormatted: string;
  appFees: NearIntentsAppFee[];
  nearTxHashes: string[];
  originChainTxHashes: string[];
  destinationChainTxHashes: string[];
  amountIn: string;
  amountInUsd: string;
  amountOut: string;
  amountOutUsd: string;
  refundTo: string;
  senders: string[];
  refundReason?: string | null;
}

export interface NearIntentsTransactionsPagesResponse {
  data?: NearIntentsTransaction[];
  totalPages?: number;
  page?: number;
  perPage?: number;
  total?: number;
  nextPage?: number | null;
  prevPage?: number | null;
}
