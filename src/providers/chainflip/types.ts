export interface ChainflipGraphqlError {
  message: string;
}

export interface ChainflipGraphqlResponse<TData> {
  data?: TData;
  errors?: ChainflipGraphqlError[];
}

export interface ChainflipPageInfo {
  hasNextPage: boolean;
  endCursor: string | null;
}

export interface ChainflipConnection<TNode> {
  nodes?: TNode[];
  pageInfo?: ChainflipPageInfo;
}

export interface ChainflipTransactionRefNode {
  id: number;
  ref: string | null;
}

export interface ChainflipTransactionRefConnection {
  nodes?: ChainflipTransactionRefNode[];
}

export interface ChainflipBroadcastNode {
  id: number;
  nativeId: number | string | null;
  destinationChain: string | null;
  transactionRefsByBroadcastId?: ChainflipTransactionRefConnection | null;
}

export interface ChainflipEgressNode {
  id: number;
  chain: string | null;
  asset: string | null;
  amount: string | null;
  valueUsd: string | null;
  broadcastByBroadcastId?: ChainflipBroadcastNode | null;
}

export interface ChainflipSwapNode {
  id: number;
  nativeId: string | null;
  type: string | null;
  swapRequestId: number | null;
  sourceChain: string | null;
  destinationChain: string | null;
  sourceAsset: string | null;
  destinationAsset: string | null;
  swapInputAmount: string | null;
  swapOutputAmount: string | null;
  swapInputValueUsd: string | null;
  swapOutputValueUsd: string | null;
  swapScheduledBlockTimestamp: string | null;
  swapExecutedBlockTimestamp: string | null;
  swapAbortedBlockTimestamp: string | null;
  retryCount: number | null;
  swapAbortedReason: string | null;
  swapRescheduledReason: string | null;
}

export interface ChainflipSwapRequestNode {
  id: number;
  nativeId: string | null;
  type: string | null;
  sourceChain: string | null;
  destinationChain: string | null;
  sourceAsset: string | null;
  destinationAsset: string | null;
  sourceAddress: string | null;
  destinationAddress: string | null;
  depositAmount: string | null;
  depositValueUsd: string | null;
  requestedBlockTimestamp: string | null;
  completedBlockTimestamp: string | null;
  brokerId: string | null;
  totalBrokerCommissionBps: number | null;
  dcaNumberOfChunks: number | null;
  isLiquidation: boolean | null;
  egressId: number | null;
  refundEgressId: number | null;
  fallbackEgressId: number | null;
  transactionRefsBySwapRequestId?: ChainflipTransactionRefConnection | null;
  egressByEgressId?: ChainflipEgressNode | null;
  egressByRefundEgressId?: ChainflipEgressNode | null;
  swapsBySwapRequestId?: ChainflipConnection<ChainflipSwapNode> | null;
}

export interface ChainflipSwapRequestsPageData {
  allSwapRequests?: ChainflipConnection<ChainflipSwapRequestNode>;
}
