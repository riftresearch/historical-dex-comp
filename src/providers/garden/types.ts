export interface GardenResponseBase {
  status?: string | null;
  error?: string | null;
}

export interface GardenAffiliateFeeAsset {
  chain?: string | null;
  asset?: string | null;
}

export interface GardenAffiliateFee {
  address?: string | null;
  asset?: string | GardenAffiliateFeeAsset | null;
  fee?: number | string | null;
}

export interface GardenSwap {
  created_at?: string | null;
  swap_id?: string | null;
  chain?: string | null;
  asset?: string | null;
  initiator?: string | null;
  redeemer?: string | null;
  delegate?: string | null;
  timelock?: number | null;
  filled_amount?: string | null;
  asset_price?: number | string | null;
  amount?: string | null;
  secret_hash?: string | null;
  secret?: string | null;
  instant_refund_tx?: string | null;
  initiate_tx_hash?: string | null;
  redeem_tx_hash?: string | null;
  refund_tx_hash?: string | null;
  initiate_block_number?: string | null;
  redeem_block_number?: string | null;
  refund_block_number?: string | null;
  required_confirmations?: number | null;
  current_confirmations?: number | null;
  initiate_timestamp?: string | null;
  redeem_timestamp?: string | null;
  refund_timestamp?: string | null;
  status?: string | null;
}

export interface GardenOrder {
  order_id?: string | null;
  created_at?: string | null;
  status?: string | null;
  source_swap?: GardenSwap | null;
  destination_swap?: GardenSwap | null;
  nonce?: string | null;
  affiliate_fees?: GardenAffiliateFee[] | null;
  integrator?: string | null;
  version?: string | null;
  solver_id?: string | null;
}

export interface GardenPaginatedOrders {
  data?: GardenOrder[];
  page?: number;
  total_pages?: number;
  total_items?: number;
  per_page?: number;
}

export interface GardenOrdersResponse extends GardenResponseBase {
  result?: GardenPaginatedOrders | null;
}
