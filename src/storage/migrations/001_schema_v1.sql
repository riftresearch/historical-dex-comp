CREATE TABLE IF NOT EXISTS schema_migrations (
  version INTEGER PRIMARY KEY,
  name VARCHAR NOT NULL,
  checksum VARCHAR NOT NULL,
  applied_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS ingest_runs (
  run_id VARCHAR PRIMARY KEY,
  provider_key VARCHAR NOT NULL,
  started_at TIMESTAMPTZ NOT NULL,
  ended_at TIMESTAMPTZ,
  status VARCHAR NOT NULL,
  from_ts TIMESTAMPTZ,
  to_ts TIMESTAMPTZ,
  records_fetched BIGINT NOT NULL DEFAULT 0,
  records_normalized BIGINT NOT NULL DEFAULT 0,
  records_upserted BIGINT NOT NULL DEFAULT 0,
  error_message VARCHAR
);

CREATE TABLE IF NOT EXISTS ingest_checkpoints (
  stream_key VARCHAR PRIMARY KEY,
  sync_scope VARCHAR NOT NULL DEFAULT 'swaps',
  newest_cursor VARCHAR,
  newest_event_at TIMESTAMPTZ,
  newest_provider_record_id VARCHAR,
  oldest_cursor VARCHAR,
  oldest_event_at TIMESTAMPTZ,
  oldest_provider_record_id VARCHAR,
  updated_at TIMESTAMPTZ NOT NULL,
  run_id VARCHAR
);

CREATE TABLE IF NOT EXISTS swaps_core (
  normalized_id VARCHAR PRIMARY KEY,
  provider_key VARCHAR NOT NULL,
  provider_record_id VARCHAR NOT NULL,
  provider_parent_id VARCHAR,
  record_granularity VARCHAR NOT NULL,
  status_canonical VARCHAR NOT NULL,
  status_raw VARCHAR,
  failure_reason_raw VARCHAR,
  created_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ,
  event_at TIMESTAMPTZ,
  source_chain_canonical VARCHAR,
  destination_chain_canonical VARCHAR,
  source_chain_raw VARCHAR,
  destination_chain_raw VARCHAR,
  source_chain_id_raw VARCHAR,
  destination_chain_id_raw VARCHAR,
  source_asset_id VARCHAR,
  destination_asset_id VARCHAR,
  source_asset_symbol VARCHAR,
  destination_asset_symbol VARCHAR,
  source_asset_decimals INTEGER,
  destination_asset_decimals INTEGER,
  amount_in_atomic VARCHAR,
  amount_out_atomic VARCHAR,
  amount_in_normalized DOUBLE,
  amount_out_normalized DOUBLE,
  amount_in_usd DOUBLE,
  amount_out_usd DOUBLE,
  fee_atomic VARCHAR,
  fee_normalized DOUBLE,
  fee_usd DOUBLE,
  slippage_bps DOUBLE,
  solver_id VARCHAR,
  route_hint VARCHAR,
  source_tx_hash VARCHAR,
  destination_tx_hash VARCHAR,
  refund_tx_hash VARCHAR,
  extra_tx_hashes JSON,
  is_final BOOLEAN,
  raw_hash_latest VARCHAR NOT NULL,
  source_endpoint VARCHAR NOT NULL,
  ingested_at TIMESTAMPTZ NOT NULL,
  run_id VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS swaps_raw (
  normalized_id VARCHAR NOT NULL,
  raw_hash VARCHAR NOT NULL,
  raw_json JSON NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL,
  source_endpoint VARCHAR NOT NULL,
  source_cursor VARCHAR,
  run_id VARCHAR NOT NULL,
  PRIMARY KEY (normalized_id, raw_hash)
);

CREATE INDEX IF NOT EXISTS idx_swaps_core_event_at ON swaps_core (event_at);
CREATE INDEX IF NOT EXISTS idx_swaps_core_provider_record_id ON swaps_core (provider_record_id);
CREATE INDEX IF NOT EXISTS idx_swaps_raw_observed_at ON swaps_raw (observed_at);
