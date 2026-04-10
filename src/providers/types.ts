import type { DuckDBConnection } from "@duckdb/node-api";
import type { ProviderKey } from "../domain/provider-key";
import type { IngestMode } from "../ingest/types";
import type { IngestCheckpoint } from "../storage/repositories/ingest-checkpoints";

export interface ProviderIngestInput {
  connection: DuckDBConnection;
  runId: string;
  mode: IngestMode;
  checkpoint: IngestCheckpoint | null;
  now: Date;
  pageSize: number;
  maxPages: number | null;
  progress?: ProviderProgressLogger;
}

export interface ProviderIngestOutput {
  recordsFetched: number;
  recordsNormalized: number;
  recordsUpserted: number;
  checkpoint: IngestCheckpoint | null;
}

export interface ProviderAdapter {
  key: ProviderKey;
  streamKey: string;
  sourceEndpoint: string;
  ingest(input: ProviderIngestInput): Promise<ProviderIngestOutput>;
}

export type ProviderProgressValue = string | number | boolean | null | undefined;

export interface ProviderProgressEvent {
  message: string;
  fields?: Record<string, ProviderProgressValue>;
}

export type ProviderProgressLogger = (event: ProviderProgressEvent) => void;
