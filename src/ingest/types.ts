export const INGEST_MODES = ["bootstrap", "sync_newer", "backfill_older"] as const;

export type IngestMode = (typeof INGEST_MODES)[number];

export function isIngestMode(value: string): value is IngestMode {
  return (INGEST_MODES as readonly string[]).includes(value);
}
