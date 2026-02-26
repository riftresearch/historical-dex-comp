export const COVERAGE_FLOOR_ISO = "2026-01-01T00:00:00.000Z";
export const COVERAGE_FLOOR = new Date(COVERAGE_FLOOR_ISO);

export interface CoverageFloorComparable {
  event_at?: Date | null;
  created_at?: Date | null;
  updated_at?: Date | null;
}

function effectiveTimestamp(input: CoverageFloorComparable): Date | null {
  return input.event_at ?? input.created_at ?? input.updated_at ?? null;
}

export function isAtOrAfterCoverageFloor(input: CoverageFloorComparable): boolean {
  const timestamp = effectiveTimestamp(input);
  if (!timestamp) {
    return true;
  }
  return timestamp.getTime() >= COVERAGE_FLOOR.getTime();
}

