export const CANONICAL_STATUSES = [
  "pending",
  "processing",
  "success",
  "refunded",
  "failed",
  "expired",
  "unknown",
] as const;

export type CanonicalStatus = (typeof CANONICAL_STATUSES)[number];

const RELAY_STATUS_TO_CANONICAL: Record<string, CanonicalStatus> = {
  waiting: "pending",
  pending: "processing",
  success: "success",
  failure: "failed",
  refund: "refunded",
};

const LIFI_STATUS_TO_CANONICAL: Record<string, CanonicalStatus> = {
  done: "success",
  completed: "success",
  refunded: "refunded",
  failed: "failed",
  invalid: "failed",
  expired: "expired",
  pending: "pending",
  waiting: "pending",
  processing: "processing",
  started: "processing",
};

const THORCHAIN_STATUS_TO_CANONICAL: Record<string, CanonicalStatus> = {
  pending: "pending",
  success: "success",
  failed: "failed",
};

const GARDEN_STATUS_TO_CANONICAL: Record<string, CanonicalStatus> = {
  "not-initiated": "pending",
  "in-progress": "processing",
  completed: "success",
  refunded: "refunded",
  expired: "expired",
};

const NEAR_INTENTS_STATUS_TO_CANONICAL: Record<string, CanonicalStatus> = {
  failed: "failed",
  incomplete_deposit: "failed",
  pending_deposit: "pending",
  processing: "processing",
  refunded: "refunded",
  success: "success",
};

export function mapRelayStatus(rawStatus: string | undefined): CanonicalStatus {
  if (!rawStatus) {
    return "unknown";
  }

  return RELAY_STATUS_TO_CANONICAL[rawStatus.toLowerCase()] ?? "unknown";
}

export function mapLifiStatus(
  rawStatus: string | null | undefined,
  rawSubstatus: string | null | undefined,
): CanonicalStatus {
  const substatus = rawSubstatus?.trim().toLowerCase();
  if (substatus) {
    const bySubstatus = LIFI_STATUS_TO_CANONICAL[substatus];
    if (bySubstatus) {
      return bySubstatus;
    }
  }

  const status = rawStatus?.trim().toLowerCase();
  if (!status) {
    return "unknown";
  }
  return LIFI_STATUS_TO_CANONICAL[status] ?? "unknown";
}

export function mapThorchainStatus(rawStatus: string | undefined): CanonicalStatus {
  if (!rawStatus) {
    return "unknown";
  }

  return THORCHAIN_STATUS_TO_CANONICAL[rawStatus.toLowerCase()] ?? "unknown";
}

export function mapGardenStatus(rawStatus: string | undefined): CanonicalStatus {
  if (!rawStatus) {
    return "unknown";
  }

  return GARDEN_STATUS_TO_CANONICAL[rawStatus.toLowerCase()] ?? "unknown";
}

export function mapNearIntentsStatus(rawStatus: string | undefined): CanonicalStatus {
  if (!rawStatus) {
    return "unknown";
  }

  return NEAR_INTENTS_STATUS_TO_CANONICAL[rawStatus.toLowerCase()] ?? "unknown";
}
