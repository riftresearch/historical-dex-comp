import type { ProviderKey } from "../../domain/provider-key";
import {
  runProviderIntegrityAudit,
  type IntegrityAuditRunReport,
  type ProviderIntegrityAuditInput,
} from "../../inspect/provider-integrity-sampler";
import { healProviders, type HealProvidersResult } from "../../storage/heal";
import { repairProviders, type RepairProvidersResult } from "../../storage/repair";

export interface MaintenanceService {
  repair(scope: ProviderKey | "all"): Promise<RepairProvidersResult>;
  heal(scope: ProviderKey | "all"): Promise<HealProvidersResult>;
  integrity(input: ProviderIntegrityAuditInput): Promise<IntegrityAuditRunReport>;
}

class DefaultMaintenanceService implements MaintenanceService {
  async repair(scope: ProviderKey | "all"): Promise<RepairProvidersResult> {
    return repairProviders(scope);
  }

  async heal(scope: ProviderKey | "all"): Promise<HealProvidersResult> {
    return healProviders(scope);
  }

  async integrity(input: ProviderIntegrityAuditInput): Promise<IntegrityAuditRunReport> {
    return runProviderIntegrityAudit(input);
  }
}

export const maintenanceService: MaintenanceService = new DefaultMaintenanceService();

