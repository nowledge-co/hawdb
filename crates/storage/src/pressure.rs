// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::path::Path;

pub const STORAGE_PRESSURE_SOFT_RATIO_PER_MILLION: u32 = 700_000;
pub const STORAGE_PRESSURE_DEFER_RATIO_PER_MILLION: u32 = 900_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoragePressureState {
    Healthy,
    SpeedUpMaintenance,
    /// Reject this attempt before mutation; the caller may retry after maintenance.
    /// This state does not queue work or sleep in the writer path.
    DeferMutation,
    StopMutation,
    RecoveryOnly,
}

impl StoragePressureState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::SpeedUpMaintenance => "speed_up_maintenance",
            Self::DeferMutation => "defer_mutation",
            Self::StopMutation => "stop_mutation",
            Self::RecoveryOnly => "recovery_only",
        }
    }

    pub const fn admits_mutation(self) -> bool {
        matches!(self, Self::Healthy | Self::SpeedUpMaintenance)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoragePressureReasonCode {
    IntegrityPoisoned,
    WalHardLimit,
    DeltaHardLimit,
    FreeSpaceReserve,
    WalDeferThreshold,
    DeltaDeferThreshold,
    WalSoftThreshold,
    DeltaSoftThreshold,
    AdjacencyDebt,
    ProjectionDebt,
    GenerationReclamationDebt,
    ReaderPinnedObsoleteGenerations,
    CachePinnedPressure,
}

impl StoragePressureReasonCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::IntegrityPoisoned => "integrity_poisoned",
            Self::WalHardLimit => "wal_hard_limit",
            Self::DeltaHardLimit => "delta_hard_limit",
            Self::FreeSpaceReserve => "free_space_reserve",
            Self::WalDeferThreshold => "wal_defer_threshold",
            Self::DeltaDeferThreshold => "delta_defer_threshold",
            Self::WalSoftThreshold => "wal_soft_threshold",
            Self::DeltaSoftThreshold => "delta_soft_threshold",
            Self::AdjacencyDebt => "adjacency_debt",
            Self::ProjectionDebt => "projection_debt",
            Self::GenerationReclamationDebt => "generation_reclamation_debt",
            Self::ReaderPinnedObsoleteGenerations => "reader_pinned_obsolete_generations",
            Self::CachePinnedPressure => "cache_pinned_pressure",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StoragePressureSignals {
    pub current_commit_epoch: u64,
    pub checkpoint_commit_epoch: u64,
    pub wal_bytes: u64,
    pub wal_age_millis: u64,
    pub max_wal_bytes: Option<u64>,
    pub delta_bytes: u64,
    pub max_delta_bytes: Option<u64>,
    pub adjacency_debt_entries: usize,
    pub projection_debt_operations: usize,
    pub generation_reclamation_retry_required: bool,
    pub generation_reclamation_pending_files: usize,
    pub generation_reclamation_pending_bytes: u64,
    pub oldest_reader_commit_epoch: Option<u64>,
    pub obsolete_generation_bytes: u64,
    pub estimated_checkpoint_temporary_bytes: u64,
    pub available_free_space_bytes: Option<u64>,
    pub cache_capacity_bytes: u64,
    pub cache_resident_bytes: u64,
    pub cache_pinned_bytes: u64,
    pub integrity_poisoned: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoragePressureSnapshot {
    pub state: StoragePressureState,
    pub current_commit_epoch: u64,
    pub checkpoint_commit_epoch: u64,
    pub wal_bytes: u64,
    pub wal_age_millis: u64,
    pub max_wal_bytes: Option<u64>,
    pub wal_pressure_ratio_per_million: Option<u32>,
    pub delta_bytes: u64,
    pub max_delta_bytes: Option<u64>,
    pub delta_pressure_ratio_per_million: Option<u32>,
    pub adjacency_debt_entries: usize,
    pub projection_debt_operations: usize,
    pub generation_reclamation_retry_required: bool,
    pub generation_reclamation_pending_files: usize,
    pub generation_reclamation_pending_bytes: u64,
    pub oldest_reader_commit_epoch: Option<u64>,
    pub oldest_reader_lag: u64,
    pub obsolete_generation_bytes: u64,
    pub estimated_checkpoint_temporary_bytes: u64,
    pub available_free_space_bytes: Option<u64>,
    pub cache_capacity_bytes: u64,
    pub cache_resident_bytes: u64,
    pub cache_pinned_bytes: u64,
    pub cache_reclaimable_bytes: u64,
    pub cache_pinned_ratio_per_million: Option<u32>,
    pub reason_codes: Vec<StoragePressureReasonCode>,
}

impl StoragePressureSnapshot {
    pub fn admits_mutation(&self) -> bool {
        self.state.admits_mutation()
    }

    pub fn recommends_checkpoint(&self) -> bool {
        if self
            .reason_codes
            .contains(&StoragePressureReasonCode::FreeSpaceReserve)
        {
            return false;
        }
        self.reason_codes.iter().any(|reason| {
            matches!(
                reason,
                StoragePressureReasonCode::WalHardLimit
                    | StoragePressureReasonCode::DeltaHardLimit
                    | StoragePressureReasonCode::WalDeferThreshold
                    | StoragePressureReasonCode::DeltaDeferThreshold
                    | StoragePressureReasonCode::WalSoftThreshold
                    | StoragePressureReasonCode::DeltaSoftThreshold
                    | StoragePressureReasonCode::GenerationReclamationDebt
            )
        })
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct StorageDebtController;

impl StorageDebtController {
    pub fn evaluate(self, signals: StoragePressureSignals) -> StoragePressureSnapshot {
        let wal_ratio = pressure_ratio(signals.wal_bytes, signals.max_wal_bytes);
        let delta_ratio = pressure_ratio(signals.delta_bytes, signals.max_delta_bytes);
        let cache_pinned_ratio = pressure_ratio(
            signals.cache_pinned_bytes,
            (signals.cache_capacity_bytes > 0).then_some(signals.cache_capacity_bytes),
        );
        let oldest_reader_lag = signals.oldest_reader_commit_epoch.map_or(0, |epoch| {
            signals.current_commit_epoch.saturating_sub(epoch)
        });
        let cache_reclaimable_bytes = signals
            .cache_resident_bytes
            .saturating_sub(signals.cache_pinned_bytes);
        let mut reasons = Vec::new();

        if signals.integrity_poisoned {
            reasons.push(StoragePressureReasonCode::IntegrityPoisoned);
        }
        classify_limit(
            wal_ratio,
            StoragePressureReasonCode::WalHardLimit,
            StoragePressureReasonCode::WalDeferThreshold,
            StoragePressureReasonCode::WalSoftThreshold,
            &mut reasons,
        );
        classify_limit(
            delta_ratio,
            StoragePressureReasonCode::DeltaHardLimit,
            StoragePressureReasonCode::DeltaDeferThreshold,
            StoragePressureReasonCode::DeltaSoftThreshold,
            &mut reasons,
        );
        if signals.estimated_checkpoint_temporary_bytes > 0
            && signals
                .available_free_space_bytes
                .is_some_and(|available| available < signals.estimated_checkpoint_temporary_bytes)
        {
            reasons.push(StoragePressureReasonCode::FreeSpaceReserve);
        }
        if signals.adjacency_debt_entries > 0 {
            reasons.push(StoragePressureReasonCode::AdjacencyDebt);
        }
        if signals.projection_debt_operations > 0 {
            reasons.push(StoragePressureReasonCode::ProjectionDebt);
        }
        if signals.generation_reclamation_retry_required {
            reasons.push(StoragePressureReasonCode::GenerationReclamationDebt);
        }
        if oldest_reader_lag > 0 && signals.obsolete_generation_bytes > 0 {
            reasons.push(StoragePressureReasonCode::ReaderPinnedObsoleteGenerations);
        }
        if cache_pinned_ratio.is_some_and(|ratio| ratio >= STORAGE_PRESSURE_DEFER_RATIO_PER_MILLION)
        {
            reasons.push(StoragePressureReasonCode::CachePinnedPressure);
        }

        let state = if reasons.contains(&StoragePressureReasonCode::IntegrityPoisoned) {
            StoragePressureState::RecoveryOnly
        } else if reasons.iter().any(|reason| {
            matches!(
                reason,
                StoragePressureReasonCode::WalHardLimit
                    | StoragePressureReasonCode::DeltaHardLimit
                    | StoragePressureReasonCode::FreeSpaceReserve
            )
        }) {
            StoragePressureState::StopMutation
        } else if reasons.iter().any(|reason| {
            matches!(
                reason,
                StoragePressureReasonCode::WalDeferThreshold
                    | StoragePressureReasonCode::DeltaDeferThreshold
            )
        }) {
            StoragePressureState::DeferMutation
        } else if reasons.is_empty() {
            StoragePressureState::Healthy
        } else {
            StoragePressureState::SpeedUpMaintenance
        };

        StoragePressureSnapshot {
            state,
            current_commit_epoch: signals.current_commit_epoch,
            checkpoint_commit_epoch: signals.checkpoint_commit_epoch,
            wal_bytes: signals.wal_bytes,
            wal_age_millis: signals.wal_age_millis,
            max_wal_bytes: signals.max_wal_bytes,
            wal_pressure_ratio_per_million: wal_ratio,
            delta_bytes: signals.delta_bytes,
            max_delta_bytes: signals.max_delta_bytes,
            delta_pressure_ratio_per_million: delta_ratio,
            adjacency_debt_entries: signals.adjacency_debt_entries,
            projection_debt_operations: signals.projection_debt_operations,
            generation_reclamation_retry_required: signals.generation_reclamation_retry_required,
            generation_reclamation_pending_files: signals.generation_reclamation_pending_files,
            generation_reclamation_pending_bytes: signals.generation_reclamation_pending_bytes,
            oldest_reader_commit_epoch: signals.oldest_reader_commit_epoch,
            oldest_reader_lag,
            obsolete_generation_bytes: signals.obsolete_generation_bytes,
            estimated_checkpoint_temporary_bytes: signals.estimated_checkpoint_temporary_bytes,
            available_free_space_bytes: signals.available_free_space_bytes,
            cache_capacity_bytes: signals.cache_capacity_bytes,
            cache_resident_bytes: signals.cache_resident_bytes,
            cache_pinned_bytes: signals.cache_pinned_bytes,
            cache_reclaimable_bytes,
            cache_pinned_ratio_per_million: cache_pinned_ratio,
            reason_codes: reasons,
        }
    }
}

fn pressure_ratio(value: u64, limit: Option<u64>) -> Option<u32> {
    let limit = limit?;
    if limit == 0 {
        return Some(if value == 0 { 0 } else { u32::MAX });
    }
    let ratio = (u128::from(value) * 1_000_000) / u128::from(limit);
    Some(ratio.min(u128::from(u32::MAX)) as u32)
}

pub fn available_storage_space(path: impl AsRef<Path>) -> Option<u64> {
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    {
        fs2::available_space(path.as_ref()).ok()
    }
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    {
        let _ = path;
        None
    }
}

fn classify_limit(
    ratio: Option<u32>,
    hard: StoragePressureReasonCode,
    defer: StoragePressureReasonCode,
    soft: StoragePressureReasonCode,
    reasons: &mut Vec<StoragePressureReasonCode>,
) {
    match ratio {
        Some(ratio) if ratio >= 1_000_000 => reasons.push(hard),
        Some(ratio) if ratio >= STORAGE_PRESSURE_DEFER_RATIO_PER_MILLION => reasons.push(defer),
        Some(ratio) if ratio >= STORAGE_PRESSURE_SOFT_RATIO_PER_MILLION => reasons.push(soft),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pressure_escalates_from_soft_debt_to_mutation_stop() {
        let controller = StorageDebtController;
        let soft = controller.evaluate(StoragePressureSignals {
            wal_bytes: 70,
            max_wal_bytes: Some(100),
            ..StoragePressureSignals::default()
        });
        assert_eq!(soft.state, StoragePressureState::SpeedUpMaintenance);
        assert!(soft.recommends_checkpoint());

        let defer = controller.evaluate(StoragePressureSignals {
            wal_bytes: 90,
            max_wal_bytes: Some(100),
            ..StoragePressureSignals::default()
        });
        assert_eq!(defer.state, StoragePressureState::DeferMutation);
        assert_eq!(defer.state.as_str(), "defer_mutation");
        assert!(!defer.admits_mutation());
        assert!(defer.recommends_checkpoint());

        let stop = controller.evaluate(StoragePressureSignals {
            wal_bytes: 100,
            max_wal_bytes: Some(100),
            ..StoragePressureSignals::default()
        });
        assert_eq!(stop.state, StoragePressureState::StopMutation);
    }

    #[test]
    fn integrity_failure_overrides_resource_pressure() {
        let snapshot = StorageDebtController.evaluate(StoragePressureSignals {
            integrity_poisoned: true,
            wal_bytes: 100,
            max_wal_bytes: Some(100),
            ..StoragePressureSignals::default()
        });
        assert_eq!(snapshot.state, StoragePressureState::RecoveryOnly);
        assert_eq!(
            snapshot.reason_codes[0],
            StoragePressureReasonCode::IntegrityPoisoned
        );
    }

    #[test]
    fn pinned_cache_and_reader_debt_accelerate_maintenance_without_blocking_writes() {
        let snapshot = StorageDebtController.evaluate(StoragePressureSignals {
            current_commit_epoch: 10,
            oldest_reader_commit_epoch: Some(4),
            obsolete_generation_bytes: 1024,
            cache_capacity_bytes: 100,
            cache_resident_bytes: 90,
            cache_pinned_bytes: 90,
            ..StoragePressureSignals::default()
        });
        assert_eq!(snapshot.state, StoragePressureState::SpeedUpMaintenance);
        assert!(snapshot.admits_mutation());
        assert_eq!(snapshot.oldest_reader_lag, 6);
        assert_eq!(snapshot.cache_reclaimable_bytes, 0);
    }

    #[test]
    fn generation_reclamation_debt_accelerates_maintenance_without_blocking_writes() {
        let snapshot = StorageDebtController.evaluate(StoragePressureSignals {
            generation_reclamation_retry_required: true,
            generation_reclamation_pending_files: 2,
            generation_reclamation_pending_bytes: 4096,
            ..StoragePressureSignals::default()
        });

        assert_eq!(snapshot.state, StoragePressureState::SpeedUpMaintenance);
        assert!(snapshot.admits_mutation());
        assert!(snapshot.recommends_checkpoint());
        assert_eq!(snapshot.generation_reclamation_pending_files, 2);
        assert_eq!(snapshot.generation_reclamation_pending_bytes, 4096);
        assert!(snapshot
            .reason_codes
            .contains(&StoragePressureReasonCode::GenerationReclamationDebt));
    }

    #[test]
    fn checkpoint_reserve_fails_closed_before_disk_exhaustion() {
        let snapshot = StorageDebtController.evaluate(StoragePressureSignals {
            estimated_checkpoint_temporary_bytes: 4096,
            available_free_space_bytes: Some(2048),
            ..StoragePressureSignals::default()
        });
        assert_eq!(snapshot.state, StoragePressureState::StopMutation);
        assert!(!snapshot.recommends_checkpoint());
        assert!(snapshot
            .reason_codes
            .contains(&StoragePressureReasonCode::FreeSpaceReserve));
    }

    #[test]
    fn ratio_does_not_under_report_when_multiplication_exceeds_u64() {
        let snapshot = StorageDebtController.evaluate(StoragePressureSignals {
            wal_bytes: u64::MAX,
            max_wal_bytes: Some(u64::MAX),
            ..StoragePressureSignals::default()
        });
        assert_eq!(snapshot.wal_pressure_ratio_per_million, Some(1_000_000));
        assert_eq!(snapshot.state, StoragePressureState::StopMutation);
    }

    #[test]
    fn zero_limit_rejects_non_zero_residency() {
        let snapshot = StorageDebtController.evaluate(StoragePressureSignals {
            wal_bytes: 1,
            max_wal_bytes: Some(0),
            ..StoragePressureSignals::default()
        });
        assert_eq!(snapshot.state, StoragePressureState::StopMutation);
        assert_eq!(snapshot.wal_pressure_ratio_per_million, Some(u32::MAX));
    }
}
