use crate::production_graph::validate_production_identity_for_current_target;
use crate::ContentStoreResourceProfileKind;
use serde::Serialize;
use skein::{
    IoConcurrencyBudget, ProductionEvidenceBinding, ProductionQualificationIdentity,
    RuntimeGovernor, RuntimeGovernorConfig, RuntimeResourceSnapshot, SkeinError,
    StorageDeviceProfile,
};
use std::path::PathBuf;

pub const PRODUCTION_CONTENT_STORE_MEMORY_QUALIFICATION_PROTOCOL: &str =
    "skein-production-content-store-memory-qualification-v1";
pub const CONTENT_STORE_SHARED_HOST_8_GIB_BYTES: u64 = 8 * 1024 * 1024 * 1024;
pub const CONTENT_STORE_SHARED_HOST_NOMINAL_AVAILABLE_BYTES: u64 = 4 * 1024 * 1024 * 1024;
pub const CONTENT_STORE_SHARED_HOST_NOMINAL_MIN_BUDGET_BYTES: u64 = 1024 * 1024 * 1024;
pub const CONTENT_STORE_SHARED_HOST_MAX_CAPACITY_BYTES: u64 = 2 * 1024 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContentStoreMemoryProfileQualificationReport {
    pub profile_kind: ContentStoreResourceProfileKind,
    pub ready: bool,
    pub blocker_codes: Vec<String>,
    pub required_effective_limit_bytes: Option<u64>,
    pub configured_memory_ceiling_bytes: Option<u64>,
    pub nominal_available_threshold_bytes: Option<u64>,
    pub nominal_budget_range_observed: bool,
    pub observed_effective_limit_bytes: Option<u64>,
    pub observed_effective_available_bytes: Option<u64>,
    pub memory_fraction_per_million: u32,
    pub memory_capacity_bytes: u64,
    pub memory_budget_bytes: u64,
    pub expected_capacity_bytes: u64,
    pub expected_dynamic_budget_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductionContentStoreMemoryQualificationConfig {
    pub evidence_binding: ProductionEvidenceBinding,
    pub expected_identity: ProductionQualificationIdentity,
    pub storage_path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductionContentStoreMemoryQualificationReport {
    pub ready: bool,
    pub blocker_codes: Vec<String>,
    pub evidence_binding: ProductionEvidenceBinding,
    pub shared_host_8_gib: ContentStoreMemoryProfileQualificationReport,
    pub capability_512_mib: ContentStoreMemoryProfileQualificationReport,
}

impl ProductionContentStoreMemoryQualificationReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": PRODUCTION_CONTENT_STORE_MEMORY_QUALIFICATION_PROTOCOL,
            "evidence_kind": "production_content_store_memory_profiles",
            "production_eligible": true,
            "ready": self.ready,
            "blocker_codes": self.blocker_codes,
            "evidence_binding": self.evidence_binding.json(),
            "shared_host_8_gib": self.shared_host_8_gib,
            "capability_512_mib": self.capability_512_mib,
        })
    }
}

impl ContentStoreMemoryProfileQualificationReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("content-store memory profile report is serializable")
    }
}

/// Evaluates the detected host or cgroup snapshot against one fixed Content
/// Store memory profile. This qualifies the governor policy only; the 512 MiB
/// workload capability additionally requires a run with the explicit governor
/// ceiling and measured peak RSS inside that envelope.
pub fn qualify_content_store_memory_profile(
    profile_kind: ContentStoreResourceProfileKind,
    resources: RuntimeResourceSnapshot,
    storage_io: IoConcurrencyBudget,
) -> Result<ContentStoreMemoryProfileQualificationReport, SkeinError> {
    let (
        required_effective_limit_bytes,
        configured_memory_ceiling_bytes,
        nominal_available_threshold_bytes,
    ) = match profile_kind {
        ContentStoreResourceProfileKind::SharedHost8Gib => (
            Some(CONTENT_STORE_SHARED_HOST_8_GIB_BYTES),
            None,
            Some(CONTENT_STORE_SHARED_HOST_NOMINAL_AVAILABLE_BYTES),
        ),
        ContentStoreResourceProfileKind::Capability512Mib => (
            None,
            Some(crate::CONTENT_STORE_512_MIB_CAPABILITY_BYTES),
            None,
        ),
        ContentStoreResourceProfileKind::ConfiguredWorkload => {
            return Err(SkeinError::Semantic(
                "fixed Content Store memory qualification requires the shared-host 8 GiB or 512 MiB capability profile"
                    .to_string(),
            ));
        }
    };

    let mut governor_config = RuntimeGovernorConfig::shared_host();
    governor_config.memory_budget_bytes = configured_memory_ceiling_bytes;
    let governor = RuntimeGovernor::new(governor_config, resources, storage_io);
    let limits = governor.snapshot().limits;
    let observed_limit = resources.memory.effective_limit_bytes;
    let observed_available = resources.memory.effective_available_bytes;
    let limit_derived_capacity = observed_limit
        .map(|limit| scale_memory(limit, governor_config.memory_fraction_per_million));
    let expected_capacity_bytes = [
        configured_memory_ceiling_bytes,
        limit_derived_capacity,
        limit_derived_capacity
            .is_none()
            .then_some(governor_config.fallback_memory_budget_bytes),
    ]
    .into_iter()
    .flatten()
    .min()
    .unwrap_or_default();
    let expected_dynamic_budget_bytes = observed_available.map(|available| {
        scale_memory(available, governor_config.memory_fraction_per_million)
            .min(expected_capacity_bytes)
    });

    let mut blocker_codes = Vec::new();
    if required_effective_limit_bytes.is_some_and(|required| observed_limit != Some(required)) {
        blocker_codes.push("content_store_memory_effective_limit_mismatch".to_string());
    }
    if observed_available.is_none() {
        blocker_codes.push("content_store_memory_available_headroom_unavailable".to_string());
    }
    if limits.memory_capacity_bytes != expected_capacity_bytes {
        blocker_codes.push("content_store_memory_capacity_policy_mismatch".to_string());
    }
    if expected_dynamic_budget_bytes != Some(limits.memory_budget_bytes) {
        blocker_codes.push("content_store_memory_dynamic_budget_policy_mismatch".to_string());
    }
    if limits.memory_budget_bytes > limits.memory_capacity_bytes {
        blocker_codes.push("content_store_memory_budget_exceeds_capacity".to_string());
    }

    let nominal_budget_range_observed = match profile_kind {
        ContentStoreResourceProfileKind::SharedHost8Gib => {
            if limits.memory_capacity_bytes > CONTENT_STORE_SHARED_HOST_MAX_CAPACITY_BYTES {
                blocker_codes
                    .push("content_store_shared_host_8_gib_capacity_exceeds_2_gib".to_string());
            }
            (CONTENT_STORE_SHARED_HOST_NOMINAL_MIN_BUDGET_BYTES
                ..=CONTENT_STORE_SHARED_HOST_MAX_CAPACITY_BYTES)
                .contains(&limits.memory_budget_bytes)
        }
        ContentStoreResourceProfileKind::Capability512Mib => {
            if limits.memory_capacity_bytes != crate::CONTENT_STORE_512_MIB_CAPABILITY_BYTES {
                blocker_codes
                    .push("content_store_512_mib_configured_capacity_not_effective".to_string());
            }
            if limits.memory_budget_bytes == 0 {
                blocker_codes.push("content_store_512_mib_budget_is_zero".to_string());
            }
            false
        }
        ContentStoreResourceProfileKind::ConfiguredWorkload => {
            unreachable!("configured workloads return before fixed-profile evaluation")
        }
    };

    blocker_codes.sort();
    blocker_codes.dedup();
    Ok(ContentStoreMemoryProfileQualificationReport {
        profile_kind,
        ready: blocker_codes.is_empty(),
        blocker_codes,
        required_effective_limit_bytes,
        configured_memory_ceiling_bytes,
        nominal_available_threshold_bytes,
        nominal_budget_range_observed,
        observed_effective_limit_bytes: observed_limit,
        observed_effective_available_bytes: observed_available,
        memory_fraction_per_million: governor_config.memory_fraction_per_million,
        memory_capacity_bytes: limits.memory_capacity_bytes,
        memory_budget_bytes: limits.memory_budget_bytes,
        expected_capacity_bytes,
        expected_dynamic_budget_bytes,
    })
}

/// Binds both fixed memory-policy evaluations to one exact release identity.
/// This proves policy derivation only; representative shared-host and 512 MiB
/// workload reports remain separate release obligations.
pub fn run_production_content_store_memory_qualification(
    config: ProductionContentStoreMemoryQualificationConfig,
) -> Result<ProductionContentStoreMemoryQualificationReport, SkeinError> {
    if !config.storage_path.exists() {
        return Err(SkeinError::Semantic(
            "production Content Store memory qualification requires an existing storage path"
                .to_string(),
        ));
    }
    validate_production_identity_for_current_target(
        &config.evidence_binding,
        &config.expected_identity,
    )
    .map_err(|error| SkeinError::Semantic(error.to_string()))?;
    let storage_io = IoConcurrencyBudget::shared_host_for_device(StorageDeviceProfile::detect(
        &config.storage_path,
    ));
    evaluate_production_content_store_memory_qualification(
        config,
        RuntimeResourceSnapshot::detect(),
        storage_io,
    )
}

fn evaluate_production_content_store_memory_qualification(
    config: ProductionContentStoreMemoryQualificationConfig,
    resources: RuntimeResourceSnapshot,
    storage_io: IoConcurrencyBudget,
) -> Result<ProductionContentStoreMemoryQualificationReport, SkeinError> {
    let shared_host_8_gib = qualify_content_store_memory_profile(
        ContentStoreResourceProfileKind::SharedHost8Gib,
        resources,
        storage_io,
    )?;
    let capability_512_mib = qualify_content_store_memory_profile(
        ContentStoreResourceProfileKind::Capability512Mib,
        resources,
        storage_io,
    )?;
    let mut blocker_codes = Vec::new();
    blocker_codes.extend(
        shared_host_8_gib
            .blocker_codes
            .iter()
            .map(|blocker| format!("shared_host_8_gib_{blocker}")),
    );
    blocker_codes.extend(
        capability_512_mib
            .blocker_codes
            .iter()
            .map(|blocker| format!("capability_512_mib_{blocker}")),
    );
    blocker_codes.sort();
    blocker_codes.dedup();
    Ok(ProductionContentStoreMemoryQualificationReport {
        ready: blocker_codes.is_empty(),
        blocker_codes,
        evidence_binding: config.evidence_binding,
        shared_host_8_gib,
        capability_512_mib,
    })
}

fn scale_memory(bytes: u64, fraction_per_million: u32) -> u64 {
    (u128::from(bytes) * u128::from(fraction_per_million) / 1_000_000).min(u128::from(u64::MAX))
        as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use skein::{
        RuntimeMemorySnapshot, RuntimeResourceBudget, PRODUCTION_QUALIFICATION_POLICY_VERSION,
    };
    use std::num::NonZeroUsize;

    fn resources(limit: u64, available: u64) -> RuntimeResourceSnapshot {
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::new(8).unwrap(), None, None),
            RuntimeMemorySnapshot::from_limits(Some(limit), Some(available), None, None, None),
        )
    }

    fn cgroup_resources(limit: u64, current: u64) -> RuntimeResourceSnapshot {
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::new(32).unwrap(), None, None),
            RuntimeMemorySnapshot::from_limits(
                Some(64 * 1024 * 1024 * 1024),
                Some(48 * 1024 * 1024 * 1024),
                Some(limit),
                None,
                Some(current),
            ),
        )
    }

    fn storage_io() -> IoConcurrencyBudget {
        IoConcurrencyBudget {
            foreground_depth: NonZeroUsize::new(4).unwrap(),
            background_depth: NonZeroUsize::new(1).unwrap(),
        }
    }

    fn identity() -> ProductionQualificationIdentity {
        ProductionQualificationIdentity {
            source_revision: "revision".to_string(),
            rust_toolchain: "rustc".to_string(),
            target_os: std::env::consts::OS.to_string(),
            target_arch: std::env::consts::ARCH.to_string(),
            enabled_features: Vec::new(),
            durable_format_version: 1,
            schema_version: 1,
            configuration_digest: "configuration".to_string(),
            deployment_profile: "shared-host".to_string(),
            dataset_fingerprint: "dataset".to_string(),
            canonical_graph_commit_epoch: 1,
            policy_version: PRODUCTION_QUALIFICATION_POLICY_VERSION,
        }
    }

    fn production_config() -> ProductionContentStoreMemoryQualificationConfig {
        let expected_identity = identity();
        ProductionContentStoreMemoryQualificationConfig {
            evidence_binding: ProductionEvidenceBinding {
                identity: expected_identity.clone(),
                generated_at_unix_seconds: 1,
            },
            expected_identity,
            storage_path: std::env::temp_dir(),
        }
    }

    #[test]
    fn shared_host_8_gib_profile_derives_headroom_bounded_budget() {
        let report = qualify_content_store_memory_profile(
            ContentStoreResourceProfileKind::SharedHost8Gib,
            resources(
                CONTENT_STORE_SHARED_HOST_8_GIB_BYTES,
                6 * 1024 * 1024 * 1024,
            ),
            storage_io(),
        )
        .expect("fixed shared-host profile should evaluate");

        assert!(
            report.ready,
            "unexpected blockers: {:?}",
            report.blocker_codes
        );
        assert_eq!(
            report.memory_capacity_bytes,
            CONTENT_STORE_SHARED_HOST_MAX_CAPACITY_BYTES
        );
        assert_eq!(report.memory_budget_bytes, 1536 * 1024 * 1024);
        assert!(report.nominal_budget_range_observed);
    }

    #[test]
    fn shared_host_profile_uses_effective_cgroup_limit_and_headroom() {
        let report = qualify_content_store_memory_profile(
            ContentStoreResourceProfileKind::SharedHost8Gib,
            cgroup_resources(
                CONTENT_STORE_SHARED_HOST_8_GIB_BYTES,
                2 * 1024 * 1024 * 1024,
            ),
            storage_io(),
        )
        .expect("cgroup-bounded shared-host profile should evaluate");

        assert!(
            report.ready,
            "unexpected blockers: {:?}",
            report.blocker_codes
        );
        assert_eq!(report.memory_capacity_bytes, 2 * 1024 * 1024 * 1024);
        assert_eq!(report.memory_budget_bytes, 1536 * 1024 * 1024);
    }

    #[test]
    fn shared_host_8_gib_profile_allows_budget_below_nominal_range_under_pressure() {
        let report = qualify_content_store_memory_profile(
            ContentStoreResourceProfileKind::SharedHost8Gib,
            resources(
                CONTENT_STORE_SHARED_HOST_8_GIB_BYTES,
                3 * 1024 * 1024 * 1024,
            ),
            storage_io(),
        )
        .expect("fixed shared-host profile should evaluate");

        assert!(
            report.ready,
            "unexpected blockers: {:?}",
            report.blocker_codes
        );
        assert_eq!(report.memory_budget_bytes, 768 * 1024 * 1024);
        assert!(!report.nominal_budget_range_observed);
    }

    #[test]
    fn constrained_512_mib_profile_preserves_host_headroom() {
        let report = qualify_content_store_memory_profile(
            ContentStoreResourceProfileKind::Capability512Mib,
            resources(
                CONTENT_STORE_SHARED_HOST_8_GIB_BYTES,
                6 * 1024 * 1024 * 1024,
            ),
            storage_io(),
        )
        .expect("fixed low-memory profile should evaluate");

        assert!(
            report.ready,
            "unexpected blockers: {:?}",
            report.blocker_codes
        );
        assert_eq!(
            report.memory_capacity_bytes,
            crate::CONTENT_STORE_512_MIB_CAPABILITY_BYTES
        );
        assert_eq!(
            report.memory_budget_bytes,
            crate::CONTENT_STORE_512_MIB_CAPABILITY_BYTES
        );
    }

    #[test]
    fn profile_identity_cannot_be_claimed_on_a_different_limit() {
        let report = qualify_content_store_memory_profile(
            ContentStoreResourceProfileKind::SharedHost8Gib,
            resources(16 * 1024 * 1024 * 1024, 12 * 1024 * 1024 * 1024),
            storage_io(),
        )
        .expect("fixed low-memory profile should evaluate");

        assert!(!report.ready);
        assert!(report
            .blocker_codes
            .iter()
            .any(|blocker| blocker.contains("effective_limit_mismatch")));
    }

    #[test]
    fn constrained_host_can_reduce_the_explicit_512_mib_capacity() {
        let report = qualify_content_store_memory_profile(
            ContentStoreResourceProfileKind::Capability512Mib,
            resources(
                crate::CONTENT_STORE_512_MIB_CAPABILITY_BYTES,
                384 * 1024 * 1024,
            ),
            storage_io(),
        )
        .expect("fixed low-memory profile should evaluate");

        assert!(!report.ready);
        assert_eq!(report.memory_capacity_bytes, 128 * 1024 * 1024);
        assert!(report
            .blocker_codes
            .iter()
            .any(|blocker| blocker.contains("configured_capacity_not_effective")));
    }

    #[test]
    fn production_matrix_binds_dynamic_shared_host_and_explicit_capability_profiles() {
        let report = evaluate_production_content_store_memory_qualification(
            production_config(),
            resources(
                CONTENT_STORE_SHARED_HOST_8_GIB_BYTES,
                6 * 1024 * 1024 * 1024,
            ),
            storage_io(),
        )
        .expect("production memory profiles should evaluate");

        assert!(
            report.ready,
            "unexpected blockers: {:?}",
            report.blocker_codes
        );
        assert_eq!(
            report.shared_host_8_gib.memory_budget_bytes,
            1536 * 1024 * 1024
        );
        assert_eq!(
            report.capability_512_mib.memory_budget_bytes,
            crate::CONTENT_STORE_512_MIB_CAPABILITY_BYTES
        );
        assert_eq!(
            report.json()["protocol"],
            PRODUCTION_CONTENT_STORE_MEMORY_QUALIFICATION_PROTOCOL
        );
    }

    #[test]
    fn production_matrix_accepts_shared_host_budget_below_nominal_range() {
        let report = evaluate_production_content_store_memory_qualification(
            production_config(),
            resources(
                CONTENT_STORE_SHARED_HOST_8_GIB_BYTES,
                3 * 1024 * 1024 * 1024,
            ),
            storage_io(),
        )
        .expect("pressured shared-host memory profiles should evaluate");

        assert!(
            report.ready,
            "unexpected blockers: {:?}",
            report.blocker_codes
        );
        assert_eq!(
            report.shared_host_8_gib.memory_budget_bytes,
            768 * 1024 * 1024
        );
        assert!(!report.shared_host_8_gib.nominal_budget_range_observed);
    }

    #[test]
    fn production_matrix_reports_wrong_shared_host_limit_and_rejects_stale_identity() {
        let report = evaluate_production_content_store_memory_qualification(
            production_config(),
            resources(16 * 1024 * 1024 * 1024, 12 * 1024 * 1024 * 1024),
            storage_io(),
        )
        .expect("wrong shared-host limit should produce bounded evidence");
        assert!(!report.ready);
        assert!(report
            .blocker_codes
            .iter()
            .any(|blocker| blocker.contains("effective_limit_mismatch")));

        let mut config = production_config();
        config.expected_identity.source_revision = "different".to_string();
        assert!(run_production_content_store_memory_qualification(config).is_err());
    }
}
