use super::production_graph::{
    run_production_graph_storage_qualification, ProductionGraphQualificationError,
    ProductionGraphStorageQualificationConfig, ProductionGraphStorageQualificationReport,
};
use skein::PersistentGraphIndexClass;

pub const PRODUCTION_GRAPH_INDEX_QUALIFICATION_MATRIX_PROTOCOL: &str =
    "skein-production-graph-index-qualification-matrix-v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductionGraphIndexQualificationMatrixConfig {
    pub cases: Vec<ProductionGraphStorageQualificationConfig>,
}

impl ProductionGraphIndexQualificationMatrixConfig {
    pub(crate) fn validate(&self) -> Result<(), ProductionGraphQualificationError> {
        validate_persistent_graph_index_matrix_classes(self.cases.iter().map(|case| {
            case.persistent_index_requirement
                .as_ref()
                .map(|requirement| requirement.class)
        }))?;

        let baseline = self
            .cases
            .first()
            .expect("complete matrix validation guarantees at least one case");
        for case in &self.cases {
            case.validate()?;
            if case.open_options != baseline.open_options {
                return Err(ProductionGraphQualificationError::new(
                    "persistent graph index matrix cases must use the same replica and open configuration",
                ));
            }
            if case.runtime_governor_config != baseline.runtime_governor_config {
                return Err(ProductionGraphQualificationError::new(
                    "persistent graph index matrix cases must use the same runtime governor configuration",
                ));
            }
            if case.expected_identity != baseline.expected_identity {
                return Err(ProductionGraphQualificationError::new(
                    "persistent graph index matrix cases must bind the same production identity and canonical generation",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductionGraphIndexQualificationMatrixReport {
    pub ready: bool,
    pub blocker_codes: Vec<String>,
    pub qualified_class_count: usize,
    pub case_reports: Vec<ProductionGraphStorageQualificationReport>,
}

impl ProductionGraphIndexQualificationMatrixReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": PRODUCTION_GRAPH_INDEX_QUALIFICATION_MATRIX_PROTOCOL,
            "evidence_kind": "representative_production_replica",
            "production_eligible": true,
            "ready": self.ready,
            "blocker_codes": self.blocker_codes,
            "qualified_class_count": self.qualified_class_count,
            "required_class_count": PersistentGraphIndexClass::ALL.len(),
            "cases": self
                .case_reports
                .iter()
                .map(ProductionGraphStorageQualificationReport::json)
                .collect::<Vec<_>>(),
        })
    }
}

pub fn run_production_graph_index_qualification_matrix(
    mut config: ProductionGraphIndexQualificationMatrixConfig,
) -> Result<ProductionGraphIndexQualificationMatrixReport, ProductionGraphQualificationError> {
    config.validate()?;
    config.cases.sort_by_key(|case| {
        case.persistent_index_requirement
            .as_ref()
            .expect("validated matrix cases have an index requirement")
            .class as usize
    });

    let mut blocker_codes = Vec::new();
    let mut qualified_class_count = 0usize;
    let mut case_reports = Vec::with_capacity(config.cases.len());
    for case in config.cases {
        let class = case
            .persistent_index_requirement
            .as_ref()
            .expect("validated matrix cases have an index requirement")
            .class;
        let report = run_production_graph_storage_qualification(case)?;
        if report.ready {
            qualified_class_count = qualified_class_count.saturating_add(1);
        }
        blocker_codes.extend(report.blocker_codes.iter().map(|blocker| {
            format!(
                "persistent_graph_index_matrix_{}_{}",
                class.as_str(),
                blocker
            )
        }));
        case_reports.push(report);
    }
    blocker_codes.sort();
    blocker_codes.dedup();
    let ready =
        blocker_codes.is_empty() && qualified_class_count == PersistentGraphIndexClass::ALL.len();

    Ok(ProductionGraphIndexQualificationMatrixReport {
        ready,
        blocker_codes,
        qualified_class_count,
        case_reports,
    })
}

pub(crate) fn validate_persistent_graph_index_matrix_classes(
    classes: impl IntoIterator<Item = Option<PersistentGraphIndexClass>>,
) -> Result<(), ProductionGraphQualificationError> {
    let classes = classes.into_iter().collect::<Vec<_>>();
    if classes.len() != PersistentGraphIndexClass::ALL.len() {
        return Err(ProductionGraphQualificationError::new(format!(
            "persistent graph index matrix requires exactly {} cases, received {}",
            PersistentGraphIndexClass::ALL.len(),
            classes.len()
        )));
    }

    let mut seen = [false; PersistentGraphIndexClass::ALL.len()];
    for class in classes {
        let class = class.ok_or_else(|| {
            ProductionGraphQualificationError::new(
                "persistent graph index matrix cases must declare an index requirement",
            )
        })?;
        if std::mem::replace(&mut seen[class as usize], true) {
            return Err(ProductionGraphQualificationError::new(format!(
                "persistent graph index matrix contains duplicate class {}",
                class.as_str()
            )));
        }
    }
    if let Some(missing) = PersistentGraphIndexClass::ALL
        .into_iter()
        .find(|class| !seen[*class as usize])
    {
        return Err(ProductionGraphQualificationError::new(format!(
            "persistent graph index matrix is missing class {}",
            missing.as_str()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::production_graph::PersistentGraphIndexProductionRequirement;
    use skein::{
        DatabaseConfig, NowledgeGraphStatement, NowledgeMemGraphMode, NowledgeMemOpenOptions,
        ProductionEvidenceBinding, ProductionQualificationIdentity, RuntimeGovernorConfig,
        StorageResidencyMode, StorageResourceProfileLimits,
        PRODUCTION_QUALIFICATION_POLICY_VERSION,
    };
    use std::collections::BTreeMap;

    fn matrix_case_config(
        graph_path: &std::path::Path,
        class: PersistentGraphIndexClass,
    ) -> ProductionGraphStorageQualificationConfig {
        let identity = ProductionQualificationIdentity {
            source_revision: "test-revision".to_string(),
            rust_toolchain: "test-toolchain".to_string(),
            target_os: std::env::consts::OS.to_string(),
            target_arch: std::env::consts::ARCH.to_string(),
            enabled_features: Vec::new(),
            durable_format_version: 1,
            schema_version: 1,
            configuration_digest: "test-config".to_string(),
            deployment_profile: "representative-production-replica".to_string(),
            dataset_fingerprint: "test-dataset".to_string(),
            canonical_graph_commit_epoch: 1,
            policy_version: PRODUCTION_QUALIFICATION_POLICY_VERSION,
        };
        ProductionGraphStorageQualificationConfig {
            open_options: NowledgeMemOpenOptions::graph_only(
                graph_path,
                NowledgeMemGraphMode::ShadowReadOnly,
            )
            .with_database_config(DatabaseConfig {
                storage_residency_mode: StorageResidencyMode::OutOfCore,
                segment_cache_capacity_bytes: 1024,
                ..DatabaseConfig::default()
            }),
            runtime_governor_config: RuntimeGovernorConfig::shared_host(),
            statement: NowledgeGraphStatement {
                cypher: "MATCH (n) RETURN n".to_string(),
                parameters: BTreeMap::new(),
            },
            limits: StorageResourceProfileLimits {
                min_canonical_artifact_bytes: 1,
                max_steady_resident_bytes: u64::MAX,
                max_peak_resident_bytes: u64::MAX,
                max_total_page_faults: Some(u64::MAX),
                max_minor_page_faults: cfg!(unix).then_some(u64::MAX),
                max_major_page_faults: cfg!(unix).then_some(u64::MAX),
                max_intermediate_rows: 1,
                max_intermediate_payload_bytes: 1,
                max_output_rows: 1,
                max_output_payload_bytes: 1,
                require_fully_streamed: true,
            },
            evidence_binding: ProductionEvidenceBinding {
                identity: identity.clone(),
                generated_at_unix_seconds: 1,
            },
            expected_identity: identity,
            measurement_runs: 2,
            persistent_index_requirement: Some(PersistentGraphIndexProductionRequirement {
                class,
                reference_output_digest: format!("sha256:{}", "0".repeat(64)),
                reference_output_rows: 0,
                max_blocks_read_per_run: 1,
                max_bytes_read_per_run: 1,
                max_cancellation_latency_micros: 1,
            }),
        }
    }

    #[test]
    fn production_index_matrix_requires_each_class_exactly_once() {
        validate_persistent_graph_index_matrix_classes(
            PersistentGraphIndexClass::ALL.into_iter().map(Some),
        )
        .expect("the complete class set should be accepted");

        let missing = validate_persistent_graph_index_matrix_classes(
            PersistentGraphIndexClass::ALL[..7]
                .iter()
                .copied()
                .map(Some),
        )
        .expect_err("an incomplete class set must be rejected");
        assert!(missing.to_string().contains("requires exactly 8 cases"));

        let mut duplicate = PersistentGraphIndexClass::ALL;
        duplicate[7] = PersistentGraphIndexClass::ForwardAdjacency;
        let duplicate =
            validate_persistent_graph_index_matrix_classes(duplicate.into_iter().map(Some))
                .expect_err("a duplicate class must be rejected");
        assert!(duplicate.to_string().contains("duplicate class"));

        let mut missing_requirement = PersistentGraphIndexClass::ALL
            .into_iter()
            .map(Some)
            .collect::<Vec<_>>();
        missing_requirement[3] = None;
        let missing_requirement =
            validate_persistent_graph_index_matrix_classes(missing_requirement)
                .expect_err("a case without an index requirement must be rejected");
        assert!(missing_requirement
            .to_string()
            .contains("must declare an index requirement"));
    }

    #[test]
    fn production_index_matrix_binds_one_replica_runtime_and_generation() {
        let path = std::env::temp_dir().join("skein-production-index-matrix-validation");
        let cases = PersistentGraphIndexClass::ALL
            .into_iter()
            .map(|class| matrix_case_config(&path, class))
            .collect::<Vec<_>>();
        ProductionGraphIndexQualificationMatrixConfig {
            cases: cases.clone(),
        }
        .validate()
        .expect("one replica and identity should be accepted");

        let mut mixed_replica = cases.clone();
        mixed_replica[1].open_options.graph_path = path.join("other");
        let mixed_replica = ProductionGraphIndexQualificationMatrixConfig {
            cases: mixed_replica,
        }
        .validate()
        .expect_err("mixed replicas must be rejected");
        assert!(mixed_replica.to_string().contains("same replica"));

        let mut mixed_runtime = cases.clone();
        mixed_runtime[2].runtime_governor_config.memory_budget_bytes = Some(32 * 1024 * 1024);
        let mixed_runtime = ProductionGraphIndexQualificationMatrixConfig {
            cases: mixed_runtime,
        }
        .validate()
        .expect_err("mixed runtime configurations must be rejected");
        assert!(mixed_runtime.to_string().contains("same runtime governor"));

        let mut mixed_generation = cases;
        mixed_generation[3]
            .expected_identity
            .canonical_graph_commit_epoch = 2;
        mixed_generation[3]
            .evidence_binding
            .identity
            .canonical_graph_commit_epoch = 2;
        let mixed_generation = ProductionGraphIndexQualificationMatrixConfig {
            cases: mixed_generation,
        }
        .validate()
        .expect_err("mixed generations must be rejected");
        assert!(mixed_generation
            .to_string()
            .contains("same production identity"));
    }
}
