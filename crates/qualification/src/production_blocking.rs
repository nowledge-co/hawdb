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

use super::{
    parameter_digest, runtime_report, validate_production_identity_for_current_target,
    MixedSoakRuntimeReport, ProductionGraphQualificationError,
};
use hawdb::executor::ExecutionMemoryConfig;
use hawdb::{
    IoConcurrencyBudget, NowledgeGraphStatement, NowledgeMemEmbeddedStoreHandle,
    NowledgeMemGraphMode, NowledgeMemOpenOptions, NowledgeMemReadOptions,
    ProductionEvidenceBinding, ProductionQualificationIdentity, RuntimeGovernor,
    RuntimeGovernorConfig, RuntimeTaskContext, StorageDeviceProfile,
};
use hawdb_query::QueryIdentity;
use serde::Serialize;
use std::collections::BTreeSet;

pub const PRODUCTION_BLOCKING_QUALIFICATION_PROTOCOL: &str =
    "hawdb-production-blocking-qualification-v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProductionBlockingOperatorKind {
    Distinct,
    CartesianBuild,
}

impl ProductionBlockingOperatorKind {
    fn operator_name(self) -> &'static str {
        match self {
            Self::Distinct => "DistinctExec",
            Self::CartesianBuild => "NodeCartesianProductExec",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProductionBlockingDisposition {
    ExternalSpillObserved,
    InMemoryWithinAdmission,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductionBlockingQualificationCase {
    pub route_name: String,
    pub operator_kind: ProductionBlockingOperatorKind,
    pub statement: NowledgeGraphStatement,
    pub read_options: NowledgeMemReadOptions,
    pub minimum_input_rows: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductionBlockingQualificationConfig {
    pub open_options: NowledgeMemOpenOptions,
    pub runtime_governor_config: RuntimeGovernorConfig,
    pub evidence_binding: ProductionEvidenceBinding,
    pub expected_identity: ProductionQualificationIdentity,
    pub cases: Vec<ProductionBlockingQualificationCase>,
}

impl ProductionBlockingQualificationConfig {
    fn validate(&self) -> Result<(), ProductionGraphQualificationError> {
        if self.open_options.mode != NowledgeMemGraphMode::ShadowReadOnly {
            return Err(ProductionGraphQualificationError::new(
                "production blocking qualification requires shadow read-only mode",
            ));
        }
        if self.open_options.database_config.is_none() {
            return Err(ProductionGraphQualificationError::new(
                "production blocking qualification requires an explicit database config",
            ));
        }
        validate_production_identity_for_current_target(
            &self.evidence_binding,
            &self.expected_identity,
        )?;
        let route_names = self
            .cases
            .iter()
            .map(|case| case.route_name.as_str())
            .collect::<BTreeSet<_>>();
        if route_names.len() != self.cases.len()
            || route_names.iter().any(|route| !valid_route_name(route))
        {
            return Err(ProductionGraphQualificationError::new(
                "production blocking route names must be non-empty and unique",
            ));
        }
        let kinds = self
            .cases
            .iter()
            .map(|case| case.operator_kind)
            .collect::<BTreeSet<_>>();
        if !kinds.contains(&ProductionBlockingOperatorKind::Distinct)
            || !kinds.contains(&ProductionBlockingOperatorKind::CartesianBuild)
        {
            return Err(ProductionGraphQualificationError::new(
                "production blocking qualification requires distinct and cartesian route cases",
            ));
        }
        for case in &self.cases {
            if case.minimum_input_rows == 0 {
                return Err(ProductionGraphQualificationError::new(format!(
                    "production blocking route {} requires a non-zero minimum input row count",
                    case.route_name
                )));
            }
            if case.read_options.max_rows.is_none()
                || case.read_options.max_estimated_payload_bytes.is_none()
            {
                return Err(ProductionGraphQualificationError::new(format!(
                    "production blocking route {} requires explicit row and payload budgets",
                    case.route_name
                )));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProductionBlockingCaseReport {
    pub route_name: String,
    pub operator_kind: ProductionBlockingOperatorKind,
    pub query_digest: String,
    pub parameter_digest: String,
    pub ready: bool,
    pub blocker_codes: Vec<String>,
    pub disposition: Option<ProductionBlockingDisposition>,
    pub input_rows: usize,
    pub minimum_input_rows: usize,
    pub budget_bytes: usize,
    pub peak_tracked_bytes: usize,
    pub max_spill_bytes: u64,
    pub max_spill_runs: usize,
    pub spilled_bytes: u64,
    pub spill_run_count: usize,
    pub spilled_rows: usize,
    pub fully_streamed: bool,
    pub output_rows: usize,
    pub output_payload_bytes: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct ProductionSpillPoolReport {
    pub max_total_bytes: u64,
    pub max_total_runs: usize,
    pub active_bytes: u64,
    pub peak_active_bytes: u64,
    pub pending_write_bytes: u64,
    pub active_runs: usize,
    pub peak_active_runs: usize,
    pub orphan_cleanup_failures: u64,
    pub run_delete_failures: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductionBlockingQualificationReport {
    pub ready: bool,
    pub blocker_codes: Vec<String>,
    pub open_report: serde_json::Value,
    pub evidence_binding: ProductionEvidenceBinding,
    pub cases: Vec<ProductionBlockingCaseReport>,
    pub spill_pool: ProductionSpillPoolReport,
    pub runtime: MixedSoakRuntimeReport,
}

impl ProductionBlockingQualificationReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": PRODUCTION_BLOCKING_QUALIFICATION_PROTOCOL,
            "evidence_kind": "active_route_blocking_operators",
            "production_eligible": true,
            "ready": self.ready,
            "blocker_codes": self.blocker_codes,
            "open_report": self.open_report,
            "evidence_binding": self.evidence_binding.json(),
            "cases": self.cases,
            "spill_pool": self.spill_pool,
            "runtime": self.runtime,
        })
    }
}

pub fn run_production_blocking_qualification(
    config: ProductionBlockingQualificationConfig,
) -> Result<ProductionBlockingQualificationReport, ProductionGraphQualificationError> {
    config.validate()?;
    let memory = config
        .open_options
        .database_config
        .as_ref()
        .expect("validated database config")
        .execution_memory
        .clone();
    let storage_io = IoConcurrencyBudget::shared_host_for_device(StorageDeviceProfile::detect(
        &config.open_options.graph_path,
    ));
    let governor = RuntimeGovernor::detect(config.runtime_governor_config, storage_io);
    let (store, open_report) =
        NowledgeMemEmbeddedStoreHandle::open_with_options_and_runtime_governor(
            config.open_options,
            governor,
        )
        .map_err(ProductionGraphQualificationError::from_error)?;
    let graph_epoch = store
        .runtime_status()
        .map_err(ProductionGraphQualificationError::from_error)?
        .graph_commit_epoch;
    if graph_epoch != config.expected_identity.canonical_graph_commit_epoch {
        return Err(ProductionGraphQualificationError::new(format!(
            "qualification graph epoch {graph_epoch} does not match expected epoch {}",
            config.expected_identity.canonical_graph_commit_epoch
        )));
    }
    let runtime_before = store
        .runtime_governor_snapshot()
        .map_err(ProductionGraphQualificationError::from_error)?;
    let mut cases = Vec::with_capacity(config.cases.len());
    for case in config.cases {
        cases.push(run_case(&store, case)?);
    }
    let runtime_after = store
        .runtime_governor_snapshot()
        .map_err(ProductionGraphQualificationError::from_error)?;
    let runtime = runtime_report(runtime_before, runtime_after);
    let spill_pool = spill_pool_report(&memory)?;
    let mut blocker_codes = cases
        .iter()
        .filter(|case| !case.ready)
        .map(|case| format!("route_{}_not_ready", case.route_name))
        .collect::<Vec<_>>();
    if runtime.admissions_delta < cases.len() as u64
        || runtime.completions_delta < cases.len() as u64
    {
        blocker_codes.push("runtime_admission_not_observed_for_every_route".to_string());
    }
    if runtime.admission_waits_delta != 0 || runtime.admission_rejections_delta != 0 {
        blocker_codes.push("blocking_route_admission_regression".to_string());
    }
    if runtime.final_active_foreground_tasks != 0
        || runtime.final_active_background_tasks != 0
        || runtime.final_active_blocking_tasks != 0
        || runtime.final_admitted_memory_bytes != 0
    {
        blocker_codes.push("runtime_permit_leak".to_string());
    }
    if runtime.final_overcommitted {
        blocker_codes.push("runtime_overcommitted".to_string());
    }
    if spill_pool.active_bytes != 0
        || spill_pool.pending_write_bytes != 0
        || spill_pool.active_runs != 0
    {
        blocker_codes.push("spill_pool_live_capacity_leak".to_string());
    }
    if spill_pool.orphan_cleanup_failures != 0 || spill_pool.run_delete_failures != 0 {
        blocker_codes.push("spill_cleanup_failure".to_string());
    }
    blocker_codes.sort();
    blocker_codes.dedup();

    Ok(ProductionBlockingQualificationReport {
        ready: blocker_codes.is_empty(),
        blocker_codes,
        open_report: open_report.json(),
        evidence_binding: config.evidence_binding,
        cases,
        spill_pool,
        runtime,
    })
}

fn valid_route_name(route: &str) -> bool {
    !route.is_empty()
        && route.len() <= 128
        && route
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

fn run_case(
    store: &NowledgeMemEmbeddedStoreHandle,
    case: ProductionBlockingQualificationCase,
) -> Result<ProductionBlockingCaseReport, ProductionGraphQualificationError> {
    let query_identity = QueryIdentity::new("cypher", &case.statement.cypher);
    let parameters_digest = parameter_digest(&case.statement.parameters);
    let query = store
        .read_query_with_params_streaming_context(
            &case.statement.cypher,
            &case.statement.parameters,
            &case.read_options,
            &RuntimeTaskContext::default(),
            |_| Ok(()),
        )
        .map_err(ProductionGraphQualificationError::from_error)?;
    let matching = query
        .execution_profile
        .blocking_operator_memory_reports
        .iter()
        .filter(|report| report.operator == case.operator_kind.operator_name())
        .collect::<Vec<_>>();
    let mut blocker_codes = Vec::new();
    if matching.len() != 1 {
        blocker_codes.push("target_operator_report_count_mismatch".to_string());
    }
    let operator = matching.first().copied();
    let input_rows = operator.map_or(0, |report| report.input_rows);
    let budget_bytes = operator.map_or(0, |report| report.budget_bytes);
    let peak_tracked_bytes = operator.map_or(0, |report| report.peak_tracked_bytes);
    let max_spill_bytes = operator.map_or(0, |report| report.max_spill_bytes);
    let max_spill_runs = operator.map_or(0, |report| report.max_spill_runs);
    let spilled_bytes = operator.map_or(0, |report| report.spilled_bytes);
    let spill_run_count = operator.map_or(0, |report| report.spill_run_count);
    let spilled_rows = operator.map_or(0, |report| report.spilled_rows);
    if input_rows < case.minimum_input_rows {
        blocker_codes.push("operator_input_below_route_minimum".to_string());
    }
    if peak_tracked_bytes > budget_bytes {
        blocker_codes.push("operator_memory_budget_exceeded".to_string());
    }
    if spilled_bytes > max_spill_bytes || spill_run_count > max_spill_runs {
        blocker_codes.push("operator_spill_budget_exceeded".to_string());
    }
    if (spilled_bytes == 0) != (spill_run_count == 0) {
        blocker_codes.push("operator_spill_accounting_inconsistent".to_string());
    }
    if spilled_rows > input_rows {
        blocker_codes.push("operator_spilled_rows_exceed_input".to_string());
    }
    if !query.fully_streamed {
        blocker_codes.push("query_not_fully_streamed".to_string());
    }
    let disposition = operator.map(|_| {
        if spilled_bytes > 0 {
            if spilled_rows == 0 {
                blocker_codes.push("operator_spilled_rows_missing".to_string());
            }
            ProductionBlockingDisposition::ExternalSpillObserved
        } else {
            ProductionBlockingDisposition::InMemoryWithinAdmission
        }
    });
    blocker_codes.sort();
    blocker_codes.dedup();

    Ok(ProductionBlockingCaseReport {
        route_name: case.route_name,
        operator_kind: case.operator_kind,
        query_digest: query_identity.query_digest().to_string(),
        parameter_digest: parameters_digest,
        ready: blocker_codes.is_empty(),
        blocker_codes,
        disposition,
        input_rows,
        minimum_input_rows: case.minimum_input_rows,
        budget_bytes,
        peak_tracked_bytes,
        max_spill_bytes,
        max_spill_runs,
        spilled_bytes,
        spill_run_count,
        spilled_rows,
        fully_streamed: query.fully_streamed,
        output_rows: query.output_rows,
        output_payload_bytes: query.output_payload_bytes,
    })
}

fn spill_pool_report(
    memory: &ExecutionMemoryConfig,
) -> Result<ProductionSpillPoolReport, ProductionGraphQualificationError> {
    let snapshot = memory
        .spill_pool_snapshot()
        .map_err(ProductionGraphQualificationError::from_error)?;
    Ok(ProductionSpillPoolReport {
        max_total_bytes: snapshot.max_total_bytes,
        max_total_runs: snapshot.max_total_runs,
        active_bytes: snapshot.active_bytes,
        peak_active_bytes: snapshot.peak_active_bytes,
        pending_write_bytes: snapshot.pending_write_bytes,
        active_runs: snapshot.active_runs,
        peak_active_runs: snapshot.peak_active_runs,
        orphan_cleanup_failures: snapshot.orphan_cleanup_failures,
        run_delete_failures: snapshot.run_delete_failures,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use hawdb::executor::ExecutionMemoryConfig;
    use hawdb::{
        Database, DatabaseConfig, StorageResidencyMode, Value,
        PRODUCTION_QUALIFICATION_POLICY_VERSION,
    };
    use std::collections::BTreeMap;
    use std::num::{NonZeroU64, NonZeroUsize};
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_ID: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn active_route_evidence_observes_bounded_distinct_and_cartesian_spill() {
        let id = TEST_ID.fetch_add(1, Ordering::SeqCst);
        let root = std::env::temp_dir().join(format!(
            "hawdb-production-blocking-{}-{id}",
            std::process::id()
        ));
        let graph_path = root.join("database");
        let spill_path = root.join("spill");
        let execution_memory = ExecutionMemoryConfig {
            blocking_operator_bytes: NonZeroUsize::new(2048).expect("non-zero"),
            max_spill_bytes: NonZeroU64::new(16 * 1024 * 1024).expect("non-zero"),
            max_spill_runs: NonZeroUsize::new(128).expect("non-zero"),
            max_total_spill_bytes: NonZeroU64::new(32 * 1024 * 1024).expect("non-zero"),
            max_total_spill_runs: NonZeroUsize::new(256).expect("non-zero"),
            min_spill_free_bytes: NonZeroU64::new(1).expect("non-zero"),
            spill_directory: spill_path,
            ..ExecutionMemoryConfig::default()
        };
        let database_config = DatabaseConfig {
            storage_residency_mode: StorageResidencyMode::Materialized,
            execution_memory,
            max_read_result_rows: Some(4096),
            max_read_result_payload_bytes: Some(16 * 1024 * 1024),
            ..DatabaseConfig::default()
        };
        let graph_commit_epoch = {
            let mut database = Database::open_with_config(&graph_path, database_config.clone())
                .expect("fixture database should open");
            let mut transaction = database.begin_transaction();
            transaction
                .query("CREATE (:Left {id: 'left'})")
                .expect("left row should be inserted");
            for row in 0..40 {
                transaction
                    .query_with_params(
                        "CREATE (:Item {value: $value})",
                        &BTreeMap::from([(
                            "value".to_string(),
                            Value::String(format!("item-{row}-{}", "x".repeat(96))),
                        )]),
                    )
                    .expect("item row should be inserted");
                transaction
                    .query_with_params(
                        "CREATE (:Right {id: $id, payload: $payload})",
                        &BTreeMap::from([
                            ("id".to_string(), Value::Int(row)),
                            ("payload".to_string(), Value::String("y".repeat(96))),
                        ]),
                    )
                    .expect("right row should be inserted");
            }
            transaction.commit().expect("fixture commit should succeed");
            database
                .checkpoint()
                .expect("fixture checkpoint should succeed");
            database.commit_epoch()
        };
        let identity = ProductionQualificationIdentity {
            source_revision: "revision".to_string(),
            rust_toolchain: "toolchain".to_string(),
            target_os: std::env::consts::OS.to_string(),
            target_arch: std::env::consts::ARCH.to_string(),
            enabled_features: Vec::new(),
            durable_format_version: 1,
            schema_version: 1,
            configuration_digest: "blocking-test".to_string(),
            deployment_profile: "production".to_string(),
            dataset_fingerprint: "dataset".to_string(),
            canonical_graph_commit_epoch: graph_commit_epoch,
            policy_version: PRODUCTION_QUALIFICATION_POLICY_VERSION,
        };
        let read_options = NowledgeMemReadOptions {
            max_rows: Some(4096),
            max_estimated_payload_bytes: Some(16 * 1024 * 1024),
        };
        let report = run_production_blocking_qualification(ProductionBlockingQualificationConfig {
            open_options: NowledgeMemOpenOptions::graph_only(
                &graph_path,
                NowledgeMemGraphMode::ShadowReadOnly,
            )
            .with_database_config(database_config),
            runtime_governor_config: RuntimeGovernorConfig {
                memory_budget_bytes: Some(128 * 1024 * 1024),
                result_budget_bytes: 16 * 1024 * 1024,
                ..RuntimeGovernorConfig::shared_host()
            },
            evidence_binding: ProductionEvidenceBinding {
                identity: identity.clone(),
                generated_at_unix_seconds: 1,
            },
            expected_identity: identity,
            cases: vec![
                ProductionBlockingQualificationCase {
                    route_name: "memory_distinct".to_string(),
                    operator_kind: ProductionBlockingOperatorKind::Distinct,
                    statement: NowledgeGraphStatement {
                        cypher: "MATCH (m:Item) RETURN DISTINCT m.value AS value".to_string(),
                        parameters: BTreeMap::new(),
                    },
                    read_options: read_options.clone(),
                    minimum_input_rows: 20,
                },
                ProductionBlockingQualificationCase {
                    route_name: "left_right_product".to_string(),
                    operator_kind: ProductionBlockingOperatorKind::CartesianBuild,
                    statement: NowledgeGraphStatement {
                        cypher:
                            "MATCH (l:Left), (r:Right) RETURN l.id AS left_id, r.id AS right_id"
                                .to_string(),
                        parameters: BTreeMap::new(),
                    },
                    read_options,
                    minimum_input_rows: 20,
                },
            ],
        })
        .expect("blocking qualification should complete");

        assert!(
            report.ready,
            "unexpected blockers: {:?}",
            report.blocker_codes
        );
        assert!(report.cases.iter().all(|case| {
            case.disposition == Some(ProductionBlockingDisposition::ExternalSpillObserved)
        }));
        assert_eq!(report.spill_pool.active_bytes, 0);
        assert_eq!(report.spill_pool.active_runs, 0);

        drop(report);
        std::fs::remove_dir_all(root).expect("fixture should be removable");
    }
}
