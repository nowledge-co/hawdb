use super::*;
use crate::evidence_digest::rows_sha256;
use skein::{QueryStreamOptions, PRODUCTION_QUALIFICATION_POLICY_VERSION};
use std::sync::atomic::{AtomicU64, Ordering};

static TEST_ID: AtomicU64 = AtomicU64::new(0);

#[test]
fn artifact_measurement_counts_growth_and_removed_overflow_files() {
    let before = BTreeMap::from([
        ("relational-overflow-1.extents.skein".to_string(), 100),
        ("checkpoint-1.skein".to_string(), 50),
    ]);
    let after_compaction = BTreeMap::from([
        ("relational-overflow-1.extents.skein".to_string(), 100),
        ("relational-overflow-2.extents.skein".to_string(), 80),
        ("checkpoint-1.skein".to_string(), 70),
    ]);
    let after_cleanup = BTreeMap::from([
        ("relational-overflow-2.extents.skein".to_string(), 80),
        ("checkpoint-1.skein".to_string(), 70),
    ]);

    assert_eq!(created_or_grown_bytes(&before, &after_compaction), 100);
    assert_eq!(removed_overflow_extents(&before, &after_cleanup), (1, 100));
}

#[test]
fn qualification_requires_a_writable_disposable_replica() {
    let path = std::env::temp_dir().join(format!(
        "skein-overflow-compaction-config-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&path).unwrap();
    let identity = ProductionQualificationIdentity {
        source_revision: "revision".to_string(),
        rust_toolchain: "rustc-test".to_string(),
        target_os: std::env::consts::OS.to_string(),
        target_arch: std::env::consts::ARCH.to_string(),
        enabled_features: Vec::new(),
        durable_format_version: 1,
        schema_version: 1,
        configuration_digest: "config".to_string(),
        deployment_profile: "disposable-production-replica".to_string(),
        dataset_fingerprint: "dataset".to_string(),
        canonical_graph_commit_epoch: 1,
        policy_version: skein::PRODUCTION_QUALIFICATION_POLICY_VERSION,
    };
    let database_config = DatabaseConfig {
        read_only: true,
        storage_residency_mode: StorageResidencyMode::OutOfCore,
        relational_index_mode: RelationalIndexMode::Authoritative,
        segment_cache_capacity_bytes: 1,
        ..DatabaseConfig::default()
    };
    let error = run_production_content_store_overflow_compaction_qualification(
        ProductionContentStoreOverflowCompactionQualificationConfig {
            replica_path: path.clone(),
            database_config,
            runtime_governor_config: RuntimeGovernorConfig::shared_host(),
            resource_profile_kind: ContentStoreResourceProfileKind::ConfiguredWorkload,
            configured_available_memory_bytes: 1024,
            evidence_binding: ProductionEvidenceBinding {
                identity: identity.clone(),
                generated_at_unix_seconds: 1,
            },
            expected_identity: identity,
            compaction: RelationalOverflowCompactionConfig::default(),
            limits: ProductionContentStoreOverflowCompactionLimits {
                process: ProductionContentStoreResourceLimits {
                    max_steady_resident_bytes: 1024,
                    max_peak_resident_bytes: 1024,
                    max_total_page_faults_per_run: None,
                    max_minor_page_faults_per_run: None,
                    max_major_page_faults_per_run: None,
                },
                max_compaction_elapsed_micros: 1,
                max_cleanup_elapsed_micros: 1,
                max_new_generation_artifact_bytes: 1,
                max_new_artifact_write_amplification_per_million: 1,
                min_reclaimable_base_extent_count: 1,
                min_physically_removed_extent_bytes: 1,
            },
            verification_cases: Vec::new(),
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("writable disposable replica"));
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn qualification_proves_exact_rewrite_physical_reclaim_and_reopen() {
    let path = std::env::temp_dir().join(format!(
        "skein-overflow-compaction-production-{}-{}",
        std::process::id(),
        TEST_ID.fetch_add(1, Ordering::SeqCst)
    ));
    let mut fixture = super::super::ContentStoreInitialRowPageQualificationConfig::synthetic(
        &path,
        "overflow-compaction-production-test",
    );
    fixture.base_message_count = 3;
    fixture.base_chunk_count = 3;
    fixture.segment_cache_capacity_bytes = 1024 * 1024;
    let corpus = nowledge_content_store_sql_corpus().unwrap();
    super::super::fixture::bootstrap_checkpoint(&fixture, &corpus).unwrap();
    let database_config =
        super::super::fixture::database_config(&fixture, RelationalIndexMode::Authoritative);
    let mut database = Database::open_with_durability_and_config(
        &path,
        DurabilityPolicy::SyncOnEveryWrite,
        database_config.clone(),
    )
    .unwrap();
    for position in 0..fixture.base_message_count {
        database
            .query_sql_with_params(
                "UPDATE thread_messages SET content = $1 WHERE content_message_id = $2",
                &[
                    Value::String(low_compression_text(position as u64, 96 * 1024)),
                    Value::String(format!("content-message-{position:08}")),
                ],
            )
            .unwrap();
    }
    database.checkpoint().unwrap();
    database
        .query_sql_with_params(
            "UPDATE thread_messages SET content = $1 WHERE content_message_id = $2",
            &[
                Value::String(low_compression_text(100, 96 * 1024)),
                Value::String("content-message-00000000".to_string()),
            ],
        )
        .unwrap();
    database.checkpoint().unwrap();
    let statement = corpus.statement("thread_messages_page").unwrap();
    let parameters = super::super::fixture::thread_page_parameters(3);
    let output = database
        .query_sql_with_params_options(
            &statement.sql,
            &parameters,
            QueryStreamOptions {
                max_rows: Some(statement.max_rows),
                max_payload_bytes: Some(statement.max_payload_bytes),
            },
        )
        .unwrap();
    let expected_output_sha256 = rows_sha256(&output.rows);
    let commit_epoch = database.commit_epoch();
    drop(database);

    let identity = ProductionQualificationIdentity {
        source_revision: "overflow-compaction-production-test".to_string(),
        rust_toolchain: "rustc-test".to_string(),
        target_os: std::env::consts::OS.to_string(),
        target_arch: std::env::consts::ARCH.to_string(),
        enabled_features: Vec::new(),
        durable_format_version: 1,
        schema_version: 1,
        configuration_digest: "overflow-compaction-config".to_string(),
        deployment_profile: "disposable-production-replica".to_string(),
        dataset_fingerprint: "overflow-compaction-dataset".to_string(),
        canonical_graph_commit_epoch: commit_epoch,
        policy_version: PRODUCTION_QUALIFICATION_POLICY_VERSION,
    };
    let mut runtime_governor_config = RuntimeGovernorConfig::shared_host();
    runtime_governor_config.memory_budget_bytes =
        Some(super::super::CONTENT_STORE_512_MIB_CAPABILITY_BYTES);
    runtime_governor_config.result_budget_bytes = 128 * 1024 * 1024;
    let report = run_production_content_store_overflow_compaction_qualification(
        ProductionContentStoreOverflowCompactionQualificationConfig {
            replica_path: path.clone(),
            database_config,
            runtime_governor_config,
            resource_profile_kind: ContentStoreResourceProfileKind::Capability512Mib,
            configured_available_memory_bytes: super::super::CONTENT_STORE_512_MIB_CAPABILITY_BYTES,
            evidence_binding: ProductionEvidenceBinding {
                identity: identity.clone(),
                generated_at_unix_seconds: 1,
            },
            expected_identity: identity,
            compaction: RelationalOverflowCompactionConfig::default(),
            limits: ProductionContentStoreOverflowCompactionLimits {
                process: ProductionContentStoreResourceLimits {
                    max_steady_resident_bytes: super::super::CONTENT_STORE_512_MIB_CAPABILITY_BYTES,
                    max_peak_resident_bytes: super::super::CONTENT_STORE_512_MIB_CAPABILITY_BYTES,
                    max_total_page_faults_per_run: None,
                    max_minor_page_faults_per_run: None,
                    max_major_page_faults_per_run: None,
                },
                max_compaction_elapsed_micros: u64::MAX,
                max_cleanup_elapsed_micros: u64::MAX,
                max_new_generation_artifact_bytes: u64::MAX,
                max_new_artifact_write_amplification_per_million: u64::MAX,
                min_reclaimable_base_extent_count: 1,
                min_physically_removed_extent_bytes: 1,
            },
            verification_cases: vec![ProductionContentStoreReadCase {
                case_name: "retained-thread-messages".to_string(),
                statement_name: statement.name.clone(),
                parameters,
                expected_output_rows: 3,
                expected_output_sha256,
                max_intermediate_rows: 1024,
                max_physical_pages_per_run: 4096,
                max_physical_bytes_per_run: 128 * 1024 * 1024,
            }],
        },
    )
    .unwrap();

    assert!(
        report.ready,
        "unexpected blockers: {:?}, initial: {:?}, reads: {:?}",
        report.blocker_codes, report.initial_residency, report.before_reads
    );
    assert_eq!(report.compaction.hydrated_values, 0);
    assert!(report.compaction.reclaimable_base_extent_count > 0);
    assert!(report.artifacts.physically_removed_extent_bytes > 0);
    assert_eq!(
        report.before_reads[0].read.output_sha256,
        report.compacted_reads[0].read.output_sha256
    );
    assert_eq!(
        report.before_reads[0].read.output_sha256,
        report.reopened_reads[0].read.output_sha256
    );
    assert_eq!(report.reopened_residency.segment_cache_pinned_bytes, 0);
    assert!(!report
        .json()
        .to_string()
        .contains(path.to_string_lossy().as_ref()));
    let release = crate::evaluate_production_release_qualification_bundle(
        crate::ProductionReleaseQualificationArtifacts {
            content_store_512_mib_overflow_compaction: Some(report.json()),
            ..crate::ProductionReleaseQualificationArtifacts::default()
        },
        report.evidence_binding.identity.clone(),
        crate::ProductionReleaseQualificationPolicy::default(),
    );
    assert!(
        release.content_store_512_mib_overflow_compaction.ready,
        "release blockers: {:?}, initial: {:?}, compacted: {:?}, final: {:?}, reopened: {:?}",
        release
            .content_store_512_mib_overflow_compaction
            .blocker_codes,
        report.initial_residency,
        report.compacted_residency,
        report.final_residency,
        report.reopened_residency,
    );

    std::fs::remove_dir_all(path).unwrap();
}

fn low_compression_text(seed: u64, bytes: usize) -> String {
    let mut state = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut text = String::with_capacity(bytes);
    for _ in 0..bytes {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let byte = 33 + (state % 90) as u8;
        text.push(char::from(byte));
    }
    text
}
