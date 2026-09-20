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

use crate::store_facade_tests::unique_test_dir;
use crate::{Database, DatabaseConfig, RelationalRowPageCompactionConfig, Value};
use hawdb_storage::{RelationalIndexMode, StorageResidencyMode};
use std::collections::BTreeMap;
use std::path::Path;

const TABLES: u64 = 16;
// Database also persists the nonempty hawdb_schema_migrations registry.
const LIVE_PAGES: u64 = TABLES + 1;

fn open_config(mode: StorageResidencyMode) -> DatabaseConfig {
    DatabaseConfig {
        storage_residency_mode: mode,
        relational_index_mode: if mode == StorageResidencyMode::OutOfCore {
            RelationalIndexMode::Authoritative
        } else {
            RelationalIndexMode::Shadow
        },
        ..DatabaseConfig::default()
    }
}

fn churn_database(path: &Path, mode: StorageResidencyMode, tables: u64) -> Database {
    let mut db =
        Database::open_with_config(path, open_config(StorageResidencyMode::Materialized)).unwrap();
    for table in 0..tables {
        db.query_sql(&format!(
            "CREATE TABLE documents_{table} (id BIGINT PRIMARY KEY, revision BIGINT NOT NULL)"
        ))
        .unwrap();
        db.query_sql(&format!(
            "INSERT INTO documents_{table} (id, revision) VALUES (1, 0)"
        ))
        .unwrap();
    }
    db.checkpoint().unwrap();
    drop(db);
    let mut db = Database::open_with_config(path, open_config(mode)).unwrap();
    // Each round leaves one cold page in the previous physical generation.
    for round in 1..tables {
        for table in round..tables {
            db.query_sql(&format!(
                "UPDATE documents_{table} SET revision = {round} WHERE id = 1"
            ))
            .unwrap();
        }
        db.checkpoint().unwrap();
    }
    db
}

fn physical_page_bytes(path: &Path) -> u64 {
    std::fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap())
        .filter(|entry| {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            name.starts_with("relational-row-pages-") && name.ends_with(".pages.hawdb")
        })
        .map(|entry| entry.metadata().unwrap().len())
        .sum()
}

#[test]
fn row_page_compaction_converges_disk_bytes_and_preserves_pinned_readers() {
    for mode in [
        StorageResidencyMode::Materialized,
        StorageResidencyMode::OutOfCore,
    ] {
        let path = unique_test_dir(&format!("row_page_compaction_churn_{mode:?}"));
        let mut db = churn_database(&path, mode, TABLES);
        let before = db.storage_residency_report().relational_rows;
        assert_eq!(before.root_page_count, LIVE_PAGES);
        assert_eq!(before.allocated_page_count, TABLES * (TABLES + 1) / 2 + 1);
        assert_eq!(before.physical_generation_count, TABLES as usize);
        if mode == StorageResidencyMode::OutOfCore {
            assert!(before.checkpoint_state_metadata_only);
            assert_eq!(before.materialized_row_count, 0);
        }
        let pinned = db.begin_read_transaction();
        let report = db
            .compact_relational_row_pages(RelationalRowPageCompactionConfig::default())
            .unwrap();
        assert_eq!(report.root_pages, LIVE_PAGES);
        assert_eq!(report.dirty_pages_written, 0);
        assert_eq!(report.relocated_pages_written, LIVE_PAGES - 1);
        assert_eq!(report.reused_pages, 1);
        assert_eq!(report.previous_allocated_pages, before.allocated_page_count);
        assert_eq!(report.allocated_pages, LIVE_PAGES);
        let compacted = db.storage_residency_report().relational_rows;
        assert_eq!(compacted.allocated_page_bytes, compacted.live_page_bytes);
        assert!(physical_page_bytes(&path) > compacted.allocated_page_bytes);
        for table in 0..TABLES {
            let sql = format!("SELECT revision FROM documents_{table} WHERE id = 1");
            let expected = vec![BTreeMap::from([(
                "revision".to_string(),
                Value::Int(table as i64),
            )])];
            assert_eq!(pinned.query_sql(&sql).unwrap().rows, expected);
            assert_eq!(db.query_sql(&sql).unwrap().rows, expected);
        }
        db.scrub_storage().unwrap();
        drop(pinned);
        db.checkpoint().unwrap();
        let reclaimed = physical_page_bytes(&path);
        assert_eq!(reclaimed, compacted.live_page_bytes);
        assert!(reclaimed * 4 < before.allocated_page_bytes);
        drop(db);

        let mut reopened = Database::open_with_config(&path, open_config(mode)).unwrap();
        assert_eq!(
            reopened
                .storage_residency_report()
                .relational_rows
                .allocated_page_count,
            LIVE_PAGES
        );
        reopened.scrub_storage().unwrap();
        for table in 0..TABLES {
            assert_eq!(
                reopened
                    .query_sql(&format!(
                        "SELECT revision FROM documents_{table} WHERE id = 1"
                    ))
                    .unwrap()
                    .rows,
                vec![BTreeMap::from([(
                    "revision".to_string(),
                    Value::Int(table as i64)
                )])]
            );
        }
        drop(reopened);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn row_page_compaction_failure_limits_leave_the_generation_retryable() {
    use hawdb_core::{RuntimeCancellationToken, RuntimeTaskContext};
    use std::num::NonZeroU64;

    let path = unique_test_dir("row_page_compaction_limits");
    let mode = StorageResidencyMode::OutOfCore;
    let mut db = churn_database(&path, mode, 4);
    let before = db.storage_residency_report().relational_rows;
    let generation = before.base_generation.unwrap() + 1;
    for config in [
        RelationalRowPageCompactionConfig {
            rewrite: hawdb_storage::RelationalRowPageRewriteConfig {
                max_scan_pages: NonZeroU64::new(1).unwrap(),
                ..Default::default()
            },
            ..Default::default()
        },
        RelationalRowPageCompactionConfig {
            rewrite: hawdb_storage::RelationalRowPageRewriteConfig {
                max_rewrite_bytes: NonZeroU64::new(1).unwrap(),
                ..Default::default()
            },
            ..Default::default()
        },
    ] {
        let error = db.compact_relational_row_pages(config).unwrap_err();
        assert!(error.to_string().contains("limit"), "{error}");
        assert_eq!(
            db.storage_residency_report()
                .relational_rows
                .base_generation,
            before.base_generation
        );
        for artifact in [
            hawdb_storage::relational_row_page_manifest_generation_file(generation),
            hawdb_storage::relational_row_page_artifact_file(generation),
            format!("checkpoint.{generation}.hawdb"),
            format!("wal.{generation}.hawdb"),
            format!(".checkpoint.{generation}.prepare"),
        ] {
            assert!(
                !path.join(&artifact).exists(),
                "stranded candidate {artifact}"
            );
        }
    }
    let cancellation = RuntimeCancellationToken::new();
    cancellation.cancel();
    let error = db
        .compact_relational_row_pages_context(
            Default::default(),
            &RuntimeTaskContext::without_deadline(cancellation),
        )
        .unwrap_err();
    assert!(error.to_string().contains("compaction stopped"));
    let report = db.compact_relational_row_pages(Default::default()).unwrap();
    assert_eq!(report.published_generation, generation);
    assert_eq!(report.allocated_pages, report.root_pages);
    db.scrub_storage().unwrap();
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn row_page_compaction_dirty_and_materialized_limits_release_admission() {
    use crate::{HawDBEmbedded, HawDBEmbeddedOpenOptions};
    use hawdb_qos::RuntimeGovernorConfig;
    use std::num::{NonZeroU64, NonZeroUsize};

    let path = unique_test_dir("row_page_compaction_dirty_limits");
    let mode = StorageResidencyMode::Materialized;
    drop(churn_database(&path, mode, 4));
    let mut engine = HawDBEmbedded::open_with_options(
        HawDBEmbeddedOpenOptions::new(&path)
            .with_config(open_config(mode))
            .with_runtime_governor_config(RuntimeGovernorConfig {
                cpu_slot_limit: NonZeroUsize::new(1),
                background_task_limit: NonZeroUsize::new(1),
                memory_budget_bytes: Some(1024 * 1024 * 1024),
                ..Default::default()
            }),
    )
    .unwrap();
    for table in 1..=2 {
        engine
            .database_mut()
            .query_sql(&format!(
                "UPDATE documents_{table} SET revision = 99 WHERE id = 1"
            ))
            .unwrap();
    }
    let generation = engine
        .database_mut()
        .storage_residency_report()
        .relational_rows
        .base_generation
        .unwrap();
    let epoch = engine.database_mut().commit_epoch();
    for (config, expected_error) in [
        (
            RelationalRowPageCompactionConfig {
                max_dirty_pages: NonZeroUsize::new(1).unwrap(),
                ..Default::default()
            },
            "dirty-page limit",
        ),
        (
            RelationalRowPageCompactionConfig {
                max_dirty_bytes: NonZeroU64::new(1).unwrap(),
                ..Default::default()
            },
            "checkpoint change key set retains",
        ),
        (
            RelationalRowPageCompactionConfig {
                max_materialized_checkpoint_bytes: NonZeroU64::new(1).unwrap(),
                ..Default::default()
            },
            "materialized checkpoint allowance",
        ),
    ] {
        let error = engine
            .database_mut()
            .compact_relational_row_pages(config)
            .unwrap_err();
        assert!(error.to_string().contains(expected_error), "{error}");
        let snapshot = engine.runtime_governor().snapshot();
        assert_eq!(snapshot.admissions, snapshot.completions);
        assert_eq!(snapshot.admission_rejections, 0);
        assert_eq!(snapshot.admitted_memory_bytes, 0);
        assert_eq!(snapshot.active_cpu_slots, 0);
        assert_eq!(snapshot.active_background_io_slots, 0);
        let db = engine.database_mut();
        assert_eq!(db.commit_epoch(), epoch);
        assert_eq!(
            db.storage_residency_report()
                .relational_rows
                .base_generation,
            Some(generation)
        );
        assert!(!path
            .join(format!(".checkpoint.{}.prepare", generation + 1))
            .exists());
        assert!(!path
            .join(hawdb_storage::relational_row_page_manifest_generation_file(
                generation + 1
            ))
            .exists());
    }
    let report = engine
        .database_mut()
        .compact_relational_row_pages(Default::default())
        .unwrap();
    assert_eq!(report.published_generation, generation + 1);
    assert_eq!(report.source_commit_epoch, epoch);
    assert_eq!(report.dirty_pages_written, 2);
    assert_eq!(engine.runtime_governor().snapshot().admissions, 4);
    for table in 1..=2 {
        let output = engine
            .database_mut()
            .query_sql(&format!(
                "SELECT revision FROM documents_{table} WHERE id = 1"
            ))
            .unwrap();
        assert_eq!(output.rows.len(), 1);
        assert_eq!(output.rows[0].get("revision"), Some(&Value::Int(99)));
    }
    engine.database_mut().scrub_storage().unwrap();
    drop(engine);
    std::fs::remove_dir_all(path).unwrap();
}

#[cfg(feature = "test-support")]
#[test]
fn row_page_compaction_checkpoint_failpoints_recover_one_complete_selection() {
    use crate::store::{set_checkpoint_failpoint, CheckpointPublishStage};

    for stage in [
        CheckpointPublishStage::CheckpointPersisted,
        CheckpointPublishStage::WalPrepared,
        CheckpointPublishStage::ManifestPublished,
    ] {
        let path = unique_test_dir(&format!("row_page_compaction_failpoint_{stage:?}"));
        let mode = StorageResidencyMode::OutOfCore;
        let mut db = churn_database(&path, mode, 4);
        let before = db.storage_residency_report().relational_rows;
        set_checkpoint_failpoint(Some(stage));
        let result = db.compact_relational_row_pages(Default::default());
        set_checkpoint_failpoint(None);
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("injected checkpoint failure"));
        drop(db);
        let mut reopened = Database::open_with_config(&path, open_config(mode)).unwrap();
        let recovered = reopened.storage_residency_report().relational_rows;
        if stage == CheckpointPublishStage::ManifestPublished {
            assert_eq!(
                recovered.base_generation,
                before.base_generation.map(|generation| generation + 1)
            );
            assert_eq!(recovered.allocated_page_count, recovered.root_page_count);
        } else {
            assert_eq!(recovered.base_generation, before.base_generation);
            assert_eq!(recovered.allocated_page_count, before.allocated_page_count);
        }
        assert_eq!(recovered.visible_commit_epoch, before.visible_commit_epoch);
        reopened.scrub_storage().unwrap();
        for table in 0..4 {
            let output = reopened
                .query_sql(&format!(
                    "SELECT revision FROM documents_{table} WHERE id = 1"
                ))
                .unwrap();
            assert_eq!(output.rows.len(), 1);
            assert_eq!(output.rows[0].get("revision"), Some(&Value::Int(table)));
        }
        reopened
            .compact_relational_row_pages(Default::default())
            .unwrap();
        reopened.scrub_storage().unwrap();
        drop(reopened);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn row_page_compaction_admits_before_building_and_shares_shadow_capacity() {
    use crate::{HawDBEmbedded, HawDBEmbeddedOpenOptions};
    use hawdb_qos::RuntimeGovernorConfig;
    use std::num::NonZeroUsize;

    let path = unique_test_dir("row_page_compaction_admission");
    let mode = StorageResidencyMode::OutOfCore;
    let mut db = churn_database(&path, mode, 4);
    db.query("CREATE (:CompactionShadow {id: 1})").unwrap();
    let before = db.storage_residency_report().relational_rows;
    drop(db);
    for memory_bytes in [1, 1024 * 1024 * 1024] {
        let config = DatabaseConfig {
            graph_columnar_shadow_checkpoint: true,
            ..open_config(mode)
        };
        let mut engine = HawDBEmbedded::open_with_options(
            HawDBEmbeddedOpenOptions::new(&path)
                .with_config(config)
                .with_runtime_governor_config(RuntimeGovernorConfig {
                    cpu_slot_limit: NonZeroUsize::new(1),
                    background_task_limit: NonZeroUsize::new(1),
                    memory_budget_bytes: Some(memory_bytes),
                    ..Default::default()
                }),
        )
        .unwrap();
        let result = engine
            .database_mut()
            .compact_relational_row_pages(Default::default());
        let snapshot = engine.runtime_governor().snapshot();
        assert_eq!(snapshot.active_background_tasks, 0);
        assert_eq!(snapshot.active_cpu_slots, 0);
        assert_eq!(snapshot.active_background_io_slots, 0);
        assert_eq!(snapshot.admitted_memory_bytes, 0);
        assert_eq!(snapshot.admissions, snapshot.completions);
        if memory_bytes == 1 {
            assert!(result.unwrap_err().to_string().contains("admission denied"));
            assert_eq!(snapshot.admissions, 0);
            assert_eq!(
                engine
                    .database_mut()
                    .storage_residency_report()
                    .relational_rows
                    .base_generation,
                before.base_generation
            );
        } else {
            let report = result.unwrap();
            assert_eq!(report.allocated_pages, report.root_pages);
            assert_eq!(snapshot.admissions, 1);
            assert_eq!(snapshot.admission_rejections, 0);
            assert_eq!(
                engine
                    .database_mut()
                    .columnar_shadow_checkpoint_report()
                    .unwrap()
                    .status,
                crate::store::ColumnarShadowCheckpointStatus::Published
            );
            engine.database_mut().scrub_storage().unwrap();
        }
        drop(engine);
    }
    std::fs::remove_dir_all(path).unwrap();
}
