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

use super::*;
use crate::{
    GraphAnalyticsAlgorithm, GraphAnalyticsFreshness, GraphAnalyticsRequest, LouvainOptions,
    PageRankOptions,
};
use std::num::NonZeroUsize;

fn request(algorithm: GraphAnalyticsAlgorithm) -> GraphAnalyticsRequest {
    GraphAnalyticsRequest {
        projection: "graph".into(),
        algorithm,
        max_rows: NonZeroUsize::new(64).unwrap(),
        max_payload_bytes: NonZeroUsize::new(64 * 1024).unwrap(),
        max_staged_bytes: NonZeroUsize::new(32 * 1024).unwrap(),
    }
}

fn fixture(db: &mut Database) {
    db.query("CREATE (:Memory {id: 1, rank: -1})-[:LINK]->(:Memory {id: 2, rank: -2})")
        .unwrap();
    db.query("CALL project_graph('graph', ['Memory'], ['LINK'])")
        .unwrap();
}

fn ranks(db: &mut Database) -> Vec<BTreeMap<String, Value>> {
    db.query("MATCH (n:Memory) RETURN n.id AS id, n.rank AS rank, n.rank_computed_at_commit_epoch AS source, n.rank_published_at_commit_epoch AS published ORDER BY n.id").unwrap().rows.into_rows()
}

#[test]
fn complete_analytics_publish_atomically_and_preserve_old_readers_and_retry_outcomes() {
    let mut db = Database::new();
    fixture(&mut db);
    let epoch = db.commit_epoch().unwrap();
    let mut reader = db.begin_read_transaction().unwrap();
    let staged = db
        .prepare_graph_analytics(
            request(GraphAnalyticsAlgorithm::PageRank(PageRankOptions::default())),
            None,
        )
        .unwrap();
    assert_eq!(staged.computed_at_commit_epoch(), epoch);
    assert_eq!(staged.row_count(), 2);
    assert_eq!(db.commit_epoch().unwrap(), epoch);
    assert_eq!(ranks(&mut db)[0]["rank"], Value::Int(-1));
    let status = db.publish_graph_analytics(&staged, "rank", None).unwrap();
    assert_eq!(status.freshness, GraphAnalyticsFreshness::Fresh);
    assert_eq!(status.computed_at_commit_epoch, Some(epoch));
    assert_eq!(status.published_at_commit_epoch, Some(epoch + 1));
    assert_eq!(db.commit_epoch().unwrap(), epoch + 1);
    assert_eq!(
        db.publish_graph_analytics(&staged, "rank", None).unwrap(),
        status
    );
    assert_eq!(db.commit_epoch().unwrap(), epoch + 1);
    let old = reader
        .query("MATCH (n:Memory) RETURN n.rank AS rank ORDER BY n.id")
        .unwrap();
    assert_eq!(old.rows[0]["rank"], Value::Int(-1));
    for row in ranks(&mut db) {
        assert!(matches!(row["rank"], Value::Float(_)));
        assert_eq!(row["source"], Value::Int(epoch as i64));
        assert_eq!(row["published"], Value::Int((epoch + 1) as i64));
    }
    db.query("CREATE (:Memory {id: 3})").unwrap();
    let stale = db
        .graph_analytics_publication_status("graph", "rank")
        .unwrap();
    assert_eq!(stale.freshness, GraphAnalyticsFreshness::Stale);
    assert_eq!(
        db.publish_graph_analytics(&staged, "rank", None).unwrap(),
        stale
    );
}

#[test]
fn louvain_publication_writes_only_the_highest_level_per_original_node() {
    let mut config = DatabaseConfig::default();
    config.mutation_limits.max_operations = NonZeroUsize::new(4 * 8 + 3).unwrap();
    let mut db = Database::new_with_config(config);
    for id in 1..=8 {
        db.query_with_params(
            "CREATE (:Memory {id: $id})",
            &BTreeMap::from([("id".into(), Value::Int(id))]),
        )
        .unwrap();
    }
    for (source, target) in [
        (1, 2),
        (2, 3),
        (3, 1),
        (3, 4),
        (4, 5),
        (5, 3),
        (5, 6),
        (6, 7),
        (7, 5),
        (7, 8),
    ] {
        db.query_with_params(
            "MATCH (a:Memory {id: $source}), (b:Memory {id: $target}) CREATE (a)-[:LINK]->(b)",
            &BTreeMap::from([
                ("source".into(), Value::Int(source)),
                ("target".into(), Value::Int(target)),
            ]),
        )
        .unwrap();
    }
    db.query("CALL project_graph('graph', ['Memory'], ['LINK'])")
        .unwrap();
    let hierarchy = db
        .query("CALL louvain('graph', maxIterations := 20, maxLevels := 3) RETURN node, level, louvain_id")
        .unwrap();
    let mut expected = BTreeMap::new();
    let mut initial = BTreeMap::new();
    for row in &hierarchy.rows {
        let (Value::Int(node), Value::Int(level)) = (&row["node"], &row["level"]) else {
            panic!("invalid hierarchy identity");
        };
        if *level == 0 {
            initial.insert(*node, row["louvain_id"].clone());
        }
        let entry = expected
            .entry(*node)
            .or_insert((*level, row["louvain_id"].clone()));
        if *level > entry.0 {
            *entry = (*level, row["louvain_id"].clone());
        }
    }
    assert_eq!(expected.len(), 8);
    assert!(hierarchy.rows.len() > expected.len());
    assert!(expected
        .iter()
        .any(|(node, (_, value))| initial[node] != *value));
    let mut options = request(GraphAnalyticsAlgorithm::Louvain(LouvainOptions {
        max_iterations: 20,
        max_levels: 3,
    }));
    // Raw hierarchy output remains bounded; retained state fits only eight rows.
    options.max_rows = NonZeroUsize::new(hierarchy.rows.len()).unwrap();
    options.max_staged_bytes = NonZeroUsize::new(1024 + 128 * 8).unwrap();
    let prepared = db.prepare_graph_analytics(options, None).unwrap();
    assert_eq!(prepared.row_count(), expected.len());
    assert_eq!(
        prepared.execution_report().output_rows,
        hierarchy.rows.len()
    );
    let status = db
        .publish_graph_analytics(&prepared, "community_id", None)
        .unwrap();
    assert_eq!(status.freshness, GraphAnalyticsFreshness::Fresh);
    let published = db
        .query("MATCH (n:Memory) RETURN id(n) AS node, n.community_id AS community")
        .unwrap();
    assert_eq!(published.rows.len(), expected.len());
    for row in &published.rows {
        let Value::Int(node) = row["node"] else {
            panic!("invalid published node ID");
        };
        assert_eq!(row["community"], expected[&node].1);
    }
    let metadata = db
        .query_sql("SELECT row_count FROM __hawdb_analytics_publications")
        .unwrap();
    assert_eq!(metadata.rows[0]["row_count"], Value::Int(8));
    assert_eq!(
        db.publish_graph_analytics(&prepared, "community_id", None)
            .unwrap(),
        status
    );
}

#[test]
fn publication_identity_filters_old_values_after_projection_redefinition() {
    let mut db = Database::new();
    fixture(&mut db);
    let options = request(GraphAnalyticsAlgorithm::PageRank(PageRankOptions::default()));
    let old = db.prepare_graph_analytics(options.clone(), None).unwrap();
    db.publish_graph_analytics(&old, "rank", None).unwrap();
    let old_values = ranks(&mut db);
    db.query("CREATE (:Included {id: 3})").unwrap();
    db.query("CALL project_graph('graph', ['Included'], ['LINK'])")
        .unwrap();
    let new = db.prepare_graph_analytics(options, None).unwrap();
    assert_eq!(new.row_count(), 1);
    let status = db.publish_graph_analytics(&new, "rank", None).unwrap();
    assert_eq!(status.freshness, GraphAnalyticsFreshness::Fresh);
    assert_eq!(ranks(&mut db), old_values);
    assert_ne!(status.publication_id.as_deref(), Some(old.publication_id()));
    let mut snapshot = db.begin_read_transaction().unwrap();
    assert_eq!(snapshot.commit_epoch(), status.current_commit_epoch);
    let mut current = Vec::new();
    snapshot
        .query_with_params_streaming(
            "MATCH (n) WHERE n.rank_publication_id = $publication RETURN n.id AS id, n.rank AS rank",
            &BTreeMap::from([(
                "publication".into(),
                Value::String(status.publication_id.unwrap()),
            )]),
            QueryStreamOptions {
                max_rows: Some(1),
                max_payload_bytes: Some(4096),
            },
            |row| {
                current.push(row);
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(current.len(), 1);
    assert_eq!(current[0]["id"], Value::Int(3));
    assert!(matches!(current[0]["rank"], Value::Float(_)));
}

#[test]
fn changed_source_different_database_cancellation_and_budgets_keep_old_results() {
    let mut db = Database::new();
    fixture(&mut db);
    let prepared = db
        .prepare_graph_analytics(
            request(GraphAnalyticsAlgorithm::PageRank(PageRankOptions::default())),
            None,
        )
        .unwrap();
    let token = hawdb_core::RuntimeCancellationToken::new();
    token.cancel();
    let context = hawdb_core::RuntimeTaskContext::without_deadline(token);
    let before = ranks(&mut db);
    let epoch = db.commit_epoch().unwrap();
    assert!(db
        .publish_graph_analytics(&prepared, "rank", Some(&context))
        .unwrap_err()
        .to_string()
        .contains("cancelled"));
    assert_eq!(ranks(&mut db), before);
    assert_eq!(db.commit_epoch().unwrap(), epoch);
    let mut other = Database::new();
    fixture(&mut other);
    assert!(other
        .publish_graph_analytics(&prepared, "rank", None)
        .unwrap_err()
        .to_string()
        .contains("different database"));
    db.query("CREATE (:Source {id: 9})").unwrap();
    assert!(db
        .publish_graph_analytics(&prepared, "rank", None)
        .unwrap_err()
        .to_string()
        .contains("source epoch changed"));
    assert_eq!(ranks(&mut db), before);
    let epoch = db.commit_epoch().unwrap();
    for bound in 0..3 {
        let mut options = request(GraphAnalyticsAlgorithm::PageRank(PageRankOptions::default()));
        match bound {
            0 => options.max_rows = NonZeroUsize::MIN,
            1 => options.max_payload_bytes = NonZeroUsize::MIN,
            _ => options.max_staged_bytes = NonZeroUsize::new(1024).unwrap(),
        }
        assert!(db.prepare_graph_analytics(options, None).is_err());
        assert_eq!(ranks(&mut db), before);
        assert_eq!(db.commit_epoch().unwrap(), epoch);
    }
}

#[test]
fn constraint_failure_rolls_back_all_staged_scores_and_metadata() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, rank: -1})").unwrap();
    db.query("CREATE (:Memory {id: 2, rank: -2})").unwrap();
    db.query("CREATE CONSTRAINT ON :Memory(rank) ASSERT UNIQUE")
        .unwrap();
    db.query("CALL project_graph('graph', ['Memory'], ['Missing'])")
        .unwrap();
    let prepared = db
        .prepare_graph_analytics(
            request(GraphAnalyticsAlgorithm::PageRank(PageRankOptions::default())),
            None,
        )
        .unwrap();
    let before = ranks(&mut db);
    let epoch = db.commit_epoch().unwrap();
    assert!(db.publish_graph_analytics(&prepared, "rank", None).is_err());
    assert_eq!(db.commit_epoch().unwrap(), epoch);
    assert_eq!(ranks(&mut db), before);
    assert_eq!(
        db.graph_analytics_publication_status("graph", "rank")
            .unwrap()
            .freshness,
        GraphAnalyticsFreshness::Unavailable
    );
}

#[test]
fn background_admission_covers_staging_and_publication() {
    let mut db = Database::new_with_config(DatabaseConfig {
        local_qos_policy: crate::LocalQosPolicy {
            max_background_operations: Some(5),
            ..crate::LocalQosPolicy::default()
        },
        ..DatabaseConfig::default()
    });
    fixture(&mut db);
    let before = ranks(&mut db);
    let epoch = db.commit_epoch().unwrap();
    // Three source records fit; eleven publication operations do not.
    let prepared = db
        .prepare_graph_analytics(
            request(GraphAnalyticsAlgorithm::PageRank(PageRankOptions::default())),
            None,
        )
        .unwrap();
    assert!(db
        .publish_graph_analytics(&prepared, "rank", None)
        .unwrap_err()
        .to_string()
        .contains("publication not admitted"));
    assert_eq!(ranks(&mut db), before);
    assert_eq!(db.commit_epoch().unwrap(), epoch);
    for id in 3..=5 {
        db.query_with_params(
            "CREATE (:Memory {id: $id})",
            &BTreeMap::from([("id".into(), Value::Int(id))]),
        )
        .unwrap();
    }
    let epoch = db.commit_epoch().unwrap();
    assert!(db
        .prepare_graph_analytics(
            request(GraphAnalyticsAlgorithm::PageRank(PageRankOptions::default())),
            None,
        )
        .unwrap_err()
        .to_string()
        .contains("graph analytics not admitted"));
    assert_eq!(db.commit_epoch().unwrap(), epoch);
}

#[test]
fn score_and_community_provenance_recover_together_under_both_durability_policies() {
    for policy in [
        crate::DurabilityPolicy::SyncOnEveryWrite,
        crate::DurabilityPolicy::SyncOnCheckpoint,
    ] {
        let path = unique_test_dir(&format!("analytics_publish_{policy:?}"));
        let config = DatabaseConfig::default();
        let (rank_status, community_status, values) = {
            let mut db =
                Database::open_with_durability_and_config(&path, policy, config.clone()).unwrap();
            fixture(&mut db);
            let rank = db
                .prepare_graph_analytics(
                    request(GraphAnalyticsAlgorithm::PageRank(PageRankOptions::default())),
                    None,
                )
                .unwrap();
            let rank_status = db.publish_graph_analytics(&rank, "rank", None).unwrap();
            let community = db
                .prepare_graph_analytics(
                    request(GraphAnalyticsAlgorithm::Louvain(LouvainOptions {
                        max_iterations: 3,
                        max_levels: 2,
                    })),
                    None,
                )
                .unwrap();
            let community_status = db
                .publish_graph_analytics(&community, "community_id", None)
                .unwrap();
            let values = ranks(&mut db);
            (rank_status, community_status, values)
        };
        for checkpoint in [false, true] {
            let mut db =
                Database::open_with_durability_and_config(&path, policy, config.clone()).unwrap();
            assert_eq!(ranks(&mut db), values);
            let rank = db
                .graph_analytics_publication_status("graph", "rank")
                .unwrap();
            assert_eq!(rank.publication_id, rank_status.publication_id);
            assert_eq!(
                rank.computed_at_commit_epoch,
                rank_status.computed_at_commit_epoch
            );
            assert_eq!(rank.freshness, GraphAnalyticsFreshness::Stale);
            assert_eq!(
                db.graph_analytics_publication_status("graph", "community_id")
                    .unwrap(),
                community_status
            );
            if checkpoint {
                db.checkpoint().unwrap();
            }
        }
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn torn_later_tail_preserves_complete_analytics_publication() {
    use std::io::Write;
    let path = unique_test_dir("analytics_torn_later_tail");
    let (expected, status) = {
        let mut db = Database::open(&path).unwrap();
        fixture(&mut db);
        db.checkpoint().unwrap();
        let staged = db
            .prepare_graph_analytics(
                request(GraphAnalyticsAlgorithm::PageRank(PageRankOptions::default())),
                None,
            )
            .unwrap();
        let status = db.publish_graph_analytics(&staged, "rank", None).unwrap();
        (ranks(&mut db), status)
    };
    // A later uncommitted tail cannot discard the completed analytics barrier.
    std::fs::OpenOptions::new()
        .append(true)
        .open(active_wal_path(&path))
        .unwrap()
        .write_all(b"partial")
        .unwrap();
    let mut db = Database::open_with_config(
        &path,
        DatabaseConfig {
            recovery_mode: crate::RecoveryMode::AutoRepairTornTail,
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    assert_eq!(ranks(&mut db), expected);
    assert_eq!(
        db.graph_analytics_publication_status("graph", "rank")
            .unwrap(),
        status
    );
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[cfg(feature = "test-support")]
#[test]
fn partial_publication_wal_write_retains_all_previous_values_and_metadata() {
    for policy in [
        crate::DurabilityPolicy::SyncOnEveryWrite,
        crate::DurabilityPolicy::SyncOnCheckpoint,
    ] {
        let path = unique_test_dir("analytics_partial_write");
        let mut db = Database::open_with_durability(&path, policy).unwrap();
        fixture(&mut db);
        let prepared = db
            .prepare_graph_analytics(
                request(GraphAnalyticsAlgorithm::PageRank(PageRankOptions::default())),
                None,
            )
            .unwrap();
        let before = ranks(&mut db);
        let epoch = db.commit_epoch().unwrap();
        crate::store::set_wal_append_failpoint(crate::store::WalAppendFailure::PartialWrite);
        assert!(db.publish_graph_analytics(&prepared, "rank", None).is_err());
        assert_eq!(db.commit_epoch().unwrap(), epoch);
        assert_eq!(ranks(&mut db), before);
        assert_eq!(
            db.graph_analytics_publication_status("graph", "rank")
                .unwrap()
                .freshness,
            GraphAnalyticsFreshness::Unavailable
        );
        drop(prepared);
        drop(db);
        let mut db = Database::open_with_durability(&path, policy).unwrap();
        assert_eq!(ranks(&mut db), before);
        assert_eq!(
            db.graph_analytics_publication_status("graph", "rank")
                .unwrap()
                .freshness,
            GraphAnalyticsFreshness::Unavailable
        );
        drop(db);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
#[test]
fn rss_child() {
    let Some(path) = std::env::var_os("HAWDB_ANALYTICS_RSS_FIXTURE") else {
        return;
    };
    let path = std::path::PathBuf::from(path);
    let mut config = DatabaseConfig {
        read_only: true,
        storage_residency_mode: crate::StorageResidencyMode::OutOfCore,
        segment_cache_capacity_bytes: 2 * 1024 * 1024,
        ..DatabaseConfig::default()
    };
    config.execution_memory.blocking_operator_bytes = NonZeroUsize::new(96 * 1024).unwrap();
    config.execution_memory.query_memory_bytes = NonZeroUsize::new(4 * 1024 * 1024).unwrap();
    config.execution_memory.batch_rows = NonZeroUsize::new(7).unwrap();
    let db = Database::open_with_config(&path, config).unwrap();
    // Include the reader's fixed optimizer/runtime state in the baseline, then
    // measure the additional memory needed by the edge-heavy algorithms.
    let mut reader = db.begin_read_transaction().unwrap();
    let before = crate::ProcessMemorySnapshot::capture().unwrap();
    let epoch = db.commit_epoch().unwrap();
    let mut records = Vec::new();
    for (algorithm, columns, count) in [
        ("page_rank", "node, pagerank_score", 128),
        ("louvain", "node, level, louvain_id", 256),
    ] {
        let bytes_before = db
            .storage_residency_report()
            .unwrap()
            .graph_index_reads
            .adjacency_bytes_read;
        let started = std::time::Instant::now();
        let report = reader.query_streaming(&format!("CALL {algorithm}('graph', maxIterations := 3, maxLevels := 2) RETURN {columns}"), QueryStreamOptions {
            max_rows: Some(256), max_payload_bytes: Some(64 * 1024),
        }, |_| Ok(())).unwrap();
        assert_eq!(report.output_rows, count);
        assert!(report
            .execution_profile
            .blocking_operator_memory_reports
            .iter()
            .any(|report| report.operator == "GraphAlgorithmStreaming"));
        assert!(report
            .execution_profile
            .blocking_operator_memory_reports
            .iter()
            .all(|report| report.peak_tracked_bytes <= report.budget_bytes));
        let memory = crate::ProcessMemorySnapshot::capture().unwrap();
        // RSS includes the bounded storage cache, query ledger and runtime
        // allocator slack; it is deliberately not equated with tracked state.
        assert!(
            memory.peak_resident_bytes <= before.peak_resident_bytes + 12 * 1024 * 1024,
            "{algorithm}: baseline {before:?}, after {memory:?}, execution {:?}, storage {:?}",
            report.execution_profile.pipeline_memory_report,
            db.storage_residency_report().unwrap(),
        );
        let bytes_read = db
            .storage_residency_report()
            .unwrap()
            .graph_index_reads
            .adjacency_bytes_read
            - bytes_before;
        assert!(bytes_read > 0);
        records.push(serde_json::json!({"algorithm": algorithm, "nodes": 128, "relationships": 16256,
            "blocking_budget": 98304, "peak_rss": memory.peak_resident_bytes, "baseline_rss": before.peak_resident_bytes,
            "adjacency_bytes_read": bytes_read, "wall_nanos": started.elapsed().as_nanos(),
            "query_peak": report.execution_profile.pipeline_memory_report.query_memory_peak_bytes}));
    }
    assert_eq!(db.commit_epoch().unwrap(), epoch);
    std::fs::write(
        path.join("analytics-rss.json"),
        serde_json::to_vec_pretty(&records).unwrap(),
    )
    .unwrap();
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
#[test]
fn persistent_edges_stream_with_bounded_rss_in_an_isolated_process() {
    let path = unique_test_dir("analytics_streaming_rss");
    {
        // Fixture preparation uses the existing library storage path; the
        // measured child opens and queries through the normal Database facade.
        let mut catalog = crate::schema::Catalog::default();
        let mut store = crate::store::GraphStore::open_with_durability(
            &path,
            &mut catalog,
            crate::DurabilityPolicy::SyncOnCheckpoint,
        )
        .unwrap();
        let nodes: Vec<_> = (0..128)
            .map(|id| {
                store
                    .create_node(
                        &mut catalog,
                        "Memory",
                        BTreeMap::from([("id".into(), Value::Int(id))]),
                    )
                    .unwrap()
            })
            .collect();
        for &source in &nodes {
            for &target in &nodes {
                if source == target {
                    continue;
                }
                store
                    .create_relationship(&mut catalog, source, target, "LINK", BTreeMap::new())
                    .unwrap();
            }
        }
        store.checkpoint(&catalog).unwrap();
    }
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CALL project_graph('graph', ['Memory'], ['LINK'])")
            .unwrap();
        db.checkpoint().unwrap();
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "api::tests::graph_analytics::rss_child",
            "--nocapture",
        ])
        .env("HAWDB_ANALYTICS_RSS_FIXTURE", &path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let evidence = std::fs::read_to_string(path.join("analytics-rss.json")).unwrap();
    eprintln!("persistent-analytics-rss {evidence}");
    std::fs::remove_dir_all(path).unwrap();
}

fn runtime_admission_governor(
    memory_bytes: u64,
    policy: Option<hawdb_qos::ProcessMemoryPolicy>,
) -> hawdb_qos::RuntimeGovernor {
    let config = hawdb_qos::RuntimeGovernorConfig {
        memory_budget_bytes: Some(memory_bytes),
        background_task_limit: NonZeroUsize::new(1),
        ..hawdb_qos::RuntimeGovernorConfig::shared_host()
    };
    let resources = hawdb_qos::RuntimeResourceSnapshot::from_parts(
        hawdb_qos::RuntimeResourceBudget::from_limits(NonZeroUsize::new(8).unwrap(), None, None),
        hawdb_qos::RuntimeMemorySnapshot::from_limits(
            Some(8 << 30),
            Some(8 << 30),
            None,
            None,
            None,
        ),
    );
    match policy {
        Some(policy) => hawdb_qos::RuntimeGovernor::new_with_process_memory_policy(
            config,
            resources,
            hawdb_qos::IoConcurrencyBudget::new(8, 2),
            policy,
        ),
        None => hawdb_qos::RuntimeGovernor::new(
            config,
            resources,
            hawdb_qos::IoConcurrencyBudget::new(8, 2),
        ),
    }
}

fn assert_runtime_idle(governor: &hawdb_qos::RuntimeGovernor, retained_bytes: u64) {
    let snapshot = governor.snapshot();
    assert_eq!(snapshot.active_foreground_tasks, 0);
    assert_eq!(snapshot.active_background_tasks, 0);
    assert_eq!(snapshot.active_blocking_tasks, 0);
    assert_eq!(snapshot.active_cpu_slots, 0);
    assert_eq!(snapshot.active_foreground_io_slots, 0);
    assert_eq!(snapshot.active_background_io_slots, 0);
    assert_eq!(snapshot.queued_admission_waiters, 0);
    assert_eq!(snapshot.admitted_memory_bytes, retained_bytes);
    assert_eq!(snapshot.admissions, snapshot.completions);
}

#[test]
fn runtime_admission_rejects_staging_larger_than_the_complete_caller_budget() {
    let mut db = Database::new();
    fixture(&mut db);
    let before = ranks(&mut db);
    let epoch = db.commit_epoch().unwrap();
    let context = hawdb_core::RuntimeTaskContext::default().with_memory_reservation(
        hawdb_core::RuntimeMemoryReservation::new(32 * 1024 * 1024, 64 * 1024),
    );
    let mut options = request(GraphAnalyticsAlgorithm::PageRank(PageRankOptions::default()));
    options.max_staged_bytes = NonZeroUsize::new(64 * 1024 * 1024).unwrap();
    assert!(db.prepare_graph_analytics(options, Some(&context)).is_err());
    assert_eq!(ranks(&mut db), before);
    assert_eq!(db.commit_epoch().unwrap(), epoch);
}

#[test]
fn runtime_admission_query_and_staging_share_the_caller_budget() {
    let mut db = Database::new();
    fixture(&mut db);
    let epoch = db.commit_epoch().unwrap();
    let caller_bytes = 64 * 1024 * 1024;
    let staged_bytes = 32 * 1024 * 1024;
    let context = hawdb_core::RuntimeTaskContext::default().with_memory_reservation(
        hawdb_core::RuntimeMemoryReservation::new(caller_bytes, 64 * 1024),
    );
    let mut options = request(GraphAnalyticsAlgorithm::PageRank(PageRankOptions::default()));
    options.max_staged_bytes = NonZeroUsize::new(staged_bytes).unwrap();
    let prepared = db.prepare_graph_analytics(options, Some(&context)).unwrap();
    let report = &prepared
        .execution_report()
        .execution_profile
        .pipeline_memory_report;
    assert_eq!(prepared.row_count(), 2);
    assert!(report.query_memory_budget_bytes as u64 + staged_bytes as u64 <= caller_bytes);
    assert_eq!(db.commit_epoch().unwrap(), epoch);
}

#[test]
fn runtime_admission_larger_caller_reservation_keeps_the_configured_budget() {
    let mut db = Database::new();
    fixture(&mut db);
    let configured_bytes = db.config.execution_memory.query_memory_bytes.get();
    let context = hawdb_core::RuntimeTaskContext::default().with_memory_reservation(
        hawdb_core::RuntimeMemoryReservation::new(512 * 1024 * 1024, 64 * 1024),
    );
    let options = request(GraphAnalyticsAlgorithm::PageRank(PageRankOptions::default()));
    let staged_bytes = options.max_staged_bytes.get();
    let prepared = db.prepare_graph_analytics(options, Some(&context)).unwrap();
    let report = &prepared
        .execution_report()
        .execution_profile
        .pipeline_memory_report;
    assert!(report.query_memory_budget_bytes + staged_bytes <= configured_bytes);
}

#[test]
fn runtime_admission_busy_background_slot_rejects_preparation() {
    let mut db = Database::new();
    fixture(&mut db);
    let before = ranks(&mut db);
    let epoch = db.commit_epoch().unwrap();
    let governor = runtime_admission_governor(512 * 1024 * 1024, None);
    db.set_runtime_governor(governor.clone());
    let held = governor
        .try_admit(hawdb_qos::RuntimeWorkRequest::background_maintenance(1024))
        .unwrap();
    assert!(db
        .prepare_graph_analytics(
            request(GraphAnalyticsAlgorithm::PageRank(PageRankOptions::default())),
            None,
        )
        .is_err());
    assert_eq!(governor.snapshot().active_background_tasks, 1);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 1024);
    assert_eq!(ranks(&mut db), before);
    assert_eq!(db.commit_epoch().unwrap(), epoch);
    drop(held);
    assert_runtime_idle(&governor, 0);
}

#[test]
fn runtime_admission_retained_results_release_slots_and_bound_multiple_preparations() {
    let mut config = DatabaseConfig::default();
    config.execution_memory.query_memory_bytes = NonZeroUsize::new(32 * 1024 * 1024).unwrap();
    let mut db = Database::new_with_config(config);
    fixture(&mut db);
    let epoch = db.commit_epoch().unwrap();
    let staging_bytes = 16 * 1024 * 1024;
    let governor = runtime_admission_governor(32 * 1024 * 1024 + 64 * 1024, None);
    db.set_runtime_governor(governor.clone());
    let mut options = request(GraphAnalyticsAlgorithm::PageRank(PageRankOptions::default()));
    options.max_staged_bytes = NonZeroUsize::new(staging_bytes).unwrap();
    let first = db.prepare_graph_analytics(options.clone(), None).unwrap();
    assert_runtime_idle(&governor, staging_bytes as u64);
    assert_eq!(governor.snapshot().admissions, 1);
    assert!(db.prepare_graph_analytics(options.clone(), None).is_err());
    assert_runtime_idle(&governor, staging_bytes as u64);
    assert_eq!(first.row_count(), 2);
    assert_eq!(db.commit_epoch().unwrap(), epoch);
    drop(first);
    assert_runtime_idle(&governor, 0);
    let next = db.prepare_graph_analytics(options, None).unwrap();
    assert_runtime_idle(&governor, staging_bytes as u64);
    drop(next);
    assert_runtime_idle(&governor, 0);
}

#[test]
fn runtime_admission_busy_publication_preserves_values_epochs_and_wal_in_both_residencies() {
    for residency in [
        crate::StorageResidencyMode::Materialized,
        crate::StorageResidencyMode::OutOfCore,
    ] {
        let path = unique_test_dir("analytics_runtime_admission");
        let config = DatabaseConfig {
            storage_residency_mode: residency,
            ..DatabaseConfig::default()
        };
        let mut db = Database::open_with_config(&path, config.clone()).unwrap();
        fixture(&mut db);
        db.checkpoint().unwrap();
        drop(db);
        let mut db = Database::open_with_config(&path, config).unwrap();
        assert_eq!(
            db.storage_residency_report().unwrap().out_of_core,
            residency == crate::StorageResidencyMode::OutOfCore,
        );
        let before = ranks(&mut db);
        let epoch = db.commit_epoch().unwrap();
        let wal = std::fs::read(active_wal_path(&path)).unwrap();
        let governor = runtime_admission_governor(512 * 1024 * 1024, None);
        db.set_runtime_governor(governor.clone());
        let prepared = db
            .prepare_graph_analytics(
                request(GraphAnalyticsAlgorithm::PageRank(PageRankOptions::default())),
                None,
            )
            .unwrap();
        let held = governor
            .try_admit(hawdb_qos::RuntimeWorkRequest::background_maintenance(1024))
            .unwrap();
        assert!(db.publish_graph_analytics(&prepared, "rank", None).is_err());
        assert_eq!(ranks(&mut db), before);
        assert_eq!(db.commit_epoch().unwrap(), epoch);
        assert_eq!(std::fs::read(active_wal_path(&path)).unwrap(), wal);
        assert_eq!(
            db.graph_analytics_publication_status("graph", "rank")
                .unwrap()
                .freshness,
            GraphAnalyticsFreshness::Unavailable
        );
        drop(held);
        assert_runtime_idle(&governor, 32 * 1024);
        let admissions = governor.snapshot().admissions;
        let published = db.publish_graph_analytics(&prepared, "rank", None).unwrap();
        assert_eq!(published.freshness, GraphAnalyticsFreshness::Fresh);
        assert_runtime_idle(&governor, 32 * 1024);
        assert_eq!(governor.snapshot().admissions, admissions + 1);
        let values = ranks(&mut db);
        assert!(values
            .iter()
            .all(|row| matches!(row["rank"], Value::Float(_))));
        drop(prepared);
        assert_runtime_idle(&governor, 0);
        drop(db);
        let mut reopened = Database::open_with_config(
            &path,
            DatabaseConfig {
                read_only: true,
                storage_residency_mode: residency,
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        assert_eq!(
            reopened.storage_residency_report().unwrap().out_of_core,
            residency == crate::StorageResidencyMode::OutOfCore,
        );
        assert_eq!(ranks(&mut reopened), values);
        assert_eq!(
            reopened
                .graph_analytics_publication_status("graph", "rank")
                .unwrap(),
            published
        );
        drop(reopened);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn runtime_admission_failure_and_parent_controls_do_not_leak_resources() {
    let mut db = Database::new();
    fixture(&mut db);
    let before = ranks(&mut db);
    let epoch = db.commit_epoch().unwrap();
    let governor = runtime_admission_governor(512 * 1024 * 1024, None);
    db.set_runtime_governor(governor.clone());
    for bound in 0..3 {
        let mut options = request(GraphAnalyticsAlgorithm::PageRank(PageRankOptions::default()));
        match bound {
            0 => options.max_rows = NonZeroUsize::MIN,
            1 => options.max_payload_bytes = NonZeroUsize::MIN,
            _ => options.max_staged_bytes = NonZeroUsize::new(1024).unwrap(),
        }
        let admissions = governor.snapshot().admissions;
        assert!(db.prepare_graph_analytics(options, None).is_err());
        assert_runtime_idle(&governor, 0);
        assert_eq!(governor.snapshot().admissions, admissions + 1);
    }
    let cancellation = hawdb_core::RuntimeCancellationToken::new();
    cancellation.cancel();
    for context in [
        hawdb_core::RuntimeTaskContext::without_deadline(cancellation),
        hawdb_core::RuntimeTaskContext::with_timeout(std::time::Duration::ZERO),
        hawdb_core::RuntimeTaskContext::default().with_memory_reservation(
            hawdb_core::RuntimeMemoryReservation::new(32 * 1024 * 1024, 0),
        ),
    ] {
        assert!(db
            .prepare_graph_analytics(
                request(GraphAnalyticsAlgorithm::PageRank(PageRankOptions::default())),
                Some(&context),
            )
            .is_err());
        assert_runtime_idle(&governor, 0);
    }
    assert_eq!(ranks(&mut db), before);
    assert_eq!(db.commit_epoch().unwrap(), epoch);
}

#[test]
fn runtime_admission_retained_results_remain_charged_to_the_shared_process_policy() {
    let policy = hawdb_qos::ProcessMemoryPolicy::new(hawdb_qos::ProcessMemoryPolicyConfig::new(
        std::num::NonZeroU64::new(512 * 1024 * 1024).unwrap(),
    ));
    policy.update(hawdb_qos::ProcessMemorySnapshot {
        capabilities: hawdb_qos::ProcessMemoryCapabilities {
            resident_memory: true,
            ..Default::default()
        },
        resident_bytes: 1024,
        peak_resident_bytes: 1024,
        total_page_faults: None,
        minor_page_faults: None,
        major_page_faults: None,
    });
    let governor = runtime_admission_governor(512 * 1024 * 1024, Some(policy.clone()));
    let mut db = Database::new();
    fixture(&mut db);
    db.set_runtime_governor(governor.clone());
    let options = request(GraphAnalyticsAlgorithm::PageRank(PageRankOptions::default()));
    let prepared = db.prepare_graph_analytics(options.clone(), None).unwrap();
    assert_eq!(policy.snapshot().unobserved_reserved_bytes, 32 * 1024);
    assert_runtime_idle(&governor, 32 * 1024);
    policy.clear_sample();
    assert!(db.prepare_graph_analytics(options, None).is_err());
    assert_runtime_idle(&governor, 32 * 1024);
    drop(prepared);
    assert_eq!(policy.snapshot().unobserved_reserved_bytes, 0);
    assert_runtime_idle(&governor, 0);
}

fn runtime_admission_busy_resource(kind: u8, code: &str) {
    let mut db = Database::new();
    fixture(&mut db);
    let before = ranks(&mut db);
    let epoch = db.commit_epoch().unwrap();
    let governor = runtime_admission_governor(512 * 1024 * 1024, None);
    let governor = if kind == 1 {
        hawdb_qos::RuntimeGovernor::new(
            hawdb_qos::RuntimeGovernorConfig {
                memory_budget_bytes: Some(512 * 1024 * 1024),
                background_task_limit: NonZeroUsize::new(2),
                ..hawdb_qos::RuntimeGovernorConfig::shared_host()
            },
            governor.snapshot().resources,
            hawdb_qos::IoConcurrencyBudget::new(8, 2),
        )
    } else {
        governor
    };
    db.set_runtime_governor(governor.clone());
    let options = request(GraphAnalyticsAlgorithm::PageRank(PageRankOptions::default()));
    let prepared = db.prepare_graph_analytics(options.clone(), None).unwrap();
    let limits = governor.snapshot().limits;
    let (count, held_request) = match kind {
        0 => (
            1,
            hawdb_qos::RuntimeWorkRequest::foreground_query(0, 0)
                .with_cpu_slots(limits.effective_cpu_slots.get())
                .with_blocking(false),
        ),
        1 => (
            1,
            hawdb_qos::RuntimeWorkRequest::io(
                hawdb_qos::RuntimeWorkPriority::Background,
                limits.background_io_depth.get(),
                0,
            ),
        ),
        _ => (
            limits.blocking_task_limit.get(),
            hawdb_qos::RuntimeWorkRequest::foreground_query(0, 0).with_cpu_slots(0),
        ),
    };
    let held: Vec<_> = (0..count)
        .map(|_| governor.try_admit(held_request).unwrap())
        .collect();
    let busy = governor.snapshot();
    assert!(db
        .prepare_graph_analytics(options, None)
        .unwrap_err()
        .to_string()
        .contains(code));
    assert!(db
        .publish_graph_analytics(&prepared, "rank", None)
        .unwrap_err()
        .to_string()
        .contains(code));
    let rejected = governor.snapshot();
    assert_eq!(
        rejected.active_foreground_tasks,
        busy.active_foreground_tasks
    );
    assert_eq!(
        rejected.active_background_tasks,
        busy.active_background_tasks
    );
    assert_eq!(rejected.active_blocking_tasks, busy.active_blocking_tasks);
    assert_eq!(rejected.active_cpu_slots, busy.active_cpu_slots);
    assert_eq!(
        rejected.active_foreground_io_slots,
        busy.active_foreground_io_slots
    );
    assert_eq!(
        rejected.active_background_io_slots,
        busy.active_background_io_slots
    );
    assert_eq!(rejected.admitted_memory_bytes, busy.admitted_memory_bytes);
    assert_eq!(rejected.admissions, busy.admissions);
    assert_eq!(rejected.completions, busy.completions);
    assert_eq!(ranks(&mut db), before);
    assert_eq!(db.commit_epoch().unwrap(), epoch);
    drop(held);
    assert_runtime_idle(&governor, 32 * 1024);
    drop(prepared);
    assert_runtime_idle(&governor, 0);
}

#[test]
fn runtime_admission_busy_cpu_rejects_both_phases_without_revoking_foreground_work() {
    runtime_admission_busy_resource(0, "cpu_saturated");
}

#[test]
fn runtime_admission_busy_background_io_rejects_both_phases_without_revoking_existing_work() {
    runtime_admission_busy_resource(1, "io_saturated");
}

#[test]
fn runtime_admission_busy_blocking_pool_rejects_both_phases_without_revoking_foreground_work() {
    runtime_admission_busy_resource(2, "blocking_task_saturated");
}

#[test]
fn runtime_admission_publication_checks_the_complete_caller_budget_before_mutation() {
    for governed in [false, true] {
        let mut db = Database::new();
        fixture(&mut db);
        let governor = runtime_admission_governor(512 * 1024 * 1024, None);
        if governed {
            db.set_runtime_governor(governor.clone());
        }
        let prepared = db
            .prepare_graph_analytics(
                request(GraphAnalyticsAlgorithm::PageRank(PageRankOptions::default())),
                None,
            )
            .unwrap();
        let before = ranks(&mut db);
        let epoch = db.commit_epoch().unwrap();
        let context = hawdb_core::RuntimeTaskContext::default().with_memory_reservation(
            hawdb_core::RuntimeMemoryReservation::new(32 * 1024 * 1024, 64 * 1024),
        );
        assert!(db
            .publish_graph_analytics(&prepared, "rank", Some(&context))
            .unwrap_err()
            .to_string()
            .contains("caller memory reservation"));
        assert_eq!(ranks(&mut db), before);
        assert_eq!(db.commit_epoch().unwrap(), epoch);
        assert_runtime_idle(&governor, if governed { 32 * 1024 } else { 0 });
        let context = hawdb_core::RuntimeTaskContext::default().with_memory_reservation(
            hawdb_core::RuntimeMemoryReservation::new(256 * 1024 * 1024, 64 * 1024),
        );
        assert_eq!(
            db.publish_graph_analytics(&prepared, "rank", Some(&context))
                .unwrap()
                .freshness,
            GraphAnalyticsFreshness::Fresh
        );
        assert_runtime_idle(&governor, if governed { 32 * 1024 } else { 0 });
        drop(prepared);
        assert_runtime_idle(&governor, 0);
    }
}
