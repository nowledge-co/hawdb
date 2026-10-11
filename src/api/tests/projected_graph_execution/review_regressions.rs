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

#[test]
fn mem_algorithm_iteration_and_final_phase_contracts_survive_both_residencies() {
    let path = unique_test_dir("mem_algorithm_reference");
    {
        let mut catalog = crate::schema::Catalog::default();
        let mut store = crate::store::GraphStore::open_with_durability(
            &path,
            &mut catalog,
            crate::DurabilityPolicy::SyncOnCheckpoint,
        )
        .unwrap();
        let ranks: Vec<_> = (0..2)
            .map(|id| {
                store
                    .create_node(
                        &mut catalog,
                        "Rank",
                        BTreeMap::from([("id".into(), Value::String(format!("rank-{id}")))]),
                    )
                    .unwrap()
            })
            .collect();
        let topics: Vec<_> = (0..9)
            .map(|id| {
                store
                    .create_node(
                        &mut catalog,
                        "Entity",
                        BTreeMap::from([("id".into(), Value::String(format!("entity-{id}")))]),
                    )
                    .unwrap()
            })
            .collect();
        // Parallel edges preserve the transition weight after projection
        // deduplication. Forced streaming is qualified by executor reference
        // tests and the separate persistent dense-graph path guard.
        for _ in 0..128 {
            store
                .create_relationship(&mut catalog, ranks[0], ranks[1], "LINK", BTreeMap::new())
                .unwrap();
            for (from, to) in [
                (0, 1),
                (1, 2),
                (2, 0),
                (3, 4),
                (4, 5),
                (5, 3),
                (6, 7),
                (7, 8),
                (8, 6),
                (2, 3),
                (5, 6),
            ] {
                store
                    .create_relationship(
                        &mut catalog,
                        topics[from],
                        topics[to],
                        "RELATES_TO",
                        BTreeMap::new(),
                    )
                    .unwrap();
            }
        }
        store.checkpoint(&catalog).unwrap();
    }
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CALL project_graph('rank', ['Rank'], ['LINK'])")
            .unwrap();
        db.query("CALL project_graph('topics', ['Entity'], ['RELATES_TO'])")
            .unwrap();
        db.checkpoint().unwrap();
    }
    let wal = read_test_wal(&path).unwrap();
    for residency in [
        StorageResidencyMode::Materialized,
        StorageResidencyMode::OutOfCore,
    ] {
        for (budget, operator) in [
            (16 * 1024 * 1024, "GraphAlgorithm"),
            (16 * 1024, "GraphAlgorithm"),
        ] {
            let mut config = DatabaseConfig {
                read_only: true,
                storage_residency_mode: residency,
                ..DatabaseConfig::default()
            };
            config.execution_memory.blocking_operator_bytes =
                std::num::NonZeroUsize::new(budget).unwrap();
            let db = Database::open_with_config(&path, config).unwrap();
            let epoch = db.commit_epoch().unwrap();
            let mut snapshot = db.begin_read_transaction().unwrap();
            for normalize in [true, false] {
                let initial = if normalize { 0.5 } else { 1.0 };
                // Independent recurrence from the pinned Ladybug source:
                // initial state counts as iteration 1; dangling mass is lost.
                let mut expected = [initial; 2];
                for iterations in 0..=3 {
                    if iterations > 1 {
                        expected = [0.15 * initial, 0.15 * initial + 0.85 * expected[0]];
                    }
                    let mut rows = Vec::new();
                    let report = snapshot.query_streaming(&format!("CALL page_rank('rank', maxIterations := {iterations}, dampingFactor := 0.85, tolerance := 0.0, normalizeInitial := {normalize}) RETURN node, node_id, node_label, pagerank_score"), QueryStreamOptions { max_rows: Some(2), max_payload_bytes: Some(4096) }, |row| { rows.push(row); Ok(()) }).unwrap();
                    assert_eq!(rows.len(), 2);
                    for row in rows {
                        let index = if row["node_id"] == Value::String("rank-0".into()) {
                            0
                        } else {
                            1
                        };
                        let Value::Float(score) = row["pagerank_score"] else {
                            panic!("missing rank")
                        };
                        assert!(
                            (score - expected[index]).abs() < 1e-12,
                            "{residency:?}/{operator}/{iterations}/{normalize}: {score} != {}",
                            expected[index]
                        );
                    }
                    assert_eq!(
                        report.execution_profile.blocking_operator_memory_reports[0].operator,
                        operator
                    );
                }
            }
            let hierarchy = snapshot.query("CALL louvain('topics', maxLevels := 20, maxIterations := 20, resolution := 0.1) RETURN node, level, louvain_id").unwrap().rows.into_rows();
            assert!(hierarchy.len() > 9, "fixture must actually contract");
            let mut final_assignments = BTreeMap::new();
            for row in hierarchy {
                final_assignments.insert(row["node"].clone(), row["louvain_id"].clone());
            }
            for query in [
                "CALL louvain('topics', maxPhases := 20, maxIterations := 20, resolution := 0.1) RETURN node, node_id, node_label, louvain_id",
                "CALL louvain('topics', maxPhases := 20, maxIterations := 20, resolution := 0.1) YIELD node AS n, louvain_id AS community WITH n, community WITH n, community RETURN n, community ORDER BY n",
            ] {
                let mut rows = Vec::new();
                let report = snapshot.query_streaming(query, QueryStreamOptions { max_rows: Some(9), max_payload_bytes: Some(16 * 1024) }, |row| { rows.push(row); Ok(()) }).unwrap();
                assert_eq!(rows.len(), 9);
                let actual: BTreeMap<_,_> = rows.iter().map(|row| (row.get("node").or_else(|| row.get("n")).unwrap().clone(), row.get("louvain_id").or_else(|| row.get("community")).unwrap().clone())).collect();
                assert_eq!(actual.len(), 9);
                assert_eq!(actual, final_assignments);
                assert_eq!(report.output_rows, 9);
            }
            assert_eq!(snapshot.commit_epoch(), epoch);
            assert_eq!(db.commit_epoch().unwrap(), epoch);
            drop(snapshot);
            drop(db);
            assert_eq!(read_test_wal(&path).unwrap(), wal);
        }
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn projected_identity_ignores_large_content_and_rejects_large_ids_without_a_prefix() {
    let path = unique_test_dir("projected_identity_admission");
    {
        let mut catalog = crate::schema::Catalog::default();
        let mut store = crate::store::GraphStore::open_with_durability(
            &path,
            &mut catalog,
            crate::DurabilityPolicy::SyncOnCheckpoint,
        )
        .unwrap();
        let nodes: Vec<_> = (0..128)
            .map(|id| {
                let identity = match id {
                    0 => "first".into(),
                    1 => "second".into(),
                    _ => format!("memory-{id}"),
                };
                let mut properties = BTreeMap::from([("id".into(), Value::String(identity))]);
                if id == 0 {
                    properties.insert(
                        "content".into(),
                        Value::String("unrelated".repeat(256 * 1024)),
                    );
                }
                store
                    .create_node(&mut catalog, "Memory", properties)
                    .unwrap()
            })
            .collect();
        store
            .create_node(
                &mut catalog,
                "Oversized",
                BTreeMap::from([("id".into(), Value::String("large-id".repeat(128 * 1024)))]),
            )
            .unwrap();
        for &from in &nodes {
            for &to in &nodes {
                if from != to {
                    store
                        .create_relationship(&mut catalog, from, to, "LINK", BTreeMap::new())
                        .unwrap();
                }
            }
        }
        store.checkpoint(&catalog).unwrap();
    }
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CALL project_graph('small', ['Memory'], ['LINK'])")
            .unwrap();
        db.query("CALL project_graph('large', ['Oversized'], ['LINK'])")
            .unwrap();
        db.checkpoint().unwrap();
    }
    let wal = read_test_wal(&path).unwrap();
    for residency in [
        StorageResidencyMode::Materialized,
        StorageResidencyMode::OutOfCore,
    ] {
        for (budget, operator) in [
            (16 * 1024 * 1024, "GraphAlgorithm"),
            (96 * 1024, "GraphAlgorithmStreaming"),
        ] {
            let mut config = DatabaseConfig {
                read_only: true,
                storage_residency_mode: residency,
                ..DatabaseConfig::default()
            };
            config.execution_memory.blocking_operator_bytes =
                std::num::NonZeroUsize::new(budget).unwrap();
            let db = Database::open_with_config(&path, config).unwrap();
            let epoch = db.commit_epoch().unwrap();
            let mut snapshot = db.begin_read_transaction().unwrap();
            let mut rows = Vec::new();
            let report = snapshot.query_streaming("CALL page_rank('small', maxIterations := 2) RETURN node, node_id, node_label, pagerank_score", QueryStreamOptions { max_rows: Some(128), max_payload_bytes: Some(64 * 1024) }, |row| { rows.push(row); Ok(()) }).unwrap();
            assert_eq!(rows.len(), 128);
            assert!(rows
                .iter()
                .all(|row| row["node_label"] == Value::String("Memory".into())));
            let identities: std::collections::BTreeSet<_> =
                rows.iter().map(|row| row["node_id"].clone()).collect();
            assert_eq!(identities.len(), 128);
            assert!(identities.contains(&Value::String("first".into())));
            assert!(identities.contains(&Value::String("second".into())));
            assert_eq!(
                report.execution_profile.blocking_operator_memory_reports[0].operator,
                operator
            );
            assert!(
                report.execution_profile.blocking_operator_memory_reports[0].peak_tracked_bytes
                    <= budget
            );
            if budget == 96 * 1024 {
                // The same graph remains queryable when identity is not requested.
                assert_eq!(snapshot.query("CALL page_rank('large', maxIterations := 1) RETURN node, pagerank_score").unwrap().rows.len(), 1);
                let mut emitted = 0;
                let error = snapshot.query_streaming("CALL page_rank('large', maxIterations := 1) RETURN node, node_id, node_label, pagerank_score", QueryStreamOptions { max_rows: Some(1), max_payload_bytes: Some(64 * 1024) }, |_| { emitted += 1; Ok(()) }).unwrap_err();
                assert!(
                    error
                        .to_string()
                        .contains("max_read_result_payload_bytes 65536"),
                    "{error}"
                );
                assert_eq!(emitted, 0);
            }
            assert_eq!(snapshot.commit_epoch(), epoch);
            assert_eq!(db.commit_epoch().unwrap(), epoch);
            drop(snapshot);
            drop(db);
            assert_eq!(read_test_wal(&path).unwrap(), wal);
        }
    }
    std::fs::remove_dir_all(path).unwrap();
}
