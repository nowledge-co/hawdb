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
fn projected_graph_streaming_predicate_options_identity_parity_survives_both_residencies() {
    let path = unique_test_dir("projected_graph_streaming_parity");
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
                store
                    .create_node(
                        &mut catalog,
                        "Memory",
                        BTreeMap::from([("id".into(), Value::String(format!("memory-{id}")))]),
                    )
                    .unwrap()
            })
            .collect();
        for (source, &from) in nodes.iter().enumerate() {
            for (target, &to) in nodes.iter().enumerate() {
                if source == target {
                    continue;
                }
                let status = if source / 64 == target / 64 {
                    "active"
                } else {
                    "inactive"
                };
                let weight = if source == 0 && target == 1 {
                    0.69
                } else {
                    0.7
                };
                store
                    .create_relationship(
                        &mut catalog,
                        from,
                        to,
                        "LINK",
                        BTreeMap::from([
                            ("status".into(), Value::String(status.into())),
                            ("weight".into(), Value::Float(weight)),
                        ]),
                    )
                    .unwrap();
            }
        }
        store.checkpoint(&catalog).unwrap();
    }
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CALL project_graph('filtered', ['Memory'], {'LINK': \"r.status = 'active' AND r.weight >= 0.7\"})").unwrap();
        db.query("CALL project_graph('all_edges', ['Memory'], ['LINK'])")
            .unwrap();
        db.checkpoint().unwrap();
    }
    let wal = read_test_wal(&path).unwrap();
    let statements = [
        "CALL page_rank('filtered', maxIterations := 7, dampingFactor := 0.5, tolerance := 0.0, normalizeInitial := true) RETURN node, node_id, node_label, pagerank_score",
        "CALL page_rank('filtered', maxIterations := 7, dampingFactor := 0.5, tolerance := 1.0, normalizeInitial := false) RETURN node, node_id, node_label, pagerank_score",
        "CALL page_rank('filtered', maxIterations := 7, dampingFactor := 0.5, tolerance := 0.0, normalizeInitial := false) RETURN node, node_id, node_label, pagerank_score",
        "CALL louvain('filtered', maxIterations := 7, maxLevels := 2, resolution := 0.8) RETURN node, node_id, node_label, level, louvain_id",
        "CALL louvain('filtered', maxIterations := 7, maxLevels := 2, resolution := 3.0) RETURN node, node_id, node_label, level, louvain_id",
    ];
    let mut reference = None;
    for residency in [
        StorageResidencyMode::Materialized,
        StorageResidencyMode::OutOfCore,
    ] {
        for (budget, operator) in [
            (1024 * 1024, "GraphAlgorithm"),
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
            assert_eq!(
                db.storage_residency_report().unwrap().out_of_core,
                residency == StorageResidencyMode::OutOfCore
            );
            let epoch = db.commit_epoch().unwrap();
            let mut snapshot = db.begin_read_transaction().unwrap();
            let mut results = Vec::new();
            for (index, statement) in statements.iter().enumerate() {
                let mut rows = Vec::new();
                let report = snapshot
                    .query_streaming(
                        statement,
                        QueryStreamOptions {
                            max_rows: Some(256),
                            max_payload_bytes: Some(256 * 1024),
                        },
                        |row| {
                            rows.push(row);
                            Ok(())
                        },
                    )
                    .unwrap();
                assert_eq!(report.output_rows, rows.len());
                assert!(!rows.is_empty());
                if index < 3 {
                    assert_eq!(rows.len(), 128);
                }
                for row in &rows {
                    let Value::Int(node) = row["node"] else {
                        panic!("missing internal identity")
                    };
                    assert_eq!(row["node_id"], Value::String(format!("memory-{node}")));
                    assert_eq!(row["node_label"], Value::String("Memory".into()));
                }
                let reports = &report.execution_profile.blocking_operator_memory_reports;
                assert_eq!(reports.len(), 1);
                assert_eq!(reports[0].operator, operator);
                assert!(reports[0].peak_tracked_bytes <= budget);
                results.push(rows);
            }
            assert_ne!(results[0], results[2], "normalization was ignored");
            assert_ne!(results[1], results[2], "tolerance was ignored");
            assert_ne!(results[3], results[4], "resolution was ignored");
            let all_edges = snapshot.query("CALL louvain('all_edges', maxIterations := 7, maxLevels := 2, resolution := 0.8) RETURN node, node_id, node_label, level, louvain_id").unwrap().rows.into_rows();
            assert_ne!(
                results[3], all_edges,
                "relationship predicates were ignored"
            );
            if let Some(expected) = &reference {
                assert_eq!(&results, expected, "{residency:?}/{operator}");
            } else {
                reference = Some(results);
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
