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
use crate::StorageResidencyMode;

mod differential;

#[test]
fn projected_graph_queries_preserve_residency_reopen_and_read_only_wal_boundaries() {
    let mut reference = None;
    for mode in [
        StorageResidencyMode::Materialized,
        StorageResidencyMode::OutOfCore,
    ] {
        let path = unique_test_dir(&format!("graph_algorithm_residency_{mode:?}"));
        {
            let mut db = Database::open(&path).unwrap();
            for statement in [
                "CREATE (:Memory {id: 1})-[:LINK]->(:Memory {id: 2})",
                "CREATE (:Source {id: 3})",
                "MATCH (m:Memory {id: 2}), (s:Source {id: 3}) CREATE (m)-[:LINK]->(s)",
                "CALL project_graph('selected', ['Memory'], ['LINK'])",
                "CALL project_graph('no_edges', ['Memory'], ['MissingType'])",
                "CALL project_graph('no_nodes', ['MissingLabel'], ['LINK'])",
            ] {
                db.query(statement).unwrap();
            }
            db.checkpoint().unwrap();
        }
        let wal = read_test_wal(&path).unwrap();
        let config = DatabaseConfig {
            storage_residency_mode: mode,
            read_only: true,
            ..DatabaseConfig::default()
        };
        for _ in 0..2 {
            {
                let mut db = Database::open_with_config(&path, config.clone()).unwrap();
                assert_eq!(
                    db.storage_residency_report().out_of_core,
                    mode == StorageResidencyMode::OutOfCore
                );
                let epoch = db.commit_epoch();
                let results: Vec<Vec<BTreeMap<String, Value>>> = [
                    "CALL page_rank('selected', dampingFactor := 0.5, maxIterations := 3) RETURN node, pagerank_score",
                    "CALL louvain('selected', maxIterations := 3, maxLevels := 1) RETURN node, level, louvain_id",
                    "CALL page_rank('no_edges', dampingFactor := 0.5, maxIterations := 3) RETURN node, pagerank_score",
                    "CALL page_rank('no_nodes') RETURN node, pagerank_score",
                    "CALL louvain('no_nodes') RETURN node, louvain_id",
                ].into_iter().map(|statement| db.query(statement).unwrap().rows.into_rows()).collect();
                assert_eq!(
                    results.iter().map(Vec::len).collect::<Vec<_>>(),
                    [2, 2, 2, 0, 0]
                );
                for rows in results.iter().take(3) {
                    let ids: BTreeSet<_> = rows.iter().map(|row| row["node"].clone()).collect();
                    assert_eq!(ids, BTreeSet::from([Value::Int(0), Value::Int(1)]));
                }
                assert!(results[2]
                    .iter()
                    .all(|row| row["pagerank_score"] == Value::Float(0.5)));
                if let Some(expected) = &reference {
                    assert_eq!(&results, expected, "{mode:?}");
                } else {
                    reference = Some(results);
                }
                assert_eq!(db.commit_epoch(), epoch);
                assert_eq!(
                    db.storage_residency_report().out_of_core,
                    mode == StorageResidencyMode::OutOfCore
                );
            }
            assert_eq!(read_test_wal(&path).unwrap(), wal);
        }
        std::fs::remove_dir_all(path).unwrap();
    }
}
