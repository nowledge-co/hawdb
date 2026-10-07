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
fn projected_graph_admission_rejections_preserve_definition_epoch_and_wal() {
    for mode in [
        StorageResidencyMode::Materialized,
        StorageResidencyMode::OutOfCore,
    ] {
        let path = unique_test_dir(&format!("projected_graph_admission_{mode:?}"));
        let mut db = Database::open_with_config(
            &path,
            DatabaseConfig {
                storage_residency_mode: mode,
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        db.query(
            "CREATE (:Entity {id: 'a'})-[:RELATES_TO {status: 'inactive'}]->(:Entity {id: 'b'})",
        )
        .unwrap();
        db.query("CALL project_graph('G', ['Entity'], {'RELATES_TO': \"r.status = 'active'\"})")
            .unwrap();
        db.checkpoint().unwrap();
        let query = "CALL page_rank('G') RETURN node, rank";
        let expected = db.query(query).unwrap().rows.into_rows();
        let epoch = db.commit_epoch().unwrap();
        let wal = read_test_wal(&path).unwrap();
        let oversized = format!(
            "CALL project_graph('G', ['Entity'], {{'RELATES_TO': \"r.status = '{}'\"}})",
            "x".repeat(16 * 1024)
        );
        let many_conjuncts = format!(
            "CALL project_graph('G', ['Entity'], {{'RELATES_TO': \"{}\"}})",
            std::iter::repeat_n("r.status = 'active'", 17)
                .collect::<Vec<_>>()
                .join(" AND ")
        );
        for invalid in [
            "CALL project_graph('G', {'Entity': '', 'Entity': ''}, ['RELATES_TO'])",
            "CALL project_graph('G', ['Entity'], {'RELATES_TO': '', 'RELATES_TO': ''})",
            "CALL page_rank('G', dampingFactor := 0.85, damping := 0.5)",
            "CALL louvain('G', maxPhases := 2, maxLevels := 1)",
            oversized.as_str(),
            many_conjuncts.as_str(),
        ] {
            assert!(
                db.query(invalid).is_err(),
                "admitted invalid query: {invalid}"
            );
            assert_eq!(db.commit_epoch().unwrap(), epoch);
            assert_eq!(db.query(query).unwrap().rows.into_rows(), expected);
            assert_eq!(read_test_wal(&path).unwrap(), wal);
        }
        drop(db);
        let mut reopened = Database::open_with_config(
            &path,
            DatabaseConfig {
                storage_residency_mode: mode,
                read_only: true,
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        assert_eq!(reopened.commit_epoch().unwrap(), epoch);
        assert_eq!(reopened.query(query).unwrap().rows.into_rows(), expected);
        drop(reopened);
        assert_eq!(read_test_wal(&path).unwrap(), wal);
        std::fs::remove_dir_all(path).unwrap();
    }
}

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
                    db.storage_residency_report().unwrap().out_of_core,
                    mode == StorageResidencyMode::OutOfCore
                );
                let epoch = db.commit_epoch().unwrap();
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
                assert_eq!(db.commit_epoch().unwrap(), epoch);
                assert_eq!(
                    db.storage_residency_report().unwrap().out_of_core,
                    mode == StorageResidencyMode::OutOfCore
                );
            }
            assert_eq!(read_test_wal(&path).unwrap(), wal);
        }
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn mem_projection_predicates_survive_checkpoint_and_filter_analytics_inputs() {
    let path = unique_test_dir("mem_projection_predicates");
    {
        let mut db = Database::open(&path).unwrap();
        for statement in [
            "CREATE (:Memory {id: 'm1'})",
            "CREATE (:Memory {id: 'm2'})",
            "CREATE (:Memory {id: 'm3'})",
            "CREATE (:Entity {id: 'e1'})",
            "CREATE (:Entity {id: 'e2'})",
            "CREATE (:Entity {id: 'e3'})",
            "MATCH (a:Memory {id: 'm1'}), (b:Memory {id: 'm2'}) CREATE (a)-[:MEMORY_RELATES_TO {status: 'active'}]->(b)",
            "MATCH (a:Memory {id: 'm1'}), (b:Memory {id: 'm3'}) CREATE (a)-[:MEMORY_RELATES_TO {status: 'suggested'}]->(b)",
            "MATCH (a:Entity {id: 'e1'}), (b:Entity {id: 'e2'}) CREATE (a)-[:RELATES_TO {confidence: 0.7, strength: 0.5}]->(b)",
            "MATCH (a:Entity {id: 'e1'}), (b:Entity {id: 'e3'}) CREATE (a)-[:RELATES_TO {confidence: 0.69, strength: 0.8}]->(b)",
        ] {
            db.query(statement).unwrap();
        }
        let unified = db
            .query("CALL PROJECT_GRAPH('UnifiedGraph', ['Memory'], {'MEMORY_RELATES_TO': \"r.status = 'active'\"})")
            .unwrap();
        assert_eq!(unified.rows[0]["edge_count"], Value::Int(1));
        let topics = db
            .query("CALL PROJECT_GRAPH('EntityTopicGraph', {'Entity': ''}, {'RELATES_TO': 'r.confidence >= 0.7 AND r.strength >= 0.5'})")
            .unwrap();
        assert_eq!(topics.rows[0]["edge_count"], Value::Int(1));
        db.checkpoint().unwrap();
    }

    let wal = read_test_wal(&path).unwrap();
    let mut db = Database::open_with_config(
        &path,
        DatabaseConfig {
            read_only: true,
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    let unified = db
        .query("CALL page_rank('UnifiedGraph', dampingFactor := 0.85, maxIterations := 20, tolerance := 0.0000001, normalizeInitial := true) RETURN node, node_id, node_label, rank")
        .unwrap();
    let unified_scores = unified
        .rows
        .iter()
        .map(|row| (row["node"].clone(), row["rank"].clone()))
        .collect::<BTreeMap<_, _>>();
    assert!(unified_scores[&Value::Int(1)] > unified_scores[&Value::Int(2)]);
    assert!(unified.rows.iter().all(|row| {
        matches!(row["node_id"], Value::String(_))
            && row["node_label"] == Value::String("Memory".to_string())
    }));

    let topics = db
        .query("CALL page_rank('EntityTopicGraph', dampingFactor := 0.85, maxIterations := 20, tolerance := 0.0000001, normalizeInitial := true) RETURN node, node_id, node_label, rank")
        .unwrap();
    let topic_scores = topics
        .rows
        .iter()
        .map(|row| (row["node"].clone(), row["rank"].clone()))
        .collect::<BTreeMap<_, _>>();
    assert!(topic_scores[&Value::Int(4)] > topic_scores[&Value::Int(5)]);
    assert!(topics.rows.iter().all(|row| {
        matches!(row["node_id"], Value::String(_))
            && row["node_label"] == Value::String("Entity".to_string())
    }));

    let communities = db
        .query("CALL louvain('EntityTopicGraph', maxPhases := 20, maxIterations := 20, resolution := 1.0) RETURN node, node_id, node_label, louvain_id")
        .unwrap();
    let community_nodes = communities
        .rows
        .iter()
        .map(|row| row["node"].clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        community_nodes,
        BTreeSet::from([Value::Int(3), Value::Int(4), Value::Int(5)])
    );
    assert!(communities.rows.iter().all(|row| {
        matches!(row["node_id"], Value::String(_))
            && row["node_label"] == Value::String("Entity".to_string())
    }));
    drop(db);
    assert_eq!(read_test_wal(&path).unwrap(), wal);
    std::fs::remove_dir_all(path).unwrap();
}
