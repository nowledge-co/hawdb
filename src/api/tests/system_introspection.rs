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
use crate::{RuntimeCapabilities, RuntimeCapability};

#[test]
fn system_sql_exposes_runtime_configuration_and_capabilities() {
    let capabilities = RuntimeCapabilities::default()
        .with(RuntimeCapability::FullTextSearch, false)
        .with(RuntimeCapability::GraphAnalytics, true);
    let mut db = Database::new_with_config(DatabaseConfig {
        max_read_result_rows: Some(16),
        max_read_result_payload_bytes: Some(4096),
        storage_residency_mode: hawdb_storage::StorageResidencyMode::OutOfCore,
        relational_index_mode: hawdb_storage::RelationalIndexMode::DemandPaged,
        runtime_capabilities: capabilities,
        ..DatabaseConfig::default()
    });

    let status = db
        .query_sql(
            "SELECT read_only, max_read_result_rows, max_read_result_payload_bytes, \
                    storage_residency_mode, relational_index_mode \
             FROM system.runtime_status",
        )
        .unwrap();
    assert_eq!(
        status.rows,
        vec![BTreeMap::from([
            ("read_only".to_string(), Value::Bool(false)),
            ("max_read_result_rows".to_string(), Value::Int(16)),
            (
                "max_read_result_payload_bytes".to_string(),
                Value::Int(4096),
            ),
            (
                "storage_residency_mode".to_string(),
                Value::String("out_of_core".to_string()),
            ),
            (
                "relational_index_mode".to_string(),
                Value::String("demand_paged".to_string()),
            ),
        ])]
    );

    let capability = db
        .query_sql_with_params(
            "SELECT enabled FROM system.runtime_capabilities WHERE capability = $1",
            &[Value::String("full_text_search".to_string())],
        )
        .unwrap();
    assert_eq!(
        capability.rows,
        vec![BTreeMap::from([(
            "enabled".to_string(),
            Value::Bool(false),
        )])]
    );
}

#[test]
fn system_graph_statistics_are_parameterized_and_snapshot_pinned() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 1, kind: 'note'})-[:MENTIONS]->(:Entity {id: 10, name: 'Rust'})",
    )
    .unwrap();
    let read_tx = db.begin_read_transaction();

    db.query("CREATE (:Memory {id: 2, kind: 'decision'})")
        .unwrap();

    let parameters = [Value::String("node_count".to_string())];
    let pinned = read_tx
        .query_sql_with_params(
            "SELECT computed_at_commit_epoch, count \
             FROM system.graph_statistics \
             WHERE statistic_kind = $1",
            &parameters,
        )
        .unwrap();
    let live = db
        .query_sql_with_params(
            "SELECT computed_at_commit_epoch, count \
             FROM system.graph_statistics \
             WHERE statistic_kind = $1",
            &parameters,
        )
        .unwrap();

    assert_eq!(
        pinned.rows,
        vec![BTreeMap::from([
            ("computed_at_commit_epoch".to_string(), Value::Int(1)),
            ("count".to_string(), Value::Int(2)),
        ])]
    );
    assert_eq!(
        live.rows,
        vec![BTreeMap::from([
            ("computed_at_commit_epoch".to_string(), Value::Int(2)),
            ("count".to_string(), Value::Int(3)),
        ])]
    );

    let memory_label = db
        .query_sql_with_params(
            "SELECT count FROM system.graph_statistics \
             WHERE statistic_kind = $1 AND label_name = $2",
            &[
                Value::String("label_count".to_string()),
                Value::String("Memory".to_string()),
            ],
        )
        .unwrap();
    assert_eq!(
        memory_label.rows,
        vec![BTreeMap::from([("count".to_string(), Value::Int(2))])]
    );
}

#[test]
fn system_graph_statistics_exposes_index_samples() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {kind: 'note'})").unwrap();
    db.query("CREATE (:Memory {kind: 'note'})").unwrap();
    db.query("CREATE (:Memory {kind: 'decision'})").unwrap();
    db.query("CREATE INDEX ON :Memory(kind)").unwrap();

    let output = db
        .query_sql(
            "SELECT index_kind, index_size, unique_values, sample_size, \
             updates_since_sample, stale \
             FROM system.graph_statistics \
             WHERE statistic_kind = 'index_sample' AND property_name = 'kind'",
        )
        .unwrap();
    assert_eq!(
        output.rows,
        vec![BTreeMap::from([
            (
                "index_kind".to_string(),
                Value::String("equality".to_string())
            ),
            ("index_size".to_string(), Value::Int(3)),
            ("unique_values".to_string(), Value::Int(2)),
            ("sample_size".to_string(), Value::Int(3)),
            ("updates_since_sample".to_string(), Value::Int(0)),
            ("stale".to_string(), Value::Bool(false)),
        ])]
    );
}

#[test]
fn system_sql_exposes_projected_graph_and_changefeed_state() {
    let path = unique_test_dir("system_projection_introspection");
    {
        let mut db = Database::open(&path).unwrap();
        db.query(
            "CREATE (:Memory {id: 1, title: 'Root'})-[:LINKS]->(:Entity {id: 2, name: 'Leaf'})",
        )
        .unwrap();
        db.query("CALL project_graph('EntityGraph', ['Memory', 'Entity'], ['LINKS'])")
            .unwrap();
        db.checkpoint().unwrap();
    }
    {
        let mut db = Database::open(&path).unwrap();
        let graph_commit_epoch = db.commit_epoch();
        let projection = db
            .query_sql_with_params(
                "SELECT name, projection_epoch, node_count, edge_count, reusable \
                 FROM system.projected_graphs WHERE name = $1",
                &[Value::String("EntityGraph".to_string())],
            )
            .unwrap();
        assert_eq!(
            projection.rows,
            vec![BTreeMap::from([
                ("name".to_string(), Value::String("EntityGraph".to_string()),),
                ("projection_epoch".to_string(), Value::Int(1)),
                ("node_count".to_string(), Value::Int(2)),
                ("edge_count".to_string(), Value::Int(1)),
                ("reusable".to_string(), Value::Bool(true)),
            ])]
        );

        let changefeed = db
            .query_sql(
                "SELECT graph_commit_epoch, retained_mutation_count, restart_recoverable \
                 FROM system.search_projection_changefeed",
            )
            .unwrap();
        assert_eq!(changefeed.rows.len(), 1);
        assert_eq!(
            changefeed.rows[0].get("graph_commit_epoch"),
            Some(&Value::Int(graph_commit_epoch as i64))
        );
        assert_eq!(
            changefeed.rows[0].get("retained_mutation_count"),
            Some(&Value::Int(1))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn system_sql_fails_closed_when_result_row_budget_is_exceeded() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_read_result_rows: Some(2),
        ..DatabaseConfig::default()
    });

    let error = db
        .query_sql("SELECT capability FROM system.runtime_capabilities")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("exceeding max_read_result_rows 2"));

    let bounded = db
        .query_sql("SELECT capability FROM system.runtime_capabilities ORDER BY capability LIMIT 2")
        .unwrap();
    assert_eq!(bounded.rows.len(), 2);
}
