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
use crate::HawDBError;

#[test]
fn identity_898_public_columns_aliases_and_typed_ids_survive_read_only_reopen() {
    let path = unique_test_dir("algorithm_identity_898");
    {
        let mut db = Database::open(&path).unwrap();
        for query in [
            "CREATE (:Memory {id: 'memory-a'})",
            "CREATE (:Entity {id: 42})",
            "CREATE (:Memory {title: 'missing-id'})",
            "CALL project_graph('identity', ['Memory', 'Entity'], [])",
        ] {
            db.query(query).unwrap();
        }
        db.checkpoint().unwrap();
    }
    let wal = read_test_wal(&path).unwrap();
    for mode in [
        StorageResidencyMode::Materialized,
        StorageResidencyMode::OutOfCore,
    ] {
        let mut db = Database::open_with_config(
            &path,
            DatabaseConfig {
                read_only: true,
                storage_residency_mode: mode,
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        let epoch = db.commit_epoch().unwrap();
        for (procedure, score) in [("page_rank", "pagerank_score"), ("louvain", "louvain_id")] {
            let legacy = db
                .query(&format!(
                    "CALL {procedure}('identity') RETURN node, node_id, node_label, {score}"
                ))
                .unwrap()
                .rows
                .into_rows();
            let rows = db.query(&format!("CALL {procedure}('identity') RETURN node, node_id, node_label, node_labels, {score}")).unwrap().rows.into_rows();
            assert_eq!(rows.len(), 3);
            for row in &rows {
                let old = legacy
                    .iter()
                    .find(|old| old["node"] == row["node"])
                    .unwrap();
                assert_eq!(old["node_id"], row["node_id"]);
                assert_eq!(old["node_label"], row["node_label"]);
                assert_eq!(old[score], row[score]);
                assert_eq!(
                    row["node_labels"],
                    Value::List(vec![row["node_label"].clone()])
                );
                assert_eq!(row.len(), 5);
            }
            assert_eq!(
                rows.iter()
                    .map(|row| row["node_id"].clone())
                    .collect::<BTreeSet<_>>(),
                BTreeSet::from([
                    Value::Null,
                    Value::Int(42),
                    Value::String("memory-a".into())
                ])
            );
            let aliases = db.query(&format!("CALL {procedure}('identity') YIELD node_id AS business_id, node_labels AS labels, {score} AS score RETURN business_id AS id, labels AS kinds, score")).unwrap();
            assert_eq!(aliases.rows.len(), 3);
            assert!(aliases
                .rows
                .iter()
                .all(|row| row.keys().eq(["id", "kinds", "score"])));
            let labels_only = db
                .query(&format!(
                    "CALL {procedure}('identity') YIELD node_labels AS labels RETURN labels"
                ))
                .unwrap();
            assert_eq!(labels_only.rows.len(), 3);
            assert!(labels_only.rows.iter().all(|row| row.len() == 1));
            let projected = db.query(&format!("CALL {procedure}('identity') RETURN node_id AS id, node_labels AS labels ORDER BY id LIMIT 2")).unwrap();
            assert_eq!(projected.rows.len(), 2);
            assert!(projected.rows.iter().all(|row| row.len() == 2));
            let with = db.query(&format!("CALL {procedure}('identity') WITH node_id AS id, node_labels AS labels RETURN id, labels")).unwrap();
            assert_eq!(with.rows.len(), 3);
            let plain = db
                .query(&format!(
                    "CALL {procedure}('identity') RETURN node, {score}"
                ))
                .unwrap();
            assert!(plain.rows.iter().all(|row| !row.contains_key("node_labels")
                && !row.contains_key("node_id")
                && !row.contains_key("node_label")));
        }
        let rank_alias = db
            .query("CALL page_rank('identity') RETURN node_labels AS labels, rank AS score")
            .unwrap();
        assert_eq!(rank_alias.rows.len(), 3);
        assert_eq!(db.commit_epoch().unwrap(), epoch);
        drop(db);
        assert_eq!(read_test_wal(&path).unwrap(), wal);
    }
    std::fs::remove_dir_all(path).unwrap();
}

// Consumer fixture only: the database accepts arbitrary multi-label nodes.
// Mem's legacy response has one business type and must reject ambiguous input.
fn mem_identity(row: &BTreeMap<String, Value>) -> crate::Result<Value> {
    let Some(Value::List(labels)) = row.get("node_labels") else {
        return Err(HawDBError::Execution(
            "algorithm identity is missing node_labels".into(),
        ));
    };
    let mut supported = labels.iter().filter(
        |label| matches!(label, Value::String(name) if name == "Entity" || name == "Memory"),
    );
    let label = supported.next().ok_or_else(|| {
        HawDBError::Execution("algorithm identity has no supported Mem label".into())
    })?;
    if supported.next().is_some() {
        return Err(HawDBError::Execution(
            "algorithm identity has ambiguous Mem labels".into(),
        ));
    }
    Ok(Value::Map(BTreeMap::from([
        (
            "id".into(),
            row.get("node_id").cloned().unwrap_or(Value::Null),
        ),
        ("_LABEL".into(), label.clone()),
    ])))
}

#[test]
fn identity_898_mem_consumer_preserves_identity_and_rejects_ambiguous_labels() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory-a'})").unwrap();
    db.query("CREATE (:Entity {id: 42})").unwrap();
    db.query("CALL project_graph('identity', ['Entity', 'Memory'], [])")
        .unwrap();
    for procedure in ["page_rank", "louvain"] {
        let rows = db
            .query(&format!(
                "CALL {procedure}('identity') RETURN node_id, node_labels"
            ))
            .unwrap();
        let identities = rows
            .rows
            .into_rows()
            .iter()
            .map(mem_identity)
            .collect::<crate::Result<BTreeSet<_>>>()
            .unwrap();
        assert_eq!(
            identities,
            BTreeSet::from([
                Value::Map(BTreeMap::from([
                    ("id".into(), Value::String("memory-a".into())),
                    ("_LABEL".into(), Value::String("Memory".into()))
                ])),
                Value::Map(BTreeMap::from([
                    ("id".into(), Value::Int(42)),
                    ("_LABEL".into(), Value::String("Entity".into()))
                ])),
            ])
        );
    }
    for labels in [vec!["Entity", "Memory"], vec!["Topic"]] {
        let row = BTreeMap::from([
            ("node_id".into(), Value::String("ambiguous".into())),
            (
                "node_labels".into(),
                Value::List(
                    labels
                        .into_iter()
                        .map(|s| Value::String(s.into()))
                        .collect(),
                ),
            ),
        ]);
        assert!(mem_identity(&row).is_err());
    }
    let row = BTreeMap::from([
        ("node_id".into(), Value::Int(42)),
        (
            "node_labels".into(),
            Value::List(
                ["Entity", "Topic"]
                    .map(|s| Value::String(s.into()))
                    .to_vec(),
            ),
        ),
    ]);
    assert!(mem_identity(&row).is_ok());
}

#[test]
fn identity_898_extended_spellings_preserve_legacy_values() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory-a'})").unwrap();
    db.query("CREATE (:Entity {id: 42})").unwrap();
    db.query("CREATE (:Memory {title: 'missing-id'})").unwrap();
    db.query("CALL project_graph('identity', ['Entity', 'Memory'], [])")
        .unwrap();
    for (procedure, score) in [("page_rank", "pagerank_score"), ("louvain", "louvain_id")] {
        let legacy = db
            .query(&format!(
                "CALL {procedure}('identity') RETURN node, node_id, node_label, {score}"
            ))
            .unwrap()
            .rows
            .into_rows();
        for (clause, columns) in [
            (format!("YIELD node, node_id, node_label, {score} RETURN node, node_id AS id, node_label AS label, {score} AS score"), vec![("node", "node"), ("node_id", "id"), ("node_label", "label"), (score, "score")]),
            (format!("RETURN node AS node, node_id AS id, node_label AS label, {score} AS score"), vec![("node", "node"), ("node_id", "id"), ("node_label", "label"), (score, "score")]),
            (format!("WITH node, node_id AS id, node_label AS label, {score} AS score RETURN node, id, label, score"), vec![("node", "node"), ("node_id", "id"), ("node_label", "label"), (score, "score")]),
            ("RETURN node, node_id".into(), vec![("node", "node"), ("node_id", "node_id")]),
            ("RETURN node_label".into(), vec![("node_label", "node_label")]),
            ("RETURN node_labels AS labels".into(), vec![("node_labels", "labels")]),
            (format!("RETURN node, {score} ORDER BY node LIMIT 2"), vec![("node", "node"), (score, score)]),
        ] {
            let mut source = legacy.clone();
            if clause.contains("LIMIT 2") {
                source.sort_by(|a, b| a["node"].cmp(&b["node"]));
                source.truncate(2);
            }
            let mut expected: Vec<BTreeMap<String, Value>> = source.iter().map(|row| {
                columns.iter().map(|(from, to)| {
                    let value = if *from == "node_labels" {
                        Value::List(vec![row["node_label"].clone()])
                    } else {
                        row[*from].clone()
                    };
                    ((*to).to_string(), value)
                }).collect()
            }).collect();
            let query = format!("CALL {procedure}('identity') {clause}");
            let mut actual = db.query(&query).unwrap().rows.into_rows();
            actual.sort();
            expected.sort();
            assert_eq!(actual, expected, "{query}");
        }
    }
}
