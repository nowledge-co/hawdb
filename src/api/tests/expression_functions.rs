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
fn return_projection_functions_cover_nowledge_fallback_reads() {
    let mut db = Database::new();
    db.query(
            "CREATE (:Memory {id: 1, content: 'Projection fallback content'})-[:MENTIONS {confidence: 0.7}]->(:Entity {id: 10})",
        )
        .unwrap();

    let output = db
            .query(
                "MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) RETURN COALESCE(m.title, LEFT(COALESCE(m.content, ''), 10)) AS label, COALESCE(r.strength, r.confidence, 0.5) AS weight",
            )
            .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("label"),
        Some(&Value::String("Projection".to_string()))
    );
    assert_eq!(output.rows[0].get("weight"), Some(&Value::Float(0.7)));

    let output = db
        .query("MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) RETURN COALESCE(null, e.id) AS fallback")
        .unwrap();
    assert_eq!(output.rows[0].get("fallback"), Some(&Value::Int(10)));
}

#[test]
fn order_by_projection_functions_cover_nowledge_rank_fallbacks() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, importance: 0.4})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, pagerank_score: 0.9, importance: 0.1})")
        .unwrap();
    db.query("CREATE (:Memory {id: 3})").unwrap();

    let output = db
            .query(
                "MATCH (m:Memory) RETURN m.id AS id ORDER BY COALESCE(m.pagerank_score, m.importance, 0.5) DESC",
            )
            .unwrap();
    assert_eq!(output.rows.len(), 3);
    assert_eq!(output.rows[0].get("id"), Some(&Value::Int(2)));
    assert_eq!(output.rows[1].get("id"), Some(&Value::Int(3)));
    assert_eq!(output.rows[2].get("id"), Some(&Value::Int(1)));
}

#[test]
fn predicate_projection_functions_cover_nowledge_fallback_filters() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'Graph foundations', created_at: 5})")
        .unwrap();
    db.query(
            "CREATE (:Memory {id: 2, title: 'Runtime strategy', last_accessed_at: 15, is_crystal: false})",
        )
        .unwrap();
    db.query("CREATE (:Memory {id: 3, title: 'Graph crystal', created_at: 20, is_crystal: true})")
        .unwrap();

    let output = db
            .query(
                "MATCH (m:Memory) WHERE COALESCE(m.is_crystal, false) = false RETURN m.id AS id ORDER BY id ASC",
            )
            .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(output.rows[0].get("id"), Some(&Value::Int(1)));
    assert_eq!(output.rows[1].get("id"), Some(&Value::Int(2)));

    let output = db
            .query_with_params(
                "MATCH (m:Memory) WHERE COALESCE(m.created_at, m.last_accessed_at) >= $cutoff AND LEFT(COALESCE(m.title, ''), 5) = 'Graph' RETURN m.id AS id",
                &BTreeMap::from([("cutoff".to_string(), Value::Int(10))]),
            )
            .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].get("id"), Some(&Value::Int(3)));
}

#[test]
fn lower_expression_predicates_cover_nowledge_grep_filters() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'Graph foundations', content: 'Runtime notes'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, title: 'Other', content: 'needle in content'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 10, name: 'Rust'})").unwrap();

    let output = db
            .query_with_params(
                "MATCH (m:Memory) WHERE LOWER(COALESCE(m.content, '')) CONTAINS LOWER($needle) OR LOWER(COALESCE(m.title, '')) CONTAINS LOWER($needle) RETURN m.id AS id ORDER BY id ASC",
                &BTreeMap::from([("needle".to_string(), Value::String("GRAPH".to_string()))]),
            )
            .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].get("id"), Some(&Value::Int(1)));

    let output = db
        .query_with_params(
            "MATCH (e:Entity) WHERE LOWER(e.name) = LOWER($mention) RETURN e.id AS id",
            &BTreeMap::from([("mention".to_string(), Value::String("rust".to_string()))]),
        )
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].get("id"), Some(&Value::Int(10)));
}

#[test]
fn scalar_expression_regex_predicates_validate_execute_and_cache_patterns() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, content: 'Graph foundations'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, content: 'Runtime strategy'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 3})").unwrap();

    let query = "MATCH (m:Memory) WHERE LOWER(COALESCE(m.content, '')) =~ $pattern RETURN m.id AS id ORDER BY id";
    let parameters = |pattern: &str| {
        BTreeMap::from([("pattern".to_string(), Value::String(pattern.to_string()))])
    };
    for (pattern, expected) in [
        ("^graph", Value::Int(1)),
        ("^runtime", Value::Int(2)),
        ("^$", Value::Int(3)),
    ] {
        let output = db.query_with_params(query, &parameters(pattern)).unwrap();
        assert_eq!(output.rows, vec![BTreeMap::from([("id".into(), expected)])]);
    }
    assert_eq!(
        db.query(
            "MATCH (m:Memory) WHERE LOWER(COALESCE(m.content, '')) =~ '^graph' RETURN m.id AS id"
        )
        .unwrap()
        .rows,
        vec![BTreeMap::from([("id".into(), Value::Int(1))])]
    );
    for (query, parameters, expected) in [
        (
            "MATCH (m:Memory) WHERE trim(m.content) =~ '^Graph' RETURN m.id AS id",
            BTreeMap::new(),
            vec![BTreeMap::from([("id".into(), Value::Int(1))])],
        ),
        (
            "MATCH (m:Memory) WHERE regexp_replace(m.content, 'foundations', 'runtime') =~ 'runtime$' RETURN m.id AS id",
            BTreeMap::new(),
            vec![BTreeMap::from([("id".into(), Value::Int(1))])],
        ),
        (
            "UNWIND [1] AS row WITH row WHERE $value =~ '^a' RETURN row",
            BTreeMap::from([("value".into(), Value::String("abc".into()))]),
            vec![BTreeMap::from([("row".into(), Value::Int(1))])],
        ),
        (
            "MATCH (m:Memory) WHERE (m.content) =~ '^Graph' RETURN m.id AS id",
            BTreeMap::new(),
            vec![BTreeMap::from([("id".into(), Value::Int(1))])],
        ),
        (
            "UNWIND [1] AS row WITH row WHERE ('abc') =~ '^a' RETURN row",
            BTreeMap::new(),
            vec![BTreeMap::from([("row".into(), Value::Int(1))])],
        ),
    ] {
        assert_eq!(
            db.query_with_params(query, &parameters).unwrap().rows,
            expected,
            "{query}"
        );
    }

    let graph = db
        .explain_query_with_params(query, &parameters("^graph"))
        .unwrap();
    let runtime = db
        .explain_query_with_params(query, &parameters("^runtime"))
        .unwrap();
    assert_eq!(graph.plan_cache_lookup, PlanCacheLookup::Hit);
    assert_eq!(runtime.plan_cache_lookup, PlanCacheLookup::Hit);
    assert_ne!(
        graph.physical_plan.instance_fingerprint(),
        runtime.physical_plan.instance_fingerprint()
    );

    let invalid = db
        .query("MATCH (m:Memory) WHERE LOWER(COALESCE(m.content, '')) =~ '[' RETURN m.id")
        .unwrap_err();
    assert!(matches!(invalid, HawDBError::Semantic(_)), "{invalid:?}");
    let non_string_pattern = db
        .query_with_params(
            query,
            &BTreeMap::from([("pattern".to_string(), Value::Int(7))]),
        )
        .unwrap_err();
    assert!(
        matches!(non_string_pattern, HawDBError::Semantic(_)),
        "{non_string_pattern:?}"
    );
    let non_string_left = db
        .query("MATCH (m:Memory) WHERE COALESCE(m.id, '') =~ '^1$' RETURN m.id")
        .unwrap_err();
    assert!(
        matches!(non_string_left, HawDBError::Execution(_)),
        "{non_string_left:?}"
    );
    assert!(db
        .query("MATCH (m:Memory) WHERE m.id =~ '^1$' RETURN m.id")
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn scalar_expression_regex_survives_reopen_and_concurrent_reads() {
    let path = super::unique_test_dir("scalar_expression_regex");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, content: 'Graph foundations'})")
            .unwrap();
        db.query("CREATE (:Memory {id: 2, content: 'Runtime strategy'})")
            .unwrap();
    }

    let db = Database::open(&path).unwrap().into_concurrent();
    let handles = [("^graph", 1), ("^runtime", 2)]
        .into_iter()
        .map(|(pattern, id)| {
            let db = db.clone();
            std::thread::spawn(move || {
                let query = format!(
                    "MATCH (m:Memory) WHERE LOWER(COALESCE(m.content, '')) =~ '{pattern}' RETURN m.id AS id"
                );
                (db.query(&query).unwrap().rows, id)
            })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        let (rows, id) = handle.join().unwrap();
        assert_eq!(rows, vec![BTreeMap::from([("id".into(), Value::Int(id))])]);
    }
    drop(db);

    let mut reopened = Database::open(&path).unwrap();
    assert_eq!(
        reopened
            .query("MATCH (m:Memory) WHERE LOWER(COALESCE(m.content, '')) =~ '^graph' RETURN m.id AS id")
            .unwrap()
            .rows,
        vec![BTreeMap::from([("id".into(), Value::Int(1))])]
    );
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn generic_case_executes_branch_expressions_with_changed_parameters() {
    let mut db = Database::new();
    db.query("CREATE (:Item {id: 1, name: 'FIRST', enabled: true})")
        .unwrap();
    db.query("CREATE (:Item {id: 2, name: 'SECOND', enabled: false})")
        .unwrap();
    db.query("CREATE (:Item {id: 3, name: 'THIRD'})").unwrap();
    let query = "MATCH (n:Item) RETURN n.id AS id, CASE WHEN n.id = $chosen THEN lower(n.name) WHEN NOT n.enabled THEN $fallback ELSE 'unknown' END AS result, CASE n.id WHEN $chosen THEN 'selected' END AS selected ORDER BY id";
    for chosen in [1, 2, 3, 1] {
        let output = db
            .query_with_params(
                query,
                &BTreeMap::from([
                    ("chosen".to_string(), Value::Int(chosen)),
                    (
                        "fallback".to_string(),
                        Value::String(format!("fallback-{chosen}")),
                    ),
                ]),
            )
            .unwrap();
        assert_eq!(output.rows.len(), 3);
        for (index, row) in output.rows.iter().enumerate() {
            let id = index as i64 + 1;
            let expected = if id == chosen {
                ["first", "second", "third"][index].to_string()
            } else if id == 2 {
                format!("fallback-{chosen}")
            } else {
                "unknown".to_string()
            };
            assert_eq!(row.get("result"), Some(&Value::String(expected)));
            assert_eq!(
                row.get("selected"),
                Some(&if id == chosen {
                    Value::String("selected".to_string())
                } else {
                    Value::Null
                })
            );
        }
    }
}

#[test]
fn generic_case_preserves_null_logic_and_unselected_branch_errors() {
    let mut db = Database::new();
    db.query("CREATE (:Item {id: 1})").unwrap();
    let output = db.query("MATCH (n:Item) RETURN CASE WHEN n.missing = 1 THEN lower(7) WHEN true OR lower(8) THEN CASE WHEN false AND lower(9) THEN 0 ELSE 4 END ELSE lower(10) END AS result").unwrap();
    assert_eq!(output.rows[0].get("result"), Some(&Value::Int(4)));
    assert!(db
        .query("MATCH (n:Item) RETURN CASE WHEN true THEN lower(7) ELSE 'unused' END AS result")
        .is_err());
    assert!(db
        .query("MATCH (n:Item) RETURN CASE WHEN missing.id = 1 THEN 1 ELSE 0 END AS result")
        .is_err());
}

#[test]
fn generic_case_preserves_with_column_dependencies_and_boolean_precedence() {
    let mut db = Database::new();
    db.query("CREATE (:Item {id: 1, name: 'FIRST'})").unwrap();
    let output = db.query("MATCH (n:Item) WITH n, CASE WHEN n.id = 1 THEN lower(n.name) ELSE 'other' END AS text RETURN CASE WHEN true THEN text ELSE 'unused' END AS result, CASE WHEN true OR false AND false THEN 4 ELSE 0 END AS precedence").unwrap();
    assert_eq!(
        output.rows[0].get("result"),
        Some(&Value::String("first".to_string()))
    );
    assert_eq!(output.rows[0].get("precedence"), Some(&Value::Int(4)));
}
