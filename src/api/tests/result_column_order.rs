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
use hawdb_core::ValueRef;

fn fixture() -> Database {
    let mut database = Database::new();
    database
        .query_sql("CREATE TABLE catalog_order (id BIGINT PRIMARY KEY, name TEXT)")
        .unwrap();
    database
}

#[test]
fn catalog_projection_order_survives_empty_results_and_all_front_doors() {
    let mut database = fixture();
    for (sql, columns, values) in [
        (
            "SELECT data_type AS z_type, column_name AS a_name FROM information_schema.columns WHERE table_name = $1 AND column_name = 'id'",
            ["z_type", "a_name"],
            ["bigint", "id"],
        ),
        (
            "SELECT tablename, schemaname FROM pg_catalog.pg_tables WHERE tablename = $1",
            ["tablename", "schemaname"],
            ["catalog_order", "public"],
        ),
        (
            "SELECT indexname, tablename FROM pg_indexes WHERE tablename = $1",
            ["indexname", "tablename"],
            ["catalog_order_pkey", "catalog_order"],
        ),
    ] {
        for (name, count) in [("catalog_order", 1), ("missing", 0)] {
            let parameters = [Value::String(name.into())];
            let mut outputs = vec![
                database.query_sql_with_params(sql, &parameters).unwrap(),
                database
                    .begin_read_transaction()
                    .unwrap()
                    .query_sql_with_params(sql, &parameters)
                    .unwrap(),
            ];
            outputs.push(
                database
                    .begin_transaction()
                    .unwrap()
                    .query_sql_with_params(sql, &parameters)
                    .unwrap(),
            );
            for output in outputs {
                assert_eq!(output.schema().columns(), columns, "{sql}");
                assert_eq!(output.rows.len(), count, "{sql}");
                if count != 0 {
                    for (ordinal, expected) in values.iter().enumerate() {
                        assert_eq!(
                            output.rows.value(0, ordinal),
                            Some(ValueRef::String(expected)),
                            "{sql}",
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn catalog_wildcards_expand_in_place_and_keep_declared_order() {
    let mut database = fixture();
    let table_columns = [
        "table_catalog",
        "table_schema",
        "table_name",
        "table_type",
        "self_referencing_column_name",
        "reference_generation",
        "user_defined_type_catalog",
        "user_defined_type_schema",
        "user_defined_type_name",
        "is_insertable_into",
        "is_typed",
        "commit_action",
    ];
    for suffix in ["", " WHERE table_name = 'missing'", " LIMIT 0"] {
        let output = database
            .query_sql(&format!("SELECT * FROM information_schema.tables{suffix}"))
            .unwrap();
        assert_eq!(output.schema().columns(), table_columns);
    }
    for suffix in ["", " WHERE metric = 'missing'", " LIMIT 0"] {
        let output = database
            .query_sql(&format!(
                "SELECT value AS z_value, *, metric AS a_metric FROM system.plan_cache{suffix}"
            ))
            .unwrap();
        assert_eq!(
            output.schema().columns(),
            ["z_value", "metric", "value", "a_metric"]
        );
        for row in output.rows.iter() {
            assert_eq!(row.get("z_value"), row.get("value"));
            assert_eq!(row.get("a_metric"), row.get("metric"));
        }
    }
    for table in ["pg_tables", "pg_catalog.pg_tables"] {
        let output = database
            .query_sql(&format!("SELECT * FROM {table}"))
            .unwrap();
        assert_eq!(
            output.schema().columns(),
            [
                "schemaname",
                "tablename",
                "tableowner",
                "tablespace",
                "hasindexes",
                "hasrules",
                "hastriggers",
                "rowsecurity",
            ]
        );
    }
}

#[test]
fn catalog_duplicate_names_fail_even_without_rows() {
    let mut database = fixture();
    for projection in [
        "value, value",
        "value AS repeated, metric AS repeated",
        "*, value",
    ] {
        for suffix in ["", " WHERE metric = 'missing'", " LIMIT 0"] {
            let error = database
                .query_sql(&format!(
                    "SELECT {projection} FROM system.plan_cache{suffix}"
                ))
                .unwrap_err();
            assert!(matches!(error, HawDBError::Semantic(_)), "{error}");
            assert!(error.to_string().contains("duplicate"), "{error}");
        }
    }
}

#[test]
fn catalog_order_preserves_row_and_payload_budget_refusal() {
    let mut database = fixture();
    let sql = "SELECT data_type, column_name FROM information_schema.columns WHERE table_name = 'catalog_order'";
    let output = database.query_sql(sql).unwrap();
    let payload = output
        .rows
        .iter()
        .map(|row| hawdb_executor::binding::map_payload_bytes(&row.to_owned_row()))
        .sum::<usize>();
    let exact = database
        .query_sql_with_params_options(
            sql,
            &[],
            QueryStreamOptions {
                max_rows: Some(2),
                max_payload_bytes: Some(payload),
            },
        )
        .unwrap();
    assert_eq!(exact.rows, output.rows);
    for (rows, bytes) in [(1, payload), (2, payload - 1)] {
        assert!(matches!(
            database.query_sql_with_params_options(
                sql,
                &[],
                QueryStreamOptions {
                    max_rows: Some(rows),
                    max_payload_bytes: Some(bytes),
                }
            ),
            Err(HawDBError::ReadBudgetExceeded(_))
        ));
    }
}

#[test]
fn cypher_return_order_survives_empty_results_cache_and_all_front_doors() {
    let mut database = Database::new();
    database
        .query("CREATE (:Stock {code: 'a', price: 7, rank: 1})")
        .unwrap();
    for query in [
        "MATCH (s:Stock) WHERE s.code = $code RETURN s.price AS price, s.code AS code",
        "MATCH (s:Stock) WHERE s.code = $code RETURN DISTINCT s.price AS price, s.code AS code",
        "MATCH (s:Stock) WHERE s.code = $code RETURN s.price AS price, s.code AS code ORDER BY s.rank LIMIT 1",
        "MATCH (s:Stock) WHERE s.code = $code WITH s.price AS p, s.code AS c RETURN p AS price, c AS code",
    ] {
        for code in ["a", "missing", "a"] {
            let parameters = BTreeMap::from([("code".into(), Value::String(code.into()))]);
            let mut outputs = vec![
                database.query_with_params(query, &parameters).unwrap(),
                database.begin_read_transaction().unwrap().query_with_params(query, &parameters).unwrap(),
            ];
            outputs.push(database.begin_transaction().unwrap().query_with_params(query, &parameters).unwrap());
            for output in outputs {
                assert_eq!(output.schema().columns(), ["price", "code"], "{query}");
                assert_eq!(output.rows.len(), usize::from(code == "a"), "{query}");
                if code == "a" {
                    assert_eq!(output.rows.value(0, 0), Some(ValueRef::Int(7)), "{query}");
                    assert_eq!(output.rows.value(0, 1), Some(ValueRef::String("a")), "{query}");
                }
            }
        }
    }
    for query in [
        "MATCH (s:Stock) RETURN s.price AS price, s.code AS code LIMIT 0",
        "MATCH (s:Stock) WHERE s.code = 'missing' RETURN s.price AS price, s.code AS code",
    ] {
        let output = database.query(query).unwrap();
        assert_eq!(output.schema().columns(), ["price", "code"]);
        assert!(output.rows.is_empty());
    }
}

#[test]
fn cypher_aggregate_and_numeric_projection_keep_declared_ordinals() {
    let mut database = Database::new();
    database
        .query("CREATE (:Stock {code: 'a', price: 7, rank: 1})")
        .unwrap();
    for (query, columns, values) in [
        (
            "MATCH (s:Stock) RETURN s.price AS z_price, s.rank AS a_rank",
            ["z_price", "a_rank"],
            [Value::Int(7), Value::Int(1)],
        ),
        (
            "MATCH (s:Stock) RETURN count(*) AS total, s.code AS code",
            ["total", "code"],
            [Value::Int(1), Value::String("a".into())],
        ),
        (
            "MATCH (s:Stock) RETURN s.price, s.code",
            ["s.price", "s.code"],
            [Value::Int(7), Value::String("a".into())],
        ),
    ] {
        let output = database.query(query).unwrap();
        assert_eq!(output.schema().columns(), columns);
        for (ordinal, value) in values.iter().enumerate() {
            assert_eq!(output.rows.value(0, ordinal), Some(value.as_ref()));
        }
    }
}

#[test]
fn cypher_duplicate_return_names_keep_collision_suffixes_even_without_rows() {
    let mut database = Database::new();
    database
        .query("CREATE (:Stock {code: 'a', price: 7})")
        .unwrap();
    for suffix in ["", " LIMIT 0"] {
        for predicate in ["", " WHERE s.code = 'missing'"] {
            for projection in [
                "s.price AS repeated, s.code AS repeated",
                "coalesce(s.price, 0), coalesce(s.code, '')",
            ] {
                let output = database
                    .query(&format!(
                        "MATCH (s:Stock){predicate} RETURN {projection}{suffix}"
                    ))
                    .unwrap();
                let base = if projection.starts_with("coalesce") {
                    "coalesce"
                } else {
                    "repeated"
                };
                assert_eq!(
                    output.schema().columns(),
                    [base.to_owned(), format!("{base}#2")]
                );
                if predicate.is_empty() && suffix.is_empty() {
                    assert_eq!(output.rows.value(0, 0), Some(ValueRef::Int(7)));
                    assert_eq!(output.rows.value(0, 1), Some(ValueRef::String("a")));
                } else {
                    assert!(output.rows.is_empty());
                }
            }
        }
    }
}

#[test]
fn cypher_mutation_return_preserves_declared_order_when_empty() {
    let mut database = Database::new();
    database
        .query("CREATE (:Stock {code: 'a', price: 7})")
        .unwrap();
    for code in ["a", "missing"] {
        let query = format!("MATCH (s:Stock) WHERE s.code = '{code}' SET s.price = 8 RETURN s.price AS price, s.code AS code");
        for output in [
            database.query(&query).unwrap(),
            database.begin_transaction().unwrap().query(&query).unwrap(),
        ] {
            assert_eq!(output.schema().columns(), ["price", "code"]);
            if code == "a" {
                assert_eq!(output.rows.value(0, 0), Some(ValueRef::Int(8)));
                assert_eq!(output.rows.value(0, 1), Some(ValueRef::String("a")));
            } else {
                assert!(output.rows.is_empty());
            }
        }
    }
}

#[test]
fn cypher_explain_declares_diagnostic_columns_across_front_doors() {
    let mut database = Database::new();
    database
        .query("CREATE (:Stock {code: 'a', price: 7})")
        .unwrap();
    for analyze in [false, true] {
        let mode = if analyze {
            "EXPLAIN ANALYZE"
        } else {
            "EXPLAIN"
        };
        let query = format!("{mode} MATCH (s:Stock) RETURN s.price AS price, s.code AS code");
        for output in [
            database.query(&query).unwrap(),
            database
                .begin_read_transaction()
                .unwrap()
                .query(&query)
                .unwrap(),
        ] {
            assert_eq!(
                output
                    .schema()
                    .columns()
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>(),
                hawdb_explain::json::explain_output_columns(analyze).collect::<Vec<_>>()
            );
            assert_eq!(
                &output.schema().columns()[..3],
                ["mode", "statement_kind", "plan"]
            );
            assert_eq!(output.rows.len(), 1);
            assert_eq!(
                output.rows.value(0, 0),
                Some(ValueRef::String(if analyze {
                    "explain_analyze"
                } else {
                    "explain"
                }))
            );
            assert!(output.rows.get(0, "plan").is_some());
        }
    }
}

#[cfg(feature = "graph-analytics")]
#[test]
fn cypher_procedure_and_fixed_summary_declare_empty_schemas() {
    let mut database = Database::new();
    database.query("CREATE NODE LABEL Memory").unwrap();
    database
        .query("CALL project_graph('graph', ['Memory'], ['LINK'])")
        .unwrap();
    for query in [
        "CALL page_rank('graph') RETURN node, pagerank_score",
        "CALL page_rank('graph') RETURN pagerank_score AS score, node",
    ] {
        let output = database.query(query).unwrap();
        let columns = if query.contains("AS score") {
            ["score", "node"]
        } else {
            ["node", "pagerank_score"]
        };
        assert_eq!(output.schema().columns(), columns);
        assert!(output.rows.is_empty());
    }
    let output = database
        .query(
            "MATCH (t:Thread) \
         OPTIONAL MATCH (ti:ThreadIdentity) WHERE ti.thread_node_id = t.id \
         WITH t, COUNT(ti) AS identity_refs \
         OPTIONAL MATCH (t)-[:CONTAINS]->(msg:Message) \
         WITH t, identity_refs, COUNT(msg) AS legacy_messages \
         OPTIONAL MATCH (t)-[:COMPACTS_TO]->(m:Memory) \
         RETURN t.id, t.thread_id, \
         CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END, \
         COALESCE(t.message_count, 0), identity_refs, legacy_messages, COUNT(m) \
         ORDER BY t.id ASC",
        )
        .unwrap();
    assert!(output.rows.is_empty());
    assert_eq!(
        output.schema().columns(),
        [
            "t.id",
            "t.thread_id",
            "CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END",
            "COALESCE(t.message_count, 0)",
            "identity_refs",
            "legacy_messages",
            "COUNT(m)",
        ]
    );
}
