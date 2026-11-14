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

//! The same public-facade contract runs natively and in a real browser.
use hawdb::{Database, DatabaseConfig, RuntimeTaskContext, Value};
use std::collections::BTreeMap;
use std::time::Duration;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use wasm_bindgen_test::{wasm_bindgen_test as test, wasm_bindgen_test_configure};
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
wasm_bindgen_test_configure!(run_in_dedicated_worker);

#[test]
fn writes_parameters_traversal_and_error_recovery_share_one_database() {
    let mut db = Database::new();
    db.query_with_params(
        "CREATE (:Memory {id: $id, title: $title})-[:MENTIONS]->(:Entity {name: 'Browser'})",
        &BTreeMap::from([
            ("id".into(), Value::Int(42)),
            ("title".into(), Value::String("WASM memory".into())),
        ]),
    )
    .unwrap();
    assert!(db.query("this is not Cypher").is_err());
    let output = db
        .query_with_params(
            "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) WHERE m.id = $id RETURN m.title AS title, e.name AS name",
            &BTreeMap::from([("id".into(), Value::Int(42))]),
        )
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("WASM memory".into()))
    );
    assert_eq!(
        output.rows[0].get("name"),
        Some(&Value::String("Browser".into()))
    );

    let mut transaction = db.begin_transaction().unwrap();
    transaction.query("CREATE (:Memory {id: 99})").unwrap();
    transaction.rollback();
    assert!(db
        .query("MATCH (m:Memory) WHERE m.id = 99 RETURN m.id AS id")
        .unwrap()
        .rows
        .is_empty());
    let mut transaction = db.begin_transaction().unwrap();
    transaction.query("CREATE (:Memory {id: 100})").unwrap();
    transaction.commit().unwrap();
    assert_eq!(
        db.query("MATCH (m:Memory) WHERE m.id = 100 RETURN m.id AS id")
            .unwrap()
            .rows
            .len(),
        1
    );
}

#[test]
fn json_value_groups_string_properties_without_host_row_scans() {
    let mut db = Database::new();
    for (id, metadata) in [
        (1, r#"{"original_source":"claude-code","count":3}"#),
        (2, r#"{"original_source":"claude-code"}"#),
        (3, r#"{"original_source":"mcp"}"#),
    ] {
        db.query_with_params(
            "CREATE (:Memory {id: $id, metadata: $metadata})",
            &BTreeMap::from([
                ("id".into(), Value::Int(id)),
                ("metadata".into(), Value::String(metadata.into())),
            ]),
        )
        .unwrap();
    }

    let grouped = db
        .query_with_params(
            "MATCH (m:Memory) WITH json_value(m.metadata, $path) AS origin, COUNT(*) AS total RETURN origin, total ORDER BY origin",
            &BTreeMap::from([(
                "path".into(),
                Value::String("$.original_source".into()),
            )]),
        )
        .unwrap();
    assert_eq!(grouped.rows.len(), 2);
    assert_eq!(
        grouped.rows[0].get("origin"),
        Some(&Value::String("claude-code".into()))
    );
    assert_eq!(grouped.rows[0].get("total"), Some(&Value::Int(2)));
    assert_eq!(
        grouped.rows[1].get("origin"),
        Some(&Value::String("mcp".into()))
    );
    assert_eq!(grouped.rows[1].get("total"), Some(&Value::Int(1)));

    let count = db
        .query_with_params(
            "MATCH (m:Memory) WHERE json_value(m.metadata, $path) = 'claude-code' RETURN COUNT(*) AS total",
            &BTreeMap::from([(
                "path".into(),
                Value::String("$.original_source".into()),
            )]),
        )
        .unwrap();
    assert_eq!(count.rows[0].get("total"), Some(&Value::Int(2)));

    let same_query =
        "MATCH (m:Memory) WHERE m.id = 1 RETURN json_value(m.metadata, $path) AS value";
    let source = db
        .query_with_params(
            same_query,
            &BTreeMap::from([("path".into(), Value::String("$.original_source".into()))]),
        )
        .unwrap();
    let numeric = db
        .query_with_params(
            same_query,
            &BTreeMap::from([("path".into(), Value::String("$.count".into()))]),
        )
        .unwrap();
    assert_eq!(
        source.rows[0].get("value"),
        Some(&Value::String("claude-code".into()))
    );
    assert_eq!(numeric.rows[0].get("value"), Some(&Value::Int(3)));

    let mut budgeted = Database::new_with_config(DatabaseConfig {
        max_read_result_rows: Some(1),
        ..DatabaseConfig::default()
    });
    budgeted
        .query(r#"CREATE (:Memory {metadata: '{"source":"a"}'})"#)
        .unwrap();
    budgeted
        .query(r#"CREATE (:Memory {metadata: '{"source":"b"}'})"#)
        .unwrap();
    let grouped_query = "MATCH (m:Memory) WITH json_value(m.metadata, '$.source') AS source, COUNT(*) AS total RETURN source, total";
    assert!(budgeted
        .query(grouped_query)
        .unwrap_err()
        .to_string()
        .contains("max_read_result_rows"));

    let mut payload_limited = Database::new_with_config(DatabaseConfig {
        max_read_result_payload_bytes: Some(16),
        ..DatabaseConfig::default()
    });
    payload_limited
        .query(r#"CREATE (:Memory {metadata: '{"source":"a-long-source-name"}'})"#)
        .unwrap();
    assert!(payload_limited
        .query("MATCH (m:Memory) RETURN json_value(m.metadata, '$.source') AS source")
        .unwrap_err()
        .to_string()
        .contains("max_read_result_payload_bytes"));

    let mut memory_limited = Database::new_with_config(DatabaseConfig {
        execution_memory: hawdb::executor::ExecutionMemoryConfig {
            query_memory_bytes: std::num::NonZeroUsize::new(1536).unwrap(),
            ..hawdb::executor::ExecutionMemoryConfig::default()
        },
        ..DatabaseConfig::default()
    });
    memory_limited
        .query(r#"CREATE (:Memory {metadata: '{"source":"limited"}'})"#)
        .unwrap();
    assert!(memory_limited
        .query("MATCH (m:Memory) RETURN m.metadata AS metadata")
        .is_ok());
    let memory_error = memory_limited
        .query("MATCH (m:Memory) RETURN json_value(m.metadata, '$.source') AS source")
        .unwrap_err()
        .to_string();
    assert!(
        memory_error.contains("query memory account"),
        "{memory_error}"
    );

    assert!(db
        .query_with_context(
            "MATCH (m:Memory) RETURN json_value(m.metadata, '$.original_source') AS source",
            &RuntimeTaskContext::with_timeout(Duration::ZERO),
        )
        .is_err());
}

#[test]
fn json_value_distinguishes_missing_invalid_and_non_string_documents() {
    let mut db = Database::new();
    db.query(r#"CREATE (:Memory {id: 1, metadata: '{"flag":true,"count":3,"nested":{"name":"Ada"},"nothing":null}'})"#)
        .unwrap();
    let row = &db
        .query("MATCH (m:Memory) RETURN json_value(m.metadata, '$.flag') AS flag, json_value(m.metadata, '$.count') AS count, json_value(m.metadata, '$.nested.name') AS name, json_value(m.metadata, '$.missing') AS missing, coalesce(json_value(m.metadata, '$.nothing'), 'fallback') AS fallback")
        .unwrap()
        .rows[0];
    assert_eq!(row.get("flag"), Some(&Value::Bool(true)));
    assert_eq!(row.get("count"), Some(&Value::Int(3)));
    assert_eq!(row.get("name"), Some(&Value::String("Ada".into())));
    assert_eq!(row.get("missing"), Some(&Value::Null));
    assert_eq!(row.get("fallback"), Some(&Value::String("fallback".into())));
    let case = db
        .query("MATCH (m:Memory) RETURN CASE WHEN json_value(m.metadata, '$.flag') = true THEN 'yes' ELSE 'no' END AS verdict")
        .unwrap();
    assert_eq!(
        case.rows[0].get("verdict"),
        Some(&Value::String("yes".into()))
    );

    assert!(db
        .query("MATCH (m:Memory) RETURN json_value(m.metadata, '$.nested') AS value")
        .unwrap_err()
        .to_string()
        .contains("not scalar"));
    assert!(db
        .query("MATCH (m:Memory) RETURN json_value(m.metadata, '$[0]') AS value")
        .unwrap_err()
        .to_string()
        .contains("path"));
    db.query("CREATE (:Memory {id: 2, metadata: '{invalid'})")
        .unwrap();
    assert!(db
        .query("MATCH (m:Memory) WHERE m.id = 2 RETURN json_value(m.metadata, '$.flag') AS value")
        .unwrap_err()
        .to_string()
        .contains("invalid JSON"));
    db.query("CREATE (:Memory {id: 3, metadata: 12})").unwrap();
    assert!(db
        .query("MATCH (m:Memory) WHERE m.id = 3 RETURN json_value(m.metadata, '$.flag') AS value")
        .unwrap_err()
        .to_string()
        .contains("string document"));
    db.query(r#"CREATE (:Memory {id: 5, metadata: '{"count":18446744073709551615}'})"#)
        .unwrap();
    assert!(db
        .query("MATCH (m:Memory) WHERE m.id = 5 RETURN json_value(m.metadata, '$.count') AS value")
        .unwrap_err()
        .to_string()
        .contains("signed 64-bit range"));
    assert!(db
        .query_with_params(
            "MATCH (m:Memory) WHERE m.id = 1 RETURN json_value(m.metadata, $path) AS value",
            &BTreeMap::from([("path".into(), Value::Int(1))]),
        )
        .unwrap_err()
        .to_string()
        .contains("path must be a string"));
    db.query_with_params(
        "CREATE (:Memory {id: 4, metadata: $metadata})",
        &BTreeMap::from([(
            "metadata".into(),
            Value::String(format!(r#"{{"field":"{}"}}"#, "a".repeat(1024 * 1024))),
        )]),
    )
    .unwrap();
    assert!(db
        .query("MATCH (m:Memory) WHERE m.id = 4 RETURN json_value(m.metadata, '$.field') AS value")
        .unwrap_err()
        .to_string()
        .contains("1 MiB input limit"));
}

#[test]
fn json_string_selects_decoded_strings_before_numeric_conversion() {
    let mut db = Database::new();
    let cases = [
        (Value::Null, "$.state", Value::Null),
        (Value::String("null".into()), "$.state", Value::Null),
        (Value::String("{}".into()), "$.state", Value::Null),
        (
            Value::String(r#"{"state":" active "}"#.into()),
            "$.state",
            Value::String(" active ".into()),
        ),
        (
            Value::String(r#"{"\u0073tate":"a\n\"b\\c\u2603","nested":{"state":"nested"}}"#.into()),
            "$.state",
            Value::String("a\n\"b\\c\u{2603}".into()),
        ),
        (
            Value::String(r#"{"state":"top","nested":{"state":"nested"}}"#.into()),
            "$.nested.state",
            Value::String("nested".into()),
        ),
        (
            Value::String(r#"{"state":"","other":"bound"}"#.into()),
            "$.state",
            Value::String(String::new()),
        ),
        (
            Value::String(r#"{"state":"wrong","other":"bound"}"#.into()),
            "$.other",
            Value::String("bound".into()),
        ),
    ];
    let non_strings = [
        "null",
        "true",
        "false",
        "0",
        "-9223372036854775808",
        "9223372036854775807",
        "9223372036854775808",
        "18446744073709551615",
        "1.25",
        "-1.25e100",
        "[]",
        "{}",
    ];
    let cases = cases
        .into_iter()
        .chain(non_strings.into_iter().map(|value| {
            (
                Value::String(format!(r#"{{"state":{value}}}"#)),
                "$.state",
                Value::Null,
            )
        }));
    db.query("CREATE (:Memory {id: 1})").unwrap();
    for (metadata, path, expected) in cases {
        db.query_with_params(
            "MATCH (m:Memory) SET m.metadata = $metadata",
            &BTreeMap::from([("metadata".into(), metadata.clone())]),
        )
        .unwrap();
        let result = db
            .query_with_params(
                "MATCH (m:Memory) RETURN json_string(m.metadata, $path) AS value",
                &BTreeMap::from([("path".into(), Value::String(path.into()))]),
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].get("value"),
            Some(&expected),
            "{metadata:?}, {path}"
        );
    }
}

#[test]
fn json_string_filters_and_aggregates_preserve_non_string_absence() {
    let mut db = Database::new();
    for (id, metadata) in [
        (1, r#"{"state":"active"}"#),
        (2, r#"{"state":18446744073709551615}"#),
        (3, r#"{"state":[]}"#),
        (4, r#"{"state":"archived"}"#),
        (5, r#"{"nested":{"state":"archived"}}"#),
    ] {
        db.query_with_params(
            "CREATE (:Memory {id: $id, metadata: $metadata})",
            &BTreeMap::from([
                ("id".into(), Value::Int(id)),
                ("metadata".into(), Value::String(metadata.into())),
            ]),
        )
        .unwrap();
    }
    let parameters = BTreeMap::from([("path".into(), Value::String("$.state".into()))]);
    let filtered = db.query_with_params("MATCH (m:Memory) WHERE coalesce(json_string(m.metadata, $path), 'active') = 'active' RETURN COUNT(*) AS total", &parameters).unwrap();
    assert_eq!(filtered.rows[0].get("total"), Some(&Value::Int(4)));
    for aggregate in ["COUNT(*)", "COUNT(DISTINCT m.id)"] {
        let result = db.query_with_params(&format!("MATCH (m:Memory) WITH json_string(m.metadata, $path) AS state, {aggregate} AS total RETURN state, total"), &parameters).unwrap();
        assert_eq!(result.rows.len(), 3);
        for (state, count) in [
            (Value::Null, 3),
            (Value::String("active".into()), 1),
            (Value::String("archived".into()), 1),
        ] {
            let row = result
                .rows
                .iter()
                .find(|row| row.get("state") == Some(&state))
                .unwrap();
            assert_eq!(row.get("total"), Some(&Value::Int(count)));
        }
    }
    let result = db.query_with_params("MATCH (m:Memory) WHERE m.id = 2 RETURN CASE WHEN json_string(m.metadata, $path) IS NULL THEN 'absent' ELSE 'present' END AS state", &parameters).unwrap();
    assert_eq!(
        result.rows[0].get("state"),
        Some(&Value::String("absent".into()))
    );
}

#[test]
fn json_string_preserves_strict_document_path_and_cancellation_errors() {
    let mut db = Database::new();
    for (metadata, expected) in [
        (Value::String("{invalid".into()), "invalid JSON"),
        (Value::Int(12), "string document"),
        (
            Value::String(format!(r#"{{"state":"{}"}}"#, "a".repeat(1024 * 1024))),
            "1 MiB input limit",
        ),
    ] {
        db.query("MATCH (m:Memory) DELETE m").unwrap();
        db.query_with_params(
            "CREATE (:Memory {id: 1, metadata: $metadata})",
            &BTreeMap::from([("metadata".into(), metadata)]),
        )
        .unwrap();
        for query in [
            "MATCH (m:Memory) RETURN json_string(m.metadata, '$.state') AS value",
            "MATCH (m:Memory) WHERE coalesce(json_string(m.metadata, '$.state'), '') = '' RETURN COUNT(*) AS total",
            "MATCH (m:Memory) WITH json_string(m.metadata, '$.state') AS value, COUNT(*) AS total RETURN value, total",
            "MATCH (m:Memory) WITH json_string(m.metadata, '$.state') AS value, COUNT(DISTINCT m.id) AS total RETURN value, total",
        ] {
            let error = db.query(query).unwrap_err();
            assert!(matches!(&error, hawdb::HawDBError::Execution(message) if message.contains(expected)), "{query}: {error}");
        }
    }
    for path in [
        Value::Int(1),
        Value::Null,
        Value::String("$[0]".into()),
        Value::String("$.".into()),
        Value::String("$.a..b".into()),
        Value::String(format!("$.{}", "a".repeat(255))),
    ] {
        let error = db
            .query_with_params(
                "MATCH (m:Memory) RETURN json_string(m.metadata, $path)",
                &BTreeMap::from([("path".into(), path)]),
            )
            .unwrap_err();
        assert!(matches!(error, hawdb::HawDBError::Semantic(_)), "{error}");
    }
    let error = db
        .query_with_context(
            "MATCH (m:Memory) RETURN json_string(m.metadata, '$.state')",
            &RuntimeTaskContext::with_timeout(Duration::ZERO),
        )
        .unwrap_err();
    assert!(
        matches!(&error, hawdb::HawDBError::Execution(message) if message.contains("runtime task stopped: deadline_exceeded")),
        "{error}"
    );
}

#[test]
fn json_value_null_on_nonscalar_maps_only_selected_arrays_and_objects_to_null() {
    let mut db = Database::new();
    for (id, metadata) in [
        (
            1,
            Value::String(
                r#"{"missing_sentinel":null,"flag":true,"count":3,"ratio":1.5,"name":"Ada\nLovelace","array":[1],"object":{"nested":1},"state":"active"}"#
                    .into(),
            ),
        ),
        (2, Value::String(r#"{"state":[]}"#.into())),
        (3, Value::String(r#"{"state":{}}"#.into())),
        (4, Value::String(r#"{}"#.into())),
        (5, Value::String(r#"{"state":null}"#.into())),
        (6, Value::String(r#"{"state":"inactive"}"#.into())),
        (7, Value::Null),
    ] {
        db.query_with_params(
            "CREATE (:Memory {id: $id, metadata: $metadata})",
            &BTreeMap::from([("id".into(), Value::Int(id)), ("metadata".into(), metadata)]),
        )
        .unwrap();
    }

    let row = &db
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN json_value(m.metadata, '$.missing' NULL ON NONSCALAR) AS missing, json_value(m.metadata, '$.missing_sentinel' NULL ON NONSCALAR) AS json_null, json_value(m.metadata, '$.flag' NULL ON NONSCALAR) AS flag, json_value(m.metadata, '$.count' NULL ON NONSCALAR) AS count, json_value(m.metadata, '$.ratio' NULL ON NONSCALAR) AS ratio, json_value(m.metadata, '$.name' NULL ON NONSCALAR) AS name, json_value(m.metadata, '$.array' NULL ON NONSCALAR) AS array, json_value(m.metadata, '$.object' NULL ON NONSCALAR) AS object")
        .unwrap()
        .rows[0];
    for name in ["missing", "json_null", "array", "object"] {
        assert_eq!(row.get(name), Some(&Value::Null), "{name}");
    }
    assert_eq!(row.get("flag"), Some(&Value::Bool(true)));
    assert_eq!(row.get("count"), Some(&Value::Int(3)));
    assert_eq!(row.get("ratio"), Some(&Value::Float(1.5)));
    assert_eq!(
        row.get("name"),
        Some(&Value::String("Ada\nLovelace".into()))
    );

    let parameters = BTreeMap::from([("path".into(), Value::String("$.state".into()))]);
    let active_or_absent = db
        .query_with_params(
            "MATCH (m:Memory) WHERE coalesce(json_value(m.metadata, $path NULL ON NONSCALAR), 'active') = 'active' RETURN COUNT(*) AS total",
            &parameters,
        )
        .unwrap();
    assert_eq!(active_or_absent.rows[0].get("total"), Some(&Value::Int(6)));

    let grouped = db
        .query_with_params(
            "MATCH (m:Memory) WITH json_value(m.metadata, $path NULL ON NONSCALAR) AS state, COUNT(*) AS total RETURN state, total ORDER BY total DESC",
            &parameters,
        )
        .unwrap();
    assert_eq!(grouped.rows.len(), 3);
    assert_eq!(grouped.rows[0].get("state"), Some(&Value::Null));
    assert_eq!(grouped.rows[0].get("total"), Some(&Value::Int(5)));

    let case = db
        .query("MATCH (m:Memory) WHERE m.id = 2 RETURN CASE WHEN json_value(m.metadata, '$.state' NULL ON NONSCALAR) IS NULL THEN 'absent' ELSE 'present' END AS visibility")
        .unwrap();
    assert_eq!(
        case.rows[0].get("visibility"),
        Some(&Value::String("absent".into()))
    );

    assert!(db
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN json_value(m.metadata, '$.object') AS value")
        .unwrap_err()
        .to_string()
        .contains("JSON_VALUE selected value is not scalar"));
}

#[test]
fn json_value_null_on_nonscalar_preserves_other_errors() {
    let mut db = Database::new();
    for (id, metadata) in [
        (1, Value::String("{invalid".into())),
        (2, Value::Int(12)),
        (3, Value::String(r#"{"value":18446744073709551615}"#.into())),
        (
            4,
            Value::String(format!(r#"{{"value":"{}"}}"#, "a".repeat(1024 * 1024))),
        ),
    ] {
        db.query_with_params(
            "CREATE (:Memory {id: $id, metadata: $metadata})",
            &BTreeMap::from([("id".into(), Value::Int(id)), ("metadata".into(), metadata)]),
        )
        .unwrap();
    }

    for (id, expected) in [
        (1, "invalid JSON"),
        (2, "string document"),
        (3, "signed 64-bit range"),
        (4, "1 MiB input limit"),
    ] {
        let error = db
            .query(&format!(
                "MATCH (m:Memory) WHERE m.id = {id} RETURN json_value(m.metadata, '$.value' NULL ON NONSCALAR) AS value"
            ))
            .unwrap_err();
        assert!(matches!(error, hawdb::HawDBError::Execution(_)), "{error}");
        assert!(error.to_string().contains(expected), "{error}");
    }
    let path_error = db
        .query_with_params(
            "MATCH (m:Memory) WHERE m.id = 1 RETURN json_value(m.metadata, $path NULL ON NONSCALAR) AS value",
            &BTreeMap::from([("path".into(), Value::Int(1))]),
        )
        .unwrap_err();
    assert!(
        path_error
            .to_string()
            .contains("JSON_VALUE path must be a string"),
        "{path_error}"
    );

    let mut memory_limited = Database::new_with_config(DatabaseConfig {
        execution_memory: hawdb::executor::ExecutionMemoryConfig {
            query_memory_bytes: std::num::NonZeroUsize::new(1536).unwrap(),
            ..hawdb::executor::ExecutionMemoryConfig::default()
        },
        ..DatabaseConfig::default()
    });
    memory_limited
        .query(r#"CREATE (:Memory {metadata: '{"value":"limited"}'})"#)
        .unwrap();
    let memory_error = memory_limited
        .query(
            "MATCH (m:Memory) RETURN json_value(m.metadata, '$.value' NULL ON NONSCALAR) AS value",
        )
        .unwrap_err();
    assert!(
        matches!(memory_error, hawdb::HawDBError::ResourceBudgetExceeded(_)),
        "{memory_error}"
    );

    let cancellation = db
        .query_with_context(
            "MATCH (m:Memory) RETURN json_value(m.metadata, '$.value' NULL ON NONSCALAR) AS value",
            &RuntimeTaskContext::with_timeout(Duration::ZERO),
        )
        .unwrap_err();
    assert!(
        matches!(&cancellation, hawdb::HawDBError::Execution(message) if message.contains("runtime task stopped: deadline_exceeded")),
        "{cancellation}"
    );
}

#[test]
fn json_value_group_keys_propagate_errors_in_partial_and_buffered_aggregates() {
    let parameters = BTreeMap::from([("path".into(), Value::String("$.source".into()))]);
    for (metadata, expected) in [
        (Value::String("{invalid".into()), "invalid JSON"),
        (Value::Int(12), "string document"),
        (
            Value::String(r#"{"source":{"nested":1}}"#.into()),
            "not scalar",
        ),
        (Value::String(r#"{"source":[1]}"#.into()), "not scalar"),
        (
            Value::String(r#"{"source":18446744073709551615}"#.into()),
            "signed 64-bit range",
        ),
        (
            Value::String(format!(r#"{{"source":"{}"}}"#, "a".repeat(1024 * 1024))),
            "1 MiB input limit",
        ),
    ] {
        let mut db = Database::new();
        db.query(r#"CREATE (:Memory {id: 1, metadata: '{"source":"valid"}'})"#)
            .unwrap();
        db.query_with_params(
            "CREATE (:Memory {id: 2, metadata: $metadata})",
            &BTreeMap::from([("metadata".into(), metadata)]),
        )
        .unwrap();
        for aggregate in ["COUNT(*)", "COUNT(DISTINCT m.id)"] {
            for expression in [
                "json_value(m.metadata, $path)",
                "coalesce(json_value(m.metadata, $path), 'fallback')",
                "CASE WHEN m.id > 0 THEN json_value(m.metadata, $path) ELSE NULL END",
            ] {
                let query = format!(
                    "MATCH (m:Memory) WITH {expression} AS origin, {aggregate} AS total RETURN origin, total"
                );
                let error = db.query_with_params(&query, &parameters).unwrap_err();
                assert!(matches!(error, hawdb::HawDBError::Execution(_)), "{error}");
                assert!(error.to_string().contains(expected), "{query}: {error}");
            }
        }
        let count = db
            .query("MATCH (m:Memory) RETURN COUNT(*) AS total")
            .unwrap();
        assert_eq!(count.rows[0].get("total"), Some(&Value::Int(2)));
    }
}

#[test]
fn json_value_group_keys_preserve_missing_and_null_values() {
    let mut db = Database::new();
    for (id, metadata) in [
        (1, Value::Null),
        (2, Value::String("{}".into())),
        (3, Value::String(r#"{"source":null}"#.into())),
        (4, Value::String(r#"{"source":"valid"}"#.into())),
    ] {
        db.query_with_params(
            "CREATE (:Memory {id: $id, metadata: $metadata})",
            &BTreeMap::from([("id".into(), Value::Int(id)), ("metadata".into(), metadata)]),
        )
        .unwrap();
    }
    for aggregate in ["COUNT(*)", "COUNT(DISTINCT m.id)"] {
        for (expression, missing) in [
            ("json_value(m.metadata, $path)", Value::Null),
            (
                "coalesce(json_value(m.metadata, $path), 'fallback')",
                Value::String("fallback".into()),
            ),
        ] {
            let query = format!(
                "MATCH (m:Memory) WITH {expression} AS origin, {aggregate} AS total RETURN origin, total"
            );
            let result = db
                .query_with_params(
                    &query,
                    &BTreeMap::from([("path".into(), Value::String("$.source".into()))]),
                )
                .unwrap();
            assert_eq!(result.rows.len(), 2, "{query}");
            for (origin, total) in [(missing, 3), (Value::String("valid".into()), 1)] {
                let row = result
                    .rows
                    .iter()
                    .find(|row| row.get("origin") == Some(&origin))
                    .unwrap();
                assert_eq!(row.get("total"), Some(&Value::Int(total)), "{query}");
            }
        }
    }
}

#[test]
fn json_expression_positions_share_configured_and_runtime_memory_admission() {
    use hawdb::{HawDBError, ResourceBudgetScope, RuntimeMemoryReservation};
    use std::num::NonZeroUsize;

    let cases = [
        ("MATCH (m:Memory) RETURN json_value(m.metadata, $path) AS value", "$.source", vec![vec![Value::String("ok".into())]; 2], "external_read"),
        ("MATCH (m:Memory) WHERE json_value(m.metadata, $path) = 'ok' RETURN COUNT(*) AS total", "$.source", vec![vec![Value::Int(2)]], "external_read"),
        ("MATCH (m:Memory) WHERE json_value(m.metadata, $path) <> '' RETURN COUNT(*) AS total", "$.source", vec![vec![Value::Int(2)]], "external_read"),
        ("MATCH (m:Memory) WHERE json_value(m.metadata, $path) > 'a' RETURN COUNT(*) AS total", "$.source", vec![vec![Value::Int(2)]], "external_read"),
        ("MATCH (m:Memory) WHERE json_value(m.metadata, $path) IN ['ok'] RETURN COUNT(*) AS total", "$.source", vec![vec![Value::Int(2)]], "external_read"),
        ("MATCH (m:Memory) WHERE json_value(m.metadata, $path) CONTAINS 'o' RETURN COUNT(*) AS total", "$.source", vec![vec![Value::Int(2)]], "external_read"),
        ("MATCH (m:Memory) WHERE coalesce(json_value(m.metadata, $path), '') = 'ok' RETURN COUNT(*) AS total", "$.source", vec![vec![Value::Int(2)]], "external_read"),
        ("MATCH (m:Memory) WHERE CASE WHEN m.id > 0 THEN json_value(m.metadata, $path) ELSE '' END = 'ok' RETURN COUNT(*) AS total", "$.source", vec![vec![Value::Int(2)]], "external_read"),
        ("MATCH (m:Memory) WITH json_value(m.metadata, $path) AS origin, COUNT(*) AS total RETURN origin, total", "$.source", vec![vec![Value::String("ok".into()), Value::Int(2)]], "blocking_state"),
        ("MATCH (m:Memory) WITH json_value(m.metadata, $path) AS origin, COUNT(DISTINCT m.id) AS total RETURN origin, total", "$.source", vec![vec![Value::String("ok".into()), Value::Int(2)]], "blocking_state"),
        ("MATCH (m:Memory) RETURN m.id AS id ORDER BY json_value(m.metadata, $path)", "$.rank", vec![vec![Value::Int(2)], vec![Value::Int(1)]], "blocking_state"),
        ("MATCH (m:Memory) RETURN m.id AS id ORDER BY json_value(m.metadata, $path) LIMIT 1", "$.rank", vec![vec![Value::Int(2)]], "blocking_state"),
    ];
    let cases: Vec<_> = cases
        .into_iter()
        .flat_map(|(query, path, expected, memory_class)| {
            let mut variants = vec![(query.to_owned(), path, expected.clone(), memory_class)];
            if path == "$.source" {
                variants.push((
                    query.replace("json_value(", "json_string("),
                    path,
                    expected,
                    memory_class,
                ));
            }
            variants
        })
        .collect();
    for (configured, runtime) in [
        (8192, None),
        (65536, Some(8192)),
        (8192, Some(65536)),
        (65536, None),
        (65536, Some(65536)),
    ] {
        let mut db = Database::new_with_config(DatabaseConfig {
            execution_memory: hawdb::executor::ExecutionMemoryConfig {
                query_memory_bytes: NonZeroUsize::new(configured).unwrap(),
                batch_rows: NonZeroUsize::MIN,
                batch_payload_bytes: NonZeroUsize::new(2048).unwrap(),
                ..Default::default()
            },
            ..Default::default()
        });
        for (id, rank) in [(1, 2), (2, 1)] {
            let metadata = format!(
                r#"{{"source":"ok","rank":{rank},"padding":"{}"}}"#,
                "x".repeat(256)
            );
            db.query_with_params(
                "CREATE (:Memory {id: $id, metadata: $metadata})",
                &BTreeMap::from([
                    ("id".into(), Value::Int(id)),
                    ("metadata".into(), Value::String(metadata)),
                ]),
            )
            .unwrap();
        }
        let context = runtime.map_or_else(RuntimeTaskContext::default, |bytes| {
            RuntimeTaskContext::default()
                .with_memory_reservation(RuntimeMemoryReservation::new(bytes, 65536))
        });
        // The source rows fit even at the low limit; only JSON working memory refuses.
        assert_eq!(
            db.query_with_context("MATCH (m:Memory) RETURN m.metadata AS value", &context)
                .unwrap()
                .rows
                .len(),
            2
        );
        for (query, path, expected, memory_class) in &cases {
            let parameters = BTreeMap::from([("path".into(), Value::String((*path).into()))]);
            let result = db.query_with_params_context(query, &parameters, &context);
            if configured == 8192 || runtime == Some(8192) {
                let error = result.unwrap_err();
                let HawDBError::ResourceBudgetExceeded(cause) = error else {
                    panic!("{query}: expected typed memory refusal, got {error}");
                };
                assert_eq!(cause.budget, 8192, "{query}");
                assert!(cause.requested > 8192, "{query}: {cause:?}");
                assert_eq!(cause.memory_class, Some(*memory_class), "{query}");
                let (scope, origin) = if configured == 8192 {
                    (
                        ResourceBudgetScope::Account,
                        "ExecutionMemoryConfig.query_memory_bytes",
                    )
                } else {
                    (
                        ResourceBudgetScope::Query,
                        "RuntimeTaskContext.memory_reservation.memory_bytes",
                    )
                };
                assert_eq!(cause.scope, scope, "{query}");
                assert_eq!(cause.budget_option, origin, "{query}");
            } else {
                let result = result.unwrap_or_else(|error| panic!("{query}: {error}"));
                assert_eq!(
                    result
                        .rows
                        .value_rows()
                        .map(<[Value]>::to_vec)
                        .collect::<Vec<_>>(),
                    *expected,
                    "{query}"
                );
            }
        }
        assert_eq!(
            db.query_with_context("MATCH (m:Memory) RETURN COUNT(*) AS total", &context)
                .unwrap()
                .rows[0]
                .get("total"),
            Some(&Value::Int(2))
        );
    }
}

#[test]
fn browser_clock_and_randomness_generate_distinct_uuidv7_values() {
    let mut db = Database::new();
    db.query_sql("CREATE TABLE items (id BIGINT PRIMARY KEY)")
        .unwrap();
    db.query_sql("INSERT INTO items (id) VALUES (1)").unwrap();
    let first = db.query_sql("SELECT uuidv7() AS id FROM items").unwrap();
    let second = db.query_sql("SELECT uuidv7() AS id FROM items").unwrap();
    assert_ne!(first.rows[0].get("id"), second.rows[0].get("id"));
    let Some(Value::Uuid(id)) = first.rows[0].get("id") else {
        panic!("expected UUID value")
    };
    assert_eq!(id.get_version_num(), 7);
}

#[test]
fn expired_deadline_returns_an_error_without_poisoning_the_database() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1})").unwrap();
    let context = RuntimeTaskContext::with_timeout(Duration::ZERO);
    assert!(db
        .query_with_context("MATCH (m:Memory) RETURN m.id AS id", &context)
        .is_err());
    assert_eq!(
        db.query("MATCH (m:Memory) RETURN m.id AS id")
            .unwrap()
            .rows
            .len(),
        1
    );
}

#[test]
fn result_budget_returns_an_error_instead_of_partial_rows() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_read_result_rows: Some(1),
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 1})").unwrap();
    db.query("CREATE (:Memory {id: 2})").unwrap();
    let error = db.query("MATCH (m:Memory) RETURN m.id AS id").unwrap_err();
    assert!(
        error.to_string().contains("max_read_result_rows"),
        "{error}"
    );
    assert_eq!(
        db.query("MATCH (m:Memory) RETURN m.id AS id LIMIT 1")
            .unwrap()
            .rows
            .len(),
        1
    );
}

#[test]
fn segment_read_waves_keep_order_and_budget_on_the_serial_backend() {
    use hawdb::{
        SegmentBytes, SegmentRangeReader, SegmentReadError, SegmentReadExecutor, SegmentReadRange,
        SegmentReadScheduler,
    };
    use std::num::{NonZeroU64, NonZeroUsize};

    struct MemoryReader;
    impl SegmentRangeReader for MemoryReader {
        fn read_range(&self, range: &SegmentReadRange) -> Result<SegmentBytes, SegmentReadError> {
            Ok(vec![range.offset as u8; range.length.get() as usize].into())
        }
    }

    let schedule = SegmentReadScheduler::new(NonZeroUsize::new(2).unwrap(), NonZeroU64::MIN)
        .schedule([
            SegmentReadRange::new(1, 1, 0, NonZeroU64::MIN),
            SegmentReadRange::new(1, 2, 1, NonZeroU64::MIN),
        ]);
    let mut bytes = Vec::new();
    let report = SegmentReadExecutor::new(NonZeroU64::new(2).unwrap())
        .execute(&MemoryReader, &schedule, |payload| {
            bytes.extend_from_slice(&payload.bytes);
            Ok::<_, std::convert::Infallible>(())
        })
        .unwrap();
    assert_eq!(bytes, [0, 1]);
    assert_eq!(report.range_count, 2);
    let error = SegmentReadExecutor::new(NonZeroU64::MIN)
        .execute(
            &MemoryReader,
            &schedule,
            |_| -> Result<(), std::convert::Infallible> {
                panic!("an over-budget wave must not deliver a partial result");
            },
        )
        .unwrap_err();
    assert!(error.to_string().contains("budget"), "{error}");
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[test]
fn persistent_open_is_rejected() {
    let error = match Database::open("browser-database") {
        Ok(_) => panic!("persistent open must fail"),
        Err(error) => error,
    };
    assert!(error
        .to_string()
        .contains("persistent storage is unavailable"));
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[test]
fn sort_that_needs_disk_spill_fails_explicitly() {
    let mut config = DatabaseConfig::default();
    config.execution_memory.blocking_operator_bytes =
        std::num::NonZeroUsize::new(64 * 1024).unwrap();
    let mut db = Database::new_with_config(config);
    for id in 0..128 {
        db.query_with_params(
            "CREATE (:Memory {id: $id, title: $title})",
            &BTreeMap::from([
                ("id".into(), Value::Int(id)),
                (
                    "title".into(),
                    Value::String(format!("{id:04}{}", "x".repeat(2048))),
                ),
            ]),
        )
        .unwrap();
    }
    let error = db
        .query("MATCH (m:Memory) RETURN m.title AS title ORDER BY title DESC")
        .unwrap_err();
    assert!(
        error.to_string().contains("disk spill is unavailable"),
        "{error}"
    );
    assert_eq!(
        db.query("MATCH (m:Memory) WHERE m.id = 0 RETURN m.id AS id")
            .unwrap()
            .rows
            .len(),
        1
    );
}

#[test]
fn fromless_select_evaluates_once_and_rejects_column_references() {
    let mut db = Database::new();
    let output = db
        .query_sql("SELECT 1 AS one, version() AS server_version")
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].get("one"), Some(&Value::Int(1)));
    assert!(matches!(
        output.rows[0].get("server_version"),
        Some(Value::String(version)) if version.starts_with("HawDB ")
    ));
    assert!(db.query_sql("SELECT does_not_exist").is_err());
}

#[test]
fn database_instance_identity_is_stable_and_distinguishes_equal_state() {
    let mut database = Database::new();
    let identity = database.instance_identity().unwrap();
    let another = Database::new();
    assert_eq!(
        database.commit_epoch().unwrap(),
        another.commit_epoch().unwrap()
    );
    assert_ne!(identity, another.instance_identity().unwrap());
    assert_eq!(identity.clone(), identity);
    database.query("CREATE (:Memory {id: 'instance'})").unwrap();
    assert_eq!(database.instance_identity().unwrap(), identity);
    let moved = database;
    assert_eq!(moved.instance_identity().unwrap(), identity);
    drop(moved);
    assert_ne!(Database::new().instance_identity().unwrap(), identity);
}

#[test]
fn consumed_retained_node_snapshot_keeps_old_data_and_task_bound_after_database_drop() {
    use hawdb::{
        RetainedNodeField, RetainedNodeSnapshotCursor, RetainedQueryOptions, RetainedQueryStatus,
    };

    let mut db = Database::new();
    db.query("CREATE NODE TABLE Item").unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE Item(key) TYPE INT")
        .unwrap();
    db.query("CREATE INDEX ON :Item(key)").unwrap();
    db.query("CREATE CONSTRAINT ON :Item(key) ASSERT UNIQUE")
        .unwrap();
    db.query_with_params(
        "UNWIND $rows AS row CREATE (:Item {key: row.key, text: 'original'})",
        &BTreeMap::from([(
            "rows".into(),
            Value::List(
                (0..17)
                    .map(|key| Value::Map(BTreeMap::from([("key".into(), Value::Int(key))])))
                    .collect(),
            ),
        )]),
    )
    .unwrap();
    let context = RuntimeTaskContext::default();
    let cancellation = context.cancellation().clone();
    let snapshot = db.begin_read_transaction_with_context(&context).unwrap();
    drop(context);
    let parameters = BTreeMap::from([("key".into(), Value::Int(7))]);
    db.query_with_params(
        "MATCH (n:Item {key: $key}) SET n.text = 'current'",
        &parameters,
    )
    .unwrap();
    let mut cursor: RetainedNodeSnapshotCursor = snapshot
        .into_retained_node_query(
            "MATCH (n:Item {key: $key}) RETURN n AS node",
            &parameters,
            RetainedQueryOptions::default(),
        )
        .unwrap();
    drop(db);
    let view = cursor.next_node().unwrap().unwrap();
    assert!(matches!(view.field("text").unwrap(),
        Some(RetainedNodeField::Value(Value::String(text))) if text == "original"));
    cancellation.cancel();
    assert!(cursor.next_node().is_err());
    assert_eq!(cursor.status(), RetainedQueryStatus::Failed);
    drop(cursor);
    assert_eq!(view.status(), RetainedQueryStatus::Failed);
    assert!(matches!(view.field("text").unwrap(),
        Some(RetainedNodeField::Value(Value::String(text))) if text == "original"));
}

#[cfg(not(feature = "vector-search"))]
#[test]
fn schema_bound_vector_index_refuses_unavailable_native_backend() {
    use hawdb::{HawDBError, RuntimeCapability, VectorIndexBuildError, VectorIndexField};
    let mut db = Database::new();
    let error = db
        .create_vector_index(
            "unavailable",
            &VectorIndexField::SqlColumn {
                table: "items".into(),
                column: "embedding".into(),
            },
            "unavailable-vector-index",
            hawdb::SearchProjectionConsumerOptions::new(std::num::NonZeroU64::new(10).unwrap()),
            Default::default(),
        )
        .unwrap_err();
    assert!(matches!(
        error,
        VectorIndexBuildError::Query(HawDBError::CapabilityUnavailable {
            capability: RuntimeCapability::VectorSearch,
        })
    ));
}
