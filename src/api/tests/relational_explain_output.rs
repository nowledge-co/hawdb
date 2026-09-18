use super::*;

fn fixture() -> Database {
    let mut database = Database::new();
    database
        .query_sql("CREATE TABLE explain_docs (id BIGINT PRIMARY KEY, body TEXT NOT NULL)")
        .unwrap();
    database
        .query_sql("INSERT INTO explain_docs (id, body) VALUES (1, 'one'), (2, 'two')")
        .unwrap();
    database
}

#[test]
fn migrated_explain_keeps_planner_coverage_and_execution_evidence() {
    let mut database = fixture();
    for (predicate, residual) in [("id = $1", false), ("body = $1", true)] {
        let parameter = if residual {
            Value::String("one".into())
        } else {
            Value::Int(1)
        };
        for analyze in [false, true] {
            let sql = format!(
                "EXPLAIN {}SELECT d.id FROM explain_docs d WHERE {predicate}",
                if analyze { "ANALYZE " } else { "" }
            );
            let output = database
                .query_sql_with_params(&sql, std::slice::from_ref(&parameter))
                .unwrap();
            assert_eq!(output.rows.iter().any(|row| matches!(row.get("id"), Some(Value::String(id)) if id.contains("logical_selection"))), residual);
            for row in output.rows.iter() {
                assert_eq!(row.get("actRows").is_some(), analyze);
                if matches!(row.get("id"), Some(Value::String(id)) if id.contains("logical_"))
                    && analyze
                {
                    assert_eq!(row.get("actRows"), Some(&Value::Null));
                }
            }
            let access = output.rows.iter().find(|row| matches!(row.get("access object"), Some(Value::String(object)) if object.starts_with("table:explain_docs"))).unwrap();
            assert!(
                matches!(access.get("operator info"), Some(Value::String(info)) if info.contains("row_runtime_path="))
            );
            if analyze {
                assert_eq!(
                    access.get("actRows"),
                    Some(&Value::Int(if residual { 2 } else { 1 }))
                );
            }
        }
    }
}

#[test]
fn migrated_explain_facade_refuses_report_truncation() {
    let mut database = fixture();
    for analyze in [false, true] {
        let sql = format!(
            "EXPLAIN {}SELECT id FROM explain_docs LIMIT 1",
            if analyze { "ANALYZE " } else { "" }
        );
        let output = database.query_sql(&sql).unwrap();
        let rows = output.rows.len();
        assert!(rows > 1);
        let exact = database
            .query_sql_with_params_options(
                &sql,
                &[],
                QueryStreamOptions {
                    max_rows: Some(rows),
                    max_payload_bytes: None,
                },
            )
            .unwrap();
        assert_eq!(exact.rows.len(), rows);
        for (options, diagnostic) in [
            (
                QueryStreamOptions {
                    max_rows: Some(rows - 1),
                    max_payload_bytes: None,
                },
                format!("max_output_rows {}", rows - 1),
            ),
            (
                QueryStreamOptions {
                    max_rows: None,
                    max_payload_bytes: Some(1),
                },
                "max_output_payload_bytes 1".into(),
            ),
        ] {
            let error = database
                .query_sql_with_params_options(&sql, &[], options)
                .unwrap_err();
            assert!(error.to_string().contains(&diagnostic), "{error}");
        }
    }
}
