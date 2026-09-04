use super::*;

fn ordering_fixture() -> Database {
    let mut database = Database::new();
    database
        .query_sql(
            "CREATE TABLE feeds (\
                id BIGINT PRIMARY KEY, \
                category TEXT NOT NULL, \
                source_title TEXT NOT NULL, \
                title TEXT NOT NULL\
            )",
        )
        .expect("create ordering fixture");
    database
        .query_sql("CREATE INDEX feeds_category_source_title_idx ON feeds (category, source_title)")
        .expect("create source title index");
    database
        .query_sql(
            "INSERT INTO feeds (id, category, source_title, title) VALUES \
             (1, 'all', 'Zulu', 'Alpha'), \
             (2, 'all', 'Alpha', 'Zulu')",
        )
        .expect("insert ordering fixture");
    database
}

fn row_ids(output: &QueryOutput) -> Vec<i64> {
    output
        .rows
        .iter()
        .map(|row| match row.get("id") {
            Some(Value::Int(id)) => *id,
            value => panic!("expected BIGINT id, received {value:?}"),
        })
        .collect()
}

#[test]
fn relational_order_by_prefers_unqualified_projection_aliases() {
    let mut database = ordering_fixture();

    let output = database
        .query_sql("SELECT id, source_title AS title FROM feeds ORDER BY title ASC")
        .expect("order by direct projection alias");
    assert_eq!(row_ids(&output), [2, 1]);

    let explain = database
        .query_sql(
            "EXPLAIN SELECT id, source_title AS title FROM feeds \
             WHERE category = 'all' ORDER BY title ASC",
        )
        .expect("explain direct projection alias order");
    assert!(explain.rows.iter().any(|row| {
        matches!(
            row.get("access object"),
            Some(Value::String(access)) if access.contains("feeds_category_source_title_idx")
        )
    }));
    assert!(explain.rows.iter().all(|row| {
        !matches!(row.get("id"), Some(Value::String(id)) if id.contains("TopNExec"))
    }));
}

#[test]
fn relational_order_by_keeps_qualified_input_column_semantics() {
    let mut database = ordering_fixture();

    let output = database
        .query_sql("SELECT id, source_title AS title FROM feeds ORDER BY feeds.title ASC")
        .expect("order by qualified input column");
    assert_eq!(row_ids(&output), [1, 2]);

    let ordinal_error = database
        .query_sql("SELECT id, source_title FROM feeds ORDER BY 1")
        .expect_err("ordinal ordering keeps its existing unsupported behavior");
    assert!(ordinal_error
        .to_string()
        .contains("expected a column reference"));
}

#[test]
fn relational_order_by_expression_alias_uses_projected_values() {
    let mut database = ordering_fixture();

    let output = database
        .query_sql("SELECT id, uuidv7() AS generated FROM feeds ORDER BY generated DESC")
        .expect("order by generated projection alias");
    let generated = output
        .rows
        .iter()
        .map(|row| match row.get("generated") {
            Some(Value::Uuid(value)) => *value,
            value => panic!("expected UUID projection, received {value:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(generated.len(), 2);
    assert!(generated[0] > generated[1]);

    let mixed = database
        .query_sql(
            "SELECT id, TRUE AS ranked FROM feeds \
             ORDER BY ranked ASC, feeds.title DESC",
        )
        .expect("order by expression alias and qualified input column");
    assert_eq!(row_ids(&mixed), [2, 1]);
}

#[test]
fn relational_distinct_orders_by_projection_aliases() {
    let mut database = ordering_fixture();

    let direct = database
        .query_sql("SELECT DISTINCT source_title AS label FROM feeds ORDER BY label DESC")
        .expect("order distinct direct projection alias");
    assert_eq!(
        direct
            .rows
            .iter()
            .map(|row| row["label"].clone())
            .collect::<Vec<_>>(),
        [
            Value::String("Zulu".to_string()),
            Value::String("Alpha".to_string())
        ]
    );

    let expression = database
        .query_sql("SELECT DISTINCT uuidv7() AS generated FROM feeds ORDER BY generated DESC")
        .expect("order distinct expression projection alias");
    let generated = expression
        .rows
        .iter()
        .map(|row| match row.get("generated") {
            Some(Value::Uuid(value)) => *value,
            value => panic!("expected UUID projection, received {value:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(generated.len(), 2);
    assert!(generated[0] > generated[1]);
}

#[test]
fn relational_order_by_rejects_ambiguous_projection_aliases() {
    let mut database = ordering_fixture();

    let error = database
        .query_sql(
            "SELECT source_title AS label, title AS label \
             FROM feeds ORDER BY label",
        )
        .expect_err("ambiguous projection alias must fail during binding");
    assert!(
        error
            .to_string()
            .contains("ambiguous relational ORDER BY alias label"),
        "unexpected error: {error}"
    );
}
