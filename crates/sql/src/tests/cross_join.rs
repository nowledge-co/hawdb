use super::*;

#[test]
fn cross_join_lowers_to_an_unconditional_inner_join() {
    let prepared = prepare_postgres_sql(
        "SELECT a.id FROM items a CROSS JOIN owners b \
         INNER JOIN tags c ON a.id = c.id WHERE b.id = $1 LIMIT $2",
    )
    .unwrap();
    assert_eq!(
        prepared
            .parameters
            .iter()
            .map(|p| p.position)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    let SqlStatement::Select(select) = prepared.statement else {
        panic!("expected SELECT");
    };
    assert_eq!(select.joins.len(), 2);
    assert_eq!(select.joins[0].kind, SqlJoinKind::Inner);
    assert_eq!(select.joins[0].alias.as_deref(), Some("b"));
    assert_eq!(
        select.joins[0].on,
        Expr::value(SqlValue::Literal(Value::Bool(true)))
    );
    assert!(matches!(select.joins[1].on.kind, ExprKind::Compare { .. }));
    assert!(
        prepare_postgres_sql("SELECT a.id FROM items a CROSS JOIN owners b WHERE b.id = $2")
            .unwrap_err()
            .to_string()
            .contains("must be dense")
    );
}

#[test]
fn cross_join_does_not_discard_constraints_or_admit_other_join_forms() {
    for sql in [
        "SELECT a.id FROM a CROSS JOIN b ON a.id = b.id",
        "SELECT a.id FROM a CROSS JOIN b USING (id)",
        "SELECT a.id FROM a RIGHT JOIN b ON a.id = b.id",
        "SELECT a.id FROM a FULL JOIN b ON a.id = b.id",
        "SELECT a.id FROM a NATURAL JOIN b",
        "SELECT a.id FROM a JOIN b USING (id)",
        "SELECT a.id FROM a CROSS JOIN LATERAL (SELECT id FROM b) x",
        "SELECT a.id FROM a JOIN b ON TRUE",
        "SELECT a.id FROM a CROSS JOIN b WHERE TRUE",
    ] {
        assert!(
            prepare_postgres_sql(sql).is_err(),
            "unexpectedly accepted {sql}"
        );
    }
}
