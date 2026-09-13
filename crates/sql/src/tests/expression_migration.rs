use crate::{
    prepare_postgres_sql, Expr, ExprKind, RelationalPlanTemplateCache, SelectProjection,
    SelectStatement, SqlSourceLocation, SqlSourceSpan, SqlStatement, SqlValue,
};
use skein_core::Value;

fn parse_select(sql: &str) -> SelectStatement {
    let SqlStatement::Select(select) = prepare_postgres_sql(sql).unwrap().statement else {
        panic!("expected SELECT");
    };
    select
}

fn span(line: u64, start: u64, end: u64) -> SqlSourceSpan {
    SqlSourceSpan {
        start: SqlSourceLocation {
            line,
            column: start,
        },
        end: SqlSourceLocation { line, column: end },
    }
}

#[test]
fn projections_predicates_and_order_retain_token_locations() {
    let select = parse_select("SELECT id, $1\nFROM items\nWHERE id = $2\nORDER BY id DESC");
    let expressions = select
        .projection
        .iter()
        .map(|projection| match projection {
            SelectProjection::Expression { expression, .. } => expression,
            _ => panic!("expected expression"),
        })
        .collect::<Vec<_>>();
    assert_eq!(expressions[0].span, span(1, 8, 10));
    assert_eq!(expressions[1].span, span(1, 12, 14));
    assert_eq!(select.order_by[0].expression.span, span(4, 10, 12));
    let predicate = select.selection.unwrap();
    assert_eq!(predicate.span, span(3, 7, 14));
    let ExprKind::Compare { left, right, .. } = predicate.kind else {
        panic!("expected comparison");
    };
    assert_eq!(left.span, span(3, 7, 9));
    assert_eq!(right.span, span(3, 12, 14));
    assert_eq!(right.as_value(), Some(&SqlValue::Parameter(2)));

    // Columns are character positions, not UTF-8 byte offsets.
    let unicode = parse_select("SELECT 'é', id FROM items");
    let SelectProjection::Expression { expression, .. } = &unicode.projection[1] else {
        panic!("expected column expression");
    };
    assert_eq!(expression.span, span(1, 13, 15));
}

#[test]
fn shared_traversal_keeps_filter_parameters_and_cached_source_metadata() {
    let sql = "SELECT COALESCE($1, id), COUNT(*) FILTER (WHERE active = $2) \
               FROM items JOIN owners ON items.owner = $3 \
               WHERE id IN ($4, $1) AND NOT name LIKE $5 ORDER BY id LIMIT $6 OFFSET $7";
    let prepared = prepare_postgres_sql(sql).unwrap();
    assert_eq!(
        prepared
            .parameters
            .iter()
            .map(|p| p.position)
            .collect::<Vec<_>>(),
        (1..=7).collect::<Vec<_>>()
    );
    assert!(prepare_postgres_sql(&sql.replace("$2", "$8"))
        .unwrap_err()
        .to_string()
        .contains("must be dense"));

    let cache = RelationalPlanTemplateCache::new(Some(2));
    let first = cache.prepare(sql).unwrap();
    let mut statement = first.statement().clone();
    let SqlStatement::Select(select) = &mut statement else {
        panic!("expected SELECT");
    };
    let SelectProjection::Expression { expression, .. } = &mut select.projection[1] else {
        panic!("expected filtered aggregate");
    };
    let mut before = Vec::new();
    expression.visit(&mut |node| before.push(node.span));
    let mut rewritten = Vec::new();
    expression
        .try_visit_mut(&mut |node: &mut Expr| {
            if let ExprKind::Value(SqlValue::Parameter(position)) = &node.kind {
                rewritten.push(*position);
                node.kind = ExprKind::Value(SqlValue::Literal(Value::Bool(true)));
            }
            Ok::<_, ()>(())
        })
        .unwrap();
    let mut after = Vec::new();
    expression.visit(&mut |node| after.push(node.span));
    assert_eq!(rewritten, vec![2]);
    assert_eq!(after, before);
    assert!(before.iter().all(|span| *span != SqlSourceSpan::default()));
    let second = cache.prepare(sql).unwrap();
    assert!(std::sync::Arc::ptr_eq(&first.template, &second.template));
    assert_eq!(*second.template, prepared);
    assert_ne!(statement, prepared.statement);
}

#[test]
fn uniform_representation_preserves_frontend_expression_boundaries() {
    for sql in [
        "SELECT id = 1 FROM items",
        "SELECT id + 1 FROM items",
        "SELECT id FROM items WHERE 1 = id",
        "SELECT id FROM items WHERE COALESCE(id, 1) = 2",
        "SELECT id FROM items WHERE id IN (other)",
        "SELECT id FROM items WHERE TRUE",
        "SELECT id FROM items ORDER BY $1",
        "SELECT id FROM items ORDER BY COALESCE(id, 1)",
    ] {
        assert!(
            prepare_postgres_sql(sql).is_err(),
            "unexpected acceptance: {sql}"
        );
    }
    for sql in [
        "SELECT id FROM items WHERE id = other",
        "SELECT COALESCE(id, $1) FROM items WHERE NOT (id IS NULL)",
        "SELECT COUNT(*) FILTER (WHERE active = $1) FROM items",
        "SELECT id FROM items HAVING id = 1",
        "SELECT id FROM items, owners",
    ] {
        assert!(
            prepare_postgres_sql(sql).is_ok(),
            "unexpected rejection: {sql}"
        );
    }
}
