use crate::{
    parse, AstNode, OrderExpression, ReturnExpressionKind, ScalarExpressionKind, SourceSpan,
    Statement, ValueExpressionKind,
};

fn assert_text<T>(node: &AstNode<T>, query: &str, text: &str) {
    let span = node.span.expect("parsed node must have source provenance");
    assert_eq!(span.text(query), Some(text), "wrong span: {span:?}");
}

#[test]
fn nested_projection_spans_use_original_utf8_byte_offsets() {
    let query = " \u{2003}EXPLAIN MATCH (m:Memory) RETURN \
                 coalesce(lower(m.title), ['caf\u{e9}', $fallback]) AS label, \
                 count(m) AS total ORDER BY left(label, 2) DESC \n";
    let Statement::Explain(explain) = parse(query).unwrap() else {
        panic!("expected EXPLAIN");
    };
    let Statement::MatchReturn(parsed) = explain.statement else {
        panic!("expected MATCH");
    };
    let item = &parsed.returns[0];
    assert_text(
        item,
        query,
        "coalesce(lower(m.title), ['caf\u{e9}', $fallback]) AS label",
    );
    assert_text(
        &item.expression,
        query,
        "coalesce(lower(m.title), ['caf\u{e9}', $fallback])",
    );
    let ReturnExpressionKind::Value(value) = &item.expression.kind else {
        panic!("expected scalar projection");
    };
    assert_eq!(value.span, item.expression.span);
    let ScalarExpressionKind::Coalesce(arguments) = &value.kind else {
        panic!("expected COALESCE");
    };
    assert_text(&arguments[0], query, "lower(m.title)");
    let ScalarExpressionKind::Lower(property) = &arguments[0].kind else {
        panic!("expected LOWER");
    };
    assert_text(property, query, "m.title");
    assert_text(&arguments[1], query, "['caf\u{e9}', $fallback]");
    let ScalarExpressionKind::Value(list) = &arguments[1].kind else {
        panic!("expected list value");
    };
    let ValueExpressionKind::List(elements) = &list.kind else {
        panic!("expected list");
    };
    assert_text(list, query, "['caf\u{e9}', $fallback]");
    assert_text(&elements[0], query, "'caf\u{e9}'");
    assert_text(&elements[1], query, "$fallback");
    let offset = query.find("$fallback").unwrap();
    assert_eq!(
        elements[1].span,
        Some(SourceSpan {
            start: offset,
            end: offset + 9
        })
    );
    assert_text(&parsed.returns[1], query, "count(m) AS total");
    assert_text(&parsed.returns[1].expression, query, "count(m)");
    assert_text(&parsed.order_by[0], query, "left(label, 2) DESC");
    let OrderExpression::Value(order) = &parsed.order_by[0].expression else {
        panic!("expected scalar ordering expression");
    };
    assert_text(order, query, "left(label, 2)");
    let ScalarExpressionKind::Left { expression, length } = &order.kind else {
        panic!("expected LEFT");
    };
    assert_text(expression, query, "label");
    assert_text(length, query, "2");
}

#[test]
fn syntax_equality_preserves_distinct_source_locations() {
    let query = "MATCH (m:Memory) RETURN coalesce($value, $value)";
    let Statement::MatchReturn(parsed) = parse(query).unwrap() else {
        panic!("expected MATCH");
    };
    let ReturnExpressionKind::Value(value) = &parsed.returns[0].expression.kind else {
        panic!("expected scalar projection");
    };
    let ScalarExpressionKind::Coalesce(arguments) = &value.kind else {
        panic!("expected COALESCE");
    };
    assert_eq!(arguments[0], arguments[1]);
    let first = arguments[0].span.unwrap();
    let second = arguments[1].span.unwrap();
    assert_ne!(first, second);
    assert!(first.end < second.start);
    assert_eq!(first.text(query), Some("$value"));
    assert_eq!(second.text(query), Some("$value"));
    let synthetic = AstNode::synthetic(arguments[0].kind.clone());
    assert_eq!(synthetic, arguments[0]);
    assert_eq!(synthetic.span, None);
}

#[test]
fn with_items_retain_group_aggregate_and_alias_boundaries() {
    for (with, items) in [
        (
            "m.id AS key, count(m) AS total",
            vec!["m.id AS key", "count(m) AS total"],
        ),
        (
            "m, count(m) AS total, collect(m) AS rows",
            vec!["m", "count(m) AS total", "collect(m) AS rows"],
        ),
        ("m, collect(m) AS rows", vec!["m", "collect(m) AS rows"]),
        (
            "m, collect(m.id) AS ids, count(m) AS total",
            vec!["m", "collect(m.id) AS ids", "count(m) AS total"],
        ),
    ] {
        let query = format!("MATCH (m:Memory) WITH {with} RETURN 1");
        let Statement::MatchReturn(parsed) = parse(&query).unwrap() else {
            panic!("expected MATCH");
        };
        let projection = parsed
            .aggregate_with
            .as_ref()
            .expect("expected aggregate WITH");
        assert_eq!(projection.items.len(), items.len());
        let mut offset = query.find("WITH ").unwrap() + 5;
        for (item, text) in projection.items.iter().zip(items) {
            assert_text(item, &query, text);
            assert_eq!(
                item.span,
                Some(SourceSpan {
                    start: offset,
                    end: offset + text.len()
                })
            );
            assert_text(&item.expression, &query, text.split(" AS ").next().unwrap());
            offset += text.len() + 2;
        }
    }
}

#[test]
fn property_comparisons_and_backtracking_keep_local_spans() {
    let query = "MATCH (m:Memory) WHERE m.id = m.other RETURN m.id";
    let Statement::MatchReturn(parsed) = parse(query).unwrap() else {
        panic!("expected MATCH");
    };
    let crate::PropertyPredicate::ExpressionEq { expression, value } = parsed.predicate.unwrap()
    else {
        panic!("expected property comparison");
    };
    assert_text(&expression, query, "m.id");
    assert_text(&value, query, "m.other");

    let query = "MATCH (e:Entity) OPTIONAL MATCH (:Memory)-[r:MENTIONS]->(e) \
                 RETURN e.id, count(r) AS total";
    let Statement::MatchReturn(parsed) = parse(query).unwrap() else {
        panic!("expected fallback MATCH");
    };
    assert_text(&parsed.returns[0], query, "e.id");
    assert_text(&parsed.returns[1].expression, query, "count(r)");
    assert_text(&parsed.returns[1], query, "count(r) AS total");
}

#[test]
fn span_text_rejects_invalid_utf8_boundaries_and_ranges() {
    assert_eq!(
        SourceSpan { start: 0, end: 2 }.text("\u{e9}"),
        Some("\u{e9}")
    );
    assert_eq!(SourceSpan { start: 1, end: 2 }.text("\u{e9}"), None);
    assert_eq!(SourceSpan { start: 0, end: 3 }.text("\u{e9}"), None);
    assert_eq!(SourceSpan { start: 2, end: 0 }.text("\u{e9}"), None);
}

#[test]
fn hints_mutation_values_and_case_branches_keep_their_own_ranges() {
    let query = "CYPHER system.work_priority = 'background' CREATE (:Memory \
                 {title: '\u{96ea}', created: CURRENT_TIMESTAMP(), \
                 history: [timestamp('2026-09-18'), CAST($created AS TIMESTAMP)]})";
    let Statement::CypherQuery(parsed) = parse(query).unwrap() else {
        panic!("expected query hints");
    };
    assert_text(&parsed.system_variables[0].value, query, "'background'");
    let Statement::CreateNode(node) = parsed.statement else {
        panic!("expected CREATE");
    };
    assert_text(&node.properties["title"], query, "'\u{96ea}'");
    assert_text(&node.properties["created"], query, "CURRENT_TIMESTAMP()");
    let history = &node.properties["history"];
    assert_text(
        history,
        query,
        "[timestamp('2026-09-18'), CAST($created AS TIMESTAMP)]",
    );
    let ValueExpressionKind::List(values) = &history.kind else {
        panic!("expected list");
    };
    assert_text(&values[0], query, "timestamp('2026-09-18')");
    assert_text(&values[1], query, "CAST($created AS TIMESTAMP)");
    let ValueExpressionKind::Timestamp(argument) = &values[1].kind else {
        panic!("expected timestamp cast");
    };
    assert_text(argument, query, "$created");

    let query = "MATCH (m:Memory) RETURN CASE WHEN m.id = $value THEN 1 \
                 WHEN m.id = $other THEN 2 ELSE 3 END AS rank";
    let Statement::MatchReturn(parsed) = parse(query).unwrap() else {
        panic!("expected MATCH");
    };
    let ReturnExpressionKind::Value(value) = &parsed.returns[0].expression.kind else {
        panic!("expected scalar projection");
    };
    assert_text(
        value,
        query,
        "CASE WHEN m.id = $value THEN 1 WHEN m.id = $other THEN 2 ELSE 3 END",
    );
    let ScalarExpressionKind::CasePropertyEqualsRank {
        branches, default, ..
    } = &value.kind
    else {
        panic!("expected CASE branches");
    };
    assert_text(&branches[0].0, query, "$value");
    assert_text(&branches[0].1, query, "1");
    assert_text(&branches[1].0, query, "$other");
    assert_text(&branches[1].1, query, "2");
    assert_text(default, query, "3");
}
