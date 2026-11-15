// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Planning pattern predicates and pattern comprehensions (#1488).

use super::*;

fn returns(query: &str, scope: &[&str], columns: &[&str]) -> Result<Vec<Projection>> {
    let pipeline = hawdb_cypher::parse_pipeline(query).unwrap();
    let hawdb_cypher::ClauseKind::Return(projection) = &pipeline.clauses.last().unwrap().kind
    else {
        panic!("expected RETURN");
    };
    match plan_return_items_with_columns(
        &scope.iter().map(|name| name.to_string()).collect(),
        &columns.iter().map(|name| name.to_string()).collect(),
        &projection.items,
        &BTreeMap::from([("tag".to_string(), Value::Int(7))]),
    )? {
        PlannedReturns::Projections(projections) => Ok(projections),
        _ => panic!("expected plain projections"),
    }
}

fn pattern(expression: &ProjectionExpression) -> PatternProjection {
    fn find(expression: &ProjectionExpression) -> Option<PatternProjection> {
        if let ProjectionExpression::Pattern(pattern) = expression {
            return Some(pattern.as_ref().clone());
        }
        let mut found = None;
        expression.all_children(|child| {
            found = find(child);
            found.is_none()
        });
        found
    }
    find(expression).unwrap_or_else(|| panic!("no pattern in {expression:?}"))
}

fn local(name: &str, property: Option<&str>) -> ProjectionExpression {
    ProjectionExpression::LocalVariable {
        name: name.to_string(),
        property: property.map(str::to_string),
    }
}

fn step_endpoints(step: &GraphMatchStep) -> (String, String, RelationshipDirection) {
    match step {
        GraphMatchStep::Expand {
            source,
            target,
            direction,
            ..
        } => (source.clone(), target.variable.clone(), *direction),
        other => panic!("expected an expand step: {other:?}"),
    }
}

#[test]
fn variables_correlate_by_where_the_enclosing_scope_binds_them() {
    // IC10: `p` is a comprehension element, `person` a native graph variable,
    // the anonymous tag node is introduced under a name no query can write.
    let items = returns(
        "MATCH (person) RETURN [p IN posts WHERE (p)-[:HAS_TAG]->()<-[:HAS_INTEREST]-(person)] AS c",
        &["person"],
        &["posts"],
    )
    .unwrap();
    let pattern = &pattern(&items[0].expression);
    assert_eq!(pattern.graph_variables, ["person"]);
    assert_eq!(pattern.arguments, [("p".to_string(), local("p", None))]);
    assert_eq!(
        pattern.program.imports,
        [GraphBindingImport {
            variable: "p".into(),
            column: "p".into(),
            kind: GraphEntityKind::Node,
        }]
    );
    assert_eq!(pattern.program.introduced, ["#anon0"]);
    assert!(pattern.predicate.is_none() && pattern.projection.is_none());
    assert!(matches!(
        &pattern.program.steps[0],
        GraphMatchStep::Node(node) if node.variable == "p"
    ));
    assert_eq!(
        step_endpoints(&pattern.program.steps[2]),
        (
            "#anon0".to_string(),
            "person".to_string(),
            RelationshipDirection::Incoming
        )
    );
}

#[test]
fn matching_starts_at_the_first_bound_node() {
    // `b` is introduced and `a` bound: expand from `a` against the pattern's
    // direction, then on to `c`.
    let items = returns(
        "MATCH (a) RETURN [(b:Post)-[:HAS_CREATOR]->(a)-[:KNOWS]->(c) | [b, c]] AS x",
        &["a"],
        &[],
    )
    .unwrap();
    let pattern = &pattern(&items[0].expression);
    assert!(matches!(
        &pattern.program.steps[0],
        GraphMatchStep::Node(node) if node.variable == "a" && node.label.is_empty()
    ));
    assert_eq!(
        step_endpoints(&pattern.program.steps[1]),
        (
            "a".to_string(),
            "c".to_string(),
            RelationshipDirection::Outgoing
        )
    );
    assert_eq!(
        step_endpoints(&pattern.program.steps[2]),
        (
            "a".to_string(),
            "b".to_string(),
            RelationshipDirection::Incoming
        )
    );
    let GraphMatchStep::Expand { target, .. } = &pattern.program.steps[2] else {
        unreachable!()
    };
    assert_eq!(target.label, "Post");
    assert_eq!(pattern.program.introduced, ["b", "c"]);
}

#[test]
fn the_body_reads_enclosing_columns_as_locals_and_introduced_variables_natively() {
    let items = returns(
        "MATCH (a) RETURN [(a)-[r:R]->(b {kind: $tag, id: k}) WHERE b.x > k | [b.name, type(r), k]] AS x",
        &["a"],
        &["k"],
    )
    .unwrap();
    let pattern = &pattern(&items[0].expression);
    assert_eq!(pattern.columns, ["k"]);
    assert_eq!(pattern.graph_variables, ["a"]);
    assert!(pattern.arguments.is_empty());
    // A parameter property stays in the pattern; a row-bound one becomes a
    // conjunct of the body predicate.
    let GraphMatchStep::Expand {
        target,
        relationship,
        ..
    } = &pattern.program.steps[1]
    else {
        panic!("expected an expand");
    };
    assert_eq!(relationship.as_deref(), Some("r"));
    assert_eq!(
        target.properties,
        BTreeMap::from([("kind".to_string(), Value::Int(7))])
    );
    let Some(ProjectionExpression::Binary {
        left,
        op: ScalarBinaryOp::And,
        right,
    }) = &pattern.predicate
    else {
        panic!("expected a conjunction: {:?}", pattern.predicate);
    };
    assert_eq!(
        **left,
        ProjectionExpression::Binary {
            left: Box::new(ProjectionExpression::Property {
                variable: "b".into(),
                property: "id".into(),
            }),
            op: ScalarBinaryOp::Eq,
            right: Box::new(local("k", None)),
        }
    );
    assert!(matches!(
        right.as_ref(),
        ProjectionExpression::Binary {
            op: ScalarBinaryOp::Gt,
            ..
        }
    ));
    assert_eq!(
        pattern.projection,
        Some(ProjectionExpression::List(vec![
            ProjectionExpression::Property {
                variable: "b".into(),
                property: "name".into(),
            },
            ProjectionExpression::RelationshipType {
                variable: "r".into(),
            },
            local("k", None),
        ]))
    );
}

#[test]
fn nested_patterns_lend_enclosing_columns_through_their_parent() {
    let items = returns(
        "MATCH (a) RETURN [(a)-[:R]->(b) | [(b)-[:R]->(c) WHERE c.x = k | c]] AS x",
        &["a"],
        &["k"],
    )
    .unwrap();
    let outer = &pattern(&items[0].expression);
    let inner = &pattern(outer.projection.as_ref().unwrap());
    assert_eq!(inner.graph_variables, ["b"]);
    assert_eq!(inner.columns, ["k"]);
    // The outer pattern lends `k`, which its own body reads only through
    // the nested pattern.
    assert_eq!(outer.columns, ["k"]);
    assert_eq!(outer.graph_variables, ["a"]);
}

#[test]
fn order_by_pattern_expressions_are_projected_before_sorting() {
    for query in [
        "MATCH (a) RETURN a.id AS id ORDER BY size([(a)-[:R]->(b) | b]) DESC",
        "MATCH (a) WITH a RETURN a.id AS id ORDER BY size([(a)-[:R]->(b) | b])",
        "MATCH (a) RETURN DISTINCT a AS a ORDER BY size([(a)-[:R]->(b) | b])",
    ] {
        let plan = crate::plan_pipeline_query(query, &BTreeMap::new()).unwrap();
        let mut sorts = 0;
        fn visit(plan: &LogicalPlan, sorts: &mut usize) {
            if let LogicalPlan::Sort { items, .. } = plan {
                *sorts += 1;
                assert!(
                    items
                        .iter()
                        .all(|item| matches!(item.key, SortKey::Column(_))),
                    "{items:?}"
                );
            }
            match plan {
                LogicalPlan::Sort { input, .. }
                | LogicalPlan::Project { input, .. }
                | LogicalPlan::Distinct { input }
                | LogicalPlan::Filter { input, .. }
                | LogicalPlan::Limit { input, .. } => visit(input, sorts),
                LogicalPlan::GraphMatch {
                    input: Some(input), ..
                } => visit(input, sorts),
                _ => {}
            }
        }
        visit(&plan, &mut sorts);
        assert_eq!(sorts, 1, "{query}: {plan:?}");
    }
}

#[test]
fn unsupported_pattern_shapes_are_rejected() {
    for (query, scope, columns, message) in [
        (
            "MATCH (a) RETURN [(a)-[:R*]->(b) | b] AS x",
            &["a"][..],
            &[][..],
            "unbounded variable-length relationships are not supported in pattern comprehensions",
        ),
        (
            "MATCH (a) RETURN [(a)-[r:R*1..2]->(b) | b] AS x",
            &["a"],
            &[],
            "relationship variables are supported only for one-hop patterns",
        ),
        (
            "MATCH (a) RETURN [(a)-[:R]->(b)-[r]->(r) | b] AS x",
            &["a"],
            &[],
            "used as both a node and a relationship",
        ),
        (
            "MATCH (a) RETURN [(a)-[:R]->(b) WHERE q.x = 1 | b] AS x",
            &["a"],
            &[],
            "unknown variable 'q'",
        ),
    ] {
        let error = returns(query, scope, columns).unwrap_err().to_string();
        assert!(error.contains(message), "{query}: {error}");
    }
}

#[test]
fn row_column_forms_read_a_copy_of_the_column() {
    // Path functions and the column CASE forms read the row column itself,
    // so the subquery row receives a copy; plain reads are lent as locals.
    for (query, read) in [
        (
            "MATCH (a) RETURN [(a)-[:R]->(c) WHERE length(p) > 1 | c] AS x",
            "length",
        ),
        (
            "MATCH (a) RETURN [(a)-[:R]->(c) | CASE WHEN m.title IS NULL OR m.title = '' THEN 'none' ELSE m.title END] AS x",
            "case",
        ),
    ] {
        let items = returns(query, &["a"], &["p", "m"]).unwrap();
        let pattern = pattern(&items[0].expression);
        let column = if read == "length" { "p" } else { "m" };
        assert_eq!(
            pattern.arguments,
            [(column.to_string(), ProjectionExpression::Column(column.to_string()))],
            "{query}: {pattern:?}"
        );
    }
    let items = returns(
        "MATCH (a) RETURN [(a)-[:R]->(c) | [m.title, CASE WHEN m.title IS NULL OR m.title = '' THEN 'none' ELSE m.title END]] AS x",
        &["a"],
        &["m"],
    )
    .unwrap();
    let pattern = pattern(&items[0].expression);
    assert_eq!(pattern.columns, ["m"]);
    assert_eq!(pattern.arguments.len(), 1);
}

#[test]
fn order_by_reads_a_projected_alias_before_the_input_variable() {
    // `a` in ORDER BY is the projected alias for `b`, not the matched `a`.
    let plan = crate::plan_pipeline_query(
        "MATCH (a)-[:KNOWS]->(b) RETURN b AS a ORDER BY size([(a)-[:KNOWS]->(x) | x])",
        &BTreeMap::new(),
    )
    .unwrap();
    let LogicalPlan::Project { input, .. } = &plan else {
        panic!("expected the final projection: {plan:?}");
    };
    let LogicalPlan::Sort { input, .. } = input.as_ref() else {
        panic!("expected a sort: {input:?}");
    };
    let LogicalPlan::Project { items, .. } = input.as_ref() else {
        panic!("expected the key projection: {input:?}");
    };
    let key = &pattern(&items.last().unwrap().expression);
    // The projected `a` is a value column imported into the pattern.
    assert_eq!(
        key.arguments,
        [(
            "a".to_string(),
            ProjectionExpression::Column("a".to_string())
        )]
    );
    assert!(key.graph_variables.is_empty());
}

#[test]
fn node_visibility_reaches_pattern_programs_and_endpoint_reads() {
    let visibility = |variable: &str| Predicate::PropertyEq {
        variable: variable.to_string(),
        property: "visibility".to_string(),
        value: Value::String("public".into()),
    };
    let plan = crate::plan_pipeline_query(
        "MATCH (a:Person) WHERE size([(a)-[:R]->(h) | h]) > 0 \
         WITH a, [(a)-[:KNOWS]->(b) | [(b)-[:KNOWS]->(c) | c.name]] AS names, [r IN $rels | startNode(r)] AS s \
         RETURN a, names, s ORDER BY size([(a)-[:R]->(x) | x])",
        &BTreeMap::from([("rels".to_string(), Value::List(Vec::new()))]),
    )
    .unwrap();
    let restricted = apply_node_visibility_predicates(plan, &visibility);
    // Every program nested in an expression, at any depth, admits only
    // visible nodes; every endpoint read carries the same policy.
    fn check(expression: &ProjectionExpression, seen: &mut usize) {
        match expression {
            ProjectionExpression::Pattern(pattern) => {
                *seen += 1;
                let predicate = format!("{:?}", pattern.program.predicate);
                for step in &pattern.program.steps {
                    let variable = match step {
                        GraphMatchStep::Node(node) => &node.variable,
                        GraphMatchStep::Expand { target, .. } => &target.variable,
                        GraphMatchStep::ShortestPath(_) => continue,
                    };
                    assert!(
                        predicate.contains(&format!("variable: \"{variable}\"")),
                        "{variable}: {predicate}"
                    );
                }
            }
            ProjectionExpression::RelationshipValueEndpoint { visibility, .. } => {
                *seen += 1;
                assert!(visibility.is_some(), "{expression:?}");
            }
            _ => {}
        }
        expression.all_children(|child| {
            check(child, seen);
            true
        });
    }
    fn visit(plan: &LogicalPlan, seen: &mut usize) {
        let mut expressions = Vec::new();
        let input = match plan {
            LogicalPlan::Project { items, input } => {
                expressions.extend(items.iter().map(|item| &item.expression));
                Some(input.as_ref())
            }
            LogicalPlan::Filter { predicate, input } => {
                if let Predicate::ExpressionCompare { expression, .. } = predicate {
                    expressions.push(expression);
                }
                Some(input.as_ref())
            }
            LogicalPlan::Sort { input, .. } | LogicalPlan::Limit { input, .. } => {
                Some(input.as_ref())
            }
            LogicalPlan::GraphMatch { program, input } => {
                if let Some(Predicate::And(conjuncts)) = &program.predicate {
                    for conjunct in conjuncts {
                        if let Predicate::ExpressionCompare { expression, .. } = conjunct {
                            expressions.push(expression);
                        }
                    }
                }
                input.as_deref()
            }
            _ => None,
        };
        for expression in expressions {
            check(expression, seen);
        }
        if let Some(input) = input {
            visit(input, seen);
        }
    }
    let mut seen = 0;
    visit(&restricted, &mut seen);
    // The WHERE, outer and nested comprehensions, the endpoint read and the
    // ORDER BY key.
    assert_eq!(seen, 5, "{restricted:?}");
}

#[test]
fn visibility_validation_refuses_any_unrestricted_pattern_or_endpoint_read() {
    let visibility = |variable: &str| Predicate::PropertyEq {
        variable: variable.to_string(),
        property: "visibility".to_string(),
        value: Value::String("public".into()),
    };
    let plan = |query: &str| {
        crate::plan_pipeline_query(
            query,
            &BTreeMap::from([("rels".to_string(), Value::List(Vec::new()))]),
        )
        .unwrap()
    };
    let refused = |plan: &LogicalPlan| {
        validate_node_visibility_predicates(plan, &visibility)
            .unwrap_err()
            .to_string()
    };
    for query in [
        "MATCH (a) RETURN [(a)-[:R]->(b) | [(b)-[:R]->(c) | c]] AS x",
        "MATCH (a) WHERE (a)-[:R]->()-[:S]->() RETURN a",
        "MATCH (a) WITH a ORDER BY size([(a)-[:R]->(b) | b]) RETURN a",
        "MATCH (a) RETURN [r IN $rels | startNode(r)] AS x",
        "UNWIND $rels AS r RETURN [x IN [1] | endNode(r)] AS x",
    ] {
        // Unrestricted, every such plan is refused; restricted, it passes.
        let original = plan(query);
        assert!(
            refused(&original).contains("access control could not restrict"),
            "{query}"
        );
        let restricted = apply_node_visibility_predicates(original, &visibility);
        validate_node_visibility_predicates(&restricted, &visibility)
            .unwrap_or_else(|error| panic!("{query}: {error}"));
    }
    // A nested program whose policy is missing fails closed even when every
    // other program is restricted, as does a different policy.
    let mut restricted = apply_node_visibility_predicates(
        plan("MATCH (a) RETURN [(a)-[:R]->(b) | [(b)-[:R]->(c) | c]] AS x"),
        &visibility,
    );
    let LogicalPlan::Project { items, .. } = &mut restricted else {
        panic!("{restricted:?}");
    };
    let ProjectionExpression::Pattern(outer) = &mut items[0].expression else {
        panic!("{items:?}");
    };
    let Some(ProjectionExpression::Pattern(inner)) = &mut outer.projection else {
        panic!("{outer:?}");
    };
    inner.program.predicate = None;
    assert!(refused(&restricted).contains("pattern expression"));
    let other = |variable: &str| Predicate::PropertyEq {
        variable: variable.to_string(),
        property: "tenant".to_string(),
        value: Value::String("x".into()),
    };
    let restricted =
        apply_node_visibility_predicates(plan("MATCH (a) RETURN [(a)-[:R]->(b) | b] AS x"), &other);
    assert!(validate_node_visibility_predicates(&restricted, &visibility).is_err());
    // An unresolved endpoint read left in a plan fails closed.
    let unresolved = LogicalPlan::Project {
        items: vec![Projection {
            expression: ProjectionExpression::RelationshipEndpoint {
                endpoint: hawdb_core::RelationshipEndpoint::Start,
                variable: "r".into(),
                endpoints: None,
            },
            name: "s".into(),
        }],
        input: Box::new(plan("MATCH ()-[r:R]->() RETURN r")),
    };
    assert!(refused(&unresolved).contains("relationship endpoint read"));
    // Plans without pattern expressions or endpoint reads pass unchanged.
    validate_node_visibility_predicates(&plan("MATCH (a)-[:R]->(b) RETURN b"), &visibility)
        .unwrap();
}

/// A node the pattern introduces gets seeds when every disjunct of the WHERE
/// equates it with an enclosing-row expression (#1607, LDBC IC14); the seeded
/// program imports it from the seed column and starts there.
#[test]
fn where_equalities_seed_an_introduced_node() {
    let seeds = |query: &str| {
        let items = returns(query, &["r"], &[]).unwrap();
        pattern(&items[0].expression).seeds
    };
    let single = seeds(
        "MATCH ()-[r:KNOWS]->() RETURN [(a:Person)<-[:HAS_CREATOR]-(m)-[:HAS_CREATOR]->(b:Person) \
         WHERE a = startNode(r) | m] AS c",
    )
    .expect("a = startNode(r) seeds a");
    assert_eq!(single.variable, "a");
    assert_eq!(single.expressions.len(), 1);
    assert_eq!(
        single.program.imports,
        [GraphBindingImport {
            variable: "a".into(),
            column: single.column.clone(),
            kind: GraphEntityKind::Node,
        }]
    );
    assert!(!single.program.introduced.contains(&"a".to_string()));
    assert!(matches!(
        &single.program.steps[0],
        GraphMatchStep::Node(node) if node.variable == "a"
    ));

    let both = seeds(
        "MATCH ()-[r:KNOWS]->() RETURN [(a:Person)<-[:HAS_CREATOR]-(m)-[:HAS_CREATOR]->(b:Person) \
         WHERE (a = startNode(r) AND b = endNode(r)) OR (endNode(r) = a AND b = startNode(r)) | m] AS c",
    )
    .expect("both orientations seed a");
    assert_eq!(both.variable, "a");
    assert_eq!(both.expressions.len(), 2);

    for unseeded in [
        // One disjunct does not pin a.
        "MATCH ()-[r:KNOWS]->() RETURN [(a:Person)<-[:HAS_CREATOR]-(m) \
         WHERE a = startNode(r) OR m.length > 3 | m] AS c",
        // The other side reads a variable the pattern introduces.
        "MATCH ()-[r:KNOWS]->() RETURN [(a:Person)<-[:HAS_CREATOR]-(m)-[:HAS_CREATOR]->(b:Person) \
         WHERE a = b | m] AS c",
        // Not an equality.
        "MATCH ()-[r:KNOWS]->() RETURN [(a:Person)<-[:HAS_CREATOR]-(m) \
         WHERE a <> startNode(r) | m] AS c",
    ] {
        assert!(seeds(unseeded).is_none(), "{unseeded}");
    }
}

#[test]
fn import_only_columns_reach_the_match_by_identity() {
    let identity = |query: &str, scope: &[&str], columns: &[&str]| {
        let items = returns(query, scope, columns).unwrap();
        pattern(&items[0].expression).identity_columns
    };
    // A comprehension element and a column imported as a node or a
    // relationship, read by the body only as locals.
    assert_eq!(
        identity(
            "MATCH (person) RETURN [p IN posts WHERE (p)-[:HAS_TAG]->()<-[:HAS_INTEREST]-(person)] AS c",
            &["person"],
            &["posts"],
        ),
        ["p"]
    );
    assert_eq!(
        identity(
            "MATCH (a) RETURN [(x)-[q]->(y) | [x.name, q.w]] AS c",
            &["a"],
            &["x", "q"],
        ),
        ["x", "q"]
    );
    // A column the body reads from the row itself keeps its value.
    assert_eq!(
        identity(
            "MATCH (a) RETURN [(x)-[:R]->(c) | CASE WHEN x.title IS NULL OR x.title = '' THEN 'none' ELSE x.title END] AS c",
            &["a"],
            &["x"],
        ),
        Vec::<String>::new()
    );
    // Graph variables are shared from the row, not imported.
    assert_eq!(
        identity("MATCH (a) RETURN [(a)-[:R]->(c) | c] AS c", &["a"], &[]),
        Vec::<String>::new()
    );
    // A seed column holds only the seed's identity.
    let items = returns(
        "MATCH ()-[r:KNOWS]->() RETURN [(a:Person)<-[:HAS_CREATOR]-(m) WHERE a = startNode(r) | m] AS c",
        &["r"],
        &[],
    )
    .unwrap();
    let seeded = pattern(&items[0].expression);
    assert_eq!(
        seeded.identity_columns,
        [seeded.seeds.as_ref().unwrap().column.clone()]
    );
    // Legacy planning copies every argument and seed value.
    for (query, scope, columns) in [
        (
            "MATCH (person) RETURN [p IN posts WHERE (p)-[:HAS_TAG]->()<-[:HAS_INTEREST]-(person)] AS c",
            &["person"][..],
            &["posts"][..],
        ),
        (
            "MATCH ()-[r:KNOWS]->() RETURN [(a:Person)<-[:HAS_CREATOR]-(m) WHERE a = startNode(r) | m] AS c",
            &["r"][..],
            &[][..],
        ),
    ] {
        let legacy =
            with_legacy_pattern_value_imports(|| returns(query, scope, columns)).unwrap();
        let legacy = pattern(&legacy[0].expression);
        assert!(!legacy.arguments.is_empty() || legacy.seeds.is_some(), "{query}");
        assert!(legacy.identity_columns.is_empty(), "{query}");
    }
}
