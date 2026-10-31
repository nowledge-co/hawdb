// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn relationship_merge_batch_preserves_scope_and_refuses_unrepresented_clauses() {
    let parameters = BTreeMap::from([("ids".into(), Value::List(vec![Value::Int(1)]))]);
    let prefix = "MATCH (s:Skill {id: 1}) UNWIND $ids AS key MATCH (m:Memory {id: key}) MERGE (s)-[r:EVIDENCE]->(m)";
    let plan = plan_pipeline_query(
        &format!("{prefix} ON CREATE SET r.weight = 1.0 RETURN m.id"),
        &parameters,
    )
    .unwrap();
    assert!(matches!(
        plan,
        LogicalPlan::UnwindMutation {
            operation: BatchMutationOperation::MergeRelationship(_),
            ..
        }
    ));
    for query in [
        format!("{prefix} RETURN r.weight"),
        format!("{prefix} RETURN DISTINCT m.id"),
        format!("{prefix} RETURN m.id LIMIT 1"),
        format!("{prefix} ON MATCH SET r.weight = 2 RETURN m.id"),
        format!("{prefix} SET s.used = true RETURN m.id"),
        "MATCH (s:Skill), (m:Memory) MERGE (s)-[s:EVIDENCE]->(m) RETURN m.id".into(),
        "MATCH (s:Skill {id: key}) UNWIND $ids AS key MATCH (m:Memory {id: key}) MERGE (s)-[:EVIDENCE]->(m) RETURN m.id".into(),
        "MATCH (s:Skill) WHERE s.id = key UNWIND $ids AS key MATCH (m:Memory {id: key}) MERGE (s)-[:EVIDENCE]->(m) RETURN m.id".into(),
        "MATCH (s:Skill) WHERE m.id = 1 UNWIND $ids AS key MATCH (m:Memory {id: key}) MERGE (s)-[:EVIDENCE]->(m) RETURN m.id".into(),
        "MATCH (s:Skill) UNWIND $ids AS s MATCH (m:Memory) MERGE (s)-[:EVIDENCE]->(m) RETURN m.id".into(),
        "MATCH (s:Skill) UNWIND $ids AS key UNWIND $ids AS other MATCH (m:Memory) MERGE (s)-[:EVIDENCE]->(m) RETURN m.id".into(),
        "MATCH (s:Skill) OPTIONAL MATCH (m:Memory) MERGE (s)-[:EVIDENCE]->(m) RETURN m.id".into(),
    ] { assert!(plan_pipeline_query(&query, &parameters).is_err(), "{query}"); }
}

#[test]
fn atomic_mutations_reject_conditions_the_storage_command_cannot_represent() {
    for query in [
        "CREATE (n {id: 1})",
        "MERGE (n {id: 1})",
        "CREATE p = (n:Node {id: 1})",
        "MERGE p = (n:Node {id: 1})",
        "MATCH (a:Node), (b:Node), (unused:Node) CREATE (a)-[:LINK]->(b)",
        "MATCH (a:Node), (b:Node) CREATE (a)-[:LINK* ALL SHORTEST 1..1]->(b)",
        "OPTIONAL MATCH (n:Node) SET n.id = 1",
        "MATCH (n:Node) WITH n SET n.id = 1",
        "MATCH (a:Node), (b:Node) MERGE (a {id: 1})-[:LINK]->(b)",
    ] {
        assert!(
            plan_pipeline_query(query, &BTreeMap::new()).is_err(),
            "{query}"
        );
    }
}

#[test]
fn procedure_yields_enforce_scopes_and_vector_hop_limits() {
    let parameters = BTreeMap::from([(
        "embedding".to_string(),
        Value::List(vec![Value::Float(1.0)]),
    )]);
    let prefix = "CALL vector_search($embedding) YIELD id AS hit, score AS rank";
    let accepted =
        format!("{prefix} MATCH (n:Node)-[:LINK*1..2]->(target:Node) RETURN hit, rank, target.id");
    plan_pipeline_query(&accepted, &parameters).unwrap();
    plan_pipeline_query(
        "CALL vector_search($embedding) RETURN ID, SCORE",
        &parameters,
    )
    .unwrap();
    plan_pipeline_query(
        "CALL vector_search($embedding) YIELD ID, SCORE MATCH (n:Node) RETURN id, score",
        &parameters,
    )
    .unwrap();
    for tail in [
        "MATCH (n:Node) RETURN score",
        "MATCH (hit:Node) RETURN hit",
        "MATCH (n:Node)-[:LINK*1..3]->(target:Node) RETURN target.id",
        "MATCH (n:Node)-[:LINK]->(a:Node) MATCH (a)-[:LINK*1..2]->(b:Node) RETURN b.id",
    ] {
        let query = format!("{prefix} {tail}");
        assert!(plan_pipeline_query(&query, &parameters).is_err(), "{query}");
    }
    for query in [
        "CALL vector_search($embedding) YIELD id AS duplicate, score AS duplicate MATCH (n:Node) RETURN n",
        "CALL vector_search($embedding) YIELD id AS external_id MATCH (n:Node) RETURN n",
        "CALL vector_search($embedding) YIELD unknown MATCH (n:Node) RETURN n",
        "CALL project_graph('g', ['Node'], ['LINK']) RETURN node",
        "CALL vector_search($embedding) MATCH (n:Node) RETURN n",
    ] {
        assert!(plan_pipeline_query(query, &parameters).is_err(), "{query}");
    }
}

#[test]
fn shortest_paths_do_not_discard_additional_predicates_or_projection_modifiers() {
    let matched = "MATCH p = (a:Node)-[:LINK* ALL SHORTEST 1..3]->(b:Node)";
    // Shapes the endpoint-id operator cannot represent use the general
    // shortest-path step, which keeps every predicate and modifier.
    for (tail, needle) in [
        (
            "WHERE a.id = 1 AND b.id = 2 AND a.active = true RETURN length(p) AS hops",
            "active",
        ),
        (
            "WHERE a.id = 1 AND a.id = 3 AND b.id = 2 RETURN length(p) AS hops",
            "Int(3)",
        ),
        (
            "WHERE a.id = 1 AND b.id = 2 RETURN length(p) AS hops LIMIT 1",
            "Limit",
        ),
    ] {
        let query = format!("{matched} {tail}");
        let plan = plan_pipeline_query(&query, &BTreeMap::new()).unwrap();
        let text = format!("{plan:?}");
        assert!(!matches!(plan, LogicalPlan::ShortestPath { .. }), "{query}");
        assert!(
            text.contains("ShortestPath(GraphMatchShortestPath"),
            "{query}"
        );
        assert!(text.contains(needle), "{query}: {text}");
    }
    let query = format!("{matched} WHERE a.id = 1 AND b.id = 2 RETURN length(other) AS hops");
    assert!(plan_pipeline_query(&query, &BTreeMap::new()).is_err());
}

fn shortest_steps(plan: &LogicalPlan, steps: &mut Vec<GraphMatchShortestPath>) {
    if let LogicalPlan::GraphMatch { program, input } = plan {
        for step in &program.steps {
            if let GraphMatchStep::ShortestPath(step) = step {
                steps.push((**step).clone());
            }
        }
        if let Some(input) = input {
            shortest_steps(input, steps);
        }
        return;
    }
    let text = format!("{plan:?}");
    if !text.contains("GraphMatch") {
        return;
    }
    match plan {
        LogicalPlan::Project { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Aggregate { input, .. } => shortest_steps(input, steps),
        _ => {}
    }
}

#[test]
fn shortest_path_functions_bind_correlated_steps() {
    let parameters = BTreeMap::from([
        ("a".to_string(), Value::Int(1)),
        ("b".to_string(), Value::Int(2)),
    ]);
    let plan = plan_normalized_pipeline_query(
        "MATCH (p1:Person {id: $a}), (p2:Person {id: $b}) OPTIONAL MATCH path = shortestPath((p1)-[:KNOWS*]-(p2)) RETURN CASE path IS NULL WHEN true THEN -1 ELSE length(path) END AS len",
        &parameters,
    )
    .unwrap();
    let mut steps = Vec::new();
    shortest_steps(&plan, &mut steps);
    assert_eq!(
        steps,
        vec![GraphMatchShortestPath {
            path: "path".into(),
            source: "p1".into(),
            target: "p2".into(),
            rel_type: "KNOWS".into(),
            direction: RelationshipDirection::Undirected,
            min_hops: 1,
            max_hops: MAX_SHORTEST_PATH_HOPS,
            bounded: false,
            selection: ShortestPathSelection::Any,
        }]
    );
    let text = format!("{plan:?}");
    assert!(text.contains("optional: true"), "{text}");
    assert!(
        text.contains("Path { function: Length, column: \"path\" }"),
        "{text}"
    );

    let plan = plan_pipeline_query(
        "MATCH (a:Item), (b:Item) WHERE a.id = 1 AND b.id = 2 MATCH p = allShortestPaths((a)<-[:LINK*0..4]-(b)) RETURN properties(nodes(p), 'id') AS ids, relationships(p) AS rels",
        &BTreeMap::new(),
    )
    .unwrap();
    let mut steps = Vec::new();
    shortest_steps(&plan, &mut steps);
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].selection, ShortestPathSelection::All);
    assert_eq!(steps[0].direction, RelationshipDirection::Incoming);
    assert_eq!((steps[0].min_hops, steps[0].max_hops), (0, 4));
    assert!(steps[0].bounded);
}

#[test]
fn required_shortest_path_clauses_bind_endpoints_before_searching() {
    let plan = plan_pipeline_query(
        "MATCH p = shortestPath((a:Item)-[:LINK*]->(b:Item)) WHERE a.id = 1 AND b.id = 2 RETURN length(p) AS hops",
        &BTreeMap::new(),
    )
    .unwrap();
    let LogicalPlan::Project { input, .. } = &plan else {
        panic!("expected projection: {plan:?}");
    };
    let LogicalPlan::GraphMatch {
        program,
        input: Some(endpoints),
    } = input.as_ref()
    else {
        panic!("expected path program: {input:?}");
    };
    // Endpoint conjuncts filter before the search; the search has no predicate.
    assert_eq!(program.introduced, vec!["p".to_string()]);
    assert!(program.predicate.is_none(), "{:?}", program.predicate);
    let LogicalPlan::GraphMatch { program, .. } = endpoints.as_ref() else {
        panic!("expected endpoint program: {endpoints:?}");
    };
    assert_eq!(program.introduced, vec!["a".to_string(), "b".to_string()]);
    let endpoint_predicate = format!("{:?}", program.predicate);
    assert!(
        !endpoint_predicate.contains("Length"),
        "{endpoint_predicate}"
    );
}

#[test]
fn shortest_path_endpoints_named_in_the_same_clause_bind_once() {
    let parameters = BTreeMap::from([
        ("a".to_string(), Value::Int(1)),
        ("b".to_string(), Value::Int(2)),
    ]);
    for query in [
        "MATCH (p1:Person {id: $a}), (p2:Person {id: $b}), path = shortestPath((p1)-[:KNOWS*]-(p2)) RETURN length(path) AS len",
        "MATCH (p1:Person {id: $a}), (p2:Person {id: $b}), path = allShortestPaths((p1)-[:KNOWS*]-(p2)) RETURN length(path) AS len",
    ] {
        let plan = plan_pipeline_query(query, &parameters).unwrap();
        let LogicalPlan::Project { input, .. } = &plan else {
            panic!("expected projection: {plan:?}");
        };
        let LogicalPlan::GraphMatch {
            input: Some(endpoints),
            ..
        } = input.as_ref()
        else {
            panic!("expected path program: {input:?}");
        };
        let LogicalPlan::GraphMatch { program, .. } = endpoints.as_ref() else {
            panic!("expected endpoint program: {endpoints:?}");
        };
        // One constrained step per endpoint, not a bare re-check per endpoint.
        let nodes = program
            .steps
            .iter()
            .map(|step| match step {
                GraphMatchStep::Node(node) => node.clone(),
                step => panic!("unexpected endpoint step {step:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            nodes,
            vec![
                GraphMatchNode {
                    variable: "p1".into(),
                    label: "Person".into(),
                    properties: BTreeMap::from([("id".into(), Value::Int(1))]),
                },
                GraphMatchNode {
                    variable: "p2".into(),
                    label: "Person".into(),
                    properties: BTreeMap::from([("id".into(), Value::Int(2))]),
                },
            ],
            "{query}"
        );
        // Independent endpoints normalize to node access, not a generic match.
        let plan = plan_normalized_pipeline_query(query, &parameters).unwrap();
        let LogicalPlan::Project { input, .. } = &plan else {
            panic!("expected projection: {plan:?}");
        };
        let LogicalPlan::GraphMatch {
            input: Some(endpoints),
            ..
        } = input.as_ref()
        else {
            panic!("expected path program: {input:?}");
        };
        assert!(
            matches!(endpoints.as_ref(), LogicalPlan::NodeCartesianProduct { .. }),
            "{query}: {endpoints:?}"
        );
    }
    // Constraints that do not fold without comparing values keep the re-check.
    let plan = plan_pipeline_query(
        "MATCH (a:Item {id: 1}), p = shortestPath((a {id: 2})-[:LINK*]->(b:Item {id: 3})) RETURN length(p) AS hops",
        &BTreeMap::new(),
    )
    .unwrap();
    let text = format!("{plan:?}");
    assert_eq!(text.matches("variable: \"a\"").count(), 3, "{text}");
}

#[test]
fn shortest_path_binding_rejects_unsupported_shapes() {
    for (query, message) in [
        (
            "MATCH p = shortestPath((a:Item)-[:LINK*]->(b:Item {id: 2})) RETURN length(p)",
            "endpoint 'a' must be bound",
        ),
        (
            "MATCH (a:Item {id: 1}), (b:Item {id: 2}) MATCH p = shortestPath((a)-[:LINK*2..3]->(b)) RETURN length(p)",
            "minimum of 0 or 1",
        ),
        (
            "MATCH (a:Item {id: 1}), (b:Item {id: 2}) MATCH p = shortestPath((a)-[:LINK*]->(b)) WHERE length(p) > 1 RETURN p",
            "filter after the search with `WITH p WHERE ...`",
        ),
        (
            "MATCH (a:Item {id: 1}), (b:Item {id: 2}) OPTIONAL MATCH p = allShortestPaths((a)-[:LINK*]-(b)) WHERE a.id = 1 AND length(p) > 1 RETURN p",
            "shortest-path variable 'p'",
        ),
        (
            "MATCH (a:Item {id: 1}), (b:Item {id: 2}) MATCH p = shortestPath((a)-[:LINK*1..257]->(b)) RETURN length(p)",
            "exceeds the limit of 256",
        ),
        (
            "MATCH (a:Item {id: 1}), (b:Item {id: 2}) MATCH p = shortestPath((a)-[:LINK* {w: 1}]->(b)) RETURN length(p)",
            "relationship property patterns",
        ),
        (
            "MATCH (a:Item {id: 1}), (b:Item {id: 2}) MATCH a = shortestPath((a)-[:LINK*]->(b)) RETURN 1",
            "already bound",
        ),
        (
            "MATCH (a:Item {id: 1})-[:LINK*1..2]->(b:Item) RETURN length(a)",
            "path function requires a path",
        ),
    ] {
        let error = plan_pipeline_query(query, &BTreeMap::new())
            .unwrap_err()
            .to_string();
        assert!(error.contains(message), "{query}: {error}");
    }
}

#[test]
fn unused_bounded_path_aliases_remain_distinct_from_node_bindings() {
    plan_pipeline_query(
        "MATCH p = (a:Node)-[:LINK*1..2]->(b:Node) RETURN b.id",
        &BTreeMap::new(),
    )
    .unwrap();
    for query in [
        "MATCH p = (p:Node)-[:LINK]->(b:Node) RETURN b.id",
        "MATCH p = (a:Node)-[:LINK]->(b:Node) MATCH (p:Node) RETURN p.id",
        "MATCH p = (a:Node)-[:LINK]->(b:Node) RETURN p",
    ] {
        assert!(
            plan_pipeline_query(query, &BTreeMap::new()).is_err(),
            "{query}"
        );
    }
}

#[test]
fn normalization_retains_match_boundaries_and_group_output_order() {
    for query in [
        "MATCH (n:Node)-[:LINK]->(n) RETURN n.id",
        "MATCH (a:Node)-[:LINK]->(b:Node)-[:LINK]->(c:Node) RETURN c.id",
        "OPTIONAL MATCH (a:Node)-[:LINK]->(b:Node) WHERE b.id = 1 RETURN b.id",
    ] {
        let plan = plan_normalized_pipeline_query(query, &BTreeMap::new()).unwrap();
        assert!(format!("{plan:?}").contains("GraphMatch {"), "{query}");
    }
    let plan = plan_normalized_pipeline_query(
        "MATCH (n:Node) RETURN COUNT(n) AS count, n.id AS id",
        &BTreeMap::new(),
    )
    .unwrap();
    assert!(matches!(plan, LogicalPlan::Project { .. }));
    let plan = plan_normalized_pipeline_query(
        "MATCH (n:Node) RETURN n.id AS id, COUNT(n) AS count",
        &BTreeMap::new(),
    )
    .unwrap();
    assert!(matches!(plan, LogicalPlan::Aggregate { .. }));
}

#[test]
fn projected_entities_only_restore_native_bindings_when_required() {
    for query in [
        "MATCH (n:Node) WITH n AS item RETURN item.id",
        "MATCH (n:Node) WITH n AS item, n.id AS key WHERE key > 0 RETURN item.id ORDER BY key",
        "MATCH (n:Node) WITH n AS item, COUNT(n) AS count RETURN item.id, count",
        "MATCH (n:Node) RETURN n ORDER BY n.id",
    ] {
        let plan = plan_normalized_pipeline_query(query, &BTreeMap::new()).unwrap();
        assert!(
            !format!("{plan:?}").contains("GraphMatch {"),
            "{query}: {plan:?}"
        );
    }
    for query in [
        "MATCH (n:Node) WITH n AS item RETURN id(item)",
        "MATCH (n:Node) WITH n AS item RETURN item._id",
        "MATCH (n:Node) WITH n AS item WHERE item.id > 0 RETURN item.id",
        "MATCH (n:Node) WITH n AS item MATCH (item)-[:LINK]->(next:Node) RETURN next.id",
    ] {
        let plan = plan_normalized_pipeline_query(query, &BTreeMap::new()).unwrap();
        assert!(format!("{plan:?}").contains("GraphMatch {"), "{query}");
    }
}

#[test]
fn independent_node_products_do_not_capture_reused_or_dropped_names() {
    for query in [
        "MATCH (a:Node), (b:Node) RETURN a.id, b.id",
        "MATCH (a:Node) MATCH (b:Node) RETURN a.id, b.id",
    ] {
        let plan = plan_normalized_pipeline_query(query, &BTreeMap::new()).unwrap();
        assert!(format!("{plan:?}").contains("NodeCartesianProduct {"));
        assert!(!format!("{plan:?}").contains("GraphMatch {"));
    }
    for query in [
        "MATCH (n:Node {id: 1}), (n:Node {id: 2}) RETURN n.id",
        "MATCH (n:Node) WITH n.id AS previous MATCH (n:Node) RETURN n.id",
    ] {
        let plan = plan_normalized_pipeline_query(query, &BTreeMap::new()).unwrap();
        assert!(format!("{plan:?}").contains("GraphMatch {"));
    }
}

#[test]
fn aggregate_projection_movement_requires_infallible_typed_selectors() {
    let query = "MATCH (n:Node) WITH n.group AS bucket, COUNT(n) AS count RETURN bucket, count ORDER BY bucket LIMIT 1";
    let plan = plan_normalized_pipeline_query(query, &BTreeMap::new()).unwrap();
    let LogicalPlan::Limit { input, .. } = plan else {
        panic!("expected LIMIT")
    };
    let LogicalPlan::Project { input, .. } = *input else {
        panic!("expected selector projection")
    };
    assert!(matches!(*input, LogicalPlan::Sort { .. }));
    let query = "MATCH (n:Node) WITH n.group AS bucket, COUNT(n) AS count RETURN bucket, 10 / (1 - bucket) AS risky ORDER BY bucket LIMIT 1";
    let plan = plan_normalized_pipeline_query(query, &BTreeMap::new()).unwrap();
    let LogicalPlan::Limit { input, .. } = plan else {
        panic!("expected LIMIT")
    };
    let LogicalPlan::Sort { input, .. } = *input else {
        panic!("risky projection must remain before SORT")
    };
    assert!(matches!(*input, LogicalPlan::Project { .. }));
}

#[test]
fn normalizes_native_expression_order_keys_without_inlining_column_order_keys() {
    let plan = plan_normalized_pipeline_query(
        "MATCH (m:Memory) \
         RETURN m.id AS id \
         ORDER BY COALESCE(m.pagerank_score, m.importance, 0.5) DESC",
        &BTreeMap::new(),
    )
    .unwrap();
    let text = format!("{plan:?}");
    assert!(
        text.contains("SortKey::Expression") || text.contains("key: Expression"),
        "{text}"
    );
    assert!(!text.contains("\\0order."), "{text}");

    let plan = plan_normalized_pipeline_query(
        "MATCH (m:Memory) WITH m AS item, m.id AS id RETURN id ORDER BY COALESCE(item.score, 0)",
        &BTreeMap::new(),
    )
    .unwrap();
    assert!(format!("{plan:?}").contains("\\0order."));
}

#[test]
fn plans_composed_and_aggregate_order_keys() {
    // Arithmetic over native bindings sorts directly, without a hidden column.
    let plan = plan_normalized_pipeline_query(
        "MATCH (m:Memory) RETURN m.id AS id ORDER BY m.importance * -1, size(m.tags) DESC",
        &BTreeMap::new(),
    )
    .unwrap();
    let text = format!("{plan:?}");
    assert!(text.contains("key: Expression(Binary {"), "{text}");
    assert!(text.contains("key: Expression(ListFunction {"), "{text}");
    assert!(!text.contains("\\0order."), "{text}");

    // Aggregate keys resolve to the projected aggregate, aliased or not.
    for query in [
        "MATCH (m:Memory) RETURN m.kind AS kind, sum(m.importance) AS total \
         ORDER BY sum(m.importance) DESC",
        "MATCH (m:Memory) RETURN m.kind AS kind, sum(m.importance) \
         ORDER BY sum(m.importance) DESC",
    ] {
        let text = format!(
            "{:?}",
            plan_normalized_pipeline_query(query, &BTreeMap::new()).unwrap()
        );
        assert!(
            text.contains("key: Column(\"total\")")
                || text.contains("key: Column(\"sum(m.importance)\")"),
            "{text}"
        );
    }
    let error = plan_normalized_pipeline_query(
        "MATCH (m:Memory) RETURN m.kind AS kind ORDER BY count(*)",
        &BTreeMap::new(),
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("must also be projected"), "{error}");
}

#[test]
fn normalizes_native_graph_columns_in_hidden_order_expressions() {
    let plan = plan_normalized_pipeline_query(
        "MATCH (s:Source) \
         WITH s, CASE WHEN s.name IS NOT NULL THEN lower(s.name) ELSE '' END AS search_name \
         RETURN s.id, search_name \
         ORDER BY COALESCE(s.memory_count, 0) DESC",
        &BTreeMap::new(),
    )
    .unwrap();
    let text = format!("{plan:?}");
    assert!(
        text.contains(
            "Expression(Coalesce([Property { variable: \"s\", property: \"memory_count\" }"
        ),
        "{text}"
    );
    assert!(!text.contains("\\0order."), "{text}");
}

#[test]
fn normalizes_a_later_bound_source_match_to_an_expand() {
    let plan = plan_normalized_pipeline_query(
        "MATCH (c:Memory {is_crystal: true})-[:SYNTHESIZED_FROM]->(src:Memory) \
         MATCH (src)-[:EVOLVES]-(newer:Memory) \
         WHERE newer.created_at > c.created_at \
         RETURN newer.id",
        &BTreeMap::new(),
    )
    .unwrap();
    let text = format!("{plan:?}");
    assert_eq!(text.matches("Expand {").count(), 2, "{text}");
    assert!(!text.contains("GraphMatch {"), "{text}");
    assert!(text.contains("source_variable: \"src\""), "{text}");
}

#[test]
fn normalizes_column_constrained_node_matches_to_node_column_lookups() {
    for query in [
        "MATCH (a:Source) WITH a.id AS source_id \
         MATCH (b:Memory) WHERE b.source_id = source_id RETURN b.id",
        "MATCH (a:Source) WITH a.id AS source_id \
         OPTIONAL MATCH (b:Memory) WHERE b.source_id = source_id RETURN b.id",
    ] {
        let plan = plan_normalized_pipeline_query(query, &BTreeMap::new()).unwrap();
        assert!(
            format!("{plan:?}").contains("NodeColumnLookup {"),
            "{query}"
        );
    }
    let plan = plan_normalized_pipeline_query(
        "MATCH (b:Memory) WHERE b.source_id = 'source' RETURN b.id",
        &BTreeMap::new(),
    )
    .unwrap();
    assert!(!format!("{plan:?}").contains("NodeColumnLookup {"));
}

#[test]
fn normalizes_predicate_free_optional_matches_to_optional_expands() {
    let plan = plan_normalized_pipeline_query(
        "MATCH (m:Memory) OPTIONAL MATCH (m)-[:HAS_LABEL]->(l:Label) \
         RETURN m.id, COLLECT(DISTINCT l.name) AS labels",
        &BTreeMap::new(),
    )
    .unwrap();
    let text = format!("{plan:?}");
    assert!(text.contains("Expand {"), "{text}");
    assert!(text.contains("optional: true"), "{text}");
    assert!(!text.contains("GraphMatch {"), "{text}");
}

#[test]
fn normalizes_distinct_fixed_type_multi_hop_matches_to_expands() {
    let plan = plan_normalized_pipeline_query(
        "MATCH (m:Memory)-[:SYNTHESIZED_FROM]->(src:Memory)-[:MENTIONS]->(e:Entity) \
         WHERE m.is_crystal = true AND e.community_id IS NOT NULL \
         RETURN m.id, e.community_id",
        &BTreeMap::new(),
    )
    .unwrap();
    let text = format!("{plan:?}");
    assert_eq!(text.matches("Expand {").count(), 2, "{text}");
    assert!(!text.contains("GraphMatch {"), "{text}");
}

#[test]
fn chained_match_does_not_move_expression_filters() {
    let plan = plan_normalized_pipeline_query(
        "MATCH (a:Node) WHERE lower(a.name) = 'alpha' \
         MATCH (a)-[:LINK]->(b:Node) \
         RETURN b.id",
        &BTreeMap::new(),
    )
    .unwrap();
    let text = format!("{plan:?}");
    assert!(text.contains("ExpressionEq"), "{text}");
    assert!(
        text.find("Expand {").unwrap() < text.find("Filter {").unwrap(),
        "{text}"
    );
}

#[test]
fn lowers_a_single_optional_relationship_count_to_optional_degree() {
    let plan = plan_pipeline_query(
        "MATCH (e:Entity {id: 1}) \
         OPTIONAL MATCH (e)-[r:RELATES_TO]->(other:Entity) \
         WITH e, COUNT(r) AS degree \
         RETURN e.id, degree",
        &BTreeMap::new(),
    )
    .unwrap();
    let text = format!("{plan:?}");
    assert!(text.contains("OptionalDegree {"), "{text}");
    assert!(!text.contains("GraphMatch {"), "{text}");

    let target_count = plan_pipeline_query(
        "MATCH (e:Entity {id: 1}) \
         OPTIONAL MATCH (e)-[:RELATES_TO]->(other:Entity) \
         WITH e, COUNT(other) AS degree \
         RETURN e.id, degree",
        &BTreeMap::new(),
    )
    .unwrap();
    assert!(format!("{target_count:?}").contains("OptionalDegree {"));

    let reversed_direct_count = plan_pipeline_query(
        "MATCH (e:Entity) \
         OPTIONAL MATCH (m:Memory)-[:MENTIONS]->(e) \
         RETURN e.id, COUNT(m) AS memory_count",
        &BTreeMap::new(),
    )
    .unwrap();
    let text = format!("{reversed_direct_count:?}");
    assert!(text.contains("OptionalDegree {"), "{text}");
    assert!(!text.contains("Aggregate {"), "{text}");

    let reversed_with_filter = plan_pipeline_query(
        "MATCH (e:Entity) \
         OPTIONAL MATCH (:Memory)-[r:MENTIONS]->(e) \
         WITH e, COUNT(r) AS mention_count WHERE mention_count < $after \
         RETURN e.id, mention_count",
        &BTreeMap::from([("after".to_string(), Value::Int(2))]),
    )
    .unwrap();
    let text = format!("{reversed_with_filter:?}");
    assert!(text.contains("OptionalDegree {"), "{text}");
    assert!(text.contains("Filter {"), "{text}");

    let searched_entities = plan_pipeline_query(
        "MATCH (e:Entity) \
         WHERE lower(e.name) CONTAINS $query \
         OPTIONAL MATCH (m:Memory)-[:MENTIONS]->(e) \
         RETURN e.id, COUNT(m) AS memory_count \
         ORDER BY CASE WHEN lower(e.name) = $query THEN 0 ELSE 1 END ASC, memory_count DESC \
         LIMIT $limit",
        &BTreeMap::from([
            ("query".to_string(), Value::String("entity".to_string())),
            ("limit".to_string(), Value::Int(2)),
        ]),
    )
    .unwrap();
    let text = format!("{searched_entities:?}");
    assert!(text.contains("OptionalDegree {"), "{text}");

    let searched_entities = plan_normalized_pipeline_query(
        "MATCH (e:Entity) \
         WHERE lower(e.name) CONTAINS $query \
         OPTIONAL MATCH (m:Memory)-[:MENTIONS]->(e) \
         RETURN e.id, COUNT(m) AS memory_count \
         ORDER BY CASE WHEN lower(e.name) = $query THEN 0 ELSE 1 END ASC, memory_count DESC \
         LIMIT $limit",
        &BTreeMap::from([
            ("query".to_string(), Value::String("entity".to_string())),
            ("limit".to_string(), Value::Int(2)),
        ]),
    )
    .unwrap();
    let text = format!("{searched_entities:?}");
    assert!(text.contains("key: Expression"), "{text}");
    assert!(!text.contains("\\0order."), "{text}");
}

#[test]
fn normalizes_global_optional_counts_to_expands() {
    for query in [
        "MATCH (t:Thread {id: 1}) OPTIONAL MATCH (t)-[:CONTAINS]->(m:Message) RETURN COUNT(m)",
        "MATCH (t:Thread)-[:CONTAINS]->(m:Message) OPTIONAL MATCH (:Memory)-[r:EXTRACTED_FROM]->(m) RETURN COUNT(r)",
    ] {
        let plan = plan_normalized_pipeline_query(query, &BTreeMap::new()).unwrap();
        let text = format!("{plan:?}");
        assert!(text.contains("Aggregate {"), "{text}");
        assert!(text.contains("Expand {"), "{text}");
        assert!(!text.contains("GraphMatch {"), "{text}");
        assert!(!text.contains("optional: true"), "{text}");
    }
}

#[test]
fn lowers_two_optional_relationship_counts_to_a_count_sum() {
    let plan = plan_pipeline_query(
        "MATCH (e:Entity {id: 1}) \
         OPTIONAL MATCH (e)-[r1:RELATES_TO]-() \
         OPTIONAL MATCH ()-[r2:RELATES_TO]->(e) \
         RETURN (COUNT(DISTINCT r1) + COUNT(DISTINCT r2))",
        &BTreeMap::new(),
    )
    .unwrap();
    let text = format!("{plan:?}");
    assert!(text.contains("OptionalRelationshipCountSum {"), "{text}");
    assert!(text.contains("distinct: true"), "{text}");

    let filtered = plan_pipeline_query(
        "MATCH (e:Entity {id: $eid}) \
         OPTIONAL MATCH (e)-[r1:RELATES_TO]-() \
         WHERE r1.source_reference <> $mid OR r1.source_reference IS NULL OR r1.source_reference = '' \
         OPTIONAL MATCH ()-[r2:RELATES_TO]->(e) \
         WHERE r2.source_reference <> $mid OR r2.source_reference IS NULL OR r2.source_reference = '' \
         RETURN (COUNT(r1) + COUNT(r2))",
        &BTreeMap::from([
            ("eid".to_string(), Value::String("entity".to_string())),
            ("mid".to_string(), Value::String("memory".to_string())),
        ]),
    )
    .unwrap();
    let text = format!("{filtered:?}");
    assert!(text.contains("PropertyNotEqOrEmpty"), "{text}");
}

#[test]
fn derives_thread_repair_stats_only_for_the_complete_fixed_schema_pipeline() {
    let query = "MATCH (t:Thread) \
        OPTIONAL MATCH (ti:ThreadIdentity) WHERE ti.thread_node_id = t.id \
        WITH t, COUNT(ti) AS identity_refs \
        OPTIONAL MATCH (t)-[:CONTAINS]->(msg:Message) \
        WITH t, identity_refs, COUNT(msg) AS legacy_messages \
        OPTIONAL MATCH (t)-[:COMPACTS_TO]->(m:Memory) \
        RETURN t.id, t.thread_id, \
            CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END, \
            COALESCE(t.message_count, 0), identity_refs, legacy_messages, COUNT(m) \
        ORDER BY t.id ASC";
    let plan = plan_pipeline_query(query, &BTreeMap::new()).unwrap();
    assert!(matches!(plan, LogicalPlan::ThreadRepairStats { .. }));

    let non_schema_query = query.replace("t.thread_id", "t.id");
    let plan = plan_pipeline_query(&non_schema_query, &BTreeMap::new()).unwrap();
    assert!(!matches!(plan, LogicalPlan::ThreadRepairStats { .. }));
}

#[test]
fn bounded_match_rejects_unrepresentable_relationship_shapes_before_normalization() {
    for range in ["0..1", "1..2", "2..2"] {
        for pattern in [
            format!("(a:Node)-[r:LINK*{range}]->(b:Node)"),
            format!("(a:Node)-[:LINK*{range} {{weight: 1}}]->(b:Node)"),
            format!("(a:Node)-[*{range}]->(b:Node)"),
        ] {
            for prefix in ["MATCH", "OPTIONAL MATCH"] {
                let query = format!("{prefix} {pattern} RETURN b.id");
                hawdb_cypher::parse_pipeline(&query).unwrap();
                for plan in [
                    plan_pipeline_query(&query, &BTreeMap::new()),
                    plan_normalized_pipeline_query(&query, &BTreeMap::new()),
                ] {
                    assert!(
                        matches!(plan, Err(HawDBError::Semantic(ref message))
                        if message.contains("supported only for one-hop patterns")),
                        "{query}: {plan:?}"
                    );
                }
            }
        }
    }
    for pattern in [
        "(a:Node)-[r:LINK]->(b:Node)",
        "(a:Node)-[:LINK {weight: 1}]->(b:Node)",
        "(a:Node)-[]->(b:Node)",
        "(a:Node)<-[:LINK]-(b:Node)",
        "(a:Node)-[:LINK]-(b:Node)",
        "(a:Node)-[:LINK*0..1]->(b:Node)",
        "(a:Node)-[:LINK*1..2]->(b:Node {id: 2})",
        "(a:Node)<-[:LINK*1..2]-(b:Node)",
        "(a:Node)-[:LINK*2..2]-(b:Node)",
        "(a:Node)<-[:LINK*0..1]-(b:Node)",
    ] {
        let query = format!("MATCH {pattern} RETURN b.id");
        plan_pipeline_query(&query, &BTreeMap::new()).unwrap();
        plan_normalized_pipeline_query(&query, &BTreeMap::new()).unwrap();
    }
}

#[test]
fn ranked_evidence_normalizes_its_input_without_changing_legacy_multi_with_plans() {
    let query = "MATCH (n:Node) WHERE n.visible = true WITH n.group_id AS g, n.id AS doc, n.score AS score WITH g, collect_top(doc, score, 2) AS evidence RETURN g, evidence";
    let parsed = hawdb_cypher::parse_pipeline(query).unwrap();
    let plan = plan_parsed_pipeline_query(&parsed, &BTreeMap::new()).unwrap();
    let debug = format!("{plan:?}");
    assert!(!debug.contains("GraphMatch {"), "{debug}");
    assert!(debug.contains("Ranked {"));
    let legacy = query.replace("collect_top(doc, score, 2)", "collect(doc)");
    let parsed = hawdb_cypher::parse_pipeline(&legacy).unwrap();
    let plan = plan_parsed_pipeline_query(&parsed, &BTreeMap::new()).unwrap();
    assert!(format!("{plan:?}").contains("GraphMatch {"));
}

fn planned_aggregations(plan: &LogicalPlan) -> &[Aggregation] {
    match plan {
        LogicalPlan::Aggregate { items, .. } => items,
        LogicalPlan::Project { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Distinct { input } => planned_aggregations(input),
        other => panic!("expected an aggregate plan, got {other:?}"),
    }
}

#[test]
fn simple_aggregate_arguments_keep_direct_targets() {
    let plan = plan_pipeline_query(
        "MATCH (n:Item) RETURN count(n.x) AS c, min(n.x) AS lo, avg(n.x) AS mean, sum(n.x) AS total, count(DISTINCT n.x) AS d",
        &BTreeMap::new(),
    )
    .unwrap();
    let property = || AggregateTarget::Property {
        variable: "n".into(),
        property: "x".into(),
    };
    let planned = planned_aggregations(&plan)
        .iter()
        .map(|item| (item.function, item.target.clone(), item.distinct))
        .collect::<Vec<_>>();
    assert_eq!(
        planned,
        [
            (AggregateFunction::Count, property(), false),
            (AggregateFunction::Min, property(), false),
            (AggregateFunction::Avg, property(), false),
            (AggregateFunction::Sum, property(), false),
            (AggregateFunction::Count, property(), true),
        ]
    );
}

#[test]
fn expression_aggregate_arguments_evaluate_per_row() {
    let plan = plan_pipeline_query(
        "MATCH (n:Item) RETURN n.g AS g, sum(n.x * 2) AS doubled, count(DISTINCT n.x % 2), max(-n.x) AS neg",
        &BTreeMap::new(),
    )
    .unwrap();
    let items = planned_aggregations(&plan);
    let property = || {
        Box::new(ProjectionExpression::Property {
            variable: "n".into(),
            property: "x".into(),
        })
    };
    assert_eq!(items.len(), 3);
    assert_eq!(items[0].function, AggregateFunction::Sum);
    assert_eq!(
        items[0].target,
        AggregateTarget::Expression(ProjectionExpression::Binary {
            left: property(),
            op: ScalarBinaryOp::Multiply,
            right: Box::new(ProjectionExpression::Literal(Value::Int(2))),
        })
    );
    assert_eq!(items[1].function, AggregateFunction::Count);
    assert!(items[1].distinct);
    assert_eq!(
        items[2].target,
        AggregateTarget::Expression(ProjectionExpression::Binary {
            left: Box::new(ProjectionExpression::Literal(Value::Int(-1))),
            op: ScalarBinaryOp::Multiply,
            right: property(),
        })
    );

    let plan = plan_pipeline_query(
        "MATCH (n:Item) WITH n.g AS g, n.x AS x RETURN g, sum(x * 2) AS s, min(x) AS lo",
        &BTreeMap::new(),
    )
    .unwrap();
    let items = planned_aggregations(&plan);
    assert_eq!(
        items[0].target,
        AggregateTarget::Expression(ProjectionExpression::Binary {
            left: Box::new(ProjectionExpression::Column("x".into())),
            op: ScalarBinaryOp::Multiply,
            right: Box::new(ProjectionExpression::Literal(Value::Int(2))),
        })
    );
    assert_eq!(items[1].target, AggregateTarget::Column("x".into()));
}

#[test]
fn nested_aggregate_arguments_are_rejected() {
    let error = plan_pipeline_query(
        "MATCH (n:Item) RETURN sum(count(n) + 1) AS s",
        &BTreeMap::new(),
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("cannot contain aggregate"),
        "{error}"
    );
}

#[test]
fn relationship_merge_batch_keeps_unrelated_mutation_errors_specific() {
    for (query, expected) in [
        (
            "MERGE (n:A {id: 1}) RETURN n",
            "mutation has unsupported following clauses",
        ),
        (
            "UNWIND [1] AS row MATCH (n:A {id: row}) SET n.seen = true",
            "UNWIND mutation pipeline is unsupported",
        ),
        (
            "UNWIND [1] AS row CREATE (:A {id: row})",
            "row-bound value 'row' is not supported in this position",
        ),
        (
            "UNWIND [1] AS row MERGE (:A {id: row})",
            "row-bound value 'row' is not supported in this position",
        ),
    ] {
        let error = plan_pipeline_query(query, &BTreeMap::new()).unwrap_err();
        assert!(error.to_string().contains(expected), "{error:?}");
    }
}

#[test]
fn relationship_merge_batch_retains_box_payload_and_projection_capacity() {
    use hawdb_core::RetainedHeapBytes;
    let parameters = BTreeMap::from([("ids".into(), Value::List(vec![Value::Int(1)]))]);
    let plan = plan_pipeline_query("MATCH (s:Skill {id: 1}) UNWIND $ids AS key MATCH (m:Memory {id: key}) MERGE (s)-[r:EVIDENCE]->(m) RETURN m.id", &parameters).unwrap();
    let LogicalPlan::UnwindMutation { mut operation, .. } = plan else {
        panic!("expected row mutation")
    };
    assert_eq!(BatchMutationValue::Row.retained_heap_bytes(), Some(0));
    let baseline = operation.retained_heap_bytes().unwrap();
    let payload_bytes = 64 * 1024;
    {
        let BatchMutationOperation::MergeRelationship(merge) = &mut operation else {
            panic!("expected relationship mutation")
        };
        assert!(baseline >= std::mem::size_of_val(merge.as_ref()));
        merge.on_create_properties.insert(
            "payload".into(),
            BatchMutationValue::Static(Value::String("x".repeat(payload_bytes))),
        );
    }
    let with_payload = operation.retained_heap_bytes().unwrap();
    assert!(with_payload >= baseline + payload_bytes);
    {
        let BatchMutationOperation::MergeRelationship(merge) = &mut operation else {
            panic!("expected relationship mutation")
        };
        let projection = &mut merge.returns.as_mut().unwrap()[0];
        projection
            .name
            .reserve_exact(payload_bytes + projection.name.capacity());
    }
    assert!(operation.retained_heap_bytes().unwrap() >= with_payload + payload_bytes);
}

#[test]
fn row_bound_pattern_properties_reuse_lookup_and_optional_predicates() {
    let parameters = BTreeMap::from([("ids".into(), Value::List(vec![Value::Int(1)]))]);
    for optional in [false, true] {
        let prefix = if optional { "OPTIONAL MATCH" } else { "MATCH" };
        let query = format!("UNWIND $ids AS candidate_id WITH candidate_id {prefix} (n:Item {{id: candidate_id}}) RETURN n.id");
        let plan = plan_normalized_pipeline_query(&query, &parameters).unwrap();
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected projection")
        };
        assert!(
            matches!(*input, LogicalPlan::NodeColumnLookup { optional: actual, .. } if actual == optional)
        );
    }
    let plan = plan_normalized_pipeline_query(
        "UNWIND $ids AS candidate_id OPTIONAL MATCH (n:Item {id: candidate_id, active: true}) WHERE n.other = 1 RETURN n.id",
        &parameters,
    ).unwrap();
    let LogicalPlan::Project { input, .. } = plan else {
        panic!("expected projection")
    };
    let LogicalPlan::GraphMatch { program, .. } = *input else {
        panic!("expected correlated MATCH")
    };
    assert!(program.optional);
    assert!(matches!(program.predicate, Some(Predicate::And(_))));
    let GraphMatchStep::Node(node) = &program.steps[0] else {
        panic!("expected node step")
    };
    assert_eq!(
        node.properties,
        BTreeMap::from([("active".into(), Value::Bool(true))])
    );
}
