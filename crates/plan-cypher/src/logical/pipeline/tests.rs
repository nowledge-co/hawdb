use super::*;

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
    for tail in [
        "WHERE a.id = 1 AND b.id = 2 AND a.active = true RETURN length(p) AS hops",
        "WHERE a.id = 1 AND a.id = 3 AND b.id = 2 RETURN length(p) AS hops",
        "WHERE a.id = 1 AND b.id = 2 RETURN length(p) AS hops LIMIT 1",
        "WHERE a.id = 1 AND b.id = 2 RETURN length(other) AS hops",
    ] {
        let query = format!("{matched} {tail}");
        assert!(
            plan_pipeline_query(&query, &BTreeMap::new()).is_err(),
            "{query}"
        );
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
