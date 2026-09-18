use super::*;

#[test]
fn filters_with_null_predicates() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'Missing deleted'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, title: 'Explicit null', deleted_at: null})")
        .unwrap();
    db.query("CREATE (:Memory {id: 3, title: 'Deleted', deleted_at: 'now'})")
        .unwrap();

    let output = db
        .query(
            "MATCH (m:Memory) WHERE m.deleted_at IS NULL RETURN m.title AS title ORDER BY title ASC",
        )
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Explicit null".to_string()))
    );
    assert_eq!(
        output.rows[1].get("title"),
        Some(&Value::String("Missing deleted".to_string()))
    );

    let output = db
        .query("MATCH (m:Memory) WHERE m.deleted_at IS NOT NULL RETURN m.title AS title")
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Deleted".to_string()))
    );
}

#[test]
fn nullable_predicates_preserve_three_valued_logic() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, optional_note: 'present'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, optional_note: 'other'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 3, optional_note: null})")
        .unwrap();
    db.query("CREATE (:Memory {id: 4})").unwrap();

    let matching = db
        .query("MATCH (m:Memory) WHERE m.optional_note = 'present' RETURN m.id AS id")
        .unwrap();
    let negated = db
        .query("MATCH (m:Memory) WHERE NOT (m.optional_note = 'present') RETURN m.id AS id")
        .unwrap();
    let nulls = db
        .query("MATCH (m:Memory) WHERE m.optional_note IS NULL RETURN m.id AS id")
        .unwrap();

    assert_eq!(matching.rows.len(), 1);
    assert_eq!(negated.rows.len(), 1);
    assert_eq!(nulls.rows.len(), 2);
    assert_eq!(matching.rows[0].get("id"), Some(&Value::Int(1)));
    assert_eq!(negated.rows[0].get("id"), Some(&Value::Int(2)));
}

#[test]
fn filters_with_literal_and_parameterized_in_predicates() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'One'})").unwrap();
    db.query("CREATE (:Memory {id: 2, title: 'Two'})").unwrap();
    db.query("CREATE (:Memory {id: 3, title: 'Three'})")
        .unwrap();

    let output = db
        .query("MATCH (m:Memory) WHERE m.id IN [1, 3] RETURN m.title AS title ORDER BY title ASC")
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("One".to_string()))
    );
    assert_eq!(
        output.rows[1].get("title"),
        Some(&Value::String("Three".to_string()))
    );

    let output = db
        .query_with_params(
            "MATCH (m:Memory) WHERE m.id IN $ids RETURN m.title AS title ORDER BY m.id DESC",
            &BTreeMap::from([(
                "ids".to_string(),
                Value::List(vec![Value::Int(1), Value::Int(2)]),
            )]),
        )
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Two".to_string()))
    );
    assert_eq!(
        output.rows[1].get("title"),
        Some(&Value::String("One".to_string()))
    );

    let output = db
        .query_with_params(
            "MATCH (m:Memory) WHERE m.id IN [1, $id] RETURN m.title AS title ORDER BY m.id ASC",
            &BTreeMap::from([("id".to_string(), Value::Int(3))]),
        )
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("One".to_string()))
    );
    assert_eq!(
        output.rows[1].get("title"),
        Some(&Value::String("Three".to_string()))
    );

    let error = db
        .query("MATCH (m:Memory) WHERE m.id IN [1, $id] RETURN m.title AS title")
        .unwrap_err();
    assert!(error.to_string().contains("missing parameter '$id'"));

    db.query("CREATE (:Entity {id: 'e1', name: 'Rust', aliases: ['Ferris', 'Rustacean']})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'e2', name: 'Kuzu', aliases: ['Graph']})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'e3', name: 'Plain', aliases: 'Ferris'})")
        .unwrap();
    let output = db
        .query_with_params(
            "MATCH (e:Entity) WHERE list_contains(e.aliases, $name) RETURN e.id AS id",
            &BTreeMap::from([("name".to_string(), Value::String("Ferris".to_string()))]),
        )
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("id"),
        Some(&Value::String("e1".to_string()))
    );

    let output = db
        .query_with_params(
            "MATCH (e:Entity) WHERE list_contains_lower(e.aliases, $query) RETURN e.id AS id",
            &BTreeMap::from([("query".to_string(), Value::String("RIS".to_string()))]),
        )
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("id"),
        Some(&Value::String("e1".to_string()))
    );
}

#[test]
fn filters_with_string_prefix_and_suffix_predicates() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, title: 'Runtime graph'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 3, title: 'Graph runtime'})")
        .unwrap();

    let output = db
        .query(
            "MATCH (m:Memory) WHERE m.title STARTS WITH 'Graph' RETURN m.title AS title ORDER BY title ASC",
        )
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Graph foundations".to_string()))
    );
    assert_eq!(
        output.rows[1].get("title"),
        Some(&Value::String("Graph runtime".to_string()))
    );

    let output = db
        .query_with_params(
            "MATCH (m:Memory) WHERE m.title ENDS WITH $suffix RETURN m.title AS title",
            &BTreeMap::from([("suffix".to_string(), Value::String("graph".to_string()))]),
        )
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Runtime graph".to_string()))
    );

    let updated = db
        .query("MATCH (m:Memory) WHERE m.title STARTS WITH 'Graph' SET m.kind = 'prefix'")
        .unwrap();
    assert_eq!(updated.rows.len(), 2);

    let output = db
        .query("MATCH (m:Memory) WHERE m.kind = 'prefix' RETURN count(*) AS total")
        .unwrap();
    assert_eq!(output.rows[0].get("total"), Some(&Value::Int(2)));
}

#[test]
fn filters_with_not_equal_predicates() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, kind: 'note', title: 'Note'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, kind: 'task', title: 'Task'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 3, kind: 'thread', title: 'Thread'})")
        .unwrap();

    let output = db
        .query("MATCH (m:Memory) WHERE m.kind <> 'task' RETURN m.title AS title ORDER BY title ASC")
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Note".to_string()))
    );
    assert_eq!(
        output.rows[1].get("title"),
        Some(&Value::String("Thread".to_string()))
    );

    let updated = db
        .query_with_params(
            "MATCH (m:Memory) WHERE id(m) <> $id SET m.visible = true",
            &BTreeMap::from([("id".to_string(), Value::Int(1))]),
        )
        .unwrap();
    assert_eq!(updated.rows.len(), 2);

    let output = db
        .query("MATCH (m:Memory) WHERE m.visible = true RETURN m.title AS title ORDER BY title ASC")
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Note".to_string()))
    );
    assert_eq!(
        output.rows[1].get("title"),
        Some(&Value::String("Thread".to_string()))
    );
}

#[test]
fn filters_with_not_predicates() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, kind: 'note', score: 3, title: 'Note'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, kind: 'task', score: 15, title: 'Task'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 3, kind: 'task', score: 25, title: 'Skip'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 4, kind: 'thread', score: 30, title: 'Thread'})")
        .unwrap();

    let output = db
        .query(
            "MATCH (m:Memory) WHERE NOT (m.kind = 'task' OR m.score < 10) RETURN m.title AS title ORDER BY title ASC",
        )
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Thread".to_string()))
    );

    let updated = db
        .query("MATCH (m:Memory) WHERE NOT m.kind = 'task' SET m.visible = true")
        .unwrap();
    assert_eq!(updated.rows.len(), 2);

    let output = db
        .query("MATCH (m:Memory) WHERE m.visible = true RETURN m.title AS title ORDER BY title ASC")
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Note".to_string()))
    );
    assert_eq!(
        output.rows[1].get("title"),
        Some(&Value::String("Thread".to_string()))
    );
}

#[test]
fn relationship_existence_predicates_cover_nowledge_orphan_entities() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {id: 'orphan', name: 'Orphan', entity_type: 'Concept'})")
        .unwrap();
    db.query(
            "CREATE (:Memory {id: 'm1'})-[:MENTIONS]->(:Entity {id: 'mentioned', name: 'Mentioned', entity_type: 'Concept'})",
        )
        .unwrap();
    db.query(
            "CREATE (:Entity {id: 'related-a', name: 'Related A', entity_type: 'Concept'})-[:RELATES_TO]->(:Entity {id: 'related-b', name: 'Related B', entity_type: 'Concept'})",
        )
        .unwrap();
    db.query("CREATE (:Entity {id: 'labeled', name: 'Labeled', entity_type: 'Concept'})")
        .unwrap();
    db.query("CREATE (:Label {id: 'label-1'})").unwrap();
    db.query(
        "MATCH (e:Entity {id: 'labeled'}), (l:Label {id: 'label-1'}) CREATE (e)-[:HAS_LABEL]->(l)",
    )
    .unwrap();

    let output = db
        .query(
            "MATCH (e:Entity)
                 WHERE NOT (e)<-[:MENTIONS]-(:Memory)
                   AND NOT (e)-[:RELATES_TO]-()
                   AND NOT (e)-[:HAS_LABEL]-()
                 RETURN e.id AS id, e.name AS name, e.entity_type AS entity_type",
        )
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("id"),
        Some(&Value::String("orphan".to_string()))
    );
}

#[test]
fn relationship_existence_stops_before_hydrating_later_large_targets() {
    let execution_memory = crate::executor::ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(1024).unwrap(),
        ..crate::executor::ExecutionMemoryConfig::default()
    };
    let mut db = Database::new_with_config(DatabaseConfig {
        execution_memory,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Probe {id: 'probe'})-[:LINK]->(:Payload {id: 'first'})")
        .unwrap();
    db.query_with_params(
        "CREATE (:Payload {id: 'later', content: $content})",
        &BTreeMap::from([("content".to_string(), Value::String("x".repeat(64 * 1024)))]),
    )
    .unwrap();
    db.query(
        "MATCH (p:Probe {id: 'probe'}), (later:Payload {id: 'later'}) \
         CREATE (p)-[:LINK]->(later)",
    )
    .unwrap();

    let cypher = "MATCH (p:Probe) WHERE (p)-[:LINK]->(:Payload) RETURN p.id AS id";
    let explain = db.explain_query(cypher).unwrap();
    let physical_plan = explain.physical_plan.explain(0);
    assert!(physical_plan.contains("RelationshipExists"));

    let output = db.query(cypher).unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("id"),
        Some(&Value::String("probe".to_string()))
    );
}

#[test]
fn deletes_only_nodes_matching_relationship_existence_predicates() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory-1'})").unwrap();
    db.query("CREATE (:Entity {id: 'orphan'})").unwrap();
    db.query("CREATE (:Entity {id: 'mentioned'})").unwrap();
    db.query(
        "MATCH (m:Memory {id: 'memory-1'}), (e:Entity {id: 'mentioned'})
         CREATE (m)-[:MENTIONS]->(e)",
    )
    .unwrap();

    db.query(
        "MATCH (e:Entity)
         WHERE e.id IN ['orphan', 'mentioned']
           AND NOT (e)<-[:MENTIONS]-(:Memory)
         DETACH DELETE e",
    )
    .unwrap();

    let output = db
        .query("MATCH (e:Entity) RETURN e.id AS id ORDER BY id")
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("id"),
        Some(&Value::String("mentioned".to_string()))
    );
}

#[test]
fn filters_with_or_predicates() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, kind: 'note', score: 3, title: 'Note'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, kind: 'task', score: 15, title: 'Task'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 3, kind: 'task', score: 25, title: 'Skip'})")
        .unwrap();

    let output = db
        .query(
            "MATCH (m:Memory) WHERE m.kind = 'note' OR m.score >= 10 AND m.score < 20 RETURN m.title AS title ORDER BY title ASC",
        )
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Note".to_string()))
    );
    assert_eq!(
        output.rows[1].get("title"),
        Some(&Value::String("Task".to_string()))
    );

    let explain = db
        .explain_query(
            "MATCH (m:Memory) WHERE m.kind = 'note' OR m.score >= 10 RETURN m.title AS title",
        )
        .unwrap();
    let physical_plan = explain.physical_plan.explain(0);
    assert!(physical_plan.contains("NodeProjectionScanExec"));
    assert!(!physical_plan.contains("FilterExec"));
    assert!(!physical_plan.contains("IndexNodeSeek"));
    assert!(!physical_plan.contains("IndexNodeRangeSeek"));
}

#[test]
fn filters_with_parenthesized_predicates() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, kind: 'note', score: 3, title: 'Low note'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, kind: 'note', score: 30, title: 'High note'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 3, kind: 'thread', score: 20, title: 'Thread'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 4, kind: 'task', score: 25, title: 'Task'})")
        .unwrap();

    let output = db
        .query(
            "MATCH (m:Memory) WHERE (m.kind = 'note' OR m.kind = 'thread') AND m.score >= 10 RETURN m.title AS title ORDER BY title ASC",
        )
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("High note".to_string()))
    );
    assert_eq!(
        output.rows[1].get("title"),
        Some(&Value::String("Thread".to_string()))
    );
}

#[test]
fn match_node_property_patterns_filter_reads_and_bind_parameters() {
    let mut db = Database::new();
    db.query("CREATE INDEX ON :Memory(id)").unwrap();
    db.query("CREATE (:Memory {id: 1, title: 'Crystal', is_crystal: true})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, title: 'Raw', is_crystal: false})")
        .unwrap();
    for id in 3..=16 {
        db.query(&format!(
            "CREATE (:Memory {{id: {id}, title: 'Extra {id}', is_crystal: false}})"
        ))
        .unwrap();
    }

    let output = db
        .query_with_params(
            "MATCH (m:Memory {id: $id, is_crystal: true}) RETURN m.title AS title",
            &BTreeMap::from([("id".to_string(), Value::Int(1))]),
        )
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Crystal".to_string()))
    );

    let explain = db
        .explain_query("MATCH (m:Memory {id: 1}) RETURN m.title AS title")
        .unwrap();
    assert!(explain.physical_plan.explain(0).contains("IndexNodeSeek"));
}

#[test]
fn match_node_property_patterns_filter_relationship_endpoints() {
    let mut db = Database::new();
    db.query(
            "CREATE (:Memory {id: 1, is_crystal: true})-[:SYNTHESIZED_FROM]->(:Memory {id: 2, kind: 'note'})",
        )
        .unwrap();
    db.query(
            "CREATE (:Memory {id: 3, is_crystal: true})-[:SYNTHESIZED_FROM]->(:Memory {id: 4, kind: 'thread'})",
        )
        .unwrap();

    let output = db
            .query(
                "MATCH (c:Memory {is_crystal: true})-[:SYNTHESIZED_FROM]->(s:Memory {kind: 'note'}) RETURN DISTINCT s.id AS id",
            )
            .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].get("id"), Some(&Value::Int(2)));
}

#[test]
fn negative_limit_is_rejected_before_execution() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'Should remain'})")
        .unwrap();

    let error = db
        .query("MATCH (m:Memory) RETURN m.title AS title LIMIT -1")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("limit must be a non-negative integer"));
    let output = db
        .query("MATCH (m:Memory) RETURN m.title AS title")
        .unwrap();
    assert_eq!(output.rows.len(), 1);
}

#[test]
fn in_predicate_rejects_non_list_parameter() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'One'})").unwrap();

    let error = db
        .query_with_params(
            "MATCH (m:Memory) WHERE m.id IN $ids RETURN m.title AS title",
            &BTreeMap::from([("ids".to_string(), Value::Int(1))]),
        )
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("IN predicate requires a list value"));
}

#[test]
fn missing_parameter_fails_before_mutation() {
    let mut db = Database::new();
    let error = db
        .query("CREATE (:Memory {id: $id, title: 'missing'})")
        .unwrap_err();
    assert!(error.to_string().contains("missing parameter '$id'"));

    let output = db
        .query("MATCH (m:Memory) RETURN m.title AS title")
        .unwrap();
    assert!(output.rows.is_empty());
}

#[test]
fn normalized_space_case_predicates_cover_thread_move_selection() {
    let mut db = Database::new();
    db.query("CREATE (:Thread {id: 'storage-1', thread_id: 'logical-1'})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'storage-2', thread_id: 'logical-2', space_id: ''})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'storage-3', thread_id: 'logical-3', space_id: 'team'})")
        .unwrap();
    for id in 0..32 {
        db.query(&format!(
            "CREATE (:Thread {{id: 'extra-storage-{id}', thread_id: 'extra-logical-{id}', space_id: 'team'}})"
        ))
        .unwrap();
    }
    db.query("CREATE INDEX ON :Thread(thread_id)").unwrap();

    let source_parameters = BTreeMap::from([
        (
            "thread_ids".to_string(),
            Value::List(vec![
                Value::String("logical-1".into()),
                Value::String("logical-2".into()),
                Value::String("logical-3".into()),
            ]),
        ),
        (
            "source_space_id".to_string(),
            Value::String("default".into()),
        ),
    ]);
    let source_query = "MATCH (t:Thread)
                 WHERE t.thread_id IN $thread_ids
                   AND CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END = $source_space_id
                 RETURN t.id, t.thread_id, t.space_id
                 ORDER BY t.id";
    let explain = db
        .explain_query_with_params(source_query, &source_parameters)
        .unwrap();
    let physical_plan = explain.physical_plan.explain(0);
    assert!(physical_plan.contains("IndexNodeMultiSeek"));
    assert!(physical_plan.contains("NodeProjectionScanExec"));
    assert!(physical_plan.contains("predicate=Some"));
    assert!(!physical_plan.contains("FilterExec"));
    assert!(explain
        .trace
        .decisions
        .iter()
        .any(|decision| decision.contains("choose IndexNodeMultiSeek")));
    let source_rows = db
        .query_with_params(source_query, &source_parameters)
        .unwrap();
    assert_eq!(source_rows.rows.len(), 2);
    assert_eq!(
        source_rows.rows[0].get("t.id"),
        Some(&Value::String("storage-1".into()))
    );
    assert_eq!(
        source_rows.rows[1].get("t.id"),
        Some(&Value::String("storage-2".into()))
    );

    let target_rows = db
        .query_with_params(
            "MATCH (t:Thread)
                 WHERE t.thread_id IN $candidate_ids
                   AND CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END <> $target_space_id
                 RETURN t.thread_id
                 ORDER BY t.thread_id",
            &BTreeMap::from([
                (
                    "candidate_ids".to_string(),
                    Value::List(vec![
                        Value::String("logical-1".into()),
                        Value::String("logical-2".into()),
                        Value::String("logical-3".into()),
                    ]),
                ),
                (
                    "target_space_id".to_string(),
                    Value::String("default".into()),
                ),
            ]),
        )
        .unwrap();
    assert_eq!(target_rows.rows.len(), 1);
    assert_eq!(
        target_rows.rows[0].get("t.thread_id"),
        Some(&Value::String("logical-3".into()))
    );
}

#[test]
fn creates_and_filters_escaped_string_literals() {
    let mut db = Database::new();
    db.query(r#"CREATE (:Memory {id: 1, title: 'It\'s graph\\ready'})"#)
        .unwrap();

    let output = db
        .query(r#"MATCH (m:Memory) WHERE m.title = 'It\'s graph\\ready' RETURN m.title AS title"#)
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("It's graph\\ready".to_string()))
    );
}
