use super::{
    parse, parse_profiled, AlterPropertyState, AlterTableState, ComparisonOp, CreateCompositeIndex,
    CreateIndex, CreateProperty, GraphAlgorithm, GraphAlgorithmKind, GraphAlgorithmOptions,
    OrderDirection, OrderExpression, ProjectGraph, PropertyPredicate, RelationshipDirection,
    ReturnExpression, ReturnValueExpression, SchemaObjectState, SchemaPropertyType,
    SchemaTableKind, SetValueExpression, Statement, ValueExpression, VectorSearch, WithAliasFilter,
    WithAliasFilterExpression, WithAliasFilterOp,
};
use skein_core::Value;

#[test]
fn parses_create_node() {
    let statement = parse("CREATE (:Memory {id: 1, title: 'hello'})").unwrap();
    let Statement::CreateNode(node) = statement else {
        panic!("expected create node");
    };
    assert_eq!(node.label, "Memory");
    assert_eq!(node.properties.len(), 2);
}

#[test]
fn parser_accepts_keyword_case_and_spacing_variants() {
    let statement = parse("  match   (m:Memory)  return  m.title as title  ").unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert_eq!(query.variable, "m");
    assert_eq!(query.returns[0].alias.as_deref(), Some("title"));
}

#[test]
fn profiled_parse_reports_input_size_for_success_and_failure() {
    let query = "MATCH (m:Memory) RETURN m.id AS id";
    let success = parse_profiled(query);
    assert_eq!(success.metrics.input_bytes, query.len());
    assert!(success.result.is_ok());

    let invalid = "MATCH (";
    let failure = parse_profiled(invalid);
    assert_eq!(failure.metrics.input_bytes, invalid.len());
    assert!(failure.result.is_err());
}

#[test]
fn parses_checkpoint_control_statement() {
    let statement = parse("CHECKPOINT;").unwrap();
    assert_eq!(statement, Statement::Checkpoint);
}

#[test]
fn parses_transaction_control_statements() {
    assert_eq!(
        parse("BEGIN TRANSACTION").unwrap(),
        Statement::BeginTransaction
    );
    assert_eq!(parse("COMMIT;").unwrap(), Statement::Commit);
    assert_eq!(parse("ROLLBACK;").unwrap(), Statement::Rollback);
}

#[test]
fn parses_set_system_variable_statement() {
    let statement = parse("SET system.work_priority = 'background'").unwrap();
    let Statement::SetSystemVariable(set) = statement else {
        panic!("expected set system variable");
    };
    assert_eq!(set.name, "work_priority");
    assert_eq!(
        set.value,
        ValueExpression::Literal(Value::String("background".to_string()))
    );
}

#[test]
fn parses_set_system_variable_keyword_statement() {
    let statement = parse("SET SYSTEM VARIABLE work_priority = 'background'").unwrap();
    let Statement::SetSystemVariable(set) = statement else {
        panic!("expected set system variable");
    };
    assert_eq!(set.name, "work_priority");
    assert_eq!(
        set.value,
        ValueExpression::Literal(Value::String("background".to_string()))
    );

    let statement = parse("SET SYSTEM VARIABLE system.work_class = 'analytics'").unwrap();
    let Statement::SetSystemVariable(set) = statement else {
        panic!("expected set system variable");
    };
    assert_eq!(set.name, "work_class");
    assert_eq!(
        set.value,
        ValueExpression::Literal(Value::String("analytics".to_string()))
    );
}

#[test]
fn parses_cypher_system_hints() {
    let statement = parse(
        "CYPHER system.work_priority = 'background' system.work_class = 'analytics' \
         system.estimated_operations = 64 system.optimizer_search = 'memo' \
         MATCH (m:Memory) RETURN m.id AS id",
    )
    .unwrap();
    let Statement::CypherQuery(query) = statement else {
        panic!("expected cypher query");
    };
    assert_eq!(query.system_variables.len(), 4);
    assert_eq!(query.system_variables[0].name, "work_priority");
    assert_eq!(
        query.system_variables[0].value,
        ValueExpression::Literal(Value::String("background".to_string()))
    );
    assert_eq!(query.system_variables[1].name, "work_class");
    assert_eq!(
        query.system_variables[1].value,
        ValueExpression::Literal(Value::String("analytics".to_string()))
    );
    assert_eq!(query.system_variables[2].name, "estimated_operations");
    assert_eq!(
        query.system_variables[2].value,
        ValueExpression::Literal(Value::Int(64))
    );
    assert_eq!(query.system_variables[3].name, "optimizer_search");
    assert_eq!(
        query.system_variables[3].value,
        ValueExpression::Literal(Value::String("memo".to_string()))
    );
    let Statement::MatchReturn(match_return) = query.statement else {
        panic!("expected inner match return");
    };
    assert_eq!(match_return.variable, "m");
}

#[test]
fn parses_explain_statements() {
    let statement = parse("EXPLAIN MATCH (m:Memory) RETURN m.id AS id").unwrap();
    let Statement::Explain(explain) = statement else {
        panic!("expected explain");
    };
    assert!(!explain.analyze);
    let Statement::MatchReturn(query) = explain.statement else {
        panic!("expected inner match return");
    };
    assert_eq!(query.variable, "m");

    let statement = parse("EXPLAIN ANALYZE MATCH (m:Memory) RETURN m.id AS id").unwrap();
    let Statement::Explain(explain) = statement else {
        panic!("expected explain analyze");
    };
    assert!(explain.analyze);
}

#[test]
fn rejects_explain_control_statements() {
    let error = parse("EXPLAIN CHECKPOINT").unwrap_err();
    assert!(error
        .to_string()
        .contains("EXPLAIN requires a query or mutation statement"));

    let error = parse("EXPLAIN EXPLAIN MATCH (m:Memory) RETURN m.id AS id").unwrap_err();
    assert!(error
        .to_string()
        .contains("EXPLAIN requires a query or mutation statement"));
}

#[test]
fn rejects_cypher_system_hints_on_control_statements() {
    let error = parse("CYPHER system.work_priority = 'background' SET system.work_class = 'query'")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("CYPHER system hints require a query or mutation statement"));

    let error = parse("CYPHER system.work_priority = 'background' CHECKPOINT").unwrap_err();
    assert!(error
        .to_string()
        .contains("CYPHER system hints require a query or mutation statement"));
}

#[test]
fn rejects_system_variable_keyword_inside_cypher_hints() {
    let error = parse(
        "CYPHER SYSTEM VARIABLE work_priority = 'background' MATCH (m:Memory) RETURN m.id AS id",
    )
    .unwrap_err();

    assert!(error.to_string().contains("expected '.'"));
}

#[test]
fn parses_unlabeled_node_match() {
    let statement = parse("MATCH (n) WHERE n.id IN $ids RETURN n.id").unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert_eq!(query.variable, "n");
    assert_eq!(query.label, "");
    assert_eq!(
        query.predicate,
        Some(PropertyPredicate::In {
            variable: "n".to_string(),
            property: "id".to_string(),
            values: ValueExpression::Parameter("ids".to_string()),
        })
    );
}

#[test]
fn parses_multi_label_node_match() {
    let statement = parse("MATCH (n:Entity:Memory) RETURN n.id").unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert_eq!(query.variable, "n");
    assert_eq!(query.label, "Entity:Memory");
}

#[test]
fn parses_variable_return_item() {
    let statement = parse("MATCH (m:Memory {id: $memory_id}) RETURN m").unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert_eq!(
        query.returns[0].expression,
        ReturnExpression::Variable("m".to_string())
    );
    assert_eq!(query.returns[0].alias, None);
}

#[test]
fn parser_keeps_keyword_boundaries() {
    let error = parse("CREATEINDEX ON :Memory(id)").unwrap_err();
    assert!(error.to_string().contains("expected BEGIN"));
}

#[test]
fn parser_reports_trailing_input_position() {
    let error = parse("MATCH (m:Memory) RETURN m.title unexpected").unwrap_err();
    let message = error.to_string();
    assert!(message.contains("unexpected trailing input"));
    assert!(message.contains("byte"));
    assert!(message.contains("line"));
    assert!(message.contains("column"));
    assert!(message.contains("near `unexpected`"));
}

#[test]
fn parser_reports_multiline_context() {
    let error = parse("MATCH (m:Memory)\nRETURN m.title unexpected").unwrap_err();
    let message = error.to_string();
    assert!(message.contains("line 2"));
    assert!(message.contains("near `unexpected`"));
}

#[test]
fn parser_rejects_invalid_identifier_start() {
    let error = parse("MATCH (1m:Memory) RETURN 1m.title").unwrap_err();
    let message = error.to_string();
    assert!(message.contains("expected identifier"));
    assert!(message.contains("near `1m:Memory) RETURN"));
}

#[test]
fn parser_rejects_invalid_numeric_literal() {
    let error = parse("CREATE (:Memory {id: -})").unwrap_err();
    let message = error.to_string();
    assert!(message.contains("expected integer digits"));
    assert!(message.contains("near `})`"));
}

#[test]
fn parses_escaped_string_literals() {
    let statement =
        parse(r#"CREATE (:Memory {title: 'It\'s graph\\ready', note: "line\nnext"})"#).unwrap();
    let Statement::CreateNode(node) = statement else {
        panic!("expected create node");
    };
    assert_eq!(
        node.properties.get("title"),
        Some(&ValueExpression::Literal(Value::String(
            "It's graph\\ready".to_string()
        )))
    );
    assert_eq!(
        node.properties.get("note"),
        Some(&ValueExpression::Literal(Value::String(
            "line\nnext".to_string()
        )))
    );
}

#[test]
fn parses_current_timestamp_value_expression() {
    let statement =
        parse("CREATE (j:AugmentationJob {job_id: 'j1', created_at: CURRENT_TIMESTAMP()})")
            .unwrap();
    let Statement::CreateNode(node) = statement else {
        panic!("expected create node");
    };
    assert_eq!(
        node.properties.get("created_at"),
        Some(&ValueExpression::CurrentTimestamp)
    );

    let statement =
        parse("MATCH (j:AugmentationJob {job_id: 'j1'}) SET j.started_at = CURRENT_TIMESTAMP()")
            .unwrap();
    let Statement::MatchSet(update) = statement else {
        panic!("expected match set");
    };
    assert_eq!(
        update.sets[0].value,
        SetValueExpression::Value(ValueExpression::CurrentTimestamp)
    );
}

#[test]
fn parses_timestamp_value_expression() {
    let statement = parse("CREATE (:Memory {id: 'm1', created_at: timestamp($now)})").unwrap();
    let Statement::CreateNode(node) = statement else {
        panic!("expected create node");
    };
    assert_eq!(
        node.properties.get("created_at"),
        Some(&ValueExpression::Timestamp(Box::new(
            ValueExpression::Parameter("now".to_string())
        )))
    );

    let statement =
        parse("MATCH (m:Memory) WHERE m.created_at > timestamp($cutoff) RETURN count(m)").unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert_eq!(
        query.predicate,
        Some(PropertyPredicate::Compare {
            variable: "m".to_string(),
            property: "created_at".to_string(),
            op: ComparisonOp::Gt,
            value: ValueExpression::Timestamp(Box::new(ValueExpression::Parameter(
                "cutoff".to_string()
            )))
        })
    );
}

#[test]
fn parses_cast_timestamp_value_expression() {
    let statement = parse(
        "MATCH (m:Memory) WHERE m.created_at >= CAST($recent_7d AS TIMESTAMP) RETURN count(m)",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert_eq!(
        query.predicate,
        Some(PropertyPredicate::Compare {
            variable: "m".to_string(),
            property: "created_at".to_string(),
            op: ComparisonOp::Gte,
            value: ValueExpression::Timestamp(Box::new(ValueExpression::Parameter(
                "recent_7d".to_string()
            )))
        })
    );
}

#[test]
fn rejects_unsupported_string_escape_literals() {
    let error = parse(r#"CREATE (:Memory {title: 'bad\q'})"#).unwrap_err();
    assert!(error
        .to_string()
        .contains("unsupported escape sequence \\q"));
}

#[test]
fn parses_schema_ddl() {
    assert_eq!(
        parse("CREATE NODE LABEL Memory").unwrap(),
        Statement::CreateNodeLabel("Memory".to_string())
    );
    assert_eq!(
        parse("CREATE RELATIONSHIP TYPE MENTIONS").unwrap(),
        Statement::CreateRelationshipType("MENTIONS".to_string())
    );
    assert_eq!(
        parse("CREATE NODE TABLE Memory").unwrap(),
        Statement::CreateNodeTable("Memory".to_string())
    );
    assert_eq!(
        parse("CREATE RELATIONSHIP TABLE MENTIONS").unwrap(),
        Statement::CreateRelationshipTable("MENTIONS".to_string())
    );
    assert_eq!(
        parse("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL").unwrap(),
        Statement::CreateProperty(CreateProperty {
            table_kind: SchemaTableKind::Node,
            table: "Memory".to_string(),
            property: "id".to_string(),
            value_type: SchemaPropertyType::Int,
            nullable: false,
        })
    );
    assert_eq!(
        parse("CREATE PROPERTY ON RELATIONSHIP TABLE MENTIONS(weight) TYPE INT").unwrap(),
        Statement::CreateProperty(CreateProperty {
            table_kind: SchemaTableKind::Relationship,
            table: "MENTIONS".to_string(),
            property: "weight".to_string(),
            value_type: SchemaPropertyType::Int,
            nullable: true,
        })
    );
    assert_eq!(
        parse("CREATE INDEX ON :Memory(id)").unwrap(),
        Statement::CreateIndex(CreateIndex {
            label: "Memory".to_string(),
            property: "id".to_string(),
        })
    );
    assert_eq!(
        parse("CREATE INDEX ON :Memory(kind, source_id)").unwrap(),
        Statement::CreateCompositeIndex(CreateCompositeIndex {
            label: "Memory".to_string(),
            properties: vec!["kind".to_string(), "source_id".to_string()],
        })
    );
    assert_eq!(
        parse("CREATE RANGE INDEX ON :Memory(created_at)").unwrap(),
        Statement::CreateRangeIndex(CreateIndex {
            label: "Memory".to_string(),
            property: "created_at".to_string(),
        })
    );
    assert_eq!(
        parse("CREATE FULLTEXT INDEX ON :Memory(title)").unwrap(),
        Statement::CreateFullTextIndex(CreateIndex {
            label: "Memory".to_string(),
            property: "title".to_string(),
        })
    );
    assert_eq!(
        parse("CREATE CONSTRAINT ON :Memory(id) ASSERT UNIQUE").unwrap(),
        Statement::CreateUniqueConstraint(CreateIndex {
            label: "Memory".to_string(),
            property: "id".to_string(),
        })
    );
    assert_eq!(
        parse("CREATE CONSTRAINT ON :Memory(id) ASSERT EXISTS").unwrap(),
        Statement::CreateNodePropertyExistsConstraint(CreateIndex {
            label: "Memory".to_string(),
            property: "id".to_string(),
        })
    );
    assert_eq!(
        parse("CREATE CONSTRAINT ON :Memory(id) ASSERT NOT NULL").unwrap(),
        Statement::CreateNodePropertyExistsConstraint(CreateIndex {
            label: "Memory".to_string(),
            property: "id".to_string(),
        })
    );
    assert_eq!(
        parse("CREATE CONSTRAINT ON -[:MENTIONS(weight)]-> ASSERT EXISTS").unwrap(),
        Statement::CreateRelationshipPropertyExistsConstraint(CreateIndex {
            label: "MENTIONS".to_string(),
            property: "weight".to_string(),
        })
    );
    assert_eq!(
        parse("CREATE CONSTRAINT ON -[:MENTIONS(id)]-> ASSERT UNIQUE").unwrap(),
        Statement::CreateRelationshipUniqueConstraint(CreateIndex {
            label: "MENTIONS".to_string(),
            property: "id".to_string(),
        })
    );
    assert_eq!(
        parse("CREATE CONSTRAINT ON -[:MENTIONS(weight)]-> ASSERT NOT NULL").unwrap(),
        Statement::CreateRelationshipPropertyExistsConstraint(CreateIndex {
            label: "MENTIONS".to_string(),
            property: "weight".to_string(),
        })
    );
    assert_eq!(
        parse("ALTER NODE TABLE Memory SET STATE WRITE_ONLY").unwrap(),
        Statement::AlterTableState(AlterTableState {
            table_kind: SchemaTableKind::Node,
            table: "Memory".to_string(),
            state: SchemaObjectState::WriteOnly,
        })
    );
    assert_eq!(
        parse("ALTER RELATIONSHIP TABLE MENTIONS SET STATE BACKFILL").unwrap(),
        Statement::AlterTableState(AlterTableState {
            table_kind: SchemaTableKind::Relationship,
            table: "MENTIONS".to_string(),
            state: SchemaObjectState::Backfill,
        })
    );
    assert_eq!(
        parse("ALTER PROPERTY ON NODE TABLE Memory(id) SET STATE VALIDATING").unwrap(),
        Statement::AlterPropertyState(AlterPropertyState {
            table_kind: SchemaTableKind::Node,
            table: "Memory".to_string(),
            property: "id".to_string(),
            state: SchemaObjectState::Validating,
        })
    );
}

#[test]
fn parses_graph_algorithm_calls() {
    assert_eq!(
        parse("CALL project_graph('EntityGraph', ['Entity'], ['RELATES_TO'])").unwrap(),
        Statement::ProjectGraph(ProjectGraph {
            name: "EntityGraph".to_string(),
            node_labels: vec!["Entity".to_string()],
            rel_types: vec!["RELATES_TO".to_string()],
        })
    );
    assert_eq!(
        parse(
            "CALL page_rank('EntityGraph', dampingFactor := 0.85, maxIterations := 20) RETURN node, pagerank_score"
        )
        .unwrap(),
        Statement::GraphAlgorithm(GraphAlgorithm {
            algorithm: GraphAlgorithmKind::PageRank,
            graph_name: "EntityGraph".to_string(),
            options: GraphAlgorithmOptions {
                damping: Some(ValueExpression::Literal(Value::Float(0.85))),
                max_iterations: Some(ValueExpression::Literal(Value::Int(20))),
                max_levels: None,
            },
            score_column: "pagerank_score".to_string(),
        })
    );
    assert_eq!(
        parse(
            "CALL PROJECT_GRAPH('UnifiedGraph', ['Entity', 'Memory'], { 'RELATES_TO': '', 'MENTIONS': '', 'MEMORY_RELATES_TO': \"r.status = 'active'\" })"
        )
        .unwrap(),
        Statement::ProjectGraph(ProjectGraph {
            name: "UnifiedGraph".to_string(),
            node_labels: vec!["Entity".to_string(), "Memory".to_string()],
            rel_types: vec![
                "RELATES_TO".to_string(),
                "MENTIONS".to_string(),
                "MEMORY_RELATES_TO".to_string(),
            ],
        })
    );
    assert_eq!(
        parse(
            "CALL page_rank('UnifiedGraph', dampingFactor := 0.85, maxIterations := 20, tolerance := 0.0000001, normalizeInitial := true) RETURN node, rank"
        )
        .unwrap(),
        Statement::GraphAlgorithm(GraphAlgorithm {
            algorithm: GraphAlgorithmKind::PageRank,
            graph_name: "UnifiedGraph".to_string(),
            options: GraphAlgorithmOptions {
                damping: Some(ValueExpression::Literal(Value::Float(0.85))),
                max_iterations: Some(ValueExpression::Literal(Value::Int(20))),
                max_levels: None,
            },
            score_column: "rank".to_string(),
        })
    );
    assert_eq!(
        parse("CALL louvain('EntityGraph', maxLevels := 2) RETURN node, level, louvain_id")
            .unwrap(),
        Statement::GraphAlgorithm(GraphAlgorithm {
            algorithm: GraphAlgorithmKind::Louvain,
            graph_name: "EntityGraph".to_string(),
            options: GraphAlgorithmOptions {
                damping: None,
                max_iterations: None,
                max_levels: Some(ValueExpression::Literal(Value::Int(2))),
            },
            score_column: "louvain_id".to_string(),
        })
    );
    assert_eq!(
        parse("CALL page_rank('EntityGraph', dampingFactor := $damping, maxIterations := $iterations) RETURN node, pagerank_score")
            .unwrap(),
        Statement::GraphAlgorithm(GraphAlgorithm {
            algorithm: GraphAlgorithmKind::PageRank,
            graph_name: "EntityGraph".to_string(),
            options: GraphAlgorithmOptions {
                damping: Some(ValueExpression::Parameter("damping".to_string())),
                max_iterations: Some(ValueExpression::Parameter("iterations".to_string())),
                max_levels: None,
            },
            score_column: "pagerank_score".to_string(),
        })
    );
}

#[test]
fn parses_parameterized_vector_search() {
    assert_eq!(
        parse("CALL vector_search($embedding, topK := 20) RETURN id, score").unwrap(),
        Statement::VectorSearch(VectorSearch {
            embedding: ValueExpression::Parameter("embedding".to_string()),
            top_k: Some(ValueExpression::Literal(Value::Int(20))),
        })
    );
}

#[test]
fn parses_vector_search_feeding_graph_match() {
    let statement = parse(
        "CALL vector_search($embedding, topK := 20) YIELD id, score \
         MATCH (m:Memory) WHERE m.space_id = $space_id \
         RETURN m.id AS memory_id, score",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected vector-seeded MATCH");
    };
    let search = query.vector_seed.expect("vector seed");
    assert_eq!(
        search.embedding,
        ValueExpression::Parameter("embedding".to_string())
    );
    assert_eq!(search.top_k, Some(ValueExpression::Literal(Value::Int(20))));
    assert_eq!(query.variable, "m");
    assert_eq!(query.label, "Memory");
    assert_eq!(query.returns.len(), 2);
}

#[test]
fn parses_merge_node() {
    let statement = parse("MERGE (:Memory {id: $id})").unwrap();
    let Statement::MergeNode(node) = statement else {
        panic!("expected merge node");
    };
    assert_eq!(node.variable, None);
    assert_eq!(node.label, "Memory");
    assert_eq!(node.properties.len(), 1);
    assert!(node.on_create_sets.is_empty());
    assert!(node.on_match_sets.is_empty());
}

#[test]
fn parses_merge_node_on_create_set() {
    let statement = parse(
        "MERGE (m:SchemaMigrationLog {id: $id}) ON CREATE SET m.applied_at = CURRENT_TIMESTAMP(), m.version = 1",
    )
    .unwrap();
    let Statement::MergeNode(node) = statement else {
        panic!("expected merge node");
    };
    assert_eq!(node.variable.as_deref(), Some("m"));
    assert_eq!(node.label, "SchemaMigrationLog");
    assert_eq!(node.properties.len(), 1);
    assert_eq!(node.on_create_sets.len(), 2);
    assert_eq!(node.on_create_sets[0].property, "applied_at");
    assert!(node.on_match_sets.is_empty());
}

#[test]
fn parses_merge_node_on_match_set_with_coalesce() {
    let statement = parse(
        "MERGE (l:Label {id: $label_id}) ON CREATE SET l.name = $name, l.canonical_name = $canonical ON MATCH SET l.updated_at = $now, l.canonical_name = COALESCE(l.canonical_name, $canonical)",
    )
    .unwrap();
    let Statement::MergeNode(node) = statement else {
        panic!("expected merge node");
    };
    assert_eq!(node.variable.as_deref(), Some("l"));
    assert_eq!(node.on_create_sets.len(), 2);
    assert_eq!(node.on_match_sets.len(), 2);
    assert_eq!(node.on_match_sets[0].property, "updated_at");
    assert_eq!(node.on_match_sets[1].property, "canonical_name");
    assert!(matches!(
        node.on_match_sets[1].value,
        SetValueExpression::CoalesceProperty { .. }
    ));
}

#[test]
fn parses_merge_node_post_merge_set() {
    let statement = parse(
        "MERGE (m:GraphMeta {meta_id: 'main'}) SET m.pagerank_applied = true, m.updated_at = CURRENT_TIMESTAMP()",
    )
    .unwrap();
    let Statement::MergeNode(node) = statement else {
        panic!("expected merge node");
    };
    assert_eq!(node.variable.as_deref(), Some("m"));
    assert_eq!(node.label, "GraphMeta");
    assert!(node.on_create_sets.is_empty());
    assert!(node.on_match_sets.is_empty());
    assert_eq!(node.post_merge_sets.len(), 2);
    assert_eq!(node.post_merge_sets[0].property, "pagerank_applied");
}

#[test]
fn parses_merge_relationship() {
    let statement =
        parse("MERGE (:Memory {id: $id})-[:MENTIONS {weight: 3}]->(:Entity {name: 'Rust'})")
            .unwrap();
    let Statement::MergeRelationship(relationship) = statement else {
        panic!("expected merge relationship");
    };
    assert_eq!(relationship.source.label, "Memory");
    assert_eq!(relationship.rel_type, "MENTIONS");
    assert_eq!(relationship.target.label, "Entity");
}

#[test]
fn parses_match_set() {
    let statement = parse("MATCH (m:Memory) WHERE m.id = $id SET m.title = 'updated'").unwrap();
    let Statement::MatchSet(update) = statement else {
        panic!("expected match set");
    };
    assert_eq!(update.variable, "m");
    assert!(update.expand.is_none());
    assert_eq!(update.sets[0].property, "title");
}

#[test]
fn parses_match_set_return() {
    let statement = parse(
        "MATCH (t:Thread) WHERE t.thread_id IN $thread_ids SET t.space_id = $target_space_id, t.updated_at = $updated_at RETURN t.thread_id",
    )
    .unwrap();
    let Statement::MatchSetReturn(update_return) = statement else {
        panic!("expected match set return");
    };
    assert_eq!(update_return.update.variable, "t");
    assert_eq!(update_return.update.sets.len(), 2);
    assert_eq!(update_return.returns.len(), 1);
    assert_eq!(update_return.returns[0].alias, None);
}

#[test]
fn parses_property_increment_set() {
    let statement =
        parse("MATCH (s:Source {id: $id}) SET s.memory_count = s.memory_count + 1").unwrap();
    let Statement::MatchSet(update) = statement else {
        panic!("expected match set");
    };
    assert_eq!(update.sets[0].variable, "s");
    assert_eq!(update.sets[0].property, "memory_count");
    assert_eq!(
        update.sets[0].value,
        SetValueExpression::PropertyAdd {
            variable: "s".to_string(),
            property: "memory_count".to_string(),
            value: ValueExpression::Literal(Value::Int(1)),
        }
    );
}

#[test]
fn parses_case_decrement_floor_zero_set() {
    let statement = parse(
        "MATCH (s:Source {id: $id}) SET s.memory_count = CASE WHEN s.memory_count > 0 THEN s.memory_count - 1 ELSE 0 END",
    )
    .unwrap();
    let Statement::MatchSet(update) = statement else {
        panic!("expected match set");
    };
    assert_eq!(update.sets[0].variable, "s");
    assert_eq!(update.sets[0].property, "memory_count");
    assert_eq!(
        update.sets[0].value,
        SetValueExpression::DecrementFloorZero {
            variable: "s".to_string(),
            property: "memory_count".to_string(),
        }
    );
}

#[test]
fn parses_coalesce_property_increment_set() {
    let statement = parse(
        "MATCH (m:Memory) WHERE m.id = $id
         SET m.access_count = COALESCE(m.access_count, 0) + 1,
             m.last_accessed_at = $now",
    )
    .unwrap();
    let Statement::MatchSet(update) = statement else {
        panic!("expected match set");
    };
    assert_eq!(update.sets.len(), 2);
    assert_eq!(update.sets[0].property, "access_count");
    assert_eq!(
        update.sets[0].value,
        SetValueExpression::CoalescePropertyAdd {
            variable: "m".to_string(),
            property: "access_count".to_string(),
            default: ValueExpression::Literal(Value::Int(0)),
            value: ValueExpression::Literal(Value::Int(1)),
        }
    );
    assert_eq!(update.sets[1].property, "last_accessed_at");
}

#[test]
fn parses_case_preserve_newer_existing_set() {
    let statement = parse(
        "MATCH (t:Thread {id: $thread_uuid}) SET t.message_count = $message_count, t.updated_at = CASE WHEN $updated_at IS NULL THEN t.updated_at WHEN $preserve_newer_existing_updated_at = true AND t.updated_at IS NOT NULL AND t.updated_at > $updated_at THEN t.updated_at ELSE $updated_at END",
    )
    .unwrap();
    let Statement::MatchSet(update) = statement else {
        panic!("expected match set");
    };
    assert_eq!(update.sets.len(), 2);
    assert_eq!(update.sets[1].property, "updated_at");
    assert_eq!(
        update.sets[1].value,
        SetValueExpression::PreserveNewerExisting {
            variable: "t".to_string(),
            property: "updated_at".to_string(),
            incoming: ValueExpression::Parameter("updated_at".to_string()),
            preserve: ValueExpression::Parameter("preserve_newer_existing_updated_at".to_string()),
        }
    );
}

#[test]
fn parses_relationship_variable_set() {
    let statement =
        parse("MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE m.id = 1 SET r.weight = 2").unwrap();
    let Statement::MatchSet(update) = statement else {
        panic!("expected match set");
    };
    assert_eq!(update.variable, "m");
    let expand = update.expand.as_ref().expect("relationship expand");
    assert_eq!(expand.variable.as_deref(), Some("r"));
    assert_eq!(expand.rel_type, "MENTIONS");
    assert_eq!(expand.target_variable, "e");
    assert_eq!(update.sets[0].variable, "r");
    assert_eq!(update.sets[0].property, "weight");
}

#[test]
fn parses_relationship_pattern_properties() {
    let statement = parse(
        "MATCH (m:Memory)-[r:MENTIONS {weight: $weight, kind: 'primary'}]->(e:Entity) RETURN e.name AS entity",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let expand = query.expand.unwrap();
    assert_eq!(expand.variable.as_deref(), Some("r"));
    assert_eq!(expand.rel_type, "MENTIONS");
    assert_eq!(expand.properties.len(), 2);
    assert_eq!(
        expand.properties.get("weight"),
        Some(&ValueExpression::Parameter("weight".to_string()))
    );
    assert_eq!(
        expand.properties.get("kind"),
        Some(&ValueExpression::Literal(Value::String(
            "primary".to_string()
        )))
    );
}

#[test]
fn parses_match_delete() {
    let statement = parse("MATCH (m:Memory) WHERE m.id = 1 DELETE m").unwrap();
    let Statement::MatchDelete(delete) = statement else {
        panic!("expected match delete");
    };
    assert_eq!(delete.variable, "m");
    assert_eq!(delete.delete_variable, "m");
    assert!(!delete.detach);
}

#[test]
fn parses_match_detach_delete() {
    let statement = parse("MATCH (m:Memory) WHERE m.id = 1 DETACH DELETE m").unwrap();
    let Statement::MatchDelete(delete) = statement else {
        panic!("expected match delete");
    };
    assert_eq!(delete.variable, "m");
    assert!(delete.detach);
}

#[test]
fn parses_match_detach_delete_after_relationship_match() {
    let statement =
        parse("MATCH (t:Thread {id: $thread_uuid})-[:CONTAINS]->(m:Message) DETACH DELETE m")
            .unwrap();
    let Statement::MatchDelete(delete) = statement else {
        panic!("expected match delete");
    };
    assert_eq!(delete.variable, "t");
    assert_eq!(delete.delete_variable, "m");
    assert!(delete.detach);
    let expand = delete.expand.expect("expected relationship expand");
    assert_eq!(expand.rel_type, "CONTAINS");
    assert_eq!(expand.target_variable, "m");
    assert_eq!(expand.target_label, "Message");
}

#[test]
fn parses_match_return() {
    let statement = parse("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title").unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert_eq!(query.variable, "m");
    assert_eq!(query.label, "Memory");
    assert!(query.properties.is_empty());
    assert!(!query.distinct);
    assert_eq!(query.returns[0].alias.as_deref(), Some("title"));
    assert_eq!(
        query.returns[0].expression,
        ReturnExpression::Property {
            variable: "m".to_string(),
            property: "title".to_string()
        }
    );
}

#[test]
fn parses_match_node_property_patterns() {
    let statement = parse(
        "MATCH (m:Memory {is_crystal: true})-[:SYNTHESIZED_FROM]->(s:Memory {kind: 'note'}) RETURN DISTINCT s.id",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert_eq!(
        query.properties.get("is_crystal"),
        Some(&ValueExpression::Literal(Value::Bool(true)))
    );
    let expand = query.expand.as_ref().expect("expected relationship expand");
    assert_eq!(
        expand.target_properties.get("kind"),
        Some(&ValueExpression::Literal(Value::String("note".to_string())))
    );
    assert!(query.distinct);
}

#[test]
fn parses_distinct_return() {
    let statement = parse("MATCH (m:Memory) RETURN DISTINCT m.kind AS kind").unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert!(query.distinct);
    assert_eq!(query.returns[0].alias.as_deref(), Some("kind"));
    assert_eq!(
        query.returns[0].expression,
        ReturnExpression::Property {
            variable: "m".to_string(),
            property: "kind".to_string()
        }
    );
}

#[test]
fn parses_id_return_items() {
    let statement = parse(
        "MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) RETURN id(m) AS memory_id, id(r) AS rel_id",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert_eq!(
        query.returns[0].expression,
        ReturnExpression::Id("m".to_string())
    );
    assert_eq!(query.returns[0].alias.as_deref(), Some("memory_id"));
    assert_eq!(
        query.returns[1].expression,
        ReturnExpression::Id("r".to_string())
    );
    assert_eq!(query.returns[1].alias.as_deref(), Some("rel_id"));

    let statement = parse(
        "MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE id(r) IN [0, $rel_id] RETURN e.name AS entity ORDER BY id(r) DESC",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert_eq!(
        query.predicate.unwrap(),
        PropertyPredicate::IdIn {
            variable: "r".to_string(),
            values: ValueExpression::List(vec![
                ValueExpression::Literal(Value::Int(0)),
                ValueExpression::Parameter("rel_id".to_string()),
            ]),
        }
    );
    assert_eq!(
        query.order_by[0].expression,
        OrderExpression::Id {
            variable: "r".to_string()
        }
    );
}

#[test]
fn parses_bounded_relationship_match() {
    let statement =
        parse("MATCH (m:Memory)-[:MENTIONS*1..3]->(e:Entity) RETURN e.name AS name").unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let expand = query.expand.unwrap();
    assert_eq!(expand.rel_type, "MENTIONS");
    assert_eq!(expand.min_hops, 1);
    assert_eq!(expand.max_hops, 3);
}

#[test]
fn parses_bounded_relationship_match_with_unused_path_binding() {
    let statement = parse(
        "MATCH p = (s:Source {id: $source_id})-[:REVISED_AS*1..10]->(older:Source) RETURN older.id AS id ORDER BY older.version DESC",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert_eq!(query.variable, "s");
    assert_eq!(query.label, "Source");
    let expand = query.expand.unwrap();
    assert_eq!(expand.rel_type, "REVISED_AS");
    assert_eq!(expand.min_hops, 1);
    assert_eq!(expand.max_hops, 10);
    assert_eq!(expand.target_variable, "older");
}

#[test]
fn parses_all_shortest_path_return() {
    let statement = parse(
        "MATCH p = (a)-[e* ALL SHORTEST 1..3]-(b) WHERE a.id = $from_id AND b.id = $to_id RETURN properties(nodes(p), 'id') AS node_ids, properties(nodes(p), 'name') AS names, length(p) AS hops",
    )
    .unwrap();
    let Statement::ShortestPathReturn(query) = statement else {
        panic!("expected shortest path return");
    };
    assert_eq!(query.path_variable, "p");
    assert_eq!(query.source_variable, "a");
    assert_eq!(query.target_variable, "b");
    assert_eq!(query.min_hops, 1);
    assert_eq!(query.max_hops, 3);
    assert_eq!(query.returns.len(), 3);
}

#[test]
fn parses_relationship_variable_delete() {
    let statement = parse("MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) DELETE r").unwrap();
    let Statement::MatchDelete(delete) = statement else {
        panic!("expected match delete");
    };
    let expand = delete.expand.unwrap();
    assert_eq!(delete.variable, "m");
    assert_eq!(delete.delete_variable, "r");
    assert_eq!(expand.variable.as_deref(), Some("r"));
    assert_eq!(expand.rel_type, "MENTIONS");
    assert_eq!(expand.target_variable, "e");
    assert_eq!(expand.target_label, "Entity");
}

#[test]
fn parses_untyped_relationship_match_and_label_return() {
    let statement =
        parse("MATCH (a)-[r]->(b) RETURN a.id AS source, b.id AS target, label(r) AS rel_type")
            .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let expand = query.expand.as_ref().expect("expected relationship expand");
    assert_eq!(query.variable, "a");
    assert_eq!(query.label, "");
    assert_eq!(expand.variable.as_deref(), Some("r"));
    assert_eq!(expand.rel_type, "");
    assert_eq!(expand.target_variable, "b");
    assert_eq!(expand.target_label, "");
    assert_eq!(
        query.returns[2].expression,
        ReturnExpression::RelationshipType("r".to_string())
    );
}

#[test]
fn parses_undirected_relationship_match() {
    let statement =
        parse("MATCH (m:Memory)-[:EVOLVES]-(other:Memory) RETURN other.id AS id").unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let expand = query.expand.as_ref().expect("expected relationship expand");
    assert_eq!(expand.rel_type, "EVOLVES");
    assert_eq!(expand.direction, RelationshipDirection::Undirected);
    assert_eq!(expand.min_hops, 1);
    assert_eq!(expand.max_hops, 1);
}

#[test]
fn parses_incoming_relationship_match() {
    let statement = parse("MATCH (e:Entity)<-[:MENTIONS]-(m:Memory) RETURN m.id AS id").unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let expand = query.expand.as_ref().expect("expected relationship expand");
    assert_eq!(query.variable, "e");
    assert_eq!(query.label, "Entity");
    assert_eq!(expand.rel_type, "MENTIONS");
    assert_eq!(expand.direction, RelationshipDirection::Incoming);
    assert_eq!(expand.target_variable, "m");
    assert_eq!(expand.target_label, "Memory");
    assert_eq!(expand.min_hops, 1);
    assert_eq!(expand.max_hops, 1);
}

#[test]
fn parses_anonymous_relationship_endpoints() {
    let statement = parse("MATCH (:Memory)-[r:MENTIONS]->() RETURN count(r) AS total").unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert!(query.variable.starts_with("__anon"));
    assert_eq!(query.label, "Memory");
    let expand = query.expand.as_ref().expect("expected relationship expand");
    assert_eq!(expand.variable.as_deref(), Some("r"));
    assert!(expand.target_variable.starts_with("__anon"));
    assert_eq!(expand.target_label, "");
    assert_eq!(
        query.returns[0].expression,
        ReturnExpression::CountVariable {
            variable: "r".to_string(),
            distinct: false
        }
    );
}

#[test]
fn rejects_unbounded_relationship_match() {
    let error =
        parse("MATCH (m:Memory)-[:MENTIONS*]->(e:Entity) RETURN e.name AS name").unwrap_err();
    assert!(error.to_string().contains("finite max hop"));
}

#[test]
fn parses_matched_relationship_create() {
    let statement = parse(
        "MATCH (m:Memory {id: $memory_id}), (s:Source {id: $source_id}) CREATE (m)-[:SOURCED_FROM {chunk_index: $chunk_index}]->(s)",
    )
    .unwrap();
    let Statement::MatchCreateRelationship(create) = statement else {
        panic!("expected matched relationship create");
    };
    assert_eq!(create.source_variable, "m");
    assert_eq!(create.source_label, "Memory");
    assert_eq!(create.target_variable, "s");
    assert_eq!(create.target_label, "Source");
    assert_eq!(create.create_source_variable, "m");
    assert_eq!(create.create_target_variable, "s");
    assert_eq!(create.rel_type, "SOURCED_FROM");
    assert_eq!(
        create.rel_properties.get("chunk_index"),
        Some(&ValueExpression::Parameter("chunk_index".to_string()))
    );
}

#[test]
fn parses_matched_relationship_create_with_relationship_variable() {
    let statement = parse(
        "MATCH (source:Memory {id: $source_memory_id}), (target:Memory {id: $target_memory_id}) CREATE (source)-[r:MEMORY_RELATES_TO {id: $relation_id}]->(target)",
    )
    .unwrap();
    let Statement::MatchCreateRelationship(create) = statement else {
        panic!("expected matched relationship create");
    };
    assert_eq!(create.create_source_variable, "source");
    assert_eq!(create.create_target_variable, "target");
    assert_eq!(create.rel_type, "MEMORY_RELATES_TO");
    assert_eq!(
        create.rel_properties.get("id"),
        Some(&ValueExpression::Parameter("relation_id".to_string()))
    );
}

#[test]
fn parses_matched_relationship_create_with_endpoint_where() {
    let statement = parse(
        "MATCH (a:Memory), (b:Memory) WHERE a.id = $older_id AND b.id = $newer_id CREATE (a)-[:EVOLVES {content_relation: 'replaces'}]->(b)",
    )
    .unwrap();
    let Statement::MatchCreateRelationship(create) = statement else {
        panic!("expected matched relationship create");
    };
    assert_eq!(create.source_variable, "a");
    assert_eq!(create.source_label, "Memory");
    assert_eq!(create.target_variable, "b");
    assert_eq!(create.target_label, "Memory");
    assert!(matches!(create.predicate, Some(PropertyPredicate::And(_))));
    assert_eq!(create.create_source_variable, "a");
    assert_eq!(create.create_target_variable, "b");
    assert_eq!(create.rel_type, "EVOLVES");
}

#[test]
fn parses_matched_relationship_merge_on_create_set() {
    let statement = parse(
        "MATCH (m:Memory {id: $memory_id}), (l:Label {id: $label_id}) MERGE (m)-[r:HAS_LABEL]->(l) ON CREATE SET r.assigned_by = $assigned_by, r.properties = '{}'",
    )
    .unwrap();
    let Statement::MatchMergeRelationship(merge) = statement else {
        panic!("expected matched relationship merge");
    };
    assert_eq!(merge.source_variable, "m");
    assert_eq!(merge.source_label, "Memory");
    assert_eq!(merge.target_variable, "l");
    assert_eq!(merge.target_label, "Label");
    assert_eq!(merge.merge_source_variable, "m");
    assert_eq!(merge.merge_target_variable, "l");
    assert_eq!(merge.rel_variable.as_deref(), Some("r"));
    assert_eq!(merge.rel_type, "HAS_LABEL");
    assert_eq!(merge.on_create_sets.len(), 2);
}

#[test]
fn parses_matched_relationship_copy_merge_on_create_set() {
    let statement = parse(
        "MATCH (c:Memory)-[r:CRYSTALLIZED_FROM]->(s:Memory) MERGE (c)-[n:SYNTHESIZED_FROM]->(s) ON CREATE SET n.weight = r.contribution_weight, n.occasion_key = '', n.created_at = r.created_at",
    )
    .unwrap();
    let Statement::MatchExpandMergeRelationship(merge) = statement else {
        panic!("expected match expand merge relationship");
    };
    assert_eq!(merge.source_variable, "c");
    assert_eq!(merge.expand.variable.as_deref(), Some("r"));
    assert_eq!(merge.expand.rel_type, "CRYSTALLIZED_FROM");
    assert_eq!(merge.expand.target_variable, "s");
    assert_eq!(merge.rel_variable.as_deref(), Some("n"));
    assert_eq!(merge.rel_type, "SYNTHESIZED_FROM");
    assert_eq!(
        merge.on_create_sets[0].value,
        SetValueExpression::Property {
            variable: "r".to_string(),
            property: "contribution_weight".to_string()
        }
    );
}

#[test]
fn parses_two_node_match_return() {
    let statement =
        parse("MATCH (m:Memory {id: $memory_id}), (s:Source {id: $source_id}) RETURN count(m)")
            .unwrap();
    let Statement::MatchNodesReturn(query) = statement else {
        panic!("expected two-node match return");
    };
    assert_eq!(query.left_variable, "m");
    assert_eq!(query.left_label, "Memory");
    assert_eq!(query.right_variable, "s");
    assert_eq!(query.right_label, "Source");
    assert_eq!(
        query.returns[0].expression,
        ReturnExpression::CountVariable {
            variable: "m".to_string(),
            distinct: false
        }
    );
}

#[test]
fn parses_consecutive_two_node_match_return() {
    let statement = parse(
        "MATCH (source:Entity {id: $source_entity_id}) MATCH (target:Entity {id: $target_entity_id}) RETURN source.id, target.id",
    )
    .unwrap();
    let Statement::MatchNodesReturn(query) = statement else {
        panic!("expected two-node match return");
    };
    assert_eq!(query.left_variable, "source");
    assert_eq!(query.left_label, "Entity");
    assert_eq!(query.right_variable, "target");
    assert_eq!(query.right_label, "Entity");
    assert_eq!(query.returns.len(), 2);
}

#[test]
fn parses_consecutive_relationship_match_return() {
    let statement = parse(
        "MATCH (c:Memory {is_crystal: true})-[:SYNTHESIZED_FROM]->(src:Memory) MATCH (src)-[:EVOLVES]-(newer:Memory) WHERE newer.created_at > c.created_at AND c.review_status <> 'dismissed' RETURN c.id, newer.id ORDER BY newer.created_at DESC LIMIT 10",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert_eq!(query.variable, "c");
    assert!(query.expand.is_some());
    let post_match = query.post_match_expand.expect("post-match expand");
    assert_eq!(post_match.source_variable, "src");
    assert_eq!(post_match.expand.rel_type, "EVOLVES");
    assert_eq!(post_match.expand.target_variable, "newer");
    assert_eq!(
        post_match.expand.direction,
        RelationshipDirection::Undirected
    );
    assert_eq!(query.returns.len(), 2);
}

#[test]
fn parses_inline_two_hop_relationship_match_return() {
    let statement = parse(
        "MATCH (m:Memory)-[:SYNTHESIZED_FROM]->(src:Memory)-[:MENTIONS]->(e:Entity) WHERE m.is_crystal = true AND e.community_id IS NOT NULL RETURN m.id, e.community_id",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let expand = query.expand.expect("first expand");
    assert_eq!(expand.rel_type, "SYNTHESIZED_FROM");
    assert_eq!(expand.target_variable, "src");
    let post_match = query.post_match_expand.expect("post-match expand");
    assert_eq!(post_match.source_variable, "src");
    assert_eq!(post_match.expand.rel_type, "MENTIONS");
    assert_eq!(post_match.expand.target_variable, "e");
    assert_eq!(post_match.expand.direction, RelationshipDirection::Outgoing);
    assert_eq!(query.returns.len(), 2);
}

#[test]
fn parses_optional_match_count_after_node_match() {
    let statement = parse(
        "MATCH (t:Thread {id: $thread_uuid}) OPTIONAL MATCH (t)-[:CONTAINS]->(m:Message) RETURN COUNT(m)",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let optional = query.optional_expand.expect("optional expand");
    assert_eq!(optional.source_variable, "t");
    assert_eq!(optional.expand.target_variable, "m");
    assert_eq!(optional.expand.rel_type, "CONTAINS");
    assert_eq!(optional.expand.direction, RelationshipDirection::Outgoing);
}

#[test]
fn parses_optional_match_count_after_relationship_match() {
    let statement = parse(
        "MATCH (t:Thread {id: $thread_uuid})-[:CONTAINS]->(m:Message) WHERE m.order_index >= $start_index OPTIONAL MATCH (:Memory)-[r:EXTRACTED_FROM]->(m) RETURN COUNT(r)",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let optional = query.optional_expand.expect("optional expand");
    assert_eq!(optional.source_variable, "m");
    assert_eq!(optional.expand.variable.as_deref(), Some("r"));
    assert_eq!(optional.expand.target_label, "Memory");
    assert_eq!(optional.expand.direction, RelationshipDirection::Incoming);
}

#[test]
fn parses_optional_match_with_degree_projection() {
    let statement = parse(
        "MATCH (e:Entity) OPTIONAL MATCH (e)-[r]-() WITH e, COUNT(r) as degree RETURN e.id, e.name, degree ORDER BY degree DESC LIMIT 10",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let optional = query.optional_expand.expect("optional expand");
    assert_eq!(optional.source_variable, "e");
    assert_eq!(optional.expand.variable.as_deref(), Some("r"));
    assert_eq!(optional.expand.direction, RelationshipDirection::Undirected);
    let optional_with = query.optional_with.expect("optional with");
    assert_eq!(optional_with.group_variable, "e");
    assert_eq!(optional_with.count_variable, "r");
    assert!(!optional_with.distinct);
    assert_eq!(optional_with.alias, "degree");
    assert_eq!(query.returns.len(), 3);
    assert_eq!(query.order_by.len(), 1);
}

#[test]
fn parses_optional_match_with_target_count_projection() {
    let statement = parse(
        "MATCH (l:Label) OPTIONAL MATCH (l)<-[:HAS_LABEL]-(n) WITH l, COUNT(n) as usage_count RETURN l.id, usage_count ORDER BY usage_count DESC",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let optional = query.optional_expand.expect("optional expand");
    assert_eq!(optional.source_variable, "l");
    assert_eq!(optional.expand.target_variable, "n");
    assert_eq!(optional.expand.rel_type, "HAS_LABEL");
    assert_eq!(optional.expand.direction, RelationshipDirection::Incoming);
    let optional_with = query.optional_with.expect("optional with");
    assert_eq!(optional_with.group_variable, "l");
    assert_eq!(optional_with.count_variable, "n");
    assert!(!optional_with.distinct);
    assert_eq!(optional_with.alias, "usage_count");
    assert_eq!(query.returns.len(), 2);
    assert_eq!(query.order_by.len(), 1);
}

#[test]
fn parses_relationship_match_with_group_count_projection() {
    let statement = parse(
        "MATCH (e:Entity {community_id: $louvain_id})<-[:MENTIONS]-(m:Memory) WHERE m.is_crystal = false WITH m, COUNT(e) AS entity_count RETURN m.id, m.title, COALESCE(m.is_latest, true), entity_count ORDER BY entity_count DESC, m.importance DESC",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert_eq!(query.variable, "e");
    let expand = query.expand.expect("relationship expand");
    assert_eq!(expand.target_variable, "m");
    assert_eq!(expand.direction, RelationshipDirection::Incoming);
    let with = query.optional_with.expect("with count");
    assert_eq!(with.group_variable, "m");
    assert_eq!(with.count_variable, "e");
    assert!(!with.distinct);
    assert_eq!(with.alias, "entity_count");
    assert_eq!(query.returns.len(), 4);
    assert_eq!(query.order_by.len(), 2);
}

#[test]
fn parses_relationship_match_with_distinct_group_count_projection() {
    let statement = parse(
        "MATCH (m:Memory)-[:HAS_LABEL]->(l:Label) WITH l, COUNT(DISTINCT m) AS memory_count RETURN l.name, memory_count ORDER BY memory_count DESC, l.name ASC SKIP $offset LIMIT $limit",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let with = query.optional_with.expect("with count");
    assert_eq!(with.group_variable, "l");
    assert_eq!(with.count_variable, "m");
    assert!(with.distinct);
    assert_eq!(with.alias, "memory_count");
    assert_eq!(query.returns.len(), 2);
    assert_eq!(query.order_by.len(), 2);
}

#[test]
fn parses_group_count_order_limit_before_return() {
    let statement = parse(
        "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) WHERE e.id IN $ids WITH m, COUNT(DISTINCT e) AS mention_breadth ORDER BY mention_breadth DESC, COALESCE(m.importance, 0.5) DESC LIMIT $top_n RETURN m.id, m.title, mention_breadth",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let with = query.optional_with.expect("with count");
    assert_eq!(with.group_variable, "m");
    assert_eq!(with.count_variable, "e");
    assert!(with.distinct);
    assert_eq!(with.alias, "mention_breadth");
    assert_eq!(query.order_by.len(), 2);
    assert_eq!(
        query.limit,
        Some(ValueExpression::Parameter("top_n".to_string()))
    );
    assert_eq!(query.returns.len(), 3);
}

#[test]
fn parses_with_variable_group_multiple_count_aggregates() {
    let statement = parse(
        "MATCH (e1:Entity)-[:RELATES_TO]-(e2:Entity) WHERE e1.community_id IN $cids AND e2.community_id IN $cids AND e1.community_id <> e2.community_id WITH e1, COUNT(DISTINCT e2.community_id) AS community_span, COUNT(*) AS bridge_strength RETURN e1.id, community_span, bridge_strength ORDER BY community_span DESC, bridge_strength DESC LIMIT $limit",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert!(query.optional_with.is_none());
    let aggregate_with = query.aggregate_with.expect("aggregate with");
    assert_eq!(aggregate_with.items.len(), 3);
    assert_eq!(
        aggregate_with.items[0].expression,
        ReturnExpression::Variable("e1".to_string())
    );
    assert_eq!(
        aggregate_with.items[1].expression,
        ReturnExpression::CountProperty {
            variable: "e2".to_string(),
            property: "community_id".to_string(),
            distinct: true,
        }
    );
    assert_eq!(
        aggregate_with.items[1].alias.as_deref(),
        Some("community_span")
    );
    assert_eq!(
        aggregate_with.items[2].expression,
        ReturnExpression::CountAll
    );
    assert_eq!(
        aggregate_with.items[2].alias.as_deref(),
        Some("bridge_strength")
    );
    assert_eq!(query.order_by.len(), 2);
    assert_eq!(
        query.limit,
        Some(ValueExpression::Parameter("limit".to_string()))
    );
}

#[test]
fn parses_with_variable_group_multiple_count_aggregates_and_filter() {
    let statement = parse(
        "MATCH (e1:Entity)-[:RELATES_TO]-(e2:Entity) WHERE e1.community_id IS NOT NULL AND e2.community_id IS NOT NULL AND e1.community_id <> e2.community_id WITH e1, COUNT(DISTINCT e2.community_id) AS community_span, COUNT(*) AS bridge_strength WHERE community_span >= 2 RETURN e1.id, e1.name, e1.community_id, community_span, bridge_strength ORDER BY community_span DESC, bridge_strength DESC LIMIT $limit",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let aggregate_with = query.aggregate_with.expect("aggregate with");
    assert_eq!(aggregate_with.items.len(), 3);
    let filter = query.aggregate_with_filter.expect("aggregate filter");
    let WithAliasFilter::Comparison { left, op, right } = filter else {
        panic!("expected comparison filter");
    };
    assert_eq!(
        left,
        WithAliasFilterExpression::Column("community_span".to_string())
    );
    assert_eq!(op, WithAliasFilterOp::Gte);
    assert_eq!(
        right,
        WithAliasFilterExpression::Value(ValueExpression::Literal(Value::Int(2)))
    );
    assert_eq!(query.returns.len(), 5);
}

#[test]
fn parses_optional_count_with_alias_filter() {
    let statement = parse(
        "MATCH (e:Entity) WHERE e.name IS NOT NULL AND e.id IS NOT NULL OPTIONAL MATCH (:Memory)-[r:MENTIONS]->(e) WITH e, COUNT(r) AS mention_count WHERE mention_count < $after_count RETURN e.id, e.name, e.updated_at, mention_count ORDER BY mention_count DESC, e.name ASC LIMIT $limit",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let optional_with = query.optional_with.expect("optional with");
    assert_eq!(optional_with.group_variable, "e");
    assert_eq!(optional_with.count_variable, "r");
    assert_eq!(optional_with.alias, "mention_count");
    let filter = query.aggregate_with_filter.expect("aggregate filter");
    let WithAliasFilter::Comparison { left, op, right } = filter else {
        panic!("expected comparison filter");
    };
    assert_eq!(
        left,
        WithAliasFilterExpression::Column("mention_count".to_string())
    );
    assert_eq!(op, WithAliasFilterOp::Lt);
    assert_eq!(
        right,
        WithAliasFilterExpression::Value(ValueExpression::Parameter("after_count".to_string()))
    );
}

#[test]
fn parses_optional_count_with_keyset_filter() {
    let statement = parse(
        "MATCH (e:Entity) WHERE e.name IS NOT NULL AND e.id IS NOT NULL OPTIONAL MATCH (:Memory)-[r:MENTIONS]->(e) WITH e, COUNT(r) AS mention_count WHERE mention_count < $after_count OR (mention_count = $after_count AND e.name > $after_name) RETURN e.id, e.name, e.updated_at, mention_count ORDER BY mention_count DESC, e.name ASC LIMIT $limit",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let filter = query.aggregate_with_filter.expect("aggregate filter");
    let WithAliasFilter::Or(filters) = filter else {
        panic!("expected disjunction filter");
    };
    assert_eq!(filters.len(), 2);
    let WithAliasFilter::And(tie_breaker) = &filters[1] else {
        panic!("expected tie-breaker conjunction");
    };
    assert_eq!(tie_breaker.len(), 2);
    let WithAliasFilter::Comparison { left, op, right } = &tie_breaker[1] else {
        panic!("expected name comparison");
    };
    assert_eq!(
        left,
        &WithAliasFilterExpression::Property {
            variable: "e".to_string(),
            property: "name".to_string(),
        }
    );
    assert_eq!(*op, WithAliasFilterOp::Gt);
    assert_eq!(
        right,
        &WithAliasFilterExpression::Value(ValueExpression::Parameter("after_name".to_string()))
    );
}

#[test]
fn parses_post_aggregate_community_lookup() {
    let statement = parse(
        "MATCH (e1:Entity)-[:RELATES_TO]-(e2:Entity) WHERE e1.community_id = $cid AND e2.community_id IS NOT NULL AND e2.community_id <> $cid WITH e2.community_id AS other_cid, COUNT(*) AS shared_edge_count ORDER BY shared_edge_count DESC LIMIT $limit MATCH (c:Community) WHERE c.community_id = other_cid RETURN c.community_id, c.name, c.ai_summary, c.description, c.member_count, shared_edge_count",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let aggregate_with = query.aggregate_with.expect("aggregate with");
    assert_eq!(aggregate_with.items.len(), 2);
    assert_eq!(aggregate_with.items[0].alias.as_deref(), Some("other_cid"));
    assert_eq!(
        aggregate_with.items[1].alias.as_deref(),
        Some("shared_edge_count")
    );
    let lookup = query.post_with_match.expect("post with match");
    assert_eq!(lookup.variable, "c");
    assert_eq!(lookup.label, "Community");
    assert_eq!(lookup.property, "community_id");
    assert_eq!(lookup.column, "other_cid");
    assert_eq!(query.returns.len(), 6);
}

#[test]
fn parses_post_aggregate_optional_community_lookup() {
    let statement = parse(
        "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) WHERE m.space_id IN $space_ids AND e.community_id IS NOT NULL WITH e.community_id AS community_id, COUNT(DISTINCT m) AS memory_count OPTIONAL MATCH (c:Community) WHERE c.community_id = community_id RETURN c.name, memory_count, c.description ORDER BY memory_count DESC LIMIT $limit",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let lookup = query.post_with_match.expect("post with match");
    assert_eq!(lookup.variable, "c");
    assert_eq!(lookup.label, "Community");
    assert_eq!(lookup.property, "community_id");
    assert_eq!(lookup.column, "community_id");
    assert!(lookup.optional);
    assert_eq!(query.order_by.len(), 1);
    assert_eq!(query.returns.len(), 3);
}

#[test]
fn parses_case_property_presence_order_item() {
    let statement = parse(
        "MATCH (c:Community) WHERE c.community_id IS NOT NULL AND c.community_id >= 0 RETURN c.community_id, c.name, c.ai_summary ORDER BY CASE WHEN c.ai_summary IS NOT NULL AND c.ai_summary <> '' THEN 0 ELSE 1 END, c.member_count DESC LIMIT $limit",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert_eq!(query.order_by.len(), 2);
    let OrderExpression::Value(ReturnValueExpression::CasePropertyNotNullOrEq {
        variable,
        property,
        ..
    }) = &query.order_by[0].expression
    else {
        panic!("expected CASE order expression");
    };
    assert_eq!(variable, "c");
    assert_eq!(property, "ai_summary");
    assert_eq!(query.order_by[0].direction, OrderDirection::Asc);
    assert_eq!(query.order_by[1].direction, OrderDirection::Desc);
}

#[test]
fn parses_case_property_equals_rank_with_projection() {
    let statement = parse(
        "MATCH (s:Skill) WHERE s.stage IS NULL OR (s.stage <> 'archived' AND s.stage <> 'rejected' AND s.stage <> 'deprecated') WITH s, CASE WHEN s.stage = 'active' THEN 4 WHEN s.stage = 'promotable' THEN 3 WHEN s.stage = 'candidate' THEN 2 WHEN s.stage = 'draft' THEN 1 ELSE 0 END AS stage_rank, COALESCE(s.evidence_count, 0) AS evidence_score ORDER BY stage_rank DESC, evidence_score DESC, s.updated_at DESC LIMIT $limit RETURN s.id, COALESCE(s.name, s.title, 'Skill')",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let with_projection = query
        .with_projection
        .as_ref()
        .expect("expected WITH projection");
    assert_eq!(with_projection.items.len(), 3);
    let ReturnExpression::CasePropertyEqualsRank {
        variable,
        property,
        branches,
        ..
    } = &with_projection.items[1].expression
    else {
        panic!("expected property equality rank expression");
    };
    assert_eq!(variable, "s");
    assert_eq!(property, "stage");
    assert_eq!(branches.len(), 4);
    assert_eq!(
        with_projection.items[1].alias.as_deref(),
        Some("stage_rank")
    );
    assert!(query.order_by.is_empty());
    assert_eq!(query.with_order_by.len(), 3);
    assert_eq!(
        query.with_order_by[0].expression,
        OrderExpression::Column("stage_rank".to_string())
    );
}

#[test]
fn parses_entity_search_rank_order_item() {
    let statement = parse(
        "MATCH (e:Entity) WHERE lower(e.name) CONTAINS $raw_query OR lower(e.name) CONTAINS $normalized_query OR list_contains(e.aliases, $raw_input) OPTIONAL MATCH (m:Memory)-[:MENTIONS]->(e) RETURN e.id, COUNT(m) AS memory_count ORDER BY CASE WHEN lower(e.name) = $raw_query THEN 0 WHEN lower(e.name) = $normalized_query THEN 0 WHEN list_contains(e.aliases, $raw_input) THEN 1 ELSE 2 END ASC, memory_count DESC LIMIT $limit",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert_eq!(query.order_by.len(), 2);
    let OrderExpression::Value(ReturnValueExpression::CaseEntitySearchRank(expression)) =
        &query.order_by[0].expression
    else {
        panic!("expected entity search rank order expression");
    };
    assert_eq!(expression.variable, "e");
    assert_eq!(expression.name_property, "name");
    assert_eq!(expression.aliases_property, "aliases");
    assert_eq!(query.order_by[0].direction, OrderDirection::Asc);
    assert_eq!(query.order_by[1].direction, OrderDirection::Desc);
}

#[test]
fn parses_community_search_projection_with_case_aliases() {
    let statement = parse(
        "MATCH (c:Community) WITH c, CASE WHEN c.name IS NOT NULL THEN lower(c.name) ELSE '' END AS c_name, CASE WHEN c.description IS NOT NULL THEN lower(c.description) ELSE '' END AS c_description, CASE WHEN c.ai_summary IS NOT NULL THEN lower(c.ai_summary) ELSE '' END AS c_summary WHERE c_name CONTAINS $raw_query OR c_description CONTAINS $normalized_query RETURN c.community_id, CASE WHEN c_name = $raw_query THEN 3 WHEN c_name = $normalized_query THEN 3 WHEN c_name CONTAINS $raw_query THEN 2 WHEN c_name CONTAINS $normalized_query THEN 2 ELSE 1 END AS match_level ORDER BY match_level DESC LIMIT $limit",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let with_projection = query
        .with_projection
        .as_ref()
        .expect("expected WITH projection");
    assert_eq!(with_projection.items.len(), 4);
    assert!(query.aggregate_with_filter.is_some());
    let ReturnExpression::CaseColumnSearchRank(expression) = &query.returns[1].expression else {
        panic!("expected column search rank expression");
    };
    assert_eq!(expression.column, "c_name");
    assert_eq!(
        query.order_by[0].expression,
        OrderExpression::Column("match_level".to_string())
    );
}

#[test]
fn parses_source_search_projection_with_coalesce_ordering() {
    let statement = parse(
        "MATCH (s:Source) WITH s, CASE WHEN s.original_name IS NOT NULL THEN lower(s.original_name) ELSE '' END AS s_name, CASE WHEN s.summary IS NOT NULL THEN lower(s.summary) ELSE '' END AS s_summary, CASE WHEN s.file_path IS NOT NULL THEN lower(s.file_path) ELSE '' END AS s_path, CASE WHEN s.source_type IS NOT NULL THEN lower(s.source_type) ELSE '' END AS s_type WHERE s_name CONTAINS $raw_query OR s_summary CONTAINS $normalized_query RETURN s.id, COALESCE(s.original_name, s.file_path, s.source_type, 'Source'), CASE WHEN s_name = $raw_query THEN 3 WHEN s_name = $normalized_query THEN 3 WHEN s_name CONTAINS $raw_query THEN 2 WHEN s_name CONTAINS $normalized_query THEN 2 ELSE 1 END AS match_level ORDER BY match_level DESC, COALESCE(s.memory_count, 0) DESC, COALESCE(s.chunk_count, 0) DESC LIMIT $limit",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let with_projection = query
        .with_projection
        .as_ref()
        .expect("expected WITH projection");
    assert_eq!(with_projection.items.len(), 5);
    assert!(query.aggregate_with_filter.is_some());
    assert!(matches!(
        query.returns[1].expression,
        ReturnExpression::Coalesce(_)
    ));
    assert!(matches!(
        query.order_by[1].expression,
        OrderExpression::Value(ReturnValueExpression::Coalesce(_))
    ));
}

#[test]
fn parses_thread_search_projection_with_coalesce_ordering() {
    let statement = parse(
        "MATCH (t:Thread) WITH t, CASE WHEN t.title IS NOT NULL THEN lower(t.title) ELSE '' END AS t_title, CASE WHEN t.summary IS NOT NULL THEN lower(t.summary) ELSE '' END AS t_summary, CASE WHEN t.source IS NOT NULL THEN lower(t.source) ELSE '' END AS t_source, CASE WHEN t.project IS NOT NULL THEN lower(t.project) ELSE '' END AS t_project, CASE WHEN t.workspace IS NOT NULL THEN lower(t.workspace) ELSE '' END AS t_workspace WHERE t_title CONTAINS $raw_query OR t_workspace CONTAINS $normalized_query RETURN t.id, COALESCE(t.title, t.source, 'Thread'), CASE WHEN t_title = $raw_query THEN 3 WHEN t_title = $normalized_query THEN 3 WHEN t_title CONTAINS $raw_query THEN 2 WHEN t_title CONTAINS $normalized_query THEN 2 ELSE 1 END AS match_level ORDER BY match_level DESC, COALESCE(t.message_count, 0) DESC LIMIT $limit",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let with_projection = query
        .with_projection
        .as_ref()
        .expect("expected WITH projection");
    assert_eq!(with_projection.items.len(), 6);
    assert!(query.aggregate_with_filter.is_some());
    assert!(matches!(
        query.returns[1].expression,
        ReturnExpression::Coalesce(_)
    ));
    assert!(matches!(
        query.order_by[1].expression,
        OrderExpression::Value(ReturnValueExpression::Coalesce(_))
    ));
}

#[test]
fn parses_cleanup_active_consumption_order_expression() {
    let statement = parse(
        "MATCH (m:Memory) RETURN m.id ORDER BY CASE WHEN COALESCE(m.access_count, 0) - COALESCE(m.appearances, 0) - COALESCE(m.clicks, 0) < 0 THEN 0 ELSE COALESCE(m.access_count, 0) - COALESCE(m.appearances, 0) - COALESCE(m.clicks, 0) END ASC",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let OrderExpression::Value(ReturnValueExpression::CaseCoalesceDifferenceFloorZero {
        variable,
        terms,
    }) = &query.order_by[0].expression
    else {
        panic!("expected cleanup active-consumption CASE order expression");
    };
    assert_eq!(variable, "m");
    assert_eq!(
        terms
            .iter()
            .map(|term| term.property.as_str())
            .collect::<Vec<_>>(),
        vec!["access_count", "appearances", "clicks"]
    );
    assert_eq!(query.order_by[0].direction, OrderDirection::Asc);
}

#[test]
fn parses_optional_match_direct_projection_count() {
    let statement = parse(
        "MATCH (l:Label) OPTIONAL MATCH (m:Memory)-[:HAS_LABEL]->(l) RETURN l.id, l.name, COUNT(m) AS usage_count ORDER BY l.name ASC SKIP $offset LIMIT $limit",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let optional = query.optional_expand.expect("optional expand");
    assert_eq!(optional.source_variable, "l");
    assert_eq!(optional.expand.target_variable, "m");
    assert_eq!(optional.expand.direction, RelationshipDirection::Incoming);
    assert_eq!(query.returns.len(), 3);
    assert!(matches!(
        query.returns[2].expression,
        ReturnExpression::CountVariable { .. }
    ));
}

#[test]
fn parses_with_collect_distinct_property() {
    let statement = parse(
        "MATCH (c:Memory)-[:SYNTHESIZED_FROM]->(s:Memory) WHERE c.id IN $ids WITH c, COLLECT(DISTINCT s.id) AS source_ids RETURN c.id, source_ids",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let collect_with = query.collect_with.expect("collect with");
    assert_eq!(collect_with.group_variable, "c");
    assert_eq!(collect_with.collect_variable, "s");
    assert_eq!(collect_with.collect_property, "id");
    assert!(collect_with.distinct);
    assert_eq!(collect_with.alias, "source_ids");
    assert_eq!(query.returns.len(), 2);
}

#[test]
fn parses_with_collect_distinct_variable_and_count() {
    let statement = parse(
        "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) WHERE e.id IN $entity_ids WITH m, COLLECT(DISTINCT e) as entity_nodes, COUNT(DISTINCT e) as entity_count RETURN m, entity_nodes, entity_count ORDER BY entity_count DESC LIMIT $limit",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let aggregate_with = query.aggregate_with.expect("aggregate with");
    assert_eq!(aggregate_with.items.len(), 3);
    assert!(matches!(
        aggregate_with.items[1].expression,
        ReturnExpression::CollectVariable {
            ref variable,
            distinct: true
        } if variable == "e"
    ));
    assert!(matches!(
        aggregate_with.items[2].expression,
        ReturnExpression::CountVariable {
            ref variable,
            distinct: true
        } if variable == "e"
    ));
    assert_eq!(query.returns.len(), 3);
    assert_eq!(query.order_by.len(), 1);
    assert_eq!(
        query.limit,
        Some(ValueExpression::Parameter("limit".to_string()))
    );
}

#[test]
fn parses_optional_match_return_collect_distinct_property() {
    let statement = parse(
        "MATCH (m:Memory) OPTIONAL MATCH (m)-[:HAS_LABEL]->(l:Label) RETURN m.id, COLLECT(DISTINCT l.name) AS labels",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let optional = query.optional_expand.expect("optional expand");
    assert_eq!(optional.source_variable, "m");
    assert_eq!(optional.expand.target_variable, "l");
    assert_eq!(query.returns.len(), 2);
    let ReturnExpression::CollectProperty {
        variable,
        property,
        distinct,
    } = &query.returns[1].expression
    else {
        panic!("expected collect return");
    };
    assert_eq!(variable, "l");
    assert_eq!(property, "name");
    assert!(*distinct);
    assert_eq!(query.returns[1].alias.as_deref(), Some("labels"));
}

#[test]
fn parses_literal_return_projection_alias() {
    let statement = parse("MATCH (m:Memory) RETURN m.id, 0 AS mention_breadth").unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert_eq!(query.returns.len(), 2);
    assert!(matches!(
        query.returns[1].expression,
        ReturnExpression::Value(_)
    ));
    assert_eq!(query.returns[1].alias.as_deref(), Some("mention_breadth"));
}

#[test]
fn parses_case_property_default_if_null_order_expression() {
    let statement = parse(
        "MATCH (m:Memory) RETURN m.id ORDER BY CASE WHEN m.importance IS NOT NULL THEN m.importance ELSE 0.5 END DESC",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let OrderExpression::Value(ReturnValueExpression::DefaultIfNull {
        variable,
        property,
        default,
    }) = &query.order_by[0].expression
    else {
        panic!("expected default-if-null order expression");
    };
    assert_eq!(variable, "m");
    assert_eq!(property, "importance");
    assert_eq!(default, &ValueExpression::Literal(Value::Float(0.5)));
}

#[test]
fn parses_with_distinct_property_alias_count() {
    let statement = parse(
        "MATCH (c:Memory)-[:CRYSTALLIZED_FROM]->(s:Memory) WITH DISTINCT c.id AS a, s.id AS b RETURN count(*)",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let distinct_with = query.distinct_with.expect("distinct with");
    assert_eq!(distinct_with.items.len(), 2);
    assert_eq!(distinct_with.items[0].alias.as_deref(), Some("a"));
    assert_eq!(distinct_with.items[1].alias.as_deref(), Some("b"));
    assert_eq!(query.returns[0].expression, ReturnExpression::CountAll);
}

#[test]
fn parses_with_aggregate_alias_filter_return() {
    let statement = parse(
        "MATCH (c:Memory)-[:SYNTHESIZED_FROM]->(s:Memory) WHERE c.is_crystal = true AND s.id IN $source_ids WITH c.id AS cid, count(DISTINCT s.id) AS covered WHERE covered = $n RETURN cid LIMIT 1",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let aggregate_with = query.aggregate_with.expect("aggregate with");
    assert_eq!(aggregate_with.items.len(), 2);
    assert_eq!(aggregate_with.items[0].alias.as_deref(), Some("cid"));
    assert_eq!(aggregate_with.items[1].alias.as_deref(), Some("covered"));
    let filter = query.aggregate_with_filter.expect("aggregate filter");
    let WithAliasFilter::Comparison { left, .. } = filter else {
        panic!("expected comparison filter");
    };
    assert_eq!(
        left,
        WithAliasFilterExpression::Column("covered".to_string())
    );
    assert_eq!(query.returns.len(), 1);
    assert_eq!(
        query.returns[0].expression,
        ReturnExpression::Variable("cid".to_string())
    );
}

#[test]
fn parses_with_aggregate_two_alias_return() {
    let statement = parse(
        "MATCH (c:Memory)-[:SYNTHESIZED_FROM]->(s:Memory) WHERE c.is_crystal = true AND s.id IN $source_ids WITH c.id AS cid, c.crystal_title AS ct, count(DISTINCT s.id) AS covered WHERE covered = $n RETURN cid, ct LIMIT 1",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let aggregate_with = query.aggregate_with.expect("aggregate with");
    assert_eq!(aggregate_with.items.len(), 3);
    assert_eq!(aggregate_with.items[0].alias.as_deref(), Some("cid"));
    assert_eq!(aggregate_with.items[1].alias.as_deref(), Some("ct"));
    assert_eq!(aggregate_with.items[2].alias.as_deref(), Some("covered"));
    assert_eq!(query.returns.len(), 2);
}

#[test]
fn parses_with_date_part_group_aggregate() {
    let statement = parse(
        "MATCH (m:Memory) WHERE m.created_at IS NOT NULL WITH date_part('year', m.created_at) AS year, date_part('month', m.created_at) AS month, COUNT(m) AS memory_count RETURN year, month, memory_count ORDER BY year DESC, month DESC LIMIT $months",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let aggregate_with = query.aggregate_with.expect("aggregate with");
    assert_eq!(aggregate_with.items.len(), 3);
    assert_eq!(aggregate_with.items[0].alias.as_deref(), Some("year"));
    assert_eq!(
        aggregate_with.items[0].expression,
        ReturnExpression::DatePart {
            part: "year".to_string(),
            variable: "m".to_string(),
            property: "created_at".to_string(),
        }
    );
    assert_eq!(aggregate_with.items[1].alias.as_deref(), Some("month"));
    assert_eq!(
        aggregate_with.items[2].alias.as_deref(),
        Some("memory_count")
    );
    assert_eq!(query.returns.len(), 3);
    assert_eq!(query.order_by.len(), 2);
}

#[test]
fn parses_normalized_space_case_predicates() {
    let statement = parse(
        "MATCH (t:Thread) WHERE t.thread_id IN $thread_ids AND CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END = $source_space_id RETURN t.id",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let Some(PropertyPredicate::And(predicates)) = query.predicate else {
        panic!("expected conjunction");
    };
    assert_eq!(predicates.len(), 2);
    assert!(matches!(
        predicates[1],
        PropertyPredicate::ExpressionEq {
            expression: ReturnValueExpression::DefaultIfNullOrEq { .. },
            ..
        }
    ));

    let statement = parse(
        "MATCH (t:Thread) WHERE CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END <> $target_space_id RETURN t.thread_id",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert!(matches!(
        query.predicate,
        Some(PropertyPredicate::ExpressionNotEq {
            expression: ReturnValueExpression::DefaultIfNullOrEq { .. },
            ..
        })
    ));
}

#[test]
fn parses_normalized_space_case_as_first_aggregate_with_item() {
    let statement = parse(
        "MATCH (t:Thread) WITH CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END AS space_id, t.thread_id AS thread_id, MAX(t.updated_at) AS last_activity RETURN space_id, thread_id, last_activity",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let aggregate_with = query.aggregate_with.expect("expected aggregate WITH");
    assert_eq!(aggregate_with.items.len(), 3);
    assert!(matches!(
        aggregate_with.items[0].expression,
        ReturnExpression::DefaultIfNullOrEq { .. }
    ));
    assert_eq!(aggregate_with.items[0].alias.as_deref(), Some("space_id"));
    assert!(matches!(
        aggregate_with.items[2].expression,
        ReturnExpression::MaxProperty { .. }
    ));
}

#[test]
fn parses_count_return_items() {
    let statement = parse(
        "MATCH (m:Memory) RETURN count(*) AS total, count(m) AS memories, min(m.score), max(m.score), avg(m.score)",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert_eq!(query.returns[0].expression, ReturnExpression::CountAll);
    assert_eq!(
        query.returns[1].expression,
        ReturnExpression::CountVariable {
            variable: "m".to_string(),
            distinct: false
        }
    );
    assert_eq!(
        query.returns[2].expression,
        ReturnExpression::MinProperty {
            variable: "m".to_string(),
            property: "score".to_string(),
        }
    );
    assert_eq!(
        query.returns[3].expression,
        ReturnExpression::MaxProperty {
            variable: "m".to_string(),
            property: "score".to_string(),
        }
    );
    assert_eq!(
        query.returns[4].expression,
        ReturnExpression::AvgProperty {
            variable: "m".to_string(),
            property: "score".to_string(),
        }
    );
}

#[test]
fn parses_order_by_count_return_item() {
    let statement = parse(
        "MATCH (e:Entity)-[r:RELATES_TO]-(:Entity) RETURN e.id, COUNT(r) ORDER BY COUNT(r) DESC LIMIT $limit",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert_eq!(
        query.returns[1].expression,
        ReturnExpression::CountVariable {
            variable: "r".to_string(),
            distinct: false,
        }
    );
    assert_eq!(
        query.order_by[0].expression,
        OrderExpression::Column("count(r)".to_string())
    );
    assert_eq!(query.order_by[0].direction, OrderDirection::Desc);
}

#[test]
fn parses_count_distinct_return_items() {
    let statement =
        parse("MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) RETURN count(DISTINCT e.id) AS entities")
            .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert_eq!(query.returns[0].alias.as_deref(), Some("entities"));
    assert_eq!(
        query.returns[0].expression,
        ReturnExpression::CountProperty {
            variable: "e".to_string(),
            property: "id".to_string(),
            distinct: true
        }
    );
}

#[test]
fn parses_coalesce_and_left_return_items() {
    let statement = parse(
        "MATCH (m:Memory) RETURN COALESCE(m.title, LEFT(COALESCE(m.content, ''), 60)) AS label",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert_eq!(query.returns[0].alias.as_deref(), Some("label"));
    assert_eq!(
        query.returns[0].expression,
        ReturnExpression::Coalesce(vec![
            ReturnValueExpression::Property {
                variable: "m".to_string(),
                property: "title".to_string(),
            },
            ReturnValueExpression::Left {
                expression: Box::new(ReturnValueExpression::Coalesce(vec![
                    ReturnValueExpression::Property {
                        variable: "m".to_string(),
                        property: "content".to_string(),
                    },
                    ReturnValueExpression::Value(ValueExpression::Literal(Value::String(
                        String::new()
                    ))),
                ])),
                length: ValueExpression::Literal(Value::Int(60)),
            },
        ])
    );
}

#[test]
fn parses_coalesce_and_left_predicates() {
    let statement = parse(
        "MATCH (m:Memory) WHERE COALESCE(m.created_at, m.last_accessed_at) >= $cutoff AND LEFT(COALESCE(m.title, ''), 4) = 'Graph' RETURN m.id",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let Some(PropertyPredicate::And(predicates)) = query.predicate else {
        panic!("expected predicate conjunction");
    };
    assert_eq!(
        predicates[0],
        PropertyPredicate::ExpressionCompare {
            expression: ReturnValueExpression::Coalesce(vec![
                ReturnValueExpression::Property {
                    variable: "m".to_string(),
                    property: "created_at".to_string(),
                },
                ReturnValueExpression::Property {
                    variable: "m".to_string(),
                    property: "last_accessed_at".to_string(),
                },
            ]),
            op: ComparisonOp::Gte,
            value: ReturnValueExpression::Value(ValueExpression::Parameter("cutoff".to_string(),)),
        }
    );
    assert_eq!(
        predicates[1],
        PropertyPredicate::ExpressionEq {
            expression: ReturnValueExpression::Left {
                expression: Box::new(ReturnValueExpression::Coalesce(vec![
                    ReturnValueExpression::Property {
                        variable: "m".to_string(),
                        property: "title".to_string(),
                    },
                    ReturnValueExpression::Value(ValueExpression::Literal(Value::String(
                        String::new()
                    ))),
                ])),
                length: ValueExpression::Literal(Value::Int(4)),
            },
            value: ReturnValueExpression::Value(ValueExpression::Literal(Value::String(
                "Graph".to_string()
            ))),
        }
    );
}

#[test]
fn parses_coalesce_float_predicate() {
    let statement =
        parse("MATCH (m:Memory) WHERE COALESCE(m.decay_score_cached, 1.0) < 0.55 RETURN m.id")
            .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let Some(PropertyPredicate::ExpressionCompare {
        expression, value, ..
    }) = query.predicate
    else {
        panic!("expected expression compare");
    };
    assert!(matches!(
        expression,
        ReturnValueExpression::Coalesce(expressions)
            if expressions.len() == 2
                && matches!(
                    expressions[1],
                    ReturnValueExpression::Value(ValueExpression::Literal(Value::Float(1.0)))
                )
    ));
    assert!(matches!(
        value,
        ReturnValueExpression::Value(ValueExpression::Literal(Value::Float(0.55)))
    ));
}

#[test]
fn parses_lower_contains_expression_predicates() {
    let statement = parse(
        "MATCH (m:Memory) WHERE LOWER(COALESCE(m.content, '')) CONTAINS LOWER($needle) RETURN m.id",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert_eq!(
        query.predicate,
        Some(PropertyPredicate::ExpressionContains {
            expression: ReturnValueExpression::Lower(Box::new(ReturnValueExpression::Coalesce(
                vec![
                    ReturnValueExpression::Property {
                        variable: "m".to_string(),
                        property: "content".to_string(),
                    },
                    ReturnValueExpression::Value(ValueExpression::Literal(Value::String(
                        String::new()
                    ))),
                ]
            ))),
            value: ReturnValueExpression::Lower(Box::new(ReturnValueExpression::Value(
                ValueExpression::Parameter("needle".to_string())
            ))),
        })
    );
}

#[test]
fn parses_lower_equality_expression_predicates() {
    let statement =
        parse("MATCH (e:Entity) WHERE LOWER(e.name) = LOWER($mention) RETURN e.id").unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert_eq!(
        query.predicate,
        Some(PropertyPredicate::ExpressionEq {
            expression: ReturnValueExpression::Lower(Box::new(ReturnValueExpression::Property {
                variable: "e".to_string(),
                property: "name".to_string(),
            })),
            value: ReturnValueExpression::Lower(Box::new(ReturnValueExpression::Value(
                ValueExpression::Parameter("mention".to_string())
            ))),
        })
    );
}

#[test]
fn parses_order_offset_and_limit() {
    let statement =
        parse("MATCH (m:Memory) RETURN m.title AS title ORDER BY title DESC SKIP $offset LIMIT 10")
            .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert_eq!(query.order_by.len(), 1);
    assert_eq!(
        query.order_by[0].expression,
        OrderExpression::Column("title".to_string())
    );
    assert_eq!(query.order_by[0].direction, OrderDirection::Desc);
    assert_eq!(
        query.offset,
        Some(ValueExpression::Parameter("offset".to_string()))
    );
    assert!(query.limit.is_some());
}

#[test]
fn parses_order_by_coalesce_expression() {
    let statement = parse(
        "MATCH (m:Memory) RETURN m.id AS id ORDER BY COALESCE(m.pagerank_score, m.importance, 0.5) DESC",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert_eq!(query.order_by.len(), 1);
    assert_eq!(
        query.order_by[0].expression,
        OrderExpression::Value(ReturnValueExpression::Coalesce(vec![
            ReturnValueExpression::Property {
                variable: "m".to_string(),
                property: "pagerank_score".to_string(),
            },
            ReturnValueExpression::Property {
                variable: "m".to_string(),
                property: "importance".to_string(),
            },
            ReturnValueExpression::Value(ValueExpression::Literal(Value::Float(0.5))),
        ]))
    );
    assert_eq!(query.order_by[0].direction, OrderDirection::Desc);
}

#[test]
fn parses_parameter_value_without_binding_it() {
    let statement = parse("MATCH (m:Memory) WHERE m.id = $id RETURN m.title").unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert_eq!(
        query.predicate.unwrap(),
        PropertyPredicate::Eq {
            variable: "m".to_string(),
            property: "id".to_string(),
            value: ValueExpression::Parameter("id".to_string()),
        }
    );
}

#[test]
fn parses_null_and_list_predicates() {
    let statement = parse("MATCH (m:Memory) WHERE m.deleted_at IS NULL RETURN m.title").unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert_eq!(
        query.predicate.unwrap(),
        PropertyPredicate::IsNull {
            variable: "m".to_string(),
            property: "deleted_at".to_string(),
        }
    );

    let statement = parse("MATCH (m:Memory) WHERE m.id IN [1, 2] RETURN m.title").unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let PropertyPredicate::In { values, .. } = query.predicate.unwrap() else {
        panic!("expected in predicate");
    };
    assert_eq!(
        values,
        ValueExpression::List(vec![
            ValueExpression::Literal(Value::Int(1)),
            ValueExpression::Literal(Value::Int(2))
        ])
    );

    let statement = parse("MATCH (m:Memory) WHERE m.id IN [1, $id] RETURN m.title").unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let PropertyPredicate::In { values, .. } = query.predicate.unwrap() else {
        panic!("expected in predicate");
    };
    assert_eq!(
        values,
        ValueExpression::List(vec![
            ValueExpression::Literal(Value::Int(1)),
            ValueExpression::Parameter("id".to_string())
        ])
    );

    let statement =
        parse("MATCH (e:Entity) WHERE list_contains(e.aliases, $name) RETURN e.id").unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert_eq!(
        query.predicate.unwrap(),
        PropertyPredicate::ListContains {
            variable: "e".to_string(),
            property: "aliases".to_string(),
            value: ValueExpression::Parameter("name".to_string()),
        }
    );

    let statement =
        parse("MATCH (e:Entity) WHERE list_contains_lower(e.aliases, $query) RETURN e.id").unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert_eq!(
        query.predicate.unwrap(),
        PropertyPredicate::ListContainsLower {
            variable: "e".to_string(),
            property: "aliases".to_string(),
            value: ValueExpression::Parameter("query".to_string()),
        }
    );
}

#[test]
fn parses_parameter_null_predicate() {
    let statement =
        parse("MATCH (t:Thread) WHERE ($source IS NULL OR t.source = $source) RETURN COUNT(t)")
            .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let Some(PropertyPredicate::Or(predicates)) = query.predicate else {
        panic!("expected OR predicate");
    };
    assert_eq!(
        predicates[0],
        PropertyPredicate::ParameterIsNull {
            parameter: "source".to_string(),
        }
    );
}

#[test]
fn parses_parameter_equality_predicate() {
    let statement = parse(
        "MATCH (m:Memory) WHERE m.space_id = $space_id OR ($space_id = $default_space_id AND (m.space_id IS NULL OR m.space_id = '')) RETURN m.id",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let Some(PropertyPredicate::Or(predicates)) = query.predicate else {
        panic!("expected OR predicate");
    };
    let PropertyPredicate::And(default_space_predicates) = &predicates[1] else {
        panic!("expected default-space conjunction");
    };
    assert_eq!(
        default_space_predicates[0],
        PropertyPredicate::ParameterEq {
            left: "space_id".to_string(),
            right: ValueExpression::Parameter("default_space_id".to_string()),
        }
    );
}

#[test]
fn parses_parameter_literal_equality_predicate() {
    let statement = parse(
        "MATCH (m:Memory) WHERE m.space_id = $space_id OR ($space_id = 'default' AND (m.space_id IS NULL OR m.space_id = '')) RETURN m.id",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let Some(PropertyPredicate::Or(predicates)) = query.predicate else {
        panic!("expected OR predicate");
    };
    let PropertyPredicate::And(default_space_predicates) = &predicates[1] else {
        panic!("expected default-space conjunction");
    };
    assert_eq!(
        default_space_predicates[0],
        PropertyPredicate::ParameterEq {
            left: "space_id".to_string(),
            right: ValueExpression::Literal(Value::String("default".to_string())),
        }
    );
}

#[test]
fn parses_contains_function_predicate() {
    let statement = parse(
        "MATCH (m:Memory) WHERE contains(LOWER(COALESCE(m.title, '')), LOWER($q)) RETURN m.id",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert!(matches!(
        query.predicate,
        Some(PropertyPredicate::ExpressionContains {
            expression: ReturnValueExpression::Lower(_),
            value: ReturnValueExpression::Lower(_),
        })
    ));
}

#[test]
fn parses_parenthesized_case_expression_predicate() {
    let statement = parse(
        "MATCH (t:Thread) WHERE (CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END) = $space_id RETURN COUNT(t)",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert!(matches!(
        query.predicate,
        Some(PropertyPredicate::ExpressionEq { .. })
    ));
}

#[test]
fn parses_range_predicates() {
    let statement = parse("MATCH (m:Memory) WHERE m.created_at >= 10 RETURN m.title").unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert_eq!(
        query.predicate.unwrap(),
        PropertyPredicate::Compare {
            variable: "m".to_string(),
            property: "created_at".to_string(),
            op: ComparisonOp::Gte,
            value: ValueExpression::Literal(Value::Int(10)),
        }
    );

    let statement = parse("MATCH (m:Memory) WHERE m.title < 'm' RETURN m.title").unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert_eq!(
        query.predicate.unwrap(),
        PropertyPredicate::Compare {
            variable: "m".to_string(),
            property: "title".to_string(),
            op: ComparisonOp::Lt,
            value: ValueExpression::Literal(Value::String("m".to_string())),
        }
    );
}

#[test]
fn parses_not_equal_predicates() {
    let statement = parse("MATCH (m:Memory) WHERE m.kind <> 'task' RETURN m.title").unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert_eq!(
        query.predicate.unwrap(),
        PropertyPredicate::NotEq {
            variable: "m".to_string(),
            property: "kind".to_string(),
            value: ValueExpression::Literal(Value::String("task".to_string())),
        }
    );

    let statement = parse("MATCH (m:Memory) WHERE id(m) <> $id RETURN m.title").unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert_eq!(
        query.predicate.unwrap(),
        PropertyPredicate::IdNotEq {
            variable: "m".to_string(),
            value: ValueExpression::Parameter("id".to_string()),
        }
    );
}

#[test]
fn parses_contains_predicates() {
    let statement =
        parse("MATCH (m:Memory) WHERE m.title CONTAINS 'graph' RETURN m.title").unwrap();
    match statement {
        Statement::MatchReturn(match_return) => {
            assert_eq!(
                match_return.predicate,
                Some(PropertyPredicate::Contains {
                    variable: "m".to_string(),
                    property: "title".to_string(),
                    value: ValueExpression::Literal(Value::String("graph".to_string())),
                })
            );
        }
        statement => panic!("unexpected statement: {statement:?}"),
    }
}

#[test]
fn parses_string_prefix_and_suffix_predicates() {
    let statement =
        parse("MATCH (m:Memory) WHERE m.title STARTS WITH 'Graph' RETURN m.title").unwrap();
    match statement {
        Statement::MatchReturn(match_return) => {
            assert_eq!(
                match_return.predicate,
                Some(PropertyPredicate::StartsWith {
                    variable: "m".to_string(),
                    property: "title".to_string(),
                    value: ValueExpression::Literal(Value::String("Graph".to_string())),
                })
            );
        }
        statement => panic!("unexpected statement: {statement:?}"),
    }

    let statement =
        parse("MATCH (m:Memory) WHERE m.title ENDS WITH $suffix RETURN m.title").unwrap();
    match statement {
        Statement::MatchReturn(match_return) => {
            assert_eq!(
                match_return.predicate,
                Some(PropertyPredicate::EndsWith {
                    variable: "m".to_string(),
                    property: "title".to_string(),
                    value: ValueExpression::Parameter("suffix".to_string()),
                })
            );
        }
        statement => panic!("unexpected statement: {statement:?}"),
    }
}

#[test]
fn parses_regex_match_predicate() {
    let statement =
        parse("MATCH (m:Memory)-[:HAS_LABEL]->(l:Label) WHERE l.name =~ $pattern RETURN m")
            .unwrap();
    match statement {
        Statement::MatchReturn(match_return) => {
            assert_eq!(
                match_return.predicate,
                Some(PropertyPredicate::RegexMatch {
                    variable: "l".to_string(),
                    property: "name".to_string(),
                    pattern: ValueExpression::Parameter("pattern".to_string()),
                })
            );
        }
        statement => panic!("unexpected statement: {statement:?}"),
    }
}

#[test]
fn parses_and_predicates() {
    let statement =
        parse("MATCH (m:Memory) WHERE m.created_at >= 10 AND m.created_at < 20 RETURN m.title")
            .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert_eq!(
        query.predicate.unwrap(),
        PropertyPredicate::And(vec![
            PropertyPredicate::Compare {
                variable: "m".to_string(),
                property: "created_at".to_string(),
                op: ComparisonOp::Gte,
                value: ValueExpression::Literal(Value::Int(10)),
            },
            PropertyPredicate::Compare {
                variable: "m".to_string(),
                property: "created_at".to_string(),
                op: ComparisonOp::Lt,
                value: ValueExpression::Literal(Value::Int(20)),
            },
        ])
    );
}

#[test]
fn parses_not_predicates() {
    let statement = parse("MATCH (m:Memory) WHERE NOT m.kind = 'task' RETURN m.title").unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert_eq!(
        query.predicate.unwrap(),
        PropertyPredicate::Not(Box::new(PropertyPredicate::Eq {
            variable: "m".to_string(),
            property: "kind".to_string(),
            value: ValueExpression::Literal(Value::String("task".to_string())),
        }))
    );

    let statement =
        parse("MATCH (m:Memory) WHERE NOT (m.kind = 'task' OR m.score < 10) RETURN m.title")
            .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert_eq!(
        query.predicate.unwrap(),
        PropertyPredicate::Not(Box::new(PropertyPredicate::Or(vec![
            PropertyPredicate::Eq {
                variable: "m".to_string(),
                property: "kind".to_string(),
                value: ValueExpression::Literal(Value::String("task".to_string())),
            },
            PropertyPredicate::Compare {
                variable: "m".to_string(),
                property: "score".to_string(),
                op: ComparisonOp::Lt,
                value: ValueExpression::Literal(Value::Int(10)),
            },
        ])))
    );
}

#[test]
fn parses_nowledge_orphan_relationship_existence_predicates() {
    let statement = parse(
        "MATCH (e:Entity)
         WHERE NOT (e)<-[:MENTIONS]-(:Memory)
           AND NOT (e)-[:RELATES_TO]-()
           AND NOT (e)-[:HAS_LABEL]-()
         RETURN e.id",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    let Some(PropertyPredicate::And(predicates)) = query.predicate else {
        panic!("expected predicate conjunction");
    };
    assert_eq!(predicates.len(), 3);
    assert_eq!(
        predicates[0],
        PropertyPredicate::Not(Box::new(PropertyPredicate::RelationshipExists {
            variable: "e".to_string(),
            rel_type: "MENTIONS".to_string(),
            direction: RelationshipDirection::Incoming,
            target_label: "Memory".to_string(),
        }))
    );
    assert_eq!(
        predicates[1],
        PropertyPredicate::Not(Box::new(PropertyPredicate::RelationshipExists {
            variable: "e".to_string(),
            rel_type: "RELATES_TO".to_string(),
            direction: RelationshipDirection::Undirected,
            target_label: String::new(),
        }))
    );
    assert_eq!(
        predicates[2],
        PropertyPredicate::Not(Box::new(PropertyPredicate::RelationshipExists {
            variable: "e".to_string(),
            rel_type: "HAS_LABEL".to_string(),
            direction: RelationshipDirection::Undirected,
            target_label: String::new(),
        }))
    );
}

#[test]
fn parses_bound_relationship_existence_subquery_predicate() {
    let statement = parse(
        "MATCH (c:Memory)-[:CRYSTALLIZED_FROM]->(s:Memory)
         WHERE NOT EXISTS { MATCH (c)-[:SYNTHESIZED_FROM]->(s) }
         RETURN count(*)",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert_eq!(
        query.predicate,
        Some(PropertyPredicate::Not(Box::new(
            PropertyPredicate::BoundRelationshipExists {
                source_variable: "c".to_string(),
                rel_type: "SYNTHESIZED_FROM".to_string(),
                direction: RelationshipDirection::Outgoing,
                target_variable: "s".to_string(),
            }
        )))
    );
}

#[test]
fn parses_or_predicates_with_and_precedence() {
    let statement = parse(
        "MATCH (m:Memory) WHERE m.kind = 'note' OR m.score >= 10 AND m.score < 20 RETURN m.title",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert_eq!(
        query.predicate.unwrap(),
        PropertyPredicate::Or(vec![
            PropertyPredicate::Eq {
                variable: "m".to_string(),
                property: "kind".to_string(),
                value: ValueExpression::Literal(Value::String("note".to_string())),
            },
            PropertyPredicate::And(vec![
                PropertyPredicate::Compare {
                    variable: "m".to_string(),
                    property: "score".to_string(),
                    op: ComparisonOp::Gte,
                    value: ValueExpression::Literal(Value::Int(10)),
                },
                PropertyPredicate::Compare {
                    variable: "m".to_string(),
                    property: "score".to_string(),
                    op: ComparisonOp::Lt,
                    value: ValueExpression::Literal(Value::Int(20)),
                },
            ]),
        ])
    );
}

#[test]
fn parses_parenthesized_predicates() {
    let statement = parse(
        "MATCH (m:Memory) WHERE (m.kind = 'note' OR m.kind = 'thread') AND m.score >= 10 RETURN m.title",
    )
    .unwrap();
    let Statement::MatchReturn(query) = statement else {
        panic!("expected match return");
    };
    assert_eq!(
        query.predicate.unwrap(),
        PropertyPredicate::And(vec![
            PropertyPredicate::Or(vec![
                PropertyPredicate::Eq {
                    variable: "m".to_string(),
                    property: "kind".to_string(),
                    value: ValueExpression::Literal(Value::String("note".to_string())),
                },
                PropertyPredicate::Eq {
                    variable: "m".to_string(),
                    property: "kind".to_string(),
                    value: ValueExpression::Literal(Value::String("thread".to_string())),
                },
            ]),
            PropertyPredicate::Compare {
                variable: "m".to_string(),
                property: "score".to_string(),
                op: ComparisonOp::Gte,
                value: ValueExpression::Literal(Value::Int(10)),
            },
        ])
    );
}

#[test]
fn parses_match_expand_match_merge_relationship() {
    let statement = parse(
        "MATCH (n:Memory)-[:HAS_LABEL]->(src:Label {id: $src}) MATCH (tgt:Label {id: $tgt}) MERGE (n)-[r:HAS_LABEL]->(tgt) ON CREATE SET r.assigned_by = 'label_merge', r.created_at = $now",
    )
    .unwrap();
    let Statement::MatchExpandMatchMergeRelationship(query) = statement else {
        panic!("expected match expand match merge relationship");
    };
    assert_eq!(query.source_variable, "n");
    assert_eq!(query.expand.target_variable, "src");
    assert_eq!(query.matched_target_variable, "tgt");
    assert_eq!(query.rel_type, "HAS_LABEL");
    assert_eq!(query.on_create_sets.len(), 2);
}

#[test]
fn parses_match_expand_comma_match_merge_relationship_with_predicate() {
    let statement = parse(
        "MATCH (older:Memory {id: $older_id})-[:HAS_LABEL]->(label:Label), (newer:Memory {id: $newer_id}) WHERE older.space_id = $space_id AND newer.space_id = $space_id MERGE (newer)-[edge:HAS_LABEL]->(label) ON CREATE SET edge.assigned_by = 'system', edge.created_at = $created_at, edge.properties = '{}'",
    )
    .unwrap();
    let Statement::MatchExpandMatchMergeRelationship(query) = statement else {
        panic!("expected match expand comma merge relationship");
    };
    assert_eq!(query.source_variable, "older");
    assert_eq!(query.expand.target_variable, "label");
    assert_eq!(query.matched_target_variable, "newer");
    assert_eq!(query.rel_variable.as_deref(), Some("edge"));
    assert_eq!(query.rel_type, "HAS_LABEL");
    assert!(query.predicate.is_some());
    assert_eq!(query.on_create_sets.len(), 3);
}
