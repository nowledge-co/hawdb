use super::*;

fn plan(query: &str, parameters: &BTreeMap<String, Value>) -> PhysicalPlan {
    let logical = hawdb_plan::plan_pipeline_query(query, parameters).unwrap();
    hawdb_optimizer::CascadesOptimizer::default().optimize(&logical)
}

struct Seed;

impl ExternalReadOperator for Seed {
    fn execute_vector_seed(
        &mut self,
        request: VectorSeedExecutionRequest<'_>,
    ) -> Result<VectorSeedExecutionOutput> {
        assert_eq!(request.embedding, &[1.0, 0.0]);
        Ok(VectorSeedExecutionOutput {
            rows: vec![
                VectorSeedExecutionRow {
                    id: "vector-hit".into(),
                    external_id: Some("keep".into()),
                    score: 0.8,
                },
                VectorSeedExecutionRow {
                    id: "vector-missing".into(),
                    external_id: Some("missing".into()),
                    score: 0.1,
                },
            ],
            report: hawdb_executor::VectorExecutionReport {
                backend: hawdb_executor::VectorExecutionBackend::ScalarFlat,
                compression_mode: hawdb_executor::VectorCompressionMode::Disabled,
                candidate_source: hawdb_plan::VectorCandidateSource::Scalar,
                backend_selection_reason: None,
                estimated_raw_vector_bytes: None,
                filter_selectivity_per_million: None,
                candidate_score_source: hawdb_executor::VectorScoreSource::RawVector,
                final_score_source: hawdb_executor::VectorScoreSource::RawVector,
                generated_candidate_count: 2,
                descriptor_pruned_count: 0,
                scalar_filtered_count: 0,
                residual_filtered_count: 0,
                candidate_scan_rounds: 1,
                reranked_candidate_count: 2,
                returned_count: 2,
                raw_vector_bytes_read: 0,
                candidate_scan_metrics: None,
                index_covered_document_count: None,
                index_candidate_document_count: None,
                index_coverage_complete: None,
                fallback_reason_codes: Vec::new(),
            },
        })
    }
}

#[test]
fn vector_yield_aliases_preserve_seed_identity_through_matches() {
    let mut store = GraphStore::in_memory();
    let mut catalog = Catalog::default();
    let mut nodes = Vec::new();
    for id in ["keep", "decoy", "neighbor"] {
        nodes.push(
            store
                .create_node(
                    &mut catalog,
                    "Document",
                    BTreeMap::from([
                        ("id".into(), Value::String(id.into())),
                        ("space".into(), Value::String("same-space".into())),
                    ]),
                )
                .unwrap(),
        );
    }
    for source in [nodes[0], nodes[1]] {
        store
            .create_relationship(&mut catalog, source, nodes[2], "LINK", BTreeMap::new())
            .unwrap();
    }
    let parameters = BTreeMap::from([(
        "embedding".into(),
        Value::List(vec![Value::Float(1.0), Value::Float(0.0)]),
    )]);
    for (query, id) in [
        ("CALL vector_search($embedding) YIELD id AS hit, score AS rank MATCH (n:Document) WHERE n.space = 'same-space' RETURN hit, rank, n.id AS node_id", "keep"),
        ("CALL vector_search($embedding) YIELD id AS hit, score AS rank MATCH (n:Document {space: 'same-space'})-[:LINK]->(m:Document) RETURN hit, rank, m.id AS node_id", "neighbor"),
    ] {
        let output = execute_with_row_limit_profile_and_external(&plan(query, &parameters), &mut catalog, &mut store, &parameters, &mut Seed, None).unwrap();
        assert_eq!(output.rows.len(), 1, "{query}");
        assert_eq!(output.rows[0].get("hit"), Some(&Value::String("vector-hit".into())));
        assert_eq!(output.rows[0].get("rank"), Some(&Value::Float(0.8)));
        assert_eq!(output.rows[0].get("node_id"), Some(&Value::String(id.into())));
    }
}

#[test]
fn graph_procedures_and_shortest_paths_execute_from_ordered_clauses() {
    let mut store = GraphStore::in_memory();
    let mut catalog = Catalog::default();
    let mut nodes = Vec::new();
    for id in 0..4 {
        nodes.push(
            store
                .create_node(
                    &mut catalog,
                    "Vertex",
                    BTreeMap::from([("id".into(), Value::Int(id))]),
                )
                .unwrap(),
        );
    }
    for (source, target) in [(0, 1), (0, 2), (1, 3), (2, 3)] {
        store
            .create_relationship(
                &mut catalog,
                nodes[source],
                nodes[target],
                "LINK",
                BTreeMap::new(),
            )
            .unwrap();
    }
    let empty = BTreeMap::new();
    execute(
        &plan("CALL project_graph('Graph', ['Vertex'], ['LINK'])", &empty),
        &mut catalog,
        &mut store,
    )
    .unwrap();
    let ranked = execute(&plan("CALL page_rank('Graph', maxIterations := 2) YIELD node AS vertex, pagerank_score AS rank RETURN vertex, rank ORDER BY rank DESC", &empty), &mut catalog, &mut store).unwrap();
    assert_eq!(ranked.len(), 4);
    assert!(ranked
        .iter()
        .all(|row| matches!(row.get("rank"), Some(Value::Float(rank)) if rank.is_finite())));
    let communities = execute(
        &plan(
            "CALL louvain('Graph', maxLevels := 1) RETURN node, level, louvain_id",
            &empty,
        ),
        &mut catalog,
        &mut store,
    )
    .unwrap();
    assert_eq!(communities.len(), 4);
    let query = "MATCH p = (source:Vertex)-[:LINK* ALL SHORTEST 1..3]->(target:Vertex) WHERE source.id = $source AND target.id = $target RETURN properties(nodes(p), 'id') AS ids, length(p) AS hops";
    let parameters = BTreeMap::from([
        ("source".into(), Value::Int(0)),
        ("target".into(), Value::Int(3)),
    ]);
    let rows = execute(&plan(query, &parameters), &mut catalog, &mut store).unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| row["hops"] == Value::Int(2)));
    let paths = rows
        .iter()
        .map(|row| row["ids"].clone())
        .collect::<Vec<_>>();
    assert!(paths.contains(&Value::List(vec![
        Value::Int(0),
        Value::Int(1),
        Value::Int(3)
    ])));
    assert!(paths.contains(&Value::List(vec![
        Value::Int(0),
        Value::Int(2),
        Value::Int(3)
    ])));
}
