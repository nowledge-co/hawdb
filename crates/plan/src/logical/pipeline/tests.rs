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
