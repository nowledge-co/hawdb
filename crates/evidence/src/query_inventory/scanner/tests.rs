use super::*;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn extracts_cooked_and_raw_rust_cypher_literals() {
    let source = r##"
        let read = "MATCH (m:Memory {id: $id})\nRETURN m.id";
        let ignored = "https://example.test/query";
        let raw = r#"MATCH (j:AugmentationJob)
                     WHERE j.status = 'pending'
                     SET j.status = 'failed'"#;
    "##;

    let queries = extract_rust_string_literals(source)
        .unwrap()
        .into_iter()
        .filter_map(|literal| normalize_cypher_literal(&literal.value))
        .collect::<Vec<_>>();

    assert_eq!(queries.len(), 2);
    assert_eq!(queries[0], "MATCH (m:Memory {id: $id}) RETURN m.id");
    assert_eq!(
        queries[1],
        "MATCH (j:AugmentationJob) WHERE j.status = 'pending' SET j.status = 'failed'"
    );
}

#[test]
fn classifies_scanned_query_families() {
    assert_eq!(
        classify_query_family("MATCH (m:Memory) RETURN m.id"),
        "read"
    );
    assert_eq!(
        classify_query_family("MATCH (m:Memory) SET m.seen = true"),
        "mutation"
    );
    assert_eq!(
        classify_query_family("CREATE RANGE INDEX ON :Memory(created_at)"),
        "schema"
    );
    assert_eq!(
        classify_query_family("MATCH (a:Memory), (b:Memory) CREATE (a)-[:R]->(b)"),
        "mutation"
    );
    assert_eq!(classify_query_family("CALL page_rank('g')"), "procedure");
}

#[test]
fn rejects_non_cypher_text_that_starts_with_create() {
    assert_eq!(
        normalize_cypher_literal("Create a crystal (knowledge synthesis)"),
        None
    );
}

#[test]
fn rejects_sql_and_prompt_text_from_inventory() {
    assert_eq!(
        normalize_cypher_literal(
            "CREATE TABLE IF NOT EXISTS content_documents (id TEXT PRIMARY KEY)"
        ),
        None
    );
    assert_eq!(normalize_cypher_literal("BEGIN IMMEDIATE"), None);
    assert_eq!(normalize_cypher_literal("Call me Wey"), None);
    assert_eq!(
        normalize_cypher_literal("CALL knowledge_search('graph')"),
        None
    );
}

#[test]
fn accepts_graph_inventory_literals() {
    assert_eq!(
        normalize_cypher_literal("CREATE INDEX ON :Memory(id)"),
        Some("CREATE INDEX ON :Memory(id)".to_string())
    );
    assert_eq!(
        normalize_cypher_literal("CALL PROJECT_GRAPH('UnifiedGraph', ['Entity'], ['LINKS'])"),
        Some("CALL PROJECT_GRAPH('UnifiedGraph', ['Entity'], ['LINKS'])".to_string())
    );
    assert_eq!(
        normalize_cypher_literal("BEGIN TRANSACTION"),
        Some("BEGIN TRANSACTION".to_string())
    );
    assert_eq!(
        normalize_cypher_literal("CHECKPOINT;"),
        Some("CHECKPOINT".to_string())
    );
    assert_eq!(normalize_cypher_literal("checkpoint"), None);
}

#[test]
fn strips_cfg_test_modules_before_scanning_literals() {
    let source = r#"
        pub fn before() -> &'static str {
            "MATCH (m:Memory) RETURN m.id"
        }

        #[cfg(test)]
        mod tests {
            #[test]
            fn ignored() {
                let query = "MATCH (t:TestOnly {shape: '{not a brace}'}) RETURN t.id";
                assert_eq!(query.len(), 1);
            }
        }

        pub fn after() -> &'static str {
            "MATCH (e:Entity) RETURN e.id"
        }
    "#;

    let stripped = strip_cfg_test_modules(source);
    let queries = extract_rust_string_literals(&stripped)
        .unwrap()
        .into_iter()
        .filter_map(|literal| normalize_cypher_literal(&literal.value))
        .collect::<Vec<_>>();

    assert_eq!(
        queries,
        vec![
            "MATCH (m:Memory) RETURN m.id".to_string(),
            "MATCH (e:Entity) RETURN e.id".to_string()
        ]
    );
}

#[test]
fn accepts_cypher_maps_but_skips_rust_format_templates() {
    assert_eq!(
        normalize_cypher_literal("MATCH (m:Memory {id: $id}) RETURN m.id"),
        Some("MATCH (m:Memory {id: $id}) RETURN m.id".to_string())
    );
    assert_eq!(
        normalize_cypher_literal("MATCH (m:Memory) WHERE m.id = $id{space_clause} RETURN m.id"),
        None
    );
    assert_eq!(
        normalize_cypher_literal(
            "MATCH p = (a)-[e* ALL SHORTEST 1..{max_depth}]-(b) RETURN length(p)"
        ),
        None
    );
    assert_eq!(
        normalize_cypher_literal("CREATE (j:AugmentationJob {result: '{}', error_message: ''})"),
        Some("CREATE (j:AugmentationJob {result: '{}', error_message: ''})".to_string())
    );
    assert_eq!(
        normalize_cypher_literal(
            "CALL PROJECT_GRAPH('{name}', {'Entity': ''}, {'RELATES_TO': ''})"
        ),
        None
    );
}

#[test]
fn skips_incomplete_match_fragments_used_for_formatting() {
    assert_eq!(
        normalize_cypher_literal("MATCH (e:Entity {name: $name, entity_type: $entity_type})"),
        None
    );
    assert_eq!(
        normalize_cypher_literal(
            "MATCH (e:Entity) WHERE e.entity_type = $entity_type AND LOWER(e.name) = LOWER($name)"
        ),
        None
    );
    assert_eq!(
        normalize_cypher_literal("MATCH (e:Entity {id: $id}) RETURN e.id"),
        Some("MATCH (e:Entity {id: $id}) RETURN e.id".to_string())
    );
    assert_eq!(
        normalize_cypher_literal("MATCH (e:Entity {id: $id}) SET e.name = $name"),
        Some("MATCH (e:Entity {id: $id}) SET e.name = $name".to_string())
    );
}

#[test]
fn skips_non_production_graph_sources() {
    assert!(!scan_source_file("crates/nmem-content/src/lib.rs"));
    assert!(!scan_source_file(
        "upstream_forks/ladybug/examples/rust/src/main.rs"
    ));
    assert!(!scan_source_file(
        "upstream_forks/rig/crates/rig-neo4j/examples/vector_search_simple.rs"
    ));
    assert!(!scan_source_file("crates/nmem-server/tests/okf_smoke.rs"));
    assert!(!scan_source_file(
        "crates/nmem-graph/src/bin/community_smoke.rs"
    ));
    assert!(scan_source_file("crates/nmem-graph/src/community.rs"));
    assert!(scan_source_file("crates/nmem-server/src/rest_fs.rs"));
}

#[test]
fn scanned_inventory_stat_error_redacts_path_and_io_details() {
    let root = std::env::temp_dir().join(format!(
        "skein-nowledge-inventory-secret-missing-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    let error = scan_nowledge_query_inventory(&root).unwrap_err();
    let message = error.to_string();

    assert_eq!(
        message,
        "execution error: failed to stat inventory path: io_error"
    );
    assert!(!message.contains(root.to_str().unwrap()));
    assert!(!message.contains("secret-missing"));
}

#[cfg(unix)]
#[test]
fn scanned_inventory_source_read_error_redacts_path_and_io_details() {
    use std::os::unix::fs::PermissionsExt;

    let root = std::env::temp_dir().join(format!(
        "skein-nowledge-inventory-secret-unreadable-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir_all(&root).unwrap();
    let source = root.join("secret_source.rs");
    fs::write(&source, "\"MATCH (m:Memory) RETURN m.secret\"").unwrap();
    let original_permissions = fs::metadata(&source).unwrap().permissions();
    let mut unreadable = original_permissions.clone();
    unreadable.set_mode(0o000);
    fs::set_permissions(&source, unreadable).unwrap();

    let error = scan_nowledge_query_inventory(&root).unwrap_err();
    fs::set_permissions(&source, original_permissions).unwrap();
    fs::remove_dir_all(&root).unwrap();
    let message = error.to_string();

    assert_eq!(
        message,
        "execution error: failed to read inventory source file: io_error"
    );
    assert!(!message.contains(source.to_str().unwrap()));
    assert!(!message.contains("secret_source"));
    assert!(!message.contains("m.secret"));
}
