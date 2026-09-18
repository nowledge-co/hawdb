use crate::{Database, Value};

#[test]
fn explain_is_directly_printable_as_a_tree_table() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
        .unwrap();

    let rendered = db
        .explain_query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
        .unwrap()
        .to_string();

    assert!(rendered.contains("| id"));
    assert!(rendered.contains("| estRows"));
    assert!(rendered.contains("NodeProjectionScanExec"));
    assert!(!rendered.contains("ProjectExec"));
    assert!(!rendered.contains("FilterExec"));
    assert!(rendered.contains("label:Memory"));
    assert!(rendered.contains("optimizer: mode=memo"));
    assert!(rendered.contains("query digest:"));
    assert!(rendered.contains("plan shape:"));
    for line in rendered
        .lines()
        .filter(|line| line.starts_with('|') && line.contains("Exec"))
    {
        let cells = line.split('|').map(str::trim).collect::<Vec<_>>();
        assert_ne!(cells[2], "N/A");
    }
}

#[test]
fn explain_analyze_prints_measured_pipeline_and_blocking_memory() {
    let mut db = Database::new();
    for id in [2, 1, 3] {
        db.query(&format!("CREATE (:Memory {{id: {id}}})")).unwrap();
    }

    let output = db
        .explain_analyze_query("MATCH (m:Memory) RETURN m.id AS id ORDER BY id")
        .unwrap();
    assert_eq!(output.output.rows[0].get("id"), Some(&Value::Int(1)));
    assert_eq!(
        output.trace.selected_plan_cardinality_estimates.len(),
        output.execution_profile.operator_cardinality_profiles.len()
    );
    assert!(output
        .execution_profile
        .operator_cardinality_profiles
        .iter()
        .all(|cardinality| cardinality.actual_rows.is_some()));
    let rendered = output.to_string();

    assert!(rendered.contains("| actRows"));
    assert!(rendered.contains("output_rows=3"));
    assert!(rendered.contains("intermediate_rows="));
    assert!(rendered.contains("SortExec"));
    assert!(rendered.contains("peak="));
    assert!(rendered.contains("/budget="));
    assert!(rendered.contains("query_memory="));
    for line in rendered
        .lines()
        .filter(|line| line.starts_with('|') && line.contains("Exec"))
    {
        let cells = line.split('|').map(str::trim).collect::<Vec<_>>();
        assert_ne!(cells[2], "N/A");
        assert_ne!(cells[3], "N/A");
    }
}
