use super::*;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

const FRONTEND: &str = include_str!("../../../../fixtures/frontend_corpus_v1.jsonl");
const BINDINGS: &str = include_str!("../../../../fixtures/frontend_create_bindings_v1.json");

fn catalog() -> Snapshot {
    use SqlDataType::*;
    let mut tables = vec![
        table("memories", &[("id", BigInt)]),
        table("documents", &[("id", BigInt)]),
        table("entities", &[("id", BigInt), ("name", Text)]),
        table(
            "chunks",
            &[("id", BigInt), ("content", Text), ("rank", BigInt)],
        ),
    ];
    let mut mentions = table(
        "mentions",
        &[
            ("document_id", BigInt),
            ("entity_id", BigInt),
            ("confidence", DoublePrecision),
        ],
    );
    mentions.primary_key = vec!["document_id".into(), "entity_id".into()];
    mentions.columns[0].nullable = false;
    mentions.columns[1].nullable = false;
    mentions.foreign_keys = [("document_id", "documents"), ("entity_id", "entities")]
        .into_iter()
        .map(|(column, target)| PgqSourceForeignKeySchema {
            columns: vec![column.into()],
            referenced_table: vec!["public".into(), target.into()],
            referenced_columns: vec!["id".into()],
        })
        .collect();
    tables.push(mentions);
    tables.extend(snapshot().0.into_values());
    let mut no_key = table("no_key", &[("id", BigInt)]);
    no_key.primary_key.clear();
    tables.push(no_key);
    Snapshot(
        tables
            .into_iter()
            .map(|table| (table.name.clone(), table))
            .collect(),
    )
}

fn graph_json(graph: &PropertyGraphSchema) -> Value {
    let labels = |labels: &BTreeMap<String, PropertyGraphElementSchema>| {
        labels
            .iter()
            .map(|(name, label)| {
                (
                    name.clone(),
                    label
                        .properties
                        .iter()
                        .map(|(name, data_type)| (name.clone(), format!("{data_type:?}")))
                        .collect::<BTreeMap<_, _>>(),
                )
            })
            .collect::<BTreeMap<_, _>>()
    };
    json!({"name":graph.name,"vertex_labels":labels(&graph.vertex_labels),"edge_labels":labels(&graph.edge_labels)})
}

fn check_fixture(fixture: &Value) -> std::result::Result<(), String> {
    if fixture["protocol"] != "skein-pgq-create-corpus-v1"
        || fixture["postgres_revision"] != "3d00537feb565c410baf41bb301eee338e4b2317"
        || fixture["source_corpus_sha256"] != format!("{:x}", Sha256::digest(FRONTEND.as_bytes()))
    {
        return Err("creation corpus provenance drift".into());
    }
    let exclusions = fixture["profile_exclusions"]
        .as_array()
        .ok_or("missing profile exclusions")?;
    let expected = BTreeSet::from([
        "six-scalars",
        "creation-functions",
        "literal-syntax",
        "expression-overloads",
        "read-only-descriptor",
    ]);
    let actual = exclusions
        .iter()
        .map(|entry| entry["id"].as_str().ok_or("missing exclusion id"))
        .collect::<std::result::Result<BTreeSet<_>, _>>()?;
    if actual != expected
        || exclusions.len() != expected.len()
        || exclusions
            .iter()
            .any(|entry| entry["reason"].as_str().is_none_or(str::is_empty))
    {
        return Err("missing, duplicate or stale profile exclusion".into());
    }
    let cases: Vec<Value> = FRONTEND
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .filter(|case: &Value| case["family"] == "create_property_graph")
        .collect();
    let bindings = fixture["bindings"]
        .as_array()
        .ok_or("missing creation bindings")?;
    if bindings.len() != cases.len() {
        return Err("missing creation binding outcome".into());
    }
    let mut seen = BTreeSet::new();
    for binding in bindings {
        let id = binding["id"].as_str().ok_or("missing case id")?;
        if !seen.insert(id) {
            return Err("duplicate creation binding".into());
        }
        let case = cases
            .iter()
            .find(|case| case["id"] == id)
            .ok_or("unreferenced creation binding")?;
        let sql = case["sql"].as_str().ok_or("missing SQL")?;
        if binding["sql_sha256"] != format!("{:x}", Sha256::digest(sql.as_bytes())) {
            return Err("creation SQL drift".into());
        }
        let actual = match parse_postgres_statement(sql) {
            Err(error) => json!({"status":"parse_reject", "code":format!("{:?}",error.code)}),
            Ok(PostgresStatementSyntax::CreatePropertyGraph(create)) => {
                match bind_postgres_create_property_graph(sql, &create, &catalog()) {
                    Ok(graph) => json!({"status":"accept","schema":graph_json(&graph)}),
                    Err(error) => json!({"status":"bind_reject","code":format!("{:?}",error.code)}),
                }
            }
            Ok(_) => return Err("creation case changed statement family".into()),
        };
        if actual != binding["outcome"] {
            return Err(format!(
                "{id}: expected {}, got {actual}",
                binding["outcome"]
            ));
        }
    }
    let extra = fixture["additional_cases"]
        .as_array()
        .ok_or("missing additional binding cases")?;
    if extra.len() != 20 {
        return Err("missing or unreviewed additional binding case".into());
    }
    let mut families = BTreeSet::new();
    for case in extra {
        let id = case["id"].as_str().ok_or("missing additional id")?;
        if !seen.insert(id) {
            return Err("duplicate binding case".into());
        }
        let family = case["family"].as_str().ok_or("missing binding family")?;
        families.insert(family);
        if case["reference"].as_str().is_none_or(str::is_empty) {
            return Err("missing binding provenance".into());
        }
        let sql = case["sql"].as_str().ok_or("missing additional SQL")?;
        let actual = match parse_postgres_statement(sql) {
            Err(error) => json!({"status":"parse_reject", "code":format!("{:?}",error.code)}),
            Ok(PostgresStatementSyntax::CreatePropertyGraph(create)) => {
                match bind_postgres_create_property_graph(sql, &create, &catalog()) {
                    Ok(graph) => json!({"status":"accept","schema":graph_json(&graph)}),
                    Err(error) => json!({"status":"bind_reject","code":format!("{:?}",error.code)}),
                }
            }
            _ => return Err("binding case changed family".into()),
        };
        if actual != case["outcome"] {
            return Err(format!("{id}: expected {}, got {actual}", case["outcome"]));
        }
    }
    if families
        != BTreeSet::from([
            "elements",
            "keys",
            "endpoints",
            "labels",
            "properties",
            "profile",
        ])
    {
        return Err("missing or stale binding family".into());
    }
    Ok(())
}

#[test]
fn all_existing_creation_corpus_cases_have_bound_outcomes() {
    check_fixture(&serde_json::from_str(BINDINGS).unwrap()).unwrap();
}

#[test]
fn creation_corpus_rejects_coverage_provenance_profile_and_schema_drift() {
    let original: Value = serde_json::from_str(BINDINGS).unwrap();
    for mutation in 0..8 {
        let mut fixture = original.clone();
        match mutation {
            0 => {
                fixture["bindings"].as_array_mut().unwrap().pop();
            }
            1 => {
                fixture["bindings"][0]["sql_sha256"] = json!("wrong");
            }
            2 => {
                fixture["profile_exclusions"].as_array_mut().unwrap().pop();
            }
            3 => {
                fixture["bindings"][0]["outcome"]["schema"]["vertex_labels"] = json!({});
            }
            4 => {
                fixture["source_corpus_sha256"] = json!("wrong");
            }
            5 => {
                fixture["bindings"][1] = fixture["bindings"][0].clone();
            }
            6 => {
                fixture["additional_cases"] = json!([]);
            }
            _ => {
                fixture["additional_cases"].as_array_mut().unwrap().pop();
            }
        }
        assert!(check_fixture(&fixture).is_err(), "mutation {mutation}");
    }
}
