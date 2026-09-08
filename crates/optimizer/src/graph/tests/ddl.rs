use crate::CascadesOptimizer;
use skein_core::SkeinError;
use skein_plan::{
    LogicalPlan, PhysicalPlan, SchemaObjectState, SchemaPropertyType, SchemaTableKind,
};

const TABLE_KINDS: [(&str, SchemaTableKind, &str); 2] = [
    ("NODE", SchemaTableKind::Node, "node"),
    (
        "RELATIONSHIP",
        SchemaTableKind::Relationship,
        "relationship",
    ),
];
const PROPERTY_TYPES: [(&str, SchemaPropertyType, &str); 12] = [
    ("ANY", SchemaPropertyType::Any, "any"),
    ("BOOL", SchemaPropertyType::Bool, "bool"),
    ("BOOLEAN", SchemaPropertyType::Bool, "bool"),
    ("INT", SchemaPropertyType::Int, "int"),
    ("INTEGER", SchemaPropertyType::Int, "int"),
    ("FLOAT", SchemaPropertyType::Float, "float"),
    ("DOUBLE", SchemaPropertyType::Float, "float"),
    ("STRING", SchemaPropertyType::String, "string"),
    ("VARCHAR", SchemaPropertyType::String, "string"),
    ("CHARACTER VARYING", SchemaPropertyType::String, "string"),
    ("TEXT", SchemaPropertyType::Text, "text"),
    ("LIST", SchemaPropertyType::List, "list"),
];
const STATES: [(&str, SchemaObjectState, &str); 6] = [
    ("DELETE_ONLY", SchemaObjectState::DeleteOnly, "delete_only"),
    ("WRITE_ONLY", SchemaObjectState::WriteOnly, "write_only"),
    ("BACKFILL", SchemaObjectState::Backfill, "backfill"),
    ("VALIDATING", SchemaObjectState::Validating, "validating"),
    ("PUBLIC", SchemaObjectState::Public, "public"),
    ("GC", SchemaObjectState::Gc, "gc"),
];

#[derive(Debug, Default, PartialEq, Eq)]
struct Counts {
    accepted: usize,
    rejected: usize,
}

fn keywords(words: &str, case: usize, gap: &str) -> String {
    let words: String = words
        .chars()
        .enumerate()
        .map(|(index, ch)| match case {
            0 => ch,
            1 => ch.to_ascii_lowercase(),
            _ if index % 2 == 0 => ch,
            _ => ch.to_ascii_lowercase(),
        })
        .collect();
    words.split_ascii_whitespace().collect::<Vec<_>>().join(gap)
}

fn assert_pipeline(
    input: &str,
    invalid_keyword: &str,
    expected_logical: LogicalPlan,
    expected_physical: PhysicalPlan,
    expected_fingerprint: &str,
    counts: &mut Counts,
) {
    let statement = skein_cypher::parse(input).expect("valid DDL");
    let logical = skein_plan::plan(&statement).expect("valid DDL plan");
    assert_eq!(logical, expected_logical, "logical DDL: {input}");
    let physical = CascadesOptimizer::default().optimize(&logical);
    assert_eq!(physical, expected_physical, "physical DDL: {input}");
    assert_eq!(
        physical.instance_fingerprint(),
        expected_fingerprint,
        "DDL fingerprint: {input}"
    );
    counts.accepted += 1;

    for invalid in [invalid_keyword, &format!("{input} unexpected")] {
        assert!(
            matches!(skein_cypher::parse(invalid), Err(SkeinError::Parse(_))),
            "expected a parse rejection: {invalid}"
        );
        counts.rejected += 1;
    }
}

fn check_variant(case: usize, gap: &str, table: &str, property: &str, counts: &mut Counts) {
    let kw = |words| keywords(words, case, gap);
    let table_key = format!("{}:{table}", table.len());
    let property_key = format!("{}:{property}", property.len());
    for (kind_sql, table_kind, kind_name) in TABLE_KINDS {
        for (type_sql, value_type, type_name) in PROPERTY_TYPES {
            for nullable in [false, true] {
                let prefix = format!(
                    "{}{gap}{}{gap}{}{gap}{table}({property}){gap}{}{gap}",
                    kw("CREATE PROPERTY ON"),
                    kw(kind_sql),
                    kw("TABLE"),
                    kw("TYPE"),
                );
                let suffix = if nullable {
                    String::new()
                } else {
                    format!("{gap}{}", kw("NOT NULL"))
                };
                let input = format!("{prefix}{}{suffix}", kw(type_sql));
                let invalid = format!("{prefix}{}X{suffix}", kw(type_sql));
                let null_name = if nullable { "nullable" } else { "not_null" };
                assert_pipeline(
                    &input,
                    &invalid,
                    LogicalPlan::CreateProperty {
                        table_kind,
                        table: table.to_owned(),
                        property: property.to_owned(),
                        value_type,
                        nullable,
                    },
                    PhysicalPlan::CreateProperty {
                        table_kind,
                        table: table.to_owned(),
                        property: property.to_owned(),
                        value_type,
                        nullable,
                    },
                    &format!(
                        "CreateProperty({kind_name}:{table_key}.{property_key}:{type_name}:{null_name})"
                    ),
                    counts,
                );
            }
        }

        for (state_sql, state, state_name) in STATES {
            let prefix = format!(
                "{}{gap}{}{gap}{}{gap}{table}{gap}{}{gap}",
                kw("ALTER"),
                kw(kind_sql),
                kw("TABLE"),
                kw("SET STATE"),
            );
            let input = format!("{prefix}{}", kw(state_sql));
            let invalid = format!("{input}X");
            assert_pipeline(
                &input,
                &invalid,
                LogicalPlan::AlterTableState {
                    table_kind,
                    table: table.to_owned(),
                    state,
                },
                PhysicalPlan::AlterTableState {
                    table_kind,
                    table: table.to_owned(),
                    state,
                },
                &format!("AlterTableState({kind_name}:{table_key}:{state_name})"),
                counts,
            );

            let prefix = format!(
                "{}{gap}{}{gap}{}{gap}{table}({property}){gap}{}{gap}",
                kw("ALTER PROPERTY ON"),
                kw(kind_sql),
                kw("TABLE"),
                kw("SET STATE"),
            );
            let input = format!("{prefix}{}", kw(state_sql));
            let invalid = format!("{input}X");
            assert_pipeline(
                &input,
                &invalid,
                LogicalPlan::AlterPropertyState {
                    table_kind,
                    table: table.to_owned(),
                    property: property.to_owned(),
                    state,
                },
                PhysicalPlan::AlterPropertyState {
                    table_kind,
                    table: table.to_owned(),
                    property: property.to_owned(),
                    state,
                },
                &format!("AlterPropertyState({kind_name}:{table_key}.{property_key}:{state_name})"),
                counts,
            );
        }
    }
}

#[test]
fn ddl_command_pipeline_preserves_variants_and_fingerprints() {
    let mut counts = Counts::default();
    check_variant(0, " ", "records", "value", &mut counts);
    assert_eq!(
        counts,
        Counts {
            accepted: 72,
            rejected: 144
        }
    );
}

#[test]
#[ignore = "deterministic local DDL command pipeline campaign"]
fn ddl_command_pipeline_campaign() {
    let mut counts = Counts::default();
    for case in 0..3 {
        for gap in [" ", "\n\t", "\u{2003}"] {
            for (table, property) in [
                ("records", "value"),
                ("MiXeDTable", "MixedProperty"),
                ("_schema_42", "_field9"),
            ] {
                check_variant(case, gap, table, property, &mut counts);
            }
        }
    }
    assert_eq!(
        counts,
        Counts {
            accepted: 1_944,
            rejected: 3_888
        }
    );
    eprintln!(
        "DDL command campaign: {} complete plans and fingerprints, {} parse rejections",
        counts.accepted, counts.rejected
    );
}
