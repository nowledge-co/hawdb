// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Binding `LOAD CSV` row sources and the per-row mutations they feed.

use super::*;
use hawdb_core::ConversionFunction;

fn plan_query(query: &str, parameters: &BTreeMap<String, Value>) -> Result<LogicalPlan> {
    plan_with_params(&hawdb_cypher::parse(query)?, parameters)
}

fn plan(query: &str) -> LogicalPlan {
    plan_query(query, &BTreeMap::new()).unwrap()
}

fn find_load_csv(plan: &LogicalPlan) -> Option<&LoadCsvSource> {
    match plan {
        LogicalPlan::LoadCsv { source } => Some(source),
        LogicalPlan::GraphMatch { input, .. } => input.as_deref().and_then(find_load_csv),
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. } => find_load_csv(input),
        _ => None,
    }
}

fn row(property: &str) -> BatchMutationValue {
    BatchMutationValue::RowProperty(property.to_string())
}

fn to_integer(input: BatchMutationValue) -> BatchMutationValue {
    BatchMutationValue::Converted {
        function: ConversionFunction::ToInteger,
        input: Box::new(input),
    }
}

#[test]
fn read_pipelines_bind_a_parameter_source_leaf_without_its_value() {
    // The parameter value is resolved at execution; planning never reads it.
    let plan_without_value =
        plan("LOAD CSV FROM $f AS line RETURN line[0] AS first, size(line) AS n");
    let source =
        find_load_csv(&plan_without_value).unwrap_or_else(|| panic!("{plan_without_value:?}"));
    assert_eq!(
        source,
        &LoadCsvSource {
            location: CsvLocation::Parameter("f".to_string()),
            with_headers: false,
            field_terminator: b',',
            variable: "line".to_string(),
        }
    );
    assert!(format!("{plan_without_value:?}").contains("ListIndex"));
    let plan_with_value = plan_query(
        "LOAD CSV FROM $f AS line RETURN line[0] AS first, size(line) AS n",
        &BTreeMap::from([("f".to_string(), Value::String("/a.csv".to_string()))]),
    )
    .unwrap();
    assert_eq!(plan_with_value, plan_without_value);

    let plan = plan(
        "LOAD CSV WITH HEADERS FROM 'people.csv' AS row FIELDTERMINATOR '\\t' \
         WITH row WHERE toInteger(row.age) > 30 RETURN count(*) AS c",
    );
    let source = find_load_csv(&plan).unwrap_or_else(|| panic!("{plan:?}"));
    assert_eq!(
        source.location,
        CsvLocation::Literal("people.csv".to_string())
    );
    assert!(source.with_headers);
    assert_eq!(source.field_terminator, b'\t');
    assert!(format!("{plan:?}").contains("Conversion"), "{plan:?}");
}

#[test]
fn node_creation_binds_row_values_and_folds_static_conversions() {
    let query = "LOAD CSV WITH HEADERS FROM $f AS row FIELDTERMINATOR '|' \
         CREATE (:Person {id: toInteger(row.id), firstName: row.firstName, \
         rank: toInteger('7'), source: $source})";
    let error = plan_query(query, &BTreeMap::new()).unwrap_err();
    assert!(
        error.to_string().contains("missing parameter '$source'"),
        "{error}"
    );
    let plan = plan_query(
        query,
        &BTreeMap::from([("source".to_string(), Value::String("ldbc".to_string()))]),
    )
    .unwrap();
    let LogicalPlan::LoadCsvMutation { source, operation } = plan else {
        panic!("{plan:?}");
    };
    assert_eq!(source.location, CsvLocation::Parameter("f".to_string()));
    assert_eq!(source.field_terminator, b'|');
    assert_eq!(
        operation,
        BatchMutationOperation::CreateNode {
            label: "Person".to_string(),
            properties: BTreeMap::from([
                ("firstName".to_string(), row("firstName")),
                ("id".to_string(), to_integer(row("id"))),
                (
                    "rank".to_string(),
                    BatchMutationValue::Static(Value::Int(7))
                ),
                (
                    "source".to_string(),
                    BatchMutationValue::Static(Value::String("ldbc".to_string()))
                ),
            ]),
        }
    );
}

#[test]
fn matched_endpoints_feed_one_relationship_create_per_row() {
    let plan = plan(
        "LOAD CSV WITH HEADERS FROM $f AS row FIELDTERMINATOR '|' \
         MATCH (a:Person {id: toInteger(row.src)}), (b:Person {id: toInteger(row.dst)}) \
         CREATE (a)-[:KNOWS {creationDate: toInteger(row.creationDate)}]->(b)",
    );
    let LogicalPlan::LoadCsvMutation { operation, .. } = plan else {
        panic!("{plan:?}");
    };
    assert_eq!(
        operation,
        BatchMutationOperation::CreateMatchedRelationship(Box::new(BatchMatchedRelationship {
            source_label: "Person".to_string(),
            source_properties: BTreeMap::from([("id".to_string(), to_integer(row("src")))]),
            target_label: "Person".to_string(),
            target_properties: BTreeMap::from([("id".to_string(), to_integer(row("dst")))]),
            rel_type: "KNOWS".to_string(),
            rel_properties: BTreeMap::from([(
                "creationDate".to_string(),
                to_integer(row("creationDate"))
            )]),
        }))
    );
}

#[test]
fn list_rows_bind_indexes_and_merge_on_create_values() {
    let plan = plan(
        "LOAD CSV FROM $f AS line MERGE (c:City {name: line[1]}) \
         ON CREATE SET c.code = toInteger(line[-1])",
    );
    let LogicalPlan::LoadCsvMutation { operation, .. } = plan else {
        panic!("{plan:?}");
    };
    assert_eq!(
        operation,
        BatchMutationOperation::MergeNode {
            label: "City".to_string(),
            match_properties: BTreeMap::from([(
                "name".to_string(),
                BatchMutationValue::RowIndex(1)
            )]),
            on_create_properties: BTreeMap::from([(
                "code".to_string(),
                to_integer(BatchMutationValue::RowIndex(-1))
            )]),
        }
    );
}

#[test]
fn unwind_shares_the_matched_relationship_row_mutation() {
    let parameters = BTreeMap::from([(
        "rows".to_string(),
        Value::List(vec![Value::Map(BTreeMap::from([
            ("src".to_string(), Value::Int(1)),
            ("dst".to_string(), Value::Int(2)),
        ]))]),
    )]);
    let plan = plan_query(
        "UNWIND $rows AS r MATCH (a:P {id: r.src}), (b:P {id: r.dst}) CREATE (a)-[:R]->(b)",
        &parameters,
    )
    .unwrap();
    let LogicalPlan::UnwindMutation {
        rows, operation, ..
    } = plan
    else {
        panic!("{plan:?}");
    };
    assert_eq!(rows.len(), 1);
    assert!(matches!(
        operation,
        BatchMutationOperation::CreateMatchedRelationship(_)
    ));
}

#[test]
fn unsupported_compositions_fail_with_semantic_errors() {
    for (query, message) in [
        (
            "MATCH (n:P) LOAD CSV FROM $f AS line RETURN line",
            "only as the first clause",
        ),
        (
            "LOAD CSV FROM $f AS line LOAD CSV FROM $f AS other RETURN line",
            "only as the first clause",
        ),
        (
            "LOAD CSV FROM $f AS line WITH line CREATE (:P {v: line[0]})",
            "exactly one following mutation clause",
        ),
        (
            "LOAD CSV FROM $f AS line OPTIONAL MATCH (a:P {id: line[0]}) CREATE (:Q)",
            "do not support OPTIONAL MATCH",
        ),
        (
            "LOAD CSV FROM $f AS line MATCH (a:P), (b:P) WHERE a.id = 1 CREATE (a)-[:R]->(b)",
            "does not support WHERE",
        ),
        (
            "LOAD CSV FROM $f AS line MATCH (a:P {id: line[0]}) SET a.v = line[1]",
            "SET and DELETE require a bound mutation target",
        ),
        (
            "LOAD CSV FROM $f AS line MATCH (a:P), (b:P) MERGE (a)-[:R]->(b)",
            "MERGE currently supports one node pattern",
        ),
        (
            "LOAD CSV FROM $f AS line CREATE (:P {v: other.x})",
            "does not reference row variable 'line'",
        ),
        (
            "LOAD CSV FROM $f AS line CREATE (:P {v: line[0]})-[:R]->(:Q)",
            "one node pattern or a relationship between matched nodes",
        ),
        (
            "CREATE (:P {v: line[0]})",
            "requires an UNWIND or LOAD CSV mutation context",
        ),
    ] {
        let error = plan_query(query, &BTreeMap::new()).unwrap_err();
        assert!(
            matches!(error, HawDBError::Semantic(_)),
            "{query}: {error:?}"
        );
        assert!(error.to_string().contains(message), "{query}: {error}");
    }
}

#[test]
fn load_csv_plans_report_their_retained_heap_bytes() {
    use hawdb_core::RetainedHeapBytes;
    use std::mem::size_of;
    let source = LoadCsvSource {
        location: CsvLocation::Literal("people.csv".to_string()),
        with_headers: true,
        field_terminator: b',',
        variable: "row".to_string(),
    };
    let CsvLocation::Literal(path) = &source.location else {
        unreachable!();
    };
    let source_bytes = path.capacity() + source.variable.capacity();
    assert_eq!(source.retained_heap_bytes(), Some(source_bytes));
    assert_eq!(
        crate::PhysicalPlan::LoadCsvExec {
            source: source.clone()
        }
        .retained_heap_bytes(),
        Some(source_bytes)
    );
    let converted = to_integer(row("id"));
    let BatchMutationValue::Converted { input, .. } = &converted else {
        unreachable!();
    };
    // The boxed input's slot and its property name.
    let converted_bytes = size_of::<BatchMutationValue>() + "id".len();
    assert_eq!(input.as_ref().retained_heap_bytes(), Some("id".len()));
    assert_eq!(converted.retained_heap_bytes(), Some(converted_bytes));
    let operation =
        BatchMutationOperation::CreateMatchedRelationship(Box::new(BatchMatchedRelationship {
            source_label: "Person".to_string(),
            source_properties: BTreeMap::from([("id".to_string(), converted)]),
            target_label: String::new(),
            target_properties: BTreeMap::new(),
            rel_type: "KNOWS".to_string(),
            rel_properties: BTreeMap::from([("i".to_string(), BatchMutationValue::RowIndex(2))]),
        }));
    let mutation = crate::PhysicalPlan::LoadCsvMutation {
        source,
        operation: operation.clone(),
    };
    let operation_bytes = operation.retained_heap_bytes().unwrap();
    assert!(operation_bytes > size_of::<BatchMatchedRelationship>() + converted_bytes);
    assert_eq!(
        mutation.retained_heap_bytes(),
        Some(source_bytes + operation_bytes)
    );
}
