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

use std::collections::BTreeMap;

use hawdb_core::{RelationshipDirection, Value};
use hawdb_plan::{LogicalPlan, ProjectionExpression};
use hawdb_sql_syntax::parse_postgres_select;

use super::*;
use crate::pgq::{
    bind_postgres_graph_tables, BoundPgqGraphTable, PgqBindingContext, PgqCatalog, PgqDataType,
    PropertyGraphCatalog, PropertyGraphElementSchema, PropertyGraphSchema,
};

fn catalog() -> PropertyGraphCatalog {
    let mut graph = PropertyGraphSchema::new(["knowledge"]);
    graph.add_vertex_label(
        "document",
        PropertyGraphElementSchema::default()
            .with_property("id", PgqDataType::Int64)
            .with_property("visible", PgqDataType::Boolean),
    );
    graph.add_vertex_label(
        "entity",
        PropertyGraphElementSchema::default()
            .with_property("id", PgqDataType::Int64)
            .with_property("name", PgqDataType::String),
    );
    graph.add_edge_label(
        "mentions",
        PropertyGraphElementSchema::default().with_property("confidence", PgqDataType::Float64),
    );
    let mut catalog = PropertyGraphCatalog::default();
    catalog.insert(graph);
    catalog
}

fn bind(sql: &str, catalog: &dyn PgqCatalog) -> BoundPgqGraphTable {
    let select = parse_postgres_select(sql).expect("owned SQL/PGQ syntax");
    bind_postgres_graph_tables(sql, &select, catalog, &PgqBindingContext::default())
        .expect("bound GRAPH_TABLE")
        .remove(0)
}

#[test]
fn lowers_single_hop_into_shared_graph_operators() {
    let sql = "SELECT matched.name FROM GRAPH_TABLE (
        knowledge
        MATCH (document IS document WHERE document.visible)
              -[mention IS mentions WHERE mention.confidence >= $1]->
              (entity IS entity)
        WHERE entity.id IN (7, 9)
        COLUMNS (lower(entity.name) AS name, mention.confidence)
    ) matched";
    let bound = bind(sql, &catalog());
    let plan = lower_bound_pgq_graph_table(&bound, &BTreeMap::from([(1, Value::Float(0.75))]))
        .expect("qualified SQL/PGQ lowering");

    let LogicalPlan::Project { items, input } = plan else {
        panic!("expected shared projection");
    };
    assert_eq!(items.len(), 2);
    assert!(matches!(
        items[0].expression,
        ProjectionExpression::Lower(_)
    ));
    let LogicalPlan::Filter { input, .. } = *input else {
        panic!("expected graph-level filter");
    };
    let LogicalPlan::Filter { input, .. } = *input else {
        panic!("expected edge predicate filter");
    };
    let LogicalPlan::Expand {
        source_variable,
        source_label,
        rel_type,
        target_variable,
        target_label,
        direction,
        input,
        ..
    } = *input
    else {
        panic!("expected shared adjacency expansion");
    };
    assert_eq!(source_variable, "document");
    assert_eq!(source_label, "document");
    assert_eq!(rel_type, "mentions");
    assert_eq!(target_variable, "entity");
    assert_eq!(target_label, "entity");
    assert_eq!(direction, RelationshipDirection::Outgoing);
    assert!(matches!(*input, LogicalPlan::Filter { .. }));
}

#[test]
fn synthesizes_deterministic_names_for_anonymous_elements() {
    let bound = bind(
        "SELECT * FROM GRAPH_TABLE (
            knowledge MATCH (IS document)-[IS mentions]->(IS entity)
            COLUMNS (1 AS one)
        ) graph_rows",
        &catalog(),
    );
    let plan =
        lower_bound_pgq_graph_table(&bound, &BTreeMap::new()).expect("anonymous slot lowering");
    let LogicalPlan::Project { input, .. } = plan else {
        panic!("expected projection");
    };
    let LogicalPlan::Expand {
        source_variable,
        rel_variable,
        target_variable,
        ..
    } = *input
    else {
        panic!("expected expansion");
    };
    assert_eq!(source_variable, "__hawdb_pgq_slot_0");
    assert_eq!(rel_variable.as_deref(), Some("__hawdb_pgq_slot_1"));
    assert_eq!(target_variable, "__hawdb_pgq_slot_2");
}

#[test]
fn rejects_unbound_parameters_before_planning() {
    let bound = bind(
        "SELECT * FROM GRAPH_TABLE (
            knowledge MATCH (entity IS entity WHERE entity.id = $1)
            COLUMNS (entity.id)
        ) graph_rows",
        &catalog(),
    );
    let error = lower_bound_pgq_graph_table(&bound, &BTreeMap::new())
        .expect_err("unbound parameter must fail closed");
    assert_eq!(error.code, PgqLoweringErrorCode::MissingParameter);
    assert!(!error.span.is_empty());
}

#[test]
fn rejects_quantified_paths_until_walk_semantics_are_qualified() {
    let bound = bind(
        "SELECT * FROM GRAPH_TABLE (
            knowledge MATCH (document IS document)-[IS mentions]->{1,2}(entity IS entity)
            COLUMNS (entity.id)
        ) graph_rows",
        &catalog(),
    );
    let error = lower_bound_pgq_graph_table(&bound, &BTreeMap::new())
        .expect_err("unqualified path semantics must fail closed");
    assert_eq!(error.code, PgqLoweringErrorCode::UnsupportedPath);
    assert!(!error.span.is_empty());
}

#[test]
fn rejects_multi_label_elements_until_shared_scan_supports_them() {
    let mut graph = PropertyGraphSchema::new(["knowledge"]);
    let element = PropertyGraphElementSchema::default().with_property("id", PgqDataType::Int64);
    graph.add_vertex_label("entity", element.clone());
    graph.add_vertex_label("document", element);
    let mut catalog = PropertyGraphCatalog::default();
    catalog.insert(graph);
    let bound = bind(
        "SELECT * FROM GRAPH_TABLE (
            knowledge MATCH (item IS entity | document) COLUMNS (item.id)
        ) graph_rows",
        &catalog,
    );
    let error = lower_bound_pgq_graph_table(&bound, &BTreeMap::new())
        .expect_err("multi-label scan must fail closed");
    assert_eq!(error.code, PgqLoweringErrorCode::UnsupportedLabels);
}

#[test]
fn rejects_reused_path_variables_until_identity_constraints_are_explicit() {
    let bound = bind(
        "SELECT * FROM GRAPH_TABLE (
            knowledge MATCH (entity IS entity)-[IS mentions]->(entity)
            COLUMNS (entity.id)
        ) graph_rows",
        &catalog(),
    );
    let error = lower_bound_pgq_graph_table(&bound, &BTreeMap::new())
        .expect_err("reused variables must not silently become an overwrite");
    assert_eq!(error.code, PgqLoweringErrorCode::UnsupportedPath);
}

#[test]
fn rejects_correlated_columns_until_relational_and_graph_plans_are_joined() {
    let sql = "SELECT * FROM source, GRAPH_TABLE (
        knowledge MATCH (entity IS entity WHERE entity.id = source.id)
        COLUMNS (entity.id)
    ) graph_rows";
    let select = parse_postgres_select(sql).expect("owned SQL/PGQ syntax");
    let context =
        PgqBindingContext::default().with_outer_column(["source", "id"], PgqDataType::Int64);
    let bound = bind_postgres_graph_tables(sql, &select, &catalog(), &context)
        .expect("correlated column binding")
        .remove(0);
    let error = lower_bound_pgq_graph_table(&bound, &BTreeMap::new())
        .expect_err("host-side correlated evaluation must not be introduced");
    assert_eq!(error.code, PgqLoweringErrorCode::UnsupportedExpression);
    assert!(!error.span.is_empty());
}
