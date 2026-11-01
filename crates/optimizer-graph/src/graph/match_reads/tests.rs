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

use super::*;
use hawdb_core::{RelationshipDirection, Value};
use hawdb_plan_cypher::{
    Aggregation, GraphBindingImport, GraphMatchNode, Projection, ScalarBinaryOp, SortDirection,
};

fn node(variable: &str, properties: &[&str]) -> GraphMatchNode {
    GraphMatchNode {
        variable: variable.to_string(),
        label: String::new(),
        properties: properties
            .iter()
            .map(|property| (property.to_string(), Value::Int(1)))
            .collect(),
    }
}

fn expand(source: &str, relationship: Option<&str>, target: GraphMatchNode) -> GraphMatchStep {
    GraphMatchStep::Expand {
        source: source.to_string(),
        relationship: relationship.map(str::to_string),
        rel_type: "R".to_string(),
        properties: BTreeMap::new(),
        direction: RelationshipDirection::Outgoing,
        min_hops: 1,
        max_hops: 1,
        target,
    }
}

/// `MATCH (a {id})-[r]->(b)-[]->(c)` with every name introduced.
fn chain(predicate: Option<Predicate>) -> PhysicalPlan {
    PhysicalPlan::GraphMatchExec {
        program: GraphMatchProgram {
            imports: Vec::new(),
            introduced: ["a", "r", "b", "c"].map(String::from).to_vec(),
            projected_nodes: BTreeMap::new(),
            predicate_steps: Vec::new(),
            steps: vec![
                GraphMatchStep::Node(node("a", &["id"])),
                expand("a", Some("r"), node("b", &[])),
                expand("b", None, node("c", &[])),
            ],
            predicate,
            optional: false,
        },
        input: None,
    }
}

fn project(items: Vec<ProjectionExpression>, input: PhysicalPlan) -> PhysicalPlan {
    PhysicalPlan::ProjectExec {
        items: items
            .into_iter()
            .enumerate()
            .map(|(index, expression)| Projection {
                expression,
                name: format!("c{index}"),
            })
            .collect(),
        input: Box::new(input),
    }
}

fn property(variable: &str, property: &str) -> ProjectionExpression {
    ProjectionExpression::Property {
        variable: variable.to_string(),
        property: property.to_string(),
    }
}

fn variable(variable: &str) -> ProjectionExpression {
    ProjectionExpression::Variable {
        variable: variable.to_string(),
    }
}

/// `projected_nodes` of every `GraphMatchExec`, outermost first.
fn projections(plan: &PhysicalPlan) -> Vec<BTreeMap<String, Vec<String>>> {
    let mut found = Vec::new();
    let mut current = Some(plan);
    while let Some(plan) = current {
        current = match plan {
            PhysicalPlan::GraphMatchExec { program, input } => {
                found.push(
                    program
                        .projected_nodes
                        .iter()
                        .map(|(name, properties)| {
                            (name.clone(), properties.iter().cloned().collect())
                        })
                        .collect(),
                );
                input.as_deref()
            }
            PhysicalPlan::ProjectExec { input, .. }
            | PhysicalPlan::AggregateExec { input, .. }
            | PhysicalPlan::FilterExec { input, .. }
            | PhysicalPlan::SortExec { input, .. }
            | PhysicalPlan::TopNExec { input, .. }
            | PhysicalPlan::LimitExec { input, .. }
            | PhysicalPlan::DistinctExec { input } => Some(input),
            _ => None,
        };
    }
    found
}

fn analyzed(plan: PhysicalPlan) -> Vec<BTreeMap<String, Vec<String>>> {
    projections(&project_graph_match_reads(plan, &mut Vec::new()))
}

fn expected(entries: &[(&str, &[&str])]) -> BTreeMap<String, Vec<String>> {
    entries
        .iter()
        .map(|(name, properties)| {
            (
                name.to_string(),
                properties
                    .iter()
                    .map(|property| property.to_string())
                    .collect(),
            )
        })
        .collect()
}

#[test]
fn property_reads_and_pattern_filters_select_node_properties() {
    let plan = project(
        vec![property("b", "name"), property("c", "age")],
        chain(None),
    );
    // `a` keeps the pattern's `id`; `r` is a relationship and stays whole.
    assert_eq!(
        analyzed(plan),
        vec![expected(&[
            ("a", &["id"]),
            ("b", &["name"]),
            ("c", &["age"])
        ])]
    );
}

#[test]
fn whole_node_uses_read_every_property() {
    let returned = project(vec![variable("b")], chain(None));
    assert_eq!(
        analyzed(returned),
        vec![expected(&[("a", &["id"]), ("c", &[])])]
    );

    // `b = c` and `b IN [a, c]` compare whole node values.
    let equal = project(
        vec![],
        chain(Some(Predicate::ExpressionEq {
            expression: variable("b"),
            value: variable("c"),
        })),
    );
    assert_eq!(analyzed(equal), vec![expected(&[("a", &["id"])])]);
    let listed = project(
        vec![ProjectionExpression::Binary {
            left: Box::new(variable("b")),
            op: ScalarBinaryOp::In,
            right: Box::new(ProjectionExpression::List(vec![
                variable("a"),
                variable("c"),
            ])),
        }],
        chain(None),
    );
    assert_eq!(analyzed(listed), vec![BTreeMap::new()]);

    // A column of the same name may carry the whole node.
    let column = project(
        vec![ProjectionExpression::ColumnProperty {
            column: "c".into(),
            property: "age".into(),
        }],
        chain(None),
    );
    assert_eq!(
        analyzed(column),
        vec![expected(&[("a", &["id"]), ("b", &[])])]
    );
}

#[test]
fn aggregates_read_identity_for_count_and_whole_nodes_for_collect() {
    let aggregate = |function, target| Aggregation {
        function,
        target,
        distinct: false,
        name: "value".into(),
    };
    let plan = PhysicalPlan::AggregateExec {
        group_keys: vec![Projection {
            expression: property("a", "name"),
            name: "name".into(),
        }],
        items: vec![
            aggregate(
                AggregateFunction::Count,
                AggregateTarget::Variable("b".into()),
            ),
            aggregate(
                AggregateFunction::Collect,
                AggregateTarget::Variable("c".into()),
            ),
        ],
        input: Box::new(chain(None)),
    };
    assert_eq!(
        analyzed(plan),
        vec![expected(&[("a", &["id", "name"]), ("b", &[])])]
    );
}

#[test]
fn predicates_and_sort_keys_add_their_properties() {
    let filtered = project(
        vec![],
        chain(Some(Predicate::And(vec![
            Predicate::PropertyCompare {
                variable: "b".into(),
                property: "date".into(),
                op: hawdb_plan_cypher::ComparisonOp::Gt,
                value: Value::Int(3),
            },
            Predicate::IdEq {
                variable: "c".into(),
                value: Value::Int(7),
            },
        ]))),
    );
    let plan = PhysicalPlan::TopNExec {
        items: vec![SortItem {
            key: SortKey::Property {
                variable: "c".into(),
                property: "rank".into(),
            },
            direction: SortDirection::Asc,
        }],
        offset: 0,
        limit: 5,
        input: Box::new(filtered),
    };
    assert_eq!(
        analyzed(plan),
        vec![expected(&[
            ("a", &["id"]),
            ("b", &["date"]),
            ("c", &["rank"])
        ])]
    );
}

#[test]
fn unknown_operators_and_graph_roots_keep_full_reads() {
    // A root that returns graph bindings, not values.
    assert_eq!(analyzed(chain(None)), vec![BTreeMap::new()]);
    // An operator the analysis does not understand hides what it reads.
    let joined = project_graph_match_reads(
        project(
            vec![property("b", "name")],
            PhysicalPlan::NodeCartesianProductExec {
                left: Box::new(chain(None)),
                right: Box::new(chain(None)),
            },
        ),
        &mut Vec::new(),
    );
    let PhysicalPlan::ProjectExec { input, .. } = joined else {
        panic!("projection root");
    };
    let PhysicalPlan::NodeCartesianProductExec { left, right } = *input else {
        panic!("cartesian product");
    };
    assert_eq!(projections(&left), vec![BTreeMap::new()]);
    assert_eq!(projections(&right), vec![BTreeMap::new()]);
}

#[test]
fn a_later_match_passes_its_reads_to_the_match_that_bound_the_node() {
    // MATCH (a {id})-[r]->(b)-[]->(c) MATCH (b {kind})-[]->(d) RETURN b.name, d.x
    let outer = PhysicalPlan::GraphMatchExec {
        program: GraphMatchProgram {
            imports: Vec::new(),
            introduced: vec!["d".into()],
            projected_nodes: BTreeMap::new(),
            predicate_steps: Vec::new(),
            steps: vec![
                GraphMatchStep::Node(node("b", &["kind"])),
                expand("b", None, node("d", &[])),
            ],
            predicate: None,
            optional: false,
        },
        input: Some(Box::new(chain(None))),
    };
    let plan = project(vec![property("b", "name"), property("d", "x")], outer);
    assert_eq!(
        analyzed(plan),
        vec![
            expected(&[("d", &["x"])]),
            expected(&[("a", &["id"]), ("b", &["kind", "name"]), ("c", &[])]),
        ]
    );
}

#[test]
fn imports_and_reintroduced_names_stop_demand_at_their_match() {
    // MATCH ... WITH b AS column ... MATCH (b)-[]->(c): `b` is imported and
    // `c` introduced again, so the inner match sees neither demand.
    let outer = PhysicalPlan::GraphMatchExec {
        program: GraphMatchProgram {
            imports: vec![GraphBindingImport {
                variable: "b".into(),
                column: "b_column".into(),
                kind: GraphEntityKind::Node,
            }],
            introduced: vec!["c".into()],
            projected_nodes: BTreeMap::new(),
            predicate_steps: Vec::new(),
            steps: vec![
                GraphMatchStep::Node(node("b", &[])),
                expand("b", None, node("c", &["tag"])),
            ],
            predicate: None,
            optional: true,
        },
        input: Some(Box::new(project(
            vec![ProjectionExpression::Id {
                variable: "b".into(),
            }],
            chain(None),
        ))),
    };
    let plan = project(vec![property("b", "name"), variable("c")], outer);
    assert_eq!(
        analyzed(plan),
        vec![
            expected(&[("b", &["name"])]),
            expected(&[("a", &["id"]), ("b", &[]), ("c", &[])]),
        ]
    );
}

#[test]
fn nested_temporal_components_and_maps_keep_every_source_property() {
    let temporal = ProjectionExpression::PropertyAccess {
        expression: Box::new(ProjectionExpression::TemporalFunction {
            function: hawdb_core::TemporalFunction::DurationBetween,
            arguments: vec![property("b", "started"), property("c", "finished")],
        }),
        key: "days".into(),
    };
    let map = ProjectionExpression::MapLiteral(vec![
        ("elapsed".into(), temporal),
        ("caption".into(), property("b", "name")),
    ]);
    let mut items = vec![map];
    items.extend(hawdb_core::ConversionFunction::ALL.map(|function| {
        ProjectionExpression::Conversion {
            function,
            argument: Box::new(property("b", "converted")),
        }
    }));
    assert_eq!(
        analyzed(project(items, chain(None))),
        vec![expected(&[
            ("a", &["id"]),
            ("b", &["converted", "name", "started"]),
            ("c", &["finished"]),
        ])]
    );
}

#[test]
fn whole_nodes_nested_in_maps_keep_full_reads() {
    let map = ProjectionExpression::MapLiteral(vec![
        ("node".into(), variable("b")),
        ("property".into(), property("c", "name")),
    ]);
    assert_eq!(
        analyzed(project(vec![map], chain(None))),
        vec![expected(&[("a", &["id"]), ("c", &["name"])])]
    );
}
