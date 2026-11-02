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
    ComparisonOp, GraphBindingImport, GraphEntityKind, GraphMatchNode, GraphMatchShortestPath,
    ListComprehensionProjection, PathFunction, ScalarBinaryOp, ShortestPathSelection,
};

fn node(variable: &str) -> GraphMatchNode {
    GraphMatchNode {
        variable: variable.to_string(),
        label: String::new(),
        properties: BTreeMap::new(),
    }
}

fn expand(
    source: &str,
    relationship: Option<&str>,
    target: &str,
    hops: (usize, usize),
) -> GraphMatchStep {
    GraphMatchStep::Expand {
        source: source.to_string(),
        relationship: relationship.map(str::to_string),
        rel_type: "R".to_string(),
        properties: BTreeMap::new(),
        direction: RelationshipDirection::Outgoing,
        min_hops: hops.0,
        max_hops: hops.1,
        target: node(target),
    }
}

/// `(a)-[r:R]->(b)-[:R]->(c)` with `a`, `r`, `b` and `c` introduced.
fn chain() -> GraphMatchProgram {
    program(
        &[],
        &["a", "r", "b", "c"],
        vec![
            GraphMatchStep::Node(node("a")),
            expand("a", Some("r"), "b", (1, 1)),
            expand("b", None, "c", (1, 1)),
        ],
    )
}

fn program(imports: &[&str], introduced: &[&str], steps: Vec<GraphMatchStep>) -> GraphMatchProgram {
    GraphMatchProgram {
        imports: imports
            .iter()
            .map(|variable| GraphBindingImport {
                variable: variable.to_string(),
                column: format!("__column_{variable}"),
                kind: GraphEntityKind::Node,
            })
            .collect(),
        introduced: introduced.iter().map(|name| name.to_string()).collect(),
        projected_nodes: BTreeMap::new(),
        steps,
        predicate: None,
        predicate_steps: Vec::new(),
        optional: false,
    }
}

fn property_eq(variable: &str, value: i64) -> Predicate {
    Predicate::PropertyEq {
        variable: variable.to_string(),
        property: "x".to_string(),
        value: Value::Int(value),
    }
}

fn property(variable: &str) -> ProjectionExpression {
    ProjectionExpression::Property {
        variable: variable.to_string(),
        property: "x".to_string(),
    }
}

fn place(
    mut program: GraphMatchProgram,
    predicate: Predicate,
) -> (GraphMatchProgram, Option<String>) {
    program.predicate = Some(predicate);
    let decision = place_graph_match_predicates(&mut program);
    (program, decision)
}

#[test]
fn single_variable_conjuncts_follow_the_step_that_binds_them() {
    let (program, decision) = place(
        chain(),
        Predicate::And(vec![
            property_eq("c", 3),
            property_eq("a", 1),
            property_eq("b", 2),
        ]),
    );
    assert_eq!(program.predicate_steps, vec![3, 1, 2]);
    assert_eq!(program.predicate_conjuncts().len(), 3);
    assert_eq!(
        decision.as_deref(),
        Some("evaluate 2 of 3 GraphMatchExec WHERE conjunct(s) before the last step: after steps [3, 1, 2] of 3")
    );
}

#[test]
fn relationship_variable_is_bound_by_its_expand_step() {
    let (program, _) = place(
        chain(),
        Predicate::And(vec![property_eq("r", 1), property_eq("c", 1)]),
    );
    assert_eq!(program.predicate_steps, vec![2, 3]);
}

#[test]
fn multi_variable_conjunct_waits_for_its_last_variable() {
    let (program, _) = place(
        chain(),
        Predicate::And(vec![
            Predicate::ExpressionEq {
                expression: property("a"),
                value: property("b"),
            },
            property_eq("c", 1),
        ]),
    );
    assert_eq!(program.predicate_steps, vec![2, 3]);
}

#[test]
fn imported_and_outer_variables_are_bound_before_the_first_step() {
    // MATCH (p)<-[:R]-(m)-[:R]->(c) with `p` imported from an earlier WITH and
    // `k` an ordinary column of the input row.
    let imported = program(
        &["p"],
        &["m", "c"],
        vec![
            GraphMatchStep::Node(node("p")),
            expand("p", None, "m", (1, 1)),
            expand("m", None, "c", (1, 1)),
        ],
    );
    let (program, _) = place(
        imported,
        Predicate::And(vec![
            property_eq("p", 1),
            Predicate::ExpressionCompare {
                expression: ProjectionExpression::Column("k".to_string()),
                op: ComparisonOp::Gt,
                value: property("m"),
            },
            property_eq("c", 1),
            Predicate::ExpressionEq {
                expression: ProjectionExpression::Column("k".to_string()),
                value: ProjectionExpression::Literal(Value::Int(1)),
            },
        ]),
    );
    assert_eq!(program.predicate_steps, vec![0, 2, 3, 0]);
}

#[test]
fn variable_length_target_waits_until_the_trail_completes() {
    let bounded = program(
        &[],
        &["a", "b", "c"],
        vec![
            GraphMatchStep::Node(node("a")),
            expand("a", None, "b", (1, 3)),
            expand("b", None, "c", (1, 1)),
        ],
    );
    let (program, _) = place(
        bounded,
        Predicate::And(vec![property_eq("b", 1), property_eq("a", 1)]),
    );
    assert_eq!(program.predicate_steps, vec![2, 1]);
}

#[test]
fn path_conjunct_waits_for_its_shortest_path_step() {
    let paths = program(
        &[],
        &["a", "b", "p"],
        vec![
            GraphMatchStep::Node(node("a")),
            GraphMatchStep::Node(node("b")),
            GraphMatchStep::ShortestPath(Box::new(GraphMatchShortestPath {
                path: "p".to_string(),
                source: "a".to_string(),
                target: "b".to_string(),
                rel_type: "R".to_string(),
                direction: RelationshipDirection::Outgoing,
                min_hops: 1,
                max_hops: 4,
                bounded: true,
                selection: ShortestPathSelection::Any,
            })),
            GraphMatchStep::Node(node("c")),
        ],
    );
    let (program, _) = place(
        paths,
        Predicate::And(vec![
            Predicate::ExpressionCompare {
                expression: ProjectionExpression::Path {
                    function: PathFunction::Length,
                    column: "p".to_string(),
                },
                op: ComparisonOp::Lt,
                value: ProjectionExpression::Literal(Value::Int(3)),
            },
            property_eq("a", 1),
        ]),
    );
    assert_eq!(program.predicate_steps, vec![3, 1]);
}

#[test]
fn disjunction_and_negation_are_placed_whole() {
    let (program, _) = place(
        chain(),
        Predicate::And(vec![
            Predicate::Or(vec![property_eq("a", 1), property_eq("b", 2)]),
            Predicate::Not(Box::new(Predicate::And(vec![
                property_eq("a", 1),
                property_eq("r", 2),
            ]))),
            Predicate::Not(Box::new(property_eq("a", 3))),
        ]),
    );
    assert_eq!(program.predicate_steps, vec![2, 2, 1]);
    assert!(matches!(program.predicate_conjuncts()[0], Predicate::Or(_)));
}

#[test]
fn null_tests_and_constant_conjuncts_are_placed_by_their_variables() {
    let (program, _) = place(
        chain(),
        Predicate::And(vec![
            Predicate::PropertyIsNull {
                variable: "b".to_string(),
                property: "x".to_string(),
            },
            Predicate::ExpressionEq {
                expression: ProjectionExpression::Literal(Value::Null),
                value: ProjectionExpression::Literal(Value::Int(1)),
            },
            Predicate::ConstantBool(false),
        ]),
    );
    assert_eq!(program.predicate_steps, vec![2, 0, 0]);
}

#[test]
fn list_comprehension_reads_its_list_and_outer_variables_only() {
    let comprehension =
        ProjectionExpression::ListComprehension(Box::new(ListComprehensionProjection {
            variable: "c".to_string(),
            list: property("a"),
            predicate: Some(ProjectionExpression::Binary {
                left: Box::new(ProjectionExpression::LocalVariable {
                    name: "c".to_string(),
                    property: None,
                }),
                op: ScalarBinaryOp::Gt,
                right: Box::new(property("r")),
            }),
            projection: None,
        }));
    let (program, _) = place(
        chain(),
        Predicate::And(vec![
            Predicate::ExpressionNotEq {
                expression: ProjectionExpression::ListFunction {
                    function: hawdb_core::ListFunction::Size,
                    arguments: vec![comprehension],
                },
                value: ProjectionExpression::Literal(Value::Int(0)),
            },
            property_eq("c", 1),
        ]),
    );
    // The comprehension's `c` is its element, not the pattern's `c`.
    assert_eq!(program.predicate_steps, vec![2, 3]);
}

#[test]
fn nested_conjunctions_are_flattened_in_order() {
    let (program, _) = place(
        chain(),
        Predicate::And(vec![
            Predicate::And(vec![property_eq("a", 1), property_eq("c", 1)]),
            property_eq("b", 1),
        ]),
    );
    assert_eq!(program.predicate_steps, vec![1, 3, 2]);
    assert_eq!(
        program.predicate,
        Some(Predicate::And(vec![
            property_eq("a", 1),
            property_eq("c", 1),
            property_eq("b", 1),
        ]))
    );
}

#[test]
fn a_single_early_predicate_is_its_own_conjunct() {
    let (program, _) = place(chain(), property_eq("a", 1));
    assert_eq!(program.predicate_steps, vec![1]);
    assert_eq!(program.predicate, Some(property_eq("a", 1)));
}

#[test]
fn predicates_that_can_only_run_last_leave_the_program_unchanged() {
    let predicate = Predicate::And(vec![Predicate::And(vec![
        property_eq("c", 1),
        Predicate::ExpressionEq {
            expression: property("a"),
            value: property("c"),
        },
    ])]);
    let (program, decision) = place(chain(), predicate.clone());
    assert!(program.predicate_steps.is_empty());
    assert_eq!(program.predicate, Some(predicate));
    assert_eq!(decision, None);
}

#[test]
fn an_introduced_variable_no_step_binds_keeps_its_conjunct_last() {
    let mut unbound = chain();
    unbound.introduced.push("ghost".to_string());
    let (program, _) = place(
        unbound,
        Predicate::And(vec![property_eq("ghost", 1), property_eq("a", 1)]),
    );
    assert_eq!(program.predicate_steps, vec![3, 1]);
}

#[test]
fn placement_is_recomputed_from_the_current_step_order() {
    let (mut program, _) = place(
        chain(),
        Predicate::And(vec![property_eq("a", 1), property_eq("c", 1)]),
    );
    // A later step reordering (for example anchoring at `c`) recomputes it.
    program.steps = vec![
        GraphMatchStep::Node(node("c")),
        GraphMatchStep::Node(node("a")),
        expand("a", Some("r"), "b", (1, 1)),
    ];
    place_graph_match_predicates(&mut program);
    assert_eq!(program.predicate_steps, vec![2, 1]);
}

#[test]
fn temporal_and_map_conjuncts_wait_for_all_nested_variables() {
    let temporal = ProjectionExpression::TemporalFunction {
        function: hawdb_core::TemporalFunction::DurationBetween,
        arguments: vec![property("a"), property("c")],
    };
    let component = ProjectionExpression::PropertyAccess {
        expression: Box::new(temporal.clone()),
        key: "days".into(),
    };
    let map = ProjectionExpression::MapLiteral(vec![
        ("first".into(), property("b")),
        ("second".into(), component.clone()),
    ]);
    let conversions =
        hawdb_core::ConversionFunction::ALL.map(|function| ProjectionExpression::Conversion {
            function,
            argument: Box::new(property("c")),
        });
    for expression in [temporal, component, map].into_iter().chain(conversions) {
        let (program, _) = place(
            chain(),
            Predicate::And(vec![
                Predicate::ExpressionEq {
                    expression,
                    value: ProjectionExpression::Literal(Value::Null),
                },
                property_eq("a", 1),
            ]),
        );
        assert_eq!(program.predicate_steps, vec![3, 1]);
    }
}

#[test]
fn output_cardinality_is_independent_of_conjunct_placement() {
    use crate::graph::costing::estimate_physical_plan_cost_breakdown;
    use crate::graph::{OptimizerCatalog, OptimizerCatalogIndexes, OptimizerCatalogStatistics};
    use hawdb_plan_cypher::PhysicalPlan;

    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::default(),
        OptimizerCatalogStatistics {
            label_counts: BTreeMap::from([("N".into(), 100)]),
            ..OptimizerCatalogStatistics::default()
        },
    );
    let mut program = chain();
    for step in &mut program.steps {
        match step {
            GraphMatchStep::Node(node) | GraphMatchStep::Expand { target: node, .. } => {
                node.label = "N".into();
            }
            GraphMatchStep::ShortestPath(_) => unreachable!(),
        }
    }
    let estimate = |program| {
        estimate_physical_plan_cost_breakdown(
            &PhysicalPlan::GraphMatchExec {
                program,
                input: None,
            },
            &catalog,
        )
    };
    let unfiltered = estimate(program.clone());
    program.predicate = Some(Predicate::And(vec![
        property_eq("a", 1),
        property_eq("b", 1),
        property_eq("c", 1),
    ]));
    let late = estimate(program.clone());
    place_graph_match_predicates(&mut program);
    assert_eq!(program.predicate_steps, vec![1, 2, 3]);
    let early = estimate(program);
    assert_eq!(early.estimated_rows, late.estimated_rows);
    assert_eq!(
        early.estimated_rows,
        unfiltered
            .estimated_rows
            .div_ceil(crate::cardinality_defaults::FILTER_SELECTIVITY_DIVISOR),
    );
    assert!(early.random_io < late.random_io);
}
