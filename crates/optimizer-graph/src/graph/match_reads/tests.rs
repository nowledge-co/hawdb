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
        rel_types: Vec::new(),
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
            projected_relationships: BTreeMap::new(),
            predicate_steps: Vec::new(),
            edge_predicates: Vec::new(),
            steps: vec![
                GraphMatchStep::Node(node("a", &["id"])),
                expand("a", Some("r"), node("b", &[])),
                expand("b", None, node("c", &[])),
            ],
            predicate,
            optional: false,
            hash_join: None,
            id_set_conjuncts: Vec::new(),
            seeded_union: Vec::new(),
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
            projected_relationships: BTreeMap::new(),
            predicate_steps: Vec::new(),
            edge_predicates: Vec::new(),
            steps: vec![
                GraphMatchStep::Node(node("b", &["kind"])),
                expand("b", None, node("d", &[])),
            ],
            predicate: None,
            optional: false,
            hash_join: None,
            id_set_conjuncts: Vec::new(),
            seeded_union: Vec::new(),
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
            projected_relationships: BTreeMap::new(),
            predicate_steps: Vec::new(),
            edge_predicates: Vec::new(),
            steps: vec![
                GraphMatchStep::Node(node("b", &[])),
                expand("b", None, node("c", &["tag"])),
            ],
            predicate: None,
            optional: true,
            hash_join: None,
            id_set_conjuncts: Vec::new(),
            seeded_union: Vec::new(),
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

/// `projected_relationships` of the first `GraphMatchExec` in `plan`.
fn relationship_projection(plan: PhysicalPlan) -> BTreeMap<String, Vec<String>> {
    fn first(plan: &PhysicalPlan) -> Option<&GraphMatchProgram> {
        match plan {
            PhysicalPlan::GraphMatchExec { program, .. } => Some(program),
            PhysicalPlan::ProjectExec { input, .. }
            | PhysicalPlan::AggregateExec { input, .. }
            | PhysicalPlan::FilterExec { input, .. } => first(input),
            _ => None,
        }
    }
    let plan = project_graph_match_reads(plan, &mut Vec::new());
    first(&plan)
        .expect("a GraphMatchExec")
        .projected_relationships
        .iter()
        .map(|(name, properties)| (name.clone(), properties.iter().cloned().collect()))
        .collect()
}

#[test]
fn relationship_reads_keep_the_properties_later_operators_and_the_predicate_use() {
    // RETURN r.since, with WHERE r.weight > 1: both properties, nothing else.
    let predicate = Predicate::PropertyCompare {
        variable: "r".into(),
        property: "weight".into(),
        op: hawdb_plan_cypher::ComparisonOp::Gt,
        value: Value::Int(1),
    };
    let plan = project(vec![property("r", "since")], chain(Some(predicate)));
    assert_eq!(
        relationship_projection(plan),
        expected(&[("r", &["since", "weight"])])
    );
    // Unread beyond its identity: only endpoints and type.
    let plan = project(vec![property("b", "name")], chain(None));
    assert_eq!(relationship_projection(plan), expected(&[("r", &[])]));
    // `count(r)` tests only that it is bound.
    let plan = PhysicalPlan::AggregateExec {
        group_keys: Vec::new(),
        items: vec![Aggregation {
            function: AggregateFunction::Count,
            target: AggregateTarget::Variable("r".into()),
            distinct: false,
            name: "n".into(),
        }],
        input: Box::new(chain(None)),
    };
    assert_eq!(relationship_projection(plan), expected(&[("r", &[])]));
}

#[test]
fn relationship_pattern_properties_are_read() {
    let mut plan = chain(None);
    if let PhysicalPlan::GraphMatchExec { program, .. } = &mut plan
        && let GraphMatchStep::Expand { properties, .. } = &mut program.steps[1]
    {
        properties.insert("kind".into(), Value::Int(1));
    }
    let plan = project(vec![property("r", "since")], plan);
    assert_eq!(
        relationship_projection(plan),
        expected(&[("r", &["kind", "since"])])
    );
}

#[test]
fn whole_relationship_uses_read_every_property() {
    // RETURN r, collect(r), r in a list, type(r) (kept whole as for nodes),
    // or a column of the same name.
    let collect = PhysicalPlan::AggregateExec {
        group_keys: Vec::new(),
        items: vec![Aggregation {
            function: AggregateFunction::Collect,
            target: AggregateTarget::Variable("r".into()),
            distinct: false,
            name: "rs".into(),
        }],
        input: Box::new(chain(None)),
    };
    let listed = project(
        vec![ProjectionExpression::List(vec![variable("r")])],
        chain(None),
    );
    let typed = project(
        vec![ProjectionExpression::RelationshipType {
            variable: "r".into(),
        }],
        chain(None),
    );
    let column = project(
        vec![ProjectionExpression::ColumnProperty {
            column: "r".into(),
            property: "since".into(),
        }],
        chain(None),
    );
    for plan in [
        project(vec![variable("r")], chain(None)),
        collect,
        listed,
        typed,
        column,
    ] {
        assert_eq!(relationship_projection(plan), BTreeMap::new());
    }
    // An operator the analysis does not understand hides what it reads.
    let joined = project_graph_match_reads(
        project(
            vec![property("r", "since")],
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
    let PhysicalPlan::NodeCartesianProductExec { left, .. } = *input else {
        panic!("cartesian product");
    };
    assert_eq!(relationship_projection(*left), BTreeMap::new());
}

#[test]
fn imported_and_variable_length_relationships_are_not_projected() {
    let program = GraphMatchProgram {
        imports: vec![GraphBindingImport {
            variable: "r".into(),
            column: "r_column".into(),
            kind: GraphEntityKind::Relationship,
        }],
        introduced: vec!["b".into(), "s".into()],
        projected_nodes: BTreeMap::new(),
        projected_relationships: BTreeMap::new(),
        predicate_steps: Vec::new(),
        edge_predicates: Vec::new(),
        steps: vec![
            GraphMatchStep::Node(node("a", &[])),
            expand("a", Some("r"), node("b", &[])),
            GraphMatchStep::Expand {
                source: "b".into(),
                relationship: Some("s".into()),
                rel_type: "R".into(),
                rel_types: Vec::new(),
                properties: BTreeMap::new(),
                direction: RelationshipDirection::Outgoing,
                min_hops: 1,
                max_hops: 2,
                target: node("c", &[]),
            },
        ],
        predicate: None,
        optional: false,
        hash_join: None,
        id_set_conjuncts: Vec::new(),
        seeded_union: Vec::new(),
    };
    let plan = project(
        vec![property("r", "since"), property("s", "since")],
        PhysicalPlan::GraphMatchExec {
            program,
            input: None,
        },
    );
    assert_eq!(relationship_projection(plan), BTreeMap::new());
}

/// `… UNWIND xs AS x` over a plan that produces the list column `xs`.
fn unwind(input: PhysicalPlan) -> PhysicalPlan {
    PhysicalPlan::UnwindRowsExec {
        expression: ProjectionExpression::Column("xs".to_string()),
        variable: "x".to_string(),
        consumes_source: false,
        input: Box::new(input),
    }
}

fn consumes(plan: &PhysicalPlan) -> bool {
    let mut current = Some(plan);
    while let Some(plan) = current {
        current = match plan {
            PhysicalPlan::UnwindRowsExec {
                consumes_source, ..
            } => return *consumes_source,
            PhysicalPlan::GraphMatchExec { input, .. } => input.as_deref(),
            PhysicalPlan::ProjectExec { input, .. } | PhysicalPlan::DistinctExec { input } => {
                Some(input)
            }
            _ => None,
        };
    }
    panic!("no UnwindRowsExec in {plan:?}")
}

/// An unwound list column that nothing above reads is consumed; any read of
/// it, or a DISTINCT over the unprojected rows, keeps it (#1606).
#[test]
fn unwind_consumes_only_an_unread_source_column() {
    let column = |name: &str| ProjectionExpression::Column(name.to_string());
    let cases = [
        (
            "only x is read",
            project(vec![column("x")], unwind(chain(None))),
            true,
        ),
        (
            "xs is read after the unwind",
            project(vec![column("x"), column("xs")], unwind(chain(None))),
            false,
        ),
        (
            "a property of xs is read",
            project(
                vec![ProjectionExpression::ColumnProperty {
                    column: "xs".to_string(),
                    property: "p".to_string(),
                }],
                unwind(chain(None)),
            ),
            false,
        ),
        (
            "DISTINCT compares the rows that still carry xs",
            project(
                vec![column("x")],
                PhysicalPlan::DistinctExec {
                    input: Box::new(unwind(chain(None))),
                },
            ),
            false,
        ),
        (
            "a projection rebuilds the columns before DISTINCT",
            PhysicalPlan::DistinctExec {
                input: Box::new(project(vec![column("x")], unwind(chain(None)))),
            },
            true,
        ),
        (
            "a later MATCH imports xs",
            project(
                vec![column("x")],
                PhysicalPlan::GraphMatchExec {
                    program: GraphMatchProgram {
                        imports: vec![GraphBindingImport {
                            variable: "n".to_string(),
                            column: "xs".to_string(),
                            kind: GraphEntityKind::Node,
                        }],
                        introduced: Vec::new(),
                        projected_nodes: BTreeMap::new(),
                        projected_relationships: BTreeMap::new(),
                        predicate_steps: Vec::new(),
                        edge_predicates: Vec::new(),
                        steps: vec![GraphMatchStep::Node(node("n", &[]))],
                        predicate: None,
                        optional: false,
                        hash_join: None,
                        id_set_conjuncts: Vec::new(),
                        seeded_union: Vec::new(),
                    },
                    input: Some(Box::new(unwind(chain(None)))),
                },
            ),
            false,
        ),
    ];
    for (case, plan, expected) in cases {
        let mut decisions = Vec::new();
        let plan = project_graph_match_reads(plan, &mut decisions);
        assert_eq!(consumes(&plan), expected, "{case}: {decisions:?}");
        assert_eq!(
            decisions
                .iter()
                .any(|decision| decision.starts_with("UnwindRowsExec takes its source column")),
            expected,
            "{case}: {decisions:?}"
        );
    }
}
