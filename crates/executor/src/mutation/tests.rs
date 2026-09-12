use super::*;
use skein_core::{PropertyType, SchemaObjectState, TableKind, Value};
use skein_ddl::{SchemaObjectState as DdlState, SchemaPropertyType, SchemaTableKind};
use skein_plan::{Predicate, SetAssignment, SetNodePropertiesReturnMode};
use skein_storage::PropertyFilter;
use std::collections::BTreeMap;

fn assert_command(plan: PhysicalPlan, expected: GraphMutation) {
    assert!(is_mutation_plan(&plan).unwrap(), "{plan:?}");
    assert_eq!(mutation_command(&plan).unwrap(), Some(expected), "{plan:?}");
}

#[test]
fn schema_commands_preserve_catalog_types_and_states() {
    for (table_kind, expected_kind) in [
        (SchemaTableKind::Node, TableKind::Node),
        (SchemaTableKind::Relationship, TableKind::Relationship),
    ] {
        for (value_type, expected_type) in [
            (SchemaPropertyType::Any, PropertyType::Any),
            (SchemaPropertyType::Bool, PropertyType::Bool),
            (SchemaPropertyType::Int, PropertyType::Int),
            (SchemaPropertyType::Float, PropertyType::Float),
            (SchemaPropertyType::String, PropertyType::String),
            (SchemaPropertyType::Text, PropertyType::Text),
            (SchemaPropertyType::List, PropertyType::List),
        ] {
            for nullable in [false, true] {
                assert_command(
                    PhysicalPlan::CreateProperty {
                        table_kind,
                        table: "table".into(),
                        property: "property".into(),
                        value_type,
                        nullable,
                    },
                    GraphMutation::CreateProperty {
                        table_kind: expected_kind,
                        table: "table".into(),
                        property: "property".into(),
                        value_type: expected_type,
                        nullable,
                    },
                );
            }
        }
        for (state, expected_state) in [
            (DdlState::DeleteOnly, SchemaObjectState::DeleteOnly),
            (DdlState::WriteOnly, SchemaObjectState::WriteOnly),
            (DdlState::Backfill, SchemaObjectState::Backfill),
            (DdlState::Validating, SchemaObjectState::Validating),
            (DdlState::Public, SchemaObjectState::Public),
            (DdlState::Gc, SchemaObjectState::Gc),
        ] {
            assert_command(
                PhysicalPlan::AlterTableState {
                    table_kind,
                    table: "table".into(),
                    state,
                },
                GraphMutation::AlterTableState {
                    table_kind: expected_kind,
                    table: "table".into(),
                    state: expected_state,
                },
            );
            assert_command(
                PhysicalPlan::AlterPropertyState {
                    table_kind,
                    table: "table".into(),
                    property: "property".into(),
                    state,
                },
                GraphMutation::AlterPropertyState {
                    table_kind: expected_kind,
                    table: "table".into(),
                    property: "property".into(),
                    state: expected_state,
                },
            );
        }
    }
}

#[test]
fn simple_schema_commands_preserve_names_and_index_order() {
    macro_rules! command {
        ($variant:ident { $($field:ident: $value:expr),+ $(,)? }) => {
            assert_command(
                PhysicalPlan::$variant { $($field: $value),+ },
                GraphMutation::$variant { $($field: $value),+ },
            );
        };
    }
    command!(CreateNodeLabel {
        label: "node".into()
    });
    command!(CreateRelationshipType {
        rel_type: "edge".into()
    });
    command!(CreateNodeTable {
        name: "node".into()
    });
    command!(CreateRelationshipTable {
        name: "edge".into()
    });
    command!(CreateIndex {
        label: "node".into(),
        property: "key".into()
    });
    command!(CreateCompositeIndex {
        label: "node".into(),
        properties: vec!["z".into(), "a".into(), "m".into()],
    });
    command!(CreateRangeIndex {
        label: "node".into(),
        property: "key".into()
    });
    command!(CreateFullTextIndex {
        label: "node".into(),
        property: "text".into()
    });
    command!(CreateUniqueConstraint {
        label: "node".into(),
        property: "key".into()
    });
    command!(CreateNodePropertyExistsConstraint {
        label: "node".into(),
        property: "key".into()
    });
    command!(CreateRelationshipUniqueConstraint {
        rel_type: "edge".into(),
        property: "key".into()
    });
    command!(CreateRelationshipPropertyExistsConstraint {
        rel_type: "edge".into(),
        property: "key".into()
    });
}

fn assignment_cases(value: &Value, amount: i64, preserve: bool) -> Vec<(SetValue, NodeSetValue)> {
    vec![
        (
            SetValue::Value(value.clone()),
            NodeSetValue::Value(value.clone()),
        ),
        (
            SetValue::Coalesce {
                property: "counter".into(),
                default: value.clone(),
            },
            NodeSetValue::Coalesce {
                default: value.clone(),
            },
        ),
        (
            SetValue::AddInt {
                property: "counter".into(),
                amount,
            },
            NodeSetValue::AddInt { amount },
        ),
        (
            SetValue::DecrementFloorZero {
                property: "counter".into(),
            },
            NodeSetValue::DecrementFloorZero,
        ),
        (
            SetValue::PreserveNewerExisting {
                property: "counter".into(),
                incoming: value.clone(),
                preserve,
            },
            NodeSetValue::PreserveNewerExisting {
                incoming: value.clone(),
                preserve,
            },
        ),
    ]
}

fn check_seed(seed: u64) -> usize {
    // Deterministic bounded inputs include nested values, empty identifiers, and
    // signed boundaries. Expected storage commands never call lowering helpers.
    let bits = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15).rotate_left(17);
    let value = match seed % 6 {
        0 => Value::Null,
        1 => Value::Bool(bits & 1 != 0),
        2 => Value::Int(bits as i64),
        3 => Value::Float(f64::from_bits(bits)),
        4 => Value::String(format!("value-\u{00e9}-\0-{seed}")),
        _ => Value::List(vec![
            Value::Null,
            Value::List(vec![Value::Int(bits as i64)]),
        ]),
    };
    let amount = match seed % 4 {
        0 => i64::MIN,
        1 => i64::MAX,
        2 => 0,
        _ => bits as i64,
    };
    let label = if seed.is_multiple_of(7) {
        String::new()
    } else {
        format!("Node-{seed}")
    };
    let predicate = Predicate::PropertyEq {
        variable: "n".into(),
        property: "match".into(),
        value: value.clone(),
    };
    let filter = Some(PropertyFilter::Eq {
        property: "match".into(),
        value: value.clone(),
    });
    let cases = assignment_cases(&value, amount, seed & 1 != 0);
    let mut checked = 0;
    for (input, expected) in &cases {
        let plan = PhysicalPlan::SetNodeProperty {
            variable: "n".into(),
            label: label.clone(),
            predicate: Some(predicate.clone()),
            property: "counter".into(),
            value: input.clone(),
        };
        let assignment = NodeSetAssignment {
            property: "counter".into(),
            value: expected.clone(),
        };
        assert_eq!(
            node_set_assignment(&SetAssignment {
                property: "counter".into(),
                value: input.clone()
            }),
            assignment,
        );
        match expected {
            NodeSetValue::Coalesce { .. } => {
                assert!(is_mutation_plan(&plan).unwrap());
                assert!(matches!(
                    mutation_command(&plan),
                    Err(SkeinError::Semantic(message))
                        if message == "COALESCE node SET is not supported in transactional MATCH SET"
                ));
            }
            NodeSetValue::Value(value) => assert_command(
                plan,
                GraphMutation::SetNodeProperty {
                    label: label.clone(),
                    filter: filter.clone(),
                    property: "counter".into(),
                    value: value.clone(),
                },
            ),
            NodeSetValue::AddInt { amount } => assert_command(
                plan,
                GraphMutation::SetNodePropertyAddInt {
                    label: label.clone(),
                    filter: filter.clone(),
                    property: "counter".into(),
                    amount: *amount,
                },
            ),
            _ => assert_command(
                plan,
                GraphMutation::SetNodeProperties {
                    label: label.clone(),
                    filter: filter.clone(),
                    assignments: vec![assignment],
                },
            ),
        }
        checked += 1;
    }
    let assignments = cases
        .iter()
        .map(|(value, _)| SetAssignment {
            property: "counter".into(),
            value: value.clone(),
        })
        .collect::<Vec<_>>();
    let expected_assignments = cases
        .iter()
        .map(|(_, value)| NodeSetAssignment {
            property: "counter".into(),
            value: value.clone(),
        })
        .collect::<Vec<_>>();
    let expected = GraphMutation::SetNodeProperties {
        label: label.clone(),
        filter: filter.clone(),
        assignments: expected_assignments.clone(),
    };
    assert_command(
        PhysicalPlan::SetNodeProperties {
            variable: "n".into(),
            label: label.clone(),
            predicate: Some(predicate.clone()),
            assignments: assignments.clone(),
        },
        expected.clone(),
    );
    for returns in [
        SetNodePropertiesReturnMode::Count {
            name: "count".into(),
        },
        SetNodePropertiesReturnMode::Project(vec![]),
    ] {
        assert_command(
            PhysicalPlan::SetNodePropertiesReturn {
                variable: "n".into(),
                label: label.clone(),
                predicate: Some(predicate.clone()),
                assignments: assignments.clone(),
                returns,
            },
            expected.clone(),
        );
    }
    let properties = BTreeMap::from([("match".into(), value.clone())]);
    assert_command(
        PhysicalPlan::CreateNode {
            label: label.clone(),
            properties: properties.clone(),
        },
        GraphMutation::CreateNode {
            label: label.clone(),
            properties: properties.clone(),
        },
    );
    // Distinct on-match/post-merge sequences catch accidental list swaps.
    assert_command(
        PhysicalPlan::MergeNode {
            label: label.clone(),
            match_properties: properties.clone(),
            on_create_properties: BTreeMap::new(),
            on_match_assignments: assignments,
            post_merge_assignments: vec![],
        },
        GraphMutation::MergeNode {
            label: label.clone(),
            match_properties: properties,
            on_create_properties: BTreeMap::new(),
            on_match_assignments: expected_assignments,
            post_merge_assignments: vec![],
        },
    );
    checked += 5;
    for detach in [false, true] {
        assert_command(
            PhysicalPlan::DeleteNode {
                variable: "n".into(),
                label: label.clone(),
                predicate: Some(predicate.clone()),
                detach,
            },
            GraphMutation::DeleteNode {
                label: label.clone(),
                filter: filter.clone(),
                detach,
            },
        );
        checked += 1;
    }
    // None, an empty conjunction, and a rejected executable predicate must not
    // collapse into one classification: root preflight relies on Err vs None.
    for (predicate, expected) in [
        (None, None),
        (
            Some(Predicate::And(vec![])),
            Some(PropertyFilter::And(vec![])),
        ),
    ] {
        assert_command(
            PhysicalPlan::DeleteNode {
                variable: "n".into(),
                label: label.clone(),
                predicate,
                detach: false,
            },
            GraphMutation::DeleteNode {
                label: label.clone(),
                filter: expected,
                detach: false,
            },
        );
        checked += 1;
    }
    let rejected = PhysicalPlan::SetNodeProperty {
        variable: "n".into(),
        label,
        predicate: Some(Predicate::ConstantBool(true)),
        property: "counter".into(),
        value: SetValue::Coalesce {
            property: "counter".into(),
            default: value,
        },
    };
    assert!(is_mutation_plan(&rejected).unwrap());
    assert!(
        matches!(mutation_command(&rejected), Err(SkeinError::Execution(message))
        if message == "expression predicates are not supported in property filters")
    );
    checked += 1;
    checked + check_relationships(seed)
}

fn check_relationships(seed: u64) -> usize {
    let source_properties = BTreeMap::from([("source-id".into(), Value::Int(seed as i64))]);
    let target_properties =
        BTreeMap::from([("target-id".into(), Value::String(format!("target-{seed}")))]);
    let new_properties = BTreeMap::from([("new-id".into(), Value::Bool(seed & 1 != 0))]);
    let rel_properties = BTreeMap::from([("weight".into(), Value::Int(-(seed as i64)))]);
    let source_filter = Some(PropertyFilter::Eq {
        property: "source-id".into(),
        value: Value::Int(seed as i64),
    });
    let target_filter = Some(PropertyFilter::Eq {
        property: "target-id".into(),
        value: Value::String(format!("target-{seed}")),
    });
    let new_filter = Some(PropertyFilter::Eq {
        property: "new-id".into(),
        value: Value::Bool(seed & 1 != 0),
    });
    let created = BTreeMap::from([("created".into(), Value::Int(i64::MAX))]);
    assert_command(
        PhysicalPlan::MergeRelationshipToMatchedTarget {
            source_label: "source".into(),
            source_properties: source_properties.clone(),
            old_rel_type: "old-edge".into(),
            old_rel_properties: rel_properties.clone(),
            old_target_label: "old-target".into(),
            old_target_properties: target_properties.clone(),
            new_target_label: "new-target".into(),
            new_target_properties: new_properties.clone(),
            new_rel_type: "new-edge".into(),
            new_rel_match_properties: BTreeMap::new(),
            on_create_properties: created.clone(),
        },
        GraphMutation::MergeRelationshipsToMatchedTarget(MatchedRelationshipRetargetMerge {
            source_label: "source".into(),
            source_filter: source_filter.clone(),
            old_rel_type: "old-edge".into(),
            old_rel_filter: rel_properties.clone(),
            old_target_label: "old-target".into(),
            old_target_filter: target_filter.clone(),
            new_target_label: "new-target".into(),
            new_target_filter: new_filter.clone(),
            new_rel_type: "new-edge".into(),
            new_rel_match_properties: BTreeMap::new(),
            on_create_properties: created.clone(),
        }),
    );
    assert_command(
        PhysicalPlan::MergeRelationshipFromMatchedTarget {
            old_source_label: "source".into(),
            old_source_properties: source_properties.clone(),
            old_rel_type: "old-edge".into(),
            old_rel_properties: rel_properties.clone(),
            old_target_label: "old-target".into(),
            old_target_properties: target_properties.clone(),
            new_source_label: "new-source".into(),
            new_source_properties: new_properties,
            new_rel_type: "new-edge".into(),
            new_rel_match_properties: BTreeMap::new(),
            on_create_properties: created,
        },
        GraphMutation::MergeRelationshipsFromMatchedTarget(
            MatchedRelationshipSourceRetargetMerge {
                old_source_label: "source".into(),
                old_source_filter: source_filter.clone(),
                old_rel_type: "old-edge".into(),
                old_rel_filter: rel_properties.clone(),
                old_target_label: "old-target".into(),
                old_target_filter: target_filter.clone(),
                new_source_label: "new-source".into(),
                new_source_filter: new_filter,
                new_rel_type: "new-edge".into(),
                new_rel_match_properties: BTreeMap::new(),
                on_create_properties: BTreeMap::from([("created".into(), Value::Int(i64::MAX))]),
            },
        ),
    );
    assert_command(
        PhysicalPlan::MergeRelationshipFromMatchedRelationship {
            source_label: "source".into(),
            source_properties,
            old_rel_type: "old-edge".into(),
            old_rel_properties: rel_properties.clone(),
            target_label: "target".into(),
            target_properties,
            new_rel_type: "new-edge".into(),
            new_rel_match_properties: BTreeMap::new(),
            on_create_properties: BTreeMap::from([
                (
                    "copied".into(),
                    RelationshipOnCreateValue::MatchedRelationshipProperty {
                        property: "weight".into(),
                    },
                ),
                (
                    "literal".into(),
                    RelationshipOnCreateValue::Value(Value::Null),
                ),
            ]),
        },
        GraphMutation::MergeRelationshipsFromMatchedRelationships(MatchedRelationshipCopyMerge {
            source_label: "source".into(),
            source_filter,
            old_rel_type: "old-edge".into(),
            old_rel_filter: rel_properties,
            target_label: "target".into(),
            target_filter,
            new_rel_type: "new-edge".into(),
            new_rel_match_properties: BTreeMap::new(),
            on_create_properties: BTreeMap::from([
                (
                    "copied".into(),
                    RelationshipOnCreatePropertyValue::MatchedRelationshipProperty {
                        property: "weight".into(),
                    },
                ),
                (
                    "literal".into(),
                    RelationshipOnCreatePropertyValue::Value(Value::Null),
                ),
            ]),
        }),
    );
    3
}

#[test]
fn read_and_projection_plans_are_not_storage_mutations() {
    for plan in [
        PhysicalPlan::EmptyExec,
        PhysicalPlan::ProjectGraph {
            name: "graph".into(),
            node_labels: vec![],
            rel_types: vec![],
        },
        PhysicalPlan::SeqNodeScan {
            variable: "n".into(),
            label: "Node".into(),
        },
    ] {
        assert!(!is_mutation_plan(&plan).unwrap());
        assert_eq!(mutation_command(&plan).unwrap(), None);
    }
}

#[test]
fn mutation_lowering_generated_smoke() {
    for seed in 0..16 {
        assert_eq!(check_seed(seed), 18, "seed {seed}");
    }
}

#[test]
#[ignore = "local differential campaign"]
fn mutation_lowering_differential_campaign() {
    let cases: usize = (0..256).map(check_seed).sum();
    assert_eq!(cases, 256 * 18);
    eprintln!("mutation lowering: 256 seeds, {cases} command/error cases");
}
