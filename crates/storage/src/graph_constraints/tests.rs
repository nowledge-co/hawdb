use super::*;
use std::collections::BTreeSet;

const TYPES: [(PropertyType, &str); 7] = [
    (PropertyType::Any, "any"),
    (PropertyType::Bool, "bool"),
    (PropertyType::Int, "int"),
    (PropertyType::Float, "float"),
    (PropertyType::String, "string"),
    (PropertyType::Text, "text"),
    (PropertyType::List, "list"),
];

const STATES: [SchemaObjectState; 6] = [
    SchemaObjectState::DeleteOnly,
    SchemaObjectState::WriteOnly,
    SchemaObjectState::Backfill,
    SchemaObjectState::Validating,
    SchemaObjectState::Public,
    SchemaObjectState::Gc,
];

fn values() -> Vec<Value> {
    vec![
        Value::Null,
        Value::Bool(false),
        Value::Bool(true),
        Value::Int(-1),
        Value::Int(0),
        Value::Int(1),
        Value::Float(0.0),
        Value::Float(-0.0),
        Value::Float(f64::from_bits(0x7ff8_0000_0000_0001)),
        Value::Float(f64::from_bits(0x7ff8_0000_0000_0002)),
        Value::String(String::new()),
        Value::String("value\n\u{e9}".into()),
        Value::Uuid(skein_core::Uuid::parse_str("01890f3e-0e3c-7f9e-9b23-1bcdef012345").unwrap()),
        Value::Binary(vec![0, 255]),
        Value::List(vec![Value::Null]),
        Value::Map(BTreeMap::from([("x".into(), Value::Int(1))])),
    ]
}

// Model allowed wire categories, without calling storage validation helpers.
fn reference_type(type_name: &str, nullable: bool, value: Option<&Value>) -> bool {
    let category = match value {
        None | Some(Value::Null) => return nullable,
        Some(Value::Bool(_)) => "bool",
        Some(Value::Int(_)) => "int",
        Some(Value::Float(_)) => "float",
        Some(Value::String(_)) => "string",
        Some(Value::List(_)) => "list",
        _ => "other",
    };
    type_name == "any" || type_name == category || (type_name == "text" && category == "string")
}

fn properties(value: Option<Value>) -> BTreeMap<String, Value> {
    value
        .map(|value| BTreeMap::from([("key".into(), value)]))
        .unwrap_or_default()
}

fn node(id: u64, label: u32, value: Option<Value>) -> NodeRecord {
    NodeRecord {
        id: NodeId(id),
        labels: BTreeSet::from([LabelId(label)]),
        properties: properties(value),
    }
}

fn relationship(id: u64, rel_type: u32, value: Option<Value>) -> RelRecord {
    RelRecord {
        id: RelId(id),
        rel_type: RelTypeId(rel_type),
        source: NodeId(0),
        target: NodeId(1),
        properties: properties(value),
    }
}

fn node_map(records: Vec<NodeRecord>) -> CowSegmentedMap<NodeId, NodeRecord> {
    records
        .into_iter()
        .map(|record| (record.id, record))
        .collect::<BTreeMap<_, _>>()
        .into()
}

fn relationship_map(records: Vec<RelRecord>) -> CowSegmentedMap<RelId, RelRecord> {
    records
        .into_iter()
        .map(|record| (record.id, record))
        .collect::<BTreeMap<_, _>>()
        .into()
}

fn catalog() -> Catalog {
    let mut catalog = Catalog::default();
    catalog.get_or_create_label("Items");
    catalog.get_or_create_label("Other");
    catalog.get_or_create_rel_type("LINKS");
    catalog.get_or_create_rel_type("OTHER");
    catalog
}

#[test]
fn scalar_type_matrix_preserves_nullability_and_exact_error_messages() {
    let values = values();
    for (value_type, name) in TYPES {
        assert_eq!(encode_property_type(value_type), name);
        for nullable in [false, true] {
            for value in std::iter::once(None).chain(values.iter().map(Some)) {
                let result = validate_property_schema_value(
                    "Items", "key", value_type, nullable, value, "node 7",
                );
                if reference_type(name, nullable, value) {
                    result.unwrap();
                } else {
                    let reason = if value.is_none_or(|value| value == &Value::Null) {
                        "property is not nullable".into()
                    } else {
                        format!("expected {name}")
                    };
                    match result {
                        Err(SkeinError::Storage(message)) => assert_eq!(
                            message,
                            format!("property schema violation on node 7 in Items(key): {reason}")
                        ),
                        other => panic!("unexpected validation result: {other:?}"),
                    }
                }
            }
        }
    }
}

#[test]
fn table_and_property_states_gate_snapshot_and_record_schema_validation() {
    let nodes = node_map(vec![node(1, 0, Some(Value::String("wrong".into())))]);
    let relationships = relationship_map(vec![relationship(1, 0, None)]);
    for table_state in STATES {
        for property_state in STATES {
            let mut catalog = catalog();
            for (kind, name) in [
                (TableKind::Node, "Items"),
                (TableKind::Relationship, "LINKS"),
            ] {
                let table = catalog.get_or_create_table(kind, name);
                let property =
                    catalog.get_or_create_property(table, "key", PropertyType::Int, false);
                assert!(catalog.set_table_state(table, table_state));
                assert!(catalog.set_property_state(property, property_state));
            }
            let expected = table_state != SchemaObjectState::Public
                || property_state != SchemaObjectState::Public;
            assert_eq!(
                validate_property_schemas(&catalog, &nodes, &relationships).is_ok(),
                expected
            );
            assert_eq!(
                validate_node_record_constraints(&catalog, nodes.values().next().unwrap()).is_ok(),
                expected
            );
            assert_eq!(
                validate_relationship_record_constraints(
                    &catalog,
                    relationships.values().next().unwrap()
                )
                .is_ok(),
                expected
            );
        }
    }
}

#[test]
fn missing_table_or_token_does_not_activate_schema_validation() {
    let nodes = node_map(vec![node(1, 0, None)]);
    let relationships = relationship_map(vec![relationship(1, 0, None)]);
    let mut catalog = Catalog::default();
    catalog.get_or_create_property(skein_core::TableId(99), "key", PropertyType::Int, false);
    for (kind, name) in [
        (TableKind::Node, "NoLabel"),
        (TableKind::Relationship, "NoType"),
    ] {
        let table = catalog.get_or_create_table(kind, name);
        catalog.get_or_create_property(table, "key", PropertyType::Int, false);
    }
    validate_property_schemas(&catalog, &nodes, &relationships).unwrap();
    validate_node_record_constraints(&catalog, nodes.values().next().unwrap()).unwrap();
    validate_relationship_record_constraints(&catalog, relationships.values().next().unwrap())
        .unwrap();
}

#[test]
fn uniqueness_preserves_value_identity_and_null_is_not_existence() {
    let catalog = catalog();
    let first_nan = Value::Float(f64::from_bits(0x7ff8_0000_0000_0001));
    let second_nan = Value::Float(f64::from_bits(0x7ff8_0000_0000_0002));
    for (left, right, unique, present) in [
        (None, None, true, false),
        (Some(Value::Null), Some(Value::Null), true, false),
        (Some(Value::Int(1)), Some(Value::Float(1.0)), true, true),
        (
            Some(Value::Float(0.0)),
            Some(Value::Float(-0.0)),
            true,
            true,
        ),
        (Some(first_nan.clone()), Some(second_nan), true, true),
        (Some(first_nan.clone()), Some(first_nan), false, true),
        (
            Some(Value::List(vec![Value::Null])),
            Some(Value::List(vec![Value::Null])),
            false,
            true,
        ),
    ] {
        let nodes = node_map(vec![
            node(1, 0, left.clone()),
            node(2, 0, right.clone()),
            node(3, 1, None),
        ]);
        let relationships = relationship_map(vec![
            relationship(1, 0, left),
            relationship(2, 0, right),
            relationship(3, 1, None),
        ]);
        assert_eq!(
            validate_unique_property(&catalog, &nodes, LabelId(0), "key").is_ok(),
            unique
        );
        assert_eq!(
            validate_unique_relationship_property(&catalog, &relationships, RelTypeId(0), "key")
                .is_ok(),
            unique
        );
        assert_eq!(
            validate_node_property_exists(&catalog, &nodes, LabelId(0), "key").is_ok(),
            present
        );
        assert_eq!(
            validate_relationship_property_exists(&catalog, &relationships, RelTypeId(0), "key")
                .is_ok(),
            present
        );
    }
}

#[test]
fn constraint_errors_preserve_subject_and_first_conflicting_ids() {
    let catalog = catalog();
    let nodes = node_map(vec![
        node(7, 0, Some(Value::Int(1))),
        node(3, 0, Some(Value::Int(1))),
        node(1, 1, Some(Value::Int(1))),
    ]);
    let relationships = relationship_map(vec![
        relationship(7, 0, Some(Value::Int(1))),
        relationship(3, 0, Some(Value::Int(1))),
        relationship(1, 1, Some(Value::Int(1))),
    ]);
    for (result, expected) in [
        (validate_unique_property(&catalog, &nodes, LabelId(0), "key"), "unique constraint violation on :Items(key) for nodes 3 and 7"),
        (validate_unique_relationship_property(&catalog, &relationships, RelTypeId(0), "key"), "relationship unique constraint violation on :LINKS(key) for relationships 3 and 7"),
        (validate_node_property_exists(&catalog, &nodes, LabelId(0), "missing"), "node property exists constraint violation on :Items(missing) for node 3"),
        (validate_relationship_property_exists(&catalog, &relationships, RelTypeId(0), "missing"), "relationship property exists constraint violation on :LINKS(missing) for relationship 3"),
    ] {
        match result {
            Err(SkeinError::Storage(message)) => assert_eq!(message, expected),
            other => panic!("unexpected constraint result: {other:?}"),
        }
    }
}

#[test]
fn record_validation_reports_schema_errors_before_existence_errors() {
    let mut catalog = catalog();
    for (kind, name) in [
        (TableKind::Node, "Items"),
        (TableKind::Relationship, "LINKS"),
    ] {
        let table = catalog.get_or_create_table(kind, name);
        catalog.get_or_create_property(table, "key", PropertyType::Int, false);
    }
    catalog.get_or_create_node_property_exists_constraint(LabelId(0), "required");
    catalog.get_or_create_relationship_property_exists_constraint(RelTypeId(0), "required");

    for (value, node_error, rel_error) in [
        (
            Value::String("wrong".into()),
            "property schema violation on node 7 in Items(key): expected int",
            "property schema violation on relationship 7 in LINKS(key): expected int",
        ),
        (
            Value::Int(1),
            "node property exists constraint violation on :Items(required) for node 7",
            "relationship property exists constraint violation on :LINKS(required) for relationship 7",
        ),
    ] {
        for (result, expected) in [
            (
                validate_node_record_constraints(&catalog, &node(7, 0, Some(value.clone()))),
                node_error,
            ),
            (
                validate_relationship_record_constraints(
                    &catalog,
                    &relationship(7, 0, Some(value)),
                ),
                rel_error,
            ),
        ] {
            match result {
                Err(SkeinError::Storage(message)) => assert_eq!(message, expected),
                other => panic!("unexpected constraint result: {other:?}"),
            }
        }
    }
}

fn next_random(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

// A pairwise oracle avoids the implementation's ordered-map duplicate detector.
fn reference_unique(values: &[Option<&Value>]) -> bool {
    for (index, value) in values.iter().enumerate() {
        let Some(value) = value.filter(|value| *value != &Value::Null) else {
            continue;
        };
        if values[..index].contains(&Some(value)) {
            return false;
        }
    }
    true
}

fn check_seed(seed: u64) -> usize {
    let mut random = seed + 1;
    let pool = values();
    let mut checks = 0;
    for case in 0..16 {
        let (value_type, type_name) = TYPES[next_random(&mut random) as usize % TYPES.len()];
        let nullable = next_random(&mut random).is_multiple_of(2);
        let table_state = STATES[next_random(&mut random) as usize % STATES.len()];
        let property_state = STATES[next_random(&mut random) as usize % STATES.len()];
        let exists = next_random(&mut random).is_multiple_of(2);
        let unique = next_random(&mut random).is_multiple_of(2);
        let mut catalog = catalog();
        for (kind, name) in [
            (TableKind::Node, "Items"),
            (TableKind::Relationship, "LINKS"),
        ] {
            let table = catalog.get_or_create_table(kind, name);
            let property = catalog.get_or_create_property(table, "key", value_type, nullable);
            catalog.set_table_state(table, table_state);
            catalog.set_property_state(property, property_state);
        }
        if exists {
            catalog.get_or_create_node_property_exists_constraint(LabelId(0), "key");
            catalog.get_or_create_relationship_property_exists_constraint(RelTypeId(0), "key");
        }
        if unique {
            catalog.get_or_create_unique_constraint(LabelId(0), "key");
            catalog.get_or_create_relationship_unique_constraint(RelTypeId(0), "key");
        }
        let mut nodes = Vec::new();
        let mut relationships = Vec::new();
        for id in 0..8 {
            let node_value = pool
                .get(next_random(&mut random) as usize % (pool.len() + 1))
                .cloned();
            let rel_value = pool
                .get(next_random(&mut random) as usize % (pool.len() + 1))
                .cloned();
            nodes.push(node(id, (next_random(&mut random) % 2) as u32, node_value));
            relationships.push(relationship(
                id,
                (next_random(&mut random) % 2) as u32,
                rel_value,
            ));
        }
        let nodes = node_map(nodes);
        let relationships = relationship_map(relationships);
        let before_nodes = nodes.clone();
        let before_relationships = relationships.clone();
        let node_values = nodes
            .values()
            .filter(|node| node.labels.contains(&LabelId(0)))
            .map(|node| node.properties.get("key"))
            .collect::<Vec<_>>();
        let rel_values = relationships
            .values()
            .filter(|rel| rel.rel_type == RelTypeId(0))
            .map(|rel| rel.properties.get("key"))
            .collect::<Vec<_>>();
        let schema_active =
            table_state == SchemaObjectState::Public && property_state == SchemaObjectState::Public;
        let node_schema = !schema_active
            || node_values
                .iter()
                .all(|value| reference_type(type_name, nullable, *value));
        let rel_schema = !schema_active
            || rel_values
                .iter()
                .all(|value| reference_type(type_name, nullable, *value));
        let node_exists = node_values
            .iter()
            .all(|value| value.is_some_and(|value| value != &Value::Null));
        let rel_exists = rel_values
            .iter()
            .all(|value| value.is_some_and(|value| value != &Value::Null));
        let mut check = |result: Result<()>, expected: bool, name: &str| {
            assert_eq!(
                result.is_ok(),
                expected,
                "seed={seed}, case={case}, check={name}: {result:?}"
            );
            if let Err(error) = result {
                assert!(matches!(error, SkeinError::Storage(_)), "{error}");
            }
            checks += 1;
        };
        check(
            validate_property_schemas(&catalog, &nodes, &relationships),
            node_schema && rel_schema,
            "schema",
        );
        check(
            validate_unique_property(&catalog, &nodes, LabelId(0), "key"),
            reference_unique(&node_values),
            "node unique",
        );
        check(
            validate_unique_relationship_property(&catalog, &relationships, RelTypeId(0), "key"),
            reference_unique(&rel_values),
            "rel unique",
        );
        check(
            validate_unique_constraints(&catalog, &nodes),
            !unique || reference_unique(&node_values),
            "node unique catalog",
        );
        check(
            validate_relationship_unique_constraints(&catalog, &relationships),
            !unique || reference_unique(&rel_values),
            "rel unique catalog",
        );
        check(
            validate_node_property_exists(&catalog, &nodes, LabelId(0), "key"),
            node_exists,
            "node exists",
        );
        check(
            validate_relationship_property_exists(&catalog, &relationships, RelTypeId(0), "key"),
            rel_exists,
            "rel exists",
        );
        check(
            validate_node_property_exists_constraints(&catalog, &nodes),
            !exists || node_exists,
            "node exists catalog",
        );
        check(
            validate_relationship_property_exists_constraints(&catalog, &relationships),
            !exists || rel_exists,
            "rel exists catalog",
        );
        for node in nodes.values() {
            let selected = node.labels.contains(&LabelId(0));
            let value = node.properties.get("key");
            let expected = !selected
                || ((!schema_active || reference_type(type_name, nullable, value))
                    && (!exists || value.is_some_and(|value| value != &Value::Null)));
            check(
                validate_node_record_constraints(&catalog, node),
                expected,
                "node record",
            );
        }
        for rel in relationships.values() {
            let selected = rel.rel_type == RelTypeId(0);
            let value = rel.properties.get("key");
            let expected = !selected
                || ((!schema_active || reference_type(type_name, nullable, value))
                    && (!exists || value.is_some_and(|value| value != &Value::Null)));
            check(
                validate_relationship_record_constraints(&catalog, rel),
                expected,
                "rel record",
            );
        }
        assert_eq!(
            nodes.iter().collect::<Vec<_>>(),
            before_nodes.iter().collect::<Vec<_>>()
        );
        assert_eq!(
            relationships.iter().collect::<Vec<_>>(),
            before_relationships.iter().collect::<Vec<_>>()
        );
    }
    checks
}

#[test]
fn graph_constraints_match_independent_reference_models() {
    assert_eq!(check_seed(0), 400);
    assert_eq!(check_seed(7), 400);
}

#[test]
#[ignore = "explicit local graph-constraint differential campaign"]
fn graph_constraints_differential_campaign() {
    let checks: usize = (0..128).map(check_seed).sum();
    assert_eq!(checks, 51_200);
    println!("graph constraints: 128 seeds, 2048 snapshots, {checks} validation checks");
}
