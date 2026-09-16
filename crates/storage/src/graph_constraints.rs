//! Graph record and snapshot constraints shared by storage mutation and recovery.
//!
//! These are internal validation kernels. Root storage retains transaction
//! orchestration, concrete out-of-core scans, and the WAL/publication boundary.

use crate::{CowSegmentedMap, NodeId, NodeRecord, RelId, RelRecord};
use skein_core::{
    Catalog, ConstraintSubject, LabelId, PropertyType, RelTypeId, Result, SchemaObjectState,
    SkeinError, TableKind, Value,
};
use std::collections::BTreeMap;

pub fn validate_property_schemas(
    catalog: &Catalog,
    nodes: &CowSegmentedMap<NodeId, NodeRecord>,
    relationships: &CowSegmentedMap<RelId, RelRecord>,
) -> Result<()> {
    for property in catalog.property_descriptors() {
        if property.state != SchemaObjectState::Public {
            continue;
        }
        let Some(table) = catalog.table_descriptor(property.table_id) else {
            continue;
        };
        if table.state != SchemaObjectState::Public {
            continue;
        }
        match table.kind {
            TableKind::Node => {
                let Some(label_id) = catalog.label_id(&table.name) else {
                    continue;
                };
                for node in nodes.values() {
                    if node.labels.contains(&label_id) {
                        validate_property_schema_value(
                            &table.name,
                            &property.name,
                            property.value_type,
                            property.nullable,
                            node.properties.get(&property.name),
                            &format!("node {}", node.id.0),
                        )?;
                    }
                }
            }
            TableKind::Relationship => {
                let Some(rel_type_id) = catalog.rel_type_id(&table.name) else {
                    continue;
                };
                for relationship in relationships.values() {
                    if relationship.rel_type == rel_type_id {
                        validate_property_schema_value(
                            &table.name,
                            &property.name,
                            property.value_type,
                            property.nullable,
                            relationship.properties.get(&property.name),
                            &format!("relationship {}", relationship.id.0),
                        )?;
                    }
                }
            }
        }
    }
    Ok(())
}

pub fn validate_node_record_constraints(catalog: &Catalog, node: &NodeRecord) -> Result<()> {
    for property in catalog.property_descriptors() {
        if property.state != SchemaObjectState::Public {
            continue;
        }
        let Some(table) = catalog.table_descriptor(property.table_id) else {
            continue;
        };
        if table.kind != TableKind::Node || table.state != SchemaObjectState::Public {
            continue;
        }
        let Some(label_id) = catalog.label_id(&table.name) else {
            continue;
        };
        if node.labels.contains(&label_id) {
            validate_property_schema_value(
                &table.name,
                &property.name,
                property.value_type,
                property.nullable,
                node.properties.get(&property.name),
                &format!("node {}", node.id.0),
            )?;
        }
    }
    for constraint in catalog.node_property_exists_constraints() {
        let ConstraintSubject::Node(label_id) = constraint.subject else {
            continue;
        };
        if node.labels.contains(&label_id)
            && !node
                .properties
                .get(&constraint.property)
                .is_some_and(|value| value != &Value::Null)
        {
            let label = catalog.label_name(label_id).unwrap_or("<unknown>");
            return Err(SkeinError::Storage(format!(
                "node property exists constraint violation on :{label}({}) for node {}",
                constraint.property, node.id.0
            )));
        }
    }
    Ok(())
}

pub fn validate_relationship_record_constraints(
    catalog: &Catalog,
    relationship: &RelRecord,
) -> Result<()> {
    for property in catalog.property_descriptors() {
        if property.state != SchemaObjectState::Public {
            continue;
        }
        let Some(table) = catalog.table_descriptor(property.table_id) else {
            continue;
        };
        if table.kind != TableKind::Relationship || table.state != SchemaObjectState::Public {
            continue;
        }
        let Some(rel_type_id) = catalog.rel_type_id(&table.name) else {
            continue;
        };
        if relationship.rel_type == rel_type_id {
            validate_property_schema_value(
                &table.name,
                &property.name,
                property.value_type,
                property.nullable,
                relationship.properties.get(&property.name),
                &format!("relationship {}", relationship.id.0),
            )?;
        }
    }
    for constraint in catalog.relationship_property_exists_constraints() {
        let ConstraintSubject::Relationship(rel_type_id) = constraint.subject else {
            continue;
        };
        if relationship.rel_type == rel_type_id
            && !relationship
                .properties
                .get(&constraint.property)
                .is_some_and(|value| value != &Value::Null)
        {
            let rel_type = catalog.rel_type_name(rel_type_id).unwrap_or("<unknown>");
            return Err(SkeinError::Storage(format!(
                "relationship property exists constraint violation on :{rel_type}({}) for relationship {}",
                constraint.property, relationship.id.0
            )));
        }
    }
    Ok(())
}

pub fn validate_property_schema_value(
    table: &str,
    property: &str,
    value_type: PropertyType,
    nullable: bool,
    value: Option<&Value>,
    record: &str,
) -> Result<()> {
    let Some(value) = value else {
        if nullable {
            return Ok(());
        }
        return Err(property_schema_error(
            table,
            property,
            record,
            "property is not nullable",
        ));
    };
    if value == &Value::Null {
        if nullable {
            return Ok(());
        }
        return Err(property_schema_error(
            table,
            property,
            record,
            "property is not nullable",
        ));
    }
    let matches = matches!(
        (value_type, value),
        (PropertyType::Any, _)
            | (PropertyType::Bool, Value::Bool(_))
            | (PropertyType::Int, Value::Int(_))
            | (PropertyType::Float, Value::Float(_))
            | (PropertyType::String, Value::String(_))
            | (PropertyType::Text, Value::String(_))
            | (PropertyType::List, Value::List(_))
    );
    if matches {
        Ok(())
    } else {
        Err(property_schema_error(
            table,
            property,
            record,
            &format!("expected {}", encode_property_type(value_type)),
        ))
    }
}

fn property_schema_error(table: &str, property: &str, record: &str, reason: &str) -> SkeinError {
    SkeinError::Storage(format!(
        "property schema violation on {record} in {table}({property}): {reason}"
    ))
}

pub fn validate_unique_constraints(
    catalog: &Catalog,
    nodes: &CowSegmentedMap<NodeId, NodeRecord>,
) -> Result<()> {
    for constraint in catalog.unique_constraints() {
        let ConstraintSubject::Node(label_id) = constraint.subject else {
            continue;
        };
        validate_unique_property(catalog, nodes, label_id, &constraint.property)?;
    }
    Ok(())
}

pub fn validate_relationship_unique_constraints(
    catalog: &Catalog,
    relationships: &CowSegmentedMap<RelId, RelRecord>,
) -> Result<()> {
    for constraint in catalog.relationship_unique_constraints() {
        let ConstraintSubject::Relationship(rel_type_id) = constraint.subject else {
            continue;
        };
        validate_unique_relationship_property(
            catalog,
            relationships,
            rel_type_id,
            &constraint.property,
        )?;
    }
    Ok(())
}

pub fn validate_node_property_exists_constraints(
    catalog: &Catalog,
    nodes: &CowSegmentedMap<NodeId, NodeRecord>,
) -> Result<()> {
    for constraint in catalog.node_property_exists_constraints() {
        let ConstraintSubject::Node(label_id) = constraint.subject else {
            continue;
        };
        validate_node_property_exists(catalog, nodes, label_id, &constraint.property)?;
    }
    Ok(())
}

pub fn validate_relationship_property_exists_constraints(
    catalog: &Catalog,
    relationships: &CowSegmentedMap<RelId, RelRecord>,
) -> Result<()> {
    for constraint in catalog.relationship_property_exists_constraints() {
        let ConstraintSubject::Relationship(rel_type_id) = constraint.subject else {
            continue;
        };
        validate_relationship_property_exists(
            catalog,
            relationships,
            rel_type_id,
            &constraint.property,
        )?;
    }
    Ok(())
}

pub fn validate_node_property_exists(
    catalog: &Catalog,
    nodes: &CowSegmentedMap<NodeId, NodeRecord>,
    label_id: LabelId,
    property: &str,
) -> Result<()> {
    for node in nodes.values() {
        if !node.labels.contains(&label_id) {
            continue;
        }
        match node.properties.get(property) {
            Some(value) if value != &Value::Null => {}
            _ => {
                let label = catalog.label_name(label_id).unwrap_or("<unknown>");
                return Err(SkeinError::Storage(format!(
                    "node property exists constraint violation on :{label}({property}) for node {}",
                    node.id.0
                )));
            }
        }
    }
    Ok(())
}

pub fn validate_relationship_property_exists(
    catalog: &Catalog,
    relationships: &CowSegmentedMap<RelId, RelRecord>,
    rel_type_id: RelTypeId,
    property: &str,
) -> Result<()> {
    for relationship in relationships.values() {
        if relationship.rel_type != rel_type_id {
            continue;
        }
        match relationship.properties.get(property) {
            Some(value) if value != &Value::Null => {}
            _ => {
                let rel_type = catalog.rel_type_name(rel_type_id).unwrap_or("<unknown>");
                return Err(SkeinError::Storage(format!(
                    "relationship property exists constraint violation on :{rel_type}({property}) for relationship {}",
                    relationship.id.0
                )));
            }
        }
    }
    Ok(())
}

pub fn validate_unique_property(
    catalog: &Catalog,
    nodes: &CowSegmentedMap<NodeId, NodeRecord>,
    label_id: LabelId,
    property: &str,
) -> Result<()> {
    let mut seen = BTreeMap::<Value, NodeId>::new();
    for node in nodes.values() {
        if !node.labels.contains(&label_id) {
            continue;
        }
        let Some(value) = node.properties.get(property) else {
            continue;
        };
        if value == &Value::Null {
            continue;
        }
        if let Some(previous) = seen.insert(value.clone(), node.id) {
            let label = catalog.label_name(label_id).unwrap_or("<unknown>");
            return Err(SkeinError::Storage(format!(
                "unique constraint violation on :{label}({property}) for nodes {} and {}",
                previous.0, node.id.0
            )));
        }
    }
    Ok(())
}

pub fn validate_unique_relationship_property(
    catalog: &Catalog,
    relationships: &CowSegmentedMap<RelId, RelRecord>,
    rel_type_id: RelTypeId,
    property: &str,
) -> Result<()> {
    let mut seen = BTreeMap::<Value, RelId>::new();
    for relationship in relationships.values() {
        if relationship.rel_type != rel_type_id {
            continue;
        }
        let Some(value) = relationship.properties.get(property) else {
            continue;
        };
        if value == &Value::Null {
            continue;
        }
        if let Some(previous) = seen.insert(value.clone(), relationship.id) {
            let rel_type = catalog.rel_type_name(rel_type_id).unwrap_or("<unknown>");
            return Err(SkeinError::Storage(format!(
                "relationship unique constraint violation on :{rel_type}({property}) for relationships {} and {}",
                previous.0, relationship.id.0
            )));
        }
    }
    Ok(())
}

pub use crate::text::encode_property_type;

#[cfg(test)]
mod tests;
