use super::super::PhysicalPlan;
use hawdb_plan::LogicalPlan;

pub(super) fn lower(logical: &LogicalPlan) -> Option<PhysicalPlan> {
    match logical {
        LogicalPlan::CreateNodeLabel { label } => Some(PhysicalPlan::CreateNodeLabel {
            label: label.clone(),
        }),
        LogicalPlan::CreateRelationshipType { rel_type } => {
            Some(PhysicalPlan::CreateRelationshipType {
                rel_type: rel_type.clone(),
            })
        }
        LogicalPlan::CreateNodeTable { name } => {
            Some(PhysicalPlan::CreateNodeTable { name: name.clone() })
        }
        LogicalPlan::CreateRelationshipTable { name } => {
            Some(PhysicalPlan::CreateRelationshipTable { name: name.clone() })
        }
        LogicalPlan::CreateProperty {
            table_kind,
            table,
            property,
            value_type,
            nullable,
        } => Some(PhysicalPlan::CreateProperty {
            table_kind: *table_kind,
            table: table.clone(),
            property: property.clone(),
            value_type: *value_type,
            nullable: *nullable,
        }),
        LogicalPlan::AlterTableState {
            table_kind,
            table,
            state,
        } => Some(PhysicalPlan::AlterTableState {
            table_kind: *table_kind,
            table: table.clone(),
            state: *state,
        }),
        LogicalPlan::AlterPropertyState {
            table_kind,
            table,
            property,
            state,
        } => Some(PhysicalPlan::AlterPropertyState {
            table_kind: *table_kind,
            table: table.clone(),
            property: property.clone(),
            state: *state,
        }),
        LogicalPlan::CreateIndex { label, property } => Some(PhysicalPlan::CreateIndex {
            label: label.clone(),
            property: property.clone(),
        }),
        LogicalPlan::CreateCompositeIndex { label, properties } => {
            Some(PhysicalPlan::CreateCompositeIndex {
                label: label.clone(),
                properties: properties.clone(),
            })
        }
        LogicalPlan::CreateRangeIndex { label, property } => Some(PhysicalPlan::CreateRangeIndex {
            label: label.clone(),
            property: property.clone(),
        }),
        LogicalPlan::CreateFullTextIndex { label, property } => {
            Some(PhysicalPlan::CreateFullTextIndex {
                label: label.clone(),
                property: property.clone(),
            })
        }
        LogicalPlan::CreateUniqueConstraint { label, property } => {
            Some(PhysicalPlan::CreateUniqueConstraint {
                label: label.clone(),
                property: property.clone(),
            })
        }
        LogicalPlan::CreateNodePropertyExistsConstraint { label, property } => {
            Some(PhysicalPlan::CreateNodePropertyExistsConstraint {
                label: label.clone(),
                property: property.clone(),
            })
        }
        LogicalPlan::CreateRelationshipUniqueConstraint { rel_type, property } => {
            Some(PhysicalPlan::CreateRelationshipUniqueConstraint {
                rel_type: rel_type.clone(),
                property: property.clone(),
            })
        }
        LogicalPlan::CreateRelationshipPropertyExistsConstraint { rel_type, property } => {
            Some(PhysicalPlan::CreateRelationshipPropertyExistsConstraint {
                rel_type: rel_type.clone(),
                property: property.clone(),
            })
        }
        _ => None,
    }
}
