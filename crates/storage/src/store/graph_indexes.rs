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

//! Schema and index DDL, maintenance, backfill, and constraint validation methods for [`GraphStore`].

use super::*;

impl GraphStore {
    pub fn create_node_label(&mut self, catalog: &mut Catalog, label: &str) -> Result<LabelId> {
        if let Some(id) = catalog.label_id(label) {
            return Ok(id);
        }
        self.append_durable_wal_batch(&[WalOp::CreateNodeLabel {
            label: label.to_string(),
        }])?;
        let id = catalog.get_or_create_label(label);
        self.finish_non_relational_commit();
        Ok(id)
    }

    pub fn create_relationship_type(
        &mut self,
        catalog: &mut Catalog,
        rel_type: &str,
    ) -> Result<RelTypeId> {
        if let Some(id) = catalog.rel_type_id(rel_type) {
            return Ok(id);
        }
        self.append_durable_wal_batch(&[WalOp::CreateRelationshipType {
            rel_type: rel_type.to_string(),
        }])?;
        let id = catalog.get_or_create_rel_type(rel_type);
        self.finish_non_relational_commit();
        Ok(id)
    }

    pub fn create_node_table(&mut self, catalog: &mut Catalog, name: &str) -> Result<TableId> {
        if let Some(id) = catalog.table_id(TableKind::Node, name) {
            return Ok(id);
        }
        self.append_durable_wal_batch(&[WalOp::CreateNodeTable {
            name: name.to_string(),
        }])?;
        catalog.get_or_create_label(name);
        let id = catalog.get_or_create_table(TableKind::Node, name);
        self.finish_non_relational_commit();
        Ok(id)
    }

    pub fn create_relationship_table(
        &mut self,
        catalog: &mut Catalog,
        name: &str,
    ) -> Result<TableId> {
        if let Some(id) = catalog.table_id(TableKind::Relationship, name) {
            return Ok(id);
        }
        self.append_durable_wal_batch(&[WalOp::CreateRelationshipTable {
            name: name.to_string(),
        }])?;
        catalog.get_or_create_rel_type(name);
        let id = catalog.get_or_create_table(TableKind::Relationship, name);
        self.finish_non_relational_commit();
        Ok(id)
    }

    pub fn create_property_descriptor(
        &mut self,
        catalog: &mut Catalog,
        table_kind: TableKind,
        table: &str,
        property: &str,
        value_type: PropertyType,
        nullable: bool,
    ) -> Result<PropertyId> {
        let table_id = ensure_table_descriptor(catalog, table_kind, table);
        if let Some(id) = catalog.property_descriptor_id(table_id, property) {
            return Ok(id);
        }
        validate_property_descriptor(catalog, self, table_id, property, value_type, nullable)?;
        self.append_durable_wal_batch(&[WalOp::CreateProperty {
            table_kind,
            table: table.to_string(),
            property: property.to_string(),
            value_type,
            nullable,
        }])?;
        let id = catalog.get_or_create_property(table_id, property, value_type, nullable);
        self.finish_non_relational_commit();
        Ok(id)
    }

    pub fn alter_table_state(
        &mut self,
        catalog: &mut Catalog,
        table_kind: TableKind,
        table: &str,
        state: SchemaObjectState,
    ) -> Result<(TableId, bool)> {
        let Some(id) = catalog.table_id(table_kind, table) else {
            return Err(HawDBError::Storage(format!(
                "schema table '{table}' does not exist"
            )));
        };
        let Some(descriptor) = catalog.table_descriptor(id) else {
            return Err(HawDBError::Storage(format!(
                "schema table '{table}' does not exist"
            )));
        };
        if descriptor.state == state {
            return Ok((id, false));
        }
        self.append_durable_wal_batch(&[WalOp::AlterTableState {
            table_kind,
            table: table.to_string(),
            state,
        }])?;
        catalog.set_table_state(id, state);
        self.finish_non_relational_commit();
        Ok((id, true))
    }

    pub fn alter_property_state(
        &mut self,
        catalog: &mut Catalog,
        table_kind: TableKind,
        table: &str,
        property: &str,
        state: SchemaObjectState,
    ) -> Result<(PropertyId, bool)> {
        let Some(table_id) = catalog.table_id(table_kind, table) else {
            return Err(HawDBError::Storage(format!(
                "schema table '{table}' does not exist"
            )));
        };
        let Some(id) = catalog.property_descriptor_id(table_id, property) else {
            return Err(HawDBError::Storage(format!(
                "schema property '{table}.{property}' does not exist"
            )));
        };
        let Some(descriptor) = catalog.property_descriptor(id) else {
            return Err(HawDBError::Storage(format!(
                "schema property '{table}.{property}' does not exist"
            )));
        };
        if descriptor.state == state {
            return Ok((id, false));
        }
        if state == SchemaObjectState::Public {
            validate_property_descriptor(
                catalog,
                self,
                table_id,
                property,
                descriptor.value_type,
                descriptor.nullable,
            )?;
        }
        self.append_durable_wal_batch(&[WalOp::AlterPropertyState {
            table_kind,
            table: table.to_string(),
            property: property.to_string(),
            state,
        }])?;
        catalog.set_property_state(id, state);
        self.finish_non_relational_commit();
        Ok((id, true))
    }

    pub fn run_schema_maintenance(
        &mut self,
        catalog: &mut Catalog,
    ) -> Result<Vec<SchemaMaintenanceAction>> {
        self.run_schema_maintenance_with_budget(catalog, None)
    }

    pub fn run_bounded_schema_maintenance(
        &mut self,
        catalog: &mut Catalog,
        max_estimated_operations: usize,
    ) -> Result<Vec<SchemaMaintenanceAction>> {
        self.run_schema_maintenance_with_budget(catalog, Some(max_estimated_operations))
    }

    fn run_schema_maintenance_with_budget(
        &mut self,
        catalog: &mut Catalog,
        max_estimated_operations: Option<usize>,
    ) -> Result<Vec<SchemaMaintenanceAction>> {
        let mut ops = Vec::new();
        let mut actions = Vec::new();
        let mut used_estimated_operations = 0usize;

        let gc_table_ids = catalog
            .table_descriptors()
            .filter(|table| table.state == SchemaObjectState::Gc)
            .map(|table| table.id)
            .collect::<BTreeSet<_>>();

        for property in catalog.property_descriptors().cloned().collect::<Vec<_>>() {
            if gc_table_ids.contains(&property.table_id) {
                continue;
            }
            let Some(table) = catalog.table_descriptor(property.table_id).cloned() else {
                continue;
            };
            match property.state {
                SchemaObjectState::Backfill => {
                    let estimated_operations =
                        self.schema_table_record_count(catalog, &table).max(1);
                    if !reserve_schema_maintenance_budget(
                        &mut used_estimated_operations,
                        max_estimated_operations,
                        estimated_operations,
                    ) {
                        continue;
                    }
                    validate_property_descriptor(
                        catalog,
                        self,
                        property.table_id,
                        &property.name,
                        property.value_type,
                        property.nullable,
                    )?;
                    ops.push(WalOp::AlterPropertyState {
                        table_kind: table.kind,
                        table: table.name.clone(),
                        property: property.name.clone(),
                        state: SchemaObjectState::Validating,
                    });
                    actions.push(SchemaMaintenanceAction {
                        object_type: "property".to_string(),
                        object: format!("{}.{}", table.name, property.name),
                        from_state: property.state,
                        to_state: Some(SchemaObjectState::Validating),
                        action: "advance".to_string(),
                    });
                }
                SchemaObjectState::Validating => {
                    let estimated_operations =
                        self.schema_table_record_count(catalog, &table).max(1);
                    if !reserve_schema_maintenance_budget(
                        &mut used_estimated_operations,
                        max_estimated_operations,
                        estimated_operations,
                    ) {
                        continue;
                    }
                    validate_property_descriptor(
                        catalog,
                        self,
                        property.table_id,
                        &property.name,
                        property.value_type,
                        property.nullable,
                    )?;
                    ops.push(WalOp::AlterPropertyState {
                        table_kind: table.kind,
                        table: table.name.clone(),
                        property: property.name.clone(),
                        state: SchemaObjectState::Public,
                    });
                    actions.push(SchemaMaintenanceAction {
                        object_type: "property".to_string(),
                        object: format!("{}.{}", table.name, property.name),
                        from_state: property.state,
                        to_state: Some(SchemaObjectState::Public),
                        action: "advance".to_string(),
                    });
                }
                SchemaObjectState::Gc => {
                    if !reserve_schema_maintenance_budget(
                        &mut used_estimated_operations,
                        max_estimated_operations,
                        1,
                    ) {
                        continue;
                    }
                    ops.push(WalOp::GcPropertyDescriptor {
                        table_kind: table.kind,
                        table: table.name.clone(),
                        property: property.name.clone(),
                    });
                    actions.push(SchemaMaintenanceAction {
                        object_type: "property".to_string(),
                        object: format!("{}.{}", table.name, property.name),
                        from_state: property.state,
                        to_state: None,
                        action: "gc".to_string(),
                    });
                }
                SchemaObjectState::DeleteOnly
                | SchemaObjectState::WriteOnly
                | SchemaObjectState::Public => {}
            }
        }

        for table in catalog.table_descriptors().cloned().collect::<Vec<_>>() {
            match table.state {
                SchemaObjectState::Backfill => {
                    let estimated_operations =
                        self.schema_table_record_count(catalog, &table).max(1);
                    if !reserve_schema_maintenance_budget(
                        &mut used_estimated_operations,
                        max_estimated_operations,
                        estimated_operations,
                    ) {
                        continue;
                    }
                    ops.push(WalOp::AlterTableState {
                        table_kind: table.kind,
                        table: table.name.clone(),
                        state: SchemaObjectState::Validating,
                    });
                    actions.push(SchemaMaintenanceAction {
                        object_type: "table".to_string(),
                        object: table.name.clone(),
                        from_state: table.state,
                        to_state: Some(SchemaObjectState::Validating),
                        action: "advance".to_string(),
                    });
                }
                SchemaObjectState::Validating => {
                    let active_property_count = catalog
                        .property_descriptors()
                        .filter(|property| {
                            property.table_id == table.id && property.state != SchemaObjectState::Gc
                        })
                        .count()
                        .max(1);
                    let estimated_operations = self
                        .schema_table_record_count(catalog, &table)
                        .max(1)
                        .saturating_mul(active_property_count);
                    if !reserve_schema_maintenance_budget(
                        &mut used_estimated_operations,
                        max_estimated_operations,
                        estimated_operations,
                    ) {
                        continue;
                    }
                    validate_table_descriptor(catalog, self, table.id)?;
                    ops.push(WalOp::AlterTableState {
                        table_kind: table.kind,
                        table: table.name.clone(),
                        state: SchemaObjectState::Public,
                    });
                    actions.push(SchemaMaintenanceAction {
                        object_type: "table".to_string(),
                        object: table.name.clone(),
                        from_state: table.state,
                        to_state: Some(SchemaObjectState::Public),
                        action: "advance".to_string(),
                    });
                }
                SchemaObjectState::Gc => {
                    let properties = catalog
                        .property_descriptors()
                        .filter(|property| property.table_id == table.id)
                        .cloned()
                        .collect::<Vec<_>>();
                    let estimated_operations = properties.len().saturating_add(1);
                    if !reserve_schema_maintenance_budget(
                        &mut used_estimated_operations,
                        max_estimated_operations,
                        estimated_operations,
                    ) {
                        continue;
                    }
                    for property in properties {
                        ops.push(WalOp::GcPropertyDescriptor {
                            table_kind: table.kind,
                            table: table.name.clone(),
                            property: property.name.clone(),
                        });
                        actions.push(SchemaMaintenanceAction {
                            object_type: "property".to_string(),
                            object: format!("{}.{}", table.name, property.name),
                            from_state: property.state,
                            to_state: None,
                            action: "gc".to_string(),
                        });
                    }
                    ops.push(WalOp::GcTableDescriptor {
                        table_kind: table.kind,
                        table: table.name.clone(),
                    });
                    actions.push(SchemaMaintenanceAction {
                        object_type: "table".to_string(),
                        object: table.name.clone(),
                        from_state: table.state,
                        to_state: None,
                        action: "gc".to_string(),
                    });
                }
                SchemaObjectState::DeleteOnly
                | SchemaObjectState::WriteOnly
                | SchemaObjectState::Public => {}
            }
        }

        if ops.is_empty() {
            return Ok(actions);
        }
        self.append_durable_wal_batch(&ops)?;
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        for op in ops {
            self.apply_schema_maintenance_op(catalog, op);
        }
        self.finish_non_relational_commit();
        Ok(actions)
    }

    pub fn plan_schema_maintenance(&self, catalog: &Catalog) -> Vec<SchemaMaintenancePlanItem> {
        let mut plan = Vec::new();

        let gc_table_ids = catalog
            .table_descriptors()
            .filter(|table| table.state == SchemaObjectState::Gc)
            .map(|table| table.id)
            .collect::<BTreeSet<_>>();

        for property in catalog.property_descriptors().cloned() {
            if gc_table_ids.contains(&property.table_id) {
                continue;
            }
            let Some(table) = catalog.table_descriptor(property.table_id) else {
                continue;
            };
            let estimated_operations = self.schema_table_record_count(catalog, table).max(1);
            match property.state {
                SchemaObjectState::Backfill => {
                    plan.push(SchemaMaintenancePlanItem {
                        object_type: "property".to_string(),
                        object: format!("{}.{}", table.name, property.name),
                        from_state: property.state,
                        to_state: Some(SchemaObjectState::Validating),
                        action: "advance".to_string(),
                        estimated_operations,
                    });
                }
                SchemaObjectState::Validating => {
                    plan.push(SchemaMaintenancePlanItem {
                        object_type: "property".to_string(),
                        object: format!("{}.{}", table.name, property.name),
                        from_state: property.state,
                        to_state: Some(SchemaObjectState::Public),
                        action: "advance".to_string(),
                        estimated_operations,
                    });
                }
                SchemaObjectState::Gc => {
                    plan.push(SchemaMaintenancePlanItem {
                        object_type: "property".to_string(),
                        object: format!("{}.{}", table.name, property.name),
                        from_state: property.state,
                        to_state: None,
                        action: "gc".to_string(),
                        estimated_operations: 1,
                    });
                }
                SchemaObjectState::DeleteOnly
                | SchemaObjectState::WriteOnly
                | SchemaObjectState::Public => {}
            }
        }

        for table in catalog.table_descriptors().cloned() {
            let record_count = self.schema_table_record_count(catalog, &table).max(1);
            match table.state {
                SchemaObjectState::Backfill => {
                    plan.push(SchemaMaintenancePlanItem {
                        object_type: "table".to_string(),
                        object: table.name.clone(),
                        from_state: table.state,
                        to_state: Some(SchemaObjectState::Validating),
                        action: "advance".to_string(),
                        estimated_operations: record_count,
                    });
                }
                SchemaObjectState::Validating => {
                    let active_property_count = catalog
                        .property_descriptors()
                        .filter(|property| {
                            property.table_id == table.id && property.state != SchemaObjectState::Gc
                        })
                        .count()
                        .max(1);
                    plan.push(SchemaMaintenancePlanItem {
                        object_type: "table".to_string(),
                        object: table.name.clone(),
                        from_state: table.state,
                        to_state: Some(SchemaObjectState::Public),
                        action: "advance".to_string(),
                        estimated_operations: record_count.saturating_mul(active_property_count),
                    });
                }
                SchemaObjectState::Gc => {
                    for property in catalog
                        .property_descriptors()
                        .filter(|property| property.table_id == table.id)
                    {
                        plan.push(SchemaMaintenancePlanItem {
                            object_type: "property".to_string(),
                            object: format!("{}.{}", table.name, property.name),
                            from_state: property.state,
                            to_state: None,
                            action: "gc".to_string(),
                            estimated_operations: 1,
                        });
                    }
                    plan.push(SchemaMaintenancePlanItem {
                        object_type: "table".to_string(),
                        object: table.name.clone(),
                        from_state: table.state,
                        to_state: None,
                        action: "gc".to_string(),
                        estimated_operations: 1,
                    });
                }
                SchemaObjectState::DeleteOnly
                | SchemaObjectState::WriteOnly
                | SchemaObjectState::Public => {}
            }
        }

        plan
    }

    fn schema_table_record_count(&self, catalog: &Catalog, table: &TableDescriptor) -> usize {
        match table.kind {
            TableKind::Node => {
                let Some(label_id) = catalog.label_id(&table.name) else {
                    return 0;
                };
                self.index_label_record_count(label_id)
            }
            TableKind::Relationship => {
                let Some(rel_type_id) = catalog.rel_type_id(&table.name) else {
                    return 0;
                };
                self.relationships
                    .values()
                    .filter(|relationship| relationship.rel_type == rel_type_id)
                    .count()
            }
        }
    }

    fn index_label_record_count(&self, label_id: LabelId) -> usize {
        self.nodes
            .values()
            .filter(|node| node.labels.contains(&label_id))
            .count()
    }

    pub fn create_property_index(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        property: &str,
    ) -> Result<IndexId> {
        let label_id = catalog.get_or_create_label(label);
        if let Some(id) = catalog.property_index_id(label_id, property) {
            return Ok(id);
        }
        self.append_durable_wal_batch(&[WalOp::CreateIndex {
            label: label.to_string(),
            property: property.to_string(),
        }])?;
        let id = catalog.get_or_create_property_index(label_id, property);
        self.backfill_property_index(catalog, label_id, property);
        self.finish_non_relational_commit();
        Ok(id)
    }

    pub fn create_composite_property_index(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        properties: &[String],
    ) -> Result<IndexId> {
        if properties.len() < 2 {
            return Err(HawDBError::Storage(
                "composite index requires at least two properties".to_string(),
            ));
        }
        let label_id = catalog.get_or_create_label(label);
        if let Some(id) = catalog.composite_property_index_id(label_id, properties) {
            return Ok(id);
        }
        self.append_durable_wal_batch(&[WalOp::CreateCompositeIndex {
            label: label.to_string(),
            properties: properties.to_vec(),
        }])?;
        let id = catalog.get_or_create_composite_property_index(label_id, properties);
        self.rebuild_composite_property_index_for_descriptor(id, label_id, properties);
        self.finish_non_relational_commit();
        Ok(id)
    }

    pub fn create_range_property_index(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        property: &str,
    ) -> Result<IndexId> {
        let label_id = catalog.get_or_create_label(label);
        if let Some(id) = catalog.property_index_id_with_kind(label_id, property, IndexKind::Range)
        {
            return Ok(id);
        }
        self.append_durable_wal_batch(&[WalOp::CreateRangeIndex {
            label: label.to_string(),
            property: property.to_string(),
        }])?;
        let id =
            catalog.get_or_create_property_index_with_kind(label_id, property, IndexKind::Range);
        self.backfill_property_index(catalog, label_id, property);
        self.finish_non_relational_commit();
        Ok(id)
    }

    pub fn create_full_text_property_index(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        property: &str,
    ) -> Result<IndexId> {
        let label_id = catalog.get_or_create_label(label);
        if let Some(id) =
            catalog.property_index_id_with_kind(label_id, property, IndexKind::FullText)
        {
            return Ok(id);
        }
        self.append_durable_wal_batch(&[WalOp::CreateFullTextIndex {
            label: label.to_string(),
            property: property.to_string(),
        }])?;
        let id =
            catalog.get_or_create_property_index_with_kind(label_id, property, IndexKind::FullText);
        self.rebuild_full_text_property_index_for_descriptor(label_id, property);
        self.finish_non_relational_commit();
        Ok(id)
    }

    pub fn rebuild_bounded_property_index_projections(
        &mut self,
        catalog: &Catalog,
        max_estimated_operations: usize,
    ) -> Vec<PropertyIndexProjectionRebuildAction> {
        let mut actions = Vec::new();
        let mut used_estimated_operations = 0usize;

        for index in catalog
            .composite_property_indexes()
            .cloned()
            .collect::<Vec<_>>()
        {
            let estimated_operations = self.index_label_record_count(index.label_id).max(1);
            if !reserve_schema_maintenance_budget(
                &mut used_estimated_operations,
                Some(max_estimated_operations),
                estimated_operations,
            ) {
                continue;
            }
            let indexed_entries =
                self.rebuild_composite_property_index_projection(index.label_id, &index.properties);
            if self.canonical_base_out_of_core {
                self.checkpoint_statistics.index_samples.remove(&index.id);
            } else {
                let unique_values = composite_property_index_unique_values(
                    &self.composite_property_index,
                    index.label_id,
                    &index.properties,
                );
                self.checkpoint_statistics.index_samples.insert(
                    index.id,
                    IndexStatisticsSample::exact(indexed_entries as u64, unique_values),
                );
            }
            actions.push(PropertyIndexProjectionRebuildAction {
                index_kind: "composite".to_string(),
                label: catalog
                    .label_name(index.label_id)
                    .unwrap_or("<unknown>")
                    .to_string(),
                properties: index.properties,
                estimated_operations,
                indexed_entries,
            });
        }

        for index in catalog.property_indexes().cloned().collect::<Vec<_>>() {
            if index.kind != IndexKind::FullText {
                continue;
            }
            let estimated_operations = self.index_label_record_count(index.label_id).max(1);
            if !reserve_schema_maintenance_budget(
                &mut used_estimated_operations,
                Some(max_estimated_operations),
                estimated_operations,
            ) {
                continue;
            }
            let indexed_entries =
                self.rebuild_full_text_property_index_projection(index.label_id, &index.property);
            actions.push(PropertyIndexProjectionRebuildAction {
                index_kind: "full_text".to_string(),
                label: catalog
                    .label_name(index.label_id)
                    .unwrap_or("<unknown>")
                    .to_string(),
                properties: vec![index.property],
                estimated_operations,
                indexed_entries,
            });
        }

        actions
    }

    pub fn bounded_property_index_projection_estimated_operations(
        &self,
        catalog: &Catalog,
        max_estimated_operations: usize,
    ) -> usize {
        let mut used_estimated_operations = 0usize;

        for index in catalog.composite_property_indexes() {
            let estimated_operations = self.index_label_record_count(index.label_id).max(1);
            let _ = reserve_schema_maintenance_budget(
                &mut used_estimated_operations,
                Some(max_estimated_operations),
                estimated_operations,
            );
        }

        for index in catalog.property_indexes() {
            if index.kind != IndexKind::FullText {
                continue;
            }
            let estimated_operations = self.index_label_record_count(index.label_id).max(1);
            let _ = reserve_schema_maintenance_budget(
                &mut used_estimated_operations,
                Some(max_estimated_operations),
                estimated_operations,
            );
        }

        used_estimated_operations
    }

    pub fn property_index_projection_estimated_operations(&self, catalog: &Catalog) -> usize {
        let composite_operations = catalog
            .composite_property_indexes()
            .map(|index| self.index_label_record_count(index.label_id).max(1))
            .fold(0usize, usize::saturating_add);
        let full_text_operations = catalog
            .property_indexes()
            .filter(|index| index.kind == IndexKind::FullText)
            .map(|index| self.index_label_record_count(index.label_id).max(1))
            .fold(0usize, usize::saturating_add);
        composite_operations.saturating_add(full_text_operations)
    }

    pub fn create_unique_constraint(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        property: &str,
    ) -> Result<ConstraintId> {
        let label_id = catalog.get_or_create_label(label);
        if let Some(id) = catalog.unique_constraint_id(label_id, property) {
            return Ok(id);
        }
        self.validate_unique_constraint(catalog, label_id, property)?;
        self.append_durable_wal_batch(&[WalOp::CreateUniqueConstraint {
            label: label.to_string(),
            property: property.to_string(),
        }])?;
        let id = catalog.get_or_create_unique_constraint(label_id, property);
        self.finish_non_relational_commit();
        Ok(id)
    }

    pub fn create_node_property_exists_constraint(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        property: &str,
    ) -> Result<ConstraintId> {
        let label_id = catalog.get_or_create_label(label);
        if let Some(id) = catalog.node_property_exists_constraint_id(label_id, property) {
            return Ok(id);
        }
        self.validate_node_property_exists_constraint(catalog, label_id, property)?;
        self.append_durable_wal_batch(&[WalOp::CreateNodePropertyExistsConstraint {
            label: label.to_string(),
            property: property.to_string(),
        }])?;
        let id = catalog.get_or_create_node_property_exists_constraint(label_id, property);
        self.finish_non_relational_commit();
        Ok(id)
    }

    pub fn create_relationship_property_exists_constraint(
        &mut self,
        catalog: &mut Catalog,
        rel_type: &str,
        property: &str,
    ) -> Result<ConstraintId> {
        let rel_type_id = catalog.get_or_create_rel_type(rel_type);
        if let Some(id) = catalog.relationship_property_exists_constraint_id(rel_type_id, property)
        {
            return Ok(id);
        }
        self.validate_relationship_property_exists_constraint(catalog, rel_type_id, property)?;
        self.append_durable_wal_batch(&[WalOp::CreateRelationshipPropertyExistsConstraint {
            rel_type: rel_type.to_string(),
            property: property.to_string(),
        }])?;
        let id =
            catalog.get_or_create_relationship_property_exists_constraint(rel_type_id, property);
        self.finish_non_relational_commit();
        Ok(id)
    }

    pub fn create_relationship_unique_constraint(
        &mut self,
        catalog: &mut Catalog,
        rel_type: &str,
        property: &str,
    ) -> Result<ConstraintId> {
        let rel_type_id = catalog.get_or_create_rel_type(rel_type);
        if let Some(id) = catalog.relationship_unique_constraint_id(rel_type_id, property) {
            return Ok(id);
        }
        self.validate_relationship_unique_constraint(catalog, rel_type_id, property)?;
        self.append_durable_wal_batch(&[WalOp::CreateRelationshipUniqueConstraint {
            rel_type: rel_type.to_string(),
            property: property.to_string(),
        }])?;
        let id = catalog.get_or_create_relationship_unique_constraint(rel_type_id, property);
        self.finish_non_relational_commit();
        Ok(id)
    }

    pub fn property_index_consistency_report(
        &self,
        catalog: &Catalog,
    ) -> PropertyIndexConsistencyReport {
        let recomputed_node_index = recompute_node_property_index(&self.nodes, catalog);
        let recomputed_relationship_index =
            recompute_relationship_property_index(&self.relationships);
        PropertyIndexConsistencyReport::new(
            self.commit_epoch,
            &self.property_index,
            &recomputed_node_index,
            &self.relationship_property_index,
            &recomputed_relationship_index,
        )
    }

    pub(super) fn add_node_to_composite_property_indexes(
        &mut self,
        catalog: &Catalog,
        node: &NodeRecord,
    ) {
        for index in catalog.composite_property_indexes() {
            if !node.labels.contains(&index.label_id) {
                continue;
            }
            let Some(key) = composite_property_index_key(node, &index.properties) else {
                continue;
            };
            self.composite_property_index
                .entry_or_default((index.label_id, key))
                .insert(node.id);
        }
    }

    pub(super) fn remove_node_from_composite_property_indexes(
        &mut self,
        catalog: &Catalog,
        node: &NodeRecord,
    ) {
        for index in catalog.composite_property_indexes() {
            if !node.labels.contains(&index.label_id) {
                continue;
            }
            let Some(key) = composite_property_index_key(node, &index.properties) else {
                continue;
            };
            let map_key = (index.label_id, key);
            if let Some(ids) = self.composite_property_index.get_mut(&map_key) {
                ids.remove(&node.id);
                if ids.is_empty() {
                    self.composite_property_index.remove(&map_key);
                }
            }
        }
    }

    /// Indexes the nodes that already carry `property` under `label_id`.
    ///
    /// Only declared properties are indexed on write, so a newly declared
    /// index starts empty and would be missing exactly the nodes written
    /// before the declaration. The pruner treats a declared index as
    /// complete, so an unbackfilled one makes queries omit rows rather than
    /// run slowly.
    pub(super) fn backfill_property_index(
        &mut self,
        catalog: &Catalog,
        label_id: LabelId,
        property: &str,
    ) {
        let nodes = self.nodes.values().cloned().collect::<Vec<_>>();
        let mut statistics_eligible = true;
        for node in nodes {
            if !node.labels.contains(&label_id) {
                continue;
            }
            let Some(value) = node.properties.get(property) else {
                continue;
            };
            if statistics_eligible
                && !node_property_supports_optimizer_statistics(
                    Some(catalog),
                    label_id,
                    property,
                    value,
                )
            {
                statistics_eligible = false;
            }
            self.property_index
                .entry_or_default((label_id, property.to_string(), value.clone()))
                .insert(node.id);
        }
        if self.canonical_base_out_of_core {
            self.checkpoint_statistics
                .property_distinct_counts
                .remove(&(label_id, property.to_string()));
            for index in catalog.property_indexes().filter(|index| {
                index.label_id == label_id
                    && index.property == property
                    && index.kind != IndexKind::FullText
            }) {
                self.checkpoint_statistics.index_samples.remove(&index.id);
            }
            return;
        }
        // The backfill already walked every node, so the distinct count costs
        // nothing extra here. Deferring it to the next checkpoint would leave
        // the optimizer on its no-statistics fallback for a property the user
        // just asked to index, which is the case where a good estimate is
        // most likely to be wanted.
        let (index_size, unique_values) =
            scalar_property_index_cardinality(&self.property_index, label_id, property);
        if !statistics_eligible || unique_values == 0 {
            self.checkpoint_statistics
                .property_distinct_counts
                .remove(&(label_id, property.to_string()));
        } else {
            self.checkpoint_statistics
                .property_distinct_counts
                .insert((label_id, property.to_string()), unique_values);
        }
        for index in catalog.property_indexes().filter(|index| {
            index.label_id == label_id
                && index.property == property
                && index.kind != IndexKind::FullText
        }) {
            self.checkpoint_statistics.index_samples.insert(
                index.id,
                IndexStatisticsSample::exact(index_size, unique_values),
            );
        }
    }

    pub(super) fn rebuild_composite_property_index_for_descriptor(
        &mut self,
        index_id: IndexId,
        label_id: LabelId,
        properties: &[String],
    ) {
        let indexed_entries =
            self.rebuild_composite_property_index_projection(label_id, properties) as u64;
        if self.canonical_base_out_of_core {
            self.checkpoint_statistics.index_samples.remove(&index_id);
            return;
        }
        let unique_values = composite_property_index_unique_values(
            &self.composite_property_index,
            label_id,
            properties,
        );
        self.checkpoint_statistics.index_samples.insert(
            index_id,
            IndexStatisticsSample::exact(indexed_entries, unique_values),
        );
    }

    fn rebuild_composite_property_index_projection(
        &mut self,
        label_id: LabelId,
        properties: &[String],
    ) -> usize {
        self.composite_property_index
            .retain(|(candidate_label_id, key), _| {
                *candidate_label_id != label_id
                    || key
                        .iter()
                        .map(|(property, _)| property)
                        .ne(properties.iter())
            });
        let mut indexed_entries = 0usize;
        let nodes = self.nodes.values().cloned().collect::<Vec<_>>();
        for node in nodes {
            if !node.labels.contains(&label_id) {
                continue;
            }
            let Some(key) = composite_property_index_key(&node, properties) else {
                continue;
            };
            self.composite_property_index
                .entry_or_default((label_id, key))
                .insert(node.id);
            indexed_entries = indexed_entries.saturating_add(1);
        }
        indexed_entries
    }

    pub(super) fn add_node_to_full_text_property_indexes(
        &mut self,
        catalog: &Catalog,
        node: &NodeRecord,
    ) {
        for index in catalog.property_indexes() {
            if index.kind != IndexKind::FullText || !node.labels.contains(&index.label_id) {
                continue;
            }
            let Some(Value::String(value)) = node.properties.get(&index.property) else {
                continue;
            };
            for token in full_text_index_tokens(value) {
                self.full_text_property_index
                    .entry_or_default((index.label_id, index.property.clone(), token))
                    .insert(node.id);
            }
        }
    }

    pub(super) fn remove_node_from_full_text_property_indexes(
        &mut self,
        catalog: &Catalog,
        node: &NodeRecord,
    ) {
        for index in catalog.property_indexes() {
            if index.kind != IndexKind::FullText || !node.labels.contains(&index.label_id) {
                continue;
            }
            let Some(Value::String(value)) = node.properties.get(&index.property) else {
                continue;
            };
            for token in full_text_index_tokens(value) {
                let map_key = (index.label_id, index.property.clone(), token);
                if let Some(ids) = self.full_text_property_index.get_mut(&map_key) {
                    ids.remove(&node.id);
                    if ids.is_empty() {
                        self.full_text_property_index.remove(&map_key);
                    }
                }
            }
        }
    }

    pub(super) fn rebuild_full_text_property_index_for_descriptor(
        &mut self,
        label_id: LabelId,
        property: &str,
    ) {
        self.rebuild_full_text_property_index_projection(label_id, property);
    }

    fn rebuild_full_text_property_index_projection(
        &mut self,
        label_id: LabelId,
        property: &str,
    ) -> usize {
        self.full_text_property_index
            .retain(|(candidate_label_id, candidate_property, _), _| {
                *candidate_label_id != label_id || candidate_property != property
            });
        let mut indexed_entries = 0usize;
        let nodes = self.nodes.values().cloned().collect::<Vec<_>>();
        for node in nodes {
            if !node.labels.contains(&label_id) {
                continue;
            }
            let Some(Value::String(value)) = node.properties.get(property) else {
                continue;
            };
            for token in full_text_index_tokens(value) {
                self.full_text_property_index
                    .entry_or_default((label_id, property.to_string(), token))
                    .insert(node.id);
                indexed_entries = indexed_entries.saturating_add(1);
            }
        }
        indexed_entries
    }

    /// Whether an equality index is declared for `property`, considering the
    /// label the scan is restricted to.
    ///
    /// An unlabelled scan would have to consult every label's index, so it
    /// only prunes when every label that declares the property agrees. The
    /// conservative answer is to decline, which costs a scan rather than a
    /// wrong result.
    pub(super) fn indexes_property(
        &self,
        catalog: &Catalog,
        label_id: Option<LabelId>,
        property: &str,
    ) -> bool {
        match label_id {
            Some(label_id) => catalog.property_index_id(label_id, property).is_some(),
            None => false,
        }
    }

    pub(super) fn validate_constraints_for_ops(
        &self,
        catalog: &Catalog,
        ops: &[WalOp],
    ) -> Result<()> {
        wal_codec::validate_wal_op_values(ops).map_err(|error| match error {
            HawDBError::Storage(message) => HawDBError::Semantic(message),
            error => error,
        })?;
        self.ensure_out_of_core_delta_admission(ops)?;
        if self.canonical_base_out_of_core {
            return self.validate_out_of_core_record_changes(catalog, ops);
        }
        let mut nodes = self.nodes.clone();
        let mut relationships = self.relationships.clone();
        for op in ops {
            apply_wal_op_to_snapshot(catalog, &mut nodes, &mut relationships, op);
        }
        match wal_ops_touched_records(ops) {
            // Pure graph-data commits validate only the records they touched:
            // pre-existing records were already valid and unchanged.
            Some(touched) => {
                validate_unique_constraints_for_records(catalog, &nodes, &touched)?;
                validate_relationship_unique_constraints_for_records(
                    catalog,
                    &relationships,
                    &touched,
                )?;
                validate_node_property_exists_constraints_for_records(catalog, &nodes, &touched)?;
                validate_relationship_property_exists_constraints_for_records(
                    catalog,
                    &relationships,
                    &touched,
                )?;
                validate_property_schemas_for_records(catalog, &nodes, &relationships, &touched)
            }
            // Schema/DDL or unrecognized ops require full validation: new
            // constraints and property types apply to pre-existing records.
            None => {
                validate_unique_constraints(catalog, &nodes)?;
                validate_relationship_unique_constraints(catalog, &relationships)?;
                validate_node_property_exists_constraints(catalog, &nodes)?;
                validate_relationship_property_exists_constraints(catalog, &relationships)?;
                validate_property_schemas(catalog, &nodes, &relationships)
            }
        }
    }

    pub(super) fn validate_unique_constraint(
        &self,
        catalog: &Catalog,
        label_id: LabelId,
        property: &str,
    ) -> Result<()> {
        if self.canonical_base_out_of_core {
            return validate_unique_property_streaming(self, catalog, label_id, property);
        }
        validate_unique_property(catalog, &self.nodes, label_id, property)
    }

    pub(super) fn validate_node_property_exists_constraint(
        &self,
        catalog: &Catalog,
        label_id: LabelId,
        property: &str,
    ) -> Result<()> {
        if self.canonical_base_out_of_core {
            let mut violation = None;
            self.visit_nodes_owned(Some(label_id), |node| {
                if !node
                    .properties
                    .get(property)
                    .is_some_and(|value| value != &Value::Null)
                {
                    violation = Some(node.id);
                    GraphScanControl::Stop
                } else {
                    GraphScanControl::Continue
                }
            })?;
            if let Some(id) = violation {
                let label = catalog.label_name(label_id).unwrap_or("<unknown>");
                return Err(HawDBError::Storage(format!(
                    "node property exists constraint violation on :{label}({property}) for node {}",
                    id.0
                )));
            }
            return Ok(());
        }
        validate_node_property_exists(catalog, &self.nodes, label_id, property)
    }

    pub(super) fn validate_relationship_unique_constraint(
        &self,
        catalog: &Catalog,
        rel_type_id: RelTypeId,
        property: &str,
    ) -> Result<()> {
        if self.canonical_base_out_of_core {
            return validate_unique_relationship_property_streaming(
                self,
                catalog,
                rel_type_id,
                property,
            );
        }
        validate_unique_relationship_property(catalog, &self.relationships, rel_type_id, property)
    }

    pub(super) fn validate_relationship_property_exists_constraint(
        &self,
        catalog: &Catalog,
        rel_type_id: RelTypeId,
        property: &str,
    ) -> Result<()> {
        if self.canonical_base_out_of_core {
            let mut violation = None;
            self.visit_relationships_owned(Some(rel_type_id), |relationship| {
                if !relationship
                    .properties
                    .get(property)
                    .is_some_and(|value| value != &Value::Null)
                {
                    violation = Some(relationship.id);
                    GraphScanControl::Stop
                } else {
                    GraphScanControl::Continue
                }
            })?;
            if let Some(id) = violation {
                let rel_type = catalog.rel_type_name(rel_type_id).unwrap_or("<unknown>");
                return Err(HawDBError::Storage(format!(
                    "relationship property exists constraint violation on :{rel_type}({property}) for relationship {}",
                    id.0
                )));
            }
            return Ok(());
        }
        validate_relationship_property_exists(catalog, &self.relationships, rel_type_id, property)
    }

    pub(super) fn validate_relationship_endpoints(&self) -> Result<()> {
        if self.canonical_base_out_of_core {
            let mut validation_error = None;
            self.visit_relationships_owned(None, |relationship| {
                for (kind, node_id) in [
                    ("source", relationship.source),
                    ("target", relationship.target),
                ] {
                    match self.node_owned(node_id) {
                        Ok(Some(_)) => {}
                        Ok(None) => {
                            validation_error = Some(HawDBError::Storage(format!(
                                "relationship {} references missing {kind} node {}",
                                relationship.id.0, node_id.0
                            )));
                            return GraphScanControl::Stop;
                        }
                        Err(error) => {
                            validation_error = Some(error);
                            return GraphScanControl::Stop;
                        }
                    }
                }
                GraphScanControl::Continue
            })?;
            return validation_error.map_or(Ok(()), Err);
        }
        for relationship in self.relationships.values() {
            if !self.nodes.contains_key(&relationship.source) {
                return Err(HawDBError::Storage(format!(
                    "relationship {} references missing source node {}",
                    relationship.id.0, relationship.source.0
                )));
            }
            if !self.nodes.contains_key(&relationship.target) {
                return Err(HawDBError::Storage(format!(
                    "relationship {} references missing target node {}",
                    relationship.id.0, relationship.target.0
                )));
            }
        }
        Ok(())
    }

    pub(super) fn add_relationship_to_property_index(&mut self, relationship: &RelRecord) {
        for (property, value) in &relationship.properties {
            self.relationship_property_index
                .entry_or_default((relationship.rel_type, property.clone(), value.clone()))
                .insert(relationship.id);
        }
    }

    pub(super) fn remove_relationship_from_property_index(&mut self, relationship: &RelRecord) {
        for (property, value) in &relationship.properties {
            let key = (relationship.rel_type, property.clone(), value.clone());
            if let Some(ids) = self.relationship_property_index.get_mut(&key) {
                ids.remove(&relationship.id);
                if ids.is_empty() {
                    self.relationship_property_index.remove(&key);
                }
            }
        }
    }
}
