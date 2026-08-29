//! WAL-op application, tombstone and adjacency maintenance on write, and basic-statistics upkeep for [`GraphStore`].

use super::*;

impl GraphStore {
    pub(super) fn refresh_basic_statistics_epoch(&mut self) {
        self.basic_statistics.computed_at_commit_epoch = self.commit_epoch;
    }

    fn add_node_to_basic_statistics(&mut self, node: &NodeRecord) {
        self.basic_statistics.node_count += 1;
        for label_id in &node.labels {
            *self
                .basic_statistics
                .label_counts
                .entry(*label_id)
                .or_default() += 1;
        }
        self.refresh_basic_statistics_epoch();
    }

    fn remove_node_from_basic_statistics(&mut self, node: &NodeRecord) {
        self.basic_statistics.node_count = self.basic_statistics.node_count.saturating_sub(1);
        for label_id in &node.labels {
            decrement_counter(&mut self.basic_statistics.label_counts, label_id);
        }
        self.refresh_basic_statistics_epoch();
    }

    fn add_relationship_to_basic_statistics(&mut self, relationship: &RelRecord) {
        self.basic_statistics.relationship_count += 1;
        *self
            .basic_statistics
            .rel_type_counts
            .entry(relationship.rel_type)
            .or_default() += 1;
        self.refresh_basic_statistics_epoch();
    }

    fn remove_relationship_from_basic_statistics(&mut self, relationship: &RelRecord) {
        self.basic_statistics.relationship_count =
            self.basic_statistics.relationship_count.saturating_sub(1);
        decrement_counter(
            &mut self.basic_statistics.rel_type_counts,
            &relationship.rel_type,
        );
        self.refresh_basic_statistics_epoch();
    }

    pub(super) fn apply_create_node(
        &mut self,
        catalog: &Catalog,
        id: NodeId,
        label_id: LabelId,
        properties: BTreeMap<String, Value>,
    ) {
        self.apply_create_node_with_labels(catalog, id, BTreeSet::from([label_id]), properties);
    }

    pub(super) fn apply_create_node_with_labels(
        &mut self,
        catalog: &Catalog,
        id: NodeId,
        labels: BTreeSet<LabelId>,
        properties: BTreeMap<String, Value>,
    ) {
        self.next_node_id = self.next_node_id.max(id.0 + 1);
        self.node_tombstones.remove(&id);
        // Shadow dirty tracking: the new primary table, plus the old primary
        // table when this create replaces a node whose label set differed.
        self.mark_columnar_node_dirty(&labels);
        if let Some(old_node) = self.nodes.remove(&id) {
            self.mark_columnar_node_dirty(&old_node.labels);
            self.remove_node_from_basic_statistics(&old_node);
        }
        self.nodes.insert(
            id,
            NodeRecord {
                id,
                labels,
                properties,
            },
        );
        if let Some(node) = self.nodes.get(&id).cloned() {
            self.add_node_to_basic_statistics(&node);
            for label_id in &node.labels {
                for (property, value) in &node.properties {
                    if !catalog.has_scalar_property_index(*label_id, property) {
                        continue;
                    }
                    self.property_index
                        .entry_or_default((*label_id, property.clone(), value.clone()))
                        .insert(id);
                }
            }
            self.add_node_to_composite_property_indexes(catalog, &node);
            self.add_node_to_full_text_property_indexes(catalog, &node);
        }
    }

    pub(super) fn apply_schema_maintenance_op(&mut self, catalog: &mut Catalog, op: WalOp) {
        match op {
            WalOp::AlterTableState {
                table_kind,
                table,
                state,
            } => {
                if let Some(id) = catalog.table_id(table_kind, &table) {
                    catalog.set_table_state(id, state);
                }
            }
            WalOp::AlterPropertyState {
                table_kind,
                table,
                property,
                state,
            } => {
                if let Some(table_id) = catalog.table_id(table_kind, &table)
                    && let Some(id) = catalog.property_descriptor_id(table_id, &property)
                {
                    catalog.set_property_state(id, state);
                }
            }
            WalOp::GcPropertyDescriptor {
                table_kind,
                table,
                property,
            } => {
                if let Some(table_id) = catalog.table_id(table_kind, &table)
                    && let Some(id) = catalog.property_descriptor_id(table_id, &property)
                {
                    catalog.remove_property_descriptor(id);
                }
            }
            WalOp::GcTableDescriptor { table_kind, table } => {
                if let Some(id) = catalog.table_id(table_kind, &table) {
                    catalog.remove_table_descriptor(id);
                }
            }
            _ => {}
        }
    }

    pub(super) fn apply_create_relationship(
        &mut self,
        id: RelId,
        source: NodeId,
        target: NodeId,
        rel_type: RelTypeId,
        properties: BTreeMap<String, Value>,
    ) {
        self.next_rel_id = self.next_rel_id.max(id.0 + 1);
        self.relationship_tombstones.remove(&id);
        // Shadow dirty tracking: the new type's table, plus the old type's
        // table when this create replaces a relationship of another type.
        self.mark_columnar_relationship_dirty(rel_type);
        if let Some(old_relationship) = self.relationships.remove(&id) {
            self.mark_columnar_relationship_dirty(old_relationship.rel_type);
            self.remove_relationship_from_basic_statistics(&old_relationship);
            self.remove_relationship_from_property_index(&old_relationship);
            self.remove_relationship_from_adjacency(&old_relationship);
        }
        self.relationships.insert(
            id,
            RelRecord {
                id,
                source,
                target,
                rel_type,
                properties,
            },
        );
        if let Some(relationship) = self.relationships.get(&id).cloned() {
            self.add_relationship_to_basic_statistics(&relationship);
            self.add_relationship_to_property_index(&relationship);
        }
        self.outgoing
            .entry_or_default((source, rel_type))
            .insert(OrderedAdjacencyEntry {
                neighbor_id: target,
                relationship_id: id,
            });
        self.incoming
            .entry_or_default((target, rel_type))
            .insert(OrderedAdjacencyEntry {
                neighbor_id: source,
                relationship_id: id,
            });
    }

    /// Plans physical adjacency consolidation without changing graph contents,
    /// epochs, WAL, or checkpoint state.
    pub fn adjacency_consolidation_plan(&self) -> AdjacencyConsolidationPlan {
        adjacency_consolidation_plan(&self.adjacency_consolidation_candidates())
    }

    pub fn bounded_adjacency_consolidation_estimated_entries(
        &self,
        max_estimated_entries: usize,
    ) -> usize {
        let mut remaining_budget = max_estimated_entries;
        let mut estimated_entries = 0usize;
        for candidate in self.adjacency_consolidation_candidates() {
            if candidate.estimated_entries > remaining_budget {
                continue;
            }
            remaining_budget = remaining_budget.saturating_sub(candidate.estimated_entries);
            estimated_entries = estimated_entries.saturating_add(candidate.estimated_entries);
        }
        estimated_entries
    }

    /// Consolidates complete posting groups whose estimated entry work fits in
    /// the caller-provided budget. Oversized groups remain streaming deltas.
    pub fn consolidate_bounded_adjacency_deltas(
        &mut self,
        max_estimated_entries: usize,
    ) -> AdjacencyConsolidationReport {
        let candidates = self.adjacency_consolidation_candidates();
        let planned = adjacency_consolidation_plan(&candidates);
        let mut remaining_budget = max_estimated_entries;
        let mut consolidated_group_count = 0usize;
        let mut consolidated_delta_entry_count = 0usize;
        let mut consolidated_estimated_entries = 0usize;

        for candidate in candidates {
            if candidate.estimated_entries > remaining_budget {
                continue;
            }
            let adjacency = match candidate.direction {
                AdjacencyDirection::Outgoing => &mut self.outgoing,
                AdjacencyDirection::Incoming => &mut self.incoming,
            };
            let Some(posting) = adjacency.get_mut(&candidate.key) else {
                continue;
            };
            if !posting.needs_consolidation() || !posting.consolidate() {
                continue;
            }
            remaining_budget = remaining_budget.saturating_sub(candidate.estimated_entries);
            consolidated_group_count = consolidated_group_count.saturating_add(1);
            consolidated_delta_entry_count =
                consolidated_delta_entry_count.saturating_add(candidate.delta_entry_count);
            consolidated_estimated_entries =
                consolidated_estimated_entries.saturating_add(candidate.estimated_entries);
        }

        AdjacencyConsolidationReport {
            planned,
            consolidated_group_count,
            consolidated_delta_entry_count,
            consolidated_estimated_entries,
            remaining: self.adjacency_consolidation_plan(),
        }
    }

    fn adjacency_consolidation_candidates(&self) -> Vec<AdjacencyConsolidationCandidate> {
        [
            (AdjacencyDirection::Outgoing, &self.outgoing),
            (AdjacencyDirection::Incoming, &self.incoming),
        ]
        .into_iter()
        .flat_map(|(direction, adjacency)| {
            adjacency.iter().filter_map(move |(key, posting)| {
                posting
                    .needs_consolidation()
                    .then_some(AdjacencyConsolidationCandidate {
                        direction,
                        key: *key,
                        delta_entry_count: posting.mini_delta_len(),
                        estimated_entries: posting
                            .pivot_len()
                            .saturating_add(posting.mini_delta_len()),
                    })
            })
        })
        .collect()
    }

    pub(super) fn apply_project_graph_definition(
        &mut self,
        name: String,
        definition: ProjectedGraphDefinition,
    ) {
        self.projected_graph_artifacts.remove(&name);
        self.projected_graphs.insert(name, definition);
    }

    pub(super) fn apply_set_node_property(
        &mut self,
        catalog: &Catalog,
        id: NodeId,
        property: String,
        value: Value,
    ) {
        if let Some(node) = self.nodes.get(&id).cloned() {
            self.remove_node_from_composite_property_indexes(catalog, &node);
            self.remove_node_from_full_text_property_indexes(catalog, &node);
        }
        let Some(node) = self.nodes.get_mut(&id) else {
            return;
        };
        let old_value = node.properties.insert(property.clone(), value.clone());
        let labels = node.labels.clone();
        self.nodes.rebalance_key(&id);
        // Shadow dirty tracking: a property write dirties the primary table.
        self.mark_columnar_node_dirty(&labels);
        for label_id in labels {
            if let Some(old_value) = &old_value {
                let key = (label_id, property.clone(), old_value.clone());
                if let Some(ids) = self.property_index.get_mut(&key) {
                    ids.remove(&id);
                    if ids.is_empty() {
                        self.property_index.remove(&key);
                    }
                }
            }
            if catalog.has_scalar_property_index(label_id, &property) {
                self.property_index
                    .entry_or_default((label_id, property.clone(), value.clone()))
                    .insert(id);
            }
        }
        if let Some(node) = self.nodes.get(&id).cloned() {
            self.add_node_to_composite_property_indexes(catalog, &node);
            self.add_node_to_full_text_property_indexes(catalog, &node);
        }
    }

    pub(super) fn apply_set_relationship_property(
        &mut self,
        id: RelId,
        property: String,
        value: Value,
    ) {
        let Some(relationship) = self.relationships.get_mut(&id) else {
            return;
        };
        let rel_type = relationship.rel_type;
        let old_value = relationship
            .properties
            .insert(property.clone(), value.clone());
        self.relationships.rebalance_key(&id);
        // Shadow dirty tracking: a property write dirties the type's table.
        self.mark_columnar_relationship_dirty(rel_type);
        if let Some(old_value) = old_value {
            let key = (rel_type, property.clone(), old_value);
            if let Some(ids) = self.relationship_property_index.get_mut(&key) {
                ids.remove(&id);
                if ids.is_empty() {
                    self.relationship_property_index.remove(&key);
                }
            }
        }
        self.relationship_property_index
            .entry_or_default((rel_type, property, value))
            .insert(id);
    }

    pub(super) fn apply_delete_relationship(&mut self, id: RelId) {
        let Some(relationship) = self.relationships.remove(&id) else {
            return;
        };
        // Shadow dirty tracking: the deleted relationship's type table.
        self.mark_columnar_relationship_dirty(relationship.rel_type);
        self.remove_relationship_from_basic_statistics(&relationship);
        self.remove_relationship_from_property_index(&relationship);
        self.remove_relationship_from_adjacency(&relationship);
    }

    fn remove_relationship_from_adjacency(&mut self, relationship: &RelRecord) {
        let outgoing_key = (relationship.source, relationship.rel_type);
        if let Some(ids) = self.outgoing.get_mut(&outgoing_key) {
            ids.remove(&OrderedAdjacencyEntry {
                neighbor_id: relationship.target,
                relationship_id: relationship.id,
            });
            if ids.is_empty() {
                self.outgoing.remove(&outgoing_key);
            }
        }
        let incoming_key = (relationship.target, relationship.rel_type);
        if let Some(ids) = self.incoming.get_mut(&incoming_key) {
            ids.remove(&OrderedAdjacencyEntry {
                neighbor_id: relationship.source,
                relationship_id: relationship.id,
            });
            if ids.is_empty() {
                self.incoming.remove(&incoming_key);
            }
        }
    }

    fn apply_delete_node(&mut self, catalog: &Catalog, id: NodeId) {
        let Some(node) = self.nodes.remove(&id) else {
            return;
        };
        // Shadow dirty tracking: the deleted node's primary table.
        self.mark_columnar_node_dirty(&node.labels);
        self.remove_node_from_basic_statistics(&node);
        self.remove_node_from_composite_property_indexes(catalog, &node);
        self.remove_node_from_full_text_property_indexes(catalog, &node);
        for label_id in node.labels {
            for (property, value) in &node.properties {
                let key = (label_id, property.clone(), value.clone());
                if let Some(ids) = self.property_index.get_mut(&key) {
                    ids.remove(&id);
                    if ids.is_empty() {
                        self.property_index.remove(&key);
                    }
                }
            }
        }
    }

    fn materialize_node_for_write(&mut self, id: NodeId) -> Result<bool> {
        if self.node_tombstones.contains(&id) {
            return Ok(false);
        }
        if self.nodes.contains_key(&id) {
            return Ok(true);
        }
        let Some(node) = self
            .canonical_base
            .as_ref()
            .map(|reader| reader.get_node(id).map_err(canonical_segment_error))
            .transpose()?
            .flatten()
        else {
            return Ok(false);
        };
        self.nodes.insert(id, node);
        Ok(true)
    }

    fn materialize_relationship_for_write(&mut self, id: RelId) -> Result<bool> {
        if self.relationship_tombstones.contains(&id) {
            return Ok(false);
        }
        if self.relationships.contains_key(&id) {
            return Ok(true);
        }
        let Some(relationship) = self
            .canonical_base
            .as_ref()
            .map(|reader| reader.get_relationship(id).map_err(canonical_segment_error))
            .transpose()?
            .flatten()
        else {
            return Ok(false);
        };
        self.relationships.insert(id, relationship);
        Ok(true)
    }

    fn record_node_index_sample_updates(&mut self, affected_indexes: Vec<IndexId>) {
        for index_id in affected_indexes {
            if let Some(sample) = self.checkpoint_statistics.index_samples.get_mut(&index_id) {
                sample.updates_since_sample = sample.updates_since_sample.saturating_add(1);
            }
        }
    }

    pub(super) fn apply_wal_op(&mut self, catalog: &mut Catalog, op: WalOp) -> Result<()> {
        let result = self.apply_wal_op_inner(catalog, op);
        if result.is_err() && self.durable.is_some() {
            self.post_wal_apply_poisoned = true;
        }
        result
    }

    fn apply_wal_op_inner(&mut self, catalog: &mut Catalog, op: WalOp) -> Result<()> {
        wal_apply_failpoint()?;
        match op {
            WalOp::CreateNodeLabel { label } => {
                catalog.get_or_create_label(&label);
            }
            WalOp::CreateRelationshipType { rel_type } => {
                catalog.get_or_create_rel_type(&rel_type);
            }
            WalOp::CreateNodeTable { name } => {
                catalog.get_or_create_label(&name);
                catalog.get_or_create_table(TableKind::Node, &name);
            }
            WalOp::CreateRelationshipTable { name } => {
                catalog.get_or_create_rel_type(&name);
                catalog.get_or_create_table(TableKind::Relationship, &name);
            }
            WalOp::CreateProperty {
                table_kind,
                table,
                property,
                value_type,
                nullable,
            } => {
                let table_id = ensure_table_descriptor(catalog, table_kind, &table);
                catalog.get_or_create_property(table_id, &property, value_type, nullable);
            }
            WalOp::AlterTableState {
                table_kind,
                table,
                state,
            } => {
                if let Some(id) = catalog.table_id(table_kind, &table) {
                    catalog.set_table_state(id, state);
                }
            }
            WalOp::AlterPropertyState {
                table_kind,
                table,
                property,
                state,
            } => {
                if let Some(table_id) = catalog.table_id(table_kind, &table)
                    && let Some(id) = catalog.property_descriptor_id(table_id, &property)
                {
                    catalog.set_property_state(id, state);
                }
            }
            WalOp::GcPropertyDescriptor {
                table_kind,
                table,
                property,
            } => {
                if let Some(table_id) = catalog.table_id(table_kind, &table)
                    && let Some(id) = catalog.property_descriptor_id(table_id, &property)
                {
                    catalog.remove_property_descriptor(id);
                }
            }
            WalOp::GcTableDescriptor { table_kind, table } => {
                if let Some(id) = catalog.table_id(table_kind, &table) {
                    catalog.remove_table_descriptor(id);
                }
            }
            WalOp::CreateIndex { label, property } => {
                let label_id = catalog.get_or_create_label(&label);
                catalog.get_or_create_property_index(label_id, &property);
                // Replay order decides whether anything is here to backfill:
                // an index declared before its nodes finds none, and one
                // declared after them finds exactly the nodes that were
                // written while the property was unindexed.
                self.backfill_property_index(catalog, label_id, &property);
            }
            WalOp::CreateCompositeIndex { label, properties } => {
                let label_id = catalog.get_or_create_label(&label);
                let index_id =
                    catalog.get_or_create_composite_property_index(label_id, &properties);
                self.rebuild_composite_property_index_for_descriptor(
                    index_id,
                    label_id,
                    &properties,
                );
            }
            WalOp::CreateRangeIndex { label, property } => {
                let label_id = catalog.get_or_create_label(&label);
                catalog.get_or_create_property_index_with_kind(
                    label_id,
                    &property,
                    IndexKind::Range,
                );
                self.backfill_property_index(catalog, label_id, &property);
            }
            WalOp::CreateFullTextIndex { label, property } => {
                let label_id = catalog.get_or_create_label(&label);
                catalog.get_or_create_property_index_with_kind(
                    label_id,
                    &property,
                    IndexKind::FullText,
                );
                self.rebuild_full_text_property_index_for_descriptor(label_id, &property);
            }
            WalOp::CreateUniqueConstraint { label, property } => {
                let label_id = catalog.get_or_create_label(&label);
                catalog.get_or_create_unique_constraint(label_id, &property);
            }
            WalOp::CreateNodePropertyExistsConstraint { label, property } => {
                let label_id = catalog.get_or_create_label(&label);
                catalog.get_or_create_node_property_exists_constraint(label_id, &property);
            }
            WalOp::CreateRelationshipUniqueConstraint { rel_type, property } => {
                let rel_type_id = catalog.get_or_create_rel_type(&rel_type);
                catalog.get_or_create_relationship_unique_constraint(rel_type_id, &property);
            }
            WalOp::CreateRelationshipPropertyExistsConstraint { rel_type, property } => {
                let rel_type_id = catalog.get_or_create_rel_type(&rel_type);
                catalog
                    .get_or_create_relationship_property_exists_constraint(rel_type_id, &property);
            }
            WalOp::CreateNode {
                id,
                label,
                properties,
            } => {
                let label_id = catalog.get_or_create_label(&label);
                let affected_indexes = node_create_index_sample_updates(
                    catalog,
                    self.nodes.get(&id),
                    label_id,
                    &properties,
                );
                self.apply_create_node(catalog, id, label_id, properties);
                self.record_node_index_sample_updates(affected_indexes);
                self.advanced_statistics_dirty.mark_node_topology();
            }
            WalOp::CreateRelationship {
                id,
                source,
                target,
                rel_type,
                properties,
            } => {
                let rel_type_id = catalog.get_or_create_rel_type(&rel_type);
                self.apply_create_relationship(id, source, target, rel_type_id, properties);
                self.advanced_statistics_dirty.mark_relationship_topology();
            }
            WalOp::SetNodeProperty {
                id,
                property,
                value,
            } => {
                self.materialize_node_for_write(id)?;
                let affected_indexes = self.nodes.get(&id).map_or_else(Vec::new, |node| {
                    node_property_index_sample_updates(catalog, node, &property, &value)
                });
                self.apply_set_node_property(catalog, id, property, value);
                self.record_node_index_sample_updates(affected_indexes);
                self.advanced_statistics_dirty.mark_node_properties();
            }
            WalOp::SetRelationshipProperty {
                id,
                property,
                value,
            } => {
                self.materialize_relationship_for_write(id)?;
                self.apply_set_relationship_property(id, property, value);
                self.advanced_statistics_dirty
                    .mark_relationship_properties();
            }
            WalOp::DeleteNode { id } => {
                let base_exists = self
                    .canonical_base
                    .as_ref()
                    .map(|reader| reader.get_node(id).map_err(canonical_segment_error))
                    .transpose()?
                    .flatten()
                    .is_some();
                self.materialize_node_for_write(id)?;
                let affected_indexes = self.nodes.get(&id).map_or_else(Vec::new, |node| {
                    node_delete_index_sample_updates(catalog, node)
                });
                self.apply_delete_node(catalog, id);
                self.record_node_index_sample_updates(affected_indexes);
                if base_exists {
                    self.node_tombstones.insert(id);
                }
                self.advanced_statistics_dirty.mark_node_topology();
            }
            WalOp::DeleteRelationship { id } => {
                let base_exists = self
                    .canonical_base
                    .as_ref()
                    .map(|reader| reader.get_relationship(id).map_err(canonical_segment_error))
                    .transpose()?
                    .flatten()
                    .is_some();
                self.materialize_relationship_for_write(id)?;
                self.apply_delete_relationship(id);
                if base_exists {
                    self.relationship_tombstones.insert(id);
                }
                self.advanced_statistics_dirty.mark_relationship_topology();
            }
            WalOp::ProjectGraph {
                name,
                node_labels,
                rel_types,
            } => {
                self.apply_project_graph_definition(
                    name,
                    ProjectedGraphDefinition {
                        node_labels,
                        rel_types,
                    },
                );
            }
            WalOp::MarkInitialImportSource { source_fingerprint } => {
                self.initial_import_source_fingerprint = Some(source_fingerprint);
            }
            WalOp::Relational { record } => {
                let batch = decode_relational_wal_batch(&record, RelationalDecodeLimits::wal())
                    .map_err(|error| SkeinError::Storage(error.to_string()))?;
                let expected_epoch = self.commit_epoch.saturating_add(1);
                if batch.epoch != expected_epoch {
                    return Err(SkeinError::Storage(format!(
                        "relational WAL epoch mismatch: expected {expected_epoch}, got {}",
                        batch.epoch
                    )));
                }
                if self.uses_sparse_read_only_relational_recovery() {
                    if batch.transaction.changes_schema() {
                        return Err(SkeinError::Storage(
                            "read-only sparse relational recovery rejects schema-changing WAL until a new canonical checkpoint is published"
                                .to_string(),
                        ));
                    }
                    if batch.replay_access.is_none() {
                        return Err(SkeinError::Storage(
                            "authoritative relational WAL is missing its exact replay access set"
                                .to_string(),
                        ));
                    }
                } else {
                    self.stage_recovered_relational_transaction(
                        batch.transaction,
                        batch.replay_access,
                        expected_epoch,
                    )
                    .map_err(|error| SkeinError::Storage(error.to_string()))?;
                }
            }
            WalOp::RelationalSnapshot { record } => {
                if self.relational_state.canonical_row_metadata_only() {
                    return Err(SkeinError::Storage(
                        "metadata-only relational recovery rejects snapshot WAL until a new canonical checkpoint is published"
                            .to_string(),
                    ));
                }
                let index_load = self.relational_checkpoint_index_load();
                let checkpoint = decode_relational_checkpoint_with_index_load(
                    &record,
                    RelationalDecodeLimits::checkpoint(),
                    index_load,
                )
                .map_err(|error| SkeinError::Storage(error.to_string()))?;
                let expected_epoch = self.commit_epoch.saturating_add(1);
                if checkpoint.epoch != expected_epoch {
                    return Err(SkeinError::Storage(format!(
                        "relational snapshot WAL epoch mismatch: expected {expected_epoch}, got {}",
                        checkpoint.epoch
                    )));
                }
                self.invalidate_relational_index_recovery(
                    expected_epoch,
                    "relational snapshot WAL replaces the complete index schema and rows",
                );
                self.invalidate_relational_row_page_recovery(
                    expected_epoch,
                    "relational snapshot WAL replaces the complete row schema and data",
                );
                self.relational_state = checkpoint.state;
            }
            WalOp::Append { record } => {
                let batch = decode_append_wal_batch(&record, AppendDecodeLimits::wal())
                    .map_err(|error| SkeinError::Storage(error.to_string()))?;
                let expected_epoch = self.commit_epoch.saturating_add(1);
                if batch.epoch != expected_epoch {
                    return Err(SkeinError::Storage(format!(
                        "append WAL epoch mismatch: expected {expected_epoch}, got {}",
                        batch.epoch
                    )));
                }
                self.append_state = self
                    .append_state
                    .stage_transaction(&batch.transaction, self.append_mutation_limits)
                    .map_err(|error| SkeinError::Storage(error.to_string()))?;
            }
            WalOp::Batch(ops) => {
                for op in ops {
                    self.apply_wal_op(catalog, op)?;
                }
            }
        }
        Ok(())
    }
}

fn node_create_index_sample_updates(
    catalog: &Catalog,
    old_node: Option<&NodeRecord>,
    new_label_id: LabelId,
    new_properties: &BTreeMap<String, Value>,
) -> Vec<IndexId> {
    let mut affected = Vec::new();
    for index in catalog
        .property_indexes()
        .filter(|index| index.kind != IndexKind::FullText)
    {
        let old_value = old_node
            .filter(|node| node.labels.contains(&index.label_id))
            .and_then(|node| node.properties.get(&index.property));
        let new_value = (index.label_id == new_label_id)
            .then(|| new_properties.get(&index.property))
            .flatten();
        if old_value != new_value {
            affected.push(index.id);
        }
    }
    for index in catalog.composite_property_indexes() {
        if !composite_create_values_equal(old_node, new_label_id, new_properties, index) {
            affected.push(index.id);
        }
    }
    affected
}

fn node_property_index_sample_updates(
    catalog: &Catalog,
    node: &NodeRecord,
    property: &str,
    value: &Value,
) -> Vec<IndexId> {
    let mut affected = catalog
        .property_indexes()
        .filter(|index| {
            index.kind != IndexKind::FullText
                && index.property == property
                && node.labels.contains(&index.label_id)
                && node.properties.get(property) != Some(value)
        })
        .map(|index| index.id)
        .collect::<Vec<_>>();
    affected.extend(
        catalog
            .composite_property_indexes()
            .filter(|index| {
                node.labels.contains(&index.label_id)
                    && index
                        .properties
                        .iter()
                        .any(|candidate| candidate == property)
                    && composite_property_update_changes_key(node, property, value, index)
            })
            .map(|index| index.id),
    );
    affected
}

fn node_delete_index_sample_updates(catalog: &Catalog, node: &NodeRecord) -> Vec<IndexId> {
    let mut affected = catalog
        .property_indexes()
        .filter(|index| {
            index.kind != IndexKind::FullText
                && node.labels.contains(&index.label_id)
                && node.properties.contains_key(&index.property)
        })
        .map(|index| index.id)
        .collect::<Vec<_>>();
    affected.extend(
        catalog
            .composite_property_indexes()
            .filter(|index| {
                node.labels.contains(&index.label_id)
                    && index
                        .properties
                        .iter()
                        .all(|property| node.properties.contains_key(property))
            })
            .map(|index| index.id),
    );
    affected
}

fn composite_create_values_equal(
    old_node: Option<&NodeRecord>,
    new_label_id: LabelId,
    new_properties: &BTreeMap<String, Value>,
    index: &CompositeIndexDescriptor,
) -> bool {
    let old_indexed = old_node.is_some_and(|node| {
        node.labels.contains(&index.label_id)
            && index
                .properties
                .iter()
                .all(|property| node.properties.contains_key(property))
    });
    let new_indexed = new_label_id == index.label_id
        && index
            .properties
            .iter()
            .all(|property| new_properties.contains_key(property));
    match (old_indexed, new_indexed) {
        (false, false) => true,
        (true, true) => index.properties.iter().all(|property| {
            old_node.and_then(|node| node.properties.get(property)) == new_properties.get(property)
        }),
        _ => false,
    }
}

fn composite_property_update_changes_key(
    node: &NodeRecord,
    property: &str,
    value: &Value,
    index: &CompositeIndexDescriptor,
) -> bool {
    let old_indexed = index
        .properties
        .iter()
        .all(|candidate| node.properties.contains_key(candidate));
    let new_indexed = index
        .properties
        .iter()
        .all(|candidate| candidate == property || node.properties.contains_key(candidate));
    match (old_indexed, new_indexed) {
        (false, false) => false,
        (true, true) => node.properties.get(property) != Some(value),
        _ => true,
    }
}
