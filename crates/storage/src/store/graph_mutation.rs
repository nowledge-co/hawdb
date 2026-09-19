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

//! Public mutation API, WAL-op builders, merge matching, and search-projection change recording for [`GraphStore`].

use super::*;

fn wal_ops_contain_relational_transaction(ops: &[WalOp]) -> bool {
    ops.iter().any(|op| match op {
        WalOp::Relational { .. } => true,
        WalOp::Batch(ops) => wal_ops_contain_relational_transaction(ops),
        _ => false,
    })
}

fn wal_ops_contain_relational_snapshot(ops: &[WalOp]) -> bool {
    ops.iter().any(|op| match op {
        WalOp::RelationalSnapshot { .. } => true,
        WalOp::Batch(ops) => wal_ops_contain_relational_snapshot(ops),
        _ => false,
    })
}

impl GraphStore {
    pub fn create_node(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        properties: BTreeMap<String, Value>,
    ) -> Result<NodeId> {
        let label_id = catalog.get_or_create_label(label);
        let id = NodeId(self.next_node_id);
        let ops = [WalOp::CreateNode {
            id,
            label: label.to_string(),
            properties: properties.clone(),
        }];
        self.validate_constraints_for_ops(catalog, &ops)?;
        self.append_durable_wal_single(ops[0].clone())?;
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        self.apply_create_node(catalog, id, label_id, properties);
        self.finish_non_relational_commit();
        Ok(id)
    }

    pub fn merge_node(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        match_properties: BTreeMap<String, Value>,
        on_create_properties: BTreeMap<String, Value>,
        on_match_assignments: &[NodeSetAssignment],
        post_merge_assignments: &[NodeSetAssignment],
    ) -> Result<(NodeId, bool)> {
        let label_id = catalog.get_or_create_label(label);
        if let Some(id) = self.find_node_by_label_and_properties(label_id, &match_properties)? {
            if !on_match_assignments.is_empty() || !post_merge_assignments.is_empty() {
                let mut assignments =
                    Vec::with_capacity(on_match_assignments.len() + post_merge_assignments.len());
                assignments.extend_from_slice(on_match_assignments);
                assignments.extend_from_slice(post_merge_assignments);
                let ops = self.node_set_property_ops(&[id], &assignments)?;
                self.validate_constraints_for_ops(catalog, &ops)?;
                self.append_durable_wal_batch(&ops)?;
                self.record_search_projection_graph_changes_for_ops(
                    catalog,
                    self.commit_epoch + 1,
                    &ops,
                );
                for op in ops {
                    self.apply_wal_op(catalog, op)?;
                }
                self.finish_non_relational_commit();
            }
            return Ok((id, false));
        }
        let mut properties = match_properties;
        for (property, value) in on_create_properties {
            properties.insert(property, value);
        }
        apply_node_assignments_to_properties(&mut properties, post_merge_assignments)?;
        let id = NodeId(self.next_node_id);
        let ops = [WalOp::CreateNode {
            id,
            label: label.to_string(),
            properties: properties.clone(),
        }];
        self.validate_constraints_for_ops(catalog, &ops)?;
        self.append_durable_wal_single(ops[0].clone())?;
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        self.apply_create_node(catalog, id, label_id, properties);
        self.finish_non_relational_commit();
        Ok((id, true))
    }

    pub fn create_relationship(
        &mut self,
        catalog: &mut Catalog,
        source: NodeId,
        target: NodeId,
        rel_type: &str,
        properties: BTreeMap<String, Value>,
    ) -> Result<RelId> {
        if self.node_owned(source)?.is_none() {
            return Err(HawDBError::Storage(format!(
                "source node {} does not exist",
                source.0
            )));
        }
        if self.node_owned(target)?.is_none() {
            return Err(HawDBError::Storage(format!(
                "target node {} does not exist",
                target.0
            )));
        }
        let rel_type_id = catalog.get_or_create_rel_type(rel_type);
        let id = RelId(self.next_rel_id);
        let ops = [WalOp::CreateRelationship {
            id,
            source,
            target,
            rel_type: rel_type.to_string(),
            properties: properties.clone(),
        }];
        self.validate_constraints_for_ops(catalog, &ops)?;
        self.append_durable_wal_single(ops[0].clone())?;
        self.apply_create_relationship(id, source, target, rel_type_id, properties);
        self.finish_non_relational_commit();
        Ok(id)
    }

    pub fn create_relationships_between_matches(
        &mut self,
        catalog: &mut Catalog,
        request: MatchedRelationshipCreate,
    ) -> Result<Vec<(NodeId, RelId, NodeId)>> {
        let source_label_id = if request.source_label.is_empty() {
            None
        } else {
            let Some(label_id) = catalog.label_id(&request.source_label) else {
                return Ok(Vec::new());
            };
            Some(label_id)
        };
        let target_label_id = if request.target_label.is_empty() {
            None
        } else {
            let Some(label_id) = catalog.label_id(&request.target_label) else {
                return Ok(Vec::new());
            };
            Some(label_id)
        };
        let sources =
            self.matching_node_ids(catalog, source_label_id, request.source_filter.as_ref())?;
        let targets =
            self.matching_node_ids(catalog, target_label_id, request.target_filter.as_ref())?;
        if sources.is_empty() || targets.is_empty() {
            return Ok(Vec::new());
        }

        catalog.get_or_create_rel_type(&request.rel_type);
        let mut next_rel_id = self.next_rel_id;
        let mut rows = Vec::with_capacity(sources.len() * targets.len());
        let mut ops = Vec::with_capacity(sources.len() * targets.len());
        for source in sources {
            for target in &targets {
                let rel = RelId(next_rel_id);
                next_rel_id += 1;
                ops.push(WalOp::CreateRelationship {
                    id: rel,
                    source,
                    target: *target,
                    rel_type: request.rel_type.clone(),
                    properties: request.rel_properties.clone(),
                });
                rows.push((source, rel, *target));
            }
        }
        self.validate_constraints_for_ops(catalog, &ops)?;
        self.append_durable_wal_batch(&ops)?;
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        for op in ops {
            self.apply_wal_op(catalog, op)?;
        }
        self.finish_non_relational_commit();
        Ok(rows)
    }

    pub fn merge_relationships_between_matches(
        &mut self,
        catalog: &mut Catalog,
        request: MatchedRelationshipMerge,
    ) -> Result<Vec<(NodeId, RelId, NodeId, bool)>> {
        let source_label_id = if request.source_label.is_empty() {
            None
        } else {
            let Some(label_id) = catalog.label_id(&request.source_label) else {
                return Ok(Vec::new());
            };
            Some(label_id)
        };
        let target_label_id = if request.target_label.is_empty() {
            None
        } else {
            let Some(label_id) = catalog.label_id(&request.target_label) else {
                return Ok(Vec::new());
            };
            Some(label_id)
        };
        let sources =
            self.matching_node_ids(catalog, source_label_id, request.source_filter.as_ref())?;
        let targets =
            self.matching_node_ids(catalog, target_label_id, request.target_filter.as_ref())?;
        if sources.is_empty() || targets.is_empty() {
            return Ok(Vec::new());
        }

        let rel_type_id = catalog.get_or_create_rel_type(&request.rel_type);
        let mut next_rel_id = self.next_rel_id;
        let mut rows = Vec::with_capacity(sources.len() * targets.len());
        let mut ops = Vec::new();
        for source in sources {
            for target in &targets {
                if let Some(rel) = self.find_relationship_by_property_subset(
                    source,
                    *target,
                    rel_type_id,
                    &request.rel_match_properties,
                )? {
                    rows.push((source, rel, *target, false));
                    continue;
                }
                let rel = RelId(next_rel_id);
                next_rel_id += 1;
                let mut properties = request.rel_match_properties.clone();
                for (property, value) in &request.on_create_properties {
                    properties.insert(property.clone(), value.clone());
                }
                ops.push(WalOp::CreateRelationship {
                    id: rel,
                    source,
                    target: *target,
                    rel_type: request.rel_type.clone(),
                    properties,
                });
                rows.push((source, rel, *target, true));
            }
        }
        if !ops.is_empty() {
            self.validate_constraints_for_ops(catalog, &ops)?;
            self.append_durable_wal_batch(&ops)?;
            self.record_search_projection_graph_changes_for_ops(
                catalog,
                self.commit_epoch + 1,
                &ops,
            );
            for op in ops {
                self.apply_wal_op(catalog, op)?;
            }
            self.finish_non_relational_commit();
        }
        Ok(rows)
    }

    pub fn merge_relationships_from_matched_relationships(
        &mut self,
        catalog: &mut Catalog,
        request: MatchedRelationshipCopyMerge,
    ) -> Result<Vec<(NodeId, RelId, NodeId, bool)>> {
        let source_label_id = if request.source_label.is_empty() {
            None
        } else {
            let Some(label_id) = catalog.label_id(&request.source_label) else {
                return Ok(Vec::new());
            };
            Some(label_id)
        };
        let target_label_id = if request.target_label.is_empty() {
            None
        } else {
            let Some(label_id) = catalog.label_id(&request.target_label) else {
                return Ok(Vec::new());
            };
            Some(label_id)
        };
        let Some(old_rel_type_id) = catalog.rel_type_id(&request.old_rel_type) else {
            return Ok(Vec::new());
        };
        let new_rel_type_id = catalog.get_or_create_rel_type(&request.new_rel_type);
        let old_relationships = self
            .scan_relationships(Some(old_rel_type_id))
            .filter(|relationship| {
                properties_contain_all(&relationship.properties, &request.old_rel_filter)
                    && self
                        .nodes
                        .get(&relationship.source)
                        .map(|node| {
                            source_label_id
                                .map(|label_id| node.labels.contains(&label_id))
                                .unwrap_or(true)
                                && request
                                    .source_filter
                                    .as_ref()
                                    .map(|filter| {
                                        property_filter_matches(filter, node.id.0, &node.properties)
                                    })
                                    .unwrap_or(true)
                        })
                        .unwrap_or(false)
                    && self
                        .nodes
                        .get(&relationship.target)
                        .map(|node| {
                            target_label_id
                                .map(|label_id| node.labels.contains(&label_id))
                                .unwrap_or(true)
                                && request
                                    .target_filter
                                    .as_ref()
                                    .map(|filter| {
                                        property_filter_matches(filter, node.id.0, &node.properties)
                                    })
                                    .unwrap_or(true)
                        })
                        .unwrap_or(false)
            })
            .cloned()
            .collect::<Vec<_>>();

        let mut next_rel_id = self.next_rel_id;
        let mut rows = Vec::with_capacity(old_relationships.len());
        let mut ops = Vec::new();
        for old_relationship in old_relationships {
            if let Some(rel) = self.find_relationship_by_property_subset(
                old_relationship.source,
                old_relationship.target,
                new_rel_type_id,
                &request.new_rel_match_properties,
            )? {
                rows.push((old_relationship.source, rel, old_relationship.target, false));
                continue;
            }
            let rel = RelId(next_rel_id);
            next_rel_id += 1;
            let mut properties = request.new_rel_match_properties.clone();
            for (property, value) in &request.on_create_properties {
                let value = match value {
                    RelationshipOnCreatePropertyValue::Value(value) => value.clone(),
                    RelationshipOnCreatePropertyValue::MatchedRelationshipProperty { property } => {
                        old_relationship
                            .properties
                            .get(property)
                            .cloned()
                            .unwrap_or(Value::Null)
                    }
                };
                properties.insert(property.clone(), value);
            }
            ops.push(WalOp::CreateRelationship {
                id: rel,
                source: old_relationship.source,
                target: old_relationship.target,
                rel_type: request.new_rel_type.clone(),
                properties,
            });
            rows.push((old_relationship.source, rel, old_relationship.target, true));
        }
        if !ops.is_empty() {
            self.validate_constraints_for_ops(catalog, &ops)?;
            self.append_durable_wal_batch(&ops)?;
            self.record_search_projection_graph_changes_for_ops(
                catalog,
                self.commit_epoch + 1,
                &ops,
            );
            for op in ops {
                self.apply_wal_op(catalog, op)?;
            }
            self.finish_non_relational_commit();
        }
        Ok(rows)
    }

    pub fn merge_relationships_to_matched_target(
        &mut self,
        catalog: &mut Catalog,
        request: MatchedRelationshipRetargetMerge,
    ) -> Result<Vec<(NodeId, RelId, NodeId, bool)>> {
        let source_label_id = optional_label_id(catalog, &request.source_label);
        if !request.source_label.is_empty() && source_label_id.is_none() {
            return Ok(Vec::new());
        }
        let Some(old_target_label_id) = catalog.label_id(&request.old_target_label) else {
            return Ok(Vec::new());
        };
        let Some(new_target_label_id) = catalog.label_id(&request.new_target_label) else {
            return Ok(Vec::new());
        };
        let Some(old_rel_type_id) = catalog.rel_type_id(&request.old_rel_type) else {
            return Ok(Vec::new());
        };
        let new_rel_type_id = catalog.get_or_create_rel_type(&request.new_rel_type);
        let source_ids = self
            .scan_relationships(Some(old_rel_type_id))
            .filter(|relationship| {
                properties_contain_all(&relationship.properties, &request.old_rel_filter)
                    && self
                        .nodes
                        .get(&relationship.source)
                        .map(|node| {
                            source_label_id
                                .map(|label_id| node.labels.contains(&label_id))
                                .unwrap_or(true)
                                && request
                                    .source_filter
                                    .as_ref()
                                    .map(|filter| {
                                        property_filter_matches(filter, node.id.0, &node.properties)
                                    })
                                    .unwrap_or(true)
                        })
                        .unwrap_or(false)
                    && self
                        .nodes
                        .get(&relationship.target)
                        .map(|node| {
                            node.labels.contains(&old_target_label_id)
                                && request
                                    .old_target_filter
                                    .as_ref()
                                    .map(|filter| {
                                        property_filter_matches(filter, node.id.0, &node.properties)
                                    })
                                    .unwrap_or(true)
                        })
                        .unwrap_or(false)
            })
            .map(|relationship| relationship.source)
            .collect::<BTreeSet<_>>();
        if source_ids.is_empty() {
            return Ok(Vec::new());
        }
        let target_ids = self.matching_node_ids(
            catalog,
            Some(new_target_label_id),
            request.new_target_filter.as_ref(),
        )?;
        let mut next_rel_id = self.next_rel_id;
        let mut rows = Vec::new();
        let mut ops = Vec::new();
        for source in source_ids {
            for target in &target_ids {
                if let Some(rel) = self.find_relationship_by_property_subset(
                    source,
                    *target,
                    new_rel_type_id,
                    &request.new_rel_match_properties,
                )? {
                    rows.push((source, rel, *target, false));
                    continue;
                }
                let rel = RelId(next_rel_id);
                next_rel_id += 1;
                let mut properties = request.new_rel_match_properties.clone();
                properties.extend(request.on_create_properties.clone());
                ops.push(WalOp::CreateRelationship {
                    id: rel,
                    source,
                    target: *target,
                    rel_type: request.new_rel_type.clone(),
                    properties,
                });
                rows.push((source, rel, *target, true));
            }
        }
        if !ops.is_empty() {
            self.validate_constraints_for_ops(catalog, &ops)?;
            self.append_durable_wal_batch(&ops)?;
            self.record_search_projection_graph_changes_for_ops(
                catalog,
                self.commit_epoch + 1,
                &ops,
            );
            for op in ops {
                self.apply_wal_op(catalog, op)?;
            }
            self.finish_non_relational_commit();
        }
        Ok(rows)
    }

    pub fn merge_relationships_from_matched_target(
        &mut self,
        catalog: &mut Catalog,
        request: MatchedRelationshipSourceRetargetMerge,
    ) -> Result<Vec<(NodeId, RelId, NodeId, bool)>> {
        let old_source_label_id = optional_label_id(catalog, &request.old_source_label);
        if !request.old_source_label.is_empty() && old_source_label_id.is_none() {
            return Ok(Vec::new());
        }
        let Some(old_target_label_id) = catalog.label_id(&request.old_target_label) else {
            return Ok(Vec::new());
        };
        let new_source_label_id = optional_label_id(catalog, &request.new_source_label);
        if !request.new_source_label.is_empty() && new_source_label_id.is_none() {
            return Ok(Vec::new());
        }
        let Some(old_rel_type_id) = catalog.rel_type_id(&request.old_rel_type) else {
            return Ok(Vec::new());
        };
        let new_rel_type_id = catalog.get_or_create_rel_type(&request.new_rel_type);
        let target_ids = self
            .scan_relationships(Some(old_rel_type_id))
            .filter(|relationship| {
                properties_contain_all(&relationship.properties, &request.old_rel_filter)
                    && self
                        .nodes
                        .get(&relationship.source)
                        .map(|node| {
                            old_source_label_id
                                .map(|label_id| node.labels.contains(&label_id))
                                .unwrap_or(true)
                                && request
                                    .old_source_filter
                                    .as_ref()
                                    .map(|filter| {
                                        property_filter_matches(filter, node.id.0, &node.properties)
                                    })
                                    .unwrap_or(true)
                        })
                        .unwrap_or(false)
                    && self
                        .nodes
                        .get(&relationship.target)
                        .map(|node| {
                            node.labels.contains(&old_target_label_id)
                                && request
                                    .old_target_filter
                                    .as_ref()
                                    .map(|filter| {
                                        property_filter_matches(filter, node.id.0, &node.properties)
                                    })
                                    .unwrap_or(true)
                        })
                        .unwrap_or(false)
            })
            .map(|relationship| relationship.target)
            .collect::<BTreeSet<_>>();
        if target_ids.is_empty() {
            return Ok(Vec::new());
        }
        let source_ids = self.matching_node_ids(
            catalog,
            new_source_label_id,
            request.new_source_filter.as_ref(),
        )?;
        let mut next_rel_id = self.next_rel_id;
        let mut rows = Vec::new();
        let mut ops = Vec::new();
        for source in source_ids {
            for target in &target_ids {
                if let Some(rel) = self.find_relationship_by_property_subset(
                    source,
                    *target,
                    new_rel_type_id,
                    &request.new_rel_match_properties,
                )? {
                    rows.push((source, rel, *target, false));
                    continue;
                }
                let rel = RelId(next_rel_id);
                next_rel_id += 1;
                let mut properties = request.new_rel_match_properties.clone();
                properties.extend(request.on_create_properties.clone());
                ops.push(WalOp::CreateRelationship {
                    id: rel,
                    source,
                    target: *target,
                    rel_type: request.new_rel_type.clone(),
                    properties,
                });
                rows.push((source, rel, *target, true));
            }
        }
        if !ops.is_empty() {
            self.validate_constraints_for_ops(catalog, &ops)?;
            self.append_durable_wal_batch(&ops)?;
            self.record_search_projection_graph_changes_for_ops(
                catalog,
                self.commit_epoch + 1,
                &ops,
            );
            for op in ops {
                self.apply_wal_op(catalog, op)?;
            }
            self.finish_non_relational_commit();
        }
        Ok(rows)
    }

    pub fn set_node_property(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        filter: Option<&PropertyFilter>,
        property: &str,
        value: Value,
    ) -> Result<Vec<NodeId>> {
        let label_id = if label.is_empty() {
            None
        } else {
            let Some(label_id) = catalog.label_id(label) else {
                return Ok(Vec::new());
            };
            Some(label_id)
        };
        let ids = self.matching_node_ids(catalog, label_id, filter)?;
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let ops = ids
            .iter()
            .map(|id| WalOp::SetNodeProperty {
                id: *id,
                property: property.to_string(),
                value: value.clone(),
            })
            .collect::<Vec<_>>();
        self.validate_constraints_for_ops(catalog, &ops)?;
        self.append_durable_wal_batch(&ops)?;
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        for op in ops {
            self.apply_wal_op(catalog, op)?;
        }
        self.finish_non_relational_commit();
        Ok(ids)
    }

    pub fn add_int_node_property(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        filter: Option<&PropertyFilter>,
        property: &str,
        amount: i64,
    ) -> Result<Vec<NodeId>> {
        let label_id = if label.is_empty() {
            None
        } else {
            let Some(label_id) = catalog.label_id(label) else {
                return Ok(Vec::new());
            };
            Some(label_id)
        };
        let ids = self.matching_node_ids(catalog, label_id, filter)?;
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let ops = ids
            .iter()
            .map(|id| {
                let node = self.node_owned(*id)?.ok_or_else(|| {
                    HawDBError::Storage(format!("node {} disappeared during property update", id.0))
                })?;
                let current = match node.properties.get(property) {
                    None | Some(Value::Null) => 0,
                    Some(Value::Int(value)) => *value,
                    Some(value) => {
                        return Err(HawDBError::Execution(format!(
                            "property increment requires an integer or null value, got {value:?}"
                        )));
                    }
                };
                let value = current.checked_add(amount).ok_or_else(|| {
                    HawDBError::Execution("property increment overflowed i64".to_string())
                })?;
                Ok(WalOp::SetNodeProperty {
                    id: *id,
                    property: property.to_string(),
                    value: Value::Int(value),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        self.validate_constraints_for_ops(catalog, &ops)?;
        self.append_durable_wal_batch(&ops)?;
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        for op in ops {
            if let WalOp::SetNodeProperty {
                id,
                property,
                value,
            } = op
            {
                self.apply_set_node_property(catalog, id, property, value);
            }
        }
        self.finish_non_relational_commit();
        Ok(ids)
    }

    pub fn set_node_properties(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        filter: Option<&PropertyFilter>,
        assignments: &[NodeSetAssignment],
    ) -> Result<Vec<NodeId>> {
        let label_id = if label.is_empty() {
            None
        } else {
            let Some(label_id) = catalog.label_id(label) else {
                return Ok(Vec::new());
            };
            Some(label_id)
        };
        let ids = self.matching_node_ids(catalog, label_id, filter)?;
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let ops = self.node_set_property_ops(&ids, assignments)?;
        self.validate_constraints_for_ops(catalog, &ops)?;
        self.append_durable_wal_batch(&ops)?;
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        for op in ops {
            self.apply_wal_op(catalog, op)?;
        }
        self.finish_non_relational_commit();
        Ok(ids)
    }

    pub fn set_node_properties_by_ids(
        &mut self,
        catalog: &mut Catalog,
        ids: &[NodeId],
        assignments: &[NodeSetAssignment],
    ) -> Result<Vec<NodeId>> {
        self.set_node_properties_by_ids_with_limits(
            catalog,
            ids,
            assignments,
            MutationLimits::default(),
        )
    }

    pub fn set_node_properties_by_ids_with_limits(
        &mut self,
        catalog: &mut Catalog,
        ids: &[NodeId],
        assignments: &[NodeSetAssignment],
        limits: MutationLimits,
    ) -> Result<Vec<NodeId>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let operation_count = ids.len().checked_mul(assignments.len()).ok_or_else(|| {
            HawDBError::Execution("mutation operation count overflow".to_string())
        })?;
        ensure_additional_mutation_limits(0, 0, operation_count, ids.len(), limits)?;
        let ops = self.node_set_property_ops(ids, assignments)?;
        self.validate_constraints_for_ops(catalog, &ops)?;
        self.append_durable_wal_batch(&ops)?;
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        for op in ops {
            self.apply_wal_op(catalog, op)?;
        }
        self.finish_non_relational_commit();
        Ok(ids.to_vec())
    }

    pub(super) fn node_set_property_ops(
        &self,
        ids: &[NodeId],
        assignments: &[NodeSetAssignment],
    ) -> Result<Vec<WalOp>> {
        let mut ops = Vec::with_capacity(ids.len().saturating_mul(assignments.len()));
        for id in ids {
            let node = self.node_owned(*id)?.ok_or_else(|| {
                HawDBError::Storage(format!("node {} disappeared during property update", id.0))
            })?;
            for assignment in assignments {
                let value = evaluate_node_set_value(&node.properties, assignment)?;
                ops.push(WalOp::SetNodeProperty {
                    id: *id,
                    property: assignment.property.clone(),
                    value,
                });
            }
        }
        Ok(ops)
    }

    pub fn delete_nodes(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        filter: Option<&PropertyFilter>,
        detach: bool,
    ) -> Result<Vec<NodeId>> {
        let label_id = if label.is_empty() {
            None
        } else {
            let Some(label_id) = catalog.label_id(label) else {
                return Ok(Vec::new());
            };
            Some(label_id)
        };
        let ids = self.matching_node_ids(catalog, label_id, filter)?;
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let ops = self.delete_node_ops(&ids, detach)?;
        self.ensure_out_of_core_delta_admission(&ops)?;
        self.append_durable_wal_batch(&ops)?;
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        for op in ops {
            self.apply_wal_op(catalog, op)?;
        }
        self.finish_non_relational_commit();
        Ok(ids)
    }

    #[doc(hidden)]
    pub fn delete_node_ids_with_limits(
        &mut self,
        catalog: &mut Catalog,
        ids: &[NodeId],
        detach: bool,
        limits: MutationLimits,
    ) -> Result<Vec<NodeId>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        ensure_additional_mutation_limits(0, 0, 0, ids.len(), limits)?;
        let ops = self.delete_node_ops_bounded(ids, detach, limits.max_operations.get())?;
        self.ensure_out_of_core_delta_admission(&ops)?;
        self.append_durable_wal_batch(&ops)?;
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        for op in ops {
            self.apply_wal_op(catalog, op)?;
        }
        self.finish_non_relational_commit();
        Ok(ids.to_vec())
    }

    pub fn delete_relationships(
        &mut self,
        catalog: &mut Catalog,
        request: RelationshipDeleteRequest,
    ) -> Result<Vec<RelId>> {
        let Some(source_label_id) = catalog.label_id(&request.source_label) else {
            return Ok(Vec::new());
        };
        let Some(target_label_id) = catalog.label_id(&request.target_label) else {
            return Ok(Vec::new());
        };
        let Some(rel_type_id) = catalog.rel_type_id(&request.rel_type) else {
            return Ok(Vec::new());
        };
        let source_ids = self
            .matching_node_ids(catalog, Some(source_label_id), request.filter.as_ref())?
            .into_iter()
            .collect::<BTreeSet<_>>();
        if source_ids.is_empty() {
            return Ok(Vec::new());
        }
        let target_ids = request
            .target_filter
            .as_ref()
            .map(|filter| {
                self.matching_node_ids(catalog, Some(target_label_id), Some(filter))
                    .map(|ids| ids.into_iter().collect::<BTreeSet<_>>())
            })
            .transpose()?;
        if target_ids.as_ref().is_some_and(BTreeSet::is_empty) {
            return Ok(Vec::new());
        }
        let mut ids = Vec::new();
        for relationship in self.relationship_records_owned() {
            let relationship = relationship?;
            if relationship.rel_type != rel_type_id
                || !source_ids.contains(&relationship.source)
                || request.rel_filter.as_ref().is_some_and(|filter| {
                    !property_filter_matches(filter, relationship.id.0, &relationship.properties)
                })
            {
                continue;
            }
            let target_matches = self.node_owned(relationship.target)?.is_some_and(|target| {
                target.labels.contains(&target_label_id)
                    && target_ids
                        .as_ref()
                        .is_none_or(|ids| ids.contains(&relationship.target))
            });
            if target_matches {
                ids.push(relationship.id);
            }
        }
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let ops = ids
            .iter()
            .copied()
            .map(|id| WalOp::DeleteRelationship { id })
            .collect::<Vec<_>>();
        self.ensure_out_of_core_delta_admission(&ops)?;
        self.append_durable_wal_batch(&ops)?;
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        for op in ops {
            self.apply_wal_op(catalog, op)?;
        }
        self.finish_non_relational_commit();
        Ok(ids)
    }

    pub fn delete_relationship_target_nodes(
        &mut self,
        catalog: &mut Catalog,
        request: RelationshipTargetNodeDelete,
    ) -> Result<Vec<NodeId>> {
        let ids = self.relationship_target_node_ids(catalog, &request)?;
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let ops = self.delete_node_ops(&ids, request.detach)?;
        self.ensure_out_of_core_delta_admission(&ops)?;
        self.append_durable_wal_batch(&ops)?;
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        for op in ops {
            self.apply_wal_op(catalog, op)?;
        }
        self.finish_non_relational_commit();
        Ok(ids)
    }

    fn relationship_target_node_ids(
        &self,
        catalog: &Catalog,
        request: &RelationshipTargetNodeDelete,
    ) -> Result<Vec<NodeId>> {
        self.relationship_target_node_ids_bounded(catalog, request, usize::MAX)
    }

    fn relationship_target_node_ids_bounded(
        &self,
        catalog: &Catalog,
        request: &RelationshipTargetNodeDelete,
        max_ids: usize,
    ) -> Result<Vec<NodeId>> {
        let Some(source_label_id) = optional_label_id(catalog, &request.source_label) else {
            return Ok(Vec::new());
        };
        let Some(target_label_id) = optional_label_id(catalog, &request.target_label) else {
            return Ok(Vec::new());
        };
        let Some(rel_type_id) = catalog.rel_type_id(&request.rel_type) else {
            return Ok(Vec::new());
        };
        let source_ids = self
            .matching_node_ids_bounded(
                Some(source_label_id),
                request.source_filter.as_ref(),
                max_ids,
                "max_mutation_affected_rows",
            )?
            .into_iter()
            .collect::<BTreeSet<_>>();
        if source_ids.is_empty() {
            return Ok(Vec::new());
        }
        let target_ids = request
            .target_filter
            .as_ref()
            .map(|filter| {
                self.matching_node_ids_bounded(
                    Some(target_label_id),
                    Some(filter),
                    max_ids,
                    "max_mutation_affected_rows",
                )
                .map(|ids| ids.into_iter().collect::<BTreeSet<_>>())
            })
            .transpose()?;
        if target_ids.as_ref().is_some_and(BTreeSet::is_empty) {
            return Ok(Vec::new());
        }
        let mut ids = BTreeSet::new();
        let mut callback_error = None;
        self.visit_relationships_owned(Some(rel_type_id), |relationship| {
            if !source_ids.contains(&relationship.source)
                || request.rel_filter.as_ref().is_some_and(|filter| {
                    !property_filter_matches(filter, relationship.id.0, &relationship.properties)
                })
                || target_ids
                    .as_ref()
                    .is_some_and(|ids| !ids.contains(&relationship.target))
            {
                return GraphScanControl::Continue;
            }
            match self.node_owned(relationship.target) {
                Ok(Some(target)) if target.labels.contains(&target_label_id) => {
                    ids.insert(relationship.target);
                    if ids.len() > max_ids {
                        callback_error = Some(HawDBError::Execution(format!(
                            "mutation would exceed max_mutation_affected_rows {max_ids}"
                        )));
                        return GraphScanControl::Stop;
                    }
                }
                Ok(_) => {}
                Err(error) => {
                    callback_error = Some(error);
                    return GraphScanControl::Stop;
                }
            }
            GraphScanControl::Continue
        })?;
        if let Some(error) = callback_error {
            return Err(error);
        }
        Ok(ids.into_iter().collect())
    }

    pub(super) fn relationship_target_node_ids_with_pending_bounded(
        &self,
        catalog: &Catalog,
        request: &RelationshipTargetNodeDelete,
        pending_nodes: &[PendingNode],
        pending_relationships: &[PendingRelationship],
        max_ids: usize,
    ) -> Result<Vec<NodeId>> {
        let Some(source_label_id) = optional_label_id(catalog, &request.source_label) else {
            return Ok(Vec::new());
        };
        let Some(target_label_id) = optional_label_id(catalog, &request.target_label) else {
            return Ok(Vec::new());
        };
        let Some(rel_type_id) = catalog.rel_type_id(&request.rel_type) else {
            return Ok(Vec::new());
        };
        let source_ids = self
            .matching_node_ids_with_pending_bounded(
                Some(source_label_id),
                request.source_filter.as_ref(),
                pending_nodes,
                max_ids,
                "max_mutation_affected_rows",
            )?
            .into_iter()
            .collect::<BTreeSet<_>>();
        if source_ids.is_empty() {
            return Ok(Vec::new());
        }
        let target_ids = request
            .target_filter
            .as_ref()
            .map(|filter| {
                self.matching_node_ids_with_pending_bounded(
                    Some(target_label_id),
                    Some(filter),
                    pending_nodes,
                    max_ids,
                    "max_mutation_affected_rows",
                )
                .map(|ids| ids.into_iter().collect::<BTreeSet<_>>())
            })
            .transpose()?;
        if target_ids.as_ref().is_some_and(BTreeSet::is_empty) {
            return Ok(Vec::new());
        }
        let mut ids = self.relationship_target_node_ids_bounded(catalog, request, max_ids)?;
        for (relationship_id, source, target, pending_rel_type_id, properties) in
            pending_relationships
        {
            if *pending_rel_type_id != rel_type_id
                || !source_ids.contains(source)
                || request.rel_filter.as_ref().is_some_and(|filter| {
                    !property_filter_matches(filter, relationship_id.0, properties)
                })
                || !node_matches_label_and_filter(
                    self,
                    pending_nodes,
                    *target,
                    target_label_id,
                    request.target_filter.as_ref(),
                )?
                || target_ids.as_ref().is_some_and(|ids| !ids.contains(target))
            {
                continue;
            }
            ids.push(*target);
            if ids.len() > max_ids {
                return Err(HawDBError::Execution(format!(
                    "mutation would exceed max_mutation_affected_rows {max_ids}"
                )));
            }
        }
        Ok(ids
            .into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect())
    }

    pub fn set_relationship_property(
        &mut self,
        catalog: &mut Catalog,
        update: RelationshipPropertyUpdate,
    ) -> Result<Vec<RelId>> {
        self.set_relationship_properties(
            catalog,
            RelationshipPropertiesUpdate {
                source_label: update.source_label,
                filter: update.filter,
                rel_type: update.rel_type,
                target_label: update.target_label,
                target_filter: update.target_filter,
                rel_filter: update.rel_filter,
                assignments: vec![RelationshipSetAssignment {
                    property: update.property,
                    value: update.value,
                }],
            },
        )
    }

    pub fn set_relationship_properties(
        &mut self,
        catalog: &mut Catalog,
        update: RelationshipPropertiesUpdate,
    ) -> Result<Vec<RelId>> {
        let Some(source_label_id) = catalog.label_id(&update.source_label) else {
            return Ok(Vec::new());
        };
        let Some(target_label_id) = catalog.label_id(&update.target_label) else {
            return Ok(Vec::new());
        };
        let Some(rel_type_id) = catalog.rel_type_id(&update.rel_type) else {
            return Ok(Vec::new());
        };
        let source_ids = self
            .matching_node_ids(catalog, Some(source_label_id), update.filter.as_ref())?
            .into_iter()
            .collect::<BTreeSet<_>>();
        if source_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut ids = Vec::new();
        for relationship in self.relationship_records_owned() {
            let relationship = relationship?;
            if relationship.rel_type != rel_type_id
                || !source_ids.contains(&relationship.source)
                || update.rel_filter.as_ref().is_some_and(|filter| {
                    !property_filter_matches(filter, relationship.id.0, &relationship.properties)
                })
            {
                continue;
            }
            if self
                .node_owned(relationship.target)?
                .is_some_and(|target| target.labels.contains(&target_label_id))
            {
                ids.push(relationship.id);
            }
        }
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let ops =
            ids.iter()
                .flat_map(|id| {
                    update.assignments.iter().map(move |assignment| {
                        WalOp::SetRelationshipProperty {
                            id: *id,
                            property: assignment.property.clone(),
                            value: assignment.value.clone(),
                        }
                    })
                })
                .collect::<Vec<_>>();
        self.validate_constraints_for_ops(catalog, &ops)?;
        self.append_durable_wal_batch(&ops)?;
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        for op in ops {
            self.apply_wal_op(catalog, op)?;
        }
        self.finish_non_relational_commit();
        Ok(ids)
    }

    pub fn create_connected_nodes(
        &mut self,
        catalog: &mut Catalog,
        request: ConnectedNodesCreate,
    ) -> Result<(NodeId, RelId, NodeId)> {
        let source_label_id = catalog.get_or_create_label(&request.source_label);
        let target_label_id = catalog.get_or_create_label(&request.target_label);
        let rel_type_id = catalog.get_or_create_rel_type(&request.rel_type);
        let source = NodeId(self.next_node_id);
        let target = NodeId(self.next_node_id + 1);
        let relationship = RelId(self.next_rel_id);
        let ops = vec![
            WalOp::CreateNode {
                id: source,
                label: request.source_label.clone(),
                properties: request.source_properties.clone(),
            },
            WalOp::CreateNode {
                id: target,
                label: request.target_label.clone(),
                properties: request.target_properties.clone(),
            },
            WalOp::CreateRelationship {
                id: relationship,
                source,
                target,
                rel_type: request.rel_type.clone(),
                properties: request.rel_properties.clone(),
            },
        ];
        self.validate_constraints_for_ops(catalog, &ops)?;
        self.append_durable_wal_batch(&ops)?;
        self.apply_create_node(catalog, source, source_label_id, request.source_properties);
        self.apply_create_node(catalog, target, target_label_id, request.target_properties);
        self.apply_create_relationship(
            relationship,
            source,
            target,
            rel_type_id,
            request.rel_properties,
        );
        self.finish_non_relational_commit();
        Ok((source, relationship, target))
    }

    pub fn merge_connected_nodes(
        &mut self,
        catalog: &mut Catalog,
        request: ConnectedNodesCreate,
    ) -> Result<(NodeId, RelId, NodeId, bool)> {
        let source_label_id = catalog.get_or_create_label(&request.source_label);
        let target_label_id = catalog.get_or_create_label(&request.target_label);
        let rel_type_id = catalog.get_or_create_rel_type(&request.rel_type);
        let source =
            self.find_node_by_label_and_properties(source_label_id, &request.source_properties)?;
        let target =
            self.find_node_by_label_and_properties(target_label_id, &request.target_properties)?;
        if let (Some(source), Some(target)) = (source, target)
            && let Some(relationship) = self.find_relationship_by_properties(
                source,
                target,
                rel_type_id,
                &request.rel_properties,
            )?
        {
            return Ok((source, relationship, target, false));
        }

        let source = source.unwrap_or(NodeId(self.next_node_id));
        let target = target.unwrap_or_else(|| {
            if source.0 == self.next_node_id {
                NodeId(self.next_node_id + 1)
            } else {
                NodeId(self.next_node_id)
            }
        });
        let relationship = RelId(self.next_rel_id);
        let ops = self.merge_connected_node_ops(&request, source, target, relationship)?;
        self.validate_constraints_for_ops(catalog, &ops)?;
        self.append_durable_wal_batch(&ops)?;
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        for op in ops {
            self.apply_wal_op(catalog, op)?;
        }
        self.finish_non_relational_commit();
        Ok((source, relationship, target, true))
    }

    pub(super) fn record_search_projection_graph_changes_for_ops(
        &mut self,
        catalog: &Catalog,
        commit_epoch: u64,
        ops: &[WalOp],
    ) {
        self.record_search_projection_changes_for_ops(catalog, commit_epoch, ops, None);
    }

    pub(super) fn record_search_projection_changes_for_ops(
        &mut self,
        catalog: &Catalog,
        commit_epoch: u64,
        ops: &[WalOp],
        relational_primary_key_changes: Option<hawdb_storage::RelationalPrimaryKeyChangeCapture>,
    ) {
        let mut upsert_node_ids = BTreeSet::new();
        let mut delete_document_ids = BTreeSet::new();
        self.collect_search_projection_graph_changes_for_ops(
            catalog,
            ops,
            &mut upsert_node_ids,
            &mut delete_document_ids,
        );
        let relational_primary_key_changes = relational_primary_key_changes.unwrap_or_else(|| {
            let reason = if wal_ops_contain_relational_snapshot(ops) {
                Some(hawdb_storage::RelationalPrimaryKeyChangeRebuildReason::SnapshotReplacement)
            } else if wal_ops_contain_relational_transaction(ops) {
                Some(hawdb_storage::RelationalPrimaryKeyChangeRebuildReason::MissingWalCapture)
            } else {
                None
            };
            match reason {
                Some(reason) => {
                    hawdb_storage::RelationalPrimaryKeyChangeCapture::RequiresRebuild { reason }
                }
                None => hawdb_storage::RelationalPrimaryKeyChangeCapture::Captured {
                    tables: Vec::new(),
                    encoded_bytes: 0,
                },
            }
        });
        let relational_primary_key_changes =
            omit_internal_search_projection_relational_changes(relational_primary_key_changes);
        if upsert_node_ids.is_empty()
            && delete_document_ids.is_empty()
            && relational_primary_key_changes.operation_count() == 0
            && !relational_primary_key_changes.requires_rebuild()
        {
            return;
        }
        let change = SearchProjectionGraphChange {
            commit_epoch,
            upsert_node_ids: upsert_node_ids.into_iter().map(|id| id.0).collect(),
            delete_document_ids: delete_document_ids.into_iter().collect(),
            relational_primary_key_changes,
        };
        self.search_projection_change_log_retained_bytes = self
            .search_projection_change_log_retained_bytes
            .saturating_add(change.estimated_retained_bytes());
        self.search_projection_graph_changes.push(change);
        self.trim_search_projection_graph_change_log();
    }

    pub(super) fn relational_primary_key_changes_from_wal_ops(
        &self,
        ops: &[WalOp],
    ) -> Result<Option<hawdb_storage::RelationalPrimaryKeyChangeCapture>> {
        let mut captures = Vec::new();
        collect_relational_primary_key_changes_from_wal_ops(ops, &mut captures)?;
        if captures.len() > 1 {
            return Ok(Some(
                hawdb_storage::RelationalPrimaryKeyChangeCapture::RequiresRebuild {
                    reason: hawdb_storage::RelationalPrimaryKeyChangeRebuildReason::MultipleRelationalTransactions,
                },
            ));
        }
        Ok(captures.pop())
    }

    pub(super) fn trim_search_projection_graph_change_log(&mut self) {
        let mut retained_bytes = self.search_projection_change_log_retained_bytes;
        let mut remove_count = 0usize;
        while remove_count < self.search_projection_graph_changes.len()
            && (self
                .max_search_projection_change_log_entries
                .is_some_and(|limit| {
                    self.search_projection_graph_changes
                        .len()
                        .saturating_sub(remove_count)
                        > limit
                })
                || self
                    .max_search_projection_change_log_bytes
                    .is_some_and(|limit| retained_bytes > limit))
        {
            retained_bytes = retained_bytes.saturating_sub(
                self.search_projection_graph_changes[remove_count].estimated_retained_bytes(),
            );
            remove_count = remove_count.saturating_add(1);
        }
        if remove_count > 0 {
            if let Some(last_removed) = self
                .search_projection_graph_changes
                .get(remove_count.saturating_sub(1))
            {
                self.search_projection_change_log_start_epoch = self
                    .search_projection_change_log_start_epoch
                    .max(last_removed.commit_epoch);
            }
            self.search_projection_graph_changes.drain(0..remove_count);
        }
        self.search_projection_change_log_retained_bytes = retained_bytes;
    }

    fn collect_search_projection_graph_changes_for_ops(
        &self,
        catalog: &Catalog,
        ops: &[WalOp],
        upsert_node_ids: &mut BTreeSet<NodeId>,
        delete_document_ids: &mut BTreeSet<String>,
    ) {
        for op in ops {
            match op {
                WalOp::CreateNode { id, .. } => {
                    // The host batch hydrator may own denormalized projection
                    // dependencies for nodes that are not direct search
                    // documents, so every live changed node must remain
                    // observable in the unified changefeed.
                    upsert_node_ids.insert(*id);
                }
                WalOp::SetNodeProperty { id, property, .. } => {
                    if let Some(node) = self.nodes.get(id) {
                        if property == "id"
                            && let Some(document_id) =
                                search_projection_document_id_for_node(catalog, node)
                        {
                            delete_document_ids.insert(document_id);
                        }
                        upsert_node_ids.insert(*id);
                    }
                    if matches!(property.as_str(), "id" | "name" | "canonical_name") {
                        self.collect_label_projection_neighbors(catalog, *id, upsert_node_ids);
                    }
                }
                WalOp::DeleteNode { id } => {
                    if let Some(node) = self.nodes.get(id)
                        && let Some(document_id) =
                            search_projection_document_id_for_node(catalog, node)
                    {
                        delete_document_ids.insert(document_id);
                    }
                    self.collect_label_projection_neighbors(catalog, *id, upsert_node_ids);
                }
                WalOp::Batch(batch_ops) => self.collect_search_projection_graph_changes_for_ops(
                    catalog,
                    batch_ops,
                    upsert_node_ids,
                    delete_document_ids,
                ),
                WalOp::CreateRelationship {
                    source,
                    target,
                    rel_type,
                    ..
                } => {
                    if rel_type == "HAS_LABEL" {
                        self.collect_has_label_projection_endpoints(
                            catalog,
                            *source,
                            *target,
                            upsert_node_ids,
                        );
                    }
                }
                WalOp::DeleteRelationship { id } => {
                    if let Some(relationship) = self.relationships.get(id) {
                        self.collect_has_label_projection_endpoints_for_relationship(
                            catalog,
                            relationship,
                            upsert_node_ids,
                        );
                    }
                }
                WalOp::CreateNodeLabel { .. }
                | WalOp::CreateRelationshipType { .. }
                | WalOp::CreateNodeTable { .. }
                | WalOp::CreateRelationshipTable { .. }
                | WalOp::CreateProperty { .. }
                | WalOp::AlterTableState { .. }
                | WalOp::AlterPropertyState { .. }
                | WalOp::GcTableDescriptor { .. }
                | WalOp::GcPropertyDescriptor { .. }
                | WalOp::CreateIndex { .. }
                | WalOp::CreateCompositeIndex { .. }
                | WalOp::CreateRangeIndex { .. }
                | WalOp::CreateFullTextIndex { .. }
                | WalOp::CreateUniqueConstraint { .. }
                | WalOp::CreateNodePropertyExistsConstraint { .. }
                | WalOp::CreateRelationshipUniqueConstraint { .. }
                | WalOp::CreateRelationshipPropertyExistsConstraint { .. }
                | WalOp::SetRelationshipProperty { .. }
                | WalOp::ProjectGraph { .. }
                | WalOp::MarkInitialImportSource { .. }
                | WalOp::Relational { .. }
                | WalOp::RelationalSnapshot { .. }
                | WalOp::Append { .. } => {}
            }
        }
    }

    fn collect_label_projection_neighbors(
        &self,
        catalog: &Catalog,
        label_node_id: NodeId,
        upsert_node_ids: &mut BTreeSet<NodeId>,
    ) {
        let Some(label_node) = self.nodes.get(&label_node_id) else {
            return;
        };
        if !Self::node_has_label(catalog, label_node, "Label") {
            return;
        }
        let Some(has_label_type_id) = catalog.rel_type_id("HAS_LABEL") else {
            return;
        };
        for relationship in self.scan_relationships(Some(has_label_type_id)) {
            if relationship.source == label_node_id {
                self.insert_projection_node_if_any(catalog, relationship.target, upsert_node_ids);
            } else if relationship.target == label_node_id {
                self.insert_projection_node_if_any(catalog, relationship.source, upsert_node_ids);
            }
        }
    }

    fn collect_has_label_projection_endpoints_for_relationship(
        &self,
        catalog: &Catalog,
        relationship: &RelRecord,
        upsert_node_ids: &mut BTreeSet<NodeId>,
    ) {
        if catalog.rel_type_name(relationship.rel_type) != Some("HAS_LABEL") {
            return;
        }
        self.collect_has_label_projection_endpoints(
            catalog,
            relationship.source,
            relationship.target,
            upsert_node_ids,
        );
    }

    fn collect_has_label_projection_endpoints(
        &self,
        catalog: &Catalog,
        source: NodeId,
        target: NodeId,
        upsert_node_ids: &mut BTreeSet<NodeId>,
    ) {
        let source_is_label = self
            .nodes
            .get(&source)
            .is_some_and(|node| Self::node_has_label(catalog, node, "Label"));
        let target_is_label = self
            .nodes
            .get(&target)
            .is_some_and(|node| Self::node_has_label(catalog, node, "Label"));
        if source_is_label {
            self.insert_projection_node_if_any(catalog, target, upsert_node_ids);
        }
        if target_is_label {
            self.insert_projection_node_if_any(catalog, source, upsert_node_ids);
        }
    }

    fn insert_projection_node_if_any(
        &self,
        catalog: &Catalog,
        node_id: NodeId,
        upsert_node_ids: &mut BTreeSet<NodeId>,
    ) {
        if self
            .nodes
            .get(&node_id)
            .and_then(|node| search_projection_document_id_for_node(catalog, node))
            .is_some()
        {
            upsert_node_ids.insert(node_id);
        }
    }

    fn node_has_label(catalog: &Catalog, node: &NodeRecord, label: &str) -> bool {
        catalog
            .label_id(label)
            .is_some_and(|label_id| node.labels.contains(&label_id))
    }

    pub(super) fn find_node_by_label_and_properties(
        &self,
        label_id: LabelId,
        properties: &BTreeMap<String, Value>,
    ) -> Result<Option<NodeId>> {
        let mut found = None;
        self.visit_nodes_owned(Some(label_id), |node| {
            if properties
                .iter()
                .all(|(key, value)| node.properties.get(key) == Some(value))
            {
                found = Some(node.id);
                GraphScanControl::Stop
            } else {
                GraphScanControl::Continue
            }
        })?;
        Ok(found)
    }

    pub(super) fn find_relationship_by_properties(
        &self,
        source: NodeId,
        target: NodeId,
        rel_type_id: RelTypeId,
        properties: &BTreeMap<String, Value>,
    ) -> Result<Option<RelId>> {
        let mut found = None;
        self.visit_adjacent_relationships_owned(
            source,
            Some(rel_type_id),
            AdjacencyDirection::Outgoing,
            |relationship| {
                if relationship.target == target && &relationship.properties == properties {
                    found = Some(relationship.id);
                    GraphScanControl::Stop
                } else {
                    GraphScanControl::Continue
                }
            },
        )?;
        Ok(found)
    }

    pub(super) fn find_relationship_by_property_subset(
        &self,
        source: NodeId,
        target: NodeId,
        rel_type_id: RelTypeId,
        properties: &BTreeMap<String, Value>,
    ) -> Result<Option<RelId>> {
        let mut found = None;
        self.visit_adjacent_relationships_owned(
            source,
            Some(rel_type_id),
            AdjacencyDirection::Outgoing,
            |relationship| {
                if relationship.target == target
                    && properties_contain_all(&relationship.properties, properties)
                {
                    found = Some(relationship.id);
                    GraphScanControl::Stop
                } else {
                    GraphScanControl::Continue
                }
            },
        )?;
        Ok(found)
    }

    fn merge_connected_node_ops(
        &self,
        request: &ConnectedNodesCreate,
        source: NodeId,
        target: NodeId,
        relationship: RelId,
    ) -> Result<Vec<WalOp>> {
        let mut ops = Vec::new();
        if self.node_owned(source)?.is_none() {
            ops.push(WalOp::CreateNode {
                id: source,
                label: request.source_label.clone(),
                properties: request.source_properties.clone(),
            });
        }
        if self.node_owned(target)?.is_none() {
            ops.push(WalOp::CreateNode {
                id: target,
                label: request.target_label.clone(),
                properties: request.target_properties.clone(),
            });
        }
        ops.push(WalOp::CreateRelationship {
            id: relationship,
            source,
            target,
            rel_type: request.rel_type.clone(),
            properties: request.rel_properties.clone(),
        });
        Ok(ops)
    }

    fn matching_node_ids(
        &self,
        catalog: &Catalog,
        label_id: Option<LabelId>,
        filter: Option<&PropertyFilter>,
    ) -> Result<Vec<NodeId>> {
        if !self.canonical_base_out_of_core {
            return Ok(self
                .scan_nodes_with_filter_pruning(catalog, label_id, filter)
                .nodes
                .into_iter()
                .map(|node| node.id)
                .collect());
        }
        let mut ids = Vec::new();
        self.visit_nodes_owned(label_id, |node| {
            if filter
                .is_none_or(|filter| property_filter_matches(filter, node.id.0, &node.properties))
            {
                ids.push(node.id);
            }
            GraphScanControl::Continue
        })?;
        Ok(ids)
    }

    pub(super) fn matching_node_ids_bounded(
        &self,
        label_id: Option<LabelId>,
        filter: Option<&PropertyFilter>,
        max_ids: usize,
        limit_name: &str,
    ) -> Result<Vec<NodeId>> {
        let mut ids = Vec::with_capacity(max_ids.min(1024));
        let mut exceeded = false;
        self.visit_nodes_owned(label_id, |node| {
            if filter
                .is_none_or(|filter| property_filter_matches(filter, node.id.0, &node.properties))
            {
                if ids.len() == max_ids {
                    exceeded = true;
                    return GraphScanControl::Stop;
                }
                ids.push(node.id);
            }
            GraphScanControl::Continue
        })?;
        if exceeded {
            return Err(HawDBError::Execution(format!(
                "mutation would exceed {limit_name} {max_ids}"
            )));
        }
        Ok(ids)
    }

    pub(super) fn matching_node_ids_with_pending_bounded(
        &self,
        label_id: Option<LabelId>,
        filter: Option<&PropertyFilter>,
        pending_nodes: &[PendingNode],
        max_ids: usize,
        limit_name: &str,
    ) -> Result<Vec<NodeId>> {
        let mut ids = self.matching_node_ids_bounded(label_id, filter, max_ids, limit_name)?;
        for id in Self::pending_node_ids_matching(label_id, filter, pending_nodes) {
            if ids.len() == max_ids {
                return Err(HawDBError::Execution(format!(
                    "mutation would exceed {limit_name} {max_ids}"
                )));
            }
            ids.push(id);
        }
        Ok(ids)
    }

    pub(super) fn pending_node_ids_matching(
        label_id: Option<LabelId>,
        filter: Option<&PropertyFilter>,
        pending_nodes: &[PendingNode],
    ) -> Vec<NodeId> {
        pending_nodes
            .iter()
            .filter(|(id, pending_label_id, properties)| {
                label_id.is_none_or(|label_id| *pending_label_id == label_id)
                    && filter.is_none_or(|filter| property_filter_matches(filter, id.0, properties))
            })
            .map(|(id, _, _)| *id)
            .collect()
    }

    fn delete_node_ops(&self, ids: &[NodeId], detach: bool) -> Result<Vec<WalOp>> {
        self.delete_node_ops_bounded(ids, detach, usize::MAX)
    }

    pub(super) fn delete_node_ops_bounded(
        &self,
        ids: &[NodeId],
        detach: bool,
        max_operations: usize,
    ) -> Result<Vec<WalOp>> {
        let mut relationship_ids = BTreeSet::new();
        for id in ids {
            for relationship in self.relationship_records_owned() {
                let relationship = relationship?;
                if relationship.source == *id || relationship.target == *id {
                    if !detach {
                        return Err(HawDBError::Storage(format!(
                            "node {} has relationships; use DETACH DELETE",
                            id.0
                        )));
                    }
                    relationship_ids.insert(relationship.id);
                    if relationship_ids.len().saturating_add(ids.len()) > max_operations {
                        return Err(HawDBError::Execution(format!(
                            "mutation would exceed max_mutation_operations {max_operations}"
                        )));
                    }
                }
            }
        }
        if ids.len() > max_operations {
            return Err(HawDBError::Execution(format!(
                "mutation would exceed max_mutation_operations {max_operations}"
            )));
        }
        let mut ops = relationship_ids
            .into_iter()
            .map(|id| WalOp::DeleteRelationship { id })
            .collect::<Vec<_>>();
        ops.extend(ids.iter().copied().map(|id| WalOp::DeleteNode { id }));
        Ok(ops)
    }
}

fn omit_internal_search_projection_relational_changes(
    capture: hawdb_storage::RelationalPrimaryKeyChangeCapture,
) -> hawdb_storage::RelationalPrimaryKeyChangeCapture {
    let hawdb_storage::RelationalPrimaryKeyChangeCapture::Captured { mut tables, .. } = capture
    else {
        return capture;
    };
    tables.retain(|table| table.table != "hawdb_schema_migrations");
    let encoded_bytes = tables.iter().fold(0usize, |total, table| {
        let table_bytes = 4usize.saturating_add(table.table.len());
        table.primary_keys.iter().fold(
            total.saturating_add(table_bytes),
            |table_total, primary_key| {
                let key_bytes = hawdb_storage::encode_relational_primary_key(primary_key)
                    .map_or(0, |encoded| encoded.len());
                table_total.saturating_add(4).saturating_add(key_bytes)
            },
        )
    });
    hawdb_storage::RelationalPrimaryKeyChangeCapture::Captured {
        tables,
        encoded_bytes,
    }
}

fn collect_relational_primary_key_changes_from_wal_ops(
    ops: &[WalOp],
    captures: &mut Vec<hawdb_storage::RelationalPrimaryKeyChangeCapture>,
) -> Result<()> {
    for op in ops {
        match op {
            WalOp::Relational { record } => {
                let batch = decode_relational_wal_batch(record, RelationalDecodeLimits::wal())
                    .map_err(|error| HawDBError::Storage(error.to_string()))?;
                captures.push(batch.primary_key_changes.unwrap_or(
                    hawdb_storage::RelationalPrimaryKeyChangeCapture::RequiresRebuild {
                        reason: hawdb_storage::RelationalPrimaryKeyChangeRebuildReason::MissingWalCapture,
                    },
                ));
            }
            WalOp::RelationalSnapshot { .. } => captures.push(
                hawdb_storage::RelationalPrimaryKeyChangeCapture::RequiresRebuild {
                    reason:
                        hawdb_storage::RelationalPrimaryKeyChangeRebuildReason::SnapshotReplacement,
                },
            ),
            WalOp::Batch(ops) => {
                collect_relational_primary_key_changes_from_wal_ops(ops, captures)?;
            }
            _ => {}
        }
    }
    Ok(())
}
