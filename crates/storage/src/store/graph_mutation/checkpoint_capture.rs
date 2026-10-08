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

//! Cooperative traversal for private replay change capture. Ordinary mutation
//! capture remains independent. Collection allocations, document-ID formatting,
//! relational capture normalization and log trimming require separate controls.

use super::*;
use crate::background::CheckpointWorkContext;
use crate::relational::{
    RelationalPrimaryKeyChangeCapture, RelationalPrimaryKeyChangeRebuildReason,
};

impl GraphStore {
    pub(in crate::store) fn record_checkpoint_search_projection_changes_for_ops(
        &mut self,
        catalog: &Catalog,
        commit_epoch: u64,
        ops: &[WalOp],
        relational_changes: Option<RelationalPrimaryKeyChangeCapture>,
        work: &CheckpointWorkContext,
    ) -> Result<()> {
        let mut upserts = BTreeSet::new();
        let mut deletes = BTreeSet::new();
        self.collect_checkpoint_search_changes(catalog, ops, &mut upserts, &mut deletes, work)?;
        let relational_changes = match relational_changes {
            Some(capture) => capture,
            None => match checkpoint_relational_reason(ops, work)? {
                Some(reason) => RelationalPrimaryKeyChangeCapture::RequiresRebuild { reason },
                None => RelationalPrimaryKeyChangeCapture::Captured {
                    tables: Vec::new(),
                    encoded_bytes: 0,
                },
            },
        };
        let relational_changes =
            omit_internal_search_projection_relational_changes(relational_changes);
        if upserts.is_empty()
            && deletes.is_empty()
            && relational_changes.operation_count() == 0
            && !relational_changes.requires_rebuild()
        {
            return work.checkpoint().map_err(HawDBError::from_storage_error);
        }
        let mut upsert_node_ids = Vec::with_capacity(upserts.len());
        for id in upserts {
            let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
            upsert_node_ids.push(id.0);
            unit.finish();
        }
        let mut delete_document_ids = Vec::with_capacity(deletes.len());
        for id in deletes {
            let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
            delete_document_ids.push(id);
            unit.finish();
        }
        work.checkpoint().map_err(HawDBError::from_storage_error)?;
        let change = SearchProjectionGraphChange {
            commit_epoch,
            upsert_node_ids,
            delete_document_ids,
            relational_primary_key_changes: relational_changes,
        };
        self.search_projection_change_log_retained_bytes = self
            .search_projection_change_log_retained_bytes
            .saturating_add(change.estimated_retained_bytes());
        self.search_projection_graph_changes.push(Arc::new(change));
        self.trim_search_projection_graph_change_log();
        Ok(())
    }

    fn collect_checkpoint_search_changes(
        &self,
        catalog: &Catalog,
        ops: &[WalOp],
        upserts: &mut BTreeSet<NodeId>,
        deletes: &mut BTreeSet<String>,
        work: &CheckpointWorkContext,
    ) -> Result<()> {
        for op in ops {
            let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
            // Inspect one operation before entering nested scans. Never retain
            // this permit while a relationship visitor starts its own unit.
            let neighbors = match op {
                WalOp::SetNodeProperty { id, property, .. }
                    if matches!(property.as_str(), "id" | "name" | "canonical_name") =>
                {
                    Some(*id)
                }
                WalOp::DeleteNode { id } => Some(*id),
                _ => None,
            };
            unit.finish();
            work.checkpoint().map_err(HawDBError::from_storage_error)?;
            match op {
                WalOp::CreateNode { id, .. } => {
                    upserts.insert(*id);
                }
                WalOp::SetNodeProperty { id, property, .. } => {
                    if let Some(node) = self.nodes.get(id) {
                        if property == "id"
                            && let Some(document_id) =
                                search_projection_document_id_for_node(catalog, node)
                        {
                            deletes.insert(document_id);
                        }
                        upserts.insert(*id);
                    }
                }
                WalOp::DeleteNode { id } => {
                    if let Some(node) = self.nodes.get(id)
                        && let Some(document_id) =
                            search_projection_document_id_for_node(catalog, node)
                    {
                        deletes.insert(document_id);
                    }
                }
                WalOp::Batch(ops) => {
                    self.collect_checkpoint_search_changes(catalog, ops, upserts, deletes, work)?;
                }
                WalOp::CreateRelationship {
                    source,
                    target,
                    rel_type,
                    ..
                } if rel_type == "HAS_LABEL" => {
                    self.collect_has_label_projection_endpoints(catalog, *source, *target, upserts);
                }
                WalOp::DeleteRelationship { id } => {
                    if let Some(relationship) = self.relationships.get(id) {
                        self.collect_has_label_projection_endpoints_for_relationship(
                            catalog,
                            relationship,
                            upserts,
                        );
                    }
                }
                _ => {}
            }
            if let Some(label) = neighbors {
                self.collect_checkpoint_label_neighbors(catalog, label, upserts, work)?;
            }
            work.checkpoint().map_err(HawDBError::from_storage_error)?;
        }
        Ok(())
    }

    fn collect_checkpoint_label_neighbors(
        &self,
        catalog: &Catalog,
        label: NodeId,
        upserts: &mut BTreeSet<NodeId>,
        work: &CheckpointWorkContext,
    ) -> Result<()> {
        let Some(node) = self.nodes.get(&label) else {
            return Ok(());
        };
        if !Self::node_has_label(catalog, node, "Label") {
            return Ok(());
        }
        let Some(rel_type) = catalog.rel_type_id("HAS_LABEL") else {
            return Ok(());
        };
        // Filtering inside Iterator::next would hide an arbitrarily long
        // nonmatching prefix. Admit every visited relationship before filtering.
        for relationship in self.relationships.values() {
            let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
            let neighbor = if relationship.rel_type != rel_type {
                None
            } else if relationship.source == label {
                Some(relationship.target)
            } else if relationship.target == label {
                Some(relationship.source)
            } else {
                None
            };
            unit.finish();
            work.checkpoint().map_err(HawDBError::from_storage_error)?;
            if let Some(neighbor) = neighbor {
                self.insert_projection_node_if_any(catalog, neighbor, upserts);
            }
        }
        Ok(())
    }
}

fn checkpoint_relational_reason(
    ops: &[WalOp],
    work: &CheckpointWorkContext,
) -> Result<Option<RelationalPrimaryKeyChangeRebuildReason>> {
    let mut reason = None;
    for op in ops {
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        let snapshot = matches!(op, WalOp::RelationalSnapshot { .. });
        let transaction = matches!(op, WalOp::Relational { .. });
        unit.finish();
        work.checkpoint().map_err(HawDBError::from_storage_error)?;
        if snapshot {
            return Ok(Some(
                RelationalPrimaryKeyChangeRebuildReason::SnapshotReplacement,
            ));
        }
        if transaction {
            reason = Some(RelationalPrimaryKeyChangeRebuildReason::MissingWalCapture);
        }
        if let WalOp::Batch(nested) = op
            && let Some(nested_reason) = checkpoint_relational_reason(nested, work)?
        {
            if nested_reason == RelationalPrimaryKeyChangeRebuildReason::SnapshotReplacement {
                return Ok(Some(nested_reason));
            }
            reason = Some(nested_reason);
        }
    }
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    Ok(reason)
}
