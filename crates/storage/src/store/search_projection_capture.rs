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

//! Pre-WAL projection capture from canonical metadata and live overlays.

use super::*;
use crate::projection::owner_capture::{
    CapturedProjectionChange, OwnerBeforeV1, OwnerEffectV1, OwnerIdV1,
};
use crate::projection::projection_document_id_for_label_and_properties;

const MAX_WORK: usize = 65_536;
const MAX_BYTES: usize = 64 * 1024 * 1024;
const MAX_PREIMAGE_BYTES: usize = 64 * 1024;
const MAX_DEPTH: usize = 64;

/// These effects cannot be reconstructed from the point graph/relational feed.
/// Keep the match exhaustive: a new WAL family must choose its capture contract.
/// The same decision is used before append, after successful direct append, and
/// during replay. Over-budget classification conservatively requires rebuild.
pub(super) fn projection_wal_requires_rebuild(ops: &[WalOp]) -> bool {
    fn requires(op: &WalOp, depth: usize, remaining: &mut usize) -> bool {
        if depth > MAX_DEPTH || *remaining == 0 {
            return true;
        }
        *remaining -= 1;
        match op {
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
            | WalOp::ProjectGraph { .. }
            | WalOp::MarkInitialImportSource { .. }
            | WalOp::Append { .. } => true,
            // Relational records carry their existing typed key capture or a
            // RequiresRebuild event, including snapshot replacement. Graph
            // point effects use the collector below. Relationship properties
            // do not affect the built-in graph Search document mapping; host
            // mapping effects still require their additional complete contract.
            WalOp::Relational { .. }
            | WalOp::RelationalSnapshot { .. }
            | WalOp::CreateNode { .. }
            | WalOp::SetNodeProperty { .. }
            | WalOp::DeleteNode { .. }
            | WalOp::CreateRelationship { .. }
            | WalOp::SetRelationshipProperty { .. }
            | WalOp::DeleteRelationship { .. } => false,
            WalOp::Batch(ops) => ops.iter().any(|op| requires(op, depth + 1, remaining)),
        }
    }
    let mut remaining = MAX_WORK;
    ops.iter().any(|op| requires(op, 0, &mut remaining))
}

pub(super) enum PreparedSearchProjectionChange {
    Complete(Option<CapturedProjectionChange>),
    Gap(u64),
}

enum CaptureError {
    Limit,
    Storage(HawDBError),
}
type CaptureResult<T> = std::result::Result<T, CaptureError>;
impl From<HawDBError> for CaptureError {
    fn from(error: HawDBError) -> Self {
        Self::Storage(error)
    }
}

struct Budget {
    checkpoint: Option<crate::background::CheckpointWorkContext>,
    work: usize,
    bytes: usize,
}
impl Budget {
    fn work(&mut self) -> CaptureResult<()> {
        if let Some(work) = &self.checkpoint {
            let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
            unit.finish();
        }
        self.work = self.work.checked_sub(1).ok_or(CaptureError::Limit)?;
        Ok(())
    }
    fn bytes(&mut self, bytes: usize) -> CaptureResult<()> {
        self.bytes = self.bytes.checked_sub(bytes).ok_or(CaptureError::Limit)?;
        Ok(())
    }
}
struct NodeMetadata {
    label: bool,
    projected: bool,
    document_id: Option<String>,
    before: OwnerBeforeV1,
}
struct CollectedOwner {
    legacy_upsert: bool,
    before: OwnerBeforeV1,
}
struct Collector<'a> {
    store: &'a GraphStore,
    catalog: &'a Catalog,
    budget: Budget,
    created_nodes: BTreeMap<NodeId, &'a str>,
    created_relationships: BTreeMap<RelId, (NodeId, NodeId, &'a str)>,
    owners: BTreeMap<NodeId, CollectedOwner>,
    deletes: BTreeSet<String>,
}
impl<'a> Collector<'a> {
    fn created(&mut self, ops: &'a [WalOp], depth: usize) -> CaptureResult<()> {
        if depth > MAX_DEPTH {
            return Err(CaptureError::Limit);
        }
        for op in ops {
            self.budget.work()?;
            match op {
                WalOp::CreateNode { id, label, .. } => {
                    self.budget.bytes(96)?;
                    self.created_nodes.insert(*id, label);
                }
                WalOp::CreateRelationship {
                    id,
                    source,
                    target,
                    rel_type,
                    ..
                } => {
                    self.budget.bytes(128)?;
                    self.created_relationships
                        .insert(*id, (*source, *target, rel_type));
                }
                WalOp::Batch(ops) => self.created(ops, depth + 1)?,
                _ => {}
            }
        }
        Ok(())
    }
    fn owner(
        &mut self,
        id: NodeId,
        legacy_upsert: bool,
        before: OwnerBeforeV1,
    ) -> CaptureResult<()> {
        if let Some(owner) = self.owners.get_mut(&id) {
            owner.legacy_upsert |= legacy_upsert;
            // Pre-transaction facts take precedence over an unneeded preimage.
            if !matches!(before, OwnerBeforeV1::NotRecorded) {
                owner.before = before;
            }
        } else {
            self.budget
                .bytes(96 + std::mem::size_of::<CollectedOwner>())?;
            self.owners.insert(
                id,
                CollectedOwner {
                    legacy_upsert,
                    before,
                },
            );
        }
        Ok(())
    }
    fn upsert(&mut self, id: NodeId) -> CaptureResult<()> {
        self.owner(id, true, OwnerBeforeV1::NotRecorded)
    }
    fn delete(&mut self, id: String) -> CaptureResult<()> {
        if !self.deletes.contains(&id) {
            self.budget.bytes(96usize.saturating_add(id.len()))?;
            self.deletes.insert(id);
        }
        Ok(())
    }
    fn node(&mut self, id: NodeId, with_id: bool) -> CaptureResult<Option<NodeMetadata>> {
        self.budget.work()?;
        let properties = if with_id {
            BTreeSet::from(["id".into()])
        } else {
            BTreeSet::new()
        };
        let mut refused = false;
        let result = self
            .store
            .projected_node_owned_admitted(id, &properties, &mut |bytes| {
                if bytes > MAX_PREIMAGE_BYTES || self.budget.bytes(bytes).is_err() {
                    refused = true;
                    return Err(HawDBError::Execution(
                        "projection capture preimage budget exceeded".into(),
                    ));
                }
                Ok(())
            });
        if refused {
            return Err(CaptureError::Limit);
        }
        let Some(node) = result? else {
            return Ok(None);
        };
        if node
            .labels
            .iter()
            .any(|label| self.catalog.label_name(*label).is_none())
        {
            return Err(HawDBError::StorageIntegrity(
                "projection preimage refers to an unknown label".into(),
            )
            .into());
        }
        let label = self
            .catalog
            .label_id("Label")
            .is_some_and(|label| node.labels.contains(&label));
        let before = if with_id {
            // Admit the additional owned copy before allocating it.
            self.budget
                .bytes(node.labels.len().saturating_mul(4).saturating_add(
                    match node.properties.get("id") {
                        Some(Value::String(id)) => id.len(),
                        _ => 0,
                    },
                ))?;
            OwnerBeforeV1::Present {
                labels: node.labels.iter().map(|label| label.0).collect(),
                id: match node.properties.get("id") {
                    None => OwnerIdV1::Absent,
                    Some(Value::String(value)) => OwnerIdV1::String(value.clone()),
                    Some(_) => OwnerIdV1::Unsupported,
                },
            }
        } else {
            OwnerBeforeV1::NotRecorded
        };
        let node = NodeRecord {
            id: node.id,
            labels: node.labels,
            properties: node.properties,
        };
        let document_id = search_projection_document_id_for_node(self.catalog, &node);
        Ok(Some(NodeMetadata {
            label,
            projected: document_id.is_some(),
            document_id: if with_id { document_id } else { None },
            before,
        }))
    }
    fn endpoint(&mut self, id: NodeId) -> CaptureResult<Option<NodeMetadata>> {
        if let Some(label) = self.created_nodes.get(&id) {
            return Ok(Some(NodeMetadata {
                label: *label == "Label",
                projected: projection_document_id_for_label_and_properties(
                    label,
                    &BTreeMap::new(),
                    id,
                )
                .is_some(),
                document_id: None,
                before: OwnerBeforeV1::Absent,
            }));
        }
        self.node(id, false)
    }
    fn endpoints(&mut self, source: NodeId, target: NodeId) -> CaptureResult<()> {
        let source_meta = self.endpoint(source)?;
        let target_meta = self.endpoint(target)?;
        if source_meta.as_ref().is_some_and(|node| node.label)
            && target_meta.as_ref().is_some_and(|node| node.projected)
        {
            self.upsert(target)?;
        }
        if target_meta.as_ref().is_some_and(|node| node.label)
            && source_meta.as_ref().is_some_and(|node| node.projected)
        {
            self.upsert(source)?;
        }
        Ok(())
    }
    fn neighbor(&mut self, id: NodeId) -> CaptureResult<()> {
        if self.endpoint(id)?.is_some_and(|node| node.projected) {
            self.upsert(id)?;
        }
        Ok(())
    }
    fn label_neighbors(&mut self, id: NodeId) -> CaptureResult<()> {
        if !self.endpoint(id)?.is_some_and(|node| node.label) {
            return Ok(());
        }
        let Some(kind) = self.catalog.rel_type_id("HAS_LABEL") else {
            return Ok(());
        };
        if self.store.canonical_base.is_some() && self.store.canonical_adjacency.is_none() {
            return Err(CaptureError::Limit);
        }
        for direction in [AdjacencyDirection::Outgoing, AdjacencyDirection::Incoming] {
            let map = match direction {
                AdjacencyDirection::Outgoing => &self.store.outgoing,
                AdjacencyDirection::Incoming => &self.store.incoming,
            };
            if let Some(entries) = map.get(&(id, kind)) {
                for entry in entries.iter() {
                    self.budget.work()?;
                    self.neighbor(entry.neighbor_id)?;
                }
            }
            if let Some(adjacency) = &self.store.canonical_adjacency {
                let mut callback_error = None;
                let result = adjacency
                    .scan_endpoint_inputs_control(id, direction, Some(kind), |entry| {
                        let result = self.budget.work().and_then(|()| {
                            if self
                                .store
                                .relationship_tombstones
                                .contains(&entry.relationship_id)
                                || self
                                    .store
                                    .relationships
                                    .contains_key(&entry.relationship_id)
                            {
                                return Ok(());
                            }
                            self.neighbor(entry.neighbor_id)
                        });
                        match result {
                            Ok(()) => Ok(CanonicalScanControl::Continue),
                            Err(error) => {
                                callback_error = Some(error);
                                Ok(CanonicalScanControl::Stop)
                            }
                        }
                    })
                    .map_err(|error| HawDBError::StorageIntegrity(error.to_string()));
                result?;
                if let Some(error) = callback_error {
                    return Err(error);
                }
            }
        }
        Ok(())
    }
    fn endpoint_owners(&mut self, source: NodeId, target: NodeId) -> CaptureResult<()> {
        for id in [source, target] {
            self.budget.work()?;
            let before = if self.created_nodes.contains_key(&id) {
                OwnerBeforeV1::Absent
            } else {
                OwnerBeforeV1::NotRecorded
            };
            // Native owner enumeration is independent of legacy Search label
            // eligibility and never copies an endpoint's property body.
            self.owner(id, false, before)?;
        }
        Ok(())
    }
    fn relationship(&mut self, id: RelId, legacy: bool) -> CaptureResult<()> {
        self.budget.work()?;
        if let Some((source, target, kind)) = self.created_relationships.get(&id).copied() {
            self.endpoint_owners(source, target)?;
            if legacy && kind == "HAS_LABEL" {
                self.endpoints(source, target)?;
            }
            return Ok(());
        }
        if self.store.relationship_tombstones.contains(&id) {
            return Err(CaptureError::Limit);
        }
        let identity = if let Some(rel) = self.store.relationships.get(&id) {
            Some((rel.source, rel.target, rel.rel_type))
        } else if let Some(reader) = &self.store.canonical_base {
            reader
                .relationship_identity(id)
                .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))?
        } else {
            None
        };
        let Some((source, target, kind)) = identity else {
            return Err(CaptureError::Limit);
        };
        let name = self.catalog.rel_type_name(kind).ok_or_else(|| {
            HawDBError::StorageIntegrity(
                "projection relationship type is absent from catalog".into(),
            )
        })?;
        self.endpoint_owners(source, target)?;
        if legacy && name == "HAS_LABEL" {
            self.endpoints(source, target)?;
        }
        Ok(())
    }
    fn collect(&mut self, ops: &[WalOp], depth: usize) -> CaptureResult<()> {
        if depth > MAX_DEPTH {
            return Err(CaptureError::Limit);
        }
        for op in ops {
            self.budget.work()?;
            match op {
                WalOp::CreateNode { id, .. } => self.owner(*id, true, OwnerBeforeV1::Absent)?,
                WalOp::SetNodeProperty { id, property, .. } => {
                    self.upsert(*id)?;
                    if property == "id" {
                        let meta = self.node(*id, true)?;
                        let (before, old) = meta.map_or((OwnerBeforeV1::Absent, None), |meta| {
                            (meta.before, meta.document_id)
                        });
                        self.owner(*id, false, before)?;
                        if let Some(old) = old {
                            self.delete(old)?;
                        }
                    }
                    if matches!(property.as_str(), "id" | "name" | "canonical_name") {
                        self.label_neighbors(*id)?;
                    }
                }
                WalOp::DeleteNode { id } => {
                    let meta = self.node(*id, true)?;
                    let (before, old) = meta.map_or((OwnerBeforeV1::Absent, None), |meta| {
                        (meta.before, meta.document_id)
                    });
                    self.owner(*id, false, before)?;
                    if let Some(old) = old {
                        self.delete(old)?;
                    }
                    self.label_neighbors(*id)?;
                }
                WalOp::CreateRelationship {
                    source,
                    target,
                    rel_type,
                    ..
                } => {
                    self.endpoint_owners(*source, *target)?;
                    if rel_type == "HAS_LABEL" {
                        self.endpoints(*source, *target)?;
                    }
                }
                WalOp::SetRelationshipProperty { id, .. } => self.relationship(*id, false)?,
                WalOp::DeleteRelationship { id } => self.relationship(*id, true)?,
                WalOp::Batch(ops) => self.collect(ops, depth + 1)?,
                _ => {}
            }
        }
        Ok(())
    }
}

impl GraphStore {
    pub(super) fn prepare_search_projection_changes_for_ops(
        &self,
        catalog: &Catalog,
        commit_epoch: u64,
        ops: &[WalOp],
        relational: Option<hawdb_storage::relational::RelationalPrimaryKeyChangeCapture>,
    ) -> Result<PreparedSearchProjectionChange> {
        self.prepare_search_projection_changes_for_ops_with_work_context(
            catalog,
            commit_epoch,
            ops,
            relational,
            None,
        )
    }

    pub(in crate::store) fn prepare_search_projection_changes_for_ops_with_work_context(
        &self,
        catalog: &Catalog,
        commit_epoch: u64,
        ops: &[WalOp],
        relational: Option<hawdb_storage::relational::RelationalPrimaryKeyChangeCapture>,
        work: Option<&crate::background::CheckpointWorkContext>,
    ) -> Result<PreparedSearchProjectionChange> {
        if let Some(work) = work {
            work.checkpoint().map_err(HawDBError::from_storage_error)?;
        }
        if projection_wal_requires_rebuild(ops) {
            return Ok(PreparedSearchProjectionChange::Gap(commit_epoch));
        }
        let mut collector = Collector {
            store: self,
            catalog,
            budget: Budget {
                checkpoint: work.cloned(),
                work: MAX_WORK,
                bytes: MAX_BYTES,
            },
            created_nodes: BTreeMap::new(),
            created_relationships: BTreeMap::new(),
            owners: BTreeMap::new(),
            deletes: BTreeSet::new(),
        };
        let result = collector
            .created(ops, 0)
            .and_then(|()| collector.collect(ops, 0));
        match result {
            Err(CaptureError::Limit) => {
                return Ok(PreparedSearchProjectionChange::Gap(commit_epoch))
            }
            Err(CaptureError::Storage(error)) => {
                let result = Err(error);
                self.poison_on_storage_error(&result);
                return result;
            }
            Ok(()) => {}
        }
        let relational = relational.unwrap_or_else(|| {
            use crate::relational::{
                RelationalPrimaryKeyChangeCapture as Capture,
                RelationalPrimaryKeyChangeRebuildReason as Reason,
            };
            let reason = if wal_ops_contain_relational_snapshot(ops) {
                Some(Reason::SnapshotReplacement)
            } else if wal_ops_contain_relational_transaction(ops) {
                Some(Reason::MissingWalCapture)
            } else {
                None
            };
            reason.map_or(
                Capture::Captured {
                    tables: Vec::new(),
                    encoded_bytes: 0,
                },
                |reason| Capture::RequiresRebuild { reason },
            )
        });
        let relational = omit_internal_search_projection_relational_changes(relational);
        if collector.owners.is_empty()
            && collector.deletes.is_empty()
            && relational.operation_count() == 0
            && !relational.requires_rebuild()
        {
            return Ok(PreparedSearchProjectionChange::Complete(None));
        }
        // Collection storage and output vectors are both charged before WAL.
        if collector
            .budget
            .bytes(
                collector
                    .owners
                    .len()
                    .saturating_mul(8 + std::mem::size_of::<OwnerEffectV1>()),
            )
            .is_err()
        {
            return Ok(PreparedSearchProjectionChange::Gap(commit_epoch));
        }
        let mut upsert_node_ids = Vec::with_capacity(collector.owners.len());
        let mut owners = Vec::with_capacity(collector.owners.len());
        for (id, owner) in collector.owners {
            if owner.legacy_upsert {
                upsert_node_ids.push(id.0);
            }
            owners.push(OwnerEffectV1 {
                node_id: id.0,
                before: owner.before,
            });
        }
        Ok(PreparedSearchProjectionChange::Complete(Some(
            CapturedProjectionChange {
                legacy: Arc::new(SearchProjectionGraphChange {
                    commit_epoch,
                    upsert_node_ids,
                    delete_document_ids: collector.deletes.into_iter().collect(),
                    relational_primary_key_changes: relational,
                }),
                owners_v1: Some(owners),
            },
        )))
    }
    pub(super) fn install_search_projection_change(
        &mut self,
        change: PreparedSearchProjectionChange,
    ) {
        match change {
            PreparedSearchProjectionChange::Complete(Some(change)) => {
                self.search_projection_change_log_retained_bytes = self
                    .search_projection_change_log_retained_bytes
                    .saturating_add(change.estimated_retained_bytes());
                self.search_projection_graph_changes.push(Arc::new(change));
                self.trim_search_projection_graph_change_log();
            }
            PreparedSearchProjectionChange::Complete(None) => {}
            PreparedSearchProjectionChange::Gap(epoch) => {
                self.search_projection_change_log_start_epoch =
                    self.search_projection_change_log_start_epoch.max(epoch);
                self.search_projection_graph_changes =
                    SearchProjectionChangeLog::at_owner_floor(epoch);
                self.search_projection_change_log_retained_bytes = 0;
            }
        }
    }
    pub(super) fn append_graph_wal_and_capture(
        &mut self,
        catalog: &Catalog,
        ops: &[WalOp],
        single: bool,
    ) -> Result<()> {
        let change = self.prepare_search_projection_changes_for_ops(
            catalog,
            self.commit_epoch + 1,
            ops,
            None,
        )?;
        if single {
            self.append_durable_wal_single(ops[0].clone())?;
        } else {
            self.append_durable_wal_batch(ops)?;
        }
        self.install_search_projection_change(change);
        Ok(())
    }
    pub(super) fn record_search_projection_changes_for_ops(
        &mut self,
        catalog: &Catalog,
        epoch: u64,
        ops: &[WalOp],
        relational: Option<hawdb_storage::relational::RelationalPrimaryKeyChangeCapture>,
    ) -> Result<()> {
        let change =
            self.prepare_search_projection_changes_for_ops(catalog, epoch, ops, relational)?;
        self.install_search_projection_change(change);
        Ok(())
    }
    pub(super) fn relational_primary_key_changes_from_wal_ops(
        &self,
        ops: &[WalOp],
    ) -> Result<Option<hawdb_storage::relational::RelationalPrimaryKeyChangeCapture>> {
        let mut captures = Vec::new();
        collect_relational_primary_key_changes_from_wal_ops(ops, &mut captures)?;
        if captures.len() > 1 {
            return Ok(Some(
                hawdb_storage::relational::RelationalPrimaryKeyChangeCapture::RequiresRebuild {
                    reason: hawdb_storage::relational::RelationalPrimaryKeyChangeRebuildReason::MultipleRelationalTransactions,
                },
            ));
        }
        Ok(captures.pop())
    }

    pub(super) fn trim_search_projection_graph_change_log(&mut self) {
        let mut retained_bytes = self.search_projection_change_log_retained_bytes;
        let mut remove_count = 0usize;
        let mut last_removed_epoch = None;
        for change in self.search_projection_graph_changes.iter() {
            let exceeds_limit =
                self.max_search_projection_change_log_entries
                    .is_some_and(|limit| {
                        self.search_projection_graph_changes
                            .len()
                            .saturating_sub(remove_count)
                            > limit
                    })
                    || self
                        .max_search_projection_change_log_bytes
                        .is_some_and(|limit| retained_bytes > limit);
            if !exceeds_limit {
                break;
            }
            retained_bytes = retained_bytes.saturating_sub(change.estimated_retained_bytes());
            remove_count = remove_count.saturating_add(1);
            last_removed_epoch = Some(change.commit_epoch);
        }
        if let Some(epoch) = last_removed_epoch {
            self.search_projection_change_log_start_epoch =
                self.search_projection_change_log_start_epoch.max(epoch);
            self.search_projection_graph_changes
                .discard_prefix(remove_count);
        }
        self.search_projection_change_log_retained_bytes = retained_bytes;
    }
}
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

fn omit_internal_search_projection_relational_changes(
    capture: hawdb_storage::relational::RelationalPrimaryKeyChangeCapture,
) -> hawdb_storage::relational::RelationalPrimaryKeyChangeCapture {
    let hawdb_storage::relational::RelationalPrimaryKeyChangeCapture::Captured {
        mut tables, ..
    } = capture
    else {
        return capture;
    };
    tables.retain(|table| table.table != "hawdb_schema_migrations");
    let encoded_bytes = tables.iter().fold(0usize, |total, table| {
        let table_bytes = 4usize.saturating_add(table.table.len());
        table.primary_keys.iter().fold(
            total.saturating_add(table_bytes),
            |table_total, primary_key| {
                let key_bytes =
                    hawdb_storage::relational::encode_relational_primary_key(primary_key)
                        .map_or(0, |encoded| encoded.len());
                table_total.saturating_add(4).saturating_add(key_bytes)
            },
        )
    });
    hawdb_storage::relational::RelationalPrimaryKeyChangeCapture::Captured {
        tables,
        encoded_bytes,
    }
}

fn collect_relational_primary_key_changes_from_wal_ops(
    ops: &[WalOp],
    captures: &mut Vec<hawdb_storage::relational::RelationalPrimaryKeyChangeCapture>,
) -> Result<()> {
    for op in ops {
        match op {
            WalOp::Relational { record } => {
                let batch = decode_relational_wal_batch(record, RelationalDecodeLimits::wal())
                    .map_err(crate::relational::RelationalError::into_wal_recovery_error)?;
                captures.push(batch.primary_key_changes.unwrap_or(
                    hawdb_storage::relational::RelationalPrimaryKeyChangeCapture::RequiresRebuild {
                        reason: hawdb_storage::relational::RelationalPrimaryKeyChangeRebuildReason::MissingWalCapture,
                    },
                ));
            }
            WalOp::RelationalSnapshot { .. } => captures.push(
                hawdb_storage::relational::RelationalPrimaryKeyChangeCapture::RequiresRebuild {
                    reason:
                        hawdb_storage::relational::RelationalPrimaryKeyChangeRebuildReason::SnapshotReplacement,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    pub(super) fn directory() -> std::path::PathBuf {
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir().join(format!(
            "hawdb-projection-capture-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[test]
    fn wal_errors_never_install_prepared_changes_or_resume_gaps() {
        for gap in [false, true] {
            for failure in [
                WalAppendFailure::PartialWrite,
                WalAppendFailure::Rollback,
                WalAppendFailure::Sync,
            ] {
                let path = directory();
                let mut catalog = Catalog::default();
                let mut store = GraphStore::open(&path, &mut catalog).unwrap();
                let id = store
                    .create_node(
                        &mut catalog,
                        "Memory",
                        BTreeMap::from([(
                            "id".into(),
                            Value::String(if gap {
                                "a".repeat(MAX_PREIMAGE_BYTES)
                            } else {
                                "a".into()
                            }),
                        )]),
                    )
                    .unwrap();
                let cursor = store.commit_epoch();
                let before = store.search_projection_changes_after(0);
                let floor = store.search_projection_change_log_start_epoch();
                let owner_before = store
                    .search_projection_graph_changes
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>();
                let owner_floor = store.search_projection_graph_changes.owner_floor();
                let op = WalOp::SetNodeProperty {
                    id,
                    property: "id".into(),
                    value: Value::String("new".into()),
                };
                set_wal_append_failpoint(failure);
                assert!(store
                    .append_graph_wal_and_capture(&catalog, std::slice::from_ref(&op), false)
                    .is_err());
                assert_eq!(store.commit_epoch(), cursor);
                assert_eq!(store.search_projection_changes_after(0), before);
                assert_eq!(store.search_projection_change_log_start_epoch(), floor);
                assert_eq!(
                    store.search_projection_graph_changes.owner_floor(),
                    owner_floor
                );
                assert_eq!(
                    store
                        .search_projection_graph_changes
                        .iter()
                        .cloned()
                        .collect::<Vec<_>>(),
                    owner_before
                );
                drop(store);
                // An uncertain failure can recover a complete operation. It
                // must recover its capture or a gap, never a silent empty prefix.
                let mut catalog = Catalog::default();
                let recovered = match GraphStore::open(&path, &mut catalog) {
                    Ok(store) => store,
                    Err(error) => {
                        assert!(failure == WalAppendFailure::Rollback);
                        assert!(error
                            .to_string()
                            .contains("strict WAL recovery rejected torn tail"));
                        assert!(std::fs::read_dir(&path).unwrap().next().is_some());
                        std::fs::remove_dir_all(path).unwrap();
                        continue;
                    }
                };
                let changed = recovered.node_owned(id).unwrap().unwrap().properties["id"]
                    == Value::String("new".into());
                if changed {
                    assert!(recovered.commit_epoch() > cursor);
                    if gap {
                        assert!(recovered.search_projection_change_log_start_epoch() > cursor);
                    } else {
                        let changes = recovered.search_projection_changes_after(cursor);
                        assert_eq!(changes.len(), 1);
                        assert_eq!(changes[0].upsert_node_ids, [id.0]);
                        assert_eq!(changes[0].delete_document_ids, ["memory:a"]);
                    }
                } else {
                    assert_eq!(recovered.commit_epoch(), cursor);
                }
                drop(recovered);
                std::fs::remove_dir_all(path).unwrap();
            }
        }
    }

    #[test]
    fn uncertain_preimage_fails_before_wal_and_poison_is_shared() {
        let path = directory();
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        let id = store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([("id".into(), Value::String("a".into()))]),
            )
            .unwrap();
        let snapshot = store.snapshot();
        let mut broken = store.nodes.get(&id).unwrap().clone();
        broken.labels.insert(LabelId(u32::MAX));
        store.nodes.insert(id, broken);
        let lsn = store.durable.as_ref().unwrap().next_lsn;
        let epoch = store.commit_epoch();
        let before = store.search_projection_changes_after(0);
        let op = WalOp::SetNodeProperty {
            id,
            property: "id".into(),
            value: Value::String("new".into()),
        };
        let error = store
            .append_graph_wal_and_capture(&catalog, &[op], false)
            .unwrap_err();
        assert!(matches!(error, HawDBError::StorageIntegrity(_)));
        assert_eq!(store.durable.as_ref().unwrap().next_lsn, lsn);
        assert_eq!(store.commit_epoch(), epoch);
        assert_eq!(store.search_projection_changes_after(0), before);
        assert!(store.ensure_usable().is_err());
        assert!(snapshot.ensure_usable().is_err());
        drop(snapshot);
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[cfg(test)]
#[path = "search_projection_capture/closure_tests.rs"]
mod closure_tests;

#[cfg(test)]
#[path = "search_projection_capture/owner_tests.rs"]
mod owner_tests;
