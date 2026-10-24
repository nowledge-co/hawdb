// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Exhaustive pinned baseline input, separate from retained change history.
use super::*;
use crate::canonical::{CanonicalLabelAction, CanonicalNodeLabelCursor};
use crate::relational::{
    RelationalHydrationBudget, RelationalKey, RelationalRowPageProjectedRange,
    RelationalRowPageProjectedRangeFields, RelationalRowPageSnapshotReadError,
    RelationalRowPageSnapshotReadLimits, RelationalRowPageSnapshotReader,
};
use std::num::NonZeroUsize;
use std::ops::Bound;

#[cfg(test)]
#[path = "projection_full_source/tests.rs"]
mod tests;

#[derive(Debug, Clone, Copy)]
pub struct ProjectionFullSourceLimits {
    pub max_tables: NonZeroUsize,
    /// Estimated cursor, table-name directory and two retained key slots.
    /// Existing snapshot/cache/row-reader ownership has its separate admission.
    pub metadata_bytes: NonZeroUsize,
    pub key_bytes: NonZeroUsize,
    pub row_reads: RelationalRowPageSnapshotReadLimits,
}
impl Default for ProjectionFullSourceLimits {
    fn default() -> Self {
        Self {
            max_tables: NonZeroUsize::new(1024).unwrap(),
            metadata_bytes: NonZeroUsize::new(256 * 1024).unwrap(),
            key_bytes: NonZeroUsize::new(4096).unwrap(),
            row_reads: RelationalRowPageSnapshotReadLimits::default(),
        }
    }
}

/// Graph candidates include allocation holes and deleted IDs. Hydration against
/// the same snapshot determines presence; every attempted ID consumes admission.
/// Every canonical relational table is announced, including empty tables.
#[derive(Debug, Clone, Copy)]
pub enum ProjectionFullSourceItem<'a> {
    GraphNodeCandidate {
        node_id: u64,
    },
    RelationalTable {
        table: &'a str,
    },
    RelationalKey {
        table: &'a str,
        key: &'a RelationalKey,
    },
    End {
        source_epoch: u64,
    },
}

/// Owns an immutable source/history pin and private resume state. Completion
/// proves enumeration only, never the correctness or coverage of staged output.
#[derive(Debug)]
pub struct ProjectionFullSourceCursor {
    source: Box<GraphStore>,
    rows: Option<RelationalRowPageSnapshotReader>,
    tables: Vec<String>,
    labels: Option<Vec<LabelId>>,
    label_base: Option<CanonicalNodeLabelCursor>,
    label_delta_after: Option<NodeId>,
    label_nodes_complete: bool,
    node: u64,
    table: usize,
    announced: bool,
    last: Option<RelationalKey>,
    pending: Option<(RelationalKey, usize)>,
    table_rows: usize,
    ended: bool,
    failed: bool,
    total: ProjectionFeedLimits,
    limits: ProjectionFullSourceLimits,
    used: ProjectionFeedUsage,
    task: RuntimeTaskContext,
}

impl GraphStore {
    #[doc(hidden)]
    pub fn capture_full_projection_source(
        &self,
        total: ProjectionFeedLimits,
        limits: ProjectionFullSourceLimits,
        task: RuntimeTaskContext,
    ) -> std::result::Result<ProjectionFullSourceCursor, ProjectionFeedError> {
        self.capture_projection_source_domains(total, limits, task, None, None)
    }

    /// Enumerate exactly the declared domains. Missing labels/tables describe
    /// empty domains; the facade must retain their absence dependencies.
    #[doc(hidden)]
    pub fn capture_projection_source_domains(
        &self,
        total: ProjectionFeedLimits,
        limits: ProjectionFullSourceLimits,
        task: RuntimeTaskContext,
        mut labels: Option<Vec<LabelId>>,
        mut tables: Option<Vec<String>>,
    ) -> std::result::Result<ProjectionFullSourceCursor, ProjectionFeedError> {
        self.ensure_usable().map_err(ProjectionFeedError::Source)?;
        task.checkpoint()
            .map_err(|e| ProjectionFeedError::Source(HawDBError::Execution(e.to_string())))?;
        let mut bytes = std::mem::size_of::<ProjectionFullSourceCursor>()
            .saturating_add(std::mem::size_of::<GraphStore>())
            .saturating_add(limits.key_bytes.get().saturating_mul(2))
            .saturating_add(labels.as_ref().map_or(0, |ids| {
                ids.capacity()
                    .saturating_mul(std::mem::size_of::<LabelId>())
            }));
        if let Some(names) = &mut tables {
            if names.len() > limits.max_tables.get() {
                return Err(ProjectionFeedError::BudgetExceeded(
                    ProjectionFeedResource::Bytes,
                ));
            }
            names.sort();
            names.dedup();
        }
        // Named scopes perform bounded point lookups. Scanning the complete
        // schema directory here would make unrelated tables uncharged work.
        let names: Box<dyn Iterator<Item = &str> + '_> = match &tables {
            Some(names) => Box::new(names.iter().map(String::as_str)),
            None => Box::new(
                self.relational_state
                    .table_schemas()
                    .map(|schema| schema.name.as_str()),
            ),
        };
        let mut captured_tables = Vec::new();
        for name in names {
            task.checkpoint().map_err(|error| {
                ProjectionFeedError::Source(HawDBError::Execution(error.to_string()))
            })?;
            let Some(schema) = self.relational_state.table_schema(name) else {
                continue;
            };
            bytes = bytes
                .saturating_add(name.len())
                .saturating_add(std::mem::size_of::<String>());
            if captured_tables.len() >= limits.max_tables.get()
                || bytes > limits.metadata_bytes.get()
            {
                return Err(ProjectionFeedError::BudgetExceeded(
                    ProjectionFeedResource::Bytes,
                ));
            }
            if schema.primary_key.is_empty() || schema.primary_key.len() > 32 {
                return Err(ProjectionFeedError::Source(HawDBError::Execution(
                    "full projection source requires bounded canonical primary keys".into(),
                )));
            }
            captured_tables.push(name.to_owned());
        }
        if bytes > limits.metadata_bytes.get() {
            return Err(ProjectionFeedError::BudgetExceeded(
                ProjectionFeedResource::Bytes,
            ));
        }
        let rows = if captured_tables.is_empty() {
            None
        } else if self.relational_state.canonical_row_metadata_only() {
            Some(
                self.open_relational_row_snapshot_reader()
                    .map_err(ProjectionFeedError::Source)?
                    .ok_or_else(|| {
                        ProjectionFeedError::Source(HawDBError::StorageIntegrity(
                            "full projection source has no canonical row reader".into(),
                        ))
                    })?,
            )
        } else {
            self.relational_state
                .require_materialized_rows("full projection source")
                .map_err(|error| {
                    ProjectionFeedError::Source(HawDBError::Storage(error.to_string()))
                })?;
            None
        };
        if let Some(labels) = &mut labels {
            labels.sort_unstable();
            labels.dedup();
        }
        let label_nodes_complete = labels.as_ref().is_some_and(Vec::is_empty);
        let source = Box::new(self.snapshot_for_read());
        let label_base =
            if labels.is_some() && !label_nodes_complete && source.canonical_base_out_of_core {
                source
                    .canonical_base
                    .clone()
                    .map(CanonicalNodeLabelCursor::new)
            } else {
                None
            };
        let node = if labels.is_some() {
            self.next_node_id
        } else {
            0
        };
        Ok(ProjectionFullSourceCursor {
            source,
            rows,
            tables: captured_tables,
            labels,
            label_base,
            label_delta_after: None,
            label_nodes_complete,
            node,
            table: 0,
            announced: false,
            last: None,
            pending: None,
            table_rows: 0,
            ended: false,
            failed: false,
            total,
            limits,
            used: ProjectionFeedUsage::default(),
            task,
        })
    }
}

fn key_bytes(key: &RelationalKey, limit: usize) -> std::result::Result<usize, ProjectionFeedError> {
    let mut bytes = std::mem::size_of::<RelationalKey>();
    if key.0.len() > 32 {
        return Err(ProjectionFeedError::BudgetExceeded(
            ProjectionFeedResource::Bytes,
        ));
    }
    for value in &key.0 {
        bytes = bytes
            .saturating_add(std::mem::size_of_val(value))
            .saturating_add(value.estimated_payload_bytes());
        if bytes > limit {
            return Err(ProjectionFeedError::BudgetExceeded(
                ProjectionFeedResource::Bytes,
            ));
        }
    }
    Ok(bytes)
}

impl ProjectionFullSourceCursor {
    pub fn source_epoch(&self) -> u64 {
        self.source.commit_epoch
    }
    pub fn usage(&self) -> ProjectionFeedUsage {
        self.used
    }
    pub fn is_complete(&self) -> bool {
        self.ended && !self.failed
    }

    pub fn finish(self) -> std::result::Result<ProjectionFeedUsage, ProjectionFeedError> {
        if self.failed {
            return Err(ProjectionFeedError::Failed);
        }
        if !self.ended {
            return Err(ProjectionFeedError::Incomplete);
        }
        self.source
            .ensure_usable()
            .map_err(ProjectionFeedError::Source)?;
        self.task
            .checkpoint()
            .map_err(|e| ProjectionFeedError::Source(HawDBError::Execution(e.to_string())))?;
        Ok(self.used)
    }

    fn integrity(&self, message: &str) -> ProjectionFeedError {
        let result = Err::<(), _>(HawDBError::StorageIntegrity(message.into()));
        self.source.poison_on_storage_error(&result);
        let Err(error) = result else {
            unreachable!("integrity result is always an error")
        };
        ProjectionFeedError::Source(error)
    }

    fn charge(
        &mut self,
        step: ProjectionFeedLimits,
        now: &mut ProjectionFeedUsage,
        work: usize,
        items: usize,
        bytes: usize,
    ) -> std::result::Result<bool, ProjectionFeedError> {
        let amounts = [
            (
                ProjectionFeedResource::Work,
                work,
                self.used.work,
                self.total.work.get(),
                now.work,
                step.work.get(),
            ),
            (
                ProjectionFeedResource::Items,
                items,
                self.used.items,
                self.total.items.get(),
                now.items,
                step.items.get(),
            ),
            (
                ProjectionFeedResource::Bytes,
                bytes,
                self.used.bytes,
                self.total.bytes.get(),
                now.bytes,
                step.bytes.get(),
            ),
        ];
        for (resource, n, used, total, _, _) in amounts {
            if n > total.saturating_sub(used) {
                return Err(ProjectionFeedError::BudgetExceeded(resource));
            }
        }
        for (resource, n, _, _, used, cap) in amounts {
            if n > cap {
                return Err(ProjectionFeedError::StepTooSmall(resource));
            }
            if n > cap - used {
                return Ok(false);
            }
        }
        self.used.work += work;
        self.used.items += items;
        self.used.bytes += bytes;
        now.work += work;
        now.items += items;
        now.bytes += bytes;
        Ok(true)
    }

    fn next_key(&self) -> std::result::Result<Option<(RelationalKey, usize)>, ProjectionFeedError> {
        let table = &self.tables[self.table];
        if let Some(reader) = &self.rows {
            let mut found = None;
            let mut error = None;
            let result = reader.visit_projected_range_fields_resolving_ref(
                RelationalRowPageProjectedRangeFields {
                    range: RelationalRowPageProjectedRange {
                        table,
                        lower: self.last.as_ref().map_or(Bound::Unbounded, Bound::Excluded),
                        upper: Bound::Unbounded,
                        requested_fields: &[],
                    },
                    hydration_fields: &[],
                },
                self.limits.row_reads,
                &mut RelationalHydrationBudget::default(),
                &self.task,
                |_, _, _| Ok(()),
                |row, _| {
                    let key = row.primary_key();
                    match key_bytes(key, self.limits.key_bytes.get()) {
                        Ok(bytes) => found = Some((key.clone(), bytes)),
                        Err(e) => error = Some(e),
                    }
                    false
                },
            );
            let result = result.map_err(|e| match e {
                RelationalRowPageSnapshotReadError::Corrupt(_)
                | RelationalRowPageSnapshotReadError::MissingTable(_) => {
                    HawDBError::StorageIntegrity(e.to_string())
                }
                RelationalRowPageSnapshotReadError::Stopped(_) => {
                    HawDBError::Execution(e.to_string())
                }
                RelationalRowPageSnapshotReadError::FileDescriptors(e) => {
                    HawDBError::FileDescriptors(e)
                }
                _ => HawDBError::Storage(e.to_string()),
            });
            self.source.poison_on_storage_error(&result);
            result.map_err(ProjectionFeedError::Source)?;
            if let Some(e) = error {
                return Err(e);
            }
            Ok(found)
        } else {
            let key = self
                .source
                .relational_state
                .first_primary_key_after(table, self.last.as_ref())
                .map_err(|e| ProjectionFeedError::Source(HawDBError::Storage(e.to_string())))?;
            key.map(|key| {
                key_bytes(key, self.limits.key_bytes.get()).map(|bytes| (key.clone(), bytes))
            })
            .transpose()
        }
    }

    pub fn visit_next(
        &mut self,
        step: ProjectionFeedLimits,
        mut visitor: impl FnMut(ProjectionFullSourceItem<'_>) -> Result<()>,
    ) -> std::result::Result<ProjectionFeedStep, ProjectionFeedError> {
        if self.failed {
            return Err(ProjectionFeedError::Failed);
        }
        if self.ended {
            return Ok(ProjectionFeedStep {
                usage: ProjectionFeedUsage::default(),
                complete: true,
            });
        }
        self.failed = true;
        let result = self.advance(step, &mut visitor);
        if result.is_ok() {
            self.failed = false;
        }
        result
    }

    fn advance(
        &mut self,
        step: ProjectionFeedLimits,
        visitor: &mut impl FnMut(ProjectionFullSourceItem<'_>) -> Result<()>,
    ) -> std::result::Result<ProjectionFeedStep, ProjectionFeedError> {
        let mut used = ProjectionFeedUsage::default();
        loop {
            self.source
                .ensure_usable()
                .map_err(ProjectionFeedError::Source)?;
            self.task
                .checkpoint()
                .map_err(|e| ProjectionFeedError::Source(HawDBError::Execution(e.to_string())))?;
            if self.labels.is_some() && !self.label_nodes_complete {
                if !self.advance_labels(step, &mut used, visitor)? {
                    break;
                }
            } else if self.node < self.source.next_node_id {
                let work = self
                    .labels
                    .as_ref()
                    .map_or(1, |labels| labels.len().saturating_add(1));
                if !self.charge(step, &mut used, work, 1, 8)? {
                    break;
                }
                if self.includes_node(NodeId(self.node))? {
                    visitor(ProjectionFullSourceItem::GraphNodeCandidate { node_id: self.node })
                        .map_err(ProjectionFeedError::Callback)?;
                }
                self.node += 1;
            } else if self.table < self.tables.len() {
                if !self.announced {
                    let bytes = self.tables[self.table].len();
                    if !self.charge(step, &mut used, 1, 1, bytes)? {
                        break;
                    }
                    visitor(ProjectionFullSourceItem::RelationalTable {
                        table: &self.tables[self.table],
                    })
                    .map_err(ProjectionFeedError::Callback)?;
                    self.announced = true;
                } else if let Some((_, bytes)) = self.pending.as_ref() {
                    let bytes = bytes.saturating_add(self.tables[self.table].len());
                    if !self.charge(step, &mut used, 1, 1, bytes)? {
                        break;
                    }
                    let (key, _) = self.pending.take().expect("pending key");
                    visitor(ProjectionFullSourceItem::RelationalKey {
                        table: &self.tables[self.table],
                        key: &key,
                    })
                    .map_err(ProjectionFeedError::Callback)?;
                    self.last = Some(key);
                    self.table_rows += 1;
                } else {
                    let table = &self.tables[self.table];
                    let count = self.source.relational_state.row_count(table);
                    let columns = self
                        .source
                        .relational_state
                        .table_schema(table)
                        .expect("pinned schema")
                        .primary_key
                        .len();
                    let work = 1 + (usize::BITS - count.leading_zeros()) as usize + columns;
                    if !self.charge(step, &mut used, work, 0, 0)? {
                        break;
                    }
                    self.pending = self.next_key()?;
                    if let Some((key, _)) = &self.pending {
                        if self.last.as_ref().is_some_and(|last| key <= last) {
                            return Err(self.integrity("full source key order regressed"));
                        }
                    } else {
                        if self.table_rows != count {
                            return Err(self
                                .integrity("full source row count differs from pinned metadata"));
                        }
                        self.table += 1;
                        self.last = None;
                        self.announced = false;
                        self.table_rows = 0;
                    }
                }
            } else {
                if !self.charge(step, &mut used, 1, 1, 8)? {
                    break;
                }
                visitor(ProjectionFullSourceItem::End {
                    source_epoch: self.source_epoch(),
                })
                .map_err(ProjectionFeedError::Callback)?;
                self.source
                    .ensure_usable()
                    .map_err(ProjectionFeedError::Source)?;
                self.task.checkpoint().map_err(|e| {
                    ProjectionFeedError::Source(HawDBError::Execution(e.to_string()))
                })?;
                self.ended = true;
                break;
            }
        }
        Ok(ProjectionFeedStep {
            usage: used,
            complete: self.ended,
        })
    }

    fn includes_node(&self, id: NodeId) -> std::result::Result<bool, ProjectionFeedError> {
        let Some(labels) = &self.labels else {
            return Ok(true);
        };
        if self.source.node_tombstones.contains(&id) {
            return Ok(false);
        }
        if let Some(node) = self.source.nodes.get(&id) {
            return Ok(labels.iter().any(|label| node.labels.contains(label)));
        }
        let Some(reader) = &self.source.canonical_base else {
            return Ok(false);
        };
        let result = reader
            .with_node_input(id, |input| {
                for label in labels {
                    if input.has_label(*label)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            })
            .map(|present| present.unwrap_or(false))
            .map_err(canonical_segment_error);
        self.source.poison_on_storage_error(&result);
        result.map_err(ProjectionFeedError::Source)
    }

    /// Every descriptor, actual row and label comparison is charged before
    /// processing. Only real snapshot records are visited; allocation holes do
    /// not manufacture work. A base row replaced by the captured delta is
    /// ignored even if its old label would otherwise match.
    fn advance_labels(
        &mut self,
        step: ProjectionFeedLimits,
        used: &mut ProjectionFeedUsage,
        visitor: &mut impl FnMut(ProjectionFullSourceItem<'_>) -> Result<()>,
    ) -> std::result::Result<bool, ProjectionFeedError> {
        if let Some(base) = &mut self.label_base {
            let action = base.action().map_err(canonical_segment_error);
            self.source.poison_on_storage_error(&action);
            match action.map_err(ProjectionFeedError::Source)? {
                CanonicalLabelAction::Segment => {
                    let work = 1 + self.labels.as_ref().expect("label scope").len();
                    if !self.charge(step, used, work, 0, 0)? {
                        return Ok(false);
                    }
                    let result = self
                        .label_base
                        .as_mut()
                        .expect("base cursor")
                        .load_segment(self.labels.as_ref().expect("label scope"), &self.task)
                        .map_err(canonical_segment_error);
                    self.source.poison_on_storage_error(&result);
                    result.map_err(ProjectionFeedError::Source)?;
                    return Ok(true);
                }
                CanonicalLabelAction::Record { id, labels } => {
                    let work = self.label_membership_work(labels);
                    if !self.charge(
                        step,
                        used,
                        work,
                        1,
                        8usize.saturating_add(labels.saturating_mul(4)),
                    )? {
                        return Ok(false);
                    }
                    let selected = self
                        .label_base
                        .as_mut()
                        .expect("base cursor")
                        .accept_record(self.labels.as_ref().expect("label scope"))
                        .map_err(canonical_segment_error);
                    self.source.poison_on_storage_error(&selected);
                    if selected.map_err(ProjectionFeedError::Source)?
                        && !self.source.node_tombstones.contains(&id)
                        && !self.source.nodes.contains_key(&id)
                    {
                        visitor(ProjectionFullSourceItem::GraphNodeCandidate { node_id: id.0 })
                            .map_err(ProjectionFeedError::Callback)?;
                    }
                    return Ok(true);
                }
                CanonicalLabelAction::End => self.label_base = None,
            }
        }
        let next = self
            .source
            .nodes
            .first_after(self.label_delta_after.as_ref())
            .map(|(id, node)| (*id, node.labels.len()));
        let Some((id, labels)) = next else {
            self.label_nodes_complete = true;
            return Ok(true);
        };
        let work = self.label_membership_work(labels);
        if !self.charge(
            step,
            used,
            work,
            1,
            8usize.saturating_add(labels.saturating_mul(4)),
        )? {
            return Ok(false);
        }
        let selected = self
            .source
            .nodes
            .get(&id)
            .expect("captured node")
            .labels
            .iter()
            .any(|label| {
                self.labels
                    .as_ref()
                    .expect("label scope")
                    .binary_search(label)
                    .is_ok()
            });
        self.label_delta_after = Some(id);
        if selected && !self.source.node_tombstones.contains(&id) {
            visitor(ProjectionFullSourceItem::GraphNodeCandidate { node_id: id.0 })
                .map_err(ProjectionFeedError::Callback)?;
        }
        Ok(true)
    }

    fn label_membership_work(&self, labels: usize) -> usize {
        let selected = self.labels.as_ref().expect("label scope").len();
        let comparisons = 1 + (usize::BITS - selected.leading_zeros()) as usize;
        1usize.saturating_add(labels.saturating_mul(comparisons))
    }
}
