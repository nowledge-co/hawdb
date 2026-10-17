// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Borrowed input enumeration. Exhaustion is not projection coverage authority.

use super::SearchProjectionChangeLog;
use crate::relational::{RelationalKey, RelationalPrimaryKeyChangeCapture, RelationalValue};
use crate::{HawDBError, Result};
use std::num::NonZeroUsize;

#[path = "cursor/owner.rs"]
mod owner;
pub use owner::{
    ProjectionOwnerBefore, ProjectionOwnerFeedCursor, ProjectionOwnerFeedItem, ProjectionOwnerId,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FeedKind {
    Legacy,
    OwnerV1,
}
enum FeedInput<'a> {
    Legacy(ProjectionFeedItem<'a>),
    Owner {
        epoch: u64,
        effect: &'a crate::projection::owner_capture::OwnerEffectV1,
    },
}

/// Independent per-step or cumulative feed admission. Bytes measure borrowed
/// key input, not total allocator usage or retained snapshot memory.
#[derive(Debug, Clone, Copy)]
pub struct ProjectionFeedLimits {
    pub work: NonZeroUsize,
    pub items: NonZeroUsize,
    pub bytes: NonZeroUsize,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProjectionFeedUsage {
    pub work: usize,
    pub items: usize,
    pub bytes: usize,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionFeedResource {
    Work,
    Items,
    Bytes,
}
#[derive(Debug)]
pub enum ProjectionFeedError {
    BehindFloor { requested: u64, floor: u64 },
    FutureCursor { requested: u64, source: u64 },
    BudgetExceeded(ProjectionFeedResource),
    StepTooSmall(ProjectionFeedResource),
    RequiresRebuild,
    Failed,
    Incomplete,
    Source(HawDBError),
    Callback(HawDBError),
}
impl std::fmt::Display for ProjectionFeedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BehindFloor { requested, floor } => write!(f, "projection feed cursor {requested} is below resume floor {floor}; full rebuild required"),
            Self::FutureCursor { requested, source } => write!(f, "projection feed cursor {requested} exceeds pinned source {source}"),
            Self::BudgetExceeded(resource) => write!(f, "projection feed cumulative {resource:?} budget exceeded"),
            Self::StepTooSmall(resource) => write!(f, "projection feed step {resource:?} budget cannot admit one input"),
            Self::RequiresRebuild => f.write_str("projection feed input requires a full rebuild"),
            Self::Failed => f.write_str("projection feed is unusable after a failed step"),
            Self::Incomplete => f.write_str("projection feed has not reached its pinned end"),
            Self::Source(error) | Self::Callback(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for ProjectionFeedError {}

/// Input is borrowed only for the callback. End identifies the immutable source
/// boundary; it does not authorize any output or visible watermark.
#[derive(Debug, Clone, Copy)]
pub enum ProjectionFeedItem<'a> {
    GraphNode {
        epoch: u64,
        node_id: u64,
    },
    DeleteDocument {
        epoch: u64,
        document_id: &'a str,
    },
    RelationalKey {
        epoch: u64,
        table: &'a str,
        key: &'a RelationalKey,
    },
    End {
        source_epoch: u64,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectionFeedStep {
    pub usage: ProjectionFeedUsage,
    pub complete: bool,
}
#[derive(Debug, Clone, Copy)]
enum Part {
    Begin,
    Nodes(usize),
    Deletes(usize),
    Table(usize),
    Key { table: usize, key: usize },
    FinishCommit,
    Done,
}

/// Immutable paged records are shared, never deep-cloned. Private ordinals can
/// stop inside a transaction; no caller-supplied resume ordinal is accepted.
#[derive(Debug)]
pub struct ProjectionFeedCursor {
    log: SearchProjectionChangeLog,
    source_epoch: u64,
    start_epoch: u64,
    event: usize,
    part: Part,
    limits: ProjectionFeedLimits,
    usage: ProjectionFeedUsage,
    failed: bool,
    kind: FeedKind,
}
impl ProjectionFeedCursor {
    pub(in crate::store) fn capture(
        log: &SearchProjectionChangeLog,
        floor: u64,
        source_epoch: u64,
        start_epoch: u64,
        limits: ProjectionFeedLimits,
    ) -> std::result::Result<Self, ProjectionFeedError> {
        if start_epoch < floor {
            return Err(ProjectionFeedError::BehindFloor {
                requested: start_epoch,
                floor,
            });
        }
        if start_epoch > source_epoch {
            return Err(ProjectionFeedError::FutureCursor {
                requested: start_epoch,
                source: source_epoch,
            });
        }
        let mut usage = ProjectionFeedUsage::default();
        let (mut low, mut high) = (0, log.len());
        while low < high {
            if usage.work == limits.work.get() {
                return Err(ProjectionFeedError::BudgetExceeded(
                    ProjectionFeedResource::Work,
                ));
            }
            usage.work += 1;
            let middle = low + (high - low) / 2;
            if log.get(middle).expect("valid paged ordinal").commit_epoch <= start_epoch {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        Ok(Self {
            log: log.clone(),
            source_epoch,
            start_epoch,
            event: low,
            part: Part::Begin,
            limits,
            usage,
            failed: false,
            kind: FeedKind::Legacy,
        })
    }
    pub fn source_epoch(&self) -> u64 {
        self.source_epoch
    }
    pub fn start_epoch(&self) -> u64 {
        self.start_epoch
    }
    pub fn usage(&self) -> ProjectionFeedUsage {
        self.usage
    }
    pub fn is_complete(&self) -> bool {
        !self.failed && matches!(self.part, Part::Done)
    }

    /// Every measured key field consumes work, including a key deferred because
    /// it no longer fits the step's remaining bytes. There is no work refund.
    /// A callback error or panic permanently invalidates the cursor.
    pub fn visit_next(
        &mut self,
        step: ProjectionFeedLimits,
        mut visitor: impl FnMut(ProjectionFeedItem<'_>) -> Result<()>,
    ) -> std::result::Result<ProjectionFeedStep, ProjectionFeedError> {
        self.visit_input(step, |input| match input {
            FeedInput::Legacy(item) => visitor(item),
            FeedInput::Owner { .. } => unreachable!("legacy cursor cannot emit owner input"),
        })
    }
    fn visit_input(
        &mut self,
        step: ProjectionFeedLimits,
        mut visitor: impl FnMut(FeedInput<'_>) -> Result<()>,
    ) -> std::result::Result<ProjectionFeedStep, ProjectionFeedError> {
        if self.failed {
            return Err(ProjectionFeedError::Failed);
        }
        self.failed = true;
        let result = self.visit_inner(step, &mut visitor);
        if result.is_ok() {
            self.failed = false;
        }
        result
    }
    fn charge_work(
        &mut self,
        work: usize,
        used: &mut ProjectionFeedUsage,
        step: ProjectionFeedLimits,
    ) -> std::result::Result<bool, ProjectionFeedError> {
        if work > self.limits.work.get().saturating_sub(self.usage.work) {
            return Err(ProjectionFeedError::BudgetExceeded(
                ProjectionFeedResource::Work,
            ));
        }
        if work > step.work.get() {
            return Err(ProjectionFeedError::StepTooSmall(
                ProjectionFeedResource::Work,
            ));
        }
        if work > step.work.get() - used.work {
            return Ok(false);
        }
        self.usage.work += work;
        used.work += work;
        Ok(true)
    }
    fn charge_bytes(
        &mut self,
        bytes: usize,
        used: &mut ProjectionFeedUsage,
        step: ProjectionFeedLimits,
    ) -> std::result::Result<bool, ProjectionFeedError> {
        if bytes > self.limits.bytes.get().saturating_sub(self.usage.bytes) {
            return Err(ProjectionFeedError::BudgetExceeded(
                ProjectionFeedResource::Bytes,
            ));
        }
        if bytes > step.bytes.get() {
            return Err(ProjectionFeedError::StepTooSmall(
                ProjectionFeedResource::Bytes,
            ));
        }
        if bytes > step.bytes.get() - used.bytes {
            return Ok(false);
        }
        self.usage.bytes += bytes;
        used.bytes += bytes;
        Ok(true)
    }
    fn visit_inner(
        &mut self,
        step: ProjectionFeedLimits,
        visitor: &mut impl FnMut(FeedInput<'_>) -> Result<()>,
    ) -> std::result::Result<ProjectionFeedStep, ProjectionFeedError> {
        let mut used = ProjectionFeedUsage::default();
        loop {
            if matches!(self.part, Part::Done) {
                return Ok(ProjectionFeedStep {
                    usage: used,
                    complete: true,
                });
            }
            if used.work == step.work.get() || used.items == step.items.get() {
                return Ok(ProjectionFeedStep {
                    usage: used,
                    complete: false,
                });
            }
            // The log's writer and checkpoint loader enforce increasing epochs.
            // A captured directory cannot acquire later append/trim mutations.
            let Some(record) = self.log.get(self.event) else {
                if self.usage.items == self.limits.items.get() {
                    return Err(ProjectionFeedError::BudgetExceeded(
                        ProjectionFeedResource::Items,
                    ));
                }
                if !self.charge_work(1, &mut used, step)? {
                    continue;
                }
                self.usage.items += 1;
                used.items += 1;
                visitor(FeedInput::Legacy(ProjectionFeedItem::End {
                    source_epoch: self.source_epoch,
                }))
                .map_err(ProjectionFeedError::Callback)?;
                self.part = Part::Done;
                continue;
            };
            let epoch = record.commit_epoch;
            let tables = match &record.relational_primary_key_changes {
                RelationalPrimaryKeyChangeCapture::Captured { tables, .. } => Some(tables),
                RelationalPrimaryKeyChangeCapture::RequiresRebuild { .. } => None,
            };
            let requires_rebuild =
                tables.is_none() || (self.kind == FeedKind::OwnerV1 && record.owners_v1.is_none());
            let (work, bytes, emit, next) = match self.part {
                Part::Begin => (1, 0, false, Part::Nodes(0)),
                Part::Nodes(index)
                    if self.kind == FeedKind::OwnerV1
                        && record
                            .owners_v1
                            .as_ref()
                            .is_some_and(|owners| index < owners.len()) =>
                {
                    let (work, bytes) = owner::measure(&record.owners_v1.as_ref().unwrap()[index]);
                    (work, bytes, true, Part::Nodes(index + 1))
                }
                Part::Nodes(index)
                    if self.kind == FeedKind::Legacy && index < record.upsert_node_ids.len() =>
                {
                    (1, 8, true, Part::Nodes(index + 1))
                }
                Part::Nodes(_) => {
                    self.part = if self.kind == FeedKind::OwnerV1 {
                        Part::Table(0)
                    } else {
                        Part::Deletes(0)
                    };
                    continue;
                }
                Part::Deletes(index) if index < record.delete_document_ids.len() => (
                    1,
                    8usize.saturating_add(record.delete_document_ids[index].len()),
                    true,
                    Part::Deletes(index + 1),
                ),
                Part::Deletes(_) => {
                    self.part = Part::Table(0);
                    continue;
                }
                Part::Table(index) if tables.is_some_and(|tables| index < tables.len()) => (
                    1,
                    8usize.saturating_add(tables.unwrap()[index].table.len()),
                    false,
                    Part::Key {
                        table: index,
                        key: 0,
                    },
                ),
                Part::Table(_) => {
                    self.part = Part::FinishCommit;
                    continue;
                }
                Part::Key { table, key } if key < tables.unwrap()[table].primary_keys.len() => (
                    1usize.saturating_add(tables.unwrap()[table].primary_keys[key].0.len()),
                    0,
                    true,
                    Part::Key {
                        table,
                        key: key + 1,
                    },
                ),
                Part::Key { table, .. } => {
                    self.part = Part::Table(table + 1);
                    continue;
                }
                Part::FinishCommit => (1, 0, false, Part::Begin),
                Part::Done => unreachable!(),
            };
            if emit && self.usage.items == self.limits.items.get() {
                return Err(ProjectionFeedError::BudgetExceeded(
                    ProjectionFeedResource::Items,
                ));
            }
            if !self.charge_work(work, &mut used, step)? {
                return Ok(ProjectionFeedStep {
                    usage: used,
                    complete: false,
                });
            }
            if matches!(self.part, Part::Begin) && requires_rebuild {
                return Err(ProjectionFeedError::RequiresRebuild);
            }
            // Reborrow after mutable admission; no key payload has been owned.
            let record = self
                .log
                .get(self.event)
                .expect("current record remains pinned");
            let bytes = if let Part::Key { table, key } = self.part {
                let RelationalPrimaryKeyChangeCapture::Captured { tables, .. } =
                    &record.relational_primary_key_changes
                else {
                    unreachable!()
                };
                key_bytes(&tables[table].primary_keys[key])?
                    .saturating_add(8)
                    .saturating_add(tables[table].table.len())
            } else {
                bytes
            };
            if !self.charge_bytes(bytes, &mut used, step)? {
                return Ok(ProjectionFeedStep {
                    usage: used,
                    complete: false,
                });
            }
            if emit {
                let record = self
                    .log
                    .get(self.event)
                    .expect("current record remains pinned");
                let item = match self.part {
                    Part::Nodes(index) if self.kind == FeedKind::OwnerV1 => FeedInput::Owner {
                        epoch,
                        effect: &record.owners_v1.as_ref().expect("qualified owner record")[index],
                    },
                    Part::Nodes(index) => FeedInput::Legacy(ProjectionFeedItem::GraphNode {
                        epoch,
                        node_id: record.upsert_node_ids[index],
                    }),
                    Part::Deletes(index) => FeedInput::Legacy(ProjectionFeedItem::DeleteDocument {
                        epoch,
                        document_id: &record.delete_document_ids[index],
                    }),
                    Part::Key { table, key } => {
                        let RelationalPrimaryKeyChangeCapture::Captured { tables, .. } =
                            &record.relational_primary_key_changes
                        else {
                            unreachable!()
                        };
                        FeedInput::Legacy(ProjectionFeedItem::RelationalKey {
                            epoch,
                            table: &tables[table].table,
                            key: &tables[table].primary_keys[key],
                        })
                    }
                    _ => unreachable!(),
                };
                self.usage.items += 1;
                used.items += 1;
                visitor(item).map_err(ProjectionFeedError::Callback)?;
            }
            if matches!(self.part, Part::FinishCommit) {
                self.event += 1;
            }
            self.part = next;
        }
    }
}
fn key_bytes(key: &RelationalKey) -> std::result::Result<usize, ProjectionFeedError> {
    let mut bytes = key
        .0
        .len()
        .saturating_mul(std::mem::size_of::<RelationalValue>());
    for value in &key.0 {
        bytes = bytes.saturating_add(match value {
            RelationalValue::Text(value) => value.len(),
            RelationalValue::Bytea(value) => value.len(),
            RelationalValue::Overflow(_) => return Err(ProjectionFeedError::RequiresRebuild),
            _ => 0,
        });
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::projection::SearchProjectionChange;
    use crate::relational::RelationalTablePrimaryKeyChanges;
    use std::sync::Arc;
    fn limits(work: usize, items: usize, bytes: usize) -> ProjectionFeedLimits {
        ProjectionFeedLimits {
            work: NonZeroUsize::new(work).unwrap(),
            items: NonZeroUsize::new(items).unwrap(),
            bytes: NonZeroUsize::new(bytes).unwrap(),
        }
    }
    fn record(epoch: u64) -> Arc<SearchProjectionChange> {
        Arc::new(SearchProjectionChange {
            commit_epoch: epoch,
            upsert_node_ids: vec![epoch],
            delete_document_ids: vec![format!("old-{epoch}")],
            relational_primary_key_changes: RelationalPrimaryKeyChangeCapture::Captured {
                tables: vec![RelationalTablePrimaryKeyChanges {
                    table: "rows".into(),
                    primary_keys: vec![RelationalKey(vec![RelationalValue::Text(format!(
                        "key-{epoch}"
                    ))])],
                }],
                encoded_bytes: 0,
            },
        })
    }
    #[test]
    fn paged_seek_split_and_borrow_survive_live_trim_and_append() {
        let original = (1..=193).map(record).collect::<Vec<_>>();
        for removed in [0, 1, 63, 64, 65, 129] {
            let mut log = original
                .iter()
                .cloned()
                .collect::<SearchProjectionChangeLog>();
            log.discard_prefix(removed);
            for start in [removed as u64, 65, 128, 192, 193]
                .into_iter()
                .filter(|start| *start >= removed as u64)
            {
                let mut cursor = ProjectionFeedCursor::capture(
                    &log,
                    removed as u64,
                    193,
                    start,
                    limits(10000, 10000, 100000),
                )
                .unwrap();
                let snapshot = log.clone();
                log.discard_prefix(log.len());
                log.push(Arc::new(
                    crate::projection::owner_capture::CapturedProjectionChange {
                        legacy: record(194),
                        owners_v1: None,
                    },
                ));
                let mut found = Vec::new();
                while !cursor.is_complete() {
                    let step = cursor
                        .visit_next(limits(5, 1, 128), |item| {
                            match item {
                                ProjectionFeedItem::GraphNode { epoch, node_id } => {
                                    assert_eq!(epoch, node_id);
                                    found.push((epoch, 0));
                                }
                                ProjectionFeedItem::DeleteDocument { epoch, document_id } => {
                                    assert_eq!(
                                        document_id.as_ptr(),
                                        original[epoch as usize - 1].delete_document_ids[0]
                                            .as_ptr()
                                    );
                                    found.push((epoch, 1));
                                }
                                ProjectionFeedItem::RelationalKey { epoch, table, key } => {
                                    assert_eq!(table, "rows");
                                    let RelationalValue::Text(value) = &key.0[0] else {
                                        panic!()
                                    };
                                    let RelationalPrimaryKeyChangeCapture::Captured {
                                        tables, ..
                                    } = &original[epoch as usize - 1]
                                        .relational_primary_key_changes
                                    else {
                                        panic!()
                                    };
                                    let RelationalValue::Text(old) =
                                        &tables[0].primary_keys[0].0[0]
                                    else {
                                        panic!()
                                    };
                                    assert_eq!(value.as_ptr(), old.as_ptr());
                                    found.push((epoch, 2));
                                }
                                ProjectionFeedItem::End { source_epoch } => {
                                    assert_eq!(source_epoch, 193);
                                    found.push((194, 3));
                                }
                            }
                            Ok(())
                        })
                        .unwrap();
                    assert!(
                        step.usage.work <= 5 && step.usage.items <= 1 && step.usage.bytes <= 128
                    );
                }
                let expected = ((start + 1)..=193)
                    .flat_map(|epoch| [(epoch, 0), (epoch, 1), (epoch, 2)])
                    .chain([(194, 3)])
                    .collect::<Vec<_>>();
                assert_eq!(found, expected);
                log = snapshot;
            }
        }
    }
    #[test]
    fn large_transaction_splits_without_cloning_or_deduplicating_work() {
        let mut event = record(1);
        Arc::make_mut(&mut event).upsert_node_ids = vec![7; 1000];
        let log = [event].into_iter().collect::<SearchProjectionChangeLog>();
        let mut cursor =
            ProjectionFeedCursor::capture(&log, 0, 1, 0, limits(50, 2000, 100000)).unwrap();
        let mut visited = 0;
        loop {
            let result = cursor.visit_next(limits(7, 3, 1000), |item| {
                if matches!(item, ProjectionFeedItem::GraphNode { .. }) {
                    visited += 1;
                }
                Ok(())
            });
            if let Err(error) = result {
                assert!(matches!(
                    error,
                    ProjectionFeedError::BudgetExceeded(ProjectionFeedResource::Work)
                ));
                break;
            }
        }
        assert!(visited > 3 && visited < 50);
        assert!(!cursor.is_complete());
        assert!(matches!(
            cursor.visit_next(limits(100, 100, 100000), |_| Ok(())),
            Err(ProjectionFeedError::Failed)
        ));
    }
    #[test]
    fn key_preflight_and_callback_failures_never_certify_an_end() {
        let mut event = record(1);
        Arc::make_mut(&mut event).upsert_node_ids.clear();
        Arc::make_mut(&mut event).delete_document_ids.clear();
        if let RelationalPrimaryKeyChangeCapture::Captured { tables, .. } =
            &mut Arc::make_mut(&mut event).relational_primary_key_changes
        {
            tables[0].primary_keys[0] =
                RelationalKey(vec![RelationalValue::Text("x".repeat(1024 * 1024))]);
        }
        let log = [event].into_iter().collect::<SearchProjectionChangeLog>();
        let mut cursor =
            ProjectionFeedCursor::capture(&log, 0, 1, 0, limits(100, 100, 2 * 1024 * 1024))
                .unwrap();
        let mut callbacks = 0;
        assert!(matches!(
            cursor.visit_next(limits(100, 100, 128), |_| {
                callbacks += 1;
                Ok(())
            }),
            Err(ProjectionFeedError::StepTooSmall(
                ProjectionFeedResource::Bytes
            ))
        ));
        assert_eq!(callbacks, 0);
        assert_eq!(cursor.usage().work, 5); // seek, event, table, key + field
        assert!(!cursor.is_complete());
        let empty = SearchProjectionChangeLog::default();
        let mut cursor = ProjectionFeedCursor::capture(&empty, 0, 8, 0, limits(5, 5, 5)).unwrap();
        assert!(matches!(
            cursor.visit_next(limits(5, 5, 5), |_| Err(HawDBError::Execution(
                "failed".into()
            ))),
            Err(ProjectionFeedError::Callback(_))
        ));
        assert!(!cursor.is_complete());
        let mut cursor = ProjectionFeedCursor::capture(&empty, 0, 8, 8, limits(5, 5, 5)).unwrap();
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = cursor.visit_next(limits(5, 5, 5), |_| panic!("visitor panic"));
        }));
        assert!(panic.is_err());
        assert!(matches!(
            cursor.visit_next(limits(5, 5, 5), |_| Ok(())),
            Err(ProjectionFeedError::Failed)
        ));
    }
    #[test]
    fn deferral_and_chunk_boundaries_do_not_refund_work_or_reset_totals() {
        let mut event = record(1);
        let mutable = Arc::make_mut(&mut event);
        mutable.upsert_node_ids.clear();
        mutable.delete_document_ids.clear();
        let RelationalPrimaryKeyChangeCapture::Captured { tables, .. } =
            &mutable.relational_primary_key_changes
        else {
            panic!()
        };
        let bytes = key_bytes(&tables[0].primary_keys[0]).unwrap() + 8 + tables[0].table.len();
        let log = [event].into_iter().collect();
        let mut cursor =
            ProjectionFeedCursor::capture(&log, 0, 1, 0, limits(100, 100, 10000)).unwrap();
        let first = cursor
            .visit_next(limits(100, 1, bytes), |_| {
                panic!("deferred input reached visitor")
            })
            .unwrap();
        assert_eq!(first.usage.items, 0);
        assert_eq!(first.usage.work, 4);
        assert_eq!(cursor.usage().work, 5);
        let second = cursor
            .visit_next(limits(100, 1, bytes), |item| {
                assert!(matches!(item, ProjectionFeedItem::RelationalKey { .. }));
                Ok(())
            })
            .unwrap();
        assert_eq!(second.usage.work, 2);
        let third = cursor
            .visit_next(limits(100, 1, bytes), |item| {
                assert!(matches!(item, ProjectionFeedItem::End { .. }));
                Ok(())
            })
            .unwrap();
        assert!(third.complete);
        assert_eq!(cursor.usage().work, 9);
        for (total, resource) in [
            (limits(100, 1, 10000), ProjectionFeedResource::Items),
            (limits(100, 100, 10), ProjectionFeedResource::Bytes),
        ] {
            let log = [record(1)].into_iter().collect();
            let mut cursor = ProjectionFeedCursor::capture(&log, 0, 1, 0, total).unwrap();
            let mut calls = 0;
            cursor
                .visit_next(limits(100, 1, 10000), |_| {
                    calls += 1;
                    Ok(())
                })
                .unwrap();
            assert!(
                matches!(cursor.visit_next(limits(100,1,10000), |_| { calls+=1; Ok(()) }),Err(ProjectionFeedError::BudgetExceeded(found)) if found == resource)
            );
            assert_eq!(calls, 1);
        }
    }

    #[test]
    fn gaps_future_cursors_and_rebuild_records_fail_closed() {
        let log = [record(4)]
            .into_iter()
            .collect::<SearchProjectionChangeLog>();
        assert!(matches!(
            ProjectionFeedCursor::capture(&log, 3, 4, 2, limits(10, 10, 1000)),
            Err(ProjectionFeedError::BehindFloor { .. })
        ));
        assert!(matches!(
            ProjectionFeedCursor::capture(&log, 3, 4, 5, limits(10, 10, 1000)),
            Err(ProjectionFeedError::FutureCursor { .. })
        ));
        let mut event = record(4);
        Arc::make_mut(&mut event).relational_primary_key_changes =
            RelationalPrimaryKeyChangeCapture::RequiresRebuild {
                reason:
                    crate::relational::RelationalPrimaryKeyChangeRebuildReason::CaptureLimitExceeded,
            };
        let log = [event].into_iter().collect();
        let mut cursor =
            ProjectionFeedCursor::capture(&log, 3, 4, 3, limits(10, 10, 1000)).unwrap();
        let mut called = false;
        assert!(matches!(
            cursor.visit_next(limits(10, 10, 1000), |_| {
                called = true;
                Ok(())
            }),
            Err(ProjectionFeedError::RequiresRebuild)
        ));
        assert!(!called);
    }
}
