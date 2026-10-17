// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Borrowed versioned source facts. End is not mapping, staging or publication authority.

use super::*;
use crate::projection::owner_capture::{OwnerBeforeV1, OwnerEffectV1, OwnerIdV1};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionOwnerId<'a> {
    Absent,
    String(&'a str),
    Unsupported,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionOwnerBefore<'a> {
    NotRecorded,
    Absent,
    /// Labels belong to the pinned source catalog. A schema gap prevents
    /// reading these identities under a different catalog definition.
    Present {
        labels: &'a [u32],
        id: ProjectionOwnerId<'a>,
    },
}
impl<'a> From<&'a OwnerBeforeV1> for ProjectionOwnerBefore<'a> {
    fn from(before: &'a OwnerBeforeV1) -> Self {
        match before {
            OwnerBeforeV1::NotRecorded => Self::NotRecorded,
            OwnerBeforeV1::Absent => Self::Absent,
            OwnerBeforeV1::Present { labels, id } => Self::Present {
                labels,
                id: match id {
                    OwnerIdV1::Absent => ProjectionOwnerId::Absent,
                    OwnerIdV1::String(id) => ProjectionOwnerId::String(id),
                    OwnerIdV1::Unsupported => ProjectionOwnerId::Unsupported,
                },
            },
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub enum ProjectionOwnerFeedItem<'a> {
    GraphOwner {
        epoch: u64,
        node_id: u64,
        before: ProjectionOwnerBefore<'a>,
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

/// Shares immutable paged records. No public resume ordinal or mutable source
/// escape can substitute a different source between steps. The owner scope is
/// node identities, endpoints of every relationship effect (including property
/// updates) and typed relational keys under capture contract V2. Endpoint facts
/// alone do not qualify arbitrary host mapping, parent-key preimages or fanout.
#[derive(Debug)]
pub struct ProjectionOwnerFeedCursor {
    inner: ProjectionFeedCursor,
}
impl ProjectionOwnerFeedCursor {
    pub(in crate::store) fn capture(
        log: &SearchProjectionChangeLog,
        legacy_floor: u64,
        source_epoch: u64,
        start_epoch: u64,
        limits: ProjectionFeedLimits,
    ) -> std::result::Result<Self, ProjectionFeedError> {
        let mut inner = ProjectionFeedCursor::capture(
            log,
            legacy_floor.max(log.owner_floor()),
            source_epoch,
            start_epoch,
            limits,
        )?;
        inner.kind = FeedKind::OwnerV1;
        Ok(Self { inner })
    }
    pub fn source_epoch(&self) -> u64 {
        self.inner.source_epoch()
    }
    pub fn start_epoch(&self) -> u64 {
        self.inner.start_epoch()
    }
    pub fn usage(&self) -> ProjectionFeedUsage {
        self.inner.usage()
    }
    pub fn is_complete(&self) -> bool {
        self.inner.is_complete()
    }
    /// Charges borrowed preimage/key input before callbacks, including repeated
    /// owners and work deferred by a step's remaining byte allowance. The same
    /// immutable E supplies all events. Completion certifies enumeration only.
    pub fn visit_next(
        &mut self,
        step: ProjectionFeedLimits,
        mut visitor: impl FnMut(ProjectionOwnerFeedItem<'_>) -> Result<()>,
    ) -> std::result::Result<ProjectionFeedStep, ProjectionFeedError> {
        self.inner.visit_input(step, |input| {
            visitor(match input {
                FeedInput::Owner { epoch, effect } => ProjectionOwnerFeedItem::GraphOwner {
                    epoch,
                    node_id: effect.node_id,
                    before: (&effect.before).into(),
                },
                FeedInput::Legacy(ProjectionFeedItem::RelationalKey { epoch, table, key }) => {
                    ProjectionOwnerFeedItem::RelationalKey { epoch, table, key }
                }
                FeedInput::Legacy(ProjectionFeedItem::End { source_epoch }) => {
                    ProjectionOwnerFeedItem::End { source_epoch }
                }
                _ => unreachable!("owner cursor cannot emit legacy document events"),
            })
        })
    }
}

pub(super) fn measure(owner: &OwnerEffectV1) -> (usize, usize) {
    let mut work = 1usize;
    // epoch, node ID and preimage tag; this is logical borrowed input, not RSS.
    let mut bytes = 17usize;
    if let OwnerBeforeV1::Present { labels, id } = &owner.before {
        work = work.saturating_add(labels.len());
        bytes = bytes
            .saturating_add(labels.len().saturating_mul(4))
            .saturating_add(1);
        if let OwnerIdV1::String(id) = id {
            bytes = bytes.saturating_add(8).saturating_add(id.len());
        }
    }
    (work, bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::projection::owner_capture::CapturedProjectionChange;
    use crate::projection::SearchProjectionGraphChange;
    use crate::relational::{RelationalPrimaryKeyChangeCapture, RelationalTablePrimaryKeyChanges};
    use std::sync::Arc;

    fn limits(work: usize, items: usize, bytes: usize) -> ProjectionFeedLimits {
        ProjectionFeedLimits {
            work: NonZeroUsize::new(work).unwrap(),
            items: NonZeroUsize::new(items).unwrap(),
            bytes: NonZeroUsize::new(bytes).unwrap(),
        }
    }
    fn record(epoch: u64, owners: Vec<OwnerEffectV1>) -> Arc<CapturedProjectionChange> {
        Arc::new(CapturedProjectionChange {
            legacy: Arc::new(SearchProjectionGraphChange {
                commit_epoch: epoch,
                upsert_node_ids: vec![],
                delete_document_ids: vec!["legacy:irrelevant".into()],
                relational_primary_key_changes: RelationalPrimaryKeyChangeCapture::Captured {
                    tables: vec![],
                    encoded_bytes: 0,
                },
            }),
            owners_v1: Some(owners),
        })
    }
    fn absent(node_id: u64) -> OwnerEffectV1 {
        OwnerEffectV1 {
            node_id,
            before: OwnerBeforeV1::Absent,
        }
    }
    fn present(node_id: u64, text: &str) -> OwnerEffectV1 {
        OwnerEffectV1 {
            node_id,
            before: OwnerBeforeV1::Present {
                labels: vec![1, 2],
                id: OwnerIdV1::String(text.into()),
            },
        }
    }
    #[test]
    fn owner_only_delete_is_borrowed_from_the_pinned_log_after_live_trim_and_append() {
        let original = record(2, vec![present(7, "thread-before")]);
        let OwnerBeforeV1::Present {
            labels,
            id: OwnerIdV1::String(id),
        } = &original.owners_v1.as_ref().unwrap()[0].before
        else {
            unreachable!()
        };
        let id_pointer = id.as_ptr();
        let labels_pointer = labels.as_ptr();
        let mut log = [Arc::clone(&original)]
            .into_iter()
            .collect::<SearchProjectionChangeLog>();
        let mut feed =
            ProjectionOwnerFeedCursor::capture(&log, 1, 2, 1, limits(100, 10, 1024)).unwrap();
        log.discard_prefix(1);
        log.push(record(3, vec![absent(8)]));
        let mut seen = 0;
        while !feed.is_complete() {
            feed.visit_next(limits(8, 1, 1024), |item| {
                match item {
                    ProjectionOwnerFeedItem::GraphOwner {
                        epoch,
                        node_id,
                        before,
                    } => {
                        assert_eq!((epoch, node_id), (2, 7));
                        let ProjectionOwnerBefore::Present {
                            labels,
                            id: ProjectionOwnerId::String(id),
                        } = before
                        else {
                            panic!("missing old owner identity");
                        };
                        assert_eq!(id, "thread-before");
                        assert_eq!(id.as_ptr(), id_pointer);
                        assert_eq!(labels.as_ptr(), labels_pointer);
                        seen += 1;
                    }
                    ProjectionOwnerFeedItem::End { source_epoch } => {
                        assert_eq!(source_epoch, 2);
                        assert_eq!(seen, 1);
                    }
                    _ => panic!("unexpected legacy event or key"),
                }
                Ok(())
            })
            .unwrap();
        }
        assert_eq!(seen, 1);
        assert_eq!(log.owner_floor(), 2);
    }
    #[test]
    fn owner_cursor_enforces_upgrade_floor_and_keeps_repeated_transaction_owners() {
        let mut log = SearchProjectionChangeLog::at_owner_floor(3);
        log.push(record(4, vec![absent(1)]));
        log.push(record(5, vec![present(1, "before-delete")]));
        assert!(matches!(
            ProjectionOwnerFeedCursor::capture(&log, 0, 5, 2, limits(100, 10, 4096)),
            Err(ProjectionFeedError::BehindFloor { floor: 3, .. })
        ));
        assert!(matches!(
            ProjectionOwnerFeedCursor::capture(&log, 0, 5, 6, limits(100, 10, 4096)),
            Err(ProjectionFeedError::FutureCursor { .. })
        ));
        let mut cursor =
            ProjectionOwnerFeedCursor::capture(&log, 0, 5, 3, limits(100, 10, 4096)).unwrap();
        let mut epochs = vec![];
        while !cursor.is_complete() {
            cursor
                .visit_next(limits(8, 1, 4096), |item| {
                    if let ProjectionOwnerFeedItem::GraphOwner { epoch, node_id, .. } = item {
                        assert_eq!(node_id, 1);
                        epochs.push(epoch);
                    }
                    Ok(())
                })
                .unwrap();
        }
        assert_eq!(epochs, [4, 5]);
    }
    #[test]
    fn preimage_deferral_charges_work_and_cumulative_budget_does_not_reset() {
        let log = [record(1, vec![absent(1), present(2, "ten-bytes!")])]
            .into_iter()
            .collect();
        let mut cursor =
            ProjectionOwnerFeedCursor::capture(&log, 0, 1, 0, limits(100, 10, 4096)).unwrap();
        let step = limits(100, 10, 48);
        let first = cursor.visit_next(step, |_| Ok(())).unwrap();
        assert!(!first.complete);
        assert_eq!(first.usage.items, 1);
        assert_eq!(first.usage.work, 5);
        let before = cursor.usage();
        let second = cursor.visit_next(step, |_| Ok(())).unwrap();
        assert!(second.complete);
        assert_eq!(second.usage.items, 2);
        assert_eq!(second.usage.work, 5);
        assert_eq!(cursor.usage().work, before.work + second.usage.work);
        let mut bounded =
            ProjectionOwnerFeedCursor::capture(&log, 0, 1, 0, limits(8, 10, 4096)).unwrap();
        bounded.visit_next(step, |_| Ok(())).unwrap();
        assert!(matches!(
            bounded.visit_next(step, |_| Ok(())),
            Err(ProjectionFeedError::BudgetExceeded(
                ProjectionFeedResource::Work
            ))
        ));
        assert!(!bounded.is_complete());
        assert!(matches!(
            bounded.visit_next(step, |_| Ok(())),
            Err(ProjectionFeedError::Failed)
        ));
        let mut tiny =
            ProjectionOwnerFeedCursor::capture(&log, 0, 1, 0, limits(100, 10, 4096)).unwrap();
        assert!(matches!(
            tiny.visit_next(limits(10, 10, 16), |_| Ok(())),
            Err(ProjectionFeedError::StepTooSmall(
                ProjectionFeedResource::Bytes
            ))
        ));
    }
    #[test]
    fn owner_callback_error_or_panic_and_relational_rebuild_never_reach_end() {
        for panic in [false, true] {
            let log = [record(1, vec![absent(1)])].into_iter().collect();
            let mut cursor =
                ProjectionOwnerFeedCursor::capture(&log, 0, 1, 0, limits(100, 10, 4096)).unwrap();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                cursor.visit_next(limits(100, 10, 4096), |_| {
                    if panic {
                        panic!("injected callback panic");
                    }
                    Err(HawDBError::Execution("injected callback error".into()))
                })
            }));
            if panic {
                assert!(result.is_err());
            } else {
                assert!(matches!(
                    result.unwrap(),
                    Err(ProjectionFeedError::Callback(_))
                ));
            }
            assert!(!cursor.is_complete());
            assert!(matches!(
                cursor.visit_next(limits(100, 10, 4096), |_| Ok(())),
                Err(ProjectionFeedError::Failed)
            ));
        }
        let mut change = Arc::unwrap_or_clone(record(1, vec![absent(1)]));
        change.relational_primary_key_changes =
            RelationalPrimaryKeyChangeCapture::RequiresRebuild {
                reason:
                    crate::relational::RelationalPrimaryKeyChangeRebuildReason::SnapshotReplacement,
            };
        let log = [Arc::new(change)].into_iter().collect();
        let mut cursor =
            ProjectionOwnerFeedCursor::capture(&log, 0, 1, 0, limits(100, 10, 4096)).unwrap();
        assert!(matches!(
            cursor.visit_next(limits(100, 10, 4096), |_| panic!(
                "rebuild must precede output"
            )),
            Err(ProjectionFeedError::RequiresRebuild)
        ));
    }
    #[test]
    fn one_large_transaction_splits_owner_and_relational_inputs_without_restarting() {
        let mut change = Arc::unwrap_or_clone(record(9, (0..300).map(absent).collect()));
        change.relational_primary_key_changes = RelationalPrimaryKeyChangeCapture::Captured {
            tables: vec![RelationalTablePrimaryKeyChanges {
                table: "messages".into(),
                primary_keys: (0..300)
                    .map(|id| RelationalKey(vec![RelationalValue::BigInt(id)]))
                    .collect(),
            }],
            encoded_bytes: 0,
        };
        let original = Arc::new(change);
        let log = [Arc::clone(&original)].into_iter().collect();
        let mut cursor =
            ProjectionOwnerFeedCursor::capture(&log, 0, 9, 0, limits(4000, 1000, 100000)).unwrap();
        let mut owners = 0;
        let mut keys = 0;
        let mut end = 0;
        while !cursor.is_complete() {
            cursor
                .visit_next(limits(8, 1, 128), |item| {
                    match item {
                        ProjectionOwnerFeedItem::GraphOwner { node_id, .. } => {
                            assert_eq!(node_id, owners);
                            owners += 1;
                        }
                        ProjectionOwnerFeedItem::RelationalKey { key, .. } => {
                            let RelationalPrimaryKeyChangeCapture::Captured { tables, .. } =
                                &original.relational_primary_key_changes
                            else {
                                unreachable!()
                            };
                            assert_eq!(key.0.as_ptr(), tables[0].primary_keys[keys].0.as_ptr());
                            keys += 1;
                        }
                        ProjectionOwnerFeedItem::End { source_epoch } => {
                            assert_eq!(source_epoch, 9);
                            end += 1;
                        }
                    }
                    Ok(())
                })
                .unwrap();
        }
        assert_eq!((owners, keys, end), (300, 300, 1));
        assert_eq!(cursor.usage().items, 601);
        assert!(cursor.usage().work < 1000);
    }
}
