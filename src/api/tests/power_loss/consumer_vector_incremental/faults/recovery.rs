// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! A second physical crash while resolving the original selected operation.

use super::*;
use hawdb_storage::consumer_allocation::ConsumerSelectionReceipt;
use hawdb_storage::power_loss::image::CrashImage;

#[derive(Debug, Clone, Copy)]
pub(super) enum Cut {
    Write,
    FileSync(ObservationBoundary),
    Rename(ObservationBoundary),
    Directory(ObservationBoundary),
}

const CUTS: [Cut; 7] = [
    Cut::Write,
    Cut::FileSync(ObservationBoundary::Before),
    Cut::FileSync(ObservationBoundary::After),
    Cut::Rename(ObservationBoundary::Before),
    Cut::Rename(ObservationBoundary::After),
    Cut::Directory(ObservationBoundary::Before),
    Cut::Directory(ObservationBoundary::After),
];

pub(super) struct Context<'a> {
    pub(super) origin: super::Cut,
    pub(super) root: &'a Path,
    pub(super) target: &'a Path,
    pub(super) id: &'a SearchProjectionConsumerId,
    pub(super) durability: DurabilityPolicy,
    pub(super) graph: bool,
    pub(super) source_epoch: u64,
    pub(super) source_updated: bool,
    pub(super) index_epoch: u64,
    pub(super) expected: &'a [String],
    pub(super) original: &'a CrashImage,
    pub(super) receipt: &'a ConsumerSelectionReceipt,
}

fn prefix(abandoned: bool) -> &'static str {
    if abandoned {
        "consumer-abandoned-update-"
    } else {
        "consumer-receipt-"
    }
}

impl Cut {
    pub(super) fn observe(self, model: &PowerLossModel, abandoned: bool) {
        let (event, boundary) = match self {
            Self::Write => (IoEvent::Write, ObservationBoundary::After),
            Self::FileSync(boundary) => (IoEvent::FileSync, boundary),
            Self::Rename(boundary) => (IoEvent::Rename, boundary),
            Self::Directory(boundary) => (IoEvent::DirectorySync, boundary),
        };
        let point = ObservationPoint {
            event,
            relative_path: PathBuf::new(),
            boundary,
            skip_matches: 0,
            include_descendants: !matches!(self, Self::Directory(_)),
            keep_last: false,
        };
        if matches!(self, Self::Directory(_)) {
            model.observe(point).unwrap();
        } else {
            model
                .observe_file_name_prefix(point, prefix(abandoned))
                .unwrap();
        }
    }

    pub(super) fn replay(
        self,
        snapshot: &PowerLossSnapshot,
        acknowledged: &PowerLossSnapshot,
        context: Context<'_>,
        abandoned: bool,
    ) {
        let (mut plans, torn) = plans_with_torn_writes(
            snapshot,
            matches!(self, Self::Write).then_some(prefix(abandoned)),
        );
        assert_eq!(torn > 0, matches!(self, Self::Write));
        let complete = acknowledged.crash(&CrashPlan::default()).unwrap();
        plans.push(CrashPlan::default());
        let total = plans.len();
        let mut seen = Vec::new();
        let mut reopens = 0;
        for (index, plan) in plans.into_iter().enumerate() {
            let acknowledged = index + 1 == total;
            let physical = if acknowledged {
                complete.clone()
            } else {
                snapshot.crash(&plan).unwrap()
            };
            if !acknowledged
                && seen
                    .iter()
                    .any(|image| same_physical_image(image, &physical))
            {
                continue;
            }
            seen.push(physical.clone());
            let image = MaterializedImage(context.root.to_owned());
            physical.materialize(&image).unwrap();
            let project =
                ProjectFileDescriptors::acquire_with_capacity_limit(&image, 32, true).unwrap();
            let model =
                PowerLossModel::resume_from_image(&project, &physical, ImageLimits::default())
                    .unwrap();
            reopens += 1;
            for repeated in 0..2 {
                let mut database = open(&image, context.durability);
                assert_eq!(database.commit_epoch().unwrap(), context.source_epoch);
                // Receipt recovery cannot change the complete C or E source
                // transaction that actually survived the first crash.
                assert_source(&mut database, context.graph, context.source_updated);
                let consumer = database
                    .open_search_projection_consumer(context.id, context.target)
                    .unwrap_or_else(|error| panic!("{self:?} abandoned={abandoned} repeat={repeated} plan={plan:?}: {error:?}"));
                assert_eq!(
                    consumer.canonical_vector_source_epoch().unwrap(),
                    Some(context.index_epoch)
                );
                assert_eq!(
                    consumer.projection().stationary_receipt().unwrap(),
                    context.receipt
                );
                let captured = model.capture().unwrap();
                let captured = captured.crash(&captured.persist_all_plan()).unwrap();
                assert_preserved_artifacts(context.root, context.original);
                for path in context.original.paths().filter(|path| is_candidate(path)) {
                    assert_eq!(
                        captured.file_inode(path),
                        context.original.file_inode(path),
                        "recovery replaced original candidate inode: {path:?}"
                    );
                }
                let permit = database
                    .search_projection_consumer_read_permit(
                        RuntimeWorkRequest::foreground_query(64 << 20, 1 << 20)
                            .with_io_wave_slots(1),
                    )
                    .unwrap();
                assert_eq!(selected_ids(&consumer, &permit), context.expected);
                if context.index_epoch != context.source_epoch {
                    assert!(matches!(
                        database
                            .begin_read_transaction()
                            .unwrap()
                            .bind_vector_index(&consumer, &permit),
                        Err(crate::VectorIndexReadError::SourceEpochMismatch { .. })
                    ));
                } else {
                    assert_eq!(current_ids(&mut database, &consumer), context.expected);
                }
                drop(permit);
                drop(consumer);
                drop(database);
            }
        }
        eprintln!("canonical-vector-recovery-power-v1 origin={:?} cut={self:?} abandoned={abandoned} durability={:?} graph={} source_epoch={} source_updated={} index_epoch={} plans={total} distinct_reopens={reopens}", context.origin, context.durability, context.graph, context.source_epoch, context.source_updated, context.index_epoch);
    }
}

fn qualify(origin: super::Cut) {
    for durability in [
        DurabilityPolicy::SyncOnEveryWrite,
        DurabilityPolicy::SyncOnCheckpoint,
    ] {
        for graph in [false, true] {
            for cut in CUTS {
                check_recovery(origin, durability, graph, Some(cut), false);
            }
        }
    }
}

#[test]
fn vector_index_incremental_power_loss_recovery_publication_retains_unselected_births() {
    qualify(super::Cut::Link);
}

#[test]
fn vector_index_incremental_power_loss_recovery_publication_settles_unselected_intents() {
    qualify(super::Cut::SelectorRename(ObservationBoundary::Before));
}

#[test]
fn vector_index_incremental_power_loss_recovery_publication_completes_selected_intents() {
    qualify(super::Cut::SelectorRename(ObservationBoundary::After));
}

fn qualify_lost_relaxed_source(origin: super::Cut, graph: bool) {
    for cut in CUTS {
        check_recovery(
            origin,
            DurabilityPolicy::SyncOnCheckpoint,
            graph,
            Some(cut),
            true,
        );
    }
}

#[test]
fn vector_index_incremental_power_loss_recovery_publication_preserves_lost_relaxed_sql_sources_after_link(
) {
    qualify_lost_relaxed_source(super::Cut::Link, false);
}

#[test]
fn vector_index_incremental_power_loss_recovery_publication_preserves_lost_relaxed_sql_sources_after_selector_write(
) {
    qualify_lost_relaxed_source(super::Cut::SelectorWrite, false);
}

#[test]
fn vector_index_incremental_power_loss_recovery_publication_preserves_lost_relaxed_node_sources_after_link(
) {
    qualify_lost_relaxed_source(super::Cut::Link, true);
}

#[test]
fn vector_index_incremental_power_loss_recovery_publication_preserves_lost_relaxed_node_sources_after_selector_write(
) {
    qualify_lost_relaxed_source(super::Cut::SelectorWrite, true);
}
