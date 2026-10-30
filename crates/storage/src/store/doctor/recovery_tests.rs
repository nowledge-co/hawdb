// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::config::{DurabilityPolicy, RecoveryMode, WalReplayConfig};
use crate::power_loss::image::{CrashPlan, ImageLimits, PersistOperation};
use crate::power_loss::PowerLossModel;
use crate::store::GraphStore;
use hawdb_core::{schema::Catalog, Value};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        Self(std::env::temp_dir().join(format!(
            "hawdb-wal-995-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )))
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn append(store: &mut GraphStore, catalog: &mut Catalog, id: i64) {
    store
        .create_node(
            catalog,
            "Record",
            BTreeMap::from([
                ("id".into(), Value::Int(id)),
                ("payload".into(), Value::String("x".repeat(40000))),
            ]),
        )
        .unwrap();
}
fn open(
    path: &Path,
    catalog: &mut Catalog,
    durability: DurabilityPolicy,
    recovery_mode: RecoveryMode,
) -> Result<GraphStore> {
    GraphStore::open_with_durability_and_recovery(path, catalog, durability, recovery_mode)
}
fn assert_suffix_evidence(report: &WalTailRepairReport, original: &[u8]) {
    assert_eq!(
        std::fs::read(
            report
                .evidence_directory
                .join("quarantine")
                .join(&report.quarantine_file)
        )
        .unwrap(),
        original[report.retained_wal_len as usize..]
    );
    assert!(report
        .evidence_directory
        .join(&report.repair_record_file)
        .exists());
}

#[test]
fn recovery_995_default_repairs_with_a_suffix_budget_smaller_than_the_wal() {
    let root = Fixture::new();
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&root.0, &mut catalog).unwrap();
    append(&mut store, &mut catalog, 1);
    let wal = store.durable.as_ref().unwrap().wal_path.clone();
    drop(store);
    std::fs::OpenOptions::new()
        .append(true)
        .open(&wal)
        .unwrap()
        .write_all(b"partial")
        .unwrap();
    let original = std::fs::read(&wal).unwrap();
    assert!(open(
        &root.0,
        &mut Catalog::default(),
        DurabilityPolicy::SyncOnEveryWrite,
        RecoveryMode::Strict
    )
    .is_err());
    for limit in [0, 6] {
        let error = GraphStore::open_with_durability_and_replay_config(
            &root.0,
            &mut Catalog::default(),
            DurabilityPolicy::SyncOnEveryWrite,
            WalReplayConfig {
                max_quarantine_bytes: limit,
                ..Default::default()
            },
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("quarantine byte limit"),
            "{error}"
        );
        assert_eq!(std::fs::read(&wal).unwrap(), original);
    }
    let store = GraphStore::open_with_durability_and_replay_config(
        &root.0,
        &mut Catalog::default(),
        DurabilityPolicy::SyncOnEveryWrite,
        WalReplayConfig {
            max_quarantine_bytes: 7,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(store.scan_nodes(None).count(), 1);
    let report = store.storage_recovery_report().wal_repair.unwrap();
    assert_eq!(report.reason, WalTailRepairReason::IncompleteFinalRecord);
    assert_eq!(report.discarded_wal_tail_bytes, 7);
    assert_suffix_evidence(&report, &original);
}

#[test]
fn recovery_995_relaxed_fault_images_recover_whole_prefixes() {
    for durability in [
        DurabilityPolicy::SyncOnCheckpoint,
        DurabilityPolicy::SyncOnEveryWrite,
    ] {
        let root = Fixture::new();
        let project = crate::file_descriptors::ProjectFileDescriptors::acquire_with_capacity_limit(
            &root.0,
            crate::file_descriptors::DEFAULT_MAX_OPEN_FILES,
            false,
        )
        .unwrap();
        let model = PowerLossModel::attach(&project, ImageLimits::default()).unwrap();
        let mut catalog = Catalog::default();
        let mut seed = GraphStore::open(&root.0, &mut catalog).unwrap();
        append(&mut seed, &mut catalog, 0);
        drop(seed);
        let mut store = open(
            &root.0,
            &mut Catalog::default(),
            durability,
            RecoveryMode::default(),
        )
        .unwrap();
        for id in 1..=3 {
            append(&mut store, &mut catalog, id);
        }
        let wal = store.durable.as_ref().unwrap().wal_path.clone();
        let relative = wal.strip_prefix(&root.0).unwrap();
        let snapshot = model.capture().unwrap();
        let writes = snapshot.uncovered_writes(relative).unwrap();
        if durability == DurabilityPolicy::SyncOnEveryWrite {
            assert!(writes.is_empty());
            let image = snapshot.crash(&CrashPlan::default()).unwrap();
            let recovered = Fixture::new();
            image.materialize(&recovered.0).unwrap();
            let reopened = GraphStore::open(&recovered.0, &mut Catalog::default()).unwrap();
            assert_eq!(reopened.scan_nodes(None).count(), 4);
            continue;
        }
        assert_eq!(writes.len(), 3);
        let cases = [
            (vec![], 1),
            (vec![PersistOperation::Whole(writes[0].operation)], 2),
            // Later data survives a lost middle transaction, with a zero-filled hole.
            (
                vec![
                    PersistOperation::Whole(writes[2].operation),
                    PersistOperation::Whole(writes[0].operation),
                ],
                2,
            ),
            (
                vec![
                    PersistOperation::TornWrite {
                        operation: writes[1].operation,
                        bytes: 0..19,
                    },
                    PersistOperation::Whole(writes[0].operation),
                ],
                2,
            ),
            // Extended length with the beginning of the final record absent.
            (
                vec![PersistOperation::TornWrite {
                    operation: writes[0].operation,
                    bytes: 100..writes[0].length,
                }],
                1,
            ),
        ];
        for (persistence, expected) in cases {
            let image = snapshot.crash(&CrashPlan { persistence }).unwrap();
            let recovered = Fixture::new();
            image.materialize(&recovered.0).unwrap();
            let original = std::fs::read(recovered.0.join(relative)).unwrap();
            let reopened = GraphStore::open(&recovered.0, &mut Catalog::default()).unwrap();
            assert_eq!(reopened.scan_nodes(None).count(), expected);
            if let Some(report) = reopened.storage_recovery_report().wal_repair {
                assert_suffix_evidence(&report, &original);
            }
            drop(reopened);
            let reopened = GraphStore::open(&recovered.0, &mut Catalog::default()).unwrap();
            assert_eq!(reopened.scan_nodes(None).count(), expected);
            assert!(reopened.storage_recovery_report().wal_repair.is_none());
        }
    }
}

#[test]
fn recovery_995_source_sync_and_policy_changes_protect_acknowledged_prefixes() {
    for grouped in [None, Some(false), Some(true)] {
        let root = Fixture::new();
        let mut catalog = Catalog::default();
        let mut store = open(
            &root.0,
            &mut catalog,
            DurabilityPolicy::SyncOnCheckpoint,
            RecoveryMode::default(),
        )
        .unwrap();
        append(&mut store, &mut catalog, 1);
        let wal = store.durable.as_ref().unwrap().wal_path.clone();
        if let Some(grouped) = grouped {
            drop(store);
            store = GraphStore::open(&root.0, &mut catalog).unwrap();
            if grouped {
                assert!(store.begin_wal_sync_group().unwrap());
            }
            append(&mut store, &mut catalog, 2);
            if grouped {
                store.finish_wal_sync_group().unwrap();
            }
        } else {
            store
                .durable
                .as_mut()
                .unwrap()
                .synchronize_source_prefix()
                .unwrap();
        }
        let durable = store.durable.as_ref().unwrap();
        let (boundary, generation, start_lsn) = (
            durable.wal_bytes,
            durable.wal_generation,
            durable.wal_replay_start_lsn,
        );
        drop(store);
        let mut damaged = std::fs::read(&wal).unwrap();
        damaged[crate::wal::frame::WAL_BINARY_FILE_HEADER_BYTES
            + crate::wal::frame::WAL_FRAGMENT_HEADER_BYTES] ^= 1;
        std::fs::write(&wal, &damaged).unwrap();
        let error = GraphStore::open(&root.0, &mut Catalog::default()).unwrap_err();
        assert!(error.to_string().contains("corruption"), "{error}");
        assert_eq!(std::fs::read(&wal).unwrap(), damaged);
        let fence = super::super::wal_fence::WalRecoveryFence::load(&wal, generation, start_lsn)
            .unwrap()
            .unwrap();
        assert_eq!(fence.protected_bytes, boundary);
    }
}

#[test]
fn recovery_995_retries_every_evidence_and_truncate_persistence_step() {
    let stages = [
        "create evidence directory",
        "synchronize doctor directory",
        "synchronize quarantine directory",
        "open WAL suffix",
        "seek WAL suffix",
        "create suffix evidence",
        "write suffix evidence",
        "flush suffix evidence",
        "synchronize suffix evidence",
        "publish suffix evidence",
        "reopen suffix evidence",
        "finish suffix evidence barrier",
        "synchronize suffix directory",
        "open retained prefix",
        "open suffix for validation",
        "inspect suffix evidence",
        "read retained prefix",
        "read suffix evidence",
        "create audit directory",
        "create audit",
        "write audit",
        "synchronize audit",
        "publish audit",
        "open WAL for repair",
        "truncate WAL",
        "synchronize repaired WAL",
        "synchronize WAL directory",
        "reopen repaired WAL",
        "finish repaired WAL barrier",
        "retire pending audit",
        "synchronize retired audit",
    ];
    for kind in [io::ErrorKind::PermissionDenied, io::ErrorKind::WouldBlock] {
        for stage in stages {
            let root = Fixture::new();
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&root.0, &mut catalog).unwrap();
            append(&mut store, &mut catalog, 1);
            let wal = store.durable.as_ref().unwrap().wal_path.clone();
            drop(store);
            std::fs::OpenOptions::new()
                .append(true)
                .open(&wal)
                .unwrap()
                .write_all(b"partial")
                .unwrap();
            let original = std::fs::read(&wal).unwrap();
            REPAIR_FAILURE.with(|failure| *failure.borrow_mut() = Some((stage, kind)));
            let error = GraphStore::open(&root.0, &mut Catalog::default()).unwrap_err();
            assert!(
                matches!(error, HawDBError::WalRecoveryIo { operation, kind: observed, .. } if operation == stage && kind == observed),
                "{stage}: {error}"
            );
            assert!(
                REPAIR_FAILURE.with(|failure| failure.borrow().is_none()),
                "unreached stage {stage}"
            );
            if ![
                "truncate WAL",
                "synchronize repaired WAL",
                "synchronize WAL directory",
                "reopen repaired WAL",
                "finish repaired WAL barrier",
                "retire pending audit",
                "synchronize retired audit",
            ]
            .contains(&stage)
            {
                assert_eq!(std::fs::read(&wal).unwrap(), original, "{stage}");
            }
            let recovered = GraphStore::open(&root.0, &mut Catalog::default()).unwrap();
            assert_eq!(recovered.scan_nodes(None).count(), 1, "{stage}");
            if let Some(report) = recovered.storage_recovery_report().wal_repair {
                assert_suffix_evidence(&report, &original);
            }
            assert!(pending_repair_records(&root.0).unwrap().is_empty());
            drop(recovered);
            let recovered = GraphStore::open(&root.0, &mut Catalog::default()).unwrap();
            assert_eq!(recovered.scan_nodes(None).count(), 1);
        }
    }
    eprintln!(
        "WAL repair typed IO retry cases verified: {}",
        stages.len() * 2
    );
}

#[test]
fn recovery_995_missing_or_damaged_fence_never_authorizes_middle_corruption() {
    for damage_fence in [false, true] {
        let root = Fixture::new();
        let mut catalog = Catalog::default();
        let mut store = open(
            &root.0,
            &mut catalog,
            DurabilityPolicy::SyncOnCheckpoint,
            RecoveryMode::default(),
        )
        .unwrap();
        append(&mut store, &mut catalog, 1);
        append(&mut store, &mut catalog, 2);
        let wal = store.durable.as_ref().unwrap().wal_path.clone();
        drop(store);
        let fence = super::super::wal_fence::fence_path(&wal);
        if damage_fence {
            std::fs::write(&fence, b"torn fence").unwrap();
        } else {
            std::fs::remove_file(&fence).unwrap();
        }
        let mut damaged = std::fs::read(&wal).unwrap();
        damaged[crate::wal::frame::WAL_BINARY_FILE_HEADER_BYTES
            + crate::wal::frame::WAL_FRAGMENT_HEADER_BYTES] ^= 1;
        std::fs::write(&wal, &damaged).unwrap();
        assert!(GraphStore::open(&root.0, &mut Catalog::default()).is_err());
        assert_eq!(std::fs::read(&wal).unwrap(), damaged);
    }
}

#[test]
fn recovery_995_sync_fence_is_durable_before_the_wal_barrier() {
    use crate::power_loss::{IoEvent, ObservationBoundary, ObservationPoint};
    for boundary in [ObservationBoundary::Before, ObservationBoundary::After] {
        let root = Fixture::new();
        let project = crate::file_descriptors::ProjectFileDescriptors::acquire_with_capacity_limit(
            &root.0,
            crate::file_descriptors::DEFAULT_MAX_OPEN_FILES,
            false,
        )
        .unwrap();
        let model = PowerLossModel::attach(&project, ImageLimits::default()).unwrap();
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&root.0, &mut catalog).unwrap();
        append(&mut store, &mut catalog, 0);
        drop(store);
        let mut store = open(
            &root.0,
            &mut catalog,
            DurabilityPolicy::SyncOnCheckpoint,
            RecoveryMode::default(),
        )
        .unwrap();
        append(&mut store, &mut catalog, 1);
        let durable = store.durable.as_ref().unwrap();
        let wal = durable.wal_path.clone();
        let (bytes, generation, start_lsn) = (
            durable.wal_bytes,
            durable.wal_generation,
            durable.wal_replay_start_lsn,
        );
        let relative = wal.strip_prefix(&root.0).unwrap();
        model
            .observe(ObservationPoint {
                event: IoEvent::FileSync,
                relative_path: relative.into(),
                boundary,
                skip_matches: 0,
                include_descendants: false,
                keep_last: false,
            })
            .unwrap();
        store.synchronize_source_prefix().unwrap();
        model.capture().unwrap();
        let cut = model
            .take_observation()
            .unwrap()
            .expect("WAL sync boundary reached");
        let image = cut.crash(&CrashPlan::default()).unwrap();
        let recovered = Fixture::new();
        image.materialize(&recovered.0).unwrap();
        let recovered_wal = recovered.0.join(relative);
        let fence =
            super::super::wal_fence::WalRecoveryFence::load(&recovered_wal, generation, start_lsn)
                .unwrap()
                .unwrap();
        assert_eq!(fence.protected_bytes, bytes);
        let original = std::fs::read(&recovered_wal).unwrap();
        let reopened = GraphStore::open(&recovered.0, &mut Catalog::default());
        match boundary {
            ObservationBoundary::Before => {
                assert!(
                    reopened.is_err(),
                    "an interrupted protected barrier must fail closed"
                );
                assert_eq!(std::fs::read(&recovered_wal).unwrap(), original);
            }
            ObservationBoundary::After => assert_eq!(reopened.unwrap().scan_nodes(None).count(), 2),
        }
    }
}

#[test]
fn recovery_995_repair_survives_every_native_persistence_boundary() {
    use crate::power_loss::{IoEvent, ObservationBoundary, ObservationPoint};
    let mut cuts = 0;
    for event in [
        IoEvent::Write,
        IoEvent::FileSync,
        IoEvent::DirectorySync,
        IoEvent::Rename,
        IoEvent::Truncate,
        IoEvent::Remove,
    ] {
        for boundary in [ObservationBoundary::Before, ObservationBoundary::After] {
            let mut observed = 0;
            for skip_matches in 0..128 {
                let root = Fixture::new();
                let project =
                    crate::file_descriptors::ProjectFileDescriptors::acquire_with_capacity_limit(
                        &root.0,
                        crate::file_descriptors::DEFAULT_MAX_OPEN_FILES,
                        false,
                    )
                    .unwrap();
                let model = PowerLossModel::attach(&project, ImageLimits::default()).unwrap();
                let mut catalog = Catalog::default();
                let mut store = GraphStore::open(&root.0, &mut catalog).unwrap();
                append(&mut store, &mut catalog, 0);
                let wal = store.durable.as_ref().unwrap().wal_path.clone();
                let prefix = std::fs::read(&wal).unwrap();
                drop(store);
                // Establish a physical torn-tail baseline through recorded IO.
                let mut file = OpenOptions::new().append(true).open(&wal).unwrap();
                file.write_all(b"partial").unwrap();
                file.sync_all().unwrap();
                drop(file);
                model
                    .observe(ObservationPoint {
                        event,
                        relative_path: PathBuf::new(),
                        boundary,
                        skip_matches,
                        include_descendants: true,
                        keep_last: false,
                    })
                    .unwrap();
                let repaired = GraphStore::open(&root.0, &mut Catalog::default()).unwrap();
                drop(repaired);
                model.capture().unwrap();
                let Some(cut) = model.take_observation().unwrap() else {
                    break;
                };
                observed += 1;
                for plan in [CrashPlan::default(), cut.persist_all_plan()] {
                    let image = cut.crash(&plan).unwrap();
                    let recovered = Fixture::new();
                    image.materialize(&recovered.0).unwrap();
                    let store = GraphStore::open(&recovered.0, &mut Catalog::default())
                        .unwrap_or_else(|error| {
                            panic!("{event:?} {boundary:?} {skip_matches}: {error}")
                        });
                    assert_eq!(store.scan_nodes(None).count(), 1);
                    assert_eq!(
                        std::fs::read(recovered.0.join(wal.strip_prefix(&root.0).unwrap()))
                            .unwrap(),
                        prefix
                    );
                    let audit = std::fs::read_dir(doctor_directory(&recovered.0))
                        .unwrap()
                        .map(|entry| entry.unwrap().path())
                        .find(|path| path.to_string_lossy().ends_with(".repair.applied.json"))
                        .unwrap();
                    let record = load_audit_record(&audit).unwrap();
                    validate_quarantine(&recovered.0, &record).unwrap();
                    assert_eq!(record.plan.discarded_wal_tail_bytes, 7);
                    assert!(pending_repair_records(&recovered.0).unwrap().is_empty());
                    drop(store);
                    assert_eq!(
                        GraphStore::open(&recovered.0, &mut Catalog::default())
                            .unwrap()
                            .scan_nodes(None)
                            .count(),
                        1
                    );
                    cuts += 1;
                }
            }
            assert!(
                observed > 0 && observed < 128,
                "missing or unbounded {event:?} {boundary:?}"
            );
        }
    }
    eprintln!("WAL repair native persistence crash images verified: {cuts}");
}
