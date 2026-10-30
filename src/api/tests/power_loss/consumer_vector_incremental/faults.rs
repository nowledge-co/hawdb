// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Physical cuts in the fixed-projector graph/visibility publication closure.

use super::super::search_projection::same_physical_image;
use super::*;
use hawdb_storage::file_io::SourceDirectory;
use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;

mod recovery;

const LEDGER: &str = "consumer-allocations.hawdb";

#[derive(Debug, Clone, Copy)]
enum Cut {
    Link,
    CandidateDirectory(ObservationBoundary),
    SelectorWrite,
    SelectorRename(ObservationBoundary),
    SelectorDirectory(ObservationBoundary),
}

fn ledger(root: &Path) -> serde_json::Value {
    let bytes = std::fs::read(root.join(LEDGER)).unwrap();
    let start = bytes.iter().position(|byte| *byte == b'\n').unwrap() + 1;
    serde_json::from_slice(&bytes[start..bytes.len() - 4]).unwrap()
}

fn is_candidate(path: &Path) -> bool {
    path.components().next().is_some_and(|component| {
        let component = component.as_os_str().to_string_lossy();
        component == "projection" || component.starts_with("hawdb-generation-")
    })
}

fn assert_preserved_artifacts(
    root: &Path,
    physical: &hawdb_storage::power_loss::image::CrashImage,
) {
    for path in physical.paths().filter(|path| is_candidate(path)) {
        let expected = physical.bytes(path).unwrap();
        let mut file = std::fs::File::open(root.join(path)).unwrap();
        assert_eq!(file.metadata().unwrap().len(), expected.len() as u64);
        // Windows locks can cover bytes past an empty lease file's EOF. Check
        // its length and read exactly the expected bytes without an EOF probe.
        let mut actual = vec![0; expected.len()];
        std::io::Read::read_exact(&mut file, &mut actual).unwrap();
        assert_eq!(
            actual, expected,
            "recovery changed Consumer evidence at {path:?}"
        );
    }
}

fn selected_ids(
    consumer: &SearchProjectionConsumer,
    permit: &hawdb_qos::RuntimePermit,
) -> Vec<String> {
    let pin = consumer.pin(permit).unwrap();
    let output = pin
        .query_vector_index(
            permit,
            Default::default(),
            hawdb_search::VectorIndexQuery {
                descriptor: consumer.vector_index_descriptor().unwrap().unwrap(),
                mode: hawdb_core::VectorIndexQueryMode::AnnWindow,
                eligible: None,
                embedding: &[1.0, 0.0],
                candidate_limit: NonZeroUsize::new(8).unwrap(),
                top_k: NonZeroUsize::new(8).unwrap(),
            },
        )
        .unwrap();
    output.hits().iter().map(|hit| hit.id.clone()).collect()
}

fn mutate(database: &mut Database, graph: bool) {
    let embedding = Value::Vector(Vector::new(&[0.0, 1.0]).unwrap());
    let inserted = Value::Vector(Vector::new(&[1.0, 0.0]).unwrap());
    let mut transaction = database.begin_transaction().unwrap();
    if graph {
        transaction
            .query_with_params(
                "MATCH (d:Document {id: 1}) SET d.embedding = $embedding",
                &BTreeMap::from([("embedding".into(), embedding)]),
            )
            .unwrap();
        transaction
            .query("MATCH (d:Document {id: 2}) DELETE d")
            .unwrap();
        transaction
            .query("MATCH (d:Document {id: 3}) SET d.embedding = NULL")
            .unwrap();
        transaction
            .query_with_params(
                "CREATE (:Document {id: 4, embedding: $embedding})",
                &BTreeMap::from([("embedding".into(), inserted)]),
            )
            .unwrap();
    } else {
        transaction
            .query_sql_with_params(
                "UPDATE vectors SET embedding = $1 WHERE id = 1",
                &[embedding],
            )
            .unwrap();
        transaction
            .query_sql("DELETE FROM vectors WHERE id = 2")
            .unwrap();
        transaction
            .query_sql("UPDATE vectors SET embedding = NULL WHERE id = 3")
            .unwrap();
        transaction
            .query_sql_with_params(
                "INSERT INTO vectors (id, embedding) VALUES (4, $1)",
                &[inserted],
            )
            .unwrap();
    }
    transaction.commit().unwrap();
}

fn assert_source(database: &mut Database, graph: bool, updated: bool) {
    let rows = if graph {
        database
            .query("MATCH (d:Document) RETURN d.id AS id, d.embedding AS embedding ORDER BY id")
            .unwrap()
    } else {
        database
            .query_sql("SELECT id, embedding FROM vectors ORDER BY id")
            .unwrap()
    }
    .rows;
    let expected = if updated {
        vec![(1, Some([0.0, 1.0])), (3, None), (4, Some([1.0, 0.0]))]
    } else {
        vec![
            (1, Some([1.0, 0.0])),
            (2, Some([0.0, 1.0])),
            (3, Some([-1.0, 0.0])),
        ]
    };
    assert_eq!(
        rows,
        expected
            .into_iter()
            .map(|(id, vector)| BTreeMap::from([
                ("id".into(), Value::Int(id)),
                (
                    "embedding".into(),
                    vector.map_or(Value::Null, |vector| Value::Vector(
                        Vector::new(&vector).unwrap()
                    ))
                ),
            ]))
            .collect::<Vec<_>>()
    );
}

fn plans(snapshot: &PowerLossSnapshot, cut: Cut) -> (Vec<CrashPlan>, usize) {
    plans_with_torn_writes(
        snapshot,
        matches!(cut, Cut::SelectorWrite).then_some("consumer-selector-"),
    )
}

fn plans_with_torn_writes(
    snapshot: &PowerLossSnapshot,
    prefix: Option<&str>,
) -> (Vec<CrashPlan>, usize) {
    let mut plans = publication_fault_plans(snapshot);
    let mut torn = 0;
    if let Some(prefix) = prefix {
        let path = snapshot.observed_path().unwrap();
        assert!(path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with(prefix));
        let writes = snapshot.uncovered_writes(path).unwrap();
        assert!(!writes.is_empty());
        for write in writes {
            assert!(write.length > 1);
            for bytes in [0..write.length / 2, write.length / 2..write.length] {
                let mut plan = snapshot.persist_all_plan();
                let operation = plan
                    .persistence
                    .iter_mut()
                    .find(|operation| **operation == PersistOperation::Whole(write.operation))
                    .unwrap();
                *operation = PersistOperation::TornWrite {
                    operation: write.operation,
                    bytes,
                };
                plans.push(plan.clone());
                plan.persistence.reverse();
                plans.push(plan);
                torn += 2;
            }
        }
    }
    (plans, torn)
}

fn check(cut: Cut, durability: DurabilityPolicy, graph: bool) {
    check_recovery(cut, durability, graph, None, false);
}

fn check_recovery(
    cut: Cut,
    durability: DurabilityPolicy,
    graph: bool,
    recovery: Option<recovery::Cut>,
    lose_source: bool,
) {
    let root = std::fs::canonicalize(std::env::temp_dir())
        .unwrap()
        .join(format!(
            "hawdb-vector-publication-power-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
    let original = MaterializedImage(root.clone());
    let project = ProjectFileDescriptors::acquire_with_capacity_limit(&root, 32, true).unwrap();
    let model = Arc::new(PowerLossModel::attach(&project, ImageLimits::default()).unwrap());
    let mut database = open(&root, durability);
    let field = create_source(&mut database, graph);
    for (id, vector) in [(1, [1.0, 0.0]), (2, [0.0, 1.0]), (3, [-1.0, 0.0])] {
        insert(&mut database, graph, id, vector);
    }
    let target = root.join("projection");
    let id = SearchProjectionConsumerId::new("vector-power").unwrap();
    let mut consumer = database
        .create_vector_index(
            "vector-power",
            &field,
            &target,
            SearchProjectionConsumerOptions::new(NonZeroU64::new(100).unwrap()),
            Default::default(),
        )
        .unwrap();
    let c = database.commit_epoch().unwrap();
    let initial_operation = consumer
        .projection()
        .stationary_receipt()
        .unwrap()
        .binding()
        .operation
        .get();
    let before = source_keys(&mut database, graph);
    let permit = database
        .search_projection_consumer_read_permit(
            RuntimeWorkRequest::foreground_query(64 << 20, 1 << 20).with_io_wave_slots(1),
        )
        .unwrap();
    let old = database
        .begin_read_transaction()
        .unwrap()
        .bind_vector_index(&consumer, &permit)
        .unwrap();
    mutate(&mut database, graph);
    let e = database.commit_epoch().unwrap();
    let after = source_keys(&mut database, graph);
    let armed = Rc::new(Cell::new(false));
    let observed = Rc::clone(&armed);
    let recorder = Arc::clone(&model);
    let observed_root = root.clone();
    SourceDirectory::observe_control_renames(
        move || {
            if observed.get() {
                return;
            }
            let value = ledger(&observed_root);
            let Some(row) = value["pending"]
                .as_array()
                .unwrap()
                .iter()
                .find(|row| row["operation"].as_u64().unwrap() > initial_operation)
            else {
                return;
            };
            let selected = value["selected"]
                .as_array()
                .unwrap()
                .iter()
                .any(|selected| selected["operation"] == row["operation"]);
            let (event, relative_path, boundary, include_descendants) = match cut {
                Cut::Link | Cut::CandidateDirectory(_)
                    if row["native_birth"]["state"] == "Recorded"
                        && (!matches!(cut, Cut::CandidateDirectory(_))
                            || row["candidate_seal"].is_object()) =>
                {
                    let parent = Path::new(row["native_birth"]["parent_locator"].as_str().unwrap());
                    let path = parent
                        .join(row["native_birth"]["generation_name"].as_str().unwrap())
                        .strip_prefix(&observed_root)
                        .unwrap()
                        .to_path_buf();
                    match cut {
                        Cut::Link => (
                            IoEvent::HardLink,
                            path.join("search_projection.out_of_core.manifest.hawdb"),
                            ObservationBoundary::After,
                            false,
                        ),
                        Cut::CandidateDirectory(boundary) => {
                            (IoEvent::DirectorySync, path, boundary, false)
                        }
                        _ => unreachable!(),
                    }
                }
                Cut::SelectorWrite if row["publication_intent"].is_object() => (
                    IoEvent::Write,
                    PathBuf::new(),
                    ObservationBoundary::After,
                    true,
                ),
                Cut::SelectorRename(boundary) if row["publication_intent"].is_object() => {
                    (IoEvent::Rename, PathBuf::new(), boundary, true)
                }
                Cut::SelectorDirectory(boundary) if selected => {
                    (IoEvent::DirectorySync, PathBuf::new(), boundary, false)
                }
                _ => return,
            };
            let point = ObservationPoint {
                event,
                relative_path,
                boundary,
                skip_matches: 0,
                include_descendants,
                keep_last: false,
            };
            if matches!(cut, Cut::SelectorWrite | Cut::SelectorRename(_)) {
                // Source synchronization may publish a WAL fence first.
                recorder
                    .observe_file_name_prefix(point, "consumer-selector-")
                    .unwrap();
            } else {
                recorder.observe(point).unwrap();
            }
            observed.set(true);
        },
        || {
            database
                .catch_up_vector_index(&mut consumer, Default::default())
                .unwrap()
        },
    )
    .unwrap();
    assert!(armed.get(), "{cut:?}");
    let acknowledged = model.capture().unwrap();
    let snapshot = model
        .take_observation()
        .unwrap()
        .expect("the actual native publication boundary must execute");
    if matches!(cut, Cut::SelectorRename(_)) {
        assert!(snapshot
            .observed_path()
            .unwrap()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("consumer-selector-"));
    }
    assert_eq!(
        ids(&old, &permit),
        vec![before[&1].clone(), before[&2].clone(), before[&3].clone()]
    );
    assert_eq!(
        current_ids(&mut database, &consumer),
        vec![after[&4].clone(), after[&1].clone()]
    );
    if let Cut::CandidateDirectory(boundary) = cut {
        let manifest = snapshot
            .observed_path()
            .unwrap()
            .join("search_projection.out_of_core.manifest.hawdb");
        assert!(
            snapshot
                .crash(&snapshot.persist_all_plan())
                .unwrap()
                .bytes(&manifest)
                .is_some(),
            "candidate directory cut must retain its sealed artifacts: {boundary:?}"
        );
    }
    let (mut schedules, torn) = if recovery.is_some() {
        // Each recovery case begins at one exact surviving physical image.
        // The ordinary family above still evaluates every first-crash plan.
        (
            vec![if lose_source {
                CrashPlan::default()
            } else {
                snapshot.persist_all_plan()
            }],
            0,
        )
    } else {
        plans(&snapshot, cut)
    };
    assert_eq!(
        torn > 0,
        recovery.is_none() && matches!(cut, Cut::SelectorWrite)
    );
    eprintln!("canonical-vector-power-v1 cut={cut:?} durability={durability:?} graph={graph} plans={} torn={torn}", schedules.len());
    let complete = acknowledged.crash(&CrashPlan::default()).unwrap();
    drop(old);
    drop(permit);
    drop(consumer);
    drop(database);
    assert_eq!(project.metrics().admitted_runtimes, 0);
    drop(model);
    drop(project);
    drop(original);
    if recovery.is_none() {
        schedules.push(CrashPlan::default());
    }
    let total = schedules.len();
    let mut seen = Vec::new();
    let mut replayed = 0;
    for (ordinal, plan) in schedules.into_iter().enumerate() {
        let acknowledged = recovery.is_none() && ordinal + 1 == total;
        let physical = if acknowledged {
            complete.clone()
        } else {
            snapshot.crash(&plan).unwrap()
        };
        // Every plan is evaluated. Full namespace, inode-alias and byte
        // equality means the same reopen input, including private evidence.
        if !acknowledged
            && seen
                .iter()
                .any(|previous| same_physical_image(previous, &physical))
        {
            continue;
        }
        seen.push(physical.clone());
        let image = MaterializedImage(root.clone());
        physical.materialize(&image).unwrap();
        let recovery_project = recovery
            .map(|_| ProjectFileDescriptors::acquire_with_capacity_limit(&root, 32, true).unwrap());
        let recovery_model = recovery_project.as_ref().map(|project| {
            PowerLossModel::resume_from_image(project, &physical, ImageLimits::default()).unwrap()
        });
        replayed += 1;
        let mut recovered = open(&image, durability);
        if let Some(recovery_cut) = recovery {
            recovery_cut.observe(recovery_model.as_ref().unwrap(), matches!(cut, Cut::Link));
        }
        let source_epoch = recovered.commit_epoch().unwrap();
        if lose_source {
            assert_eq!(durability, DurabilityPolicy::SyncOnCheckpoint);
            assert_eq!(
                source_epoch, c,
                "the relaxed E transaction must actually be lost"
            );
        }
        assert!(
            source_epoch == c || source_epoch == e,
            "{cut:?}: source={source_epoch} C={c} E={e}"
        );
        // A private selector is sealed during preparation, before the host
        // synchronizes canonical E. Only selection follows that source barrier.
        if durability == DurabilityPolicy::SyncOnEveryWrite
            || acknowledged
            || matches!(cut, Cut::SelectorRename(_) | Cut::SelectorDirectory(_))
            || plan == snapshot.persist_all_plan()
        {
            assert_eq!(source_epoch, e);
        }
        assert_source(&mut recovered, graph, source_epoch == e);
        let mut consumer = recovered
            .open_search_projection_consumer(&id, &target)
            .unwrap_or_else(|error| {
                panic!("{cut:?} {durability:?} graph={graph} plan={plan:?}: {error:?}")
            });
        let index_epoch = consumer.canonical_vector_source_epoch().unwrap().unwrap();
        let receipt = consumer.projection().stationary_receipt().unwrap().clone();
        let recovery_snapshot = recovery.map(|_| {
            let model = recovery_model.as_ref().unwrap();
            // Complete native traversal proves recovery has no unrecorded IO.
            let acknowledged = model.capture().unwrap();
            (
                model
                    .take_observation()
                    .unwrap()
                    .expect("recovery must execute the actual publication boundary"),
                acknowledged,
            )
        });
        // Logical recovery must preserve every existing candidate byte. It
        // grants no right to adopt a partial producer or delete unknown files.
        assert_preserved_artifacts(&root, &physical);
        assert!(index_epoch == c || index_epoch == e);
        assert!(index_epoch <= source_epoch);
        if acknowledged || matches!(cut, Cut::SelectorDirectory(ObservationBoundary::After)) {
            assert_eq!(index_epoch, e);
        } else if matches!(
            cut,
            Cut::Link
                | Cut::CandidateDirectory(_)
                | Cut::SelectorWrite
                | Cut::SelectorRename(ObservationBoundary::Before)
        ) {
            assert_eq!(index_epoch, c);
        }
        let admission = recovered
            .search_projection_consumer_read_permit(
                RuntimeWorkRequest::foreground_query(64 << 20, 1 << 20).with_io_wave_slots(1),
            )
            .unwrap();
        let expected = if index_epoch == c {
            vec![before[&1].clone(), before[&2].clone(), before[&3].clone()]
        } else {
            vec![after[&4].clone(), after[&1].clone()]
        };
        assert_eq!(selected_ids(&consumer, &admission), expected);
        if index_epoch != source_epoch {
            assert!(matches!(
                recovered
                    .begin_read_transaction()
                    .unwrap()
                    .bind_vector_index(&consumer, &admission),
                Err(crate::VectorIndexReadError::SourceEpochMismatch { .. })
            ));
            recovered
                .catch_up_vector_index(&mut consumer, Default::default())
                .unwrap();
        }
        assert_eq!(
            current_ids(&mut recovered, &consumer),
            if source_epoch == c {
                vec![before[&1].clone(), before[&2].clone(), before[&3].clone()]
            } else {
                vec![after[&4].clone(), after[&1].clone()]
            }
        );
        drop(consumer);
        drop(recovered);
        drop(admission);
        drop(recovery_model);
        drop(recovery_project);
        drop(image);
        if let Some((snapshot, acknowledged)) = recovery_snapshot {
            recovery.unwrap().replay(
                &snapshot,
                &acknowledged,
                recovery::Context {
                    origin: cut,
                    root: &root,
                    target: &target,
                    id: &id,
                    durability,
                    graph,
                    source_epoch,
                    source_updated: source_epoch == e,
                    index_epoch,
                    expected: &expected,
                    original: &physical,
                    receipt: &receipt,
                },
                matches!(cut, Cut::Link),
            );
        }
    }
    eprintln!("canonical-vector-power-replay-v1 cut={cut:?} durability={durability:?} graph={graph} plans={total} distinct_reopens={replayed}");
}

fn qualify(cuts: &[Cut]) {
    for &cut in cuts {
        for durability in [
            DurabilityPolicy::SyncOnEveryWrite,
            DurabilityPolicy::SyncOnCheckpoint,
        ] {
            for graph in [false, true] {
                check(cut, durability, graph);
            }
        }
    }
}

#[test]
fn vector_index_incremental_power_loss_retains_the_linked_artifact_closure() {
    qualify(&[
        Cut::Link,
        Cut::CandidateDirectory(ObservationBoundary::Before),
        Cut::CandidateDirectory(ObservationBoundary::After),
    ]);
}

#[test]
fn vector_index_incremental_power_loss_rejects_torn_private_selectors() {
    qualify(&[Cut::SelectorWrite]);
}

#[test]
fn vector_index_incremental_power_loss_recovers_atomic_selector_and_root_barriers() {
    qualify(&[
        Cut::SelectorRename(ObservationBoundary::Before),
        Cut::SelectorRename(ObservationBoundary::After),
        Cut::SelectorDirectory(ObservationBoundary::Before),
        Cut::SelectorDirectory(ObservationBoundary::After),
    ]);
}
