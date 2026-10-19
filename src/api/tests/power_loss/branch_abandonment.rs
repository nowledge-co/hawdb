use super::*;
use crate::{BranchAbandonmentLimits, BranchReclamationLimits};
use hawdb_storage::branch_catalog::{self, BranchState, CreateOutcome};
use hawdb_storage::file_io::{self as fs, File};
use hawdb_storage::immutable_object::{ImmutableObjectStore, ObjectKind, ObjectReference};

fn run_abandonment_gc_cut(durability: DurabilityPolicy, case: usize) {
    let started = std::time::Instant::now();
    let mut schedules = 0;
    let mut fixture = Fixture::new();
    let mut database = open(&fixture.root, durability);
    commit_mixed(&mut database);
    let main = database
        .describe_branch(BranchSelector::Name("main".into()))
        .unwrap()
        .id;
    let child = fork(&mut database, main, "abandoned-child");
    let catalog_path = fixture.root.join("branches/catalog.hawdb");
    let mut catalog = branch_catalog::read_catalog(&catalog_path).unwrap();
    let record = catalog
        .branches
        .iter_mut()
        .find(|record| record.id.as_uuid() == child)
        .unwrap();
    record.state = BranchState::Creating;
    record.create_outcome = CreateOutcome::Pending;
    branch_catalog::write_catalog(&catalog_path, &catalog).unwrap();
    let private = PathBuf::from("branches").join(child.to_string());
    let head = fixture.root.join(&private).join("branch.head");
    fs::write(&head, b"damaged pending head").unwrap();
    // Windows requires a writable handle for the persistence barrier.
    File::options()
        .write(true)
        .open(&head)
        .unwrap()
        .sync_all()
        .unwrap();
    let mut objects = ImmutableObjectStore::open(fixture.root.join("branches/objects")).unwrap();
    let orphan = ObjectReference::for_bytes(
        ObjectKind::CheckpointArtifact,
        1,
        b"corrupt orphan qualification",
    );
    objects
        .publish(orphan, b"corrupt orphan qualification")
        .unwrap();
    let orphan_path = objects.object_path(orphan);
    fs::write(&orphan_path, b"retained corrupt object").unwrap();
    File::options()
        .write(true)
        .open(&orphan_path)
        .unwrap()
        .sync_all()
        .unwrap();
    drop(database);
    let mut database = open(&fixture.root, durability);
    let plan = database
        .inspect_pending_branch_abandonment(child, BranchAbandonmentLimits::default())
        .unwrap();
    let acknowledgement = plan.acknowledge_potential_data_loss();
    database
        .abandon_pending_branch(acknowledgement.clone())
        .unwrap();
    let retired = PathBuf::from("branches").join(format!(".reclaim-{child}"));
    let (event, relative_path, descendants) = match case {
        0 => (
            IoEvent::Remove,
            orphan_path
                .strip_prefix(&fixture.root)
                .unwrap()
                .to_path_buf(),
            false,
        ),
        1 => (IoEvent::Rename, retired.clone(), false),
        2 => (IoEvent::Remove, retired.clone(), true),
        3 => (IoEvent::Remove, retired.clone(), false),
        _ => unreachable!(),
    };
    fixture
        .model
        .observe(ObservationPoint {
            event,
            relative_path,
            boundary: ObservationBoundary::After,
            skip_matches: 0,
            include_descendants: descendants,
            keep_last: false,
        })
        .unwrap();
    let report = database
        .reclaim_branch_storage(BranchReclamationLimits::default())
        .unwrap();
    assert_eq!(report.reclaimed_branch_directories, 1);
    let swept = fixture.model.capture().unwrap();
    let cut = fixture
        .model
        .take_observation()
        .unwrap()
        .expect("actual abandonment GC cut");
    drop(database);
    for (snapshot, fault) in publication_fault_plans(&cut)
        .into_iter()
        .map(|fault| (&cut, fault))
        .chain([(&swept, CrashPlan::default())])
    {
        let image = fixture.image(snapshot, &fault);
        let mut reopened = open(&image, durability);
        let report = reopened
            .abandon_pending_branch(acknowledgement.clone())
            .unwrap();
        assert!(report.replayed);
        // Replay verifies every archived byte even when an earlier
        // GC removed some or all of the original branch directory.
        assert!(report.quarantine_directory.join("committed").is_file());
        reopened
            .reclaim_branch_storage(BranchReclamationLimits::default())
            .unwrap();
        assert!(assert_atomic(&mut reopened, main));
        assert!(!image.join(&private).exists());
        assert!(!image.join(&retired).exists());
        schedules += 1;
    }
    eprintln!(
        "branch-abandonment-gc-power-v2 durability={durability:?} cut={case} schedules={schedules} elapsed_ms={}",
        started.elapsed().as_millis()
    );
}

#[test]
fn synchronous_orphan_unlink() {
    run_abandonment_gc_cut(DurabilityPolicy::SyncOnEveryWrite, 0);
}

#[test]
fn synchronous_directory_retirement() {
    run_abandonment_gc_cut(DurabilityPolicy::SyncOnEveryWrite, 1);
}

#[test]
fn synchronous_retired_child_unlink() {
    run_abandonment_gc_cut(DurabilityPolicy::SyncOnEveryWrite, 2);
}

#[test]
fn synchronous_retired_directory_unlink() {
    run_abandonment_gc_cut(DurabilityPolicy::SyncOnEveryWrite, 3);
}

#[test]
fn relaxed_orphan_unlink() {
    run_abandonment_gc_cut(DurabilityPolicy::SyncOnCheckpoint, 0);
}

#[test]
fn relaxed_directory_retirement() {
    run_abandonment_gc_cut(DurabilityPolicy::SyncOnCheckpoint, 1);
}

#[test]
fn relaxed_retired_child_unlink() {
    run_abandonment_gc_cut(DurabilityPolicy::SyncOnCheckpoint, 2);
}

#[test]
fn relaxed_retired_directory_unlink() {
    run_abandonment_gc_cut(DurabilityPolicy::SyncOnCheckpoint, 3);
}
