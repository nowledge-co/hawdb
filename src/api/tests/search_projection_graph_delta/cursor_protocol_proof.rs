//! Private feasibility evidence for issue #455; not a production cursor protocol.

use super::*;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;

const CRASH_STAGE: &str = "SKEIN_CURSOR_PROOF_CRASH_STAGE";
const CRASH_ROOT: &str = "SKEIN_CURSOR_PROOF_CRASH_ROOT";

#[derive(Debug, Clone, PartialEq, Eq)]
struct CheckpointWitness {
    epoch: u64,
    snapshot_digest: String,
}

impl CheckpointWitness {
    fn checkpoint(index: &SearchIndex, path: &Path) -> Self {
        index.checkpoint().expect("durable projection checkpoint");
        let epoch = index
            .projection_freshness()
            .durable_source_graph_commit_epoch
            .expect("locally sourced projection");
        Self {
            epoch,
            snapshot_digest: snapshot_digest(path),
        }
    }

    fn matches(&self, index: &SearchIndex, path: &Path) -> bool {
        index
            .projection_freshness()
            .durable_source_graph_commit_epoch
            == Some(self.epoch)
            && snapshot_digest(path) == self.snapshot_digest
    }

    fn persist(&self, path: &Path) {
        let temporary = path.with_extension("tmp");
        let mut file = File::create(&temporary).unwrap();
        writeln!(file, "PRIVATE_CURSOR_PROOF_V1").unwrap();
        writeln!(file, "{}", self.epoch).unwrap();
        writeln!(file, "{}", self.snapshot_digest).unwrap();
        file.sync_all().unwrap();
        drop(file);
        skein_storage::durable_replace_file(&temporary, path).unwrap();
    }

    fn load(path: &Path) -> Self {
        let text = fs::read_to_string(path).unwrap();
        let mut lines = text.lines();
        assert_eq!(lines.next(), Some("PRIVATE_CURSOR_PROOF_V1"));
        let epoch = lines.next().unwrap().parse().unwrap();
        let snapshot_digest = lines.next().unwrap().to_string();
        assert_eq!(snapshot_digest.len(), 64);
        assert!(lines.next().is_none());
        Self {
            epoch,
            snapshot_digest,
        }
    }
}

fn snapshot_digest(path: &Path) -> String {
    let mut file = File::open(path.join("search_projection.skein")).unwrap();
    let mut hasher = skein_integrity::IntegrityHasher::new();
    let mut buffer = [0u8; 8192];
    loop {
        let count = file.read(&mut buffer).unwrap();
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    hasher.finish().sha256.to_string()
}

fn seed_projection(db: &Database, path: &Path) -> SearchIndex {
    let mut index = SearchIndex::open(path).unwrap();
    db.rebuild_search_projection(&mut index, SearchRebuildOptions::default())
        .unwrap();
    index.checkpoint().unwrap();
    index
}

#[test]
fn path_derived_cache_identity_does_not_identify_a_database_incarnation() {
    let root = unique_test_dir("cursor_proof_path_identity");
    let original = root.join("database");
    let moved = root.join("moved");
    fs::create_dir_all(&original).unwrap();
    let before = skein_storage::artifact_files::store_id_for_path(&original).unwrap();
    fs::rename(&original, &moved).unwrap();
    let after_move = skein_storage::artifact_files::store_id_for_path(&moved).unwrap();
    fs::create_dir(&original).unwrap();
    let replacement = skein_storage::artifact_files::store_id_for_path(&original).unwrap();
    assert_ne!(before, after_move);
    assert_eq!(before, replacement);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn equal_durable_freshness_does_not_prove_database_projection_identity() {
    let root = unique_test_dir("cursor_proof_freshness_identity");
    let mut first = Database::new();
    let mut second = Database::new();
    first
        .query("CREATE (:Memory {id: 'first', title: 'First source'})")
        .unwrap();
    second
        .query("CREATE (:Memory {id: 'second', title: 'Second source'})")
        .unwrap();
    let first_index = seed_projection(&first, &root.join("first"));
    let second_index = seed_projection(&second, &root.join("second"));
    assert_eq!(
        first_index.projection_freshness(),
        second_index.projection_freshness()
    );
    assert!(first_index.document("memory:first").is_some());
    assert!(second_index.document("memory:first").is_none());
    assert_ne!(
        snapshot_digest(&root.join("first")),
        snapshot_digest(&root.join("second"))
    );
    drop((first_index, second_index));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn ordinary_relational_cursor_metadata_changes_the_source_epoch_and_changefeed() {
    let mut db = Database::new();
    db.query_sql("CREATE TABLE cursor_proof (id TEXT PRIMARY KEY, epoch BIGINT NOT NULL)")
        .unwrap();
    let before = db.commit_epoch();
    db.query_sql("INSERT INTO cursor_proof (id, epoch) VALUES ('search', 0)")
        .unwrap();
    assert_eq!(db.commit_epoch(), before + 1);
    let batch = db
        .build_search_projection_change_batch_after(before, Some(1))
        .unwrap()
        .unwrap();
    assert!(batch.has_relational_changes());
    assert_eq!(
        batch.complete_through_commit_epoch(),
        Some(db.commit_epoch())
    );
}

#[test]
fn unchanged_epoch_does_not_prove_unchanged_projection_contents() {
    let root = unique_test_dir("cursor_proof_direct_projection_mutation");
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'm1', title: 'Original'})")
        .unwrap();
    let mut index = seed_projection(&db, &root);
    let before = index.projection_freshness();
    let witness = CheckpointWitness::checkpoint(&index, &root);
    index
        .upsert(SearchDocument {
            id: "memory:m1".to_string(),
            title: "Changed outside changefeed".to_string(),
            content: "external mutation".to_string(),
            embedding: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();
    assert_eq!(index.projection_freshness(), before);
    index.checkpoint().unwrap();
    assert_eq!(index.projection_freshness(), before);
    assert!(!witness.matches(&index, &root));
    drop(index);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn hard_entry_and_byte_floors_remain_authoritative_for_a_stalled_cursor() {
    for (name, entries, bytes) in [
        ("entries", Some(1), None),
        ("bytes", None, Some(1)),
        ("zero", Some(0), Some(0)),
    ] {
        let mut db = Database::new_with_config(DatabaseConfig {
            max_search_projection_change_log_entries: entries,
            max_search_projection_change_log_bytes: bytes,
            ..DatabaseConfig::default()
        });
        db.query("CREATE (:Memory {id: 'm1', title: 'One'})")
            .unwrap();
        let stalled_cursor = db.commit_epoch();
        db.query("CREATE (:Memory {id: 'm2', title: 'Two'})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'm3', title: 'Three'})")
            .unwrap();
        let status = db.store.search_projection_changefeed_status();
        assert!(
            entries.is_none_or(|limit| status.retained_mutation_count <= limit),
            "{name}"
        );
        assert!(
            bytes.is_none_or(|limit| status.retained_bytes <= limit),
            "{name}"
        );
        assert!(stalled_cursor < status.resume_floor_commit_epoch, "{name}");
        assert!(
            db.build_search_projection_change_batch_after(stalled_cursor, Some(8))
                .unwrap_err()
                .to_string()
                .contains("full search projection rebuild required"),
            "{name}"
        );
    }
}

#[test]
fn cursor_file_publication_does_not_create_another_source_commit() {
    let root = unique_test_dir("cursor_proof_metadata_epoch");
    let mut db = Database::open(root.join("database")).unwrap();
    db.query("CREATE (:Memory {id: 'm1', title: 'One'})")
        .unwrap();
    let projection = root.join("projection");
    let index = seed_projection(&db, &projection);
    let before = db.store.search_projection_changefeed_status();
    let witness = CheckpointWitness::checkpoint(&index, &projection);
    witness.persist(&root.join("cursor-proof"));
    assert_eq!(db.store.search_projection_changefeed_status(), before);
    drop((index, db));
    let reopened = Database::open(root.join("database")).unwrap();
    let reopened_index = SearchIndex::open(&projection).unwrap();
    assert_eq!(reopened.commit_epoch(), before.graph_commit_epoch);
    assert!(
        CheckpointWitness::load(&root.join("cursor-proof")).matches(&reopened_index, &projection)
    );
    drop((reopened, reopened_index));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn crash_child() {
    let Ok(stage) = std::env::var(CRASH_STAGE) else {
        return;
    };
    let root = std::path::PathBuf::from(std::env::var_os(CRASH_ROOT).unwrap());
    let mut db = Database::open(root.join("database")).unwrap();
    let projection = root.join("projection");
    let mut index = SearchIndex::open(&projection).unwrap();
    db.query("CREATE (:Memory {id: 'm2', title: 'Two'})")
        .unwrap();
    let request = db
        .build_search_projection_graph_delta_request_from_freshness(&index, Some(8))
        .unwrap()
        .unwrap();
    db.apply_search_projection_graph_delta(&mut index, request)
        .unwrap();
    if stage == "before_checkpoint" {
        std::process::exit(86);
    }
    let witness = CheckpointWitness::checkpoint(&index, &projection);
    if stage == "after_checkpoint" {
        std::process::exit(86);
    }
    witness.persist(&root.join("cursor-proof"));
    if stage == "after_cursor" {
        std::process::exit(86);
    }
    panic!("unknown crash stage {stage}");
}

#[test]
fn process_crashes_preserve_checkpoint_before_cursor_ordering() {
    for stage in ["before_checkpoint", "after_checkpoint", "after_cursor"] {
        let root = unique_test_dir(&format!("cursor_proof_crash_{stage}"));
        let projection = root.join("projection");
        let epoch = {
            let mut db = Database::open(root.join("database")).unwrap();
            db.query("CREATE (:Memory {id: 'm1', title: 'One'})")
                .unwrap();
            db.checkpoint().unwrap();
            let index = seed_projection(&db, &projection);
            let witness = CheckpointWitness::checkpoint(&index, &projection);
            witness.persist(&root.join("cursor-proof"));
            witness.epoch
        };
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "api::tests::search_projection_graph_delta::cursor_protocol_proof::crash_child",
                "--nocapture",
            ])
            .env(CRASH_STAGE, stage)
            .env(CRASH_ROOT, &root)
            .status()
            .unwrap();
        assert_eq!(child.code(), Some(86), "{stage}");
        let db = Database::open(root.join("database")).unwrap();
        let mut index = SearchIndex::open(&projection).unwrap();
        let cursor = CheckpointWitness::load(&root.join("cursor-proof"));
        let durable = index
            .projection_freshness()
            .durable_source_graph_commit_epoch
            .unwrap();
        assert!(cursor.epoch <= durable, "{stage}");
        assert_eq!(cursor.epoch, epoch + u64::from(stage == "after_cursor"));
        assert_eq!(durable, epoch + u64::from(stage != "before_checkpoint"));
        assert!(db
            .store
            .search_projection_changefeed_status()
            .can_resume_after(cursor.epoch));
        // A newer projection with an older witness must be revalidated, not silently trusted.
        assert_eq!(
            cursor.matches(&index, &projection),
            stage != "after_checkpoint"
        );
        db.catch_up_search_projection(&mut index, 8, 8).unwrap();
        assert_eq!(index.document_count(), 2);
        assert!(index.document("memory:m1").is_some());
        assert!(index.document("memory:m2").is_some());
        drop((index, db));
        fs::remove_dir_all(root).unwrap();
    }
}

fn initialize_from_pinned_snapshot<F>(
    db: &mut Database,
    projection: &Path,
    initialize: F,
) -> crate::Result<CheckpointWitness>
where
    F: FnOnce(&mut DatabaseReadTransaction, &mut SearchIndex) -> crate::Result<()>,
{
    if db.store.wal_sync_group_active() {
        return Err(SkeinError::Storage(
            "source WAL sync is deferred".to_string(),
        ));
    }
    let mut snapshot = db.begin_read_transaction();
    let epoch = snapshot.commit_epoch();
    let mut index = SearchIndex::open(projection)?;
    initialize(&mut snapshot, &mut index)?;
    index.apply_projection_delta(SearchProjectionDelta {
        source_graph_commit_epoch: Some(epoch),
        max_operations: Some(0),
        ..SearchProjectionDelta::default()
    })?;
    Ok(CheckpointWitness::checkpoint(&index, projection))
}

#[test]
fn pinned_initializer_can_cover_graph_and_relational_projection_content() {
    let root = unique_test_dir("cursor_proof_mixed_initializer");
    let mut db = Database::open(root.join("database")).unwrap();
    db.query("CREATE (:Memory {id: 'm1', title: 'Graph document'})")
        .unwrap();
    db.query_sql("CREATE TABLE cursor_messages (id BIGINT PRIMARY KEY, body TEXT NOT NULL)")
        .unwrap();
    db.query_sql("INSERT INTO cursor_messages (id, body) VALUES (1, 'Relational document')")
        .unwrap();
    let epoch = db.commit_epoch();
    let projection = root.join("projection");
    let witness = initialize_from_pinned_snapshot(&mut db, &projection, |snapshot, index| {
        snapshot.rebuild_search_projection(index, SearchRebuildOptions { max_rows: Some(1) })?;
        let output = snapshot.query_sql_with_params_bounded(
            "SELECT body FROM cursor_messages WHERE id = $1 LIMIT $2",
            &[Value::Int(1), Value::Int(1)],
            Some(1),
        )?;
        let Value::String(body) = &output.rows[0]["body"] else {
            panic!("expected message body");
        };
        index.upsert(SearchDocument {
            id: "thread:1".to_string(),
            title: "message".to_string(),
            content: body.clone(),
            embedding: None,
            metadata: BTreeMap::new(),
        })
    })
    .unwrap();
    assert_eq!(witness.epoch, epoch);
    assert_eq!(db.commit_epoch(), epoch);
    let index = SearchIndex::open(&projection).unwrap();
    assert!(witness.matches(&index, &projection));
    assert!(index.document("memory:m1").is_some());
    assert_eq!(
        index.document("thread:1").unwrap().content,
        "Relational document"
    );
    drop((index, db));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn initializer_failure_and_deferred_source_sync_do_not_publish_a_checkpoint() {
    let root = unique_test_dir("cursor_proof_initializer_rejection");
    let mut db = Database::open(root.join("database")).unwrap();
    let failed = root.join("failed");
    let error = initialize_from_pinned_snapshot(&mut db, &failed, |_, index| {
        index.upsert(SearchDocument {
            id: "partial".to_string(),
            title: "partial".to_string(),
            content: "partial".to_string(),
            embedding: None,
            metadata: BTreeMap::new(),
        })?;
        Err(SkeinError::Execution("hydration failed".to_string()))
    })
    .unwrap_err();
    assert!(error.to_string().contains("hydration failed"));
    assert!(!failed.join("search_projection.skein").exists());
    assert!(db.begin_wal_sync_group().unwrap());
    let invoked = std::cell::Cell::new(false);
    let deferred = root.join("deferred");
    let error = initialize_from_pinned_snapshot(&mut db, &deferred, |_, _| {
        invoked.set(true);
        Ok(())
    })
    .unwrap_err();
    assert!(error.to_string().contains("source WAL sync is deferred"));
    assert!(!invoked.get());
    assert!(!deferred.exists());
    db.finish_wal_sync_group().unwrap();
    drop(db);
    fs::remove_dir_all(root).unwrap();
}
