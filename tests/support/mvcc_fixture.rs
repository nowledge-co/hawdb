// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use hawdb::{
    Database, DurabilityPolicy, PreparedReadLimits, ProjectionGenerationBatchLimits,
    ProjectionGenerationBegin, ProjectionGenerationDigestBuilder, ProjectionGenerationIdentity,
    ProjectionGenerationMember, ProjectionGenerationStore, SealedProjectionGeneration,
};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(super) struct TestRoot(pub(super) PathBuf);
impl TestRoot {
    pub(super) fn new() -> Self {
        static NEXT_ROOT: AtomicUsize = AtomicUsize::new(0);
        Self(std::env::temp_dir().join(format!(
            "hawdb_mvcc_publication_{}_{}_{}",
                std::process::id(),
            NEXT_ROOT.fetch_add(1, Ordering::Relaxed),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            )))
    }
}
impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub(super) fn limits() -> PreparedReadLimits {
    PreparedReadLimits {
        dependency_entries: NonZeroUsize::new(32).unwrap(),
        dependency_bytes: NonZeroUsize::new(4096).unwrap(),
        statement_rows: NonZeroUsize::new(32).unwrap(),
        statement_payload_bytes: NonZeroUsize::new(4096).unwrap(),
    }
}

pub(super) fn fixture(policy: DurabilityPolicy) -> (TestRoot, Database) {
    let root = TestRoot::new();
    let mut db = Database::open_with_durability(&root.0, policy)
        .unwrap_or_else(|error| panic!("opening fixture {}: {error}", root.0.display()));
    db.query("CREATE (:Memory {id: 'a', value: 1})").unwrap();
    db.query("CREATE (:Other {id: 'b', value: 1})").unwrap();
    (root, db)
}

pub(super) fn seal(
    store: &ProjectionGenerationStore,
    generation: &str,
    epoch: u64,
    expected: Option<&str>,
) -> SealedProjectionGeneration {
    let mut writer = store
        .begin_candidate(
            ProjectionGenerationBegin {
                identity: ProjectionGenerationIdentity {
                    projection: "test_mvcc".into(),
                    owner_key: b"workspace".to_vec(),
                    generation: generation.into(),
                },
                source_watermark: epoch,
                projection_version: 1,
                expected_head: expected.map(str::to_owned),
            },
            ProjectionGenerationBatchLimits::default(),
        )
        .unwrap();
    let member = ProjectionGenerationMember {
        collection: "values".into(),
        key: b"a".to_vec(),
        payload: b"one".to_vec(),
    };
    writer.append_batch(std::slice::from_ref(&member)).unwrap();
    let mut digest = ProjectionGenerationDigestBuilder::default();
    digest.update(&member).unwrap();
    writer.seal(digest.finish()).unwrap()
}
