// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
//! Explicit publication of a validated checkpoint's transitive artifacts.
//!
//! The caller supplies the complete list obtained from the validated durable
//! manifest.  This module deliberately never scans a storage directory or
//! infers dependencies from filenames.

use crate::immutable_object::{
    ImmutableObjectError, ImmutableObjectStore, ObjectKind, ObjectReference,
};
use crate::sealed_root::{SealedRoot, SealedRootError, SealedWalReference};
use std::collections::BTreeSet;
use std::fmt::{self, Display, Formatter};
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct CheckpointArtifactInput {
    pub path: PathBuf,
    pub reference: ObjectReference,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedCheckpointClosure {
    pub references: Vec<ObjectReference>,
    pub total_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CheckpointArtifactFamily {
    Canonical,
    Adjacency,
    PropertySpill,
    PropertyProjection,
    RelationalRow,
    RelationalOverflow,
    RelationalIndex,
    Append,
}

const ALL_FAMILIES: [CheckpointArtifactFamily; 8] = [
    CheckpointArtifactFamily::Canonical,
    CheckpointArtifactFamily::Adjacency,
    CheckpointArtifactFamily::PropertySpill,
    CheckpointArtifactFamily::PropertyProjection,
    CheckpointArtifactFamily::RelationalRow,
    CheckpointArtifactFamily::RelationalOverflow,
    CheckpointArtifactFamily::RelationalIndex,
    CheckpointArtifactFamily::Append,
];

/// Accumulates the manifest bindings and the descendant artifacts proven by
/// each family reader.  A caller must explicitly mark every family present or
/// absent before publication; omission cannot silently become an incomplete
/// closure.
#[derive(Debug, Default)]
pub struct CheckpointClosurePlan {
    inputs: Vec<CheckpointArtifactInput>,
    completed_families: BTreeSet<CheckpointArtifactFamily>,
}

impl CheckpointClosurePlan {
    pub fn new(inputs: Vec<CheckpointArtifactInput>) -> Self {
        Self {
            inputs,
            completed_families: BTreeSet::new(),
        }
    }

    pub fn add_family_artifacts(
        &mut self,
        family: CheckpointArtifactFamily,
        inputs: impl IntoIterator<Item = CheckpointArtifactInput>,
    ) -> Result<(), CheckpointClosureError> {
        self.complete_family(family)?;
        self.inputs.extend(inputs);
        Ok(())
    }

    /// Adds a family's physical artifacts while coalescing files with the
    /// same immutable content identity. Multiple physical generations may
    /// legitimately share one content-addressed object.
    pub fn add_family_artifacts_deduplicating(
        &mut self,
        family: CheckpointArtifactFamily,
        inputs: impl IntoIterator<Item = CheckpointArtifactInput>,
    ) -> Result<(), CheckpointClosureError> {
        self.complete_family(family)?;
        let existing = self
            .inputs
            .iter()
            .map(|input| input.reference)
            .collect::<BTreeSet<_>>();
        let mut references = existing;
        self.inputs.extend(
            inputs
                .into_iter()
                .filter(|input| references.insert(input.reference)),
        );
        Ok(())
    }

    pub fn inputs(&self) -> &[CheckpointArtifactInput] {
        &self.inputs
    }

    pub fn mark_family_empty(
        &mut self,
        family: CheckpointArtifactFamily,
    ) -> Result<(), CheckpointClosureError> {
        self.complete_family(family)
    }

    pub fn publish(
        self,
        store: &mut ImmutableObjectStore,
    ) -> Result<PublishedCheckpointClosure, CheckpointClosureError> {
        let missing = ALL_FAMILIES
            .into_iter()
            .filter(|family| !self.completed_families.contains(family))
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(CheckpointClosureError::Incomplete { missing });
        }
        publish_checkpoint_closure(store, &self.inputs)
    }

    fn complete_family(
        &mut self,
        family: CheckpointArtifactFamily,
    ) -> Result<(), CheckpointClosureError> {
        if !self.completed_families.insert(family) {
            return Err(CheckpointClosureError::FamilyAlreadyBound(family));
        }
        Ok(())
    }
}

/// Builds the root metadata only after every checkpoint artifact has been
/// published and validated.  WAL references are supplied by the sealing
/// boundary and remain ordered/interval-checked by `SealedRoot::validate`.
pub fn build_sealed_root(
    closure: &PublishedCheckpointClosure,
    checkpoint_epoch: u64,
    commit_epoch: u64,
    wal_replay_start_lsn: u64,
    sealed_wals: Vec<SealedWalReference>,
) -> Result<SealedRoot, SealedRootError> {
    let root = SealedRoot {
        checkpoint_epoch,
        commit_epoch,
        wal_replay_start_lsn,
        checkpoint_references: closure.references.clone(),
        sealed_wals,
    };
    root.validate()?;
    Ok(root)
}

#[derive(Debug)]
pub enum CheckpointClosureError {
    Empty,
    DuplicatePath(PathBuf),
    DuplicateReference(ObjectReference),
    InvalidKind(ObjectKind),
    FamilyAlreadyBound(CheckpointArtifactFamily),
    Incomplete {
        missing: Vec<CheckpointArtifactFamily>,
    },
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    Publish(ImmutableObjectError),
}

impl Display for CheckpointClosureError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("checkpoint closure is empty"),
            Self::DuplicatePath(path) => write!(f, "checkpoint closure repeats {}", path.display()),
            Self::DuplicateReference(reference) => {
                write!(f, "checkpoint closure repeats {reference:?}")
            }
            Self::InvalidKind(kind) => write!(f, "invalid checkpoint closure object kind {kind:?}"),
            Self::FamilyAlreadyBound(family) => {
                write!(f, "checkpoint closure family is already bound: {family:?}")
            }
            Self::Incomplete { missing } => {
                write!(
                    f,
                    "checkpoint closure is missing family decisions: {missing:?}"
                )
            }
            Self::Io { path, source } => {
                write!(f, "read checkpoint artifact {}: {source}", path.display())
            }
            Self::Publish(error) => Display::fmt(error, f),
        }
    }
}

impl std::error::Error for CheckpointClosureError {}

/// Publishes every manifest-bound artifact and returns its canonical order.
pub fn publish_checkpoint_closure(
    store: &mut ImmutableObjectStore,
    inputs: &[CheckpointArtifactInput],
) -> Result<PublishedCheckpointClosure, CheckpointClosureError> {
    if inputs.is_empty() {
        return Err(CheckpointClosureError::Empty);
    }
    let mut paths = BTreeSet::new();
    let mut references = BTreeSet::new();
    let mut published = Vec::with_capacity(inputs.len());
    let mut total_bytes = 0_u64;

    for input in inputs {
        if !matches!(
            input.reference.kind,
            ObjectKind::Checkpoint | ObjectKind::CheckpointArtifact
        ) {
            return Err(CheckpointClosureError::InvalidKind(input.reference.kind));
        }
        if !paths.insert(input.path.clone()) {
            return Err(CheckpointClosureError::DuplicatePath(input.path.clone()));
        }
        if !references.insert(input.reference) {
            return Err(CheckpointClosureError::DuplicateReference(input.reference));
        }
        let bytes = fs::read(&input.path).map_err(|source| CheckpointClosureError::Io {
            path: input.path.clone(),
            source,
        })?;
        total_bytes = total_bytes.saturating_add(bytes.len() as u64);
        let computed = ObjectReference::for_bytes(
            input.reference.kind,
            input.reference.format_version,
            &bytes,
        );
        if computed != input.reference {
            return Err(CheckpointClosureError::Publish(
                ImmutableObjectError::ReferenceMismatch,
            ));
        }
        store
            .publish(input.reference, &bytes)
            .map_err(CheckpointClosureError::Publish)?;
        published.push(input.reference);
    }
    published.sort_unstable();
    Ok(PublishedCheckpointClosure {
        references: published,
        total_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TempDir(PathBuf);
    impl TempDir {
        fn new() -> Self {
            let suffix = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!("hawdb-closure-{suffix}"));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
        fn path(&self) -> &std::path::Path {
            &self.0
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn input(path: PathBuf, kind: ObjectKind, bytes: &[u8]) -> CheckpointArtifactInput {
        CheckpointArtifactInput {
            path,
            reference: ObjectReference::for_bytes(kind, 1, bytes),
        }
    }

    #[test]
    fn publishes_explicit_complete_closure_in_canonical_order() {
        let dir = TempDir::new();
        let file_a = dir.path().join("a");
        let file_b = dir.path().join("b");
        fs::write(&file_a, b"artifact-a").unwrap();
        fs::write(&file_b, b"checkpoint").unwrap();
        let mut store = ImmutableObjectStore::open(dir.path().join("objects")).unwrap();
        let result = publish_checkpoint_closure(
            &mut store,
            &[
                input(file_a, ObjectKind::CheckpointArtifact, b"artifact-a"),
                input(file_b, ObjectKind::Checkpoint, b"checkpoint"),
            ],
        )
        .unwrap();
        assert_eq!(result.references.len(), 2);
        assert_eq!(result.total_bytes, 20);
        assert!(result.references[0] < result.references[1]);
    }

    #[test]
    fn rejects_missing_or_mismatched_dependencies_without_partial_lie() {
        let dir = TempDir::new();
        let missing = dir.path().join("missing");
        let mut store = ImmutableObjectStore::open(dir.path().join("objects")).unwrap();
        let reference = ObjectReference::for_bytes(ObjectKind::Checkpoint, 1, b"expected");
        let error = publish_checkpoint_closure(
            &mut store,
            &[CheckpointArtifactInput {
                path: missing,
                reference,
            }],
        )
        .unwrap_err();
        assert!(matches!(error, CheckpointClosureError::Io { .. }));
    }

    #[test]
    fn root_builder_accepts_only_the_published_closure() {
        let dir = TempDir::new();
        let file = dir.path().join("checkpoint");
        fs::write(&file, b"checkpoint").unwrap();
        let mut store = ImmutableObjectStore::open(dir.path().join("objects")).unwrap();
        let closure = publish_checkpoint_closure(
            &mut store,
            &[input(file, ObjectKind::Checkpoint, b"checkpoint")],
        )
        .unwrap();
        let root = build_sealed_root(&closure, 3, 4, 10, Vec::new()).unwrap();
        assert_eq!(root.checkpoint_references, closure.references);
    }

    #[test]
    fn plan_requires_an_explicit_decision_for_each_family() {
        let dir = TempDir::new();
        let file = dir.path().join("checkpoint");
        fs::write(&file, b"checkpoint").unwrap();
        let reference = ObjectReference::for_bytes(ObjectKind::Checkpoint, 1, b"checkpoint");
        let mut plan = CheckpointClosurePlan::new(vec![CheckpointArtifactInput {
            path: file,
            reference,
        }]);
        plan.mark_family_empty(CheckpointArtifactFamily::Canonical)
            .unwrap();
        let mut store = ImmutableObjectStore::open(dir.path().join("objects")).unwrap();
        let error = plan.publish(&mut store).unwrap_err();
        assert!(matches!(error, CheckpointClosureError::Incomplete { .. }));
    }

    #[test]
    fn deduplicating_family_coalesces_identical_physical_artifacts() {
        let dir = TempDir::new();
        let checkpoint = dir.path().join("checkpoint");
        let first = dir.path().join("first");
        let second = dir.path().join("second");
        fs::write(&checkpoint, b"checkpoint").unwrap();
        fs::write(&first, b"same-bytes").unwrap();
        fs::write(&second, b"same-bytes").unwrap();
        let mut plan = CheckpointClosurePlan::new(vec![input(
            checkpoint,
            ObjectKind::Checkpoint,
            b"checkpoint",
        )]);
        plan.add_family_artifacts_deduplicating(
            CheckpointArtifactFamily::Canonical,
            vec![
                input(first, ObjectKind::CheckpointArtifact, b"same-bytes"),
                input(second, ObjectKind::CheckpointArtifact, b"same-bytes"),
            ],
        )
        .unwrap();
        for family in [
            CheckpointArtifactFamily::Adjacency,
            CheckpointArtifactFamily::PropertySpill,
            CheckpointArtifactFamily::PropertyProjection,
            CheckpointArtifactFamily::RelationalRow,
            CheckpointArtifactFamily::RelationalOverflow,
            CheckpointArtifactFamily::RelationalIndex,
            CheckpointArtifactFamily::Append,
        ] {
            plan.mark_family_empty(family).unwrap();
        }
        let mut store = ImmutableObjectStore::open(dir.path().join("objects")).unwrap();
        let closure = plan.publish(&mut store).unwrap();
        assert_eq!(closure.references.len(), 2);
    }
}
