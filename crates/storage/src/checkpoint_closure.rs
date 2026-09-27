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

#[derive(Debug)]
pub enum CheckpointClosureError {
    Empty,
    DuplicatePath(PathBuf),
    DuplicateReference(ObjectReference),
    InvalidKind(ObjectKind),
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
}
