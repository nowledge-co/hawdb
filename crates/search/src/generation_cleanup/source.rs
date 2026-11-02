// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Captured enumeration/unlink authority borrowed from actual publication.
//! Only complete captured retention can authorize either production sweep.
use super::{CleanupCandidate, SearchProjectionCleanupOptions, SearchProjectionGenerations};
use crate::out_of_core::SearchProjectionPublishLease;
use crate::{HawDBError, Result};
use hawdb_storage::file_io::{self as fs, SourceDirectory};
use std::io;
use std::path::Path;

/// Only this owned verification result can authorize production relative
/// enumeration/unlink. Its borrow keeps the actual publication custody live.
pub(crate) struct VerifiedRetention<'a> {
    publication: &'a SearchProjectionPublishLease,
    identity: hawdb_storage::file_io::SourceDirectoryIdentity,
    generations: SearchProjectionGenerations,
    _memory: Option<crate::build_memory::read::ReaderMemory>,
}
impl<'a> VerifiedRetention<'a> {
    pub(crate) fn verify(
        root_label: &Path,
        publication: &'a SearchProjectionPublishLease,
        analyzer: &crate::SearchAnalyzerLexicon,
        options: SearchProjectionCleanupOptions,
        task: &hawdb_core::RuntimeTaskContext,
        admission: Option<&crate::build_memory::read::ReadAdmission<'_>>,
    ) -> Result<Self> {
        let verified = crate::out_of_core::captured_artifact_generations(
            root_label,
            publication,
            analyzer,
            task,
            options,
            admission,
        )?;
        let resident_vector_absence_verified = verified.resident_vector_absence_verified;
        let resident_vector_receipt_verified = verified.resident_vector_receipt_verified;
        let generations =
            verified
                .generations
                .map_or_else(SearchProjectionGenerations::default, |current| {
                    let rabitq = current.rabitq_generations.last().copied();
                    SearchProjectionGenerations {
                        lexical: current.lexical_generations.last().copied(),
                        out_of_core: Some(current.active_generation),
                        retained_lexical: current.lexical_generations,
                        retained_out_of_core: current.out_of_core_generations,
                        // Only the captured, verified closure can name retained
                        // standalone vector generations.
                        rabitq,
                        retained_rabitq: current.rabitq_generations,
                        rabitq_remove_all: resident_vector_absence_verified,
                        out_of_core_discovery_failed: false,
                        rabitq_discovery_failed: !resident_vector_absence_verified
                            && (!resident_vector_receipt_verified || rabitq.is_none()),
                    }
                });
        Ok(Self {
            publication,
            identity: publication.source_identity(),
            generations,
            _memory: verified.memory,
        })
    }
    pub(crate) fn with_source<T>(
        self,
        action: impl FnOnce(CleanupSource<'_>, SearchProjectionGenerations) -> T,
    ) -> Result<T> {
        let Self {
            publication,
            identity,
            generations,
            _memory: memory,
        } = self;
        let result = publication.with_captured_directory(|directory| {
            if directory.identity() != identity {
                return Err(HawDBError::StorageIntegrity(
                    "captured cleanup authority changed".into(),
                ));
            }
            Ok(action(
                CleanupSource {
                    directory,
                    publication,
                },
                generations,
            ))
        });
        // The retention sets have been consumed/dropped by the sweep before
        // their originating governor can reclaim the backing capacity.
        drop(memory);
        result
    }
}

#[derive(Clone, Copy)]
pub(crate) struct CleanupSource<'a> {
    directory: &'a SourceDirectory,
    publication: &'a SearchProjectionPublishLease,
}
impl<'a> CleanupSource<'a> {
    #[cfg(test)]
    pub(crate) fn new(
        root: &Path,
        directory: &'a SourceDirectory,
        publication: &'a SearchProjectionPublishLease,
    ) -> Result<Self> {
        // Until retention discovery is entirely captured-relative, reject a
        // replaced namespace rather than applying a foreign retention set.
        if directory.identity() != publication.source_identity()
            || !directory.matches_namespace(root)?
        {
            return Err(HawDBError::StorageIntegrity(
                "cleanup namespace differs from captured publication".into(),
            ));
        }
        Ok(Self {
            directory,
            publication,
        })
    }
    pub(super) fn protects<S: AsRef<str>>(&self, candidate: &CleanupCandidate<S>) -> bool {
        self.publication.protects(candidate.artifact_key())
    }
    pub(super) fn remove(&self, name: &str) -> io::Result<()> {
        self.directory
            .remove_child(&self.directory.operation_context(), name)
    }
    fn visit(&self, visitor: &mut dyn FnMut(&str) -> io::Result<()>) -> io::Result<()> {
        // Streaming enumeration preserves the existing full-sweep policy;
        // no candidate list or corpus-sized pathname vector is materialized.
        self.directory
            .visit_children(&self.directory.operation_context(), usize::MAX, visitor)
    }
}

pub(super) fn visit_names(
    root: &Path,
    source: Option<CleanupSource<'_>>,
    visitor: &mut dyn FnMut(&str) -> io::Result<()>,
) -> io::Result<()> {
    if let Some(source) = source {
        return source.visit(visitor);
    }
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if let Some(name) = entry.file_name().to_str() {
            visitor(name)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build_memory::BuildMemory;
    use crate::generation_cleanup::{
        once::PreparedCleanup, SearchProjectionCleanupOptions, SearchProjectionCleanupState,
        SearchProjectionGenerations,
    };
    use hawdb_core::RuntimeTaskContext;
    use hawdb_storage::file_descriptors::ProjectFileDescriptors;
    use std::path::PathBuf;

    struct Fixture(PathBuf);
    impl Fixture {
        fn new() -> Self {
            let parent = std::env::temp_dir().join(format!(
                "hawdb-captured-cleanup-{}",
                hawdb_core::generate_uuidv7().unwrap()
            ));
            std::fs::create_dir(&parent).unwrap();
            Self(parent)
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }
    const NAME: &str = "search_projection_segment_payloads.17.hawdb";

    fn publication_before_replacement(
        named: &Path,
        moved: &Path,
        project: &ProjectFileDescriptors,
    ) -> SearchProjectionPublishLease {
        let directory = SourceDirectory::capture(named, &project.io_context()).unwrap();
        // Windows refuses ancestor moves once the child lock is open. Move
        // after native directory capture, before acquiring the child lease;
        // Unix additionally exercises replacement after lease/source binding.
        if cfg!(windows) {
            std::fs::rename(named, moved).unwrap();
        }
        let task = RuntimeTaskContext::default();
        let memory = BuildMemory::new(&task).unwrap();
        let publication =
            SearchProjectionPublishLease::acquire_captured(directory, &memory, &task).unwrap();
        if cfg!(windows) {
            assert!(publication.matches_source_directory(moved).unwrap());
            assert!(!named.exists());
        }
        publication
    }

    #[test]
    fn relative_enumeration_and_unlink_cannot_follow_replacement_after_capture() {
        let fixture = Fixture::new();
        let named = fixture.0.join("named");
        let moved = fixture.0.join("moved");
        let project = ProjectFileDescriptors::acquire(&named, 8).unwrap();
        std::fs::write(named.join(NAME), b"captured").unwrap();
        let publication = publication_before_replacement(&named, &moved, &project);
        let bound_name = if cfg!(windows) { &moved } else { &named };
        publication
            .with_cleanup_source(bound_name, |source| {
                if !cfg!(windows) {
                    std::fs::rename(&named, &moved).unwrap();
                }
                std::fs::create_dir(&named).unwrap();
                std::fs::write(named.join(NAME), b"foreign").unwrap();
                std::fs::write(named.join("foreign-only.hawdb"), b"foreign only").unwrap();
                let mut names = Vec::new();
                visit_names(&named, Some(source), &mut |name| {
                    names.push(name.to_string());
                    Ok(())
                })
                .unwrap();
                assert!(names.iter().any(|name| name == NAME));
                assert!(!names.iter().any(|name| name == "foreign-only.hawdb"));
                source.remove(NAME).unwrap();
            })
            .unwrap();
        assert!(!moved.join(NAME).exists());
        assert_eq!(std::fs::read(named.join(NAME)).unwrap(), b"foreign");
        drop(publication);
        assert_eq!(project.metrics().open, 0);
    }

    #[test]
    fn a_directory_cannot_borrow_a_different_roots_publication_authority() {
        let fixture = Fixture::new();
        let first = fixture.0.join("first");
        let second = fixture.0.join("second");
        let project = ProjectFileDescriptors::acquire(&first, 8).unwrap();
        std::fs::create_dir(&second).unwrap();
        let publication = SearchProjectionPublishLease::acquire_for_cleanup(&first).unwrap();
        let directory = SourceDirectory::capture(&second, &project.io_context()).unwrap();
        assert!(CleanupSource::new(&second, &directory, &publication).is_err());
        drop(directory);
        drop(publication);
        assert_eq!(project.metrics().open, 0);
    }

    #[test]
    fn both_sweeps_refuse_foreign_retention_and_preserve_the_retry_queue() {
        let fixture = Fixture::new();
        let named = fixture.0.join("named");
        let moved = fixture.0.join("moved");
        let project = ProjectFileDescriptors::acquire(&named, 8).unwrap();
        std::fs::write(named.join(NAME), b"captured").unwrap();
        let options = SearchProjectionCleanupOptions::default();
        let generations = SearchProjectionGenerations {
            out_of_core: Some(30),
            ..Default::default()
        };
        let mut state = SearchProjectionCleanupState::default();
        let first = state.run_with_remover(&named, generations.clone(), options, |_| {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "injected refusal",
            ))
        });
        assert_eq!(first.pending_after, 1);
        let task = RuntimeTaskContext::default();
        let memory = BuildMemory::new(&task).unwrap();
        let immediate = PreparedCleanup::prepare(&named, &memory, &task).unwrap();
        let publication = publication_before_replacement(&named, &moved, &project);
        if !cfg!(windows) {
            std::fs::rename(&named, &moved).unwrap();
        }
        std::fs::create_dir(&named).unwrap();
        std::fs::write(named.join(NAME), b"foreign").unwrap();
        assert!(publication
            .with_cleanup_discovery(&named, || -> Result<()> {
                panic!("foreign discovery must never run")
            })
            .is_err());
        let report = state.run_with_bound_test_generations(
            &named,
            generations.clone(),
            options,
            &publication,
        );
        assert!(report.retry_required);
        assert_eq!(report.pending_after, 1);
        assert_eq!(report.delete_attempts, 0);
        assert_eq!(report.scanned_entries, 0);
        assert_eq!(
            report.failure_kinds.get("captured_namespace_unavailable"),
            Some(&1)
        );
        let report = immediate.run_with_bound_test_generations(
            &named,
            generations,
            options,
            &task,
            &publication,
        );
        assert!(report.retry_required);
        assert_eq!(report.deleted_files, 0);
        assert_eq!(std::fs::read(moved.join(NAME)).unwrap(), b"captured");
        assert_eq!(std::fs::read(named.join(NAME)).unwrap(), b"foreign");
        drop(publication);
        assert_eq!(project.metrics().open, 0);
    }
}
