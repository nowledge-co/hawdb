// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Operation-local immutable validation evidence for post-publication cleanup.

use super::{
    SearchOutOfCoreConfig, SearchOutOfCoreManifestBody, SearchOutOfCoreMutationRunManifest,
    SearchOutOfCoreReader, SearchOutOfCoreSegmentManifest, SearchOutOfCoreSegmentReader,
};
use crate::build_control::checkpoint;
use crate::build_memory::{checked_add as add, checked_mul as mul, BuildMemory};
use crate::{HawDBError, Result, SearchLexicalSourcePolicy, SearchLexicalTermPolicy};
use hawdb_core::RuntimeTaskContext;
use hawdb_executor::QueryMemoryLease;
use std::mem::size_of;

#[derive(Clone, Copy)]
pub(super) struct View<'a> {
    pub(super) manifest: &'a SearchOutOfCoreManifestBody,
    pub(super) segments: &'a [SearchOutOfCoreSegmentReader],
}

impl<'a> View<'a> {
    pub(super) fn from_reader(reader: &'a SearchOutOfCoreReader) -> Self {
        Self {
            manifest: &reader.manifest,
            segments: &reader.segments,
        }
    }
}

#[derive(Debug)]
pub(super) struct ValidatedArtifacts {
    manifest: SearchOutOfCoreManifestBody,
    segments: Vec<SearchOutOfCoreSegmentReader>,
    pub(super) config: SearchOutOfCoreConfig,
    pub(super) source_policy: SearchLexicalSourcePolicy,
    pub(super) term_policy: SearchLexicalTermPolicy,
    // Only reference metadata is copied; descriptor/layout and open files are shared.
    _memory: QueryMemoryLease,
}

impl ValidatedArtifacts {
    pub(super) fn capture(
        reader: &SearchOutOfCoreReader,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        checkpoint(task)?;
        let manifest = &reader.manifest;
        let mut bytes = add(
            size_of::<Self>(),
            mul(
                manifest.segments.len(),
                size_of::<SearchOutOfCoreSegmentManifest>(),
            )?,
        )?;
        bytes = add(
            bytes,
            mul(
                manifest.mutation_runs.len(),
                size_of::<SearchOutOfCoreMutationRunManifest>(),
            )?,
        )?;
        bytes = add(
            bytes,
            mul(
                reader.segments.len(),
                size_of::<SearchOutOfCoreSegmentReader>(),
            )?,
        )?;
        bytes = add(bytes, manifest.format.len())?;
        bytes = add(
            bytes,
            reader
                .config
                .spill_directory
                .as_os_str()
                .as_encoded_bytes()
                .len(),
        )?;
        for identity in [
            manifest.embedding_model.as_deref(),
            manifest.embedding_version.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            bytes = add(bytes, identity.len())?;
        }
        for reference in &manifest.segments {
            checkpoint(task)?;
            for name in [
                reference.descriptor_file.as_str(),
                reference.payload_file.as_str(),
                reference.metadata_payload_file.as_str(),
                reference.vector_payload_file.as_str(),
                reference.layout_file.as_str(),
                reference.lexical_manifest_file.as_str(),
            ]
            .into_iter()
            .chain(reference.rabitq_artifact_file.as_deref())
            {
                bytes = add(bytes, name.len())?;
            }
        }
        for reference in &manifest.mutation_runs {
            checkpoint(task)?;
            bytes = add(bytes, reference.file.len())?;
        }
        let lease = memory.retained.reserve(bytes)?;
        let manifest = manifest.clone();
        let segments = reader.segments.clone();
        if manifest.segments.capacity() != reader.manifest.segments.len()
            || manifest.mutation_runs.capacity() != reader.manifest.mutation_runs.len()
            || segments.capacity() != reader.segments.len()
        {
            return Err(HawDBError::Storage(
                "search cleanup reuse exceeded admitted reference capacity".into(),
            ));
        }
        Ok(Self {
            manifest,
            segments,
            config: reader.config.clone(),
            source_policy: reader.lexical_source_policy,
            term_policy: reader.lexical_term_policy,
            _memory: lease,
        })
    }

    pub(super) fn view(&self) -> View<'_> {
        View {
            manifest: &self.manifest,
            segments: &self.segments,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SearchDocument, SearchOutOfCoreGenerationWriter};
    use hawdb_core::RuntimeMemoryReservation;
    use std::sync::Arc;

    fn reader(name: &str) -> SearchOutOfCoreReader {
        let root = crate::out_of_core::tests::test_dir(name);
        let mut writer =
            SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
        writer
            .push(SearchDocument {
                id: "a".into(),
                title: "alpha".into(),
                content: "shared immutable metadata".into(),
                embedding: None,
                metadata: Default::default(),
            })
            .unwrap();
        writer.finish().unwrap();
        SearchOutOfCoreReader::open(root).unwrap()
    }

    #[test]
    fn cleanup_reuse_admits_reference_copies_and_keeps_shared_artifacts_after_reader_drop() {
        let reader = reader("cleanup_reuse_ownership");
        let task = RuntimeTaskContext::default();
        let memory = BuildMemory::new(&task).unwrap();
        let denied_task = task
            .clone()
            .with_memory_reservation(RuntimeMemoryReservation::new(1, 0));
        let denied = BuildMemory::new(&denied_task).unwrap();
        assert!(ValidatedArtifacts::capture(&reader, &denied, &denied_task).is_err());
        assert_eq!(denied.ledger.snapshot().used_bytes, 0);
        assert_eq!(Arc::strong_count(&reader.segments[0].descriptor), 1);

        let cache = ValidatedArtifacts::capture(&reader, &memory, &task).unwrap();
        assert!(memory.ledger.snapshot().used_bytes > 0);
        assert!(Arc::ptr_eq(
            &reader.segments[0].descriptor,
            &cache.segments[0].descriptor
        ));
        assert!(Arc::ptr_eq(
            &reader.segments[0].layout,
            &cache.segments[0].layout
        ));
        assert!(Arc::ptr_eq(
            &reader.segments[0].payload,
            &cache.segments[0].payload
        ));
        let root = reader.root.clone();
        let analyzer = reader.analyzer_lexicon.clone();
        drop(reader);
        let generations =
            super::super::published_artifact_generations_with_reuse(&root, &analyzer, Some(&cache))
                .unwrap()
                .unwrap();
        assert_eq!(generations.active_generation, 1);
        assert_eq!(generations.out_of_core_generations.len(), 1);
        drop(cache);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn cleanup_reuse_rejects_changed_same_generation_heads_and_missing_artifacts() {
        let reader = reader("cleanup_reuse_integrity");
        let task = RuntimeTaskContext::default();
        let memory = BuildMemory::new(&task).unwrap();
        let cache = ValidatedArtifacts::capture(&reader, &memory, &task).unwrap();
        let head = reader.root.join(super::super::OUT_OF_CORE_MANIFEST_FILE);
        let original = std::fs::read(&head).unwrap();
        let mut changed = reader.manifest.clone();
        changed.import_source_graph_commit_epoch = Some(9);
        std::fs::write(&head, changed.encode().unwrap()).unwrap();
        let error = super::super::published_artifact_generations_with_reuse(
            &reader.root,
            reader.analyzer_lexicon(),
            Some(&cache),
        )
        .err()
        .expect("changed same-generation head must be rejected");
        assert!(
            error.to_string().contains("regressed or changed"),
            "{error}"
        );
        std::fs::write(&head, original).unwrap();
        std::fs::remove_file(reader.root.join(&reader.manifest.segments[0].payload_file)).unwrap();
        assert!(super::super::published_artifact_generations_with_reuse(
            &reader.root,
            reader.analyzer_lexicon(),
            Some(&cache),
        )
        .is_err());
        let root = reader.root.clone();
        drop(reader);
        drop(cache);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        std::fs::remove_dir_all(root).unwrap();
    }
}
