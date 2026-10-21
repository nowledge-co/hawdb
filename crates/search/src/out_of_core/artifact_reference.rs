// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Version-four producer references. The wire fixes kind/version to
//! CheckpointArtifact/1; there is no absent-reference or native-ID fallback.

use super::*;
use hawdb_integrity::Sha256Digest;
use hawdb_storage::immutable_object::{ObjectKind, ObjectReference};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ArtifactObject {
    pub(super) byte_length: u64,
    // A fixed array avoids per-reference String allocations in decoded views.
    sha256: [u8; 32],
}
impl ArtifactObject {
    pub(super) fn from_reference(reference: ObjectReference) -> Result<Self> {
        if reference.kind != ObjectKind::CheckpointArtifact || reference.format_version != 1 {
            return Err(HawDBError::StorageIntegrity(
                "invalid Search artifact identity domain".into(),
            ));
        }
        Ok(Self {
            byte_length: reference.byte_length,
            sha256: *reference.sha256.as_bytes(),
        })
    }
    pub(super) fn reference(self) -> ObjectReference {
        ObjectReference {
            kind: ObjectKind::CheckpointArtifact,
            format_version: 1,
            byte_length: self.byte_length,
            sha256: Sha256Digest::from_bytes(self.sha256),
        }
    }
    pub(super) fn for_bytes(bytes: &[u8]) -> Self {
        let reference = ObjectReference::for_bytes(ObjectKind::CheckpointArtifact, 1, bytes);
        Self {
            byte_length: reference.byte_length,
            sha256: *reference.sha256.as_bytes(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SegmentObjects {
    pub(super) lexical_generation: u64,
    pub(super) descriptor: ArtifactObject,
    pub(super) payload: ArtifactObject,
    pub(super) metadata: ArtifactObject,
    pub(super) vector: ArtifactObject,
    pub(super) layout: ArtifactObject,
    pub(super) lexical_manifest: ArtifactObject,
    pub(super) lexical_payload: ArtifactObject,
    pub(super) rabitq: Option<ArtifactObject>,
}
impl SegmentObjects {
    pub(super) fn validate<S: AsRef<str>>(
        self,
        segment: &SearchOutOfCoreSegmentManifest<S>,
    ) -> Result<()> {
        let valid = self.lexical_generation != 0
            && self.descriptor.byte_length == segment.descriptor_len
            && self.payload.byte_length == segment.payload_len
            && self.metadata.byte_length == segment.metadata_payload_len
            && self.vector.byte_length == segment.vector_payload_len
            && self.layout.byte_length == segment.layout_len
            && self.lexical_manifest.byte_length == segment.lexical_manifest_len
            && self.rabitq.map(|object| object.byte_length) == segment.rabitq_artifact_len;
        if !valid {
            return Err(HawDBError::StorageIntegrity(
                "Search artifact reference lengths disagree with manifest".into(),
            ));
        }
        Ok(())
    }
}

impl<S: AsRef<str>> SearchOutOfCoreSegmentManifest<S> {
    pub(super) fn object_for(&self, name: &str) -> Option<ArtifactObject> {
        let objects = self.objects;
        for (file, object) in [
            (self.descriptor_file.as_ref(), objects.descriptor),
            (self.payload_file.as_ref(), objects.payload),
            (self.metadata_payload_file.as_ref(), objects.metadata),
            (self.vector_payload_file.as_ref(), objects.vector),
            (self.layout_file.as_ref(), objects.layout),
            (
                self.lexical_manifest_file.as_ref(),
                objects.lexical_manifest,
            ),
        ] {
            if file == name {
                return Some(object);
            }
        }
        if self
            .rabitq_artifact_file
            .as_ref()
            .is_some_and(|file| file.as_ref() == name)
        {
            return objects.rabitq;
        }
        if name.starts_with("search_lexical.")
            && !name.starts_with("search_lexical.manifest.")
            && artifact_key::ArtifactKey::from_file_name(name)
                == Some(artifact_key::ArtifactKey::Lexical(
                    objects.lexical_generation,
                ))
        {
            return Some(objects.lexical_payload);
        }
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ResidentSnapshot {
    pub(super) object: ArtifactObject,
    pub(super) vector_document_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SearchDocument, SearchOutOfCoreGenerationWriter, SearchProjectionDelta};
    use std::collections::BTreeMap;

    fn document(id: &str) -> SearchDocument {
        SearchDocument {
            id: id.into(),
            title: "producer reference".into(),
            content: "bounded immutable graph payload".into(),
            embedding: Some(vec![1.0, 0.5]),
            metadata: BTreeMap::from([("kind".into(), "memory".into())]),
        }
    }
    fn check(root: &Path, manifest: &SearchOutOfCoreManifestBody) {
        let matches = |name: &str, object: ArtifactObject| {
            let bytes = std::fs::read(root.join(name)).unwrap();
            assert_eq!(
                object.reference(),
                ObjectReference::for_bytes(ObjectKind::CheckpointArtifact, 1, &bytes),
                "{name}"
            );
        };
        for segment in &manifest.segments {
            let objects = segment.objects;
            for (name, reference) in [
                (&segment.descriptor_file, objects.descriptor),
                (&segment.payload_file, objects.payload),
                (&segment.metadata_payload_file, objects.metadata),
                (&segment.vector_payload_file, objects.vector),
                (&segment.layout_file, objects.layout),
                (&segment.lexical_manifest_file, objects.lexical_manifest),
            ] {
                matches(name, reference);
            }
            matches(
                &crate::lexical_projection::artifact_file(objects.lexical_generation),
                objects.lexical_payload,
            );
            if let Some(name) = &segment.rabitq_artifact_file {
                matches(name, objects.rabitq.unwrap());
            }
        }
        for run in &manifest.mutation_runs {
            matches(&run.file, run.object);
        }
    }
    struct Fixture(std::path::PathBuf);
    impl Fixture {
        fn new() -> Self {
            Self(crate::test_temp_dir().join(format!(
                "hawdb-producer-references-{}",
                hawdb_core::generate_uuidv7().unwrap()
            )))
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            if self.0.exists() {
                std::fs::remove_dir_all(&self.0).unwrap();
            }
        }
    }
    #[test]
    fn both_search_producers_persist_exact_domain_separated_artifact_references() {
        let fixture = Fixture::new();
        let root = &fixture.0;
        let mut writer = SearchOutOfCoreGenerationWriter::create(root, Default::default()).unwrap();
        writer.push(document("memory:a")).unwrap();
        writer.push(document("memory:b")).unwrap();
        writer.finish().unwrap();
        let reader = SearchOutOfCoreReader::open(root).unwrap();
        check(root, &reader.manifest);
        SearchOutOfCoreGenerationWriter::prepare_delta(
            &reader,
            SearchProjectionDelta {
                deletes: vec!["memory:a".into()],
                ..Default::default()
            },
            Default::default(),
        )
        .unwrap()
        .finish()
        .unwrap();
        drop(reader);
        let reader = SearchOutOfCoreReader::open(root).unwrap();
        check(root, &reader.manifest);
        assert!(!reader.manifest.mutation_runs.is_empty());
        drop(reader);
        let resident = Fixture::new();
        let mut index = crate::SearchIndex::open(&resident.0).unwrap();
        index.upsert(document("memory:resident")).unwrap();
        index.checkpoint().unwrap();
        let reader = SearchOutOfCoreReader::open(&resident.0).unwrap();
        check(&resident.0, &reader.manifest);
        #[cfg(feature = "vector-search")]
        assert!(reader.manifest.segments[0].objects.rabitq.is_some());
    }

    #[test]
    fn consumer_manifest_requires_exact_declared_files_and_source_epoch() {
        use crate::consumer::generation_metadata::ArtifactReference;

        let fixture = Fixture::new();
        let root = &fixture.0;
        let mut index = crate::SearchIndex::open(root).unwrap();
        index
            .apply_projection_delta(crate::SearchProjectionDelta {
                upserts: vec![crate::SearchProjectionRow {
                    kind: crate::SearchProjectionKind::Memory,
                    external_id: "m1".into(),
                    title: "candidate".into(),
                    body: "semantic closure".into(),
                    embedding: None,
                    source_id: None,
                    metadata: Default::default(),
                }],
                source_graph_commit_epoch: Some(7),
                ..Default::default()
            })
            .unwrap();
        index.checkpoint().unwrap();
        drop(index);
        let manifest = std::fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
        let mut names = std::fs::read_dir(root)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .filter(|name| name == crate::SEARCH_SNAPSHOT_FILE || name.ends_with(".1.hawdb"))
            .collect::<Vec<_>>();
        names.sort();
        let mut artifacts = names
            .iter()
            .map(|name| ArtifactReference {
                name,
                reference: ObjectReference::for_bytes(
                    ObjectKind::CheckpointArtifact,
                    1,
                    &std::fs::read(root.join(name)).unwrap(),
                ),
            })
            .collect::<Vec<_>>();
        let task = hawdb_core::RuntimeTaskContext::default();
        validate_consumer_manifest_artifacts(&manifest, 7, &artifacts, &task).unwrap();
        assert!(validate_consumer_manifest_artifacts(&manifest, 8, &artifacts, &task).is_err());
        assert!(
            validate_consumer_manifest_artifacts(&manifest, 7, &artifacts[1..], &task).is_err()
        );
        artifacts[0].reference =
            ObjectReference::for_bytes(ObjectKind::CheckpointArtifact, 1, b"wrong producer bytes");
        assert!(validate_consumer_manifest_artifacts(&manifest, 7, &artifacts, &task).is_err());
        artifacts[0].reference = ObjectReference::for_bytes(
            ObjectKind::CheckpointArtifact,
            1,
            &std::fs::read(root.join(&names[0])).unwrap(),
        );
        let extra = "search_rabitq.99.hawdb";
        artifacts.push(ArtifactReference {
            name: extra,
            reference: ObjectReference::for_bytes(ObjectKind::CheckpointArtifact, 1, b"extra"),
        });
        assert!(validate_consumer_manifest_artifacts(&manifest, 7, &artifacts, &task).is_err());
    }
}
