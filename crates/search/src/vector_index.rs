// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Schema-binding metadata sealed with ordinary search projection artifacts.

use crate::{HawDBError, Result, SearchIndex};
use hawdb_core::vector_index::MAX_VECTOR_INDEX_DESCRIPTOR_BYTES;
use hawdb_core::VectorIndexDescriptor;

mod query;
pub use query::{
    VectorIndexHit, VectorIndexQuery, VectorIndexQueryReport, VectorIndexRetainedOutput,
};

/// Logical identity of a schema-bound Consumer whose rows live exclusively in
/// the immutable artifact closure. This is never a resident serving index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VectorDocumentSummary {
    pub(crate) document_count: usize,
    pub(crate) vector_document_count: usize,
    pub(crate) documents_digest: u64,
}

#[derive(Debug)]
pub(crate) struct CanonicalVectorDelta {
    pub(crate) base_epoch: u64,
    pub(crate) deletes: Vec<String>,
}

impl VectorDocumentSummary {
    pub(crate) fn validate(self) -> Result<()> {
        if self.vector_document_count > self.document_count {
            return Err(invalid(
                "vector summary count exceeds logical document count",
            ));
        }
        Ok(())
    }
}

pub(crate) fn encode(descriptor: &VectorIndexDescriptor) -> String {
    use std::fmt::Write;
    let bytes = descriptor.encode();
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("writing a String cannot fail");
    }
    output
}

pub(crate) fn decode(encoded: &str) -> Result<VectorIndexDescriptor> {
    if encoded.len() > MAX_VECTOR_INDEX_DESCRIPTOR_BYTES * 2 || !encoded.len().is_multiple_of(2) {
        return Err(invalid("invalid encoded descriptor length"));
    }
    let mut bytes = Vec::with_capacity(encoded.len() / 2);
    for pair in encoded.as_bytes().chunks_exact(2) {
        let digit = |byte: u8| match byte {
            b'0'..=b'9' => Ok(byte - b'0'),
            b'a'..=b'f' => Ok(byte - b'a' + 10),
            _ => Err(invalid("descriptor must use canonical lowercase hex")),
        };
        bytes.push(digit(pair[0])? * 16 + digit(pair[1])?);
    }
    VectorIndexDescriptor::decode(&bytes)
        .map_err(|error| invalid(&format!("invalid descriptor: {error}")))
}

pub(crate) fn invalid(message: &str) -> HawDBError {
    HawDBError::StorageIntegrity(format!("vector index binding: {message}"))
}

#[cfg(feature = "vector-search")]
pub(crate) fn validate_projection(
    descriptor: &VectorIndexDescriptor,
    artifact: &hawdb_vector_projection::ProjectionManifest,
) -> Result<()> {
    let hawdb_core::VectorIndexBackend::RaBitQ {
        bit_width,
        segment_rows,
        transform_seed,
    } = descriptor.backend()
    else {
        return Err(invalid("RaBitQ artifact has a different bound backend"));
    };
    if artifact.dimension != descriptor.dimension().get() as usize
        || artifact.bit_width != bit_width
        || artifact.requested_segment_rows != segment_rows as usize
        || artifact.transform_seed != transform_seed
    {
        return Err(invalid(
            "RaBitQ artifact disagrees with its bound definition",
        ));
    }
    Ok(())
}

impl SearchIndex {
    /// Terminal handoff from the facade's fixed canonical vector projector.
    /// Enumeration alone is insufficient: the facade must hydrate every mapped
    /// key from the same pinned source before completing this private builder.
    #[doc(hidden)]
    pub fn finish_canonical_vector_source(
        &mut self,
        epoch: u64,
        base_epoch: Option<u64>,
        deletes: Vec<String>,
    ) -> Result<()> {
        let _mutation = self.borrow_consumer_mutation()?;
        let definition = self
            .vector_index_descriptor
            .as_ref()
            .ok_or_else(|| invalid("canonical source has no vector binding"))?;
        if self.path.is_some()
            || self.consumer_binding.is_some()
            || self.canonical_vector_source_epoch.is_some()
            || base_epoch.is_some_and(|base| base > epoch)
            || base_epoch.is_none() && !deletes.is_empty()
            || deletes.windows(2).any(|pair| pair[0] >= pair[1])
            || deletes.iter().any(|id| self.documents.contains_key(id))
        {
            return Err(invalid("canonical source requires an unsealed private builder and disjoint ordered changes"));
        }
        let native_id = |id: &str| {
            let (prefix, fixed_length) = match definition.target() {
                hawdb_core::VectorIndexTarget::NodeProperty { .. } => ("n:", Some(16)),
                hawdb_core::VectorIndexTarget::Relational { .. } => ("r:", None),
            };
            id.strip_prefix(prefix).is_some_and(|key| {
                !key.is_empty()
                    && key.len().is_multiple_of(2)
                    && fixed_length.is_none_or(|length| key.len() == length)
                    && key
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            })
        };
        if self.documents.values().any(|document| {
            !native_id(&document.id)
                || document.embedding.is_none()
                || !document.title.is_empty()
                || !document.content.is_empty()
                || !document.metadata.is_empty()
        }) || deletes.iter().any(|id| !native_id(id))
        {
            return Err(invalid(
                "canonical source contains a non-native identity or non-vector payload",
            ));
        }
        self.canonical_vector_source_epoch = Some(epoch);
        self.canonical_vector_delta = base_epoch.map(|base_epoch| CanonicalVectorDelta {
            base_epoch,
            deletes,
        });
        Ok(())
    }

    pub fn vector_index_descriptor(&self) -> Option<&VectorIndexDescriptor> {
        self.vector_index_descriptor.as_ref()
    }

    /// Ordinary Search relevance metric. Unbound projections are cosine by
    /// construction; a binding seals its own metric into every artifact.
    pub(crate) fn vector_metric(&self) -> hawdb_core::VectorDistance {
        self.vector_index_descriptor
            .as_ref()
            .map_or(hawdb_core::VectorDistance::Cosine, |descriptor| {
                descriptor.distance()
            })
    }

    /// Binds an empty private builder. The facade owns schema resolution and
    /// source capture; a serving Consumer cannot be redefined in place.
    #[doc(hidden)]
    pub fn bind_vector_index(&mut self, descriptor: VectorIndexDescriptor) -> Result<()> {
        self.runtime_capabilities
            .require(hawdb_core::RuntimeCapability::VectorSearch)?;
        self.require_unregistered_publication()?;
        if !self.documents.is_empty() || self.vector_index_descriptor.is_some() {
            return Err(HawDBError::Semantic(
                "a vector index definition requires an empty unbound builder".into(),
            ));
        }
        let dimension = descriptor.dimension().get() as usize;
        if self
            .embedding_dimension
            .is_some_and(|existing| existing != dimension)
        {
            return Err(HawDBError::Semantic(
                "vector index dimension disagrees with the projection".into(),
            ));
        }
        self.restore_vector_index(descriptor);
        self.embedding_dimension = Some(dimension);
        Ok(())
    }

    pub(crate) fn restore_vector_index(&mut self, descriptor: VectorIndexDescriptor) {
        #[cfg(feature = "vector-search")]
        if let hawdb_core::VectorIndexBackend::RaBitQ {
            bit_width,
            segment_rows,
            transform_seed,
        } = descriptor.backend()
        {
            self.rabitq_build_options.bit_width = match bit_width {
                1 => hawdb_vector_projection::RaBitQBitWidth::One,
                4 => hawdb_vector_projection::RaBitQBitWidth::Four,
                _ => unreachable!("validated vector index backend"),
            };
            self.rabitq_build_options.segment_rows = segment_rows as usize;
            self.rabitq_build_options.transform_seed = transform_seed;
        }
        self.vector_index_descriptor = Some(descriptor);
    }
}

#[cfg(feature = "vector-search")]
pub(crate) fn diskann_build_config(
    backend: hawdb_core::VectorIndexBackend,
    max_working_bytes: usize,
) -> Option<hawdb_vector_projection::diskann::DiskAnnBuildConfig> {
    let hawdb_core::VectorIndexBackend::DiskAnn {
        max_degree,
        build_beam,
        alpha_milli,
        lsg,
    } = backend
    else {
        return None;
    };
    Some(hawdb_vector_projection::diskann::DiskAnnBuildConfig {
        max_degree: max_degree as usize,
        build_beam: build_beam as usize,
        alpha: f64::from(alpha_milli) / 1000.0,
        lsg: lsg.map(|value| hawdb_vector_projection::diskann::LsgBuildConfig {
            sample_size: value.sample_size as usize,
            neighbors: value.neighbors as usize,
            exponent: f64::from(value.exponent_milli) / 1000.0,
        }),
        max_working_bytes,
    })
}

#[cfg(all(test, feature = "vector-search"))]
mod tests {
    use super::*;
    use crate::{SearchDocument, SearchOutOfCoreReader};
    use hawdb_core::{VectorDimension, VectorDistance, VectorIndexBackend, VectorIndexTarget};
    use std::collections::BTreeMap;

    fn descriptor() -> VectorIndexDescriptor {
        VectorIndexDescriptor::new(
            "memories.embedding".into(),
            VectorIndexTarget::Relational {
                table: "memories".into(),
                column: "embedding".into(),
                column_ordinal: 1,
            },
            VectorDimension::new(2).unwrap(),
            VectorDistance::Cosine,
            VectorIndexBackend::RaBitQ {
                bit_width: 1,
                segment_rows: 64,
                transform_seed: 42,
            },
            [7; 32],
        )
        .unwrap()
    }

    fn document(id: &str, embedding: Option<Vec<f32>>) -> SearchDocument {
        SearchDocument {
            id: id.into(),
            title: String::new(),
            content: String::new(),
            embedding,
            metadata: BTreeMap::new(),
        }
    }

    fn summary_snapshot(records: &str) -> String {
        let binding = crate::consumer::ConsumerBinding {
            database_uuid: hawdb_core::generate_uuidv7().unwrap(),
            projection_uuid: hawdb_core::generate_uuidv7().unwrap(),
            consumer_id: "vector-summary".into(),
            registration_uuid: hawdb_core::generate_uuidv7().unwrap(),
            checkpoint_uuid: hawdb_core::generate_uuidv7().unwrap(),
        };
        let body = format!(
            "HAWDB_SEARCH_PROJECTION_V1\n{}source_graph_commit_epoch\t1\nvector_index_binding\t{}\nembedding_dimension\t2\n{records}",
            binding.record(), encode(&descriptor()),
        );
        format!(
            "{body}checksum\t{}\n",
            crate::checksum_bytes(body.as_bytes())
        )
    }

    #[test]
    fn vector_index_summary_retains_logical_counts_without_resident_rows() {
        let mut index = SearchIndex::in_memory();
        index
            .parse_snapshot_text(&summary_snapshot(
                "vector_document_summary\t1000000\t999999\t42\n",
            ))
            .unwrap();
        assert!(index.documents.is_empty());
        assert_eq!(index.document_count(), 1_000_000);
        assert_eq!(index.projection_freshness().document_count, 1_000_000);
        assert_eq!(index.vector_document_summary.unwrap().documents_digest, 42);
        assert_eq!(index.vector_index_descriptor(), Some(&descriptor()));
        for records in [
            "vector_document_summary\t1\t2\t42\n",
            "vector_document_summary\t1\t1\t42\nvector_document_summary\t1\t1\t42\n",
            "vector_document_summary\t1\t1\t42\ndoc\t61\t\t\t\t\n",
            "doc\t61\t\t\t\t\nvector_document_summary\t1\t1\t42\n",
        ] {
            assert!(
                SearchIndex::in_memory()
                    .parse_snapshot_text(&summary_snapshot(records))
                    .is_err(),
                "{records}"
            );
        }
        let text = summary_snapshot("vector_document_summary\t1\t1\t42\n");
        let (body, _) = crate::split_checksum(&text).unwrap();
        for prefix in ["projection_consumer_binding\t", "vector_index_binding\t"] {
            let body = body
                .lines()
                .filter(|line| !line.starts_with(prefix))
                .map(|line| format!("{line}\n"))
                .collect::<String>();
            let text = format!(
                "{body}checksum\t{}\n",
                crate::checksum_bytes(body.as_bytes())
            );
            assert!(SearchIndex::in_memory().parse_snapshot_text(&text).is_err());
        }
    }

    #[test]
    fn vector_index_definition_is_sealed_in_snapshot_and_generation() {
        let path = std::env::temp_dir().join(format!(
            "hawdb-vector-index-binding-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        let binding = descriptor();
        {
            let mut index = SearchIndex::open(&path).unwrap();
            index.bind_vector_index(binding.clone()).unwrap();
            index.upsert(document("a", Some(vec![1.0, 0.0]))).unwrap();
            index.upsert(document("b", Some(vec![-1.0, 0.0]))).unwrap();
            index.upsert(document("null", None)).unwrap();
            index.checkpoint().unwrap();
        }
        let index = SearchIndex::open(&path).unwrap();
        assert_eq!(index.vector_index_descriptor(), Some(&binding));
        assert_eq!(index.rabitq_build_options.bit_width.bits(), 1);
        assert_eq!(index.rabitq_build_options.segment_rows, 64);
        assert_eq!(index.rabitq_build_options.transform_seed, 42);
        let reader = SearchOutOfCoreReader::open(&path).unwrap();
        assert_eq!(reader.vector_index_descriptor().unwrap(), Some(binding));
        drop(reader);
        drop(index);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn vector_index_invalid_vectors_do_not_mutate_documents() {
        let mut index = SearchIndex::in_memory();
        index.bind_vector_index(descriptor()).unwrap();
        for embedding in [vec![0.0, 0.0], vec![1.0], vec![f32::NAN, 1.0]] {
            assert!(index.upsert(document("invalid", Some(embedding))).is_err());
            assert!(index.documents.is_empty());
        }
        assert!(index.bind_vector_index(descriptor()).is_err());
        index
            .upsert(document("opposite", Some(vec![-1.0, 0.0])))
            .unwrap();
        assert_eq!(index.documents.len(), 1);
    }

    #[test]
    fn vector_index_metadata_rejects_noncanonical_and_oversized_inputs() {
        let encoded = encode(&descriptor());
        assert_eq!(decode(&encoded).unwrap(), descriptor());
        for value in [
            "x".into(),
            "gg".into(),
            "00".repeat(MAX_VECTOR_INDEX_DESCRIPTOR_BYTES + 1),
            encoded.to_uppercase(),
        ] {
            assert!(decode(&value).is_err());
        }
    }

    #[test]
    fn vector_index_reopen_refuses_artifact_build_parameter_drift() {
        let path = std::env::temp_dir().join(format!(
            "hawdb-vector-drift-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        let mut index = SearchIndex::open(&path).unwrap();
        index.bind_vector_index(descriptor()).unwrap();
        index.upsert(document("a", Some(vec![1.0, 0.0]))).unwrap();
        index.rabitq_build_options.transform_seed += 1;
        index.checkpoint().unwrap();
        assert!(matches!(
            SearchOutOfCoreReader::open(&path),
            Err(HawDBError::StorageIntegrity(_))
        ));
        drop(index);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn vector_index_delta_inherits_nondefault_backend_and_definition() {
        let path = std::env::temp_dir().join(format!(
            "hawdb-vector-delta-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        let binding = descriptor();
        let mut index = SearchIndex::open(&path).unwrap();
        index.bind_vector_index(binding.clone()).unwrap();
        index.upsert(document("a", Some(vec![1.0, 0.0]))).unwrap();
        index.checkpoint().unwrap();
        drop(index);
        let previous = SearchOutOfCoreReader::open(&path).unwrap();
        let (update, _, _) = crate::SearchOutOfCoreGenerationWriter::prepare_delta(
            &previous,
            crate::SearchProjectionDelta {
                upserts: vec![crate::SearchProjectionRow {
                    kind: crate::SearchProjectionKind::Memory,
                    external_id: "b".into(),
                    title: String::new(),
                    body: String::new(),
                    embedding: Some(vec![-1.0, 0.0]),
                    source_id: None,
                    metadata: BTreeMap::new(),
                }],
                source_graph_commit_epoch: Some(1),
                ..Default::default()
            },
            Default::default(),
        )
        .unwrap()
        .finish()
        .unwrap();
        assert_eq!(update.after_document_count, 2);
        let next = SearchOutOfCoreReader::open(&path).unwrap();
        assert_eq!(next.vector_index_descriptor().unwrap(), Some(binding));
        assert!(next.generation() > previous.generation());
        assert_eq!(previous.document_count(), 1);
        drop(next);
        drop(previous);
        std::fs::remove_dir_all(path).unwrap();
    }
}

pub(crate) fn validate_embedding(
    descriptor: Option<&VectorIndexDescriptor>,
    embedding: &[f32],
    checkpoint: impl FnMut() -> Result<()>,
) -> Result<()> {
    let Some(descriptor) = descriptor else {
        return Ok(());
    };
    if embedding.len() != descriptor.dimension().get() as usize {
        return Err(invalid(
            "embedding dimension differs from its schema binding",
        ));
    }
    descriptor
        .distance()
        .distance_slices_with_work(embedding, embedding, checkpoint)?;
    Ok(())
}
