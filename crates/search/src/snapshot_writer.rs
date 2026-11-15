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

use super::{
    encode_search_document_line, encode_string, SearchDocument, SearchEmbeddingManifest,
    SEARCH_COMPRESSION_HEADER, SEARCH_COMPRESSION_LEVEL,
};
use crate::error::{HawDBError, Result};
use hawdb_integrity::{Crc32cHasher, IntegrityHasher, Sha256Digest};
use hawdb_storage::durability::durable_replace_file;
use hawdb_storage::file_io::{self as fs, File, OpenOptions};
use hawdb_storage::immutable_object::{ObjectKind, ObjectReference, ObjectReferenceWriter};
use serde::Serialize;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SearchCheckpointReport {
    /// Number of logical documents included in the checkpoint.
    pub document_count: usize,
    /// Bytes written through the compressor, including the logical checksum footer.
    pub snapshot_uncompressed_bytes: u64,
    /// Compressed payload bytes, excluding the fixed envelope header.
    pub snapshot_compressed_bytes: u64,
    /// Largest individual encoded document record retained by the snapshot writer.
    pub snapshot_peak_record_bytes: u64,
    /// Immutable out-of-core generation published by this checkpoint.
    pub projection_generation: u64,
    /// Bytes newly written while building and publishing derived search artifacts.
    /// The snapshot has its own byte counters and is not included here.
    pub projection_bytes_written: u64,
    /// Whether the snapshot avoided corpus-sized uncompressed and compressed buffers.
    pub snapshot_streamed: bool,
    /// Generation cleanup run after publication; `None` for in-memory indexes.
    /// A `retry_required` outcome leaves superseded generations on disk.
    pub projection_cleanup: Option<crate::SearchProjectionCleanupOutcome>,
}

impl SearchCheckpointReport {
    pub(super) const fn in_memory(document_count: usize) -> Self {
        Self {
            document_count,
            snapshot_uncompressed_bytes: 0,
            snapshot_compressed_bytes: 0,
            snapshot_peak_record_bytes: 0,
            projection_generation: 0,
            projection_bytes_written: 0,
            snapshot_streamed: false,
            projection_cleanup: None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct SearchSnapshotWriteReport {
    document_count: usize,
    uncompressed_bytes: u64,
    compressed_bytes: u64,
    peak_record_bytes: u64,
    pub(super) encoded_len: u64,
    pub(super) encoded_sha256: Sha256Digest,
    pub(super) encoded_object: ObjectReference,
    pub(super) vector_document_count: usize,
}

impl SearchSnapshotWriteReport {
    pub(super) const fn finish(
        self,
        projection_generation: u64,
        projection_bytes_written: u64,
        projection_cleanup: Option<crate::SearchProjectionCleanupOutcome>,
    ) -> SearchCheckpointReport {
        SearchCheckpointReport {
            document_count: self.document_count,
            snapshot_uncompressed_bytes: self.uncompressed_bytes,
            snapshot_compressed_bytes: self.compressed_bytes,
            snapshot_peak_record_bytes: self.peak_record_bytes,
            projection_generation,
            projection_bytes_written,
            snapshot_streamed: true,
            projection_cleanup,
        }
    }
}

#[derive(Default)]
pub(super) struct SearchSnapshotMetadata<'a> {
    pub source_graph_commit_epoch: Option<u64>,
    pub import_source_graph_commit_epoch: Option<u64>,
    pub embedding_manifest: Option<&'a SearchEmbeddingManifest>,
    pub embedding_dimension: Option<usize>,
    pub consumer_binding: Option<&'a super::consumer::ConsumerBinding>,
    pub vector_index_descriptor: Option<&'a hawdb_core::VectorIndexDescriptor>,
    pub vector_scoring: crate::SearchVectorScoring,
    pub vector_document_summary: Option<crate::vector_index::VectorDocumentSummary>,
    pub canonical_vector_source_epoch: Option<u64>,
    pub rabitq_checkpoint: Option<(u8, usize, u64)>,
}

#[cfg(test)]
pub(super) fn write_search_snapshot<'a>(
    target: &Path,
    metadata: SearchSnapshotMetadata<'_>,
    documents: impl Iterator<Item = &'a SearchDocument>,
) -> Result<SearchSnapshotWriteReport> {
    let task = hawdb_core::RuntimeTaskContext::default();
    let memory = crate::build_memory::BuildMemory::new(&task)?;
    write_search_snapshot_with_context(target, metadata, documents, &memory, &task)
}

pub(crate) fn write_search_snapshot_with_context<'a>(
    target: &Path,
    metadata: SearchSnapshotMetadata<'_>,
    documents: impl Iterator<Item = &'a SearchDocument>,
    memory: &crate::build_memory::BuildMemory,
    task: &hawdb_core::RuntimeTaskContext,
) -> Result<SearchSnapshotWriteReport> {
    use crate::build_control::{checkpoint, CheckedWriter};
    use crate::build_memory::{checked_add, checked_mul};
    checkpoint(task)?;
    if metadata.canonical_vector_source_epoch.is_some()
        && (metadata.canonical_vector_source_epoch != metadata.source_graph_commit_epoch
            || !metadata
                .vector_document_summary
                .is_some_and(|summary| summary.document_count == summary.vector_document_count))
    {
        return Err(crate::vector_index::invalid(
            "invalid canonical vector snapshot provenance",
        ));
    }
    let mut documents = documents.peekable();
    if let Some(summary) = metadata.vector_document_summary {
        summary.validate()?;
        if metadata.consumer_binding.is_none()
            || metadata.vector_index_descriptor.is_none()
            || documents.peek().is_some()
        {
            return Err(crate::vector_index::invalid(
                "metadata-only vector snapshot requires a bound Consumer and no resident rows",
            ));
        }
    }
    crate::build_memory::compression::require_qualified_zstd("workspace admission")?;
    let _workspace = memory
        .retained
        .reserve(crate::build_memory::compression::COMPRESSION_WORKSPACE_BYTES)?;
    let _metadata = memory.spool.reserve(checked_add(
        64 * 1024,
        checked_mul(target.as_os_str().as_encoded_bytes().len(), 16)?,
    )?)?;
    let SearchSnapshotMetadata {
        source_graph_commit_epoch,
        import_source_graph_commit_epoch,
        embedding_manifest,
        embedding_dimension,
        consumer_binding,
        vector_index_descriptor,
        vector_scoring,
        vector_document_summary,
        canonical_vector_source_epoch,
        rabitq_checkpoint,
    } = metadata;
    let compressed_path = target.with_extension("hawdb.zstd.tmp");
    let mut compressed_guard = TemporaryFile::new(compressed_path.clone());
    let compressed_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&compressed_path)?;
    let counted = CountingChecksumWriter::new(compressed_file);
    let mut encoder =
        hawdb_storage::compression::Encoder::new(counted, SEARCH_COMPRESSION_LEVEL)
            .map_err(|error| HawDBError::Storage(format!("zstd compression failed: {error}")))?;
    let mut body_checksum = Crc32cHasher::new();
    let mut uncompressed_checksum = Crc32cHasher::new();
    let mut uncompressed_bytes = 0u64;
    let mut peak_record_bytes = 0u64;

    write_body_chunk(
        &mut encoder,
        &mut body_checksum,
        &mut uncompressed_checksum,
        &mut uncompressed_bytes,
        b"HAWDB_SEARCH_PROJECTION_V1\n",
    )?;
    if let Some(binding) = consumer_binding {
        write_body_chunk(
            &mut encoder,
            &mut body_checksum,
            &mut uncompressed_checksum,
            &mut uncompressed_bytes,
            binding.record().as_bytes(),
        )?;
    }
    if let Some(epoch) = source_graph_commit_epoch {
        let line = format!("source_graph_commit_epoch\t{epoch}\n");
        write_body_chunk(
            &mut encoder,
            &mut body_checksum,
            &mut uncompressed_checksum,
            &mut uncompressed_bytes,
            line.as_bytes(),
        )?;
    }
    if let Some(epoch) = import_source_graph_commit_epoch {
        let line = format!("import_source_graph_commit_epoch\t{epoch}\n");
        write_body_chunk(
            &mut encoder,
            &mut body_checksum,
            &mut uncompressed_checksum,
            &mut uncompressed_bytes,
            line.as_bytes(),
        )?;
    }
    if vector_scoring == crate::SearchVectorScoring::LinearSquaredL2 {
        write_body_chunk(
            &mut encoder,
            &mut body_checksum,
            &mut uncompressed_checksum,
            &mut uncompressed_bytes,
            b"vector_scoring\tlinear_squared_l2\n",
        )?;
    }
    if let Some(descriptor) = vector_index_descriptor {
        let line = format!(
            "vector_index_binding\t{}\n",
            crate::vector_index::encode(descriptor)
        );
        write_body_chunk(
            &mut encoder,
            &mut body_checksum,
            &mut uncompressed_checksum,
            &mut uncompressed_bytes,
            line.as_bytes(),
        )?;
    }
    if let Some(manifest) = embedding_manifest {
        let line = format!(
            "embedding_manifest\t{}\t{}\t{}\n",
            encode_string(&manifest.model),
            encode_string(manifest.version.as_deref().unwrap_or_default()),
            manifest.dimension
        );
        write_body_chunk(
            &mut encoder,
            &mut body_checksum,
            &mut uncompressed_checksum,
            &mut uncompressed_bytes,
            line.as_bytes(),
        )?;
    }
    if let Some((bits, rows, seed)) = rabitq_checkpoint {
        if !matches!(bits, 1 | 4) || rows == 0 || rows > u32::MAX as usize {
            return Err(HawDBError::Storage(
                "invalid explicit RaBitQ checkpoint settings".into(),
            ));
        }
        let line = format!("rabitq_checkpoint\t{bits}\t{rows}\t{seed}\n");
        write_body_chunk(
            &mut encoder,
            &mut body_checksum,
            &mut uncompressed_checksum,
            &mut uncompressed_bytes,
            line.as_bytes(),
        )?;
    }
    if let Some(dimension) = embedding_dimension {
        let line = format!("embedding_dimension\t{dimension}\n");
        write_body_chunk(
            &mut encoder,
            &mut body_checksum,
            &mut uncompressed_checksum,
            &mut uncompressed_bytes,
            line.as_bytes(),
        )?;
    }

    if let Some(summary) = vector_document_summary {
        let line = format!(
            "vector_document_summary\t{}\t{}\t{}\n",
            summary.document_count, summary.vector_document_count, summary.documents_digest
        );
        write_body_chunk(
            &mut encoder,
            &mut body_checksum,
            &mut uncompressed_checksum,
            &mut uncompressed_bytes,
            line.as_bytes(),
        )?;
    }
    if let Some(epoch) = canonical_vector_source_epoch {
        let line = format!("canonical_vector_source_epoch\t{epoch}\n");
        write_body_chunk(
            &mut encoder,
            &mut body_checksum,
            &mut uncompressed_checksum,
            &mut uncompressed_bytes,
            line.as_bytes(),
        )?;
    }
    let mut document_count = vector_document_summary.map_or(0, |summary| summary.document_count);
    let mut vector_document_count =
        vector_document_summary.map_or(0, |summary| summary.vector_document_count);
    for document in documents {
        checkpoint(task)?;
        let _record_memory = memory.input.reserve(checked_add(
            4096,
            checked_mul(crate::build_memory::document_bytes(document)?, 24)?,
        )?)?;
        let record = encode_search_document_line(document);
        peak_record_bytes = peak_record_bytes.max(record.len() as u64);
        write_body_chunk(
            &mut encoder,
            &mut body_checksum,
            &mut uncompressed_checksum,
            &mut uncompressed_bytes,
            record.as_bytes(),
        )?;
        checkpoint(task)?;
        document_count = document_count.saturating_add(1);
        vector_document_count =
            vector_document_count.saturating_add(usize::from(document.embedding.is_some()));
    }

    let footer = format!("checksum\t{}\n", body_checksum.finish());
    write_envelope_chunk(
        &mut encoder,
        &mut uncompressed_checksum,
        &mut uncompressed_bytes,
        footer.as_bytes(),
    )?;
    let counted = encoder
        .finish()
        .map_err(|error| HawDBError::Storage(format!("zstd compression failed: {error}")))?;
    let (mut compressed_file, compressed_bytes, compressed_checksum) = counted.finish()?;
    compressed_file.sync_all()?;
    compressed_file.seek(SeekFrom::Start(0))?;

    let temporary = target.with_extension("hawdb.tmp");
    let mut target_guard = TemporaryFile::new(temporary.clone());
    let header = format!(
        "{SEARCH_COMPRESSION_HEADER}\ncodec\tzstd\nuncompressed_checksum\t{}\ncompressed_checksum\t{compressed_checksum}\nuncompressed_len\t{uncompressed_bytes}\ncompressed_len\t{compressed_bytes}\n\n",
        uncompressed_checksum.finish()
    );
    let encoded_sha256;
    let encoded_object;
    let encoded_len = (header.len() as u64)
        .checked_add(compressed_bytes)
        .ok_or_else(|| HawDBError::Storage("snapshot encoded length overflow".into()))?;
    {
        let mut output = DigestWriter {
            file: File::create(&temporary)?,
            hasher: IntegrityHasher::new(),
            object: ObjectReferenceWriter::new(ObjectKind::CheckpointArtifact, 1, encoded_len)
                .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))?,
        };
        output.write_all(header.as_bytes())?;
        let mut copied = 0u64;
        let mut buffer = [0u8; 8192];
        loop {
            checkpoint(task)?;
            let read = std::io::Read::read(&mut compressed_file, &mut buffer)?;
            if read == 0 {
                break;
            }
            CheckedWriter::new(&mut output, Some(task)).write_all(&buffer[..read])?;
            copied += read as u64;
        }
        if copied != compressed_bytes {
            return Err(HawDBError::Storage(format!(
                "search checkpoint copied {copied} compressed bytes, expected {compressed_bytes}"
            )));
        }
        output.file.sync_all()?;
        encoded_sha256 = output.hasher.finish().sha256;
        encoded_object = output
            .object
            .finish()
            .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))?;
    }
    drop(compressed_file);
    fs::remove_file(&compressed_path)?;
    compressed_guard.disarm();
    checkpoint(task)?;
    durable_replace_file(&temporary, target)?;
    target_guard.disarm();

    Ok(SearchSnapshotWriteReport {
        document_count,
        uncompressed_bytes,
        compressed_bytes,
        peak_record_bytes,
        encoded_len,
        encoded_object,
        vector_document_count,
        encoded_sha256,
    })
}

struct DigestWriter {
    file: File,
    hasher: IntegrityHasher,
    object: ObjectReferenceWriter,
}

impl Write for DigestWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let written = self.file.write(bytes)?;
        self.hasher.update(&bytes[..written]);
        self.object.write_all(&bytes[..written])?;
        Ok(written)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

fn write_body_chunk<W: Write>(
    writer: &mut W,
    body_checksum: &mut Crc32cHasher,
    uncompressed_checksum: &mut Crc32cHasher,
    uncompressed_bytes: &mut u64,
    bytes: &[u8],
) -> Result<()> {
    body_checksum.update(bytes);
    write_envelope_chunk(writer, uncompressed_checksum, uncompressed_bytes, bytes)
}

fn write_envelope_chunk<W: Write>(
    writer: &mut W,
    checksum: &mut Crc32cHasher,
    byte_count: &mut u64,
    bytes: &[u8],
) -> Result<()> {
    writer.write_all(bytes)?;
    checksum.update(bytes);
    *byte_count = byte_count
        .checked_add(bytes.len() as u64)
        .ok_or_else(|| HawDBError::Storage("search checkpoint byte count overflow".to_string()))?;
    Ok(())
}

struct CountingChecksumWriter<W> {
    inner: W,
    byte_count: u64,
    checksum: Crc32cHasher,
}

impl<W> CountingChecksumWriter<W> {
    const fn new(inner: W) -> Self {
        Self {
            inner,
            byte_count: 0,
            checksum: Crc32cHasher::new(),
        }
    }
}

impl<W: Write> CountingChecksumWriter<W> {
    fn finish(mut self) -> Result<(W, u64, u64)> {
        self.inner.flush()?;
        Ok((self.inner, self.byte_count, self.checksum.finish()))
    }
}

impl<W: Write> Write for CountingChecksumWriter<W> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(buffer)?;
        self.checksum.update(&buffer[..written]);
        self.byte_count = self
            .byte_count
            .checked_add(written as u64)
            .ok_or_else(|| std::io::Error::other("compressed checkpoint length overflow"))?;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

struct TemporaryFile {
    path: PathBuf,
    armed: bool,
}

impl TemporaryFile {
    const fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for TemporaryFile {
    fn drop(&mut self) {
        if self.armed {
            let _ = fs::remove_file(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::read_search_snapshot_text;
    use std::collections::BTreeMap;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn streaming_writer_preserves_snapshot_format_without_corpus_materialization() {
        let root = unique_test_dir("streaming_snapshot_writer");
        fs::create_dir_all(&root).unwrap();
        let target = root.join("search_projection.hawdb");
        let documents = [
            SearchDocument {
                id: "memory:a".to_string(),
                title: "A".to_string(),
                content: "checkpoint streaming".repeat(64),
                embedding: Some(vec![1.0, 0.5]),
                metadata: BTreeMap::from([("kind".to_string(), "memory".to_string())]),
            },
            SearchDocument {
                id: "memory:b".to_string(),
                title: "B".to_string(),
                content: "bounded record".to_string(),
                embedding: None,
                metadata: BTreeMap::new(),
            },
        ];
        let manifest = SearchEmbeddingManifest {
            model: "test-model".to_string(),
            version: Some("v1".to_string()),
            dimension: 2,
        };
        let report = write_search_snapshot(
            &target,
            SearchSnapshotMetadata {
                source_graph_commit_epoch: Some(11),
                import_source_graph_commit_epoch: Some(7),
                embedding_manifest: Some(&manifest),
                embedding_dimension: Some(2),
                ..Default::default()
            },
            documents.iter(),
        )
        .unwrap();

        let text = read_search_snapshot_text(&target).unwrap();
        assert!(text.starts_with("HAWDB_SEARCH_PROJECTION_V1\n"));
        assert!(text.contains("source_graph_commit_epoch\t11\n"));
        assert!(text.contains("import_source_graph_commit_epoch\t7\n"));
        assert!(text.contains("doc\t6d656d6f72793a61\t"));
        assert_eq!(report.document_count, 2);
        assert_eq!(report.vector_document_count, 1);
        let bytes = fs::read(&target).unwrap();
        assert_eq!(
            report.encoded_object,
            ObjectReference::for_bytes(ObjectKind::CheckpointArtifact, 1, &bytes)
        );
        assert_eq!(report.encoded_object.byte_length, report.encoded_len);
        assert_eq!(report.uncompressed_bytes, text.len() as u64);
        assert!(report.compressed_bytes > 0);
        assert!(report.peak_record_bytes < report.uncompressed_bytes);
        assert!(!target.with_extension("hawdb.zstd.tmp").exists());
        assert!(!target.with_extension("hawdb.tmp").exists());
        fs::remove_dir_all(root).unwrap();
    }

    fn unique_test_dir(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "hawdb-search-snapshot-{name}-{}-{nonce}",
            std::process::id()
        ))
    }
}
