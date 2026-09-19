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
use hawdb_storage::durable_replace_file;
use serde::Serialize;
use std::fs::{self, File};
use std::io::Write;
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
}

impl SearchSnapshotWriteReport {
    pub(super) const fn finish(
        self,
        projection_generation: u64,
        projection_bytes_written: u64,
    ) -> SearchCheckpointReport {
        SearchCheckpointReport {
            document_count: self.document_count,
            snapshot_uncompressed_bytes: self.uncompressed_bytes,
            snapshot_compressed_bytes: self.compressed_bytes,
            snapshot_peak_record_bytes: self.peak_record_bytes,
            projection_generation,
            projection_bytes_written,
            snapshot_streamed: true,
        }
    }
}

pub(super) fn write_search_snapshot<'a>(
    target: &Path,
    source_graph_commit_epoch: Option<u64>,
    import_source_graph_commit_epoch: Option<u64>,
    embedding_manifest: Option<&SearchEmbeddingManifest>,
    embedding_dimension: Option<usize>,
    consumer_binding: Option<&super::consumer::ConsumerBinding>,
    documents: impl Iterator<Item = &'a SearchDocument>,
) -> Result<SearchSnapshotWriteReport> {
    let compressed_path = target.with_extension("hawdb.zstd.tmp");
    let mut compressed_guard = TemporaryFile::new(compressed_path.clone());
    let compressed_file = File::create(&compressed_path)?;
    let counted = CountingChecksumWriter::new(compressed_file);
    let mut encoder = zstd::stream::write::Encoder::new(counted, SEARCH_COMPRESSION_LEVEL)
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

    let mut document_count = 0usize;
    for document in documents {
        let record = encode_search_document_line(document);
        peak_record_bytes = peak_record_bytes.max(record.len() as u64);
        write_body_chunk(
            &mut encoder,
            &mut body_checksum,
            &mut uncompressed_checksum,
            &mut uncompressed_bytes,
            record.as_bytes(),
        )?;
        document_count = document_count.saturating_add(1);
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
    let (compressed_file, compressed_bytes, compressed_checksum) = counted.finish()?;
    compressed_file.sync_all()?;
    drop(compressed_file);

    let temporary = target.with_extension("hawdb.tmp");
    let mut target_guard = TemporaryFile::new(temporary.clone());
    let header = format!(
        "{SEARCH_COMPRESSION_HEADER}\ncodec\tzstd\nuncompressed_checksum\t{}\ncompressed_checksum\t{compressed_checksum}\nuncompressed_len\t{uncompressed_bytes}\ncompressed_len\t{compressed_bytes}\n\n",
        uncompressed_checksum.finish()
    );
    let encoded_sha256;
    {
        let mut output = DigestWriter {
            file: File::create(&temporary)?,
            hasher: IntegrityHasher::new(),
        };
        output.write_all(header.as_bytes())?;
        let mut compressed = File::open(&compressed_path)?;
        let copied = std::io::copy(&mut compressed, &mut output)?;
        if copied != compressed_bytes {
            return Err(HawDBError::Storage(format!(
                "search checkpoint copied {copied} compressed bytes, expected {compressed_bytes}"
            )));
        }
        output.file.sync_all()?;
        encoded_sha256 = output.hasher.finish().sha256;
    }
    fs::remove_file(&compressed_path)?;
    compressed_guard.disarm();
    durable_replace_file(&temporary, target)?;
    target_guard.disarm();

    Ok(SearchSnapshotWriteReport {
        document_count,
        uncompressed_bytes,
        compressed_bytes,
        peak_record_bytes,
        encoded_len: header.len() as u64 + compressed_bytes,
        encoded_sha256,
    })
}

struct DigestWriter {
    file: File,
    hasher: IntegrityHasher,
}

impl Write for DigestWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let written = self.file.write(bytes)?;
        self.hasher.update(&bytes[..written]);
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
            Some(11),
            Some(7),
            Some(&manifest),
            Some(2),
            None,
            documents.iter(),
        )
        .unwrap();

        let text = read_search_snapshot_text(&target).unwrap();
        assert!(text.starts_with("HAWDB_SEARCH_PROJECTION_V1\n"));
        assert!(text.contains("source_graph_commit_epoch\t11\n"));
        assert!(text.contains("import_source_graph_commit_epoch\t7\n"));
        assert!(text.contains("doc\t6d656d6f72793a61\t"));
        assert_eq!(report.document_count, 2);
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
