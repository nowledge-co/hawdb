use crate::build_control::checkpoint;
use crate::build_io::{self, Buffer};
use crate::build_memory::{checked_add, AdmittedDocument, BuildMemory};
use crate::document_codec::{write_embedding, write_hex, write_line, write_metadata};
use crate::error::{Result, SkeinError};
use crate::{checksum_bytes, SEARCH_COMPRESSION_HEADER, SEARCH_COMPRESSION_LEVEL};
use skein_core::RuntimeTaskContext;
use std::io::Write;

// Pinned zstd 1.5.7, level 3, one worker, no dictionary/LDM/sequence producer:
// windowLog=21, chainLog=16, hashLog=17, 128 KiB blocks. 8 MiB covers two
// complete C workspaces including context/tables/tokens/buffer/alignment slack;
// zstd 0.13.3's Rust stream writer additionally owns a fixed 32 KiB buffer.
// Qualify this source-backed envelope again before upgrading the dependency.
pub(super) const COMPRESSION_WORKSPACE_BYTES: usize = 8 * 1024 * 1024 + 32 * 1024;
const _: () = assert!(SEARCH_COMPRESSION_LEVEL == 3);

#[derive(Clone, Copy)]
pub(super) enum Kind {
    Document,
    Metadata,
    Vector,
}

pub(super) fn body(
    kind: Kind,
    documents: &[AdmittedDocument],
    vector_base: u64,
    limit: u64,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
) -> Result<Buffer> {
    let vector_count = documents
        .iter()
        .filter(|document| document.embedding.is_some())
        .count();
    vector_base
        .checked_add(vector_count as u64)
        .ok_or_else(|| SkeinError::Storage("search vector ordinal range overflow".to_owned()))?;
    build_io::formatted(limit, memory, task, |output| {
        output.write_str(match kind {
            Kind::Document => "SKEIN_SEARCH_SEGMENT_V1\n",
            Kind::Metadata => "SKEIN_SEARCH_METADATA_SEGMENT_V1\n",
            Kind::Vector => "SKEIN_SEARCH_VECTOR_SEGMENT_V1\n",
        })?;
        let mut ordinal = vector_base;
        for document in documents {
            match kind {
                Kind::Document => write_line(output, document)?,
                Kind::Metadata => {
                    output.write_str("meta\t")?;
                    write_hex(output, &document.id)?;
                    if document.embedding.is_some() {
                        write!(output, "\t{ordinal}\t")?;
                        ordinal += 1;
                    } else {
                        output.write_str("\t-\t")?;
                    }
                    write_metadata(output, &document.metadata)?;
                    output.write_char('\n')?;
                }
                Kind::Vector => {
                    if let Some(embedding) = document.embedding.as_deref() {
                        write!(output, "vector\t{ordinal}\t")?;
                        write_hex(output, &document.id)?;
                        output.write_char('\t')?;
                        write_embedding(output, Some(embedding))?;
                        output.write_char('\n')?;
                        ordinal += 1;
                    }
                }
            }
        }
        Ok(())
    })
}

pub(super) fn compress(
    body: &[u8],
    limit: u64,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
) -> Result<Buffer> {
    checkpoint(task)?;
    if zstd::zstd_safe::version_number() != 10507 {
        return Err(SkeinError::Execution(
            "search zstd workspace admission requires qualification for this version".to_owned(),
        ));
    }
    let workspace = memory.retained.reserve(COMPRESSION_WORKSPACE_BYTES)?;
    let capacity = zstd::zstd_safe::compress_bound(body.len())
        .min(usize::try_from(limit).unwrap_or(usize::MAX));
    let mut compressed = Buffer::new(capacity, memory, task)?;
    #[cfg(test)]
    evidence::compression();
    let result = zstd::stream::copy_encode(body, &mut compressed, SEARCH_COMPRESSION_LEVEL);
    checkpoint(task)?;
    result.map_err(|error| {
        SkeinError::Storage(format!(
            "search segment compressed bytes budget or codec failure: {error}"
        ))
    })?;
    // The native context and Rust stream buffer have dropped; retain the output.
    drop(workspace);
    let compressed_checksum = checksum_bytes(compressed.as_ref());
    let uncompressed_checksum = checksum_bytes(body);
    let header = build_io::formatted(256, memory, task, |output| {
        write!(output, "{SEARCH_COMPRESSION_HEADER}\ncodec\tzstd\nuncompressed_checksum\t{uncompressed_checksum}\ncompressed_checksum\t{compressed_checksum}\nuncompressed_len\t{}\ncompressed_len\t{}\n\n", body.len(), compressed.len())
    })?;
    let length = checked_add(header.len(), compressed.len())?;
    if length as u64 > limit {
        return Err(SkeinError::Storage(
            "search segment compressed bytes budget exceeded".to_owned(),
        ));
    }
    let mut output = Buffer::new(length, memory, task)?;
    output.write_all(header.as_ref())?;
    output.write_all(compressed.as_ref())?;
    checkpoint(task)?;
    Ok(output)
}

#[cfg(test)]
pub(super) mod evidence {
    use std::cell::Cell;
    thread_local! { static COMPRESSIONS: Cell<usize> = const { Cell::new(0) }; }
    pub(super) fn compression() {
        COMPRESSIONS.with(|count| count.set(count.get() + 1));
    }
    pub(in super::super) fn take() -> usize {
        COMPRESSIONS.with(|count| count.replace(0))
    }
}
