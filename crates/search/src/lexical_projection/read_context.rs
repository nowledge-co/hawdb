use super::{checksum, dictionary, dictionary_store, LexicalProjectionReader};
use crate::error::{Result, SkeinError};
use skein_core::RuntimeTaskContext;
use skein_storage::{ContentDigest, ManifestGeneration, RepresentationKind, SegmentCacheKey};
use std::num::NonZeroUsize;
use std::sync::Arc;

/// Per-operation controls, never retained in an immutable shared reader.
#[derive(Clone, Copy)]
pub(super) struct ReadContext<'a> {
    pub(super) projection: &'a LexicalProjectionReader,
    pub(super) task: Option<&'a RuntimeTaskContext>,
}

impl ReadContext<'_> {
    pub(super) fn checkpoint(&self) -> Result<()> {
        if let Some(task) = self.task {
            task.checkpoint()
                .map_err(|reason| SkeinError::Execution(format!("lexical task {reason}")))?;
        }
        Ok(())
    }

    fn validate_range(&self, offset: u64, length: usize) -> Result<()> {
        self.checkpoint()?;
        if length as u64 > self.projection.config.max_block_bytes.get()
            || offset
                .checked_add(length as u64)
                .is_none_or(|end| end > self.projection.manifest.artifact_len)
        {
            return Err(SkeinError::Storage(
                "lexical range exceeds its artifact or read budget".to_string(),
            ));
        }
        Ok(())
    }

    pub(super) fn read_range(&self, offset: u64, length: usize) -> Result<Vec<u8>> {
        self.validate_range(offset, length)?;
        let _permit = self
            .task
            .map(|task| {
                task.acquire_io_wave(NonZeroUsize::MIN).map_err(|error| {
                    SkeinError::Execution(format!("lexical I/O admission failed: {error}"))
                })
            })
            .transpose()?;
        self.checkpoint()?;
        let mut bytes = vec![0; length];
        crate::out_of_core::read_exact_at(&self.projection.file, offset, &mut bytes)?;
        self.checkpoint()?;
        Ok(bytes)
    }

    pub(super) fn read_cached_range(
        &self,
        offset: u64,
        length: usize,
        digest: u64,
    ) -> Result<(Arc<[u8]>, u64)> {
        self.validate_range(offset, length)?;
        let key = SegmentCacheKey {
            store_id: self.projection.cache_namespace,
            manifest_generation: ManifestGeneration(self.projection.manifest.generation),
            segment_id: offset,
            content_digest: ContentDigest(digest),
            representation: RepresentationKind::LexicalProjectionBlock,
        };
        if let Some(lease) = self.projection.cache.get(&key) {
            if lease.len() != length {
                return Err(SkeinError::Storage(
                    "lexical cache extent mismatch".to_string(),
                ));
            }
            return Ok((lease.into_arc(), 0));
        }
        let bytes = self.read_range(offset, length)?;
        if checksum(&bytes) != digest {
            return Err(SkeinError::Storage(
                "lexical range checksum mismatch".to_string(),
            ));
        }
        let lease = self.projection.cache.insert(key, bytes).map_err(|error| {
            SkeinError::Storage(format!("lexical cache admission failed: {error}"))
        })?;
        Ok((lease.into_arc(), length as u64))
    }

    pub(super) fn term_metadata(
        &self,
        term: &str,
        bytes_read: &mut u64,
    ) -> Result<Option<dictionary::Metadata>> {
        self.checkpoint()?;
        let dictionaries = &self.projection.manifest.dictionaries;
        let index = dictionaries.partition_point(|block| block.max_term.as_str() < term);
        let Some(descriptor) = dictionaries
            .get(index)
            .filter(|block| block.min_term.as_str() <= term)
        else {
            return Ok(None);
        };
        let length = usize::try_from(descriptor.length)
            .map_err(|_| SkeinError::Storage("dictionary exceeds the address space".to_string()))?;
        let (bytes, read) =
            self.read_cached_range(descriptor.offset, length, descriptor.checksum)?;
        *bytes_read = bytes_read.saturating_add(read);
        let dictionary = dictionary::Dictionary::open(
            &bytes,
            dictionary_store::limits(self.projection.config)?,
            &mut || {
                self.task.map_or(Ok(()), |task| {
                    task.checkpoint().map_err(|reason| reason.as_str())
                })
            },
        )
        .map_err(|error| {
            self.checkpoint().err().unwrap_or_else(|| {
                SkeinError::Storage(format!("invalid lexical dictionary: {error}"))
            })
        })?;
        descriptor.validate_dictionary(&dictionary, self.projection.manifest.document_count)?;
        self.checkpoint()?;
        dictionary
            .get(term)
            .map_err(|error| SkeinError::Storage(error.to_string()))
    }
}
