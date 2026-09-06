use super::{decode_block_header, BlockDescriptor, BlockKind, ReadContext, SliceCursor};
use crate::error::{Result, SkeinError};
use std::sync::Arc;

/// One bounded mapping block serves the monotonically increasing posting merge.
pub(super) struct DocumentLookup<'a> {
    read: ReadContext<'a>,
    bytes: Arc<[u8]>,
    block: Option<&'a BlockDescriptor>,
    cursor: usize,
    next_ordinal: u64,
    pub(super) bytes_read: u64,
}

impl<'a> DocumentLookup<'a> {
    #[cfg(test)]
    pub(super) fn new(projection: &'a super::LexicalProjectionReader) -> Self {
        Self::with_context(ReadContext {
            projection,
            task: None,
        })
    }

    pub(super) fn with_context(read: ReadContext<'a>) -> Self {
        Self {
            read,
            bytes: Arc::from([]),
            block: None,
            cursor: 0,
            next_ordinal: 0,
            bytes_read: 0,
        }
    }

    pub(super) fn get(&mut self, ordinal: u64) -> Result<(String, u32)> {
        self.read.checkpoint()?;
        if ordinal >= self.read.projection.manifest.document_count || ordinal < self.next_ordinal {
            return Err(SkeinError::Storage(
                "lexical document lookup ordinal is invalid or unordered".to_string(),
            ));
        }
        if self
            .block
            .is_none_or(|block| ordinal >= block.ordinal_start + u64::from(block.entry_count))
        {
            let blocks = &self.read.projection.manifest.blocks;
            let index = blocks.partition_point(|block| {
                block.kind == BlockKind::Documents
                    && block.ordinal_start + u64::from(block.entry_count) <= ordinal
            });
            let block = blocks.get(index).ok_or_else(|| {
                SkeinError::Storage("lexical document mapping is missing".to_string())
            })?;
            if block.kind != BlockKind::Documents || ordinal < block.ordinal_start {
                return Err(SkeinError::Storage(
                    "lexical document mapping is missing".to_string(),
                ));
            }
            // Release the old allocation before admitting another mapping block.
            self.bytes = Arc::from([]);
            let (bytes, read) = self.read.read_cached_range(
                block.offset,
                usize::try_from(block.length).map_err(|_| {
                    SkeinError::Storage("document mapping exceeds the address space".to_string())
                })?,
                block.checksum,
            )?;
            self.bytes = bytes;
            validate_document_block_with_checkpoint(
                &self.bytes,
                self.read.projection.manifest.generation,
                block,
                &mut || self.read.checkpoint(),
            )?;
            self.bytes_read = self.bytes_read.saturating_add(read);
            self.block = Some(block);
            self.cursor = 29;
            self.next_ordinal = block.ordinal_start;
        }
        let mut cursor = SliceCursor {
            bytes: &self.bytes,
            offset: self.cursor,
        };
        loop {
            if self.next_ordinal.is_multiple_of(128) {
                self.read.checkpoint()?;
            }
            let id = cursor.str(1024 * 1024)?;
            let length = cursor.u32()?;
            let current = self.next_ordinal;
            self.next_ordinal += 1;
            self.cursor = cursor.offset;
            if current == ordinal {
                return Ok((id.to_owned(), length));
            }
        }
    }
}

#[cfg(test)]
pub(super) fn validate_document_block(
    bytes: &[u8],
    generation: u64,
    descriptor: &BlockDescriptor,
) -> Result<()> {
    validate_document_block_with_checkpoint(bytes, generation, descriptor, &mut || Ok(()))
}

fn validate_document_block_with_checkpoint(
    bytes: &[u8],
    generation: u64,
    descriptor: &BlockDescriptor,
    checkpoint: &mut impl FnMut() -> Result<()>,
) -> Result<()> {
    let mut cursor = SliceCursor::new(bytes);
    let count = decode_block_header(&mut cursor, generation, descriptor, BlockKind::Documents)?;
    let mut first = None;
    let mut previous = None;
    for index in 0..count {
        if index.is_multiple_of(128) {
            checkpoint()?;
        }
        let id = cursor.str(1024 * 1024)?;
        let _length = cursor.u32()?;
        if previous.is_some_and(|previous| previous >= id) {
            return Err(SkeinError::Storage(
                "lexical document IDs are not ordered".to_string(),
            ));
        }
        first.get_or_insert(id);
        previous = Some(id);
    }
    if !cursor.is_empty()
        || first != Some(descriptor.min_key.as_str())
        || previous != Some(descriptor.max_key.as_str())
    {
        return Err(SkeinError::Storage(
            "lexical document mapping bounds are invalid".to_string(),
        ));
    }
    Ok(())
}
