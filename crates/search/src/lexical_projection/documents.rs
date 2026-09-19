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
    decode_block_header, BlockDescriptor, BlockKind, HawDBError, LexicalProjectionReader, Result,
    SliceCursor,
};

/// One bounded mapping block serves the monotonically increasing posting merge.
pub(super) struct DocumentLookup<'a> {
    projection: &'a LexicalProjectionReader,
    bytes: Vec<u8>,
    block: Option<&'a BlockDescriptor>,
    cursor: usize,
    next_ordinal: u64,
    pub(super) bytes_read: u64,
}

impl<'a> DocumentLookup<'a> {
    pub(super) fn new(projection: &'a LexicalProjectionReader) -> Self {
        Self {
            projection,
            bytes: Vec::new(),
            block: None,
            cursor: 0,
            next_ordinal: 0,
            bytes_read: 0,
        }
    }

    pub(super) fn get(&mut self, ordinal: u64) -> Result<(String, u32)> {
        if ordinal >= self.projection.manifest.document_count || ordinal < self.next_ordinal {
            return Err(HawDBError::Storage(
                "lexical document lookup ordinal is invalid or unordered".to_string(),
            ));
        }
        if self
            .block
            .is_none_or(|block| ordinal >= block.ordinal_start + u64::from(block.entry_count))
        {
            let blocks = &self.projection.manifest.blocks;
            let index = blocks.partition_point(|block| {
                block.kind == BlockKind::Documents
                    && block.ordinal_start + u64::from(block.entry_count) <= ordinal
            });
            let block = blocks.get(index).ok_or_else(|| {
                HawDBError::Storage("lexical document mapping is missing".to_string())
            })?;
            if block.kind != BlockKind::Documents || ordinal < block.ordinal_start {
                return Err(HawDBError::Storage(
                    "lexical document mapping is missing".to_string(),
                ));
            }
            self.bytes = self.projection.read_block(block)?;
            validate_document_block(&self.bytes, self.projection.manifest.generation, block)?;
            self.bytes_read = self.bytes_read.saturating_add(self.bytes.len() as u64);
            self.block = Some(block);
            self.cursor = 29;
            self.next_ordinal = block.ordinal_start;
        }
        let mut cursor = SliceCursor {
            bytes: &self.bytes,
            offset: self.cursor,
        };
        loop {
            let id = cursor.string(1024 * 1024)?;
            let length = cursor.u32()?;
            let current = self.next_ordinal;
            self.next_ordinal = self.next_ordinal.checked_add(1).ok_or_else(|| {
                HawDBError::Storage("lexical document ordinal overflow".to_string())
            })?;
            self.cursor = cursor.offset;
            if current == ordinal {
                return Ok((id, length));
            }
        }
    }
}

pub(super) fn validate_document_block(
    bytes: &[u8],
    generation: u64,
    descriptor: &BlockDescriptor,
) -> Result<()> {
    let mut cursor = SliceCursor::new(bytes);
    let count = decode_block_header(&mut cursor, generation, descriptor, BlockKind::Documents)?;
    let mut first = None;
    let mut previous = None;
    for _ in 0..count {
        let id = cursor.string(1024 * 1024)?;
        let _length = cursor.u32()?;
        if previous
            .as_ref()
            .is_some_and(|previous: &String| previous >= &id)
        {
            return Err(HawDBError::Storage(
                "lexical document IDs are not ordered".to_string(),
            ));
        }
        first.get_or_insert_with(|| id.clone());
        previous = Some(id);
    }
    if !cursor.is_empty()
        || first.as_deref() != Some(descriptor.min_key.as_str())
        || previous.as_deref() != Some(descriptor.max_key.as_str())
    {
        return Err(HawDBError::Storage(
            "lexical document mapping bounds are invalid".to_string(),
        ));
    }
    Ok(())
}
