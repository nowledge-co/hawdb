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
use hawdb_core::RuntimeTaskContext;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DocumentIdProbe {
    pub present: bool,
    pub bytes_read: u64,
}

/// One bounded mapping block serves the monotonically increasing posting merge.
pub(super) struct DocumentLookup<'a> {
    projection: &'a LexicalProjectionReader,
    task: Option<&'a RuntimeTaskContext>,
    bytes: Vec<u8>,
    block: Option<&'a BlockDescriptor>,
    cursor: usize,
    next_ordinal: u64,
    pub(super) bytes_read: u64,
}

impl<'a> DocumentLookup<'a> {
    pub(super) fn new(
        projection: &'a LexicalProjectionReader,
        task: Option<&'a RuntimeTaskContext>,
    ) -> Self {
        Self {
            projection,
            task,
            bytes: Vec::new(),
            block: None,
            cursor: 0,
            next_ordinal: 0,
            bytes_read: 0,
        }
    }

    pub(super) fn get(&mut self, ordinal: u64) -> Result<(String, u32)> {
        if let Some(task) = self.task {
            super::query_context::checkpoint(task)?;
        }
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
            self.bytes = self.projection.read_block_with_task(block, self.task)?;
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
            if let Some(task) = self.task {
                super::query_context::checkpoint(task)?;
            }
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

impl LexicalProjectionReader {
    /// Conservative capacity for one mapping block and transient decoded IDs.
    /// Probes run sequentially, so a caller can retain this admission across them.
    pub(crate) fn document_id_probe_memory_bytes(&self) -> Result<usize> {
        let largest = self
            .manifest
            .blocks
            .iter()
            .filter(|block| block.kind == BlockKind::Documents)
            .map(|block| block.length)
            .max()
            .unwrap_or(0);
        let bytes = usize::try_from(largest)
            .map_err(|_| HawDBError::Execution("document ID probe exceeds address space".into()))?;
        bytes
            .checked_mul(8)
            .and_then(|bytes| bytes.checked_add(4 * std::mem::size_of::<String>()))
            .ok_or_else(|| HawDBError::Execution("document ID probe capacity overflow".into()))
    }

    /// Probes one document ID through its bounded, file-backed mapping block.
    ///
    /// The manifest keeps document blocks ordered by their ID bounds, so a
    /// membership probe reads at most one already-admitted block. This keeps
    /// callers from retaining a complete artifact ID set merely to resolve a
    /// newer segment's version of a document.
    pub(crate) fn probe_document_id(&self, document_id: &str) -> Result<DocumentIdProbe> {
        self.probe_document_id_with_admission(document_id, None)
    }

    pub(crate) fn probe_document_id_with_admission(
        &self,
        document_id: &str,
        admission: Option<(&crate::build_memory::read::ReadAdmission<'_>, u64)>,
    ) -> Result<DocumentIdProbe> {
        let context =
            admission.map(|(admission, _)| super::QueryContext::from_read_admission(admission));
        super::query_context::check(context)?;
        let Some(block) = self.document_id_probe_block(document_id) else {
            return Ok(DocumentIdProbe {
                present: false,
                bytes_read: 0,
            });
        };

        let _decode = admission
            .map(|(admission, _)| admission.scratch(self.document_id_probe_memory_bytes()?))
            .transpose()?;
        if block.length > self.config.max_block_bytes.get() {
            return Err(HawDBError::Storage(
                "lexical document block exceeds read budget".into(),
            ));
        }
        let bytes = match admission {
            Some((admission, maximum)) => admission.read_file_range(
                &self.file,
                self.manifest.artifact_len,
                maximum,
                crate::build_memory::read::FileRange {
                    offset: block.offset,
                    length: block.length,
                    checksum: block.checksum,
                },
            )?,
            None => crate::build_memory::read::ReadBytes::unaccounted(self.read_block(block)?),
        };
        probe_document_bytes(
            &bytes,
            self.manifest.generation,
            block,
            document_id,
            context,
        )
    }

    /// Borrow one query-funded block under the original task's I/O scope.
    pub(crate) fn probe_document_id_with_query(
        &self,
        document_id: &str,
        context: super::QueryContext<'_>,
    ) -> Result<DocumentIdProbe> {
        context.checkpoint()?;
        let Some(block) = self.document_id_probe_block(document_id) else {
            return Ok(DocumentIdProbe {
                present: false,
                bytes_read: 0,
            });
        };
        if block.length > self.config.max_block_bytes.get()
            || block
                .offset
                .checked_add(block.length)
                .is_none_or(|end| end > self.manifest.artifact_len)
        {
            return Err(HawDBError::Storage(
                "lexical document range exceeds its artifact or read budget".into(),
            ));
        }
        let length = usize::try_from(block.length)
            .map_err(|_| HawDBError::Execution("document ID probe exceeds address space".into()))?;
        let _memory = context.memory.reserve(length)?;
        let mut bytes = vec![0; length];
        let mut input = crate::build_memory::read::QueryFileRange {
            file: &self.file,
            expected_length: Some(self.manifest.artifact_len),
            offset: block.offset,
            remaining: block.length,
            context,
            require_execution: context.execution.is_some(),
        };
        std::io::Read::read_exact(&mut input, &mut bytes)
            .map_err(crate::build_memory::read::io_error)?;
        if super::checksum(&bytes) != block.checksum {
            return Err(HawDBError::Storage(
                "lexical document block checksum mismatch".into(),
            ));
        }
        let result = probe_document_bytes(
            &bytes,
            self.manifest.generation,
            block,
            document_id,
            Some(context),
        )?;
        context.checkpoint()?;
        Ok(result)
    }

    fn document_id_probe_block(&self, document_id: &str) -> Option<&BlockDescriptor> {
        let blocks = &self.manifest.blocks;
        let index = blocks.partition_point(|block| {
            block.kind == BlockKind::Documents && block.max_key.as_str() < document_id
        });
        blocks.get(index).filter(|block| {
            block.kind == BlockKind::Documents && block.min_key.as_str() <= document_id
        })
    }
}

fn probe_document_bytes(
    bytes: &[u8],
    generation: u64,
    block: &BlockDescriptor,
    document_id: &str,
    context: Option<super::QueryContext<'_>>,
) -> Result<DocumentIdProbe> {
    validate_document_block_with_context(bytes, generation, block, context)?;
    let mut cursor = SliceCursor::new(bytes);
    let count = decode_block_header(&mut cursor, generation, block, BlockKind::Documents)?;
    for _ in 0..count {
        super::query_context::check(context)?;
        let id = cursor.borrowed_string(1024 * 1024)?;
        let ordering = id.cmp(document_id);
        if ordering.is_ge() {
            return Ok(DocumentIdProbe {
                present: ordering.is_eq(),
                bytes_read: bytes.len() as u64,
            });
        }
        let _length = cursor.u32()?;
    }
    Ok(DocumentIdProbe {
        present: false,
        bytes_read: bytes.len() as u64,
    })
}

pub(super) fn validate_document_block(
    bytes: &[u8],
    generation: u64,
    descriptor: &BlockDescriptor,
) -> Result<()> {
    validate_document_block_with_context(bytes, generation, descriptor, None)
}

fn validate_document_block_with_context(
    bytes: &[u8],
    generation: u64,
    descriptor: &BlockDescriptor,
    context: Option<super::QueryContext<'_>>,
) -> Result<()> {
    let mut cursor = SliceCursor::new(bytes);
    let count = decode_block_header(&mut cursor, generation, descriptor, BlockKind::Documents)?;
    let mut first = None;
    let mut previous = None;
    for _ in 0..count {
        super::query_context::check(context)?;
        let id = cursor.borrowed_string(1024 * 1024)?;
        let _length = cursor.u32()?;
        if previous.is_some_and(|previous: &str| previous >= id) {
            return Err(HawDBError::Storage(
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
        return Err(HawDBError::Storage(
            "lexical document mapping bounds are invalid".to_string(),
        ));
    }
    Ok(())
}
