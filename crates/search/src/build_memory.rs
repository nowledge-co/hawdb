//! One generation operation owns these accounts across all of its build stages.

use crate::build_control::checkpoint;
use crate::error::{Result, SkeinError};
use crate::SearchDocument;
use crate::SearchProjectionRow;
use skein_core::RuntimeTaskContext;
use skein_executor::{QueryMemoryAccount, QueryMemoryClass, QueryMemoryLease, QueryMemoryLedger};
use std::borrow::Borrow;
use std::mem::size_of;
use std::num::NonZeroUsize;
use std::ops::Deref;

// Conservative B-tree node/split allowances, not allocator or RSS measurements.
pub(crate) const MAP_ENTRY_BYTES: usize = 2048;
pub(crate) const SET_ENTRY_BYTES: usize = 1024;
pub(crate) const SPOOL_BUFFER_BYTES: usize = 8192;

pub(crate) mod capacity;
pub(crate) mod compression;
pub(crate) mod decoder;
pub(crate) mod directory;
pub(crate) mod path;
pub(crate) mod reserved;
pub(crate) mod shared;

#[derive(Debug, Clone)]
pub(crate) struct BuildMemory {
    #[cfg(test)]
    pub(crate) ledger: QueryMemoryLedger,
    pub(crate) input: QueryMemoryAccount,
    pub(crate) spool: QueryMemoryAccount,
    pub(crate) retained: QueryMemoryAccount,
}

impl BuildMemory {
    pub(crate) fn new(task: &RuntimeTaskContext) -> Result<Self> {
        checkpoint(task)?;
        let limit = task
            .memory_reservation()
            .map_or(Ok(usize::MAX), |reservation| {
                usize::try_from(reservation.memory_bytes()).map_err(|_| {
                    SkeinError::Execution(
                        "search build memory reservation does not fit the address space".into(),
                    )
                })
            })?;
        let limit = NonZeroUsize::new(limit).ok_or_else(|| {
            SkeinError::Execution("search build has no admitted working memory".into())
        })?;
        let ledger = QueryMemoryLedger::new(limit);
        // The ledger retains account metadata until operation end. Reuse these
        // accounts for every record and clone them when work crosses stages.
        let input = ledger.account(
            QueryMemoryClass::PipelineBatch,
            "search build documents",
            limit,
        );
        let spool = ledger.account(QueryMemoryClass::SpillStaging, "search build spool", limit);
        let retained = ledger.account(QueryMemoryClass::BlockingState, "search build state", limit);
        Ok(Self {
            #[cfg(test)]
            ledger,
            input,
            spool,
            retained,
        })
    }

    pub(crate) fn admit_document(&self, document: SearchDocument) -> Result<AdmittedDocument> {
        let memory = self.input.reserve(document_bytes(&document)?)?;
        Ok(AdmittedDocument {
            document,
            _memory: DocumentMemory::Individual(memory),
        })
    }
}

pub(crate) struct AdmittedDocument {
    // Declaration order releases the owned payload before its capacity lease.
    pub(crate) document: SearchDocument,
    _memory: DocumentMemory,
}

enum DocumentMemory {
    Individual(QueryMemoryLease),
    Batch(shared::Shared<QueryMemoryLease>),
}

impl AdmittedDocument {
    pub(crate) fn from_admitted_parts(document: SearchDocument, memory: QueryMemoryLease) -> Self {
        Self {
            document,
            _memory: DocumentMemory::Individual(memory),
        }
    }

    pub(crate) fn from_batch(
        document: SearchDocument,
        memory: shared::Shared<QueryMemoryLease>,
    ) -> Self {
        let admitted = Self {
            document,
            _memory: DocumentMemory::Batch(memory),
        };
        debug_assert!(admitted.retained_bytes() >= document_bytes(&admitted).unwrap());
        admitted
    }

    #[cfg(test)]
    pub(crate) fn into_parts(self) -> (SearchDocument, QueryMemoryLease) {
        match self._memory {
            DocumentMemory::Individual(memory) => (self.document, memory),
            DocumentMemory::Batch(_) => panic!("spool test helper requires an individual owner"),
        }
    }

    pub(crate) fn retained_bytes(&self) -> usize {
        match &self._memory {
            DocumentMemory::Individual(memory) => memory.bytes(),
            DocumentMemory::Batch(memory) => memory.bytes(),
        }
    }
}

impl Borrow<SearchDocument> for AdmittedDocument {
    fn borrow(&self) -> &SearchDocument {
        &self.document
    }
}

impl Deref for AdmittedDocument {
    type Target = SearchDocument;

    fn deref(&self) -> &Self::Target {
        &self.document
    }
}

pub(crate) fn checked_add(left: usize, right: usize) -> Result<usize> {
    left.checked_add(right).ok_or_else(overflow)
}

pub(crate) fn checked_mul(left: usize, right: usize) -> Result<usize> {
    left.checked_mul(right).ok_or_else(overflow)
}

pub(crate) fn reserve_capacity<T>(
    values: &mut Vec<T>,
    capacity: usize,
    lease: &mut QueryMemoryLease,
) -> Result<()> {
    capacity::reserve(
        values,
        capacity,
        capacity::Memory::Lease(lease),
        "search build capacity",
    )
}

pub(crate) fn grow_slots<T>(values: &mut Vec<T>, lease: &mut QueryMemoryLease) -> Result<()> {
    if values.len() == values.capacity() {
        reserve_capacity(values, checked_mul(values.capacity().max(2), 2)?, lease)?;
    }
    Ok(())
}

fn overflow() -> SkeinError {
    SkeinError::Execution("search build memory accounting overflow".into())
}

pub(crate) fn document_bytes(document: &SearchDocument) -> Result<usize> {
    checked_add(
        size_of::<SearchDocument>(),
        field_bytes(
            [&document.id, &document.title, &document.content],
            document.embedding.as_ref().map_or(0, Vec::capacity),
            &document.metadata,
        )?,
    )
}

pub(crate) fn projection_row_bytes(row: &SearchProjectionRow) -> Result<usize> {
    checked_add(
        checked_add(
            size_of::<SearchProjectionRow>(),
            row.source_id.as_ref().map_or(0, String::capacity),
        )?,
        field_bytes(
            [&row.external_id, &row.title, &row.body],
            row.embedding.as_ref().map_or(0, Vec::capacity),
            &row.metadata,
        )?,
    )
}

fn field_bytes(
    strings: [&String; 3],
    embedding_capacity: usize,
    metadata: &std::collections::BTreeMap<String, String>,
) -> Result<usize> {
    // Removing the final entry may leave an allocated empty leaf root.
    let mut bytes = if metadata.is_empty() {
        MAP_ENTRY_BYTES
    } else {
        0
    };
    for field in strings {
        bytes = checked_add(bytes, field.capacity())?;
    }
    bytes = checked_add(bytes, checked_mul(embedding_capacity, size_of::<f32>())?)?;
    for (key, value) in metadata {
        bytes = checked_add(bytes, MAP_ENTRY_BYTES)?;
        bytes = checked_add(bytes, key.capacity())?;
        bytes = checked_add(bytes, value.capacity())?;
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests;
