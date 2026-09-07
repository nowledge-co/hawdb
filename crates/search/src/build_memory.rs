//! Operation-owned generation working-set accounting shared across build stages.

pub(crate) mod path;

use crate::document_codec::Fields;
use crate::error::{Result, SkeinError};
use crate::{SearchDocument, SearchProjectionRow};
use skein_core::RuntimeTaskContext;
use skein_executor::{QueryMemoryAccount, QueryMemoryClass, QueryMemoryLease, QueryMemoryLedger};
use std::mem::size_of;
use std::num::NonZeroUsize;
use std::ops::Deref;

// Conservative per-entry container allowances, not allocator/RSS measurements.
// Include String slots, B-tree node occupancy and transient node split slack.
pub(crate) const SET_ENTRY_BYTES: usize = 1024;
pub(crate) const MAP_ENTRY_BYTES: usize = 2048;
pub(crate) const SPOOL_BUFFER_BYTES: usize = 8192;

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
        let limit = task.memory_reservation().map_or(usize::MAX, |reservation| {
            usize::try_from(reservation.memory_bytes()).unwrap_or(usize::MAX)
        });
        let limit = NonZeroUsize::new(limit).ok_or_else(|| {
            SkeinError::Execution("search build has no admitted working memory".to_string())
        })?;
        let ledger = QueryMemoryLedger::new(limit);
        // Accounts are operation-scoped, not record-scoped. The ledger keeps
        // account metadata until operation end, so creating one per row leaks
        // accounting state in an otherwise streaming build.
        let input = ledger.account(
            QueryMemoryClass::PipelineBatch,
            "search build documents",
            limit,
        );
        let spool = ledger.account(QueryMemoryClass::SpillStaging, "search build spool", limit);
        let retained = ledger.account(
            QueryMemoryClass::BlockingState,
            "search build retained state",
            limit,
        );
        Ok(Self {
            #[cfg(test)]
            ledger,
            input,
            spool,
            retained,
        })
    }

    pub(crate) fn admit_document(&self, document: SearchDocument) -> Result<AdmittedDocument> {
        let lease = self.input.reserve(document_bytes(&document)?)?;
        Ok(AdmittedDocument {
            document,
            _lease: lease,
        })
    }

    pub(crate) fn decode_document(
        &self,
        line: &str,
        max_metadata_fields: usize,
    ) -> Result<AdmittedDocument> {
        let admitted = decoded_document_bytes(line, max_metadata_fields)?;
        let mut lease = self.input.reserve(admitted)?;
        #[cfg(test)]
        decode_evidence::record();
        let document = crate::decode_search_document_line(line)?;
        let actual = document_bytes(&document)?;
        if actual > admitted {
            return Err(SkeinError::Execution(
                "search decoded document capacity exceeded preflight".to_string(),
            ));
        }
        lease.shrink(admitted - actual);
        Ok(AdmittedDocument {
            document,
            _lease: lease,
        })
    }
}

pub(crate) struct AdmittedDocument {
    // Rust drops fields in declaration order: release heap data before capacity.
    pub(crate) document: SearchDocument,
    _lease: QueryMemoryLease,
}

impl AdmittedDocument {
    #[cfg(test)]
    pub(crate) fn into_parts(self) -> (SearchDocument, QueryMemoryLease) {
        (self.document, self._lease)
    }

    #[cfg(test)]
    pub(crate) fn retained_bytes(&self) -> usize {
        self._lease.bytes()
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

fn overflow() -> SkeinError {
    SkeinError::Execution("search build memory accounting overflow".to_string())
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
    let mut bytes = 0;
    // Removing the final map entry may retain an allocated empty leaf root.
    if metadata.is_empty() {
        bytes = checked_add(bytes, MAP_ENTRY_BYTES)?;
    }
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

fn decoded_document_bytes(line: &str, max_metadata_fields: usize) -> Result<usize> {
    decoded_document_bytes_with_context(line, max_metadata_fields, None)
}

pub(crate) fn decoded_document_bytes_with_context(
    line: &str,
    max_metadata_fields: usize,
    task: Option<&RuntimeTaskContext>,
) -> Result<usize> {
    let checkpoint = || task.map_or(Ok(()), crate::build_control::checkpoint);
    checkpoint()?;
    let fields = Fields::parse(line)?;
    let mut bytes = size_of::<SearchDocument>();
    for field in [fields.id, fields.title, fields.content] {
        bytes = checked_add(bytes, field.len() / 2)?;
    }
    if !fields.embedding.is_empty() {
        let mut count = 0;
        for _ in fields.embedding.split(',') {
            checkpoint()?;
            count = checked_add(count, 1)?;
        }
        bytes = checked_add(bytes, checked_mul(count, size_of::<f32>())?)?;
    }
    if fields.metadata.is_empty() {
        // Match the owned-document contract even for a newly empty map.
        bytes = checked_add(bytes, MAP_ENTRY_BYTES)?;
    } else {
        for (index, pair) in fields.metadata.split(';').enumerate() {
            checkpoint()?;
            if index >= max_metadata_fields {
                return Err(SkeinError::Storage(
                    "search spool metadata field count exceeds admission".to_string(),
                ));
            }
            let (key, value) = pair.split_once('=').ok_or_else(|| {
                SkeinError::Storage("search spool metadata pair is invalid".to_string())
            })?;
            bytes = checked_add(bytes, MAP_ENTRY_BYTES)?;
            bytes = checked_add(bytes, key.len() / 2)?;
            bytes = checked_add(bytes, value.len() / 2)?;
        }
    }
    checkpoint()?;
    Ok(bytes)
}

#[cfg(test)]
pub(crate) mod decode_evidence {
    use std::cell::Cell;
    thread_local! {
        static CALLS: Cell<usize> = const { Cell::new(0) };
    }
    pub(super) fn record() {
        CALLS.with(|calls| calls.set(calls.get() + 1));
    }
    pub(crate) fn take() -> usize {
        CALLS.with(|calls| calls.replace(0))
    }
}

#[cfg(test)]
mod tests;
