//! Incremental document frequencies with operation-owned retained-state charges.

use super::{DeltaDocument, LexicalProjectionConfig};
use crate::build_control::checkpoint;
use crate::build_memory::{checked_add, BuildMemory, SET_ENTRY_BYTES};
use crate::error::{Result, SkeinError};
use crate::token_parts::IdentifierParts;
use crate::{SearchAnalyzerLexicon, SearchDocument, TITLE_TERM_FREQUENCY_WEIGHT};
use skein_core::RuntimeTaskContext;
use skein_executor::QueryMemoryLease;
use std::collections::{BTreeMap, BTreeSet};

pub(super) mod tokens;

pub(super) struct AnalyzedDocument {
    pub(super) document: DeltaDocument,
    // Keep the charge while the consuming iterator still owns map nodes/keys.
    pub(super) _memory: Charge,
}

pub(super) struct Charge(Option<QueryMemoryLease>);

impl Charge {
    pub(super) fn new(memory: Option<&BuildMemory>) -> Result<Self> {
        Ok(Self(
            memory
                .map(|memory| memory.retained.reserve(0))
                .transpose()?,
        ))
    }

    pub(super) fn grow(&mut self, bytes: usize) -> Result<()> {
        if let Some(lease) = &mut self.0 {
            lease.grow(bytes)?;
        }
        Ok(())
    }

    fn shrink(&mut self, bytes: usize) {
        if let Some(lease) = &mut self.0 {
            lease.shrink(bytes);
        }
    }
}

struct Frequencies<'a> {
    document: DeltaDocument,
    memory: Charge,
    config: LexicalProjectionConfig,
    task: Option<&'a RuntimeTaskContext>,
}

impl Frequencies<'_> {
    fn check(&self) -> Result<()> {
        self.task.map_or(Ok(()), checkpoint)
    }

    fn push(&mut self, token: &str, weight: usize) -> Result<()> {
        self.check()?;
        if token.len() as u64 > self.config.max_term_bytes.get() {
            return Err(SkeinError::Storage(format!(
                "lexical term uses {} bytes, exceeding {}",
                token.len(),
                self.config.max_term_bytes
            )));
        }
        let count = checked_add(self.document.document_len as usize, weight)?;
        if count > self.config.max_document_tokens.get() {
            return Err(SkeinError::Storage(format!(
                "lexical document produced more than {} tokens",
                self.config.max_document_tokens
            )));
        }
        let count = u32::try_from(count)
            .map_err(|_| SkeinError::Storage("lexical document length exceeds u32".to_string()))?;
        if let Some(frequency) = self.document.frequencies.get_mut(token) {
            *frequency = frequency.saturating_add(weight as u32);
        } else {
            let required = self
                .document
                .resident_bytes
                .saturating_add(token.len() as u64 + 32);
            if required > self.config.build_memory_bytes.get() {
                return Err(SkeinError::Storage(format!(
                    "lexical document requires more than {} analyzer bytes",
                    self.config.build_memory_bytes
                )));
            }
            self.memory
                .grow(checked_add(SET_ENTRY_BYTES, token.len())?)?;
            #[cfg(test)]
            evidence::insert();
            self.document
                .frequencies
                .insert(token.to_string(), weight as u32);
            self.document.resident_bytes = required;
        }
        self.document.document_len = count;
        Ok(())
    }

    fn field(
        &mut self,
        text: &str,
        weight: usize,
        lexicon: &SearchAnalyzerLexicon,
        memory: Option<&BuildMemory>,
    ) -> Result<()> {
        let mut seen_memory = Charge::new(memory)?;
        let mut seen = BTreeSet::<String>::new();
        let mut previous = None::<&str>;
        for raw in text.split(|ch: char| !ch.is_alphanumeric() && ch != '_') {
            self.check()?;
            if raw.is_empty() {
                continue;
            }
            #[cfg(test)]
            evidence::raw();
            let mut parts = IdentifierParts::new(raw);
            let first = parts.next();
            if let (Some(previous), Some(first)) = (previous, first) {
                let boundary = tokens::Text::boundary(previous, first, memory)?;
                tokens::boundary(boundary.as_str(), lexicon, self.task, memory, |token| {
                    // Boundary tokens are unique across this field. Ordinary
                    // identifier occurrences still contribute repeatedly.
                    if !seen.contains(token) {
                        self.push(token, weight)?;
                        seen_memory.grow(checked_add(SET_ENTRY_BYTES, token.len())?)?;
                        seen.insert(token.to_string());
                    }
                    Ok(())
                })?;
            }
            tokens::identifier(raw, lexicon, self.task, memory, |token| {
                self.push(token, weight)?;
                if !seen.contains(token) {
                    seen_memory.grow(checked_add(SET_ENTRY_BYTES, token.len())?)?;
                    seen.insert(token.to_string());
                }
                Ok(())
            })?;
            if let Some(last) = parts.last().or(first) {
                previous = Some(last);
            }
        }
        drop(seen);
        drop(seen_memory);
        Ok(())
    }
}

pub(super) fn analyze(
    document: &SearchDocument,
    lexicon: &SearchAnalyzerLexicon,
    config: LexicalProjectionConfig,
    task: Option<&RuntimeTaskContext>,
    memory: Option<&BuildMemory>,
) -> Result<AnalyzedDocument> {
    task.map_or(Ok(()), checkpoint)?;
    let source_bytes = document
        .title
        .len()
        .saturating_add(document.content.len())
        .saturating_add(
            document
                .metadata
                .iter()
                .fold(0usize, |bytes, (key, value)| {
                    bytes.saturating_add(key.len()).saturating_add(value.len())
                }),
        );
    if source_bytes as u64 > config.max_document_source_bytes.get() {
        return Err(SkeinError::Storage(format!(
            "lexical document uses {source_bytes} source bytes, exceeding {}",
            config.max_document_source_bytes
        )));
    }
    let mut frequencies = Frequencies {
        document: DeltaDocument {
            document_len: 0,
            frequencies: BTreeMap::new(),
            resident_bytes: document.id.len() as u64 + 64,
            base: None,
        },
        memory: Charge::new(memory)?,
        config,
        task,
    };
    frequencies.field(
        &document.title,
        TITLE_TERM_FREQUENCY_WEIGHT,
        lexicon,
        memory,
    )?;
    frequencies.field(&document.content, 1, lexicon, memory)?;
    for key in ["kind", "external_id", "source_id", "space_id"] {
        if let Some(text) = document.metadata.get(key) {
            frequencies.field(text, 1, lexicon, memory)?;
        }
    }
    Ok(AnalyzedDocument {
        document: frequencies.document,
        _memory: frequencies.memory,
    })
}

#[cfg(test)]
pub(super) mod evidence {
    use std::cell::Cell;
    thread_local! {
        static COUNTS: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
    }
    pub(super) fn insert() {
        COUNTS.with(|cell| {
            let (raw, inserts) = cell.get();
            cell.set((raw, inserts + 1));
        });
    }
    pub(super) fn raw() {
        COUNTS.with(|cell| {
            let (raw, inserts) = cell.get();
            cell.set((raw + 1, inserts));
        });
    }
    pub(in super::super) fn take() -> (usize, usize) {
        COUNTS.with(|cell| cell.replace((0, 0)))
    }
}
