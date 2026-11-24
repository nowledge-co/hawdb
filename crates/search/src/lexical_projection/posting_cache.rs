// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Optional query-local reuse between corpus statistics and posting streams.

use super::*;

type Key = (usize, u64, u64, u64);
type Dictionary = fst::Map<PostingDictionaryBytes>;

pub(super) enum DictionaryStorage {
    Owned(Dictionary),
    Shared(Arc<Dictionary>),
}

impl std::ops::Deref for DictionaryStorage {
    type Target = Dictionary;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Owned(dictionary) => dictionary,
            Self::Shared(dictionary) => dictionary,
        }
    }
}

impl DictionaryStorage {
    pub(super) fn into_bytes(self) -> Option<Vec<u8>> {
        let dictionary = match self {
            Self::Owned(dictionary) => dictionary,
            Self::Shared(dictionary) => Arc::try_unwrap(dictionary).ok()?,
        };
        Some(dictionary.into_fst().into_inner().bytes)
    }
}

struct Entry {
    dictionary: Arc<Dictionary>,
    bytes: usize,
    _memory: Option<QueryMemoryLease>,
}

#[derive(Default)]
struct Entries {
    values: BTreeMap<Key, Entry>,
    bytes: usize,
}

/// Scoped to one query over its pinned readers. A reader address is only an
/// identity token; it is never dereferenced or retained beyond that query.
pub(crate) struct PostingBlockCache {
    entries: RefCell<Entries>,
    limit: usize,
    accepting: std::cell::Cell<bool>,
}

impl PostingBlockCache {
    pub(crate) fn new(working_bytes: usize) -> Self {
        Self {
            entries: RefCell::default(),
            limit: (working_bytes / 8).min(2 * 1024 * 1024),
            accepting: std::cell::Cell::new(true),
        }
    }

    fn key(reader: &LexicalProjectionReader, block: &BlockDescriptor) -> Key {
        (
            std::ptr::from_ref(reader) as usize,
            block.offset,
            block.length,
            block.checksum,
        )
    }

    pub(super) fn get(
        &self,
        reader: &LexicalProjectionReader,
        block: &BlockDescriptor,
    ) -> Option<Arc<Dictionary>> {
        self.entries
            .borrow()
            .values
            .get(&Self::key(reader, block))
            .map(|entry| Arc::clone(&entry.dictionary))
    }

    pub(super) fn retain(
        &self,
        reader: &LexicalProjectionReader,
        block: &BlockDescriptor,
        dictionary: Dictionary,
        context: Option<QueryContext<'_>>,
    ) -> DictionaryStorage {
        if !self.accepting.get() {
            return DictionaryStorage::Owned(dictionary);
        }
        let Some(bytes) = dictionary
            .as_fst()
            .as_inner()
            .bytes
            .capacity()
            .checked_add(MAP_ENTRY_BYTES)
        else {
            return DictionaryStorage::Owned(dictionary);
        };
        let mut entries = self.entries.borrow_mut();
        let key = Self::key(reader, block);
        let limit = self
            .limit
            .min(usize::try_from(reader.config.query_memory_bytes.get() / 8).unwrap_or(usize::MAX));
        if entries.values.contains_key(&key) || bytes > limit.saturating_sub(entries.bytes) {
            return DictionaryStorage::Owned(dictionary);
        }
        // Reuse is optional: a cache must never turn a successful bounded query
        // into a refusal. The stream's existing workspace funds borrowed blocks.
        let memory = match context.map(|query| query.memory.reserve(bytes)).transpose() {
            Ok(memory) => memory,
            Err(_) => return DictionaryStorage::Owned(dictionary),
        };
        let dictionary = Arc::new(dictionary);
        entries.values.insert(
            key,
            Entry {
                dictionary: Arc::clone(&dictionary),
                bytes,
                _memory: memory,
            },
        );
        entries.bytes += bytes;
        DictionaryStorage::Shared(dictionary)
    }

    pub(crate) fn release_reader(&self, reader: &LexicalProjectionReader) {
        let address = std::ptr::from_ref(reader) as usize;
        let mut entries = self.entries.borrow_mut();
        entries.values.retain(|key, _| key.0 != address);
        entries.bytes = entries.values.values().map(|entry| entry.bytes).sum();
    }

    pub(crate) fn clear(&self) {
        self.accepting.set(false);
        self.entries.replace(Entries::default());
    }

    pub(super) fn prepare_scoring(&self, workspace: u64, budget: u64) {
        self.accepting.set(false);
        if workspace.saturating_add(self.entries.borrow().bytes as u64) > budget {
            self.clear();
        }
    }

    pub(crate) fn reclaim_on_refusal<T>(
        cache: Option<&Self>,
        mut operation: impl FnMut() -> Result<T>,
    ) -> Result<T> {
        match operation() {
            Err(HawDBError::ResourceBudgetExceeded(error))
                if error.resource == hawdb_core::ResourceBudgetKind::MemoryBytes
                    && cache.is_some_and(|cache| cache.entries.borrow().bytes != 0) =>
            {
                cache.expect("nonempty cache").clear();
                operation()
            }
            result => result,
        }
    }
}
