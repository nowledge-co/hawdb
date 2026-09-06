//! Document-map staging whose requested capacity remains owned through encoding.

use super::{encode_block_header, write_string, BlockKind, LexicalProjectionConfig};
use crate::build_memory::{checked_add, checked_mul, BuildMemory};
use crate::error::{Result, SkeinError};
use skein_executor::QueryMemoryLease;
use std::mem::size_of;

pub(super) struct Documents {
    entries: Vec<(String, u32)>,
    pub(super) bytes: usize,
    memory: BuildMemory,
    slots: QueryMemoryLease,
    keys: QueryMemoryLease,
}

pub(super) struct EncodedDocuments {
    pub(super) payload: Vec<u8>,
    pub(super) min_key: String,
    pub(super) max_key: String,
    // Keep all encoded data alive before releasing this overlapping charge.
    pub(super) _memory: QueryMemoryLease,
}

impl Documents {
    pub(super) fn new(memory: BuildMemory) -> Result<Self> {
        Ok(Self {
            entries: Vec::new(),
            bytes: 0,
            slots: memory.retained.reserve(0)?,
            keys: memory.retained.reserve(0)?,
            memory,
        })
    }

    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(super) fn record_bytes(id: &str, config: LexicalProjectionConfig) -> Result<usize> {
        let bytes = checked_add(id.len(), 8)?;
        if u32::try_from(id.len()).is_err()
            || checked_add(bytes, 29)? as u64 > config.max_block_bytes.get()
        {
            return Err(SkeinError::Storage(format!(
                "lexical build produced a {} byte block, exceeding {}",
                checked_add(bytes, 29)?,
                config.max_block_bytes
            )));
        }
        Ok(bytes)
    }

    pub(super) fn push(&mut self, id: &str, length: u32) -> Result<()> {
        let next_bytes = checked_add(self.bytes, checked_add(id.len(), 8)?)?;
        if self.entries.len() == self.entries.capacity() {
            let capacity = checked_mul(self.entries.capacity().max(2), 2)?;
            let next_slots = self
                .memory
                .retained
                .reserve(checked_mul(capacity, size_of::<(String, u32)>())?)?;
            self.entries
                .try_reserve_exact(capacity - self.entries.len())
                .map_err(|error| SkeinError::Execution(error.to_string()))?;
            self.slots = next_slots;
        }
        self.keys.grow(id.len())?;
        #[cfg(test)]
        evidence::key();
        self.entries.push((id.to_string(), length));
        self.bytes = next_bytes;
        Ok(())
    }

    pub(super) fn encode(
        &self,
        generation: u64,
        block_id: u64,
        config: LexicalProjectionConfig,
        task: &skein_core::RuntimeTaskContext,
    ) -> Result<EncodedDocuments> {
        crate::build_control::checkpoint(task)?;
        let bytes = checked_add(self.bytes, 29)?;
        if bytes as u64 > config.max_block_bytes.get() {
            return Err(SkeinError::Storage(format!(
                "lexical build produced a {bytes} byte block, exceeding {}",
                config.max_block_bytes
            )));
        }
        let first = &self.entries.first().expect("nonempty document block").0;
        let last = &self.entries.last().expect("nonempty document block").0;
        let reservation = self
            .memory
            .retained
            .reserve(checked_add(bytes, checked_add(first.len(), last.len())?)?)?;
        #[cfg(test)]
        evidence::encode();
        let mut payload = Vec::new();
        payload
            .try_reserve_exact(bytes)
            .map_err(|error| SkeinError::Execution(error.to_string()))?;
        encode_block_header(
            &mut payload,
            generation,
            block_id,
            BlockKind::Documents,
            self.entries.len(),
        )?;
        for (id, length) in &self.entries {
            crate::build_control::checkpoint(task)?;
            write_string(&mut payload, id)?;
            payload.extend_from_slice(&length.to_le_bytes());
        }
        Ok(EncodedDocuments {
            payload,
            min_key: first.clone(),
            max_key: last.clone(),
            _memory: reservation,
        })
    }

    pub(super) fn clear(&mut self) {
        self.entries.clear();
        self.keys.reset();
        self.bytes = 0;
    }

    pub(super) fn release(&mut self) {
        self.clear();
        self.entries = Vec::new();
        self.slots.reset();
    }

    #[cfg(test)]
    pub(super) fn retained_bytes(&self) -> usize {
        self.slots.bytes() + self.keys.bytes()
    }
}

#[cfg(test)]
pub(super) mod evidence {
    use std::cell::Cell;
    thread_local! { static COUNTS: Cell<(usize, usize)> = const { Cell::new((0, 0)) }; }
    pub(super) fn key() {
        COUNTS.with(|v| {
            let (a, b) = v.get();
            v.set((a + 1, b));
        });
    }
    pub(super) fn encode() {
        COUNTS.with(|v| {
            let (a, b) = v.get();
            v.set((a, b + 1));
        });
    }
    pub(in super::super) fn take() -> (usize, usize) {
        COUNTS.with(|v| v.replace((0, 0)))
    }
}
