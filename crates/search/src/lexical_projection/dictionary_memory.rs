//! Owners that carry dictionary key and encoded-buffer admission across calls.

use super::dictionary::{self, Limits, Metadata};
use crate::build_control::checkpoint;
use crate::build_memory::BuildMemory;
use crate::error::{Result, SkeinError};
use skein_core::RuntimeTaskContext;
use skein_executor::QueryMemoryLease;

pub(super) struct Term {
    text: String,
    _memory: QueryMemoryLease,
}

impl Term {
    pub(super) fn new(text: &str, memory: &BuildMemory) -> Result<Self> {
        let lease = memory.retained.reserve(text.len())?;
        #[cfg(test)]
        evidence::key();
        Ok(Self {
            text: text.to_owned(),
            _memory: lease,
        })
    }

    #[cfg(test)]
    pub(super) fn from_owned(text: String, memory: &BuildMemory) -> Result<Self> {
        let lease = memory.retained.reserve(text.capacity())?;
        Ok(Self {
            text,
            _memory: lease,
        })
    }

    pub(super) fn as_str(&self) -> &str {
        &self.text
    }

    pub(super) fn capacity(&self) -> usize {
        self.text.capacity()
    }
}

impl AsRef<str> for Term {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

pub(super) struct Encoded {
    pub(super) bytes: Vec<u8>,
    pub(super) memory: QueryMemoryLease,
}

impl Encoded {
    pub(super) fn validate(
        &self,
        limits: Limits,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<std::result::Result<(), &'static str>> {
        checkpoint(task)?;
        let validation = match dictionary::validation_reservation(&self.bytes, limits) {
            Ok(bytes) => bytes,
            Err(error) => return Ok(Err(error)),
        };
        let _validation_memory = memory.retained.reserve(validation)?;
        #[cfg(test)]
        evidence::validation();
        let mut check = || task.checkpoint().map_err(|reason| reason.as_str());
        let validated = dictionary::Dictionary::open(&self.bytes, limits, &mut check).map(|_| ());
        checkpoint(task)?;
        Ok(validated)
    }
}

// An inner codec rejection can split a bounded partition. Admission and
// cancellation errors are terminal and must not trigger speculative retries.
pub(super) fn encode<K: AsRef<str>>(
    entries: &[(K, Metadata)],
    limits: Limits,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
) -> Result<std::result::Result<Encoded, &'static str>> {
    checkpoint(task)?;
    let reservation = match dictionary::builder_reservation(entries, limits) {
        Ok(reservation) => reservation,
        Err(error) => return Ok(Err(error)),
    };
    let lease = memory.retained.reserve(reservation)?;
    let mut check = || task.checkpoint().map_err(|reason| reason.as_str());
    #[cfg(test)]
    evidence::build();
    let bytes = dictionary::build(entries, limits, &mut check);
    checkpoint(task)?;
    let bytes = match bytes {
        Ok(bytes) => bytes,
        Err(error) => return Ok(Err(error)),
    };
    let mut encoded = Encoded {
        bytes,
        memory: lease,
    };
    let released = reservation
        .checked_sub(encoded.bytes.capacity())
        .ok_or_else(|| {
            SkeinError::Execution("dictionary output exceeded builder reservation".to_string())
        })?;
    // The dependency's builder and registry have dropped, but its output lives
    // through validation, directory admission and the final spill write.
    encoded.memory.shrink(released);
    let validated = encoded.validate(limits, memory, task)?;
    Ok(validated.map(|()| encoded))
}

#[cfg(test)]
pub(super) mod evidence {
    use std::cell::Cell;
    thread_local! {
        static KEYS: Cell<usize> = const { Cell::new(0) };
        static BUILDS: Cell<usize> = const { Cell::new(0) };
        static VALIDATIONS: Cell<usize> = const { Cell::new(0) };
    }
    pub(super) fn key() {
        KEYS.with(|count| count.set(count.get() + 1));
    }
    pub(crate) fn build() {
        BUILDS.with(|count| count.set(count.get() + 1));
    }
    pub(crate) fn validation() {
        VALIDATIONS.with(|count| count.set(count.get() + 1));
    }
    pub(crate) fn take() -> (usize, usize, usize) {
        (
            KEYS.with(|count| count.replace(0)),
            BUILDS.with(|count| count.replace(0)),
            VALIDATIONS.with(|count| count.replace(0)),
        )
    }
}
