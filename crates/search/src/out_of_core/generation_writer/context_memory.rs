//! Operation-owned build options and writer startup paths.

use super::SearchOutOfCoreGenerationBuildOptions;
use crate::build_control::checkpoint;
pub(super) use crate::build_memory::path::OwnedPath;
use crate::build_memory::{checked_add as add, checked_mul as mul, BuildMemory, SET_ENTRY_BYTES};
use crate::Result;
use skein_core::RuntimeTaskContext;
use skein_executor::QueryMemoryLease;
use std::mem::size_of;
use std::ops::Deref;

#[derive(Debug)]
pub(super) struct Options {
    value: SearchOutOfCoreGenerationBuildOptions,
    // All owned payload drops before its originating reservation.
    _memory: QueryMemoryLease,
}

impl Options {
    pub(super) fn new(
        value: SearchOutOfCoreGenerationBuildOptions,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        checkpoint(task)?;
        let lease = memory.retained.reserve(options_bytes(&value, task)?)?;
        Ok(Self {
            value,
            _memory: lease,
        })
    }
}

impl Deref for Options {
    type Target = SearchOutOfCoreGenerationBuildOptions;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

fn options_bytes(
    value: &SearchOutOfCoreGenerationBuildOptions,
    task: &RuntimeTaskContext,
) -> Result<usize> {
    let lexicon = &value.analyzer_lexicon;
    let mut bytes = mul(
        lexicon.alias_rules.capacity(),
        size_of::<crate::SearchAnalyzerAliasRule>(),
    )?;
    for rule in &lexicon.alias_rules {
        checkpoint(task)?;
        for words in [&rule.inputs, &rule.aliases] {
            bytes = add(bytes, mul(words.capacity(), size_of::<String>())?)?;
            for word in words {
                checkpoint(task)?;
                bytes = add(bytes, word.capacity())?;
            }
        }
    }
    // The caller may have emptied a tree without dropping its retained leaf.
    bytes = add(bytes, mul(lexicon.stopwords.len().max(1), SET_ENTRY_BYTES)?)?;
    for word in &lexicon.stopwords {
        checkpoint(task)?;
        bytes = add(bytes, word.capacity())?;
    }
    if let Some(identity) = &value.embedding_manifest {
        bytes = add(
            bytes,
            add(
                identity.model.capacity(),
                identity.version.as_ref().map_or(0, String::capacity),
            )?,
        )?;
    }
    Ok(bytes)
}
