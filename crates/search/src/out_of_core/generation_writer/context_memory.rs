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

//! Operation-owned build options and writer startup paths.

use super::SearchOutOfCoreGenerationBuildOptions;
use crate::build_control::checkpoint;
pub(super) use crate::build_memory::path::OwnedPath;
use crate::build_memory::{checked_add as add, checked_mul as mul, BuildMemory, SET_ENTRY_BYTES};
use crate::Result;
use hawdb_core::RuntimeTaskContext;
use hawdb_executor::QueryMemoryLease;
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

    pub(super) fn bind_delta_identity(
        &mut self,
        reader: &crate::SearchOutOfCoreReader,
        source_graph_commit_epoch: Option<u64>,
    ) -> Result<()> {
        use crate::HawDBError;
        if self.value.source_graph_commit_epoch.is_some()
            && self.value.source_graph_commit_epoch != source_graph_commit_epoch
        {
            return Err(HawDBError::Storage(
                "search generation update source graph epoch does not match the delta".into(),
            ));
        }
        if self.value.import_source_graph_commit_epoch.is_some()
            && self.value.import_source_graph_commit_epoch
                != reader.import_source_graph_commit_epoch()
        {
            return Err(HawDBError::Storage(
                "search generation update import provenance does not match the active generation"
                    .into(),
            ));
        }
        let expected = reader
            .manifest
            .embedding_model
            .as_deref()
            .zip(reader.manifest.embedding_dimension)
            .map(|(model, dimension)| {
                (
                    model,
                    reader.manifest.embedding_version.as_deref(),
                    dimension,
                )
            });
        let requested = self.value.embedding_manifest.as_ref().map(|identity| {
            (
                identity.model.as_str(),
                identity.version.as_deref(),
                identity.dimension,
            )
        });
        if requested.is_some() && requested != expected {
            return Err(HawDBError::Storage(
                "search generation update embedding identity does not match the active generation"
                    .into(),
            ));
        }
        if self.value.analyzer_lexicon != *reader.analyzer_lexicon() {
            return Err(HawDBError::Storage(
                "search generation update analyzer does not match the active generation".into(),
            ));
        }
        let old = self
            .value
            .embedding_manifest
            .as_ref()
            .map_or(0, |identity| {
                identity.model.capacity() + identity.version.as_ref().map_or(0, String::capacity)
            });
        let new = expected.map_or(0, |(model, version, _)| {
            model.len() + version.map_or(0, str::len)
        });
        self._memory.grow(new)?;
        self.value.embedding_manifest = reader.embedding_manifest();
        self._memory.shrink(old);
        self.value.source_graph_commit_epoch = source_graph_commit_epoch;
        self.value.import_source_graph_commit_epoch = reader.import_source_graph_commit_epoch();
        Ok(())
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
