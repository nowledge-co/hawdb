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
use crate::{Result, SearchLexicalSourcePolicy};
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
    pub(super) fn try_clone(
        &self,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        let lease = memory.retained.reserve(options_bytes(&self.value, task)?)?;
        Ok(Self {
            value: self.value.clone(),
            _memory: lease,
        })
    }

    pub(super) fn new(
        mut value: SearchOutOfCoreGenerationBuildOptions,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        checkpoint(task)?;
        Self::apply_backend_options(&mut value)?;
        let lease = memory.retained.reserve(options_bytes(&value, task)?)?;
        Ok(Self {
            value,
            _memory: lease,
        })
    }

    fn apply_backend_options(value: &mut SearchOutOfCoreGenerationBuildOptions) -> Result<()> {
        if value.vector_index_descriptor.is_none()
            && let Some(hawdb_core::VectorIndexBackend::RaBitQ {
                bit_width,
                segment_rows,
                transform_seed,
            }) = value.vector_backend
        {
            #[cfg(feature = "vector-search")]
            {
                value.rabitq_bit_width = match bit_width {
                    1 => hawdb_vector_projection::RaBitQBitWidth::One,
                    4 => hawdb_vector_projection::RaBitQBitWidth::Four,
                    _ => {
                        return Err(crate::HawDBError::Semantic(
                            "RaBitQ bit width must be 1 or 4".into(),
                        ))
                    }
                };
            }
            #[cfg(not(feature = "vector-search"))]
            let _ = bit_width;
            value.rabitq_segment_rows = std::num::NonZeroUsize::new(segment_rows as usize)
                .ok_or_else(|| {
                    crate::HawDBError::Semantic("RaBitQ segment rows must be positive".into())
                })?;
            value.rabitq_transform_seed = transform_seed;
        }
        Ok(())
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
        if !self.value.vector_scoring.is_default()
            && self.value.vector_scoring != reader.vector_scoring()
        {
            return Err(HawDBError::Semantic(
                "vector scoring differs from the active generation".into(),
            ));
        }
        self.value.vector_scoring = reader.vector_scoring();
        if self.value.analyzer_lexicon != *reader.analyzer_lexicon() {
            return Err(HawDBError::Storage(
                "search generation update analyzer does not match the active generation".into(),
            ));
        }
        #[cfg(feature = "vector-search")]
        if reader.manifest.vector_index_binding.is_none()
            && let Some(projection) = reader
                .segments
                .iter()
                .find_map(|artifact| artifact.rabitq_projection.as_ref())
        {
            // A delta inherits the pinned base's algorithm and topology settings.
            // Changing a backend requires a full replacement, never mixed layers.
            self.value.vector_backend = Some(projection.backend()?);
            Self::apply_backend_options(&mut self.value)?;
        }
        if reader.manifest.vector_index_binding.is_some()
            || self.value.vector_index_descriptor.is_some()
        {
            // Definition changes require a new full build. Generic delta/compaction
            // must retain the binding even when the caller supplies no override.
            self._memory
                .grow(hawdb_core::vector_index::MAX_VECTOR_INDEX_DESCRIPTOR_BYTES * 2)?;
            let expected_binding = reader
                .manifest
                .vector_index_binding
                .as_deref()
                .map(crate::vector_index::decode)
                .transpose()?;
            if self.value.vector_index_descriptor.is_some()
                && self.value.vector_index_descriptor != expected_binding
            {
                return Err(HawDBError::StorageIntegrity(
                    "vector index definition differs from the active generation".into(),
                ));
            }
            let previous_bytes = self
                .value
                .vector_index_descriptor
                .as_ref()
                .map_or(0, hawdb_core::VectorIndexDescriptor::retained_bytes);
            let retained_bytes = expected_binding
                .as_ref()
                .map_or(0, hawdb_core::VectorIndexDescriptor::retained_bytes);
            if let Some(binding) = &expected_binding
                && let hawdb_core::VectorIndexBackend::RaBitQ {
                    bit_width,
                    segment_rows,
                    transform_seed,
                } = binding.backend()
            {
                #[cfg(feature = "vector-search")]
                {
                    self.value.rabitq_bit_width = match bit_width {
                        1 => hawdb_vector_projection::RaBitQBitWidth::One,
                        4 => hawdb_vector_projection::RaBitQBitWidth::Four,
                        _ => unreachable!("validated vector index backend"),
                    };
                }
                #[cfg(not(feature = "vector-search"))]
                let _ = bit_width;
                self.value.rabitq_segment_rows = std::num::NonZeroUsize::new(segment_rows as usize)
                    .expect("validated vector index segment size");
                self.value.rabitq_transform_seed = transform_seed;
            }
            self.value.vector_index_descriptor = expected_binding;
            self._memory.shrink(
                hawdb_core::vector_index::MAX_VECTOR_INDEX_DESCRIPTOR_BYTES * 2 + previous_bytes
                    - retained_bytes,
            );
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

    pub(super) fn bind_replacement_epoch(&mut self, epoch: u64) -> Result<()> {
        if self
            .value
            .source_graph_commit_epoch
            .is_some_and(|previous| previous > epoch)
        {
            return Err(crate::HawDBError::Execution(
                "replacement source epoch regresses".into(),
            ));
        }
        self.value.source_graph_commit_epoch = Some(epoch);
        Ok(())
    }

    pub(super) fn set_lexical_source_policy(&mut self, policy: SearchLexicalSourcePolicy) {
        self.value.lexical_max_document_source_bytes = policy.max_document_source_bytes();
        self.value.lexical_max_document_tokens = policy.max_document_tokens();
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
    if let Some(descriptor) = &value.vector_index_descriptor {
        bytes = add(bytes, descriptor.retained_bytes())?;
    }
    Ok(bytes)
}
