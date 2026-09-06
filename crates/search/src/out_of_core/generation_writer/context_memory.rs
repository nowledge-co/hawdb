//! Operation-owned build options and writer startup paths.

use super::SearchOutOfCoreGenerationBuildOptions;
use crate::build_control::checkpoint;
use crate::build_memory::{checked_add as add, checked_mul as mul, BuildMemory, SET_ENTRY_BYTES};
use crate::{Result, SearchEmbeddingManifest, SearchOutOfCoreReader, SkeinError};
use skein_core::RuntimeTaskContext;
use skein_executor::QueryMemoryLease;
use std::mem::size_of;
use std::ops::Deref;
use std::path::{Component, Path, PathBuf};

#[derive(Debug)]
pub(super) struct Options {
    value: SearchOutOfCoreGenerationBuildOptions,
    // All owned payload drops before its originating reservation.
    _memory: QueryMemoryLease,
    inherited_memory: Option<QueryMemoryLease>,
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
            inherited_memory: None,
        })
    }

    pub(super) fn bind_identity(
        &mut self,
        reader: &SearchOutOfCoreReader,
        source_epoch: Option<u64>,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<()> {
        checkpoint(task)?;
        if self.value.source_graph_commit_epoch.is_some()
            && self.value.source_graph_commit_epoch != source_epoch
        {
            return Err(SkeinError::Storage(
                "search generation update source graph epoch does not match the delta".into(),
            ));
        }
        if self.value.import_source_graph_commit_epoch.is_some()
            && self.value.import_source_graph_commit_epoch
                != reader.import_source_graph_commit_epoch()
        {
            return Err(SkeinError::Storage(
                "search generation update import provenance does not match the active generation"
                    .into(),
            ));
        }
        // Borrow the same optional identity represented by embedding_manifest(),
        // without materializing its strings merely to compare them.
        let expected = reader
            .manifest
            .embedding_model
            .as_deref()
            .zip(reader.manifest.embedding_dimension);
        if let Some(identity) = self.value.embedding_manifest.as_ref()
            && !expected.is_some_and(|(model, dimension)| {
                identity.model == model
                    && identity.dimension == dimension
                    && identity.version.as_deref() == reader.manifest.embedding_version.as_deref()
            })
        {
            return Err(SkeinError::Storage(
                "search generation update embedding identity does not match the active generation"
                    .into(),
            ));
        }
        if self.value.analyzer_lexicon != *reader.analyzer_lexicon() {
            return Err(SkeinError::Storage(
                "search generation update analyzer does not match the active generation".into(),
            ));
        }
        let inherited = if self.value.embedding_manifest.is_none()
            && let Some((model, dimension)) = expected
        {
            let version = reader.manifest.embedding_version.as_deref();
            let lease = memory
                .retained
                .reserve(add(model.len(), version.map_or(0, str::len))?)?;
            checkpoint(task)?;
            #[cfg(test)]
            tests::record_identity();
            let identity = SearchEmbeddingManifest {
                model: model.to_owned(),
                version: version.map(str::to_owned),
                dimension,
            };
            Some((identity, lease))
        } else {
            None
        };
        checkpoint(task)?;
        if let Some((identity, lease)) = inherited {
            self.value.embedding_manifest = Some(identity);
            self.inherited_memory = Some(lease);
        }
        self.value.source_graph_commit_epoch = source_epoch;
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

#[derive(Debug)]
pub(super) struct OwnedPath {
    value: PathBuf,
    _memory: QueryMemoryLease,
}

impl OwnedPath {
    pub(super) fn copy(
        path: &Path,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        checkpoint(task)?;
        let bytes = path.as_os_str().as_encoded_bytes().len();
        let lease = memory.retained.reserve(bytes)?;
        #[cfg(test)]
        tests::record_path();
        Self::finish(path.to_path_buf(), lease, task)
    }

    pub(super) fn join(
        parent: &Path,
        name: &Path,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        checkpoint(task)?;
        let verbatim = matches!(parent.components().next(), Some(Component::Prefix(prefix)) if prefix.kind().is_verbatim());
        let bytes = join_bytes(
            parent.as_os_str().as_encoded_bytes().len(),
            name.as_os_str().as_encoded_bytes().len(),
            verbatim,
        )?;
        let lease = memory.retained.reserve(bytes)?;
        #[cfg(test)]
        tests::record_path();
        Self::finish(parent.join(name), lease, task)
    }

    pub(super) fn with_extension(
        path: &Path,
        extension: &str,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        checkpoint(task)?;
        // Rust 1.97.1 Path::_with_extension reserves the full result up front.
        // Retaining the old extension in this bound also covers extensionless
        // and non-file paths without replacing native path semantics.
        let bytes = add(
            path.as_os_str().as_encoded_bytes().len(),
            add(extension.len(), 1)?,
        )?;
        let lease = memory.retained.reserve(bytes)?;
        #[cfg(test)]
        tests::record_path();
        Self::finish(path.with_extension(extension), lease, task)
    }

    fn finish(value: PathBuf, lease: QueryMemoryLease, task: &RuntimeTaskContext) -> Result<Self> {
        let mut owned = Self {
            value,
            _memory: lease,
        };
        checkpoint(task)?;
        if owned.value.capacity() > owned._memory.bytes() {
            return Err(SkeinError::Execution(
                "search writer path exceeds preflight capacity".into(),
            ));
        }
        owned
            ._memory
            .shrink(owned._memory.bytes() - owned.value.capacity());
        Ok(owned)
    }
}

impl Deref for OwnedPath {
    type Target = Path;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl AsRef<Path> for OwnedPath {
    fn as_ref(&self) -> &Path {
        &self.value
    }
}

fn join_bytes(parent: usize, name: usize, verbatim: bool) -> Result<usize> {
    let length = add(parent, add(name, 1)?)?;
    // Rust 1.97.1 PathBuf::_push uses amortized OsString growth. Verbatim
    // prefixes additionally collect components and reconstruct another buffer.
    // Preserve the standard library's platform-specific normalization semantics.
    if verbatim {
        add(
            mul(length.max(8), 4)?,
            mul(mul(length.max(4), 4)?, size_of::<Component<'_>>())?,
        )
    } else {
        mul(length.max(8), 3)
    }
}

#[cfg(test)]
mod tests;
