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

//! Private complete artifact construction; source/GC authority remains separate.
use super::*;
use crate::build_memory::{
    checked_add as add, checked_mul as mul,
    path::OwnedPath,
    read::{ReadAdmission, ReadBytes, ReaderMemory},
    SET_ENTRY_BYTES,
};
use hawdb_storage::file_descriptors::ProjectFileDescriptors;
use std::mem::size_of;

pub(super) struct OpenContext<'a, 'execution> {
    pub(super) root: &'a Path,
    // Incoming host-owned buffers are moved, not cloned. Their actual capacity
    // is admitted before they can become retained reader ownership.
    pub(super) config: SearchOutOfCoreConfig,
    pub(super) analyzer_lexicon: SearchAnalyzerLexicon,
    pub(super) lexical_term_policy: SearchLexicalTermPolicy,
    pub(super) lexical_source_policy: SearchLexicalSourcePolicy,
    pub(super) admission: Option<&'a ReadAdmission<'execution>>,
    // Admission must borrow an existing project, never create an unaccounted
    // process-global descriptor/cache registry inside this constructor.
    pub(super) project: Option<&'a ProjectFileDescriptors>,
    pub(super) max_artifact_read_bytes: u64,
}

// Deliberately no Deref or public serving/refresh conversion for admitted roots.
// Until GC/cache ownership and fresh serving contexts are complete, internal
// construction evidence must not become an ungoverned public reader capability.
pub(super) struct OpenedRoot {
    reader: SearchOutOfCoreReader,
    _root_memory: Option<ReaderMemory>,
    _manifest_memory: Option<ReaderMemory>,
    _collections_memory: Option<ReaderMemory>,
}
impl OpenedRoot {
    pub(super) fn into_ordinary(self) -> Result<SearchOutOfCoreReader> {
        if self._root_memory.is_some() {
            return Err(HawDBError::Execution(
                "admitted root has no public serving/GC capability".into(),
            ));
        }
        Ok(self.reader)
    }
}

pub(super) fn open(context: OpenContext<'_, '_>) -> Result<OpenedRoot> {
    let OpenContext {
        root,
        config,
        analyzer_lexicon,
        lexical_term_policy,
        lexical_source_policy,
        admission,
        project,
        max_artifact_read_bytes,
    } = context;
    let root_memory = admission
        .map(|admission| {
            admission.retained(root_bytes(
                root,
                &config,
                &analyzer_lexicon,
                admission.task,
            )?)
        })
        .transpose()?;
    let root = root.to_path_buf();
    let project_files = match admission {
        Some(admission) => {
            let project = project.ok_or_else(|| {
                HawDBError::Execution("admitted root requires an existing project authority".into())
            })?;
            let _io = admission
                .io()
                .map_err(|error| HawDBError::Execution(error.to_string()))?;
            let canonical = OwnedPath::canonicalize(&root, admission.memory, admission.task)?;
            if !canonical.starts_with(project.root()) {
                return Err(HawDBError::StorageIntegrity(
                    "reader root is outside its supplied project".into(),
                ));
            }
            // Containment is not a complete incarnation/binding proof.
            project.clone()
        }
        None => ProjectFileDescriptors::acquire_component(&root, false)?,
    };
    let bytes = match admission {
        Some(admission) => {
            let path = OwnedPath::join(
                &root,
                Path::new(OUT_OF_CORE_MANIFEST_FILE),
                admission.memory,
                admission.task,
            )?;
            admission.read_unbound(
                &path,
                MAX_OUT_OF_CORE_MANIFEST_BYTES.min(max_artifact_read_bytes),
            )?
        }
        None => ReadBytes::unaccounted(read_bounded_file(
            &root.join(OUT_OF_CORE_MANIFEST_FILE),
            MAX_OUT_OF_CORE_MANIFEST_BYTES,
        )?),
    };
    let manifest_memory = admission
        .map(|admission| {
            admission.retained(SearchOutOfCoreManifestBody::decode_capacity(
                &bytes,
                admission.task,
            )?)
        })
        .transpose()?;
    let manifest = SearchOutOfCoreManifestBody::decode_with_context(
        &bytes,
        admission.map(|admission| admission.task),
    )?;
    drop(bytes);
    let closure = load_artifact_closure_with_context(
        ClosureLoadContext {
            root: &root,
            config: &config,
            analyzer_lexicon: &analyzer_lexicon,
            lexical_term_policy,
            lexical_source_policy,
            previous: None,
            admission,
            max_artifact_read_bytes,
        },
        manifest,
    )?;
    Ok(OpenedRoot {
        reader: SearchOutOfCoreReader {
            root,
            _project_files: project_files,
            config,
            analyzer_lexicon,
            manifest: closure.manifest,
            segments: closure.segments,
            visibility: closure.visibility,
            lexical_source_policy,
            lexical_term_policy,
            runtime_capabilities: crate::compiled_runtime_capabilities(),
        },
        _root_memory: root_memory,
        _manifest_memory: manifest_memory,
        _collections_memory: closure.collections_memory,
    })
}

fn root_bytes(
    root: &Path,
    config: &SearchOutOfCoreConfig,
    analyzer: &SearchAnalyzerLexicon,
    task: &hawdb_core::RuntimeTaskContext,
) -> Result<usize> {
    crate::build_control::checkpoint(task)?;
    let mut bytes = add(
        size_of::<OpenedRoot>(),
        add(
            root.as_os_str().as_encoded_bytes().len(),
            mul(config.spill_directory.capacity(), 3)?,
        )?,
    )?;
    bytes = add(
        bytes,
        mul(
            analyzer.alias_rules.capacity(),
            size_of::<crate::SearchAnalyzerAliasRule>(),
        )?,
    )?;
    for rule in &analyzer.alias_rules {
        crate::build_control::checkpoint(task)?;
        for words in [&rule.inputs, &rule.aliases] {
            bytes = add(bytes, mul(words.capacity(), size_of::<String>())?)?;
            for word in words {
                crate::build_control::checkpoint(task)?;
                bytes = add(bytes, word.capacity())?;
            }
        }
    }
    bytes = add(bytes, mul(analyzer.stopwords.len(), SET_ENTRY_BYTES)?)?;
    for word in &analyzer.stopwords {
        crate::build_control::checkpoint(task)?;
        bytes = add(bytes, word.capacity())?;
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests;
