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

//! Borrowed construction admission for complete immutable mutation runs.
use super::*;
use crate::build_memory::{
    checked_add as plus, checked_mul as times, path::OwnedPath, read::ReadAdmission,
};
use crate::lexical_projection::QueryContext;

pub(super) struct OpenContext<'a, 'execution> {
    pub(super) root: &'a Path,
    pub(super) admission: &'a ReadAdmission<'execution>,
    pub(super) maximum_encoded_bytes: u64,
    pub(super) maximum_artifact_bytes: u64,
    pub(super) analyzer_digest: u64,
    pub(super) budget: &'a mut MutationRunBudget,
}

impl SearchMutationRun {
    pub(super) fn open_admitted(
        context: OpenContext<'_, '_>,
        manifest: &SearchOutOfCoreMutationRunManifest,
    ) -> Result<Self> {
        let OpenContext {
            root,
            admission,
            maximum_encoded_bytes,
            maximum_artifact_bytes,
            analyzer_digest,
            budget,
        } = context;
        crate::build_control::checkpoint(admission.task)?;
        if manifest.len > maximum_encoded_bytes {
            return Err(HawDBError::Storage(
                "search mutation-run artifact exceeds the configured read budget".into(),
            ));
        }
        let path = OwnedPath::join(
            root,
            Path::new(&manifest.file),
            admission.memory,
            admission.task,
        )?;
        let (file, scalar) =
            streaming::preflight_admitted(&path, manifest, admission, maximum_artifact_bytes)?;
        let working = streaming::working_bytes(manifest.entry_count, scalar)?;
        budget
            .check(working)
            .map_err(|error| HawDBError::Execution(error.to_string()))?;
        let working = usize::try_from(working).map_err(|_| size_overflow())?;
        // Exact entry Vec, one owned document ID per record, body format, and
        // the run's inline storage. Terms keep shared file ranges, not strings.
        let retained = plus(
            plus(
                std::mem::size_of::<Self>(),
                times(
                    manifest.entry_count,
                    plus(std::mem::size_of::<SearchMutationRunEntry>(), scalar)?,
                )?,
            )?,
            scalar,
        )?;
        let memory = admission.retained(retained)?;
        let _scratch = admission.scratch(working)?;
        let file = terms::TermFile::new_admitted(file, manifest.len, admission)?;
        let envelope =
            streaming::decode_admitted(file, manifest, scalar, admission, maximum_artifact_bytes)?;
        let query = QueryContext::from_read_admission(admission);
        if read_encoding::checksum(&envelope.body, query)? != envelope.checksum {
            return Err(HawDBError::Storage(
                "search mutation-run envelope checksum mismatch".into(),
            ));
        }
        envelope.body.validate_with_query_context(Some(query))?;
        if envelope.body.generation != manifest.generation
            || envelope.body.analyzer_digest != manifest.analyzer_digest
            || envelope.body.entries.len() != manifest.entry_count
        {
            return Err(HawDBError::Storage(
                "search mutation-run metadata does not match its manifest reference".into(),
            ));
        }
        if envelope.body.analyzer_digest != analyzer_digest {
            return Err(HawDBError::Storage(
                "search mutation-run analyzer identity does not match the active reader".into(),
            ));
        }
        let logical = add(
            envelope.body.retained_capacity()?,
            (std::mem::size_of::<terms::TermFile>() + 2 * std::mem::size_of::<usize>()) as u64,
        )?;
        budget
            .retain(add(
                logical,
                multiply(
                    manifest.entry_count as u64,
                    crate::build_memory::SET_ENTRY_BYTES,
                )?,
            )?)
            .map_err(|error| HawDBError::Execution(error.to_string()))?;
        Ok(Self {
            body: envelope.body,
            open_working_bytes: working as u64,
            _reader_memory: Some(memory),
        })
    }
}

#[cfg(test)]
mod tests;
