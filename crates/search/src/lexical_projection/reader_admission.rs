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

//! Owned lexical metadata admitted before decode; artifact verification streams.

use super::{
    build_manifest, BlockDescriptor, LexicalProjectionConfig, LexicalProjectionReader,
    ManifestBody, ARTIFACT_HEADER,
};
use crate::build_control::{checkpoint, json};
use crate::build_memory::{
    checked_add as add, checked_mul as mul, path::OwnedPath, read::ReadAdmission,
    reserved::native_path,
};
use crate::{HawDBError, Result};
use std::path::Path;
use std::sync::Arc;

#[derive(Clone, Copy)]
pub(crate) struct Identity {
    pub(crate) source_epoch: Option<u64>,
    pub(crate) analyzer_digest: u64,
    pub(crate) documents_digest: u64,
}

pub(crate) struct OpenContext<'a, 'execution> {
    pub(crate) root: &'a Path,
    pub(crate) identity: Identity,
    pub(crate) config: LexicalProjectionConfig,
    pub(crate) admission: &'a ReadAdmission<'execution>,
    // Explicit per-artifact work boundary. A complete root constructor must
    // additionally account for cumulative physical work across its closure.
    pub(crate) max_artifact_read_bytes: u64,
}

impl LexicalProjectionReader {
    /// Return the owned payload, so its caller can put it into the common
    /// Shared owner. A plain Arc<Self> would refund an in-payload guard before
    /// destroying the Arc control block covered by that same guard.
    pub(crate) fn load_admitted_manifest_bytes(
        context: OpenContext<'_, '_>,
        bytes: &[u8],
    ) -> Result<Option<Self>> {
        let OpenContext {
            root,
            identity,
            config,
            admission,
            max_artifact_read_bytes,
        } = context;
        checkpoint(admission.task)?;
        if bytes.len() as u64 > config.max_manifest_bytes.get() {
            return Err(HawDBError::Storage(
                "lexical projection manifest exceeds its read budget".into(),
            ));
        }
        // This bounds arbitrary received bytes, not a trusted re-encoding of
        // a previously decoded body. Compact record arrays count as containers.
        let decode = json::decode_capacity(
            bytes,
            std::mem::size_of::<BlockDescriptor>(),
            2,
            admission.task,
        )?;
        let native = add(mul(native_path::bytes(root)?, 3)?, 4096)?;
        let retained =
            admission.retained(add(add(decode, build_manifest::reader_bytes())?, native)?)?;
        let decoded = ManifestBody::decode_with_context(bytes, Some(admission.task));
        checkpoint(admission.task)?;
        let manifest = decoded?;
        if manifest.source_graph_commit_epoch != identity.source_epoch
            || manifest.analyzer_digest != identity.analyzer_digest
            || manifest.documents_digest != identity.documents_digest
        {
            return Ok(None);
        }
        let required_term_bytes = manifest.required_term_bytes(Some(admission.task))?;
        super::admit_term_bytes(required_term_bytes, config.max_term_bytes)?;
        for block in &manifest.blocks {
            checkpoint(admission.task)?;
            if block.length > config.max_block_bytes.get() {
                return Err(HawDBError::Storage(
                    "lexical projection contains a block above the read admission limit".into(),
                ));
            }
        }
        let path = OwnedPath::join(
            root,
            Path::new(&manifest.artifact_file),
            admission.memory,
            admission.task,
        )?;
        let mut header = [0u8; 24];
        let mut prefix = 0;
        let file = admission.visit_bound(
            &path,
            manifest.artifact_len,
            manifest.artifact_checksum,
            max_artifact_read_bytes,
            |chunk| {
                let count = (header.len() - prefix).min(chunk.len());
                header[prefix..prefix + count].copy_from_slice(&chunk[..count]);
                prefix += count;
                Ok(())
            },
        )?;
        if prefix != header.len()
            || &header[..16] != ARTIFACT_HEADER
            || u64::from_le_bytes(header[16..24].try_into().expect("fixed header"))
                != manifest.generation
        {
            return Err(HawDBError::Storage(
                "lexical projection artifact header mismatch".into(),
            ));
        }
        Ok(Some(Self {
            manifest,
            file: Arc::new(file),
            config,
            required_term_bytes,
            _build_memory: None,
            _reader_memory: Some(retained),
        }))
    }
}

#[cfg(test)]
mod tests;
