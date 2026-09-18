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

//! Complete manifest validation under the writer's existing operation budget.

use super::io::GenerationIo;
use crate::build_control::{checkpoint, json};
use crate::build_memory::{checked_add as add, checked_mul as mul, directory, BuildMemory};
use crate::lexical_projection::admitted_manifest_generation;
use crate::out_of_core::{
    SearchOutOfCoreManifestEnvelope, MAX_OUT_OF_CORE_MANIFEST_BYTES, OUT_OF_CORE_MANIFEST_FILE,
};
use crate::{HawDBError, Result};
use hawdb_core::RuntimeTaskContext;
use std::fs;
use std::path::Path;

pub(super) fn active(
    root: &Path,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
) -> Result<Option<u64>> {
    let io = GenerationIo::new(memory, task);
    let manifest = io.path(root, Path::new(OUT_OF_CORE_MANIFEST_FILE))?;
    // Preserve the existing absent-path behavior; every materialized file is
    // still read through the same opened handle that supplied its length.
    if !io.native(&[&manifest], || manifest.exists())? {
        return Ok(None);
    }
    let bytes = io.read(&manifest, MAX_OUT_OF_CORE_MANIFEST_BYTES)?;
    let capacity = add(json::decode_capacity(&bytes.bytes, 0, 0, task)?, 3 * 128)?;
    let _decode = memory.spool.reserve(capacity)?;
    let envelope: SearchOutOfCoreManifestEnvelope =
        serde_json::from_slice(&bytes.bytes).map_err(|error| {
            HawDBError::Storage(format!("invalid search out-of-core manifest: {error}"))
        })?;
    checkpoint(task)?;
    if json::checksum_with_context(&envelope.body, Some(task))? != envelope.checksum {
        return Err(HawDBError::Storage(
            "search out-of-core manifest checksum mismatch".into(),
        ));
    }
    envelope.body.validate_names()?;
    checkpoint(task)?;
    Ok(Some(envelope.body.generation))
}

pub(super) fn next(
    root: &Path,
    max_lexical_manifest_bytes: u64,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
) -> Result<u64> {
    let active = match active(root, memory, task) {
        Ok(None) => return Ok(1),
        Ok(Some(generation)) => generation,
        Err(error @ HawDBError::Execution(_)) => return Err(error),
        Err(_) => {
            checkpoint(task)?;
            latest_lexical(root, max_lexical_manifest_bytes, memory, task)?
        }
    };
    active
        .checked_add(1)
        .ok_or_else(|| HawDBError::Storage("search out-of-core generation overflow".into()))
}

fn latest_lexical(
    root: &Path,
    max_bytes: u64,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
) -> Result<u64> {
    let io = GenerationIo::new(memory, task);
    let _directory = memory
        .spool
        .reserve(directory::retained_scan_bytes(root)?)?;
    let startup = memory.spool.reserve(directory::scan_startup_bytes(root)?)?;
    let mut entries = io.native(&[root], || fs::read_dir(root))??;
    drop(startup);
    let mut latest = 0;
    loop {
        checkpoint(task)?;
        // Native directory records have a u16 byte extent on the supported Unix
        // targets; Windows' fixed WCHAR name is smaller. Keep both the entry's
        // owned name and file_name()'s copy admitted until the iteration ends.
        let _entry = memory.spool.reserve(mul(2, directory::ENTRY_NAME_BYTES)?)?;
        let Some(entry) = entries.next() else {
            break;
        };
        let entry = entry?;
        let name = entry.file_name();
        if name.as_encoded_bytes().len() >= directory::ENTRY_NAME_BYTES {
            return Err(HawDBError::Execution(
                "directory entry exceeds native admission".into(),
            ));
        }
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(generation) = name
            .strip_prefix("search_lexical.manifest.")
            .and_then(|value| value.strip_suffix(".hawdb"))
            .and_then(|value| value.parse::<u64>().ok())
        else {
            continue;
        };
        let path = io.path(root, Path::new(name))?;
        let bytes = io.read(&path, max_bytes)?;
        if admitted_manifest_generation(&bytes.bytes, memory, task)? == Some(generation) {
            latest = latest.max(generation);
        }
    }
    Ok(latest)
}

#[cfg(test)]
mod tests;
