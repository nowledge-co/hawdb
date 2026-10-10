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

//! Bounded work hooks for the shared row-root manifest grammar.
//! Encoded buffers, allocator/capacity/drop and shared hard resources remain open.

use super::super::root::checkpoint::{hash, work_error};
use super::*;
use crate::background::CheckpointWorkContext;

pub(super) fn read_encoded(
    path: &Path,
    config: RelationalRowPagePublicationConfig,
    work: &CheckpointWorkContext,
) -> Result<crate::background::CheckpointBytes, RelationalRowPagePublicationError> {
    use super::super::publisher::checkpoint::io;
    let max_bytes = config.max_manifest_bytes.get();
    let max_bytes_u64 = u64::try_from(max_bytes).map_err(|_| {
        RelationalRowPagePublicationError::Admission("row-page manifest limit overflows u64".into())
    })?;
    let read_limit = max_bytes_u64.checked_add(1).ok_or_else(|| {
        RelationalRowPagePublicationError::Admission(
            "row-page manifest read limit overflows u64".into(),
        )
    })?;
    let file = io(Some(work), || {
        File::open(path).map_err(durability("open row-page manifest"))
    })?;
    let encoded_len = io(Some(work), || {
        file.metadata()
            .map(|metadata| metadata.len())
            .map_err(durability("read row-page manifest metadata"))
    })?;
    if encoded_len > max_bytes_u64 {
        return Err(RelationalRowPagePublicationError::Admission(format!(
            "row-page manifest contains {encoded_len} bytes, exceeding limit {}",
            config.max_manifest_bytes
        )));
    }
    let capacity = usize::try_from(encoded_len).map_err(|_| {
        RelationalRowPagePublicationError::Admission(
            "row-page manifest length overflows usize".into(),
        )
    })?;
    let unit = work.start_unit().map_err(work_error)?;
    let mut buffer = [0; 64 * 1024];
    unit.finish();
    let mut encoded =
        crate::background::CheckpointBytes::new(capacity, work).map_err(work_error)?;
    let mut file = file.take(read_limit);
    loop {
        let length = match io(Some(work), || Ok(file.read(&mut buffer)))? {
            Ok(length) => length,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(durability("read row-page manifest")(error)),
        };
        if length == 0 {
            break;
        }
        encoded
            .append(&buffer[..length], work)
            .map_err(work_error)?;
    }
    if encoded.len() > max_bytes {
        return Err(RelationalRowPagePublicationError::Admission(format!(
            "row-page manifest exceeds limit {}",
            config.max_manifest_bytes
        )));
    }
    work.checkpoint().map_err(work_error)?;
    Ok(encoded)
}

pub(super) fn decode_text(
    bytes: &[u8],
    context: &str,
    work: &CheckpointWorkContext,
) -> Result<String, RelationalRowPagePublicationError> {
    let unit = work.start_unit().map_err(work_error)?;
    let mut output = String::with_capacity(bytes.len());
    unit.finish();
    let mut start = 0;
    while start < bytes.len() {
        let end = start.saturating_add(64 * 1024).min(bytes.len());
        let unit = work.start_unit().map_err(work_error)?;
        match std::str::from_utf8(&bytes[start..end]) {
            Ok(text) => {
                output.push_str(text);
                start = end;
            }
            Err(error) if error.error_len().is_none() && end < bytes.len() => {
                let valid = error.valid_up_to();
                output.push_str(
                    std::str::from_utf8(&bytes[start..start + valid])
                        .expect("UTF-8 error's valid prefix is valid"),
                );
                start += valid;
            }
            Err(error) => {
                let position = start + error.valid_up_to();
                let description = match error.error_len() {
                    Some(len) => {
                        format!("invalid utf-8 sequence of {len} bytes from index {position}")
                    }
                    None => format!("incomplete utf-8 byte sequence from index {position}"),
                };
                return Err(RelationalRowPagePublicationError::Corrupt(format!(
                    "row-page {context} is not valid UTF-8: {description}"
                )));
            }
        }
        unit.finish();
    }
    work.checkpoint().map_err(work_error)?;
    Ok(output)
}

pub(in crate::relational::row_page::publication) fn encode_manifest_with_work_context(
    manifest: &RelationalRowPageRootManifest,
    config: RelationalRowPagePublicationConfig,
    work: &CheckpointWorkContext,
) -> Result<Vec<u8>, RelationalRowPagePublicationError> {
    super::encode_manifest_inner(manifest, config, Some(work))
}

pub(in crate::relational::row_page::publication) fn root_set_digest_with_work_context(
    tables: &[RelationalRowPageTableRoot],
    work: &CheckpointWorkContext,
) -> Result<Sha256Digest, RelationalRowPagePublicationError> {
    let mut hasher = IntegrityHasher::new();
    for table in tables {
        let encoded = super::encode_tables(std::slice::from_ref(table), Some(work))?;
        hash(&mut hasher, &encoded, work)?;
    }
    work.checkpoint().map_err(work_error)?;
    Ok(hasher.finish().sha256)
}
