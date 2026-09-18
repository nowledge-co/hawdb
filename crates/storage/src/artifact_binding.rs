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

//! Bound artifact admission and integrity checks for durable storage orchestration.

use hawdb_core::{HawDBError, Result};
use hawdb_integrity::{integrity_digest, Sha256Digest};
use std::fs::File;
use std::io::Read;
use std::path::Path;

#[derive(Debug, Clone, Copy)]
pub struct GraphManifestOpenBudget {
    max_encoded_bytes: u64,
    admitted_encoded_bytes: u64,
}

impl GraphManifestOpenBudget {
    pub const fn new(max_encoded_bytes: u64) -> Self {
        Self {
            max_encoded_bytes,
            admitted_encoded_bytes: 0,
        }
    }

    pub fn admit(&mut self, encoded_bytes: u64, artifact: &str) -> Result<()> {
        let required = self
            .admitted_encoded_bytes
            .checked_add(encoded_bytes)
            .ok_or_else(|| {
                HawDBError::Storage("aggregate graph manifest open bytes overflow u64".to_string())
            })?;
        if required > self.max_encoded_bytes {
            return Err(HawDBError::Storage(format!(
                "{artifact} requires {required} aggregate encoded graph manifest bytes during open, exceeding configured limit {}",
                self.max_encoded_bytes
            )));
        }
        self.admitted_encoded_bytes = required;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurableArtifactMetadata {
    pub encoded_len: u64,
    pub encoded_checksum: u64,
    pub encoded_sha256: Sha256Digest,
}

impl DurableArtifactMetadata {
    pub fn for_bytes(bytes: &[u8]) -> Self {
        let digest = integrity_digest(bytes);
        Self {
            encoded_len: bytes.len() as u64,
            encoded_checksum: digest.crc32c.as_u64(),
            encoded_sha256: digest.sha256,
        }
    }
}

pub fn admit_graph_manifest_binding(
    expected_len: u64,
    format_max_bytes: u64,
    artifact: &str,
    open_budget: &mut GraphManifestOpenBudget,
) -> Result<()> {
    if expected_len > format_max_bytes {
        return Err(HawDBError::Storage(format!(
            "{artifact} exceeds format limit {format_max_bytes} bytes"
        )));
    }
    open_budget.admit(expected_len, artifact)
}

pub fn read_bound_graph_manifest(
    path: &Path,
    expected_len: u64,
    expected_checksum: u64,
    expected_sha256: Sha256Digest,
    format_max_bytes: u64,
    artifact: &str,
    open_budget: &mut GraphManifestOpenBudget,
) -> Result<Vec<u8>> {
    admit_graph_manifest_binding(expected_len, format_max_bytes, artifact, open_budget)?;
    let read_limit = expected_len
        .checked_add(1)
        .ok_or_else(|| HawDBError::Storage(format!("{artifact} read limit overflows u64")))?;
    let file = File::open(path)?;
    let actual_len = file.metadata()?.len();
    if actual_len > expected_len {
        return Err(HawDBError::Storage(format!(
            "{artifact} contains {actual_len} bytes, exceeding its admitted bound {expected_len}"
        )));
    }
    let capacity = usize::try_from(actual_len)
        .map_err(|_| HawDBError::Storage(format!("{artifact} length does not fit usize")))?;
    let mut encoded = Vec::with_capacity(capacity);
    file.take(read_limit).read_to_end(&mut encoded)?;
    if encoded.len() as u64 > expected_len {
        return Err(HawDBError::Storage(format!(
            "{artifact} grew beyond its admitted bound {expected_len} during open"
        )));
    }
    verify_integrity(
        &encoded,
        expected_len,
        expected_checksum,
        expected_sha256,
        artifact,
    )?;
    Ok(encoded)
}

pub fn verify_integrity(
    bytes: &[u8],
    expected_len: u64,
    expected_checksum: u64,
    expected_sha256: Sha256Digest,
    artifact: &str,
) -> Result<()> {
    let actual_len = bytes.len() as u64;
    if actual_len != expected_len {
        return Err(HawDBError::Storage(format!(
            "{artifact} encoded length mismatch: expected {expected_len}, got {actual_len}"
        )));
    }
    let actual = integrity_digest(bytes);
    if actual.crc32c.as_u64() != expected_checksum {
        return Err(HawDBError::Storage(format!(
            "{artifact} CRC32C mismatch: expected {expected_checksum}, got {}",
            actual.crc32c
        )));
    }
    if actual.sha256 != expected_sha256 {
        return Err(HawDBError::Storage(format!(
            "{artifact} SHA-256 mismatch: expected {expected_sha256}, got {}",
            actual.sha256
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
