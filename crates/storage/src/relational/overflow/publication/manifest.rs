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

use super::{
    durability, RelationalOverflowArtifactMetadata, RelationalOverflowPublicationConfig,
    RelationalOverflowPublicationError, RelationalOverflowRootManifest,
};
use hawdb_integrity::{integrity_digest, Sha256Digest, SHA256_BYTES};
use std::fs::{self, File};
use std::io::Read;
use std::path::Path;

const MANIFEST_MAGIC: &[u8; 8] = b"SKOVRM01";
const MANIFEST_VERSION: u16 = 1;
pub(super) const MANIFEST_HEADER_BYTES: usize = 208;
const MANIFEST_INTEGRITY_OFFSET: usize = 172;
const ARTIFACT_METADATA_BYTES: usize = 44;

pub(super) fn encode_manifest(
    manifest: &RelationalOverflowRootManifest,
    config: RelationalOverflowPublicationConfig,
) -> Result<Vec<u8>, RelationalOverflowPublicationError> {
    validate_manifest(manifest, config, ErrorClass::Admission)?;
    if MANIFEST_HEADER_BYTES > config.max_manifest_bytes.get() {
        return Err(RelationalOverflowPublicationError::Admission(format!(
            "overflow manifest contains {MANIFEST_HEADER_BYTES} bytes, exceeding limit {}",
            config.max_manifest_bytes
        )));
    }

    let mut encoded = Vec::with_capacity(MANIFEST_HEADER_BYTES);
    encoded.extend_from_slice(MANIFEST_MAGIC);
    encoded.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
    encoded.extend_from_slice(&0u16.to_le_bytes());
    encoded.extend_from_slice(&manifest.generation.to_le_bytes());
    encoded.extend_from_slice(&manifest.source_commit_epoch.to_le_bytes());
    encoded.extend_from_slice(&manifest.previous_generation.unwrap_or(0).to_le_bytes());
    encoded.extend_from_slice(&manifest.extent_count.to_le_bytes());
    encoded.extend_from_slice(&manifest.new_extent_count.to_le_bytes());
    encode_artifact(manifest.extent_artifact, &mut encoded);
    encode_artifact(manifest.descriptor_artifact, &mut encoded);
    encoded.extend_from_slice(manifest.root_set_digest.as_bytes());
    debug_assert_eq!(encoded.len(), MANIFEST_INTEGRITY_OFFSET);
    encoded.extend_from_slice(&0u32.to_le_bytes());
    encoded.extend_from_slice(&[0u8; SHA256_BYTES]);
    debug_assert_eq!(encoded.len(), MANIFEST_HEADER_BYTES);

    let digest = integrity_digest(&encoded[..MANIFEST_INTEGRITY_OFFSET]);
    encoded[172..176].copy_from_slice(&digest.crc32c.get().to_le_bytes());
    encoded[176..208].copy_from_slice(digest.sha256.as_bytes());
    Ok(encoded)
}

pub(super) fn read_manifest_if_exists(
    path: &Path,
    config: RelationalOverflowPublicationConfig,
) -> Result<Option<RelationalOverflowRootManifest>, RelationalOverflowPublicationError> {
    match fs::metadata(path) {
        Ok(_) => read_manifest(path, config).map(Some),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(durability("read overflow manifest metadata")(error)),
    }
}

pub(super) fn read_manifest(
    path: &Path,
    config: RelationalOverflowPublicationConfig,
) -> Result<RelationalOverflowRootManifest, RelationalOverflowPublicationError> {
    let encoded = read_encoded_manifest(path, config)?;
    decode_manifest(&encoded, config)
}

pub(super) fn read_bound_manifest(
    path: &Path,
    config: RelationalOverflowPublicationConfig,
    expected: RelationalOverflowArtifactMetadata,
) -> Result<RelationalOverflowRootManifest, RelationalOverflowPublicationError> {
    let encoded = read_encoded_manifest(path, config)?;
    let digest = integrity_digest(&encoded);
    if encoded.len() as u64 != expected.encoded_len
        || digest.crc32c.get() != expected.encoded_crc32c
        || digest.sha256 != expected.encoded_sha256
    {
        return Err(RelationalOverflowPublicationError::Corrupt(
            "overflow generation manifest does not match its canonical binding".to_string(),
        ));
    }
    decode_manifest(&encoded, config)
}

fn read_encoded_manifest(
    path: &Path,
    config: RelationalOverflowPublicationConfig,
) -> Result<Vec<u8>, RelationalOverflowPublicationError> {
    let max_bytes = config.max_manifest_bytes.get();
    let max_bytes_u64 = u64::try_from(max_bytes).map_err(|_| {
        RelationalOverflowPublicationError::Admission(
            "overflow manifest limit overflows u64".to_string(),
        )
    })?;
    let read_limit = max_bytes_u64.checked_add(1).ok_or_else(|| {
        RelationalOverflowPublicationError::Admission(
            "overflow manifest read limit overflows u64".to_string(),
        )
    })?;
    let file = File::open(path).map_err(durability("open overflow manifest"))?;
    let encoded_len = file
        .metadata()
        .map_err(durability("read overflow manifest metadata"))?
        .len();
    if encoded_len > max_bytes_u64 {
        return Err(RelationalOverflowPublicationError::Admission(format!(
            "overflow manifest contains {encoded_len} bytes, exceeding limit {}",
            config.max_manifest_bytes
        )));
    }
    let capacity = usize::try_from(encoded_len).map_err(|_| {
        RelationalOverflowPublicationError::Admission(
            "overflow manifest length overflows usize".to_string(),
        )
    })?;
    let mut encoded = Vec::with_capacity(capacity);
    file.take(read_limit)
        .read_to_end(&mut encoded)
        .map_err(durability("read overflow manifest"))?;
    if encoded.len() > max_bytes {
        return Err(RelationalOverflowPublicationError::Admission(format!(
            "overflow manifest exceeds limit {}",
            config.max_manifest_bytes
        )));
    }
    Ok(encoded)
}

fn decode_manifest(
    encoded: &[u8],
    config: RelationalOverflowPublicationConfig,
) -> Result<RelationalOverflowRootManifest, RelationalOverflowPublicationError> {
    if encoded.len() != MANIFEST_HEADER_BYTES || &encoded[..8] != MANIFEST_MAGIC {
        return Err(RelationalOverflowPublicationError::Corrupt(
            "invalid overflow manifest header".to_string(),
        ));
    }
    let version = read_u16(&encoded[8..10]);
    let flags = read_u16(&encoded[10..12]);
    if version != MANIFEST_VERSION || flags != 0 {
        return Err(RelationalOverflowPublicationError::Corrupt(format!(
            "unsupported overflow manifest version {version} or flags {flags}"
        )));
    }
    let digest = integrity_digest(&encoded[..MANIFEST_INTEGRITY_OFFSET]);
    if digest.crc32c.get() != read_u32(&encoded[172..176])
        || digest.sha256.as_bytes() != &encoded[176..208]
    {
        return Err(RelationalOverflowPublicationError::Corrupt(
            "overflow manifest checksum mismatch".to_string(),
        ));
    }
    let previous_generation = read_u64(&encoded[28..36]);
    let manifest = RelationalOverflowRootManifest {
        generation: read_u64(&encoded[12..20]),
        source_commit_epoch: read_u64(&encoded[20..28]),
        previous_generation: (previous_generation != 0).then_some(previous_generation),
        extent_count: read_u64(&encoded[36..44]),
        new_extent_count: read_u64(&encoded[44..52]),
        extent_artifact: decode_artifact(&encoded[52..96]),
        descriptor_artifact: decode_artifact(&encoded[96..140]),
        root_set_digest: Sha256Digest::from_bytes(
            encoded[140..172]
                .try_into()
                .expect("overflow root digest has a fixed length"),
        ),
    };
    validate_manifest(&manifest, config, ErrorClass::Corrupt)?;
    Ok(manifest)
}

fn validate_manifest(
    manifest: &RelationalOverflowRootManifest,
    config: RelationalOverflowPublicationConfig,
    class: ErrorClass,
) -> Result<(), RelationalOverflowPublicationError> {
    let fail = |message| class.error(message);
    if manifest.generation == 0 || (manifest.source_commit_epoch == 0 && manifest.extent_count != 0)
    {
        return Err(fail(format!(
            "invalid overflow generation/epoch {}/{}",
            manifest.generation, manifest.source_commit_epoch
        )));
    }
    if manifest
        .previous_generation
        .is_some_and(|previous| previous >= manifest.generation)
    {
        return Err(fail(format!(
            "previous overflow generation {:?} does not precede generation {}",
            manifest.previous_generation, manifest.generation
        )));
    }
    if manifest.extent_count > config.max_extents.get()
        || manifest.new_extent_count > manifest.extent_count
    {
        return Err(fail(format!(
            "overflow manifest declares {}/{} new/total extents outside admitted total {}",
            manifest.new_extent_count, manifest.extent_count, config.max_extents
        )));
    }
    if manifest.extent_artifact.encoded_len > config.max_new_extent_bytes.get() {
        return Err(fail(format!(
            "overflow extent artifact contains {} bytes, exceeding limit {}",
            manifest.extent_artifact.encoded_len, config.max_new_extent_bytes
        )));
    }
    if (manifest.new_extent_count == 0) != (manifest.extent_artifact.encoded_len == 0) {
        return Err(fail(
            "overflow extent artifact emptiness does not match new extent count".to_string(),
        ));
    }
    let expected_descriptor_bytes = manifest
        .extent_count
        .checked_mul(super::reader::DESCRIPTOR_BYTES as u64)
        .ok_or_else(|| fail("overflow descriptor length overflow".to_string()))?;
    if manifest.descriptor_artifact.encoded_len != expected_descriptor_bytes
        || manifest.descriptor_artifact.encoded_len > config.max_descriptor_bytes.get()
    {
        return Err(fail(format!(
            "overflow descriptor artifact contains {} bytes, expected {expected_descriptor_bytes} within limit {}",
            manifest.descriptor_artifact.encoded_len, config.max_descriptor_bytes
        )));
    }
    Ok(())
}

fn encode_artifact(metadata: RelationalOverflowArtifactMetadata, encoded: &mut Vec<u8>) {
    encoded.extend_from_slice(&metadata.encoded_len.to_le_bytes());
    encoded.extend_from_slice(&metadata.encoded_crc32c.to_le_bytes());
    encoded.extend_from_slice(metadata.encoded_sha256.as_bytes());
}

fn decode_artifact(encoded: &[u8]) -> RelationalOverflowArtifactMetadata {
    debug_assert_eq!(encoded.len(), ARTIFACT_METADATA_BYTES);
    RelationalOverflowArtifactMetadata {
        encoded_len: read_u64(&encoded[..8]),
        encoded_crc32c: read_u32(&encoded[8..12]),
        encoded_sha256: Sha256Digest::from_bytes(
            encoded[12..44]
                .try_into()
                .expect("artifact digest has a fixed length"),
        ),
    }
}

#[derive(Clone, Copy)]
enum ErrorClass {
    Admission,
    Corrupt,
}

impl ErrorClass {
    fn error(self, message: String) -> RelationalOverflowPublicationError {
        match self {
            Self::Admission => RelationalOverflowPublicationError::Admission(message),
            Self::Corrupt => RelationalOverflowPublicationError::Corrupt(message),
        }
    }
}

fn read_u16(encoded: &[u8]) -> u16 {
    u16::from_le_bytes(encoded.try_into().expect("u16 slice has fixed length"))
}

fn read_u32(encoded: &[u8]) -> u32 {
    u32::from_le_bytes(encoded.try_into().expect("u32 slice has fixed length"))
}

fn read_u64(encoded: &[u8]) -> u64 {
    u64::from_le_bytes(encoded.try_into().expect("u64 slice has fixed length"))
}
