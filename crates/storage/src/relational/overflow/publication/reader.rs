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

use super::super::{
    admit_overflow_hydration, decode_overflow_envelope, RelationalHydrationBudget,
    RelationalOverflowRef,
};
use super::{
    durability, manifest, relational_overflow_descriptor_file, relational_overflow_extent_file,
    relational_overflow_manifest_generation_file, RelationalOverflowExtentDescriptor,
    RelationalOverflowPublicationConfig, RelationalOverflowPublicationError,
    RelationalOverflowRootManifest, RELATIONAL_OVERFLOW_MANIFEST_FILE,
};
use crate::relational::{RelationalError, RelationalScalarType, RelationalValue};
use hawdb_integrity::{integrity_digest, IntegrityHasher, Sha256Digest};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub(super) const DESCRIPTOR_BYTES: usize = 120;
const DESCRIPTOR_BINDING_OFFSET: usize = 88;

#[derive(Debug, Clone)]
pub struct RelationalOverflowRootReader {
    directory: PathBuf,
    manifest: Arc<RelationalOverflowRootManifest>,
    config: RelationalOverflowPublicationConfig,
}

impl RelationalOverflowRootReader {
    pub fn open_latest(
        directory: &Path,
        config: RelationalOverflowPublicationConfig,
    ) -> Result<Option<Self>, RelationalOverflowPublicationError> {
        let path = directory.join(RELATIONAL_OVERFLOW_MANIFEST_FILE);
        manifest::read_manifest_if_exists(&path, config)?
            .map(|manifest| Self::from_manifest(directory, manifest, config))
            .transpose()
    }

    pub fn open_generation(
        directory: &Path,
        generation: u64,
        config: RelationalOverflowPublicationConfig,
    ) -> Result<Self, RelationalOverflowPublicationError> {
        let path = directory.join(relational_overflow_manifest_generation_file(generation));
        let manifest = manifest::read_manifest(&path, config)?;
        if manifest.generation != generation {
            return Err(RelationalOverflowPublicationError::Corrupt(format!(
                "overflow generation manifest {generation} identifies generation {}",
                manifest.generation
            )));
        }
        Self::from_manifest(directory, manifest, config)
    }

    pub fn open_bound_generation(
        directory: &Path,
        binding: super::RelationalOverflowGenerationArtifacts,
        config: RelationalOverflowPublicationConfig,
    ) -> Result<Self, RelationalOverflowPublicationError> {
        let path = directory.join(relational_overflow_manifest_generation_file(
            binding.generation,
        ));
        let manifest = manifest::read_bound_manifest(&path, config, binding.manifest_artifact)?;
        if manifest.generation != binding.generation
            || manifest.source_commit_epoch != binding.source_commit_epoch
            || manifest.root_set_digest != binding.root_set_digest
        {
            return Err(RelationalOverflowPublicationError::Corrupt(
                "overflow generation identity does not match its canonical binding".to_string(),
            ));
        }
        Self::from_manifest(directory, manifest, config)
    }

    fn from_manifest(
        directory: &Path,
        manifest: RelationalOverflowRootManifest,
        config: RelationalOverflowPublicationConfig,
    ) -> Result<Self, RelationalOverflowPublicationError> {
        validate_artifact_length(
            &directory.join(relational_overflow_extent_file(manifest.generation)),
            manifest.extent_artifact.encoded_len,
            "overflow extent artifact",
        )?;
        validate_artifact_length(
            &directory.join(relational_overflow_descriptor_file(manifest.generation)),
            manifest.descriptor_artifact.encoded_len,
            "overflow descriptor artifact",
        )?;
        Ok(Self {
            directory: directory.to_path_buf(),
            manifest: Arc::new(manifest),
            config,
        })
    }

    pub fn manifest(&self) -> &RelationalOverflowRootManifest {
        &self.manifest
    }

    pub fn contains(
        &self,
        reference: &RelationalOverflowRef,
    ) -> Result<bool, RelationalOverflowPublicationError> {
        Ok(self.find_descriptor(reference)?.is_some())
    }

    pub fn find_descriptor(
        &self,
        reference: &RelationalOverflowRef,
    ) -> Result<Option<RelationalOverflowExtentDescriptor>, RelationalOverflowPublicationError>
    {
        let mut descriptors = File::open(self.descriptor_path())
            .map_err(durability("open overflow descriptor artifact"))?;
        let mut lower = 0u64;
        let mut upper = self.manifest.extent_count;
        while lower < upper {
            let middle = lower + (upper - lower) / 2;
            let descriptor = self.read_descriptor_from(&mut descriptors, middle)?;
            if descriptor.reference.digest < reference.digest {
                lower = middle.checked_add(1).ok_or_else(|| {
                    RelationalOverflowPublicationError::Corrupt(
                        "overflow descriptor search overflow".to_string(),
                    )
                })?;
            } else {
                upper = middle;
            }
        }
        if lower == self.manifest.extent_count {
            return Ok(None);
        }
        let descriptor = self.read_descriptor_from(&mut descriptors, lower)?;
        if descriptor.reference.digest != reference.digest {
            return Ok(None);
        }
        if descriptor.reference != *reference {
            return Err(RelationalOverflowPublicationError::Corrupt(format!(
                "overflow descriptor {} metadata differs from its content identity",
                reference.digest
            )));
        }
        Ok(Some(descriptor))
    }

    pub fn hydrate(
        &self,
        reference: &RelationalOverflowRef,
        budget: &mut RelationalHydrationBudget,
        task_context: Option<&hawdb_core::RuntimeTaskContext>,
    ) -> Result<RelationalValue, RelationalOverflowPublicationError> {
        let descriptor = self.find_descriptor(reference)?.ok_or(
            RelationalOverflowPublicationError::MissingExtent(reference.digest),
        )?;
        admit_extent_hydration(reference, descriptor.envelope_bytes, budget)?;
        let encoded = self.read_encoded_extent(&descriptor)?;
        decode_overflow_envelope(reference, &encoded, budget, task_context)
            .map_err(publication_error)
    }

    /// Reads and verifies one encoded envelope without decompressing it.
    /// Exact overflow compaction uses this path to rewrite physical closure
    /// while keeping memory bounded to one value.
    pub(super) fn read_encoded_extent(
        &self,
        descriptor: &RelationalOverflowExtentDescriptor,
    ) -> Result<Arc<[u8]>, RelationalOverflowPublicationError> {
        let reference = descriptor.reference;
        let path = self.directory.join(relational_overflow_extent_file(
            descriptor.physical_generation,
        ));
        let end = descriptor
            .physical_offset
            .checked_add(descriptor.envelope_bytes)
            .ok_or_else(|| {
                RelationalOverflowPublicationError::Corrupt(
                    "overflow extent range overflow".to_string(),
                )
            })?;
        let artifact_bytes = fs::metadata(&path)
            .map_err(durability("read overflow extent artifact metadata"))?
            .len();
        if end > artifact_bytes {
            return Err(RelationalOverflowPublicationError::Corrupt(format!(
                "overflow extent {} range ends at {end}, beyond artifact length {artifact_bytes}",
                reference.digest
            )));
        }
        let envelope_bytes = usize::try_from(descriptor.envelope_bytes).map_err(|_| {
            RelationalOverflowPublicationError::Admission(
                "overflow envelope length exceeds this target".to_string(),
            )
        })?;
        let mut encoded = vec![0u8; envelope_bytes];
        let mut artifact = File::open(path).map_err(durability("open overflow extent artifact"))?;
        artifact
            .seek(SeekFrom::Start(descriptor.physical_offset))
            .map_err(durability("seek overflow extent"))?;
        artifact
            .read_exact(&mut encoded)
            .map_err(durability("read overflow extent"))?;
        let digest = integrity_digest(&encoded);
        if digest.crc32c.get() != descriptor.envelope_crc32c || digest.sha256 != reference.digest {
            return Err(RelationalOverflowPublicationError::Corrupt(format!(
                "overflow extent {} checksum mismatch",
                reference.digest
            )));
        }
        Ok(encoded.into())
    }

    pub fn visit_descriptors(
        &self,
        mut visitor: impl FnMut(
            &RelationalOverflowExtentDescriptor,
        ) -> Result<(), RelationalOverflowPublicationError>,
    ) -> Result<(), RelationalOverflowPublicationError> {
        let mut descriptors = File::open(self.descriptor_path())
            .map_err(durability("open overflow descriptor artifact"))?;
        let mut previous_digest = None;
        for ordinal in 0..self.manifest.extent_count {
            let descriptor = self.read_descriptor_from(&mut descriptors, ordinal)?;
            if previous_digest.is_some_and(|digest| digest >= descriptor.reference.digest) {
                return Err(RelationalOverflowPublicationError::Corrupt(
                    "overflow descriptors are not strictly ordered".to_string(),
                ));
            }
            previous_digest = Some(descriptor.reference.digest);
            visitor(&descriptor)?;
        }
        Ok(())
    }

    pub(super) fn read_descriptor_from(
        &self,
        descriptors: &mut File,
        ordinal: u64,
    ) -> Result<RelationalOverflowExtentDescriptor, RelationalOverflowPublicationError> {
        if ordinal >= self.manifest.extent_count {
            return Err(RelationalOverflowPublicationError::Admission(format!(
                "overflow descriptor ordinal {ordinal} exceeds extent count {}",
                self.manifest.extent_count
            )));
        }
        let offset = ordinal
            .checked_mul(DESCRIPTOR_BYTES as u64)
            .ok_or_else(|| {
                RelationalOverflowPublicationError::Corrupt(
                    "overflow descriptor offset overflow".to_string(),
                )
            })?;
        descriptors
            .seek(SeekFrom::Start(offset))
            .map_err(durability("seek overflow descriptor"))?;
        let mut encoded = [0u8; DESCRIPTOR_BYTES];
        descriptors
            .read_exact(&mut encoded)
            .map_err(durability("read overflow descriptor"))?;
        decode_descriptor(&encoded, self.manifest.generation, ordinal, self.config)
    }

    pub(super) fn descriptor_path(&self) -> PathBuf {
        self.directory.join(relational_overflow_descriptor_file(
            self.manifest.generation,
        ))
    }
}

fn publication_error(error: RelationalError) -> RelationalOverflowPublicationError {
    match error {
        RelationalError::Admission(message) => {
            RelationalOverflowPublicationError::Admission(message)
        }
        RelationalError::Durability(message) => {
            RelationalOverflowPublicationError::Durability(message)
        }
        RelationalError::Schema(message)
        | RelationalError::Constraint(message)
        | RelationalError::Corruption(message) => {
            RelationalOverflowPublicationError::Corrupt(message)
        }
    }
}

fn admit_extent_hydration(
    reference: &RelationalOverflowRef,
    envelope_bytes: u64,
    budget: &RelationalHydrationBudget,
) -> Result<(), RelationalOverflowPublicationError> {
    admit_overflow_hydration(reference, budget).map_err(publication_error)?;
    let envelope_bytes = usize::try_from(envelope_bytes).map_err(|_| {
        RelationalOverflowPublicationError::Admission(
            "overflow envelope length exceeds this target".to_string(),
        )
    })?;
    let uncompressed_bytes = usize::try_from(reference.uncompressed_bytes).map_err(|_| {
        RelationalOverflowPublicationError::Admission(
            "overflow uncompressed length exceeds this target".to_string(),
        )
    })?;
    let transient_memory_bytes = budget
        .memory_bytes
        .checked_add(envelope_bytes)
        .and_then(|bytes| bytes.checked_add(uncompressed_bytes))
        .ok_or_else(|| {
            RelationalOverflowPublicationError::Admission(
                "overflow hydration transient memory count overflow".to_string(),
            )
        })?;
    if transient_memory_bytes > budget.max_memory_bytes {
        return Err(RelationalOverflowPublicationError::Admission(format!(
            "overflow hydration requires {transient_memory_bytes} transient memory bytes, exceeding limit {}",
            budget.max_memory_bytes
        )));
    }
    Ok(())
}

pub(super) fn encode_descriptor(
    descriptor: RelationalOverflowExtentDescriptor,
    root_generation: u64,
    ordinal: u64,
    config: RelationalOverflowPublicationConfig,
) -> Result<[u8; DESCRIPTOR_BYTES], RelationalOverflowPublicationError> {
    validate_descriptor(&descriptor, root_generation, config, ErrorClass::Admission)?;
    let mut encoded = [0u8; DESCRIPTOR_BYTES];
    encoded[..32].copy_from_slice(descriptor.reference.digest.as_bytes());
    encoded[32] = scalar_type_tag(descriptor.reference.scalar_type)?;
    encoded[40..48].copy_from_slice(&descriptor.reference.compressed_bytes.to_le_bytes());
    encoded[48..56].copy_from_slice(&descriptor.reference.uncompressed_bytes.to_le_bytes());
    encoded[56..64].copy_from_slice(&descriptor.physical_generation.to_le_bytes());
    encoded[64..72].copy_from_slice(&descriptor.physical_offset.to_le_bytes());
    encoded[72..80].copy_from_slice(&descriptor.envelope_bytes.to_le_bytes());
    encoded[80..84].copy_from_slice(&descriptor.envelope_crc32c.to_le_bytes());
    let binding = descriptor_binding(
        &encoded[..DESCRIPTOR_BINDING_OFFSET],
        root_generation,
        ordinal,
    );
    encoded[DESCRIPTOR_BINDING_OFFSET..].copy_from_slice(binding.as_bytes());
    Ok(encoded)
}

fn decode_descriptor(
    encoded: &[u8; DESCRIPTOR_BYTES],
    root_generation: u64,
    ordinal: u64,
    config: RelationalOverflowPublicationConfig,
) -> Result<RelationalOverflowExtentDescriptor, RelationalOverflowPublicationError> {
    if encoded[33..40] != [0u8; 7] || encoded[84..88] != [0u8; 4] {
        return Err(RelationalOverflowPublicationError::Corrupt(
            "overflow descriptor has non-zero reserved bytes".to_string(),
        ));
    }
    let expected_binding = descriptor_binding(
        &encoded[..DESCRIPTOR_BINDING_OFFSET],
        root_generation,
        ordinal,
    );
    if expected_binding.as_bytes() != &encoded[DESCRIPTOR_BINDING_OFFSET..] {
        return Err(RelationalOverflowPublicationError::Corrupt(
            "overflow descriptor binding mismatch".to_string(),
        ));
    }
    let descriptor = RelationalOverflowExtentDescriptor {
        reference: RelationalOverflowRef {
            digest: Sha256Digest::from_bytes(
                encoded[..32]
                    .try_into()
                    .expect("overflow digest has a fixed length"),
            ),
            scalar_type: scalar_type_from_tag(encoded[32])?,
            compressed_bytes: read_u64(&encoded[40..48]),
            uncompressed_bytes: read_u64(&encoded[48..56]),
        },
        physical_generation: read_u64(&encoded[56..64]),
        physical_offset: read_u64(&encoded[64..72]),
        envelope_bytes: read_u64(&encoded[72..80]),
        envelope_crc32c: read_u32(&encoded[80..84]),
    };
    validate_descriptor(&descriptor, root_generation, config, ErrorClass::Corrupt)?;
    Ok(descriptor)
}

fn validate_descriptor(
    descriptor: &RelationalOverflowExtentDescriptor,
    root_generation: u64,
    config: RelationalOverflowPublicationConfig,
    class: ErrorClass,
) -> Result<(), RelationalOverflowPublicationError> {
    let fail = |message| class.error(message);
    if descriptor.physical_generation == 0 || descriptor.physical_generation > root_generation {
        return Err(fail(format!(
            "overflow extent physical generation {} is outside 1..={root_generation}",
            descriptor.physical_generation
        )));
    }
    let max_value_bytes = config.max_value_bytes.get() as u64;
    if descriptor.reference.compressed_bytes == 0
        || descriptor.reference.uncompressed_bytes == 0
        || descriptor.reference.compressed_bytes > max_value_bytes
        || descriptor.reference.uncompressed_bytes > max_value_bytes
    {
        return Err(fail(format!(
            "overflow descriptor lengths {}/{} are outside admitted range 1..={max_value_bytes}",
            descriptor.reference.compressed_bytes, descriptor.reference.uncompressed_bytes
        )));
    }
    let expected_envelope_bytes = descriptor
        .reference
        .compressed_bytes
        .checked_add(super::super::envelope::OVERFLOW_HEADER_BYTES as u64)
        .ok_or_else(|| fail("overflow envelope length overflow".to_string()))?;
    if descriptor.envelope_bytes != expected_envelope_bytes {
        return Err(fail(format!(
            "overflow descriptor envelope contains {} bytes, expected {expected_envelope_bytes}",
            descriptor.envelope_bytes
        )));
    }
    descriptor
        .physical_offset
        .checked_add(descriptor.envelope_bytes)
        .ok_or_else(|| fail("overflow extent range overflow".to_string()))?;
    Ok(())
}

fn descriptor_binding(encoded: &[u8], root_generation: u64, ordinal: u64) -> Sha256Digest {
    let mut hasher = IntegrityHasher::new();
    hasher.update(&root_generation.to_le_bytes());
    hasher.update(&ordinal.to_le_bytes());
    hasher.update(encoded);
    hasher.finish().sha256
}

fn scalar_type_tag(
    scalar_type: RelationalScalarType,
) -> Result<u8, RelationalOverflowPublicationError> {
    match scalar_type {
        RelationalScalarType::Text => Ok(1),
        RelationalScalarType::Bytea => Ok(2),
        _ => Err(RelationalOverflowPublicationError::Admission(
            "only TEXT and BYTEA can use overflow storage".to_string(),
        )),
    }
}

fn scalar_type_from_tag(
    tag: u8,
) -> Result<RelationalScalarType, RelationalOverflowPublicationError> {
    match tag {
        1 => Ok(RelationalScalarType::Text),
        2 => Ok(RelationalScalarType::Bytea),
        _ => Err(RelationalOverflowPublicationError::Corrupt(format!(
            "invalid overflow descriptor scalar type {tag}"
        ))),
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

fn validate_artifact_length(
    path: &Path,
    expected: u64,
    context: &str,
) -> Result<(), RelationalOverflowPublicationError> {
    let actual = fs::metadata(path)
        .map_err(durability("read overflow artifact metadata"))?
        .len();
    if actual != expected {
        return Err(RelationalOverflowPublicationError::Corrupt(format!(
            "{context} contains {actual} bytes, expected {expected}"
        )));
    }
    Ok(())
}

fn read_u32(encoded: &[u8]) -> u32 {
    u32::from_le_bytes(encoded.try_into().expect("u32 slice has fixed length"))
}

fn read_u64(encoded: &[u8]) -> u64 {
    u64::from_le_bytes(encoded.try_into().expect("u64 slice has fixed length"))
}
