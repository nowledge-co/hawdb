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

//! Manifest-last publication for immutable relational overflow extents.

use super::RelationalOverflowRef;
use hawdb_integrity::Sha256Digest;
use std::fmt;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;

mod manifest;
mod publisher;
mod reader;

pub use publisher::RelationalOverflowPublisher;
pub use reader::RelationalOverflowRootReader;

pub const RELATIONAL_OVERFLOW_MANIFEST_FILE: &str = "relational-overflow.manifest.hawdb";
const RELATIONAL_OVERFLOW_PUBLICATION_LOCK_FILE: &str = "relational-overflow.lock";

pub const DEFAULT_RELATIONAL_OVERFLOW_MANIFEST_BYTES: usize = 1024 * 1024;
pub const DEFAULT_RELATIONAL_OVERFLOW_EXTENTS: u64 = 1_000_000;
pub const DEFAULT_RELATIONAL_OVERFLOW_NEW_EXTENT_BYTES: u64 = 512 * 1024 * 1024;
const DEFAULT_RELATIONAL_OVERFLOW_DESCRIPTOR_BYTES: u64 = 256 * 1024 * 1024;

pub fn relational_overflow_extent_file(generation: u64) -> String {
    format!("relational-overflow-{generation}.extents.hawdb")
}

pub fn relational_overflow_descriptor_file(generation: u64) -> String {
    format!("relational-overflow-root-{generation}.descriptors.hawdb")
}

pub fn relational_overflow_manifest_generation_file(generation: u64) -> String {
    format!("relational-overflow-{generation}.manifest.hawdb")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalOverflowPublicationConfig {
    pub max_manifest_bytes: NonZeroUsize,
    pub max_extents: NonZeroU64,
    pub max_new_extent_bytes: NonZeroU64,
    pub max_descriptor_bytes: NonZeroU64,
    pub max_value_bytes: NonZeroUsize,
}

impl Default for RelationalOverflowPublicationConfig {
    fn default() -> Self {
        Self {
            max_manifest_bytes: NonZeroUsize::new(DEFAULT_RELATIONAL_OVERFLOW_MANIFEST_BYTES)
                .expect("default overflow manifest limit is non-zero"),
            max_extents: NonZeroU64::new(DEFAULT_RELATIONAL_OVERFLOW_EXTENTS)
                .expect("default overflow extent limit is non-zero"),
            max_new_extent_bytes: NonZeroU64::new(DEFAULT_RELATIONAL_OVERFLOW_NEW_EXTENT_BYTES)
                .expect("default overflow artifact limit is non-zero"),
            max_descriptor_bytes: NonZeroU64::new(DEFAULT_RELATIONAL_OVERFLOW_DESCRIPTOR_BYTES)
                .expect("default overflow descriptor limit is non-zero"),
            max_value_bytes: NonZeroUsize::new(super::DEFAULT_MAX_RELATIONAL_HYDRATION_BYTES)
                .expect("default overflow value limit is non-zero"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationalOverflowExtentInput {
    Reuse(RelationalOverflowRef),
    Write {
        reference: RelationalOverflowRef,
        encoded: Arc<[u8]>,
    },
}

impl RelationalOverflowExtentInput {
    pub fn encode(
        scalar_type: super::RelationalScalarType,
        raw: &[u8],
        config: super::RelationalOverflowConfig,
    ) -> Result<Self, super::RelationalError> {
        let encoded = super::encode_overflow_envelope(scalar_type, raw, config)?;
        Ok(Self::Write {
            reference: encoded.reference,
            encoded: encoded.bytes,
        })
    }

    pub const fn reference(&self) -> &RelationalOverflowRef {
        match self {
            Self::Reuse(reference) | Self::Write { reference, .. } => reference,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalOverflowArtifactMetadata {
    pub encoded_len: u64,
    pub encoded_crc32c: u32,
    pub encoded_sha256: Sha256Digest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalOverflowExtentDescriptor {
    pub reference: RelationalOverflowRef,
    pub physical_generation: u64,
    pub physical_offset: u64,
    pub envelope_bytes: u64,
    pub envelope_crc32c: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalOverflowRootBinding {
    pub generation: u64,
    pub source_commit_epoch: u64,
    pub root_set_digest: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalOverflowRootManifest {
    pub generation: u64,
    pub source_commit_epoch: u64,
    pub previous_generation: Option<u64>,
    pub extent_count: u64,
    pub new_extent_count: u64,
    pub extent_artifact: RelationalOverflowArtifactMetadata,
    pub descriptor_artifact: RelationalOverflowArtifactMetadata,
    pub root_set_digest: Sha256Digest,
}

impl RelationalOverflowRootManifest {
    pub const fn binding(&self) -> RelationalOverflowRootBinding {
        RelationalOverflowRootBinding {
            generation: self.generation,
            source_commit_epoch: self.source_commit_epoch,
            root_set_digest: self.root_set_digest,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationalOverflowPublicationPhase {
    CandidateStarted,
    CandidateExtentsDurable,
    CandidateRootDurable,
    CandidateManifestDurable,
    BaseRevalidated,
    CanonicalSelectionDeferred,
    LatestManifestPublished,
}

const COMPLETE_PUBLICATION_TRACE: [RelationalOverflowPublicationPhase; 6] = [
    RelationalOverflowPublicationPhase::CandidateStarted,
    RelationalOverflowPublicationPhase::CandidateExtentsDurable,
    RelationalOverflowPublicationPhase::CandidateRootDurable,
    RelationalOverflowPublicationPhase::CandidateManifestDurable,
    RelationalOverflowPublicationPhase::BaseRevalidated,
    RelationalOverflowPublicationPhase::LatestManifestPublished,
];

const CANDIDATE_PUBLICATION_TRACE: [RelationalOverflowPublicationPhase; 6] = [
    RelationalOverflowPublicationPhase::CandidateStarted,
    RelationalOverflowPublicationPhase::CandidateExtentsDurable,
    RelationalOverflowPublicationPhase::CandidateRootDurable,
    RelationalOverflowPublicationPhase::CandidateManifestDurable,
    RelationalOverflowPublicationPhase::BaseRevalidated,
    RelationalOverflowPublicationPhase::CanonicalSelectionDeferred,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalOverflowGenerationArtifacts {
    pub generation: u64,
    pub source_commit_epoch: u64,
    pub root_set_digest: Sha256Digest,
    pub manifest_artifact: RelationalOverflowArtifactMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalOverflowPublicationReport {
    pub generation: u64,
    pub source_commit_epoch: u64,
    pub extent_count: u64,
    pub new_extent_count: u64,
    pub reused_extent_count: u64,
    pub extent_artifact_bytes: u64,
    pub descriptor_artifact_bytes: u64,
    pub manifest_bytes: u64,
    pub generation_artifacts: RelationalOverflowGenerationArtifacts,
    pub events: [RelationalOverflowPublicationPhase; 6],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalOverflowExactPublicationReport {
    pub publication: RelationalOverflowPublicationReport,
    pub copied_base_extent_count: u64,
    pub introduced_extent_count: u64,
}

pub struct RelationalOverflowExactGenerationRequest<'a> {
    pub directory: &'a std::path::Path,
    pub generation: u64,
    pub source_commit_epoch: u64,
    pub base: &'a RelationalOverflowRootReader,
    pub expected_previous_generation: u64,
    pub references: &'a super::RelationalOverflowReferenceSet,
    pub task: &'a hawdb_core::RuntimeTaskContext,
}

#[derive(Debug)]
pub enum RelationalOverflowPublicationError {
    Admission(String),
    Corrupt(String),
    Durability(String),
    MissingExtent(Sha256Digest),
    Stopped(hawdb_core::RuntimeCancellationReason),
    StaleGeneration {
        expected_previous: Option<u64>,
        actual_previous: Option<u64>,
    },
}

impl fmt::Display for RelationalOverflowPublicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission(message) => {
                write!(formatter, "relational overflow publication admission failed: {message}")
            }
            Self::Corrupt(message) => {
                write!(formatter, "corrupt relational overflow publication: {message}")
            }
            Self::Durability(message) => {
                write!(formatter, "relational overflow publication durability failed: {message}")
            }
            Self::MissingExtent(digest) => {
                write!(formatter, "relational overflow root has no extent {digest}")
            }
            Self::Stopped(reason) => {
                write!(formatter, "relational overflow publication stopped: {reason}")
            }
            Self::StaleGeneration {
                expected_previous,
                actual_previous,
            } => write!(
                formatter,
                "relational overflow generation changed: expected {expected_previous:?}, found {actual_previous:?}"
            ),
        }
    }
}

impl std::error::Error for RelationalOverflowPublicationError {}

fn durability(
    context: &'static str,
) -> impl FnOnce(std::io::Error) -> RelationalOverflowPublicationError {
    move |error| RelationalOverflowPublicationError::Durability(format!("{context}: {error}"))
}

#[cfg(test)]
#[path = "publication/tests.rs"]
mod tests;
