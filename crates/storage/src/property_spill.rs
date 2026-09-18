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

use crate::graph_descriptor_tree::demand::{
    GraphDescriptorTreeDemandReader, GraphDescriptorTreeReadLimits, GraphDescriptorTreeReadReport,
    GraphDescriptorTreeScanControl,
};
use crate::{
    content_digest, durable_replace_file, ContentDigest, FileSegmentRangeReader,
    GraphDescriptorKind, GraphDescriptorPageError, GraphDescriptorTreeArtifactMetadata,
    GraphDescriptorTreeBuildConfig, GraphDescriptorTreeBuilder, GraphDescriptorTreeError,
    GraphDescriptorTreeGenerationArtifacts, GraphDescriptorTreePaths,
    GraphDescriptorTreeRootReader, GraphDescriptorTreeWriteOutput, ManifestGeneration,
    PreparedGraphDescriptorTree, SegmentCache, SegmentRangeRead, SegmentReadError,
    SegmentReadRange, StoreId,
};
use hawdb_integrity::{Crc32cHasher, IntegrityHasher, Sha256Digest};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

const ARTIFACT_HEADER: &[u8; 16] = b"HAWDBPROPSPILL01";
const BLOCK_HEADER: &[u8; 8] = b"SKNPRP01";
const MANIFEST_HEADER: &str = "HAWDB_PROPERTY_SPILL_MANIFEST_V1";
const ARTIFACT_ID: u64 = 0x534b_5052_5350_4c31;
const DESCRIPTOR_ARTIFACT_ID: u64 = 0x534b_5052_5350_4431;
const BLOCK_ID_BASE: u64 = 1 << 62;
const BLOCK_FIXED_BYTES: u64 = 8 + 8 + 8 + 4;
const RECORD_FIXED_BYTES: u64 = 8 + 8;
const DESCRIPTOR_VALUE_MAGIC: &[u8; 8] = b"SKPSDSC1";
const DESCRIPTOR_VALUE_VERSION: u16 = 1;
const DESCRIPTOR_VALUE_BYTES: usize = 68;

pub fn property_spill_descriptor_page_file(generation: u64) -> String {
    format!("property-spill-descriptors-{generation}.pages.hawdb")
}

pub fn property_spill_descriptor_root_file(generation: u64) -> String {
    format!("property-spill-descriptors-{generation}.root.hawdb")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PropertySpillConfig {
    pub spill_threshold_bytes: NonZeroU64,
    pub target_block_bytes: NonZeroU64,
    pub max_value_bytes: NonZeroU64,
}

impl Default for PropertySpillConfig {
    fn default() -> Self {
        Self {
            spill_threshold_bytes: NonZeroU64::new(64 * 1024)
                .expect("default property spill threshold is non-zero"),
            target_block_bytes: NonZeroU64::new(1024 * 1024)
                .expect("default property spill block size is non-zero"),
            max_value_bytes: NonZeroU64::new(1024 * 1024 * 1024)
                .expect("default property spill value limit is non-zero"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PersistentPropertySpillDescriptorTree {
    paths: GraphDescriptorTreePaths,
    config: GraphDescriptorTreeBuildConfig,
}

impl PersistentPropertySpillDescriptorTree {
    pub const fn new(
        paths: GraphDescriptorTreePaths,
        config: GraphDescriptorTreeBuildConfig,
    ) -> Self {
        Self { paths, config }
    }

    fn into_parts(self) -> (GraphDescriptorTreePaths, GraphDescriptorTreeBuildConfig) {
        (self.paths, self.config)
    }
}

#[derive(Debug, Clone)]
pub struct PropertySpillWriteOptions<'a> {
    pub artifact_path: &'a Path,
    pub source_commit_epoch: u64,
    pub config: PropertySpillConfig,
    pub descriptor_tree: PersistentPropertySpillDescriptorTree,
}

#[derive(Debug)]
pub enum PropertySpillError {
    Io(std::io::Error),
    Read(SegmentReadError),
    DescriptorTree(GraphDescriptorTreeError),
    Corrupt(String),
    ValueTooLarge { value_bytes: u64, max_bytes: u64 },
    BlockTooLarge { block_bytes: u64, max_bytes: u64 },
}

impl Display for PropertySpillError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => Display::fmt(error, formatter),
            Self::Read(error) => Display::fmt(error, formatter),
            Self::DescriptorTree(error) => Display::fmt(error, formatter),
            Self::Corrupt(message) => formatter.write_str(message),
            Self::ValueTooLarge {
                value_bytes,
                max_bytes,
            } => write!(
                formatter,
                "property spill value uses {value_bytes} bytes, exceeding {max_bytes}"
            ),
            Self::BlockTooLarge {
                block_bytes,
                max_bytes,
            } => write!(
                formatter,
                "property spill block uses {block_bytes} bytes, exceeding {max_bytes}"
            ),
        }
    }
}

impl Error for PropertySpillError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Read(error) => Some(error),
            Self::DescriptorTree(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for PropertySpillError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<SegmentReadError> for PropertySpillError {
    fn from(error: SegmentReadError) -> Self {
        Self::Read(error)
    }
}

impl From<GraphDescriptorTreeError> for PropertySpillError {
    fn from(error: GraphDescriptorTreeError) -> Self {
        Self::DescriptorTree(error)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertySpillBlockDescriptor {
    pub block_id: u64,
    pub offset: u64,
    pub length: NonZeroU64,
    pub content_digest: ContentDigest,
    pub min_spill_id: u64,
    pub max_spill_id: u64,
    pub value_count: u32,
}

impl PropertySpillBlockDescriptor {
    pub fn descriptor_tree_key(&self) -> Vec<u8> {
        self.max_spill_id.to_be_bytes().to_vec()
    }

    pub fn encode_descriptor_tree_value(&self) -> Result<Vec<u8>, PropertySpillError> {
        self.validate_descriptor_identity()?;
        let mut encoded = Vec::with_capacity(DESCRIPTOR_VALUE_BYTES);
        encoded.extend_from_slice(DESCRIPTOR_VALUE_MAGIC);
        encoded.extend_from_slice(&DESCRIPTOR_VALUE_VERSION.to_le_bytes());
        encoded.extend_from_slice(&0u16.to_le_bytes());
        encoded.extend_from_slice(&self.block_id.to_le_bytes());
        encoded.extend_from_slice(&self.offset.to_le_bytes());
        encoded.extend_from_slice(&self.length.get().to_le_bytes());
        encoded.extend_from_slice(&self.content_digest.0.to_le_bytes());
        encoded.extend_from_slice(&self.min_spill_id.to_le_bytes());
        encoded.extend_from_slice(&self.max_spill_id.to_le_bytes());
        encoded.extend_from_slice(&self.value_count.to_le_bytes());
        encoded.extend_from_slice(&0u32.to_le_bytes());
        debug_assert_eq!(encoded.len(), DESCRIPTOR_VALUE_BYTES);
        Ok(encoded)
    }

    pub fn decode_descriptor_tree_entry(
        key: &[u8],
        value: &[u8],
    ) -> Result<Self, PropertySpillError> {
        if key.len() != 8
            || value.len() != DESCRIPTOR_VALUE_BYTES
            || &value[..8] != DESCRIPTOR_VALUE_MAGIC
        {
            return Err(PropertySpillError::Corrupt(
                "property spill descriptor tree entry has an invalid header or length".to_string(),
            ));
        }
        let version = u16::from_le_bytes(value[8..10].try_into().expect("fixed-width u16"));
        let flags = u16::from_le_bytes(value[10..12].try_into().expect("fixed-width u16"));
        let reserved = u32::from_le_bytes(value[64..68].try_into().expect("fixed-width u32"));
        if version != DESCRIPTOR_VALUE_VERSION || flags != 0 || reserved != 0 {
            return Err(PropertySpillError::Corrupt(format!(
                "property spill descriptor has unsupported version {version}, flags {flags}, or reserved fields"
            )));
        }
        let descriptor = Self {
            block_id: read_u64_at(value, 12),
            offset: read_u64_at(value, 20),
            length: NonZeroU64::new(read_u64_at(value, 28)).ok_or_else(|| {
                PropertySpillError::Corrupt(
                    "property spill descriptor block length is zero".to_string(),
                )
            })?,
            content_digest: ContentDigest(read_u64_at(value, 36)),
            min_spill_id: read_u64_at(value, 44),
            max_spill_id: read_u64_at(value, 52),
            value_count: u32::from_le_bytes(value[60..64].try_into().expect("fixed-width u32")),
        };
        descriptor.validate_descriptor_identity()?;
        if key != descriptor.descriptor_tree_key() {
            return Err(PropertySpillError::Corrupt(
                "property spill descriptor key does not match its value".to_string(),
            ));
        }
        Ok(descriptor)
    }

    fn validate_descriptor_identity(&self) -> Result<(), PropertySpillError> {
        let expected_count = self
            .max_spill_id
            .checked_sub(self.min_spill_id)
            .and_then(|difference| difference.checked_add(1));
        if self.block_id < BLOCK_ID_BASE
            || self.value_count == 0
            || self.min_spill_id > self.max_spill_id
            || expected_count != Some(u64::from(self.value_count))
            || self.content_digest.0 > u64::from(u32::MAX)
            || self.offset.checked_add(self.length.get()).is_none()
        {
            return Err(PropertySpillError::Corrupt(format!(
                "property spill block {} has invalid descriptor identity",
                self.block_id
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertySpillManifest {
    pub generation: ManifestGeneration,
    pub source_commit_epoch: u64,
    pub artifact_id: u64,
    pub artifact_len: u64,
    pub artifact_digest: ContentDigest,
    pub artifact_sha256: Sha256Digest,
    pub value_count: u64,
    pub value_bytes: u64,
    pub block_count: u64,
    pub descriptor_root_artifact: GraphDescriptorTreeArtifactMetadata,
}

impl PropertySpillManifest {
    pub const fn descriptor_generation_artifacts(&self) -> GraphDescriptorTreeGenerationArtifacts {
        GraphDescriptorTreeGenerationArtifacts {
            kind: GraphDescriptorKind::PropertySpill,
            generation: self.generation.0,
            source_commit_epoch: self.source_commit_epoch,
            root_artifact: self.descriptor_root_artifact,
        }
    }

    pub fn validate(&self) -> Result<(), PropertySpillError> {
        if self.artifact_id != ARTIFACT_ID {
            return Err(PropertySpillError::Corrupt(
                "property spill manifest has an unsupported artifact id".to_string(),
            ));
        }
        if self.artifact_len < ARTIFACT_HEADER.len() as u64 + 8 {
            return Err(PropertySpillError::Corrupt(
                "property spill artifact is shorter than its header".to_string(),
            ));
        }
        let header_bytes = ARTIFACT_HEADER.len() as u64 + 8;
        if self.block_count > self.value_count
            || (self.block_count == 0) != (self.value_count == 0)
            || (self.block_count == 0 && self.artifact_len != header_bytes)
            || (self.block_count != 0 && self.artifact_len == header_bytes)
            || (self.value_count == 0 && self.value_bytes != 0)
        {
            return Err(PropertySpillError::Corrupt(
                "property spill compact manifest counts or artifact length are inconsistent"
                    .to_string(),
            ));
        }
        if self.descriptor_root_artifact.encoded_len == 0 {
            return Err(PropertySpillError::Corrupt(
                "property spill descriptor root artifact is empty".to_string(),
            ));
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<String, PropertySpillError> {
        self.validate()?;
        let body = format!(
            "{MANIFEST_HEADER}\ngeneration\t{}\nsource_commit_epoch\t{}\nartifact_id\t{}\nartifact_len\t{}\nartifact_digest\t{}\nartifact_sha256\t{}\nvalue_count\t{}\nvalue_bytes\t{}\nblock_count\t{}\ndescriptor_root_len\t{}\ndescriptor_root_crc32c\t{}\ndescriptor_root_sha256\t{}\n",
            self.generation.0,
            self.source_commit_epoch,
            self.artifact_id,
            self.artifact_len,
            self.artifact_digest.0,
            self.artifact_sha256,
            self.value_count,
            self.value_bytes,
            self.block_count,
            self.descriptor_root_artifact.encoded_len,
            self.descriptor_root_artifact.encoded_crc32c,
            self.descriptor_root_artifact.encoded_sha256
        );
        let checksum = content_digest(body.as_bytes()).0;
        Ok(format!("{body}checksum\t{checksum}\n"))
    }

    pub fn decode(encoded: &str) -> Result<Self, PropertySpillError> {
        let marker = "checksum\t";
        let checksum_offset = encoded.rfind(marker).ok_or_else(|| {
            PropertySpillError::Corrupt("property spill manifest is missing checksum".to_string())
        })?;
        let body = &encoded[..checksum_offset];
        let checksum_line = encoded[checksum_offset..].trim_end();
        if checksum_line.contains('\n') {
            return Err(PropertySpillError::Corrupt(
                "property spill manifest has data after checksum".to_string(),
            ));
        }
        let expected = parse_u64(
            checksum_line.strip_prefix(marker).unwrap_or_default(),
            "manifest checksum",
        )?;
        let actual = content_digest(body.as_bytes()).0;
        if expected != actual {
            return Err(PropertySpillError::Corrupt(format!(
                "property spill manifest checksum mismatch: expected {expected}, got {actual}"
            )));
        }
        let mut generation = None;
        let mut source_commit_epoch = None;
        let mut artifact_id = None;
        let mut artifact_len = None;
        let mut artifact_digest = None;
        let mut artifact_sha256 = None;
        let mut value_count = None;
        let mut value_bytes = None;
        let mut block_count = None;
        let mut descriptor_root_len = None;
        let mut descriptor_root_crc32c = None;
        let mut descriptor_root_sha256 = None;
        let mut saw_header = false;
        for line in body.lines() {
            if line == MANIFEST_HEADER {
                saw_header = true;
                continue;
            }
            let fields = line.split('\t').collect::<Vec<_>>();
            match fields.as_slice() {
                ["generation", value] => generation = Some(parse_u64(value, "generation")?),
                ["source_commit_epoch", value] => {
                    source_commit_epoch = Some(parse_u64(value, "source commit epoch")?)
                }
                ["artifact_id", value] => artifact_id = Some(parse_u64(value, "artifact id")?),
                ["artifact_len", value] => {
                    artifact_len = Some(parse_u64(value, "artifact length")?)
                }
                ["artifact_digest", value] => {
                    artifact_digest = Some(parse_u64(value, "artifact digest")?)
                }
                ["artifact_sha256", value] => {
                    artifact_sha256 = Some(value.parse().map_err(|error| {
                        PropertySpillError::Corrupt(format!(
                            "invalid artifact SHA-256 digest: {error}"
                        ))
                    })?)
                }
                ["value_count", value] => value_count = Some(parse_u64(value, "value count")?),
                ["value_bytes", value] => value_bytes = Some(parse_u64(value, "value bytes")?),
                ["block_count", value] => block_count = Some(parse_u64(value, "block count")?),
                ["descriptor_root_len", value] => {
                    descriptor_root_len = Some(parse_u64(value, "descriptor root length")?)
                }
                ["descriptor_root_crc32c", value] => {
                    descriptor_root_crc32c = Some(parse_u32(value, "descriptor root CRC32C")?)
                }
                ["descriptor_root_sha256", value] => {
                    descriptor_root_sha256 = Some(value.parse().map_err(|error| {
                        PropertySpillError::Corrupt(format!(
                            "invalid descriptor root SHA-256 digest: {error}"
                        ))
                    })?)
                }
                [""] => {}
                _ => {
                    return Err(PropertySpillError::Corrupt(format!(
                        "invalid property spill manifest line: {line}"
                    )));
                }
            }
        }
        if !saw_header {
            return Err(PropertySpillError::Corrupt(
                "property spill manifest has an invalid header".to_string(),
            ));
        }
        let manifest = Self {
            generation: ManifestGeneration(required(generation, "generation")?),
            source_commit_epoch: required(source_commit_epoch, "source commit epoch")?,
            artifact_id: required(artifact_id, "artifact id")?,
            artifact_len: required(artifact_len, "artifact length")?,
            artifact_digest: ContentDigest(required(artifact_digest, "artifact digest")?),
            artifact_sha256: required(artifact_sha256, "artifact SHA-256 digest")?,
            value_count: required(value_count, "value count")?,
            value_bytes: required(value_bytes, "value bytes")?,
            block_count: required(block_count, "block count")?,
            descriptor_root_artifact: GraphDescriptorTreeArtifactMetadata {
                encoded_len: required(descriptor_root_len, "descriptor root length")?,
                encoded_crc32c: required(descriptor_root_crc32c, "descriptor root CRC32C")?,
                encoded_sha256: required(descriptor_root_sha256, "descriptor root SHA-256 digest")?,
            },
        };
        manifest.validate()?;
        Ok(manifest)
    }
}

pub struct PropertySpillWriter {
    path: PathBuf,
    file: File,
    generation: ManifestGeneration,
    config: PropertySpillConfig,
    artifact_digest: IntegrityHasher,
    artifact_len: u64,
    next_spill_id: u64,
    next_block_id: u64,
    value_bytes: u64,
    pending: Vec<(u64, Vec<u8>)>,
    pending_bytes: u64,
    block_count: u64,
    descriptor_tree: GraphDescriptorTreeBuilder,
}

impl PropertySpillWriter {
    pub fn create(
        path: impl Into<PathBuf>,
        generation: ManifestGeneration,
        source_commit_epoch: u64,
        config: PropertySpillConfig,
        descriptor_tree: PersistentPropertySpillDescriptorTree,
    ) -> Result<Self, PropertySpillError> {
        let path = path.into();
        let (descriptor_paths, descriptor_config) = descriptor_tree.into_parts();
        let mut file = File::create(&path)?;
        let mut artifact_digest = IntegrityHasher::new();
        write_hashed(&mut file, &mut artifact_digest, ARTIFACT_HEADER)?;
        write_hashed(&mut file, &mut artifact_digest, &generation.0.to_le_bytes())?;
        Ok(Self {
            path,
            file,
            generation,
            config,
            artifact_digest,
            artifact_len: ARTIFACT_HEADER.len() as u64 + 8,
            next_spill_id: 0,
            next_block_id: BLOCK_ID_BASE,
            value_bytes: 0,
            pending: Vec::new(),
            pending_bytes: 0,
            block_count: 0,
            descriptor_tree: GraphDescriptorTreeBuilder::create(
                descriptor_paths,
                GraphDescriptorKind::PropertySpill,
                generation.0,
                source_commit_epoch,
                DESCRIPTOR_ARTIFACT_ID,
                descriptor_config,
            )?,
        })
    }

    pub fn should_spill(&self, encoded_value_bytes: usize) -> bool {
        encoded_value_bytes as u64 >= self.config.spill_threshold_bytes.get()
    }

    pub fn push(&mut self, encoded_value: Vec<u8>) -> Result<u64, PropertySpillError> {
        let value_bytes = encoded_value.len() as u64;
        if value_bytes > self.config.max_value_bytes.get() {
            return Err(PropertySpillError::ValueTooLarge {
                value_bytes,
                max_bytes: self.config.max_value_bytes.get(),
            });
        }
        let record_bytes = RECORD_FIXED_BYTES.saturating_add(value_bytes);
        if !self.pending.is_empty()
            && BLOCK_FIXED_BYTES
                .saturating_add(self.pending_bytes)
                .saturating_add(record_bytes)
                > self.config.target_block_bytes.get()
        {
            self.flush_block()?;
        }
        let spill_id = self.next_spill_id;
        self.next_spill_id = self
            .next_spill_id
            .checked_add(1)
            .ok_or_else(|| PropertySpillError::Corrupt("property spill id overflow".to_string()))?;
        self.value_bytes = self.value_bytes.saturating_add(value_bytes);
        self.pending_bytes = self.pending_bytes.saturating_add(record_bytes);
        self.pending.push((spill_id, encoded_value));
        Ok(spill_id)
    }

    pub(crate) fn finish(mut self) -> Result<PreparedPropertySpillArtifact, PropertySpillError> {
        self.flush_block()?;
        self.file.sync_all()?;
        let artifact_integrity = self.artifact_digest.finish();
        let descriptor_tree = self.descriptor_tree.finish()?;
        Ok(PreparedPropertySpillArtifact {
            temporary_artifact_path: self.path,
            generation: self.generation,
            artifact_len: self.artifact_len,
            artifact_digest: ContentDigest(artifact_integrity.crc32c.as_u64()),
            artifact_sha256: artifact_integrity.sha256,
            value_count: self.next_spill_id,
            value_bytes: self.value_bytes,
            block_count: self.block_count,
            descriptor_tree,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn flush_block(&mut self) -> Result<(), PropertySpillError> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let block_bytes = BLOCK_FIXED_BYTES.saturating_add(self.pending_bytes);
        let hard_max = self.config.target_block_bytes.get().max(
            self.config
                .max_value_bytes
                .get()
                .saturating_add(BLOCK_FIXED_BYTES)
                .saturating_add(RECORD_FIXED_BYTES),
        );
        if block_bytes > hard_max {
            return Err(PropertySpillError::BlockTooLarge {
                block_bytes,
                max_bytes: hard_max,
            });
        }
        let min_spill_id = self.pending.first().expect("pending block is non-empty").0;
        let max_spill_id = self.pending.last().expect("pending block is non-empty").0;
        let value_count = u32::try_from(self.pending.len()).map_err(|_| {
            PropertySpillError::Corrupt("property spill block count exceeds u32".to_string())
        })?;
        let mut block_digest = Crc32cHasher::new();
        write_double_hashed(
            &mut self.file,
            &mut self.artifact_digest,
            &mut block_digest,
            BLOCK_HEADER,
        )?;
        write_double_hashed(
            &mut self.file,
            &mut self.artifact_digest,
            &mut block_digest,
            &self.generation.0.to_le_bytes(),
        )?;
        write_double_hashed(
            &mut self.file,
            &mut self.artifact_digest,
            &mut block_digest,
            &self.next_block_id.to_le_bytes(),
        )?;
        write_double_hashed(
            &mut self.file,
            &mut self.artifact_digest,
            &mut block_digest,
            &value_count.to_le_bytes(),
        )?;
        for (spill_id, value) in &self.pending {
            write_double_hashed(
                &mut self.file,
                &mut self.artifact_digest,
                &mut block_digest,
                &spill_id.to_le_bytes(),
            )?;
            write_double_hashed(
                &mut self.file,
                &mut self.artifact_digest,
                &mut block_digest,
                &(value.len() as u64).to_le_bytes(),
            )?;
            write_double_hashed(
                &mut self.file,
                &mut self.artifact_digest,
                &mut block_digest,
                value,
            )?;
        }
        let length = NonZeroU64::new(block_bytes).expect("property spill block is non-empty");
        let descriptor = PropertySpillBlockDescriptor {
            block_id: self.next_block_id,
            offset: self.artifact_len,
            length,
            content_digest: ContentDigest(block_digest.finish()),
            min_spill_id,
            max_spill_id,
            value_count,
        };
        self.descriptor_tree.push(
            descriptor.descriptor_tree_key(),
            descriptor.encode_descriptor_tree_value()?,
        )?;
        self.block_count = self.block_count.checked_add(1).ok_or_else(|| {
            PropertySpillError::Corrupt("property spill block count overflow".to_string())
        })?;
        self.artifact_len = self.artifact_len.saturating_add(block_bytes);
        self.next_block_id = self.next_block_id.saturating_add(1);
        self.pending.clear();
        self.pending_bytes = 0;
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct PreparedPropertySpillArtifact {
    temporary_artifact_path: PathBuf,
    generation: ManifestGeneration,
    artifact_len: u64,
    artifact_digest: ContentDigest,
    artifact_sha256: Sha256Digest,
    value_count: u64,
    value_bytes: u64,
    block_count: u64,
    descriptor_tree: PreparedGraphDescriptorTree,
}

impl PreparedPropertySpillArtifact {
    pub(crate) fn publish(
        self,
        artifact_path: &Path,
    ) -> Result<PropertySpillWriteOutput, PropertySpillError> {
        durable_replace_file(&self.temporary_artifact_path, artifact_path)?;
        let descriptor_tree = self.descriptor_tree.publish()?;
        if descriptor_tree.root.kind != GraphDescriptorKind::PropertySpill
            || descriptor_tree.root.generation != self.generation.0
            || descriptor_tree.root.descriptor_count != self.block_count
        {
            return Err(PropertySpillError::Corrupt(
                "property spill descriptor root identity is inconsistent".to_string(),
            ));
        }
        let manifest = PropertySpillManifest {
            generation: self.generation,
            source_commit_epoch: descriptor_tree.root.source_commit_epoch,
            artifact_id: ARTIFACT_ID,
            artifact_len: self.artifact_len,
            artifact_digest: self.artifact_digest,
            artifact_sha256: self.artifact_sha256,
            value_count: self.value_count,
            value_bytes: self.value_bytes,
            block_count: self.block_count,
            descriptor_root_artifact: descriptor_tree.root_artifact,
        };
        manifest.validate()?;
        Ok(PropertySpillWriteOutput {
            manifest,
            descriptor_tree,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertySpillWriteOutput {
    pub manifest: PropertySpillManifest,
    pub descriptor_tree: GraphDescriptorTreeWriteOutput,
}

#[derive(Debug, Clone)]
pub struct PropertySpillReader {
    path: PathBuf,
    manifest: PropertySpillManifest,
    descriptor_reader: GraphDescriptorTreeDemandReader,
    range_reader: FileSegmentRangeReader,
    max_block_bytes: NonZeroU64,
    poisoned: Arc<AtomicBool>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PropertySpillReadReport {
    pub generation: u64,
    pub descriptor_pages_visited: u64,
    pub descriptor_page_bytes_decoded: u64,
    pub descriptor_storage_bytes_read: u64,
    pub descriptors_examined: u64,
    pub descriptor_cache_hits: u64,
    pub descriptor_cache_misses: u64,
    pub descriptor_cache_admission_rejections: u64,
    pub blocks_read: u64,
    pub bytes_read: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertySpillReadOutput {
    pub value: Option<Arc<[u8]>>,
    pub report: PropertySpillReadReport,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PropertySpillScrubReport {
    pub descriptor_pages_checked: u64,
    pub descriptors_checked: u64,
    pub descriptor_bytes_checked: u64,
    pub spill_blocks_checked: u64,
    pub spill_values_checked: u64,
    pub spill_bytes_hashed: u64,
}

impl PropertySpillReader {
    pub fn open(
        path: impl Into<PathBuf>,
        manifest: PropertySpillManifest,
        descriptor_tree: PersistentPropertySpillDescriptorTree,
        cache: Arc<SegmentCache>,
        store_id: StoreId,
        max_block_bytes: NonZeroU64,
    ) -> Result<Self, PropertySpillError> {
        manifest.validate()?;
        let (descriptor_paths, descriptor_config) = descriptor_tree.into_parts();
        let path = path.into();
        let metadata = fs::metadata(&path)?;
        if metadata.len() != manifest.artifact_len {
            return Err(PropertySpillError::Corrupt(format!(
                "property spill artifact length mismatch: expected {}, got {}",
                manifest.artifact_len,
                metadata.len()
            )));
        }
        let mut header = [0u8; 24];
        File::open(&path)?.read_exact(&mut header)?;
        if &header[..16] != ARTIFACT_HEADER {
            return Err(PropertySpillError::Corrupt(
                "property spill artifact has an invalid header".to_string(),
            ));
        }
        let generation = u64::from_le_bytes(header[16..24].try_into().expect("fixed header"));
        if generation != manifest.generation.0 {
            return Err(PropertySpillError::Corrupt(format!(
                "property spill artifact generation {generation} does not match manifest generation {}",
                manifest.generation.0
            )));
        }
        let root_reader = GraphDescriptorTreeRootReader::open_bound(
            descriptor_paths,
            manifest.descriptor_generation_artifacts(),
            descriptor_config,
        )?;
        let root = root_reader.root();
        if root.kind != GraphDescriptorKind::PropertySpill
            || root.generation != manifest.generation.0
            || root.source_commit_epoch != manifest.source_commit_epoch
            || root.page_artifact_id != DESCRIPTOR_ARTIFACT_ID
            || root.descriptor_count != manifest.block_count
            || root.root.is_some() != (manifest.block_count != 0)
        {
            return Err(PropertySpillError::Corrupt(
                "property spill descriptor root does not match its compact manifest".to_string(),
            ));
        }
        let descriptor_reader = GraphDescriptorTreeDemandReader::open(
            root_reader,
            descriptor_config,
            Arc::clone(&cache),
            store_id,
        )?;
        let mut range_reader =
            FileSegmentRangeReader::new().with_cache(cache, store_id, manifest.generation);
        range_reader.register(manifest.artifact_id, path.clone());
        Ok(Self {
            path,
            manifest,
            descriptor_reader,
            range_reader,
            max_block_bytes,
            poisoned: Arc::new(AtomicBool::new(false)),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn manifest(&self) -> &PropertySpillManifest {
        &self.manifest
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire) || self.descriptor_reader.is_poisoned()
    }

    pub fn get(&self, spill_id: u64) -> Result<Option<Arc<[u8]>>, PropertySpillError> {
        self.get_with_report(spill_id).map(|output| output.value)
    }

    pub fn get_with_report(
        &self,
        spill_id: u64,
    ) -> Result<PropertySpillReadOutput, PropertySpillError> {
        self.ensure_healthy()?;
        let mut report = PropertySpillReadReport {
            generation: self.manifest.generation.0,
            ..PropertySpillReadReport::default()
        };
        if spill_id >= self.manifest.value_count {
            return Ok(PropertySpillReadOutput {
                value: None,
                report,
            });
        }
        let limits = GraphDescriptorTreeReadLimits {
            max_descriptors: NonZeroU64::new(1).expect("property spill seek emits one block"),
            ..GraphDescriptorTreeReadLimits::default()
        };
        let mut selected = None;
        let mut descriptor_error = None;
        let descriptor_result =
            self.descriptor_reader
                .scan_from(&spill_id.to_be_bytes(), limits, |key, value| {
                    match PropertySpillBlockDescriptor::decode_descriptor_tree_entry(key, value) {
                        Ok(block) => selected = Some(block),
                        Err(error) => descriptor_error = Some(error),
                    }
                    Ok(GraphDescriptorTreeScanControl::Stop)
                });
        let result = match (descriptor_result, descriptor_error) {
            (_, Some(error)) => Err(error),
            (Err(error), None) => Err(error.into()),
            (Ok((descriptor_report, _)), None) => (|| {
                report.record_descriptor_read(descriptor_report);
                let block = selected.ok_or_else(|| {
                    PropertySpillError::Corrupt(format!(
                        "property spill descriptor tree has no block for admitted id {spill_id}"
                    ))
                })?;
                if spill_id < block.min_spill_id || spill_id > block.max_spill_id {
                    return Err(PropertySpillError::Corrupt(format!(
                        "property spill descriptor block {} does not contain admitted id {spill_id}",
                        block.block_id
                    )));
                }
                let read = self.read_block(&block)?;
                report.blocks_read = 1;
                report.bytes_read = read.payload.len() as u64;
                report.cache_hits = u64::from(read.cache_hit);
                report.cache_misses = u64::from(read.cache_miss);
                decode_block_value(&read.payload, self.manifest.generation, &block, spill_id)
                    .map(|value| PropertySpillReadOutput { value, report })
            })(),
        };
        self.poison_on_physical_failure(&result);
        result
    }

    pub fn deep_scrub(&self) -> Result<PropertySpillScrubReport, PropertySpillError> {
        self.ensure_healthy()?;
        let result = self.deep_scrub_inner();
        self.poison_on_physical_failure(&result);
        result
    }

    fn deep_scrub_inner(&self) -> Result<PropertySpillScrubReport, PropertySpillError> {
        let spill_bytes_hashed = self.verify_whole_artifact()?;
        let mut artifact = File::open(&self.path)?;
        let mut expected_offset = (ARTIFACT_HEADER.len() + 8) as u64;
        let mut expected_block_id = BLOCK_ID_BASE;
        let mut expected_spill_id = 0u64;
        let mut blocks_checked = 0u64;
        let mut values_checked = 0u64;
        let mut visit_error = None;
        let scrub = self.descriptor_reader.deep_visit(|key, value| {
            if visit_error.is_some() {
                return Ok(GraphDescriptorTreeScanControl::Continue);
            }
            let step = PropertySpillBlockDescriptor::decode_descriptor_tree_entry(key, value)
                .and_then(|block| {
                    if block.offset != expected_offset
                        || block.block_id != expected_block_id
                        || block.min_spill_id != expected_spill_id
                    {
                        return Err(PropertySpillError::Corrupt(format!(
                            "property spill descriptor closure is not contiguous at block {}",
                            block.block_id
                        )));
                    }
                    let encoded =
                        read_spill_block_uncached(&mut artifact, &block, self.max_block_bytes)?;
                    decode_block_value_inner(&encoded, self.manifest.generation, &block, None)?;
                    blocks_checked = blocks_checked.checked_add(1).ok_or_else(|| {
                        PropertySpillError::Corrupt(
                            "property spill scrub block count overflow".to_string(),
                        )
                    })?;
                    values_checked = values_checked
                        .checked_add(u64::from(block.value_count))
                        .ok_or_else(|| {
                            PropertySpillError::Corrupt(
                                "property spill scrub value count overflow".to_string(),
                            )
                        })?;
                    expected_offset =
                        expected_offset
                            .checked_add(block.length.get())
                            .ok_or_else(|| {
                                PropertySpillError::Corrupt(
                                    "property spill scrub offset overflow".to_string(),
                                )
                            })?;
                    expected_block_id = expected_block_id.checked_add(1).ok_or_else(|| {
                        PropertySpillError::Corrupt(
                            "property spill scrub block id overflow".to_string(),
                        )
                    })?;
                    expected_spill_id = block.max_spill_id.checked_add(1).ok_or_else(|| {
                        PropertySpillError::Corrupt("property spill scrub id overflow".to_string())
                    })?;
                    Ok(())
                });
            if let Err(error) = step {
                visit_error = Some(error);
            }
            Ok(GraphDescriptorTreeScanControl::Continue)
        });
        let scrub = match (scrub, visit_error) {
            (_, Some(error)) => return Err(error),
            (Err(error), None) => return Err(error.into()),
            (Ok(report), None) => report,
        };
        if expected_offset != self.manifest.artifact_len
            || blocks_checked != self.manifest.block_count
            || values_checked != self.manifest.value_count
            || expected_spill_id != self.manifest.value_count
            || scrub.checked_descriptors != self.manifest.block_count
        {
            return Err(PropertySpillError::Corrupt(format!(
                "property spill scrub closure bytes/blocks/values {expected_offset}/{blocks_checked}/{values_checked} do not match {}/{}/{}",
                self.manifest.artifact_len,
                self.manifest.block_count,
                self.manifest.value_count
            )));
        }
        Ok(PropertySpillScrubReport {
            descriptor_pages_checked: scrub.checked_pages,
            descriptors_checked: scrub.checked_descriptors,
            descriptor_bytes_checked: scrub.page_bytes_decoded,
            spill_blocks_checked: blocks_checked,
            spill_values_checked: values_checked,
            spill_bytes_hashed,
        })
    }

    fn read_block(
        &self,
        block: &PropertySpillBlockDescriptor,
    ) -> Result<SegmentRangeRead, PropertySpillError> {
        if block.length.get() > self.max_block_bytes.get() {
            return Err(PropertySpillError::BlockTooLarge {
                block_bytes: block.length.get(),
                max_bytes: self.max_block_bytes.get(),
            });
        }
        Ok(self
            .range_reader
            .read_range_with_report(&SegmentReadRange {
                artifact_id: self.manifest.artifact_id,
                segment_ids: vec![block.block_id],
                offset: block.offset,
                length: block.length,
                content_digest: Some(block.content_digest),
            })?)
    }

    fn verify_whole_artifact(&self) -> Result<u64, PropertySpillError> {
        let mut file = File::open(&self.path)?;
        if file.metadata()?.len() != self.manifest.artifact_len {
            return Err(PropertySpillError::Corrupt(
                "property spill artifact length changed after open".to_string(),
            ));
        }
        let mut hasher = IntegrityHasher::new();
        let mut buffer = vec![0u8; 64 * 1024];
        let mut total = 0u64;
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
            total = total.checked_add(read as u64).ok_or_else(|| {
                PropertySpillError::Corrupt("property spill scrub byte count overflow".to_string())
            })?;
        }
        let digest = hasher.finish();
        if digest.crc32c.as_u64() != self.manifest.artifact_digest.0
            || digest.sha256 != self.manifest.artifact_sha256
        {
            return Err(PropertySpillError::Corrupt(
                "property spill artifact checksum mismatch during scrub".to_string(),
            ));
        }
        Ok(total)
    }

    fn ensure_healthy(&self) -> Result<(), PropertySpillError> {
        if self.is_poisoned() {
            return Err(PropertySpillError::Corrupt(
                "property spill reader is poisoned by an earlier physical failure".to_string(),
            ));
        }
        Ok(())
    }

    fn poison_on_physical_failure<T>(&self, result: &Result<T, PropertySpillError>) {
        if result
            .as_ref()
            .is_err_and(property_spill_error_requires_poison)
        {
            self.poisoned.store(true, Ordering::Release);
        }
    }
}

fn decode_block_value(
    bytes: &[u8],
    generation: ManifestGeneration,
    descriptor: &PropertySpillBlockDescriptor,
    wanted: u64,
) -> Result<Option<Arc<[u8]>>, PropertySpillError> {
    decode_block_value_inner(bytes, generation, descriptor, Some(wanted))
}

fn decode_block_value_inner(
    bytes: &[u8],
    generation: ManifestGeneration,
    descriptor: &PropertySpillBlockDescriptor,
    wanted: Option<u64>,
) -> Result<Option<Arc<[u8]>>, PropertySpillError> {
    let mut cursor = Cursor::new(bytes);
    if cursor.read_exact(8)? != BLOCK_HEADER {
        return Err(PropertySpillError::Corrupt(format!(
            "property spill block {} has an invalid header",
            descriptor.block_id
        )));
    }
    let stored_generation = cursor.read_u64()?;
    let block_id = cursor.read_u64()?;
    let value_count = cursor.read_u32()?;
    if stored_generation != generation.0
        || block_id != descriptor.block_id
        || value_count != descriptor.value_count
    {
        return Err(PropertySpillError::Corrupt(format!(
            "property spill block {} metadata does not match its manifest",
            descriptor.block_id
        )));
    }
    let mut first_id = None;
    let mut previous_id = None;
    let mut found = None;
    for _ in 0..value_count {
        let spill_id = cursor.read_u64()?;
        let length = usize::try_from(cursor.read_u64()?).map_err(|_| {
            PropertySpillError::Corrupt("property spill value length exceeds usize".to_string())
        })?;
        if previous_id.is_some_and(|previous| spill_id <= previous) {
            return Err(PropertySpillError::Corrupt(format!(
                "property spill block {} ids are not strictly ordered",
                descriptor.block_id
            )));
        }
        let value = cursor.read_exact(length)?;
        if wanted == Some(spill_id) {
            found = Some(Arc::<[u8]>::from(value));
        }
        if first_id.is_none() {
            first_id = Some(spill_id);
        }
        previous_id = Some(spill_id);
    }
    if !cursor.is_empty()
        || first_id != Some(descriptor.min_spill_id)
        || previous_id != Some(descriptor.max_spill_id)
    {
        return Err(PropertySpillError::Corrupt(format!(
            "property spill block {} payload bounds are inconsistent",
            descriptor.block_id
        )));
    }
    Ok(found)
}

impl PropertySpillReadReport {
    fn record_descriptor_read(&mut self, report: GraphDescriptorTreeReadReport) {
        self.descriptor_pages_visited = report.pages_visited;
        self.descriptor_page_bytes_decoded = report.page_bytes_decoded;
        self.descriptor_storage_bytes_read = report.storage_bytes_read;
        self.descriptors_examined = report.descriptors_emitted;
        self.descriptor_cache_hits = report.cache_hits;
        self.descriptor_cache_misses = report.cache_misses;
        self.descriptor_cache_admission_rejections = report.cache_admission_rejections;
    }
}

fn read_spill_block_uncached(
    file: &mut File,
    block: &PropertySpillBlockDescriptor,
    max_block_bytes: NonZeroU64,
) -> Result<Vec<u8>, PropertySpillError> {
    if block.length.get() > max_block_bytes.get() {
        return Err(PropertySpillError::BlockTooLarge {
            block_bytes: block.length.get(),
            max_bytes: max_block_bytes.get(),
        });
    }
    let length =
        usize::try_from(block.length.get()).map_err(|_| PropertySpillError::BlockTooLarge {
            block_bytes: block.length.get(),
            max_bytes: usize::MAX as u64,
        })?;
    file.seek(SeekFrom::Start(block.offset))?;
    let mut encoded = vec![0u8; length];
    file.read_exact(&mut encoded)?;
    if content_digest(&encoded) != block.content_digest {
        return Err(PropertySpillError::Corrupt(format!(
            "property spill block {} failed content digest verification",
            block.block_id
        )));
    }
    Ok(encoded)
}

fn property_spill_error_requires_poison(error: &PropertySpillError) -> bool {
    match error {
        PropertySpillError::Io(_)
        | PropertySpillError::Read(_)
        | PropertySpillError::Corrupt(_) => true,
        PropertySpillError::DescriptorTree(error) => matches!(
            error,
            GraphDescriptorTreeError::Io(_)
                | GraphDescriptorTreeError::Page(GraphDescriptorPageError::Corrupt(_))
                | GraphDescriptorTreeError::Corrupt(_)
        ),
        PropertySpillError::ValueTooLarge { .. } | PropertySpillError::BlockTooLarge { .. } => {
            false
        }
    }
}

fn parse_u64(value: &str, name: &str) -> Result<u64, PropertySpillError> {
    value.parse().map_err(|_| {
        PropertySpillError::Corrupt(format!(
            "property spill manifest has invalid {name}: {value}"
        ))
    })
}

fn read_u64_at(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("fixed-width u64"),
    )
}

fn parse_u32(value: &str, name: &str) -> Result<u32, PropertySpillError> {
    value.parse().map_err(|_| {
        PropertySpillError::Corrupt(format!(
            "property spill manifest has invalid {name}: {value}"
        ))
    })
}

fn required<T>(value: Option<T>, name: &str) -> Result<T, PropertySpillError> {
    value.ok_or_else(|| {
        PropertySpillError::Corrupt(format!("property spill manifest is missing {name}"))
    })
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_exact(&mut self, length: usize) -> Result<&'a [u8], PropertySpillError> {
        let end = self.offset.checked_add(length).ok_or_else(|| {
            PropertySpillError::Corrupt("property spill cursor offset overflow".to_string())
        })?;
        let value = self.bytes.get(self.offset..end).ok_or_else(|| {
            PropertySpillError::Corrupt(
                "property spill block ended before its declared length".to_string(),
            )
        })?;
        self.offset = end;
        Ok(value)
    }

    fn read_u32(&mut self) -> Result<u32, PropertySpillError> {
        Ok(u32::from_le_bytes(
            self.read_exact(4)?.try_into().expect("fixed-width u32"),
        ))
    }

    fn read_u64(&mut self) -> Result<u64, PropertySpillError> {
        Ok(u64::from_le_bytes(
            self.read_exact(8)?.try_into().expect("fixed-width u64"),
        ))
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

fn write_hashed(
    writer: &mut impl Write,
    digest: &mut IntegrityHasher,
    bytes: &[u8],
) -> Result<(), PropertySpillError> {
    writer.write_all(bytes)?;
    digest.update(bytes);
    Ok(())
}

fn write_double_hashed(
    writer: &mut impl Write,
    artifact_digest: &mut IntegrityHasher,
    block_digest: &mut Crc32cHasher,
    bytes: &[u8],
) -> Result<(), PropertySpillError> {
    writer.write_all(bytes)?;
    artifact_digest.update(bytes);
    block_digest.update(bytes);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spill_blocks_round_trip_and_fail_closed_on_corruption() {
        let root = std::env::temp_dir().join(format!(
            "hawdb-property-spill-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let path = root.join("properties.hawdb");
        let config = PropertySpillConfig {
            spill_threshold_bytes: NonZeroU64::new(8).unwrap(),
            target_block_bytes: NonZeroU64::new(160).unwrap(),
            max_value_bytes: NonZeroU64::new(1024).unwrap(),
        };
        let descriptor_paths = GraphDescriptorTreePaths::new(
            root.join(property_spill_descriptor_page_file(3)),
            root.join(property_spill_descriptor_root_file(3)),
        );
        let mut writer = PropertySpillWriter::create(
            path.with_extension("hawdb.tmp"),
            ManifestGeneration(3),
            11,
            config,
            PersistentPropertySpillDescriptorTree::new(
                descriptor_paths.clone(),
                GraphDescriptorTreeBuildConfig::default(),
            ),
        )
        .unwrap();
        let first = writer.push(vec![1; 32]).unwrap();
        let second = writer.push(vec![2; 48]).unwrap();
        let third = writer.push(vec![3; 48]).unwrap();
        let output = writer.finish().unwrap().publish(&path).unwrap();
        let manifest = output.manifest;
        assert_eq!(manifest.block_count, 2);
        assert_eq!(manifest.source_commit_epoch, 11);
        assert_eq!(output.descriptor_tree.root.descriptor_count, 2);
        assert_eq!(
            output.descriptor_tree.root.kind,
            GraphDescriptorKind::PropertySpill
        );
        let root_reader = crate::GraphDescriptorTreeRootReader::open_bound(
            descriptor_paths.clone(),
            manifest.descriptor_generation_artifacts(),
            GraphDescriptorTreeBuildConfig::default(),
        )
        .unwrap();
        assert_eq!(root_reader.root(), &output.descriptor_tree.root);
        let encoded_manifest = manifest.encode().unwrap();
        assert!(encoded_manifest.len() < 1024);
        assert!(!encoded_manifest.contains("\nblock\t"));
        let manifest = PropertySpillManifest::decode(&encoded_manifest).unwrap();
        let limited_reader = PropertySpillReader::open(
            &path,
            manifest.clone(),
            PersistentPropertySpillDescriptorTree::new(
                descriptor_paths.clone(),
                GraphDescriptorTreeBuildConfig::default(),
            ),
            Arc::new(SegmentCache::new(1024)),
            StoreId(8),
            NonZeroU64::new(1).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            limited_reader.get(first),
            Err(PropertySpillError::BlockTooLarge { .. })
        ));
        assert!(!limited_reader.is_poisoned());
        let cache = Arc::new(SegmentCache::new(1024));
        let reader = PropertySpillReader::open(
            &path,
            manifest,
            PersistentPropertySpillDescriptorTree::new(
                descriptor_paths,
                GraphDescriptorTreeBuildConfig::default(),
            ),
            Arc::clone(&cache),
            StoreId(9),
            NonZeroU64::new(2048).unwrap(),
        )
        .unwrap();
        assert_eq!(cache.snapshot().entry_count, 0);
        let first_read = reader.get_with_report(first).unwrap();
        let cold = first_read.report;
        assert_eq!(first_read.value.unwrap().as_ref(), &[1; 32]);
        assert_eq!(cold.descriptors_examined, 1);
        assert!(cold.descriptor_pages_visited > 0);
        assert!(cold.descriptor_storage_bytes_read > 0);
        assert_eq!(cold.blocks_read, 1);
        assert_eq!(cold.cache_misses, 1);
        let second_read = reader.get_with_report(second).unwrap();
        let warm = second_read.report;
        assert_eq!(second_read.value.unwrap().as_ref(), &[2; 48]);
        assert_eq!(warm.descriptors_examined, 1);
        assert!(warm.descriptor_cache_hits > 0);
        assert_eq!(warm.cache_hits, 1);
        assert_eq!(reader.get(third).unwrap().unwrap().as_ref(), &[3; 48]);
        assert!(reader.get(99).unwrap().is_none());
        let scrub = reader.deep_scrub().unwrap();
        assert_eq!(scrub.descriptors_checked, 2);
        assert_eq!(scrub.spill_blocks_checked, 2);
        assert_eq!(scrub.spill_values_checked, 3);
        assert_eq!(scrub.spill_bytes_hashed, reader.manifest().artifact_len);
        assert!(!reader.is_poisoned());

        let mut artifact = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        artifact.seek(SeekFrom::Start(40)).unwrap();
        artifact.write_all(&[0xff]).unwrap();
        artifact.sync_all().unwrap();
        let error = reader
            .deep_scrub()
            .expect_err("deep scrub must detect corruption outside cached reads");
        assert!(error.to_string().contains("checksum mismatch"));
        assert!(reader.is_poisoned());
        assert!(reader
            .get(first)
            .unwrap_err()
            .to_string()
            .contains("poisoned"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn descriptor_tree_entry_codec_is_order_preserving_and_symmetric() {
        let first = PropertySpillBlockDescriptor {
            block_id: BLOCK_ID_BASE,
            offset: 24,
            length: NonZeroU64::new(80).unwrap(),
            content_digest: ContentDigest(7),
            min_spill_id: 0,
            max_spill_id: 1,
            value_count: 2,
        };
        let second = PropertySpillBlockDescriptor {
            block_id: BLOCK_ID_BASE + 1,
            offset: 104,
            length: NonZeroU64::new(48).unwrap(),
            content_digest: ContentDigest(9),
            min_spill_id: 2,
            max_spill_id: 2,
            value_count: 1,
        };
        assert!(first.descriptor_tree_key() < second.descriptor_tree_key());
        for descriptor in [first, second] {
            let key = descriptor.descriptor_tree_key();
            let value = descriptor.encode_descriptor_tree_value().unwrap();
            assert_eq!(
                PropertySpillBlockDescriptor::decode_descriptor_tree_entry(&key, &value).unwrap(),
                descriptor
            );
        }
    }
}
