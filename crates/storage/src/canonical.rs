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
    content_digest, durable_replace_file, wire, ContentDigest, FileSegmentRangeReader,
    GraphDescriptorKind, GraphDescriptorPageError, GraphDescriptorTreeArtifactMetadata,
    GraphDescriptorTreeBuildConfig, GraphDescriptorTreeBuilder, GraphDescriptorTreeError,
    GraphDescriptorTreeGenerationArtifacts, GraphDescriptorTreePaths,
    GraphDescriptorTreeRootReader, ManifestGeneration, NodeId, NodeRecord,
    PreparedGraphDescriptorTree, ProjectedNodeRecord, PropertySpillError, PropertySpillManifest,
    PropertySpillReader, PropertySpillWriteOptions, PropertySpillWriteOutput, PropertySpillWriter,
    RelId, RelRecord, SegmentCache, SegmentRangeRead, SegmentReadError, SegmentReadRange, StoreId,
};
use hawdb_core::{LabelId, RelTypeId, Value};
use hawdb_integrity::{IntegrityHasher, Sha256Digest};
use std::borrow::Borrow;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

const ARTIFACT_HEADER: &[u8; 16] = b"HAWDBCANONICAL01";
const MANIFEST_HEADER_V1: &str = "HAWDB_CANONICAL_MANIFEST_V1";
const SEGMENT_HEADER: &[u8; 8] = b"SKNSEG01";
const ARTIFACT_ID: u64 = 0x534b_4341_4e4f_4e31;
pub(crate) const MAX_VALUE_DEPTH: usize = 32;
const BLOOM_MIN_WORDS: usize = 4;
const BLOOM_MAX_WORDS: usize = 16 * 1024;
const BLOOM_BITS_PER_ITEM: usize = 10;
const BLOOM_HASHES: u8 = 7;
const DESCRIPTOR_VALUE_MAGIC: &[u8; 8] = b"SKCNSDS1";
const DESCRIPTOR_VALUE_VERSION: u16 = 1;
const DESCRIPTOR_VALUE_FIXED_BYTES: usize = 80;
const DESCRIPTOR_SCAN_BATCH: u64 = 256;
pub const CANONICAL_SEGMENT_DESCRIPTOR_ARTIFACT_ID: u64 = 0x534b_4341_4e44_5331;

pub fn canonical_segment_descriptor_page_file(generation: u64) -> String {
    format!("canonical-segment-descriptors-{generation}.pages.hawdb")
}

pub fn canonical_segment_descriptor_root_file(generation: u64) -> String {
    format!("canonical-segment-descriptors-{generation}.root.hawdb")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CanonicalSegmentKind {
    Nodes,
    Relationships,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CanonicalScanControl {
    Continue,
    Stop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CanonicalEndpointDirection {
    Source,
    Target,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalEndpointBloom {
    words: Box<[u64]>,
    hash_count: u8,
}

impl Default for CanonicalEndpointBloom {
    fn default() -> Self {
        Self {
            words: vec![0; BLOOM_MIN_WORDS].into_boxed_slice(),
            hash_count: BLOOM_HASHES,
        }
    }
}

impl CanonicalEndpointBloom {
    fn from_keys(keys: &[u64]) -> Self {
        if keys.is_empty() {
            return Self::default();
        }
        let requested_bits = keys.len().saturating_mul(BLOOM_BITS_PER_ITEM);
        let word_count = requested_bits
            .div_ceil(u64::BITS as usize)
            .clamp(BLOOM_MIN_WORDS, BLOOM_MAX_WORDS);
        let mut bloom = Self {
            words: vec![0; word_count].into_boxed_slice(),
            hash_count: BLOOM_HASHES,
        };
        for key in keys {
            bloom.insert(*key);
        }
        bloom
    }

    fn insert(&mut self, id: u64) {
        for seed in 0..u64::from(self.hash_count) {
            let bit =
                endpoint_bloom_hash(id, seed) as usize % (self.words.len() * u64::BITS as usize);
            self.words[bit / u64::BITS as usize] |= 1u64 << (bit % u64::BITS as usize);
        }
    }

    pub fn might_contain(&self, id: u64) -> bool {
        (0..u64::from(self.hash_count)).all(|seed| {
            let bit =
                endpoint_bloom_hash(id, seed) as usize % (self.words.len() * u64::BITS as usize);
            self.words[bit / u64::BITS as usize] & (1u64 << (bit % u64::BITS as usize)) != 0
        })
    }

    pub fn bit_len(&self) -> usize {
        self.words.len().saturating_mul(u64::BITS as usize)
    }

    pub fn estimated_false_positive_rate(&self) -> f64 {
        let set_bits = self
            .words
            .iter()
            .map(|word| word.count_ones() as usize)
            .sum::<usize>();
        if set_bits == 0 {
            return 0.0;
        }
        (set_bits as f64 / self.bit_len() as f64).powi(i32::from(self.hash_count))
    }

    fn encode_descriptor_bytes(&self) -> Result<Vec<u8>, CanonicalSegmentError> {
        let word_count = u32::try_from(self.words.len()).map_err(|_| {
            CanonicalSegmentError::Corrupt(
                "canonical endpoint bloom word count exceeds u32".to_string(),
            )
        })?;
        let mut encoded = Vec::with_capacity(8usize.saturating_add(self.words.len() * 8));
        encoded.push(self.hash_count);
        encoded.extend_from_slice(&[0u8; 3]);
        encoded.extend_from_slice(&word_count.to_le_bytes());
        for word in &self.words {
            encoded.extend_from_slice(&word.to_le_bytes());
        }
        Ok(encoded)
    }

    fn decode_descriptor_bytes(encoded: &[u8]) -> Result<Self, CanonicalSegmentError> {
        if encoded.len() < 8 || encoded[1..4] != [0u8; 3] {
            return Err(CanonicalSegmentError::Corrupt(
                "canonical descriptor bloom has an invalid header".to_string(),
            ));
        }
        let hash_count = encoded[0];
        let word_count = u32::from_le_bytes(
            encoded[4..8]
                .try_into()
                .expect("canonical descriptor bloom has a fixed header"),
        ) as usize;
        let expected_len = 8usize
            .checked_add(word_count.checked_mul(8).ok_or_else(|| {
                CanonicalSegmentError::Corrupt(
                    "canonical descriptor bloom length overflow".to_string(),
                )
            })?)
            .ok_or_else(|| {
                CanonicalSegmentError::Corrupt(
                    "canonical descriptor bloom length overflow".to_string(),
                )
            })?;
        if hash_count == 0
            || word_count == 0
            || word_count > BLOOM_MAX_WORDS
            || encoded.len() != expected_len
        {
            return Err(CanonicalSegmentError::Corrupt(
                "canonical descriptor bloom exceeds its format bounds".to_string(),
            ));
        }
        let words = encoded[8..]
            .chunks_exact(8)
            .map(|word| u64::from_le_bytes(word.try_into().expect("fixed bloom word")))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(Self { words, hash_count })
    }
}

impl CanonicalSegmentKind {
    const fn tag(self) -> u8 {
        match self {
            Self::Nodes => 1,
            Self::Relationships => 2,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, CanonicalSegmentError> {
        match tag {
            1 => Ok(Self::Nodes),
            2 => Ok(Self::Relationships),
            _ => Err(CanonicalSegmentError::Corrupt(format!(
                "unknown canonical segment kind {tag}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalSegmentDescriptor {
    pub segment_id: u64,
    pub kind: CanonicalSegmentKind,
    pub offset: u64,
    pub length: NonZeroU64,
    pub content_digest: ContentDigest,
    pub min_record_id: u64,
    pub max_record_id: u64,
    pub record_count: u32,
    pub source_endpoint_bloom: CanonicalEndpointBloom,
    pub target_endpoint_bloom: CanonicalEndpointBloom,
    pub node_property_bloom: CanonicalEndpointBloom,
}

impl CanonicalSegmentDescriptor {
    pub fn descriptor_tree_key(&self) -> Vec<u8> {
        let mut key = Vec::with_capacity(17);
        key.push(self.kind.tag());
        key.extend_from_slice(&self.max_record_id.to_be_bytes());
        key.extend_from_slice(&self.segment_id.to_be_bytes());
        key
    }

    pub fn encode_descriptor_tree_value(&self) -> Result<Vec<u8>, CanonicalSegmentError> {
        self.validate_identity()?;
        let source_bloom = self.source_endpoint_bloom.encode_descriptor_bytes()?;
        let target_bloom = self.target_endpoint_bloom.encode_descriptor_bytes()?;
        let property_bloom = self.node_property_bloom.encode_descriptor_bytes()?;
        let source_len = u32_len(source_bloom.len(), "canonical source bloom")?;
        let target_len = u32_len(target_bloom.len(), "canonical target bloom")?;
        let property_len = u32_len(property_bloom.len(), "canonical property bloom")?;
        let capacity = DESCRIPTOR_VALUE_FIXED_BYTES
            .checked_add(source_bloom.len())
            .and_then(|value| value.checked_add(target_bloom.len()))
            .and_then(|value| value.checked_add(property_bloom.len()))
            .ok_or_else(|| {
                CanonicalSegmentError::Corrupt(
                    "canonical descriptor value length overflow".to_string(),
                )
            })?;
        let mut encoded = Vec::with_capacity(capacity);
        encoded.extend_from_slice(DESCRIPTOR_VALUE_MAGIC);
        encoded.extend_from_slice(&DESCRIPTOR_VALUE_VERSION.to_le_bytes());
        encoded.extend_from_slice(&0u16.to_le_bytes());
        encoded.extend_from_slice(&self.segment_id.to_le_bytes());
        encoded.push(self.kind.tag());
        encoded.extend_from_slice(&[0u8; 3]);
        encoded.extend_from_slice(&self.offset.to_le_bytes());
        encoded.extend_from_slice(&self.length.get().to_le_bytes());
        encoded.extend_from_slice(&self.content_digest.0.to_le_bytes());
        encoded.extend_from_slice(&self.min_record_id.to_le_bytes());
        encoded.extend_from_slice(&self.max_record_id.to_le_bytes());
        encoded.extend_from_slice(&self.record_count.to_le_bytes());
        encoded.extend_from_slice(&source_len.to_le_bytes());
        encoded.extend_from_slice(&target_len.to_le_bytes());
        encoded.extend_from_slice(&property_len.to_le_bytes());
        debug_assert_eq!(encoded.len(), DESCRIPTOR_VALUE_FIXED_BYTES);
        encoded.extend_from_slice(&source_bloom);
        encoded.extend_from_slice(&target_bloom);
        encoded.extend_from_slice(&property_bloom);
        Ok(encoded)
    }

    pub fn decode_descriptor_tree_entry(
        key: &[u8],
        encoded: &[u8],
    ) -> Result<Self, CanonicalSegmentError> {
        if key.len() != 17
            || encoded.len() < DESCRIPTOR_VALUE_FIXED_BYTES
            || &encoded[..8] != DESCRIPTOR_VALUE_MAGIC
        {
            return Err(CanonicalSegmentError::Corrupt(
                "canonical descriptor tree entry has an invalid header or length".to_string(),
            ));
        }
        let version = u16::from_le_bytes(encoded[8..10].try_into().expect("fixed version"));
        let flags = u16::from_le_bytes(encoded[10..12].try_into().expect("fixed flags"));
        if version != DESCRIPTOR_VALUE_VERSION || flags != 0 || encoded[21..24] != [0u8; 3] {
            return Err(CanonicalSegmentError::Corrupt(format!(
                "canonical descriptor has unsupported version {version}, flags {flags}, or reserved fields"
            )));
        }
        let source_len = read_u32_at(encoded, 68) as usize;
        let target_len = read_u32_at(encoded, 72) as usize;
        let property_len = read_u32_at(encoded, 76) as usize;
        let source_end = DESCRIPTOR_VALUE_FIXED_BYTES
            .checked_add(source_len)
            .ok_or_else(descriptor_length_overflow)?;
        let target_end = source_end
            .checked_add(target_len)
            .ok_or_else(descriptor_length_overflow)?;
        let property_end = target_end
            .checked_add(property_len)
            .ok_or_else(descriptor_length_overflow)?;
        if property_end != encoded.len() {
            return Err(CanonicalSegmentError::Corrupt(
                "canonical descriptor bloom lengths do not match its value".to_string(),
            ));
        }
        let descriptor = Self {
            segment_id: read_u64_at(encoded, 12),
            kind: CanonicalSegmentKind::from_tag(encoded[20])?,
            offset: read_u64_at(encoded, 24),
            length: NonZeroU64::new(read_u64_at(encoded, 32)).ok_or_else(|| {
                CanonicalSegmentError::Corrupt(
                    "canonical descriptor segment length is zero".to_string(),
                )
            })?,
            content_digest: ContentDigest(read_u64_at(encoded, 40)),
            min_record_id: read_u64_at(encoded, 48),
            max_record_id: read_u64_at(encoded, 56),
            record_count: read_u32_at(encoded, 64),
            source_endpoint_bloom: CanonicalEndpointBloom::decode_descriptor_bytes(
                &encoded[DESCRIPTOR_VALUE_FIXED_BYTES..source_end],
            )?,
            target_endpoint_bloom: CanonicalEndpointBloom::decode_descriptor_bytes(
                &encoded[source_end..target_end],
            )?,
            node_property_bloom: CanonicalEndpointBloom::decode_descriptor_bytes(
                &encoded[target_end..property_end],
            )?,
        };
        descriptor.validate_identity()?;
        if key != descriptor.descriptor_tree_key() {
            return Err(CanonicalSegmentError::Corrupt(
                "canonical descriptor tree key does not match its value".to_string(),
            ));
        }
        Ok(descriptor)
    }

    fn validate_identity(&self) -> Result<(), CanonicalSegmentError> {
        if self.segment_id == 0
            || self.record_count == 0
            || self.min_record_id > self.max_record_id
            || self.content_digest.0 > u64::from(u32::MAX)
            || self.offset.checked_add(self.length.get()).is_none()
        {
            return Err(CanonicalSegmentError::Corrupt(format!(
                "canonical segment {} has invalid descriptor identity",
                self.segment_id
            )));
        }
        Ok(())
    }
}

fn descriptor_length_overflow() -> CanonicalSegmentError {
    CanonicalSegmentError::Corrupt("canonical descriptor value length overflow".to_string())
}

#[derive(Debug, Clone)]
pub struct PersistentCanonicalSegmentDescriptorTree {
    paths: GraphDescriptorTreePaths,
    config: GraphDescriptorTreeBuildConfig,
}

impl PersistentCanonicalSegmentDescriptorTree {
    pub const fn new(
        paths: GraphDescriptorTreePaths,
        config: GraphDescriptorTreeBuildConfig,
    ) -> Self {
        Self { paths, config }
    }

    pub fn for_artifact(path: &Path, generation: ManifestGeneration) -> Self {
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        let durable_name = format!("canonical.{}.hawdb", generation.0);
        let (page_artifact, root_manifest) = if path
            .file_name()
            .is_some_and(|name| name == durable_name.as_str())
        {
            (
                parent.join(canonical_segment_descriptor_page_file(generation.0)),
                parent.join(canonical_segment_descriptor_root_file(generation.0)),
            )
        } else {
            (
                path.with_extension("descriptors.pages.hawdb"),
                path.with_extension("descriptors.root.hawdb"),
            )
        };
        Self::new(
            GraphDescriptorTreePaths::new(page_artifact, root_manifest),
            GraphDescriptorTreeBuildConfig::default(),
        )
    }

    fn into_parts(self) -> (GraphDescriptorTreePaths, GraphDescriptorTreeBuildConfig) {
        (self.paths, self.config)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalSegmentManifest {
    pub generation: ManifestGeneration,
    pub source_commit_epoch: u64,
    pub artifact_id: u64,
    pub artifact_len: u64,
    pub artifact_digest: ContentDigest,
    pub artifact_sha256: Sha256Digest,
    pub node_count: u64,
    pub relationship_count: u64,
    pub segment_count: u64,
    pub node_segment_count: u64,
    pub relationship_segment_count: u64,
    pub descriptor_root_artifact: GraphDescriptorTreeArtifactMetadata,
    /// The record property key table. Record payloads encode `u32` ids into
    /// this manifest-owned table; no inline-key compatibility layout exists.
    pub property_keys: Vec<String>,
}

impl CanonicalSegmentManifest {
    pub const fn descriptor_generation_artifacts(&self) -> GraphDescriptorTreeGenerationArtifacts {
        GraphDescriptorTreeGenerationArtifacts {
            kind: GraphDescriptorKind::CanonicalSegment,
            generation: self.generation.0,
            source_commit_epoch: self.source_commit_epoch,
            root_artifact: self.descriptor_root_artifact,
        }
    }

    pub const fn segment_count_for_kind(&self, kind: CanonicalSegmentKind) -> u64 {
        match kind {
            CanonicalSegmentKind::Nodes => self.node_segment_count,
            CanonicalSegmentKind::Relationships => self.relationship_segment_count,
        }
    }

    pub fn validate(&self) -> Result<(), CanonicalSegmentError> {
        if self.artifact_id != ARTIFACT_ID {
            return Err(CanonicalSegmentError::Corrupt(
                "canonical manifest has an unsupported artifact id".to_string(),
            ));
        }
        u32_len(self.property_keys.len(), "canonical property key table")?;
        let declared_segment_count = self
            .node_segment_count
            .checked_add(self.relationship_segment_count)
            .ok_or_else(|| {
                CanonicalSegmentError::Corrupt(
                    "canonical manifest segment count overflows u64".to_string(),
                )
            })?;
        if self.artifact_len < ARTIFACT_HEADER.len() as u64 + 8
            || self.segment_count != declared_segment_count
            || self.node_segment_count > self.node_count
            || self.relationship_segment_count > self.relationship_count
            || (self.node_count == 0) != (self.node_segment_count == 0)
            || (self.relationship_count == 0) != (self.relationship_segment_count == 0)
            || self.descriptor_root_artifact.encoded_len == 0
        {
            return Err(CanonicalSegmentError::Corrupt(
                "canonical manifest descriptor count or root binding is inconsistent".to_string(),
            ));
        }
        let mut seen = BTreeSet::new();
        for key in &self.property_keys {
            if !seen.insert(key.as_str()) {
                return Err(CanonicalSegmentError::Corrupt(
                    "canonical manifest property keys are not unique".to_string(),
                ));
            }
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<String, CanonicalSegmentError> {
        self.validate()?;
        let mut body = format!(
            "{MANIFEST_HEADER_V1}\nrecord_layout\tproperty_key_ids\ngeneration\t{}\nsource_commit_epoch\t{}\nartifact_id\t{}\nartifact_len\t{}\nartifact_digest\t{}\nartifact_sha256\t{}\nnode_count\t{}\nrelationship_count\t{}\nsegment_count\t{}\nnode_segment_count\t{}\nrelationship_segment_count\t{}\ndescriptor_root_len\t{}\ndescriptor_root_crc32c\t{}\ndescriptor_root_sha256\t{}\n",
            self.generation.0,
            self.source_commit_epoch,
            self.artifact_id,
            self.artifact_len,
            self.artifact_digest.0,
            self.artifact_sha256,
            self.node_count,
            self.relationship_count,
            self.segment_count,
            self.node_segment_count,
            self.relationship_segment_count,
            self.descriptor_root_artifact.encoded_len,
            self.descriptor_root_artifact.encoded_crc32c,
            self.descriptor_root_artifact.encoded_sha256
        );
        for (id, key) in self.property_keys.iter().enumerate() {
            body.push_str(&format!(
                "property_key\t{id}\t{}\n",
                encode_property_key_hex(key)
            ));
        }
        let checksum = content_digest(body.as_bytes()).0;
        Ok(format!("{body}checksum\t{checksum}\n"))
    }

    pub fn decode(text: &str) -> Result<Self, CanonicalSegmentError> {
        let marker = "checksum\t";
        let checksum_offset = text.rfind(marker).ok_or_else(|| {
            CanonicalSegmentError::Corrupt("canonical manifest is missing its checksum".to_string())
        })?;
        let body = &text[..checksum_offset];
        let checksum_line = text[checksum_offset..].trim_end();
        if checksum_line.contains('\n') {
            return Err(CanonicalSegmentError::Corrupt(
                "canonical manifest has data after its checksum".to_string(),
            ));
        }
        let expected = parse_u64(
            checksum_line.strip_prefix(marker).unwrap_or_default(),
            "canonical manifest checksum",
        )?;
        let actual = content_digest(body.as_bytes()).0;
        if actual != expected {
            return Err(CanonicalSegmentError::Corrupt(format!(
                "canonical manifest checksum mismatch: expected {expected}, got {actual}"
            )));
        }

        let mut generation = None;
        let mut source_commit_epoch = None;
        let mut artifact_id = None;
        let mut artifact_len = None;
        let mut artifact_digest = None;
        let mut artifact_sha256 = None;
        let mut node_count = None;
        let mut relationship_count = None;
        let mut segment_count = None;
        let mut node_segment_count = None;
        let mut relationship_segment_count = None;
        let mut descriptor_root_len = None;
        let mut descriptor_root_crc32c = None;
        let mut descriptor_root_sha256 = None;
        let mut property_keys = Vec::new();
        let mut saw_header = false;
        let mut saw_record_layout = false;
        for line in body.lines() {
            if line == MANIFEST_HEADER_V1 {
                if saw_header {
                    return Err(CanonicalSegmentError::Corrupt(
                        "canonical manifest has a duplicate format header".to_string(),
                    ));
                }
                saw_header = true;
                continue;
            }
            let fields = line.split('\t').collect::<Vec<_>>();
            match fields.as_slice() {
                ["record_layout", "property_key_ids"] => {
                    if saw_record_layout {
                        return Err(CanonicalSegmentError::Corrupt(
                            "canonical manifest has a duplicate record layout".to_string(),
                        ));
                    }
                    saw_record_layout = true;
                }
                ["generation", value] => set_once(
                    &mut generation,
                    parse_u64(value, "generation")?,
                    "generation",
                )?,
                ["source_commit_epoch", value] => set_once(
                    &mut source_commit_epoch,
                    parse_u64(value, "source commit epoch")?,
                    "source commit epoch",
                )?,
                ["artifact_id", value] => set_once(
                    &mut artifact_id,
                    parse_u64(value, "artifact id")?,
                    "artifact id",
                )?,
                ["artifact_len", value] => set_once(
                    &mut artifact_len,
                    parse_u64(value, "artifact length")?,
                    "artifact length",
                )?,
                ["artifact_digest", value] => set_once(
                    &mut artifact_digest,
                    parse_u64(value, "artifact digest")?,
                    "artifact digest",
                )?,
                ["artifact_sha256", value] => set_once(
                    &mut artifact_sha256,
                    value.parse().map_err(|error| {
                        CanonicalSegmentError::Corrupt(format!(
                            "invalid artifact SHA-256 digest: {error}"
                        ))
                    })?,
                    "artifact SHA-256 digest",
                )?,
                ["node_count", value] => set_once(
                    &mut node_count,
                    parse_u64(value, "node count")?,
                    "node count",
                )?,
                ["relationship_count", value] => set_once(
                    &mut relationship_count,
                    parse_u64(value, "relationship count")?,
                    "relationship count",
                )?,
                ["segment_count", value] => set_once(
                    &mut segment_count,
                    parse_u64(value, "segment count")?,
                    "segment count",
                )?,
                ["node_segment_count", value] => set_once(
                    &mut node_segment_count,
                    parse_u64(value, "node segment count")?,
                    "node segment count",
                )?,
                ["relationship_segment_count", value] => set_once(
                    &mut relationship_segment_count,
                    parse_u64(value, "relationship segment count")?,
                    "relationship segment count",
                )?,
                ["descriptor_root_len", value] => set_once(
                    &mut descriptor_root_len,
                    parse_u64(value, "descriptor root length")?,
                    "descriptor root length",
                )?,
                ["descriptor_root_crc32c", value] => set_once(
                    &mut descriptor_root_crc32c,
                    parse_u32(value, "descriptor root CRC32C")?,
                    "descriptor root CRC32C",
                )?,
                ["descriptor_root_sha256", value] => set_once(
                    &mut descriptor_root_sha256,
                    value.parse().map_err(|error| {
                        CanonicalSegmentError::Corrupt(format!(
                            "invalid descriptor root SHA-256 digest: {error}"
                        ))
                    })?,
                    "descriptor root SHA-256 digest",
                )?,
                ["property_key", id, key] => {
                    if parse_u32(id, "property key id")? as usize != property_keys.len() {
                        return Err(CanonicalSegmentError::Corrupt(
                            "canonical manifest property key ids are not contiguous".to_string(),
                        ));
                    }
                    property_keys.push(decode_property_key_hex(key)?);
                }
                [""] => {}
                _ => {
                    return Err(CanonicalSegmentError::Corrupt(format!(
                        "invalid canonical manifest line: {line}"
                    )));
                }
            }
        }
        if !saw_header {
            return Err(CanonicalSegmentError::Corrupt(
                "canonical manifest is missing its format header".to_string(),
            ));
        }
        if !saw_record_layout {
            return Err(CanonicalSegmentError::Corrupt(
                "canonical manifest is missing its property-key-id record layout".to_string(),
            ));
        }
        let manifest = Self {
            generation: ManifestGeneration(required(generation, "generation")?),
            source_commit_epoch: required(source_commit_epoch, "source commit epoch")?,
            artifact_id: required(artifact_id, "artifact id")?,
            artifact_len: required(artifact_len, "artifact length")?,
            artifact_digest: ContentDigest(required(artifact_digest, "artifact digest")?),
            artifact_sha256: required(artifact_sha256, "artifact SHA-256 digest")?,
            node_count: required(node_count, "node count")?,
            relationship_count: required(relationship_count, "relationship count")?,
            segment_count: required(segment_count, "segment count")?,
            node_segment_count: required(node_segment_count, "node segment count")?,
            relationship_segment_count: required(
                relationship_segment_count,
                "relationship segment count",
            )?,
            descriptor_root_artifact: GraphDescriptorTreeArtifactMetadata {
                encoded_len: required(descriptor_root_len, "descriptor root length")?,
                encoded_crc32c: required(descriptor_root_crc32c, "descriptor root CRC32C")?,
                encoded_sha256: required(descriptor_root_sha256, "descriptor root SHA-256 digest")?,
            },
            property_keys,
        };
        manifest.validate()?;
        Ok(manifest)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CanonicalSegmentConfig {
    pub target_segment_bytes: NonZeroU64,
    pub max_record_bytes: NonZeroU64,
}

impl Default for CanonicalSegmentConfig {
    fn default() -> Self {
        Self {
            target_segment_bytes: NonZeroU64::new(4 * 1024 * 1024)
                .expect("default canonical segment size is non-zero"),
            max_record_bytes: NonZeroU64::new(16 * 1024 * 1024)
                .expect("default canonical record size is non-zero"),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CanonicalReadReport {
    pub generation: u64,
    pub descriptor_pages_visited: u64,
    pub descriptor_page_bytes_decoded: u64,
    pub descriptor_storage_bytes_read: u64,
    pub descriptors_examined: u64,
    pub descriptor_cache_hits: u64,
    pub descriptor_cache_misses: u64,
    pub descriptor_cache_admission_rejections: u64,
    pub segments_considered: u64,
    pub segments_pruned: u64,
    pub segments_read: u64,
    pub bytes_read: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub records_decoded: u64,
    pub peak_segment_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CanonicalSegmentScrubReport {
    pub descriptor_pages_checked: u64,
    pub descriptors_checked: u64,
    pub descriptor_bytes_checked: u64,
    pub segments_checked: u64,
    pub records_checked: u64,
    pub canonical_bytes_hashed: u64,
}

#[derive(Debug)]
pub enum CanonicalSegmentError {
    Io(std::io::Error),
    Read(SegmentReadError),
    DescriptorTree(GraphDescriptorTreeError),
    PropertySpill(PropertySpillError),
    RecordTooLarge { record_bytes: u64, max_bytes: u64 },
    SegmentTooLarge { segment_bytes: u64, max_bytes: u64 },
    Source(String),
    Corrupt(String),
}

impl Display for CanonicalSegmentError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => Display::fmt(error, formatter),
            Self::Read(error) => Display::fmt(error, formatter),
            Self::DescriptorTree(error) => Display::fmt(error, formatter),
            Self::PropertySpill(error) => Display::fmt(error, formatter),
            Self::RecordTooLarge {
                record_bytes,
                max_bytes,
            } => write!(
                formatter,
                "canonical record uses {record_bytes} bytes, exceeding {max_bytes}"
            ),
            Self::SegmentTooLarge {
                segment_bytes,
                max_bytes,
            } => write!(
                formatter,
                "canonical segment uses {segment_bytes} bytes, exceeding {max_bytes}"
            ),
            Self::Source(message) | Self::Corrupt(message) => formatter.write_str(message),
        }
    }
}

impl Error for CanonicalSegmentError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Read(error) => Some(error),
            Self::DescriptorTree(error) => Some(error),
            Self::PropertySpill(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for CanonicalSegmentError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<SegmentReadError> for CanonicalSegmentError {
    fn from(error: SegmentReadError) -> Self {
        Self::Read(error)
    }
}

impl From<GraphDescriptorTreeError> for CanonicalSegmentError {
    fn from(error: GraphDescriptorTreeError) -> Self {
        Self::DescriptorTree(error)
    }
}

impl From<PropertySpillError> for CanonicalSegmentError {
    fn from(error: PropertySpillError) -> Self {
        Self::PropertySpill(error)
    }
}

pub struct CanonicalSegmentWriter {
    config: CanonicalSegmentConfig,
}

impl CanonicalSegmentWriter {
    pub const fn new(config: CanonicalSegmentConfig) -> Self {
        Self { config }
    }

    pub fn write<N, R>(
        &self,
        path: &Path,
        generation: ManifestGeneration,
        nodes: N,
        relationships: R,
    ) -> Result<CanonicalSegmentManifest, CanonicalSegmentError>
    where
        N: IntoIterator,
        N::Item: Borrow<NodeRecord>,
        R: IntoIterator,
        R::Item: Borrow<RelRecord>,
    {
        self.write_fallible(
            path,
            generation,
            nodes.into_iter().map(|node| Ok(node.borrow().clone())),
            relationships
                .into_iter()
                .map(|relationship| Ok(relationship.borrow().clone())),
        )
    }

    pub fn write_fallible<N, R>(
        &self,
        path: &Path,
        generation: ManifestGeneration,
        nodes: N,
        relationships: R,
    ) -> Result<CanonicalSegmentManifest, CanonicalSegmentError>
    where
        N: IntoIterator<Item = Result<NodeRecord, CanonicalSegmentError>>,
        R: IntoIterator<Item = Result<RelRecord, CanonicalSegmentError>>,
    {
        let tmp_path = path.with_extension("hawdb.tmp");
        let source_commit_epoch = generation.0;
        let descriptor_tree =
            create_canonical_descriptor_tree(path, generation, source_commit_epoch)?;
        let result = self.write_inner(
            &tmp_path,
            CanonicalWriteIdentity {
                generation,
                source_commit_epoch,
            },
            nodes,
            relationships,
            None,
            descriptor_tree,
        );
        let prepared = match result {
            Ok(prepared) => prepared,
            Err(error) => {
                let _ = fs::remove_file(&tmp_path);
                return Err(error);
            }
        };
        durable_replace_file(&tmp_path, path)?;
        prepared.publish_descriptor_tree()
    }

    pub fn write_fallible_with_property_spills<N, R>(
        &self,
        path: &Path,
        generation: ManifestGeneration,
        nodes: N,
        relationships: R,
        property_spill: PropertySpillWriteOptions<'_>,
    ) -> Result<(CanonicalSegmentManifest, PropertySpillWriteOutput), CanonicalSegmentError>
    where
        N: IntoIterator<Item = Result<NodeRecord, CanonicalSegmentError>>,
        R: IntoIterator<Item = Result<RelRecord, CanonicalSegmentError>>,
    {
        let tmp_path = path.with_extension("hawdb.tmp");
        let spill_tmp_path = property_spill.artifact_path.with_extension("hawdb.tmp");
        let source_commit_epoch = property_spill.source_commit_epoch;
        let descriptor_tree =
            create_canonical_descriptor_tree(path, generation, source_commit_epoch)?;
        let mut spill_writer = PropertySpillWriter::create(
            &spill_tmp_path,
            generation,
            property_spill.source_commit_epoch,
            property_spill.config,
            property_spill.descriptor_tree,
        )?;
        let result = self.write_inner(
            &tmp_path,
            CanonicalWriteIdentity {
                generation,
                source_commit_epoch,
            },
            nodes,
            relationships,
            Some(&mut spill_writer),
            descriptor_tree,
        );
        let prepared_canonical = match result {
            Ok(prepared) => prepared,
            Err(error) => {
                drop(spill_writer);
                let _ = fs::remove_file(&tmp_path);
                let _ = fs::remove_file(&spill_tmp_path);
                return Err(error);
            }
        };
        let prepared_spill = match spill_writer.finish() {
            Ok(prepared) => prepared,
            Err(error) => {
                let _ = fs::remove_file(&tmp_path);
                let _ = fs::remove_file(&spill_tmp_path);
                return Err(error.into());
            }
        };
        let spill_output = match prepared_spill.publish(property_spill.artifact_path) {
            Ok(output) => output,
            Err(error) => {
                let _ = fs::remove_file(&tmp_path);
                return Err(error.into());
            }
        };
        durable_replace_file(&tmp_path, path)?;
        let canonical_manifest = prepared_canonical.publish_descriptor_tree()?;
        Ok((canonical_manifest, spill_output))
    }

    fn write_inner<N, R>(
        &self,
        path: &Path,
        identity: CanonicalWriteIdentity,
        nodes: N,
        relationships: R,
        mut property_spills: Option<&mut PropertySpillWriter>,
        mut descriptor_tree: GraphDescriptorTreeBuilder,
    ) -> Result<PreparedCanonicalSegmentArtifact, CanonicalSegmentError>
    where
        N: IntoIterator<Item = Result<NodeRecord, CanonicalSegmentError>>,
        R: IntoIterator<Item = Result<RelRecord, CanonicalSegmentError>>,
    {
        let CanonicalWriteIdentity {
            generation,
            source_commit_epoch,
        } = identity;
        let mut file = File::create(path)?;
        let mut artifact_digest = IntegrityHasher::new();
        write_hashed(&mut file, &mut artifact_digest, ARTIFACT_HEADER)?;
        write_hashed(&mut file, &mut artifact_digest, &generation.0.to_le_bytes())?;
        let mut artifact_len = ARTIFACT_HEADER.len() as u64 + 8;
        let mut segment_id = 1u64;
        let mut node_segment_count = 0u64;
        let mut relationship_segment_count = 0u64;
        let mut node_count = 0u64;
        let mut relationship_count = 0u64;
        let mut property_keys = PropertyKeyDictionary::default();

        let mut accumulator = SegmentAccumulator::new(
            CanonicalSegmentKind::Nodes,
            generation,
            segment_id,
            self.config,
        );
        for node in nodes {
            let node = node?;
            let payload = encode_node_with_property_spills(
                &node,
                property_spills.as_deref_mut(),
                Some(&mut property_keys),
            )?;
            if accumulator.would_exceed(node.id.0, payload.len()) && !accumulator.is_empty() {
                let descriptor =
                    accumulator.flush(&mut file, &mut artifact_digest, artifact_len)?;
                artifact_len = artifact_len.saturating_add(descriptor.length.get());
                node_count = node_count.saturating_add(u64::from(descriptor.record_count));
                descriptor_tree.push(
                    descriptor.descriptor_tree_key(),
                    descriptor.encode_descriptor_tree_value()?,
                )?;
                node_segment_count = node_segment_count.checked_add(1).ok_or_else(|| {
                    CanonicalSegmentError::Corrupt(
                        "canonical node segment count overflow".to_string(),
                    )
                })?;
                segment_id = segment_id.saturating_add(1);
                accumulator = SegmentAccumulator::new(
                    CanonicalSegmentKind::Nodes,
                    generation,
                    segment_id,
                    self.config,
                );
            }
            accumulator.add_node_properties(&node)?;
            accumulator.push(node.id.0, &payload, None)?;
        }
        if !accumulator.is_empty() {
            let descriptor = accumulator.flush(&mut file, &mut artifact_digest, artifact_len)?;
            artifact_len = artifact_len.saturating_add(descriptor.length.get());
            node_count = node_count.saturating_add(u64::from(descriptor.record_count));
            descriptor_tree.push(
                descriptor.descriptor_tree_key(),
                descriptor.encode_descriptor_tree_value()?,
            )?;
            node_segment_count = node_segment_count.checked_add(1).ok_or_else(|| {
                CanonicalSegmentError::Corrupt("canonical node segment count overflow".to_string())
            })?;
            segment_id = segment_id.saturating_add(1);
        }

        let mut accumulator = SegmentAccumulator::new(
            CanonicalSegmentKind::Relationships,
            generation,
            segment_id,
            self.config,
        );
        for relationship in relationships {
            let relationship = relationship?;
            let payload = encode_relationship_with_property_spills(
                &relationship,
                property_spills.as_deref_mut(),
                Some(&mut property_keys),
            )?;
            if accumulator.would_exceed(relationship.id.0, payload.len()) && !accumulator.is_empty()
            {
                let descriptor =
                    accumulator.flush(&mut file, &mut artifact_digest, artifact_len)?;
                artifact_len = artifact_len.saturating_add(descriptor.length.get());
                relationship_count =
                    relationship_count.saturating_add(u64::from(descriptor.record_count));
                descriptor_tree.push(
                    descriptor.descriptor_tree_key(),
                    descriptor.encode_descriptor_tree_value()?,
                )?;
                relationship_segment_count =
                    relationship_segment_count.checked_add(1).ok_or_else(|| {
                        CanonicalSegmentError::Corrupt(
                            "canonical relationship segment count overflow".to_string(),
                        )
                    })?;
                segment_id = segment_id.saturating_add(1);
                accumulator = SegmentAccumulator::new(
                    CanonicalSegmentKind::Relationships,
                    generation,
                    segment_id,
                    self.config,
                );
            }
            accumulator.push(
                relationship.id.0,
                &payload,
                Some((relationship.source.0, relationship.target.0)),
            )?;
        }
        if !accumulator.is_empty() {
            let descriptor = accumulator.flush(&mut file, &mut artifact_digest, artifact_len)?;
            artifact_len = artifact_len.saturating_add(descriptor.length.get());
            relationship_count =
                relationship_count.saturating_add(u64::from(descriptor.record_count));
            descriptor_tree.push(
                descriptor.descriptor_tree_key(),
                descriptor.encode_descriptor_tree_value()?,
            )?;
            relationship_segment_count =
                relationship_segment_count.checked_add(1).ok_or_else(|| {
                    CanonicalSegmentError::Corrupt(
                        "canonical relationship segment count overflow".to_string(),
                    )
                })?;
        }
        file.sync_all()?;
        let artifact_integrity = artifact_digest.finish();
        let descriptor_tree = descriptor_tree.finish()?;
        Ok(PreparedCanonicalSegmentArtifact {
            generation,
            source_commit_epoch,
            artifact_id: ARTIFACT_ID,
            artifact_len,
            artifact_digest: ContentDigest(artifact_integrity.crc32c.as_u64()),
            artifact_sha256: artifact_integrity.sha256,
            node_count,
            relationship_count,
            node_segment_count,
            relationship_segment_count,
            property_keys: property_keys.into_keys(),
            descriptor_tree,
        })
    }
}

#[derive(Clone, Copy)]
struct CanonicalWriteIdentity {
    generation: ManifestGeneration,
    source_commit_epoch: u64,
}

fn create_canonical_descriptor_tree(
    path: &Path,
    generation: ManifestGeneration,
    source_commit_epoch: u64,
) -> Result<GraphDescriptorTreeBuilder, CanonicalSegmentError> {
    let descriptor_tree = PersistentCanonicalSegmentDescriptorTree::for_artifact(path, generation);
    let (paths, config) = descriptor_tree.into_parts();
    GraphDescriptorTreeBuilder::create(
        paths,
        GraphDescriptorKind::CanonicalSegment,
        generation.0,
        source_commit_epoch,
        CANONICAL_SEGMENT_DESCRIPTOR_ARTIFACT_ID,
        config,
    )
    .map_err(Into::into)
}

struct PreparedCanonicalSegmentArtifact {
    generation: ManifestGeneration,
    source_commit_epoch: u64,
    artifact_id: u64,
    artifact_len: u64,
    artifact_digest: ContentDigest,
    artifact_sha256: Sha256Digest,
    node_count: u64,
    relationship_count: u64,
    node_segment_count: u64,
    relationship_segment_count: u64,
    property_keys: Vec<String>,
    descriptor_tree: PreparedGraphDescriptorTree,
}

impl PreparedCanonicalSegmentArtifact {
    fn publish_descriptor_tree(self) -> Result<CanonicalSegmentManifest, CanonicalSegmentError> {
        let Self {
            generation,
            source_commit_epoch,
            artifact_id,
            artifact_len,
            artifact_digest,
            artifact_sha256,
            node_count,
            relationship_count,
            node_segment_count,
            relationship_segment_count,
            property_keys,
            descriptor_tree,
        } = self;
        let segment_count = node_segment_count
            .checked_add(relationship_segment_count)
            .ok_or_else(|| {
                CanonicalSegmentError::Corrupt(
                    "canonical descriptor count overflows u64".to_string(),
                )
            })?;
        let descriptor_tree = descriptor_tree.publish()?;
        if descriptor_tree.root.kind != GraphDescriptorKind::CanonicalSegment
            || descriptor_tree.root.generation != generation.0
            || descriptor_tree.root.source_commit_epoch != source_commit_epoch
            || descriptor_tree.root.descriptor_count != segment_count
        {
            return Err(CanonicalSegmentError::Corrupt(
                "canonical descriptor root does not match its data artifact".to_string(),
            ));
        }
        let manifest = CanonicalSegmentManifest {
            generation,
            source_commit_epoch,
            artifact_id,
            artifact_len,
            artifact_digest,
            artifact_sha256,
            node_count,
            relationship_count,
            segment_count,
            node_segment_count,
            relationship_segment_count,
            descriptor_root_artifact: descriptor_tree.root_artifact,
            property_keys,
        };
        manifest.validate()?;
        Ok(manifest)
    }
}

struct SegmentAccumulator {
    kind: CanonicalSegmentKind,
    generation: ManifestGeneration,
    segment_id: u64,
    config: CanonicalSegmentConfig,
    records: Vec<u8>,
    min_record_id: Option<u64>,
    max_record_id: u64,
    record_count: u32,
    source_endpoint_keys: Vec<u64>,
    target_endpoint_keys: Vec<u64>,
    node_property_keys: Vec<u64>,
}

impl SegmentAccumulator {
    fn new(
        kind: CanonicalSegmentKind,
        generation: ManifestGeneration,
        segment_id: u64,
        config: CanonicalSegmentConfig,
    ) -> Self {
        Self {
            kind,
            generation,
            segment_id,
            config,
            records: Vec::new(),
            min_record_id: None,
            max_record_id: 0,
            record_count: 0,
            source_endpoint_keys: Vec::new(),
            target_endpoint_keys: Vec::new(),
            node_property_keys: Vec::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.record_count == 0
    }

    fn would_exceed(&self, _record_id: u64, payload_bytes: usize) -> bool {
        segment_header_len()
            .saturating_add(self.records.len())
            .saturating_add(8 + 4)
            .saturating_add(payload_bytes)
            > usize::try_from(self.config.target_segment_bytes.get()).unwrap_or(usize::MAX)
    }

    fn push(
        &mut self,
        record_id: u64,
        payload: &[u8],
        endpoints: Option<(u64, u64)>,
    ) -> Result<(), CanonicalSegmentError> {
        let record_bytes = 8u64.saturating_add(4).saturating_add(payload.len() as u64);
        if record_bytes > self.config.max_record_bytes.get() {
            return Err(CanonicalSegmentError::RecordTooLarge {
                record_bytes,
                max_bytes: self.config.max_record_bytes.get(),
            });
        }
        self.records.extend_from_slice(&record_id.to_le_bytes());
        self.records
            .extend_from_slice(&u32_len(payload.len(), "canonical record")?.to_le_bytes());
        self.records.extend_from_slice(payload);
        self.min_record_id.get_or_insert(record_id);
        if record_id < self.max_record_id && self.record_count > 0 {
            return Err(CanonicalSegmentError::Corrupt(
                "canonical records must be ordered by id".to_string(),
            ));
        }
        self.max_record_id = record_id;
        if let Some((source, target)) = endpoints {
            self.source_endpoint_keys.push(source);
            self.target_endpoint_keys.push(target);
        }
        self.record_count = self.record_count.checked_add(1).ok_or_else(|| {
            CanonicalSegmentError::Corrupt("canonical segment record count overflow".to_string())
        })?;
        Ok(())
    }

    fn add_node_properties(&mut self, node: &NodeRecord) -> Result<(), CanonicalSegmentError> {
        for label in &node.labels {
            for (property, value) in &node.properties {
                self.node_property_keys
                    .push(node_property_bloom_key(*label, property, value)?);
            }
        }
        Ok(())
    }

    fn flush(
        self,
        file: &mut File,
        artifact_digest: &mut IntegrityHasher,
        offset: u64,
    ) -> Result<CanonicalSegmentDescriptor, CanonicalSegmentError> {
        let mut bytes = Vec::with_capacity(segment_header_len().saturating_add(self.records.len()));
        bytes.extend_from_slice(SEGMENT_HEADER);
        bytes.push(self.kind.tag());
        bytes.extend_from_slice(&self.generation.0.to_le_bytes());
        bytes.extend_from_slice(&self.segment_id.to_le_bytes());
        bytes.extend_from_slice(&self.record_count.to_le_bytes());
        bytes.extend_from_slice(&self.records);
        let length = NonZeroU64::new(bytes.len() as u64).expect("segment bytes are non-zero");
        let hard_max = self.config.target_segment_bytes.get().max(
            self.config
                .max_record_bytes
                .get()
                .saturating_add(segment_header_len() as u64),
        );
        if length.get() > hard_max {
            return Err(CanonicalSegmentError::SegmentTooLarge {
                segment_bytes: length.get(),
                max_bytes: hard_max,
            });
        }
        let digest = content_digest(&bytes);
        write_hashed(file, artifact_digest, &bytes)?;
        Ok(CanonicalSegmentDescriptor {
            segment_id: self.segment_id,
            kind: self.kind,
            offset,
            length,
            content_digest: digest,
            min_record_id: self.min_record_id.expect("flushed segment is non-empty"),
            max_record_id: self.max_record_id,
            record_count: self.record_count,
            source_endpoint_bloom: CanonicalEndpointBloom::from_keys(&self.source_endpoint_keys),
            target_endpoint_bloom: CanonicalEndpointBloom::from_keys(&self.target_endpoint_keys),
            node_property_bloom: CanonicalEndpointBloom::from_keys(&self.node_property_keys),
        })
    }
}

type CanonicalDescriptorSelection = Option<(Vec<u8>, CanonicalSegmentDescriptor)>;
type CanonicalDescriptorSeekResult =
    Result<(CanonicalDescriptorSelection, GraphDescriptorTreeReadReport), CanonicalSegmentError>;

#[derive(Debug, Clone)]
pub struct CanonicalSegmentReader {
    path: PathBuf,
    manifest: CanonicalSegmentManifest,
    range_reader: FileSegmentRangeReader,
    descriptor_reader: GraphDescriptorTreeDemandReader,
    property_spills: Option<PropertySpillReader>,
    max_segment_bytes: NonZeroU64,
    poisoned: Arc<AtomicBool>,
}

impl CanonicalSegmentReader {
    pub fn open(
        path: impl Into<PathBuf>,
        manifest: CanonicalSegmentManifest,
        cache: Arc<SegmentCache>,
        store_id: StoreId,
        max_segment_bytes: NonZeroU64,
    ) -> Result<Self, CanonicalSegmentError> {
        Self::open_inner(
            path.into(),
            manifest,
            cache,
            store_id,
            max_segment_bytes,
            None,
        )
    }

    pub fn open_with_property_spills(
        path: impl Into<PathBuf>,
        manifest: CanonicalSegmentManifest,
        cache: Arc<SegmentCache>,
        store_id: StoreId,
        max_segment_bytes: NonZeroU64,
        property_spills: PropertySpillReader,
    ) -> Result<Self, CanonicalSegmentError> {
        Self::open_inner(
            path.into(),
            manifest,
            cache,
            store_id,
            max_segment_bytes,
            Some(property_spills),
        )
    }

    fn open_inner(
        path: PathBuf,
        manifest: CanonicalSegmentManifest,
        cache: Arc<SegmentCache>,
        store_id: StoreId,
        max_segment_bytes: NonZeroU64,
        property_spills: Option<PropertySpillReader>,
    ) -> Result<Self, CanonicalSegmentError> {
        manifest.validate()?;
        if let Some(property_spills) = &property_spills
            && (property_spills.manifest().generation != manifest.generation
                || property_spills.manifest().source_commit_epoch != manifest.source_commit_epoch)
        {
            return Err(CanonicalSegmentError::Corrupt(
                "canonical and property spill artifacts do not share one generation and source epoch"
                    .to_string(),
            ));
        }
        let descriptor_tree =
            PersistentCanonicalSegmentDescriptorTree::for_artifact(&path, manifest.generation);
        let (descriptor_paths, descriptor_config) = descriptor_tree.into_parts();
        let descriptor_root = GraphDescriptorTreeRootReader::open_bound(
            descriptor_paths,
            manifest.descriptor_generation_artifacts(),
            descriptor_config,
        )?;
        if descriptor_root.root().descriptor_count != manifest.segment_count {
            return Err(CanonicalSegmentError::Corrupt(
                "canonical descriptor root count does not match its manifest".to_string(),
            ));
        }
        let descriptor_reader = GraphDescriptorTreeDemandReader::open(
            descriptor_root.clone(),
            descriptor_config,
            Arc::clone(&cache),
            store_id,
        )?;
        let metadata = fs::metadata(&path)?;
        if metadata.len() != manifest.artifact_len {
            return Err(CanonicalSegmentError::Corrupt(format!(
                "canonical artifact length mismatch: expected {}, got {}",
                manifest.artifact_len,
                metadata.len()
            )));
        }
        let mut header = [0u8; 24];
        File::open(&path)?.read_exact(&mut header)?;
        if &header[..16] != ARTIFACT_HEADER {
            return Err(CanonicalSegmentError::Corrupt(
                "canonical artifact has an invalid header".to_string(),
            ));
        }
        let generation = u64::from_le_bytes(header[16..24].try_into().expect("fixed header"));
        if generation != manifest.generation.0 {
            return Err(CanonicalSegmentError::Corrupt(format!(
                "canonical artifact generation {generation} does not match manifest generation {}",
                manifest.generation.0
            )));
        }
        let mut range_reader =
            FileSegmentRangeReader::new().with_cache(cache, store_id, manifest.generation);
        range_reader.register(manifest.artifact_id, path.clone());
        Ok(Self {
            path,
            manifest,
            range_reader,
            descriptor_reader,
            property_spills,
            max_segment_bytes,
            poisoned: Arc::new(AtomicBool::new(false)),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn manifest(&self) -> &CanonicalSegmentManifest {
        &self.manifest
    }

    pub fn property_spill_manifest(&self) -> Option<&PropertySpillManifest> {
        self.property_spills
            .as_ref()
            .map(PropertySpillReader::manifest)
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
            || self.descriptor_reader.is_poisoned()
            || self
                .property_spills
                .as_ref()
                .is_some_and(PropertySpillReader::is_poisoned)
    }

    pub fn deep_scrub(&self) -> Result<CanonicalSegmentScrubReport, CanonicalSegmentError> {
        self.ensure_healthy()?;
        let result = self.deep_scrub_inner();
        self.poison_on_physical_failure(&result);
        result
    }

    fn deep_scrub_inner(&self) -> Result<CanonicalSegmentScrubReport, CanonicalSegmentError> {
        if let Some(property_spills) = &self.property_spills {
            property_spills.deep_scrub()?;
        }
        let canonical_bytes_hashed = self.verify_whole_artifact()?;
        let mut artifact = File::open(&self.path)?;
        let mut expected_offset = (ARTIFACT_HEADER.len() + 8) as u64;
        let mut expected_segment_id = 1u64;
        let mut previous_kind = None;
        let mut previous_node_max = None;
        let mut previous_relationship_max = None;
        let mut node_segments = 0u64;
        let mut relationship_segments = 0u64;
        let mut nodes = 0u64;
        let mut relationships = 0u64;
        let mut visit_error = None;
        let scrub = self.descriptor_reader.deep_visit(|key, value| {
            let step = CanonicalSegmentDescriptor::decode_descriptor_tree_entry(key, value)
                .and_then(|descriptor| {
                    self.validate_selected_descriptor(&descriptor)?;
                    if descriptor.offset != expected_offset
                        || descriptor.segment_id != expected_segment_id
                        || previous_kind.is_some_and(|kind| descriptor.kind < kind)
                    {
                        return Err(CanonicalSegmentError::Corrupt(format!(
                            "canonical descriptor closure is not contiguous at segment {}",
                            descriptor.segment_id
                        )));
                    }
                    let (segment_count, record_count, previous_max) = match descriptor.kind {
                        CanonicalSegmentKind::Nodes => {
                            (&mut node_segments, &mut nodes, &mut previous_node_max)
                        }
                        CanonicalSegmentKind::Relationships => (
                            &mut relationship_segments,
                            &mut relationships,
                            &mut previous_relationship_max,
                        ),
                    };
                    if previous_max.is_some_and(|maximum| descriptor.min_record_id <= maximum) {
                        return Err(CanonicalSegmentError::Corrupt(format!(
                            "canonical segment {} record ranges overlap or are out of order",
                            descriptor.segment_id
                        )));
                    }
                    let encoded = read_canonical_segment_uncached(
                        &mut artifact,
                        &descriptor,
                        self.max_segment_bytes,
                    )?;
                    let mut decoded = 0u64;
                    decode_segment_records(
                        &encoded,
                        self.manifest.generation,
                        &descriptor,
                        |_, payload| {
                            validate_canonical_record_payload(
                                descriptor.kind,
                                payload,
                                self.property_keys(),
                                self.property_spill_manifest()
                                    .map(|manifest| manifest.value_count),
                            )?;
                            decoded = decoded.checked_add(1).ok_or_else(|| {
                                CanonicalSegmentError::Corrupt(
                                    "canonical scrub record count overflow".to_string(),
                                )
                            })?;
                            Ok(())
                        },
                    )?;
                    if decoded != u64::from(descriptor.record_count) {
                        return Err(CanonicalSegmentError::Corrupt(format!(
                            "canonical segment {} decoded record count is inconsistent",
                            descriptor.segment_id
                        )));
                    }
                    *segment_count = segment_count.checked_add(1).ok_or_else(|| {
                        CanonicalSegmentError::Corrupt(
                            "canonical scrub segment count overflow".to_string(),
                        )
                    })?;
                    *record_count = record_count.checked_add(decoded).ok_or_else(|| {
                        CanonicalSegmentError::Corrupt(
                            "canonical scrub total record count overflow".to_string(),
                        )
                    })?;
                    *previous_max = Some(descriptor.max_record_id);
                    previous_kind = Some(descriptor.kind);
                    expected_offset = expected_offset
                        .checked_add(descriptor.length.get())
                        .ok_or_else(|| {
                            CanonicalSegmentError::Corrupt(
                                "canonical scrub artifact offset overflow".to_string(),
                            )
                        })?;
                    expected_segment_id = expected_segment_id.checked_add(1).ok_or_else(|| {
                        CanonicalSegmentError::Corrupt(
                            "canonical scrub segment id overflow".to_string(),
                        )
                    })?;
                    Ok(())
                });
            match step {
                Ok(()) => Ok(GraphDescriptorTreeScanControl::Continue),
                Err(error) => {
                    visit_error = Some(error);
                    Ok(GraphDescriptorTreeScanControl::Stop)
                }
            }
        });
        let scrub = match (scrub, visit_error) {
            (_, Some(error)) => return Err(error),
            (Err(error), None) => return Err(error.into()),
            (Ok(report), None) => report,
        };
        if expected_offset != self.manifest.artifact_len
            || node_segments != self.manifest.node_segment_count
            || relationship_segments != self.manifest.relationship_segment_count
            || nodes != self.manifest.node_count
            || relationships != self.manifest.relationship_count
            || scrub.checked_descriptors != self.manifest.segment_count
        {
            return Err(CanonicalSegmentError::Corrupt(format!(
                "canonical scrub closure bytes/segments/records {expected_offset}/{node_segments}/{relationship_segments}/{nodes}/{relationships} do not match {}/{}/{}/{}/{}",
                self.manifest.artifact_len,
                self.manifest.node_segment_count,
                self.manifest.relationship_segment_count,
                self.manifest.node_count,
                self.manifest.relationship_count
            )));
        }
        Ok(CanonicalSegmentScrubReport {
            descriptor_pages_checked: scrub.checked_pages,
            descriptors_checked: scrub.checked_descriptors,
            descriptor_bytes_checked: scrub.page_bytes_decoded,
            segments_checked: node_segments.saturating_add(relationship_segments),
            records_checked: nodes.saturating_add(relationships),
            canonical_bytes_hashed,
        })
    }

    fn verify_whole_artifact(&self) -> Result<u64, CanonicalSegmentError> {
        let mut file = File::open(&self.path)?;
        if file.metadata()?.len() != self.manifest.artifact_len {
            return Err(CanonicalSegmentError::Corrupt(
                "canonical artifact length changed after open".to_string(),
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
                CanonicalSegmentError::Corrupt("canonical scrub byte count overflow".to_string())
            })?;
        }
        let digest = hasher.finish();
        if digest.crc32c.as_u64() != self.manifest.artifact_digest.0
            || digest.sha256 != self.manifest.artifact_sha256
        {
            return Err(CanonicalSegmentError::Corrupt(
                "canonical artifact checksum mismatch during scrub".to_string(),
            ));
        }
        Ok(total)
    }

    fn property_keys(&self) -> &[String] {
        &self.manifest.property_keys
    }

    pub fn get_node(&self, id: NodeId) -> Result<Option<NodeRecord>, CanonicalSegmentError> {
        self.get_node_with_report(id).map(|(node, _)| node)
    }

    pub fn get_node_with_report(
        &self,
        id: NodeId,
    ) -> Result<(Option<NodeRecord>, CanonicalReadReport), CanonicalSegmentError> {
        self.ensure_healthy()?;
        let result = (|| {
            let mut report = self.empty_read_report();
            let (segment, descriptor_report) =
                self.find_descriptor_for_id(CanonicalSegmentKind::Nodes, id.0)?;
            report.record_descriptor_read(descriptor_report);
            let Some(segment) = segment else {
                return Ok((None, report));
            };
            report.segments_considered = 1;
            let read = self.read_segment_with_report(&segment)?;
            update_report_for_segment(&mut report, &read);
            let node = decode_node_by_id(
                &read.payload,
                self.manifest.generation,
                &segment,
                id.0,
                self.property_spills.as_ref(),
                Some(self.property_keys()),
            )?;
            report.records_decoded = u64::from(node.is_some());
            Ok((node, report))
        })();
        self.poison_on_physical_failure(&result);
        result
    }

    pub fn get_projected_node(
        &self,
        id: NodeId,
        required_properties: &BTreeSet<String>,
    ) -> Result<Option<ProjectedNodeRecord>, CanonicalSegmentError> {
        self.ensure_healthy()?;
        let result = (|| {
            let (segment, _) = self.find_descriptor_for_id(CanonicalSegmentKind::Nodes, id.0)?;
            let Some(segment) = segment else {
                return Ok(None);
            };
            let read = self.read_segment_with_report(&segment)?;
            decode_projected_node_by_id(
                &read.payload,
                self.manifest.generation,
                &segment,
                id.0,
                self.property_spills.as_ref(),
                Some(self.property_keys()),
                required_properties,
            )
        })();
        self.poison_on_physical_failure(&result);
        result
    }

    pub fn get_relationship(&self, id: RelId) -> Result<Option<RelRecord>, CanonicalSegmentError> {
        self.get_relationship_with_report(id)
            .map(|(relationship, _)| relationship)
    }

    pub fn get_relationship_with_report(
        &self,
        id: RelId,
    ) -> Result<(Option<RelRecord>, CanonicalReadReport), CanonicalSegmentError> {
        self.ensure_healthy()?;
        let result = (|| {
            let mut report = self.empty_read_report();
            let (segment, descriptor_report) =
                self.find_descriptor_for_id(CanonicalSegmentKind::Relationships, id.0)?;
            report.record_descriptor_read(descriptor_report);
            let Some(segment) = segment else {
                return Ok((None, report));
            };
            report.segments_considered = 1;
            let read = self.read_segment_with_report(&segment)?;
            update_report_for_segment(&mut report, &read);
            let relationship = decode_relationship_by_id(
                &read.payload,
                self.manifest.generation,
                &segment,
                id.0,
                self.property_spills.as_ref(),
                Some(self.property_keys()),
            )?;
            report.records_decoded = u64::from(relationship.is_some());
            Ok((relationship, report))
        })();
        self.poison_on_physical_failure(&result);
        result
    }

    pub fn node_records(&self) -> CanonicalNodeIterator {
        CanonicalNodeIterator::new(self.clone())
    }

    pub fn relationship_records(&self) -> CanonicalRelationshipIterator {
        CanonicalRelationshipIterator::new(self.clone())
    }

    pub fn scan_nodes(
        &self,
        mut consumer: impl FnMut(NodeRecord) -> Result<(), CanonicalSegmentError>,
    ) -> Result<CanonicalReadReport, CanonicalSegmentError> {
        self.scan_nodes_control(|node| {
            consumer(node)?;
            Ok(CanonicalScanControl::Continue)
        })
        .map(|(report, _)| report)
    }

    pub fn scan_nodes_control(
        &self,
        mut consumer: impl FnMut(NodeRecord) -> Result<CanonicalScanControl, CanonicalSegmentError>,
    ) -> Result<(CanonicalReadReport, CanonicalScanControl), CanonicalSegmentError> {
        self.ensure_healthy()?;
        let mut report = self.empty_read_report();
        let result = self.scan_descriptor_kind(CanonicalSegmentKind::Nodes, |descriptor| {
            report.segments_considered = report.segments_considered.saturating_add(1);
            let read = self.read_segment_with_report(descriptor)?;
            update_report_for_segment(&mut report, &read);
            decode_segment_records_control(
                &read.payload,
                self.manifest.generation,
                descriptor,
                |id, payload| {
                    let control = consumer(decode_node_with_property_spills(
                        id,
                        payload,
                        self.property_spills.as_ref(),
                        Some(self.property_keys()),
                    )?)?;
                    report.records_decoded = report.records_decoded.saturating_add(1);
                    Ok(control)
                },
            )
        });
        let result = result.map(|(descriptor_report, control)| {
            report.record_descriptor_read(descriptor_report);
            (report, control)
        });
        self.poison_on_physical_failure(&result);
        result
    }

    pub fn scan_projected_nodes_control(
        &self,
        required_properties: &BTreeSet<String>,
        mut consumer: impl FnMut(
            ProjectedNodeRecord,
        ) -> Result<CanonicalScanControl, CanonicalSegmentError>,
    ) -> Result<(CanonicalReadReport, CanonicalScanControl), CanonicalSegmentError> {
        self.ensure_healthy()?;
        let mut report = self.empty_read_report();
        let result = self.scan_descriptor_kind(CanonicalSegmentKind::Nodes, |descriptor| {
            report.segments_considered = report.segments_considered.saturating_add(1);
            let read = self.read_segment_with_report(descriptor)?;
            update_report_for_segment(&mut report, &read);
            decode_segment_records_control(
                &read.payload,
                self.manifest.generation,
                descriptor,
                |id, payload| {
                    let control = consumer(decode_projected_node_with_property_spills(
                        id,
                        payload,
                        self.property_spills.as_ref(),
                        Some(self.property_keys()),
                        required_properties,
                    )?)?;
                    report.records_decoded = report.records_decoded.saturating_add(1);
                    Ok(control)
                },
            )
        });
        let result = result.map(|(descriptor_report, control)| {
            report.record_descriptor_read(descriptor_report);
            (report, control)
        });
        self.poison_on_physical_failure(&result);
        result
    }

    pub fn scan_relationships(
        &self,
        mut consumer: impl FnMut(RelRecord) -> Result<(), CanonicalSegmentError>,
    ) -> Result<CanonicalReadReport, CanonicalSegmentError> {
        self.scan_relationships_control(|relationship| {
            consumer(relationship)?;
            Ok(CanonicalScanControl::Continue)
        })
        .map(|(report, _)| report)
    }

    pub fn scan_relationships_control(
        &self,
        mut consumer: impl FnMut(RelRecord) -> Result<CanonicalScanControl, CanonicalSegmentError>,
    ) -> Result<(CanonicalReadReport, CanonicalScanControl), CanonicalSegmentError> {
        self.ensure_healthy()?;
        let mut report = self.empty_read_report();
        let result = self.scan_descriptor_kind(CanonicalSegmentKind::Relationships, |descriptor| {
            report.segments_considered = report.segments_considered.saturating_add(1);
            let read = self.read_segment_with_report(descriptor)?;
            update_report_for_segment(&mut report, &read);
            decode_segment_records_control(
                &read.payload,
                self.manifest.generation,
                descriptor,
                |id, payload| {
                    let control = consumer(decode_relationship_with_property_spills(
                        id,
                        payload,
                        self.property_spills.as_ref(),
                        Some(self.property_keys()),
                    )?)?;
                    report.records_decoded = report.records_decoded.saturating_add(1);
                    Ok(control)
                },
            )
        });
        let result = result.map(|(descriptor_report, control)| {
            report.record_descriptor_read(descriptor_report);
            (report, control)
        });
        self.poison_on_physical_failure(&result);
        result
    }

    pub fn scan_relationships_for_endpoint_control(
        &self,
        node_id: NodeId,
        direction: CanonicalEndpointDirection,
        rel_type: Option<RelTypeId>,
        mut consumer: impl FnMut(RelRecord) -> Result<CanonicalScanControl, CanonicalSegmentError>,
    ) -> Result<(CanonicalReadReport, CanonicalScanControl), CanonicalSegmentError> {
        self.ensure_healthy()?;
        let mut report = self.empty_read_report();
        let result = self.scan_descriptor_kind(CanonicalSegmentKind::Relationships, |descriptor| {
            report.segments_considered = report.segments_considered.saturating_add(1);
            let might_contain = match direction {
                CanonicalEndpointDirection::Source => &descriptor.source_endpoint_bloom,
                CanonicalEndpointDirection::Target => &descriptor.target_endpoint_bloom,
            }
            .might_contain(node_id.0);
            if !might_contain {
                report.segments_pruned = report.segments_pruned.saturating_add(1);
                return Ok(CanonicalScanControl::Continue);
            }
            let read = self.read_segment_with_report(descriptor)?;
            update_report_for_segment(&mut report, &read);
            let mut control = CanonicalScanControl::Continue;
            decode_segment_records(
                &read.payload,
                self.manifest.generation,
                descriptor,
                |id, payload| {
                    if control == CanonicalScanControl::Stop {
                        return Ok(());
                    }
                    let relationship = decode_relationship_with_property_spills(
                        id,
                        payload,
                        self.property_spills.as_ref(),
                        Some(self.property_keys()),
                    )?;
                    report.records_decoded = report.records_decoded.saturating_add(1);
                    let endpoint_matches = match direction {
                        CanonicalEndpointDirection::Source => relationship.source == node_id,
                        CanonicalEndpointDirection::Target => relationship.target == node_id,
                    };
                    if endpoint_matches
                        && rel_type.is_none_or(|rel_type| relationship.rel_type == rel_type)
                    {
                        control = consumer(relationship)?;
                    }
                    Ok(())
                },
            )?;
            Ok(control)
        });
        let result = result.map(|(descriptor_report, control)| {
            report.record_descriptor_read(descriptor_report);
            (report, control)
        });
        self.poison_on_physical_failure(&result);
        result
    }

    pub fn scan_nodes_by_property_control(
        &self,
        label_id: LabelId,
        property: &str,
        value: &Value,
        mut consumer: impl FnMut(NodeRecord) -> Result<CanonicalScanControl, CanonicalSegmentError>,
    ) -> Result<(CanonicalReadReport, CanonicalScanControl), CanonicalSegmentError> {
        self.ensure_healthy()?;
        let bloom_key = node_property_bloom_key(label_id, property, value)?;
        let mut report = self.empty_read_report();
        let result = self.scan_descriptor_kind(CanonicalSegmentKind::Nodes, |descriptor| {
            report.segments_considered = report.segments_considered.saturating_add(1);
            if !descriptor.node_property_bloom.might_contain(bloom_key) {
                report.segments_pruned = report.segments_pruned.saturating_add(1);
                return Ok(CanonicalScanControl::Continue);
            }
            let read = self.read_segment_with_report(descriptor)?;
            update_report_for_segment(&mut report, &read);
            let mut control = CanonicalScanControl::Continue;
            decode_segment_records(
                &read.payload,
                self.manifest.generation,
                descriptor,
                |id, payload| {
                    if control == CanonicalScanControl::Stop {
                        return Ok(());
                    }
                    let node = decode_node_with_property_spills(
                        id,
                        payload,
                        self.property_spills.as_ref(),
                        Some(self.property_keys()),
                    )?;
                    report.records_decoded = report.records_decoded.saturating_add(1);
                    if node.labels.contains(&label_id)
                        && node.properties.get(property) == Some(value)
                    {
                        control = consumer(node)?;
                    }
                    Ok(())
                },
            )?;
            Ok(control)
        });
        let result = result.map(|(descriptor_report, control)| {
            report.record_descriptor_read(descriptor_report);
            (report, control)
        });
        self.poison_on_physical_failure(&result);
        result
    }

    fn scan_descriptor_kind(
        &self,
        kind: CanonicalSegmentKind,
        mut consumer: impl FnMut(
            &CanonicalSegmentDescriptor,
        ) -> Result<CanonicalScanControl, CanonicalSegmentError>,
    ) -> Result<(GraphDescriptorTreeReadReport, CanonicalScanControl), CanonicalSegmentError> {
        let expected = self.manifest.segment_count_for_kind(kind);
        let mut emitted = 0u64;
        let mut lower_bound = vec![kind.tag()];
        let mut aggregate = GraphDescriptorTreeReadReport::default();
        loop {
            let mut batch_emitted = 0u64;
            let mut last_key = None;
            let mut reached_kind_end = false;
            let mut control = CanonicalScanControl::Continue;
            let mut scan_error = None;
            let descriptor_result = self.descriptor_reader.scan_from(
                &lower_bound,
                GraphDescriptorTreeReadLimits {
                    max_descriptors: NonZeroU64::new(DESCRIPTOR_SCAN_BATCH + 1)
                        .expect("canonical descriptor batch limit is non-zero"),
                    ..GraphDescriptorTreeReadLimits::default()
                },
                |key, value| {
                    let step = CanonicalSegmentDescriptor::decode_descriptor_tree_entry(key, value)
                        .and_then(|descriptor| {
                            if descriptor.kind > kind {
                                reached_kind_end = true;
                                return Ok(CanonicalScanControl::Stop);
                            }
                            if descriptor.kind < kind {
                                return Err(CanonicalSegmentError::Corrupt(
                                    "canonical descriptor scan moved backwards across kinds"
                                        .to_string(),
                                ));
                            }
                            emitted = emitted.checked_add(1).ok_or_else(|| {
                                CanonicalSegmentError::Corrupt(
                                    "canonical descriptor scan count overflow".to_string(),
                                )
                            })?;
                            batch_emitted = batch_emitted.checked_add(1).ok_or_else(|| {
                                CanonicalSegmentError::Corrupt(
                                    "canonical descriptor batch count overflow".to_string(),
                                )
                            })?;
                            if emitted > expected {
                                return Err(CanonicalSegmentError::Corrupt(format!(
                                    "canonical descriptor kind {:?} contains more than the declared {expected} segments",
                                    kind
                                )));
                            }
                            self.validate_selected_descriptor(&descriptor)?;
                            last_key = Some(key.to_vec());
                            consumer(&descriptor)
                        });
                    match step {
                        Ok(next) => {
                            control = next;
                            Ok(if next == CanonicalScanControl::Stop
                                || batch_emitted == DESCRIPTOR_SCAN_BATCH
                            {
                                GraphDescriptorTreeScanControl::Stop
                            } else {
                                GraphDescriptorTreeScanControl::Continue
                            })
                        }
                        Err(error) => {
                            scan_error = Some(error);
                            Ok(GraphDescriptorTreeScanControl::Stop)
                        }
                    }
                },
            );
            let (batch_report, tree_control) = match (descriptor_result, scan_error) {
                (_, Some(error)) => return Err(error),
                (Err(error), None) => return Err(error.into()),
                (Ok(result), None) => result,
            };
            aggregate_descriptor_report(&mut aggregate, batch_report);
            if control == CanonicalScanControl::Stop && !reached_kind_end {
                return Ok((aggregate, control));
            }
            if reached_kind_end || tree_control == GraphDescriptorTreeScanControl::Continue {
                if emitted != expected {
                    return Err(CanonicalSegmentError::Corrupt(format!(
                        "canonical descriptor kind {:?} contains {emitted} segments, expected {expected}",
                        kind
                    )));
                }
                return Ok((aggregate, CanonicalScanControl::Continue));
            }
            let key = last_key.ok_or_else(|| {
                CanonicalSegmentError::Corrupt(
                    "canonical descriptor batch stopped without a selected key".to_string(),
                )
            })?;
            let Some(next) = lexicographic_successor(&key) else {
                if emitted != expected {
                    return Err(CanonicalSegmentError::Corrupt(format!(
                        "canonical descriptor key space ended after {emitted} of {expected} entries"
                    )));
                }
                return Ok((aggregate, CanonicalScanControl::Continue));
            };
            lower_bound = next;
        }
    }

    fn find_descriptor_for_id(
        &self,
        kind: CanonicalSegmentKind,
        id: u64,
    ) -> Result<
        (
            Option<CanonicalSegmentDescriptor>,
            GraphDescriptorTreeReadReport,
        ),
        CanonicalSegmentError,
    > {
        let mut lower_bound = Vec::with_capacity(9);
        lower_bound.push(kind.tag());
        lower_bound.extend_from_slice(&id.to_be_bytes());
        let (selected, report) = self.next_descriptor_from(kind, &lower_bound)?;
        let descriptor = selected.and_then(|(_, descriptor)| {
            (descriptor.min_record_id <= id && id <= descriptor.max_record_id).then_some(descriptor)
        });
        Ok((descriptor, report))
    }

    fn next_descriptor_from(
        &self,
        kind: CanonicalSegmentKind,
        lower_bound: &[u8],
    ) -> CanonicalDescriptorSeekResult {
        let limits = GraphDescriptorTreeReadLimits {
            max_descriptors: NonZeroU64::new(1)
                .expect("canonical descriptor seek emits at most one segment"),
            ..GraphDescriptorTreeReadLimits::default()
        };
        let mut selected = None;
        let mut decode_error = None;
        let descriptor_result =
            self.descriptor_reader
                .scan_from(lower_bound, limits, |key, value| {
                    match CanonicalSegmentDescriptor::decode_descriptor_tree_entry(key, value) {
                        Ok(descriptor) => selected = Some((key.to_vec(), descriptor)),
                        Err(error) => decode_error = Some(error),
                    }
                    Ok(GraphDescriptorTreeScanControl::Stop)
                });
        let report = match (descriptor_result, decode_error) {
            (_, Some(error)) => return Err(error),
            (Err(error), None) => return Err(error.into()),
            (Ok((report, _)), None) => report,
        };
        let selected = match selected {
            Some((key, descriptor)) if descriptor.kind == kind => {
                self.validate_selected_descriptor(&descriptor)?;
                Some((key, descriptor))
            }
            Some((_, descriptor)) if descriptor.kind < kind => {
                return Err(CanonicalSegmentError::Corrupt(
                    "canonical descriptor seek moved backwards across kinds".to_string(),
                ));
            }
            Some(_) | None => None,
        };
        Ok((selected, report))
    }

    fn validate_selected_descriptor(
        &self,
        descriptor: &CanonicalSegmentDescriptor,
    ) -> Result<(), CanonicalSegmentError> {
        descriptor.validate_identity()?;
        let minimum_offset = (ARTIFACT_HEADER.len() + 8) as u64;
        let end = descriptor
            .offset
            .checked_add(descriptor.length.get())
            .ok_or_else(|| {
                CanonicalSegmentError::Corrupt(
                    "canonical descriptor range overflows u64".to_string(),
                )
            })?;
        if descriptor.segment_id > self.manifest.segment_count
            || descriptor.offset < minimum_offset
            || end > self.manifest.artifact_len
        {
            return Err(CanonicalSegmentError::Corrupt(format!(
                "canonical segment {} is outside its selected artifact",
                descriptor.segment_id
            )));
        }
        if descriptor.length.get() > self.max_segment_bytes.get() {
            return Err(CanonicalSegmentError::SegmentTooLarge {
                segment_bytes: descriptor.length.get(),
                max_bytes: self.max_segment_bytes.get(),
            });
        }
        Ok(())
    }

    fn empty_read_report(&self) -> CanonicalReadReport {
        CanonicalReadReport {
            generation: self.manifest.generation.0,
            ..CanonicalReadReport::default()
        }
    }

    fn read_segment(
        &self,
        descriptor: &CanonicalSegmentDescriptor,
    ) -> Result<crate::SegmentBytes, CanonicalSegmentError> {
        self.read_segment_with_report(descriptor)
            .map(|read| read.payload)
    }

    fn read_segment_with_report(
        &self,
        descriptor: &CanonicalSegmentDescriptor,
    ) -> Result<SegmentRangeRead, CanonicalSegmentError> {
        self.validate_selected_descriptor(descriptor)?;
        self.range_reader
            .read_range_with_report(
                &SegmentReadRange::new(
                    self.manifest.artifact_id,
                    descriptor.segment_id,
                    descriptor.offset,
                    descriptor.length,
                )
                .with_content_digest(descriptor.content_digest),
            )
            .map_err(Into::into)
    }

    fn ensure_healthy(&self) -> Result<(), CanonicalSegmentError> {
        if self.is_poisoned() {
            return Err(CanonicalSegmentError::Corrupt(
                "canonical segment reader is poisoned by an earlier physical failure".to_string(),
            ));
        }
        Ok(())
    }

    fn poison_on_physical_failure<T>(&self, result: &Result<T, CanonicalSegmentError>) {
        if result.as_ref().is_err_and(canonical_error_requires_poison) {
            self.poisoned.store(true, Ordering::Release);
        }
    }

    fn poison_error(&self, error: &CanonicalSegmentError) {
        if canonical_error_requires_poison(error) {
            self.poisoned.store(true, Ordering::Release);
        }
    }
}

pub struct CanonicalNodeIterator {
    reader: CanonicalSegmentReader,
    cursor: CanonicalDescriptorCursor,
    current: std::vec::IntoIter<NodeRecord>,
    failed: bool,
}

impl CanonicalNodeIterator {
    fn new(reader: CanonicalSegmentReader) -> Self {
        let expected = reader
            .manifest
            .segment_count_for_kind(CanonicalSegmentKind::Nodes);
        Self {
            reader,
            cursor: CanonicalDescriptorCursor::new(CanonicalSegmentKind::Nodes, expected),
            current: Vec::new().into_iter(),
            failed: false,
        }
    }
}

impl Iterator for CanonicalNodeIterator {
    type Item = Result<NodeRecord, CanonicalSegmentError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(node) = self.current.next() {
                return Some(Ok(node));
            }
            if self.failed {
                return None;
            }
            let descriptor = match self.cursor.next(&self.reader) {
                Ok(Some(descriptor)) => descriptor,
                Ok(None) => return None,
                Err(error) => {
                    self.reader.poison_error(&error);
                    self.failed = true;
                    return Some(Err(error));
                }
            };
            let bytes = match self.reader.read_segment(&descriptor) {
                Ok(bytes) => bytes,
                Err(error) => {
                    self.reader.poison_error(&error);
                    self.failed = true;
                    return Some(Err(error));
                }
            };
            let mut records = Vec::with_capacity(descriptor.record_count as usize);
            if let Err(error) = decode_segment_records(
                &bytes,
                self.reader.manifest.generation,
                &descriptor,
                |id, payload| {
                    records.push(decode_node_with_property_spills(
                        id,
                        payload,
                        self.reader.property_spills.as_ref(),
                        Some(self.reader.property_keys()),
                    )?);
                    Ok(())
                },
            ) {
                self.reader.poison_error(&error);
                self.failed = true;
                return Some(Err(error));
            }
            self.current = records.into_iter();
        }
    }
}

pub struct CanonicalRelationshipIterator {
    reader: CanonicalSegmentReader,
    cursor: CanonicalDescriptorCursor,
    current: std::vec::IntoIter<RelRecord>,
    failed: bool,
}

impl CanonicalRelationshipIterator {
    fn new(reader: CanonicalSegmentReader) -> Self {
        let expected = reader
            .manifest
            .segment_count_for_kind(CanonicalSegmentKind::Relationships);
        Self {
            reader,
            cursor: CanonicalDescriptorCursor::new(CanonicalSegmentKind::Relationships, expected),
            current: Vec::new().into_iter(),
            failed: false,
        }
    }
}

impl Iterator for CanonicalRelationshipIterator {
    type Item = Result<RelRecord, CanonicalSegmentError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(relationship) = self.current.next() {
                return Some(Ok(relationship));
            }
            if self.failed {
                return None;
            }
            let descriptor = match self.cursor.next(&self.reader) {
                Ok(Some(descriptor)) => descriptor,
                Ok(None) => return None,
                Err(error) => {
                    self.reader.poison_error(&error);
                    self.failed = true;
                    return Some(Err(error));
                }
            };
            let bytes = match self.reader.read_segment(&descriptor) {
                Ok(bytes) => bytes,
                Err(error) => {
                    self.reader.poison_error(&error);
                    self.failed = true;
                    return Some(Err(error));
                }
            };
            let mut records = Vec::with_capacity(descriptor.record_count as usize);
            if let Err(error) = decode_segment_records(
                &bytes,
                self.reader.manifest.generation,
                &descriptor,
                |id, payload| {
                    records.push(decode_relationship_with_property_spills(
                        id,
                        payload,
                        self.reader.property_spills.as_ref(),
                        Some(self.reader.property_keys()),
                    )?);
                    Ok(())
                },
            ) {
                self.reader.poison_error(&error);
                self.failed = true;
                return Some(Err(error));
            }
            self.current = records.into_iter();
        }
    }
}

struct CanonicalDescriptorCursor {
    kind: CanonicalSegmentKind,
    next_lower_bound: Option<Vec<u8>>,
    descriptors_seen: u64,
    expected_descriptors: u64,
    finished: bool,
}

impl CanonicalDescriptorCursor {
    fn new(kind: CanonicalSegmentKind, expected_descriptors: u64) -> Self {
        Self {
            kind,
            next_lower_bound: Some(vec![kind.tag()]),
            descriptors_seen: 0,
            expected_descriptors,
            finished: false,
        }
    }

    fn next(
        &mut self,
        reader: &CanonicalSegmentReader,
    ) -> Result<Option<CanonicalSegmentDescriptor>, CanonicalSegmentError> {
        reader.ensure_healthy()?;
        if self.finished {
            return Ok(None);
        }
        let Some(lower_bound) = self.next_lower_bound.as_deref() else {
            if self.descriptors_seen == self.expected_descriptors {
                self.finished = true;
                return Ok(None);
            }
            return Err(CanonicalSegmentError::Corrupt(
                "canonical descriptor key space ended before its declared count".to_string(),
            ));
        };
        let (selected, _) = reader.next_descriptor_from(self.kind, lower_bound)?;
        if self.descriptors_seen == self.expected_descriptors {
            self.finished = true;
            if selected.is_some() {
                return Err(CanonicalSegmentError::Corrupt(format!(
                    "canonical descriptor kind {:?} contains more than the declared {} segments",
                    self.kind, self.expected_descriptors
                )));
            }
            return Ok(None);
        }
        let (key, descriptor) = selected.ok_or_else(|| {
            CanonicalSegmentError::Corrupt(format!(
                "canonical descriptor kind {:?} ended after {} of {} segments",
                self.kind, self.descriptors_seen, self.expected_descriptors
            ))
        })?;
        self.descriptors_seen = self.descriptors_seen.checked_add(1).ok_or_else(|| {
            CanonicalSegmentError::Corrupt("canonical descriptor cursor count overflow".to_string())
        })?;
        self.next_lower_bound = lexicographic_successor(&key);
        if self.next_lower_bound.is_none() && self.descriptors_seen != self.expected_descriptors {
            return Err(CanonicalSegmentError::Corrupt(
                "canonical descriptor key space ended before its declared count".to_string(),
            ));
        }
        Ok(Some(descriptor))
    }
}

fn lexicographic_successor(key: &[u8]) -> Option<Vec<u8>> {
    let mut next = key.to_vec();
    for index in (0..next.len()).rev() {
        if next[index] != u8::MAX {
            next[index] = next[index].saturating_add(1);
            next.truncate(index + 1);
            return Some(next);
        }
    }
    None
}

fn endpoint_bloom_hash(id: u64, seed: u64) -> u64 {
    let mut value = id.wrapping_add(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15));
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn node_property_bloom_key(
    label: LabelId,
    property: &str,
    value: &Value,
) -> Result<u64, CanonicalSegmentError> {
    let mut bytes = Vec::with_capacity(8usize.saturating_add(property.len()));
    bytes.extend_from_slice(&label.0.to_le_bytes());
    encode_string(property, &mut bytes)?;
    encode_value(value, &mut bytes, 0)?;
    Ok(content_digest(&bytes).0)
}

fn update_report_for_segment(report: &mut CanonicalReadReport, read: &SegmentRangeRead) {
    report.segments_read = report.segments_read.saturating_add(1);
    report.bytes_read = report.bytes_read.saturating_add(read.payload.len() as u64);
    report.cache_hits = report.cache_hits.saturating_add(u64::from(read.cache_hit));
    report.cache_misses = report
        .cache_misses
        .saturating_add(u64::from(read.cache_miss));
    report.peak_segment_bytes = report.peak_segment_bytes.max(read.payload.len() as u64);
}

impl CanonicalReadReport {
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

fn aggregate_descriptor_report(
    aggregate: &mut GraphDescriptorTreeReadReport,
    batch: GraphDescriptorTreeReadReport,
) {
    aggregate.pages_visited = aggregate.pages_visited.saturating_add(batch.pages_visited);
    aggregate.page_bytes_decoded = aggregate
        .page_bytes_decoded
        .saturating_add(batch.page_bytes_decoded);
    aggregate.storage_bytes_read = aggregate
        .storage_bytes_read
        .saturating_add(batch.storage_bytes_read);
    aggregate.leaf_entries_examined = aggregate
        .leaf_entries_examined
        .saturating_add(batch.leaf_entries_examined);
    aggregate.descriptors_emitted = aggregate
        .descriptors_emitted
        .saturating_add(batch.descriptors_emitted);
    aggregate.cache_hits = aggregate.cache_hits.saturating_add(batch.cache_hits);
    aggregate.cache_misses = aggregate.cache_misses.saturating_add(batch.cache_misses);
    aggregate.cache_admission_rejections = aggregate
        .cache_admission_rejections
        .saturating_add(batch.cache_admission_rejections);
    aggregate.maximum_depth = aggregate.maximum_depth.max(batch.maximum_depth);
}

fn read_canonical_segment_uncached(
    file: &mut File,
    descriptor: &CanonicalSegmentDescriptor,
    max_segment_bytes: NonZeroU64,
) -> Result<Vec<u8>, CanonicalSegmentError> {
    if descriptor.length.get() > max_segment_bytes.get() {
        return Err(CanonicalSegmentError::SegmentTooLarge {
            segment_bytes: descriptor.length.get(),
            max_bytes: max_segment_bytes.get(),
        });
    }
    let length = usize::try_from(descriptor.length.get()).map_err(|_| {
        CanonicalSegmentError::SegmentTooLarge {
            segment_bytes: descriptor.length.get(),
            max_bytes: usize::MAX as u64,
        }
    })?;
    file.seek(SeekFrom::Start(descriptor.offset))?;
    let mut encoded = vec![0u8; length];
    file.read_exact(&mut encoded)?;
    if content_digest(&encoded) != descriptor.content_digest {
        return Err(CanonicalSegmentError::Corrupt(format!(
            "canonical segment {} failed content digest verification",
            descriptor.segment_id
        )));
    }
    Ok(encoded)
}

fn validate_canonical_record_payload(
    kind: CanonicalSegmentKind,
    payload: &[u8],
    property_keys: &[String],
    property_spill_count: Option<u64>,
) -> Result<(), CanonicalSegmentError> {
    let mut cursor = SliceCursor::new(payload);
    match kind {
        CanonicalSegmentKind::Nodes => {
            let label_count = cursor.read_u32()?;
            for _ in 0..label_count {
                cursor.read_u32()?;
            }
        }
        CanonicalSegmentKind::Relationships => {
            cursor.read_u64()?;
            cursor.read_u64()?;
            cursor.read_u32()?;
        }
    }
    validate_record_property_payload(&mut cursor, property_keys, property_spill_count)?;
    if !cursor.is_empty() {
        return Err(CanonicalSegmentError::Corrupt(
            "canonical record has trailing bytes during scrub".to_string(),
        ));
    }
    Ok(())
}

fn validate_record_property_payload(
    cursor: &mut SliceCursor<'_>,
    property_keys: &[String],
    property_spill_count: Option<u64>,
) -> Result<(), CanonicalSegmentError> {
    let property_count = cursor.read_u32()?;
    let mut seen = BTreeSet::new();
    for _ in 0..property_count {
        let key_id = cursor.read_u32()?;
        if property_keys.get(key_id as usize).is_none() || !seen.insert(key_id) {
            return Err(CanonicalSegmentError::Corrupt(format!(
                "canonical record has an unknown or duplicate property key id {key_id}"
            )));
        }
        validate_encoded_value(cursor, 1, property_spill_count)?;
    }
    Ok(())
}

fn validate_encoded_value(
    cursor: &mut SliceCursor<'_>,
    depth: usize,
    property_spill_count: Option<u64>,
) -> Result<(), CanonicalSegmentError> {
    ensure_depth(depth)?;
    match cursor.read_u8()? {
        0 => Ok(()),
        1 => match cursor.read_u8()? {
            0 | 1 => Ok(()),
            value => Err(CanonicalSegmentError::Corrupt(format!(
                "invalid canonical boolean {value}"
            ))),
        },
        2 | 3 => {
            cursor.read_u64()?;
            Ok(())
        }
        4 => validate_encoded_string(cursor),
        5 => {
            let count = cursor.read_u32()?;
            for _ in 0..count {
                validate_encoded_value(cursor, depth.saturating_add(1), property_spill_count)?;
            }
            Ok(())
        }
        6 => {
            let count = cursor.read_u32()?;
            let mut seen = BTreeSet::new();
            for _ in 0..count {
                let key = read_encoded_string(cursor)?;
                if !seen.insert(key) {
                    return Err(CanonicalSegmentError::Corrupt(
                        "canonical nested property map has duplicate keys".to_string(),
                    ));
                }
                validate_encoded_value(cursor, depth.saturating_add(2), property_spill_count)?;
            }
            Ok(())
        }
        7 => {
            let spill_id = cursor.read_u64()?;
            match property_spill_count {
                Some(count) if spill_id < count => {}
                Some(_) => {
                    return Err(CanonicalSegmentError::Corrupt(format!(
                        "canonical record references property spill {spill_id} outside its selected artifact"
                    )));
                }
                None => {
                    return Err(CanonicalSegmentError::Corrupt(format!(
                        "canonical record references property spill {spill_id} without a selected spill artifact"
                    )));
                }
            }
            Ok(())
        }
        8 => {
            let length = cursor.read_u32()? as usize;
            cursor.read_exact(length)?;
            Ok(())
        }
        9 => {
            cursor.read_exact(16)?;
            Ok(())
        }
        tag => Err(CanonicalSegmentError::Corrupt(format!(
            "unknown canonical value tag {tag}"
        ))),
    }
}

fn validate_encoded_string(cursor: &mut SliceCursor<'_>) -> Result<(), CanonicalSegmentError> {
    read_encoded_string(cursor).map(|_| ())
}

fn read_encoded_string(cursor: &mut SliceCursor<'_>) -> Result<String, CanonicalSegmentError> {
    let length = cursor.read_u32()? as usize;
    let encoded = cursor.read_exact(length)?;
    std::str::from_utf8(encoded)
        .map(str::to_owned)
        .map_err(|_| CanonicalSegmentError::Corrupt("canonical string is not UTF-8".to_string()))
}

fn canonical_error_requires_poison(error: &CanonicalSegmentError) -> bool {
    match error {
        CanonicalSegmentError::Io(_)
        | CanonicalSegmentError::Read(_)
        | CanonicalSegmentError::Corrupt(_) => true,
        CanonicalSegmentError::DescriptorTree(error) => matches!(
            error,
            GraphDescriptorTreeError::Io(_)
                | GraphDescriptorTreeError::Page(GraphDescriptorPageError::Corrupt(_))
                | GraphDescriptorTreeError::Corrupt(_)
        ),
        CanonicalSegmentError::PropertySpill(_)
        | CanonicalSegmentError::RecordTooLarge { .. }
        | CanonicalSegmentError::SegmentTooLarge { .. }
        | CanonicalSegmentError::Source(_) => false,
    }
}

fn decode_node_by_id(
    bytes: &[u8],
    generation: ManifestGeneration,
    descriptor: &CanonicalSegmentDescriptor,
    id: u64,
    property_spills: Option<&PropertySpillReader>,
    property_keys: Option<&[String]>,
) -> Result<Option<NodeRecord>, CanonicalSegmentError> {
    let mut found = None;
    decode_segment_records_control(bytes, generation, descriptor, |record_id, payload| {
        // Record ids strictly increase, so the first id at or past the target
        // settles the question either way and the rest of the segment does not
        // need to be walked.
        if record_id < id {
            return Ok(CanonicalScanControl::Continue);
        }
        if record_id == id {
            found = Some(decode_node_with_property_spills(
                record_id,
                payload,
                property_spills,
                property_keys,
            )?);
        }
        Ok(CanonicalScanControl::Stop)
    })?;
    Ok(found)
}

fn decode_projected_node_by_id(
    bytes: &[u8],
    generation: ManifestGeneration,
    descriptor: &CanonicalSegmentDescriptor,
    id: u64,
    property_spills: Option<&PropertySpillReader>,
    property_keys: Option<&[String]>,
    required_properties: &BTreeSet<String>,
) -> Result<Option<ProjectedNodeRecord>, CanonicalSegmentError> {
    let mut found = None;
    decode_segment_records_control(bytes, generation, descriptor, |record_id, payload| {
        if record_id < id {
            return Ok(CanonicalScanControl::Continue);
        }
        if record_id == id {
            found = Some(decode_projected_node_with_property_spills(
                record_id,
                payload,
                property_spills,
                property_keys,
                required_properties,
            )?);
        }
        Ok(CanonicalScanControl::Stop)
    })?;
    Ok(found)
}

fn decode_relationship_by_id(
    bytes: &[u8],
    generation: ManifestGeneration,
    descriptor: &CanonicalSegmentDescriptor,
    id: u64,
    property_spills: Option<&PropertySpillReader>,
    property_keys: Option<&[String]>,
) -> Result<Option<RelRecord>, CanonicalSegmentError> {
    let mut found = None;
    decode_segment_records_control(bytes, generation, descriptor, |record_id, payload| {
        if record_id < id {
            return Ok(CanonicalScanControl::Continue);
        }
        if record_id == id {
            found = Some(decode_relationship_with_property_spills(
                record_id,
                payload,
                property_spills,
                property_keys,
            )?);
        }
        Ok(CanonicalScanControl::Stop)
    })?;
    Ok(found)
}

fn decode_segment_records(
    bytes: &[u8],
    expected_generation: ManifestGeneration,
    descriptor: &CanonicalSegmentDescriptor,
    mut consumer: impl FnMut(u64, &[u8]) -> Result<(), CanonicalSegmentError>,
) -> Result<(), CanonicalSegmentError> {
    decode_segment_records_control(bytes, expected_generation, descriptor, |id, payload| {
        consumer(id, payload)?;
        Ok(CanonicalScanControl::Continue)
    })
    .map(|_| ())
}

/// Walks a segment's records, letting the consumer stop early.
///
/// A consumer that runs to the end also validates the segment's framing: ids
/// strictly increase, the last one matches the descriptor, and no bytes are
/// left over. Stopping early necessarily skips the part of that check it never
/// reached. Callers that answer a question — a point lookup, a scan that has
/// filled its limit — can afford that, because the bytes arrive
/// content-verified: `read_range` checks the segment digest on every miss and
/// `SegmentCache::insert` checks it again before caching. The framing walk is
/// a second opinion about data already proven intact rather than the thing
/// standing between a bit flip and a decoded record. Callers whose purpose is
/// to validate keep using `decode_segment_records` and pay for the full walk.
fn decode_segment_records_control(
    bytes: &[u8],
    expected_generation: ManifestGeneration,
    descriptor: &CanonicalSegmentDescriptor,
    mut consumer: impl FnMut(u64, &[u8]) -> Result<CanonicalScanControl, CanonicalSegmentError>,
) -> Result<CanonicalScanControl, CanonicalSegmentError> {
    let mut cursor = SliceCursor::new(bytes);
    if cursor.read_exact(8)? != SEGMENT_HEADER {
        return Err(CanonicalSegmentError::Corrupt(format!(
            "canonical segment {} has an invalid header",
            descriptor.segment_id
        )));
    }
    let kind = CanonicalSegmentKind::from_tag(cursor.read_u8()?)?;
    let generation = cursor.read_u64()?;
    let segment_id = cursor.read_u64()?;
    let record_count = cursor.read_u32()?;
    if kind != descriptor.kind
        || generation != expected_generation.0
        || segment_id != descriptor.segment_id
        || record_count != descriptor.record_count
    {
        return Err(CanonicalSegmentError::Corrupt(format!(
            "canonical segment {} metadata does not match its manifest",
            descriptor.segment_id
        )));
    }
    let mut previous_id = None;
    for _ in 0..record_count {
        let id = cursor.read_u64()?;
        if previous_id.is_some_and(|previous| id <= previous) {
            return Err(CanonicalSegmentError::Corrupt(format!(
                "canonical segment {} record ids are not strictly increasing",
                descriptor.segment_id
            )));
        }
        let payload_len = cursor.read_u32()? as usize;
        let payload = cursor.read_exact(payload_len)?;
        let control = consumer(id, payload)?;
        previous_id = Some(id);
        if control == CanonicalScanControl::Stop {
            return Ok(CanonicalScanControl::Stop);
        }
    }
    if !cursor.is_empty() || previous_id.is_none() || previous_id != Some(descriptor.max_record_id)
    {
        return Err(CanonicalSegmentError::Corrupt(format!(
            "canonical segment {} payload bounds are inconsistent",
            descriptor.segment_id
        )));
    }
    Ok(CanonicalScanControl::Continue)
}

/// Interns each distinct top-level record property key into a `u32` id in
/// first-seen order. Nodes and relationships share one dictionary per
/// artifact; the manifest publishes the resulting key table.
#[derive(Default)]
struct PropertyKeyDictionary {
    ids: BTreeMap<String, u32>,
    keys: Vec<String>,
}

impl PropertyKeyDictionary {
    fn intern(&mut self, key: &str) -> Result<u32, CanonicalSegmentError> {
        if let Some(id) = self.ids.get(key) {
            return Ok(*id);
        }
        let id = u32_len(self.keys.len(), "canonical property key table")?;
        self.ids.insert(key.to_string(), id);
        self.keys.push(key.to_string());
        Ok(id)
    }

    fn into_keys(self) -> Vec<String> {
        self.keys
    }
}

fn encode_node_with_property_spills(
    node: &NodeRecord,
    property_spills: Option<&mut PropertySpillWriter>,
    property_keys: Option<&mut PropertyKeyDictionary>,
) -> Result<Vec<u8>, CanonicalSegmentError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&u32_len(node.labels.len(), "node labels")?.to_le_bytes());
    for label in &node.labels {
        bytes.extend_from_slice(&label.0.to_le_bytes());
    }
    encode_properties_with_property_spills(
        &node.properties,
        &mut bytes,
        property_spills,
        property_keys,
    )?;
    Ok(bytes)
}

fn decode_node_with_property_spills(
    id: u64,
    payload: &[u8],
    property_spills: Option<&PropertySpillReader>,
    property_keys: Option<&[String]>,
) -> Result<NodeRecord, CanonicalSegmentError> {
    let mut cursor = SliceCursor::new(payload);
    let label_count = cursor.read_u32()? as usize;
    let mut labels = BTreeSet::new();
    for _ in 0..label_count {
        labels.insert(LabelId(cursor.read_u32()?));
    }
    let properties = decode_record_properties(&mut cursor, property_spills, property_keys)?;
    if !cursor.is_empty() {
        return Err(CanonicalSegmentError::Corrupt(
            "node record has trailing bytes".to_string(),
        ));
    }
    Ok(NodeRecord {
        id: NodeId(id),
        labels,
        properties,
    })
}

fn decode_projected_node_with_property_spills(
    id: u64,
    payload: &[u8],
    property_spills: Option<&PropertySpillReader>,
    property_keys: Option<&[String]>,
    required_properties: &BTreeSet<String>,
) -> Result<ProjectedNodeRecord, CanonicalSegmentError> {
    let mut cursor = SliceCursor::new(payload);
    let label_count = cursor.read_u32()? as usize;
    let mut labels = BTreeSet::new();
    for _ in 0..label_count {
        labels.insert(LabelId(cursor.read_u32()?));
    }
    let properties = decode_projected_record_properties(
        &mut cursor,
        property_spills,
        property_keys,
        required_properties,
    )?;
    if !cursor.is_empty() {
        return Err(CanonicalSegmentError::Corrupt(
            "projected node record has trailing bytes".to_string(),
        ));
    }
    Ok(ProjectedNodeRecord {
        id: NodeId(id),
        labels,
        properties,
    })
}

pub(crate) fn encode_relationship(
    relationship: &RelRecord,
) -> Result<Vec<u8>, CanonicalSegmentError> {
    encode_relationship_with_property_spills(relationship, None, None)
}

fn encode_relationship_with_property_spills(
    relationship: &RelRecord,
    property_spills: Option<&mut PropertySpillWriter>,
    property_keys: Option<&mut PropertyKeyDictionary>,
) -> Result<Vec<u8>, CanonicalSegmentError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&relationship.source.0.to_le_bytes());
    bytes.extend_from_slice(&relationship.target.0.to_le_bytes());
    bytes.extend_from_slice(&relationship.rel_type.0.to_le_bytes());
    encode_properties_with_property_spills(
        &relationship.properties,
        &mut bytes,
        property_spills,
        property_keys,
    )?;
    Ok(bytes)
}

pub(crate) fn decode_relationship(
    id: u64,
    payload: &[u8],
) -> Result<RelRecord, CanonicalSegmentError> {
    decode_relationship_with_property_spills(id, payload, None, None)
}

fn decode_relationship_with_property_spills(
    id: u64,
    payload: &[u8],
    property_spills: Option<&PropertySpillReader>,
    property_keys: Option<&[String]>,
) -> Result<RelRecord, CanonicalSegmentError> {
    let mut cursor = SliceCursor::new(payload);
    let source = NodeId(cursor.read_u64()?);
    let target = NodeId(cursor.read_u64()?);
    let rel_type = RelTypeId(cursor.read_u32()?);
    let properties = decode_record_properties(&mut cursor, property_spills, property_keys)?;
    if !cursor.is_empty() {
        return Err(CanonicalSegmentError::Corrupt(
            "relationship record has trailing bytes".to_string(),
        ));
    }
    Ok(RelRecord {
        id: RelId(id),
        source,
        target,
        rel_type,
        properties,
    })
}

fn encode_properties(
    properties: &BTreeMap<String, Value>,
    output: &mut Vec<u8>,
    depth: usize,
) -> Result<(), CanonicalSegmentError> {
    output.extend_from_slice(&u32_len(properties.len(), "property map")?.to_le_bytes());
    for (key, value) in properties {
        encode_string(key, output)?;
        encode_value(value, output, depth.saturating_add(1))?;
    }
    Ok(())
}

fn encode_properties_with_property_spills(
    properties: &BTreeMap<String, Value>,
    output: &mut Vec<u8>,
    mut property_spills: Option<&mut PropertySpillWriter>,
    mut property_keys: Option<&mut PropertyKeyDictionary>,
) -> Result<(), CanonicalSegmentError> {
    output.extend_from_slice(&u32_len(properties.len(), "property map")?.to_le_bytes());
    for (key, value) in properties {
        match property_keys.as_deref_mut() {
            Some(dictionary) => output.extend_from_slice(&dictionary.intern(key)?.to_le_bytes()),
            None => encode_string(key, output)?,
        }
        let mut encoded_value = Vec::new();
        encode_value(value, &mut encoded_value, 1)?;
        if let Some(spills) = property_spills.as_deref_mut()
            && spills.should_spill(encoded_value.len())
        {
            let spill_id = spills.push(encoded_value)?;
            output.push(7);
            output.extend_from_slice(&spill_id.to_le_bytes());
        } else {
            output.extend_from_slice(&encoded_value);
        }
    }
    Ok(())
}

fn decode_record_properties(
    cursor: &mut SliceCursor<'_>,
    property_spills: Option<&PropertySpillReader>,
    property_keys: Option<&[String]>,
) -> Result<BTreeMap<String, Value>, CanonicalSegmentError> {
    let count = cursor.read_u32()? as usize;
    let mut properties = BTreeMap::new();
    for _ in 0..count {
        let key = match property_keys {
            Some(keys) => {
                let key_id = cursor.read_u32()?;
                keys.get(key_id as usize)
                    .ok_or_else(|| {
                        CanonicalSegmentError::Corrupt(format!(
                            "canonical record references unknown property key id {key_id}"
                        ))
                    })?
                    .clone()
            }
            None => cursor.read_string()?,
        };
        if properties
            .insert(
                key,
                decode_value_with_property_spills(cursor, 1, property_spills)?,
            )
            .is_some()
        {
            return Err(CanonicalSegmentError::Corrupt(
                "canonical property map has duplicate keys".to_string(),
            ));
        }
    }
    Ok(properties)
}

fn decode_projected_record_properties(
    cursor: &mut SliceCursor<'_>,
    property_spills: Option<&PropertySpillReader>,
    property_keys: Option<&[String]>,
    required_properties: &BTreeSet<String>,
) -> Result<BTreeMap<String, Value>, CanonicalSegmentError> {
    let count = cursor.read_u32()? as usize;
    let spill_count = property_spills.map(|reader| reader.manifest().value_count);
    let mut properties = BTreeMap::new();
    if let Some(keys) = property_keys {
        let mut seen = BTreeSet::new();
        for _ in 0..count {
            let key_id = cursor.read_u32()?;
            let key = keys.get(key_id as usize).ok_or_else(|| {
                CanonicalSegmentError::Corrupt(format!(
                    "canonical record references unknown property key id {key_id}"
                ))
            })?;
            if !seen.insert(key_id) {
                return Err(CanonicalSegmentError::Corrupt(format!(
                    "canonical property map has duplicate key id {key_id}"
                )));
            }
            if required_properties.contains(key) {
                properties.insert(
                    key.clone(),
                    decode_value_with_property_spills(cursor, 1, property_spills)?,
                );
            } else {
                validate_encoded_value(cursor, 1, spill_count)?;
            }
        }
    } else {
        let mut seen = BTreeSet::new();
        for _ in 0..count {
            let key = cursor.read_string()?;
            if !seen.insert(key.clone()) {
                return Err(CanonicalSegmentError::Corrupt(
                    "canonical property map has duplicate keys".to_string(),
                ));
            }
            if required_properties.contains(&key) {
                properties.insert(
                    key,
                    decode_value_with_property_spills(cursor, 1, property_spills)?,
                );
            } else {
                validate_encoded_value(cursor, 1, spill_count)?;
            }
        }
    }
    Ok(properties)
}

fn decode_properties_with_property_spills(
    cursor: &mut SliceCursor<'_>,
    depth: usize,
    property_spills: Option<&PropertySpillReader>,
) -> Result<BTreeMap<String, Value>, CanonicalSegmentError> {
    ensure_depth(depth)?;
    let count = cursor.read_u32()? as usize;
    let mut properties = BTreeMap::new();
    for _ in 0..count {
        let key = cursor.read_string()?;
        if properties
            .insert(
                key,
                decode_value_with_property_spills(
                    cursor,
                    depth.saturating_add(1),
                    property_spills,
                )?,
            )
            .is_some()
        {
            return Err(CanonicalSegmentError::Corrupt(
                "canonical property map has duplicate keys".to_string(),
            ));
        }
    }
    Ok(properties)
}

fn encode_value(
    value: &Value,
    output: &mut Vec<u8>,
    depth: usize,
) -> Result<(), CanonicalSegmentError> {
    ensure_depth(depth)?;
    match value {
        Value::Null => output.push(0),
        Value::Bool(value) => {
            output.push(1);
            output.push(u8::from(*value));
        }
        Value::Int(value) => {
            output.push(2);
            output.extend_from_slice(&value.to_le_bytes());
        }
        Value::Float(value) => {
            output.push(3);
            output.extend_from_slice(&value.to_bits().to_le_bytes());
        }
        Value::String(value) => {
            output.push(4);
            encode_string(value, output)?;
        }
        Value::Binary(value) => {
            output.push(8);
            output.extend_from_slice(&u32_len(value.len(), "binary")?.to_le_bytes());
            output.extend_from_slice(value);
        }
        Value::Uuid(value) => {
            output.push(9);
            output.extend_from_slice(value.as_bytes());
        }
        Value::List(values) => {
            output.push(5);
            output.extend_from_slice(&u32_len(values.len(), "value list")?.to_le_bytes());
            for value in values {
                encode_value(value, output, depth.saturating_add(1))?;
            }
        }
        Value::Map(values) => {
            output.push(6);
            encode_properties(values, output, depth.saturating_add(1))?;
        }
    }
    Ok(())
}

pub(crate) fn encode_standalone_value(value: &Value) -> Result<Vec<u8>, CanonicalSegmentError> {
    let mut encoded = Vec::new();
    encode_value(value, &mut encoded, 0)?;
    Ok(encoded)
}

fn decode_value(
    cursor: &mut SliceCursor<'_>,
    depth: usize,
) -> Result<Value, CanonicalSegmentError> {
    decode_value_with_property_spills(cursor, depth, None)
}

pub(crate) fn decode_standalone_value(encoded: &[u8]) -> Result<Value, CanonicalSegmentError> {
    let mut cursor = SliceCursor::new(encoded);
    let value = decode_value(&mut cursor, 0)?;
    if !cursor.is_empty() {
        return Err(CanonicalSegmentError::Corrupt(
            "standalone canonical value has trailing bytes".to_string(),
        ));
    }
    Ok(value)
}

// Residual-row wire schema (§3.5.1 field-tagged varint discipline). The row
// is `repeated field 1 (LEN): property`; each property envelope carries the
// interned key id plus exactly one length-delimited value field whose body
// is the canonical tagged-value encoding — one value mapping for every
// `Value` variant, so a future variant extends the canonical codec once
// instead of two synchronized mappings. Unknown field ids are skippable by
// wire type at both levels, so future readers within a storage version stay
// forward compatible.
const RESIDUAL_ROW_PROPERTY_FIELD: u32 = 1;
const RESIDUAL_KEY_ID_FIELD: u32 = 1;
/// Every value — scalar or nested — rides the canonical tagged-value codec
/// as the length-delimited field body; no second value encoding exists.
const RESIDUAL_CANONICAL_VALUE_FIELD: u32 = 2;

fn wire_corrupt(error: hawdb_core::error::HawDBError) -> CanonicalSegmentError {
    CanonicalSegmentError::Corrupt(format!("residual row wire payload is invalid: {error}"))
}

/// Exact canonical encoded length of one value, computed without
/// materializing any bytes — the streaming writers use it for
/// length-delimited framing.
fn encoded_value_len(value: &Value, depth: usize) -> Result<u64, CanonicalSegmentError> {
    ensure_depth(depth)?;
    Ok(match value {
        Value::Null => 1,
        Value::Bool(_) => 2,
        Value::Int(_) | Value::Float(_) => 9,
        Value::String(value) => {
            u32_len(value.len(), "string")?;
            1 + 4 + value.len() as u64
        }
        Value::Binary(value) => {
            u32_len(value.len(), "binary")?;
            1 + 4 + value.len() as u64
        }
        Value::Uuid(_) => 17,
        Value::List(values) => {
            u32_len(values.len(), "value list")?;
            let mut total = 1u64 + 4;
            for value in values {
                total = total.saturating_add(encoded_value_len(value, depth.saturating_add(1))?);
            }
            total
        }
        Value::Map(entries) => {
            u32_len(entries.len(), "property map")?;
            let mut total = 1u64 + 4;
            for (key, value) in entries {
                u32_len(key.len(), "string")?;
                total = total
                    .saturating_add(4 + key.len() as u64)
                    .saturating_add(encoded_value_len(value, depth.saturating_add(2))?);
            }
            total
        }
    })
}

pub(crate) fn validate_property_value(value: &Value) -> Result<(), CanonicalSegmentError> {
    encoded_value_len(value, 1).map(|_| ())
}

/// Streams one value's canonical tagged encoding into `out` without an
/// intermediate value-sized buffer: strings write their borrowed bytes
/// directly, so the transient memory is O(recursion frame), never
/// O(value). Byte-for-byte identical to [`encode_value`].
fn write_value_streaming<W: Write + ?Sized>(
    out: &mut W,
    value: &Value,
    depth: usize,
) -> Result<(), CanonicalSegmentError> {
    ensure_depth(depth)?;
    match value {
        Value::Null => out.write_all(&[0])?,
        Value::Bool(value) => out.write_all(&[1, u8::from(*value)])?,
        Value::Int(value) => {
            out.write_all(&[2])?;
            out.write_all(&value.to_le_bytes())?;
        }
        Value::Float(value) => {
            out.write_all(&[3])?;
            out.write_all(&value.to_bits().to_le_bytes())?;
        }
        Value::String(value) => {
            out.write_all(&[4])?;
            out.write_all(&u32_len(value.len(), "string")?.to_le_bytes())?;
            out.write_all(value.as_bytes())?;
        }
        Value::Binary(value) => {
            out.write_all(&[8])?;
            out.write_all(&u32_len(value.len(), "binary")?.to_le_bytes())?;
            out.write_all(value)?;
        }
        Value::Uuid(value) => {
            out.write_all(&[9])?;
            out.write_all(value.as_bytes())?;
        }
        Value::List(values) => {
            out.write_all(&[5])?;
            out.write_all(&u32_len(values.len(), "value list")?.to_le_bytes())?;
            for value in values {
                write_value_streaming(out, value, depth.saturating_add(1))?;
            }
        }
        Value::Map(entries) => {
            out.write_all(&[6])?;
            out.write_all(&u32_len(entries.len(), "property map")?.to_le_bytes())?;
            for (key, value) in entries {
                out.write_all(&u32_len(key.len(), "string")?.to_le_bytes())?;
                out.write_all(key.as_bytes())?;
                write_value_streaming(out, value, depth.saturating_add(2))?;
            }
        }
    }
    Ok(())
}

fn varint_len(value: u64) -> u64 {
    let mut scratch = Vec::with_capacity(10);
    wire::encode_varint_u64(value, &mut scratch);
    scratch.len() as u64
}

fn residual_property_envelope_len(
    key_id: u32,
    value: &Value,
) -> Result<u64, CanonicalSegmentError> {
    let value_len = encoded_value_len(value, 1)?;
    let mut key_field = Vec::new();
    wire::encode_varint_field(RESIDUAL_KEY_ID_FIELD, u64::from(key_id), &mut key_field);
    let mut value_tag = Vec::new();
    wire::encode_tag(
        RESIDUAL_CANONICAL_VALUE_FIELD,
        wire::WIRE_TYPE_LEN,
        &mut value_tag,
    );
    Ok((key_field.len() + value_tag.len()) as u64 + varint_len(value_len) + value_len)
}

/// Exact encoded length of one residual row, computed without
/// materializing it.
pub fn residual_row_properties_encoded_len(
    entries: &[(u32, &Value)],
) -> Result<u64, CanonicalSegmentError> {
    let mut total = 0u64;
    let mut row_tag = Vec::new();
    wire::encode_tag(
        RESIDUAL_ROW_PROPERTY_FIELD,
        wire::WIRE_TYPE_LEN,
        &mut row_tag,
    );
    for (key_id, value) in entries {
        let envelope_len = residual_property_envelope_len(*key_id, value)?;
        total = total
            .saturating_add(row_tag.len() as u64)
            .saturating_add(varint_len(envelope_len))
            .saturating_add(envelope_len);
    }
    Ok(total)
}

/// Streams one residual row into `out`: the small framing pieces are built
/// in tiny scratch buffers while every value's bytes stream through the
/// canonical writer, so transient memory is O(recursion frame), never
/// O(value). Byte-for-byte identical to
/// [`encode_residual_row_properties`].
pub fn write_residual_row_properties<W: Write + ?Sized>(
    out: &mut W,
    entries: &[(u32, &Value)],
) -> Result<(), CanonicalSegmentError> {
    for (key_id, value) in entries {
        let value_len = encoded_value_len(value, 1)?;
        let mut framing = Vec::new();
        wire::encode_tag(
            RESIDUAL_ROW_PROPERTY_FIELD,
            wire::WIRE_TYPE_LEN,
            &mut framing,
        );
        wire::encode_varint_u64(
            residual_property_envelope_len(*key_id, value)?,
            &mut framing,
        );
        wire::encode_varint_field(RESIDUAL_KEY_ID_FIELD, u64::from(*key_id), &mut framing);
        wire::encode_tag(
            RESIDUAL_CANONICAL_VALUE_FIELD,
            wire::WIRE_TYPE_LEN,
            &mut framing,
        );
        wire::encode_varint_u64(value_len, &mut framing);
        out.write_all(&framing)?;
        write_value_streaming(out, value, 1)?;
    }
    Ok(())
}

/// Encodes one residual-column row for the columnar shadow (§3.1.3) in the
/// §3.5.1 field-tagged varint style: each property is a length-delimited
/// envelope of `(varint key id, length-delimited canonical tagged value)`.
/// Buffered convenience over [`write_residual_row_properties`].
pub fn encode_residual_row_properties(
    entries: &[(u32, &Value)],
) -> Result<Vec<u8>, CanonicalSegmentError> {
    let mut bytes = Vec::new();
    write_residual_row_properties(&mut bytes, entries)?;
    Ok(bytes)
}

fn decode_residual_property(body: &[u8]) -> Result<(u32, Value), CanonicalSegmentError> {
    let mut pos = 0usize;
    let mut key_id = None;
    let mut value = None;
    while pos < body.len() {
        let (field_id, wire_type) = wire::decode_tag(body, &mut pos).map_err(wire_corrupt)?;
        match (field_id, wire_type) {
            (RESIDUAL_KEY_ID_FIELD, wire::WIRE_TYPE_VARINT) => {
                let raw = wire::decode_varint_u64(body, &mut pos).map_err(wire_corrupt)?;
                let id = u32::try_from(raw).map_err(|_| {
                    CanonicalSegmentError::Corrupt(
                        "residual row property key id overflows u32".to_string(),
                    )
                })?;
                if key_id.replace(id).is_some() {
                    return Err(CanonicalSegmentError::Corrupt(
                        "residual row property repeats its key id".to_string(),
                    ));
                }
            }
            (RESIDUAL_CANONICAL_VALUE_FIELD, wire::WIRE_TYPE_LEN) => {
                let encoded = wire::decode_len_body(body, &mut pos).map_err(wire_corrupt)?;
                let mut cursor = SliceCursor::new(encoded);
                let decoded = decode_value(&mut cursor, 1)?;
                if !cursor.is_empty() {
                    return Err(CanonicalSegmentError::Corrupt(
                        "residual row canonical value has trailing bytes".to_string(),
                    ));
                }
                if value.replace(decoded).is_some() {
                    return Err(CanonicalSegmentError::Corrupt(
                        "residual row property carries two values".to_string(),
                    ));
                }
            }
            // Unknown field ids are future extensions: skip by wire type.
            (_, wire_type) => {
                wire::skip_field(body, &mut pos, wire_type).map_err(wire_corrupt)?;
            }
        }
    }
    match (key_id, value) {
        (Some(key_id), Some(value)) => Ok((key_id, value)),
        _ => Err(CanonicalSegmentError::Corrupt(
            "residual row property is missing its key id or value".to_string(),
        )),
    }
}

/// Decodes one residual-column row back to its `(interned key id, value)`
/// pairs, skipping unknown fields and rejecting duplicates and truncation.
pub fn decode_residual_row_properties(
    bytes: &[u8],
) -> Result<Vec<(u32, Value)>, CanonicalSegmentError> {
    let mut pos = 0usize;
    let mut entries = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    while pos < bytes.len() {
        let (field_id, wire_type) = wire::decode_tag(bytes, &mut pos).map_err(wire_corrupt)?;
        if field_id == RESIDUAL_ROW_PROPERTY_FIELD && wire_type == wire::WIRE_TYPE_LEN {
            let body = wire::decode_len_body(bytes, &mut pos).map_err(wire_corrupt)?;
            let (key_id, value) = decode_residual_property(body)?;
            if !seen.insert(key_id) {
                return Err(CanonicalSegmentError::Corrupt(format!(
                    "residual row repeats property key id {key_id}"
                )));
            }
            entries.push((key_id, value));
        } else {
            // Unknown row-level field ids are future extensions.
            wire::skip_field(bytes, &mut pos, wire_type).map_err(wire_corrupt)?;
        }
    }
    Ok(entries)
}

fn decode_value_with_property_spills(
    cursor: &mut SliceCursor<'_>,
    depth: usize,
    property_spills: Option<&PropertySpillReader>,
) -> Result<Value, CanonicalSegmentError> {
    ensure_depth(depth)?;
    match cursor.read_u8()? {
        0 => Ok(Value::Null),
        1 => match cursor.read_u8()? {
            0 => Ok(Value::Bool(false)),
            1 => Ok(Value::Bool(true)),
            value => Err(CanonicalSegmentError::Corrupt(format!(
                "invalid canonical boolean {value}"
            ))),
        },
        2 => Ok(Value::Int(cursor.read_i64()?)),
        3 => Ok(Value::Float(f64::from_bits(cursor.read_u64()?))),
        4 => Ok(Value::String(cursor.read_string()?)),
        5 => {
            let count = cursor.read_u32()? as usize;
            let mut values = Vec::with_capacity(count.min(1024));
            for _ in 0..count {
                values.push(decode_value_with_property_spills(
                    cursor,
                    depth.saturating_add(1),
                    property_spills,
                )?);
            }
            Ok(Value::List(values))
        }
        6 => Ok(Value::Map(decode_properties_with_property_spills(
            cursor,
            depth.saturating_add(1),
            property_spills,
        )?)),
        7 => {
            let spill_id = cursor.read_u64()?;
            let reader = property_spills.ok_or_else(|| {
                CanonicalSegmentError::Corrupt(format!(
                    "canonical value references property spill {spill_id} without a published spill artifact"
                ))
            })?;
            let encoded = reader.get(spill_id)?.ok_or_else(|| {
                CanonicalSegmentError::Corrupt(format!(
                    "canonical value references missing property spill {spill_id}"
                ))
            })?;
            let mut spilled = SliceCursor::new(&encoded);
            let value = decode_value(&mut spilled, depth)?;
            if !spilled.is_empty() {
                return Err(CanonicalSegmentError::Corrupt(format!(
                    "property spill {spill_id} has trailing bytes"
                )));
            }
            Ok(value)
        }
        8 => {
            let length = cursor.read_u32()? as usize;
            Ok(Value::Binary(cursor.read_exact(length)?.to_vec()))
        }
        9 => Ok(Value::Uuid(hawdb_core::Uuid::from_bytes(
            cursor
                .read_exact(16)?
                .try_into()
                .expect("UUID has a fixed length"),
        ))),
        tag => Err(CanonicalSegmentError::Corrupt(format!(
            "unknown canonical value tag {tag}"
        ))),
    }
}

fn encode_string(value: &str, output: &mut Vec<u8>) -> Result<(), CanonicalSegmentError> {
    output.extend_from_slice(&u32_len(value.len(), "string")?.to_le_bytes());
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn ensure_depth(depth: usize) -> Result<(), CanonicalSegmentError> {
    if depth > MAX_VALUE_DEPTH {
        return Err(CanonicalSegmentError::Corrupt(format!(
            "canonical value nesting exceeds {MAX_VALUE_DEPTH}"
        )));
    }
    Ok(())
}

fn u32_len(length: usize, name: &str) -> Result<u32, CanonicalSegmentError> {
    u32::try_from(length).map_err(|_| {
        CanonicalSegmentError::Corrupt(format!("{name} length exceeds the storage format"))
    })
}

fn read_u64_at(encoded: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        encoded[offset..offset + 8]
            .try_into()
            .expect("validated canonical descriptor has fixed-width u64"),
    )
}

fn read_u32_at(encoded: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        encoded[offset..offset + 4]
            .try_into()
            .expect("validated canonical descriptor has fixed-width u32"),
    )
}

fn parse_u64(value: &str, name: &str) -> Result<u64, CanonicalSegmentError> {
    value
        .parse()
        .map_err(|_| CanonicalSegmentError::Corrupt(format!("invalid canonical {name}: {value}")))
}

fn parse_u32(value: &str, name: &str) -> Result<u32, CanonicalSegmentError> {
    value
        .parse()
        .map_err(|_| CanonicalSegmentError::Corrupt(format!("invalid canonical {name}: {value}")))
}

fn encode_property_key_hex(key: &str) -> String {
    key.bytes().map(|byte| format!("{byte:02x}")).collect()
}

fn decode_property_key_hex(value: &str) -> Result<String, CanonicalSegmentError> {
    if !value.is_ascii() || !value.len().is_multiple_of(2) {
        return Err(CanonicalSegmentError::Corrupt(
            "canonical property key has an invalid encoded length".to_string(),
        ));
    }
    let mut bytes = Vec::with_capacity(value.len() / 2);
    for index in (0..value.len()).step_by(2) {
        bytes.push(
            u8::from_str_radix(&value[index..index + 2], 16).map_err(|_| {
                CanonicalSegmentError::Corrupt(
                    "canonical property key contains invalid hexadecimal data".to_string(),
                )
            })?,
        );
    }
    String::from_utf8(bytes).map_err(|error| {
        CanonicalSegmentError::Corrupt(format!("canonical property key is not UTF-8: {error}"))
    })
}

fn set_once<T>(target: &mut Option<T>, value: T, name: &str) -> Result<(), CanonicalSegmentError> {
    if target.replace(value).is_some() {
        return Err(CanonicalSegmentError::Corrupt(format!(
            "canonical manifest has duplicate {name}"
        )));
    }
    Ok(())
}

fn required<T>(value: Option<T>, name: &str) -> Result<T, CanonicalSegmentError> {
    value.ok_or_else(|| {
        CanonicalSegmentError::Corrupt(format!("canonical manifest is missing {name}"))
    })
}

const fn segment_header_len() -> usize {
    8 + 1 + 8 + 8 + 4
}

struct SliceCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> SliceCursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }

    fn read_exact(&mut self, length: usize) -> Result<&'a [u8], CanonicalSegmentError> {
        let end = self.offset.checked_add(length).ok_or_else(|| {
            CanonicalSegmentError::Corrupt("canonical decode offset overflow".to_string())
        })?;
        let bytes = self.bytes.get(self.offset..end).ok_or_else(|| {
            CanonicalSegmentError::Corrupt("canonical record is truncated".to_string())
        })?;
        self.offset = end;
        Ok(bytes)
    }

    fn read_u8(&mut self) -> Result<u8, CanonicalSegmentError> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u32(&mut self) -> Result<u32, CanonicalSegmentError> {
        Ok(u32::from_le_bytes(
            self.read_exact(4)?.try_into().expect("fixed-width u32"),
        ))
    }

    fn read_u64(&mut self) -> Result<u64, CanonicalSegmentError> {
        Ok(u64::from_le_bytes(
            self.read_exact(8)?.try_into().expect("fixed-width u64"),
        ))
    }

    fn read_i64(&mut self) -> Result<i64, CanonicalSegmentError> {
        Ok(i64::from_le_bytes(
            self.read_exact(8)?.try_into().expect("fixed-width i64"),
        ))
    }

    fn read_string(&mut self) -> Result<String, CanonicalSegmentError> {
        let length = self.read_u32()? as usize;
        String::from_utf8(self.read_exact(length)?.to_vec()).map_err(|error| {
            CanonicalSegmentError::Corrupt(format!("canonical string is not UTF-8: {error}"))
        })
    }
}

fn write_hashed(
    file: &mut File,
    digest: &mut IntegrityHasher,
    bytes: &[u8],
) -> Result<(), CanonicalSegmentError> {
    file.write_all(bytes)?;
    digest.update(bytes);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PersistentPropertySpillDescriptorTree, PropertySpillConfig};

    fn collect_descriptors(
        reader: &CanonicalSegmentReader,
        kind: CanonicalSegmentKind,
    ) -> Vec<CanonicalSegmentDescriptor> {
        let mut descriptors = Vec::new();
        let (_, control) = reader
            .scan_descriptor_kind(kind, |descriptor| {
                descriptors.push(descriptor.clone());
                Ok(CanonicalScanControl::Continue)
            })
            .unwrap();
        assert_eq!(control, CanonicalScanControl::Continue);
        descriptors
    }

    fn replace_descriptor_tree(
        path: &Path,
        manifest: &mut CanonicalSegmentManifest,
        mut descriptors: Vec<CanonicalSegmentDescriptor>,
    ) {
        descriptors.sort_by_key(CanonicalSegmentDescriptor::descriptor_tree_key);
        let tree =
            PersistentCanonicalSegmentDescriptorTree::for_artifact(path, manifest.generation);
        let (paths, config) = tree.into_parts();
        let _ = std::fs::remove_file(&paths.page_artifact);
        let _ = std::fs::remove_file(&paths.root_manifest);
        let mut builder = GraphDescriptorTreeBuilder::create(
            paths,
            GraphDescriptorKind::CanonicalSegment,
            manifest.generation.0,
            manifest.source_commit_epoch,
            CANONICAL_SEGMENT_DESCRIPTOR_ARTIFACT_ID,
            config,
        )
        .unwrap();
        for descriptor in descriptors {
            builder
                .push(
                    descriptor.descriptor_tree_key(),
                    descriptor.encode_descriptor_tree_value().unwrap(),
                )
                .unwrap();
        }
        let output = builder.finish().unwrap().publish().unwrap();
        manifest.descriptor_root_artifact = output.root_artifact;
    }

    #[test]
    fn canonical_segments_round_trip_with_bounded_cache() {
        let path = unique_path("round_trip");
        let nodes = (0..200)
            .map(|id| NodeRecord {
                id: NodeId(id),
                labels: BTreeSet::from([LabelId((id % 3) as u32)]),
                properties: BTreeMap::from([
                    ("id".to_string(), Value::Int(id as i64)),
                    ("body".to_string(), Value::String("x".repeat(128))),
                ]),
            })
            .collect::<Vec<_>>();
        let relationships = (0..100)
            .map(|id| RelRecord {
                id: RelId(id),
                source: NodeId(id),
                target: NodeId(id + 1),
                rel_type: RelTypeId(1),
                properties: BTreeMap::from([("weight".to_string(), Value::Int(id as i64))]),
            })
            .collect::<Vec<_>>();
        let config = CanonicalSegmentConfig {
            target_segment_bytes: NonZeroU64::new(4096).unwrap(),
            max_record_bytes: NonZeroU64::new(2048).unwrap(),
        };
        let manifest = CanonicalSegmentWriter::new(config)
            .write(&path, ManifestGeneration(7), &nodes, &relationships)
            .unwrap();
        assert_eq!(
            CanonicalSegmentManifest::decode(&manifest.encode().unwrap()).unwrap(),
            manifest
        );
        assert!(manifest.segment_count > 2);
        assert!(!manifest.encode().unwrap().contains("\nsegment\t"));
        let cache = Arc::new(SegmentCache::new(8192));
        let reader = CanonicalSegmentReader::open(
            &path,
            manifest,
            Arc::clone(&cache),
            StoreId(11),
            NonZeroU64::new(4096).unwrap(),
        )
        .unwrap();
        assert_eq!(
            reader.get_node(NodeId(117)).unwrap(),
            Some(nodes[117].clone())
        );
        assert_eq!(
            reader.get_relationship(RelId(44)).unwrap(),
            Some(relationships[44].clone())
        );
        let mut scanned = Vec::new();
        let report = reader
            .scan_nodes(|node| {
                scanned.push(node.id);
                Ok(())
            })
            .unwrap();
        assert_eq!(scanned.len(), nodes.len());
        assert!(report.peak_segment_bytes <= 4096);
        assert!(cache.snapshot().resident_bytes <= 8192);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn canonical_descriptor_demand_reader_is_lazy_and_exhaustively_scrubbed() {
        let path = unique_path("descriptor_shadow");
        let nodes = (0..96)
            .map(|id| NodeRecord {
                id: NodeId(id * 2),
                labels: BTreeSet::from([LabelId(1)]),
                properties: BTreeMap::from([(
                    "payload".to_string(),
                    Value::String("x".repeat(96)),
                )]),
            })
            .collect::<Vec<_>>();
        let config = CanonicalSegmentConfig {
            target_segment_bytes: NonZeroU64::new(512).unwrap(),
            max_record_bytes: NonZeroU64::new(256).unwrap(),
        };
        let manifest = CanonicalSegmentWriter::new(config)
            .write(
                &path,
                ManifestGeneration(17),
                &nodes,
                std::iter::empty::<RelRecord>(),
            )
            .unwrap();
        assert!(manifest.segment_count > 1);
        let cache = Arc::new(SegmentCache::new(8 * 1024 * 1024));
        let reader = CanonicalSegmentReader::open(
            &path,
            manifest.clone(),
            Arc::clone(&cache),
            StoreId(17),
            NonZeroU64::new(512).unwrap(),
        )
        .unwrap();
        assert_eq!(cache.snapshot().resident_bytes, 0);
        let descriptors = collect_descriptors(&reader, CanonicalSegmentKind::Nodes);
        assert_eq!(descriptors.len() as u64, manifest.node_segment_count);
        for descriptor in descriptors {
            let key = descriptor.descriptor_tree_key();
            let value = descriptor.encode_descriptor_tree_value().unwrap();
            assert_eq!(
                CanonicalSegmentDescriptor::decode_descriptor_tree_entry(&key, &value).unwrap(),
                descriptor
            );
        }
        drop(reader);
        let scrub_cache = Arc::new(SegmentCache::new(0));
        let scrub_reader = CanonicalSegmentReader::open(
            &path,
            manifest.clone(),
            Arc::clone(&scrub_cache),
            StoreId(17),
            NonZeroU64::new(512).unwrap(),
        )
        .unwrap();
        let report = scrub_reader.deep_scrub().unwrap();
        assert_eq!(report.descriptors_checked, manifest.segment_count);
        assert_eq!(report.segments_checked, manifest.segment_count);
        assert_eq!(report.records_checked, manifest.node_count);
        assert!(report.descriptor_pages_checked > 0);
        assert_eq!(report.canonical_bytes_hashed, manifest.artifact_len);
        assert_eq!(scrub_cache.snapshot().resident_bytes, 0);

        let descriptor_tree =
            PersistentCanonicalSegmentDescriptorTree::for_artifact(&path, manifest.generation);
        let (descriptor_paths, _) = descriptor_tree.into_parts();
        let mut pages = std::fs::read(&descriptor_paths.page_artifact).unwrap();
        pages[0] ^= 0xff;
        std::fs::write(&descriptor_paths.page_artifact, pages).unwrap();
        assert!(scrub_reader.deep_scrub().is_err());
        assert!(scrub_reader.is_poisoned());

        drop(scrub_reader);
        remove_fixture(&path, manifest.generation);
    }

    #[test]
    fn projected_node_read_returns_only_requested_properties() {
        let path = unique_path("projected_node_read");
        let node = NodeRecord {
            id: NodeId(7),
            labels: BTreeSet::from([LabelId(1)]),
            properties: BTreeMap::from([
                ("rank".to_string(), Value::Int(9)),
                ("title".to_string(), Value::String("selected".to_string())),
                ("content".to_string(), Value::String("x".repeat(128 * 1024))),
            ]),
        };
        let manifest = CanonicalSegmentWriter::new(CanonicalSegmentConfig::default())
            .write(
                &path,
                ManifestGeneration(71),
                std::iter::once(node),
                std::iter::empty::<RelRecord>(),
            )
            .unwrap();
        let reader = CanonicalSegmentReader::open(
            &path,
            manifest.clone(),
            Arc::new(SegmentCache::new(1024 * 1024)),
            StoreId(71),
            NonZeroU64::new(1024 * 1024).unwrap(),
        )
        .unwrap();

        let projected = reader
            .get_projected_node(
                NodeId(7),
                &BTreeSet::from(["rank".to_string(), "title".to_string()]),
            )
            .unwrap()
            .unwrap();

        assert_eq!(projected.id, NodeId(7));
        assert_eq!(projected.labels, BTreeSet::from([LabelId(1)]));
        assert_eq!(
            projected.properties,
            BTreeMap::from([
                ("rank".to_string(), Value::Int(9)),
                ("title".to_string(), Value::String("selected".to_string())),
            ])
        );
        remove_fixture(&path, manifest.generation);
    }

    #[test]
    fn deep_scrub_requires_the_exact_property_spill_closure() {
        let generation = ManifestGeneration(29);
        let canonical_path = unique_path("spill_closure_canonical");
        let spill_path = unique_path("spill_closure_values");
        let spill_paths = GraphDescriptorTreePaths::new(
            spill_path.with_extension("descriptors.pages.hawdb"),
            spill_path.with_extension("descriptors.root.hawdb"),
        );
        let spill_config = PropertySpillConfig {
            spill_threshold_bytes: NonZeroU64::new(1).unwrap(),
            target_block_bytes: NonZeroU64::new(4096).unwrap(),
            max_value_bytes: NonZeroU64::new(4096).unwrap(),
        };
        let nodes = (0..2).map(|id| {
            Ok(NodeRecord {
                id: NodeId(id),
                labels: BTreeSet::from([LabelId(1)]),
                properties: BTreeMap::from([(
                    "payload".to_string(),
                    Value::String(format!("payload-{id}")),
                )]),
            })
        });
        let (manifest, spill_output) = CanonicalSegmentWriter::new(CanonicalSegmentConfig {
            target_segment_bytes: NonZeroU64::new(4096).unwrap(),
            max_record_bytes: NonZeroU64::new(4096).unwrap(),
        })
        .write_fallible_with_property_spills(
            &canonical_path,
            generation,
            nodes,
            std::iter::empty(),
            PropertySpillWriteOptions {
                artifact_path: &spill_path,
                source_commit_epoch: generation.0,
                config: spill_config,
                descriptor_tree: PersistentPropertySpillDescriptorTree::new(
                    spill_paths.clone(),
                    GraphDescriptorTreeBuildConfig::default(),
                ),
            },
        )
        .unwrap();
        assert_eq!(spill_output.manifest.value_count, 2);

        let reader_without_spills = CanonicalSegmentReader::open(
            &canonical_path,
            manifest.clone(),
            Arc::new(SegmentCache::new(0)),
            StoreId(29),
            NonZeroU64::new(4096).unwrap(),
        )
        .unwrap();
        let error = reader_without_spills.deep_scrub().unwrap_err();
        assert!(error
            .to_string()
            .contains("without a selected spill artifact"));
        assert!(reader_without_spills.is_poisoned());

        let short_spill_path = unique_path("spill_closure_short_values");
        let short_spill_paths = GraphDescriptorTreePaths::new(
            short_spill_path.with_extension("descriptors.pages.hawdb"),
            short_spill_path.with_extension("descriptors.root.hawdb"),
        );
        let mut short_spill_writer = PropertySpillWriter::create(
            short_spill_path.with_extension("hawdb.tmp"),
            generation,
            generation.0,
            spill_config,
            PersistentPropertySpillDescriptorTree::new(
                short_spill_paths.clone(),
                GraphDescriptorTreeBuildConfig::default(),
            ),
        )
        .unwrap();
        assert_eq!(short_spill_writer.push(vec![0]).unwrap(), 0);
        let short_spill_output = short_spill_writer
            .finish()
            .unwrap()
            .publish(&short_spill_path)
            .unwrap();
        let short_property_spills = PropertySpillReader::open(
            &short_spill_path,
            short_spill_output.manifest,
            PersistentPropertySpillDescriptorTree::new(
                short_spill_paths,
                GraphDescriptorTreeBuildConfig::default(),
            ),
            Arc::new(SegmentCache::new(0)),
            StoreId(29),
            NonZeroU64::new(8192).unwrap(),
        )
        .unwrap();
        let reader_with_short_spills = CanonicalSegmentReader::open_with_property_spills(
            &canonical_path,
            manifest.clone(),
            Arc::new(SegmentCache::new(0)),
            StoreId(29),
            NonZeroU64::new(4096).unwrap(),
            short_property_spills,
        )
        .unwrap();
        let error = reader_with_short_spills.deep_scrub().unwrap_err();
        assert!(error.to_string().contains("outside its selected artifact"));
        assert!(reader_with_short_spills.is_poisoned());

        let property_spills = PropertySpillReader::open(
            &spill_path,
            spill_output.manifest,
            PersistentPropertySpillDescriptorTree::new(
                spill_paths,
                GraphDescriptorTreeBuildConfig::default(),
            ),
            Arc::new(SegmentCache::new(0)),
            StoreId(29),
            NonZeroU64::new(8192).unwrap(),
        )
        .unwrap();
        let reader = CanonicalSegmentReader::open_with_property_spills(
            &canonical_path,
            manifest,
            Arc::new(SegmentCache::new(0)),
            StoreId(29),
            NonZeroU64::new(4096).unwrap(),
            property_spills,
        )
        .unwrap();
        assert_eq!(reader.deep_scrub().unwrap().records_checked, 2);
    }

    #[test]
    fn compact_manifest_residency_is_independent_of_segment_count() {
        let small_path = unique_path("compact_manifest_small");
        let large_path = unique_path("compact_manifest_large");
        let config = CanonicalSegmentConfig {
            target_segment_bytes: NonZeroU64::new(256).unwrap(),
            max_record_bytes: NonZeroU64::new(512).unwrap(),
        };
        let make_node = |id| NodeRecord {
            id: NodeId(id),
            labels: BTreeSet::from([LabelId(1)]),
            properties: BTreeMap::from([("payload".to_string(), Value::String("x".repeat(96)))]),
        };
        let small = CanonicalSegmentWriter::new(config)
            .write(
                &small_path,
                ManifestGeneration(31),
                [make_node(1)],
                std::iter::empty::<RelRecord>(),
            )
            .unwrap();
        let large = CanonicalSegmentWriter::new(config)
            .write(
                &large_path,
                ManifestGeneration(32),
                (0..1_024).map(make_node),
                std::iter::empty::<RelRecord>(),
            )
            .unwrap();
        assert!(large.node_segment_count > 256);
        assert!(large.artifact_len > small.artifact_len * 256);
        let small_manifest_bytes = small.encode().unwrap().len();
        let large_manifest_bytes = large.encode().unwrap().len();
        assert!(large_manifest_bytes.abs_diff(small_manifest_bytes) < 64);
        remove_fixture(&small_path, small.generation);
        remove_fixture(&large_path, large.generation);
    }

    #[test]
    fn canonical_segment_admission_does_not_poison_the_reader() {
        let path = unique_path("segment_admission");
        let manifest = CanonicalSegmentWriter::new(CanonicalSegmentConfig::default())
            .write(
                &path,
                ManifestGeneration(33),
                [NodeRecord {
                    id: NodeId(1),
                    labels: BTreeSet::from([LabelId(1)]),
                    properties: BTreeMap::new(),
                }],
                std::iter::empty::<RelRecord>(),
            )
            .unwrap();
        let reader = CanonicalSegmentReader::open(
            &path,
            manifest.clone(),
            Arc::new(SegmentCache::new(1024 * 1024)),
            StoreId(33),
            NonZeroU64::new(1).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            reader.get_node(NodeId(1)),
            Err(CanonicalSegmentError::SegmentTooLarge { .. })
        ));
        assert!(!reader.is_poisoned());
        assert!(matches!(
            reader.get_node(NodeId(1)),
            Err(CanonicalSegmentError::SegmentTooLarge { .. })
        ));
        drop(reader);
        remove_fixture(&path, manifest.generation);
    }

    #[test]
    fn canonical_scan_streams_more_than_one_descriptor_batch() {
        let path = unique_path("descriptor_batches");
        let config = CanonicalSegmentConfig {
            target_segment_bytes: NonZeroU64::new(256).unwrap(),
            max_record_bytes: NonZeroU64::new(512).unwrap(),
        };
        let manifest = CanonicalSegmentWriter::new(config)
            .write(
                &path,
                ManifestGeneration(34),
                (0..1_024).map(|id| NodeRecord {
                    id: NodeId(id),
                    labels: BTreeSet::from([LabelId(1)]),
                    properties: BTreeMap::from([(
                        "payload".to_string(),
                        Value::String("x".repeat(96)),
                    )]),
                }),
                std::iter::empty::<RelRecord>(),
            )
            .unwrap();
        assert!(manifest.node_segment_count > DESCRIPTOR_SCAN_BATCH);
        let cache = Arc::new(SegmentCache::new(64 * 1024));
        let reader = CanonicalSegmentReader::open(
            &path,
            manifest.clone(),
            Arc::clone(&cache),
            StoreId(34),
            NonZeroU64::new(512).unwrap(),
        )
        .unwrap();
        let mut rows = 0u64;
        let report = reader
            .scan_nodes(|_| {
                rows = rows.saturating_add(1);
                Ok(())
            })
            .unwrap();
        assert_eq!(rows, manifest.node_count);
        assert_eq!(report.segments_considered, manifest.node_segment_count);
        assert_eq!(report.descriptors_examined, manifest.node_segment_count);
        assert!(report.descriptor_pages_visited > 1);
        assert!(cache.snapshot().resident_bytes <= 64 * 1024);
        drop(reader);
        remove_fixture(&path, manifest.generation);
    }

    #[test]
    fn canonical_open_rejects_descriptor_root_binding_drift() {
        let path = unique_path("descriptor_root_drift");
        let nodes = [NodeRecord {
            id: NodeId(1),
            labels: BTreeSet::from([LabelId(1)]),
            properties: BTreeMap::new(),
        }];
        let manifest = CanonicalSegmentWriter::new(CanonicalSegmentConfig::default())
            .write(
                &path,
                ManifestGeneration(18),
                nodes,
                std::iter::empty::<RelRecord>(),
            )
            .unwrap();
        let descriptor_tree =
            PersistentCanonicalSegmentDescriptorTree::for_artifact(&path, manifest.generation);
        let (descriptor_paths, _) = descriptor_tree.into_parts();
        let mut root = std::fs::read(&descriptor_paths.root_manifest).unwrap();
        root[24] ^= 0x01;
        std::fs::write(&descriptor_paths.root_manifest, root).unwrap();

        assert!(matches!(
            CanonicalSegmentReader::open(
                &path,
                manifest.clone(),
                Arc::new(SegmentCache::new(1024 * 1024)),
                StoreId(18),
                NonZeroU64::new(16 * 1024 * 1024).unwrap(),
            ),
            Err(CanonicalSegmentError::DescriptorTree(_))
        ));
        remove_fixture(&path, manifest.generation);
    }

    #[test]
    fn canonical_manifest_rejects_incomplete_segment_counts() {
        let path = unique_path("incomplete_manifest");
        let nodes = vec![NodeRecord {
            id: NodeId(1),
            labels: BTreeSet::from([LabelId(1)]),
            properties: BTreeMap::from([("id".to_string(), Value::Int(1))]),
        }];
        let relationships = vec![RelRecord {
            id: RelId(2),
            source: NodeId(1),
            target: NodeId(3),
            rel_type: RelTypeId(1),
            properties: BTreeMap::new(),
        }];
        let manifest = CanonicalSegmentWriter::new(CanonicalSegmentConfig::default())
            .write(&path, ManifestGeneration(8), &nodes, &relationships)
            .unwrap();
        let encoded = manifest.encode().unwrap();
        let body = encoded
            .lines()
            .filter(|line| {
                !line.starts_with("checksum\t") && !line.starts_with("node_segment_count\t")
            })
            .map(str::to_string)
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        let incomplete = format!("{body}checksum\t{}\n", content_digest(body.as_bytes()).0);
        let error = CanonicalSegmentManifest::decode(&incomplete).unwrap_err();
        assert!(error.to_string().contains("missing node segment count"));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn endpoint_bloom_prunes_relationship_segments_without_false_negatives() {
        let path = unique_path("endpoint_bloom");
        let relationships = (0..256)
            .map(|id| RelRecord {
                id: RelId(id),
                source: NodeId(id),
                target: NodeId(10_000 + id),
                rel_type: RelTypeId(1),
                properties: BTreeMap::from([(
                    "payload".to_string(),
                    Value::String("x".repeat(96)),
                )]),
            })
            .collect::<Vec<_>>();
        let config = CanonicalSegmentConfig {
            target_segment_bytes: NonZeroU64::new(512).unwrap(),
            max_record_bytes: NonZeroU64::new(256).unwrap(),
        };
        let manifest = CanonicalSegmentWriter::new(config)
            .write(
                &path,
                ManifestGeneration(9),
                std::iter::empty::<NodeRecord>(),
                &relationships,
            )
            .unwrap();
        let relationship_segment_count = manifest.relationship_segment_count;
        assert!(relationship_segment_count > 8);
        let reader = CanonicalSegmentReader::open(
            &path,
            manifest,
            Arc::new(SegmentCache::new(4096)),
            StoreId(12),
            NonZeroU64::new(512).unwrap(),
        )
        .unwrap();
        let mut found = Vec::new();
        let (report, control) = reader
            .scan_relationships_for_endpoint_control(
                NodeId(127),
                CanonicalEndpointDirection::Source,
                Some(RelTypeId(1)),
                |relationship| {
                    found.push(relationship.id);
                    Ok(CanonicalScanControl::Continue)
                },
            )
            .unwrap();
        assert_eq!(control, CanonicalScanControl::Continue);
        assert_eq!(found, vec![RelId(127)]);
        assert!(report.segments_read < relationship_segment_count);
        assert_eq!(report.segments_considered, relationship_segment_count);
        assert_eq!(
            report.segments_pruned,
            report.segments_considered - report.segments_read
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn endpoint_bloom_scales_with_default_segment_cardinality() {
        let path = unique_path("adaptive_endpoint_bloom");
        let relationships = (0..30_000)
            .map(|id| RelRecord {
                id: RelId(id),
                source: NodeId(id),
                target: NodeId(100_000 + id),
                rel_type: RelTypeId(1),
                properties: BTreeMap::new(),
            })
            .collect::<Vec<_>>();
        let manifest = CanonicalSegmentWriter::new(CanonicalSegmentConfig::default())
            .write(
                &path,
                ManifestGeneration(11),
                std::iter::empty::<NodeRecord>(),
                &relationships,
            )
            .unwrap();
        let reader = CanonicalSegmentReader::open(
            &path,
            manifest.clone(),
            Arc::new(SegmentCache::new(8 * 1024 * 1024)),
            StoreId(20),
            NonZeroU64::new(4 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        let descriptors = collect_descriptors(&reader, CanonicalSegmentKind::Relationships);
        assert!(!descriptors.is_empty());
        assert!(descriptors
            .iter()
            .all(|descriptor| descriptor.source_endpoint_bloom.bit_len() > 256));
        assert!(descriptors.iter().all(|descriptor| {
            descriptor
                .source_endpoint_bloom
                .estimated_false_positive_rate()
                < 0.02
        }));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn property_bloom_prunes_node_segments_without_false_negatives() {
        let path = unique_path("property_bloom");
        let nodes = (0..256)
            .map(|id| NodeRecord {
                id: NodeId(id),
                labels: BTreeSet::from([LabelId(1)]),
                properties: BTreeMap::from([
                    ("id".to_string(), Value::Int(id as i64)),
                    ("payload".to_string(), Value::String("x".repeat(96))),
                ]),
            })
            .collect::<Vec<_>>();
        let config = CanonicalSegmentConfig {
            target_segment_bytes: NonZeroU64::new(512).unwrap(),
            max_record_bytes: NonZeroU64::new(256).unwrap(),
        };
        let manifest = CanonicalSegmentWriter::new(config)
            .write(
                &path,
                ManifestGeneration(10),
                &nodes,
                std::iter::empty::<RelRecord>(),
            )
            .unwrap();
        let node_segment_count = manifest.node_segment_count;
        assert!(node_segment_count > 8);
        let reader = CanonicalSegmentReader::open(
            &path,
            manifest,
            Arc::new(SegmentCache::new(4096)),
            StoreId(13),
            NonZeroU64::new(512).unwrap(),
        )
        .unwrap();
        let mut found = Vec::new();
        let (report, control) = reader
            .scan_nodes_by_property_control(LabelId(1), "id", &Value::Int(127), |node| {
                found.push(node.id);
                Ok(CanonicalScanControl::Continue)
            })
            .unwrap();
        assert_eq!(control, CanonicalScanControl::Continue);
        assert_eq!(found, vec![NodeId(127)]);
        assert!(report.segments_read < node_segment_count);
        assert_eq!(report.segments_considered, node_segment_count);
        assert_eq!(
            report.segments_pruned,
            report.segments_considered - report.segments_read
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn canonical_reader_fails_closed_on_segment_corruption() {
        let path = unique_path("corrupt");
        let nodes = vec![NodeRecord {
            id: NodeId(1),
            labels: BTreeSet::from([LabelId(1)]),
            properties: BTreeMap::from([("id".to_string(), Value::Int(1))]),
        }];
        let manifest = CanonicalSegmentWriter::new(CanonicalSegmentConfig::default())
            .write(
                &path,
                ManifestGeneration(1),
                &nodes,
                std::iter::empty::<RelRecord>(),
            )
            .unwrap();
        let descriptor_reader = CanonicalSegmentReader::open(
            &path,
            manifest.clone(),
            Arc::new(SegmentCache::new(8 * 1024 * 1024)),
            StoreId(1),
            NonZeroU64::new(16 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        let descriptor = collect_descriptors(&descriptor_reader, CanonicalSegmentKind::Nodes)
            .into_iter()
            .next()
            .unwrap();
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[descriptor.offset as usize] ^= 0xff;
        std::fs::write(&path, bytes).unwrap();
        let reader = CanonicalSegmentReader::open(
            &path,
            manifest,
            Arc::new(SegmentCache::new(8 * 1024 * 1024)),
            StoreId(1),
            NonZeroU64::new(16 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            reader.get_node(NodeId(1)),
            Err(CanonicalSegmentError::Read(
                SegmentReadError::DigestMismatch { .. }
            ))
        ));
        assert!(reader.is_poisoned());
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    #[ignore = "resource profile; run explicitly for larger-than-memory-budget evidence"]
    fn canonical_scan_keeps_resident_cache_below_a_smaller_memory_budget() {
        let path = unique_path("larger_than_memory_budget");
        let node_count = 16_384u64;
        let payload_bytes = 4096usize;
        let manifest = CanonicalSegmentWriter::new(CanonicalSegmentConfig::default())
            .write(
                &path,
                ManifestGeneration(9),
                (0..node_count).map(|id| NodeRecord {
                    id: NodeId(id),
                    labels: BTreeSet::from([LabelId(1)]),
                    properties: BTreeMap::from([(
                        "body".to_string(),
                        Value::String("x".repeat(payload_bytes)),
                    )]),
                }),
                std::iter::empty::<RelRecord>(),
            )
            .unwrap();
        let cache_budget = 8 * 1024 * 1024;
        assert!(manifest.artifact_len > cache_budget * 4);
        let cache = Arc::new(SegmentCache::new(cache_budget));
        let reader = CanonicalSegmentReader::open(
            &path,
            manifest,
            Arc::clone(&cache),
            StoreId(19),
            NonZeroU64::new(16 * 1024 * 1024 + 64).unwrap(),
        )
        .unwrap();
        let mut decoded = 0u64;
        let report = reader
            .scan_nodes(|_| {
                decoded = decoded.saturating_add(1);
                Ok(())
            })
            .unwrap();
        assert_eq!(decoded, node_count);
        assert!(report.peak_segment_bytes <= 4 * 1024 * 1024);
        let cache_snapshot = cache.snapshot();
        assert!(cache_snapshot.resident_bytes <= cache_budget);
        assert!(cache_snapshot.eviction_count > 0);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn point_lookups_resolve_sparse_ids_across_many_segments() {
        // Ids leave gaps the way deletes do, so a lookup lands between two
        // segments as often as inside one. Both answers have to be right: the
        // binary search settles on a candidate by `max_record_id` and then has
        // to reject it when the id sits below that segment's `min_record_id`.
        let path = unique_path("sparse_point_lookup");
        let nodes = (0..400)
            .map(|index| NodeRecord {
                id: NodeId(1_000 + index * 10),
                labels: BTreeSet::from([LabelId(1)]),
                properties: BTreeMap::from([("body".to_string(), Value::String("x".repeat(64)))]),
            })
            .collect::<Vec<_>>();
        let config = CanonicalSegmentConfig {
            target_segment_bytes: NonZeroU64::new(1024).unwrap(),
            max_record_bytes: NonZeroU64::new(2048).unwrap(),
        };
        let manifest = CanonicalSegmentWriter::new(config)
            .write(
                &path,
                ManifestGeneration(3),
                &nodes,
                Vec::<RelRecord>::new(),
            )
            .unwrap();
        assert!(manifest.node_segment_count > 8);
        let descriptor_reader = CanonicalSegmentReader::open(
            &path,
            manifest.clone(),
            Arc::new(SegmentCache::new(1024 * 1024)),
            StoreId(22),
            NonZeroU64::new(4096).unwrap(),
        )
        .unwrap();
        let segments = collect_descriptors(&descriptor_reader, CanonicalSegmentKind::Nodes);
        // Ids that fall between two segments, plus one below the first segment
        // and one past the last. None of these is inside any range, so the
        // range bounds alone settle them.
        let mut unreadable_probes = vec![segments[0].min_record_id - 1, u64::MAX];
        unreadable_probes.extend(
            segments
                .windows(2)
                .filter(|pair| pair[0].max_record_id + 1 < pair[1].min_record_id)
                .map(|pair| pair[0].max_record_id + 1),
        );
        assert!(unreadable_probes.len() > 4);

        // Descriptor pages may be read and cached, but a gap settled by range
        // metadata must not read a canonical data segment.
        for probe in unreadable_probes {
            let reader = CanonicalSegmentReader::open(
                &path,
                manifest.clone(),
                Arc::new(SegmentCache::new(1024 * 1024)),
                StoreId(23),
                NonZeroU64::new(4096).unwrap(),
            )
            .unwrap();
            let (node, report) = reader.get_node_with_report(NodeId(probe)).unwrap();
            assert_eq!(node, None);
            assert_eq!(report.segments_read, 0, "probe {probe}");
            assert_eq!(report.bytes_read, 0, "probe {probe}");
        }

        let reader = CanonicalSegmentReader::open(
            &path,
            manifest,
            Arc::new(SegmentCache::new(1024 * 1024)),
            StoreId(23),
            NonZeroU64::new(4096).unwrap(),
        )
        .unwrap();
        for node in &nodes {
            assert_eq!(reader.get_node(node.id).unwrap().as_ref(), Some(node));
        }
        // Ids inside a segment's range but between two of its records still
        // resolve to nothing, which is the early exit's boundary: it stops at
        // the first id at or past the target instead of running to the end.
        for index in 0..400u64 {
            assert_eq!(
                reader.get_node(NodeId(1_000 + index * 10 + 1)).unwrap(),
                None
            );
            assert_eq!(
                reader.get_node(NodeId(1_000 + index * 10 - 1)).unwrap(),
                None
            );
        }
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn deep_scrub_rejects_descriptor_kind_count_drift() {
        let path = unique_path("interleaved_kinds");
        let nodes = (0..32)
            .map(|id| NodeRecord {
                id: NodeId(id),
                labels: BTreeSet::from([LabelId(1)]),
                properties: BTreeMap::from([("body".to_string(), Value::String("x".repeat(64)))]),
            })
            .collect::<Vec<_>>();
        let relationships = (0..32)
            .map(|id| RelRecord {
                id: RelId(id),
                source: NodeId(id),
                target: NodeId(id + 1),
                rel_type: RelTypeId(1),
                properties: BTreeMap::from([("body".to_string(), Value::String("x".repeat(64)))]),
            })
            .collect::<Vec<_>>();
        let config = CanonicalSegmentConfig {
            target_segment_bytes: NonZeroU64::new(512).unwrap(),
            max_record_bytes: NonZeroU64::new(2048).unwrap(),
        };
        let mut manifest = CanonicalSegmentWriter::new(config)
            .write(&path, ManifestGeneration(11), &nodes, &relationships)
            .unwrap();
        let reader = CanonicalSegmentReader::open(
            &path,
            manifest.clone(),
            Arc::new(SegmentCache::new(1024 * 1024)),
            StoreId(24),
            NonZeroU64::new(4096).unwrap(),
        )
        .unwrap();
        let mut descriptors = collect_descriptors(&reader, CanonicalSegmentKind::Nodes);
        descriptors.extend(collect_descriptors(
            &reader,
            CanonicalSegmentKind::Relationships,
        ));
        drop(reader);
        let relationship = descriptors
            .iter_mut()
            .find(|descriptor| descriptor.kind == CanonicalSegmentKind::Relationships)
            .unwrap();
        relationship.kind = CanonicalSegmentKind::Nodes;
        replace_descriptor_tree(&path, &mut manifest, descriptors);
        let reader = CanonicalSegmentReader::open(
            &path,
            manifest.clone(),
            Arc::new(SegmentCache::new(1024 * 1024)),
            StoreId(24),
            NonZeroU64::new(4096).unwrap(),
        )
        .unwrap();
        assert!(reader.deep_scrub().is_err());
        assert!(reader.is_poisoned());
        drop(reader);
        remove_fixture(&path, manifest.generation);
    }

    #[test]
    fn deep_scrub_rejects_descriptor_record_range_overlap() {
        let path = unique_path("overlapping_ranges");
        let nodes = (0..64)
            .map(|id| NodeRecord {
                id: NodeId(id),
                labels: BTreeSet::from([LabelId(1)]),
                properties: BTreeMap::from([("body".to_string(), Value::String("x".repeat(64)))]),
            })
            .collect::<Vec<_>>();
        let config = CanonicalSegmentConfig {
            target_segment_bytes: NonZeroU64::new(512).unwrap(),
            max_record_bytes: NonZeroU64::new(2048).unwrap(),
        };
        let mut manifest = CanonicalSegmentWriter::new(config)
            .write(
                &path,
                ManifestGeneration(5),
                &nodes,
                Vec::<RelRecord>::new(),
            )
            .unwrap();
        assert!(manifest.node_segment_count > 2);
        let reader = CanonicalSegmentReader::open(
            &path,
            manifest.clone(),
            Arc::new(SegmentCache::new(1024 * 1024)),
            StoreId(25),
            NonZeroU64::new(4096).unwrap(),
        )
        .unwrap();
        let mut descriptors = collect_descriptors(&reader, CanonicalSegmentKind::Nodes);
        drop(reader);
        descriptors[0].max_record_id = descriptors[1].max_record_id;
        replace_descriptor_tree(&path, &mut manifest, descriptors);
        let reader = CanonicalSegmentReader::open(
            &path,
            manifest.clone(),
            Arc::new(SegmentCache::new(1024 * 1024)),
            StoreId(25),
            NonZeroU64::new(4096).unwrap(),
        )
        .unwrap();
        assert!(reader.deep_scrub().is_err());
        assert!(reader.is_poisoned());
        drop(reader);
        remove_fixture(&path, manifest.generation);
    }

    fn fixture_nodes() -> Vec<NodeRecord> {
        vec![
            NodeRecord {
                id: NodeId(1),
                labels: BTreeSet::from([LabelId(1)]),
                properties: BTreeMap::from([
                    ("active".to_string(), Value::Bool(true)),
                    ("age".to_string(), Value::Int(34)),
                    ("name".to_string(), Value::String("alice".to_string())),
                    ("score".to_string(), Value::Float(4.5)),
                ]),
            },
            NodeRecord {
                id: NodeId(2),
                labels: BTreeSet::from([LabelId(1)]),
                properties: BTreeMap::from([
                    ("active".to_string(), Value::Bool(false)),
                    ("age".to_string(), Value::Int(41)),
                    ("name".to_string(), Value::String("bob".to_string())),
                    (
                        "tags".to_string(),
                        Value::List(vec![Value::String("x".to_string()), Value::Int(7)]),
                    ),
                ]),
            },
            NodeRecord {
                id: NodeId(3),
                labels: BTreeSet::from([LabelId(1)]),
                properties: BTreeMap::from([
                    (
                        "meta".to_string(),
                        Value::Map(BTreeMap::from([
                            ("k".to_string(), Value::String("v".to_string())),
                            ("n".to_string(), Value::Int(1)),
                        ])),
                    ),
                    ("name".to_string(), Value::String("carol".to_string())),
                    ("note".to_string(), Value::Null),
                ]),
            },
            NodeRecord {
                id: NodeId(4),
                labels: BTreeSet::from([LabelId(2)]),
                properties: BTreeMap::from([
                    ("age".to_string(), Value::Int(28)),
                    ("name".to_string(), Value::String("dave".to_string())),
                    ("score".to_string(), Value::Float(-0.25)),
                ]),
            },
            NodeRecord {
                id: NodeId(5),
                labels: BTreeSet::from([LabelId(2)]),
                properties: BTreeMap::from([
                    (
                        "meta".to_string(),
                        Value::Map(BTreeMap::from([("k".to_string(), Value::Float(2.0))])),
                    ),
                    ("name".to_string(), Value::String("erin".to_string())),
                    (
                        "tags".to_string(),
                        Value::List(vec![Value::Bool(true), Value::Null]),
                    ),
                ]),
            },
            NodeRecord {
                id: NodeId(6),
                labels: BTreeSet::from([LabelId(1), LabelId(2)]),
                properties: BTreeMap::from([
                    ("active".to_string(), Value::Bool(true)),
                    ("age".to_string(), Value::Int(52)),
                    ("name".to_string(), Value::String("多字节".to_string())),
                    ("note".to_string(), Value::String("shared".to_string())),
                ]),
            },
        ]
    }

    fn fixture_relationships() -> Vec<RelRecord> {
        vec![
            RelRecord {
                id: RelId(1),
                source: NodeId(1),
                target: NodeId(2),
                rel_type: RelTypeId(1),
                properties: BTreeMap::from([
                    ("since".to_string(), Value::Int(2020)),
                    ("weight".to_string(), Value::Float(1.5)),
                ]),
            },
            RelRecord {
                id: RelId(2),
                source: NodeId(2),
                target: NodeId(3),
                rel_type: RelTypeId(1),
                properties: BTreeMap::from([
                    ("kind".to_string(), Value::String("knows".to_string())),
                    ("weight".to_string(), Value::Float(0.25)),
                ]),
            },
            RelRecord {
                id: RelId(3),
                source: NodeId(3),
                target: NodeId(4),
                rel_type: RelTypeId(2),
                properties: BTreeMap::from([
                    ("active".to_string(), Value::Bool(true)),
                    ("weight".to_string(), Value::Float(2.0)),
                ]),
            },
            RelRecord {
                id: RelId(4),
                source: NodeId(5),
                target: NodeId(6),
                rel_type: RelTypeId(2),
                properties: BTreeMap::from([
                    ("note".to_string(), Value::Null),
                    ("weight".to_string(), Value::Float(0.5)),
                ]),
            },
        ]
    }

    #[test]
    fn v1_manifest_publishes_property_keys_in_first_seen_order() {
        let path = unique_path("v1_key_table");
        let manifest = CanonicalSegmentWriter::new(CanonicalSegmentConfig::default())
            .write(
                &path,
                ManifestGeneration(1),
                &fixture_nodes(),
                &fixture_relationships(),
            )
            .unwrap();
        assert_eq!(
            manifest.property_keys,
            ["active", "age", "name", "score", "tags", "meta", "note", "since", "weight", "kind"]
                .map(str::to_string)
                .to_vec()
        );
        let encoded = manifest.encode().unwrap();
        assert!(encoded.starts_with(MANIFEST_HEADER_V1));
        assert_eq!(
            CanonicalSegmentManifest::decode(&encoded).unwrap(),
            manifest
        );
        let inline_layout = reseal_manifest(&encoded, |line| {
            if line == "record_layout\tproperty_key_ids" {
                "record_layout\tinline_keys".to_string()
            } else {
                line.to_string()
            }
        });
        assert!(CanonicalSegmentManifest::decode(&inline_layout)
            .unwrap_err()
            .to_string()
            .contains("invalid canonical manifest line"));
        let old_header = reseal_manifest(&encoded, |line| {
            if line == MANIFEST_HEADER_V1 {
                "HAWDB_CANONICAL_MANIFEST_V2".to_string()
            } else {
                line.to_string()
            }
        });
        assert!(CanonicalSegmentManifest::decode(&old_header)
            .unwrap_err()
            .to_string()
            .contains("invalid canonical manifest line"));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn v1_manifest_round_trips_hostile_property_keys() {
        let path = unique_path("hostile_keys");
        let nodes = vec![NodeRecord {
            id: NodeId(1),
            labels: BTreeSet::from([LabelId(1)]),
            properties: BTreeMap::from([
                ("with\ttab".to_string(), Value::Int(1)),
                ("with\nnewline".to_string(), Value::Int(2)),
                ("键值".to_string(), Value::String("多字节".to_string())),
                (String::new(), Value::Null),
            ]),
        }];
        let manifest = CanonicalSegmentWriter::new(CanonicalSegmentConfig::default())
            .write(
                &path,
                ManifestGeneration(2),
                &nodes,
                std::iter::empty::<RelRecord>(),
            )
            .unwrap();
        let encoded = manifest.encode().unwrap();
        assert_eq!(
            CanonicalSegmentManifest::decode(&encoded).unwrap(),
            manifest
        );
        let reader = CanonicalSegmentReader::open(
            &path,
            manifest,
            Arc::new(SegmentCache::new(8 * 1024 * 1024)),
            StoreId(1),
            NonZeroU64::new(16 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        assert_eq!(reader.get_node(NodeId(1)).unwrap(), Some(nodes[0].clone()));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn v1_manifest_rejects_duplicate_and_non_contiguous_property_keys() {
        let path = unique_path("bad_key_table");
        let manifest = CanonicalSegmentWriter::new(CanonicalSegmentConfig::default())
            .write(
                &path,
                ManifestGeneration(1),
                &fixture_nodes(),
                &fixture_relationships(),
            )
            .unwrap();
        let encoded = manifest.encode().unwrap();
        let first_key_hex = encoded
            .lines()
            .find_map(|line| line.strip_prefix("property_key\t0\t"))
            .unwrap()
            .to_string();
        let duplicated = reseal_manifest(&encoded, |line| {
            match line.strip_prefix("property_key\t1\t") {
                Some(_) => format!("property_key\t1\t{first_key_hex}"),
                None => line.to_string(),
            }
        });
        assert!(CanonicalSegmentManifest::decode(&duplicated)
            .unwrap_err()
            .to_string()
            .contains("property keys are not unique"));
        let non_contiguous = reseal_manifest(&encoded, |line| {
            match line.strip_prefix("property_key\t1\t") {
                Some(key) => format!("property_key\t5\t{key}"),
                None => line.to_string(),
            }
        });
        assert!(CanonicalSegmentManifest::decode(&non_contiguous)
            .unwrap_err()
            .to_string()
            .contains("property key ids are not contiguous"));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn v1_reader_rejects_out_of_range_property_key_ids() {
        let path = unique_path("out_of_range_key_id");
        let mut manifest = CanonicalSegmentWriter::new(CanonicalSegmentConfig::default())
            .write(
                &path,
                ManifestGeneration(1),
                &fixture_nodes(),
                &fixture_relationships(),
            )
            .unwrap();
        manifest.property_keys = Vec::new();
        let reader = CanonicalSegmentReader::open(
            &path,
            manifest,
            Arc::new(SegmentCache::new(8 * 1024 * 1024)),
            StoreId(1),
            NonZeroU64::new(16 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            reader.get_node(NodeId(1)),
            Err(CanonicalSegmentError::Corrupt(message))
                if message.contains("unknown property key id")
        ));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn residual_rows_round_trip_every_value_shape_on_the_wire_codec() {
        let values = [
            (4u32, Value::Bool(false)),
            (5, Value::Int(i64::MIN)),
            (6, Value::Float(-2.5)),
            (7, Value::String("列存".to_string())),
            (8, Value::Binary(vec![0, 1, 0xfe, 0xff])),
            (9, Value::Null),
            (
                10,
                Value::List(vec![Value::Int(1), Value::String("x".to_string())]),
            ),
            (
                11,
                Value::Map(BTreeMap::from([("k".to_string(), Value::Bool(true))])),
            ),
        ];
        let entries: Vec<(u32, &Value)> = values
            .iter()
            .map(|(key_id, value)| (*key_id, value))
            .collect();
        let encoded = encode_residual_row_properties(&entries).unwrap();
        let decoded = decode_residual_row_properties(&encoded).unwrap();
        assert_eq!(
            decoded,
            entries
                .iter()
                .map(|(key_id, value)| (*key_id, (*value).clone()))
                .collect::<Vec<_>>()
        );
        // An empty row is zero bytes.
        assert!(encode_residual_row_properties(&[]).unwrap().is_empty());
        assert_eq!(decode_residual_row_properties(&[]).unwrap(), Vec::new());
    }

    #[test]
    fn residual_rows_skip_unknown_fields_and_reject_corruption() {
        let value = Value::Int(41);
        let mut encoded = encode_residual_row_properties(&[(4, &value)]).unwrap();
        // A future row-level field and a future property-level field are
        // both skipped by wire type (§3.5.1 forward compatibility).
        wire::encode_varint_field(90, 7, &mut encoded);
        wire::encode_fixed64_field(91, u64::MAX, &mut encoded);
        let mut future_property = Vec::new();
        wire::encode_varint_field(1, 12, &mut future_property);
        let mut canonical_value = Vec::new();
        encode_value(&Value::Int(-3), &mut canonical_value, 1).unwrap();
        wire::encode_len_field(2, &canonical_value, &mut future_property);
        wire::encode_len_field(77, b"future-extension", &mut future_property);
        wire::encode_len_field(1, &future_property, &mut encoded);
        assert_eq!(
            decode_residual_row_properties(&encoded).unwrap(),
            vec![(4, Value::Int(41)), (12, Value::Int(-3))]
        );
        // Truncation, duplicate keys, and value-less properties fail closed.
        assert!(matches!(
            decode_residual_row_properties(&encoded[..encoded.len() - 1]),
            Err(CanonicalSegmentError::Corrupt(_))
        ));
        let duplicate = encode_residual_row_properties(&[(4, &value), (4, &value)]).unwrap();
        assert!(matches!(
            decode_residual_row_properties(&duplicate),
            Err(CanonicalSegmentError::Corrupt(_))
        ));
        let mut missing_value = Vec::new();
        let mut property = Vec::new();
        wire::encode_varint_field(1, 4, &mut property);
        wire::encode_len_field(1, &property, &mut missing_value);
        assert!(matches!(
            decode_residual_row_properties(&missing_value),
            Err(CanonicalSegmentError::Corrupt(_))
        ));
    }

    fn reseal_manifest(encoded: &str, rewrite: impl FnMut(&str) -> String) -> String {
        let body = encoded
            .lines()
            .filter(|line| !line.starts_with("checksum\t"))
            .map(rewrite)
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        format!("{body}checksum\t{}\n", content_digest(body.as_bytes()).0)
    }

    struct CanonicalFixturePath(PathBuf);

    impl std::ops::Deref for CanonicalFixturePath {
        type Target = Path;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl AsRef<Path> for CanonicalFixturePath {
        fn as_ref(&self) -> &Path {
            &self.0
        }
    }

    impl From<&CanonicalFixturePath> for PathBuf {
        fn from(path: &CanonicalFixturePath) -> Self {
            path.0.clone()
        }
    }

    impl Drop for CanonicalFixturePath {
        fn drop(&mut self) {
            for owned in [
                self.0.clone(),
                self.0.with_extension("descriptors.pages.hawdb"),
                self.0.with_extension("descriptors.root.hawdb"),
            ] {
                let _ = std::fs::remove_file(owned);
            }
        }
    }

    fn unique_path(name: &str) -> CanonicalFixturePath {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        CanonicalFixturePath(
            std::env::temp_dir().join(format!("hawdb-canonical-{name}-{nonce}.hawdb")),
        )
    }

    fn remove_fixture(path: &Path, generation: ManifestGeneration) {
        let descriptor_tree =
            PersistentCanonicalSegmentDescriptorTree::for_artifact(path, generation);
        let (descriptor_paths, _) = descriptor_tree.into_parts();
        for owned in [
            path.to_path_buf(),
            descriptor_paths.page_artifact,
            descriptor_paths.root_manifest,
        ] {
            match std::fs::remove_file(owned) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => panic!("failed to remove canonical fixture: {error}"),
            }
        }
    }
}
