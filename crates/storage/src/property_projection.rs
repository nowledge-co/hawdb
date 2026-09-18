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

use crate::canonical::{
    decode_standalone_value, encode_standalone_value, CanonicalScanControl, CanonicalSegmentError,
};
use crate::graph_descriptor_tree::demand::{
    GraphDescriptorTreeDemandReader, GraphDescriptorTreeReadLimits, GraphDescriptorTreeReadReport,
    GraphDescriptorTreeScanControl,
};
use crate::predicate::{comparable_value_ordering, range_bounds_match};
use crate::{
    content_digest, durable_replace_file, ContentDigest, FileSegmentRangeReader,
    GraphDescriptorKind, GraphDescriptorPageError, GraphDescriptorTreeArtifactMetadata,
    GraphDescriptorTreeBuildConfig, GraphDescriptorTreeBuilder, GraphDescriptorTreeError,
    GraphDescriptorTreeGenerationArtifacts, GraphDescriptorTreePaths,
    GraphDescriptorTreeRootReader, GraphDescriptorTreeWriteOutput, ManifestGeneration, NodeId,
    NodeRecord, PreparedGraphDescriptorTree, RelId, RelRecord, SegmentCache, SegmentRangeRead,
    SegmentReadError, SegmentReadRange, StoreId,
};
use hawdb_core::{LabelId, RelTypeId, Value};
use hawdb_integrity::{Crc32cHasher, IntegrityHasher, Sha256Digest};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, VecDeque};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

const ARTIFACT_HEADER: &[u8; 16] = b"HAWDBPROPINDEX01";
const BLOCK_HEADER: &[u8; 8] = b"SKNIDX01";
const RUN_HEADER: &[u8; 8] = b"SKNIDXR1";
const MANIFEST_HEADER: &str = "HAWDB_PROPERTY_PROJECTION_MANIFEST_V1";
const ARTIFACT_ID: u64 = 0x534b_5052_4944_5831;
const DESCRIPTOR_ARTIFACT_ID: u64 = 0x534b_5052_4453_4331;
const BLOCK_ID_BASE: u64 = 3 << 60;
const COMPOSITE_PROPERTY_IDENTITY_PREFIX: &str = "hawdb-composite-property-v1";
const DESCRIPTOR_VALUE_MAGIC: &[u8; 8] = b"SKPPDSC1";
const DESCRIPTOR_VALUE_VERSION: u16 = 1;
const DESCRIPTOR_VALUE_HEADER_BYTES: usize = 68;

pub fn property_projection_descriptor_page_file(generation: u64) -> String {
    format!("property-index-descriptors-{generation}.pages.hawdb")
}

pub fn property_projection_descriptor_root_file(generation: u64) -> String {
    format!("property-index-descriptors-{generation}.root.hawdb")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PersistentPropertyProjectionKind {
    Equality,
    Range,
    FullText,
    CompositeEquality,
    RelationshipEquality,
    RelationshipRange,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PersistentPropertyProjectionRecord {
    Node(NodeRecord),
    Relationship(RelRecord),
}

pub fn persistent_composite_property_identity(
    properties: &[String],
) -> Result<String, PersistentPropertyProjectionError> {
    if properties.len() < 2 {
        return Err(PersistentPropertyProjectionError::Source(
            "persistent composite property projection requires at least two properties".to_string(),
        ));
    }
    let mut identity = String::from(COMPOSITE_PROPERTY_IDENTITY_PREFIX);
    for property in properties {
        identity.push(':');
        identity.push_str(&encode_hex(property.as_bytes()));
    }
    Ok(identity)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PersistentPropertyProjectionDefinition {
    pub label_id: LabelId,
    pub property: String,
    pub kind: PersistentPropertyProjectionKind,
    pub complete: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PersistentPropertyProjectionConfig {
    pub memory_budget_bytes: NonZeroU64,
    pub max_definition_count: NonZeroUsize,
    pub max_definition_bytes: NonZeroU64,
    pub max_spill_bytes: NonZeroU64,
    pub max_spill_runs: NonZeroUsize,
    pub max_merge_fan_in: NonZeroUsize,
    pub target_block_bytes: NonZeroU64,
    pub max_index_key_bytes: NonZeroU64,
    pub max_generated_entries: NonZeroU64,
}

impl Default for PersistentPropertyProjectionConfig {
    fn default() -> Self {
        Self {
            memory_budget_bytes: NonZeroU64::new(32 * 1024 * 1024)
                .expect("default property projection memory budget is non-zero"),
            max_definition_count: NonZeroUsize::new(65_536)
                .expect("default property projection definition limit is non-zero"),
            max_definition_bytes: NonZeroU64::new(8 * 1024 * 1024)
                .expect("default property projection definition byte limit is non-zero"),
            max_spill_bytes: NonZeroU64::new(4 * 1024 * 1024 * 1024 * 1024)
                .expect("default property projection spill budget is non-zero"),
            max_spill_runs: NonZeroUsize::new(4_096)
                .expect("default property projection run budget is non-zero"),
            max_merge_fan_in: NonZeroUsize::new(32)
                .expect("default property projection merge fan-in is non-zero"),
            target_block_bytes: NonZeroU64::new(1024 * 1024)
                .expect("default property projection block size is non-zero"),
            max_index_key_bytes: NonZeroU64::new(4 * 1024)
                .expect("default property projection key limit is non-zero"),
            max_generated_entries: NonZeroU64::new(100_000_000)
                .expect("default property projection fact budget is non-zero"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PersistentPropertyProjectionDescriptorTree {
    paths: GraphDescriptorTreePaths,
    config: GraphDescriptorTreeBuildConfig,
}

impl PersistentPropertyProjectionDescriptorTree {
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

#[derive(Debug)]
pub enum PersistentPropertyProjectionError {
    Io(std::io::Error),
    Read(SegmentReadError),
    Canonical(CanonicalSegmentError),
    DescriptorTree(GraphDescriptorTreeError),
    Source(String),
    Corrupt(String),
    MemoryBudgetExceeded {
        required_bytes: u64,
        max_bytes: u64,
    },
    SpillBudgetExceeded {
        required_bytes: u64,
        max_bytes: u64,
    },
    SpillRunBudgetExceeded {
        required_runs: usize,
        max_runs: usize,
    },
    GeneratedEntryBudgetExceeded {
        required_entries: u64,
        max_entries: u64,
    },
    DefinitionCountBudgetExceeded {
        required_definitions: usize,
        max_definitions: usize,
    },
    DefinitionBytesBudgetExceeded {
        required_bytes: u64,
        max_bytes: u64,
    },
    BlockTooLarge {
        block_bytes: u64,
        max_bytes: u64,
    },
}

impl Display for PersistentPropertyProjectionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => Display::fmt(error, formatter),
            Self::Read(error) => Display::fmt(error, formatter),
            Self::Canonical(error) => Display::fmt(error, formatter),
            Self::DescriptorTree(error) => Display::fmt(error, formatter),
            Self::Source(message) | Self::Corrupt(message) => formatter.write_str(message),
            Self::MemoryBudgetExceeded {
                required_bytes,
                max_bytes,
            } => write!(
                formatter,
                "property projection build requires {required_bytes} resident bytes, exceeding {max_bytes}"
            ),
            Self::SpillBudgetExceeded {
                required_bytes,
                max_bytes,
            } => write!(
                formatter,
                "property projection build requires {required_bytes} spill bytes, exceeding {max_bytes}"
            ),
            Self::SpillRunBudgetExceeded {
                required_runs,
                max_runs,
            } => write!(
                formatter,
                "property projection build requires {required_runs} spill runs, exceeding {max_runs}"
            ),
            Self::GeneratedEntryBudgetExceeded {
                required_entries,
                max_entries,
            } => write!(
                formatter,
                "property projection build requires {required_entries} generated entries, exceeding {max_entries}"
            ),
            Self::DefinitionCountBudgetExceeded {
                required_definitions,
                max_definitions,
            } => write!(
                formatter,
                "property projection build requires {required_definitions} definitions, exceeding {max_definitions}"
            ),
            Self::DefinitionBytesBudgetExceeded {
                required_bytes,
                max_bytes,
            } => write!(
                formatter,
                "property projection definitions require {required_bytes} resident bytes, exceeding {max_bytes}"
            ),
            Self::BlockTooLarge {
                block_bytes,
                max_bytes,
            } => write!(
                formatter,
                "property projection block uses {block_bytes} bytes, exceeding {max_bytes}"
            ),
        }
    }
}

impl Error for PersistentPropertyProjectionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Read(error) => Some(error),
            Self::Canonical(error) => Some(error),
            Self::DescriptorTree(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for PersistentPropertyProjectionError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<SegmentReadError> for PersistentPropertyProjectionError {
    fn from(error: SegmentReadError) -> Self {
        Self::Read(error)
    }
}

impl From<CanonicalSegmentError> for PersistentPropertyProjectionError {
    fn from(error: CanonicalSegmentError) -> Self {
        Self::Canonical(error)
    }
}

impl From<GraphDescriptorTreeError> for PersistentPropertyProjectionError {
    fn from(error: GraphDescriptorTreeError) -> Self {
        Self::DescriptorTree(error)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PersistentPropertyProjectionDefinitionAdmission {
    max_definitions: usize,
    max_bytes: u64,
    definition_count: usize,
    resident_bytes: u64,
}

impl PersistentPropertyProjectionDefinitionAdmission {
    pub fn new(config: PersistentPropertyProjectionConfig) -> Self {
        Self {
            max_definitions: config.max_definition_count.get(),
            max_bytes: config.max_definition_bytes.get(),
            definition_count: 0,
            resident_bytes: 0,
        }
    }

    pub fn admit(
        &mut self,
        definition: &PersistentPropertyProjectionDefinition,
    ) -> Result<(), PersistentPropertyProjectionError> {
        let required_definitions = self.definition_count.saturating_add(1);
        if required_definitions > self.max_definitions {
            return Err(
                PersistentPropertyProjectionError::DefinitionCountBudgetExceeded {
                    required_definitions,
                    max_definitions: self.max_definitions,
                },
            );
        }
        let definition_bytes = (std::mem::size_of::<PersistentPropertyProjectionDefinition>()
            as u64)
            .saturating_add(definition.property.len() as u64);
        let required_bytes = self.resident_bytes.saturating_add(definition_bytes);
        if required_bytes > self.max_bytes {
            return Err(
                PersistentPropertyProjectionError::DefinitionBytesBudgetExceeded {
                    required_bytes,
                    max_bytes: self.max_bytes,
                },
            );
        }
        self.definition_count = required_definitions;
        self.resident_bytes = required_bytes;
        Ok(())
    }

    pub const fn definition_count(&self) -> usize {
        self.definition_count
    }

    pub const fn resident_bytes(&self) -> u64 {
        self.resident_bytes
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistentPropertyProjectionBlockDescriptor {
    pub block_id: u64,
    pub label_id: LabelId,
    pub property: String,
    pub kind: PersistentPropertyProjectionKind,
    pub min_key: Value,
    pub max_key: Value,
    pub offset: u64,
    pub length: NonZeroU64,
    pub content_digest: ContentDigest,
    pub entry_count: u32,
}

impl PersistentPropertyProjectionBlockDescriptor {
    pub fn descriptor_tree_key(&self) -> Vec<u8> {
        let mut key =
            property_projection_descriptor_prefix(self.kind, self.label_id, &self.property);
        key.extend_from_slice(&self.block_id.to_be_bytes());
        key
    }

    pub fn encode_descriptor_tree_value(
        &self,
    ) -> Result<Vec<u8>, PersistentPropertyProjectionError> {
        let property_len = u32::try_from(self.property.len()).map_err(|_| {
            PersistentPropertyProjectionError::Corrupt(
                "property projection descriptor property length exceeds u32".to_string(),
            )
        })?;
        let min_key = encode_standalone_value(&self.min_key)?;
        let max_key = encode_standalone_value(&self.max_key)?;
        let min_key_len = u32::try_from(min_key.len()).map_err(|_| {
            PersistentPropertyProjectionError::Corrupt(
                "property projection descriptor minimum key length exceeds u32".to_string(),
            )
        })?;
        let max_key_len = u32::try_from(max_key.len()).map_err(|_| {
            PersistentPropertyProjectionError::Corrupt(
                "property projection descriptor maximum key length exceeds u32".to_string(),
            )
        })?;
        let capacity = DESCRIPTOR_VALUE_HEADER_BYTES
            .checked_add(self.property.len())
            .and_then(|bytes| bytes.checked_add(min_key.len()))
            .and_then(|bytes| bytes.checked_add(max_key.len()))
            .ok_or_else(|| {
                PersistentPropertyProjectionError::Corrupt(
                    "property projection descriptor length overflow".to_string(),
                )
            })?;
        let mut encoded = Vec::with_capacity(capacity);
        encoded.extend_from_slice(DESCRIPTOR_VALUE_MAGIC);
        encoded.extend_from_slice(&DESCRIPTOR_VALUE_VERSION.to_le_bytes());
        encoded.extend_from_slice(&0u16.to_le_bytes());
        encoded.extend_from_slice(&self.block_id.to_le_bytes());
        encoded.push(kind_tag(self.kind));
        encoded.extend_from_slice(&[0u8; 3]);
        encoded.extend_from_slice(&self.label_id.0.to_le_bytes());
        encoded.extend_from_slice(&self.entry_count.to_le_bytes());
        encoded.extend_from_slice(&self.offset.to_le_bytes());
        encoded.extend_from_slice(&self.length.get().to_le_bytes());
        encoded.extend_from_slice(&self.content_digest.0.to_le_bytes());
        encoded.extend_from_slice(&property_len.to_le_bytes());
        encoded.extend_from_slice(&min_key_len.to_le_bytes());
        encoded.extend_from_slice(&max_key_len.to_le_bytes());
        encoded.extend_from_slice(self.property.as_bytes());
        encoded.extend_from_slice(&min_key);
        encoded.extend_from_slice(&max_key);
        debug_assert_eq!(encoded.len(), capacity);
        Ok(encoded)
    }

    pub fn decode_descriptor_tree_entry(
        key: &[u8],
        encoded: &[u8],
    ) -> Result<Self, PersistentPropertyProjectionError> {
        if encoded.len() < DESCRIPTOR_VALUE_HEADER_BYTES || &encoded[..8] != DESCRIPTOR_VALUE_MAGIC
        {
            return Err(PersistentPropertyProjectionError::Corrupt(
                "property projection descriptor has an invalid header or length".to_string(),
            ));
        }
        let version = u16::from_le_bytes(encoded[8..10].try_into().expect("fixed version"));
        let flags = u16::from_le_bytes(encoded[10..12].try_into().expect("fixed flags"));
        if version != DESCRIPTOR_VALUE_VERSION || flags != 0 || encoded[21..24] != [0u8; 3] {
            return Err(PersistentPropertyProjectionError::Corrupt(format!(
                "property projection descriptor has unsupported version {version}, flags {flags}, or reserved fields"
            )));
        }
        let property_len =
            u32::from_le_bytes(encoded[56..60].try_into().expect("fixed property length")) as usize;
        let min_key_len = u32::from_le_bytes(
            encoded[60..64]
                .try_into()
                .expect("fixed minimum key length"),
        ) as usize;
        let max_key_len = u32::from_le_bytes(
            encoded[64..68]
                .try_into()
                .expect("fixed maximum key length"),
        ) as usize;
        let property_end = DESCRIPTOR_VALUE_HEADER_BYTES
            .checked_add(property_len)
            .ok_or_else(|| descriptor_length_overflow("property"))?;
        let min_key_end = property_end
            .checked_add(min_key_len)
            .ok_or_else(|| descriptor_length_overflow("minimum key"))?;
        let max_key_end = min_key_end
            .checked_add(max_key_len)
            .ok_or_else(|| descriptor_length_overflow("maximum key"))?;
        if max_key_end != encoded.len() {
            return Err(PersistentPropertyProjectionError::Corrupt(
                "property projection descriptor field lengths do not match its encoded length"
                    .to_string(),
            ));
        }
        let property = std::str::from_utf8(&encoded[DESCRIPTOR_VALUE_HEADER_BYTES..property_end])
            .map_err(|error| {
                PersistentPropertyProjectionError::Corrupt(format!(
                    "property projection descriptor property is not UTF-8: {error}"
                ))
            })?
            .to_string();
        let length = NonZeroU64::new(u64::from_le_bytes(
            encoded[40..48].try_into().expect("fixed block length"),
        ))
        .ok_or_else(|| {
            PersistentPropertyProjectionError::Corrupt(
                "property projection descriptor block length is zero".to_string(),
            )
        })?;
        let descriptor = Self {
            block_id: u64::from_le_bytes(encoded[12..20].try_into().expect("fixed block id")),
            kind: kind_from_tag(encoded[20])?,
            label_id: LabelId(u32::from_le_bytes(
                encoded[24..28].try_into().expect("fixed label id"),
            )),
            entry_count: u32::from_le_bytes(encoded[28..32].try_into().expect("fixed entry count")),
            offset: u64::from_le_bytes(encoded[32..40].try_into().expect("fixed offset")),
            length,
            content_digest: ContentDigest(u64::from_le_bytes(
                encoded[48..56].try_into().expect("fixed content digest"),
            )),
            property,
            min_key: decode_standalone_value(&encoded[property_end..min_key_end])?,
            max_key: decode_standalone_value(&encoded[min_key_end..max_key_end])?,
        };
        if descriptor.block_id < BLOCK_ID_BASE
            || descriptor.entry_count == 0
            || descriptor.min_key > descriptor.max_key
            || descriptor.descriptor_tree_key() != key
        {
            return Err(PersistentPropertyProjectionError::Corrupt(
                "property projection descriptor identity or bounds are inconsistent".to_string(),
            ));
        }
        if descriptor.kind == PersistentPropertyProjectionKind::CompositeEquality {
            let arity = decode_composite_property_identity(&descriptor.property)?.len();
            if !composite_key_has_arity(&descriptor.min_key, arity)
                || !composite_key_has_arity(&descriptor.max_key, arity)
            {
                return Err(PersistentPropertyProjectionError::Corrupt(
                    "property projection composite descriptor has invalid key arity".to_string(),
                ));
            }
        }
        Ok(descriptor)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistentPropertyProjectionManifest {
    pub generation: ManifestGeneration,
    pub source_commit_epoch: u64,
    pub artifact_id: u64,
    pub artifact_len: u64,
    pub artifact_digest: ContentDigest,
    pub artifact_sha256: Sha256Digest,
    pub entry_count: u64,
    pub block_count: u64,
    pub definitions: Vec<PersistentPropertyProjectionDefinition>,
    pub descriptor_root_artifact: GraphDescriptorTreeArtifactMetadata,
}

impl PersistentPropertyProjectionManifest {
    pub const fn descriptor_generation_artifacts(&self) -> GraphDescriptorTreeGenerationArtifacts {
        GraphDescriptorTreeGenerationArtifacts {
            kind: GraphDescriptorKind::PropertyProjection,
            generation: self.generation.0,
            source_commit_epoch: self.source_commit_epoch,
            root_artifact: self.descriptor_root_artifact,
        }
    }

    pub fn validate(&self) -> Result<(), PersistentPropertyProjectionError> {
        if self.artifact_id != ARTIFACT_ID {
            return Err(PersistentPropertyProjectionError::Corrupt(
                "property projection manifest has an unsupported artifact id".to_string(),
            ));
        }
        if self.artifact_len < ARTIFACT_HEADER.len() as u64 + 8 {
            return Err(PersistentPropertyProjectionError::Corrupt(
                "property projection artifact is shorter than its header".to_string(),
            ));
        }
        if !self
            .definitions
            .windows(2)
            .all(|pair| definition_key(&pair[0]) < definition_key(&pair[1]))
        {
            return Err(PersistentPropertyProjectionError::Corrupt(
                "property projection definitions are not strictly ordered".to_string(),
            ));
        }
        for definition in &self.definitions {
            if definition.kind == PersistentPropertyProjectionKind::CompositeEquality {
                decode_composite_property_identity(&definition.property)?;
            }
        }
        if (self.entry_count == 0) != (self.block_count == 0) {
            return Err(PersistentPropertyProjectionError::Corrupt(
                "property projection entry and block emptiness are inconsistent".to_string(),
            ));
        }
        if self.descriptor_root_artifact.encoded_len == 0 {
            return Err(PersistentPropertyProjectionError::Corrupt(
                "property projection descriptor root artifact is empty".to_string(),
            ));
        }
        Ok(())
    }

    pub fn supports(
        &self,
        label_id: LabelId,
        property: &str,
        kind: PersistentPropertyProjectionKind,
    ) -> bool {
        self.definitions
            .binary_search_by(|definition| {
                definition_key(definition).cmp(&(kind, label_id, property))
            })
            .ok()
            .is_some_and(|index| self.definitions[index].complete)
    }

    pub fn supports_composite_equality(&self, label_id: LabelId, properties: &[String]) -> bool {
        persistent_composite_property_identity(properties)
            .ok()
            .is_some_and(|identity| {
                self.supports(
                    label_id,
                    &identity,
                    PersistentPropertyProjectionKind::CompositeEquality,
                )
            })
    }

    pub fn supports_relationship(
        &self,
        rel_type: RelTypeId,
        property: &str,
        kind: PersistentPropertyProjectionKind,
    ) -> bool {
        matches!(
            kind,
            PersistentPropertyProjectionKind::RelationshipEquality
                | PersistentPropertyProjectionKind::RelationshipRange
        ) && self.supports(LabelId(rel_type.0), property, kind)
    }

    pub fn encode(&self) -> Result<String, PersistentPropertyProjectionError> {
        self.validate()?;
        let mut body = format!(
            "{MANIFEST_HEADER}\ngeneration\t{}\nsource_commit_epoch\t{}\nartifact_id\t{}\nartifact_len\t{}\nartifact_digest\t{}\nartifact_sha256\t{}\nentry_count\t{}\nblock_count\t{}\ndescriptor_root_len\t{}\ndescriptor_root_crc32c\t{}\ndescriptor_root_sha256\t{}\n",
            self.generation.0,
            self.source_commit_epoch,
            self.artifact_id,
            self.artifact_len,
            self.artifact_digest.0,
            self.artifact_sha256,
            self.entry_count,
            self.block_count,
            self.descriptor_root_artifact.encoded_len,
            self.descriptor_root_artifact.encoded_crc32c,
            self.descriptor_root_artifact.encoded_sha256
        );
        for definition in &self.definitions {
            body.push_str(&format!(
                "definition\t{}\t{}\t{}\t{}\n",
                kind_tag(definition.kind),
                definition.label_id.0,
                encode_hex(definition.property.as_bytes()),
                u8::from(definition.complete)
            ));
        }
        let checksum = content_digest(body.as_bytes()).0;
        Ok(format!("{body}checksum\t{checksum}\n"))
    }

    pub fn decode(encoded: &str) -> Result<Self, PersistentPropertyProjectionError> {
        let marker = "checksum\t";
        let checksum_offset = encoded.rfind(marker).ok_or_else(|| {
            PersistentPropertyProjectionError::Corrupt(
                "property projection manifest is missing checksum".to_string(),
            )
        })?;
        let body = &encoded[..checksum_offset];
        let checksum_line = encoded[checksum_offset..].trim_end();
        if checksum_line.contains('\n') {
            return Err(PersistentPropertyProjectionError::Corrupt(
                "property projection manifest has data after checksum".to_string(),
            ));
        }
        let expected = parse_u64(
            checksum_line.strip_prefix(marker).unwrap_or_default(),
            "manifest checksum",
        )?;
        let actual = content_digest(body.as_bytes()).0;
        if expected != actual {
            return Err(PersistentPropertyProjectionError::Corrupt(format!(
                "property projection manifest checksum mismatch: expected {expected}, got {actual}"
            )));
        }
        let mut generation = None;
        let mut source_commit_epoch = None;
        let mut artifact_id = None;
        let mut artifact_len = None;
        let mut artifact_digest = None;
        let mut artifact_sha256 = None;
        let mut entry_count = None;
        let mut block_count = None;
        let mut descriptor_root_len = None;
        let mut descriptor_root_crc32c = None;
        let mut descriptor_root_sha256 = None;
        let mut definitions = Vec::new();
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
                        PersistentPropertyProjectionError::Corrupt(format!(
                            "invalid artifact SHA-256 digest: {error}"
                        ))
                    })?)
                }
                ["entry_count", value] => entry_count = Some(parse_u64(value, "entry count")?),
                ["block_count", value] => block_count = Some(parse_u64(value, "block count")?),
                ["descriptor_root_len", value] => {
                    descriptor_root_len = Some(parse_u64(value, "descriptor root length")?)
                }
                ["descriptor_root_crc32c", value] => {
                    descriptor_root_crc32c = Some(parse_u32(value, "descriptor root CRC32C")?)
                }
                ["descriptor_root_sha256", value] => {
                    descriptor_root_sha256 = Some(value.parse().map_err(|error| {
                        PersistentPropertyProjectionError::Corrupt(format!(
                            "invalid descriptor root SHA-256 digest: {error}"
                        ))
                    })?)
                }
                ["definition", kind, label, property, complete] => {
                    definitions.push(PersistentPropertyProjectionDefinition {
                        label_id: LabelId(parse_u32(label, "definition label")?),
                        property: decode_utf8_hex(property, "definition property")?,
                        kind: kind_from_tag(parse_u8(kind, "definition kind")?)?,
                        complete: match parse_u8(complete, "definition completeness")? {
                            0 => false,
                            1 => true,
                            value => {
                                return Err(PersistentPropertyProjectionError::Corrupt(format!(
                                    "invalid property projection completeness {value}"
                                )));
                            }
                        },
                    });
                }
                [""] => {}
                _ => {
                    return Err(PersistentPropertyProjectionError::Corrupt(format!(
                        "invalid property projection manifest line: {line}"
                    )));
                }
            }
        }
        if !saw_header {
            return Err(PersistentPropertyProjectionError::Corrupt(
                "property projection manifest has an invalid header".to_string(),
            ));
        }
        let manifest = Self {
            generation: ManifestGeneration(required(generation, "generation")?),
            source_commit_epoch: required(source_commit_epoch, "source commit epoch")?,
            artifact_id: required(artifact_id, "artifact id")?,
            artifact_len: required(artifact_len, "artifact length")?,
            artifact_digest: ContentDigest(required(artifact_digest, "artifact digest")?),
            artifact_sha256: required(artifact_sha256, "artifact SHA-256 digest")?,
            entry_count: required(entry_count, "entry count")?,
            block_count: required(block_count, "block count")?,
            definitions,
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PersistentPropertyProjectionBuildReport {
    pub definition_count: usize,
    pub definition_bytes: u64,
    pub input_record_count: u64,
    pub generated_entry_count: u64,
    pub persisted_entry_count: u64,
    pub block_count: u64,
    pub spill_run_count: usize,
    pub spill_bytes: u64,
    pub peak_resident_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct PersistentPropertyProjectionWriteOutput {
    pub manifest: PersistentPropertyProjectionManifest,
    pub report: PersistentPropertyProjectionBuildReport,
    pub descriptor_tree: GraphDescriptorTreeWriteOutput,
}

struct PreparedPropertyProjectionArtifact {
    artifact_len: u64,
    artifact_digest: ContentDigest,
    artifact_sha256: Sha256Digest,
    entry_count: u64,
    block_count: u64,
    definitions: Vec<PersistentPropertyProjectionDefinition>,
    descriptor_tree: PreparedGraphDescriptorTree,
    report: PersistentPropertyProjectionBuildReport,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct EntryKey {
    kind: PersistentPropertyProjectionKind,
    label_id: LabelId,
    property: String,
    value: Value,
    node_id: NodeId,
}

impl EntryKey {
    fn encoded_len(&self) -> Result<u64, PersistentPropertyProjectionError> {
        Ok(1u64
            .saturating_add(4)
            .saturating_add(4)
            .saturating_add(self.property.len() as u64)
            .saturating_add(4)
            .saturating_add(encode_standalone_value(&self.value)?.len() as u64)
            .saturating_add(8))
    }

    fn resident_bytes(&self) -> Result<u64, PersistentPropertyProjectionError> {
        Ok(self
            .encoded_len()?
            .saturating_add(std::mem::size_of::<Self>() as u64))
    }
}

pub struct PersistentPropertyProjectionWriter {
    config: PersistentPropertyProjectionConfig,
}

impl PersistentPropertyProjectionWriter {
    pub const fn new(config: PersistentPropertyProjectionConfig) -> Self {
        Self { config }
    }

    pub fn write_fallible<N>(
        &self,
        path: &Path,
        generation: ManifestGeneration,
        source_commit_epoch: u64,
        definitions: Vec<PersistentPropertyProjectionDefinition>,
        nodes: N,
        descriptor_tree: PersistentPropertyProjectionDescriptorTree,
    ) -> Result<PersistentPropertyProjectionWriteOutput, PersistentPropertyProjectionError>
    where
        N: IntoIterator<
            Item = Result<PersistentPropertyProjectionRecord, PersistentPropertyProjectionError>,
        >,
    {
        self.write_fallible_inner(
            path,
            generation,
            source_commit_epoch,
            definitions,
            nodes,
            descriptor_tree,
        )
    }

    fn write_fallible_inner<N>(
        &self,
        path: &Path,
        generation: ManifestGeneration,
        source_commit_epoch: u64,
        mut definitions: Vec<PersistentPropertyProjectionDefinition>,
        nodes: N,
        descriptor_tree: PersistentPropertyProjectionDescriptorTree,
    ) -> Result<PersistentPropertyProjectionWriteOutput, PersistentPropertyProjectionError>
    where
        N: IntoIterator<
            Item = Result<PersistentPropertyProjectionRecord, PersistentPropertyProjectionError>,
        >,
    {
        let (descriptor_paths, descriptor_config) = descriptor_tree.into_parts();
        definitions.sort_by(|left, right| definition_key(left).cmp(&definition_key(right)));
        definitions.dedup_by(|left, right| {
            left.label_id == right.label_id
                && left.property == right.property
                && left.kind == right.kind
        });
        let mut definition_admission =
            PersistentPropertyProjectionDefinitionAdmission::new(self.config);
        for definition in &definitions {
            definition_admission.admit(definition)?;
        }
        let definition_count = definition_admission.definition_count();
        let definition_bytes = definition_admission.resident_bytes();
        let mut by_subject: BTreeMap<ProjectionSubject, Vec<PreparedProjectionDefinition>> =
            BTreeMap::new();
        for (index, definition) in definitions.iter_mut().enumerate() {
            definition.complete = true;
            let value_source =
                if definition.kind == PersistentPropertyProjectionKind::CompositeEquality {
                    ProjectionValueSource::Composite(decode_composite_property_identity(
                        &definition.property,
                    )?)
                } else {
                    ProjectionValueSource::Scalar
                };
            by_subject
                .entry(definition_subject(definition))
                .or_default()
                .push(PreparedProjectionDefinition {
                    definition_index: index,
                    value_source,
                });
        }
        let mut runs = ProjectionSpillRuns::new(path, generation, self.config);
        let mut chunk = Vec::new();
        let mut chunk_bytes = 0u64;
        let mut generated_entries = 0u64;
        let mut input_records = 0u64;
        let mut peak_resident_bytes = 0u64;
        for record in nodes {
            let record = record?;
            input_records = input_records.saturating_add(1);
            let (subjects, properties, entity_id) = match &record {
                PersistentPropertyProjectionRecord::Node(node) => (
                    node.labels
                        .iter()
                        .copied()
                        .map(ProjectionSubject::Node)
                        .collect::<Vec<_>>(),
                    &node.properties,
                    NodeId(node.id.0),
                ),
                PersistentPropertyProjectionRecord::Relationship(relationship) => (
                    vec![ProjectionSubject::Relationship(relationship.rel_type)],
                    &relationship.properties,
                    NodeId(relationship.id.0),
                ),
            };
            for subject in subjects {
                let Some(indexes) = by_subject.get(&subject) else {
                    continue;
                };
                for prepared in indexes {
                    let definition_index = prepared.definition_index;
                    let definition = &definitions[definition_index];
                    match definition.kind {
                        PersistentPropertyProjectionKind::Equality
                        | PersistentPropertyProjectionKind::RelationshipEquality => {
                            let Some(value) = properties.get(&definition.property) else {
                                continue;
                            };
                            let encoded = encode_standalone_value(value)?;
                            if encoded.len() as u64 > self.config.max_index_key_bytes.get() {
                                definitions[definition_index].complete = false;
                                continue;
                            }
                            self.emit(
                                EntryKey {
                                    kind: definition.kind,
                                    label_id: definition.label_id,
                                    property: definition.property.clone(),
                                    value: value.clone(),
                                    node_id: entity_id,
                                },
                                &mut runs,
                                &mut chunk,
                                &mut chunk_bytes,
                                &mut generated_entries,
                                &mut peak_resident_bytes,
                            )?;
                        }
                        PersistentPropertyProjectionKind::Range
                        | PersistentPropertyProjectionKind::RelationshipRange => {
                            let Some(value) = properties.get(&definition.property) else {
                                continue;
                            };
                            if !is_range_value(value) {
                                continue;
                            }
                            let encoded = encode_standalone_value(value)?;
                            if encoded.len() as u64 > self.config.max_index_key_bytes.get() {
                                definitions[definition_index].complete = false;
                                continue;
                            }
                            self.emit(
                                EntryKey {
                                    kind: definition.kind,
                                    label_id: definition.label_id,
                                    property: definition.property.clone(),
                                    value: value.clone(),
                                    node_id: entity_id,
                                },
                                &mut runs,
                                &mut chunk,
                                &mut chunk_bytes,
                                &mut generated_entries,
                                &mut peak_resident_bytes,
                            )?;
                        }
                        PersistentPropertyProjectionKind::FullText => {
                            let Some(value) = properties.get(&definition.property) else {
                                continue;
                            };
                            let Value::String(value) = value else {
                                continue;
                            };
                            for token in full_text_tokens_streaming(value) {
                                self.emit(
                                    EntryKey {
                                        kind: definition.kind,
                                        label_id: definition.label_id,
                                        property: definition.property.clone(),
                                        value: Value::String(token),
                                        node_id: entity_id,
                                    },
                                    &mut runs,
                                    &mut chunk,
                                    &mut chunk_bytes,
                                    &mut generated_entries,
                                    &mut peak_resident_bytes,
                                )?;
                            }
                        }
                        PersistentPropertyProjectionKind::CompositeEquality => {
                            let ProjectionValueSource::Composite(composite_properties) =
                                &prepared.value_source
                            else {
                                return Err(PersistentPropertyProjectionError::Corrupt(
                                    "composite property projection has a scalar value source"
                                        .to_string(),
                                ));
                            };
                            let Some(values) = composite_properties
                                .iter()
                                .map(|property| properties.get(property).cloned())
                                .collect::<Option<Vec<_>>>()
                            else {
                                continue;
                            };
                            let value = Value::List(values);
                            let encoded = encode_standalone_value(&value)?;
                            if encoded.len() as u64 > self.config.max_index_key_bytes.get() {
                                definitions[definition_index].complete = false;
                                continue;
                            }
                            self.emit(
                                EntryKey {
                                    kind: definition.kind,
                                    label_id: definition.label_id,
                                    property: definition.property.clone(),
                                    value,
                                    node_id: entity_id,
                                },
                                &mut runs,
                                &mut chunk,
                                &mut chunk_bytes,
                                &mut generated_entries,
                                &mut peak_resident_bytes,
                            )?;
                        }
                    }
                }
            }
        }
        if !chunk.is_empty() {
            runs.spill(&mut chunk)?;
        }
        runs.compact()?;
        let tmp_path = path.with_extension("hawdb.tmp");
        let output = self.merge_runs(
            &tmp_path,
            generation,
            source_commit_epoch,
            definitions,
            definition_count,
            definition_bytes,
            input_records,
            generated_entries,
            peak_resident_bytes,
            &runs,
            descriptor_paths,
            descriptor_config,
        );
        let prepared = match output {
            Ok(output) => output,
            Err(error) => {
                let _ = fs::remove_file(&tmp_path);
                return Err(error);
            }
        };
        durable_replace_file(&tmp_path, path)?;
        let descriptor_tree = prepared.descriptor_tree.publish()?;
        if descriptor_tree.root.kind != GraphDescriptorKind::PropertyProjection
            || descriptor_tree.root.generation != generation.0
            || descriptor_tree.root.source_commit_epoch != source_commit_epoch
            || descriptor_tree.root.descriptor_count != prepared.block_count
        {
            return Err(PersistentPropertyProjectionError::Corrupt(
                "property projection descriptor root identity is inconsistent".to_string(),
            ));
        }
        let manifest = PersistentPropertyProjectionManifest {
            generation,
            source_commit_epoch,
            artifact_id: ARTIFACT_ID,
            artifact_len: prepared.artifact_len,
            artifact_digest: prepared.artifact_digest,
            artifact_sha256: prepared.artifact_sha256,
            entry_count: prepared.entry_count,
            block_count: prepared.block_count,
            definitions: prepared.definitions,
            descriptor_root_artifact: descriptor_tree.root_artifact,
        };
        manifest.validate()?;
        Ok(PersistentPropertyProjectionWriteOutput {
            manifest,
            report: prepared.report,
            descriptor_tree,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn emit(
        &self,
        entry: EntryKey,
        runs: &mut ProjectionSpillRuns,
        chunk: &mut Vec<EntryKey>,
        chunk_bytes: &mut u64,
        generated_entries: &mut u64,
        peak_resident_bytes: &mut u64,
    ) -> Result<(), PersistentPropertyProjectionError> {
        let required_entries = generated_entries.saturating_add(1);
        if required_entries > self.config.max_generated_entries.get() {
            return Err(
                PersistentPropertyProjectionError::GeneratedEntryBudgetExceeded {
                    required_entries,
                    max_entries: self.config.max_generated_entries.get(),
                },
            );
        }
        let entry_bytes = entry.resident_bytes()?;
        if entry_bytes > self.config.memory_budget_bytes.get() {
            return Err(PersistentPropertyProjectionError::MemoryBudgetExceeded {
                required_bytes: entry_bytes,
                max_bytes: self.config.memory_budget_bytes.get(),
            });
        }
        if !chunk.is_empty()
            && chunk_bytes.saturating_add(entry_bytes) > self.config.memory_budget_bytes.get()
        {
            runs.spill(chunk)?;
            *chunk_bytes = 0;
        }
        *chunk_bytes = chunk_bytes.saturating_add(entry_bytes);
        *peak_resident_bytes = (*peak_resident_bytes).max(*chunk_bytes);
        *generated_entries = required_entries;
        chunk.push(entry);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn merge_runs(
        &self,
        path: &Path,
        generation: ManifestGeneration,
        source_commit_epoch: u64,
        definitions: Vec<PersistentPropertyProjectionDefinition>,
        definition_count: usize,
        definition_bytes: u64,
        input_records: u64,
        generated_entries: u64,
        peak_resident_bytes: u64,
        runs: &ProjectionSpillRuns,
        descriptor_paths: GraphDescriptorTreePaths,
        descriptor_config: GraphDescriptorTreeBuildConfig,
    ) -> Result<PreparedPropertyProjectionArtifact, PersistentPropertyProjectionError> {
        let mut readers = runs
            .paths
            .iter()
            .map(|path| ProjectionRunReader::open(path, self.config.max_index_key_bytes))
            .collect::<Result<Vec<_>, _>>()?;
        let mut current = Vec::with_capacity(readers.len());
        let mut heap = BinaryHeap::new();
        for (index, reader) in readers.iter_mut().enumerate() {
            let key = reader.next_key()?;
            if let Some(key) = &key {
                heap.push(Reverse((key.clone(), index)));
            }
            current.push(key);
        }
        let file = File::create(path)?;
        let descriptor_tree = GraphDescriptorTreeBuilder::create(
            descriptor_paths,
            GraphDescriptorKind::PropertyProjection,
            generation.0,
            source_commit_epoch,
            DESCRIPTOR_ARTIFACT_ID,
            descriptor_config,
        )?;
        let mut artifact = ProjectionArtifactBuilder::new(
            file,
            generation,
            definitions,
            self.config,
            descriptor_tree,
        )?;
        let mut previous = None;
        while let Some(Reverse((key, run_index))) = heap.pop() {
            if current[run_index].as_ref() != Some(&key) {
                return Err(PersistentPropertyProjectionError::Corrupt(
                    "property projection spill heap does not match its reader".to_string(),
                ));
            }
            if previous.as_ref() != Some(&key) {
                artifact.push(key.clone())?;
                previous = Some(key);
            }
            current[run_index] = readers[run_index].next_key()?;
            if let Some(next) = &current[run_index] {
                heap.push(Reverse((next.clone(), run_index)));
            }
        }
        let mut prepared = artifact.finish()?;
        prepared.report = PersistentPropertyProjectionBuildReport {
            definition_count,
            definition_bytes,
            input_record_count: input_records,
            generated_entry_count: generated_entries,
            persisted_entry_count: prepared.entry_count,
            block_count: prepared.block_count,
            spill_run_count: runs.next_run_sequence,
            spill_bytes: runs.spill_bytes,
            peak_resident_bytes,
        };
        Ok(prepared)
    }
}

struct PreparedProjectionDefinition {
    definition_index: usize,
    value_source: ProjectionValueSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ProjectionSubject {
    Node(LabelId),
    Relationship(RelTypeId),
}

enum ProjectionValueSource {
    Scalar,
    Composite(Vec<String>),
}

struct ProjectionSpillRuns {
    prefix: PathBuf,
    generation: ManifestGeneration,
    config: PersistentPropertyProjectionConfig,
    paths: Vec<PathBuf>,
    spill_bytes: u64,
    next_run_sequence: usize,
}

impl ProjectionSpillRuns {
    fn new(
        path: &Path,
        generation: ManifestGeneration,
        config: PersistentPropertyProjectionConfig,
    ) -> Self {
        Self {
            prefix: path.to_path_buf(),
            generation,
            config,
            paths: Vec::new(),
            spill_bytes: 0,
            next_run_sequence: 0,
        }
    }

    fn spill(
        &mut self,
        entries: &mut Vec<EntryKey>,
    ) -> Result<(), PersistentPropertyProjectionError> {
        let required_runs = self.paths.len().saturating_add(1);
        if required_runs > self.config.max_spill_runs.get() {
            return Err(PersistentPropertyProjectionError::SpillRunBudgetExceeded {
                required_runs,
                max_runs: self.config.max_spill_runs.get(),
            });
        }
        entries.sort_unstable();
        entries.dedup();
        let run_bytes = entries
            .iter()
            .try_fold(RUN_HEADER.len() as u64, |bytes, entry| {
                entry
                    .encoded_len()
                    .map(|entry_bytes| bytes.saturating_add(entry_bytes))
            })?;
        let required_bytes = self.spill_bytes.saturating_add(run_bytes);
        if required_bytes > self.config.max_spill_bytes.get() {
            return Err(PersistentPropertyProjectionError::SpillBudgetExceeded {
                required_bytes,
                max_bytes: self.config.max_spill_bytes.get(),
            });
        }
        let path = self.next_path()?;
        let mut writer = BufWriter::new(File::create(&path)?);
        writer.write_all(RUN_HEADER)?;
        for entry in entries.iter() {
            write_entry_key(&mut writer, entry)?;
        }
        writer.flush()?;
        self.paths.push(path);
        self.spill_bytes = required_bytes;
        entries.clear();
        Ok(())
    }

    fn compact(&mut self) -> Result<(), PersistentPropertyProjectionError> {
        let fan_in = self.config.max_merge_fan_in.get();
        if fan_in < 2 {
            return Err(PersistentPropertyProjectionError::Corrupt(
                "property projection merge fan-in must be at least two".to_string(),
            ));
        }
        while self.paths.len() > fan_in {
            let old_paths = std::mem::take(&mut self.paths);
            let mut merged_paths = Vec::with_capacity(old_paths.len().div_ceil(fan_in));
            for group in old_paths.chunks(fan_in) {
                let path = self.next_path()?;
                let bytes = match merge_projection_run_group(group, &path, self.config) {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        let _ = fs::remove_file(&path);
                        for stale in old_paths.iter().chain(merged_paths.iter()) {
                            let _ = fs::remove_file(stale);
                        }
                        return Err(error);
                    }
                };
                let required_bytes = self.spill_bytes.saturating_add(bytes);
                if required_bytes > self.config.max_spill_bytes.get() {
                    let _ = fs::remove_file(&path);
                    for stale in old_paths.iter().chain(merged_paths.iter()) {
                        let _ = fs::remove_file(stale);
                    }
                    return Err(PersistentPropertyProjectionError::SpillBudgetExceeded {
                        required_bytes,
                        max_bytes: self.config.max_spill_bytes.get(),
                    });
                }
                self.spill_bytes = required_bytes;
                merged_paths.push(path);
                for source in group {
                    fs::remove_file(source)?;
                }
            }
            self.paths = merged_paths;
        }
        Ok(())
    }

    fn next_path(&mut self) -> Result<PathBuf, PersistentPropertyProjectionError> {
        let required_runs = self.next_run_sequence.saturating_add(1);
        if required_runs > self.config.max_spill_runs.get() {
            return Err(PersistentPropertyProjectionError::SpillRunBudgetExceeded {
                required_runs,
                max_runs: self.config.max_spill_runs.get(),
            });
        }
        let sequence = self.next_run_sequence;
        self.next_run_sequence = self.next_run_sequence.saturating_add(1);
        Ok(self.prefix.with_file_name(format!(
            ".property-index.{}.run.{sequence}.tmp",
            self.generation.0
        )))
    }
}

impl Drop for ProjectionSpillRuns {
    fn drop(&mut self) {
        for path in &self.paths {
            let _ = fs::remove_file(path);
        }
    }
}

struct ProjectionRunReader {
    reader: BufReader<File>,
    max_key_bytes: NonZeroU64,
}

impl ProjectionRunReader {
    fn open(
        path: &Path,
        max_key_bytes: NonZeroU64,
    ) -> Result<Self, PersistentPropertyProjectionError> {
        let mut reader = BufReader::new(File::open(path)?);
        let mut header = [0u8; 8];
        reader.read_exact(&mut header)?;
        if &header != RUN_HEADER {
            return Err(PersistentPropertyProjectionError::Corrupt(
                "property projection spill run has an invalid header".to_string(),
            ));
        }
        Ok(Self {
            reader,
            max_key_bytes,
        })
    }

    fn next_key(&mut self) -> Result<Option<EntryKey>, PersistentPropertyProjectionError> {
        let mut kind = [0u8; 1];
        match self.reader.read(&mut kind)? {
            0 => return Ok(None),
            1 => {}
            _ => unreachable!("one byte read buffer"),
        }
        let label_id = LabelId(read_u32(&mut self.reader)?);
        let property = read_bounded_string(&mut self.reader, self.max_key_bytes.get())?;
        let value_len = read_u32(&mut self.reader)? as usize;
        if value_len as u64 > self.max_key_bytes.get() {
            return Err(PersistentPropertyProjectionError::Corrupt(format!(
                "property projection spill key uses {value_len} bytes, exceeding {}",
                self.max_key_bytes
            )));
        }
        let mut value = vec![0u8; value_len];
        self.reader.read_exact(&mut value)?;
        Ok(Some(EntryKey {
            kind: kind_from_tag(kind[0])?,
            label_id,
            property,
            value: decode_standalone_value(&value)?,
            node_id: NodeId(read_u64(&mut self.reader)?),
        }))
    }
}

fn merge_projection_run_group(
    sources: &[PathBuf],
    destination: &Path,
    config: PersistentPropertyProjectionConfig,
) -> Result<u64, PersistentPropertyProjectionError> {
    let mut readers = sources
        .iter()
        .map(|path| ProjectionRunReader::open(path, config.max_index_key_bytes))
        .collect::<Result<Vec<_>, _>>()?;
    let mut current = Vec::with_capacity(readers.len());
    let mut heap = BinaryHeap::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        let key = reader.next_key()?;
        if let Some(key) = &key {
            heap.push(Reverse((key.clone(), index)));
        }
        current.push(key);
    }
    let mut writer = BufWriter::new(File::create(destination)?);
    writer.write_all(RUN_HEADER)?;
    let mut bytes = RUN_HEADER.len() as u64;
    let mut previous = None;
    while let Some(Reverse((key, run_index))) = heap.pop() {
        if current[run_index].as_ref() != Some(&key) {
            return Err(PersistentPropertyProjectionError::Corrupt(
                "property projection spill compaction heap mismatch".to_string(),
            ));
        }
        if previous.as_ref() != Some(&key) {
            write_entry_key(&mut writer, &key)?;
            bytes = bytes.saturating_add(key.encoded_len()?);
            previous = Some(key);
        }
        current[run_index] = readers[run_index].next_key()?;
        if let Some(next) = &current[run_index] {
            heap.push(Reverse((next.clone(), run_index)));
        }
    }
    writer.flush()?;
    Ok(bytes)
}

struct ProjectionArtifactBuilder {
    writer: BufWriter<File>,
    artifact_digest: IntegrityHasher,
    generation: ManifestGeneration,
    definitions: Vec<PersistentPropertyProjectionDefinition>,
    config: PersistentPropertyProjectionConfig,
    artifact_len: u64,
    next_block_id: u64,
    entry_count: u64,
    block_count: u64,
    pending: Vec<EntryKey>,
    pending_bytes: u64,
    pending_resident_bytes: u64,
    descriptor_tree: GraphDescriptorTreeBuilder,
}

impl ProjectionArtifactBuilder {
    fn new(
        file: File,
        generation: ManifestGeneration,
        definitions: Vec<PersistentPropertyProjectionDefinition>,
        config: PersistentPropertyProjectionConfig,
        descriptor_tree: GraphDescriptorTreeBuilder,
    ) -> Result<Self, PersistentPropertyProjectionError> {
        let mut writer = BufWriter::new(file);
        let mut artifact_digest = IntegrityHasher::new();
        write_hashed(&mut writer, &mut artifact_digest, ARTIFACT_HEADER)?;
        write_hashed(
            &mut writer,
            &mut artifact_digest,
            &generation.0.to_le_bytes(),
        )?;
        Ok(Self {
            writer,
            artifact_digest,
            generation,
            definitions,
            config,
            artifact_len: ARTIFACT_HEADER.len() as u64 + 8,
            next_block_id: BLOCK_ID_BASE,
            entry_count: 0,
            block_count: 0,
            pending: Vec::new(),
            pending_bytes: 0,
            pending_resident_bytes: 0,
            descriptor_tree,
        })
    }

    fn push(&mut self, entry: EntryKey) -> Result<(), PersistentPropertyProjectionError> {
        let entry_bytes = 4u64
            .saturating_add(encode_standalone_value(&entry.value)?.len() as u64)
            .saturating_add(8);
        let group_changed = self.pending.first().is_some_and(|first| {
            first.kind != entry.kind
                || first.label_id != entry.label_id
                || first.property != entry.property
        });
        let projected_header = 8u64
            .saturating_add(8)
            .saturating_add(8)
            .saturating_add(1)
            .saturating_add(4)
            .saturating_add(4)
            .saturating_add(entry.property.len() as u64)
            .saturating_add(4);
        let entry_resident_bytes = entry.resident_bytes()?;
        if entry_resident_bytes > self.config.memory_budget_bytes.get() {
            return Err(PersistentPropertyProjectionError::MemoryBudgetExceeded {
                required_bytes: entry_resident_bytes,
                max_bytes: self.config.memory_budget_bytes.get(),
            });
        }
        if !self.pending.is_empty()
            && (group_changed
                || projected_header
                    .saturating_add(self.pending_bytes)
                    .saturating_add(entry_bytes)
                    > self.config.target_block_bytes.get()
                || self
                    .pending_resident_bytes
                    .saturating_add(entry_resident_bytes)
                    > self.config.memory_budget_bytes.get())
        {
            self.flush_block()?;
        }
        self.pending_bytes = self.pending_bytes.saturating_add(entry_bytes);
        self.pending_resident_bytes = self
            .pending_resident_bytes
            .saturating_add(entry_resident_bytes);
        self.pending.push(entry);
        Ok(())
    }

    fn flush_block(&mut self) -> Result<(), PersistentPropertyProjectionError> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let first = self.pending.first().expect("projection block is non-empty");
        let property_len = u32::try_from(first.property.len()).map_err(|_| {
            PersistentPropertyProjectionError::Corrupt(
                "property projection property name exceeds u32".to_string(),
            )
        })?;
        let entry_count = u32::try_from(self.pending.len()).map_err(|_| {
            PersistentPropertyProjectionError::Corrupt(
                "property projection block count exceeds u32".to_string(),
            )
        })?;
        let header_bytes = 8u64
            .saturating_add(8)
            .saturating_add(8)
            .saturating_add(1)
            .saturating_add(4)
            .saturating_add(4)
            .saturating_add(first.property.len() as u64)
            .saturating_add(4);
        let block_bytes = header_bytes.saturating_add(self.pending_bytes);
        let hard_max = self.config.target_block_bytes.get().max(
            self.config
                .max_index_key_bytes
                .get()
                .saturating_add(header_bytes)
                .saturating_add(16),
        );
        if block_bytes > hard_max {
            return Err(PersistentPropertyProjectionError::BlockTooLarge {
                block_bytes,
                max_bytes: hard_max,
            });
        }
        let min_key = first.value.clone();
        let max_key = self
            .pending
            .last()
            .expect("projection block is non-empty")
            .value
            .clone();
        let mut block_digest = Crc32cHasher::new();
        for bytes in [
            BLOCK_HEADER.as_slice(),
            &self.generation.0.to_le_bytes(),
            &self.next_block_id.to_le_bytes(),
            &[kind_tag(first.kind)],
            &first.label_id.0.to_le_bytes(),
            &property_len.to_le_bytes(),
            first.property.as_bytes(),
            &entry_count.to_le_bytes(),
        ] {
            write_double_hashed(
                &mut self.writer,
                &mut self.artifact_digest,
                &mut block_digest,
                bytes,
            )?;
        }
        for entry in &self.pending {
            let value = encode_standalone_value(&entry.value)?;
            write_double_hashed(
                &mut self.writer,
                &mut self.artifact_digest,
                &mut block_digest,
                &(value.len() as u32).to_le_bytes(),
            )?;
            write_double_hashed(
                &mut self.writer,
                &mut self.artifact_digest,
                &mut block_digest,
                &value,
            )?;
            write_double_hashed(
                &mut self.writer,
                &mut self.artifact_digest,
                &mut block_digest,
                &entry.node_id.0.to_le_bytes(),
            )?;
        }
        let length = NonZeroU64::new(block_bytes).expect("projection block is non-empty");
        let descriptor = PersistentPropertyProjectionBlockDescriptor {
            block_id: self.next_block_id,
            label_id: first.label_id,
            property: first.property.clone(),
            kind: first.kind,
            min_key,
            max_key,
            offset: self.artifact_len,
            length,
            content_digest: ContentDigest(block_digest.finish()),
            entry_count,
        };
        self.descriptor_tree.push(
            descriptor.descriptor_tree_key(),
            descriptor.encode_descriptor_tree_value()?,
        )?;
        self.artifact_len = self.artifact_len.checked_add(block_bytes).ok_or_else(|| {
            PersistentPropertyProjectionError::Corrupt(
                "property projection artifact length overflows u64".to_string(),
            )
        })?;
        self.next_block_id = self.next_block_id.checked_add(1).ok_or_else(|| {
            PersistentPropertyProjectionError::Corrupt(
                "property projection block id overflows u64".to_string(),
            )
        })?;
        self.entry_count = self
            .entry_count
            .checked_add(u64::from(entry_count))
            .ok_or_else(|| {
                PersistentPropertyProjectionError::Corrupt(
                    "property projection entry count overflows u64".to_string(),
                )
            })?;
        self.block_count = self.block_count.checked_add(1).ok_or_else(|| {
            PersistentPropertyProjectionError::Corrupt(
                "property projection block count overflows u64".to_string(),
            )
        })?;
        self.pending.clear();
        self.pending_bytes = 0;
        self.pending_resident_bytes = 0;
        Ok(())
    }

    fn finish(
        mut self,
    ) -> Result<PreparedPropertyProjectionArtifact, PersistentPropertyProjectionError> {
        self.flush_block()?;
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        let artifact_integrity = self.artifact_digest.finish();
        let descriptor_tree = self.descriptor_tree.finish()?;
        Ok(PreparedPropertyProjectionArtifact {
            artifact_len: self.artifact_len,
            artifact_digest: ContentDigest(artifact_integrity.crc32c.as_u64()),
            artifact_sha256: artifact_integrity.sha256,
            entry_count: self.entry_count,
            block_count: self.block_count,
            definitions: self.definitions,
            descriptor_tree,
            report: PersistentPropertyProjectionBuildReport::default(),
        })
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PersistentPropertyProjectionReadReport {
    pub descriptor_pages_visited: u64,
    pub descriptor_page_bytes_decoded: u64,
    pub descriptor_storage_bytes_read: u64,
    pub descriptors_examined: u64,
    pub descriptor_cache_hits: u64,
    pub descriptor_cache_misses: u64,
    pub descriptor_cache_admission_rejections: u64,
    pub blocks_considered: u64,
    pub blocks_pruned: u64,
    pub blocks_read: u64,
    pub bytes_read: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub entries_decoded: u64,
    pub candidates_returned: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PersistentPropertyProjectionScrubReport {
    pub descriptor_pages_checked: u64,
    pub descriptors_checked: u64,
    pub descriptor_bytes_checked: u64,
    pub projection_blocks_checked: u64,
    pub projection_entries_checked: u64,
    pub projection_bytes_hashed: u64,
}

#[derive(Debug, Clone)]
pub struct PersistentPropertyProjectionReader {
    path: PathBuf,
    manifest: PersistentPropertyProjectionManifest,
    descriptor_reader: GraphDescriptorTreeDemandReader,
    range_reader: FileSegmentRangeReader,
    max_block_bytes: NonZeroU64,
    poisoned: Arc<AtomicBool>,
}

impl PersistentPropertyProjectionReader {
    pub fn open(
        path: impl Into<PathBuf>,
        manifest: PersistentPropertyProjectionManifest,
        descriptor_tree: PersistentPropertyProjectionDescriptorTree,
        cache: Arc<SegmentCache>,
        store_id: StoreId,
        max_block_bytes: NonZeroU64,
    ) -> Result<Self, PersistentPropertyProjectionError> {
        manifest.validate()?;
        let (descriptor_paths, descriptor_config) = descriptor_tree.into_parts();
        let path = path.into();
        let metadata = fs::metadata(&path)?;
        if metadata.len() != manifest.artifact_len {
            return Err(PersistentPropertyProjectionError::Corrupt(format!(
                "property projection artifact length mismatch: expected {}, got {}",
                manifest.artifact_len,
                metadata.len()
            )));
        }
        let mut header = [0u8; 24];
        File::open(&path)?.read_exact(&mut header)?;
        if &header[..16] != ARTIFACT_HEADER {
            return Err(PersistentPropertyProjectionError::Corrupt(
                "property projection artifact has an invalid header".to_string(),
            ));
        }
        let generation = u64::from_le_bytes(header[16..24].try_into().expect("fixed header"));
        if generation != manifest.generation.0 {
            return Err(PersistentPropertyProjectionError::Corrupt(format!(
                "property projection artifact generation {generation} does not match manifest generation {}",
                manifest.generation.0
            )));
        }
        let root_reader = GraphDescriptorTreeRootReader::open_bound(
            descriptor_paths,
            manifest.descriptor_generation_artifacts(),
            descriptor_config,
        )?;
        let root = root_reader.root();
        if root.kind != GraphDescriptorKind::PropertyProjection
            || root.generation != manifest.generation.0
            || root.source_commit_epoch != manifest.source_commit_epoch
            || root.page_artifact_id != DESCRIPTOR_ARTIFACT_ID
            || root.descriptor_count != manifest.block_count
            || root.root.is_some() != (manifest.block_count != 0)
        {
            return Err(PersistentPropertyProjectionError::Corrupt(
                "property projection descriptor root does not match its compact manifest"
                    .to_string(),
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

    pub fn manifest(&self) -> &PersistentPropertyProjectionManifest {
        &self.manifest
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire) || self.descriptor_reader.is_poisoned()
    }

    pub fn scan_range_candidates(
        &self,
        label_id: LabelId,
        property: &str,
        lower: Option<&(Value, bool)>,
        upper: Option<&(Value, bool)>,
        mut consumer: impl FnMut(
            NodeId,
        )
            -> Result<CanonicalScanControl, PersistentPropertyProjectionError>,
    ) -> Result<
        (PersistentPropertyProjectionReadReport, CanonicalScanControl),
        PersistentPropertyProjectionError,
    > {
        self.scan_candidates(
            label_id,
            property,
            PersistentPropertyProjectionKind::Range,
            |value| range_bounds_match(value, lower, upper),
            |block| range_block_might_match(block, lower, upper),
            &mut consumer,
        )
    }

    pub fn scan_equality_candidates(
        &self,
        label_id: LabelId,
        property: &str,
        value: &Value,
        mut consumer: impl FnMut(
            NodeId,
        )
            -> Result<CanonicalScanControl, PersistentPropertyProjectionError>,
    ) -> Result<
        (PersistentPropertyProjectionReadReport, CanonicalScanControl),
        PersistentPropertyProjectionError,
    > {
        self.scan_candidates(
            label_id,
            property,
            PersistentPropertyProjectionKind::Equality,
            |candidate| candidate == value,
            |block| block.min_key <= *value && *value <= block.max_key,
            &mut consumer,
        )
    }

    pub fn scan_composite_equality_candidates(
        &self,
        label_id: LabelId,
        properties: &[String],
        values: &[&Value],
        mut consumer: impl FnMut(
            NodeId,
        )
            -> Result<CanonicalScanControl, PersistentPropertyProjectionError>,
    ) -> Result<
        (PersistentPropertyProjectionReadReport, CanonicalScanControl),
        PersistentPropertyProjectionError,
    > {
        if properties.len() != values.len() {
            return Err(PersistentPropertyProjectionError::Source(
                "composite property projection key arity does not match its definition".to_string(),
            ));
        }
        let identity = persistent_composite_property_identity(properties)?;
        self.scan_candidates(
            label_id,
            &identity,
            PersistentPropertyProjectionKind::CompositeEquality,
            |candidate| {
                composite_key_ordering(candidate, values).is_some_and(|order| order.is_eq())
            },
            |block| {
                composite_key_ordering(&block.min_key, values).is_some_and(|order| order.is_le())
                    && composite_key_ordering(&block.max_key, values)
                        .is_some_and(|order| order.is_ge())
            },
            &mut consumer,
        )
    }

    pub fn scan_composite_range_candidates(
        &self,
        label_id: LabelId,
        index_properties: &[String],
        equality_values: &[&Value],
        lower: Option<&(Value, bool)>,
        upper: Option<&(Value, bool)>,
        mut consumer: impl FnMut(
            NodeId,
        )
            -> Result<CanonicalScanControl, PersistentPropertyProjectionError>,
    ) -> Result<
        (PersistentPropertyProjectionReadReport, CanonicalScanControl),
        PersistentPropertyProjectionError,
    > {
        if equality_values.is_empty() || equality_values.len() >= index_properties.len() {
            return Err(PersistentPropertyProjectionError::Source(
                "composite range projection requires a non-empty leading equality prefix"
                    .to_string(),
            ));
        }
        if lower.is_none() && upper.is_none() {
            return Err(PersistentPropertyProjectionError::Source(
                "composite range projection requires at least one range bound".to_string(),
            ));
        }
        let identity = persistent_composite_property_identity(index_properties)?;
        self.scan_candidates(
            label_id,
            &identity,
            PersistentPropertyProjectionKind::CompositeEquality,
            |candidate| composite_range_key_matches(candidate, equality_values, lower, upper),
            |block| {
                composite_range_block_might_match(
                    &block.min_key,
                    &block.max_key,
                    equality_values,
                    lower,
                    upper,
                )
            },
            &mut consumer,
        )
    }

    pub fn scan_relationship_equality_candidates(
        &self,
        rel_type: RelTypeId,
        property: &str,
        value: &Value,
        mut consumer: impl FnMut(
            RelId,
        )
            -> Result<CanonicalScanControl, PersistentPropertyProjectionError>,
    ) -> Result<
        (PersistentPropertyProjectionReadReport, CanonicalScanControl),
        PersistentPropertyProjectionError,
    > {
        self.scan_candidates(
            LabelId(rel_type.0),
            property,
            PersistentPropertyProjectionKind::RelationshipEquality,
            |candidate| candidate == value,
            |block| block.min_key <= *value && *value <= block.max_key,
            &mut |id| consumer(RelId(id.0)),
        )
    }

    pub fn scan_relationship_range_candidates(
        &self,
        rel_type: RelTypeId,
        property: &str,
        lower: Option<&(Value, bool)>,
        upper: Option<&(Value, bool)>,
        mut consumer: impl FnMut(
            RelId,
        )
            -> Result<CanonicalScanControl, PersistentPropertyProjectionError>,
    ) -> Result<
        (PersistentPropertyProjectionReadReport, CanonicalScanControl),
        PersistentPropertyProjectionError,
    > {
        self.scan_candidates(
            LabelId(rel_type.0),
            property,
            PersistentPropertyProjectionKind::RelationshipRange,
            |value| range_bounds_match(value, lower, upper),
            |block| range_block_might_match(block, lower, upper),
            &mut |id| consumer(RelId(id.0)),
        )
    }

    pub fn estimate_relationship_equality_entries(
        &self,
        rel_type: RelTypeId,
        property: &str,
        value: &Value,
    ) -> Result<(u64, PersistentPropertyProjectionReadReport), PersistentPropertyProjectionError>
    {
        self.estimate_matching_entries(
            PersistentPropertyProjectionKind::RelationshipEquality,
            LabelId(rel_type.0),
            property,
            |block| block.min_key <= *value && *value <= block.max_key,
        )
    }

    pub fn estimate_relationship_range_entries(
        &self,
        rel_type: RelTypeId,
        property: &str,
        lower: Option<&(Value, bool)>,
        upper: Option<&(Value, bool)>,
    ) -> Result<(u64, PersistentPropertyProjectionReadReport), PersistentPropertyProjectionError>
    {
        self.estimate_matching_entries(
            PersistentPropertyProjectionKind::RelationshipRange,
            LabelId(rel_type.0),
            property,
            |block| range_block_might_match(block, lower, upper),
        )
    }

    pub fn scan_full_text_token_candidates(
        &self,
        label_id: LabelId,
        property: &str,
        token: &str,
        mut consumer: impl FnMut(
            NodeId,
        )
            -> Result<CanonicalScanControl, PersistentPropertyProjectionError>,
    ) -> Result<
        (PersistentPropertyProjectionReadReport, CanonicalScanControl),
        PersistentPropertyProjectionError,
    > {
        let token = Value::String(token.to_string());
        self.scan_candidates(
            label_id,
            property,
            PersistentPropertyProjectionKind::FullText,
            |value| value == &token,
            |block| block.min_key <= token && token <= block.max_key,
            &mut consumer,
        )
    }

    pub fn estimate_full_text_token_entries(
        &self,
        label_id: LabelId,
        property: &str,
        token: &str,
    ) -> Result<(u64, PersistentPropertyProjectionReadReport), PersistentPropertyProjectionError>
    {
        let token = Value::String(token.to_string());
        self.estimate_matching_entries(
            PersistentPropertyProjectionKind::FullText,
            label_id,
            property,
            |block| block.min_key <= token && token <= block.max_key,
        )
    }

    fn scan_candidates(
        &self,
        label_id: LabelId,
        property: &str,
        kind: PersistentPropertyProjectionKind,
        mut value_matches: impl FnMut(&Value) -> bool,
        mut block_matches: impl FnMut(&PersistentPropertyProjectionBlockDescriptor) -> bool,
        consumer: &mut impl FnMut(
            NodeId,
        )
            -> Result<CanonicalScanControl, PersistentPropertyProjectionError>,
    ) -> Result<
        (PersistentPropertyProjectionReadReport, CanonicalScanControl),
        PersistentPropertyProjectionError,
    > {
        self.ensure_healthy()?;
        if !self.manifest.supports(label_id, property, kind) {
            return Err(PersistentPropertyProjectionError::Source(
                "requested persistent property projection is unavailable or incomplete".to_string(),
            ));
        }
        let mut report = PersistentPropertyProjectionReadReport::default();
        let prefix = property_projection_descriptor_prefix(kind, label_id, property);
        let mut scan_control = CanonicalScanControl::Continue;
        let mut scan_error = None;
        let descriptor_result = self.descriptor_reader.scan_prefix(
            &prefix,
            GraphDescriptorTreeReadLimits::default(),
            |key, value| {
                let step =
                    PersistentPropertyProjectionBlockDescriptor::decode_descriptor_tree_entry(
                        key, value,
                    )
                    .and_then(|block| {
                        self.validate_selected_descriptor(&block, kind, label_id, property)?;
                        report.blocks_considered =
                            report.blocks_considered.checked_add(1).ok_or_else(|| {
                                PersistentPropertyProjectionError::Corrupt(
                                    "property projection block accounting overflow".to_string(),
                                )
                            })?;
                        if !block_matches(&block) {
                            report.blocks_pruned =
                                report.blocks_pruned.checked_add(1).ok_or_else(|| {
                                    PersistentPropertyProjectionError::Corrupt(
                                        "property projection prune accounting overflow".to_string(),
                                    )
                                })?;
                            return Ok(CanonicalScanControl::Continue);
                        }
                        self.scan_one_block(&block, &mut report, &mut value_matches, consumer)
                    });
                match step {
                    Ok(control) => {
                        scan_control = control;
                        Ok(if control == CanonicalScanControl::Stop {
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
        let result = match (descriptor_result, scan_error) {
            (_, Some(error)) => Err(error),
            (Err(error), None) => Err(error.into()),
            (Ok((descriptor_report, _)), None) => {
                report.record_descriptor_read(descriptor_report);
                Ok((report, scan_control))
            }
        };
        self.poison_on_physical_failure(&result);
        result
    }

    fn estimate_matching_entries(
        &self,
        kind: PersistentPropertyProjectionKind,
        label_id: LabelId,
        property: &str,
        mut block_matches: impl FnMut(&PersistentPropertyProjectionBlockDescriptor) -> bool,
    ) -> Result<(u64, PersistentPropertyProjectionReadReport), PersistentPropertyProjectionError>
    {
        self.ensure_healthy()?;
        if !self.manifest.supports(label_id, property, kind) {
            return Err(PersistentPropertyProjectionError::Source(
                "requested persistent property projection is unavailable or incomplete".to_string(),
            ));
        }
        let prefix = property_projection_descriptor_prefix(kind, label_id, property);
        let mut estimate = 0u64;
        let mut report = PersistentPropertyProjectionReadReport::default();
        let mut scan_error = None;
        let descriptor_result = self.descriptor_reader.scan_prefix(
            &prefix,
            GraphDescriptorTreeReadLimits::default(),
            |key, value| {
                let step =
                    PersistentPropertyProjectionBlockDescriptor::decode_descriptor_tree_entry(
                        key, value,
                    )
                    .and_then(|block| {
                        self.validate_selected_descriptor(&block, kind, label_id, property)?;
                        report.blocks_considered =
                            report.blocks_considered.checked_add(1).ok_or_else(|| {
                                PersistentPropertyProjectionError::Corrupt(
                                    "property projection estimate block accounting overflow"
                                        .to_string(),
                                )
                            })?;
                        if block_matches(&block) {
                            estimate = estimate
                                .checked_add(u64::from(block.entry_count))
                                .ok_or_else(|| {
                                    PersistentPropertyProjectionError::Corrupt(
                                        "property projection estimate overflows u64".to_string(),
                                    )
                                })?;
                        } else {
                            report.blocks_pruned =
                                report.blocks_pruned.checked_add(1).ok_or_else(|| {
                                    PersistentPropertyProjectionError::Corrupt(
                                        "property projection estimate prune accounting overflow"
                                            .to_string(),
                                    )
                                })?;
                        }
                        Ok(())
                    });
                match step {
                    Ok(()) => Ok(GraphDescriptorTreeScanControl::Continue),
                    Err(error) => {
                        scan_error = Some(error);
                        Ok(GraphDescriptorTreeScanControl::Stop)
                    }
                }
            },
        );
        let result = match (descriptor_result, scan_error) {
            (_, Some(error)) => Err(error),
            (Err(error), None) => Err(error.into()),
            (Ok((descriptor_report, _)), None) => {
                report.record_descriptor_read(descriptor_report);
                Ok((estimate, report))
            }
        };
        self.poison_on_physical_failure(&result);
        result
    }

    fn scan_one_block(
        &self,
        block: &PersistentPropertyProjectionBlockDescriptor,
        report: &mut PersistentPropertyProjectionReadReport,
        value_matches: &mut impl FnMut(&Value) -> bool,
        consumer: &mut impl FnMut(
            NodeId,
        )
            -> Result<CanonicalScanControl, PersistentPropertyProjectionError>,
    ) -> Result<CanonicalScanControl, PersistentPropertyProjectionError> {
        let read = self.read_block(block)?;
        report.blocks_read = report.blocks_read.checked_add(1).ok_or_else(|| {
            PersistentPropertyProjectionError::Corrupt(
                "property projection read block accounting overflow".to_string(),
            )
        })?;
        report.bytes_read = report
            .bytes_read
            .checked_add(read.payload.len() as u64)
            .ok_or_else(|| {
                PersistentPropertyProjectionError::Corrupt(
                    "property projection read byte accounting overflow".to_string(),
                )
            })?;
        report.cache_hits = report
            .cache_hits
            .checked_add(u64::from(read.cache_hit))
            .ok_or_else(|| {
                PersistentPropertyProjectionError::Corrupt(
                    "property projection cache hit accounting overflow".to_string(),
                )
            })?;
        report.cache_misses = report
            .cache_misses
            .checked_add(u64::from(read.cache_miss))
            .ok_or_else(|| {
                PersistentPropertyProjectionError::Corrupt(
                    "property projection cache miss accounting overflow".to_string(),
                )
            })?;
        let mut control = CanonicalScanControl::Continue;
        decode_projection_block(
            &read.payload,
            self.manifest.generation,
            block,
            |value, node_id| {
                report.entries_decoded =
                    report.entries_decoded.checked_add(1).ok_or_else(|| {
                        PersistentPropertyProjectionError::Corrupt(
                            "property projection decoded entry accounting overflow".to_string(),
                        )
                    })?;
                if control == CanonicalScanControl::Continue && value_matches(&value) {
                    report.candidates_returned =
                        report.candidates_returned.checked_add(1).ok_or_else(|| {
                            PersistentPropertyProjectionError::Corrupt(
                                "property projection candidate accounting overflow".to_string(),
                            )
                        })?;
                    control = consumer(node_id)?;
                }
                Ok(())
            },
        )?;
        Ok(control)
    }

    fn validate_selected_descriptor(
        &self,
        block: &PersistentPropertyProjectionBlockDescriptor,
        kind: PersistentPropertyProjectionKind,
        label_id: LabelId,
        property: &str,
    ) -> Result<(), PersistentPropertyProjectionError> {
        if block.kind != kind || block.label_id != label_id || block.property != property {
            return Err(PersistentPropertyProjectionError::Corrupt(
                "property projection descriptor escaped its selected prefix".to_string(),
            ));
        }
        if !self
            .manifest
            .supports(block.label_id, &block.property, block.kind)
        {
            return Err(PersistentPropertyProjectionError::Corrupt(format!(
                "property projection block {} has no complete definition",
                block.block_id
            )));
        }
        let minimum_offset = (ARTIFACT_HEADER.len() + 8) as u64;
        let end = block
            .offset
            .checked_add(block.length.get())
            .ok_or_else(|| {
                PersistentPropertyProjectionError::Corrupt(
                    "property projection block range overflows u64".to_string(),
                )
            })?;
        if block.offset < minimum_offset || end > self.manifest.artifact_len {
            return Err(PersistentPropertyProjectionError::Corrupt(format!(
                "property projection block {} is outside its selected artifact",
                block.block_id
            )));
        }
        if block.length.get() > self.max_block_bytes.get() {
            return Err(PersistentPropertyProjectionError::BlockTooLarge {
                block_bytes: block.length.get(),
                max_bytes: self.max_block_bytes.get(),
            });
        }
        Ok(())
    }

    fn read_block(
        &self,
        block: &PersistentPropertyProjectionBlockDescriptor,
    ) -> Result<SegmentRangeRead, PersistentPropertyProjectionError> {
        if block.length.get() > self.max_block_bytes.get() {
            return Err(PersistentPropertyProjectionError::BlockTooLarge {
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

    pub fn deep_scrub(
        &self,
    ) -> Result<PersistentPropertyProjectionScrubReport, PersistentPropertyProjectionError> {
        self.ensure_healthy()?;
        let result = self.deep_scrub_inner();
        self.poison_on_physical_failure(&result);
        result
    }

    fn deep_scrub_inner(
        &self,
    ) -> Result<PersistentPropertyProjectionScrubReport, PersistentPropertyProjectionError> {
        let projection_bytes_hashed = self.verify_whole_artifact()?;
        let mut artifact = File::open(&self.path)?;
        let mut expected_offset = (ARTIFACT_HEADER.len() + 8) as u64;
        let mut expected_block_id = BLOCK_ID_BASE;
        let mut blocks_checked = 0u64;
        let mut entries_checked = 0u64;
        let mut visit_error = None;
        let scrub = self.descriptor_reader.deep_visit(|key, value| {
            let step = PersistentPropertyProjectionBlockDescriptor::decode_descriptor_tree_entry(
                key, value,
            )
            .and_then(|block| {
                if block.offset != expected_offset || block.block_id != expected_block_id {
                    return Err(PersistentPropertyProjectionError::Corrupt(format!(
                        "property projection descriptor closure is not contiguous at block {}",
                        block.block_id
                    )));
                }
                if !self
                    .manifest
                    .supports(block.label_id, &block.property, block.kind)
                {
                    return Err(PersistentPropertyProjectionError::Corrupt(format!(
                        "property projection block {} has no complete definition",
                        block.block_id
                    )));
                }
                let encoded =
                    read_projection_block_uncached(&mut artifact, &block, self.max_block_bytes)?;
                let mut decoded = 0u64;
                decode_projection_block(&encoded, self.manifest.generation, &block, |_, _| {
                    decoded = decoded.checked_add(1).ok_or_else(|| {
                        PersistentPropertyProjectionError::Corrupt(
                            "property projection scrub entry count overflow".to_string(),
                        )
                    })?;
                    Ok(())
                })?;
                entries_checked = entries_checked.checked_add(decoded).ok_or_else(|| {
                    PersistentPropertyProjectionError::Corrupt(
                        "property projection scrub total entry count overflow".to_string(),
                    )
                })?;
                blocks_checked = blocks_checked.checked_add(1).ok_or_else(|| {
                    PersistentPropertyProjectionError::Corrupt(
                        "property projection scrub block count overflow".to_string(),
                    )
                })?;
                expected_offset =
                    expected_offset
                        .checked_add(block.length.get())
                        .ok_or_else(|| {
                            PersistentPropertyProjectionError::Corrupt(
                                "property projection scrub offset overflow".to_string(),
                            )
                        })?;
                expected_block_id = expected_block_id.checked_add(1).ok_or_else(|| {
                    PersistentPropertyProjectionError::Corrupt(
                        "property projection scrub block id overflow".to_string(),
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
            || blocks_checked != self.manifest.block_count
            || entries_checked != self.manifest.entry_count
            || scrub.checked_descriptors != self.manifest.block_count
        {
            return Err(PersistentPropertyProjectionError::Corrupt(format!(
                "property projection scrub closure bytes/blocks/entries {expected_offset}/{blocks_checked}/{entries_checked} do not match {}/{}/{}",
                self.manifest.artifact_len,
                self.manifest.block_count,
                self.manifest.entry_count
            )));
        }
        Ok(PersistentPropertyProjectionScrubReport {
            descriptor_pages_checked: scrub.checked_pages,
            descriptors_checked: scrub.checked_descriptors,
            descriptor_bytes_checked: scrub.page_bytes_decoded,
            projection_blocks_checked: blocks_checked,
            projection_entries_checked: entries_checked,
            projection_bytes_hashed,
        })
    }

    fn verify_whole_artifact(&self) -> Result<u64, PersistentPropertyProjectionError> {
        let mut file = File::open(&self.path)?;
        if file.metadata()?.len() != self.manifest.artifact_len {
            return Err(PersistentPropertyProjectionError::Corrupt(
                "property projection artifact length changed after open".to_string(),
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
                PersistentPropertyProjectionError::Corrupt(
                    "property projection scrub byte count overflow".to_string(),
                )
            })?;
        }
        let digest = hasher.finish();
        if digest.crc32c.as_u64() != self.manifest.artifact_digest.0
            || digest.sha256 != self.manifest.artifact_sha256
        {
            return Err(PersistentPropertyProjectionError::Corrupt(
                "property projection artifact checksum mismatch during scrub".to_string(),
            ));
        }
        Ok(total)
    }

    fn ensure_healthy(&self) -> Result<(), PersistentPropertyProjectionError> {
        if self.is_poisoned() {
            return Err(PersistentPropertyProjectionError::Corrupt(
                "property projection reader is poisoned by an earlier physical failure".to_string(),
            ));
        }
        Ok(())
    }

    fn poison_on_physical_failure<T>(&self, result: &Result<T, PersistentPropertyProjectionError>) {
        if result
            .as_ref()
            .is_err_and(property_projection_error_requires_poison)
        {
            self.poisoned.store(true, Ordering::Release);
        }
    }
}

impl PersistentPropertyProjectionReadReport {
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

fn read_projection_block_uncached(
    file: &mut File,
    block: &PersistentPropertyProjectionBlockDescriptor,
    max_block_bytes: NonZeroU64,
) -> Result<Vec<u8>, PersistentPropertyProjectionError> {
    if block.length.get() > max_block_bytes.get() {
        return Err(PersistentPropertyProjectionError::BlockTooLarge {
            block_bytes: block.length.get(),
            max_bytes: max_block_bytes.get(),
        });
    }
    let length = usize::try_from(block.length.get()).map_err(|_| {
        PersistentPropertyProjectionError::BlockTooLarge {
            block_bytes: block.length.get(),
            max_bytes: usize::MAX as u64,
        }
    })?;
    file.seek(SeekFrom::Start(block.offset))?;
    let mut encoded = vec![0u8; length];
    file.read_exact(&mut encoded)?;
    if content_digest(&encoded) != block.content_digest {
        return Err(PersistentPropertyProjectionError::Corrupt(format!(
            "property projection block {} failed content digest verification",
            block.block_id
        )));
    }
    Ok(encoded)
}

fn property_projection_error_requires_poison(error: &PersistentPropertyProjectionError) -> bool {
    match error {
        PersistentPropertyProjectionError::Io(_)
        | PersistentPropertyProjectionError::Read(_)
        | PersistentPropertyProjectionError::Corrupt(_) => true,
        PersistentPropertyProjectionError::DescriptorTree(error) => matches!(
            error,
            GraphDescriptorTreeError::Io(_)
                | GraphDescriptorTreeError::Page(GraphDescriptorPageError::Corrupt(_))
                | GraphDescriptorTreeError::Corrupt(_)
        ),
        PersistentPropertyProjectionError::Canonical(_)
        | PersistentPropertyProjectionError::Source(_)
        | PersistentPropertyProjectionError::MemoryBudgetExceeded { .. }
        | PersistentPropertyProjectionError::SpillBudgetExceeded { .. }
        | PersistentPropertyProjectionError::SpillRunBudgetExceeded { .. }
        | PersistentPropertyProjectionError::GeneratedEntryBudgetExceeded { .. }
        | PersistentPropertyProjectionError::DefinitionCountBudgetExceeded { .. }
        | PersistentPropertyProjectionError::DefinitionBytesBudgetExceeded { .. }
        | PersistentPropertyProjectionError::BlockTooLarge { .. } => false,
    }
}

fn decode_projection_block(
    bytes: &[u8],
    generation: ManifestGeneration,
    descriptor: &PersistentPropertyProjectionBlockDescriptor,
    mut consumer: impl FnMut(Value, NodeId) -> Result<(), PersistentPropertyProjectionError>,
) -> Result<(), PersistentPropertyProjectionError> {
    let mut cursor = Cursor::new(bytes);
    if cursor.read_exact(8)? != BLOCK_HEADER {
        return Err(PersistentPropertyProjectionError::Corrupt(format!(
            "property projection block {} has an invalid header",
            descriptor.block_id
        )));
    }
    let stored_generation = cursor.read_u64()?;
    let block_id = cursor.read_u64()?;
    let kind = kind_from_tag(cursor.read_u8()?)?;
    let label_id = LabelId(cursor.read_u32()?);
    let property = cursor.read_string()?;
    let entry_count = cursor.read_u32()?;
    if stored_generation != generation.0
        || block_id != descriptor.block_id
        || kind != descriptor.kind
        || label_id != descriptor.label_id
        || property != descriptor.property
        || entry_count != descriptor.entry_count
    {
        return Err(PersistentPropertyProjectionError::Corrupt(format!(
            "property projection block {} metadata does not match its manifest",
            descriptor.block_id
        )));
    }
    let composite_arity = if kind == PersistentPropertyProjectionKind::CompositeEquality {
        Some(decode_composite_property_identity(&property)?.len())
    } else {
        None
    };
    let mut first = None;
    let mut previous = None;
    for _ in 0..entry_count {
        let value_len = cursor.read_u32()? as usize;
        let value = decode_standalone_value(cursor.read_exact(value_len)?)?;
        if composite_arity.is_some_and(|arity| !composite_key_has_arity(&value, arity)) {
            return Err(PersistentPropertyProjectionError::Corrupt(format!(
                "composite property projection block {} contains a key with invalid arity",
                descriptor.block_id
            )));
        }
        let node_id = NodeId(cursor.read_u64()?);
        let key = (value.clone(), node_id);
        if previous.as_ref().is_some_and(|previous| previous >= &key) {
            return Err(PersistentPropertyProjectionError::Corrupt(format!(
                "property projection block {} entries are not strictly ordered",
                descriptor.block_id
            )));
        }
        consumer(value, node_id)?;
        if first.is_none() {
            first = Some(key.clone());
        }
        previous = Some(key);
    }
    if !cursor.is_empty()
        || first.as_ref().map(|value| &value.0) != Some(&descriptor.min_key)
        || previous.as_ref().map(|value| &value.0) != Some(&descriptor.max_key)
    {
        return Err(PersistentPropertyProjectionError::Corrupt(format!(
            "property projection block {} payload bounds are inconsistent",
            descriptor.block_id
        )));
    }
    Ok(())
}

fn write_entry_key(
    writer: &mut impl Write,
    entry: &EntryKey,
) -> Result<(), PersistentPropertyProjectionError> {
    writer.write_all(&[kind_tag(entry.kind)])?;
    writer.write_all(&entry.label_id.0.to_le_bytes())?;
    write_string(writer, &entry.property)?;
    let value = encode_standalone_value(&entry.value)?;
    writer.write_all(&(value.len() as u32).to_le_bytes())?;
    writer.write_all(&value)?;
    writer.write_all(&entry.node_id.0.to_le_bytes())?;
    Ok(())
}

fn full_text_tokens_streaming(value: &str) -> impl Iterator<Item = String> + '_ {
    let mut window = VecDeque::with_capacity(3);
    value
        .chars()
        .flat_map(char::to_lowercase)
        .flat_map(move |ch| {
            if window.len() == 3 {
                window.pop_front();
            }
            window.push_back(ch);
            let chars = window.iter().copied().collect::<Vec<_>>();
            (1..=chars.len())
                .rev()
                .filter_map(|width| {
                    let token = chars[chars.len() - width..].iter().collect::<String>();
                    (!token.chars().all(char::is_whitespace)).then_some(token)
                })
                .collect::<Vec<_>>()
        })
}

fn is_range_value(value: &Value) -> bool {
    matches!(value, Value::Int(_) | Value::Float(_) | Value::String(_))
}

fn composite_key_has_arity(value: &Value, arity: usize) -> bool {
    matches!(value, Value::List(values) if values.len() == arity)
}

fn composite_key_ordering(value: &Value, expected: &[&Value]) -> Option<std::cmp::Ordering> {
    let Value::List(values) = value else {
        return None;
    };
    for (left, right) in values.iter().zip(expected) {
        let ordering = left.cmp(right);
        if !ordering.is_eq() {
            return Some(ordering);
        }
    }
    Some(values.len().cmp(&expected.len()))
}

fn composite_range_key_matches(
    value: &Value,
    equality_values: &[&Value],
    lower: Option<&(Value, bool)>,
    upper: Option<&(Value, bool)>,
) -> bool {
    let Value::List(values) = value else {
        return false;
    };
    if values.len() <= equality_values.len()
        || !values
            .iter()
            .zip(equality_values)
            .all(|(candidate, expected)| candidate == *expected)
    {
        return false;
    }
    range_bounds_match(&values[equality_values.len()], lower, upper)
}

fn composite_range_block_might_match(
    min_key: &Value,
    max_key: &Value,
    equality_values: &[&Value],
    lower: Option<&(Value, bool)>,
    upper: Option<&(Value, bool)>,
) -> bool {
    let (Value::List(min_values), Value::List(max_values)) = (min_key, max_key) else {
        return false;
    };
    let prefix_len = equality_values.len();
    if min_values.len() <= prefix_len || max_values.len() <= prefix_len {
        return false;
    }
    let Some(min_order) = composite_prefix_ordering(min_values, equality_values) else {
        return false;
    };
    let Some(max_order) = composite_prefix_ordering(max_values, equality_values) else {
        return false;
    };
    if max_order.is_lt() || min_order.is_gt() {
        return false;
    }
    if max_order.is_eq() && !range_bounds_match(&max_values[prefix_len], lower, None) {
        return false;
    }
    if min_order.is_eq() && !range_bounds_match(&min_values[prefix_len], None, upper) {
        return false;
    }
    true
}

fn composite_prefix_ordering(values: &[Value], expected: &[&Value]) -> Option<std::cmp::Ordering> {
    if values.len() < expected.len() {
        return None;
    }
    for (candidate, expected) in values.iter().zip(expected) {
        let ordering = candidate.cmp(expected);
        if !ordering.is_eq() {
            return Some(ordering);
        }
    }
    Some(std::cmp::Ordering::Equal)
}

fn range_block_might_match(
    block: &PersistentPropertyProjectionBlockDescriptor,
    lower: Option<&(Value, bool)>,
    upper: Option<&(Value, bool)>,
) -> bool {
    if let Some((lower, inclusive)) = lower
        && let Some(ordering) = comparable_value_ordering(&block.max_key, lower)
        && (ordering.is_lt() || (ordering.is_eq() && !inclusive))
    {
        return false;
    }
    if let Some((upper, inclusive)) = upper
        && let Some(ordering) = comparable_value_ordering(&block.min_key, upper)
        && (ordering.is_gt() || (ordering.is_eq() && !inclusive))
    {
        return false;
    }
    true
}

fn definition_key(
    definition: &PersistentPropertyProjectionDefinition,
) -> (PersistentPropertyProjectionKind, LabelId, &str) {
    (definition.kind, definition.label_id, &definition.property)
}

fn definition_subject(definition: &PersistentPropertyProjectionDefinition) -> ProjectionSubject {
    match definition.kind {
        PersistentPropertyProjectionKind::RelationshipEquality
        | PersistentPropertyProjectionKind::RelationshipRange => {
            ProjectionSubject::Relationship(RelTypeId(definition.label_id.0))
        }
        PersistentPropertyProjectionKind::Equality
        | PersistentPropertyProjectionKind::Range
        | PersistentPropertyProjectionKind::FullText
        | PersistentPropertyProjectionKind::CompositeEquality => {
            ProjectionSubject::Node(definition.label_id)
        }
    }
}

fn kind_tag(kind: PersistentPropertyProjectionKind) -> u8 {
    match kind {
        PersistentPropertyProjectionKind::Equality => 3,
        PersistentPropertyProjectionKind::Range => 1,
        PersistentPropertyProjectionKind::FullText => 2,
        PersistentPropertyProjectionKind::CompositeEquality => 4,
        PersistentPropertyProjectionKind::RelationshipEquality => 5,
        PersistentPropertyProjectionKind::RelationshipRange => 6,
    }
}

fn kind_order_tag(kind: PersistentPropertyProjectionKind) -> u8 {
    match kind {
        PersistentPropertyProjectionKind::Equality => 1,
        PersistentPropertyProjectionKind::Range => 2,
        PersistentPropertyProjectionKind::FullText => 3,
        PersistentPropertyProjectionKind::CompositeEquality => 4,
        PersistentPropertyProjectionKind::RelationshipEquality => 5,
        PersistentPropertyProjectionKind::RelationshipRange => 6,
    }
}

fn property_projection_descriptor_prefix(
    kind: PersistentPropertyProjectionKind,
    label_id: LabelId,
    property: &str,
) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(7usize.saturating_add(property.len()));
    encoded.push(kind_order_tag(kind));
    encoded.extend_from_slice(&label_id.0.to_be_bytes());
    for byte in property.as_bytes() {
        if *byte == 0 {
            encoded.extend_from_slice(&[0, 1]);
        } else {
            encoded.push(*byte);
        }
    }
    encoded.extend_from_slice(&[0, 0]);
    encoded
}

fn descriptor_length_overflow(field: &str) -> PersistentPropertyProjectionError {
    PersistentPropertyProjectionError::Corrupt(format!(
        "property projection descriptor {field} length overflows usize"
    ))
}

fn kind_from_tag(
    tag: u8,
) -> Result<PersistentPropertyProjectionKind, PersistentPropertyProjectionError> {
    match tag {
        1 => Ok(PersistentPropertyProjectionKind::Range),
        2 => Ok(PersistentPropertyProjectionKind::FullText),
        3 => Ok(PersistentPropertyProjectionKind::Equality),
        4 => Ok(PersistentPropertyProjectionKind::CompositeEquality),
        5 => Ok(PersistentPropertyProjectionKind::RelationshipEquality),
        6 => Ok(PersistentPropertyProjectionKind::RelationshipRange),
        _ => Err(PersistentPropertyProjectionError::Corrupt(format!(
            "invalid property projection kind {tag}"
        ))),
    }
}

fn decode_composite_property_identity(
    identity: &str,
) -> Result<Vec<String>, PersistentPropertyProjectionError> {
    let encoded = identity
        .strip_prefix(COMPOSITE_PROPERTY_IDENTITY_PREFIX)
        .and_then(|suffix| suffix.strip_prefix(':'))
        .ok_or_else(|| {
            PersistentPropertyProjectionError::Corrupt(
                "composite property projection has an invalid identity prefix".to_string(),
            )
        })?;
    let properties = encoded
        .split(':')
        .map(|property| decode_utf8_hex(property, "composite property identity"))
        .collect::<Result<Vec<_>, _>>()?;
    if properties.len() < 2 {
        return Err(PersistentPropertyProjectionError::Corrupt(
            "composite property projection identity has fewer than two properties".to_string(),
        ));
    }
    Ok(properties)
}

fn write_string(
    writer: &mut impl Write,
    value: &str,
) -> Result<(), PersistentPropertyProjectionError> {
    let length = u32::try_from(value.len()).map_err(|_| {
        PersistentPropertyProjectionError::Corrupt(
            "property projection string exceeds u32".to_string(),
        )
    })?;
    writer.write_all(&length.to_le_bytes())?;
    writer.write_all(value.as_bytes())?;
    Ok(())
}

fn read_bounded_string(
    reader: &mut impl Read,
    max_bytes: u64,
) -> Result<String, PersistentPropertyProjectionError> {
    let length = read_u32(reader)? as usize;
    if length as u64 > max_bytes {
        return Err(PersistentPropertyProjectionError::Corrupt(format!(
            "property projection spill string uses {length} bytes, exceeding {max_bytes}"
        )));
    }
    let mut bytes = vec![0u8; length];
    reader.read_exact(&mut bytes)?;
    String::from_utf8(bytes).map_err(|error| {
        PersistentPropertyProjectionError::Corrupt(format!(
            "property projection spill string is not UTF-8: {error}"
        ))
    })
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn decode_hex(value: &str, name: &str) -> Result<Vec<u8>, PersistentPropertyProjectionError> {
    if !value.len().is_multiple_of(2) {
        return Err(PersistentPropertyProjectionError::Corrupt(format!(
            "property projection {name} has an invalid hexadecimal length"
        )));
    }
    (0..value.len())
        .step_by(2)
        .map(|offset| {
            value
                .get(offset..offset + 2)
                .and_then(|pair| u8::from_str_radix(pair, 16).ok())
                .ok_or_else(|| {
                    PersistentPropertyProjectionError::Corrupt(format!(
                        "property projection {name} has invalid hexadecimal data"
                    ))
                })
        })
        .collect()
}

fn decode_utf8_hex(value: &str, name: &str) -> Result<String, PersistentPropertyProjectionError> {
    String::from_utf8(decode_hex(value, name)?).map_err(|error| {
        PersistentPropertyProjectionError::Corrupt(format!(
            "property projection {name} is not UTF-8: {error}"
        ))
    })
}

fn parse_u8(value: &str, name: &str) -> Result<u8, PersistentPropertyProjectionError> {
    value.parse().map_err(|_| {
        PersistentPropertyProjectionError::Corrupt(format!(
            "property projection manifest has invalid {name}: {value}"
        ))
    })
}

fn parse_u32(value: &str, name: &str) -> Result<u32, PersistentPropertyProjectionError> {
    value.parse().map_err(|_| {
        PersistentPropertyProjectionError::Corrupt(format!(
            "property projection manifest has invalid {name}: {value}"
        ))
    })
}

fn parse_u64(value: &str, name: &str) -> Result<u64, PersistentPropertyProjectionError> {
    value.parse().map_err(|_| {
        PersistentPropertyProjectionError::Corrupt(format!(
            "property projection manifest has invalid {name}: {value}"
        ))
    })
}

fn required<T>(value: Option<T>, name: &str) -> Result<T, PersistentPropertyProjectionError> {
    value.ok_or_else(|| {
        PersistentPropertyProjectionError::Corrupt(format!(
            "property projection manifest is missing {name}"
        ))
    })
}

fn read_u32(reader: &mut impl Read) -> Result<u32, PersistentPropertyProjectionError> {
    let mut bytes = [0u8; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64(reader: &mut impl Read) -> Result<u64, PersistentPropertyProjectionError> {
    let mut bytes = [0u8; 8];
    reader.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_exact(&mut self, length: usize) -> Result<&'a [u8], PersistentPropertyProjectionError> {
        let end = self.offset.checked_add(length).ok_or_else(|| {
            PersistentPropertyProjectionError::Corrupt(
                "property projection cursor offset overflow".to_string(),
            )
        })?;
        let bytes = self.bytes.get(self.offset..end).ok_or_else(|| {
            PersistentPropertyProjectionError::Corrupt(
                "property projection block ended before its declared length".to_string(),
            )
        })?;
        self.offset = end;
        Ok(bytes)
    }

    fn read_u8(&mut self) -> Result<u8, PersistentPropertyProjectionError> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u32(&mut self) -> Result<u32, PersistentPropertyProjectionError> {
        Ok(u32::from_le_bytes(
            self.read_exact(4)?.try_into().expect("fixed-width u32"),
        ))
    }

    fn read_u64(&mut self) -> Result<u64, PersistentPropertyProjectionError> {
        Ok(u64::from_le_bytes(
            self.read_exact(8)?.try_into().expect("fixed-width u64"),
        ))
    }

    fn read_string(&mut self) -> Result<String, PersistentPropertyProjectionError> {
        let length = self.read_u32()? as usize;
        String::from_utf8(self.read_exact(length)?.to_vec()).map_err(|error| {
            PersistentPropertyProjectionError::Corrupt(format!(
                "property projection block string is not UTF-8: {error}"
            ))
        })
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

fn write_hashed(
    writer: &mut impl Write,
    digest: &mut IntegrityHasher,
    bytes: &[u8],
) -> Result<(), PersistentPropertyProjectionError> {
    writer.write_all(bytes)?;
    digest.update(bytes);
    Ok(())
}

fn write_double_hashed(
    writer: &mut impl Write,
    artifact_digest: &mut IntegrityHasher,
    block_digest: &mut Crc32cHasher,
    bytes: &[u8],
) -> Result<(), PersistentPropertyProjectionError> {
    writer.write_all(bytes)?;
    artifact_digest.update(bytes);
    block_digest.update(bytes);
    Ok(())
}

#[cfg(test)]
mod hex_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};

    fn node(id: u64, rank: i64, text: &str) -> NodeRecord {
        NodeRecord {
            id: NodeId(id),
            labels: BTreeSet::from([LabelId(1)]),
            properties: BTreeMap::from([
                ("rank".to_string(), Value::Int(rank)),
                ("text".to_string(), Value::String(text.to_string())),
            ]),
        }
    }

    fn relationship(id: u64, rank: i64) -> RelRecord {
        RelRecord {
            id: RelId(id),
            source: NodeId(1),
            target: NodeId(2),
            rel_type: RelTypeId(1),
            properties: BTreeMap::from([("rank".to_string(), Value::Int(rank))]),
        }
    }

    fn test_descriptor_paths(root: &Path, stem: &str) -> GraphDescriptorTreePaths {
        GraphDescriptorTreePaths::new(
            root.join(format!("{stem}-descriptors.pages.hawdb")),
            root.join(format!("{stem}-descriptors.root.hawdb")),
        )
    }

    fn descriptor(
        block_id: u64,
        kind: PersistentPropertyProjectionKind,
        label_id: u32,
        property: &str,
        min_key: Value,
        max_key: Value,
    ) -> PersistentPropertyProjectionBlockDescriptor {
        PersistentPropertyProjectionBlockDescriptor {
            block_id,
            label_id: LabelId(label_id),
            property: property.to_string(),
            kind,
            min_key,
            max_key,
            offset: 24,
            length: NonZeroU64::new(128).unwrap(),
            content_digest: ContentDigest(91),
            entry_count: 3,
        }
    }

    #[test]
    fn descriptor_tree_entry_codec_is_order_preserving_and_symmetric() {
        let descriptors = [
            descriptor(
                BLOCK_ID_BASE,
                PersistentPropertyProjectionKind::Equality,
                1,
                "a",
                Value::Int(1),
                Value::Int(4),
            ),
            descriptor(
                BLOCK_ID_BASE + 1,
                PersistentPropertyProjectionKind::Equality,
                1,
                "a\0b",
                Value::Int(5),
                Value::Int(8),
            ),
            descriptor(
                BLOCK_ID_BASE + 2,
                PersistentPropertyProjectionKind::Range,
                1,
                "a",
                Value::Int(1),
                Value::Int(8),
            ),
        ];
        let keys = descriptors
            .iter()
            .map(PersistentPropertyProjectionBlockDescriptor::descriptor_tree_key)
            .collect::<Vec<_>>();
        assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
        for (descriptor, key) in descriptors.iter().zip(&keys) {
            let encoded = descriptor.encode_descriptor_tree_value().unwrap();
            assert_eq!(
                PersistentPropertyProjectionBlockDescriptor::decode_descriptor_tree_entry(
                    key, &encoded,
                )
                .unwrap(),
                *descriptor
            );
            let mut mismatched_key = key.clone();
            *mismatched_key.last_mut().unwrap() ^= 1;
            assert!(
                PersistentPropertyProjectionBlockDescriptor::decode_descriptor_tree_entry(
                    &mismatched_key,
                    &encoded,
                )
                .is_err()
            );
        }
    }

    #[test]
    fn external_projection_round_trips_node_composite_and_relationship_candidates() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "hawdb-property-projection-{}-{nonce}",
            std::process::id(),
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let path = root.join("projection.hawdb");
        let descriptor_paths = GraphDescriptorTreePaths::new(
            root.join("projection-descriptors.pages.hawdb"),
            root.join("projection-descriptors.root.hawdb"),
        );
        let config = PersistentPropertyProjectionConfig {
            memory_budget_bytes: NonZeroU64::new(256).unwrap(),
            max_merge_fan_in: NonZeroUsize::new(2).unwrap(),
            target_block_bytes: NonZeroU64::new(128).unwrap(),
            ..PersistentPropertyProjectionConfig::default()
        };
        let composite_properties = vec!["rank".to_string(), "text".to_string()];
        let definitions = vec![
            PersistentPropertyProjectionDefinition {
                label_id: LabelId(1),
                property: "rank".to_string(),
                kind: PersistentPropertyProjectionKind::Equality,
                complete: false,
            },
            PersistentPropertyProjectionDefinition {
                label_id: LabelId(1),
                property: "rank".to_string(),
                kind: PersistentPropertyProjectionKind::Range,
                complete: false,
            },
            PersistentPropertyProjectionDefinition {
                label_id: LabelId(1),
                property: "text".to_string(),
                kind: PersistentPropertyProjectionKind::FullText,
                complete: false,
            },
            PersistentPropertyProjectionDefinition {
                label_id: LabelId(1),
                property: persistent_composite_property_identity(&composite_properties).unwrap(),
                kind: PersistentPropertyProjectionKind::CompositeEquality,
                complete: false,
            },
            PersistentPropertyProjectionDefinition {
                label_id: LabelId(1),
                property: "rank".to_string(),
                kind: PersistentPropertyProjectionKind::RelationshipEquality,
                complete: false,
            },
            PersistentPropertyProjectionDefinition {
                label_id: LabelId(1),
                property: "rank".to_string(),
                kind: PersistentPropertyProjectionKind::RelationshipRange,
                complete: false,
            },
        ];
        let output = PersistentPropertyProjectionWriter::new(config)
            .write_fallible(
                &path,
                ManifestGeneration(2),
                11,
                definitions.clone(),
                vec![
                    Ok(PersistentPropertyProjectionRecord::Node(node(
                        1,
                        10,
                        "Graph Memory",
                    ))),
                    Ok(PersistentPropertyProjectionRecord::Node(node(
                        2, 20, "Other",
                    ))),
                    Ok(PersistentPropertyProjectionRecord::Node(node(
                        3,
                        30,
                        "Memory Graph",
                    ))),
                    Ok(PersistentPropertyProjectionRecord::Relationship(
                        relationship(7, 20),
                    )),
                ],
                PersistentPropertyProjectionDescriptorTree::new(
                    descriptor_paths.clone(),
                    GraphDescriptorTreeBuildConfig::default(),
                ),
            )
            .unwrap();
        assert!(output.report.spill_run_count > 1);
        let descriptor_tree = &output.descriptor_tree;
        assert_eq!(
            descriptor_tree.root.kind,
            GraphDescriptorKind::PropertyProjection
        );
        assert_eq!(
            descriptor_tree.root.descriptor_count,
            output.report.block_count
        );
        let reopened_descriptor_root = crate::GraphDescriptorTreeRootReader::open_bound(
            descriptor_paths.clone(),
            output.manifest.descriptor_generation_artifacts(),
            GraphDescriptorTreeBuildConfig::default(),
        )
        .unwrap();
        assert_eq!(reopened_descriptor_root.root(), &descriptor_tree.root);
        let data_before = fs::read(&path).unwrap();
        let pages_before = fs::read(&descriptor_paths.page_artifact).unwrap();
        let root_before = fs::read(&descriptor_paths.root_manifest).unwrap();
        let overwrite = PersistentPropertyProjectionWriter::new(config)
            .write_fallible(
                &path,
                ManifestGeneration(2),
                11,
                definitions,
                vec![Ok(PersistentPropertyProjectionRecord::Node(node(
                    9,
                    90,
                    "replacement",
                )))],
                PersistentPropertyProjectionDescriptorTree::new(
                    descriptor_paths.clone(),
                    GraphDescriptorTreeBuildConfig::default(),
                ),
            )
            .expect_err("same-generation descriptor artifacts are immutable");
        assert!(overwrite.to_string().contains("already exists"));
        assert_eq!(fs::read(&path).unwrap(), data_before);
        assert_eq!(
            fs::read(&descriptor_paths.page_artifact).unwrap(),
            pages_before
        );
        assert_eq!(
            fs::read(&descriptor_paths.root_manifest).unwrap(),
            root_before
        );
        let encoded_manifest = output.manifest.encode().unwrap();
        assert!(!encoded_manifest
            .lines()
            .any(|line| line.starts_with("block\t")));
        assert!(output.manifest.block_count >= 8);
        assert!(encoded_manifest.len() < 4 * 1024);
        let manifest = PersistentPropertyProjectionManifest::decode(&encoded_manifest).unwrap();
        let block_count = manifest.block_count;
        let cache = Arc::new(SegmentCache::new(1024 * 1024));
        let reader = PersistentPropertyProjectionReader::open(
            &path,
            manifest,
            PersistentPropertyProjectionDescriptorTree::new(
                descriptor_paths,
                GraphDescriptorTreeBuildConfig::default(),
            ),
            Arc::clone(&cache),
            StoreId(4),
            NonZeroU64::new(1024 * 1024).unwrap(),
        )
        .unwrap();
        assert_eq!(cache.snapshot().resident_bytes, 0);
        let mut equality = Vec::new();
        let (equality_report, _) = reader
            .scan_equality_candidates(LabelId(1), "rank", &Value::Int(20), |id| {
                equality.push(id.0);
                Ok(CanonicalScanControl::Continue)
            })
            .unwrap();
        assert_eq!(equality, vec![2]);
        assert_eq!(equality_report.blocks_read, 1);
        assert!(equality_report.blocks_considered < block_count);
        assert!(equality_report.blocks_read < block_count);
        assert!(equality_report.descriptor_pages_visited > 0);
        assert!(cache.snapshot().entry_count >= 2);
        equality.clear();
        let (warm_equality_report, _) = reader
            .scan_equality_candidates(LabelId(1), "rank", &Value::Int(20), |id| {
                equality.push(id.0);
                Ok(CanonicalScanControl::Continue)
            })
            .unwrap();
        assert_eq!(equality, vec![2]);
        assert!(warm_equality_report.descriptor_cache_hits > 0);
        assert_eq!(warm_equality_report.descriptor_storage_bytes_read, 0);
        assert_eq!(warm_equality_report.cache_hits, 1);
        let mut range = Vec::new();
        reader
            .scan_range_candidates(
                LabelId(1),
                "rank",
                Some(&(Value::Int(15), true)),
                Some(&(Value::Int(30), false)),
                |id| {
                    range.push(id.0);
                    Ok(CanonicalScanControl::Continue)
                },
            )
            .unwrap();
        assert_eq!(range, vec![2]);
        let mut text = Vec::new();
        reader
            .scan_full_text_token_candidates(LabelId(1), "text", "mem", |id| {
                text.push(id.0);
                Ok(CanonicalScanControl::Continue)
            })
            .unwrap();
        assert_eq!(text, vec![1, 3]);
        assert!(reader
            .manifest()
            .supports_composite_equality(LabelId(1), &composite_properties));
        let mut composite = Vec::new();
        let (composite_report, _) = reader
            .scan_composite_equality_candidates(
                LabelId(1),
                &composite_properties,
                &[&Value::Int(20), &Value::String("Other".to_string())],
                |id| {
                    composite.push(id.0);
                    Ok(CanonicalScanControl::Continue)
                },
            )
            .unwrap();
        assert_eq!(composite, vec![2]);
        assert_eq!(composite_report.candidates_returned, 1);

        composite.clear();
        reader
            .scan_composite_equality_candidates(
                LabelId(1),
                &composite_properties,
                &[&Value::Int(20), &Value::String("Graph Memory".to_string())],
                |id| {
                    composite.push(id.0);
                    Ok(CanonicalScanControl::Continue)
                },
            )
            .unwrap();
        assert!(composite.is_empty());
        let mut relationships = Vec::new();
        reader
            .scan_relationship_equality_candidates(RelTypeId(1), "rank", &Value::Int(20), |id| {
                relationships.push(id.0);
                Ok(CanonicalScanControl::Continue)
            })
            .unwrap();
        assert_eq!(relationships, vec![7]);
        relationships.clear();
        reader
            .scan_relationship_range_candidates(
                RelTypeId(1),
                "rank",
                Some(&(Value::Int(15), true)),
                Some(&(Value::Int(25), true)),
                |id| {
                    relationships.push(id.0);
                    Ok(CanonicalScanControl::Continue)
                },
            )
            .unwrap();
        assert_eq!(relationships, vec![7]);
        let scrub = reader.deep_scrub().unwrap();
        assert_eq!(scrub.descriptors_checked, block_count);
        assert_eq!(scrub.projection_blocks_checked, block_count);
        assert_eq!(
            scrub.projection_entries_checked,
            reader.manifest().entry_count
        );
        assert_eq!(
            scrub.projection_bytes_hashed,
            reader.manifest().artifact_len
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn deep_scrub_finds_cold_block_corruption_and_poisons_the_reader() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "hawdb-property-projection-scrub-{}-{nonce}",
            std::process::id(),
        ));
        fs::create_dir_all(&root).unwrap();
        let path = root.join("projection.hawdb");
        let descriptor_paths = test_descriptor_paths(&root, "projection");
        let definitions = vec![
            PersistentPropertyProjectionDefinition {
                label_id: LabelId(1),
                property: "rank".to_string(),
                kind: PersistentPropertyProjectionKind::Equality,
                complete: false,
            },
            PersistentPropertyProjectionDefinition {
                label_id: LabelId(1),
                property: "rank".to_string(),
                kind: PersistentPropertyProjectionKind::Range,
                complete: false,
            },
        ];
        let output =
            PersistentPropertyProjectionWriter::new(PersistentPropertyProjectionConfig::default())
                .write_fallible(
                    &path,
                    ManifestGeneration(8),
                    21,
                    definitions,
                    (0..64).map(|id| {
                        Ok(PersistentPropertyProjectionRecord::Node(node(
                            id + 1,
                            id as i64,
                            "payload",
                        )))
                    }),
                    PersistentPropertyProjectionDescriptorTree::new(
                        descriptor_paths.clone(),
                        GraphDescriptorTreeBuildConfig::default(),
                    ),
                )
                .unwrap();
        let cache = Arc::new(SegmentCache::new(1024 * 1024));
        let reader = PersistentPropertyProjectionReader::open(
            &path,
            output.manifest.clone(),
            PersistentPropertyProjectionDescriptorTree::new(
                descriptor_paths.clone(),
                GraphDescriptorTreeBuildConfig::default(),
            ),
            Arc::clone(&cache),
            StoreId(17),
            NonZeroU64::new(1024 * 1024).unwrap(),
        )
        .unwrap();
        reader
            .scan_equality_candidates(LabelId(1), "rank", &Value::Int(7), |_| {
                Ok(CanonicalScanControl::Continue)
            })
            .unwrap();

        let root_reader = GraphDescriptorTreeRootReader::open_bound(
            descriptor_paths,
            output.manifest.descriptor_generation_artifacts(),
            GraphDescriptorTreeBuildConfig::default(),
        )
        .unwrap();
        let descriptor_reader = GraphDescriptorTreeDemandReader::open(
            root_reader,
            GraphDescriptorTreeBuildConfig::default(),
            Arc::new(SegmentCache::new(0)),
            StoreId(18),
        )
        .unwrap();
        let mut cold_block = None;
        descriptor_reader
            .scan_prefix(
                &property_projection_descriptor_prefix(
                    PersistentPropertyProjectionKind::Range,
                    LabelId(1),
                    "rank",
                ),
                GraphDescriptorTreeReadLimits::default(),
                |key, value| {
                    cold_block = Some(
                        PersistentPropertyProjectionBlockDescriptor::decode_descriptor_tree_entry(
                            key, value,
                        )
                        .unwrap(),
                    );
                    Ok(GraphDescriptorTreeScanControl::Stop)
                },
            )
            .unwrap();
        let cold_block = cold_block.expect("range descriptor exists");
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.seek(SeekFrom::Start(cold_block.offset + 25)).unwrap();
        file.write_all(&[0xff]).unwrap();
        file.sync_all().unwrap();

        let error = reader.deep_scrub().unwrap_err();
        assert!(error.to_string().contains("checksum mismatch"));
        assert!(reader.is_poisoned());
        let poisoned = reader
            .scan_equality_candidates(LabelId(1), "rank", &Value::Int(7), |_| {
                Ok(CanonicalScanControl::Continue)
            })
            .unwrap_err();
        assert!(poisoned.to_string().contains("poisoned"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn composite_property_identity_is_unambiguous_and_validated() {
        let properties = vec!["a:b".to_string(), "".to_string(), "\u{1f9f5}".to_string()];
        let identity = persistent_composite_property_identity(&properties).unwrap();
        assert_eq!(
            decode_composite_property_identity(&identity).unwrap(),
            properties
        );

        let error = persistent_composite_property_identity(&["only".to_string()]).unwrap_err();
        assert!(error.to_string().contains("at least two properties"));
        let error =
            decode_composite_property_identity("hawdb-composite-property-v1:zz:61").unwrap_err();
        assert!(error.to_string().contains("invalid hexadecimal data"));
    }

    #[test]
    fn composite_range_projection_prunes_blocks_and_preserves_bound_semantics() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "hawdb-composite-range-projection-{}-{nonce}",
            std::process::id(),
        ));
        fs::create_dir_all(&root).unwrap();
        let path = root.join("projection.hawdb");
        let descriptor_paths = test_descriptor_paths(&root, "projection");
        let properties = vec!["rank".to_string(), "text".to_string()];
        let definition = PersistentPropertyProjectionDefinition {
            label_id: LabelId(1),
            property: persistent_composite_property_identity(&properties).unwrap(),
            kind: PersistentPropertyProjectionKind::CompositeEquality,
            complete: false,
        };
        let output = PersistentPropertyProjectionWriter::new(PersistentPropertyProjectionConfig {
            memory_budget_bytes: NonZeroU64::new(512).unwrap(),
            max_merge_fan_in: NonZeroUsize::new(2).unwrap(),
            target_block_bytes: NonZeroU64::new(128).unwrap(),
            ..PersistentPropertyProjectionConfig::default()
        })
        .write_fallible(
            &path,
            ManifestGeneration(9),
            31,
            vec![definition],
            (0..96).map(|index| {
                Ok(PersistentPropertyProjectionRecord::Node(node(
                    index + 1,
                    (index / 32) as i64,
                    &format!("item-{index:03}"),
                )))
            }),
            PersistentPropertyProjectionDescriptorTree::new(
                descriptor_paths.clone(),
                GraphDescriptorTreeBuildConfig::default(),
            ),
        )
        .unwrap();
        assert!(output.manifest.block_count > 8);

        let reader = PersistentPropertyProjectionReader::open(
            &path,
            output.manifest,
            PersistentPropertyProjectionDescriptorTree::new(
                descriptor_paths,
                GraphDescriptorTreeBuildConfig::default(),
            ),
            Arc::new(SegmentCache::new(1024 * 1024)),
            StoreId(23),
            NonZeroU64::new(1024 * 1024).unwrap(),
        )
        .unwrap();
        let lower = (Value::String("item-040".to_string()), false);
        let upper = (Value::String("item-048".to_string()), false);
        let mut candidates = Vec::new();
        let (report, _) = reader
            .scan_composite_range_candidates(
                LabelId(1),
                &properties,
                &[&Value::Int(1)],
                Some(&lower),
                Some(&upper),
                |id| {
                    candidates.push(id.0);
                    Ok(CanonicalScanControl::Continue)
                },
            )
            .unwrap();

        assert_eq!(candidates, (42..=48).collect::<Vec<_>>());
        assert!(report.blocks_pruned > 0);
        assert!(report.blocks_read < report.blocks_considered);
        assert_eq!(report.candidates_returned, 7);

        let no_prefix = reader
            .scan_composite_range_candidates(
                LabelId(1),
                &properties,
                &[],
                Some(&lower),
                None,
                |_| Ok(CanonicalScanControl::Continue),
            )
            .unwrap_err();
        assert!(no_prefix.to_string().contains("leading equality prefix"));
        let no_bound = reader
            .scan_composite_range_candidates(
                LabelId(1),
                &properties,
                &[&Value::Int(1)],
                None,
                None,
                |_| Ok(CanonicalScanControl::Continue),
            )
            .unwrap_err();
        assert!(no_bound.to_string().contains("at least one range bound"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn oversized_composite_key_disables_the_projection_without_partial_coverage() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "hawdb-oversized-composite-projection-{}-{nonce}",
            std::process::id(),
        ));
        fs::create_dir_all(&root).unwrap();
        let path = root.join("projection.hawdb");
        let descriptor_paths = test_descriptor_paths(&root, "projection");
        let properties = vec!["rank".to_string(), "text".to_string()];
        let definition = PersistentPropertyProjectionDefinition {
            label_id: LabelId(1),
            property: persistent_composite_property_identity(&properties).unwrap(),
            kind: PersistentPropertyProjectionKind::CompositeEquality,
            complete: false,
        };
        let output = PersistentPropertyProjectionWriter::new(PersistentPropertyProjectionConfig {
            max_index_key_bytes: NonZeroU64::new(8).unwrap(),
            ..PersistentPropertyProjectionConfig::default()
        })
        .write_fallible(
            &path,
            ManifestGeneration(3),
            12,
            vec![definition],
            vec![Ok(PersistentPropertyProjectionRecord::Node(node(
                1,
                10,
                "long composite value",
            )))],
            PersistentPropertyProjectionDescriptorTree::new(
                descriptor_paths.clone(),
                GraphDescriptorTreeBuildConfig::default(),
            ),
        )
        .unwrap();

        assert_eq!(output.manifest.entry_count, 0);
        assert!(!output
            .manifest
            .supports_composite_equality(LabelId(1), &properties));
        let reader = PersistentPropertyProjectionReader::open(
            &path,
            output.manifest,
            PersistentPropertyProjectionDescriptorTree::new(
                descriptor_paths,
                GraphDescriptorTreeBuildConfig::default(),
            ),
            Arc::new(SegmentCache::new(1024)),
            StoreId(5),
            NonZeroU64::new(1024).unwrap(),
        )
        .unwrap();
        let error = reader
            .scan_composite_equality_candidates(
                LabelId(1),
                &properties,
                &[
                    &Value::Int(10),
                    &Value::String("long composite value".to_string()),
                ],
                |_| Ok(CanonicalScanControl::Continue),
            )
            .unwrap_err();
        assert!(error.to_string().contains("unavailable or incomplete"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn definition_admission_rejects_before_artifact_creation() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "hawdb-property-projection-definition-budget-{}-{nonce}",
            std::process::id(),
        ));
        fs::create_dir_all(&root).unwrap();
        let definitions = vec![
            PersistentPropertyProjectionDefinition {
                label_id: LabelId(1),
                property: "rank".to_string(),
                kind: PersistentPropertyProjectionKind::RelationshipEquality,
                complete: false,
            },
            PersistentPropertyProjectionDefinition {
                label_id: LabelId(1),
                property: "rank".to_string(),
                kind: PersistentPropertyProjectionKind::RelationshipRange,
                complete: false,
            },
        ];
        let count_path = root.join("count.hawdb");
        let count_descriptor_paths = test_descriptor_paths(&root, "count");
        let count_error =
            PersistentPropertyProjectionWriter::new(PersistentPropertyProjectionConfig {
                max_definition_count: NonZeroUsize::new(1).unwrap(),
                ..PersistentPropertyProjectionConfig::default()
            })
            .write_fallible(
                &count_path,
                ManifestGeneration(1),
                1,
                definitions.clone(),
                Vec::<Result<_, PersistentPropertyProjectionError>>::new(),
                PersistentPropertyProjectionDescriptorTree::new(
                    count_descriptor_paths,
                    GraphDescriptorTreeBuildConfig::default(),
                ),
            )
            .unwrap_err();
        assert!(matches!(
            count_error,
            PersistentPropertyProjectionError::DefinitionCountBudgetExceeded {
                required_definitions: 2,
                max_definitions: 1,
            }
        ));
        assert!(!count_path.exists());

        let bytes_path = root.join("bytes.hawdb");
        let bytes_descriptor_paths = test_descriptor_paths(&root, "bytes");
        let bytes_error =
            PersistentPropertyProjectionWriter::new(PersistentPropertyProjectionConfig {
                max_definition_bytes: NonZeroU64::new(1).unwrap(),
                ..PersistentPropertyProjectionConfig::default()
            })
            .write_fallible(
                &bytes_path,
                ManifestGeneration(1),
                1,
                definitions,
                Vec::<Result<_, PersistentPropertyProjectionError>>::new(),
                PersistentPropertyProjectionDescriptorTree::new(
                    bytes_descriptor_paths,
                    GraphDescriptorTreeBuildConfig::default(),
                ),
            )
            .unwrap_err();
        assert!(matches!(
            bytes_error,
            PersistentPropertyProjectionError::DefinitionBytesBudgetExceeded { max_bytes: 1, .. }
        ));
        assert!(!bytes_path.exists());
        fs::remove_dir_all(root).unwrap();
    }
}
